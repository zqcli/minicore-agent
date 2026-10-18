use super::*;

pub(crate) fn forward_loop_event(
    session_id: SessionId,
    envelope: minicore_runtime::LoopEventEnvelope,
    session: &Session,
) {
    session
        .shared
        .events
        .record_core_drops(envelope.dropped_before);
    // The terminal structured facts are read from the same ToolRef-keyed data
    // source as `tool.read`, before the envelope is consumed below.
    let execution = tool_execution_event(session_id, &envelope.event, session);
    if let Some(event) = map_loop_event(session_id, envelope.event, session) {
        session.shared.events.try_send(event);
    }
    if let Some(event) = execution {
        session.shared.events.try_send(event);
    }
}

/// Best-effort `tool_execution` event built from the same structured record
/// `tool.read` returns, so live and query clients cannot disagree.
fn tool_execution_event(
    session_id: SessionId,
    event: &minicore_runtime::LoopEvent,
    session: &Session,
) -> Option<AgentEvent> {
    let minicore_runtime::LoopEvent::ToolFinished {
        loop_id,
        request_index,
        call_id,
        outcome,
        ..
    } = event
    else {
        return None;
    };
    let tool_ref = crate::tool_data::ToolRef {
        session_id,
        loop_id: *loop_id,
        request_index: *request_index,
        tool_call_id: call_id.clone(),
    };
    // Record the authoritative terminal state before reading it back, so the
    // event never reports a stale non-terminal state.
    let data = session
        .shared
        .tool_data
        .finish_and_snapshot(&tool_ref, *outcome)?;
    Some(AgentEvent::ToolExecution {
        turn: TurnRef {
            session_id,
            loop_id: *loop_id,
        },
        data,
        meta: EventMeta {
            session_id,
            loop_id: Some(*loop_id),
            dropped_before: 0,
        },
    })
}

fn extract_loop_id(event: &minicore_runtime::LoopEvent) -> Option<LoopId> {
    match event {
        minicore_runtime::LoopEvent::Started { loop_id }
        | minicore_runtime::LoopEvent::RequestStarted { loop_id, .. }
        | minicore_runtime::LoopEvent::OutputDelta { loop_id, .. }
        | minicore_runtime::LoopEvent::ToolStarted { loop_id, .. }
        | minicore_runtime::LoopEvent::ToolProgress { loop_id, .. }
        | minicore_runtime::LoopEvent::ToolFinished { loop_id, .. }
        | minicore_runtime::LoopEvent::InteractionRequested { loop_id, .. }
        | minicore_runtime::LoopEvent::InteractionResolved { loop_id, .. }
        | minicore_runtime::LoopEvent::Finished { loop_id, .. } => Some(*loop_id),
        minicore_runtime::LoopEvent::StateChanged { state } => Some(state.loop_id),
        _ => None,
    }
}

fn map_loop_event(
    session_id: SessionId,
    event: minicore_runtime::LoopEvent,
    session: &Session,
) -> Option<AgentEvent> {
    let loop_id = extract_loop_id(&event)?;
    let turn = TurnRef {
        session_id,
        loop_id,
    };
    let meta = EventMeta {
        session_id,
        loop_id: Some(loop_id),
        dropped_before: 0,
    };
    match event {
        minicore_runtime::LoopEvent::Started { .. } => Some(AgentEvent::TurnStarted { turn, meta }),
        minicore_runtime::LoopEvent::StateChanged { .. } => Some(AgentEvent::SessionState {
            state: session.state(),
            meta,
        }),
        minicore_runtime::LoopEvent::RequestStarted {
            config_revision,
            model,
            reasoning,
            request_index,
            ..
        } => Some(AgentEvent::RequestStarted {
            turn,
            request_index,
            config_revision,
            model: model.as_str().to_owned(),
            reasoning,
            meta,
        }),
        minicore_runtime::LoopEvent::OutputDelta {
            request_index,
            channel,
            delta,
            ..
        } => Some(AgentEvent::OutputDelta {
            turn,
            request_index,
            channel: OutputChannel::from_runtime(channel),
            delta: delta.as_str().to_owned(),
            meta,
        }),
        minicore_runtime::LoopEvent::ToolStarted {
            request_index,
            call_id,
            tool_name,
            ..
        } => {
            // Runtime accepted the call; arguments are not available yet.
            session.shared.tool_data.note_requested(
                &crate::tool_data::ToolRef {
                    session_id,
                    loop_id,
                    request_index,
                    tool_call_id: call_id.clone(),
                },
                tool_name.as_str(),
            );
            Some(AgentEvent::ToolStarted {
                turn,
                request_index,
                tool_call_id: call_id,
                tool_name: tool_name.to_string(),
                meta,
            })
        }
        minicore_runtime::LoopEvent::ToolProgress {
            request_index,
            call_id,
            progress,
            ..
        } => {
            // A real `ToolContext.progress` phase is the only thing recorded
            // here; raw stream bytes never travel through progress messages.
            if let Some(message) = progress.message.as_ref() {
                session.shared.tool_data.note_phase(
                    &crate::tool_data::ToolRef {
                        session_id,
                        loop_id,
                        request_index,
                        tool_call_id: call_id.clone(),
                    },
                    message.as_str(),
                );
            }
            Some(AgentEvent::ToolProgress {
                turn,
                request_index,
                tool_call_id: call_id,
                progress: ToolProgressView::from(&progress),
                meta,
            })
        }
        minicore_runtime::LoopEvent::ToolFinished {
            request_index,
            call_id,
            outcome,
            output_bytes,
            ..
        } => {
            // The wrapper normally finishes before Runtime emits this event;
            // the lookup remains best-effort because the event and presentation
            // channels have independent delivery/drop semantics. Terminal
            // structured facts are recorded once by `tool_execution_event`.
            let (content, content_truncated) = session
                .presentation()
                .take_tool_result(
                    crate::tools::observe::RequestKey {
                        loop_id,
                        request_index,
                    },
                    &call_id,
                )
                .map_or((None, false), |(content, truncated)| {
                    (Some(content), truncated)
                });
            Some(AgentEvent::ToolFinished {
                turn,
                request_index,
                tool_call_id: call_id,
                result: ToolResultView {
                    outcome,
                    content_bytes: output_bytes,
                    content,
                    content_truncated,
                },
                meta,
            })
        }
        minicore_runtime::LoopEvent::InteractionRequested { interaction, .. } => {
            // `awaiting_policy` is recorded by the policy wrapper from the
            // exact ToolRef; the best-effort interaction event is not used to
            // guess which call is waiting.
            Some(AgentEvent::InteractionRequested {
                turn,
                interaction: (&interaction).into(),
                meta,
            })
        }
        minicore_runtime::LoopEvent::InteractionResolved { interaction_id, .. } => {
            Some(AgentEvent::InteractionResolved {
                turn,
                interaction_id,
                meta,
            })
        }
        // Runtime Finished is not an Agent TurnFinished: JSONL persistence and
        // history merge have not happened yet. Its drops were already recorded.
        minicore_runtime::LoopEvent::Finished { .. } => None,
        _ => None,
    }
}

impl LoopOutcomeView {
    pub(crate) fn from_report(report: &LoopReport) -> Self {
        match &report.outcome {
            minicore_runtime::LoopOutcome::Completed => Self::Completed,
            minicore_runtime::LoopOutcome::Cancelled(reason) => Self::Cancelled {
                reason: match reason {
                    minicore_runtime::CancelReason::User => CancelReasonView::User,
                    minicore_runtime::CancelReason::OwnerDropped => CancelReasonView::OwnerDropped,
                    minicore_runtime::CancelReason::Shutdown => CancelReasonView::Shutdown,
                    minicore_runtime::CancelReason::Deadline => CancelReasonView::Deadline,
                    _ => CancelReasonView::Unknown,
                },
            },
            minicore_runtime::LoopOutcome::Failed(failure) => Self::Failed {
                kind: loop_failure_kind(&failure.kind).to_owned(),
                model_error: failure.model_error().map(ModelErrorView::from_model),
            },
            _ => Self::Unknown,
        }
    }
}

fn loop_failure_kind(kind: &minicore_runtime::LoopFailureKind) -> &'static str {
    match kind {
        minicore_runtime::LoopFailureKind::Prompt => "prompt",
        minicore_runtime::LoopFailureKind::Model => "model",
        minicore_runtime::LoopFailureKind::InvalidModelResponse => "invalid_model_response",
        minicore_runtime::LoopFailureKind::OutputLimit => "output_limit",
        minicore_runtime::LoopFailureKind::Refused => "refused",
        minicore_runtime::LoopFailureKind::ContentFiltered => "content_filtered",
        minicore_runtime::LoopFailureKind::Policy => "policy",
        minicore_runtime::LoopFailureKind::Interaction => "interaction",
        minicore_runtime::LoopFailureKind::MaxToolRounds => "max_tool_rounds",
        minicore_runtime::LoopFailureKind::Internal => "internal",
        _ => "internal",
    }
}

impl Session {
    pub(super) fn emit_state(&self) {
        let state = self.state();
        let loop_id = state
            .active_loop
            .as_ref()
            .map(|loop_state| loop_state.loop_id);
        self.shared.events.try_send(AgentEvent::SessionState {
            meta: EventMeta {
                session_id: state.session_id,
                loop_id,
                dropped_before: 0,
            },
            state,
        });
    }
}
