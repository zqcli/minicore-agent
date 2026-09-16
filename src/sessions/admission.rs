//! Startup preparation and admission for one submission.
use super::compact::run_compaction_inner;
use super::*;

/// Whether one submission needs a startup summary preparation.
enum AdmissionNeed {
    /// The request fits; start the loop directly.
    None,
    /// The request exceeds the runtime limits or the automatic trigger.
    Compact,
    /// System plus current User/Steer plus tool schemas already exceed the
    /// hard input ceiling; no summary can help without dropping constraints.
    Uncompressible,
}

/// Snapshot captured when automatic startup admission is reserved. Reloads
/// and other future-turn updates must not change the model/configuration of a
/// request that is already waiting for its admission decision.
struct AdmissionReservation {
    compaction: CompactionReservation,
    config: ExecutionConfig,
    options: LoopOptions,
    auto: AutoContext,
    system_prompt: Option<BoundedText>,
    /// The raw projected request was known to fit the hard model window before
    /// a trigger-started durable compaction attempt. It is a safe fallback if
    /// the optional summary attempt makes no progress.
    raw_fits_hard: bool,
}

impl AdmissionOperation {
    fn new(
        operation: Arc<CompactionOperation>,
    ) -> (Arc<Self>, watch::Receiver<Option<PreparedLoop>>) {
        let (result, receiver) = watch::channel(None);
        (
            Arc::new(Self {
                result,
                join: tokio::sync::Mutex::new(None),
                operation,
            }),
            receiver,
        )
    }

    pub(super) async fn join(&self) -> Result<(), AgentError> {
        let mut slot = self.join.lock().await;
        let Some(handle) = slot.as_mut() else {
            return Ok(());
        };
        let result = std::pin::Pin::new(handle).await;
        slot.take();
        result.map_err(|_| AgentError::Internal)
    }
}

impl Session {
    /// Decides whether the next submission must compact its settled history
    /// before a loop can start. The reservation has already captured the
    /// model/configuration, so reloads cannot change this request mid-flight.
    /// The irreducible minimum is checked with the real current input before a
    /// futile summary model call is started.
    async fn admission_needed(
        &self,
        input: &UserInput,
        reservation: &mut AdmissionReservation,
    ) -> Result<AdmissionNeed, PreparationFailure> {
        let operation = &reservation.compaction.operation;
        if operation.cancellation_requested() {
            return Err(PreparationFailure::Cancelled);
        }
        let deadline = reservation.compaction.deadline;
        if Instant::now() >= deadline {
            return Err(PreparationFailure::Compaction("timeout"));
        }
        let history = &reservation.compaction.history;
        let (projected, summary) = match self.shared.compaction.project(history) {
            Some(projection) => (projection.suffix.to_vec(), Some(projection.summary)),
            None => (history.to_vec(), None),
        };
        let read = reservation
            .compaction
            .workspace
            .read_prefix(crate::prompt::AGENTS_PATH, crate::prompt::MAX_AGENTS_BYTES);
        tokio::pin!(read);
        let prefix = tokio::select! {
            biased;
            _ = operation.cancellation.cancelled() => {
                return Err(PreparationFailure::Cancelled);
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(PreparationFailure::Compaction("timeout"));
            }
            result = &mut read => result,
        };
        let agents = match prefix {
            Ok(prefix) => Some(
                crate::prompt::decode_agents(&prefix).map_err(|_| PreparationFailure::Internal)?,
            ),
            Err(WorkspaceError::NotFound) => None,
            Err(_) => return Err(PreparationFailure::Internal),
        };
        if operation.cancellation_requested() {
            return Err(PreparationFailure::Cancelled);
        }
        let system_base = minicore_runtime::value::BoundedText::new(
            reservation.compaction.record.system_prompt.clone(),
        )
        .map_err(|_| PreparationFailure::Internal)?;
        let system = crate::prompt::build_system_prompt(&system_base, agents)
            .map_err(|_| PreparationFailure::Internal)?;
        reservation.system_prompt = Some(system.clone());
        let budget = reservation
            .auto
            .policy
            .budget(reservation.config.descriptor().context_window);
        let tools = &reservation.compaction.tool_schemas;
        // The irreducible request alone may already exceed the budget; no
        // summary could make it fit without dropping current user constraints.
        let minimal_tokens = crate::compaction::estimate_minimal(
            &system,
            &[input.as_text()],
            tools,
            reservation.compaction.record.reasoning,
            &*reservation.auto.budget,
        )
        .map_err(|_| PreparationFailure::Internal)?;
        let minimal_message_count =
            ((!system.is_empty()) as usize).saturating_add((!input.as_text().is_empty()) as usize);
        if Instant::now() >= deadline {
            return Err(PreparationFailure::Compaction("timeout"));
        }
        if minimal_tokens > budget.hard_tokens
            || minimal_message_count > reservation.options.limits.max_prompt_messages
        {
            return Ok(AdmissionNeed::Uncompressible);
        }

        let runtime_over_limit =
            !history_fits_runtime_limits(&projected, &reservation.options.limits);
        let projected_message_count = projected
            .len()
            .saturating_add((!system.is_empty()) as usize)
            .saturating_add(summary.is_some() as usize)
            .saturating_add((!input.as_text().is_empty()) as usize);
        let compacted_message_count =
            (!system.is_empty()) as usize + 1 + (!input.as_text().is_empty()) as usize;
        if (runtime_over_limit
            || projected_message_count > reservation.options.limits.max_prompt_messages)
            && compacted_message_count > reservation.options.limits.max_prompt_messages
        {
            return Ok(AdmissionNeed::Uncompressible);
        }
        // Do not compose/estimate a full request when the Runtime history
        // itself is already outside its structural admission limits. The
        // bounded utility summary is the recovery path for that history.
        if runtime_over_limit {
            return Ok(AdmissionNeed::Compact);
        }
        if projected_message_count > reservation.options.limits.max_prompt_messages {
            return Ok(AdmissionNeed::Compact);
        }

        // With no settled history there is nothing automatic compaction can
        // reduce. A request between trigger and hard is valid and must start
        // normally without a utility call.
        if projected.is_empty() && summary.is_none() {
            let estimate = crate::compaction::estimate_startup_exact(
                &system,
                None,
                &projected,
                input.as_text(),
                tools,
                reservation.compaction.record.reasoning,
                &*reservation.auto.budget,
            )
            .map_err(|_| PreparationFailure::Internal)?;
            self.shared
                .compaction
                .update_automatic(&operation.operation_id, |observation| {
                    observation.before_tokens = Some(estimate);
                    observation.after_tokens = Some(estimate);
                });
            self.shared.compaction.note_request_estimate(estimate);
            if estimate > budget.hard_tokens {
                return Ok(AdmissionNeed::Uncompressible);
            }
            if Instant::now() >= deadline {
                return Err(PreparationFailure::Compaction("timeout"));
            }
            return Ok(AdmissionNeed::None);
        }

        let request_safe = crate::compaction::startup_history_is_request_safe(&projected);
        let estimate = if request_safe {
            match crate::compaction::estimate_startup_exact(
                &system,
                summary.as_ref(),
                &projected,
                input.as_text(),
                tools,
                reservation.compaction.record.reasoning,
                &*reservation.auto.budget,
            ) {
                Ok(estimate) => estimate,
                // A request-safe history can still fail ModelRequest
                // validation because of an invalid value. Let compaction
                // replace it rather than sending it to Runtime.
                Err(_) => return Ok(AdmissionNeed::Compact),
            }
        } else {
            // A malformed/structurally invalid full history is not sent. A
            // durable summary can replace it, provided the minimum above
            // still fits the hard boundary.
            crate::compaction::estimate_startup(
                &system,
                summary.as_ref(),
                &projected,
                input.as_text(),
                tools,
                reservation.compaction.record.reasoning,
            )
            .map_err(|_| PreparationFailure::Internal)?
        };
        self.shared
            .compaction
            .update_automatic(&operation.operation_id, |observation| {
                observation.before_tokens = Some(estimate);
                observation.after_tokens = Some(estimate);
            });
        self.shared.compaction.note_request_estimate(estimate);
        if operation.cancellation_requested() {
            return Err(PreparationFailure::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(PreparationFailure::Compaction("timeout"));
        }
        if request_safe && estimate <= budget.hard_tokens {
            reservation.raw_fits_hard = true;
        }
        if !request_safe {
            return Ok(AdmissionNeed::Compact);
        }
        if estimate <= budget.trigger_tokens {
            Ok(AdmissionNeed::None)
        } else {
            Ok(AdmissionNeed::Compact)
        }
    }
}

impl Session {
    fn build_admission_execution(
        &self,
        input: UserInput,
        reservation: &AdmissionReservation,
        admission: &Arc<AdmissionOperation>,
    ) -> Result<ExecutionInput, AgentError> {
        {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing
                || inner.active.is_some()
                || !inner
                    .admission
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, admission))
                || !Arc::ptr_eq(&inner.history, &reservation.compaction.history)
            {
                return Err(AgentError::SessionBusy);
            }
        }
        let (history, summary) = match self
            .shared
            .compaction
            .project(&reservation.compaction.history)
        {
            Some(projection) => (projection.suffix.to_vec().into(), Some(projection.summary)),
            None => (Arc::clone(&reservation.compaction.history), None),
        };
        if !history_fits_runtime_limits(&history, &reservation.options.limits) {
            return Err(AgentError::HistoryTooLarge);
        }
        let system = reservation
            .system_prompt
            .as_ref()
            .ok_or(AgentError::InvalidState)?;
        let message_count = history
            .len()
            .saturating_add((!system.is_empty()) as usize)
            .saturating_add(summary.is_some() as usize)
            .saturating_add((!input.as_text().is_empty()) as usize);
        if message_count > reservation.options.limits.max_prompt_messages {
            return Err(AgentError::ContextUncompressible);
        }
        let estimate = crate::compaction::estimate_startup_exact(
            system,
            summary.as_ref(),
            &history,
            input.as_text(),
            &reservation.compaction.tool_schemas,
            reservation.compaction.record.reasoning,
            &*reservation.auto.budget,
        )
        .map_err(|_| AgentError::ContextUncompressible)?;
        if estimate > reservation.compaction.hard_tokens {
            return Err(AgentError::ContextUncompressible);
        }
        self.shared.compaction.update_automatic(
            &reservation.compaction.operation.operation_id,
            |observation| {
                observation.after_tokens = Some(estimate);
            },
        );
        self.shared.compaction.note_request_estimate(estimate);
        let config = self.bind_execution_config(
            reservation.config.clone(),
            summary.clone(),
            reservation.compaction.record.system_prompt.clone(),
            Some(reservation.auto.clone()),
        )?;
        Ok(ExecutionInput {
            request: LoopRequest::new(history, input, config),
            options: reservation.options.clone(),
            summary,
        })
    }
}

impl Session {
    /// Reserves a Session-owned admission preparation, spawning the summary
    /// worker. The returned receiver yields the loop it finally created.
    pub(super) async fn start_admission(
        &self,
        input: UserInput,
    ) -> Result<PreparationWaiter, AgentError> {
        let operation_id = {
            let mut inner = self.shared.inner.lock().unwrap();
            let Some(next) = inner.next_admission_id.checked_add(1) else {
                return Err(AgentError::InvalidState);
            };
            let operation_id = format!("admission-{}-{}", inner.record.session_id, next);
            inner.next_admission_id = next;
            operation_id
        };
        let (operation, _receiver) = CompactionOperation::new(operation_id.clone());
        let (admission, result_receiver) = AdmissionOperation::new(Arc::clone(&operation));
        let mut join_slot = admission.join.lock().await;
        let reservation = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing
                || inner.active.is_some()
                || inner.compaction.is_some()
                || inner.admission.is_some()
            {
                return Err(AgentError::SessionBusy);
            }
            let auto = inner.auto.clone().ok_or(AgentError::SessionBusy)?;
            // The automatic binding always carries the raw configured model,
            // never the presentation-wrapped one, so the summary utility has
            // its own no-tools identity.
            let model = Arc::clone(&auto.model);
            let descriptor = inner.config.descriptor().clone();
            let history = Arc::clone(&inner.history);
            // Reuse an already-validated durable summary as the fold starting
            // point instead of re-summarizing its covered prefix.
            let previous = self.shared.compaction.project(&history);
            let (previous_summary, previous_covered_item_count) = previous
                .map(|projection| {
                    let covered = history.len().saturating_sub(projection.suffix.len());
                    (Some(projection.summary), covered)
                })
                .unwrap_or((None, 0));
            let retained_item_count = history.len().saturating_sub(previous_covered_item_count);
            let budget = auto.policy.budget(descriptor.context_window);
            self.shared
                .compaction
                .begin_automatic(AutomaticCompactionObservation {
                    operation_id: operation.operation_id.clone(),
                    loop_id: None,
                    request_index: None,
                    before_tokens: None,
                    after_tokens: None,
                    utility_before_tokens: None,
                    utility_after_tokens: None,
                    hard_tokens: budget.hard_tokens,
                    trigger_tokens: budget.trigger_tokens,
                    target_tokens: budget.target_tokens,
                    utility_usage: None,
                    outcome: "preparing".to_owned(),
                });
            let options = inner.options.clone();
            let compaction = CompactionReservation {
                operation: Arc::clone(&operation),
                model,
                record: inner.record.clone(),
                history,
                workspace: Arc::clone(&inner.workspace),
                tool_schemas: frozen_tool_specs(&inner.config, &inner.record),
                previous_summary,
                previous_covered_item_count,
                hard_tokens: budget.hard_tokens,
                target_tokens: budget.target_tokens,
                safe_before_estimate: true,
                refresh_summary: true,
                // Startup compaction has its own operation deadline. The
                // effective automatic preparation budget covers the configured
                // model/prompt timeout floor and is shared by all utility
                // chunks; no individual chunk gets a fresh timeout.
                deadline: Instant::now()
                    .checked_add(options.prompt_timeout.max(options.model_timeout))
                    .unwrap_or_else(Instant::now),
            };
            inner.compaction = Some(Arc::clone(&operation));
            inner.admission = Some(Arc::clone(&admission));
            inner.compaction_progress = Some(CompactionProgress {
                operation_id: operation.operation_id.clone(),
                phase: CompactionPhase::Preparing,
                covered_item_count: previous_covered_item_count,
                retained_item_count,
            });
            AdmissionReservation {
                compaction,
                config: inner.config.clone(),
                options,
                auto,
                system_prompt: None,
                raw_fits_hard: false,
            }
        };
        // Publish the reservation before spawning a worker that may complete
        // synchronously (for example, an already-fitting request). This keeps
        // SessionState notifications in causal order.
        self.emit_state();
        // Do not let a very fast worker finish and clear the Session slot
        // before its JoinHandle has been stored. The gate closes that small
        // shutdown/ownership window without adding an await to admission.
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let worker_session = self.clone();
        let worker_admission = Arc::clone(&admission);
        let task = tokio::spawn(async move {
            if start_rx.await.is_ok() {
                run_admission(worker_session, worker_admission, reservation, input).await;
            }
        });
        *join_slot = Some(task);
        drop(join_slot);
        let _ = start_tx.send(());
        Ok(PreparationWaiter {
            session: self.clone(),
            operation_id,
            receiver: result_receiver,
        })
    }
}

impl Session {
    /// Clears only the wire-visible progress for one admission. The admission
    /// and its JoinHandle remain Session-owned until the worker has actually
    /// been joined by cleanup or shutdown.
    fn clear_admission(&self, admission: &Arc<AdmissionOperation>) {
        let mut inner = self.shared.inner.lock().unwrap();
        let current = inner
            .admission
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, admission));
        if current {
            inner.compaction_progress = None;
        }
    }
}

impl Session {
    pub(super) async fn reap_finished_admission(&self) -> Result<bool, AgentError> {
        let admission = {
            let inner = self.shared.inner.lock().unwrap();
            inner.admission.clone()
        };
        let Some(admission) = admission else {
            return Ok(false);
        };
        if admission.result.borrow().is_none() {
            return Ok(false);
        }
        if admission.join().await.is_err() {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .admission
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission))
            {
                inner.admission = None;
            }
            if inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission.operation))
            {
                inner.compaction = None;
                inner.compaction_progress = None;
            }
            inner.blocked = Some(SessionBlockReason::Internal);
            drop(inner);
            self.emit_state();
            return Err(AgentError::SessionBlocked);
        }
        let cleared = {
            let mut inner = self.shared.inner.lock().unwrap();
            let same_admission = inner
                .admission
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission));
            if same_admission {
                inner.admission = None;
            }
            let same_compaction = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission.operation));
            if same_compaction {
                inner.compaction = None;
            }
            let progress_cleared = inner.compaction_progress.take().is_some();
            same_admission || same_compaction || progress_cleared
        };
        if cleared {
            self.emit_state();
        }
        Ok(true)
    }
}

/// The single startup-admission worker: it folds the settled prefix into a
/// durable summary, then creates the loop. It never fabricates a `TurnRef`
/// before the summary is committed, and a cancelled preparation terminates
/// without starting any loop. A drop guard guarantees the waiter always sees a
/// terminal outcome even if the worker panics.
async fn run_admission(
    session: Session,
    admission: Arc<AdmissionOperation>,
    mut reservation: AdmissionReservation,
    input: UserInput,
) {
    let mut guard = AdmissionCompletionGuard {
        session: session.clone(),
        admission: Arc::clone(&admission),
        operation_id: reservation.compaction.operation.operation_id.clone(),
        outcome: None,
    };
    let operation = Arc::clone(&reservation.compaction.operation);
    let outcome = if operation.cancellation_requested() {
        PreparedLoop::Failed(PreparationFailure::Cancelled)
    } else {
        match session.admission_needed(&input, &mut reservation).await {
            Err(failure) => PreparedLoop::Failed(failure),
            Ok(AdmissionNeed::Uncompressible) => {
                PreparedLoop::Failed(PreparationFailure::ContextUncompressible)
            }
            Ok(AdmissionNeed::None) => {
                install_admitted_loop(&session, &admission, input, &reservation)
            }
            Ok(AdmissionNeed::Compact) => {
                let result = run_compaction_inner(&session, &reservation.compaction).await;
                session.shared.compaction.update_automatic(
                    &operation.operation_id,
                    |observation| {
                        if observation.before_tokens.is_none() {
                            observation.before_tokens = result.before_tokens;
                        }
                        if observation.after_tokens.is_none() {
                            observation.after_tokens = result.after_tokens;
                        }
                        if result.before_tokens.is_some() {
                            observation.utility_before_tokens = result.before_tokens;
                        }
                        if result.after_tokens.is_some() {
                            observation.utility_after_tokens = result.after_tokens;
                        }
                        if result.utility_usage.is_some() {
                            observation.utility_usage = result.utility_usage.clone();
                        }
                    },
                );
                match result.status {
                    CompactionStatus::Compacted | CompactionStatus::Noop => {
                        if operation.cancellation_requested() {
                            PreparedLoop::Failed(PreparationFailure::Cancelled)
                        } else {
                            install_admitted_loop(&session, &admission, input, &reservation)
                        }
                    }
                    CompactionStatus::Failed => {
                        if reservation.raw_fits_hard
                            && !operation.cancellation_requested()
                            && Instant::now() < reservation.compaction.deadline
                        {
                            install_admitted_loop(&session, &admission, input, &reservation)
                        } else {
                            PreparedLoop::Failed(admission_failure_kind(
                                result.failure_kind.as_deref(),
                            ))
                        }
                    }
                    CompactionStatus::UnknownWrite => {
                        PreparedLoop::Failed(admission_failure_kind(result.failure_kind.as_deref()))
                    }
                }
            }
        }
    };
    guard.outcome = Some(outcome);
    drop(guard);
    #[cfg(test)]
    if let Some(gate) = take_pause_after_admission_result(session.session_id()) {
        gate.started.notify_one();
        gate.release.notified().await;
    }
}

/// Publishes exactly one terminal outcome for an admission. If the worker is
/// dropped without publishing (panic or task abort), a later admission cannot
/// hang on a sender that will never send.
struct AdmissionCompletionGuard {
    session: Session,
    admission: Arc<AdmissionOperation>,
    operation_id: String,
    outcome: Option<PreparedLoop>,
}

impl Drop for AdmissionCompletionGuard {
    fn drop(&mut self) {
        let mut outcome = self
            .outcome
            .take()
            .unwrap_or(PreparedLoop::Failed(PreparationFailure::Internal));
        if !matches!(&outcome, PreparedLoop::Started(_))
            && self.admission.operation.cancellation_requested()
        {
            outcome = PreparedLoop::Failed(PreparationFailure::Cancelled);
        }
        if matches!(
            &outcome,
            PreparedLoop::Failed(PreparationFailure::ContextUncompressible)
        ) {
            self.session
                .shared
                .compaction
                .note_prepare_failure(crate::compaction::CONTEXT_UNCOMPRESSIBLE);
        } else if let PreparedLoop::Failed(PreparationFailure::Compaction(kind)) = &outcome {
            self.session.shared.compaction.note_prepare_failure(kind);
        } else if matches!(&outcome, PreparedLoop::Started(_)) {
            self.session.shared.compaction.clear_prepare_failure();
        }
        let observation_outcome = match &outcome {
            PreparedLoop::Started(_) => "started",
            PreparedLoop::Failed(PreparationFailure::Cancelled) => "cancelled",
            PreparedLoop::Failed(PreparationFailure::ContextUncompressible) => {
                "context_uncompressible"
            }
            PreparedLoop::Failed(PreparationFailure::HistoryTooLarge) => "history_too_large",
            PreparedLoop::Failed(PreparationFailure::SessionBlocked) => "session_blocked",
            PreparedLoop::Failed(PreparationFailure::SessionBusy) => "session_busy",
            PreparedLoop::Failed(PreparationFailure::Compaction(kind)) => kind,
            PreparedLoop::Failed(PreparationFailure::Internal) => "internal",
        };
        self.session.shared.compaction.finish_automatic(
            &self.operation_id,
            observation_outcome,
            None,
            None,
        );
        if !matches!(&outcome, PreparedLoop::Started(_)) {
            self.admission.operation.finish();
            self.session.clear_admission(&self.admission);
        }
        self.admission.result.send_replace(Some(outcome));
        self.session.emit_state();
    }
}

fn install_admitted_loop(
    session: &Session,
    admission: &Arc<AdmissionOperation>,
    input: UserInput,
    reservation: &AdmissionReservation,
) -> PreparedLoop {
    if reservation.compaction.operation.cancellation_requested() {
        return PreparedLoop::Failed(PreparationFailure::Cancelled);
    }
    if Instant::now() >= reservation.compaction.deadline {
        return PreparedLoop::Failed(PreparationFailure::Compaction("timeout"));
    }
    match session.build_admission_execution(input, reservation, admission) {
        Ok(execution) => match session.install_loop(execution, Some(admission)) {
            Ok(accepted) => PreparedLoop::Started(accepted),
            Err(error) => PreparedLoop::Failed(admission_failure(&error)),
        },
        Err(error) => PreparedLoop::Failed(admission_failure(&error)),
    }
}

fn admission_failure(error: &AgentError) -> PreparationFailure {
    match error {
        AgentError::HistoryTooLarge => PreparationFailure::HistoryTooLarge,
        AgentError::SessionBlocked => PreparationFailure::SessionBlocked,
        AgentError::SessionBusy => PreparationFailure::SessionBusy,
        AgentError::InvalidState => PreparationFailure::Cancelled,
        AgentError::ContextUncompressible => PreparationFailure::ContextUncompressible,
        _ => PreparationFailure::Internal,
    }
}

fn admission_failure_kind(kind: Option<&str>) -> PreparationFailure {
    match kind {
        Some("cancelled") => PreparationFailure::Cancelled,
        Some("history_too_large") => PreparationFailure::HistoryTooLarge,
        Some("context_uncompressible") => PreparationFailure::ContextUncompressible,
        Some("timeout") => PreparationFailure::Compaction("timeout"),
        Some("workspace") => PreparationFailure::Compaction("workspace"),
        Some("prompt") => PreparationFailure::Compaction("prompt"),
        Some("store") => PreparationFailure::Compaction("store"),
        Some("history_changed") => PreparationFailure::Compaction("history_changed"),
        Some("too_large") => PreparationFailure::Compaction("too_large"),
        Some("budget_exceeded") => PreparationFailure::Compaction("budget_exceeded"),
        Some("no_progress") => PreparationFailure::Compaction("no_progress"),
        Some("model_failure") => PreparationFailure::Compaction("model_failure"),
        Some("invalid_model_response") => PreparationFailure::Compaction("invalid_model_response"),
        Some("tool_call_rejected") => PreparationFailure::Compaction("tool_call_rejected"),
        Some("serialization_failure") => PreparationFailure::Compaction("serialization_failure"),
        Some("write_outcome_unknown") => PreparationFailure::Compaction("write_outcome_unknown"),
        _ => PreparationFailure::Compaction("compaction_failure"),
    }
}
