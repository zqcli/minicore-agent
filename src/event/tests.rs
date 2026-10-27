use super::*;

impl AgentEventStream {
    /// Synchronous `recv` for producer tests: same lane priority and drop accounting.
    pub(crate) fn try_recv(&mut self) -> Option<AgentEvent> {
        let event = self
            .lifecycle
            .try_recv()
            .or_else(|_| self.receiver.try_recv())
            .ok()?;
        Some(self.delivered(event))
    }

    pub(crate) fn len(&self) -> usize {
        self.lifecycle.len() + self.receiver.len()
    }
}

#[tokio::test]
async fn streaming_pressure_cannot_displace_lifecycle_and_drops_follow_delivery() {
    let (sink, mut stream) = AgentEventSink::channel(1);
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id: LoopId::new().unwrap(),
    };
    let delta = || AgentEvent::OutputDelta {
        turn,
        request_index: 1,
        channel: OutputChannel::Text,
        delta: "text".into(),
        meta: EventMeta::for_turn(turn),
    };
    assert_eq!(sink.try_send(delta()), AgentSendResult::Sent);
    assert_eq!(sink.try_send(delta()), AgentSendResult::Dropped);
    sink.record_core_drops(3);
    assert_eq!(
        sink.try_send(AgentEvent::TurnFinished {
            turn,
            outcome: LoopOutcomeView::Completed,
            persistence: TurnPersistence::Persisted,
            meta: EventMeta::for_turn(turn)
        }),
        AgentSendResult::Sent
    );
    let terminal = stream.recv().await.unwrap();
    assert!(matches!(terminal, AgentEvent::TurnFinished { .. }));
    assert_eq!(terminal.meta().dropped_before, 4);
    assert_eq!(sink.try_send(delta()), AgentSendResult::Dropped);
    let queued = stream.recv().await.unwrap();
    assert!(matches!(queued, AgentEvent::OutputDelta { .. }));
    assert_eq!(queued.meta().dropped_before, 1);
    drop(sink);
    assert!(stream.recv().await.is_none());
}

#[test]
fn process_output_chunks_stream_while_command_state_stays_lifecycle() {
    let session_id = SessionId::new().unwrap();
    let process = |chunk: bool| AgentEvent::ToolProcess {
        turn: TurnRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
        },
        data: crate::tool_data::ToolProcessData {
            tool_ref: crate::tool_data::ToolRef {
                session_id,
                loop_id: LoopId::new().unwrap(),
                request_index: 0,
                tool_call_id: minicore_runtime::ToolCallId::new("call").unwrap(),
            },
            chunk: chunk.then(|| crate::tool_data::ToolProcessChunk {
                stream: crate::tool_data::ToolDataStream::Stdout,
                encoding: "base64",
                data: String::new(),
                base_offset: 0,
                next_offset: 0,
                observed_end: 0,
                dropped: false,
                expired: false,
            }),
            command: None,
        },
        meta: EventMeta::session(session_id),
    };
    assert!(process(false).is_lifecycle());
    assert!(!process(true).is_lifecycle());
}

#[tokio::test]
async fn lifecycle_capacity_is_bounded_and_both_lanes_drain_on_close() {
    let (sink, mut stream) = AgentEventSink::channel(1);
    let session_id = SessionId::new().unwrap();
    let event = || AgentEvent::SessionClosed {
        session_id,
        meta: EventMeta::session(session_id),
    };
    for _ in 0..64 {
        assert_eq!(sink.try_send(event()), AgentSendResult::Sent);
    }
    assert_eq!(sink.try_send(event()), AgentSendResult::Dropped);
    drop(sink);
    let mut delivered = 0;
    let mut dropped = 0;
    while let Some(event) = stream.recv().await {
        delivered += 1;
        dropped += event.meta().dropped_before;
    }
    assert_eq!((delivered, dropped), (64, 1));
}

#[tokio::test]
async fn preview_updates_cannot_displace_their_terminal_transition() {
    use crate::presentation::ToolArgumentsPreviewState;
    let (sink, mut stream) = AgentEventSink::channel(1);
    let mut preview = super::snapshots::events()
        .into_iter()
        .find(|event| matches!(event, AgentEvent::ToolArgumentsPreview { .. }))
        .unwrap();
    preview.set_dropped_before(0);
    for current in 3..20 {
        if let AgentEvent::ToolArgumentsPreview {
            revision, state, ..
        } = &mut preview
        {
            *revision = current;
            *state = ToolArgumentsPreviewState::Generating;
        }
        sink.try_send(preview.clone());
    }
    if let AgentEvent::ToolArgumentsPreview {
        revision, state, ..
    } = &mut preview
    {
        *revision = 20;
        *state = ToolArgumentsPreviewState::Discarded;
    }
    assert_eq!(sink.try_send(preview), AgentSendResult::Sent);
    let terminal = stream.recv().await.unwrap();
    assert_eq!(terminal.meta().dropped_before, 16);
    assert!(matches!(
        terminal,
        AgentEvent::ToolArgumentsPreview {
            state: ToolArgumentsPreviewState::Discarded,
            ..
        }
    ));
}

#[test]
fn tool_result_debug_redacts_display_content() {
    let view = ToolResultView {
        outcome: ToolResultOutcome::Success,
        content_bytes: 42,
        content: Some("secret command output".to_owned()),
        content_truncated: true,
    };

    let debug = format!("{view:?}");
    assert!(!debug.contains("secret command output"));
    assert!(debug.contains("content_len"));
    assert!(debug.contains("content_truncated"));
}

#[test]
fn tool_progress_debug_redacts_message() {
    let view = ToolProgressView {
        message: Some("secret progress path".to_owned()),
        completed: Some(1),
        total: Some(2),
    };
    let debug = format!("{view:?}");
    assert!(!debug.contains("secret progress path"));
    assert!(debug.contains("message_len"));
}

use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{DeliveryState, ModelErrorKind};
use minicore_runtime::value::BoundedText;

fn error(kind: ModelErrorKind, delivery: DeliveryState, text: &str) -> ModelError {
    ModelError::permanent(
        kind,
        delivery,
        DiagnosticSummary::new(
            DiagnosticCode::InvalidConfiguration,
            DiagnosticCategory::Model,
            BoundedText::new(text).unwrap(),
            false,
        )
        .unwrap(),
    )
}
const VALID: &str = "local serialized context estimate exceeds input budget: estimated_tokens=32001, input_budget_tokens=32000; use /compact or a larger model";

#[test]
fn only_exact_local_numeric_preflight_is_exposed_and_persisted() {
    let error = error(
        ModelErrorKind::InvalidRequest,
        DeliveryState::NotStarted,
        VALID,
    );
    let view = ModelErrorView::from_model(&error);
    assert_eq!(
        view.local_context_budget,
        Some(LocalContextBudgetFailure {
            estimated_tokens: 32001,
            input_budget_tokens: 32000
        })
    );
    let stored = ModelErrorView::from_model(&error);
    let encoded = serde_json::to_vec(&stored).unwrap();
    let restored: ModelErrorView = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored, view);
    assert!(
        !String::from_utf8(encoded)
            .unwrap()
            .contains("serialized context estimate")
    );
}
#[test]
fn provider_diagnostics_and_lookalike_numbers_are_not_exposed() {
    for text in [
        "provider private body",
        "local serialized context estimate exceeds input budget; use /compact or a larger model",
        &VALID.replace("32001", "032001"),
        &VALID.replace("32001", "+32001"),
        &VALID.replace("32001", "32000"),
        &VALID.replace("32001", "18446744073709551616"),
        &format!("{VALID} secret"),
    ] {
        assert!(
            LocalContextBudgetFailure::from_model(&error(
                ModelErrorKind::InvalidRequest,
                DeliveryState::NotStarted,
                text
            ))
            .is_none()
        );
    }
    for delivery in [DeliveryState::Started, DeliveryState::Unknown] {
        assert!(
            LocalContextBudgetFailure::from_model(&error(
                ModelErrorKind::InvalidRequest,
                delivery,
                VALID
            ))
            .is_none()
        );
    }
    assert!(
        LocalContextBudgetFailure::from_model(&error(
            ModelErrorKind::ContextOverflow,
            DeliveryState::NotStarted,
            VALID
        ))
        .is_none()
    );
}
#[test]
fn legacy_and_nullable_stored_errors_keep_optional_field_absent() {
    for suffix in ["", ",\"local_context_budget\":null", ",\"provider\":null"] {
        let old = format!(
            "{{\"kind\":\"invalid_request\",\"delivery\":\"not_started\",\"retryable\":false,\"retry_after_millis\":null{suffix}}}"
        );
        let stored: ModelErrorView = serde_json::from_str(&old).unwrap();
        assert!(stored.local_context_budget.is_none());
        assert!(stored.provider.is_none());
        let value = serde_json::to_value(stored).unwrap();
        assert!(value.get("local_context_budget").is_none() && value.get("provider").is_none());
    }
}

fn provider_failure(provider: &ProviderErrorView) -> ModelError {
    ModelError::permanent(
        ModelErrorKind::ProviderUnavailable,
        DeliveryState::Unknown,
        DiagnosticSummary::new(
            DiagnosticCode::ModelUnavailable,
            DiagnosticCategory::Model,
            BoundedText::new(provider.to_diagnostic_text()).unwrap(),
            false,
        )
        .unwrap(),
    )
}

#[test]
fn provider_text_over_the_limit_is_cut_on_a_char_boundary_and_marked() {
    let message = "é".repeat(9_000);
    let view = ProviderErrorView::new(None, None, None, Some(&message)).unwrap();
    let cut = view.message.unwrap();
    assert_eq!(cut.len(), 16 * 1024 - 1);
    assert_eq!(cut.strip_suffix('…').unwrap(), "é".repeat(8_190));

    let view = ProviderErrorView::new(None, Some(&"c".repeat(300)), Some("t"), None).unwrap();
    let code = view.code.unwrap();
    assert_eq!(code.len(), 256);
    assert!(code.ends_with('…'));
    assert_eq!(view.error_type.as_deref(), Some("t"));
}

#[test]
fn provider_text_drops_control_characters_except_newline_and_tab() {
    let view = ProviderErrorView::new(
        None,
        Some("co\u{0}de"),
        None,
        Some("a\u{1b}[31mb\r\n\tc\u{7f}\u{85}d"),
    )
    .unwrap();
    assert_eq!(view.code.as_deref(), Some("code"));
    assert_eq!(view.message.as_deref(), Some("a[31mb\n\tcd"));
}

#[test]
fn blank_provider_text_is_absent_and_absent_fields_are_not_serialized() {
    assert_eq!(
        ProviderErrorView::new(None, Some(" "), Some(""), Some("\u{0}\n")),
        None
    );
    let view = ProviderErrorView::new(Some(502), None, None, Some("bad gateway")).unwrap();
    assert_eq!(
        serde_json::to_string(&view).unwrap(),
        r#"{"status":502,"message":"bad gateway"}"#
    );
}

#[test]
fn provider_text_round_trips_through_the_model_error_diagnostic() {
    // The worst case for JSON escaping must still fit the Runtime's diagnostic bound.
    let quotes = "\"".repeat(20_000);
    for message in ["quote \" and \\ and\nnewline é 🦀", quotes.as_str()] {
        let provider =
            ProviderErrorView::new(Some(429), Some("c"), Some("t"), Some(message)).unwrap();
        let error = provider_failure(&provider);
        let view = ModelErrorView::from_model(&error);
        assert_eq!(view.provider, Some(provider));
        assert!(!format!("{error:?}").contains("quote"));
    }
    let none = ModelErrorView::from_model(&error(
        ModelErrorKind::Timeout,
        DeliveryState::Unknown,
        "provider error: not json",
    ));
    assert_eq!(none.provider, None);
}
