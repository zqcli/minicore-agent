use super::*;
use crate::presentation::{ToolArgumentsPreviewState, ToolDisplay};
use crate::tool_data::*;

pub(super) fn events() -> Vec<AgentEvent> {
    let session_id = "ses_00000000000000000000000000000001".parse().unwrap();
    let loop_id = "lup_00000000000000000000000000000002".parse().unwrap();
    let interaction_id = "int_00000000000000000000000000000003".parse().unwrap();
    let tool_call_id = ToolCallId::new("call_1").unwrap();
    let turn = TurnRef {
        session_id,
        loop_id,
    };
    let meta = EventMeta {
        session_id,
        loop_id: Some(loop_id),
        dropped_before: 7,
    };
    let tool_ref = ToolRef {
        session_id,
        loop_id,
        request_index: 1,
        tool_call_id: tool_call_id.clone(),
    };
    let display = ToolDisplay {
        detail: "file.txt".into(),
        expanded_input: Some("hello\n".into()),
        input_line_count: Some(1),
        hidden_line_count: Some(1),
        truncated: false,
        body_truncated: false,
    };
    vec![
        AgentEvent::SessionOpened {
            session: SessionInfo {
                session_id,
                title: None,
                profile: "default".into(),
                workspace: "/workspace".into(),
                model: "main".into(),
                reasoning: minicore_runtime::model::ReasoningPreference::Disabled,
                loaded: true,
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-01-01T00:00:00Z".into(),
            },
            meta,
        },
        AgentEvent::SessionClosed { session_id, meta },
        AgentEvent::SessionState {
            state: SessionState {
                session_id,
                status: SessionStatus::Blocked,
                active_loop: None,
                block_reason: Some(SessionBlockReason::Internal),
                compaction: None,
            },
            meta,
        },
        AgentEvent::TurnStarted { turn, meta },
        AgentEvent::RequestStarted {
            turn,
            request_index: 1,
            config_revision: ConfigRevision::INITIAL,
            model: "main".into(),
            reasoning: minicore_runtime::model::ReasoningPreference::Disabled,
            meta,
        },
        AgentEvent::RequestUsage {
            turn,
            request_index: 1,
            usage: Usage::default(),
            meta,
        },
        AgentEvent::OutputDelta {
            turn,
            request_index: 1,
            channel: OutputChannel::Text,
            delta: "hello\n\"world\"".into(),
            meta,
        },
        AgentEvent::ToolStarted {
            turn,
            request_index: 1,
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".into(),
            meta,
        },
        AgentEvent::ToolInvocation {
            turn,
            data: ToolInvocationData {
                tool_ref: tool_ref.clone(),
                name: "read".into(),
                subject: ToolSubject::File {
                    path: "file.txt".into(),
                },
                subject_truncated: false,
                input: ToolInputSummary {
                    total_bytes: 2,
                    preview: "{}".into(),
                    truncated: false,
                    encoding: "utf8",
                },
            },
            meta,
        },
        AgentEvent::ToolExecution {
            turn,
            data: ToolExecutionData {
                output_line_count: Some(1),
                tool_ref: tool_ref.clone(),
                name: "read".into(),
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input_availability: ToolDataAvailability::Available,
                output_availability: ToolDataAvailability::Available,
                input_bytes: 2,
                result_bytes: 6,
                input_truncated: false,
                result_truncated: false,
                command: None,
                recording: ToolRecordingState::Saved,
            },
            meta,
        },
        AgentEvent::ToolProcess {
            turn,
            data: ToolProcessData {
                tool_ref,
                chunk: Some(ToolProcessChunk {
                    stream: ToolDataStream::Stdout,
                    encoding: "base64",
                    data: "aGk=".into(),
                    base_offset: 0,
                    next_offset: 2,
                    observed_end: 2,
                    dropped: false,
                    expired: false,
                }),
                command: None,
            },
            meta,
        },
        AgentEvent::ToolPresentation {
            turn,
            request_index: 1,
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".into(),
            display: display.clone(),
            meta,
        },
        AgentEvent::ToolArgumentsPreview {
            turn,
            request_index: 1,
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".into(),
            attempt: 2,
            revision: 3,
            state: ToolArgumentsPreviewState::Generated,
            partial: false,
            display,
            meta,
        },
        AgentEvent::ToolProgress {
            turn,
            request_index: 1,
            tool_call_id: tool_call_id.clone(),
            progress: ToolProgressView {
                message: Some("reading".into()),
                completed: Some(1),
                total: None,
            },
            meta,
        },
        AgentEvent::ToolFinished {
            turn,
            request_index: 1,
            tool_call_id: tool_call_id.clone(),
            result: ToolResultView {
                outcome: ToolResultOutcome::Success,
                content_bytes: 6,
                content: Some("hello\n".into()),
                content_truncated: false,
            },
            meta,
        },
        AgentEvent::SteerProgress {
            turn,
            request_index: 1,
            applied_count: 2,
            meta,
        },
        AgentEvent::InteractionRequested {
            turn,
            interaction: PendingInteractionView {
                interaction_id,
                tool_call_id,
                tool_name: "read".into(),
                kind: InteractionKindView::Approval {
                    prompt: "Allow read?".into(),
                    risk: ApprovalRiskView::Low,
                },
            },
            meta,
        },
        AgentEvent::InteractionResolved {
            turn,
            interaction_id,
            meta,
        },
        AgentEvent::TurnFinished {
            turn,
            outcome: LoopOutcomeView::Completed,
            persistence: TurnPersistence::Persisted,
            meta,
        },
    ]
}

#[test]
fn all_event_variants_match_snapshot() {
    let actual = events()
        .iter()
        .map(|event| serde_json::to_string(event).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    assert_eq!(actual, include_str!("events.jsonl"));
}

#[test]
fn history_lines_round_trip() {
    let outcomes = [
        r#"{"type":"completed"}"#,
        r#"{"type":"cancelled","reason":"user"}"#,
        r#"{"type":"cancelled","reason":"owner_dropped"}"#,
        r#"{"type":"cancelled","reason":"shutdown"}"#,
        r#"{"type":"cancelled","reason":"deadline"}"#,
        r#"{"type":"failed","kind":"internal","model_error":null}"#,
        r#"{"type":"failed","kind":"model","model_error":{"kind":"timeout","delivery":"not_started","retryable":true,"retry_after_millis":12}}"#,
        r#"{"type":"failed","kind":"model","model_error":{"kind":"invalid_request","delivery":"not_started","retryable":false,"retry_after_millis":null,"local_context_budget":{"estimated_tokens":100,"input_budget_tokens":90}}}"#,
        r#"{"type":"failed","kind":"model","model_error":{"kind":"rate_limited","delivery":"not_started","retryable":true,"retry_after_millis":12000,"provider":{"status":429,"code":"rate_limit_exceeded","type":"requests","message":"Rate limit reached"}}}"#,
    ];
    for outcome in outcomes {
        let line = format!(
            r#"{{"loop_id":"lup_00000000000000000000000000000002","outcome":{outcome},"items":[],"usage":{{"input_tokens":0,"output_tokens":0}},"requests":1,"tool_rounds":0,"final_config_revision":1,"completed_at":"2026-01-01T00:00:00Z"}}"#
        );
        let record: crate::store::StoredLoopRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), line);
    }
}
