use super::*;
use crate::event::{AgentEvent, EventMeta, PendingInteractionView};
use crate::rpc::protocol::{AgentEventNotification, RpcId, RpcResponse, SessionStateResult};
use crate::sessions::{
    CompletionEntry, CompletionKind, CompletionOutcome, CompletionWindow, InputMode, InputPushed,
    Queue, QueueItem, ResultAvailability, SessionExecution, SessionSnapshot, SnapshotInteraction,
    StartedTurn, TurnPersistence, TurnRef,
};
use minicore_runtime::interaction::{InteractionKind, PendingInteraction};
use minicore_runtime::tools::{ToolInputAnswerKind, ToolInputRequest};
use minicore_runtime::value::BoundedText;

#[test]
fn maximum_escaped_control_and_legacy_receipts_fit_with_the_largest_request_id() {
    let session_id = SessionId::new().unwrap();
    let load = crate::test_support::load_id();
    let loop_id = minicore_runtime::LoopId::new().unwrap();
    let text = "\u{1}".repeat(BoundedText::MAX_BYTES);
    minicore_runtime::execution::UserInput::text(&text).unwrap();
    let input = QueueItem {
        id: format!("inp_{:032x}", u64::MAX - 32),
        mode: InputMode::FollowUp,
        text,
        accepted_at: Some("2026-10-31T23:59:59Z".into()),
    };
    let queue = Queue {
        items: (0..32)
            .map(|index| QueueItem {
                id: format!("inp_{:032x}", u64::MAX - 31 + index),
                ..input.clone()
            })
            .collect(),
        paused: false,
    };
    let request = ToolInputRequest::new(
        "\"".repeat(8192),
        vec![BoundedText::new("\"".repeat(1024)).unwrap(); 32],
        ToolInputAnswerKind::SingleChoice,
    )
    .unwrap();
    let interaction = PendingInteraction {
        interaction_id: minicore_runtime::InteractionId::new().unwrap(),
        tool_call_id: ToolCallId::new("c".repeat(256)).unwrap(),
        tool_name: "read".parse().unwrap(),
        kind: InteractionKind::ToolInput(request),
    };
    let completed_loops: Vec<_> = (0..256)
        .map(|_| minicore_runtime::LoopId::new().unwrap())
        .collect();
    let entries = completed_loops
        .iter()
        .enumerate()
        .map(|(index, completed)| CompletionEntry {
            seq: index as u64 + 1,
            loop_id: *completed,
            input_id: Some(format!("inp_{:032x}", index + 1)),
            completion_kind: CompletionKind::Reported,
            outcome_kind: Some(CompletionOutcome::Completed),
            persistence: Some(TurnPersistence::Persisted),
            persistence_uncertain: false,
            availability: ResultAvailability::Stored,
            unavailable_reason: None,
            next_loop_id: Some(completed_loops.get(index + 1).copied().unwrap_or(loop_id)),
            after_saved_loop_id: index.checked_sub(1).map(|index| completed_loops[index]),
        })
        .collect::<Vec<_>>();
    for entry in &entries {
        assert!(crate::budget::encoded_len(entry) <= 2048);
    }
    assert_eq!(
        queue
            .items
            .iter()
            .map(|item| &item.id)
            .collect::<BTreeSet<_>>()
            .len(),
        32
    );
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.loop_id)
            .collect::<BTreeSet<_>>()
            .len(),
        256
    );
    assert!(entries.iter().all(|entry| entry.loop_id != loop_id));
    let snapshot = SessionSnapshot {
        session_id,
        load,
        revision: u64::MAX,
        execution: SessionExecution::Running {
            loop_id,
            input: input.clone(),
            request_index: u32::MAX,
            phase: crate::event::LoopStatusView::WaitingForInput,
        },
        pending_interaction: Some(SnapshotInteraction {
            loop_id,
            request_index: u32::MAX,
            interaction: PendingInteractionView::from(&interaction),
        }),
        queue: queue.clone(),
        compaction: None,
        block_reason: None,
        completions: CompletionWindow {
            through_seq: 256,
            retained_from_seq: 1,
            entries,
            truncated: false,
        },
    };
    let state = crate::sessions::SessionState {
        session_id,
        status: crate::sessions::SessionStatus::WaitingForInput,
        active_loop: Some(minicore_runtime::LoopState {
            loop_id,
            status: minicore_runtime::LoopStatus::WaitingForInput,
            request_index: u32::MAX,
            config_revision: minicore_runtime::execution::ConfigRevision::new(u64::MAX),
            model: None,
            pending_interaction: Some(interaction),
        }),
        compaction: None,
        block_reason: None,
    };
    let id = RpcId::String("i".repeat(MAX_RPC_LINE_BYTES - 512));
    let response = RpcResponse::snapshot_success(
        id.clone(),
        SessionStateResult {
            state: crate::event::SessionStateView::from(&state),
            snapshot: snapshot.clone(),
        },
    )
    .unwrap();
    assert!(response.encoded().len() < super::super::MAX_RPC_FRAME_BYTES);
    let decoded: Value = serde_json::from_slice(response.encoded()).unwrap();
    assert_eq!(
        decoded["result"]["snapshot"]["queue"]["items"]
            .as_array()
            .unwrap()
            .len(),
        32
    );
    assert_eq!(
        decoded["result"]["snapshot"]["execution"]["input"]["text"]
            .as_str()
            .unwrap(),
        input.text
    );
    assert_eq!(
        decoded["result"]["snapshot"]["pending_interaction"]["kind"]["data"]["choices"]
            .as_array()
            .unwrap()
            .len(),
        32
    );
    assert_eq!(
        decoded["result"]["snapshot"]["completions"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        256
    );
    let notification = AgentEventNotification::new(AgentEvent::SessionSnapshot {
        snapshot,
        meta: EventMeta::session(session_id, load),
    });
    assert!(crate::budget::encoded_len(&notification) < super::super::MAX_RPC_FRAME_BYTES);
    let pushed = InputPushed {
        session_id,
        load,
        accepted: true,
        id: queue.items.last().unwrap().id.clone(),
        accepted_at: input.accepted_at.clone(),
        started: Some(StartedTurn {
            turn: TurnRef {
                session_id,
                loop_id,
            },
            input,
        }),
        start_error: None,
        queue,
    };
    let response = RpcResponse::success(id, pushed).unwrap();
    assert!(response.encoded().len() < super::super::MAX_RPC_FRAME_BYTES);
}
