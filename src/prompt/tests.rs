use std::time::Duration;

use minicore_runtime::history::HistoryView;
use minicore_runtime::model::{ModelDescriptor, ModelRef, ReasoningPreference};
use minicore_runtime::prompt::PromptError;
use tokio_util::sync::CancellationToken;

use super::*;

fn prefix(bytes: Vec<u8>, has_more: bool) -> ReadPrefix {
    ReadPrefix { bytes, has_more }
}

#[test]
fn empty_content_is_an_empty_instruction_set() {
    assert_eq!(decode_agents(&prefix(Vec::new(), false)).unwrap(), "");
}

#[test]
fn valid_crlf_is_normalized() {
    let content = b"line one\r\nline two\r\n".to_vec();
    let decoded = decode_agents(&prefix(content, false)).unwrap();
    assert_eq!(decoded, "line one\nline two\n");
}

#[test]
fn invalid_utf8_is_a_prompt_error() {
    let content = vec![b'a'; 16];
    let mut content = content;
    content[8] = 0xff;
    assert_eq!(
        decode_agents(&prefix(content, false)),
        Err(AgentPromptError::InvalidHistory)
    );
}

#[test]
fn incomplete_utf8_at_eof_is_a_prompt_error() {
    // A file that ends mid-code-point is invalid UTF-8, not "absent".
    let mut content = vec![b'a'; 10];
    content.extend_from_slice(&[0xe4]); // lead byte of a 3-byte char
    assert_eq!(
        decode_agents(&prefix(content, false)),
        Err(AgentPromptError::InvalidHistory)
    );
}

#[test]
fn bare_carriage_return_is_rejected() {
    let content = b"line one\rX".to_vec();
    assert_eq!(
        decode_agents(&prefix(content, false)),
        Err(AgentPromptError::InvalidHistory)
    );
}

#[test]
fn control_character_is_rejected() {
    let content = b"line one\x01".to_vec();
    assert_eq!(
        decode_agents(&prefix(content, false)),
        Err(AgentPromptError::InvalidHistory)
    );
}

#[test]
fn truncated_content_gets_marker() {
    let content = b"visible prefix".to_vec();
    let decoded = decode_agents(&prefix(content, true)).unwrap();
    assert!(decoded.ends_with("\n[truncated]"));
    assert!(decoded.starts_with("visible prefix"));
}

#[test]
fn crlf_across_the_boundary_is_kept_together() {
    let mut content = vec![b'a'; MAX_AGENTS_BYTES];
    content[MAX_AGENTS_BYTES - 1] = b'\r';
    content.push(b'\n'); // the LF lives in the lookahead beyond the cap
    content.extend_from_slice(b"more\n");
    let prefix = prefix(content, true);
    assert!(prefix.has_more);
    let decoded = decode_agents(&prefix).unwrap();
    assert!(!decoded.contains('\r'), "CRLF pair must stay together");
    assert!(decoded.ends_with("\n[truncated]"));
}

#[test]
fn multibyte_code_point_across_the_boundary_is_completed() {
    let mut content = vec![b'a'; MAX_AGENTS_BYTES - 1];
    // A 3-byte char whose final byte sits in the lookahead region.
    content.extend_from_slice(&[0xe4, 0xb8, 0xad]);
    content.extend_from_slice(b"tail\n");
    let prefix = prefix(content, true);
    let decoded = decode_agents(&prefix).unwrap();
    assert!(decoded.ends_with("\u{4e2d}\n[truncated]"));
}

#[test]
fn incomplete_multibyte_at_boundary_without_lookahead_drops_the_char() {
    // Visible region ends with a lead byte and there is no more data, so
    // the file itself is invalid UTF-8.
    let mut content = vec![b'a'; MAX_AGENTS_BYTES];
    content[MAX_AGENTS_BYTES - 1] = 0xe4;
    assert_eq!(
        decode_agents(&prefix(content, false)),
        Err(AgentPromptError::InvalidHistory)
    );
}

#[tokio::test]
async fn pre_cancelled_prompt_is_rejected_before_workspace_read() {
    let workspace = Arc::new(
        Workspace::open(std::env::temp_dir().to_path_buf())
            .await
            .unwrap(),
    );
    let provider = ProjectPromptProvider::with_auto(
        workspace,
        "system".to_owned(),
        CompactionState::new(),
        None,
    )
    .unwrap();
    let model = ModelDescriptor::new(
        "test".parse::<ModelRef>().unwrap(),
        16_384,
        [ReasoningPreference::Auto].into_iter().collect(),
        true,
    )
    .unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = provider
        .prepare(PromptRequest {
            loop_id: minicore_runtime::LoopId::new().unwrap(),
            request_index: 0,
            history: HistoryView::new(&[], &[]),
            model: &model,
            reasoning: ReasoningPreference::Auto,
            tools: &[],
            cancellation,
            deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        })
        .await;
    assert!(matches!(result, Err(PromptError::Cancelled)));
}

#[tokio::test]
async fn expired_prompt_deadline_is_rejected_before_workspace_read() {
    let workspace = Arc::new(
        Workspace::open(std::env::temp_dir().to_path_buf())
            .await
            .unwrap(),
    );
    let provider = ProjectPromptProvider::with_auto(
        workspace,
        "system".to_owned(),
        CompactionState::new(),
        None,
    )
    .unwrap();
    let model = ModelDescriptor::new(
        "test".parse::<ModelRef>().unwrap(),
        16_384,
        [ReasoningPreference::Auto].into_iter().collect(),
        true,
    )
    .unwrap();
    let result = provider
        .prepare(PromptRequest {
            loop_id: minicore_runtime::LoopId::new().unwrap(),
            request_index: 0,
            history: HistoryView::new(&[], &[]),
            model: &model,
            reasoning: ReasoningPreference::Auto,
            tools: &[],
            cancellation: CancellationToken::new(),
            deadline: tokio::time::Instant::now() - Duration::from_millis(1),
        })
        .await;
    assert!(matches!(result, Err(PromptError::Cancelled)));
}

#[tokio::test]
async fn cancellation_after_read_future_is_pending_returns_cancelled() {
    let temp = crate::test_support::TempDir::new("prompt-gate");
    let workspace = Arc::new(Workspace::open(temp.path().to_path_buf()).await.unwrap());
    let gate = Arc::new(crate::test_support::Gate::new());
    READ_GATE.arm(workspace.root().to_owned(), Arc::clone(&gate));
    let provider = ProjectPromptProvider::with_auto(
        workspace,
        "system".to_owned(),
        CompactionState::new(),
        None,
    )
    .unwrap();
    let model = ModelDescriptor::new(
        "test".parse::<ModelRef>().unwrap(),
        16_384,
        [ReasoningPreference::Auto].into_iter().collect(),
        true,
    )
    .unwrap();
    let cancellation = CancellationToken::new();
    let mut future = provider.prepare(PromptRequest {
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        history: HistoryView::new(&[], &[]),
        model: &model,
        reasoning: ReasoningPreference::Auto,
        tools: &[],
        cancellation: cancellation.clone(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    });
    tokio::select! {
        _ = gate.wait_started() => {}
        result = &mut future => panic!("read completed before it was cancelled: {result:?}"),
    }
    cancellation.cancel();
    assert!(matches!(future.await, Err(PromptError::Cancelled)));
}

#[tokio::test]
async fn deadline_after_read_future_is_pending_returns_cancelled() {
    let temp = crate::test_support::TempDir::new("prompt-gate");
    let workspace = Arc::new(Workspace::open(temp.path().to_path_buf()).await.unwrap());
    let gate = Arc::new(crate::test_support::Gate::new());
    READ_GATE.arm(workspace.root().to_owned(), Arc::clone(&gate));
    let provider = ProjectPromptProvider::with_auto(
        workspace,
        "system".to_owned(),
        CompactionState::new(),
        None,
    )
    .unwrap();
    let model = ModelDescriptor::new(
        "test".parse::<ModelRef>().unwrap(),
        16_384,
        [ReasoningPreference::Auto].into_iter().collect(),
        true,
    )
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
    let mut future = provider.prepare(PromptRequest {
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        history: HistoryView::new(&[], &[]),
        model: &model,
        reasoning: ReasoningPreference::Auto,
        tools: &[],
        cancellation: CancellationToken::new(),
        deadline,
    });
    tokio::select! {
        _ = gate.wait_started() => {}
        result = &mut future => panic!("read completed before its deadline: {result:?}"),
    }
    tokio::time::sleep_until(deadline).await;
    // Runtime exposes both provider cancellation and an expired prompt
    // deadline as PromptError::Cancelled.
    assert!(matches!(future.await, Err(PromptError::Cancelled)));
}

pub(super) async fn read_agents(workspace: &Workspace) -> Result<ReadPrefix, WorkspaceError> {
    if let Some(gate) = READ_GATE.take(&workspace.root().to_owned()) {
        gate.pass().await;
    }
    workspace.read_prefix(AGENTS_PATH, MAX_AGENTS_BYTES).await
}
