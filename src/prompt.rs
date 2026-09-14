use std::sync::Arc;

use minicore_runtime::history::HistoryView;
use minicore_runtime::model::ModelMessage;
use minicore_runtime::prompt::{
    DefaultPromptProvider, PromptError, PromptFuture, PromptProvider, PromptRequest,
};
use minicore_runtime::value::BoundedText;

use crate::compaction::{
    AutoContext, CompactionState, PlanError, UtilityError, plan as plan_auto, summary_data_message,
};
use crate::workspace::{ReadPrefix, Workspace, WorkspaceError};

pub(crate) const AGENTS_PATH: &str = "AGENTS.md";
pub(crate) const MAX_AGENTS_BYTES: usize = 64 * 1024;
const PROJECT_ENVELOPE: &str = "[minicore-project-instructions source=AGENTS.md]";
const TRUNCATED: &str = "[truncated]";

/// Request-level prompt provider that merges the session system prompt with a
/// freshly read workspace `AGENTS.md`, applies the session-local or
/// execution-bound derived snapshot projection, and delegates the remaining
/// history to the runtime `DefaultPromptProvider`.
///
/// `AGENTS.md` is re-read for every model request, so edits become visible at
/// the next request boundary without a file cache or watcher. With automatic
/// compaction enabled, the same request also performs a bounded context
/// estimate and folds complete tool exchanges or settled base items into
/// ephemeral semantic summaries when needed. The disabled path retains Runtime's normal prompt
/// preparation behavior.
pub(crate) struct ProjectPromptProvider {
    workspace: Arc<Workspace>,
    system_prompt: BoundedText,
    compaction: Option<Arc<CompactionState>>,
    bound_summary: Option<BoundedText>,
    auto: Option<AutoContext>,
    #[cfg(test)]
    read_gate: Option<Arc<PromptReadGate>>,
}

impl ProjectPromptProvider {
    pub(crate) fn new(
        workspace: Arc<Workspace>,
        system_prompt: String,
        compaction: Arc<CompactionState>,
    ) -> Result<Self, AgentPromptError> {
        Self::with_auto(workspace, system_prompt, compaction, None)
    }

    pub(crate) fn with_auto(
        workspace: Arc<Workspace>,
        system_prompt: String,
        compaction: Arc<CompactionState>,
        auto: Option<AutoContext>,
    ) -> Result<Self, AgentPromptError> {
        let system_prompt =
            BoundedText::new(system_prompt).map_err(|_| AgentPromptError::InvalidSystemPrompt)?;
        Ok(Self {
            workspace,
            system_prompt,
            compaction: Some(compaction),
            bound_summary: None,
            auto,
            #[cfg(test)]
            read_gate: None,
        })
    }

    pub(crate) fn new_bound(
        workspace: Arc<Workspace>,
        system_prompt: String,
        summary: BoundedText,
        auto: Option<AutoContext>,
        compaction: Arc<CompactionState>,
    ) -> Result<Self, AgentPromptError> {
        let system_prompt =
            BoundedText::new(system_prompt).map_err(|_| AgentPromptError::InvalidSystemPrompt)?;
        Ok(Self {
            workspace,
            system_prompt,
            compaction: Some(compaction),
            bound_summary: Some(summary),
            auto,
            #[cfg(test)]
            read_gate: None,
        })
    }

    #[cfg(test)]
    fn new_with_read_gate(
        workspace: Arc<Workspace>,
        system_prompt: String,
        compaction: Arc<CompactionState>,
        read_gate: Arc<PromptReadGate>,
    ) -> Result<Self, AgentPromptError> {
        let mut provider = Self::new(workspace, system_prompt, compaction)?;
        provider.read_gate = Some(read_gate);
        Ok(provider)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentPromptError {
    InvalidSystemPrompt,
    InvalidHistory,
}

impl PromptProvider for ProjectPromptProvider {
    fn prepare<'a>(&'a self, request: PromptRequest<'a>) -> PromptFuture<'a> {
        if request.cancellation.is_cancelled() || request.deadline <= tokio::time::Instant::now() {
            return Box::pin(async { Err(PromptError::Cancelled) });
        }
        let workspace = Arc::clone(&self.workspace);
        let system_base = self.system_prompt.clone();
        let cancellation = request.cancellation.clone();
        let deadline = request.deadline;
        let loop_id = request.loop_id;
        let request_index = request.request_index;
        let history = request.history;
        let model = request.model;
        let reasoning = request.reasoning;
        let tools = request.tools;
        let compaction = self.compaction.as_ref().map(Arc::clone);
        let bound_summary = self.bound_summary.clone();
        let auto = self.auto.clone();
        #[cfg(test)]
        let read_gate = self.read_gate.clone();
        Box::pin(async move {
            #[cfg(test)]
            let read = read_agents_with_gate(workspace, read_gate);
            #[cfg(not(test))]
            let read = workspace.read_prefix(AGENTS_PATH, MAX_AGENTS_BYTES);
            tokio::pin!(read);
            let prefix = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(PromptError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(PromptError::Cancelled);
                }
                result = &mut read => result,
            };
            let agents = match prefix {
                Ok(prefix) => match decode_agents(&prefix) {
                    Ok(content) => Some(content),
                    Err(_) => return Err(PromptError::InvalidHistory),
                },
                Err(WorkspaceError::NotFound) => None,
                Err(_) => return Err(PromptError::InvalidHistory),
            };
            let system = build_system_prompt(&system_base, agents)
                .map_err(|_| PromptError::InvalidHistory)?;
            let compaction_for_clear = compaction.clone();
            let (projected_base, summary) = if let Some(summary) = bound_summary {
                (history.base(), Some(summary))
            } else if let Some(compaction) = compaction {
                match compaction.project(history.base()) {
                    Some(projection) => (projection.suffix, Some(projection.summary)),
                    None => (history.base(), None),
                }
            } else {
                (history.base(), None)
            };
            let Some(auto) = auto.filter(|auto| auto.policy.enabled) else {
                // Keep the disabled path on Runtime's original provider so
                // automatic compaction does not change established history
                // projection semantics.
                let provider = DefaultPromptProvider::new(Some(system));
                let projected_history = HistoryView::new(projected_base, history.appended());
                let mut prepared = provider
                    .prepare(PromptRequest {
                        loop_id,
                        request_index,
                        history: projected_history,
                        model,
                        reasoning,
                        tools,
                        cancellation,
                        deadline,
                    })
                    .await?;
                if let Some(summary) = summary {
                    let summary_message =
                        summary_data_message(&summary).map_err(|_| PromptError::InvalidHistory)?;
                    let insert_at = prepared
                        .messages
                        .iter()
                        .position(|message| matches!(message, ModelMessage::System(_)))
                        .map_or(0, |index| index + 1);
                    prepared.messages.insert(insert_at, summary_message);
                }
                if let Some(compaction) = compaction_for_clear {
                    compaction.clear_prepare_failure();
                }
                return Ok(prepared);
            };
            // History items are projected exactly as the runtime
            // `DefaultPromptProvider` would, then kept separate from the
            // system/durable-summary prefix so groups can be folded.
            let (fixed, history_messages) = crate::compaction::auto_compose(
                &system,
                summary.as_ref(),
                projected_base,
                history.appended(),
            )
            .map_err(|_| PromptError::InvalidHistory)?;
            let budget = auto.policy.budget(model.context_window);
            match plan_auto(
                &fixed,
                history_messages,
                &system,
                projected_base,
                history.appended(),
                tools,
                reasoning,
                budget,
                &auto,
                deadline.into_std(),
                &cancellation,
                loop_id,
                request_index,
            )
            .await
            {
                Ok(messages) => {
                    auto.state.clear_prepare_failure();
                    Ok(minicore_runtime::prompt::PreparedPrompt { messages })
                }
                Err(PlanError::Cancelled) => Err(PromptError::Cancelled),
                Err(PlanError::Uncompressible) => {
                    auto.state
                        .note_prepare_failure(crate::compaction::CONTEXT_UNCOMPRESSIBLE);
                    Err(PromptError::InvalidHistory)
                }
                Err(PlanError::Utility(UtilityError::Cancelled)) => Err(PromptError::Cancelled),
                Err(PlanError::Utility(UtilityError::Timeout)) => {
                    auto.state
                        .note_prepare_failure(UtilityError::Timeout.kind());
                    Err(PromptError::Cancelled)
                }
                // A failed semantic summary means the request cannot be
                // prepared within budget; report it as uncompressible rather
                // than sending a truncated or untrusted context.
                Err(PlanError::Utility(error)) => {
                    auto.state.note_prepare_failure(error.kind());
                    Err(PromptError::InvalidHistory)
                }
            }
        })
    }
}

#[cfg(test)]
struct PromptReadGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl PromptReadGate {
    fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    async fn wait_until_pending(&self) {
        self.entered.notified().await;
    }
}

#[cfg(test)]
async fn read_agents_with_gate(
    workspace: Arc<Workspace>,
    gate: Option<Arc<PromptReadGate>>,
) -> Result<ReadPrefix, WorkspaceError> {
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    workspace.read_prefix(AGENTS_PATH, MAX_AGENTS_BYTES).await
}

/// Reads and validates the workspace `AGENTS.md` root file content. An
/// absent file is handled by the caller; any invalid content (bad UTF-8 or a
/// disallowed control character) is a prompt error, never silently treated as
/// "no AGENTS.md".
pub(crate) fn decode_agents(prefix: &ReadPrefix) -> Result<String, AgentPromptError> {
    if prefix.bytes.is_empty() {
        return Ok(String::new());
    }
    let cap = MAX_AGENTS_BYTES;
    let total = prefix.bytes.len();
    let has_more = prefix.has_more;

    // Retained byte length: the visible prefix (up to `cap`) plus only the
    // lookahead bytes needed to keep one straddling CRLF pair or one
    // multi-byte code point intact across the 64 KiB boundary.
    let mut end = if has_more { cap.min(total) } else { total };
    if end < total && end > 0 && prefix.bytes[end - 1] == b'\r' && prefix.bytes[end] == b'\n' {
        end += 1;
    }
    while end < total && prefix.bytes[end] & 0b1100_0000 == 0b1000_0000 {
        end += 1;
    }
    let content = match std::str::from_utf8(&prefix.bytes[..end]) {
        Ok(text) => text,
        Err(error) => {
            if error.error_len().is_some() || !has_more {
                // A genuinely invalid byte sequence, or a file that ends
                // mid-code-point: both are invalid UTF-8, never "no AGENTS".
                return Err(AgentPromptError::InvalidHistory);
            }
            // Incomplete trailing code point that did not fit in the
            // lookahead: drop it and truncate at the last clean boundary.
            std::str::from_utf8(&prefix.bytes[..error.valid_up_to()])
                .map_err(|_| AgentPromptError::InvalidHistory)?
        }
    };

    // No control characters other than LF/TAB survive; a bare CR (one that is
    // not part of a CRLF pair retained across the boundary) is rejected too.
    let mut content = content.replace("\r\n", "\n");
    if content
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(AgentPromptError::InvalidHistory);
    }
    if has_more {
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(TRUNCATED);
    }
    Ok(content)
}

/// Combines the session system prompt with optional project instructions,
/// keeping the profile half intact and truncating only the AGENTS tail.
pub(crate) fn build_system_prompt(
    system_prompt: &BoundedText,
    agents: Option<String>,
) -> Result<BoundedText, ()> {
    let Some(agents) = agents.filter(|content| !content.is_empty()) else {
        return Ok(system_prompt.clone());
    };
    let mut text = String::with_capacity(
        system_prompt.byte_len() + PROJECT_ENVELOPE.len() + agents.len() + 16,
    );
    text.push_str(system_prompt.as_str());
    if !system_prompt.as_str().ends_with('\n') {
        text.push('\n');
    }
    text.push('\n');
    text.push_str(PROJECT_ENVELOPE);
    text.push('\n');
    text.push_str(&agents);
    BoundedText::new(text).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use minicore_runtime::history::HistoryView;
    use minicore_runtime::model::{ModelDescriptor, ModelRef, ReasoningPreference};
    use minicore_runtime::prompt::PromptError;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn prefix(bytes: Vec<u8>, has_more: bool) -> ReadPrefix {
        let visible_len = bytes.len().min(MAX_AGENTS_BYTES);
        ReadPrefix {
            bytes,
            visible_len,
            has_more,
        }
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
        let provider =
            ProjectPromptProvider::new(workspace, "system".to_owned(), CompactionState::new())
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
        let provider =
            ProjectPromptProvider::new(workspace, "system".to_owned(), CompactionState::new())
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
        let workspace = Arc::new(
            Workspace::open(std::env::temp_dir().to_path_buf())
                .await
                .unwrap(),
        );
        let gate = Arc::new(PromptReadGate::new());
        let provider = ProjectPromptProvider::new_with_read_gate(
            workspace,
            "system".to_owned(),
            CompactionState::new(),
            Arc::clone(&gate),
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
            _ = gate.wait_until_pending() => {}
            result = &mut future => panic!("read completed before it was cancelled: {result:?}"),
        }
        cancellation.cancel();
        assert!(matches!(future.await, Err(PromptError::Cancelled)));
    }

    #[tokio::test]
    async fn deadline_after_read_future_is_pending_returns_cancelled() {
        let workspace = Arc::new(
            Workspace::open(std::env::temp_dir().to_path_buf())
                .await
                .unwrap(),
        );
        let gate = Arc::new(PromptReadGate::new());
        let provider = ProjectPromptProvider::new_with_read_gate(
            workspace,
            "system".to_owned(),
            CompactionState::new(),
            Arc::clone(&gate),
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
            _ = gate.wait_until_pending() => {}
            result = &mut future => panic!("read completed before its deadline: {result:?}"),
        }
        tokio::time::sleep_until(deadline).await;
        // Runtime exposes both provider cancellation and an expired prompt
        // deadline as PromptError::Cancelled.
        assert!(matches!(future.await, Err(PromptError::Cancelled)));
    }
}
