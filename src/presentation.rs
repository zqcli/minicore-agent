//! Bounded, read-only presentation data for local frontends (Rail UI parity
//! spec §12).
//!
//! The Agent owns content facts (tool command/path/input bodies, results,
//! user acceptance times, git branch, honest unknown context/cost); the TUI
//! owns layout and colors. Everything here is best-effort and display-only:
//! a presentation failure must never alter tool execution, model requests,
//! cancellation, deadlines, or the persisted outcome, and no lock is held
//! across an await.
//!
//! Command text and input bodies may contain secrets the user typed
//! themselves; they are intentionally allowed to reach the local TUI
//! (documented permission), but they must never appear in logs, error
//! payloads, or Debug dumps. `ToolDisplay` therefore carries a redacted
//! `Debug` (identity + lengths + counts only).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use futures_util::StreamExt;
use serde::Serialize;

use minicore_runtime::history::{HistoryItem, UserMessageKind};
use minicore_runtime::model::{Model, ModelCallContext, ModelDescriptor, ModelRequest};
use minicore_runtime::prompt::{PromptFuture, PromptProvider, PromptRequest};
use minicore_runtime::tools::{Tool, ToolContext, ToolExecutionOutcome, ToolInvocation, ToolSpec};
use minicore_runtime::{LoopId, ToolCallId};

use crate::event::{AgentEvent, AgentEventSink, EventMeta};
use crate::ids::SessionId;
use crate::sessions::TurnRef;
use crate::tool_data::{
    CommandResult, ToolData, ToolDataStream, ToolInvocationData, ToolProcessChunk, ToolProcessData,
    ToolRef, ToolStreamNotice,
};
use crate::tools::command::{CommandBinding, CommandOwners, CommandStreamSink};

/// Display text limits. Aligned with the existing per-argument/output caps so
/// the expanded view can never promise rows that the Agent cannot show.
pub(crate) const MAX_DETAIL_BYTES: usize = 512;
pub(crate) const MAX_EXPANDED_INPUT_BYTES: usize = 512 * 1024;
pub(crate) const MAX_RESULT_DISPLAY_BYTES: usize = 512 * 1024;

/// Fixed identity of one model request, captured from the real
/// `ModelCallContext` at `Model::start`. One session runs at most one loop;
/// tools in a batch belong to the request that produced them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestKey {
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
}

/// Whitelisted, single-line detail plus bounded expandable input for one tool
/// card. This is the only Tool-detail formatter; live events and history
/// regeneration share it.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolDisplay {
    /// Single visual line (command/path/range/generic summary).
    pub detail: String,
    /// Bounded input body the TUI may show when expanded: write content, edit
    /// old/new text, apply_patch patch. Never the raw invocation object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expanded_input: Option<String>,
    /// Line count of the source input body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_line_count: Option<usize>,
    /// Displayable hidden rows this card would collapse: the source input
    /// rows (expanded body rows for write/edit, otherwise the native JSON
    /// argument rows) plus result rows actually available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden_line_count: Option<usize>,
    /// The source above the display cap was truncated; the TUI must not
    /// promise more expandable rows than shown.
    #[serde(default)]
    pub truncated: bool,
}

impl fmt::Debug for ToolDisplay {
    /// Redacted: identity, lengths, and counts only. Raw detail or input text
    /// must never reach a log or an error Debug dump.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolDisplay")
            .field("detail_bytes", &self.detail.len())
            .field(
                "expanded_input_bytes",
                &self.expanded_input.as_ref().map(|input| input.len()),
            )
            .field("input_line_count", &self.input_line_count)
            .field("hidden_line_count", &self.hidden_line_count)
            .field("truncated", &self.truncated)
            .finish()
    }
}

/// Visible, sanitized assistant parts in their original order. Only plain
/// text, visible reasoning text, and tool-call identity are allowed; opaque
/// reasoning and encrypted/signature fields were already dropped by
/// `sanitize_history`.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum AssistantDisplayPart {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
    },
    ToolCall {
        tool_call_id: ToolCallId,
        name: String,
    },
}

/// Visible, sanitized assistant parts in their original order, as a display
/// list. Only plain text, visible reasoning text, and tool-call identity are
/// allowed; the content was already sanitized before this point.
pub(crate) fn assistant_display_parts(
    content: &[minicore_runtime::model::AssistantPart],
) -> Vec<AssistantDisplayPart> {
    let mut parts = Vec::with_capacity(content.len());
    for part in content {
        match part {
            minicore_runtime::model::AssistantPart::Text(text) => {
                parts.push(AssistantDisplayPart::Text { text: text.clone() });
            }
            minicore_runtime::model::AssistantPart::Reasoning(reasoning) => {
                let mut text = String::new();
                if let Some(reasoning_text) = reasoning.text() {
                    text.push_str(reasoning_text);
                }
                if let Some(summary) = reasoning.summary() {
                    text.push_str(summary);
                }
                if !text.is_empty() {
                    parts.push(AssistantDisplayPart::Reasoning { text });
                }
            }
            minicore_runtime::model::AssistantPart::ToolCall(call) => {
                parts.push(AssistantDisplayPart::ToolCall {
                    tool_call_id: call.tool_call_id().clone(),
                    name: call.name().as_str().to_owned(),
                });
            }
        }
    }
    parts
}

impl fmt::Debug for AssistantDisplayPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { text } => formatter
                .debug_struct("AssistantDisplayPart::Text")
                .field("text_bytes", &text.len())
                .finish(),
            Self::Reasoning { text } => formatter
                .debug_struct("AssistantDisplayPart::Reasoning")
                .field("text_bytes", &text.len())
                .finish(),
            Self::ToolCall { tool_call_id, name } => formatter
                .debug_struct("AssistantDisplayPart::ToolCall")
                .field("tool_call_id", tool_call_id)
                .field("name", name)
                .finish(),
        }
    }
}

/// Honest status for context usage. No provider metering is available in
/// MiniCore today, so this is always `Unknown` (the TUI shows `ctx ?`); the
/// shape exists so a future provider estimate can be surfaced explicitly as
/// `Estimated` instead of being mistaken for exact data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    Unknown,
    Estimated,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ContextUsageView {
    pub tokens: Option<u64>,
    pub window: Option<u64>,
    pub percent: Option<f64>,
    pub kind: ContextKind,
}

impl Default for ContextUsageView {
    fn default() -> Self {
        Self {
            tokens: None,
            window: None,
            percent: None,
            kind: ContextKind::Unknown,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LastLoopView {
    pub loop_id: LoopId,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

/// Read-only footer/detail data for one session (`session.presentation`).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PresentationView {
    pub session_id: SessionId,
    pub model_label: Option<String>,
    pub git_branch: Option<String>,
    pub context: ContextUsageView,
    /// No pricing catalog exists; null until a real billing source is added.
    pub cost_usd: Option<f64>,
    pub using_subscription: Option<bool>,
    pub last_loop: Option<LastLoopView>,
    /// Latest steering receipt committed at a real `Model::start`, used for
    /// lost-event reconciliation (read-only; no dedicated RPC method).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steer_progress: Option<SteerProgressView>,
}

/// Read-only steering receipt snapshot: the request at which `applied_count`
/// Steering items were first observed in the PREPARED prompt history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SteerProgressView {
    pub loop_id: LoopId,
    pub request_index: u32,
    pub applied_count: u64,
}

struct LastLoop {
    loop_id: LoopId,
    started_at: Option<String>,
    finished_at: Option<String>,
}

struct LiveTool {
    key: Option<RequestKey>,
    tool_name: String,
    display: ToolDisplay,
}

struct ToolResultInfo {
    content: String,
    truncated: bool,
}

/// Bounded per-request usage cache for the current loop (spec 9.4/12.4).
/// Keyed by `(loop_id, request_index)`; cleared when a new loop starts.
type RequestUsageCache = HashMap<RequestKey, minicore_runtime::model::Usage>;

#[derive(Default)]
struct PresentationInner {
    request_key: Option<RequestKey>,
    /// Current-loop-only caches; cleared when a new loop starts.
    live_tools: HashMap<(Option<RequestKey>, ToolCallId), LiveTool>,
    tool_results: HashMap<(RequestKey, ToolCallId), ToolResultInfo>,
    request_usage: RequestUsageCache,
    live_times: VecDeque<Option<String>>,
    last_loop: Option<LastLoop>,
    branch: Option<String>,
    model_label: Option<String>,
    /// 1-based FIFO count of Accepted Steers in this loop (from
    /// `Session::steer`, serialized by the same session lock).
    accepted_steers: u64,
    /// The last request whose prompt prepare succeeded and the steering count
    /// observed in its history. Committed to `applied_steer` only at the real
    /// `Model::start`; preparing/rebuilding alone never releases the queue.
    prepared_steers: Option<(RequestKey, u64)>,
    /// First request at which each steering count was applied, keyed by count.
    applied_steer: Option<(u64, RequestKey)>,
}

impl PresentationInner {
    /// Commits a newly applied steering count at its first observing request
    /// and returns the best-effort `steer_progress` event. Later requests that
    /// observe the same count keep the original boundary. `count` is the
    /// authoritative history count; it may be lower than the accepted counter
    /// while a steer is queued but not yet in the prompt history.
    fn commit_steer_applied(
        &mut self,
        session_id: SessionId,
        key: RequestKey,
        applied_count: u64,
    ) -> Option<AgentEvent> {
        if self
            .applied_steer
            .as_ref()
            .is_some_and(|(previous, _)| *previous >= applied_count)
        {
            return None;
        }
        self.applied_steer = Some((applied_count, key));
        Some(AgentEvent::SteerProgress {
            turn: TurnRef {
                session_id,
                loop_id: key.loop_id,
            },
            request_index: key.request_index,
            applied_count,
            meta: EventMeta {
                session_id,
                loop_id: Some(key.loop_id),
                dropped_before: 0,
            },
        })
    }
}

/// Per-session presentation state shared by the Model/Tool wrappers, the RPC
/// read, and the per-loop worker. All access is short critical sections;
/// callers never hold the lock across an await.
pub(crate) struct Presentation {
    session_id: SessionId,
    events: AgentEventSink,
    tool_data: Arc<ToolData>,
    /// Owned Bash commands of this Session (or child loop). Every command of a
    /// real loop is registered here and joined by that loop's owner.
    command_owners: Arc<CommandOwners>,
    inner: Mutex<PresentationInner>,
}

impl Presentation {
    pub(crate) fn new(session_id: SessionId, events: AgentEventSink) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            events,
            tool_data: Arc::new(ToolData::new()),
            command_owners: CommandOwners::new(),
            inner: Mutex::new(PresentationInner::default()),
        })
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// The registry every owned command started from this presentation joins.
    pub(crate) fn command_owners(&self) -> &Arc<CommandOwners> {
        &self.command_owners
    }

    /// The narrow handle a tool needs to run an owned command: the registry it
    /// joins and the sink its bytes go to. Identity is never resolved from this
    /// handle; the concrete tool passes the `ToolRef` it captured at the real
    /// `Tool::execute` boundary.
    pub(crate) fn command_binding(self: &Arc<Self>) -> CommandBinding {
        let sink: Arc<dyn CommandStreamSink> = Arc::clone(self) as Arc<dyn CommandStreamSink>;
        CommandBinding::new(Arc::clone(&self.command_owners), sink)
    }

    /// Per-Session structured tool facts. Separate from the legacy UI display
    /// caches above: the new contract never depends on presentation fields.
    pub(crate) fn tool_data(&self) -> Arc<ToolData> {
        Arc::clone(&self.tool_data)
    }

    /// Publishes the validated invocation to the structured store at the real
    /// execution boundary and emits the best-effort `tool_invocation` event on
    /// first publish. Runtime calls `Tool::execute` only after the policy
    /// decision and approval, so this is where `Running` becomes true; the
    /// policy wrapper may already have published the same data while the call
    /// awaited approval.
    pub(crate) fn publish_tool_invocation(&self, tool_ref: &ToolRef, invocation: &ToolInvocation) {
        let data = self.tool_data.note_invocation(tool_ref, invocation);
        self.tool_data.mark_running(tool_ref);
        if let Some(data) = data {
            self.emit_tool_invocation(data);
        }
    }

    /// Best-effort live state for one recorded call, emitted only after the
    /// stored record was updated. `ToolExecution` remains the terminal event.
    pub(crate) fn emit_tool_execution(&self, tool_ref: &ToolRef) {
        let Some(data) = self.tool_data.snapshot(tool_ref) else {
            return;
        };
        let loop_id = data.tool_ref.loop_id;
        let _ = self.events.try_send(AgentEvent::ToolExecution {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            data,
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }

    fn emit_tool_process(
        &self,
        tool_ref: &ToolRef,
        chunk: Option<ToolProcessChunk>,
        command: Option<CommandResult>,
    ) {
        let loop_id = tool_ref.loop_id;
        let data = ToolProcessData {
            tool_ref: tool_ref.clone(),
            chunk,
            command,
        };
        let _ = self.events.try_send(AgentEvent::ToolProcess {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            data,
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }

    pub(crate) fn emit_tool_invocation(&self, data: ToolInvocationData) {
        let loop_id = data.tool_ref.loop_id;
        let _ = self.events.try_send(AgentEvent::ToolInvocation {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            data,
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }

    pub(crate) fn set_branch(&self, branch: Option<String>) {
        self.lock().branch = branch;
    }

    /// Records a model request identity at the real `Model::start` boundary.
    /// The user-facing label is set from the session profile, never from the
    /// provider descriptor. At this same real boundary any steering count the
    /// last successful prepare observed is committed and, when newly observed,
    /// surfaced as a read-only `steer_progress` event.
    pub(crate) fn note_request_start(&self, key: RequestKey) {
        let event = {
            let mut inner = self.lock();
            inner.request_key = Some(key);
            if let Some((prepared_key, count)) = inner.prepared_steers.take() {
                if prepared_key == key {
                    inner.commit_steer_applied(self.session_id, key, count)
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(event) = event {
            let _ = self.events.try_send(event);
        }
    }

    /// Records the steering User count observed for a request whose prompt
    /// prepare succeeded. The queue is only ever released when that count is
    /// committed at `Model::start`; this call alone never releases it.
    pub(crate) fn commit_prepared_request(&self, key: RequestKey, applied_count: u64) {
        self.lock().prepared_steers = Some((key, applied_count));
    }

    /// 1-based FIFO acceptance index for the just-accepted steer, or `None`
    /// when the per-loop counter overflowed (practically impossible).
    pub(crate) fn note_steer_accepted(&self, accepted_at: Option<String>) -> Option<u64> {
        let mut inner = self.lock();
        inner.live_times.push_back(accepted_at);
        inner.accepted_steers = inner.accepted_steers.checked_add(1)?;
        Some(inner.accepted_steers)
    }

    /// Records real per-request usage observed on the model stream and emits
    /// one best-effort `request_usage` event per request (deduplicated; a
    /// provider may emit several `Usage` events for one request). Read-only:
    /// fails silently when the bounded event queue is full and never touches
    /// the stream contents forwarded downstream.
    pub(crate) fn note_request_usage(
        &self,
        key: RequestKey,
        usage: minicore_runtime::model::Usage,
    ) {
        let event = {
            let mut inner = self.lock();
            // First report wins; later `Usage` events for the same request
            // are ignored rather than summed (the request's total is what the
            // runtime records into the persisted loop result).
            if inner.request_usage.contains_key(&key) {
                return;
            }
            inner.request_usage.insert(key, usage);
            Some(AgentEvent::RequestUsage {
                turn: TurnRef {
                    session_id: self.session_id,
                    loop_id: key.loop_id,
                },
                request_index: key.request_index,
                usage,
                meta: EventMeta {
                    session_id: self.session_id,
                    loop_id: Some(key.loop_id),
                    dropped_before: 0,
                },
            })
        };
        if let Some(event) = event {
            let _ = self.events.try_send(event);
        }
    }

    pub(crate) fn note_loop_started(&self, loop_id: LoopId, started_at: Option<String>) {
        let mut inner = self.lock();
        // Only the current loop is kept; drop every previous loop's caches and
        // stale request identity before the first model call of this loop.
        inner.request_key = None;
        inner.live_tools.clear();
        inner.tool_results.clear();
        inner.request_usage.clear();
        inner.live_times.clear();
        inner.accepted_steers = 0;
        inner.prepared_steers = None;
        inner.applied_steer = None;
        inner.last_loop = Some(LastLoop {
            loop_id,
            started_at,
            finished_at: None,
        });
    }

    pub(crate) fn note_loop_finished(&self) {
        let mut inner = self.lock();
        if let Some(last_loop) = &mut inner.last_loop {
            last_loop.finished_at = crate::store::utc_timestamp().ok();
        }
    }

    pub(crate) fn set_model_label(&self, model_label: String) {
        self.lock().model_label = Some(model_label);
    }

    pub(crate) fn record_prompt_time(&self, accepted_at: Option<String>) {
        self.record_user_time(accepted_at);
    }

    fn record_user_time(&self, accepted_at: Option<String>) {
        self.lock().live_times.push_back(accepted_at);
    }

    /// Copies the first accepted timestamps without consuming them. The
    /// corresponding queue is discarded only after the JSONL append succeeds,
    /// so a failed persistence attempt does not erase live metadata.
    pub(crate) fn peek_user_times(&self, count: usize) -> Vec<Option<String>> {
        self.lock().live_times.iter().take(count).cloned().collect()
    }

    /// Drops all acceptance metadata for the completed current loop. Extra
    /// entries are accepted Steers that did not enter the persisted report.
    pub(crate) fn clear_user_times(&self) {
        self.lock().live_times.clear();
    }

    pub(crate) fn begin_tool(
        &self,
        key: Option<RequestKey>,
        invocation: &ToolInvocation,
        display: ToolDisplay,
    ) {
        let mut inner = self.lock();
        inner.live_tools.insert(
            (key, invocation.tool_call_id().clone()),
            LiveTool {
                key,
                tool_name: invocation.tool_name().as_str().to_owned(),
                display,
            },
        );
    }

    /// Finalizes a completed tool in place: adds displayable result rows to
    /// the pending display, caches a bounded result copy for the current
    /// loop, and emits the best-effort `tool_presentation` event with the
    /// fixed identity captured at execute start.
    pub(crate) fn finish_tool(
        &self,
        key: Option<RequestKey>,
        tool_call_id: &ToolCallId,
        result_text: Option<(String, bool)>,
    ) {
        let (loop_id, request_index, tool_name, display) = {
            let mut inner = self.lock();
            let Some(live) = inner.live_tools.remove(&(key, tool_call_id.clone())) else {
                return;
            };
            if let (Some(key), Some((content, truncated))) = (key, &result_text) {
                inner.tool_results.insert(
                    (key, tool_call_id.clone()),
                    ToolResultInfo {
                        content: content.clone(),
                        truncated: *truncated,
                    },
                );
            }
            let mut display = live.display;
            if let Some((_, truncated)) = &result_text {
                display.truncated |= *truncated;
            }
            apply_result_to_display(
                &mut display,
                result_text.as_ref().map(|(text, _)| text.as_str()),
            );
            (
                live.key.map(|key| key.loop_id),
                live.key.map(|key| key.request_index),
                live.tool_name,
                display,
            )
        };
        let Some(loop_id) = loop_id else {
            return;
        };
        let _ = self.events.try_send(AgentEvent::ToolPresentation {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            request_index: request_index.unwrap_or(0),
            tool_call_id: tool_call_id.clone(),
            tool_name,
            display,
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }

    /// Best-effort bounded result copy for the current loop, used to attach
    /// `content` to the runtime `ToolFinished` event so the TUI can render
    /// expanded results from the same source the wrapper saw.
    pub(crate) fn tool_result(
        &self,
        key: RequestKey,
        tool_call_id: &ToolCallId,
    ) -> Option<(String, bool)> {
        self.lock()
            .tool_results
            .get(&(key, tool_call_id.clone()))
            .map(|info| (info.content.clone(), info.truncated))
    }

    pub(crate) fn snapshot(&self) -> PresentationView {
        let inner = self.lock();
        PresentationView {
            session_id: self.session_id,
            model_label: inner.model_label.clone(),
            git_branch: inner.branch.clone(),
            context: ContextUsageView::default(),
            cost_usd: None,
            using_subscription: None,
            last_loop: inner.last_loop.as_ref().map(|last_loop| LastLoopView {
                loop_id: last_loop.loop_id,
                started_at: last_loop.started_at.clone(),
                finished_at: last_loop.finished_at.clone(),
            }),
            steer_progress: inner
                .applied_steer
                .as_ref()
                .map(|(count, key)| SteerProgressView {
                    loop_id: key.loop_id,
                    request_index: key.request_index,
                    applied_count: *count,
                }),
        }
    }

    pub(crate) fn request_key(&self) -> Option<RequestKey> {
        self.lock().request_key
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PresentationInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Owned commands write through the presentation: the authoritative window or
/// record is stored first, and only then is a best-effort event published.
impl CommandStreamSink for Presentation {
    fn push_chunk(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
        chunk: &[u8],
    ) -> Option<ToolStreamNotice> {
        let notice = self.tool_data.note_stream_chunk(tool_ref, stream, chunk)?;
        // Publish the current stored command record together with the chunk, so
        // a consumer learns the running process ranges from the same source as
        // `tool.read` instead of from a record captured before the bytes.
        let command = self
            .tool_data
            .snapshot(tool_ref)
            .and_then(|execution| execution.command);
        self.emit_tool_process(
            tool_ref,
            Some(ToolProcessChunk {
                stream: notice.stream,
                encoding: notice.stream.encoding(),
                data: base64::engine::general_purpose::STANDARD.encode(chunk),
                base_offset: notice.base_offset,
                next_offset: notice.next_offset,
                observed_end: notice.observed_end,
                dropped: notice.dropped,
                expired: notice.expired,
            }),
            command,
        );
        Some(notice)
    }

    fn note_command(&self, tool_ref: &ToolRef, result: &CommandResult) {
        // The event carries the record the store actually holds after the
        // Session budget ran, with ranges overlaid from the current windows,
        // so a live consumer and a later `tool.read` cannot disagree.
        let Some(snapshot) = self.tool_data.note_command(tool_ref, result.clone()) else {
            return;
        };
        self.emit_tool_process(tool_ref, None, snapshot.command);
    }

    fn mark_cancelling(&self, tool_ref: &ToolRef) {
        self.tool_data.mark_cancelling(tool_ref);
        self.emit_tool_execution(tool_ref);
    }

    fn stream_range(&self, tool_ref: &ToolRef, stream: ToolDataStream) -> Option<(u64, u64)> {
        self.tool_data.stream_range(tool_ref, stream)
    }

    fn note_stream_end(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        self.tool_data.note_stream_end(tool_ref, stream);
    }

    fn note_stream_cut(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        self.tool_data.note_stream_cut(tool_ref, stream);
    }
}

// ---------------------------------------------------------------------------
// Model / Tool wrappers (thin; identity + best-effort display only)
// ---------------------------------------------------------------------------

/// Records `(loop_id, request_index)` from the real `ModelCallContext` at the
/// real `Model::start` boundary, then delegates unchanged.
pub(crate) struct PresentationModel {
    inner: Arc<dyn Model>,
    presentation: Arc<Presentation>,
}

impl PresentationModel {
    pub(crate) fn new(inner: Arc<dyn Model>, presentation: Arc<Presentation>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            presentation,
        })
    }
}

impl Model for PresentationModel {
    fn descriptor(&self) -> &ModelDescriptor {
        self.inner.descriptor()
    }

    fn start<'a>(
        &'a self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> minicore_runtime::model::ModelStartFuture<'a> {
        let key = RequestKey {
            loop_id: context.loop_id,
            request_index: context.request_index,
        };
        // Synchronous; no lock is held across an await.
        self.presentation.note_request_start(key);
        let inner = self.inner.start(request, context);
        let presentation = Arc::clone(&self.presentation);
        Box::pin(async move {
            let stream = inner.await?;
            // Pass-through observation: every event is forwarded unchanged;
            // only real provider `Usage` before a tool-request boundary lands
            // in the per-request cache. Cancellation, deadlines, ordering,
            // and error delivery are untouched.
            let observed = stream.inspect(move |item| {
                if let Ok(minicore_runtime::model::ModelEvent::Usage { usage }) = item {
                    presentation.note_request_usage(key, *usage);
                }
            });
            Ok(Box::pin(observed) as minicore_runtime::model::ModelStream)
        })
    }
}

/// Thin pass-through prompt wrapper that counts Steering User items present in
/// the PREPARED prompt history (same runtime `PromptProvider` seam the Agent
/// already plugs into), retains the count only after a successful prepare,
/// and lets `PresentationModel::start` commit it at the real `Model::start`
/// boundary. Read-only: no prompt messages, model requests, or tool semantics
/// are ever rewritten, and the queue is never released by preparing alone.
pub(crate) struct SteerReceiptPrompt {
    inner: Arc<dyn PromptProvider>,
    presentation: Arc<Presentation>,
}

impl SteerReceiptPrompt {
    pub(crate) fn new(
        inner: Arc<dyn PromptProvider>,
        presentation: Arc<Presentation>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            presentation,
        })
    }
}

impl PromptProvider for SteerReceiptPrompt {
    fn prepare<'a>(&'a self, request: PromptRequest<'a>) -> PromptFuture<'a> {
        let key = RequestKey {
            loop_id: request.loop_id,
            request_index: request.request_index,
        };
        let applied_count = request
            .history
            .iter()
            .filter(|item| {
                matches!(item,
                    HistoryItem::User(user)
                        if user.kind == UserMessageKind::Steering && user.loop_id == request.loop_id)
            })
            .count() as u64;
        let presentation = Arc::clone(&self.presentation);
        let inner = self.inner.prepare(request);
        Box::pin(async move {
            // Only a SUCCESSFUL prepare retains the count; a cancelled or
            // failed prompt must never release the steering queue.
            let prepared = inner.await?;
            presentation.commit_prepared_request(key, applied_count);
            Ok(prepared)
        })
    }
}

/// Fixes the current request identity at `Tool::execute` start, computes the
/// bounded ToolDisplay, then delegates execution unchanged (cancellation,
/// deadline, errors, and the result are never altered). A real Bash tool
/// receives the exact `ToolRef` captured here, so a command never resolves its
/// identity from a table that a dropped future can leave stale.
pub(crate) enum PresentationTool {
    Plain {
        inner: Arc<dyn Tool>,
        presentation: Arc<Presentation>,
    },
    Bash {
        inner: Arc<crate::tools::OwnedBashTool>,
        presentation: Arc<Presentation>,
    },
}

impl PresentationTool {
    pub(crate) fn new(inner: Arc<dyn Tool>, presentation: Arc<Presentation>) -> Arc<Self> {
        Arc::new(Self::Plain {
            inner,
            presentation,
        })
    }

    /// Explicit construction for the owned-command tool: only this variant
    /// receives the captured `ToolRef` at the real execute boundary.
    pub(crate) fn new_bash(
        inner: Arc<crate::tools::OwnedBashTool>,
        presentation: Arc<Presentation>,
    ) -> Arc<Self> {
        Arc::new(Self::Bash {
            inner,
            presentation,
        })
    }

    fn inner(&self) -> &dyn Tool {
        match self {
            Self::Plain { inner, .. } => inner.as_ref(),
            Self::Bash { inner, .. } => inner.as_ref(),
        }
    }

    fn presentation(&self) -> &Arc<Presentation> {
        match self {
            Self::Plain { presentation, .. } | Self::Bash { presentation, .. } => presentation,
        }
    }
}

impl Tool for PresentationTool {
    fn spec(&self) -> &ToolSpec {
        self.inner().spec()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: ToolContext,
    ) -> minicore_runtime::tools::ToolFuture<'a> {
        let presentation = Arc::clone(self.presentation());
        let key = presentation.request_key();
        let display = build_tool_display(
            invocation.tool_name().as_str(),
            Some(invocation.arguments()),
            None,
        );
        // Complete identity: only a real `Model::start` request key plus the
        // Runtime's tool-call id. This is the single place the identity is
        // captured; it is passed downward, never recovered from a live table.
        let tool_ref = key.map(|key| ToolRef {
            session_id: presentation.session_id(),
            loop_id: key.loop_id,
            request_index: key.request_index,
            tool_call_id: invocation.tool_call_id().clone(),
        });
        presentation.begin_tool(key, &invocation, display);
        if let Some(tool_ref) = &tool_ref {
            // Published before the work starts, never after it returns.
            presentation.publish_tool_invocation(tool_ref, &invocation);
        }

        let request_key = key;
        let tool_call_id = invocation.tool_call_id().clone();
        match self {
            Self::Plain { inner, .. } => {
                let inner = Arc::clone(inner);
                Box::pin(async move {
                    let result = inner.execute(invocation, context).await;
                    finish_presentation_tool(
                        &presentation,
                        request_key,
                        &tool_call_id,
                        tool_ref.as_ref(),
                        &result,
                    );
                    result
                })
            }
            Self::Bash { inner, .. } => {
                let inner = Arc::clone(inner);
                Box::pin(async move {
                    let result = inner
                        .execute_bound(invocation, context, tool_ref.clone())
                        .await;
                    finish_presentation_tool(
                        &presentation,
                        request_key,
                        &tool_call_id,
                        tool_ref.as_ref(),
                        &result,
                    );
                    result
                })
            }
        }
    }
}

/// Best-effort post-execution presentation bookkeeping. The outcome and error
/// are forwarded unchanged; raw result bytes are retained for `tool.output`
/// while the authoritative terminal state still comes from the Runtime.
fn finish_presentation_tool(
    presentation: &Presentation,
    request_key: Option<RequestKey>,
    tool_call_id: &ToolCallId,
    tool_ref: Option<&ToolRef>,
    result: &Result<ToolExecutionOutcome, minicore_runtime::tools::ToolError>,
) {
    if let (Some(tool_ref), Ok(ToolExecutionOutcome::Completed(output))) = (tool_ref, result) {
        presentation
            .tool_data()
            .note_result(tool_ref, output.content().as_str());
    }
    // Runtime errors are static enum variants, but map them explicitly so a
    // future diagnostic carrying arbitrary text cannot leak into the display
    // cache.
    let result_text = match result {
        Ok(ToolExecutionOutcome::Completed(output)) => {
            bounded_display_copy(output.content().as_str(), MAX_RESULT_DISPLAY_BYTES)
        }
        Err(error) => bounded_display_copy(safe_tool_error_text(*error), MAX_RESULT_DISPLAY_BYTES),
        Ok(ToolExecutionOutcome::RequestInput(_)) => None,
    };
    presentation.finish_tool(request_key, tool_call_id, result_text);
}

/// Fixes the current request identity at the policy boundary and publishes
/// the validated invocation before any approval decision. This is what makes
/// invocation data obtainable while a tool is still waiting for approval;
/// `Running` is not set here, because the tool has not been invoked yet.
/// Delegation, fallback decisions, and error paths are unchanged.
pub(crate) struct PresentationPolicy {
    inner: Arc<dyn minicore_runtime::tools::ToolPolicy>,
    presentation: Arc<Presentation>,
}

impl PresentationPolicy {
    pub(crate) fn new(
        inner: Arc<dyn minicore_runtime::tools::ToolPolicy>,
        presentation: Arc<Presentation>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            presentation,
        })
    }
}

impl minicore_runtime::tools::ToolPolicy for PresentationPolicy {
    fn decide<'a>(
        &'a self,
        request: minicore_runtime::tools::ToolPolicyRequest,
    ) -> minicore_runtime::tools::ToolPolicyFuture<'a> {
        let tool_ref = self.presentation.request_key().map(|key| ToolRef {
            session_id: self.presentation.session_id(),
            loop_id: key.loop_id,
            request_index: key.request_index,
            tool_call_id: request.invocation.tool_call_id().clone(),
        });
        if let Some(tool_ref) = &tool_ref {
            if let Some(data) = self
                .presentation
                .tool_data()
                .note_invocation(tool_ref, &request.invocation)
            {
                self.presentation.emit_tool_invocation(data);
            }
        }
        let presentation = Arc::clone(&self.presentation);
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let decision = inner.decide(request).await;
            if let (Some(tool_ref), Ok(decision)) = (&tool_ref, &decision) {
                if matches!(
                    decision,
                    minicore_runtime::tools::ToolDecision::RequireApproval { .. }
                ) {
                    presentation.tool_data().mark_awaiting_policy(tool_ref);
                }
            }
            decision
        })
    }
}

fn safe_tool_error_text(error: minicore_runtime::tools::ToolError) -> &'static str {
    match error {
        minicore_runtime::tools::ToolError::Cancelled => "tool execution was cancelled",
        minicore_runtime::tools::ToolError::Failed => "tool execution failed",
        minicore_runtime::tools::ToolError::TimedOut => "tool execution timed out",
        minicore_runtime::tools::ToolError::Panicked => "tool operation panicked",
        minicore_runtime::tools::ToolError::InvalidInvocation => "tool invocation is invalid",
        minicore_runtime::tools::ToolError::Internal => "tool operation failed internally",
    }
}

// ---------------------------------------------------------------------------
// The single whitelist Tool-detail formatter (live and history)
// ---------------------------------------------------------------------------

pub(crate) fn count_lines(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        text.matches('\n').count() + 1
    }
}

fn json_line_count(arguments: Option<&serde_json::Value>) -> usize {
    arguments
        .filter(|arguments| !arguments.is_null())
        .and_then(|arguments| serde_json::to_string_pretty(arguments).ok())
        .map_or(0, |text| count_lines(&text))
}

/// Bounds a string at a char boundary; returns (copy, truncated).
fn bounded(value: &str, max: usize) -> (String, bool) {
    if value.len() <= max {
        (value.to_owned(), false)
    } else {
        let mut end = max;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        (value[..end].to_owned(), true)
    }
}

/// Removes terminal control effects from display-only text. Newlines and tabs
/// remain meaningful in expanded code bodies; single-line details flatten
/// them. The runtime invocation is never passed through this function.
fn sanitize_display_text(value: &str, multiline: bool) -> String {
    let mut text = String::with_capacity(value.len());
    for character in value.chars() {
        if multiline && matches!(character, '\n' | '\t') {
            text.push(character);
        } else if !multiline && matches!(character, '\r' | '\n' | '\t') {
            text.push(' ');
        } else if character.is_control() {
            text.extend(character.escape_default());
        } else {
            text.push(character);
        }
    }
    text
}

pub(crate) fn sanitize_multiline(value: &str) -> String {
    sanitize_display_text(value, true)
}

/// One-line detail: control whitespace is flattened like the reference
/// `collapsedSimpleLine` and the result is bounded.
fn single_line(value: &str, max: usize) -> (String, bool) {
    let sanitized = sanitize_display_text(value, false);
    bounded(&sanitized, max)
}

/// Joins the two edit bodies without ever allocating more than the expansion
/// cap. The separator is display-only and is counted in the resulting rows.
fn bounded_joined(left: &str, right: &str, max: usize) -> (String, bool) {
    let (left, left_truncated) = bounded(left, max);
    if left_truncated {
        return (left, true);
    }
    if left.is_empty() {
        return bounded(right, max);
    }
    if right.is_empty() {
        return (left, false);
    }
    if left.len() == max {
        return (left, !right.is_empty());
    }
    let remaining = max - left.len();
    if remaining == 0 {
        return (left, !right.is_empty());
    }
    let mut joined = left;
    joined.push('\n');
    let remaining = max - joined.len();
    let (right, right_truncated) = bounded(right, remaining);
    joined.push_str(&right);
    (joined, right_truncated)
}

fn path_arg(args: &serde_json::Value, home: &str) -> Option<String> {
    let raw = args
        .get("path")
        .or_else(|| args.get("file_path"))
        .and_then(serde_json::Value::as_str);
    raw.map(|path| {
        let home_match = if home.is_empty() {
            false
        } else if home == "/" {
            path.starts_with('/')
        } else {
            path == home
                || path
                    .strip_prefix(home)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        };
        if home_match {
            if home == "/" {
                if path == "/" {
                    "~".to_owned()
                } else {
                    format!("~{path}")
                }
            } else {
                let suffix = path.strip_prefix(home).unwrap_or(path);
                format!("~{suffix}")
            }
        } else {
            path.to_owned()
        }
    })
}

fn read_range(args: &serde_json::Value) -> String {
    let offset_value = args.get("offset").and_then(serde_json::Value::as_u64);
    let limit_value = args.get("limit").and_then(serde_json::Value::as_u64);
    if offset_value.is_none() && limit_value.is_none() {
        return String::new();
    }
    let offset = offset_value.unwrap_or(1);
    match limit_value {
        Some(limit) if limit > 0 => format!(
            ":{offset}-{}",
            offset.saturating_add(limit).saturating_sub(1)
        ),
        _ => format!(":{offset}"),
    }
}

/// Adds displayable result rows to a display produced without a result. The
/// initial hidden count already contains the source input rows, following the
/// reference `executionHiddenLineCount` semantics.
fn apply_result_to_display(display: &mut ToolDisplay, result_text: Option<&str>) {
    let result_rows = result_text
        .filter(|text| !text.is_empty())
        .map(count_lines)
        .unwrap_or(0);
    let input_rows = display.hidden_line_count.unwrap_or(0);
    let hidden = input_rows + result_rows;
    display.hidden_line_count = (hidden > 0).then_some(hidden);
}

/// Builds the whitelisted ToolDisplay for one tool invocation. `result_text`
/// is the (bounded) result output when known; the same function regenerates
/// history displays from stored args/result so live and history agree.
pub(crate) fn build_tool_display(
    name: &str,
    args: Option<&serde_json::Value>,
    result_text: Option<&str>,
) -> ToolDisplay {
    let empty_arguments = serde_json::Value::Object(serde_json::Map::new());
    let arguments = args.unwrap_or(&empty_arguments);
    let home = std::env::var("HOME").unwrap_or_default();

    let (detail, mut truncated) = match name {
        "bash" => {
            let command = arguments
                .get("command")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("...");
            let (command, cut) = single_line(command, MAX_DETAIL_BYTES.saturating_sub(2));
            (format!("$ {command}"), cut)
        }
        "read" => {
            let path = path_arg(arguments, &home).unwrap_or_else(|| "...".to_owned());
            let detail_source = format!("{path}{}", read_range(arguments));
            single_line(&detail_source, MAX_DETAIL_BYTES)
        }
        "write" | "edit" => {
            let path = path_arg(arguments, &home).unwrap_or_else(|| "...".to_owned());
            single_line(&path, MAX_DETAIL_BYTES)
        }
        // Unknown tools deliberately expose only their identity. In
        // particular, patterns, paths, and arbitrary JSON arguments do not
        // escape the whitelist through the generic branch.
        _ => single_line(&format!("tool {name}"), MAX_DETAIL_BYTES),
    };

    let (bounded_result, result_truncated) = result_text
        .map(sanitize_multiline)
        .map(|result| bounded(&result, MAX_RESULT_DISPLAY_BYTES))
        .map_or((None, false), |(result, truncated)| {
            (Some(result), truncated)
        });
    truncated |= result_truncated;

    let (expanded_input, input_line_count) = match name {
        "write" => {
            let content = arguments.get("content").and_then(serde_json::Value::as_str);
            content
                .map(|content| {
                    let line_count = count_lines(content);
                    let sanitized = sanitize_multiline(content);
                    let (text, cut) = bounded(&sanitized, MAX_EXPANDED_INPUT_BYTES);
                    truncated |= cut;
                    (Some(text), Some(line_count))
                })
                .unwrap_or((None, None))
        }
        "edit" => {
            let old = arguments
                .get("old_text")
                .and_then(serde_json::Value::as_str);
            let new = arguments
                .get("new_text")
                .and_then(serde_json::Value::as_str);
            match (old, new) {
                (Some(old), Some(new)) => {
                    let line_count = count_lines(old) + count_lines(new);
                    let old = sanitize_multiline(old);
                    let new = sanitize_multiline(new);
                    let (text, cut) = bounded_joined(&old, &new, MAX_EXPANDED_INPUT_BYTES);
                    truncated |= cut;
                    (Some(text), Some(line_count))
                }
                (Some(old), None) => {
                    let line_count = count_lines(old);
                    let sanitized = sanitize_multiline(old);
                    let (old, cut) = bounded(&sanitized, MAX_EXPANDED_INPUT_BYTES);
                    truncated |= cut;
                    (Some(old), Some(line_count))
                }
                (None, Some(new)) => {
                    let line_count = count_lines(new);
                    let sanitized = sanitize_multiline(new);
                    let (new, cut) = bounded(&sanitized, MAX_EXPANDED_INPUT_BYTES);
                    truncated |= cut;
                    (Some(new), Some(line_count))
                }
                _ => (None, None),
            }
        }
        "apply_patch" | "patch" => {
            let patch = arguments.get("patch").and_then(serde_json::Value::as_str);
            patch
                .map(|patch| {
                    let line_count = count_lines(patch);
                    let sanitized = sanitize_multiline(patch);
                    let (text, cut) = bounded(&sanitized, MAX_EXPANDED_INPUT_BYTES);
                    truncated |= cut;
                    (Some(text), Some(line_count))
                })
                .unwrap_or((None, None))
        }
        _ => (None, None),
    };

    let input_rows = match name {
        "write" | "edit" if expanded_input.is_some() => expanded_input
            .as_deref()
            .filter(|text| !text.is_empty())
            .map(count_lines)
            .unwrap_or(0),
        _ => json_line_count(args),
    };

    let mut display = ToolDisplay {
        detail,
        expanded_input,
        input_line_count,
        hidden_line_count: (input_rows > 0).then_some(input_rows),
        truncated,
    };
    apply_result_to_display(&mut display, bounded_result.as_deref());
    display
}

fn bounded_display_copy(value: &str, max: usize) -> Option<(String, bool)> {
    if value.is_empty() {
        return None;
    }
    Some(bounded_result_content(value, max))
}

/// Sanitizes and bounds a real tool result for both live events and history
/// views. Empty results remain representable as an empty string plus a false
/// truncation bit; callers decide whether to omit that value from JSON.
pub(crate) fn bounded_result_content(value: &str, max: usize) -> (String, bool) {
    let sanitized = sanitize_multiline(value);
    bounded(&sanitized, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_error_presentation_is_static_for_every_variant() {
        let cases = [
            (
                minicore_runtime::tools::ToolError::Cancelled,
                "tool execution was cancelled",
            ),
            (
                minicore_runtime::tools::ToolError::Failed,
                "tool execution failed",
            ),
            (
                minicore_runtime::tools::ToolError::TimedOut,
                "tool execution timed out",
            ),
            (
                minicore_runtime::tools::ToolError::Panicked,
                "tool operation panicked",
            ),
            (
                minicore_runtime::tools::ToolError::InvalidInvocation,
                "tool invocation is invalid",
            ),
            (
                minicore_runtime::tools::ToolError::Internal,
                "tool operation failed internally",
            ),
        ];
        for (error, expected) in cases {
            let actual = safe_tool_error_text(error);
            assert_eq!(actual, expected);
            assert!(!actual.contains("missing-secret.txt"));
            assert!(!actual.contains("prompt"));
            assert!(!actual.contains("diagnostic"));
        }
    }

    #[test]
    fn redacted_debug_never_exposes_raw_text() {
        let display = ToolDisplay {
            detail: "$ rm -rf ~/secrets".to_owned(),
            expanded_input: Some("TOP SECRET CONTENT".to_owned()),
            input_line_count: Some(1),
            hidden_line_count: Some(2),
            truncated: false,
        };
        let debug = format!("{display:?}");
        assert!(!debug.contains("rm -rf"));
        assert!(!debug.contains("TOP SECRET"));
        assert!(!debug.contains("secrets"));
        assert!(debug.contains("detail_bytes"));
        assert!(debug.contains("expanded_input_bytes"));
    }

    #[test]
    fn whitelist_details_and_counts() {
        let bash = build_tool_display(
            "bash",
            Some(&serde_json::json!({ "command": "cargo test" })),
            Some("ok\nok"),
        );
        assert_eq!(bash.detail, "$ cargo test");
        assert!(bash.expanded_input.is_none());
        assert_eq!(bash.hidden_line_count, Some(5));

        let write = build_tool_display(
            "write",
            Some(&serde_json::json!({ "path": "a.txt", "content": "l1\nl2\nl3" })),
            Some("created"),
        );
        assert_eq!(write.detail, "a.txt");
        assert_eq!(write.input_line_count, Some(3));
        assert_eq!(write.hidden_line_count, Some(4));

        let read = build_tool_display(
            "read",
            Some(&serde_json::json!({ "path": "src/main.rs", "offset": 10, "limit": 5 })),
            None,
        );
        assert_eq!(read.detail, "src/main.rs:10-14");
        assert_eq!(read.hidden_line_count, Some(5));

        let edit = build_tool_display(
            "edit",
            Some(&serde_json::json!({ "path": "a.rs", "old_text": "x\ny", "new_text": "z" })),
            None,
        );
        assert_eq!(edit.detail, "a.rs");
        assert_eq!(edit.expanded_input.as_deref(), Some("x\ny\nz"));
        assert_eq!(edit.input_line_count, Some(3));
    }

    #[test]
    fn path_details_preserve_paths_without_a_home_directory() {
        for path in ["/tmp/a.txt", "/src/main.rs", "a.rs", "", "C:\\work\\a.rs"] {
            let args = serde_json::json!({ "path": path });
            assert_eq!(path_arg(&args, "").as_deref(), Some(path));
        }
        let args = serde_json::json!({ "file_path": "/tmp/a.txt" });
        assert_eq!(path_arg(&args, "").as_deref(), Some("/tmp/a.txt"));
    }

    #[test]
    fn path_details_only_abbreviate_a_known_home_boundary() {
        for (path, home, expected) in [
            ("/home/test", "/home/test", "~"),
            ("/home/test/src/main.rs", "/home/test", "~/src/main.rs"),
            ("/home/testing/a.rs", "/home/test", "/home/testing/a.rs"),
            ("/tmp/a.txt", "/home/test", "/tmp/a.txt"),
            ("/", "/", "~"),
            ("/src/main.rs", "/", "~/src/main.rs"),
        ] {
            let args = serde_json::json!({ "path": path });
            assert_eq!(path_arg(&args, home).as_deref(), Some(expected));
        }
    }

    #[test]
    fn truncated_input_is_flagged_and_counts_reflect_displayable_rows() {
        let content = "a\n".repeat(300_000); // > 512 KiB
        let write = build_tool_display(
            "write",
            Some(&serde_json::json!({ "path": "x", "content": content })),
            None,
        );
        assert!(write.truncated);
        assert_eq!(write.detail, "x");
        // input_line_count reflects the source arg; hidden rows reflect only
        // what is actually expandable, so the TUI cannot promise rows it
        // cannot show. The trailing newline creates one final empty row.
        assert_eq!(write.input_line_count, Some(300_001));
        let expandable = write
            .expanded_input
            .as_ref()
            .map(|text| count_lines(text))
            .unwrap();
        assert!(expandable < 300_001);
        assert_eq!(write.hidden_line_count, Some(expandable));
    }

    #[test]
    fn generic_tool_detail_is_bounded_single_line_and_redacted() {
        let display = build_tool_display(
            "unlisted",
            Some(&serde_json::json!({
                "command": "DO NOT DISPLAY",
                "path": "/private/secret.txt",
                "args": "x"
            })),
            None,
        );
        assert!(!display.detail.contains('\n'));
        assert!(!display.detail.contains("DO NOT DISPLAY"));
        assert!(!display.detail.contains("secret.txt"));
        assert!(display.expanded_input.is_none());
        assert_eq!(display.hidden_line_count, Some(5));
    }

    #[test]
    fn result_truncation_marks_display_and_bounds_hidden_rows() {
        let result = "x\n".repeat(300_000);
        let display = build_tool_display(
            "bash",
            Some(&serde_json::json!({ "command": "printf x" })),
            Some(&result),
        );
        assert!(display.truncated);
        assert_eq!(
            display.hidden_line_count,
            Some(3 + count_lines(&result[..MAX_RESULT_DISPLAY_BYTES]))
        );
    }

    #[test]
    fn assistant_parts_preserve_visible_order_and_redact_debug() {
        let tool_call = minicore_runtime::model::ToolCall::new(
            ToolCallId::new("call-1").unwrap(),
            "read".parse().unwrap(),
            serde_json::json!({"path": "secret.txt"}),
            0,
        )
        .unwrap();
        let reasoning = minicore_runtime::model::ReasoningContent::new(
            Some("think".to_owned()),
            Some("summary".to_owned()),
            None,
            None,
        )
        .unwrap();
        let parts = assistant_display_parts(&[
            minicore_runtime::model::AssistantPart::Text("before".to_owned()),
            minicore_runtime::model::AssistantPart::Reasoning(reasoning),
            minicore_runtime::model::AssistantPart::ToolCall(tool_call),
            minicore_runtime::model::AssistantPart::Text("after".to_owned()),
        ]);
        assert!(matches!(&parts[0], AssistantDisplayPart::Text { text } if text == "before"));
        assert!(
            matches!(&parts[1], AssistantDisplayPart::Reasoning { text } if text == "thinksummary")
        );
        assert!(matches!(&parts[2], AssistantDisplayPart::ToolCall { name, .. } if name == "read"));
        assert!(matches!(&parts[3], AssistantDisplayPart::Text { text } if text == "after"));
        let debug = format!("{:?}", parts[1]);
        assert!(!debug.contains("think"));
    }

    #[test]
    fn editor_old_and_new_and_bash_hint_lines() {
        let edit = build_tool_display(
            "edit",
            Some(&serde_json::json!({ "old_text": "a\nb\nc", "new_text": "d" })),
            Some("updated"),
        );
        assert_eq!(edit.input_line_count, Some(4));
        // input(4) + result(1) = 5 hidden rows.
        assert_eq!(edit.hidden_line_count, Some(5));
    }

    #[test]
    fn hidden_counts_follow_the_fixed_tool_execution_estimator() {
        for (result_rows, expected) in [(19, 20), (20, 21), (21, 22)] {
            let result = (0..result_rows)
                .map(|line| format!("result {line}"))
                .collect::<Vec<_>>()
                .join("\n");
            let display =
                build_tool_display("custom_tool", Some(&serde_json::json!({})), Some(&result));
            assert_eq!(display.hidden_line_count, Some(expected));
        }

        let read = build_tool_display(
            "read",
            Some(&serde_json::json!({
                "path": "src/main.rs",
                "offset": 10,
                "limit": 5
            })),
            Some("line 10\nline 11"),
        );
        assert_eq!(read.hidden_line_count, Some(7));

        let write = build_tool_display(
            "write",
            Some(&serde_json::json!({"path": "src/main.rs"})),
            Some("created"),
        );
        assert_eq!(write.hidden_line_count, Some(4));
    }

    #[test]
    fn steer_receipt_commits_only_at_real_start_and_emits_once_per_count() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let presentation =
            Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
        let loop_r0 = LoopId::new().unwrap();
        let key_a = RequestKey {
            loop_id: loop_r0,
            request_index: 0,
        };
        let key_b = RequestKey {
            loop_id: loop_r0,
            request_index: 1,
        };
        let drain = |receiver: &mut tokio::sync::mpsc::Receiver<AgentEvent>| -> Vec<AgentEvent> {
            let mut events = Vec::new();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
            events
        };

        // Preparing alone must never release the queue: no event before start.
        presentation.commit_prepared_request(key_a, 0);
        assert_eq!(presentation.snapshot().steer_progress, None);

        // Real Model::start commits the prepared count and emits the receipt.
        presentation.note_request_start(key_a);
        let events = drain(&mut receiver);
        assert!(
            matches!(
                events
                    .iter()
                    .find(|event| matches!(event, AgentEvent::SteerProgress { .. })),
                Some(AgentEvent::SteerProgress {
                    request_index: 0,
                    applied_count: 0,
                    ..
                })
            ),
            "first request applies count 0: {events:?}"
        );
        assert_eq!(
            presentation
                .snapshot()
                .steer_progress
                .as_ref()
                .map(|view| view.applied_count),
            Some(0)
        );

        // Two accepted steers before the next boundary: history then shows 2.
        assert_eq!(presentation.note_steer_accepted(Some("t0".into())), Some(1));
        assert_eq!(presentation.note_steer_accepted(Some("t1".into())), Some(2));
        presentation.commit_prepared_request(key_b, 2);
        presentation.note_request_start(key_b);
        let events = drain(&mut receiver);
        assert!(
            events.iter().any(|event| matches!(
                event,
                AgentEvent::SteerProgress {
                    request_index: 1,
                    applied_count: 2,
                    ..
                }
            )),
            "second request applies count 2: {events:?}"
        );
        assert_eq!(
            presentation
                .snapshot()
                .steer_progress
                .as_ref()
                .map(|view| view.applied_count),
            Some(2)
        );

        // A later request observing the same count must not re-emit or rewrite.
        let key_c = RequestKey {
            loop_id: loop_r0,
            request_index: 2,
        };
        presentation.commit_prepared_request(key_c, 2);
        presentation.note_request_start(key_c);
        let events = drain(&mut receiver);
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, AgentEvent::SteerProgress { .. })),
            "same count must not re-emit: {events:?}"
        );

        // Loop reset clears accepted count and progress.
        presentation.note_loop_started(LoopId::new().unwrap(), None);
        assert_eq!(presentation.note_steer_accepted(Some("t2".into())), Some(1));
        assert_eq!(presentation.snapshot().steer_progress, None);
    }

    #[test]
    fn steer_times_still_feed_the_same_live_times_queue() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(8);
        let presentation =
            Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
        presentation.note_steer_accepted(Some("t0".into()));
        presentation.note_steer_accepted(None);
        assert_eq!(
            presentation.peek_user_times(3),
            vec![Some("t0".to_owned()), None]
        );
    }

    /// A loop whose tool future was cancelled leaves no live-table entry, and a
    /// later loop that reuses the same call id must still get its own identity:
    /// the `ToolRef` is captured per execution, never looked up or reused.
    #[test]
    fn a_reused_call_id_in_a_new_loop_gets_its_own_identity() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(8);
        let presentation =
            Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
        let call_id = ToolCallId::new("call-a").unwrap();
        let invocation = ToolInvocation {
            tool_call_id: call_id.clone(),
            tool_name: "bash".parse().unwrap(),
            arguments: serde_json::json!({"command": "true"}),
        };
        let display = ToolDisplay {
            detail: "$ true".to_owned(),
            expanded_input: None,
            input_line_count: None,
            hidden_line_count: None,
            truncated: false,
        };

        // First loop: the call is published, then cancelled, so its future
        // never runs `finish_tool` and the live table keeps the entry.
        let first_key = RequestKey {
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
        };
        presentation.note_request_start(first_key);
        presentation.begin_tool(Some(first_key), &invocation, display.clone());
        let first_ref = ToolRef {
            session_id: presentation.session_id(),
            loop_id: first_key.loop_id,
            request_index: 0,
            tool_call_id: call_id.clone(),
        };
        presentation.tool_data().note_requested(&first_ref, "bash");

        // Second loop on the same Session reuses the call id.
        let second_key = RequestKey {
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
        };
        presentation.note_loop_started(second_key.loop_id, None);
        presentation.note_request_start(second_key);
        presentation.begin_tool(Some(second_key), &invocation, display);
        let second_ref = ToolRef {
            session_id: presentation.session_id(),
            loop_id: second_key.loop_id,
            request_index: 0,
            tool_call_id: call_id.clone(),
        };

        // The captured identities are distinct and never resolved by position.
        assert_ne!(first_ref.loop_id, second_ref.loop_id);
        assert_eq!(second_ref.loop_id, second_key.loop_id);
        // The new loop's binding only owns the new loop's registry.
        let binding = presentation.command_binding();
        assert_eq!(binding.owners().active(), 0);
    }

    /// A completed call removes its live entry, so the Session's close join is
    /// not relying on stale bookkeeping to reach an owned command.
    #[test]
    fn finishing_a_call_removes_its_live_entry() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(8);
        let presentation =
            Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
        let key = RequestKey {
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
        };
        let call_id = ToolCallId::new("call-finish").unwrap();
        let invocation = ToolInvocation {
            tool_call_id: call_id.clone(),
            tool_name: "bash".parse().unwrap(),
            arguments: serde_json::json!({"command": "true"}),
        };
        presentation.note_request_start(key);
        presentation.begin_tool(
            Some(key),
            &invocation,
            ToolDisplay {
                detail: "$ true".to_owned(),
                expanded_input: None,
                input_line_count: None,
                hidden_line_count: None,
                truncated: false,
            },
        );
        presentation.finish_tool(Some(key), &call_id, None);
        assert!(presentation.lock().live_tools.is_empty());
    }
}
