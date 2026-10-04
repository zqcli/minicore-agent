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
use std::sync::{Arc, Mutex, Weak};

use futures_util::StreamExt;
use serde::Serialize;

use minicore_runtime::history::{HistoryItem, UserMessageKind};
use minicore_runtime::model::{Model, ModelCallContext, ModelDescriptor, ModelRequest, Usage};
use minicore_runtime::prompt::{PromptFuture, PromptProvider, PromptRequest};
use minicore_runtime::tools::{Tool, ToolContext, ToolExecutionOutcome, ToolInvocation, ToolSpec};
use minicore_runtime::{LoopId, ToolCallId};

use crate::event::{AgentEvent, AgentEventSink, EventMeta};
use crate::ids::SessionId;
use crate::sessions::TurnRef;
use crate::tool_data::ToolRef;
use crate::tools::observe::{RequestKey, ToolObserver};
use crate::tools::{NativeApplyPatchTool, NativeEditTool, NativeWriteTool};

/// Display text limits. Aligned with the existing per-argument/output caps so
/// the expanded view can never promise rows that the Agent cannot show.
pub(crate) const MAX_DETAIL_BYTES: usize = 512;
pub(crate) const MAX_EXPANDED_INPUT_BYTES: usize = 512 * 1024;
pub(crate) const MAX_RESULT_DISPLAY_BYTES: usize = 512 * 1024;

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
    /// Logical line count of the actual rendered input body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_line_count: Option<usize>,
    /// Displayable hidden rows this card would collapse: the source input
    /// rows of whitelisted expanded input plus result rows actually available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden_line_count: Option<usize>,
    /// The source above the display cap was truncated; the TUI must not
    /// promise more expandable rows than shown.
    #[serde(default)]
    pub truncated: bool,
    /// Expanded body reached its display cap; shortened details do not count.
    #[serde(default)]
    pub body_truncated: bool,
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

/// Source of the displayed context usage. Reported usage is the last API
/// request's footprint, not an estimate of an in-flight or future request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    Unknown,
    Estimated,
    Reported,
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
    /// Current-loop-only caches; cleared when a new loop starts.
    live_tools: HashMap<(Option<RequestKey>, ToolCallId), LiveTool>,
    tool_results: HashMap<(RequestKey, ToolCallId), ToolResultInfo>,
    request_usage: RequestUsageCache,
    live_times: VecDeque<Option<String>>,
    last_loop: Option<LastLoop>,
    branch: Option<String>,
    model_label: Option<String>,
    context: ContextUsageView,
    context_settings: (u64, u64),
    installed_model: Option<Weak<dyn Model>>,
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
    compaction: Arc<crate::compaction::CompactionState>,
    inner: Mutex<PresentationInner>,
}

impl Presentation {
    pub(crate) fn new(
        session_id: SessionId,
        events: AgentEventSink,
        compaction: Arc<crate::compaction::CompactionState>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            events,
            compaction,
            inner: Mutex::new(PresentationInner::default()),
        })
    }

    #[cfg(test)]
    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn set_branch(&self, branch: Option<String>) {
        self.lock().branch = branch;
    }

    /// Records the real model-start boundary for display-only steering
    /// receipts. The execution request key lives in `ToolObserver`; this cache
    /// only commits a prepared steering count and emits its read-only event.
    /// The user-facing label is set from the session profile, never from the
    /// provider descriptor.
    pub(crate) fn note_request_start(&self, key: RequestKey) {
        let event = {
            let mut inner = self.lock();
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
        usage: Usage,
        context_generation: Option<u64>,
        physical_window: u64,
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
            let settings = self.compaction.request_settings();
            if context_generation == Some(settings.config_generation) {
                inner.context = reported_context(usage, physical_window);
                inner.context_settings = (settings.config_generation, settings.summary_generation);
            }
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

    /// Clears observation state owned by the previous loop before the Runtime
    /// task is spawned. The next loop may begin synchronously from
    /// `AgentLoop::start`, so this must not run after that call.
    pub(crate) fn reset_before_loop_start(&self) {
        let mut inner = self.lock();
        inner.live_tools.clear();
        inner.tool_results.clear();
        inner.request_usage.clear();
        inner.live_times.clear();
        inner.accepted_steers = 0;
        inner.prepared_steers = None;
        inner.applied_steer = None;
    }

    /// Binds metadata for the loop that was successfully started. This is
    /// deliberately metadata-only: the first real model request may already
    /// have installed its observation identity.
    pub(crate) fn bind_started_loop(&self, loop_id: LoopId, started_at: Option<String>) {
        self.lock().last_loop = Some(LastLoop {
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
        let mut inner = self.lock();
        if inner.model_label.as_ref() != Some(&model_label) {
            inner.context = ContextUsageView::default();
        }
        inner.model_label = Some(model_label);
    }

    /// Bind only when the execution config is installed, never while building
    /// a candidate. Weak identity prevents old same-label wrappers from
    /// claiming usage after a settings reload without creating a reference cycle.
    pub(crate) fn install_model(&self, model: &Arc<dyn Model>) {
        let mut inner = self.lock();
        if inner.installed_model.is_some() {
            inner.context = ContextUsageView::default();
        }
        inner.installed_model = Some(Arc::downgrade(model));
    }

    fn context_generation(&self, model: &dyn Model) -> Option<u64> {
        let inner = self.lock();
        inner
            .installed_model
            .as_ref()
            .filter(|installed| std::ptr::addr_eq(installed.as_ptr(), model as *const dyn Model))
            .map(|_| self.compaction.request_settings().config_generation)
    }

    /// The caller supplies only active post-compaction history. Do not carry
    /// an older model's usage across a model boundary when reopening.
    pub(crate) fn restore_context(
        &self,
        history: &[HistoryItem],
        model_ref: &str,
        physical_window: u64,
    ) {
        let mut inner = self.lock();
        for item in history.iter().rev() {
            if let HistoryItem::Assistant(assistant) = item {
                if assistant.model.as_str() != model_ref {
                    break;
                }
                let context = reported_context(assistant.usage, physical_window);
                if context.tokens.is_some() {
                    inner.context = context;
                    let settings = self.compaction.request_settings();
                    inner.context_settings =
                        (settings.config_generation, settings.summary_generation);
                    break;
                }
            }
        }
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
        let settings = self.compaction.request_settings();
        let context = if inner.context_settings
            == (settings.config_generation, settings.summary_generation)
        {
            inner.context.clone()
        } else {
            ContextUsageView::default()
        };
        PresentationView {
            session_id: self.session_id,
            model_label: inner.model_label.clone(),
            git_branch: inner.branch.clone(),
            context,
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

    fn lock(&self) -> std::sync::MutexGuard<'_, PresentationInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Usage components are normalized by the provider adapter into disjoint
/// counts. Never guess a missing component is zero or sum a native total twice.
fn reported_context(usage: Usage, physical_window: u64) -> ContextUsageView {
    let tokens = usage.provider_total_tokens().or_else(|| {
        usage
            .input_tokens()?
            .checked_add(usage.output_tokens()?)?
            .checked_add(usage.reasoning_tokens()?)?
            .checked_add(usage.cache_read_tokens()?)?
            .checked_add(usage.cache_write_tokens()?)
    });
    let Some(tokens) = tokens else {
        return ContextUsageView::default();
    };
    let window = (physical_window > 0).then_some(physical_window);
    ContextUsageView {
        tokens: Some(tokens),
        window,
        percent: window.map(|window| tokens as f64 / window as f64 * 100.0),
        kind: ContextKind::Reported,
    }
}

// ---------------------------------------------------------------------------
// Model / Tool wrappers (thin; identity + best-effort display only)
// ---------------------------------------------------------------------------

/// Records the real request key in `ToolObserver` at the model boundary and
/// mirrors that boundary into the display-only steering cache, then delegates
/// unchanged.
pub(crate) struct PresentationModel {
    physical_window: u64,
    inner: Arc<dyn Model>,
    presentation: Arc<Presentation>,
    observer: Arc<ToolObserver>,
}

impl PresentationModel {
    pub(crate) fn new_with_observer(
        inner: Arc<dyn Model>,
        physical_window: u64,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            physical_window,
            presentation,
            observer,
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
        self.observer.note_request_start(key);
        self.presentation.note_request_start(key);
        let context_generation = self.presentation.context_generation(self);
        let physical_window = self.physical_window;
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
                    presentation.note_request_usage(
                        key,
                        *usage,
                        context_generation,
                        physical_window,
                    );
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

/// Captures the current request identity in `ToolObserver` at `Tool::execute`
/// start, computes the bounded ToolDisplay, then delegates execution unchanged
/// (cancellation, deadline, errors, and the result are never altered). Real
/// Bash and native file tools receive the exact `ToolRef` captured here, so
/// commands and mutation records never resolve identity from a table that a
/// dropped future can leave stale.
pub(crate) struct PresentationTool {
    inner: ToolImpl,
    presentation: Arc<Presentation>,
    observer: Arc<ToolObserver>,
}

#[derive(Clone)]
enum ToolImpl {
    Plain(Arc<dyn Tool>),
    Bash(Arc<crate::tools::OwnedBashTool>),
    Write(Arc<NativeWriteTool>),
    Edit(Arc<NativeEditTool>),
    ApplyPatch(Arc<NativeApplyPatchTool>),
}

impl PresentationTool {
    fn new(
        inner: ToolImpl,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            presentation,
            observer,
        })
    }

    pub(crate) fn new_with_observer(
        inner: Arc<dyn Tool>,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Self::new(ToolImpl::Plain(inner), presentation, observer)
    }

    /// Explicit construction for the owned-command tool: only this variant
    /// receives the captured `ToolRef` at the real execute boundary.
    pub(crate) fn new_bash_with_observer(
        inner: Arc<crate::tools::OwnedBashTool>,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Self::new(ToolImpl::Bash(inner), presentation, observer)
    }

    pub(crate) fn new_write_with_observer(
        inner: Arc<NativeWriteTool>,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Self::new(ToolImpl::Write(inner), presentation, observer)
    }

    pub(crate) fn new_edit_with_observer(
        inner: Arc<NativeEditTool>,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Self::new(ToolImpl::Edit(inner), presentation, observer)
    }

    pub(crate) fn new_apply_patch_with_observer(
        inner: Arc<NativeApplyPatchTool>,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Self::new(ToolImpl::ApplyPatch(inner), presentation, observer)
    }

    fn tool(&self) -> &dyn Tool {
        match &self.inner {
            ToolImpl::Plain(inner) => inner.as_ref(),
            ToolImpl::Bash(inner) => inner.as_ref(),
            ToolImpl::Write(inner) => inner.as_ref(),
            ToolImpl::Edit(inner) => inner.as_ref(),
            ToolImpl::ApplyPatch(inner) => inner.as_ref(),
        }
    }
}

impl Tool for PresentationTool {
    fn spec(&self) -> &ToolSpec {
        self.tool().spec()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: ToolContext,
    ) -> minicore_runtime::tools::ToolFuture<'a> {
        let presentation = Arc::clone(&self.presentation);
        let observer = Arc::clone(&self.observer);
        let key = observer.request_key();
        let display = build_tool_display(
            invocation.tool_name().as_str(),
            Some(invocation.arguments()),
            None,
        );
        // Complete identity: only a real `Model::start` request key plus the
        // Runtime's tool-call id. This is the single place the identity is
        // captured; it is passed downward, never recovered from a live table.
        let tool_ref = key.map(|key| ToolRef {
            session_id: observer.session_id(),
            loop_id: key.loop_id,
            request_index: key.request_index,
            tool_call_id: invocation.tool_call_id().clone(),
        });
        presentation.begin_tool(key, &invocation, display);
        if let Some(tool_ref) = &tool_ref {
            // Published before the work starts, never after it returns.
            observer.publish_tool_invocation(tool_ref, &invocation);
        }

        let request_key = key;
        let tool_call_id = invocation.tool_call_id().clone();
        let inner = self.inner.clone();
        let tool_data = observer.tool_data();
        Box::pin(async move {
            let result = match inner {
                ToolImpl::Plain(inner) => inner.execute(invocation, context).await,
                ToolImpl::Bash(inner) => {
                    inner
                        .execute_bound(invocation, context, tool_ref.clone())
                        .await
                }
                ToolImpl::Write(inner) => {
                    inner
                        .execute_bound(invocation, context, tool_ref.clone(), Some(tool_data))
                        .await
                }
                ToolImpl::Edit(inner) => {
                    inner
                        .execute_bound(invocation, context, tool_ref.clone(), Some(tool_data))
                        .await
                }
                ToolImpl::ApplyPatch(inner) => {
                    inner
                        .execute_bound(invocation, context, tool_ref.clone(), Some(tool_data))
                        .await
                }
            };
            finish_presentation_tool(
                &presentation,
                &observer,
                request_key,
                &tool_call_id,
                tool_ref.as_ref(),
                &result,
            );
            result
        })
    }
}

/// Best-effort post-execution presentation bookkeeping. The outcome and error
/// are forwarded unchanged; raw result bytes are retained for `tool.output`
/// while the authoritative terminal state still comes from the Runtime.
fn finish_presentation_tool(
    presentation: &Presentation,
    observer: &ToolObserver,
    request_key: Option<RequestKey>,
    tool_call_id: &ToolCallId,
    tool_ref: Option<&ToolRef>,
    result: &Result<ToolExecutionOutcome, minicore_runtime::tools::ToolError>,
) {
    if let (Some(tool_ref), Ok(ToolExecutionOutcome::Completed(output))) = (tool_ref, result) {
        observer
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

/// Reads the request identity from `ToolObserver` at the policy boundary and
/// publishes the validated invocation before any approval decision. This is
/// what makes invocation data obtainable while a tool is still waiting for
/// approval; `Running` is not set here, because the tool has not been invoked
/// yet. Delegation, fallback decisions, and error paths are unchanged.
pub(crate) struct PresentationPolicy {
    inner: Arc<dyn minicore_runtime::tools::ToolPolicy>,
    observer: Arc<ToolObserver>,
}

impl PresentationPolicy {
    pub(crate) fn new_with_observer(
        inner: Arc<dyn minicore_runtime::tools::ToolPolicy>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Arc::new(Self { inner, observer })
    }
}

impl minicore_runtime::tools::ToolPolicy for PresentationPolicy {
    fn decide<'a>(
        &'a self,
        request: minicore_runtime::tools::ToolPolicyRequest,
    ) -> minicore_runtime::tools::ToolPolicyFuture<'a> {
        let tool_ref = self.observer.request_key().map(|key| ToolRef {
            session_id: self.observer.session_id(),
            loop_id: key.loop_id,
            request_index: key.request_index,
            tool_call_id: request.invocation.tool_call_id().clone(),
        });
        if let Some(tool_ref) = &tool_ref {
            if let Some(data) = self
                .observer
                .tool_data()
                .note_invocation(tool_ref, &request.invocation)
            {
                self.observer.emit_tool_invocation(data);
            }
        }
        let observer = Arc::clone(&self.observer);
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let decision = inner.decide(request).await;
            if let (Some(tool_ref), Ok(decision)) = (&tool_ref, &decision) {
                if matches!(
                    decision,
                    minicore_runtime::tools::ToolDecision::RequireApproval { .. }
                ) {
                    observer.tool_data().mark_awaiting_policy(tool_ref);
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

/// A display-only line diff with an explicit format marker. Bound both the
/// diff inputs and emitted text; fixed complexity limits keep projection
/// deterministic and prevent expensive alignments. No raw argument object
/// is ever exposed through this whitelist.
fn bounded_edit_diff(old: &str, new: &str) -> (String, bool) {
    use similar::{Algorithm, ChangeTag, TextDiff};
    let (old, old_cut) = bounded(old, MAX_EXPANDED_INPUT_BYTES);
    let (new, new_cut) = bounded(new, MAX_EXPANDED_INPUT_BYTES);
    let mut text = String::from("--- before\n+++ after\n@@\n");
    // A clock deadline can choose different alignments for the same pinned
    // history item. Keep small edits exact and larger edits linear and stable.
    if old.len() + new.len() > 64 * 1024 || count_lines(&old) + count_lines(&new) > 2_000 {
        for (prefix, body) in [('-', old.as_str()), ('+', new.as_str())] {
            for line in body.split_inclusive('\n') {
                let row = format!(
                    "{prefix}{}\n",
                    sanitize_multiline(line.trim_end_matches('\n'))
                );
                let (row, cut) = bounded(&row, MAX_EXPANDED_INPUT_BYTES - text.len());
                text.push_str(&row);
                if cut {
                    return (text, true);
                }
            }
        }
        return (text, old_cut || new_cut);
    }
    let diff = TextDiff::configure()
        .algorithm(Algorithm::Myers)
        .diff_lines(&old, &new);
    for op in diff.ops() {
        for change in diff.iter_changes(op) {
            let prefix = match change.tag() {
                ChangeTag::Equal => ' ',
                ChangeTag::Delete => '-',
                ChangeTag::Insert => '+',
            };
            // Alignment must compare source bytes: escaping first can collapse
            // distinct edits (for example CRLF and a literal backslash-r).
            let value = sanitize_multiline(change.value().trim_end_matches('\n'));
            let row = format!("{prefix}{value}\n");
            let (row, cut) = bounded(&row, MAX_EXPANDED_INPUT_BYTES - text.len());
            text.push_str(&row);
            if cut {
                return (text, true);
            }
        }
    }
    (text, old_cut || new_cut)
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

    let detail_truncated = truncated;
    truncated = false;
    let expanded_input = if name == "edit" {
        match (
            arguments
                .get("old_text")
                .and_then(serde_json::Value::as_str),
            arguments
                .get("new_text")
                .and_then(serde_json::Value::as_str),
        ) {
            (Some(old), Some(new)) => {
                let (text, cut) = bounded_edit_diff(old, new);
                truncated |= cut;
                Some(text)
            }
            (body, None) | (None, body) => body.map(|body| {
                let (text, cut) = bounded(&sanitize_multiline(body), MAX_EXPANDED_INPUT_BYTES);
                truncated |= cut;
                text
            }),
        }
    } else {
        let field = match name {
            "bash" => "command",
            "write" => "content",
            "apply_patch" | "patch" => "patch",
            _ => "",
        };
        (!field.is_empty())
            .then(|| arguments.get(field))
            .flatten()
            .and_then(serde_json::Value::as_str)
            .map(|body| {
                let (text, cut) = bounded(&sanitize_multiline(body), MAX_EXPANDED_INPUT_BYTES);
                truncated |= cut;
                text
            })
    };

    let input_rows = expanded_input.as_deref().map_or(0, count_lines);
    let input_line_count = expanded_input.as_ref().map(|_| input_rows);

    let mut display = ToolDisplay {
        detail,
        expanded_input,
        input_line_count,
        hidden_line_count: (input_rows > 0).then_some(input_rows),
        body_truncated: truncated || result_truncated,
        truncated: detail_truncated || truncated,
    };
    apply_result_to_display(&mut display, bounded_result.as_deref());
    display
}

/// The compact card target is a command's first physical line or an explicit
/// patch path. Body formatting remains shared with live/legacy displays.
pub(crate) fn build_tool_card_display(name: &str, args: Option<&serde_json::Value>) -> ToolDisplay {
    let mut display = build_tool_display(name, args, None);
    let field = match name {
        "bash" => "command",
        "apply_patch" | "patch" => "path",
        _ => return display,
    };
    let source = args
        .and_then(|args| args.get(field))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(name)
        .lines()
        .next()
        .unwrap_or("");
    let (detail, cut) = single_line(source, MAX_DETAIL_BYTES);
    display.detail = detail;
    display.truncated = display.body_truncated || cut;
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
mod tests;
