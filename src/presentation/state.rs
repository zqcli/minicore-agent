use super::*;

/// Source of the displayed context usage. Reported usage is the last API
/// request's footprint, not an estimate of an in-flight or future request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    #[default]
    Unknown,
    Estimated,
    Reported,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ContextUsageView {
    pub tokens: Option<u64>,
    pub window: Option<u64>,
    pub percent: Option<f64>,
    pub kind: ContextKind,
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
}

pub(super) struct LiveTool {
    tool_name: String,
    display: ToolDisplay,
}

pub(super) struct ToolResultInfo {
    content: String,
    truncated: bool,
}

/// Bound of each streamed field kept for an interrupted reply; the record must
/// fit one `session.read` page, so this stays well under the 256 KiB default.
const MAX_STREAMED_FIELD_BYTES: usize = 16 * 1024;

/// Head of the visible output the newest model request has streamed.
struct StreamedRequest {
    key: RequestKey,
    model: String,
    reasoning: String,
    text: String,
}

fn push_head(buffer: &mut String, delta: &str) {
    let room = MAX_STREAMED_FIELD_BYTES.saturating_sub(buffer.len());
    buffer.push_str(crate::text::truncate(delta, room).0);
}

/// Bounded per-request usage cache for the current loop (spec 9.4/12.4).
/// Keyed by `(loop_id, request_index)`; cleared when a new loop starts.
type RequestUsageCache = HashMap<RequestKey, minicore_runtime::model::Usage>;

#[derive(Default)]
pub(super) struct PresentationInner {
    /// Current-loop-only caches; cleared when a new loop starts.
    pub(super) live_tools: HashMap<(Option<RequestKey>, ToolCallId), LiveTool>,
    pub(super) tool_results: HashMap<(RequestKey, ToolCallId), ToolResultInfo>,
    request_usage: RequestUsageCache,
    streamed: Option<StreamedRequest>,
    live_times: VecDeque<Option<String>>,
    last_loop: Option<LastLoopView>,
    branch: Option<String>,
    model_label: Option<String>,
    context: ContextUsageView,
    context_settings: (u64, u64),
    installed_model: Option<Weak<dyn Model>>,
}

/// Per-session presentation state shared by the Model/Tool wrappers, the RPC
/// read, and the per-loop worker. All access is short critical sections;
/// callers never hold the lock across an await.
pub(crate) struct Presentation {
    pub(super) preview_attempt: AtomicU64,
    pub(super) session_id: SessionId,
    pub(super) events: AgentEventSink,
    pub(super) compaction: Arc<crate::compaction::CompactionState>,
    inner: Mutex<PresentationInner>,
}

impl Presentation {
    pub(crate) fn new(
        session_id: SessionId,
        events: AgentEventSink,
        compaction: Arc<crate::compaction::CompactionState>,
    ) -> Arc<Self> {
        Arc::new(Self {
            preview_attempt: AtomicU64::new(0),
            session_id,
            events,
            compaction,
            inner: Mutex::new(PresentationInner::default()),
        })
    }

    pub(super) fn next_preview_attempt(&self) -> Option<u64> {
        let mut previous = self.preview_attempt.load(Ordering::Relaxed);
        loop {
            let next = previous.checked_add(1)?;
            match self.preview_attempt.compare_exchange_weak(
                previous,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(next),
                Err(actual) => previous = actual,
            }
        }
    }

    pub(crate) fn set_branch(&self, branch: Option<String>) {
        self.lock().branch = branch;
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
            let turn = TurnRef {
                session_id: self.session_id,
                loop_id: key.loop_id,
            };
            AgentEvent::RequestUsage {
                turn,
                request_index: key.request_index,
                usage,
                meta: EventMeta::for_turn(turn),
            }
        };
        let _ = self.events.try_send(event);
    }

    /// Starts capturing a request's output; a retry of the same request starts over.
    pub(crate) fn begin_streamed(&self, key: RequestKey, model: String) {
        self.lock().streamed = Some(StreamedRequest {
            key,
            model,
            reasoning: String::new(),
            text: String::new(),
        });
    }

    pub(crate) fn note_streamed(&self, key: RequestKey, reasoning: bool, delta: &str) {
        let mut inner = self.lock();
        if let Some(streamed) = inner
            .streamed
            .as_mut()
            .filter(|streamed| streamed.key == key)
        {
            push_head(
                if reasoning {
                    &mut streamed.reasoning
                } else {
                    &mut streamed.text
                },
                delta,
            );
        }
    }

    /// The output of a request that never became one of `completed`'s assistant
    /// items, if it streamed any.
    pub(crate) fn take_interrupted(
        &self,
        loop_id: LoopId,
        completed: &[HistoryItem],
    ) -> Option<InterruptedRequest> {
        let streamed = self.lock().streamed.take()?;
        let key = streamed.key;
        let finished = completed.iter().any(|item| {
            matches!(item, HistoryItem::Assistant(assistant)
                if assistant.loop_id == key.loop_id && assistant.request_index == key.request_index)
        });
        (key.loop_id == loop_id
            && !finished
            && !(streamed.reasoning.is_empty() && streamed.text.is_empty()))
        .then_some(InterruptedRequest {
            stop_reason: PartialStopReason::Aborted,
            request_index: key.request_index,
            model: streamed.model,
            reasoning: streamed.reasoning,
            text: streamed.text,
        })
    }

    /// Clears observation state owned by the previous loop before the Runtime
    /// task is spawned. The next loop may begin synchronously from
    /// `AgentLoop::start`, so this must not run after that call.
    pub(crate) fn reset_before_loop_start(&self) {
        let mut inner = self.lock();
        inner.live_tools.clear();
        inner.tool_results.clear();
        inner.request_usage.clear();
        inner.streamed = None;
        inner.live_times.clear();
    }

    /// Binds metadata for the loop that was successfully started. This is
    /// deliberately metadata-only: the first real model request may already
    /// have installed its observation identity.
    pub(crate) fn bind_started_loop(&self, loop_id: LoopId, started_at: Option<String>) {
        self.lock().last_loop = Some(LastLoopView {
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

    pub(super) fn context_generation(&self, model: &dyn Model) -> Option<u64> {
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
        self.lock().live_times.push_back(accepted_at);
    }

    /// Copies the first accepted timestamps without consuming them. The
    /// corresponding queue is discarded only after the JSONL append succeeds,
    /// so a failed persistence attempt does not erase live metadata.
    pub(crate) fn peek_user_times(&self, count: usize) -> Vec<Option<String>> {
        self.lock().live_times.iter().take(count).cloned().collect()
    }

    /// Drops all acceptance metadata for the completed current loop.
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
                tool_name: invocation.tool_name().as_str().to_owned(),
                display,
            },
        );
    }

    /// Finalizes a completed tool in place: adds displayable result rows to
    /// the pending display, retains the result until `ToolFinished`, and
    /// emits the best-effort `tool_presentation` event with the
    /// fixed identity captured at execute start.
    pub(crate) fn finish_tool(
        &self,
        key: Option<RequestKey>,
        tool_call_id: &ToolCallId,
        result_text: Option<(String, bool)>,
    ) {
        let (tool_name, display) = {
            let mut inner = self.lock();
            let Some(live) = inner.live_tools.remove(&(key, tool_call_id.clone())) else {
                return;
            };
            let mut display = live.display;
            if let Some((_, truncated)) = &result_text {
                display.truncated |= *truncated;
            }
            apply_result_to_display(
                &mut display,
                result_text.as_ref().map(|(text, _)| text.as_str()),
            );
            if let (Some(key), Some((content, truncated))) = (key, result_text) {
                inner.tool_results.insert(
                    (key, tool_call_id.clone()),
                    ToolResultInfo { content, truncated },
                );
            }
            (live.tool_name, display)
        };
        let Some(key) = key else {
            return;
        };
        let turn = TurnRef {
            session_id: self.session_id,
            loop_id: key.loop_id,
        };
        let _ = self.events.try_send(AgentEvent::ToolPresentation {
            turn,
            request_index: key.request_index,
            tool_call_id: tool_call_id.clone(),
            tool_name,
            display,
            meta: EventMeta::for_turn(turn),
        });
    }

    /// Takes the pending result once, used to attach
    /// `content` to the runtime `ToolFinished` event so the TUI can render
    /// expanded results from the same source the wrapper saw.
    pub(crate) fn take_tool_result(
        &self,
        key: RequestKey,
        tool_call_id: &ToolCallId,
    ) -> Option<(String, bool)> {
        self.lock()
            .tool_results
            .remove(&(key, tool_call_id.clone()))
            .map(|info| (info.content, info.truncated))
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
            last_loop: inner.last_loop.clone(),
        }
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, PresentationInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub(crate) fn context_tokens(usage: Usage) -> Option<u64> {
    usage.provider_total_tokens().or_else(|| {
        usage
            .input_tokens()?
            .checked_add(usage.output_tokens()?)?
            .checked_add(usage.reasoning_tokens()?)?
            .checked_add(usage.cache_read_tokens()?)?
            .checked_add(usage.cache_write_tokens()?)
    })
}

/// Usage components are normalized by the provider adapter into disjoint
/// counts. Never guess a missing component is zero or sum a native total twice.
pub(super) fn reported_context(usage: Usage, physical_window: u64) -> ContextUsageView {
    let tokens = context_tokens(usage);
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
