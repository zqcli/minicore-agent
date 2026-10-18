use super::Gate;
use futures_util::stream;
use minicore_runtime::ToolCallId;
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason, ModelRef,
    ModelRequest, ModelStartFuture, ModelStream, ReasoningPreference, Usage,
};
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub(crate) fn supported_reasoning() -> BTreeSet<ReasoningPreference> {
    BTreeSet::from([
        ReasoningPreference::Auto,
        ReasoningPreference::Disabled,
        ReasoningPreference::Low,
        ReasoningPreference::Medium,
        ReasoningPreference::High,
        ReasoningPreference::XHigh,
        ReasoningPreference::Max,
        ReasoningPreference::Ultra,
    ])
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub(crate) enum Step {
    Events(Vec<ModelEvent>),
    Gated(Arc<Gate>, Box<Step>),
    Fail(ModelError),
    Pending,
    Custom(Box<dyn FnOnce(ModelRequest, ModelCallContext) -> ModelStartFuture<'static> + Send>),
    ReplayOnly(minicore_runtime::model::ProviderReplay),
    PreviewEvents(Vec<Result<ModelEvent, ModelError>>, Option<Arc<Gate>>),
    Text(&'static str),
    /// Text answer with a distinct per-request usage so per-request identity
    /// (loop_id, request_index) can be asserted (spec 9.4/12.4).
    TextWithUsage(&'static str, u64, u64),
    ToolCall(&'static str, serde_json::Value),
    /// Parks until `release` fires and signals `entered` when parked.
    BlockUntilDrop(Arc<Gate>, Arc<AtomicBool>, &'static str),

    /// Emits many text deltas back-to-back with no awaits so the runtime can
    /// saturate its best-effort event queue deterministically, then ends the
    /// request with one tool call so the loop runs a tool (with an await)
    /// before its next request.
    BurstTextThenTool(&'static str, usize, &'static str, serde_json::Value),
    ContextOverflowNotStarted,
    StreamErrorAfterText(&'static str, ModelError),
}

pub(crate) struct ScriptedModel {
    pacing: std::time::Duration,
    descriptor: ModelDescriptor,
    pub(crate) scripts: Mutex<VecDeque<Step>>,
    fallback: Box<dyn Fn() -> Step + Send + Sync>,
    pub(crate) requests: Arc<Mutex<Vec<ModelRequest>>>,
    pub(crate) contexts: Arc<Mutex<Vec<ModelCallContext>>>,
    start_entered: Arc<Notify>,
    pub(crate) calls: Arc<AtomicUsize>,
}

impl ScriptedModel {
    /// Configure delivery spacing before sharing the model with the agent.
    pub(crate) fn with_pacing(mut self: Arc<Self>, pacing: std::time::Duration) -> Arc<Self> {
        Arc::get_mut(&mut self).unwrap().pacing = pacing;
        self
    }

    pub(crate) fn with_fallback(
        mut self: Arc<Self>,
        fallback: impl Fn() -> Step + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::get_mut(&mut self).unwrap().fallback = Box::new(fallback);
        self
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub(crate) fn inert(model_ref: &str, window: u64) -> Arc<Self> {
        Self::with_reasoning(
            model_ref,
            window,
            [ReasoningPreference::Auto].into(),
            [Step::Custom(Box::new(|_, _| {
                panic!("inert model was started")
            }))],
        )
    }

    pub(crate) fn new(model_ref: &str, scripts: impl IntoIterator<Item = Step>) -> Arc<Self> {
        Self::with_window(model_ref, 16_384, scripts)
    }

    pub(crate) fn with_window(
        model_ref: &str,
        context_window: u64,
        scripts: impl IntoIterator<Item = Step>,
    ) -> Arc<Self> {
        Self::with_reasoning(model_ref, context_window, supported_reasoning(), scripts)
    }

    pub(crate) fn with_reasoning(
        model_ref: &str,
        context_window: u64,
        supported_reasoning: BTreeSet<ReasoningPreference>,
        scripts: impl IntoIterator<Item = Step>,
    ) -> Arc<Self> {
        let model_ref: ModelRef = model_ref.parse().unwrap();
        let descriptor =
            ModelDescriptor::new(model_ref.clone(), context_window, supported_reasoning, true)
                .unwrap();
        Arc::new(Self {
            pacing: std::time::Duration::ZERO,
            descriptor,
            scripts: Mutex::new(scripts.into_iter().collect()),
            fallback: Box::new(|| Step::Text("default")),
            requests: Arc::new(Mutex::new(Vec::new())),
            contexts: Arc::new(Mutex::new(Vec::new())),
            start_entered: Arc::new(Notify::new()),
            calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(crate) fn requests(&self) -> Arc<Mutex<Vec<ModelRequest>>> {
        Arc::clone(&self.requests)
    }

    pub(crate) fn contexts(&self) -> Arc<Mutex<Vec<ModelCallContext>>> {
        Arc::clone(&self.contexts)
    }

    pub(crate) fn start_entered(&self) -> Arc<Notify> {
        Arc::clone(&self.start_entered)
    }
}

impl Model for ScriptedModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start(&self, request: ModelRequest, context: ModelCallContext) -> ModelStartFuture<'_> {
        self.start_entered.notify_one();
        let pacing = self.pacing;
        self.requests.lock().unwrap().push(request.clone());
        self.contexts.lock().unwrap().push(context.clone());
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| (self.fallback)());
        Box::pin(async move {
            let events = |values| events(values, pacing);
            let mut script = script;
            loop {
                break match script {
                    Step::Gated(gate, next) => {
                        gate.pass().await;
                        script = *next;
                        continue;
                    }
                    Step::Events(values) => events(values),
                    Step::Fail(error) => Err(error),
                    Step::Pending => std::future::pending().await,
                    Step::Custom(start) => start(request, context).await,
                    Step::PreviewEvents(events, gate) => {
                        let stream: ModelStream = Box::pin(stream::unfold(
                            (events.into_iter(), gate),
                            |(mut events, gate)| async move {
                                if let Some(event) = events.next() {
                                    tokio::task::yield_now().await;
                                    Some((event, (events, gate)))
                                } else {
                                    if let Some(gate) = gate {
                                        gate.pass().await;
                                    }
                                    None
                                }
                            },
                        ));
                        Ok(stream)
                    }
                    Step::BlockUntilDrop(gate, dropped, text) => {
                        let _signal = DropSignal(dropped);
                        gate.pass().await;
                        events(vec![
                            ModelEvent::text_delta(text).unwrap(),
                            ModelEvent::Usage {
                                usage: Usage::new(1, 1, 0),
                            },
                            ModelEvent::Finish {
                                reason: ModelFinishReason::Stop,
                            },
                        ])
                    }
                    Step::ToolCall(name, arguments) => {
                        let tool_call_id =
                            ToolCallId::new(format!("{name}-call-{call_index}")).unwrap();
                        events(vec![
                            ModelEvent::ToolCallStart {
                                tool_call_id: tool_call_id.clone(),
                                tool_name: name.parse().unwrap(),
                            },
                            ModelEvent::tool_call_arguments_delta(
                                tool_call_id.clone(),
                                arguments.to_string(),
                            )
                            .unwrap(),
                            ModelEvent::ToolCallEnd { tool_call_id },
                            ModelEvent::Usage {
                                usage: Usage::new(1, 1, 0),
                            },
                            ModelEvent::Finish {
                                reason: ModelFinishReason::ToolCalls,
                            },
                        ])
                    }
                    Step::BurstTextThenTool(delta, count, name, arguments) => {
                        let tool_call_id =
                            ToolCallId::new(format!("{name}-call-{call_index}")).unwrap();
                        let event_rows: Vec<Result<ModelEvent, ModelError>> = (0..count)
                            .map(move |_| ModelEvent::text_delta(delta).unwrap())
                            .chain([
                                ModelEvent::ToolCallStart {
                                    tool_call_id: tool_call_id.clone(),
                                    tool_name: name.parse().unwrap(),
                                },
                                ModelEvent::tool_call_arguments_delta(
                                    tool_call_id.clone(),
                                    arguments.to_string(),
                                )
                                .unwrap(),
                                ModelEvent::ToolCallEnd { tool_call_id },
                                ModelEvent::Usage {
                                    usage: Usage::new(1, 1, 0),
                                },
                                ModelEvent::Finish {
                                    reason: ModelFinishReason::ToolCalls,
                                },
                            ])
                            .map(Ok::<ModelEvent, ModelError>)
                            .collect();
                        let stream: ModelStream = Box::pin(stream::iter(event_rows));
                        Ok(stream)
                    }
                    Step::ReplayOnly(replay) => events(vec![
                        ModelEvent::ProviderReplay { replay },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]),
                    Step::Text(text) => {
                        // Respect ModelEvent's delta bound while allowing fixtures
                        // to exercise the independently bounded assembled response.
                        let mut rows = Vec::new();
                        let mut remaining = text;
                        while !remaining.is_empty() {
                            let mut end = remaining.len().min(4_096);
                            while !remaining.is_char_boundary(end) {
                                end -= 1;
                            }
                            rows.push(ModelEvent::text_delta(&remaining[..end]).unwrap());
                            remaining = &remaining[end..];
                        }
                        rows.push(ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        });
                        rows.push(ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        });
                        events(rows)
                    }
                    Step::TextWithUsage(text, input, output) => events(vec![
                        ModelEvent::text_delta(text).unwrap(),
                        ModelEvent::Usage {
                            usage: Usage::new(input, output, 0)
                                .with_provider_total_tokens(Some(input + output)),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]),
                    Step::ContextOverflowNotStarted => Err(context_overflow()),
                    Step::StreamErrorAfterText(text, err) => {
                        let event_rows = vec![Ok(ModelEvent::text_delta(text).unwrap()), Err(err)];
                        let stream: ModelStream = Box::pin(stream::iter(event_rows));
                        Ok(stream)
                    }
                };
            }
        })
    }
}

fn events(values: Vec<ModelEvent>, pacing: std::time::Duration) -> Result<ModelStream, ModelError> {
    // Agent tests opt into spacing to avoid dropping best-effort loop events.
    Ok(Box::pin(stream::unfold(
        values.into_iter(),
        move |mut remaining| async move {
            let event = remaining.next()?;
            if !pacing.is_zero() {
                tokio::time::sleep(pacing).await;
            }
            Some((Ok(event), remaining))
        },
    )))
}

pub(crate) fn context_overflow() -> ModelError {
    let diagnostic = minicore_runtime::error::DiagnosticSummary::new(
        minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
        minicore_runtime::error::DiagnosticCategory::Model,
        minicore_runtime::value::BoundedText::new("context window exceeded").unwrap(),
        false,
    )
    .unwrap();
    ModelError::permanent(
        minicore_runtime::model::ModelErrorKind::ContextOverflow,
        minicore_runtime::model::DeliveryState::NotStarted,
        diagnostic,
    )
}

impl Step {
    pub(crate) fn gated_tool(
        gate: Arc<Gate>,
        name: &'static str,
        arguments: serde_json::Value,
    ) -> Self {
        Self::Gated(gate, Box::new(Self::ToolCall(name, arguments)))
    }

    pub(crate) fn gated_text(gate: Arc<Gate>, text: &'static str) -> Self {
        Self::Gated(gate, Box::new(Self::Text(text)))
    }
}
