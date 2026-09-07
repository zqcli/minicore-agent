use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{
    AssistantPart, DeliveryState, Model, ModelCallContext, ModelDescriptor, ModelError,
    ModelErrorKind, ModelEvent, ModelFinishReason, ModelMessage, ModelRef, ModelRequest,
    ModelStartFuture, ModelStream, ReasoningPreference, RetryHint, Usage,
};
use minicore_runtime::tools::ToolName;
use minicore_runtime::value::{BoundedText, MAX_JSON_BYTES};
use minicore_runtime::{LoopId, ToolCallId};

use super::ModelConfigError;

const USER_AGENT: &str = concat!("minicore-agent/", env!("CARGO_PKG_VERSION"));
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_SSE_LINE_BYTES: usize = 1024 * 1024;
const MAX_SSE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_QUEUED_SSE_FRAMES: usize = 4_096;
const MAX_EVENT_BYTES: usize = minicore_runtime::model::MAX_MODEL_EVENT_TEXT_BYTES;
const MAX_OPENAI_CALL_ID_BYTES: usize = 64;
const MAX_CONTINUATION_ITEMS_PER_ROUND: usize = 256;
const MAX_CONTINUATION_BYTES_PER_LOOP: usize = 4 * 1024 * 1024;
const MAX_ACTIVE_CONTINUATIONS: usize = 256;

pub(super) struct OpenAiResponsesSettings {
    pub(super) model_ref: ModelRef,
    pub(super) provider_model: String,
    pub(super) endpoint: reqwest::Url,
    pub(super) api_key: String,
    pub(super) effective_context_window: u64,
    pub(super) output_budget_tokens: u32,
    pub(super) supported_reasoning: BTreeSet<ReasoningPreference>,
    pub(super) supports_tools: bool,
    pub(super) request_timeout: Option<Duration>,
}

/// Continuations are keyed by the runtime loop that owns the model requests;
/// one agent turn maps to one loop. `request_index` is zero-based and must be
/// consecutive for replay within the same loop/model instance.
type ContinuationStore = Arc<Mutex<HashMap<LoopId, LoopContinuation>>>;

fn remove_oldest_continuation(continuations: &mut HashMap<LoopId, LoopContinuation>) -> bool {
    let oldest_key = continuations
        .iter()
        .map(|(key, continuation)| (*key, continuation.updated_at))
        .min_by_key(|(key, updated_at)| (*updated_at, *key))
        .map(|(key, _)| key);
    let Some(oldest_key) = oldest_key else {
        return false;
    };
    continuations.remove(&oldest_key);
    true
}

fn trim_active_continuations(continuations: &mut HashMap<LoopId, LoopContinuation>) {
    while continuations.len() > MAX_ACTIVE_CONTINUATIONS {
        if !remove_oldest_continuation(continuations) {
            break;
        }
    }
}

fn make_room_for_continuation(continuations: &mut HashMap<LoopId, LoopContinuation>) {
    while continuations.len() >= MAX_ACTIVE_CONTINUATIONS {
        if !remove_oldest_continuation(continuations) {
            break;
        }
    }
}

/// Redacted identity observed by tracing only: provider continuation is keyed
/// by `LoopId` and `request_index`, never by prompts, arguments, or responses.
#[derive(Clone, Copy)]
struct RequestTraceContext {
    loop_id: LoopId,
    request_index: u32,
}

impl From<&ModelCallContext> for RequestTraceContext {
    fn from(context: &ModelCallContext) -> Self {
        Self {
            loop_id: context.loop_id,
            request_index: context.request_index,
        }
    }
}

struct LoopContinuation {
    cancellation: CancellationToken,
    updated_at: Instant,
    total_output_item_bytes: usize,
    requests: Vec<ProviderRequestReplay>,
}

#[derive(Clone)]
struct ProviderRequestReplay {
    request_index: u32,
    tool_call_ids: Vec<ToolCallId>,
    output_items: Arc<[Value]>,
    output_item_bytes: usize,
}

pub(super) struct OpenAiResponsesModel {
    descriptor: ModelDescriptor,
    client: reqwest::Client,
    endpoint: reqwest::Url,
    provider_model: String,
    authorization: HeaderValue,
    output_budget_tokens: u32,
    continuations: ContinuationStore,
}

impl OpenAiResponsesModel {
    pub(super) fn new(settings: OpenAiResponsesSettings) -> Result<Self, ModelConfigError> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", settings.api_key))
            .map_err(|_| ModelConfigError::InvalidConfiguration)?;
        authorization.set_sensitive(true);
        drop(settings.api_key);
        let descriptor = ModelDescriptor::new(
            settings.model_ref,
            settings.effective_context_window,
            settings.supported_reasoning,
            settings.supports_tools,
        )
        .map_err(|_| ModelConfigError::InvalidConfiguration)?;
        let mut client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(USER_AGENT);
        if let Some(timeout) = settings.request_timeout {
            client = client.timeout(timeout);
        }
        let client = client.build().map_err(|_| ModelConfigError::ClientBuild)?;
        Ok(Self {
            descriptor,
            client,
            endpoint: settings.endpoint,
            provider_model: settings.provider_model,
            authorization,
            output_budget_tokens: settings.output_budget_tokens,
            continuations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn build_request(&self, request: &ModelRequest) -> Result<Vec<u8>, ModelError> {
        self.build_request_with_replay(request, &[])
            .map(|(encoded, _)| encoded)
    }

    fn build_request_with_replay(
        &self,
        request: &ModelRequest,
        replay: &[ProviderRequestReplay],
    ) -> Result<(Vec<u8>, Vec<u32>), ModelError> {
        if !self.descriptor.supports_reasoning(request.reasoning())
            || (!request.tools().is_empty() && !self.descriptor.supports_tools)
        {
            return Err(local_error(ModelErrorKind::InvalidRequest));
        }
        let (body, matched_request_indexes) = ResponsesRequest::from_runtime(
            &self.provider_model,
            self.output_budget_tokens,
            request,
            replay,
        )?;
        let encoded =
            serde_json::to_vec(&body).map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
        let estimated_tokens = encoded.len().div_ceil(4) as u64;
        if estimated_tokens > self.descriptor.context_window {
            return Err(local_error(ModelErrorKind::ContextOverflow));
        }
        Ok((encoded, matched_request_indexes))
    }

    fn continuation_snapshot(
        &self,
        loop_id: LoopId,
        request_index: u32,
        enabled: bool,
    ) -> Result<Vec<ProviderRequestReplay>, ModelError> {
        let mut continuations = self
            .continuations
            .lock()
            .map_err(|_| local_error(ModelErrorKind::Internal))?;
        continuations.retain(|_, continuation| !continuation.cancellation.is_cancelled());
        if request_index == 0 || !enabled {
            continuations.remove(&loop_id);
        }
        trim_active_continuations(&mut continuations);
        if !enabled {
            return Ok(Vec::new());
        }
        let Some(continuation) = continuations.get(&loop_id) else {
            return Ok(Vec::new());
        };
        let next_request_index = continuation
            .requests
            .iter()
            .map(|replay| replay.request_index)
            .max()
            .and_then(|highest| highest.checked_add(1));
        if next_request_index != Some(request_index) {
            // A non-consecutive index (model switch, stale continue) starts
            // a clean request; old continuation is discarded.
            continuations.remove(&loop_id);
            return Ok(Vec::new());
        }
        let recomputed_total = continuation
            .requests
            .iter()
            .try_fold(0_usize, |total, replay| {
                total.checked_add(replay.output_item_bytes).ok_or(())
            });
        if recomputed_total != Ok(continuation.total_output_item_bytes)
            || continuation.total_output_item_bytes > MAX_CONTINUATION_BYTES_PER_LOOP
        {
            continuations.remove(&loop_id);
            return Err(local_error(ModelErrorKind::Internal));
        }
        let mut snapshot = continuations
            .get(&loop_id)
            .into_iter()
            .flat_map(|continuation| continuation.requests.iter())
            .filter(|replay| replay.request_index < request_index)
            .cloned()
            .collect::<Vec<_>>();
        snapshot.sort_by_key(|replay| replay.request_index);
        Ok(snapshot)
    }

    fn remove_continuation(&self, loop_id: LoopId) {
        let Ok(mut continuations) = self.continuations.lock() else {
            return;
        };
        continuations.remove(&loop_id);
    }

    async fn start_request(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
        loop_id: LoopId,
        continuation_enabled: bool,
    ) -> Result<ModelStream, ModelError> {
        let trace = RequestTraceContext::from(&context);
        let request_index = context.request_index;
        let replay = self.continuation_snapshot(loop_id, request_index, continuation_enabled)?;
        if context.cancellation.is_cancelled() {
            return Err(local_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            return Err(local_error(ModelErrorKind::Timeout));
        }
        let (body, matched_request_indexes) = if replay.is_empty() {
            (self.build_request(&request)?, Vec::new())
        } else {
            self.build_request_with_replay(&request, &replay)?
        };
        let mut matched_request_ids = BTreeSet::new();
        let prior_output_item_bytes =
            matched_request_indexes
                .iter()
                .try_fold(0_usize, |total, matched_index| {
                    if !matched_request_ids.insert(*matched_index) {
                        return Err(local_error(ModelErrorKind::Internal));
                    }
                    let replay = replay
                        .iter()
                        .find(|replay| replay.request_index == *matched_index)
                        .ok_or_else(|| local_error(ModelErrorKind::Internal))?;
                    total
                        .checked_add(replay.output_item_bytes)
                        .ok_or_else(|| local_error(ModelErrorKind::Internal))
                })?;
        if prior_output_item_bytes > MAX_CONTINUATION_BYTES_PER_LOOP {
            return Err(local_error(ModelErrorKind::Internal));
        }
        let request = self
            .client
            .post(self.endpoint.clone())
            .header(AUTHORIZATION, self.authorization.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "text/event-stream")
            .body(body)
            .build()
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;

        if context.cancellation.is_cancelled() {
            return Err(local_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            return Err(local_error(ModelErrorKind::Timeout));
        }
        let cancellation = context.cancellation.clone();
        let deadline = TokioInstant::from_std(context.deadline);
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(unknown_error(ModelErrorKind::Cancelled));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(unknown_error(ModelErrorKind::Timeout));
            }
            response = self.client.execute(request) => {
                response.map_err(classify_send_error)?
            }
        };

        if !response.status().is_success() {
            let status_class = http_status_class(response.status());
            let error = classify_http_error(
                response,
                cancellation,
                deadline,
                Instant::now(),
                SystemTime::now(),
            )
            .await;
            log_provider_request_failure(trace, &error, status_class);
            return Err(error);
        }
        let is_sse = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));
        if !is_sse {
            return Err(unknown_error(ModelErrorKind::InvalidProviderResponse));
        }

        let bytes: ByteStream = Box::pin(response.bytes_stream());
        let continuation = continuation_enabled.then(|| StreamContinuation {
            request_index,
            cancellation: cancellation.clone(),
            prior_output_item_bytes,
            matched_request_indexes,
            guard: ContinuationGuard::new(Arc::clone(&self.continuations), loop_id),
        });
        let state = StreamState::new_with_continuation(
            bytes,
            cancellation,
            deadline,
            continuation,
            Some(trace),
        );
        Ok(Box::pin(stream::unfold(state, next_stream_event)))
    }
}

impl Model for OpenAiResponsesModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start(&self, request: ModelRequest, context: ModelCallContext) -> ModelStartFuture<'_> {
        let loop_id = context.loop_id;
        let trace = RequestTraceContext::from(&context);
        let continuation_enabled = request.reasoning() != ReasoningPreference::Disabled;
        Box::pin(async move {
            tracing::debug!(
                loop_id = %trace.loop_id,
                request_index = trace.request_index,
                "provider request start"
            );
            let result = self
                .start_request(request, context, loop_id, continuation_enabled)
                .await;
            if let Err(error) = &result {
                tracing::debug!(
                    loop_id = %trace.loop_id,
                    request_index = trace.request_index,
                    error_kind = ?error.kind(),
                    delivery = ?error.delivery(),
                    retryable = model_error_retryable(error),
                    "provider request start failed"
                );
                let preserve = error.delivery() == DeliveryState::NotStarted
                    && matches!(error.retry_hint(), RetryHint::Retryable { .. });
                if continuation_enabled && !preserve {
                    self.remove_continuation(loop_id);
                }
            } else {
                tracing::debug!(
                    loop_id = %trace.loop_id,
                    request_index = trace.request_index,
                    "provider response stream opened"
                );
            }
            result
        })
    }
}

pub(super) fn endpoint(base_url: &str) -> Result<reqwest::Url, ModelConfigError> {
    let parsed =
        reqwest::Url::parse(base_url).map_err(|_| ModelConfigError::InvalidConfiguration)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.host_str().is_none()
    {
        return Err(ModelConfigError::InvalidConfiguration);
    }
    let endpoint = format!("{}/responses", base_url.trim_end_matches('/'));
    reqwest::Url::parse(&endpoint).map_err(|_| ModelConfigError::InvalidConfiguration)
}

#[derive(Serialize)]
struct ResponsesRequest<'a> {
    model: &'a str,
    input: Vec<InputItem>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<FunctionTool>,
    stream: bool,
    store: bool,
    truncation: &'static str,
    max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ReasoningWire>,
}

impl<'a> ResponsesRequest<'a> {
    fn from_runtime(
        model: &'a str,
        output_budget_tokens: u32,
        request: &ModelRequest,
        replay: &[ProviderRequestReplay],
    ) -> Result<(Self, Vec<u32>), ModelError> {
        let mut input = Vec::new();
        let mut seen_tool_call_groups = BTreeSet::<Vec<ToolCallId>>::new();
        let mut used_replays = BTreeSet::<usize>::new();
        let mut matched_request_indexes = Vec::new();
        for (message_index, message) in request.messages().iter().enumerate() {
            match message {
                ModelMessage::System(text) => input.push(InputItem::Message(InputMessage {
                    item_type: "message",
                    role: "developer",
                    content: vec![InputContent {
                        content_type: "input_text",
                        text: text.clone(),
                        annotations: None,
                    }],
                    status: None,
                    id: None,
                })),
                ModelMessage::User(text) => input.push(InputItem::Message(InputMessage {
                    item_type: "message",
                    role: "user",
                    content: vec![InputContent {
                        content_type: "input_text",
                        text: text.clone(),
                        annotations: None,
                    }],
                    status: None,
                    id: None,
                })),
                ModelMessage::Assistant(parts) => {
                    let tool_call_ids = parts
                        .iter()
                        .filter_map(AssistantPart::as_tool_call)
                        .map(|call| call.tool_call_id().clone())
                        .collect::<Vec<_>>();
                    if !tool_call_ids.is_empty() {
                        if !seen_tool_call_groups.insert(tool_call_ids.clone()) {
                            return Err(local_error(ModelErrorKind::InvalidRequest));
                        }
                        if let Some((replay_index, provider_replay)) = replay
                            .iter()
                            .enumerate()
                            .find(|(replay_index, provider_replay)| {
                                !used_replays.contains(replay_index)
                                    && provider_replay.tool_call_ids == tool_call_ids
                            })
                        {
                            used_replays.insert(replay_index);
                            matched_request_indexes.push(provider_replay.request_index);
                            input.extend(
                                provider_replay
                                    .output_items
                                    .iter()
                                    .cloned()
                                    .map(InputItem::Raw),
                            );
                            continue;
                        }
                    }
                    for (part_index, part) in parts.iter().enumerate() {
                        match part {
                            AssistantPart::Text(text) => {
                                input.push(InputItem::Message(InputMessage {
                                    item_type: "message",
                                    role: "assistant",
                                    content: vec![InputContent {
                                        content_type: "output_text",
                                        text: text.clone(),
                                        annotations: Some(Vec::new()),
                                    }],
                                    status: Some("completed"),
                                    id: Some(format!("msg_minicore_{message_index}_{part_index}")),
                                }));
                            }
                            AssistantPart::Reasoning(_) => {
                                // MiniCore's public ReasoningContent does not preserve the exact
                                // provider item identity needed for safe Responses replay.
                            }
                            AssistantPart::ToolCall(call) => {
                                input.push(InputItem::FunctionCall(
                                    FunctionCallInput::from_runtime(call)?,
                                ));
                            }
                        }
                    }
                }
                ModelMessage::Tool {
                    tool_call_id,
                    output,
                    ..
                } => input.push(InputItem::FunctionCallOutput(
                    FunctionCallOutputInput::from_runtime(tool_call_id, output)?,
                )),
            }
        }
        let tools = request
            .tools()
            .iter()
            .map(|tool| FunctionTool {
                tool_type: "function",
                name: tool.name().as_str().to_owned(),
                description: tool.description().as_str().to_owned(),
                parameters: tool.input_schema().clone(),
            })
            .collect();
        let reasoning = match request.reasoning() {
            ReasoningPreference::Auto => None,
            ReasoningPreference::Disabled => Some(ReasoningWire {
                effort: "none",
                summary: None,
            }),
            ReasoningPreference::Low => Some(ReasoningWire {
                effort: "low",
                summary: Some("auto"),
            }),
            ReasoningPreference::Medium => Some(ReasoningWire {
                effort: "medium",
                summary: Some("auto"),
            }),
            ReasoningPreference::High => Some(ReasoningWire {
                effort: "high",
                summary: Some("auto"),
            }),
            ReasoningPreference::XHigh => Some(ReasoningWire {
                effort: "xhigh",
                summary: Some("auto"),
            }),
            ReasoningPreference::Max => Some(ReasoningWire {
                effort: "max",
                summary: Some("auto"),
            }),
            ReasoningPreference::Ultra => Some(ReasoningWire {
                effort: "ultra",
                summary: Some("auto"),
            }),
        };
        Ok((
            Self {
                model,
                input,
                tools,
                stream: true,
                store: false,
                truncation: "disabled",
                max_output_tokens: output_budget_tokens,
                reasoning,
            },
            matched_request_indexes,
        ))
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum InputItem {
    Raw(Value),
    Message(InputMessage),
    FunctionCall(FunctionCallInput),
    FunctionCallOutput(FunctionCallOutputInput),
}

#[derive(Serialize)]
struct InputMessage {
    #[serde(rename = "type")]
    item_type: &'static str,
    role: &'static str,
    content: Vec<InputContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

#[derive(Serialize)]
struct InputContent {
    #[serde(rename = "type")]
    content_type: &'static str,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    annotations: Option<Vec<Value>>,
}

#[derive(Serialize)]
struct FunctionCallInput {
    #[serde(rename = "type")]
    item_type: &'static str,
    call_id: String,
    name: String,
    arguments: String,
    status: &'static str,
}

impl FunctionCallInput {
    fn from_runtime(call: &minicore_runtime::model::ToolCall) -> Result<Self, ModelError> {
        let call_id = openai_request_call_id(call.tool_call_id())?;
        let arguments = serde_json::to_string(call.arguments())
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
        Ok(Self {
            item_type: "function_call",
            call_id,
            name: call.name().as_str().to_owned(),
            arguments,
            status: "completed",
        })
    }
}

#[derive(Serialize)]
struct FunctionCallOutputInput {
    #[serde(rename = "type")]
    item_type: &'static str,
    call_id: String,
    output: String,
    status: &'static str,
}

impl FunctionCallOutputInput {
    fn from_runtime(
        tool_call_id: &ToolCallId,
        output: &minicore_runtime::tools::ToolOutput,
    ) -> Result<Self, ModelError> {
        Ok(Self {
            item_type: "function_call_output",
            call_id: openai_request_call_id(tool_call_id)?,
            output: output.content().as_str().to_owned(),
            status: "completed",
        })
    }
}

#[derive(Serialize)]
struct FunctionTool {
    #[serde(rename = "type")]
    tool_type: &'static str,
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Serialize)]
struct ReasoningWire {
    effort: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<&'static str>,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    #[serde(default)]
    error: Option<ProviderErrorCode>,
}

#[derive(Deserialize)]
struct ProviderErrorCode {
    #[serde(default)]
    code: Option<String>,
    #[serde(default, rename = "type")]
    error_type: Option<String>,
}

async fn classify_http_error(
    response: reqwest::Response,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
    received_monotonic: Instant,
    received_system: SystemTime,
) -> ModelError {
    let status = response.status();
    let retry_target = retry_target(response.headers(), received_monotonic, received_system);
    let body = match collect_error_body(response, cancellation, deadline).await {
        Ok(body) => body,
        Err(error) => return error,
    };
    let envelope = serde_json::from_slice::<ErrorEnvelope>(&body).ok();
    let code = envelope
        .as_ref()
        .and_then(|envelope| envelope.error.as_ref())
        .and_then(|error| error.code.as_deref());
    let error_type = envelope
        .as_ref()
        .and_then(|envelope| envelope.error.as_ref())
        .and_then(|error| error.error_type.as_deref());
    let context_overflow = [code, error_type].into_iter().flatten().any(|value| {
        matches!(
            value,
            "context_length_exceeded" | "context_window_exceeded" | "context_overflow"
        )
    });
    let quota_exceeded = [code, error_type].into_iter().flatten().any(is_quota_code);

    match status.as_u16() {
        400 | 422 if context_overflow => local_error(ModelErrorKind::ContextOverflow),
        400 | 422 => local_error(ModelErrorKind::InvalidRequest),
        401 | 403 => local_error(ModelErrorKind::AuthRejected),
        408 => unknown_error(ModelErrorKind::Timeout),
        413 => local_error(ModelErrorKind::ContextOverflow),
        429 if quota_exceeded => local_error(ModelErrorKind::QuotaExceeded),
        429 => retryable_error(
            ModelErrorKind::RateLimited,
            retry_target.and_then(|target| target.remaining(Instant::now(), SystemTime::now())),
        ),
        500..=599 => unknown_error(ModelErrorKind::ProviderUnavailable),
        400..=499 => local_error(ModelErrorKind::InvalidRequest),
        _ => unknown_error(ModelErrorKind::ProviderUnavailable),
    }
}

fn http_status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() {
        400..=499 => "client_error",
        500..=599 => "server_error",
        _ => "other",
    }
}

fn model_error_retryable(error: &ModelError) -> bool {
    matches!(error.retry_hint(), RetryHint::Retryable { .. })
}

fn log_provider_request_failure(
    trace: RequestTraceContext,
    error: &ModelError,
    status_class: &'static str,
) {
    tracing::warn!(
        loop_id = %trace.loop_id,
        request_index = trace.request_index,
        error_kind = ?error.kind(),
        delivery = ?error.delivery(),
        retryable = model_error_retryable(error),
        status_class = status_class,
        "provider request failed"
    );
}

fn is_quota_code(value: &str) -> bool {
    matches!(
        value,
        "insufficient_quota"
            | "quota_exceeded"
            | "credit_balance_exhausted"
            | "billing_hard_limit_reached"
            | "billing_hard_limit_exceeded"
            | "billing_limit_reached"
            | "usage_limit_reached"
            | "usage_limit_exceeded"
            | "organization_quota_exceeded"
            | "project_quota_exceeded"
            | "organization_usage_limit_reached"
            | "organization_usage_limit_exceeded"
            | "project_usage_limit_reached"
            | "project_usage_limit_exceeded"
            | "organization_limit_reached"
            | "organization_limit_exceeded"
            | "project_limit_reached"
            | "project_limit_exceeded"
            | "budget_exceeded"
            | "organization_budget_exceeded"
            | "project_budget_exceeded"
            | "spend_limit_reached"
            | "spend_limit_exceeded"
            | "organization_spend_limit_reached"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_reached"
            | "project_spend_limit_exceeded"
    )
}

#[derive(Clone, Copy)]
enum RetryTarget {
    Monotonic {
        received_at: Instant,
        retry_at: Instant,
    },
    System {
        received_at: SystemTime,
        retry_at: SystemTime,
    },
}

impl RetryTarget {
    fn remaining(self, current_monotonic: Instant, current_system: SystemTime) -> Option<Duration> {
        let remaining = match self {
            Self::Monotonic {
                received_at,
                retry_at,
            } => {
                current_monotonic.checked_duration_since(received_at)?;
                retry_at.checked_duration_since(current_monotonic)?
            }
            Self::System {
                received_at,
                retry_at,
            } => {
                current_system.duration_since(received_at).ok()?;
                retry_at.duration_since(current_system).ok()?
            }
        };
        (!remaining.is_zero()).then_some(remaining)
    }
}

fn retry_target(
    headers: &reqwest::header::HeaderMap,
    received_monotonic: Instant,
    received_system: SystemTime,
) -> Option<RetryTarget> {
    let monotonic_target = |duration: Duration| {
        Some(RetryTarget::Monotonic {
            received_at: received_monotonic,
            retry_at: received_monotonic.checked_add(duration)?,
        })
    };
    headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| positive_duration(value, 1_000.0))
        .and_then(monotonic_target)
        .or_else(|| {
            let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
            if let Some(duration) = positive_duration(value, 1.0) {
                return monotonic_target(duration);
            }
            Some(RetryTarget::System {
                received_at: received_system,
                retry_at: httpdate::parse_http_date(value).ok()?,
            })
        })
}

fn positive_duration(value: &str, divisor: f64) -> Option<Duration> {
    let seconds = value.trim().parse::<f64>().ok()? / divisor;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(seconds)
        .ok()
        .filter(|duration| !duration.is_zero())
}

async fn collect_error_body(
    response: reqwest::Response,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
) -> Result<Vec<u8>, ModelError> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while body.len() < MAX_ERROR_BODY_BYTES {
        let chunk = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(unknown_error(ModelErrorKind::Cancelled));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(unknown_error(ModelErrorKind::Timeout));
            }
            chunk = stream.next() => chunk,
        };
        match chunk {
            Some(Ok(chunk)) => {
                let remaining = MAX_ERROR_BODY_BYTES - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    break;
                }
            }
            Some(Err(error)) if error.is_timeout() => {
                return Err(unknown_error(ModelErrorKind::Timeout));
            }
            Some(Err(_)) => return Err(unknown_error(ModelErrorKind::RequestOutcomeUnknown)),
            None => break,
        }
    }
    Ok(body)
}

fn classify_send_error(error: reqwest::Error) -> ModelError {
    if error.is_builder() {
        local_error(ModelErrorKind::InvalidRequest)
    } else if error.is_connect() {
        retryable_error(ModelErrorKind::TransportUnavailable, None)
    } else if error.is_timeout() {
        unknown_error(ModelErrorKind::Timeout)
    } else {
        unknown_error(ModelErrorKind::RequestOutcomeUnknown)
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct ContinuationGuard {
    store: ContinuationStore,
    key: LoopId,
    preserve: bool,
}

impl ContinuationGuard {
    fn new(store: ContinuationStore, key: LoopId) -> Self {
        Self {
            store,
            key,
            preserve: false,
        }
    }

    fn clear(&self) -> Result<(), ()> {
        self.store.lock().map_err(|_| ())?.remove(&self.key);
        Ok(())
    }

    fn preserve(&mut self) {
        self.preserve = true;
    }
}

impl Drop for ContinuationGuard {
    fn drop(&mut self) {
        if self.preserve {
            return;
        }
        if let Ok(mut continuations) = self.store.lock() {
            continuations.remove(&self.key);
        }
    }
}

struct StreamContinuation {
    request_index: u32,
    cancellation: CancellationToken,
    prior_output_item_bytes: usize,
    matched_request_indexes: Vec<u32>,
    guard: ContinuationGuard,
}

struct StreamState {
    bytes: ByteStream,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
    parser: SseParser,
    pending: VecDeque<Result<ModelEvent, ModelError>>,
    tools: BTreeMap<u32, ToolState>,
    tool_ids: BTreeSet<ToolCallId>,
    output_item_indexes: BTreeSet<u32>,
    output_items: Option<BTreeMap<u32, Value>>,
    output_item_bytes: usize,
    continuation: Option<StreamContinuation>,
    trace: Option<RequestTraceContext>,
    provider_event_seen: bool,
    semantic_seen: bool,
    reasoning_seen: bool,
    refusal_seen: bool,
    terminal_seen: bool,
    reasoning_parts: ReasoningParts,
    done: bool,
}

struct ToolState {
    tool_call_id: ToolCallId,
    name: ToolName,
    arguments: String,
    arguments_done: bool,
    item_done: bool,
    ended: bool,
}

impl StreamState {
    #[cfg(test)]
    fn new(
        bytes: ByteStream,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: TokioInstant,
    ) -> Self {
        Self::new_with_continuation(bytes, cancellation, deadline, None, None)
    }

    fn new_with_continuation(
        bytes: ByteStream,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: TokioInstant,
        continuation: Option<StreamContinuation>,
        trace: Option<RequestTraceContext>,
    ) -> Self {
        let output_items = continuation.as_ref().map(|_| BTreeMap::new());
        Self {
            bytes,
            cancellation,
            deadline,
            parser: SseParser::default(),
            pending: VecDeque::new(),
            tools: BTreeMap::new(),
            tool_ids: BTreeSet::new(),
            output_item_indexes: BTreeSet::new(),
            output_items,
            output_item_bytes: 0,
            continuation,
            trace,
            provider_event_seen: false,
            semantic_seen: false,
            reasoning_seen: false,
            refusal_seen: false,
            terminal_seen: false,
            reasoning_parts: ReasoningParts::default(),
            done: false,
        }
    }

    fn queue(&mut self, event: ModelEvent) {
        self.semantic_seen = true;
        self.pending.push_back(Ok(event));
    }

    fn fail(&mut self, error: ModelError) {
        if let Some(trace) = self.trace {
            tracing::warn!(
                loop_id = %trace.loop_id,
                request_index = trace.request_index,
                error_kind = ?error.kind(),
                delivery = ?error.delivery(),
                "provider stream failed"
            );
        }
        let _ = self.clear_continuation();
        self.done = true;
        self.pending.push_back(Err(error));
    }

    fn malformed(&mut self) {
        let error = stream_error(
            ModelErrorKind::InvalidProviderResponse,
            self.provider_event_seen,
        );
        self.fail(error);
    }

    fn prior_output_item_bytes(&self) -> usize {
        self.continuation
            .as_ref()
            .map_or(0, |continuation| continuation.prior_output_item_bytes)
    }

    fn validate_continuation_bytes(&self, output_item_bytes: usize) -> Result<(), ()> {
        let total = self
            .prior_output_item_bytes()
            .checked_add(output_item_bytes)
            .ok_or(())?;
        if total > MAX_CONTINUATION_BYTES_PER_LOOP {
            return Err(());
        }
        Ok(())
    }

    fn finalize_output_items(&mut self, terminal_output: Option<Vec<Value>>) -> Result<(), ()> {
        let Some(done_items) = self.output_items.as_ref() else {
            return Ok(());
        };
        let Some(terminal_output) = terminal_output else {
            return Ok(());
        };
        if terminal_output.len() > MAX_CONTINUATION_ITEMS_PER_ROUND
            || terminal_output
                .iter()
                .any(|item| validate_output_item(item).is_err())
        {
            return Err(());
        }
        let output_item_bytes = serialized_output_item_bytes(terminal_output.iter())?;
        self.validate_continuation_bytes(output_item_bytes)?;
        for (output_index, done_item) in done_items {
            let output_index = usize::try_from(*output_index).map_err(|_| ())?;
            if terminal_output.get(output_index) != Some(done_item) {
                return Err(());
            }
        }
        let output_items = terminal_output
            .into_iter()
            .enumerate()
            .map(|(output_index, item)| {
                u32::try_from(output_index)
                    .map(|output_index| (output_index, item))
                    .map_err(|_| ())
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        self.output_item_indexes = output_items.keys().copied().collect();
        self.output_items = Some(output_items);
        self.output_item_bytes = output_item_bytes;
        Ok(())
    }

    fn save_tool_round(&mut self) -> Result<(), ()> {
        let Some(continuation) = self.continuation.as_ref() else {
            return Ok(());
        };
        let output_items = self.output_items.as_ref().ok_or(())?;
        if self.reasoning_seen
            && !output_items
                .values()
                .any(|item| item_type(item) == Some("reasoning"))
        {
            return Err(());
        }
        let output_item_bytes = serialized_output_item_bytes(output_items.values())?;
        if output_item_bytes != self.output_item_bytes {
            return Err(());
        }
        self.validate_continuation_bytes(output_item_bytes)?;
        for (output_index, item) in output_items {
            if item_type(item) != Some("function_call") {
                continue;
            }
            let tool = self.tools.get(output_index).ok_or(())?;
            let item: FunctionCallItem = from_value(item.clone())?;
            if item.call_id.as_str() != tool.tool_call_id.as_str()
                || item.name.as_str() != tool.name.as_str()
                || item.arguments.as_str() != tool.arguments.as_str()
            {
                return Err(());
            }
        }
        for output_index in self.tools.keys() {
            if output_items.get(output_index).and_then(item_type) != Some("function_call") {
                return Err(());
            }
        }
        let replay = ProviderRequestReplay {
            request_index: continuation.request_index,
            tool_call_ids: self
                .tools
                .values()
                .map(|tool| tool.tool_call_id.clone())
                .collect(),
            output_items: output_items.values().cloned().collect::<Vec<_>>().into(),
            output_item_bytes,
        };
        let updated_at = Instant::now();
        {
            let mut continuations = continuation.guard.store.lock().map_err(|_| ())?;
            let existing_requests = continuations
                .get(&continuation.guard.key)
                .map(|continuation| continuation.requests.clone());
            let key_exists = existing_requests.is_some();
            let mut requests = existing_requests.unwrap_or_default();
            requests.retain(|existing| {
                existing.request_index != replay.request_index
                    && continuation
                        .matched_request_indexes
                        .contains(&existing.request_index)
            });
            requests.push(replay);
            requests.sort_by_key(|replay| replay.request_index);
            let total_output_item_bytes = requests.iter().try_fold(0_usize, |total, replay| {
                total.checked_add(replay.output_item_bytes).ok_or(())
            })?;
            if total_output_item_bytes > MAX_CONTINUATION_BYTES_PER_LOOP {
                return Err(());
            }
            if !key_exists {
                make_room_for_continuation(&mut continuations);
            }
            let turn_continuation =
                continuations
                    .entry(continuation.guard.key)
                    .or_insert_with(|| LoopContinuation {
                        cancellation: continuation.cancellation.clone(),
                        updated_at,
                        total_output_item_bytes,
                        requests: Vec::new(),
                    });
            turn_continuation.cancellation = continuation.cancellation.clone();
            turn_continuation.updated_at = updated_at.max(turn_continuation.updated_at);
            turn_continuation.total_output_item_bytes = total_output_item_bytes;
            turn_continuation.requests = requests;
        }
        self.continuation.as_mut().ok_or(())?.guard.preserve();
        Ok(())
    }

    fn clear_continuation(&self) -> Result<(), ()> {
        let Some(continuation) = self.continuation.as_ref() else {
            return Ok(());
        };
        continuation.guard.clear()
    }
}

async fn next_stream_event(
    mut state: StreamState,
) -> Option<(Result<ModelEvent, ModelError>, StreamState)> {
    loop {
        if let Some(event) = state.pending.pop_front() {
            return Some((event, state));
        }
        if state.done {
            return None;
        }
        let next = tokio::select! {
            biased;
            _ = state.cancellation.cancelled() => {
                let error = stream_error(ModelErrorKind::Cancelled, state.provider_event_seen);
                state.fail(error);
                continue;
            }
            _ = tokio::time::sleep_until(state.deadline) => {
                let error = stream_error(ModelErrorKind::Timeout, state.provider_event_seen);
                state.fail(error);
                continue;
            }
            next = state.bytes.next() => next,
        };
        match next {
            Some(Ok(chunk)) => {
                let parser_failed = state.parser.feed(&chunk).is_err();
                while let Some(frame) = state.parser.pop_frame() {
                    if handle_frame(&mut state, &frame).is_err() {
                        state.malformed();
                        break;
                    }
                    if state.done {
                        break;
                    }
                }
                if parser_failed && !state.done {
                    state.malformed();
                }
            }
            Some(Err(error)) => {
                let kind = if error.is_timeout() {
                    ModelErrorKind::Timeout
                } else if state.semantic_seen {
                    ModelErrorKind::StreamInterrupted
                } else {
                    ModelErrorKind::RequestOutcomeUnknown
                };
                let error = stream_error(kind, state.provider_event_seen);
                state.fail(error);
            }
            None => {
                if state.terminal_seen {
                    state.done = true;
                } else {
                    let kind = if state.semantic_seen {
                        ModelErrorKind::StreamInterrupted
                    } else {
                        ModelErrorKind::IncompleteResponse
                    };
                    let error = stream_error(kind, state.provider_event_seen);
                    state.fail(error);
                }
            }
        }
    }
}

#[derive(Default)]
struct SseParser {
    line: Vec<u8>,
    frame_data: Vec<u8>,
    frame_has_data: bool,
    pending_cr: bool,
    frames: VecDeque<Vec<u8>>,
    queued_frame_bytes: usize,
}

impl SseParser {
    fn feed(&mut self, chunk: &[u8]) -> Result<(), ()> {
        for &byte in chunk {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => {
                    self.pending_cr = true;
                    self.end_line()?;
                }
                b'\n' => self.end_line()?,
                _ => {
                    if self.line.len() >= MAX_SSE_LINE_BYTES {
                        return Err(());
                    }
                    self.line.push(byte);
                }
            }
        }
        Ok(())
    }

    fn end_line(&mut self) -> Result<(), ()> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            if self.frame_has_data {
                if self.frame_data.last() == Some(&b'\n') {
                    self.frame_data.pop();
                }
                let queued = self
                    .queued_frame_bytes
                    .checked_add(self.frame_data.len())
                    .ok_or(())?;
                if queued > MAX_SSE_FRAME_BYTES || self.frames.len() >= MAX_QUEUED_SSE_FRAMES {
                    return Err(());
                }
                self.queued_frame_bytes = queued;
                self.frames.push_back(std::mem::take(&mut self.frame_data));
            }
            self.frame_data.clear();
            self.frame_has_data = false;
            return Ok(());
        }
        if line.first() == Some(&b':') {
            return Ok(());
        }
        if let Some(data) = line.strip_prefix(b"data:") {
            let data = data.strip_prefix(b" ").unwrap_or(data);
            let new_len = self
                .frame_data
                .len()
                .checked_add(data.len() + 1)
                .ok_or(())?;
            if new_len > MAX_SSE_FRAME_BYTES {
                return Err(());
            }
            self.frame_data.extend_from_slice(data);
            self.frame_data.push(b'\n');
            self.frame_has_data = true;
        }
        Ok(())
    }

    fn pop_frame(&mut self) -> Option<Vec<u8>> {
        let frame = self.frames.pop_front()?;
        self.queued_frame_bytes = self.queued_frame_bytes.saturating_sub(frame.len());
        Some(frame)
    }
}

fn handle_frame(state: &mut StreamState, frame: &[u8]) -> Result<(), ()> {
    if frame == b"[DONE]" {
        let error = stream_error(
            ModelErrorKind::IncompleteResponse,
            state.provider_event_seen,
        );
        state.fail(error);
        return Ok(());
    }
    let value: Value = serde_json::from_slice(frame).map_err(|_| ())?;
    if !value.is_object() {
        return Err(());
    }
    let event_type = value.get("type").and_then(Value::as_str).ok_or(())?;
    state.provider_event_seen = true;
    match event_type {
        "response.output_text.delta" => {
            let event: DeltaEvent = from_value(value)?;
            queue_text(state, event.delta, false)?;
        }
        "response.refusal.delta" => {
            let event: DeltaEvent = from_value(value)?;
            state.refusal_seen = true;
            queue_text(state, event.delta, false)?;
        }
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            let event: ReasoningSummaryDeltaEvent = from_value(value)?;
            queue_reasoning_delta(state, event)?;
            state.reasoning_seen = true;
        }
        // Explicit summary-part lifecycle: some providers split ONE reasoning
        // item into several summary parts and only signal the part via these
        // events (the delta frames carry no index). Each event is a real part
        // boundary, so a following delta (even index-less) starts a new part
        // while deltas within the same part keep concatenating.
        "response.reasoning_summary_part.added" | "response.reasoning_summary_part.done" => {
            state.reasoning_parts.mark_item_boundary();
        }
        "response.output_item.added" => {
            let event: OutputItemEvent = from_value(value)?;
            if item_type(&event.item) == Some("function_call") {
                let item: FunctionCallItem = from_value(event.item)?;
                start_tool(state, event.output_index, item, true)?;
            } else if item_type(&event.item) == Some("reasoning") {
                state.reasoning_parts.mark_item_boundary();
            }
        }
        "response.function_call_arguments.delta" => {
            let event: ArgumentsDeltaEvent = from_value(value)?;
            append_tool_arguments(state, event.output_index, &event.delta)?;
        }
        "response.function_call_arguments.done" => {
            let event: ArgumentsDoneEvent = from_value(value)?;
            finish_tool_arguments(state, event.output_index, &event.arguments, true)?;
        }
        "response.output_item.done" => {
            let event: OutputItemEvent = from_value(value)?;
            capture_output_item(state, event.output_index, &event.item)?;
            if item_type(&event.item) == Some("function_call") {
                let item: FunctionCallItem = from_value(event.item)?;
                finish_tool_item(state, event.output_index, item)?;
            } else if item_type(&event.item) == Some("reasoning") {
                state.reasoning_parts.mark_item_boundary();
            }
        }
        "response.completed" => {
            let event: TerminalEvent = from_value(value)?;
            finish_response(state, event.response, TerminalKind::Completed)?;
        }
        "response.incomplete" => {
            let event: TerminalEvent = from_value(value)?;
            finish_response(state, event.response, TerminalKind::Incomplete)?;
        }
        "response.failed" | "error" => {
            state.terminal_seen = true;
            state.fail(started_error(ModelErrorKind::ProviderUnavailable));
        }
        _ => {}
    }
    Ok(())
}

fn item_type(value: &Value) -> Option<&str> {
    value.get("type").and_then(Value::as_str)
}

/// Reasoning summary parts are separated by a single newline, never by
/// text/capitalization heuristics. Each provider `...summary_text.delta`
/// carries (when present) the item identity (`item_id`/`output_index`) and
/// the per-item `summary_index`; a boundary is any change in that identity or
/// an explicit reasoning item `output_item.added`/`done`.
///
/// Missing identity is NOT a boundary: a fragment with no fields continues the
/// current part (the known identity is retained across present->absent->
/// present gaps), so a provider that drops metadata mid-part and restores it
/// later still concatenates. Only a genuine KNOWN-field mismatch or a
/// lifecycle transition separates parts.
#[derive(Default)]
struct ReasoningParts {
    /// Identity of the part the previous delta belonged to (fully-known so
    /// far: omitted fields inherit this value).
    last_key: Option<ReasoningPartKey>,
    /// Lifecycle bumped by reasoning `output_item.added`/`done` and
    /// `reasoning_summary_part.added`/`done`, so explicit boundaries separate
    /// parts even when deltas carry no index.
    lifecycle: u64,
    /// The `lifecycle` observed on the previous delta.
    last_lifecycle: u64,
    /// True once any nonempty reasoning text was handed to the stream.
    emitted: bool,
    /// Whether the last emitted nonempty text ends with a raw newline.
    last_ends_newline: bool,
    /// A boundary observed on an EMPTY delta (e.g. a new `summary_index` with
    /// no text): the next nonempty delta emits the separator without the
    /// empty delta itself producing a blank line.
    pending_boundary: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReasoningPartKey {
    output_index: Option<u32>,
    item_id: Option<String>,
    summary_index: Option<u32>,
}

#[derive(Deserialize)]
struct ReasoningSummaryDeltaEvent {
    delta: String,
    #[serde(default)]
    output_index: Option<serde_json::Value>,
    #[serde(default)]
    item_id: Option<String>,
    #[serde(default)]
    summary_index: Option<serde_json::Value>,
}

impl ReasoningParts {
    fn mark_item_boundary(&mut self) {
        self.lifecycle = self.lifecycle.wrapping_add(1);
    }
}

fn queue_reasoning_delta(
    state: &mut StreamState,
    event: ReasoningSummaryDeltaEvent,
) -> Result<(), ()> {
    let delta = event.delta.clone();
    // The delta's explicit fields override the previous known part; omitted
    // fields inherit it, so a metadata gap never invents a boundary.
    let merged = merge_reasoning_key(state.reasoning_parts.last_key.clone(), &event);
    let identity_changed = merged != state.reasoning_parts.last_key;
    let lifecycle_changed = state.reasoning_parts.last_lifecycle != state.reasoning_parts.lifecycle;

    if delta.is_empty() {
        // An empty delta can still announce the next part (a new summary index
        // before any text): remember the transition so the following
        // identity-less nonempty delta starts a new part, with no blank line.
        if identity_changed || lifecycle_changed {
            state.reasoning_parts.last_key = merged;
            state.reasoning_parts.last_lifecycle = state.reasoning_parts.lifecycle;
            state.reasoning_parts.pending_boundary = true;
        }
        return Ok(());
    }

    let boundary = identity_changed || lifecycle_changed || state.reasoning_parts.pending_boundary;
    // A single newline separates two actual nonempty parts; a raw provider
    // break at either side prevents doubling it.
    let needs_separator = boundary
        && state.reasoning_parts.emitted
        && !state.reasoning_parts.last_ends_newline
        && !delta.starts_with('\n');
    if needs_separator {
        queue_text(state, "\n".to_owned(), true)?;
    }
    queue_text(state, delta.clone(), true)?;
    if boundary {
        state.reasoning_parts.last_key = merged;
        state.reasoning_parts.last_lifecycle = state.reasoning_parts.lifecycle;
    }
    state.reasoning_parts.pending_boundary = false;
    state.reasoning_parts.emitted = true;
    state.reasoning_parts.last_ends_newline = delta.ends_with('\n');
    Ok(())
}

/// Overlays a delta's EXPLICIT identity fields over the last known part.
/// Missing identity (no fields at all) returns the previous key unchanged:
/// NOT a boundary. A partial delta retains the known fields and only overrides
/// the fields it actually carries, so a dropped-then-restored index on the
/// same part stays glued.
fn merge_reasoning_key(
    previous: Option<ReasoningPartKey>,
    event: &ReasoningSummaryDeltaEvent,
) -> Option<ReasoningPartKey> {
    let has_identity =
        event.output_index.is_some() || event.item_id.is_some() || event.summary_index.is_some();
    if !has_identity {
        return previous;
    }
    let previous = previous.unwrap_or(ReasoningPartKey {
        output_index: None,
        item_id: None,
        summary_index: None,
    });
    Some(ReasoningPartKey {
        output_index: event
            .output_index
            .as_ref()
            .and_then(|value| value_to_u32(value.clone()))
            .or(previous.output_index),
        item_id: event.item_id.clone().or(previous.item_id),
        summary_index: event
            .summary_index
            .as_ref()
            .and_then(|value| value_to_u32(value.clone()))
            .or(previous.summary_index),
    })
}

fn value_to_u32(value: serde_json::Value) -> Option<u32> {
    value.as_u64().and_then(|number| u32::try_from(number).ok())
}

fn capture_output_item(state: &mut StreamState, output_index: u32, item: &Value) -> Result<(), ()> {
    validate_output_item(item)?;
    if state.output_item_indexes.contains(&output_index)
        || state
            .output_items
            .as_ref()
            .is_some_and(|items| items.len() >= MAX_CONTINUATION_ITEMS_PER_ROUND)
    {
        return Err(());
    }
    let output_item_bytes = if state.output_items.is_some() {
        let item_bytes = serialized_output_item_len(item)?;
        let output_item_bytes = state.output_item_bytes.checked_add(item_bytes).ok_or(())?;
        state.validate_continuation_bytes(output_item_bytes)?;
        Some(output_item_bytes)
    } else {
        None
    };
    state.output_item_indexes.insert(output_index);
    if let Some(output_items) = state.output_items.as_mut() {
        output_items.insert(output_index, item.clone());
        state.output_item_bytes = output_item_bytes.ok_or(())?;
    }
    Ok(())
}

fn validate_output_item(item: &Value) -> Result<(), ()> {
    if item.is_object() && item_type(item).is_some() {
        Ok(())
    } else {
        Err(())
    }
}

fn serialized_output_item_len(item: &Value) -> Result<usize, ()> {
    serde_json::to_vec(item)
        .map(|encoded| encoded.len())
        .map_err(|_| ())
}

fn serialized_output_item_bytes<'a>(
    items: impl IntoIterator<Item = &'a Value>,
) -> Result<usize, ()> {
    items.into_iter().try_fold(0_usize, |total, item| {
        total
            .checked_add(serialized_output_item_len(item)?)
            .ok_or(())
    })
}

fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, ()> {
    serde_json::from_value(value).map_err(|_| ())
}

#[derive(Deserialize)]
struct DeltaEvent {
    delta: String,
}

#[derive(Deserialize)]
struct OutputItemEvent {
    output_index: u32,
    item: Value,
}

#[derive(Deserialize)]
struct FunctionCallItem {
    call_id: String,
    name: String,
    #[serde(default)]
    arguments: String,
}

#[derive(Deserialize)]
struct ArgumentsDeltaEvent {
    output_index: u32,
    delta: String,
}

#[derive(Deserialize)]
struct ArgumentsDoneEvent {
    output_index: u32,
    arguments: String,
}

#[derive(Deserialize)]
struct TerminalEvent {
    response: ProviderResponse,
}

#[derive(Deserialize)]
struct ProviderResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    usage: Option<ProviderUsage>,
    #[serde(default)]
    incomplete_details: Option<IncompleteDetails>,
    #[serde(default)]
    output: Option<Vec<Value>>,
}

#[derive(Deserialize)]
struct ProviderUsage {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    input_tokens_details: InputTokenDetails,
    output_tokens_details: OutputTokenDetails,
}

#[derive(Deserialize)]
struct InputTokenDetails {
    cached_tokens: u64,
    cache_write_tokens: u64,
}

#[derive(Deserialize)]
struct OutputTokenDetails {
    reasoning_tokens: u64,
}

#[derive(Deserialize)]
struct IncompleteDetails {
    #[serde(default)]
    reason: Option<String>,
}

fn queue_text(state: &mut StreamState, text: String, reasoning: bool) -> Result<(), ()> {
    if text.is_empty()
        || text
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(());
    }
    for chunk in utf8_chunks(&text, MAX_EVENT_BYTES) {
        let event = if reasoning {
            ModelEvent::reasoning_delta(chunk)
        } else {
            ModelEvent::text_delta(chunk)
        }
        .map_err(|_| ())?;
        state.queue(event);
    }
    Ok(())
}

fn utf8_chunks(value: &str, maximum: usize) -> Vec<&str> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < value.len() {
        let mut end = (start + maximum).min(value.len());
        while end > start && !value.is_char_boundary(end) {
            end -= 1;
        }
        chunks.push(&value[start..end]);
        start = end;
    }
    chunks
}

fn start_tool(
    state: &mut StreamState,
    output_index: u32,
    item: FunctionCallItem,
    reject_existing: bool,
) -> Result<(), ()> {
    if state.tools.contains_key(&output_index) {
        return if reject_existing { Err(()) } else { Ok(()) };
    }
    let tool_call_id = openai_tool_call_id(&item.call_id)?;
    let name = item.name.parse::<ToolName>().map_err(|_| ())?;
    if !state.tool_ids.insert(tool_call_id.clone()) {
        return Err(());
    }
    state.queue(ModelEvent::ToolCallStart {
        tool_call_id: tool_call_id.clone(),
        tool_name: name.clone(),
    });
    state.tools.insert(
        output_index,
        ToolState {
            tool_call_id,
            name,
            arguments: String::new(),
            arguments_done: false,
            item_done: false,
            ended: false,
        },
    );
    if !item.arguments.is_empty() {
        append_tool_arguments(state, output_index, &item.arguments)?;
    }
    Ok(())
}

fn append_tool_arguments(
    state: &mut StreamState,
    output_index: u32,
    delta: &str,
) -> Result<(), ()> {
    if delta.is_empty() {
        return Ok(());
    }
    let tool = state.tools.get_mut(&output_index).ok_or(())?;
    if tool.arguments_done || tool.item_done {
        return Err(());
    }
    let length = tool.arguments.len().checked_add(delta.len()).ok_or(())?;
    if length > MAX_JSON_BYTES {
        return Err(());
    }
    tool.arguments.push_str(delta);
    let tool_call_id = tool.tool_call_id.clone();
    let chunks = utf8_chunks(delta, MAX_EVENT_BYTES)
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for chunk in chunks {
        let event =
            ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), chunk).map_err(|_| ())?;
        state.queue(event);
    }
    Ok(())
}

fn reconcile_arguments(
    state: &mut StreamState,
    output_index: u32,
    complete: &str,
) -> Result<(), ()> {
    let current = state.tools.get(&output_index).ok_or(())?.arguments.clone();
    if complete == current {
        return Ok(());
    }
    let remainder = complete.strip_prefix(&current).ok_or(())?;
    append_tool_arguments(state, output_index, remainder)
}

fn finish_tool_arguments(
    state: &mut StreamState,
    output_index: u32,
    arguments: &str,
    arguments_done: bool,
) -> Result<(), ()> {
    if state
        .tools
        .get(&output_index)
        .is_some_and(|tool| tool.arguments_done)
    {
        return Err(());
    }
    reconcile_arguments(state, output_index, arguments)?;
    let tool = state.tools.get_mut(&output_index).ok_or(())?;
    if arguments_done {
        tool.arguments_done = true;
    }
    end_tool_once(state, output_index)
}

fn finish_tool_item(
    state: &mut StreamState,
    output_index: u32,
    item: FunctionCallItem,
) -> Result<(), ()> {
    openai_tool_call_id(&item.call_id)?;
    if !state.tools.contains_key(&output_index) {
        start_tool(state, output_index, item, false)?;
    } else {
        let tool = state.tools.get(&output_index).ok_or(())?;
        if tool.tool_call_id.to_string() != item.call_id || tool.name.as_str() != item.name {
            return Err(());
        }
        if tool.item_done {
            return Err(());
        }
        reconcile_arguments(state, output_index, &item.arguments)?;
    }
    state.tools.get_mut(&output_index).ok_or(())?.item_done = true;
    end_tool_once(state, output_index)
}

fn openai_tool_call_id(value: &str) -> Result<ToolCallId, ()> {
    validate_openai_call_id(value)?;
    ToolCallId::new(value).map_err(|_| ())
}

fn openai_request_call_id(value: &ToolCallId) -> Result<String, ModelError> {
    validate_openai_call_id(value.as_str())
        .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
    Ok(value.to_string())
}

fn validate_openai_call_id(value: &str) -> Result<(), ()> {
    if value.is_empty() || value.len() > MAX_OPENAI_CALL_ID_BYTES {
        return Err(());
    }
    ToolCallId::new(value).map(|_| ()).map_err(|_| ())
}

fn end_tool_once(state: &mut StreamState, output_index: u32) -> Result<(), ()> {
    let tool = state.tools.get_mut(&output_index).ok_or(())?;
    if tool.ended {
        return Ok(());
    }
    if tool.arguments.is_empty() {
        return Err(());
    }
    tool.ended = true;
    let tool_call_id = tool.tool_call_id.clone();
    state.queue(ModelEvent::ToolCallEnd { tool_call_id });
    Ok(())
}

#[derive(Clone, Copy)]
enum TerminalKind {
    Completed,
    Incomplete,
}

fn finish_response(
    state: &mut StreamState,
    response: ProviderResponse,
    kind: TerminalKind,
) -> Result<(), ()> {
    if state.terminal_seen || state.tools.values().any(|tool| !tool.ended) {
        return Err(());
    }
    let expected_status = match kind {
        TerminalKind::Completed => "completed",
        TerminalKind::Incomplete => "incomplete",
    };
    if response
        .status
        .as_deref()
        .is_some_and(|status| status != expected_status)
    {
        return Err(());
    }
    let usage = response.usage.map(provider_usage).transpose()?;
    state.finalize_output_items(response.output)?;
    let reason = if matches!(kind, TerminalKind::Incomplete) {
        match response
            .incomplete_details
            .and_then(|details| details.reason)
        {
            Some(reason) if matches!(reason.as_str(), "max_output_tokens" | "max_tokens") => {
                ModelFinishReason::Length
            }
            Some(reason) if reason == "content_filter" => ModelFinishReason::ContentFiltered,
            Some(reason) if reason == "refusal" => ModelFinishReason::Refused,
            _ => ModelFinishReason::Unknown,
        }
    } else if state.refusal_seen {
        ModelFinishReason::Refused
    } else if !state.tools.is_empty() {
        ModelFinishReason::ToolCalls
    } else {
        ModelFinishReason::Stop
    };
    match reason {
        ModelFinishReason::ToolCalls => state.save_tool_round()?,
        ModelFinishReason::Stop | ModelFinishReason::Refused | ModelFinishReason::Length => {
            state.clear_continuation()?;
        }
        ModelFinishReason::ContentFiltered | ModelFinishReason::Unknown => {}
    }
    state.terminal_seen = true;
    if let Some(trace) = state.trace {
        tracing::debug!(
            loop_id = %trace.loop_id,
            request_index = trace.request_index,
            finish_reason = ?reason,
            "provider request terminal"
        );
    }
    if let Some(usage) = usage {
        state.queue(ModelEvent::Usage { usage });
    }
    state.queue(ModelEvent::Finish { reason });
    state.done = true;
    Ok(())
}

fn provider_usage(usage: ProviderUsage) -> Result<Usage, ()> {
    let cached = usage.input_tokens_details.cached_tokens;
    let cache_write = usage.input_tokens_details.cache_write_tokens;
    let cached_and_written = cached.checked_add(cache_write).ok_or(())?;
    let reasoning = usage.output_tokens_details.reasoning_tokens;
    let input = usage
        .input_tokens
        .checked_sub(cached_and_written)
        .ok_or(())?;
    let output = usage.output_tokens.checked_sub(reasoning).ok_or(())?;
    let combined = usage
        .input_tokens
        .checked_add(usage.output_tokens)
        .ok_or(())?;
    if usage.total_tokens != combined {
        return Err(());
    }
    Ok(
        Usage::from_optional(Some(input), Some(output), Some(reasoning))
            .with_cache_read_tokens(Some(cached))
            .with_cache_write_tokens(Some(cache_write))
            .with_provider_total_tokens(Some(usage.total_tokens)),
    )
}

fn local_error(kind: ModelErrorKind) -> ModelError {
    error_with_delivery(kind, DeliveryState::NotStarted)
}

fn unknown_error(kind: ModelErrorKind) -> ModelError {
    error_with_delivery(kind, DeliveryState::Unknown)
}

fn started_error(kind: ModelErrorKind) -> ModelError {
    error_with_delivery(kind, DeliveryState::Started)
}

fn retryable_error(kind: ModelErrorKind, retry_after: Option<Duration>) -> ModelError {
    ModelError::not_started(kind, retry_after, diagnostic(kind, true))
}

fn stream_error(kind: ModelErrorKind, provider_event_seen: bool) -> ModelError {
    if provider_event_seen {
        started_error(kind)
    } else {
        unknown_error(kind)
    }
}

fn error_with_delivery(kind: ModelErrorKind, delivery: DeliveryState) -> ModelError {
    let diagnostic = diagnostic(kind, false);
    match delivery {
        DeliveryState::NotStarted => ModelError::permanent(kind, delivery, diagnostic),
        DeliveryState::Started => ModelError::started(kind, diagnostic),
        DeliveryState::Unknown => ModelError::unknown(kind, diagnostic),
    }
}

fn diagnostic(kind: ModelErrorKind, retryable: bool) -> DiagnosticSummary {
    let (code, message) = match kind {
        ModelErrorKind::Cancelled => (DiagnosticCode::RuntimeTerminated, "model request cancelled"),
        ModelErrorKind::Timeout => (DiagnosticCode::ModelTimeout, "model request timed out"),
        ModelErrorKind::InvalidRequest | ModelErrorKind::ContextOverflow => (
            DiagnosticCode::InvalidConfiguration,
            "model request is invalid",
        ),
        ModelErrorKind::InvalidProviderResponse
        | ModelErrorKind::IncompleteResponse
        | ModelErrorKind::StreamInterrupted
        | ModelErrorKind::RequestOutcomeUnknown
        | ModelErrorKind::UnexpectedToolCall => (
            DiagnosticCode::ModelMalformedResponse,
            "model provider response is unavailable",
        ),
        ModelErrorKind::AuthMissing
        | ModelErrorKind::AuthRejected
        | ModelErrorKind::RateLimited
        | ModelErrorKind::QuotaExceeded
        | ModelErrorKind::TransportUnavailable
        | ModelErrorKind::ProviderUnavailable
        | ModelErrorKind::Unavailable => (
            DiagnosticCode::ModelUnavailable,
            "model provider is unavailable",
        ),
        ModelErrorKind::Panicked | ModelErrorKind::Internal => {
            (DiagnosticCode::Internal, "model operation failed")
        }
    };
    DiagnosticSummary::new(
        code,
        DiagnosticCategory::Model,
        BoundedText::new(message).expect("static model diagnostic must fit"),
        retryable,
    )
}

#[cfg(test)]
mod tests;
