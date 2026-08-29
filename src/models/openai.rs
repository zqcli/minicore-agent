use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::pin::Pin;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::time::Instant as TokioInstant;

use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::ids::ToolCallId;
use minicore_runtime::model::{
    AssistantPart, DeliveryState, Model, ModelCallContext, ModelDescriptor, ModelError,
    ModelErrorKind, ModelEvent, ModelFinishReason, ModelMessage, ModelRef, ModelRequest,
    ModelStartFuture, ModelStream, ReasoningPreference, Usage,
};
use minicore_runtime::tools::ToolName;
use minicore_runtime::value::{BoundedText, MAX_JSON_BYTES};

use super::ModelConfigError;

const USER_AGENT: &str = concat!("minicore-agent/", env!("CARGO_PKG_VERSION"));
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_SSE_LINE_BYTES: usize = 1024 * 1024;
const MAX_SSE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_QUEUED_SSE_FRAMES: usize = 4_096;
const MAX_EVENT_BYTES: usize = minicore_runtime::model::MAX_MODEL_EVENT_TEXT_BYTES;

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

pub(super) struct OpenAiResponsesModel {
    descriptor: ModelDescriptor,
    client: reqwest::Client,
    endpoint: reqwest::Url,
    provider_model: String,
    api_key: String,
    output_budget_tokens: u32,
}

impl OpenAiResponsesModel {
    pub(super) fn new(settings: OpenAiResponsesSettings) -> Result<Self, ModelConfigError> {
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
            api_key: settings.api_key,
            output_budget_tokens: settings.output_budget_tokens,
        })
    }

    fn build_request(&self, request: &ModelRequest) -> Result<Vec<u8>, ModelError> {
        if !self.descriptor.supports_reasoning(request.reasoning())
            || (!request.tools().is_empty() && !self.descriptor.supports_tools)
        {
            return Err(local_error(ModelErrorKind::InvalidRequest));
        }
        let body = ResponsesRequest::from_runtime(
            &self.provider_model,
            self.output_budget_tokens,
            request,
        )?;
        let encoded =
            serde_json::to_vec(&body).map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
        let estimated_tokens = encoded.len().div_ceil(4) as u64;
        if estimated_tokens > self.descriptor.context_window {
            return Err(local_error(ModelErrorKind::ContextOverflow));
        }
        Ok(encoded)
    }

    async fn start_request(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> Result<ModelStream, ModelError> {
        if context.cancellation.is_cancelled() {
            return Err(local_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            return Err(local_error(ModelErrorKind::Timeout));
        }
        let body = self.build_request(&request)?;
        let authorization = HeaderValue::from_str(&format!("Bearer {}", self.api_key))
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
        let request = self
            .client
            .post(self.endpoint.clone())
            .header(AUTHORIZATION, authorization)
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
            return Err(classify_http_error(response, cancellation, deadline).await);
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
        let state = StreamState::new(bytes, cancellation, deadline);
        Ok(Box::pin(stream::unfold(state, next_stream_event)))
    }
}

impl Model for OpenAiResponsesModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start<'a>(
        &'a self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelStartFuture<'a> {
        Box::pin(self.start_request(request, context))
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
    ) -> Result<Self, ModelError> {
        let mut input = Vec::new();
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
                                let arguments = serde_json::to_string(call.arguments())
                                    .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
                                input.push(InputItem::FunctionCall(FunctionCallInput {
                                    item_type: "function_call",
                                    call_id: call.tool_call_id().to_string(),
                                    name: call.name().as_str().to_owned(),
                                    arguments,
                                    status: "completed",
                                }));
                            }
                        }
                    }
                }
                ModelMessage::Tool {
                    tool_call_id,
                    output,
                    ..
                } => input.push(InputItem::FunctionCallOutput(FunctionCallOutputInput {
                    item_type: "function_call_output",
                    call_id: tool_call_id.to_string(),
                    output: output.content().as_str().to_owned(),
                    status: "completed",
                })),
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
        };
        Ok(Self {
            model,
            input,
            tools,
            stream: true,
            store: false,
            truncation: "disabled",
            max_output_tokens: output_budget_tokens,
            reasoning,
        })
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum InputItem {
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

#[derive(Serialize)]
struct FunctionCallOutputInput {
    #[serde(rename = "type")]
    item_type: &'static str,
    call_id: String,
    output: String,
    status: &'static str,
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
) -> ModelError {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
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

    match status.as_u16() {
        400 | 422 if context_overflow => local_error(ModelErrorKind::ContextOverflow),
        400 | 422 => local_error(ModelErrorKind::InvalidRequest),
        401 | 403 => local_error(ModelErrorKind::AuthRejected),
        408 => unknown_error(ModelErrorKind::Timeout),
        413 => local_error(ModelErrorKind::ContextOverflow),
        429 => retryable_error(ModelErrorKind::RateLimited, retry_after),
        500..=599 => unknown_error(ModelErrorKind::ProviderUnavailable),
        400..=499 => local_error(ModelErrorKind::InvalidRequest),
        _ => unknown_error(ModelErrorKind::ProviderUnavailable),
    }
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

struct StreamState {
    bytes: ByteStream,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
    parser: SseParser,
    pending: VecDeque<Result<ModelEvent, ModelError>>,
    tools: BTreeMap<u32, ToolState>,
    tool_ids: BTreeSet<ToolCallId>,
    semantic_seen: bool,
    refusal_seen: bool,
    terminal_seen: bool,
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
    fn new(
        bytes: ByteStream,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: TokioInstant,
    ) -> Self {
        Self {
            bytes,
            cancellation,
            deadline,
            parser: SseParser::default(),
            pending: VecDeque::new(),
            tools: BTreeMap::new(),
            tool_ids: BTreeSet::new(),
            semantic_seen: false,
            refusal_seen: false,
            terminal_seen: false,
            done: false,
        }
    }

    fn queue(&mut self, event: ModelEvent) {
        self.semantic_seen = true;
        self.pending.push_back(Ok(event));
    }

    fn fail(&mut self, error: ModelError) {
        self.done = true;
        self.pending.push_back(Err(error));
    }

    fn malformed(&mut self) {
        let error = stream_error(ModelErrorKind::InvalidProviderResponse, self.semantic_seen);
        self.fail(error);
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
                let error = stream_error(ModelErrorKind::Cancelled, state.semantic_seen);
                state.fail(error);
                continue;
            }
            _ = tokio::time::sleep_until(state.deadline) => {
                let error = stream_error(ModelErrorKind::Timeout, state.semantic_seen);
                state.fail(error);
                continue;
            }
            next = state.bytes.next() => next,
        };
        match next {
            Some(Ok(chunk)) => {
                if state.parser.feed(&chunk).is_err() {
                    state.malformed();
                    continue;
                }
                while let Some(frame) = state.parser.pop_frame() {
                    if handle_frame(&mut state, &frame).is_err() {
                        state.malformed();
                        break;
                    }
                    if state.done {
                        break;
                    }
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
                let error = stream_error(kind, state.semantic_seen);
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
                    let error = stream_error(kind, state.semantic_seen);
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
        let error = stream_error(ModelErrorKind::IncompleteResponse, state.semantic_seen);
        state.fail(error);
        return Ok(());
    }
    let value: Value = serde_json::from_slice(frame).map_err(|_| ())?;
    let event_type = value.get("type").and_then(Value::as_str).ok_or(())?;
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
            let event: DeltaEvent = from_value(value)?;
            queue_text(state, event.delta, true)?;
        }
        "response.output_item.added" => {
            let event: OutputItemEvent = from_value(value)?;
            if item_type(&event.item) == Some("function_call") {
                let item: FunctionCallItem = from_value(event.item)?;
                start_tool(state, event.output_index, item, true)?;
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
            if item_type(&event.item) == Some("function_call") {
                let item: FunctionCallItem = from_value(event.item)?;
                finish_tool_item(state, event.output_index, item)?;
            }
        }
        "response.completed" => {
            let event: TerminalEvent = from_value(value)?;
            finish_response(state, event.response, false)?;
        }
        "response.incomplete" => {
            let event: TerminalEvent = from_value(value)?;
            finish_response(state, event.response, true)?;
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

#[derive(Default, Deserialize)]
struct ProviderResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    usage: Option<ProviderUsage>,
    #[serde(default)]
    incomplete_details: Option<IncompleteDetails>,
}

#[derive(Default, Deserialize)]
struct ProviderUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
    #[serde(default)]
    input_tokens_details: Option<InputTokenDetails>,
    #[serde(default)]
    output_tokens_details: Option<OutputTokenDetails>,
}

#[derive(Default, Deserialize)]
struct InputTokenDetails {
    #[serde(default)]
    cached_tokens: Option<u64>,
    #[serde(default)]
    cache_write_tokens: Option<u64>,
}

#[derive(Default, Deserialize)]
struct OutputTokenDetails {
    #[serde(default)]
    reasoning_tokens: Option<u64>,
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
    let tool_call_id = ToolCallId::new(item.call_id).map_err(|_| ())?;
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

fn finish_response(
    state: &mut StreamState,
    response: ProviderResponse,
    incomplete: bool,
) -> Result<(), ()> {
    if state.terminal_seen || state.tools.values().any(|tool| !tool.ended) {
        return Err(());
    }
    state.terminal_seen = true;
    let usage = provider_usage(response.usage.unwrap_or_default());
    state.queue(ModelEvent::Usage { usage });
    let reason = if incomplete {
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
    } else if response.status.as_deref() == Some("incomplete") {
        ModelFinishReason::Unknown
    } else {
        ModelFinishReason::Stop
    };
    state.queue(ModelEvent::Finish { reason });
    state.done = true;
    Ok(())
}

fn provider_usage(usage: ProviderUsage) -> Usage {
    let cached = usage
        .input_tokens_details
        .as_ref()
        .and_then(|details| details.cached_tokens);
    let cache_write = usage
        .input_tokens_details
        .as_ref()
        .and_then(|details| details.cache_write_tokens);
    let reasoning = usage
        .output_tokens_details
        .as_ref()
        .and_then(|details| details.reasoning_tokens);
    let input = usage.input_tokens.map(|total| {
        total
            .saturating_sub(cached.unwrap_or(0))
            .saturating_sub(cache_write.unwrap_or(0))
    });
    let output = usage
        .output_tokens
        .map(|total| total.saturating_sub(reasoning.unwrap_or(0)));
    Usage::from_optional(input, output, reasoning)
        .with_cache_read_tokens(cached)
        .with_cache_write_tokens(cache_write)
        .with_provider_total_tokens(usage.total_tokens)
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

fn stream_error(kind: ModelErrorKind, semantic_seen: bool) -> ModelError {
    if semantic_seen {
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
