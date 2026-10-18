use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsStr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream as futures_stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::time::Instant as TokioInstant;

use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{
    AssistantPart, DeliveryState, Model, ModelCallContext, ModelDescriptor, ModelError,
    ModelErrorKind, ModelEvent, ModelFinishReason, ModelMessage, ModelRequest, ModelStartFuture,
    ModelStream, ReasoningPreference, RetryHint, Usage,
};
use minicore_runtime::tools::ToolName;
use minicore_runtime::value::{BoundedText, MAX_JSON_BYTES};
use minicore_runtime::{LoopId, ToolCallId};

use super::{ModelConfigError, ProviderBudget};

const USER_AGENT: &str = concat!("minicore-agent/", env!("CARGO_PKG_VERSION"));
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_DRAIN_BYTES: usize = 64 * 1024;
const DRAIN_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_SSE_LINE_BYTES: usize = 1024 * 1024;
const MAX_SSE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_QUEUED_SSE_FRAMES: usize = 4_096;
const MAX_EVENT_BYTES: usize = minicore_runtime::model::MAX_MODEL_EVENT_TEXT_BYTES;
const MAX_OPENAI_CALL_ID_BYTES: usize = 64;
const MAX_REPLAY_ITEMS_PER_RESPONSE: usize = 256;
mod errors;
mod http_error;
mod reasoning;
mod replay;
mod request;
mod sse;
mod stream;
mod terminal;
mod tools;

use errors::{local_error, retryable_error, started_error, stream_error, unknown_error};
use http_error::{
    classify_http_error, classify_send_error, http_status_class, log_provider_request_failure,
    model_error_retryable,
};
use reasoning::{ReasoningParts, ReasoningSummaryDeltaEvent, queue_reasoning_delta};
use request::{InputMessage, ResponsesRequest};
pub(super) use request::{endpoint, serialize_request_for_budget};
use sse::{SseParser, handle_frame, item_type};
use stream::{
    ByteStream, StreamState, ToolState, event_text_chunks, next_stream_event, queue_text,
};
use terminal::{TerminalEvent, TerminalKind, capture_output_item, finish_response};
use tools::{
    FunctionCallItem, append_tool_arguments, finish_tool_arguments, finish_tool_item,
    openai_request_call_id, openai_tool_call_id, start_tool,
};

#[cfg(test)]
use http_error::retry_target;
#[cfg(test)]
use request::{FunctionCallInput, FunctionCallOutputInput};
#[cfg(test)]
use terminal::provider_usage;

const MAX_REPLAY_BYTES: usize = minicore_runtime::model::ProviderReplay::MAX_BYTES;

pub(super) struct OpenAiResponsesSettings {
    pub(super) descriptor: ModelDescriptor,
    pub(super) provider_model: String,
    pub(super) endpoint: reqwest::Url,
    pub(super) api_key: String,
    pub(super) output_budget_tokens: u32,
    pub(super) request_timeout: Option<Duration>,
}

/// Redacted request tracing identity only; no provider replay cache is keyed
/// by these fields. Prompts, arguments, and responses are never trace fields.
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

pub(super) struct OpenAiResponsesModel {
    descriptor: ModelDescriptor,
    client: reqwest::Client,
    endpoint: reqwest::Url,
    provider_model: String,
    authorization: HeaderValue,
    output_budget_tokens: u32,
}

impl OpenAiResponsesModel {
    pub(super) fn new(settings: OpenAiResponsesSettings) -> Result<Self, ModelConfigError> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", settings.api_key))
            .map_err(|_| ModelConfigError::InvalidConfiguration)?;
        authorization.set_sensitive(true);
        drop(settings.api_key);
        let descriptor = settings.descriptor;
        let ssl_cert_file = std::env::var_os("SSL_CERT_FILE");
        let client = build_http_client(settings.request_timeout, ssl_cert_file.as_deref())?;
        if plaintext_remote_endpoint(&settings.endpoint) {
            tracing::warn!(model = %descriptor.model_ref, "non-loopback HTTP model sends bearer credentials in plaintext");
        }
        Ok(Self {
            descriptor,
            client,
            endpoint: settings.endpoint,
            provider_model: settings.provider_model,
            authorization,
            output_budget_tokens: settings.output_budget_tokens,
        })
    }

    fn build_request(&self, request: &ModelRequest) -> Result<Vec<u8>, ModelError> {
        if !self.descriptor.supports_reasoning(request.reasoning())
            || (!request.tools().is_empty() && !self.descriptor.supports_tools)
        {
            return Err(local_error(ModelErrorKind::InvalidRequest));
        }
        serde_json::to_vec(&self.request_body(request)?)
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))
    }

    fn request_body(&self, request: &ModelRequest) -> Result<ResponsesRequest<'_>, ModelError> {
        ResponsesRequest::from_runtime(
            &self.provider_model,
            self.output_budget_tokens,
            request,
            Some(self.endpoint.as_str()),
            self.descriptor
                .supported_reasoning
                .iter()
                .any(|preference| {
                    !matches!(
                        preference,
                        ReasoningPreference::Auto | ReasoningPreference::Disabled
                    )
                }),
        )
    }

    async fn start_request(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> Result<ModelStream, ModelError> {
        let trace = RequestTraceContext::from(&context);
        if context.cancellation.is_cancelled() {
            return Err(local_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            return Err(local_error(ModelErrorKind::Timeout));
        }
        let body = self.build_request(&request)?;
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
        let state = StreamState::new_with_replay(
            bytes,
            cancellation,
            deadline,
            Some((
                self.endpoint.as_str().to_owned(),
                self.provider_model.clone(),
            )),
            Some(trace),
        );
        // Surface initial SSE rejections through start(), where compaction can
        // recover ContextOverflow before the runtime begins consuming output.
        let (first, state) = next_stream_event(state)
            .await
            .expect("initial stream event");
        let first = first?;
        Ok(Box::pin(
            futures_stream::once(async move { Ok(first) })
                .chain(futures_stream::unfold(state, next_stream_event)),
        ))
    }
}

fn plaintext_remote_endpoint(endpoint: &reqwest::Url) -> bool {
    endpoint.scheme() == "http"
        && !endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
}

fn build_http_client(
    request_timeout: Option<Duration>,
    ssl_cert_file: Option<&OsStr>,
) -> Result<reqwest::Client, ModelConfigError> {
    let mut client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(USER_AGENT);
    if let Some(timeout) = request_timeout {
        client = client.timeout(timeout);
    }
    if let Some(path) = ssl_cert_file {
        let pem = std::fs::read(path).map_err(|_| ModelConfigError::InvalidTlsTrustStore)?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|_| ModelConfigError::InvalidTlsTrustStore)?;
        if certificates.is_empty() {
            return Err(ModelConfigError::InvalidTlsTrustStore);
        }
        // An explicit bundle replaces the default roots; never fall back on failure.
        client = client.tls_built_in_root_certs(false);
        for certificate in certificates {
            client = client.add_root_certificate(certificate);
        }
    }
    client.build().map_err(|_| {
        if ssl_cert_file.is_some() {
            ModelConfigError::InvalidTlsTrustStore
        } else {
            ModelConfigError::ClientBuild
        }
    })
}

impl Model for OpenAiResponsesModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start(&self, request: ModelRequest, context: ModelCallContext) -> ModelStartFuture<'_> {
        let trace = RequestTraceContext::from(&context);
        Box::pin(async move {
            tracing::debug!(
                loop_id = %trace.loop_id,
                request_index = trace.request_index,
                "provider request start"
            );
            let result = self.start_request(request, context).await;
            if let Err(error) = &result {
                tracing::debug!(
                    loop_id = %trace.loop_id,
                    request_index = trace.request_index,
                    error_kind = ?error.kind(),
                    delivery = ?error.delivery(),
                    retryable = model_error_retryable(error),
                    "provider request start failed"
                );
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

impl ProviderBudget for OpenAiResponsesModel {
    fn estimate_request_tokens(
        &self,
        request: &ModelRequest,
        _loop_id: Option<LoopId>,
        _request_index: Option<u32>,
    ) -> Result<u64, ModelError> {
        let body = serde_json::to_vec(&self.request_body(request)?)
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
        Ok(super::estimate_tokens(&body))
    }

    fn estimate_request_bytes(
        &self,
        request: &ModelRequest,
        _loop_id: Option<LoopId>,
        _request_index: Option<u32>,
    ) -> Result<usize, ModelError> {
        serde_json::to_vec(&self.request_body(request)?)
            .map(|bytes| bytes.len())
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))
    }
}

#[cfg(test)]
mod tests;

pub(super) fn validate_history_replay(
    replay: &minicore_runtime::model::ProviderReplay,
    parts: &[AssistantPart],
) -> Result<(), ()> {
    self::replay::validate_history(replay, parts)
}
