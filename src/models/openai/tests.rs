use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use minicore_runtime::model::{
    AssistantPart, DeliveryState, ModelFinishReason, ModelLimits, ModelMessage, ModelRequest,
    ReasoningContent, RetryHint, ToolCall, Usage,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome, ToolSpec};
use minicore_runtime::{LoopId, ToolCallId};

use crate::agent::{Agent, CreateSession, SendMessage};
use crate::config::{AgentConfig, CompactionConfig, LoopOverrides, Profile};
use crate::event::{AgentEvent, OutputChannel};
use crate::history::{GetHistory, HistoryItemView};
use crate::models::{ModelConfig, Models};
use crate::profiles::ApprovalMode;
use crate::sessions::TurnRef;

use super::*;

use crate::openai_mock::{MockResponse, MockServer, sse_body};

const TEST_CA_BUNDLE: &[u8] = include_bytes!("../../../tests/fixtures/amazon-root-ca-3.pem");

struct TlsTempDir(PathBuf);

impl TlsTempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "minicore-agent-tls-{}",
            crate::ids::SessionId::new().unwrap()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TlsTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn tls_client_without_custom_bundle_builds() {
    // Pass the setting explicitly: no process-wide environment mutations.
    assert!(build_http_client(None, None).is_ok());
    assert!(build_http_client(Some(Duration::from_secs(5)), None).is_ok());
}

#[test]
fn tls_client_accepts_single_and_multiple_certificates() {
    let temp = TlsTempDir::new();
    let path = temp.0.join("ca.pem");
    let text = std::str::from_utf8(TEST_CA_BUNDLE).unwrap();
    for bundle in [
        TEST_CA_BUNDLE.to_vec(),
        TEST_CA_BUNDLE.repeat(2),
        text.replace('\n', "\r\n").into_bytes(),
        text.replace('\n', "\r").into_bytes(),
        format!("# explanatory preamble\n\n{text}\n# between certificates\n\n{text}").into_bytes(),
    ] {
        std::fs::write(&path, bundle).unwrap();
        assert!(build_http_client(None, Some(path.as_os_str())).is_ok());
    }
}

#[test]
fn tls_client_rejects_empty_missing_and_unreadable_paths() {
    let temp = TlsTempDir::new();
    let missing = temp.0.join("PRIVATE-PATH-MARKER-missing.pem");
    for path in [OsStr::new(""), missing.as_os_str(), temp.0.as_os_str()] {
        let error = build_http_client(None, Some(path)).unwrap_err();
        assert_eq!(error, ModelConfigError::InvalidTlsTrustStore);
        assert_eq!(error.to_string(), "model TLS trust store is invalid");
        assert_eq!(format!("{error:?}"), "InvalidTlsTrustStore");
    }
}

#[test]
fn tls_client_rejects_empty_malformed_and_invalid_der_bundles() {
    let temp = TlsTempDir::new();
    let path = temp.0.join("PRIVATE-PATH-MARKER-ca.pem");
    let malformed_pem =
        b"-----BEGIN CERTIFICATE-----\n!PRIVATE-CONTENT-MARKER!\n-----END CERTIFICATE-----\n";
    // This is valid PEM encoding but invalid certificate DER: client build must fail.
    let invalid_der = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
    for bundle in [
        Vec::new(),
        b" \n\t".to_vec(),
        b"PRIVATE-CONTENT-MARKER".to_vec(),
        malformed_pem.to_vec(),
        invalid_der.to_vec(),
        [TEST_CA_BUNDLE, malformed_pem].concat(),
        [TEST_CA_BUNDLE, invalid_der].concat(),
    ] {
        std::fs::write(&path, bundle).unwrap();
        let error = build_http_client(None, Some(path.as_os_str())).unwrap_err();
        assert_eq!(error, ModelConfigError::InvalidTlsTrustStore);
        assert_eq!(error.to_string(), "model TLS trust store is invalid");
        assert_eq!(format!("{error:?}"), "InvalidTlsTrustStore");
    }
}

#[cfg(unix)]
#[test]
fn tls_client_accepts_non_utf8_bundle_path() {
    use std::os::unix::ffi::OsStrExt;

    let temp = TlsTempDir::new();
    let path = temp.0.join(OsStr::from_bytes(b"ca-\xff.pem"));
    std::fs::write(&path, TEST_CA_BUNDLE).unwrap();
    assert!(build_http_client(None, Some(path.as_os_str())).is_ok());
}

fn settings(base_url: &str) -> OpenAiResponsesSettings {
    OpenAiResponsesSettings {
        model_ref: "main".parse().unwrap(),
        provider_model: "provider-model".to_owned(),
        endpoint: endpoint(base_url).unwrap(),
        api_key: "TEST-API-KEY-SECRET".to_owned(),
        effective_context_window: 16_384,
        output_budget_tokens: 1_024,
        supported_reasoning: BTreeSet::from([
            ReasoningPreference::Auto,
            ReasoningPreference::Disabled,
            ReasoningPreference::Low,
            ReasoningPreference::Medium,
            ReasoningPreference::High,
            ReasoningPreference::XHigh,
            ReasoningPreference::Max,
            ReasoningPreference::Ultra,
        ]),
        supports_tools: true,
        request_timeout: Some(Duration::from_secs(5)),
    }
}

fn model(base_url: &str) -> OpenAiResponsesModel {
    OpenAiResponsesModel::new(settings(base_url)).unwrap()
}

fn agent_model_config(base_url: &str) -> ModelConfig {
    ModelConfig::OpenAiResponses {
        model: "provider-model".to_owned(),
        base_url: base_url.to_owned(),
        api_key_env: "MINICORE_UNUSED_OPENAI_AGENT_KEY".to_owned(),
        physical_context_window: 18_408,
        output_budget_tokens: 1_024,
        safety_margin_tokens: 1_000,
        supported_reasoning: BTreeSet::from([
            ReasoningPreference::Auto,
            ReasoningPreference::Disabled,
            ReasoningPreference::Low,
            ReasoningPreference::Medium,
            ReasoningPreference::High,
            ReasoningPreference::XHigh,
            ReasoningPreference::Max,
            ReasoningPreference::Ultra,
        ]),
        supports_tools: true,
        request_timeout_seconds: Some(5),
    }
}

fn context(cancellation: CancellationToken, deadline: Duration) -> ModelCallContext {
    context_for_request(0, cancellation, deadline)
}

fn context_for_request(
    request_index: u32,
    cancellation: CancellationToken,
    deadline: Duration,
) -> ModelCallContext {
    context_for_loop(
        "lup_00000000000000000000000000000001".parse().unwrap(),
        request_index,
        cancellation,
        deadline,
    )
}

fn context_for_loop(
    loop_id: LoopId,
    request_index: u32,
    cancellation: CancellationToken,
    deadline: Duration,
) -> ModelCallContext {
    ModelCallContext::new(
        loop_id,
        request_index,
        cancellation,
        Instant::now() + deadline,
    )
}

fn basic_request(reasoning: ReasoningPreference) -> ModelRequest {
    ModelRequest::new(
        vec![ModelMessage::user("hello").unwrap()],
        Vec::new(),
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        reasoning,
    )
    .unwrap()
}

fn read_tool() -> ToolSpec {
    ToolSpec::new(
        "read".parse().unwrap(),
        "Read a file",
        json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": false
        }),
    )
    .unwrap()
}

fn history_request(reasoning: ReasoningPreference) -> ModelRequest {
    let call_id = ToolCallId::new("call-history").unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "read".parse().unwrap(),
        json!({"path": "src/lib.rs"}),
        0,
    )
    .unwrap();
    ModelRequest::new(
        vec![
            ModelMessage::system("system instructions").unwrap(),
            ModelMessage::user("inspect").unwrap(),
            ModelMessage::assistant(vec![
                AssistantPart::Reasoning(
                    ReasoningContent::new(Some("private reasoning".to_owned()), None, None, None)
                        .unwrap(),
                ),
                AssistantPart::Text("prior answer".to_owned()),
                AssistantPart::ToolCall(call),
            ])
            .unwrap(),
            ModelMessage::tool_with_outcome(
                call_id,
                ToolOutput::new("durable tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        reasoning,
    )
    .unwrap()
}

fn tool_exchange_request(call_id: &str) -> ModelRequest {
    let call_id = ToolCallId::new(call_id).unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "read".parse().unwrap(),
        json!({"path": "input.txt"}),
        0,
    )
    .unwrap();
    ModelRequest::new(
        vec![
            ModelMessage::user("read").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                call_id,
                ToolOutput::new("result").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::Auto,
    )
    .unwrap()
}

fn completed(usage: Value) -> Value {
    json!({
        "type": "response.completed",
        "future_terminal_field": true,
        "response": {
            "status": "completed",
            "usage": usage,
            "future_response_field": true
        }
    })
}

fn incomplete(reason: &str, usage: Value) -> Value {
    json!({
        "type": "response.incomplete",
        "future_terminal_field": true,
        "response": {
            "status": "incomplete",
            "incomplete_details": {"reason": reason},
            "usage": usage,
            "future_response_field": true
        }
    })
}

fn usage() -> Value {
    json!({
        "input_tokens": 20,
        "input_tokens_details": {
            "cached_tokens": 5,
            "cache_write_tokens": 3,
            "future_input_detail": true
        },
        "output_tokens": 11,
        "output_tokens_details": {"reasoning_tokens": 4, "future_output_detail": true},
        "total_tokens": 31,
        "future_usage_field": true
    })
}

async fn run_model(
    model: &OpenAiResponsesModel,
    request: ModelRequest,
    context: ModelCallContext,
) -> Result<Vec<ModelEvent>, ModelError> {
    let mut stream = model.start(request, context).await?;
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        let event = event?;
        if !matches!(event, ModelEvent::ProviderReplay { .. }) {
            events.push(event);
        }
    }
    Ok(events)
}

async fn start_error(
    model: &OpenAiResponsesModel,
    request: ModelRequest,
    context: ModelCallContext,
) -> ModelError {
    match model.start(request, context).await {
        Ok(_) => panic!("model start unexpectedly succeeded"),
        Err(error) => error,
    }
}

async fn run_until_error(
    model: &OpenAiResponsesModel,
    request: ModelRequest,
    context: ModelCallContext,
) -> (Vec<ModelEvent>, ModelError) {
    let mut stream = model.start(request, context).await.unwrap();
    let mut events = Vec::new();
    loop {
        match stream.next().await {
            Some(Ok(event)) => events.push(event),
            Some(Err(error)) => return (events, error),
            None => panic!("model stream ended without an error"),
        }
    }
}

fn event_text(event: &ModelEvent) -> Option<&str> {
    match event {
        ModelEvent::TextDelta { delta } | ModelEvent::ReasoningDelta { delta } => {
            Some(delta.as_str())
        }
        _ => None,
    }
}

fn assert_error(
    error: &ModelError,
    kind: ModelErrorKind,
    delivery: DeliveryState,
    retryable: bool,
) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.delivery(), delivery);
    assert_eq!(
        matches!(error.retry_hint(), RetryHint::Retryable { .. }),
        retryable
    );
    assert!(!error.diagnostic().message.as_str().contains("SECRET"));
}

#[tokio::test]
async fn descriptor_and_request_mapping_are_exact_and_secret_safe() {
    let response = MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "ok"}),
        completed(usage()),
    ]);
    let server = MockServer::spawn([response]).await;
    let model = model(server.base_url());
    assert_eq!(model.descriptor().model_ref.as_str(), "main");
    assert_eq!(model.descriptor().context_window, 16_384);
    assert!(model.descriptor().supports_tools);
    assert!(model.authorization.is_sensitive());
    assert!(!format!("{:?}", model.authorization).contains("TEST-API-KEY-SECRET"));

    let events = run_model(
        &model,
        history_request(ReasoningPreference::Medium),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert!(events.iter().any(|event| event_text(event) == Some("ok")));
    let requests = server.finish().await;
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method(), "POST");
    assert_eq!(request.path(), "/responses");
    assert_eq!(
        request.header("authorization"),
        Some("Bearer TEST-API-KEY-SECRET")
    );
    assert_eq!(request.header("accept"), Some("text/event-stream"));
    assert_eq!(
        request.header("user-agent"),
        Some(concat!("minicore-agent/", env!("CARGO_PKG_VERSION")))
    );
    let body = request.json_body();
    assert_eq!(body["model"], "provider-model");
    assert_eq!(body["stream"], true);
    assert_eq!(body["store"], false);
    assert_eq!(body["truncation"], "disabled");
    assert_eq!(body["max_output_tokens"], 1_024);
    assert_eq!(
        body["reasoning"],
        json!({"effort": "medium", "summary": "auto"})
    );
    assert_eq!(body["tools"][0]["type"], "function");
    assert_eq!(body["tools"][0]["name"], "read");
    assert_eq!(body["tools"][0]["description"], "Read a file");
    assert!(body["tools"][0]["parameters"].is_object());
    assert!(body["tools"][0].get("strict").is_none());
    let input = body["input"].as_array().unwrap();
    assert_eq!(input[0]["role"], "developer");
    assert_eq!(input[0]["content"][0]["type"], "input_text");
    assert_eq!(input[1]["role"], "user");
    assert_eq!(input[2]["role"], "assistant");
    assert_eq!(input[2]["content"][0]["type"], "output_text");
    assert_eq!(input[2]["id"], "msg_minicore_2_1");
    assert_eq!(input[2]["content"][0]["annotations"], json!([]));
    assert_eq!(input[3]["type"], "function_call");
    assert_eq!(input[3]["call_id"], "call-history");
    assert_eq!(input[3]["arguments"], r#"{"path":"src/lib.rs"}"#);
    assert_eq!(input[3]["status"], "completed");
    assert_eq!(input[4]["type"], "function_call_output");
    assert_eq!(input[4]["output"], "durable tool output");
    assert_eq!(input[4]["status"], "completed");
    assert!(
        !input
            .iter()
            .any(|item| item.get("type") == Some(&json!("reasoning"))),
        "reasoning history without an exact provider item identity must be omitted"
    );
    assert!(!String::from_utf8_lossy(request.body()).contains("TEST-API-KEY-SECRET"));
}

#[tokio::test]
async fn request_replay_call_ids_validate_before_any_http_request() {
    let valid_id = "v".repeat(64);
    let server = MockServer::spawn([MockResponse::sse(&[completed(usage())])]).await;
    run_model(
        &model(server.base_url()),
        tool_exchange_request(&valid_id),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let requests = server.finish().await;
    assert_eq!(requests.len(), 1);
    let input = requests[0].json_body()["input"].as_array().unwrap().clone();
    assert!(
        input
            .iter()
            .any(|item| { item["type"] == "function_call" && item["call_id"] == valid_id })
    );
    assert!(
        input
            .iter()
            .any(|item| { item["type"] == "function_call_output" && item["call_id"] == valid_id })
    );

    let invalid_id = "x".repeat(65);
    for (call_id, valid) in [(&valid_id, true), (&invalid_id, false)] {
        let call_id = ToolCallId::new(call_id).unwrap();
        let call = ToolCall::new(
            call_id.clone(),
            "read".parse().unwrap(),
            json!({"path": "input.txt"}),
            0,
        )
        .unwrap();
        let input = FunctionCallInput::from_runtime(&call);
        let output =
            FunctionCallOutputInput::from_runtime(&call_id, &ToolOutput::new("result").unwrap());
        if valid {
            assert_eq!(input.unwrap().call_id, valid_id);
            assert_eq!(output.unwrap().call_id, valid_id);
        } else {
            let input_error = match input {
                Err(error) => error,
                Ok(_) => panic!("invalid assistant ToolCall ID was accepted"),
            };
            let output_error = match output {
                Err(error) => error,
                Ok(_) => panic!("invalid Tool result ID was accepted"),
            };
            for error in [input_error, output_error] {
                assert_error(
                    &error,
                    ModelErrorKind::InvalidRequest,
                    DeliveryState::NotStarted,
                    false,
                );
            }
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let accepted = tokio::spawn(async move {
        match tokio::time::timeout(Duration::from_millis(200), listener.accept()).await {
            Ok(Ok((_stream, _))) => 1,
            Ok(Err(error)) => panic!("loopback accept failed: {error}"),
            Err(_) => 0,
        }
    });
    let error = start_error(
        &model(&base_url),
        tool_exchange_request(&invalid_id),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    let request_count = accepted.await.unwrap();
    assert_error(
        &error,
        ModelErrorKind::InvalidRequest,
        DeliveryState::NotStarted,
        false,
    );
    assert_eq!(request_count, 0);
}

#[test]
fn reasoning_mapping_preserves_structure_without_ordinary_estimate_veto() {
    let model = model("http://127.0.0.1:1");
    for (reasoning, expected) in [
        (ReasoningPreference::Auto, Value::Null),
        (ReasoningPreference::Disabled, json!({"effort": "none"})),
        (
            ReasoningPreference::Low,
            json!({"effort": "low", "summary": "auto"}),
        ),
        (
            ReasoningPreference::Medium,
            json!({"effort": "medium", "summary": "auto"}),
        ),
        (
            ReasoningPreference::High,
            json!({"effort": "high", "summary": "auto"}),
        ),
        (
            ReasoningPreference::XHigh,
            json!({"effort": "xhigh", "summary": "auto"}),
        ),
        (
            ReasoningPreference::Max,
            json!({"effort": "max", "summary": "auto"}),
        ),
        (
            ReasoningPreference::Ultra,
            json!({"effort": "ultra", "summary": "auto"}),
        ),
    ] {
        let body = model.build_request(&basic_request(reasoning)).unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        if reasoning == ReasoningPreference::Auto {
            assert!(body.get("reasoning").is_none());
        } else {
            assert_eq!(body["reasoning"], expected);
        }
    }

    let request = basic_request(ReasoningPreference::Auto);
    let encoded = model.build_request(&request).unwrap();
    let estimated = encoded.len().div_ceil(4) as u64;
    let mut exact = settings("http://127.0.0.1:1");
    exact.effective_context_window = estimated;
    assert!(
        OpenAiResponsesModel::new(exact)
            .unwrap()
            .build_request(&request)
            .is_ok()
    );
    let mut too_small = settings("http://127.0.0.1:1");
    too_small.effective_context_window = estimated - 1;
    assert_eq!(
        OpenAiResponsesModel::new(too_small)
            .unwrap()
            .build_request(&request)
            .unwrap(),
        encoded
    );

    for reasoning in [
        ReasoningPreference::High,
        ReasoningPreference::XHigh,
        ReasoningPreference::Max,
        ReasoningPreference::Ultra,
    ] {
        let mut unsupported = settings("http://127.0.0.1:1");
        unsupported.supported_reasoning = BTreeSet::from([ReasoningPreference::Auto]);
        let error = OpenAiResponsesModel::new(unsupported)
            .unwrap()
            .build_request(&basic_request(reasoning))
            .unwrap_err();
        assert_error(
            &error,
            ModelErrorKind::InvalidRequest,
            DeliveryState::NotStarted,
            false,
        );
    }
}

#[tokio::test]
async fn text_reasoning_refusal_and_finish_reasons_map_semantically() {
    let cases = [
        (
            vec![
                json!({"type": "response.reasoning_summary_text.delta", "delta": "think"}),
                json!({"type": "response.reasoning_text.delta", "delta": "detail"}),
                json!({"type": "response.output_text.delta", "delta": "answer"}),
                completed(usage()),
            ],
            ModelFinishReason::Stop,
            vec!["think", "detail", "answer"],
        ),
        (
            vec![
                json!({"type": "response.refusal.delta", "delta": "cannot comply"}),
                completed(usage()),
            ],
            ModelFinishReason::Refused,
            vec!["cannot comply"],
        ),
        (
            vec![
                json!({"type": "response.output_text.delta", "delta": "partial"}),
                incomplete("max_output_tokens", usage()),
            ],
            ModelFinishReason::Length,
            vec!["partial"],
        ),
        (
            vec![
                json!({"type": "response.output_text.delta", "delta": "filtered"}),
                incomplete("content_filter", usage()),
            ],
            ModelFinishReason::ContentFiltered,
            vec!["filtered"],
        ),
        (
            vec![
                json!({"type": "response.output_text.delta", "delta": "unknown"}),
                incomplete("provider_specific", usage()),
            ],
            ModelFinishReason::Unknown,
            vec!["unknown"],
        ),
    ];
    for (events, expected_finish, expected_text) in cases {
        let server = MockServer::spawn([MockResponse::sse(&events)]).await;
        let events = run_model(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
        let text = events.iter().filter_map(event_text).collect::<Vec<_>>();
        assert_eq!(text, expected_text);
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Finish { reason }) if *reason == expected_finish
        ));
        server.finish().await;
    }
}

#[tokio::test]
async fn tool_calls_support_split_done_only_and_multiple_without_duplicate_events() {
    let first = vec![
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "call-1", "name": "read", "arguments": ""}
        }),
        json!({"type": "response.function_call_arguments.delta", "output_index": 0, "delta": "{\"path\":"}),
        json!({"type": "response.function_call_arguments.delta", "output_index": 0, "delta": "\"a.txt\"}"}),
        json!({"type": "response.function_call_arguments.done", "output_index": 0, "arguments": "{\"path\":\"a.txt\"}"}),
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "call-1", "name": "read", "arguments": "{\"path\":\"a.txt\"}"}
        }),
        completed(usage()),
    ];
    let done_only = vec![
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "call-2", "name": "read", "arguments": "{\"path\":\"b.txt\"}"}
        }),
        completed(usage()),
    ];
    let multiple = vec![
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "call-3", "name": "read", "arguments": "{\"path\":\"c.txt\"}"}
        }),
        json!({
            "type": "response.output_item.done",
            "output_index": 1,
            "item": {"type": "function_call", "call_id": "call-4", "name": "read", "arguments": "{\"path\":\"d.txt\"}"}
        }),
        completed(usage()),
    ];
    for (events, expected_arguments) in [
        (first, vec![("call-1", r#"{"path":"a.txt"}"#)]),
        (done_only, vec![("call-2", r#"{"path":"b.txt"}"#)]),
        (
            multiple,
            vec![
                ("call-3", r#"{"path":"c.txt"}"#),
                ("call-4", r#"{"path":"d.txt"}"#),
            ],
        ),
    ] {
        let server = MockServer::spawn([MockResponse::sse(&events)]).await;
        let events = run_model(
            &model(server.base_url()),
            ModelRequest::new(
                vec![ModelMessage::user("read").unwrap()],
                vec![read_tool()],
                ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
                ReasoningPreference::Auto,
            )
            .unwrap(),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelEvent::ToolCallStart { .. }))
                .count(),
            expected_arguments.len()
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelEvent::ToolCallEnd { .. }))
                .count(),
            expected_arguments.len()
        );
        let mut arguments = BTreeMap::<String, String>::new();
        for event in &events {
            if let ModelEvent::ToolCallArgumentsDelta {
                tool_call_id,
                delta,
            } = event
            {
                arguments
                    .entry(tool_call_id.to_string())
                    .or_default()
                    .push_str(delta.as_str());
            }
        }
        assert_eq!(
            arguments,
            expected_arguments
                .into_iter()
                .map(|(id, value)| (id.to_owned(), value.to_owned()))
                .collect()
        );
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls
            })
        ));
        server.finish().await;
    }
}

#[tokio::test]
async fn openai_call_ids_are_limited_before_any_tool_event_is_queued() {
    let split_id = "s".repeat(64);
    let done_id = "d".repeat(64);
    let server = MockServer::spawn([MockResponse::sse(&[
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": split_id, "name": "read", "arguments": ""}
        }),
        json!({
            "type": "response.function_call_arguments.done",
            "output_index": 0,
            "arguments": "{}"
        }),
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": split_id, "name": "read", "arguments": "{}"}
        }),
        json!({
            "type": "response.output_item.done",
            "output_index": 1,
            "item": {"type": "function_call", "call_id": done_id, "name": "read", "arguments": "{}"}
        }),
        completed(usage()),
    ])])
    .await;
    let request = ModelRequest::new(
        vec![ModelMessage::user("read").unwrap()],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::Auto,
    )
    .unwrap();
    let events = run_model(
        &model(server.base_url()),
        request,
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ModelEvent::ToolCallStart { .. }))
            .count(),
        2
    );
    server.finish().await;

    for (length, done_only) in [(65, false), (65, true), (256, false), (256, true)] {
        let call_id = "x".repeat(length);
        let event = if done_only {
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": call_id, "name": "read", "arguments": "{}"}
            })
        } else {
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": call_id, "name": "read", "arguments": ""}
            })
        };
        let server = MockServer::spawn([MockResponse::sse(&[event])]).await;
        let request = ModelRequest::new(
            vec![ModelMessage::user("read").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::Auto,
        )
        .unwrap();
        let (events, error) = run_until_error(
            &model(server.base_url()),
            request,
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await;
        assert!(events.is_empty(), "invalid call ID queued a Tool event");
        assert_error(
            &error,
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Started,
            false,
        );
        server.finish().await;
    }
}

#[test]
fn openai_call_id_parser_enforces_all_lengths_and_runtime_grammar() {
    assert!(openai_tool_call_id(&"x".repeat(64)).is_ok());
    for length in 65..=256 {
        assert!(openai_tool_call_id(&"x".repeat(length)).is_err());
    }
    for value in ["", "bad id", "bad\"id", "bad\\id", "line\nbreak"] {
        assert!(openai_tool_call_id(value).is_err());
    }
}

#[tokio::test]
async fn usage_is_disjoint_and_preserves_cache_reasoning_and_provider_total() {
    let server = MockServer::spawn([MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "usage"}),
        completed(usage()),
    ])])
    .await;
    let events = run_model(
        &model(server.base_url()),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let usage = events
        .iter()
        .find_map(|event| match event {
            ModelEvent::Usage { usage } => Some(serde_json::to_value(usage).unwrap()),
            _ => None,
        })
        .unwrap();
    assert_eq!(usage["input_tokens"], 12);
    assert_eq!(usage["output_tokens"], 7);
    assert_eq!(usage["reasoning_tokens"], 4);
    assert_eq!(usage["cache_read_tokens"], 5);
    assert_eq!(usage["cache_write_tokens"], 3);
    assert_eq!(usage["provider_total_tokens"], 31);
    server.finish().await;

    let zero_boundary = json!({
        "input_tokens": 8,
        "input_tokens_details": {"cached_tokens": 5, "cache_write_tokens": 3},
        "output_tokens": 4,
        "output_tokens_details": {"reasoning_tokens": 4},
        "total_tokens": 12
    });
    let server = MockServer::spawn([MockResponse::sse(&[completed(zero_boundary)])]).await;
    let events = run_model(
        &model(server.base_url()),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let usage = events
        .iter()
        .find_map(|event| match event {
            ModelEvent::Usage { usage } => Some(serde_json::to_value(usage).unwrap()),
            _ => None,
        })
        .unwrap();
    assert_eq!(usage["input_tokens"], 0);
    assert_eq!(usage["output_tokens"], 0);
    assert_eq!(usage["reasoning_tokens"], 4);
    assert_eq!(usage["cache_read_tokens"], 5);
    assert_eq!(usage["cache_write_tokens"], 3);
    assert_eq!(usage["provider_total_tokens"], 12);
    server.finish().await;
}

#[tokio::test]
async fn malformed_usage_fails_the_terminal_without_usage_or_finish() {
    let cases = [
        json!({}),
        json!({"input_tokens": 1}),
        json!({"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}),
        json!({
            "input_tokens": 1,
            "output_tokens": 1,
            "total_tokens": 2,
            "input_tokens_details": {},
            "output_tokens_details": {"reasoning_tokens": 0}
        }),
        json!({
            "input_tokens": 1,
            "output_tokens": 1,
            "total_tokens": 2,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens_details": {}
        }),
        json!({
            "input_tokens": u64::MAX,
            "output_tokens": 0,
            "total_tokens": u64::MAX,
            "input_tokens_details": {"cached_tokens": u64::MAX, "cache_write_tokens": 1},
            "output_tokens_details": {"reasoning_tokens": 0}
        }),
        json!({
            "input_tokens": 5,
            "output_tokens": 0,
            "total_tokens": 5,
            "input_tokens_details": {"cached_tokens": 3, "cache_write_tokens": 3},
            "output_tokens_details": {"reasoning_tokens": 0}
        }),
        json!({
            "input_tokens": 0,
            "output_tokens": 3,
            "total_tokens": 3,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 4}
        }),
        json!({
            "input_tokens": u64::MAX,
            "output_tokens": 1,
            "total_tokens": u64::MAX,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 0}
        }),
        json!({
            "input_tokens": 2,
            "output_tokens": 3,
            "total_tokens": 4,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 0}
        }),
    ];
    for usage in cases {
        let server = MockServer::spawn([MockResponse::sse(&[completed(usage)])]).await;
        let (events, error) = run_until_error(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ModelEvent::Usage { .. } | ModelEvent::Finish { .. })),
            "malformed terminal emitted Usage or Finish"
        );
        assert_error(
            &error,
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Started,
            false,
        );
        server.finish().await;
    }
}

#[tokio::test]
async fn terminal_status_and_optional_usage_are_strict() {
    for event_type in ["response.completed", "response.incomplete"] {
        let valid_status = if event_type == "response.completed" {
            "completed"
        } else {
            "incomplete"
        };
        for status in [
            if valid_status == "completed" {
                "incomplete"
            } else {
                "completed"
            },
            "failed",
            "cancelled",
            "queued",
            "in_progress",
        ] {
            let response = json!({"status": status, "usage": usage()});
            let event = json!({"type": event_type, "response": response});
            let server = MockServer::spawn([MockResponse::sse(&[event])]).await;
            let (events, error) = run_until_error(
                &model(server.base_url()),
                basic_request(ReasoningPreference::Auto),
                context(CancellationToken::new(), Duration::from_secs(5)),
            )
            .await;
            assert!(!events.iter().any(|event| matches!(
                event,
                ModelEvent::Usage { .. } | ModelEvent::Finish { .. }
            )));
            assert_error(
                &error,
                ModelErrorKind::InvalidProviderResponse,
                DeliveryState::Started,
                false,
            );
            server.finish().await;
        }
    }

    for (event_type, explicit_null, expected_reason) in [
        ("response.completed", false, ModelFinishReason::Stop),
        ("response.completed", true, ModelFinishReason::Stop),
        ("response.incomplete", false, ModelFinishReason::Length),
        ("response.incomplete", true, ModelFinishReason::Length),
    ] {
        let mut response = json!({
            "usage": usage(),
            "incomplete_details": {"reason": "max_output_tokens"}
        });
        if explicit_null {
            response["status"] = Value::Null;
        }
        let event = json!({"type": event_type, "response": response});
        let server = MockServer::spawn([MockResponse::sse(&[event])]).await;
        let events = run_model(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::Usage { .. }))
        );
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Finish { reason }) if *reason == expected_reason
        ));
        server.finish().await;
    }

    for (event, expected_reason) in [
        (
            json!({
                "type": "response.completed",
                "response": {"status": "completed"}
            }),
            ModelFinishReason::Stop,
        ),
        (
            json!({
                "type": "response.incomplete",
                "response": {
                    "status": "incomplete",
                    "incomplete_details": {"reason": "max_output_tokens"}
                }
            }),
            ModelFinishReason::Length,
        ),
    ] {
        let server = MockServer::spawn([MockResponse::sse(&[event])]).await;
        let events = run_model(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ModelEvent::Usage { .. }))
        );
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Finish { reason }) if *reason == expected_reason
        ));
        server.finish().await;
    }
}

#[tokio::test]
async fn fragmented_multidata_crlf_and_cr_sse_frames_are_incremental() {
    let completed = completed(usage()).to_string();
    let body = format!(
        ":comment\r\ndata: {{\"type\":\r\ndata: \"response.output_text.delta\",\"delta\":\"fragmented\"}}\r\n\r\ndata: {completed}\r\r"
    );
    let bytes = body.into_bytes();
    let chunks = vec![
        bytes[..7].to_vec(),
        bytes[7..31].to_vec(),
        bytes[31..77].to_vec(),
        bytes[77..].to_vec(),
    ];
    let server = MockServer::spawn([MockResponse::sse_bytes(Vec::new()).with_chunks(chunks)]).await;
    let events = run_model(
        &model(server.base_url()),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event_text(event) == Some("fragmented"))
    );
    assert!(matches!(events.last(), Some(ModelEvent::Finish { .. })));
    server.finish().await;
}

#[tokio::test]
async fn large_provider_text_is_split_on_utf8_boundaries() {
    let text = format!("{}你", "a".repeat(MAX_EVENT_BYTES + 32));
    let server = MockServer::spawn([MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": text}),
        completed(usage()),
    ])])
    .await;
    let events = run_model(
        &model(server.base_url()),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let chunks = events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::TextDelta { delta } => Some(delta.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(chunks.len() >= 2);
    assert!(chunks.iter().all(|chunk| chunk.len() <= MAX_EVENT_BYTES));
    assert_eq!(chunks.concat(), text);
    server.finish().await;
}

#[tokio::test]
async fn unsafe_or_oversized_sse_and_invalid_tool_lifecycle_fail_closed() {
    let cases = vec![
        MockResponse::sse(&[json!({
            "type": "response.output_text.delta",
            "delta": "bad\u{1}text"
        })]),
        MockResponse::sse_bytes({
            let mut body = b"data: ".to_vec();
            body.extend(std::iter::repeat_n(b'x', MAX_SSE_LINE_BYTES + 1));
            body
        }),
        MockResponse::sse_bytes({
            let mut body = Vec::new();
            for _ in 0..1_025 {
                body.extend_from_slice(b"data: ");
                body.extend(std::iter::repeat_n(b'x', 1_024));
                body.push(b'\n');
            }
            body.push(b'\n');
            body
        }),
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "large", "name": "read", "arguments": ""}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": "x".repeat(MAX_JSON_BYTES + 1)
            }),
        ]),
        MockResponse::sse(&[json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "invalid-name", "name": "bad name", "arguments": "{}"}
        })]),
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "duplicate", "name": "read", "arguments": "{}"}
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {"type": "function_call", "call_id": "duplicate", "name": "read", "arguments": "{}"}
            }),
        ]),
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "open", "name": "read", "arguments": "{}"}
            }),
            completed(usage()),
        ]),
    ];
    for response in cases {
        let server = MockServer::spawn([response]).await;
        let request = ModelRequest::new(
            vec![ModelMessage::user("read").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::Auto,
        )
        .unwrap();
        let error = run_model(
            &model(server.base_url()),
            request,
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.kind(),
            ModelErrorKind::InvalidProviderResponse | ModelErrorKind::StreamInterrupted
        ));
        assert_ne!(error.delivery(), DeliveryState::NotStarted);
        server.finish().await;
    }
}

#[tokio::test]
async fn provider_event_evidence_controls_stream_error_delivery() {
    let no_event = MockResponse::sse_bytes(Vec::new());
    let after_event_body = sse_body(&[json!({
        "type": "response.output_text.delta",
        "delta": "partial"
    })]);
    let after_event = MockResponse::sse_bytes(after_event_body.clone().into_bytes());
    let transport_before = MockResponse::sse_bytes(Vec::new()).with_declared_length(10);
    let transport_after = MockResponse::sse_bytes(after_event_body.clone().into_bytes())
        .with_declared_length(after_event_body.len() + 10);
    let created_body = sse_body(&[json!({"type": "response.created"})]);
    let unknown_body = sse_body(&[json!({"type": "response.future_event"})]);
    let created_eof = MockResponse::sse_bytes(created_body.clone().into_bytes());
    let created_transport = MockResponse::sse_bytes(created_body.clone().into_bytes())
        .with_declared_length(created_body.len() + 10);
    let unknown_eof = MockResponse::sse_bytes(unknown_body.into_bytes());
    let malformed_after_created =
        MockResponse::sse_bytes(format!("{created_body}data: {{not-json}}\n\n").into_bytes());
    let malformed_typed_event = MockResponse::sse(&[json!({"type": "response.output_text.delta"})]);
    let malformed_untyped_object = MockResponse::sse(&[json!({"delta": "missing type"})]);
    let created_then_parser_overflow = MockResponse::sse_bytes({
        let mut body = created_body.clone().into_bytes();
        body.extend_from_slice(b"data: ");
        body.extend(std::iter::repeat_n(b'x', MAX_SSE_LINE_BYTES + 1));
        body
    });
    let done_only = MockResponse::sse_bytes(b"data: [DONE]\n\n".to_vec());
    let cases = [
        (
            no_event,
            ModelErrorKind::IncompleteResponse,
            DeliveryState::Unknown,
        ),
        (
            after_event,
            ModelErrorKind::StreamInterrupted,
            DeliveryState::Started,
        ),
        (
            transport_before,
            ModelErrorKind::RequestOutcomeUnknown,
            DeliveryState::Unknown,
        ),
        (
            transport_after,
            ModelErrorKind::StreamInterrupted,
            DeliveryState::Started,
        ),
        (
            created_eof,
            ModelErrorKind::IncompleteResponse,
            DeliveryState::Started,
        ),
        (
            created_transport,
            ModelErrorKind::RequestOutcomeUnknown,
            DeliveryState::Started,
        ),
        (
            unknown_eof,
            ModelErrorKind::IncompleteResponse,
            DeliveryState::Started,
        ),
        (
            malformed_after_created,
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Started,
        ),
        (
            malformed_typed_event,
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Started,
        ),
        (
            malformed_untyped_object,
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Unknown,
        ),
        (
            created_then_parser_overflow,
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Started,
        ),
        (
            done_only,
            ModelErrorKind::IncompleteResponse,
            DeliveryState::Unknown,
        ),
    ];
    for (response, kind, delivery) in cases {
        let server = MockServer::spawn([response]).await;
        let error = run_model(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap_err();
        assert_error(&error, kind, delivery, false);
        server.finish().await;
    }
}

#[tokio::test]
async fn created_event_makes_stream_cancellation_and_timeout_started() {
    for timeout in [false, true] {
        let cancellation = CancellationToken::new();
        let deadline = if timeout {
            TokioInstant::now()
        } else {
            TokioInstant::now() + Duration::from_secs(5)
        };
        let bytes: ByteStream = Box::pin(futures_util::stream::pending());
        let mut state = StreamState::new(bytes, cancellation.clone(), deadline);
        handle_frame(&mut state, br#"{"type":"response.in_progress"}"#).unwrap();
        if !timeout {
            cancellation.cancel();
        }
        let (result, _) = next_stream_event(state).await.unwrap();
        let error = result.unwrap_err();
        assert_error(
            &error,
            if timeout {
                ModelErrorKind::Timeout
            } else {
                ModelErrorKind::Cancelled
            },
            DeliveryState::Started,
            false,
        );
    }
}

#[test]
fn retry_targets_subtract_body_time_and_reject_clock_anomalies() {
    fn headers(values: &[(&str, &str)]) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in values {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                reqwest::header::HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    let received_monotonic = Instant::now();
    let received_system = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let current_monotonic = received_monotonic + Duration::from_secs(10);
    let current_system = received_system + Duration::from_secs(10);
    let remaining = |values: &[(&str, &str)]| {
        retry_target(&headers(values), received_monotonic, received_system)
            .and_then(|target| target.remaining(current_monotonic, current_system))
    };

    assert_eq!(
        remaining(&[("retry-after", "35")]),
        Some(Duration::from_secs(25))
    );
    assert_eq!(remaining(&[("retry-after", "5")]), None);
    assert_eq!(
        remaining(&[("retry-after-ms", "35000"), ("retry-after", "90")]),
        Some(Duration::from_secs(25))
    );
    assert_eq!(
        remaining(&[("retry-after-ms", "35500")]),
        Some(Duration::from_millis(25_500))
    );
    assert_eq!(
        remaining(&[("retry-after-ms", "bad"), ("retry-after", "35.5")]),
        Some(Duration::from_millis(25_500))
    );
    let date = httpdate::fmt_http_date(received_system + Duration::from_secs(35));
    assert_eq!(
        remaining(&[("retry-after", &date)]),
        Some(Duration::from_secs(25))
    );

    for values in [
        vec![("retry-after-ms", "-1")],
        vec![("retry-after", "bad")],
        vec![("retry-after", "0")],
        vec![("retry-after", "-1")],
        vec![("retry-after", "NaN")],
        vec![("retry-after", "inf")],
        vec![("retry-after", "1e300")],
    ] {
        assert!(retry_target(&headers(&values), received_monotonic, received_system,).is_none());
    }

    let monotonic_target = retry_target(
        &headers(&[("retry-after", "35")]),
        received_monotonic,
        received_system,
    )
    .unwrap();
    assert_eq!(
        monotonic_target.remaining(
            received_monotonic
                .checked_sub(Duration::from_secs(1))
                .unwrap(),
            current_system,
        ),
        None
    );
    let date_target = retry_target(
        &headers(&[("retry-after", &date)]),
        received_monotonic,
        received_system,
    )
    .unwrap();
    assert_eq!(
        date_target.remaining(
            current_monotonic,
            received_system.checked_sub(Duration::from_secs(1)).unwrap(),
        ),
        None
    );
}

#[tokio::test]
async fn http_status_connect_timeout_and_non_sse_errors_have_conservative_delivery() {
    let unavailable_model = model("http://127.0.0.1:0");
    let error = start_error(
        &unavailable_model,
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    assert_error(
        &error,
        ModelErrorKind::TransportUnavailable,
        DeliveryState::NotStarted,
        true,
    );

    let cases = [
        (
            MockResponse::json(400, r#"{"error":{"code":"invalid_request"}}"#),
            ModelErrorKind::InvalidRequest,
            DeliveryState::NotStarted,
            false,
        ),
        (
            MockResponse::json(
                400,
                r#"{"error":{"code":"context_length_exceeded","message":"SECRET"}}"#,
            ),
            ModelErrorKind::ContextOverflow,
            DeliveryState::NotStarted,
            false,
        ),
        (
            MockResponse::json(401, r#"{"error":{"message":"SECRET"}}"#),
            ModelErrorKind::AuthRejected,
            DeliveryState::NotStarted,
            false,
        ),
        (
            MockResponse::json(403, r#"{"error":{"message":"SECRET"}}"#),
            ModelErrorKind::AuthRejected,
            DeliveryState::NotStarted,
            false,
        ),
        (
            MockResponse::json(413, r#"{"error":{"message":"SECRET"}}"#),
            ModelErrorKind::InvalidRequest,
            DeliveryState::NotStarted,
            false,
        ),
        (
            MockResponse::json(
                422,
                r#"{"error":{"type":"context_window_exceeded","message":"SECRET"}}"#,
            ),
            ModelErrorKind::ContextOverflow,
            DeliveryState::NotStarted,
            false,
        ),
        (
            MockResponse::json(408, r#"{"error":{"message":"SECRET"}}"#),
            ModelErrorKind::Timeout,
            DeliveryState::Unknown,
            false,
        ),
        (
            MockResponse::json(429, r#"{"error":{"message":"SECRET"}}"#)
                .with_header("Retry-After", "7"),
            ModelErrorKind::RateLimited,
            DeliveryState::NotStarted,
            true,
        ),
        (
            MockResponse::json(500, r#"{"error":{"message":"SECRET"}}"#),
            ModelErrorKind::ProviderUnavailable,
            DeliveryState::Unknown,
            false,
        ),
        (
            MockResponse::json(200, b"{}".to_vec()),
            ModelErrorKind::InvalidProviderResponse,
            DeliveryState::Unknown,
            false,
        ),
    ];
    for (response, kind, delivery, retryable) in cases {
        let server = MockServer::spawn([response]).await;
        let model = model(server.base_url());
        let error = start_error(
            &model,
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await;
        assert_error(&error, kind, delivery, retryable);
        if kind == ModelErrorKind::RateLimited {
            let delay = match error.retry_hint() {
                RetryHint::Retryable {
                    retry_after: Some(delay),
                } => *delay,
                _ => panic!("rate limit retry delay is missing"),
            };
            assert!(delay <= Duration::from_secs(7));
            assert!(delay >= Duration::from_millis(6_900));
        }
        server.finish().await;
    }

    let server = MockServer::spawn([
        MockResponse::sse(&[completed(usage())]).with_header_delay(Duration::from_millis(100))
    ])
    .await;
    let mut settings = settings(server.base_url());
    settings.request_timeout = Some(Duration::from_millis(20));
    let timeout_model = OpenAiResponsesModel::new(settings).unwrap();
    let error = start_error(
        &timeout_model,
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    assert_error(
        &error,
        ModelErrorKind::Timeout,
        DeliveryState::Unknown,
        false,
    );
    server.finish().await;
}

#[tokio::test]
async fn rate_limits_distinguish_quota_and_parse_retry_after_strictly() {
    let quota_codes = [
        "insufficient_quota",
        "quota_exceeded",
        "credit_balance_exhausted",
        "billing_hard_limit_reached",
        "usage_limit_reached",
        "organization_quota_exceeded",
        "project_quota_exceeded",
        "organization_usage_limit_exceeded",
        "project_usage_limit_exceeded",
        "spend_limit_reached",
        "spend_limit_exceeded",
        "organization_spend_limit_reached",
        "organization_spend_limit_exceeded",
        "project_spend_limit_reached",
        "project_spend_limit_exceeded",
    ];
    for (index, code) in quota_codes.into_iter().enumerate() {
        let body = if index % 2 == 0 {
            json!({"error": {"code": code}})
        } else {
            json!({"error": {"type": code}})
        };
        let server = MockServer::spawn([
            MockResponse::json(429, body.to_string()).with_header("retry-after-ms", "1500")
        ])
        .await;
        let error = start_error(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await;
        assert_error(
            &error,
            ModelErrorKind::QuotaExceeded,
            DeliveryState::NotStarted,
            false,
        );
        assert_eq!(error.retry_hint(), &RetryHint::Never);
        server.finish().await;
    }

    let future = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(120));
    let cases = [
        (
            MockResponse::json(429, r#"{"error":{"code":"rate_limit_exceeded"}}"#)
                .with_header("retry-after-ms", "1500")
                .with_header("Retry-After", "9"),
            Some(Duration::from_millis(1_500)),
        ),
        (
            MockResponse::json(429, "{}")
                .with_header("retry-after-ms", "bad")
                .with_header("Retry-After", "1.25"),
            Some(Duration::from_millis(1_250)),
        ),
        (
            MockResponse::json(429, "{}").with_header("Retry-After", &future),
            None,
        ),
        (
            MockResponse::json(429, "{}").with_header("retry-after-ms", "-1"),
            None,
        ),
        (
            MockResponse::json(429, "{}").with_header("Retry-After", "NaN"),
            None,
        ),
        (
            MockResponse::json(429, "{}").with_header("Retry-After", "1e999"),
            None,
        ),
    ];
    for (index, (response, expected)) in cases.into_iter().enumerate() {
        let server = MockServer::spawn([response]).await;
        let error = start_error(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await;
        assert_error(
            &error,
            ModelErrorKind::RateLimited,
            DeliveryState::NotStarted,
            true,
        );
        let retry_after = match error.retry_hint() {
            RetryHint::Retryable { retry_after } => *retry_after,
            RetryHint::Never => panic!("rate limit must be retryable"),
        };
        if index == 2 {
            let retry_after = retry_after.expect("future HTTP date must produce a delay");
            assert!(retry_after >= Duration::from_secs(118));
            assert!(retry_after <= Duration::from_secs(120));
        } else if let Some(expected) = expected {
            let retry_after = retry_after.expect("numeric retry delay must be present");
            assert!(retry_after <= expected);
            assert!(retry_after >= expected.saturating_sub(Duration::from_millis(100)));
        } else {
            assert_eq!(retry_after, None);
        }
        server.finish().await;
    }
}

#[tokio::test]
async fn cancellation_before_send_and_during_stream_drop_owned_http_work() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled_model = model("http://127.0.0.1:1");
    let error = start_error(
        &cancelled_model,
        basic_request(ReasoningPreference::Auto),
        context(cancellation, Duration::from_secs(5)),
    )
    .await;
    assert_error(
        &error,
        ModelErrorKind::Cancelled,
        DeliveryState::NotStarted,
        false,
    );

    let server = MockServer::spawn([
        MockResponse::sse(&[completed(usage())]).with_header_delay(Duration::from_millis(100))
    ])
    .await;
    let cancellation = CancellationToken::new();
    let request_cancellation = cancellation.clone();
    let send_model = Arc::new(model(server.base_url()));
    let task_model = Arc::clone(&send_model);
    let send = tokio::spawn(async move {
        task_model
            .start(
                basic_request(ReasoningPreference::Auto),
                context(request_cancellation, Duration::from_secs(5)),
            )
            .await
    });
    server.wait_for_requests(1).await;
    cancellation.cancel();
    let error = match send.await.unwrap() {
        Ok(_) => panic!("cancelled send unexpectedly started a stream"),
        Err(error) => error,
    };
    assert_error(
        &error,
        ModelErrorKind::Cancelled,
        DeliveryState::Unknown,
        false,
    );
    server.finish().await;

    let delayed_terminal = sse_body(&[completed(usage())]).into_bytes();
    let server = MockServer::spawn([MockResponse::sse_bytes(Vec::new())
        .with_chunks([Vec::new(), delayed_terminal])
        .with_chunk_delay(Duration::from_millis(100))])
    .await;
    let cancellation = CancellationToken::new();
    let mut stream = model(server.base_url())
        .start(
            basic_request(ReasoningPreference::Auto),
            context(cancellation.clone(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    cancellation.cancel();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_error(
        &error,
        ModelErrorKind::Cancelled,
        DeliveryState::Unknown,
        false,
    );
    assert!(stream.next().await.is_none());
    server.finish().await;

    let first = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"seen\"}\n\n".to_vec();
    let second = sse_body(&[completed(usage())]).into_bytes();
    let server = MockServer::spawn([MockResponse::sse_bytes(Vec::new())
        .with_chunks([first, second])
        .with_chunk_delay(Duration::from_millis(100))])
    .await;
    let cancellation = CancellationToken::new();
    let mut stream = model(server.base_url())
        .start(
            basic_request(ReasoningPreference::Auto),
            context(cancellation.clone(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Ok(ModelEvent::TextDelta { .. }))
    ));
    cancellation.cancel();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_error(
        &error,
        ModelErrorKind::Cancelled,
        DeliveryState::Started,
        false,
    );
    assert!(stream.next().await.is_none());
    server.finish().await;
}

#[tokio::test]
async fn provider_failure_event_is_started_and_never_exposes_raw_details() {
    for event in [
        json!({"type": "response.failed", "response": {
            "error": {"code": "server_error", "message": "RAW-SECRET"}
        }}),
        json!({"type": "error", "code": "server_error", "message": "RAW-SECRET"}),
    ] {
        let server = MockServer::spawn([MockResponse::sse(&[event])]).await;
        let error = run_model(
            &model(server.base_url()),
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap_err();
        assert_error(
            &error,
            ModelErrorKind::ProviderUnavailable,
            DeliveryState::Started,
            false,
        );
        assert!(!format!("{error:?}").contains("RAW-SECRET"));
        server.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_agent_loop_uses_mock_openai_then_read_tool_then_final_model() {
    let first = MockResponse::sse(&[
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "read-call", "name": "read", "arguments": ""}
        }),
        json!({"type": "response.function_call_arguments.delta", "output_index": 0, "delta": "{\"path\":\"input.txt\"}"}),
        json!({"type": "response.function_call_arguments.done", "output_index": 0, "arguments": "{\"path\":\"input.txt\"}"}),
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {"type": "function_call", "call_id": "read-call", "name": "read", "arguments": "{\"path\":\"input.txt\"}"}
        }),
        completed(usage()),
    ]);
    let second = MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "read complete"}),
        completed(usage()),
    ]);
    let server = MockServer::spawn([first, second]).await;
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-openai-loop-{}",
        crate::ids::SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::write(workspace.join("input.txt"), "REAL-READ-CONTENT")
        .await
        .unwrap();
    let model: Arc<dyn Model> = Arc::new(model(server.base_url()));
    let models = Models::from_values(BTreeMap::from([("main".to_owned(), model)]));
    let config = AgentConfig {
        data_dir: base.join("data"),
        event_capacity: 256,
        default_profile: "test".to_owned(),
        profiles: BTreeMap::from([(
            "test".to_owned(),
            Profile {
                model: "main".to_owned(),
                reasoning: ReasoningPreference::Auto,
                system_prompt: "Use the read tool.".to_owned(),
                tools: vec!["read".to_owned()],
                max_tool_rounds: 4,
                approval: ApprovalMode::Auto,
            },
        )]),
        models: BTreeMap::from([("main".to_owned(), agent_model_config(server.base_url()))]),
        loop_options: LoopOverrides::default(),
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
    };
    let mut agent = Agent::open_with_models(config, models).await.unwrap();
    let session = agent
        .create_session(CreateSession {
            workspace: workspace.clone(),
            profile: String::new(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: session.session_id,
            text: "read input".to_owned(),
        })
        .await
        .unwrap();
    let result = agent.wait_turn(turn).await.unwrap();
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let history = agent
        .history(GetHistory {
            session_id: session.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let serialized = serde_json::to_string(&history).unwrap();
    assert!(serialized.contains("REAL-READ-CONTENT"));
    assert!(serialized.contains("read complete"));
    agent.shutdown().await.unwrap();
    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].json_body()["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "function_call_output")
    );
    let _ = tokio::fs::remove_dir_all(base).await;
}

struct LiveOpenAiConfig {
    api_key: String,
    provider_model: String,
    base_url: String,
    reasoning: ReasoningPreference,
}

fn required_live_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) | Err(std::env::VarError::NotPresent) => {
            panic!("required live environment variable is missing or empty")
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("required live environment variable must be valid Unicode")
        }
    }
}

fn live_config() -> LiveOpenAiConfig {
    let reasoning = match std::env::var("MINICORE_AGENT_LIVE_REASONING") {
        Err(std::env::VarError::NotPresent) => ReasoningPreference::Medium,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("optional live reasoning environment variable must be valid Unicode")
        }
        Ok(value) => match value.as_str() {
            "medium" => ReasoningPreference::Medium,
            "auto" => ReasoningPreference::Auto,
            "disabled" => ReasoningPreference::Disabled,
            "low" => ReasoningPreference::Low,
            "high" => ReasoningPreference::High,
            "xhigh" => ReasoningPreference::XHigh,
            "max" => ReasoningPreference::Max,
            "ultra" => ReasoningPreference::Ultra,
            _ => {
                panic!(
                    "MINICORE_AGENT_LIVE_REASONING must be auto, disabled, low, medium, high, xhigh, max, or ultra"
                )
            }
        },
    };
    let base_url = match std::env::var("MINICORE_AGENT_LIVE_BASE_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "https://api.openai.com/v1".to_owned(),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("optional live base URL environment variable must be valid Unicode")
        }
    };
    assert!(
        !base_url.trim().is_empty(),
        "MINICORE_AGENT_LIVE_BASE_URL must be non-empty when set"
    );
    LiveOpenAiConfig {
        api_key: required_live_env("OPENAI_API_KEY"),
        provider_model: required_live_env("MINICORE_AGENT_LIVE_MODEL"),
        base_url,
        reasoning,
    }
}

fn live_reasoning_tool_config() -> LiveOpenAiConfig {
    let config = live_config();
    assert!(
        matches!(
            config.reasoning,
            ReasoningPreference::Low
                | ReasoningPreference::Medium
                | ReasoningPreference::High
                | ReasoningPreference::XHigh
                | ReasoningPreference::Max
                | ReasoningPreference::Ultra
        ),
        "reasoning Tool smoke requires low, medium, high, xhigh, max, or ultra reasoning"
    );
    config
}

fn live_settings(config: &LiveOpenAiConfig) -> OpenAiResponsesSettings {
    let mut value = settings(&config.base_url);
    value.provider_model.clone_from(&config.provider_model);
    value.api_key.clone_from(&config.api_key);
    value.supported_reasoning = BTreeSet::from([config.reasoning]);
    value.request_timeout = Some(Duration::from_secs(120));
    value
}

fn live_model_config(config: &LiveOpenAiConfig) -> ModelConfig {
    ModelConfig::OpenAiResponses {
        model: config.provider_model.clone(),
        base_url: config.base_url.clone(),
        api_key_env: "OPENAI_API_KEY".to_owned(),
        physical_context_window: 18_408,
        output_budget_tokens: 1_024,
        safety_margin_tokens: 1_000,
        supported_reasoning: BTreeSet::from([config.reasoning]),
        supports_tools: true,
        request_timeout_seconds: Some(120),
    }
}

fn assert_live_usage(usage: &Usage) {
    let partitions = [
        usage.input_tokens(),
        usage.output_tokens(),
        usage.reasoning_tokens(),
        usage.cache_read_tokens(),
        usage.cache_write_tokens(),
    ];
    let all_present = partitions.iter().all(Option::is_some);
    let known_sum = partitions
        .into_iter()
        .flatten()
        .try_fold(0_u64, |sum, value| sum.checked_add(value));
    let known_sum = known_sum.expect("live Usage partitions must not overflow");
    if let Some(total) = usage.provider_total_tokens() {
        assert!(known_sum <= total);
        if all_present {
            assert_eq!(known_sum, total);
        }
    }
}

const LIVE_SMOKE_TOKEN: &str = "MINICORE_TUI_SMOKE_7F42";

struct LiveTempDir {
    path: PathBuf,
}

impl LiveTempDir {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "minicore-agent-openai-live-{}",
                crate::ids::SessionId::new().expect("live temp directory ID")
            )),
        }
    }
}

impl Drop for LiveTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct LiveToolEvidence {
    turn: TurnRef,
    outcome: minicore_runtime::LoopOutcome,
    report_usage: Usage,
    session_reasoning: ReasoningPreference,
    assistant_rounds: usize,
    assistant_usages: Vec<Usage>,
    saw_read_call: bool,
    successful_read_with_token: bool,
    final_text_with_token: bool,
}

async fn run_live_reasoning_tool(
    agent: &mut Agent,
    workspace: &Path,
) -> Result<LiveToolEvidence, &'static str> {
    let session = agent
        .create_session(CreateSession {
            workspace: workspace.to_owned(),
            profile: String::new(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .map_err(|_| "live Session creation failed")?;
    let turn = agent
        .send(SendMessage {
            session_id: session.session_id,
            text: "Use the read tool to read SMOKE.txt.\nAfter reading it, reply with the exact token."
                .to_owned(),
        })
        .await
        .map_err(|_| "live Turn send failed")?;
    let result = tokio::time::timeout(Duration::from_secs(300), agent.wait_turn(turn))
        .await
        .map_err(|_| "live reasoning/tool Turn timed out")?
        .map_err(|_| "live reasoning/tool Turn wait failed")?;
    let report = &result.report;
    let page = agent
        .history(GetHistory {
            session_id: session.session_id,
            offset: 0,
            limit: 32,
        })
        .map_err(|_| "live History read failed")?;

    let mut assistant_rounds = 0;
    let mut assistant_usages = Vec::new();
    let mut saw_read_call = false;
    let mut successful_read_with_token = false;
    let mut final_text_with_token = false;
    for item in &page.items {
        match &item.item {
            HistoryItemView::Assistant(view) => {
                assistant_rounds += 1;
                assistant_usages.push(view.usage);
                saw_read_call |= view.tool_calls.iter().any(|call| call.name == "read");
                if view.tool_calls.is_empty() && view.text.contains(LIVE_SMOKE_TOKEN) {
                    final_text_with_token = true;
                }
            }
            HistoryItemView::ToolResult(view)
                if view.tool_name == "read"
                    && view.outcome == ToolResultOutcome::Success
                    && view.content.contains(LIVE_SMOKE_TOKEN) =>
            {
                successful_read_with_token = true;
            }
            _ => {}
        }
    }
    Ok(LiveToolEvidence {
        turn,
        outcome: report.outcome.clone(),
        report_usage: report.usage,
        session_reasoning: session.reasoning,
        assistant_rounds,
        assistant_usages,
        saw_read_call,
        successful_read_with_token,
        final_text_with_token,
    })
}

#[ignore = "requires OPENAI_API_KEY and MINICORE_AGENT_LIVE_MODEL"]
#[tokio::test]
async fn openai_live_text_smoke() {
    let config = live_config();
    let events = run_model(
        &OpenAiResponsesModel::new(live_settings(&config))
            .expect("live OpenAI settings must be valid"),
        basic_request(config.reasoning),
        context(CancellationToken::new(), Duration::from_secs(120)),
    )
    .await
    .unwrap_or_else(|_| panic!("live text request failed; requested reasoning is not downgraded"));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelEvent::TextDelta { .. }))
    );
    for event in &events {
        if let ModelEvent::Usage { usage } = event {
            assert_live_usage(usage);
        }
    }
    assert!(matches!(
        events.last(),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::Stop
        })
    ));
}

#[ignore = "requires OPENAI_API_KEY and MINICORE_AGENT_LIVE_MODEL"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openai_live_reasoning_tool_smoke() {
    let live = live_reasoning_tool_config();
    let temp = LiveTempDir::new();
    let workspace = temp.path.join("workspace");
    tokio::fs::create_dir_all(&workspace)
        .await
        .unwrap_or_else(|_| panic!("live workspace creation failed"));
    tokio::fs::write(workspace.join("SMOKE.txt"), LIVE_SMOKE_TOKEN)
        .await
        .unwrap_or_else(|_| panic!("live smoke file creation failed"));
    let config = AgentConfig {
        data_dir: temp.path.join("data"),
        event_capacity: 256,
        default_profile: "live".to_owned(),
        profiles: BTreeMap::from([(
            "live".to_owned(),
            Profile {
                model: "main".to_owned(),
                reasoning: live.reasoning,
                system_prompt: "Use read when requested; never guess file contents.".to_owned(),
                tools: vec!["read".to_owned()],
                max_tool_rounds: 4,
                approval: ApprovalMode::Auto,
            },
        )]),
        models: BTreeMap::from([("main".to_owned(), live_model_config(&live))]),
        loop_options: LoopOverrides::default(),
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
    };
    let mut agent = Agent::open(config)
        .await
        .unwrap_or_else(|_| panic!("live Agent setup failed without exposing credentials"));
    let mut events = match agent.take_events() {
        Ok(events) => events,
        Err(_) => {
            let shutdown = AssertUnwindSafe(agent.shutdown()).catch_unwind().await;
            let _ = tokio::fs::remove_dir_all(&temp.path).await;
            match shutdown {
                Err(payload) => resume_unwind(payload),
                Ok(Err(_)) => panic!("live Agent shutdown failed"),
                Ok(Ok(())) => panic!("live Agent event stream setup failed"),
            }
        }
    };
    let collector = tokio::spawn(async move {
        let mut reasoning_by_turn = HashMap::<TurnRef, usize>::new();
        while let Some(event) = events.recv().await {
            if let AgentEvent::OutputDelta {
                turn,
                channel: OutputChannel::Reasoning,
                delta,
                ..
            } = event
            {
                if !delta.is_empty() {
                    *reasoning_by_turn.entry(turn).or_default() += 1;
                }
            }
        }
        reasoning_by_turn
    });
    let execution = AssertUnwindSafe(run_live_reasoning_tool(&mut agent, &workspace))
        .catch_unwind()
        .await;
    let shutdown = AssertUnwindSafe(agent.shutdown()).catch_unwind().await;
    let reasoning_by_turn = collector.await;
    let _ = tokio::fs::remove_dir_all(&temp.path).await;

    let evidence = match execution {
        Ok(Ok(evidence)) => evidence,
        Ok(Err(message)) => panic!("{message}"),
        Err(payload) => resume_unwind(payload),
    };
    match shutdown {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("live Agent shutdown failed"),
        Err(payload) => resume_unwind(payload),
    }
    let reasoning_by_turn = reasoning_by_turn.expect("live event collector panicked");
    assert_eq!(evidence.session_reasoning, live.reasoning);
    assert_eq!(
        evidence.outcome,
        minicore_runtime::LoopOutcome::Completed,
        "live reasoning/tool Turn failed; requested reasoning is not downgraded"
    );
    assert_live_usage(&evidence.report_usage);
    for usage in &evidence.assistant_usages {
        assert_live_usage(usage);
    }
    assert!(
        evidence.assistant_rounds >= 2,
        "expected a second live Model round"
    );
    assert!(evidence.saw_read_call, "expected a live read ToolCall");
    assert!(
        evidence.successful_read_with_token,
        "expected a successful live read ToolResult containing the token"
    );
    assert!(
        evidence.final_text_with_token,
        "expected a final live answer containing the token"
    );
    assert!(
        reasoning_by_turn
            .get(&evidence.turn)
            .is_some_and(|count| *count > 0),
        "expected a non-empty reasoning OutputDelta for the exact live Turn"
    );
}

// ============================================================================
// 0.2.4 reasoning summary part boundaries. A single newline is inserted ONLY
// between actual nonempty reasoning parts/items (summary_index/item boundary
// or explicit reasoning item output_item.added/done), never per delta and
// never via text heuristics. Legacy deltas without any boundary stay concat.
// ============================================================================

async fn reasoning_text_of(events: Vec<Value>) -> String {
    let server = MockServer::spawn([MockResponse::sse(&events)]).await;
    let model = model(server.base_url());
    let request = ModelRequest::new(
        vec![ModelMessage::user("reason").unwrap()],
        vec![],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let events = run_model(
        &model,
        request,
        context_for_request(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::ReasoningDelta { delta } => Some(delta.as_str()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn reasoning_two_summary_indexes_in_one_item_separate_with_single_newline() {
    let text = reasoning_text_of(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_x","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_x","output_index":0,"summary_index":0,"delta":"Plan"}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_x","output_index":0,"summary_index":0,"delta":"ning ... caveats"}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_x","output_index":0,"summary_index":1,"delta":"Detailing ... timeline"}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_x","output_index":0,"summary_index":2,"delta":"Analyzing ..."}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_x","status":"completed","summary":[{"type":"summary_text","text":"x"}],"provider":{"name":"loopback","trace_id":"t"}}}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "Planning ... caveats\nDetailing ... timeline\nAnalyzing ...",
        "same-item summary_index parts: fragments concat, parts separated by one newline"
    );
}

#[tokio::test]
async fn reasoning_item_id_change_with_index_reset_still_separates() {
    let text = reasoning_text_of(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_a","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_a","output_index":0,"summary_index":0,"delta":"First"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_a","status":"completed","summary":[{"type":"summary_text","text":"a"}]}}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"reasoning","id":"rs_b","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_b","output_index":1,"summary_index":0,"delta":"Second"}),
        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"reasoning","id":"rs_b","status":"completed","summary":[{"type":"summary_text","text":"b"}]}}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "First\nSecond",
        "item_id change with summary_index reset to 0 is still a boundary"
    );
}

#[tokio::test]
async fn reasoning_item_lifecycle_separates_when_deltas_carry_no_index() {
    let text = reasoning_text_of(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_a","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"Plan"}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"ning"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_a","status":"completed","summary":[{"type":"summary_text","text":"a"}]}}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"reasoning","id":"rs_b","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"Detail"}),
        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"reasoning","id":"rs_b","status":"completed","summary":[{"type":"summary_text","text":"b"}]}}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "Planning\nDetail",
        "explicit reasoning item added/done boundaries separate parts without an index"
    );
}

#[tokio::test]
async fn reasoning_split_and_empty_deltas_do_not_add_lines() {
    let text = reasoning_text_of(vec![
        json!({"type":"response.reasoning_summary_text.delta","summary_index":0,"delta":"Foo"}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":0,"delta":""}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":0,"delta":"bar"}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":1,"delta":"Baz"}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "Foobar\nBaz",
        "split fragments concat, empty deltas add no line, one newline between parts"
    );
}

#[tokio::test]
async fn reasoning_existing_newlines_are_never_doubled_at_boundaries() {
    let text = reasoning_text_of(vec![
        json!({"type":"response.reasoning_summary_text.delta","summary_index":0,"delta":"Alpha\n"}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":1,"delta":"Beta"}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":1,"delta":"\nGamma"}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":2,"delta":"Delta"}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "Alpha\nBeta\nGamma\nDelta",
        "a raw trailing/leading provider newline at a boundary must not be doubled"
    );
}

#[tokio::test]
async fn reasoning_adjacent_responses_keep_their_own_boundaries() {
    // Two adjacent model responses (request 0 and request 1 in one loop) each
    // have independent part state; nothing leaks across the response boundary.
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({"type":"response.reasoning_summary_text.delta","summary_index":0,"delta":"One"}),
            json!({"type":"response.reasoning_summary_text.delta","summary_index":1,"delta":"Two"}),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type":"response.reasoning_summary_text.delta","summary_index":0,"delta":"Three"}),
            json!({"type":"response.reasoning_summary_text.delta","summary_index":1,"delta":"Four"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let mut texts = Vec::new();
    for index in 0..2 {
        let request = ModelRequest::new(
            vec![ModelMessage::user("reason").unwrap()],
            vec![],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap();
        let events = run_model(
            &model,
            request,
            context_for_request(index, CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
        texts.push(
            events
                .iter()
                .filter_map(|event| match event {
                    ModelEvent::ReasoningDelta { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<String>(),
        );
    }
    assert_eq!(texts, vec!["One\nTwo", "Three\nFour"]);
}

#[test]
fn queue_reasoning_delta_missing_identity_is_not_a_boundary_and_partial_fields_retain_known() {
    // White-box parser check for the metadata gap: a provider may drop the
    // index on a fragment ("ning") and later restore it (" phase"). The known
    // identity must be RETAINED through the gap (present->absent->present on
    // the SAME part concatenates); only a genuine known change (summary 0->1)
    // or a lifecycle transition may separate parts.
    let bytes: ByteStream = Box::pin(futures_util::stream::empty());
    let mut state = StreamState::new(bytes, CancellationToken::new(), TokioInstant::now());
    let mut frame = |delta: &str,
                     output_index: Option<u32>,
                     item_id: Option<&str>,
                     summary_index: Option<u32>| {
        let event = ReasoningSummaryDeltaEvent {
            delta: delta.to_owned(),
            output_index: output_index.map(serde_json::Value::from),
            item_id: item_id.map(str::to_owned),
            summary_index: summary_index.map(serde_json::Value::from),
        };
        queue_reasoning_delta(&mut state, event).unwrap();
    };
    frame("Plan", Some(0), Some("rs_x"), Some(0));
    frame("ning", None, None, None);
    frame(" phase", Some(0), Some("rs_x"), Some(0));
    frame("Detail", None, None, Some(1));
    let text: String = state
        .pending
        .iter()
        .filter_map(|event| match event {
            Ok(ModelEvent::ReasoningDelta { delta }) => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        text, "Planning phase\nDetail",
        "missing identity is NOT a boundary; the later index change separates once"
    );
}

#[test]
fn queue_reasoning_delta_empty_delta_with_new_index_signals_the_next_part() {
    // An EMPTY delta carrying a new summary index announces the next part
    // before any text; the following identity-less nonempty delta must start a
    // new part while the empty delta itself emits no line.
    let bytes: ByteStream = Box::pin(futures_util::stream::empty());
    let mut state = StreamState::new(bytes, CancellationToken::new(), TokioInstant::now());
    let mut frame = |delta: &str, summary_index: Option<u32>| {
        let event = ReasoningSummaryDeltaEvent {
            delta: delta.to_owned(),
            output_index: None,
            item_id: None,
            summary_index: summary_index.map(serde_json::Value::from),
        };
        queue_reasoning_delta(&mut state, event).unwrap();
    };
    frame("Foo", Some(0));
    frame("", Some(1));
    frame("bar", None);
    let text: String = state
        .pending
        .iter()
        .filter_map(|event| match event {
            Ok(ModelEvent::ReasoningDelta { delta }) => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        text, "Foo\nbar",
        "the empty delta carries the boundary, never a blank line"
    );
}

#[tokio::test]
async fn reasoning_missing_identity_recovers_known_part_with_single_newline() {
    // Live SSE "final" value for the metadata gap sequence: fragments around a
    // dropped index stay glued, one newline at the genuine summary change.
    let text = reasoning_text_of(vec![
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_x","output_index":0,"summary_index":0,"delta":"Plan"}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"ning"}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_x","output_index":0,"summary_index":0,"delta":" phase"}),
        json!({"type":"response.reasoning_summary_text.delta","summary_index":1,"delta":"Detail"}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "Planning phase\nDetail",
        "live stream must flatten to exactly one newline at the real boundary"
    );
}

#[tokio::test]
async fn reasoning_summary_part_lifecycle_separates_parts_within_one_item() {
    // Some providers split one reasoning item into multiple summary parts and
    // only signal the part boundary via `reasoning_summary_part.added/done`;
    // the delta frames themselves carry no index. Two parts of the SAME item
    // must still separate with one newline while fragments stay concatenated.
    let text = reasoning_text_of(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_one","summary":[]}}),
        json!({"type":"response.reasoning_summary_part.added","output_index":0,"item_id":"rs_one","summary_index":0}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"Plan"}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"ning ... caveats"}),
        json!({"type":"response.reasoning_summary_part.done","output_index":0,"item_id":"rs_one","summary_index":0}),
        json!({"type":"response.reasoning_summary_part.added","output_index":0,"item_id":"rs_one","summary_index":1}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"Detailing ... timeline"}),
        json!({"type":"response.reasoning_summary_part.done","output_index":0,"item_id":"rs_one","summary_index":1}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_one","status":"completed","summary":[{"type":"summary_text","text":"a"}]}}),
        completed(usage()),
    ])
    .await;
    assert_eq!(
        text, "Planning ... caveats\nDetailing ... timeline",
        "part lifecycle inside one reasoning item separates exactly once"
    );
}

#[path = "replay_tests.rs"]
mod replay_tests;

#[tokio::test]
async fn utility_handle_enforces_exact_provider_budget_before_http() {
    let mut configured = settings("http://127.0.0.1:1");
    configured.effective_context_window = 1;
    let raw = std::sync::Arc::new(OpenAiResponsesModel::new(configured).unwrap());
    let utility = super::super::BudgetCheckedModel {
        inner: std::sync::Arc::clone(&raw) as std::sync::Arc<dyn Model>,
        budget: raw as std::sync::Arc<dyn super::super::ProviderBudget>,
    };
    let error = match utility
        .start(
            basic_request(ReasoningPreference::Auto),
            context(CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
    {
        Ok(_) => panic!("utility estimate must be checked before HTTP"),
        Err(error) => error,
    };
    assert_error(
        &error,
        ModelErrorKind::InvalidRequest,
        DeliveryState::NotStarted,
        false,
    );
}
