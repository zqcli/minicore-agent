use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use minicore_runtime::ids::{SessionId, SessionInstanceId, ToolCallId, TurnId};
use minicore_runtime::model::{
    AssistantPart, DeliveryState, ModelFinishReason, ModelLimits, ModelMessage, ModelRequest,
    ReasoningContent, RetryHint, ToolCall,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome, ToolSpec};

use crate::agent::{Agent, CreateSession, GetTranscript, SendMessage};
use crate::config::{AgentConfig, KernelOverrides, Profile};
use crate::models::Models;
use crate::profiles::{ApprovalMode, ProfileCompaction};

use super::*;

#[path = "../../../tests/support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{MockResponse, MockServer, sse_body};

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
        ]),
        supports_tools: true,
        request_timeout: Some(Duration::from_secs(5)),
    }
}

fn model(base_url: &str) -> OpenAiResponsesModel {
    OpenAiResponsesModel::new(settings(base_url)).unwrap()
}

fn context(cancellation: CancellationToken, deadline: Duration) -> ModelCallContext {
    ModelCallContext::new(
        "ses_00000000000000000000000000000001"
            .parse::<SessionId>()
            .unwrap(),
        "ins_00000000000000000000000000000001"
            .parse::<SessionInstanceId>()
            .unwrap(),
        "trn_00000000000000000000000000000001"
            .parse::<TurnId>()
            .unwrap(),
        0,
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
        events.push(event?);
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
    assert_eq!(request.header("user-agent"), Some("minicore-agent/0.1.0"));
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
fn reasoning_request_mapping_and_preflight_overflow_are_conservative() {
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
    let error = OpenAiResponsesModel::new(too_small)
        .unwrap()
        .build_request(&request)
        .unwrap_err();
    assert_error(
        &error,
        ModelErrorKind::ContextOverflow,
        DeliveryState::NotStarted,
        false,
    );

    let mut unsupported = settings("http://127.0.0.1:1");
    unsupported.supported_reasoning = BTreeSet::from([ReasoningPreference::Auto]);
    let error = OpenAiResponsesModel::new(unsupported)
        .unwrap()
        .build_request(&basic_request(ReasoningPreference::High))
        .unwrap_err();
    assert_error(
        &error,
        ModelErrorKind::InvalidRequest,
        DeliveryState::NotStarted,
        false,
    );
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
            "input_tokens_details": {"cached_tokens": 0},
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
            ModelErrorKind::ContextOverflow,
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
        SessionId::new().unwrap()
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
                compaction: ProfileCompaction::Disabled,
            },
        )]),
        models: BTreeMap::new(),
        kernel: KernelOverrides::default(),
    };
    let mut agent = Agent::open_with_models(config, models).await.unwrap();
    let session = agent
        .create_session(CreateSession {
            workspace: workspace.clone(),
            profile: String::new(),
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
    let outcome = agent.turn_handle(turn).unwrap().wait().await.unwrap();
    assert_eq!(outcome.terminal, minicore_runtime::TurnTerminal::Completed);
    let transcript = agent
        .transcript(GetTranscript {
            session_id: session.session_id,
            after: None,
            limit: 100,
        })
        .await
        .unwrap();
    let transcript = serde_json::to_string(&transcript).unwrap();
    assert!(transcript.contains("REAL-READ-CONTENT"));
    assert!(transcript.contains("read complete"));
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

#[tokio::test]
#[ignore = "requires OPENAI_API_KEY and MINICORE_AGENT_LIVE_MODEL"]
async fn openai_live_smoke() {
    let (Ok(api_key), Ok(provider_model)) = (
        std::env::var("OPENAI_API_KEY"),
        std::env::var("MINICORE_AGENT_LIVE_MODEL"),
    ) else {
        return;
    };
    if api_key.trim().is_empty() || provider_model.trim().is_empty() {
        return;
    }
    let base_url = std::env::var("MINICORE_AGENT_LIVE_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_owned());
    let mut settings = settings(&base_url);
    settings.provider_model = provider_model;
    settings.api_key = api_key;
    settings.request_timeout = Some(Duration::from_secs(120));
    let events = run_model(
        &OpenAiResponsesModel::new(settings).unwrap(),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(120)),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelEvent::TextDelta { .. }))
    );
    assert!(matches!(events.last(), Some(ModelEvent::Finish { .. })));
}
