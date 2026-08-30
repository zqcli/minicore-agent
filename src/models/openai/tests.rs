use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use minicore_runtime::ids::{SessionId, SessionInstanceId, ToolCallId, TurnId};
use minicore_runtime::model::{
    AssistantPart, DeliveryState, ModelFinishReason, ModelLimits, ModelMessage, ModelRequest,
    ReasoningContent, RetryHint, ToolCall, Usage,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome, ToolSpec};

use crate::agent::{Agent, CreateSession, GetTranscript, SendMessage, TurnRef};
use crate::config::{AgentConfig, KernelOverrides, Profile};
use crate::event::{AgentEvent, OutputChannel};
use crate::models::{ModelConfig, Models};
use crate::profiles::{ApprovalMode, ProfileCompaction};

use super::*;

#[path = "../../../tests/support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{ConcurrentMockServer, MockResponse, MockServer, sse_body};

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
        ]),
        supports_tools: true,
        request_timeout_seconds: Some(5),
    }
}

fn context(cancellation: CancellationToken, deadline: Duration) -> ModelCallContext {
    context_for_round(0, cancellation, deadline)
}

fn context_for_round(
    round: u16,
    cancellation: CancellationToken,
    deadline: Duration,
) -> ModelCallContext {
    context_for_identity(
        "ins_00000000000000000000000000000001"
            .parse::<SessionInstanceId>()
            .unwrap(),
        "trn_00000000000000000000000000000001"
            .parse::<TurnId>()
            .unwrap(),
        round,
        cancellation,
        deadline,
    )
}

fn context_for_identity(
    instance_id: SessionInstanceId,
    turn_id: TurnId,
    round: u16,
    cancellation: CancellationToken,
    deadline: Duration,
) -> ModelCallContext {
    context_for_session_identity(
        "ses_00000000000000000000000000000001"
            .parse::<SessionId>()
            .unwrap(),
        instance_id,
        turn_id,
        round,
        cancellation,
        deadline,
    )
}

fn context_for_session_identity(
    session_id: SessionId,
    instance_id: SessionInstanceId,
    turn_id: TurnId,
    round: u16,
    cancellation: CancellationToken,
    deadline: Duration,
) -> ModelCallContext {
    ModelCallContext::new(
        session_id,
        instance_id,
        turn_id,
        round,
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

fn delayed_concurrent_sse(events: &[Value]) -> MockResponse {
    let body = sse_body(events).into_bytes();
    let split = body.len() / 2;
    MockResponse::sse_bytes(Vec::new())
        .with_chunks([body[..split].to_vec(), body[split..].to_vec()])
        .with_chunk_delay(Duration::from_millis(50))
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
async fn same_turn_next_round_replays_exact_reasoning_and_function_call_items() {
    const SYSTEM_MESSAGE: &str = "current system instructions";
    const CONTEXT_MESSAGE: &str = "current workspace context";
    const CALL_ID: &str = "call-a4-t01";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "round-zero.txt", "line": 7 }"#;
    const ENCRYPTED_CONTENT: &str = "enc::A4-T01::AAECAwQFBgcICQ==::tail";

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_a4_t01_round_zero",
        "encrypted_content": ENCRYPTED_CONTENT,
        "summary": [{"type": "summary_text", "text": "inspect the requested file"}],
        "status": "completed",
        "provider": {
            "name": "loopback-openai",
            "trace_id": "provider-reasoning-a4-t01",
            "opaque": [1, true, "preserve-exactly"]
        }
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_a4_t01_round_zero",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {
            "trace_id": "provider-function-call-a4-t01",
            "opaque": {"preserve": "this field"}
        }
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.reasoning_summary_text.delta",
                "delta": "inspect the requested file"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_item.clone()
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_item.clone()
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());

    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system(SYSTEM_MESSAGE).unwrap(),
                ModelMessage::system(CONTEXT_MESSAGE).unwrap(),
                ModelMessage::user("read the requested file").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::reasoning_delta("inspect the requested file").unwrap(),
        ModelEvent::ToolCallStart {
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), FUNCTION_ARGUMENTS).unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: tool_call_id.clone(),
        },
        ModelEvent::Usage {
            usage: Usage::from_optional(Some(12), Some(7), Some(4))
                .with_cache_read_tokens(Some(5))
                .with_cache_write_tokens(Some(3))
                .with_provider_total_tokens(Some(31)),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        },
    ];
    assert_eq!(round_zero_events, expected_round_zero_events);

    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system(SYSTEM_MESSAGE).unwrap(),
            ModelMessage::system(CONTEXT_MESSAGE).unwrap(),
            ModelMessage::user("read the requested file").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh round-one tool result").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "current system instructions"
            }]
        },
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "current workspace context"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "read the requested file"
            }]
        },
        {
            "type": "reasoning",
            "id": "rs_a4_t01_round_zero",
            "encrypted_content": "enc::A4-T01::AAECAwQFBgcICQ==::tail",
            "summary": [{
                "type": "summary_text",
                "text": "inspect the requested file"
            }],
            "status": "completed",
            "provider": {
                "name": "loopback-openai",
                "trace_id": "provider-reasoning-a4-t01",
                "opaque": [1, true, "preserve-exactly"]
            }
        },
        {
            "type": "function_call",
            "id": "fc_a4_t01_round_zero",
            "call_id": "call-a4-t01",
            "name": "read",
            "arguments": r#"{ "path": "round-zero.txt", "line": 7 }"#,
            "status": "completed",
            "provider": {
                "trace_id": "provider-function-call-a4-t01",
                "opaque": {"preserve": "this field"}
            }
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t01",
            "output": "fresh round-one tool result",
            "status": "completed"
        }
    ]);
    assert_eq!(
        second_body["input"], expected_input,
        "same-Turn continuation must preserve the exact provider replay and input order"
    );
}

#[tokio::test]
async fn three_round_tool_loop_replays_each_prior_provider_round_once() {
    const CALL_A: &str = "call-a4-t02-a";
    const CALL_B: &str = "call-a4-t02-b";
    const ARGUMENTS_A: &str = r#"{ "path": "a.txt" }"#;
    const ARGUMENTS_B: &str = r#"{ "path": "b.txt" }"#;

    let reasoning_a = json!({
        "type": "reasoning",
        "id": "rs_a4_t02_a",
        "encrypted_content": "enc::A4-T02-A::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare tool A"}],
        "status": "completed",
        "provider": {"opaque": "round-a"}
    });
    let function_a = json!({
        "type": "function_call",
        "id": "fc_a4_t02_a",
        "call_id": CALL_A,
        "name": "read",
        "arguments": ARGUMENTS_A,
        "status": "completed",
        "provider": {"opaque": "function-a"}
    });
    let reasoning_b = json!({
        "type": "reasoning",
        "id": "rs_a4_t02_b",
        "encrypted_content": "enc::A4-T02-B::CAkKCwwNDg8=",
        "summary": [{"type": "summary_text", "text": "prepare tool B"}],
        "status": "completed",
        "provider": {"opaque": "round-b"}
    });
    let function_b = json!({
        "type": "function_call",
        "id": "fc_a4_t02_b",
        "call_id": CALL_B,
        "name": "read",
        "arguments": ARGUMENTS_B,
        "status": "completed",
        "provider": {"opaque": "function-b"}
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_a
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_a
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_b
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_b
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "three rounds complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());

    run_model(
        &model,
        ModelRequest::new(
            vec![ModelMessage::user("produce tool A").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let call_id_a = ToolCallId::new(CALL_A).unwrap();
    let call_a = ToolCall::new(
        call_id_a.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(ARGUMENTS_A).unwrap(),
        0,
    )
    .unwrap();
    run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::user("produce tool B after A").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(call_a.clone())]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id_a.clone(),
                    ToolOutput::new("fresh output A").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let call_id_b = ToolCallId::new(CALL_B).unwrap();
    let call_b = ToolCall::new(
        call_id_b.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(ARGUMENTS_B).unwrap(),
        0,
    )
    .unwrap();
    run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("round two system").unwrap(),
                ModelMessage::system("round two current context").unwrap(),
                ModelMessage::user("round two final request").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(call_a)]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id_a,
                    ToolOutput::new("fresh output A").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(call_b)]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id_b,
                    ToolOutput::new("fresh output B").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(2, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 3);
    let round_two_body = requests[2].json_body();
    assert_eq!(round_two_body["store"], false);
    let expected_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "round two system"}]
        },
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "round two current context"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "round two final request"}]
        },
        {
            "type": "reasoning",
            "id": "rs_a4_t02_a",
            "encrypted_content": "enc::A4-T02-A::AAECAwQFBgc=",
            "summary": [{"type": "summary_text", "text": "prepare tool A"}],
            "status": "completed",
            "provider": {"opaque": "round-a"}
        },
        {
            "type": "function_call",
            "id": "fc_a4_t02_a",
            "call_id": "call-a4-t02-a",
            "name": "read",
            "arguments": r#"{ "path": "a.txt" }"#,
            "status": "completed",
            "provider": {"opaque": "function-a"}
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t02-a",
            "output": "fresh output A",
            "status": "completed"
        },
        {
            "type": "reasoning",
            "id": "rs_a4_t02_b",
            "encrypted_content": "enc::A4-T02-B::CAkKCwwNDg8=",
            "summary": [{"type": "summary_text", "text": "prepare tool B"}],
            "status": "completed",
            "provider": {"opaque": "round-b"}
        },
        {
            "type": "function_call",
            "id": "fc_a4_t02_b",
            "call_id": "call-a4-t02-b",
            "name": "read",
            "arguments": r#"{ "path": "b.txt" }"#,
            "status": "completed",
            "provider": {"opaque": "function-b"}
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t02-b",
            "output": "fresh output B",
            "status": "completed"
        }
    ]);
    assert_eq!(round_two_body["input"], expected_input);
    let input = round_two_body["input"].as_array().unwrap();
    for raw_id in ["rs_a4_t02_a", "fc_a4_t02_a", "rs_a4_t02_b", "fc_a4_t02_b"] {
        assert_eq!(
            input.iter().filter(|item| item["id"] == raw_id).count(),
            1,
            "each raw Provider item must appear exactly once"
        );
    }
    for call_id in [CALL_A, CALL_B] {
        assert_eq!(
            input
                .iter()
                .filter(|item| {
                    item["type"] == "function_call_output" && item["call_id"] == call_id
                })
                .count(),
            1,
            "each authoritative tool output must appear exactly once"
        );
    }
}

#[tokio::test]
async fn round_one_concurrent_turn_continuations_are_isolated_by_identity() {
    const CALL_A: &str = "call-a4-t03-a";
    const CALL_B: &str = "call-a4-t03-b";

    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t03_a",
                    "call_id": CALL_A,
                    "name": "read",
                    "arguments": r#"{ "path": "identity-a.txt" }"#,
                    "status": "completed",
                    "provider": {"opaque": "identity-a-only"}
                }
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t03_b",
                    "call_id": CALL_B,
                    "name": "read",
                    "arguments": r#"{ "path": "identity-b.txt" }"#,
                    "status": "completed",
                    "provider": {"opaque": "identity-b-only"}
                }
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "identity complete"}),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "identity complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = Arc::new(model(server.base_url()));
    let instance_a = "ins_0000000000000000000000000000000a"
        .parse::<SessionInstanceId>()
        .unwrap();
    let turn_a = "trn_0000000000000000000000000000000a"
        .parse::<TurnId>()
        .unwrap();
    let instance_b = "ins_0000000000000000000000000000000b"
        .parse::<SessionInstanceId>()
        .unwrap();
    let turn_b = "trn_0000000000000000000000000000000b"
        .parse::<TurnId>()
        .unwrap();

    run_model(
        &model,
        ModelRequest::new(
            vec![ModelMessage::user("save identity A").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_identity(
            instance_a,
            turn_a,
            0,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();
    run_model(
        &model,
        ModelRequest::new(
            vec![ModelMessage::user("save identity B").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_identity(
            instance_b,
            turn_b,
            0,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();

    let call_id_a = ToolCallId::new(CALL_A).unwrap();
    let request_a = ModelRequest::new(
        vec![
            ModelMessage::system("identity A system").unwrap(),
            ModelMessage::user("identity A current user").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(
                ToolCall::new(
                    call_id_a.clone(),
                    "read".parse().unwrap(),
                    json!({"path": "identity-a.txt"}),
                    0,
                )
                .unwrap(),
            )])
            .unwrap(),
            ModelMessage::tool_with_outcome(
                call_id_a,
                ToolOutput::new("identity A output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let call_id_b = ToolCallId::new(CALL_B).unwrap();
    let request_b = ModelRequest::new(
        vec![
            ModelMessage::system("identity B system").unwrap(),
            ModelMessage::user("identity B current user").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(
                ToolCall::new(
                    call_id_b.clone(),
                    "read".parse().unwrap(),
                    json!({"path": "identity-b.txt"}),
                    0,
                )
                .unwrap(),
            )])
            .unwrap(),
            ModelMessage::tool_with_outcome(
                call_id_b,
                ToolOutput::new("identity B output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let (result_a, result_b) = tokio::join!(
        run_model(
            &model,
            request_a,
            context_for_identity(
                instance_a,
                turn_a,
                1,
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
        ),
        run_model(
            &model,
            request_b,
            context_for_identity(
                instance_b,
                turn_b,
                1,
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
        ),
    );
    result_a.unwrap();
    result_b.unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 4);
    let probe_bodies = requests[2..]
        .iter()
        .map(|request| request.json_body())
        .collect::<Vec<_>>();
    let body_a = probe_bodies
        .iter()
        .find(|body| body.to_string().contains("identity A current user"))
        .expect("identity A request must be captured");
    let body_b = probe_bodies
        .iter()
        .find(|body| body.to_string().contains("identity B current user"))
        .expect("identity B request must be captured");
    assert_eq!(body_a["store"], false);
    assert_eq!(body_b["store"], false);
    let expected_a = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "identity A system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "identity A current user"}]
        },
        {
            "type": "function_call",
            "id": "fc_a4_t03_a",
            "call_id": "call-a4-t03-a",
            "name": "read",
            "arguments": r#"{ "path": "identity-a.txt" }"#,
            "status": "completed",
            "provider": {"opaque": "identity-a-only"}
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t03-a",
            "output": "identity A output",
            "status": "completed"
        }
    ]);
    let expected_b = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "identity B system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "identity B current user"}]
        },
        {
            "type": "function_call",
            "id": "fc_a4_t03_b",
            "call_id": "call-a4-t03-b",
            "name": "read",
            "arguments": r#"{ "path": "identity-b.txt" }"#,
            "status": "completed",
            "provider": {"opaque": "identity-b-only"}
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t03-b",
            "output": "identity B output",
            "status": "completed"
        }
    ]);
    assert_eq!(body_a["input"], expected_a);
    assert_eq!(body_b["input"], expected_b);
    assert!(!body_a.to_string().contains("identity-b-only"));
    assert!(!body_b.to_string().contains("identity-a-only"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_streams_save_and_replay_isolated_continuations() {
    const CALL_A: &str = "call-a4-t03-overlap-a";
    const CALL_B: &str = "call-a4-t03-overlap-b";

    let server = ConcurrentMockServer::spawn([
        delayed_concurrent_sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t03_overlap_a",
                    "call_id": CALL_A,
                    "name": "read",
                    "arguments": r#"{ "path": "overlap-a.txt" }"#,
                    "status": "completed",
                    "provider": {"opaque": "overlap-a-only"}
                }
            }),
            completed(usage()),
        ]),
        delayed_concurrent_sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t03_overlap_b",
                    "call_id": CALL_B,
                    "name": "read",
                    "arguments": r#"{ "path": "overlap-b.txt" }"#,
                    "status": "completed",
                    "provider": {"opaque": "overlap-b-only"}
                }
            }),
            completed(usage()),
        ]),
        delayed_concurrent_sse(&[
            json!({"type": "response.output_text.delta", "delta": "overlap final"}),
            completed(usage()),
        ]),
        delayed_concurrent_sse(&[
            json!({"type": "response.output_text.delta", "delta": "overlap final"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = Arc::new(model(server.base_url()));
    let session_a = "ses_000000000000000000000000000000a1"
        .parse::<SessionId>()
        .unwrap();
    let instance_a = "ins_000000000000000000000000000000a1"
        .parse::<SessionInstanceId>()
        .unwrap();
    let turn_a = "trn_000000000000000000000000000000a1"
        .parse::<TurnId>()
        .unwrap();
    let session_b = "ses_000000000000000000000000000000b1"
        .parse::<SessionId>()
        .unwrap();
    let instance_b = "ins_000000000000000000000000000000b1"
        .parse::<SessionInstanceId>()
        .unwrap();
    let turn_b = "trn_000000000000000000000000000000b1"
        .parse::<TurnId>()
        .unwrap();

    let (round_zero_a, round_zero_b) = tokio::join!(
        run_model(
            &model,
            ModelRequest::new(
                vec![ModelMessage::user("overlap identity A round zero").unwrap()],
                vec![read_tool()],
                ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
                ReasoningPreference::High,
            )
            .unwrap(),
            context_for_session_identity(
                session_a,
                instance_a,
                turn_a,
                0,
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
        ),
        run_model(
            &model,
            ModelRequest::new(
                vec![ModelMessage::user("overlap identity B round zero").unwrap()],
                vec![read_tool()],
                ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
                ReasoningPreference::High,
            )
            .unwrap(),
            context_for_session_identity(
                session_b,
                instance_b,
                turn_b,
                0,
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
        ),
    );
    let round_zero_a = round_zero_a.unwrap();
    let round_zero_b = round_zero_b.unwrap();
    let observed_call = |events: &[ModelEvent]| {
        events
            .iter()
            .find_map(|event| match event {
                ModelEvent::ToolCallStart { tool_call_id, .. } => Some(tool_call_id.clone()),
                _ => None,
            })
            .expect("round zero must emit a ToolCallStart")
    };
    let call_a = observed_call(&round_zero_a);
    let call_b = observed_call(&round_zero_b);
    assert_ne!(call_a, call_b);

    let arguments_for = |call_id: &ToolCallId| match call_id.as_str() {
        CALL_A => json!({"path": "overlap-a.txt"}),
        CALL_B => json!({"path": "overlap-b.txt"}),
        _ => panic!("unexpected concurrent ToolCallId"),
    };
    let request_for = |system: &str, user: &str, output: &str, call_id: &ToolCallId| {
        let call = ToolCall::new(
            call_id.clone(),
            "read".parse().unwrap(),
            arguments_for(call_id),
            0,
        )
        .unwrap();
        ModelRequest::new(
            vec![
                ModelMessage::system(system).unwrap(),
                ModelMessage::user(user).unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(call)]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id.clone(),
                    ToolOutput::new(output).unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap()
    };
    let request_a = request_for(
        "overlap identity A system",
        "overlap identity A current user",
        "overlap identity A output",
        &call_a,
    );
    let request_b = request_for(
        "overlap identity B system",
        "overlap identity B current user",
        "overlap identity B output",
        &call_b,
    );
    let (round_one_a, round_one_b) = tokio::join!(
        run_model(
            &model,
            request_a,
            context_for_session_identity(
                session_a,
                instance_a,
                turn_a,
                1,
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
        ),
        run_model(
            &model,
            request_b,
            context_for_session_identity(
                session_b,
                instance_b,
                turn_b,
                1,
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
        ),
    );
    round_one_a.unwrap();
    round_one_b.unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 4);
    let round_one_bodies = requests[2..]
        .iter()
        .map(|request| request.json_body())
        .collect::<Vec<_>>();
    let body_a = round_one_bodies
        .iter()
        .find(|body| body.to_string().contains("overlap identity A current user"))
        .expect("overlap identity A round one request must be captured");
    let body_b = round_one_bodies
        .iter()
        .find(|body| body.to_string().contains("overlap identity B current user"))
        .expect("overlap identity B round one request must be captured");
    assert_eq!(body_a["store"], false);
    assert_eq!(body_b["store"], false);

    let expected_input =
        |system: &str, user: &str, output: &str, call_id: &ToolCallId| match call_id.as_str() {
            CALL_A => json!([
                {
                    "type": "message",
                    "role": "developer",
                    "content": [{"type": "input_text", "text": system}]
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": user}]
                },
                {
                    "type": "function_call",
                    "id": "fc_a4_t03_overlap_a",
                    "call_id": "call-a4-t03-overlap-a",
                    "name": "read",
                    "arguments": r#"{ "path": "overlap-a.txt" }"#,
                    "status": "completed",
                    "provider": {"opaque": "overlap-a-only"}
                },
                {
                    "type": "function_call_output",
                    "call_id": "call-a4-t03-overlap-a",
                    "output": output,
                    "status": "completed"
                }
            ]),
            CALL_B => json!([
                {
                    "type": "message",
                    "role": "developer",
                    "content": [{"type": "input_text", "text": system}]
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": user}]
                },
                {
                    "type": "function_call",
                    "id": "fc_a4_t03_overlap_b",
                    "call_id": "call-a4-t03-overlap-b",
                    "name": "read",
                    "arguments": r#"{ "path": "overlap-b.txt" }"#,
                    "status": "completed",
                    "provider": {"opaque": "overlap-b-only"}
                },
                {
                    "type": "function_call_output",
                    "call_id": "call-a4-t03-overlap-b",
                    "output": output,
                    "status": "completed"
                }
            ]),
            _ => panic!("unexpected concurrent ToolCallId"),
        };
    assert_eq!(
        body_a["input"],
        expected_input(
            "overlap identity A system",
            "overlap identity A current user",
            "overlap identity A output",
            &call_a,
        )
    );
    assert_eq!(
        body_b["input"],
        expected_input(
            "overlap identity B system",
            "overlap identity B current user",
            "overlap identity B output",
            &call_b,
        )
    );
    let other_a = if call_a.as_str() == CALL_A {
        "overlap-b-only"
    } else {
        "overlap-a-only"
    };
    let other_b = if call_b.as_str() == CALL_A {
        "overlap-b-only"
    } else {
        "overlap-a-only"
    };
    assert!(!body_a.to_string().contains(other_a));
    assert!(!body_b.to_string().contains(other_b));
}

#[tokio::test]
async fn fully_consumed_final_stream_does_not_replay_into_new_turn() {
    const CALL_ID: &str = "call-a4-t04";
    const ARGUMENTS: &str = r#"{ "path": "turn-one.txt" }"#;

    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t04",
                    "call_id": CALL_ID,
                    "name": "read",
                    "arguments": ARGUMENTS,
                    "status": "completed",
                    "provider": {"opaque": "must-end-with-turn-one"}
                }
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "turn one final"}),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "new turn complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let instance = "ins_00000000000000000000000000000004"
        .parse::<SessionInstanceId>()
        .unwrap();
    let turn_one = "trn_00000000000000000000000000000004"
        .parse::<TurnId>()
        .unwrap();
    let turn_two = "trn_00000000000000000000000000000005"
        .parse::<TurnId>()
        .unwrap();

    run_model(
        &model,
        ModelRequest::new(
            vec![ModelMessage::user("turn one tool round").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_identity(
            instance,
            turn_one,
            0,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();
    let call_id = ToolCallId::new(CALL_ID).unwrap();
    let old_call = ToolCall::new(
        call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let final_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::user("finish turn one").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(old_call.clone())]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id.clone(),
                    ToolOutput::new("turn one output").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_identity(
            instance,
            turn_one,
            1,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        final_events.last(),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::Stop
        })
    ));

    run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("new turn system").unwrap(),
                ModelMessage::user("new turn probe with old history").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(old_call)]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id,
                    ToolOutput::new("new turn fresh output").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_identity(
            instance,
            turn_two,
            0,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 3);
    let probe_body = requests[2].json_body();
    assert_eq!(probe_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "new turn system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "new turn probe with old history"}]
        },
        {
            "type": "function_call",
            "call_id": "call-a4-t04",
            "name": "read",
            "arguments": r#"{"path":"turn-one.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t04",
            "output": "new turn fresh output",
            "status": "completed"
        }
    ]);
    assert_eq!(probe_body["input"], expected_normalized_input);
    assert!(!probe_body.to_string().contains("must-end-with-turn-one"));
}

#[tokio::test]
async fn started_stream_failure_purges_replay_before_same_round_probe() {
    const CALL_ID: &str = "call-a4-t06";
    const ARGUMENTS: &str = r#"{ "path": "stream-failure.txt" }"#;

    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t06",
                    "call_id": CALL_ID,
                    "name": "read",
                    "arguments": ARGUMENTS,
                    "status": "completed",
                    "provider": {"opaque": "must-be-purged-after-started-error"}
                }
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({
                "type": "response.reasoning_summary_text.delta",
                "delta": "partial reasoning before malformed event"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "reasoning",
                    "id": "rs_a4_t06_partial",
                    "encrypted_content": "enc::A4-T06-partial::AAECAwQ=",
                    "summary": [{
                        "type": "summary_text",
                        "text": "partial reasoning before malformed event"
                    }],
                    "status": "completed"
                }
            }),
            json!({"type": "response.output_text.delta", "delta": 7}),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "probe complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    run_model(
        &model,
        ModelRequest::new(
            vec![ModelMessage::user("save replay before stream failure").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let call_id = ToolCallId::new(CALL_ID).unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let failed_request = ModelRequest::new(
        vec![
            ModelMessage::user("trigger a started stream failure").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(call.clone())]).unwrap(),
            ModelMessage::tool_with_outcome(
                call_id.clone(),
                ToolOutput::new("stream failure output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let (events, error) = run_until_error(
        &model,
        failed_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    assert_eq!(
        events,
        vec![ModelEvent::reasoning_delta("partial reasoning before malformed event").unwrap()]
    );
    assert_error(
        &error,
        ModelErrorKind::InvalidProviderResponse,
        DeliveryState::Started,
        false,
    );

    run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("post-failure system").unwrap(),
                ModelMessage::user("probe after started failure").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(call)]).unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id,
                    ToolOutput::new("post-failure fresh output").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 3);
    let probe_body = requests[2].json_body();
    assert_eq!(probe_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "post-failure system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "probe after started failure"}]
        },
        {
            "type": "function_call",
            "call_id": "call-a4-t06",
            "name": "read",
            "arguments": r#"{"path":"stream-failure.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t06",
            "output": "post-failure fresh output",
            "status": "completed"
        }
    ]);
    assert_eq!(probe_body["input"], expected_normalized_input);
    assert!(
        !probe_body
            .to_string()
            .contains("must-be-purged-after-started-error")
    );
}

#[tokio::test]
async fn reasoning_disabled_tool_loop_remains_normalized_and_stateless() {
    const CALL_ID: &str = "call-a4-t10";
    const ARGUMENTS: &str = r#"{ "path": "disabled.txt" }"#;

    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_a4_t10_provider",
                    "call_id": CALL_ID,
                    "name": "read",
                    "arguments": ARGUMENTS,
                    "status": "completed",
                    "provider": {"opaque": "disabled-must-not-replay"}
                }
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "disabled complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    run_model(
        &model,
        ModelRequest::new(
            vec![ModelMessage::user("disabled tool round").unwrap()],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::Disabled,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let call_id = ToolCallId::new(CALL_ID).unwrap();
    run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("disabled system").unwrap(),
                ModelMessage::user("disabled second request").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(
                    ToolCall::new(
                        call_id.clone(),
                        "read".parse().unwrap(),
                        serde_json::from_str(ARGUMENTS).unwrap(),
                        0,
                    )
                    .unwrap(),
                )])
                .unwrap(),
                ModelMessage::tool_with_outcome(
                    call_id,
                    ToolOutput::new("disabled fresh output").unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::Disabled,
        )
        .unwrap(),
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    assert_eq!(second_body["reasoning"], json!({"effort": "none"}));
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "disabled system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "disabled second request"}]
        },
        {
            "type": "function_call",
            "call_id": "call-a4-t10",
            "name": "read",
            "arguments": r#"{"path":"disabled.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t10",
            "output": "disabled fresh output",
            "status": "completed"
        }
    ]);
    assert_eq!(second_body["input"], expected_normalized_input);
    assert!(!second_body.to_string().contains("disabled-must-not-replay"));
    assert!(!second_body.to_string().contains("fc_a4_t10_provider"));
}

#[tokio::test]
async fn terminal_output_replays_exact_items_when_done_events_are_absent() {
    const CALL_ID: &str = "call-terminal-fallback";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "terminal.txt" }"#;

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_terminal_fallback",
        "encrypted_content": "enc::terminal-fallback::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "inspect terminal output"}],
        "status": "completed",
        "provider": {
            "trace_id": "provider-terminal-reasoning",
            "opaque": ["preserve", 7, true]
        }
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_terminal_fallback",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {
            "trace_id": "provider-terminal-function-call",
            "opaque": {"preserve": "exactly"}
        }
    });
    let terminal = json!({
        "type": "response.completed",
        "response": {
            "status": "completed",
            "output": [reasoning_item.clone(), function_call_item.clone()],
            "usage": usage()
        }
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.reasoning_summary_text.delta",
                "delta": "inspect terminal output"
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "call_id": CALL_ID,
                    "name": "read",
                    "arguments": ""
                }
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 1,
                "delta": FUNCTION_ARGUMENTS
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 1,
                "arguments": FUNCTION_ARGUMENTS
            }),
            terminal,
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());

    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("terminal fallback system").unwrap(),
                ModelMessage::user("read terminal output").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::reasoning_delta("inspect terminal output").unwrap(),
        ModelEvent::ToolCallStart {
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), FUNCTION_ARGUMENTS).unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: tool_call_id.clone(),
        },
        ModelEvent::Usage {
            usage: Usage::from_optional(Some(12), Some(7), Some(4))
                .with_cache_read_tokens(Some(5))
                .with_cache_write_tokens(Some(3))
                .with_provider_total_tokens(Some(31)),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        },
    ];
    assert_eq!(round_zero_events, expected_round_zero_events);

    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("terminal fallback system").unwrap(),
            ModelMessage::user("read terminal output").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh terminal tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "terminal fallback system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "read terminal output"
            }]
        },
        {
            "type": "reasoning",
            "id": "rs_terminal_fallback",
            "encrypted_content": "enc::terminal-fallback::AAECAwQFBgc=",
            "summary": [{
                "type": "summary_text",
                "text": "inspect terminal output"
            }],
            "status": "completed",
            "provider": {
                "trace_id": "provider-terminal-reasoning",
                "opaque": ["preserve", 7, true]
            }
        },
        {
            "type": "function_call",
            "id": "fc_terminal_fallback",
            "call_id": "call-terminal-fallback",
            "name": "read",
            "arguments": r#"{ "path": "terminal.txt" }"#,
            "status": "completed",
            "provider": {
                "trace_id": "provider-terminal-function-call",
                "opaque": {"preserve": "exactly"}
            }
        },
        {
            "type": "function_call_output",
            "call_id": "call-terminal-fallback",
            "output": "fresh terminal tool output",
            "status": "completed"
        }
    ]);
    assert_eq!(
        second_body["input"], expected_input,
        "terminal response.output must supply exact continuation items when done events are absent"
    );
}

#[tokio::test]
async fn terminal_output_rejects_unobserved_function_calls_without_replay() {
    const OBSERVED_CALL_ID: &str = "call-terminal-observed";
    const OBSERVED_ARGUMENTS: &str = r#"{"path":"observed.txt"}"#;

    let terminal = json!({
        "type": "response.completed",
        "response": {
            "status": "completed",
            "output": [
                {
                    "type": "function_call",
                    "id": "fc_terminal_hidden",
                    "call_id": "call-terminal-hidden",
                    "name": "read",
                    "arguments": "{\"path\":\"hidden.txt\"}",
                    "status": "completed",
                    "provider": {"opaque": "hidden-call-must-be-rejected"}
                },
                {
                    "type": "function_call",
                    "id": "fc_terminal_observed",
                    "call_id": OBSERVED_CALL_ID,
                    "name": "read",
                    "arguments": OBSERVED_ARGUMENTS,
                    "status": "completed",
                    "provider": {"opaque": "observed-call"}
                }
            ],
            "usage": usage()
        }
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "call_id": OBSERVED_CALL_ID,
                    "name": "read",
                    "arguments": ""
                }
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 1,
                "delta": OBSERVED_ARGUMENTS
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 1,
                "arguments": OBSERVED_ARGUMENTS
            }),
            terminal,
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let round_zero_request = ModelRequest::new(
        vec![
            ModelMessage::system("hidden function-call system").unwrap(),
            ModelMessage::user("observe only call A").unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let mut stream = model
        .start(
            round_zero_request,
            context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    let mut round_zero_events = Vec::new();
    let mut round_zero_error = None;
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) if round_zero_error.is_none() => round_zero_events.push(event),
            Ok(_) => panic!("model stream emitted an event after its terminal error"),
            Err(error) if round_zero_error.is_none() => round_zero_error = Some(error),
            Err(_) => panic!("model stream emitted more than one terminal error"),
        }
    }
    drop(stream);

    let tool_call_id = ToolCallId::new(OBSERVED_CALL_ID).unwrap();
    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(OBSERVED_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("hidden function-call system").unwrap(),
            ModelMessage::user("observe only call A").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh observed tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "hidden function-call system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "observe only call A"}]
        },
        {
            "type": "function_call",
            "call_id": "call-terminal-observed",
            "name": "read",
            "arguments": r#"{"path":"observed.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-terminal-observed",
            "output": "fresh observed tool output",
            "status": "completed"
        }
    ]);
    let second_input = second_body["input"].as_array().unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::ToolCallStart {
            tool_call_id: ToolCallId::new(OBSERVED_CALL_ID).unwrap(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(
            ToolCallId::new(OBSERVED_CALL_ID).unwrap(),
            OBSERVED_ARGUMENTS,
        )
        .unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: ToolCallId::new(OBSERVED_CALL_ID).unwrap(),
        },
    ];
    let observed_error = round_zero_error
        .as_ref()
        .map(|error| (error.kind(), error.delivery()));
    let hidden_replay_count = second_input
        .iter()
        .filter(|item| item["id"] == "fc_terminal_hidden")
        .count();
    let observed_raw_replay_count = second_input
        .iter()
        .filter(|item| item["id"] == "fc_terminal_observed")
        .count();
    assert_eq!(
        (
            round_zero_events,
            observed_error,
            second_body["input"] == expected_normalized_input,
            hidden_replay_count,
            observed_raw_replay_count,
        ),
        (
            expected_round_zero_events,
            Some((
                ModelErrorKind::InvalidProviderResponse,
                DeliveryState::Started,
            )),
            true,
            0,
            0,
        ),
        "terminal output must reject hidden function calls without saving replay"
    );
}

#[tokio::test]
async fn reasoning_delta_without_complete_item_fails_without_replay() {
    const CALL_ID: &str = "call-missing-reasoning-item";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "missing-reasoning.txt" }"#;

    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.reasoning_summary_text.delta",
                "delta": "reasoning without complete item"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "id": "fc_missing_reasoning_item",
                    "call_id": CALL_ID,
                    "name": "read",
                    "arguments": FUNCTION_ARGUMENTS,
                    "status": "completed",
                    "provider": {"opaque": "must-not-survive-missing-reasoning"}
                }
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let round_zero_request = ModelRequest::new(
        vec![
            ModelMessage::system("missing reasoning item system").unwrap(),
            ModelMessage::user("reason then call a tool").unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let mut stream = model
        .start(
            round_zero_request,
            context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    let mut round_zero_events = Vec::new();
    let mut round_zero_error = None;
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) if round_zero_error.is_none() => round_zero_events.push(event),
            Ok(_) => panic!("model stream emitted an event after its terminal error"),
            Err(error) if round_zero_error.is_none() => round_zero_error = Some(error),
            Err(_) => panic!("model stream emitted more than one terminal error"),
        }
    }
    drop(stream);

    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("missing reasoning item system").unwrap(),
            ModelMessage::user("reason then call a tool").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh missing-reasoning tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "missing reasoning item system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "reason then call a tool"}]
        },
        {
            "type": "function_call",
            "call_id": "call-missing-reasoning-item",
            "name": "read",
            "arguments": r#"{"path":"missing-reasoning.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-missing-reasoning-item",
            "output": "fresh missing-reasoning tool output",
            "status": "completed"
        }
    ]);
    let second_input = second_body["input"].as_array().unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::reasoning_delta("reasoning without complete item").unwrap(),
        ModelEvent::ToolCallStart {
            tool_call_id: ToolCallId::new(CALL_ID).unwrap(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(
            ToolCallId::new(CALL_ID).unwrap(),
            FUNCTION_ARGUMENTS,
        )
        .unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: ToolCallId::new(CALL_ID).unwrap(),
        },
    ];
    let observed_error = round_zero_error
        .as_ref()
        .map(|error| (error.kind(), error.delivery()));
    let raw_replay_count = second_input
        .iter()
        .filter(|item| item["id"] == "fc_missing_reasoning_item")
        .count();
    assert_eq!(
        (
            round_zero_events,
            observed_error,
            second_body["input"] == expected_normalized_input,
            raw_replay_count,
        ),
        (
            expected_round_zero_events,
            Some((
                ModelErrorKind::InvalidProviderResponse,
                DeliveryState::Started,
            )),
            true,
            0,
        ),
        "reasoning deltas require a complete reasoning item before ToolCalls can finish"
    );
}

#[tokio::test]
async fn cancelled_turn_token_purges_continuation_before_next_round() {
    const CALL_ID: &str = "call-cancelled-turn";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "cancelled.txt" }"#;

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_cancelled_turn",
        "encrypted_content": "enc::cancelled-turn::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare cancellation replay"}],
        "status": "completed",
        "provider": {
            "trace_id": "provider-cancelled-reasoning",
            "opaque": ["must", "be", "purged"]
        }
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_cancelled_turn",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {
            "trace_id": "provider-cancelled-function-call",
            "opaque": {"purge": true}
        }
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_item
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_item
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let turn_cancellation = CancellationToken::new();
    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("cancelled turn system").unwrap(),
                ModelMessage::user("save then cancel continuation").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, turn_cancellation.clone(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::ToolCallStart {
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), FUNCTION_ARGUMENTS).unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: tool_call_id.clone(),
        },
        ModelEvent::Usage {
            usage: Usage::from_optional(Some(12), Some(7), Some(4))
                .with_cache_read_tokens(Some(5))
                .with_cache_write_tokens(Some(3))
                .with_provider_total_tokens(Some(31)),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        },
    ];
    assert_eq!(round_zero_events, expected_round_zero_events);
    turn_cancellation.cancel();

    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("cancelled turn system").unwrap(),
            ModelMessage::user("save then cancel continuation").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh cancellation tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "cancelled turn system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "save then cancel continuation"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-cancelled-turn",
            "name": "read",
            "arguments": r#"{"path":"cancelled.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-cancelled-turn",
            "output": "fresh cancellation tool output",
            "status": "completed"
        }
    ]);
    assert_eq!(
        second_body["input"], expected_normalized_input,
        "a cancelled original Turn token must purge exact continuation before the next round"
    );
}

#[tokio::test]
async fn dropping_unpolled_stream_purges_turn_continuation() {
    const CALL_ID: &str = "call-dropped-stream";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "dropped.txt" }"#;

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_dropped_stream",
        "encrypted_content": "enc::dropped-stream::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare dropped stream replay"}],
        "status": "completed",
        "provider": {
            "trace_id": "provider-dropped-reasoning",
            "opaque": ["drop", "must", "purge"]
        }
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_dropped_stream",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {
            "trace_id": "provider-dropped-function-call",
            "opaque": {"drop_guard": true}
        }
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_item
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_item
            }),
            completed(usage()),
        ]),
        MockResponse::sse_bytes(Vec::new()),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "probe complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("dropped stream system").unwrap(),
                ModelMessage::user("save then drop continuation stream").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::ToolCallStart {
            tool_call_id: tool_call_id.clone(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), FUNCTION_ARGUMENTS).unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: tool_call_id.clone(),
        },
        ModelEvent::Usage {
            usage: Usage::from_optional(Some(12), Some(7), Some(4))
                .with_cache_read_tokens(Some(5))
                .with_cache_write_tokens(Some(3))
                .with_provider_total_tokens(Some(31)),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        },
    ];
    assert_eq!(round_zero_events, expected_round_zero_events);

    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("dropped stream system").unwrap(),
            ModelMessage::user("save then drop continuation stream").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh dropped-stream tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let stream = model
        .start(
            round_one_request.clone(),
            context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    drop(stream);

    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 3);
    let third_body = requests[2].json_body();
    assert_eq!(third_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "dropped stream system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "save then drop continuation stream"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-dropped-stream",
            "name": "read",
            "arguments": r#"{"path":"dropped.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-dropped-stream",
            "output": "fresh dropped-stream tool output",
            "status": "completed"
        }
    ]);
    assert_eq!(
        third_body["input"], expected_normalized_input,
        "dropping an unpolled stream must purge exact continuation before the next probe"
    );
}

async fn run_provider_start_error_retry_case(
    error_response: MockResponse,
    expected_kind: ModelErrorKind,
    expected_delivery: DeliveryState,
    expected_retryable: bool,
    preserve_replay: bool,
) {
    const CALL_ID: &str = "call-provider-start-error";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "start-error.txt" }"#;

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_provider_start_error",
        "encrypted_content": "enc::provider-start-error::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare start-error retry"}],
        "status": "completed",
        "provider": {
            "trace_id": "provider-start-error-reasoning",
            "opaque": ["retain", "only", "when", "not-started"]
        }
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_provider_start_error",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {
            "trace_id": "provider-start-error-function-call",
            "opaque": {"retry": "classification-sensitive"}
        }
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_item
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_item
            }),
            completed(usage()),
        ]),
        error_response,
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "retry complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("provider start-error system").unwrap(),
                ModelMessage::user("save exact replay before provider error").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert!(matches!(
        round_zero_events.last(),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls
        })
    ));

    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("provider start-error system").unwrap(),
            ModelMessage::user("save exact replay before provider error").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh provider-error tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let error = start_error(
        &model,
        round_one_request.clone(),
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    assert_error(&error, expected_kind, expected_delivery, expected_retryable);

    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 3);
    let exact_replay_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "provider start-error system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "save exact replay before provider error"
            }]
        },
        {
            "type": "reasoning",
            "id": "rs_provider_start_error",
            "encrypted_content": "enc::provider-start-error::AAECAwQFBgc=",
            "summary": [{
                "type": "summary_text",
                "text": "prepare start-error retry"
            }],
            "status": "completed",
            "provider": {
                "trace_id": "provider-start-error-reasoning",
                "opaque": ["retain", "only", "when", "not-started"]
            }
        },
        {
            "type": "function_call",
            "id": "fc_provider_start_error",
            "call_id": "call-provider-start-error",
            "name": "read",
            "arguments": r#"{ "path": "start-error.txt" }"#,
            "status": "completed",
            "provider": {
                "trace_id": "provider-start-error-function-call",
                "opaque": {"retry": "classification-sensitive"}
            }
        },
        {
            "type": "function_call_output",
            "call_id": "call-provider-start-error",
            "output": "fresh provider-error tool output",
            "status": "completed"
        }
    ]);
    let normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "provider start-error system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "save exact replay before provider error"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-provider-start-error",
            "name": "read",
            "arguments": r#"{"path":"start-error.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-provider-start-error",
            "output": "fresh provider-error tool output",
            "status": "completed"
        }
    ]);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    assert_eq!(
        second_body["input"], exact_replay_input,
        "the provider-error request must begin with the saved exact replay"
    );
    let third_body = requests[2].json_body();
    assert_eq!(third_body["store"], false);
    let expected_retry_input = if preserve_replay {
        exact_replay_input
    } else {
        normalized_input
    };
    assert_eq!(
        third_body["input"], expected_retry_input,
        "provider start-error delivery must control continuation retention"
    );
}

#[tokio::test]
async fn unknown_provider_start_error_purges_continuation_before_same_round_retry() {
    run_provider_start_error_retry_case(
        MockResponse::json(
            500,
            br#"{"error":{"type":"server_error","code":"server_error"}}"#.to_vec(),
        ),
        ModelErrorKind::ProviderUnavailable,
        DeliveryState::Unknown,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn retryable_not_started_rate_limit_preserves_continuation_for_same_round_retry() {
    run_provider_start_error_retry_case(
        MockResponse::json(
            429,
            br#"{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}"#.to_vec(),
        )
        .with_header("retry-after", "0.01"),
        ModelErrorKind::RateLimited,
        DeliveryState::NotStarted,
        true,
        true,
    )
    .await;
}

#[tokio::test]
async fn active_continuation_limit_evicts_oldest_without_clearing_newest() {
    const EXPECTED_MAX_ACTIVE_CONTINUATIONS: usize = 256;
    const CASE_COUNT: usize = EXPECTED_MAX_ACTIVE_CONTINUATIONS + 1;

    struct ActiveCase {
        index: u32,
        instance_id: SessionInstanceId,
        turn_id: TurnId,
        cancellation: CancellationToken,
        call_id: String,
        arguments: String,
    }

    let mut responses = Vec::with_capacity(CASE_COUNT + 2);
    let mut cases = Vec::with_capacity(CASE_COUNT);
    for index in 0..CASE_COUNT {
        let index = u32::try_from(index).unwrap();
        let call_id = format!("call-active-{index:03}");
        let arguments = format!(r#"{{ "path": "active-{index:03}.txt" }}"#);
        responses.push(MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "reasoning",
                    "id": format!("rs_active_{index:03}"),
                    "encrypted_content": format!("enc-active-{index:03}"),
                    "summary": [],
                    "status": "completed",
                    "provider": {"ordinal": index}
                }
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "id": format!("fc_active_{index:03}"),
                    "call_id": call_id.clone(),
                    "name": "read",
                    "arguments": arguments.clone(),
                    "status": "completed",
                    "provider": {"ordinal": index, "opaque": "active-bound"}
                }
            }),
            completed(usage()),
        ]));
        cases.push(ActiveCase {
            index,
            instance_id: SessionInstanceId::new().unwrap(),
            turn_id: TurnId::new().unwrap(),
            cancellation: CancellationToken::new(),
            call_id,
            arguments,
        });
    }
    responses.extend([
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "oldest probe complete"}),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "newest probe complete"}),
            completed(usage()),
        ]),
    ]);
    assert_eq!(cases.len(), 257);
    assert_eq!(responses.len(), 259);

    let server = MockServer::spawn(responses).await;
    let model = model(server.base_url());
    let round_zero_request = ModelRequest::new(
        vec![
            ModelMessage::system("active continuation system").unwrap(),
            ModelMessage::user("save active continuation").unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    for case in &cases {
        let events = run_model(
            &model,
            round_zero_request.clone(),
            context_for_identity(
                case.instance_id,
                case.turn_id,
                0,
                case.cancellation.clone(),
                Duration::from_secs(5),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls
            })
        ));
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(cases.iter().all(|case| !case.cancellation.is_cancelled()));

    let probe_request = |case: &ActiveCase| {
        let tool_call_id = ToolCallId::new(&case.call_id).unwrap();
        let tool_call = ToolCall::new(
            tool_call_id.clone(),
            "read".parse().unwrap(),
            serde_json::from_str(&case.arguments).unwrap(),
            0,
        )
        .unwrap();
        ModelRequest::new(
            vec![
                ModelMessage::system("active continuation system").unwrap(),
                ModelMessage::user("save active continuation").unwrap(),
                ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
                ModelMessage::tool_with_outcome(
                    tool_call_id,
                    ToolOutput::new(format!("fresh active output {}", case.index)).unwrap(),
                    ToolResultOutcome::Success,
                )
                .unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap()
    };
    let oldest = &cases[0];
    let newest = &cases[CASE_COUNT - 1];
    run_model(
        &model,
        probe_request(oldest),
        context_for_identity(
            oldest.instance_id,
            oldest.turn_id,
            1,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();
    run_model(
        &model,
        probe_request(newest),
        context_for_identity(
            newest.instance_id,
            newest.turn_id,
            1,
            CancellationToken::new(),
            Duration::from_secs(5),
        ),
    )
    .await
    .unwrap();

    let normalized_input = |case: &ActiveCase| {
        json!([
            {
                "type": "message",
                "role": "developer",
                "content": [{
                    "type": "input_text",
                    "text": "active continuation system"
                }]
            },
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "save active continuation"}]
            },
            {
                "type": "function_call",
                "call_id": case.call_id.clone(),
                "name": "read",
                "arguments": format!(r#"{{"path":"active-{:03}.txt"}}"#, case.index),
                "status": "completed"
            },
            {
                "type": "function_call_output",
                "call_id": case.call_id.clone(),
                "output": format!("fresh active output {}", case.index),
                "status": "completed"
            }
        ])
    };
    let exact_input = |case: &ActiveCase| {
        json!([
            {
                "type": "message",
                "role": "developer",
                "content": [{
                    "type": "input_text",
                    "text": "active continuation system"
                }]
            },
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "save active continuation"}]
            },
            {
                "type": "reasoning",
                "id": format!("rs_active_{:03}", case.index),
                "encrypted_content": format!("enc-active-{:03}", case.index),
                "summary": [],
                "status": "completed",
                "provider": {"ordinal": case.index}
            },
            {
                "type": "function_call",
                "id": format!("fc_active_{:03}", case.index),
                "call_id": case.call_id.clone(),
                "name": "read",
                "arguments": case.arguments.clone(),
                "status": "completed",
                "provider": {"ordinal": case.index, "opaque": "active-bound"}
            },
            {
                "type": "function_call_output",
                "call_id": case.call_id.clone(),
                "output": format!("fresh active output {}", case.index),
                "status": "completed"
            }
        ])
    };
    let requests = server.finish().await;
    assert_eq!(requests.len(), 259);
    let oldest_probe = requests[257].json_body();
    let newest_probe = requests[258].json_body();
    assert_eq!(oldest_probe["store"], false);
    assert_eq!(newest_probe["store"], false);
    assert_eq!(
        (oldest_probe["input"].clone(), newest_probe["input"].clone()),
        (normalized_input(oldest), exact_input(newest)),
        "active continuation bound must evict only the oldest Turn"
    );
}

#[tokio::test]
async fn continuation_item_count_overflow_fails_without_replaying_partial_items() {
    const CALL_ID: &str = "call-a4-t07";
    const FUNCTION_ARGUMENTS: &str = r#"{"path":"bound.txt"}"#;

    let mut overflow_events = Vec::with_capacity(258);
    for output_index in 0..255_u32 {
        overflow_events.push(json!({
            "type": "response.output_item.done",
            "output_index": output_index,
            "item": {
                "type": "reasoning",
                "id": format!("rs_a4_t07_{output_index:03}"),
                "encrypted_content": format!("enc-a4-t07-{output_index:03}"),
                "summary": [],
                "status": "completed",
                "provider": {"ordinal": output_index}
            }
        }));
    }
    overflow_events.push(json!({
        "type": "response.output_item.done",
        "output_index": 255,
        "item": {
            "type": "function_call",
            "id": "fc_a4_t07",
            "call_id": CALL_ID,
            "name": "read",
            "arguments": FUNCTION_ARGUMENTS,
            "status": "completed",
            "provider": {"opaque": "must-not-be-replayed-after-overflow"}
        }
    }));
    overflow_events.push(json!({
        "type": "response.output_item.done",
        "output_index": 256,
        "item": {
            "type": "reasoning",
            "id": "rs_a4_t07_overflow",
            "encrypted_content": "enc-a4-t07-overflow",
            "summary": [],
            "status": "completed",
            "provider": {"ordinal": 256}
        }
    }));
    assert_eq!(overflow_events.len(), 257);
    overflow_events.push(completed(usage()));

    let server = MockServer::spawn([
        MockResponse::sse(&overflow_events),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let round_zero_request = ModelRequest::new(
        vec![
            ModelMessage::system("A4-T07 system").unwrap(),
            ModelMessage::user("exercise the continuation item bound").unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let mut stream = model
        .start(
            round_zero_request,
            context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    let mut round_zero_events = Vec::new();
    let mut round_zero_error = None;
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) if round_zero_error.is_none() => round_zero_events.push(event),
            Ok(_) => panic!("model stream emitted an event after its terminal error"),
            Err(error) if round_zero_error.is_none() => round_zero_error = Some(error),
            Err(_) => panic!("model stream emitted more than one terminal error"),
        }
    }
    drop(stream);

    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("A4-T07 system").unwrap(),
            ModelMessage::user("exercise the continuation item bound").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh bounded tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "A4-T07 system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "exercise the continuation item bound"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-a4-t07",
            "name": "read",
            "arguments": r#"{"path":"bound.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t07",
            "output": "fresh bounded tool output",
            "status": "completed"
        }
    ]);
    let second_input = second_body["input"].as_array().unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::ToolCallStart {
            tool_call_id: ToolCallId::new(CALL_ID).unwrap(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(
            ToolCallId::new(CALL_ID).unwrap(),
            FUNCTION_ARGUMENTS,
        )
        .unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: ToolCallId::new(CALL_ID).unwrap(),
        },
    ];
    let observed_error = round_zero_error
        .as_ref()
        .map(|error| (error.kind(), error.delivery()));
    let opaque_replay_count = second_input
        .iter()
        .filter(|item| item["type"] == "reasoning")
        .count();
    let raw_provider_function_call_count = second_input
        .iter()
        .filter(|item| item["id"] == "fc_a4_t07")
        .count();
    assert_eq!(
        (
            round_zero_events,
            observed_error,
            second_body["input"] == expected_normalized_input,
            opaque_replay_count,
            raw_provider_function_call_count,
        ),
        (
            expected_round_zero_events,
            Some((
                ModelErrorKind::InvalidProviderResponse,
                DeliveryState::Started,
            )),
            true,
            0,
            0,
        ),
        "the function call must complete before item 257 fails without partial replay"
    );
}

#[tokio::test]
async fn continuation_raw_byte_overflow_fails_without_replaying_partial_items() {
    const CALL_ID: &str = "call-a4-t07-bytes";
    const FUNCTION_ARGUMENTS: &str = r#"{"path":"byte-bound.txt"}"#;
    const EXPECTED_MAX_CONTINUATION_BYTES: usize = 4 * 1024 * 1024;

    let mut raw_items = vec![json!({
        "type": "function_call",
        "id": "fc_a4_t07_bytes",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {"opaque": "must-not-be-replayed-after-byte-overflow"}
    })];
    let opaque_payload = "x".repeat(899_900);
    for output_index in 1..=5_u32 {
        raw_items.push(json!({
            "type": "reasoning",
            "id": format!("rs_a4_t07_bytes_{output_index}"),
            "encrypted_content": format!("enc-{output_index}:{opaque_payload}"),
            "summary": [],
            "status": "completed",
            "provider": {"opaque": format!("provider-{output_index}")}
        }));
    }
    assert_eq!(raw_items.len(), 6);
    let total_raw_bytes = raw_items
        .iter()
        .map(|item| serde_json::to_vec(item).unwrap().len())
        .sum::<usize>();
    assert!(
        total_raw_bytes > EXPECTED_MAX_CONTINUATION_BYTES,
        "fixture raw items must exceed the 4 MiB continuation byte bound"
    );

    let mut overflow_events = raw_items
        .iter()
        .enumerate()
        .map(|(output_index, item)| {
            json!({
                "type": "response.output_item.done",
                "output_index": output_index,
                "item": item.clone()
            })
        })
        .collect::<Vec<_>>();
    overflow_events.push(completed(usage()));
    let response_chunks = overflow_events
        .iter()
        .map(|event| {
            let frame = sse_body(std::slice::from_ref(event));
            assert!(
                frame.len() < MAX_SSE_FRAME_BYTES,
                "each fixture SSE event must remain below the existing 1 MiB frame limit"
            );
            frame.into_bytes()
        })
        .collect::<Vec<_>>();

    let server = MockServer::spawn([
        MockResponse::sse_bytes(Vec::new())
            .with_chunks(response_chunks)
            .with_chunk_delay(Duration::from_millis(1)),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round one complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let mut byte_bound_settings = settings(server.base_url());
    byte_bound_settings.effective_context_window = 2_000_000;
    let model = OpenAiResponsesModel::new(byte_bound_settings).unwrap();
    let round_zero_request = ModelRequest::new(
        vec![
            ModelMessage::system("A4-T07 byte-bound system").unwrap(),
            ModelMessage::user("exercise the continuation byte bound").unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(2_000_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    let mut stream = model
        .start(
            round_zero_request,
            context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
        )
        .await
        .unwrap();
    let mut round_zero_events = Vec::new();
    let mut round_zero_error = None;
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) if round_zero_error.is_none() => round_zero_events.push(event),
            Ok(_) => panic!("model stream emitted an event after its terminal error"),
            Err(error) if round_zero_error.is_none() => round_zero_error = Some(error),
            Err(_) => panic!("model stream emitted more than one terminal error"),
        }
    }
    drop(stream);

    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let round_one_request = ModelRequest::new(
        vec![
            ModelMessage::system("A4-T07 byte-bound system").unwrap(),
            ModelMessage::user("exercise the continuation byte bound").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh byte-bound tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(2_000_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_one_request,
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "A4-T07 byte-bound system"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "exercise the continuation byte bound"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-a4-t07-bytes",
            "name": "read",
            "arguments": r#"{"path":"byte-bound.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-a4-t07-bytes",
            "output": "fresh byte-bound tool output",
            "status": "completed"
        }
    ]);
    let second_input = second_body["input"].as_array().unwrap();
    let expected_round_zero_events = vec![
        ModelEvent::ToolCallStart {
            tool_call_id: ToolCallId::new(CALL_ID).unwrap(),
            tool_name: "read".parse().unwrap(),
        },
        ModelEvent::tool_call_arguments_delta(
            ToolCallId::new(CALL_ID).unwrap(),
            FUNCTION_ARGUMENTS,
        )
        .unwrap(),
        ModelEvent::ToolCallEnd {
            tool_call_id: ToolCallId::new(CALL_ID).unwrap(),
        },
    ];
    let observed_error = round_zero_error
        .as_ref()
        .map(|error| (error.kind(), error.delivery()));
    let opaque_replay_count = second_input
        .iter()
        .filter(|item| item["type"] == "reasoning")
        .count();
    let raw_provider_function_call_count = second_input
        .iter()
        .filter(|item| item["id"] == "fc_a4_t07_bytes")
        .count();
    assert_eq!(
        (
            round_zero_events,
            observed_error,
            second_body["input"] == expected_normalized_input,
            opaque_replay_count,
            raw_provider_function_call_count,
        ),
        (
            expected_round_zero_events,
            Some((
                ModelErrorKind::InvalidProviderResponse,
                DeliveryState::Started,
            )),
            true,
            0,
            0,
        ),
        "continuation raw bytes over 4 MiB must fail without preserving partial replay"
    );
}

#[tokio::test]
async fn compacted_request_prunes_unmatched_replay_before_later_round() {
    const CALL_A: &str = "call-stale-a";
    const CALL_B: &str = "call-current-b";
    const ARGUMENTS_A: &str = r#"{ "path": "a.txt" }"#;
    const ARGUMENTS_B: &str = r#"{ "path": "b.txt" }"#;

    let reasoning_a = json!({
        "type": "reasoning",
        "id": "rs_stale_a",
        "encrypted_content": "enc::stale-a::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare tool A"}],
        "status": "completed",
        "provider": {"opaque": "stale-a-must-be-pruned"}
    });
    let function_call_a = json!({
        "type": "function_call",
        "id": "fc_stale_a",
        "call_id": CALL_A,
        "name": "read",
        "arguments": ARGUMENTS_A,
        "status": "completed",
        "provider": {"opaque": "raw-a-must-not-return"}
    });
    let reasoning_b = json!({
        "type": "reasoning",
        "id": "rs_current_b",
        "encrypted_content": "enc::current-b::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare tool B"}],
        "status": "completed",
        "provider": {"opaque": "current-b-must-remain"}
    });
    let function_call_b = json!({
        "type": "function_call",
        "id": "fc_current_b",
        "call_id": CALL_B,
        "name": "read",
        "arguments": ARGUMENTS_B,
        "status": "completed",
        "provider": {"opaque": "raw-b-must-remain"}
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_a
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_a
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_b
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_b
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round two complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());

    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("stale replay system round zero").unwrap(),
                ModelMessage::user("produce tool A").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert!(matches!(
        round_zero_events.last(),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls
        })
    ));

    let round_one_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("stale replay system round one").unwrap(),
                ModelMessage::user("compacted history produces tool B").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(1, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert!(matches!(
        round_one_events.last(),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls
        })
    ));

    let tool_call_id_a = ToolCallId::new(CALL_A).unwrap();
    let tool_call_a = ToolCall::new(
        tool_call_id_a.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(ARGUMENTS_A).unwrap(),
        0,
    )
    .unwrap();
    let tool_call_id_b = ToolCallId::new(CALL_B).unwrap();
    let tool_call_b = ToolCall::new(
        tool_call_id_b.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(ARGUMENTS_B).unwrap(),
        0,
    )
    .unwrap();
    let round_two_request = ModelRequest::new(
        vec![
            ModelMessage::system("stale replay system round two").unwrap(),
            ModelMessage::user("probe compacted replay storage").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call_a)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id_a,
                ToolOutput::new("fresh output A").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call_b)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id_b,
                ToolOutput::new("fresh output B").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        round_two_request,
        context_for_round(2, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 3);
    let round_one_body = requests[1].json_body();
    assert_eq!(round_one_body["store"], false);
    let expected_round_one_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "stale replay system round one"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "compacted history produces tool B"
            }]
        }
    ]);
    assert_eq!(round_one_body["input"], expected_round_one_input);

    let round_two_body = requests[2].json_body();
    assert_eq!(round_two_body["store"], false);
    let expected_round_two_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "stale replay system round two"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "probe compacted replay storage"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-stale-a",
            "name": "read",
            "arguments": r#"{"path":"a.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-stale-a",
            "output": "fresh output A",
            "status": "completed"
        },
        {
            "type": "reasoning",
            "id": "rs_current_b",
            "encrypted_content": "enc::current-b::AAECAwQFBgc=",
            "summary": [{"type": "summary_text", "text": "prepare tool B"}],
            "status": "completed",
            "provider": {"opaque": "current-b-must-remain"}
        },
        {
            "type": "function_call",
            "id": "fc_current_b",
            "call_id": "call-current-b",
            "name": "read",
            "arguments": r#"{ "path": "b.txt" }"#,
            "status": "completed",
            "provider": {"opaque": "raw-b-must-remain"}
        },
        {
            "type": "function_call_output",
            "call_id": "call-current-b",
            "output": "fresh output B",
            "status": "completed"
        }
    ]);
    assert_eq!(
        round_two_body["input"], expected_round_two_input,
        "saving round B after compaction must prune unmatched exact replay A"
    );
}

#[tokio::test]
async fn skipped_model_round_purges_stale_continuation_before_request() {
    const CALL_ID: &str = "call-skipped-round";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "skipped.txt" }"#;

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_skipped_round",
        "encrypted_content": "enc::skipped-round::AAECAwQFBgc=",
        "summary": [{"type": "summary_text", "text": "prepare skipped round replay"}],
        "status": "completed",
        "provider": {"opaque": "must-be-purged-on-round-gap"}
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_skipped_round",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {"opaque": "raw-call-must-not-cross-round-gap"}
    });
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning_item
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": function_call_item
            }),
            completed(usage()),
        ]),
        MockResponse::sse(&[
            json!({"type": "response.output_text.delta", "delta": "round two complete"}),
            completed(usage()),
        ]),
    ])
    .await;
    let model = model(server.base_url());
    let round_zero_events = run_model(
        &model,
        ModelRequest::new(
            vec![
                ModelMessage::system("skipped round system").unwrap(),
                ModelMessage::user("save round zero replay").unwrap(),
            ],
            vec![read_tool()],
            ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
            ReasoningPreference::High,
        )
        .unwrap(),
        context_for_round(0, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert!(matches!(
        round_zero_events.last(),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls
        })
    ));

    let tool_call_id = ToolCallId::new(CALL_ID).unwrap();
    let tool_call = ToolCall::new(
        tool_call_id.clone(),
        "read".parse().unwrap(),
        serde_json::from_str(FUNCTION_ARGUMENTS).unwrap(),
        0,
    )
    .unwrap();
    let skipped_round_request = ModelRequest::new(
        vec![
            ModelMessage::system("skipped round system").unwrap(),
            ModelMessage::user("probe round two after skipping round one").unwrap(),
            ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)]).unwrap(),
            ModelMessage::tool_with_outcome(
                tool_call_id,
                ToolOutput::new("fresh skipped-round tool output").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap(),
        ],
        vec![read_tool()],
        ModelLimits::new(Some(8_000), Some(1_024)).unwrap(),
        ReasoningPreference::High,
    )
    .unwrap();
    run_model(
        &model,
        skipped_round_request,
        context_for_round(2, CancellationToken::new(), Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_normalized_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "skipped round system"}]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "probe round two after skipping round one"
            }]
        },
        {
            "type": "function_call",
            "call_id": "call-skipped-round",
            "name": "read",
            "arguments": r#"{"path":"skipped.txt"}"#,
            "status": "completed"
        },
        {
            "type": "function_call_output",
            "call_id": "call-skipped-round",
            "output": "fresh skipped-round tool output",
            "status": "completed"
        }
    ]);
    assert_eq!(
        second_body["input"], expected_normalized_input,
        "a skipped model round must purge stale exact continuation before request build"
    );
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
        models: BTreeMap::from([("main".to_owned(), agent_model_config(server.base_url()))]),
        kernel: KernelOverrides::default(),
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
            _ => {
                panic!("MINICORE_AGENT_LIVE_REASONING must be auto, disabled, low, medium, or high")
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
            ReasoningPreference::Low | ReasoningPreference::Medium | ReasoningPreference::High
        ),
        "reasoning Tool smoke requires low, medium, or high reasoning"
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
                SessionId::new().expect("live temp directory ID")
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
    terminal: minicore_runtime::TurnTerminal,
    outcome_usage: Usage,
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
    let outcome = tokio::time::timeout(Duration::from_secs(300), agent.wait_turn(turn))
        .await
        .map_err(|_| "live reasoning/tool Turn timed out")?
        .map_err(|_| "live reasoning/tool Turn wait failed")?;
    let transcript = agent
        .transcript(GetTranscript {
            session_id: session.session_id,
            after: None,
            limit: 32,
        })
        .await
        .map_err(|_| "live Transcript read failed")?;
    let assistants = transcript
        .entries
        .iter()
        .filter_map(|entry| match entry {
            minicore_runtime::ConversationEntry::AssistantMessage(entry)
                if entry.turn_id == turn.turn_id =>
            {
                Some(entry)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let read_call_ids = assistants
        .iter()
        .flat_map(|entry| &entry.tool_calls)
        .filter(|call| call.name().as_str() == "read")
        .map(|call| call.tool_call_id().clone())
        .collect::<BTreeSet<_>>();
    let successful_read_with_token = transcript.entries.iter().any(|entry| {
        matches!(
            entry,
            minicore_runtime::ConversationEntry::ToolResult(result)
                if result.turn_id == turn.turn_id
                    && result.tool_name.as_str() == "read"
                    && result.outcome == ToolResultOutcome::Success
                    && read_call_ids.contains(&result.tool_call_id)
                    && result.content.as_str().contains(LIVE_SMOKE_TOKEN)
        )
    });
    let final_text_with_token = assistants.iter().any(|entry| {
        entry.tool_calls.is_empty()
            && entry
                .text
                .as_ref()
                .is_some_and(|text| text.as_str().contains(LIVE_SMOKE_TOKEN))
    });
    Ok(LiveToolEvidence {
        turn,
        terminal: outcome.terminal,
        outcome_usage: outcome.usage,
        session_reasoning: session.reasoning,
        assistant_rounds: assistants.len(),
        assistant_usages: assistants.iter().map(|entry| entry.usage).collect(),
        saw_read_call: !read_call_ids.is_empty(),
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
                compaction: ProfileCompaction::Disabled,
            },
        )]),
        models: BTreeMap::from([("main".to_owned(), live_model_config(&live))]),
        kernel: KernelOverrides::default(),
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
        evidence.terminal,
        minicore_runtime::TurnTerminal::Completed,
        "live reasoning/tool Turn failed; requested reasoning is not downgraded"
    );
    assert_live_usage(&evidence.outcome_usage);
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
