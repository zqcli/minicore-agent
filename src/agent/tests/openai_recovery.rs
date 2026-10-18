use super::*;

/// Marker carried by every opaque provider replay item in the loopback
/// fixture.
const REPLAY_MARKER: &str = "provider-replay-opaque-";

/// Real-HTTP loopback for provider replay budgeting.
///
/// Both modes answer the tool round with the same opaque replay item. With
/// `preflight` the replay alone exceeds the effective hard budget, so the
/// preparation plan must summarize before sending and the server sees
/// tool -> utility -> recovered. Without it the replaying request is sent and
/// the provider itself rejects it with a structured 400 before recovery
/// summarizes.
async fn openai_overflow_loopback(preflight: bool) -> MockServer {
    let replay_units = if preflight { 3_000 } else { 120 };
    let opaque_replay = REPLAY_MARKER.repeat(replay_units);

    let tool_call_event = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "id": "fc_001",
            "call_id": "call_read_1",
            "name": "read",
            "arguments": "{\"path\":\"file.txt\"}",
            "status": "in_progress"
        }
    });
    let tool_call_done = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "id": "fc_001",
            "call_id": "call_read_1",
            "name": "read",
            "arguments": "{\"path\":\"file.txt\"}",
            "status": "completed",
            "provider": {"opaque": opaque_replay}
        }
    });
    let tool_turn_done = json!({
        "type": "response.completed",
        "response": {
            "status": "completed",
            "output": [{
                "type": "function_call",
                "id": "fc_001",
                "call_id": "call_read_1",
                "name": "read",
                "arguments": "{\"path\":\"file.txt\"}",
                "status": "completed",
                "provider": {"opaque": opaque_replay}
            }]
        }
    });

    let overflow_error_body = json!({
        "error": {
            "message": "This model's maximum context length is 16384 tokens. However, your messages resulted in 17000 tokens.",
            "type": "invalid_request_error",
            "code": "context_length_exceeded"
        }
    });

    let summary_events = vec![
        json!({
            "type": "response.output_text.delta",
            "delta": "Summary of prior read execution"
        }),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Summary of prior read execution"}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "id": "msg_fixture",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Summary of prior read execution"}]
                }]
            }
        }),
    ];

    let recovered_events = vec![
        json!({
            "type": "response.output_text.delta",
            "delta": "Recovered: file contains hello"
        }),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Recovered: file contains hello"}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "id": "msg_fixture",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Recovered: file contains hello"}]
                }]
            }
        }),
    ];

    let tool_move = MockResponse::sse(&[tool_call_event, tool_call_done, tool_turn_done]);
    let responses = if preflight {
        // Ordinary byte estimates do not veto the second request. The server
        // can accept this replay even when the local estimate exceeds budget.
        vec![tool_move, MockResponse::sse(&recovered_events)]
    } else {
        vec![
            tool_move,
            MockResponse::json(400, serde_json::to_vec(&overflow_error_body).unwrap()),
            MockResponse::sse(&summary_events),
            MockResponse::sse(&recovered_events),
        ]
    };
    MockServer::spawn(responses).await
}

/// Extract the single projected conversation from a utility source request.
fn utility_source_payload(text: &str) -> Option<String> {
    let start = text.find("<conversation>\n")? + "<conversation>\n".len();
    let end = text.rfind("</conversation>")?;
    text.get(start..end).map(str::to_owned)
}

/// Drives one agent turn through the loopback fixture and asserts the
/// provider-aware budget invariants shared by both modes.
async fn run_openai_overflow_loopback(preflight: bool) {
    let label = if preflight {
        "p3b2-openai-preflight"
    } else {
        "p3b2-openai-real-loop"
    };
    let (data_dir, _guard) = fixture_dir(label);
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let file_content = "hello ".repeat(800);
    std::fs::write(workspace.join("file.txt"), &file_content).unwrap();

    let server = openai_overflow_loopback(preflight).await;

    let mut m_config = model_config("PATH");
    let ModelConfig::OpenAiResponses {
        base_url,
        physical_context_window,
        ..
    } = &mut m_config;
    *base_url = server.base_url().to_owned();
    *physical_context_window = 16_384;

    let config = auto_config(
        data_dir.to_path_buf(),
        BTreeMap::from([("main".to_owned(), m_config)]),
        read_profile(),
    );

    let mut agent = Agent::open(config).await.unwrap();

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read file").await;
    let result = wait_text(&agent, turn).await;
    if preflight {
        assert_completed(&result);
        assert!(
            agent
                .session_context(info.session_id)
                .unwrap()
                .recovery
                .is_none()
        );
        let history = read_store_history(&data_dir, info.session_id).await;
        assert_eq!(
            history
                .iter()
                .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
                .count(),
            1
        );
        let captured = server.finish().await;
        assert_eq!(
            captured.len(),
            2,
            "tool then accepted ordinary continuation; no threshold utility for its unread result"
        );
        let body = captured[1].json_body();
        assert!(
            serde_json::to_string(&body)
                .unwrap()
                .contains(REPLAY_MARKER)
        );
        assert!(serde_json::to_vec(&body).unwrap().len().div_ceil(4) > 16_384);
        return;
    }
    assert_completed(&result);

    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "Recovered: file contains hello"));
    // Durable history retains provider replay; public/prose views strip it.
    assert!(
        serde_json::to_string(&history)
            .unwrap()
            .contains(REPLAY_MARKER)
    );
    assert!(
        !serde_json::to_string(crate::history::sanitize_history(&history).unwrap().as_ref())
            .unwrap()
            .contains(REPLAY_MARKER)
    );

    let context = agent.session_context(info.session_id).unwrap();
    if preflight {
        // No provider rejection is needed: the plan compressed the over-budget
        // replaying request before it was sent.
        assert!(context.recovery.is_none());
        let automatic = context
            .automatic
            .last
            .expect("preflight plan observation must be retained");
        assert_eq!(automatic.outcome, "compacted");
        let before = automatic.before_tokens.expect("plan before tokens");
        let after = automatic.after_tokens.expect("plan after tokens");
        assert!(after < before, "preflight body must strictly shrink");
        assert!(
            after <= automatic.hard_tokens,
            "preflight body must fit the hard window"
        );
        assert!(automatic.utility_usage.is_some());
    } else {
        let recovery = context.recovery.expect("recovery observation must exist");
        assert_eq!(recovery.outcome, "recovered");
        let before = recovery.before_tokens.expect("recovery before tokens");
        let after = recovery.after_tokens.expect("recovery after tokens");
        assert!(after < before, "recovered body must strictly shrink");
        assert!(recovery.utility_usage.is_some());
    }

    let captured = server.finish().await;
    assert_eq!(captured.len(), if preflight { 3 } else { 4 });
    let bodies = captured
        .iter()
        .map(CapturedRequest::json_body)
        .collect::<Vec<_>>();
    let encoded = |index: usize| serde_json::to_string(&bodies[index]).unwrap();
    // The utility summarizer is the only tool-free request; it carries the
    // source data but never a provider replay.
    let utility = bodies
        .iter()
        .position(|body| body.get("tools").is_none())
        .expect("utility request must be tool-free");
    assert_eq!(utility, if preflight { 1 } else { 2 });
    assert!(encoded(utility).contains("MINICORE HISTORICAL SOURCE DATA"));
    assert!(!encoded(utility).contains(REPLAY_MARKER));
    // Summary projection retains the matching call and result identity while
    // explicitly truncating only the result text, without changing history.
    let utility_payload = bodies[utility]["input"]
        .as_array()
        .expect("utility input array")
        .iter()
        .flat_map(|item| item["content"].as_array().into_iter().flatten())
        .filter_map(|content| content["text"].as_str())
        .filter_map(utility_source_payload)
        .collect::<String>();
    assert_eq!(utility_payload.matches("[Tool result:").count(), 1);
    assert!(
        utility_payload
            .contains("[Tool result: name=read, call_id=call_read_1, outcome=\"success\"]: ")
    );
    let original_output = format!("1: {file_content}");
    let head: String = original_output.chars().take(2000).collect();
    assert!(utility_payload.contains(&head));
    assert!(utility_payload.contains(&format!(
        "[... {} more characters truncated]",
        original_output.chars().count() - 2000
    )));
    assert_eq!(
        utility_payload
            .matches("[Assistant tool call: name=read, call_id=call_read_1]")
            .count(),
        1
    );
    assert!(utility_payload.contains("\"path\":\"file.txt\""));

    let folded = encoded(bodies.len() - 1);
    assert!(folded.contains("MINICORE HISTORICAL SUMMARY DATA"));
    assert!(!folded.contains(REPLAY_MARKER));
    assert!(!folded.contains("\"function_call_output\""));
    // The folded request retains the original user prompt exactly once and
    // does not resend the summarized tool result.
    let folded_input = bodies[bodies.len() - 1]["input"]
        .as_array()
        .expect("input array")
        .clone();
    let repeated_prompt = folded_input
        .iter()
        .filter(|item| item["type"] == json!("message") && item["role"] == json!("user"))
        .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
        .filter(|content| content["text"] == json!("read file"))
        .count();
    assert_eq!(
        repeated_prompt, 1,
        "the original user prompt must appear once"
    );
    if preflight {
        // The over-budget replay body was never sent: only the tool request,
        // the utility call, and the folded request reached the provider.
        assert!(bodies[0].get("tools").is_some());
    } else {
        // The rejected attempt replayed the provider-owned raw item exactly as
        // the provider returned it, and the folded retry strictly shrank.
        let replayed = encoded(1);
        assert!(replayed.contains(REPLAY_MARKER));
        assert!(replayed.contains("\"function_call_output\""));
        let rejected_len = serde_json::to_vec(&bodies[1]).unwrap().len();
        let folded_len = serde_json::to_vec(&bodies[bodies.len() - 1]).unwrap().len();
        assert!(
            folded_len < rejected_len,
            "folded body must strictly shrink"
        );
    }
}

#[tokio::test]
async fn test_openai_loopback_http_overflow_recovery_real_loop() {
    run_openai_overflow_loopback(false).await;
}

#[tokio::test]
async fn test_openai_loopback_accepts_over_estimated_budget_without_local_veto() {
    run_openai_overflow_loopback(true).await;
}

fn emergency_http_tool_response(call_id: &str, path: &str, opaque: &str) -> MockResponse {
    let item = json!({
        "type":"function_call", "id":format!("fc_{call_id}"), "call_id":call_id,
        "name":"read", "arguments":serde_json::to_string(&json!({"path":path})).unwrap(),
        "status":"completed", "provider":{"opaque":opaque},
    });
    MockResponse::sse(&[
        json!({"type":"response.output_item.added", "output_index":0, "item":item}),
        json!({"type":"response.output_item.done", "output_index":0, "item":item}),
        json!({"type":"response.completed", "response":{"status":"completed","output":[item]}}),
    ])
}

fn emergency_http_text_response(text: &str) -> MockResponse {
    let item = json!({"type":"message", "id":"msg_fixture", "role":"assistant", "content":[{"type":"output_text","text":text}]});
    MockResponse::sse(&[
        json!({"type":"response.output_text.delta", "delta":text}),
        json!({"type":"response.output_item.added", "output_index":0, "item":item}),
        json!({"type":"response.completed", "response":{"status":"completed","output":[item]}}),
    ])
}

#[tokio::test]
async fn openai_session_emergency_projection_survives_driver_retry_followups_and_new_growth() {
    let (data_dir, _guard) = fixture_dir("openai-emergency-full-lifecycle");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    for name in ["a", "b", "c"] {
        std::fs::write(
            workspace.join(format!("{name}.txt")),
            format!("RAW-HTTP-{name} ").repeat(400),
        )
        .unwrap();
    }
    let overflow = || {
        MockResponse::json(
            400,
            r#"{"error":{"code":"context_length_exceeded","type":"invalid_request_error"}}"#,
        )
    };
    let server = MockServer::spawn(vec![
        emergency_http_tool_response("call_a", "a.txt", &REPLAY_MARKER.repeat(100)),
        overflow(),
        emergency_http_text_response("SUMMARY-HTTP-A: read a already executed"),
        MockResponse::json(429, r#"{"error":{"code":"rate_limit_exceeded"}}"#)
            .with_header("Retry-After", "0"),
        emergency_http_tool_response("call_b", "b.txt", "opaque-b"),
        emergency_http_tool_response("call_c", "c.txt", "opaque-c"),
        overflow(),
        emergency_http_text_response("SUMMARY-HTTP-B: read b already executed"),
        emergency_http_text_response("all three tools settled"),
    ])
    .await;
    let mut config = model_config("PATH");
    let ModelConfig::OpenAiResponses {
        base_url,
        physical_context_window,
        ..
    } = &mut config;
    *base_url = server.base_url().to_owned();
    *physical_context_window = 16_384;
    let mut agent = Agent::open(auto_config(
        data_dir.clone(),
        BTreeMap::from([("main".to_owned(), config)]),
        read_profile(),
    ))
    .await
    .unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read a, b, c once").await;
    let result = wait_text(&agent, turn).await;
    let captured = server.finish().await;
    assert!(
        matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Completed
        ),
        "{:?}",
        result.report.outcome
    );
    assert_eq!(result.report.tool_rounds, 3);
    assert_eq!(captured.len(), 9);
    let bodies = captured
        .iter()
        .map(CapturedRequest::json_body)
        .collect::<Vec<_>>();
    assert_eq!(
        bodies[3], bodies[4],
        "normal Driver retry must resend the reduced body, not raw history"
    );
    for index in [0, 1, 3, 4, 5, 6, 8] {
        assert_eq!(bodies[index]["store"], json!(false));
        assert_eq!(bodies[index]["truncation"], json!("disabled"));
        assert!(bodies[index].get("context_management").is_none());
        assert!(bodies[index].get("previous_response_id").is_none());
    }
    for index in [3, 4, 5, 6, 8] {
        let body = serde_json::to_string(&bodies[index]).unwrap();
        assert!(
            body.contains("SUMMARY-HTTP-A"),
            "request {index} lost reduced base"
        );
        assert!(!body.contains("RAW-HTTP-a"));
        assert!(!body.contains(REPLAY_MARKER));
    }
    for index in [2, 7] {
        assert!(
            bodies[index].get("tools").is_none(),
            "utility cannot dispatch tools"
        );
        assert!(
            serde_json::to_string(&bodies[index])
                .unwrap()
                .contains("HISTORICAL SOURCE DATA")
        );
    }
    assert!(
        serde_json::to_string(&bodies[8])
            .unwrap()
            .contains("SUMMARY-HTTP-B")
    );
    let rejected = serde_json::to_vec(&bodies[1]).unwrap().len();
    let reduced = serde_json::to_vec(&bodies[3]).unwrap().len();
    assert!(
        reduced < rejected,
        "actual emitted context must strictly shrink"
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(
        stored
            .iter()
            .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
            .count(),
        3
    );
    let raw = serde_json::to_string(&stored).unwrap();
    for name in ["a", "b", "c"] {
        assert!(raw.contains(&format!("RAW-HTTP-{name}")));
    }
    assert!(raw.contains(REPLAY_MARKER));
    assert!(
        !serde_json::to_string(crate::history::sanitize_history(&stored).unwrap().as_ref())
            .unwrap()
            .contains(REPLAY_MARKER)
    );
    agent.close_session(info.session_id).await.unwrap();
}
