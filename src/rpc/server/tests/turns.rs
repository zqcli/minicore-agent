use super::*;

#[tokio::test]
async fn history_view_is_safe_and_pages() {
    let (agent, base, workspace) = test_agent(
        "safe-view",
        [ModelScript::ToolCalls(vec![ToolCallScript {
            name: "read",
            arguments: json!({"path": "secret.txt"}),
        }])],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "read file"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    let presentation_event = harness.event("tool_presentation").await;
    assert_eq!(
        presentation_event["params"]["data"]["turn"]["session_id"],
        session_id
    );
    assert_eq!(
        presentation_event["params"]["data"]["request_index"],
        json!(0)
    );
    assert_eq!(
        presentation_event["params"]["data"]["display"]["detail"],
        json!("secret.txt")
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    // Whitelisted path detail is available to the local TUI, but raw tool
    // argument object fields are not.
    assert!(serialized.contains("secret.txt"));
    assert!(!serialized.contains("arguments"));
    assert!(!serialized.contains("\"path\""));
    // Tool lifecycle is visible through the safe view.
    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert_eq!(state["result"]["status"], json!("idle"));
    assert!(state["result"]["active_loop"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn persistence_failure_returns_failed_and_blocked_state() {
    let (agent, base, workspace) = test_agent(
        "persist-fail",
        [ModelScript::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    crate::store::APPEND_FAILURES.arm(session_id.as_str().unwrap().parse().unwrap(), true);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert_eq!(state["result"]["status"], json!("blocked"));
    assert_eq!(state["result"]["block_reason"], json!("persistence"));

    // Blocked sessions refuse further sends.
    harness
        .send(
            json!("send2"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "again"})),
        )
        .await;
    let send2 = harness.response(json!("send2")).await;
    assert_eq!(send2["error"]["code"], json!(-32004));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn blocked_send_does_not_discard_previous_turn_result_rpc() {
    let (agent, base, workspace) = test_agent(
        "blocked-discard",
        [ModelScript::Text("hello")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    crate::store::APPEND_FAILURES.arm(session_id.as_str().unwrap().parse().unwrap(), true);

    // 1. send first turn
    harness
        .send(
            json!("send1"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let sent = harness.response(json!("send1")).await;
    let turn = sent["result"]["turn"].clone();

    // 2. Deterministically wait for session to reach blocked state before waiting turn.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        harness
            .send(
                json!("poll_state"),
                "session.state",
                Some(json!({"session_id": session_id})),
            )
            .await;
        let state = harness.response(json!("poll_state")).await;
        if state["result"]["status"] == json!("blocked") {
            assert_eq!(state["result"]["block_reason"], json!("persistence"));
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "session did not reach blocked; final state: {:?}",
                state["result"]
            );
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
    }

    // 3. Second send fails with -32004 session_blocked
    harness
        .send(
            json!("send2"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "again"})),
        )
        .await;
    let send2 = harness.response(json!("send2")).await;
    assert_eq!(send2["error"]["code"], json!(-32004));

    // 4. First turn.wait still succeeds (not -32007 turn_not_found)
    harness
        .send(json!("wait1"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited1 = harness.response(json!("wait1")).await;
    assert_eq!(waited1["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited1["result"]["persistence"], json!("failed"));

    // 5. Repeated turn.wait also succeeds
    harness
        .send(json!("wait2"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited2 = harness.response(json!("wait2")).await;
    assert_eq!(waited2["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited2["result"]["persistence"], json!("failed"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn interaction_answer_resolves_an_approval() {
    let (agent, base, workspace) = test_agent(
        "approval",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "out.txt", "content": "approved"}),
            }]),
            ModelScript::Text("wrote it"),
        ],
        &["write"],
        ApprovalMode::Ask,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "write the file"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();

    let interaction = harness.event("interaction_requested").await;
    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
    assert_eq!(
        interaction["params"]["data"]["interaction"]["kind"]["type"],
        json!("approval")
    );

    harness
        .send(
            json!("answer"),
            "interaction.answer",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "interaction_id": interaction_id,
                "answer": {"type": "approval", "decision": "allow_once"}
            })),
        )
        .await;
    let answer = harness.response(json!("answer")).await;
    assert_eq!(answer["result"], json!({"ok": true}));

    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn shutdown_cancels_and_awaits_an_active_loop() {
    let (agent, base, workspace) = test_agent(
        "shutdown-active",
        [ModelScript::Block],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    harness.response(json!("send")).await;
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn error_mapping_covers_blocked_and_stale_turn() {
    let (agent, base, workspace) = test_agent(
        "errors",
        [ModelScript::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    // stale loop id
    harness
        .send(
            json!("wait-stale"),
            "turn.wait",
            Some(json!({
                "session_id": session_id,
                "loop_id": "lup_00000000000000000000000000000001"
            })),
        )
        .await;
    let stale = harness.response(json!("wait-stale")).await;
    assert_eq!(stale["error"]["code"], json!(-32007));

    // unknown session
    harness
        .send(
            json!("state-missing"),
            "session.state",
            Some(json!({"session_id": "ses_00000000000000000000000000000001"})),
        )
        .await;
    let missing = harness.response(json!("state-missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32002));

    harness.shutdown().await;
    remove_base(&base).await;
}
