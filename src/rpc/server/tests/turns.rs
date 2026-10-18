use super::*;

#[tokio::test]
async fn history_view_is_safe_and_pages() {
    let mut fixture = RpcFixture::new(
        "safe-view",
        [tool_calls(vec![ToolCallScript {
            name: "read",
            arguments: json!({"path": "secret.txt"}),
        }])],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let sent = harness
        .call_ok(
            "turn.send",
            Some(json!({"session_id": session_id, "text": "read file"})),
        )
        .await;
    let turn = sent["result"]["turn"].clone();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
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

    let history = harness
        .call_ok(
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let serialized = history["result"].to_string();
    // Whitelisted path detail is available to the local TUI, but raw tool
    // argument object fields are not.
    assert!(serialized.contains("secret.txt"));
    assert!(!serialized.contains("arguments"));
    assert!(!serialized.contains("\"path\""));
    // Tool lifecycle is visible through the safe view.
    let state = harness
        .call_ok("session.state", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(state["result"]["status"], json!("idle"));
    assert!(state["result"]["active_loop"].is_null());

    harness.shutdown().await;
}

#[tokio::test]
async fn persistence_failure_returns_failed_and_blocked_state() {
    let mut fixture = RpcFixture::new(
        "persist-fail",
        [Step::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    crate::store::APPEND_FAILURES.arm(session_id.as_str().unwrap().parse().unwrap(), true);

    let sent = harness
        .call_ok(
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let turn = sent["result"]["turn"].clone();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    let state = harness
        .call_ok("session.state", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(state["result"]["status"], json!("blocked"));
    assert_eq!(state["result"]["block_reason"], json!("persistence"));

    // Blocked sessions refuse further sends.
    let send2 = harness
        .call(
            "turn.send",
            Some(json!({"session_id": session_id, "text": "again"})),
        )
        .await;
    assert_eq!(send2["error"]["code"], json!(-32004));

    harness.shutdown().await;
}

#[tokio::test]
async fn blocked_send_does_not_discard_previous_turn_result_rpc() {
    let mut fixture = RpcFixture::new(
        "blocked-discard",
        [Step::Text("hello")],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    crate::store::APPEND_FAILURES.arm(session_id.as_str().unwrap().parse().unwrap(), true);

    // 1. send first turn
    let sent = harness
        .call_ok(
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let turn = sent["result"]["turn"].clone();

    // 2. Deterministically wait for session to reach blocked state before waiting turn.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let state = harness
            .call_ok("session.state", Some(json!({"session_id": session_id})))
            .await;
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
    let send2 = harness
        .call(
            "turn.send",
            Some(json!({"session_id": session_id, "text": "again"})),
        )
        .await;
    assert_eq!(send2["error"]["code"], json!(-32004));

    // 4. First turn.wait still succeeds (not -32007 turn_not_found)
    let waited1 = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited1["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited1["result"]["persistence"], json!("failed"));

    // 5. Repeated turn.wait also succeeds
    let waited2 = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited2["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited2["result"]["persistence"], json!("failed"));

    harness.shutdown().await;
}

#[tokio::test]
async fn interaction_answer_resolves_an_approval() {
    let mut fixture = RpcFixture::new(
        "approval",
        [
            tool_calls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "out.txt", "content": "approved"}),
            }]),
            Step::Text("wrote it"),
        ],
        &["write"],
        ApprovalMode::Ask,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let sent = harness
        .call_ok(
            "turn.send",
            Some(json!({"session_id": session_id, "text": "write the file"})),
        )
        .await;
    let turn = sent["result"]["turn"].clone();

    let interaction = harness.event("interaction_requested").await;
    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
    assert_eq!(
        interaction["params"]["data"]["interaction"]["kind"]["type"],
        json!("approval")
    );

    let answer = harness
        .call_ok(
            "interaction.answer",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "interaction_id": interaction_id,
                "answer": {"type": "approval", "decision": "allow_once"}
            })),
        )
        .await;
    assert_eq!(answer["result"], json!({"ok": true}));

    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));

    harness.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_and_awaits_an_active_loop() {
    let mut fixture = RpcFixture::new(
        "shutdown-active",
        [Step::Pending],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
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
}

#[tokio::test]
async fn error_mapping_covers_blocked_and_stale_turn() {
    let mut fixture = RpcFixture::new(
        "errors",
        [Step::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    // stale loop id
    let stale = harness
        .call(
            "turn.wait",
            Some(json!({
                "session_id": session_id,
                "loop_id": "lup_00000000000000000000000000000001"
            })),
        )
        .await;
    assert_eq!(stale["error"]["code"], json!(-32007));

    // unknown session
    let missing = harness
        .call(
            "session.state",
            Some(json!({"session_id": "ses_00000000000000000000000000000001"})),
        )
        .await;
    assert_eq!(missing["error"]["code"], json!(-32002));

    harness.shutdown().await;
}
