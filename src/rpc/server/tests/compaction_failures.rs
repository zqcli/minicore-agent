use super::*;

#[tokio::test]
async fn manual_compaction_rejects_busy_and_blocked_sessions() {
    let mut fixture = RpcFixture::new("manual-admission", [Step::Pending], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "busy"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-busy"
            })),
        )
        .await;
    let busy = harness.response(json!("compact")).await;
    assert_eq!(busy["error"]["data"]["kind"], json!("session_busy"));

    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    // A persistence failure blocks the Session, and compaction must preserve
    // the existing blocked admission semantics.
    let mut blocked_fixture = RpcFixture::new(
        "manual-blocked",
        [Step::Text("done")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace2 = blocked_fixture.workspace.clone();
    let mut blocked = RpcHarness::spawn(blocked_fixture.open().await);
    let blocked_id = create_and_open(&mut blocked, &workspace2).await;
    crate::store::APPEND_FAILURES.arm(blocked_id.as_str().unwrap().parse().unwrap(), true);
    blocked
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": blocked_id, "text": "block"})),
        )
        .await;
    let blocked_turn = blocked.response(json!("send")).await["result"]["turn"].clone();
    blocked
        .send(json!("wait"), "turn.wait", Some(turn_params(&blocked_turn)))
        .await;
    let _ = blocked.response(json!("wait")).await;
    blocked
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": blocked_id,
                "operation_id": "compact-blocked"
            })),
        )
        .await;
    let rejected = blocked.response(json!("compact")).await;
    assert_eq!(rejected["error"]["data"]["kind"], json!("session_blocked"));

    harness.shutdown().await;
    blocked.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_cancel_is_exact_and_keeps_ping_responsive() {
    let mut fixture = RpcFixture::new(
        "manual-cancel",
        [Step::Text("settled"), Step::Pending, Step::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(4_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-1"
            })),
        )
        .await;
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    harness
        .send(
            json!("duplicate"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-2"
            })),
        )
        .await;
    let duplicate = harness.response(json!("duplicate")).await;
    assert_eq!(duplicate["error"]["data"]["kind"], json!("session_busy"));

    harness
        .send(
            json!("reused-id"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-1"
            })),
        )
        .await;
    let reused = harness.response(json!("reused-id")).await;
    assert_eq!(
        reused["error"]["data"],
        json!({"kind": "invalid_state", "retryable": false})
    );

    harness
        .send(
            json!("busy-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "not admitted"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("busy-send")).await["error"]["data"]["kind"],
        json!("session_busy")
    );
    harness
        .send(
            json!("busy-update"),
            "session.update",
            Some(json!({"session_id": session_id, "reasoning": "high"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("busy-update")).await["error"]["data"]["kind"],
        json!("session_busy")
    );

    harness
        .send(
            json!("wrong"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-old"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("wrong")).await["result"]["cancelled"],
        json!(false)
    );
    harness
        .send(
            json!("cancel"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-1"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("cancelled"));

    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_keeps_owned_handle_until_delayed_worker_drain() {
    let mut fixture = RpcFixture::new(
        "manual-owned-handle",
        [Step::Text("settled"), Step::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    let gate = Arc::new(Gate::new());
    PAUSE_AFTER_COMPACTION_RESULT.arm(
        session_id.as_str().unwrap().parse().unwrap(),
        Arc::clone(&gate),
    );
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-owned-handle"
            })),
        )
        .await;
    gate.wait_started().await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(
        result["result"]["status"],
        json!("compacted"),
        "unexpected manual compaction result: {result}"
    );

    harness
        .send(
            json!("send-busy"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "must wait"})),
        )
        .await;
    let pending = tokio::time::timeout(
        Duration::from_millis(25),
        harness.response(json!("send-busy")),
    )
    .await;
    assert!(
        pending.is_err(),
        "next work must wait for the owned worker join"
    );
    gate.release();
    assert!(harness.response(json!("send-busy")).await["result"]["turn"].is_object());
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_write_failure_preserves_history_and_old_snapshot() {
    let mut fixture = RpcFixture::new(
        "manual-write-failure",
        [
            Step::TextWithUsage("settled", 2, 3),
            Step::TextWithUsage("initial summary", 5, 7),
            Step::TextWithUsage("new turn answer", 11, 13),
            Step::TextWithUsage("new summary", 17, 19),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;
    let session_dir = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap());
    let history_path = session_dir.join("history.jsonl");
    let initial = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-initial"
            })),
        )
        .await;
    assert_eq!(
        initial["result"]["status"],
        json!("compacted"),
        "unexpected initial compaction result: {initial}"
    );
    let summary_path = session_dir.join("summary.json");
    let summary_before = std::fs::read(&summary_path).unwrap();

    let new_turn_text = "new turn history ".repeat(128);
    harness
        .send(
            json!("next-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": new_turn_text})),
        )
        .await;
    let next_turn = harness.response(json!("next-send")).await["result"]["turn"].clone();
    let _ = harness
        .call("turn.wait", Some(turn_params(&next_turn)))
        .await;
    append_compaction_tail(&mut harness, &base, &session_id).await;
    let history_before = std::fs::read(&history_path).unwrap();
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::SUMMARY_WRITE_FAILURES.arm(typed_id, ());

    let result = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-write-failure"
            })),
        )
        .await;
    assert_eq!(
        result["result"]["status"],
        json!("failed"),
        "unexpected write-failure compaction result: {result}"
    );
    assert_eq!(result["result"]["failure_kind"], json!("store"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(17)
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_before);

    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_rejects_model_failures_and_tool_events() {
    let mut fixture = RpcFixture::new(
        "manual-model-failure",
        [Step::Text("settled"), Step::Fail(fake_model_error())],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-model-failure"
            })),
        )
        .await;
    let failure = harness.response(json!("compact")).await;
    assert_eq!(failure["result"]["status"], json!("failed"));
    assert_eq!(failure["result"]["failure_kind"], json!("model_failure"));
    harness.shutdown().await;

    let mut fixture = RpcFixture::new(
        "manual-tool-event",
        [
            Step::Text("settled"),
            tool_calls(vec![ToolCallScript {
                name: "read",
                arguments: json!({}),
            }]),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-tool-event"
            })),
        )
        .await;
    let failure = harness.response(json!("compact")).await;
    assert_eq!(failure["result"]["status"], json!("failed"));
    assert_eq!(
        failure["result"]["failure_kind"],
        json!("tool_call_rejected")
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_reports_unknown_write_without_publishing_state() {
    let mut fixture = RpcFixture::new(
        "manual-unknown-write",
        [Step::Text("settled"), Step::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;
    crate::store::SUMMARY_UNKNOWN_WRITES.arm(session_id.as_str().unwrap().parse().unwrap(), ());
    let result = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-unknown-write"
            })),
        )
        .await;
    assert_eq!(
        result["result"]["status"],
        json!("unknown_write"),
        "unexpected unknown-write compaction result: {result}"
    );
    assert_eq!(
        result["result"]["failure_kind"],
        json!("write_outcome_unknown")
    );
    assert!(
        !base
            .join("data")
            .join("sessions")
            .join(session_id.as_str().unwrap())
            .join("summary.json")
            .exists()
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_revalidates_history_before_commit() {
    let mut fixture = RpcFixture::new(
        "manual-revalidate",
        [Step::Text("settled"), Step::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .compaction_model();
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let newline = history_before
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("settled history has one complete line");
    let first_line = history_before[..=newline].to_vec();
    let gate = Arc::new(crate::test_support::Gate::new());
    crate::store::SUMMARY_COMMIT_GATES.arm(
        session_id.as_str().unwrap().parse().unwrap(),
        Arc::clone(&gate),
    );
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-revalidate"
            })),
        )
        .await;
    if tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .is_err()
    {
        gate.release();
        let response = tokio::time::timeout(TIMEOUT, harness.response(json!("compact"))).await;
        panic!("manual compaction did not reach commit gate: {response:?}");
    }
    let mut mutated = history_before.clone();
    mutated.extend(first_line);
    std::fs::write(&history_path, mutated).unwrap();
    gate.release();
    let result = harness.response(json!("compact")).await;
    assert_eq!(
        result["result"]["status"],
        json!("failed"),
        "unexpected history revalidation result: {result}"
    );
    assert_eq!(result["result"]["failure_kind"], json!("history_changed"));
    assert!(!history_path.parent().unwrap().join("summary.json").exists());

    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_deadline_covers_commit_wait() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-deadline-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.loop_options.model_timeout_seconds = Some(1);
    let model = fake_model(40_000, [Step::Text("settled"), Step::Text("summary")]);
    let agent = Agent::open_with_models(config, test_models(model))
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    let gate = Arc::new(crate::test_support::Gate::new());
    crate::store::SUMMARY_COMMIT_GATES.arm(
        session_id.as_str().unwrap().parse().unwrap(),
        Arc::clone(&gate),
    );
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-deadline"
            })),
        )
        .await;
    gate.wait_started().await;
    let result = tokio::time::timeout(Duration::from_secs(3), harness.response(json!("compact")))
        .await
        .expect("commit wait must be bounded by the operation deadline");
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("timeout"));
    gate.release();

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_rejects_incomplete_late_empty_and_oversize_output() {
    assert_manual_failure_case(
        "manual-no-finish",
        finished(&["incomplete"], None),
        "invalid_model_response",
    )
    .await;
    assert_manual_failure_case(
        "manual-wrong-finish",
        finished(&["wrong finish"], Some(ModelFinishReason::Length)),
        "invalid_model_response",
    )
    .await;
    assert_manual_failure_case(
        "manual-late-content",
        late_content(),
        "invalid_model_response",
    )
    .await;
    let duplicate = assert_manual_failure_case(
        "manual-duplicate-usage",
        usage_events(
            "duplicate usage",
            &[Usage::new(7, 11, 0), Usage::new(13, 17, 0)],
        ),
        "invalid_model_response",
    )
    .await;
    assert_eq!(duplicate["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(
        duplicate["result"]["utility_usage"]["complete"],
        json!(false)
    );
    assert!(duplicate["result"]["utility_usage"]["usage"].is_null());
    assert_manual_failure_case(
        "manual-whitespace",
        finished(
            &[" 
	"],
            Some(ModelFinishReason::Stop),
        ),
        "no_progress",
    )
    .await;
    assert_manual_failure_case("manual-oversize", oversize(), "too_large").await;
}
