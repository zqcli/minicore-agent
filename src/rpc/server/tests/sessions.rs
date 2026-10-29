use super::*;

#[tokio::test]
async fn extended_reasoning_round_trips_through_rpc_and_reopen() {
    let mut fixture = RpcFixture::new("extended-reasoning", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);

    let created = harness
        .call_ok(
            "session.create",
            Some(json!({"workspace": workspace, "reasoning": "xhigh"})),
        )
        .await;
    assert_eq!(created["result"]["session"]["reasoning"], json!("xhigh"));
    assert_eq!(
        serde_json::from_value::<ReasoningPreference>(
            created["result"]["session"]["reasoning"].clone()
        )
        .unwrap(),
        ReasoningPreference::XHigh
    );
    let opened = harness.event("session_opened").await;
    assert_eq!(
        opened["params"]["data"]["session"]["reasoning"],
        json!("xhigh")
    );
    let session_id = session_id(&created);

    for (wire, expected) in [
        ("max", ReasoningPreference::Max),
        ("ultra", ReasoningPreference::Ultra),
    ] {
        let request_id = json!(format!("update-{wire}"));
        harness
            .send(
                request_id.clone(),
                "session.update",
                Some(json!({"session_id": session_id, "reasoning": wire})),
            )
            .await;
        let updated = harness.response(request_id).await;
        assert_eq!(updated["result"]["session"]["reasoning"], json!(wire));
        assert_eq!(
            serde_json::from_value::<ReasoningPreference>(
                updated["result"]["session"]["reasoning"].clone()
            )
            .unwrap(),
            expected
        );
    }

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

    let reopened = harness
        .call_ok("session.open", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(reopened["result"]["session"]["reasoning"], json!("ultra"));
    let reopened_event = harness.event("session_opened").await;
    assert_eq!(
        reopened_event["params"]["data"]["session"]["reasoning"],
        json!("ultra")
    );

    harness
        .send(
            json!("close-again"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close-again")).await["result"],
        json!({"ok": true})
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn session_rename_is_a_separate_rpc_and_persists_title_without_config_revision() {
    let mut fixture = RpcFixture::new(
        "rename-basic",
        [Step::Text("done")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let renamed = harness
        .call_ok(
            "session.rename",
            Some(json!({"session_id": session_id, "title": "  Unicode 名称  "})),
        )
        .await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Unicode 名称"));
    assert!(renamed["result"].get("active_revision").is_none());
    assert_eq!(renamed["result"]["session"]["model"], json!("fake"));
    assert_eq!(renamed["result"]["session"]["reasoning"], json!("medium"));

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

    let reopened = harness
        .call_ok("session.open", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(
        reopened["result"]["session"]["title"],
        json!("Unicode 名称")
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn session_rename_supports_unloaded_sessions_and_clears_title() {
    let mut fixture = RpcFixture::new("rename-unloaded", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

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

    let renamed = harness
        .call_ok(
            "session.rename",
            Some(json!({"session_id": session_id, "title": "  Unloaded  "})),
        )
        .await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Unloaded"));
    assert_eq!(renamed["result"]["session"]["loaded"], json!(false));

    let cleared = harness
        .call_ok(
            "session.rename",
            Some(json!({"session_id": session_id, "title": " \t "})),
        )
        .await;
    assert!(cleared["result"]["session"]["title"].is_null());

    let reopened = harness
        .call_ok("session.open", Some(json!({"session_id": session_id})))
        .await;
    assert!(reopened["result"]["session"]["title"].is_null());

    harness.shutdown().await;
}

#[tokio::test]
async fn session_rename_rejects_invalid_titles_and_unknown_fields() {
    let mut fixture = RpcFixture::new("rename-invalid", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let titles = ["bad\nname", "bad\0name", &"x".repeat(4_097)];
    for (index, title) in titles.iter().enumerate() {
        let id = json!(format!("invalid-{index}"));
        harness
            .send(
                id.clone(),
                "session.rename",
                Some(json!({"session_id": session_id, "title": title})),
            )
            .await;
        let response = harness.response(id).await;
        assert_rpc_error(&response, -32602, "invalid_params");
        assert!(!response.to_string().contains("bad"));
    }

    let unknown = harness
        .call(
            "session.rename",
            Some(json!({"session_id": session_id, "title": "ok", "extra": true})),
        )
        .await;
    assert_eq!(unknown["error"]["code"], json!(-32602));

    let missing_id = SessionId::new().unwrap();
    harness
        .call_err(
            "session.rename",
            Some(json!({
                "session_id": missing_id,
                "title": "missing"
            })),
            -32001,
            "session_not_found",
        )
        .await;

    harness.shutdown().await;
}

#[tokio::test]
async fn session_rename_write_failure_does_not_change_loaded_state() {
    let mut fixture = RpcFixture::new("rename-write-failure", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::RECORD_WRITE_FAILURES.arm(typed_id, ());

    let failure = harness
        .call_err(
            "session.rename",
            Some(json!({"session_id": session_id, "title": "not persisted"})),
            -32011,
            "store_error",
        )
        .await;
    assert!(!failure.to_string().contains("not persisted"));

    let listed = harness.call_ok("session.list", Some(json!({}))).await;
    assert_eq!(
        listed["result"]["sessions"][0]["title"],
        json!("rpc session")
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn session_rename_is_allowed_while_a_loop_is_busy() {
    let mut fixture = RpcFixture::new("rename-busy-rpc", [Step::Pending], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "busy"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "Busy title"})),
        )
        .await;
    let renamed = harness.response(json!("rename")).await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Busy title"));
    assert!(renamed["result"].get("active_revision").is_none());

    harness
        .send(
            json!("cancel"),
            "turn.cancel",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"]
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    harness
        .send(
            json!("wait"),
            "turn.wait",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"]
            })),
        )
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["persistence"], json!("persisted"));

    harness.shutdown().await;
}

#[tokio::test]
async fn session_rename_is_allowed_for_a_blocked_session() {
    let mut fixture = RpcFixture::new(
        "rename-blocked-rpc",
        [Step::Text("done")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::APPEND_FAILURES.arm(typed_id, true);

    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "block"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    harness
        .send(
            json!("wait"),
            "turn.wait",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"]
            })),
        )
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "Blocked title"})),
        )
        .await;
    let renamed = harness.response(json!("rename")).await;
    assert_eq!(
        renamed["result"]["session"]["title"],
        json!("Blocked title")
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn create_open_history_and_send_wait_deferred() {
    let mut fixture = RpcFixture::new(
        "flow",
        [Step::Text("complete")],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("presentation"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let presentation = harness.response(json!("presentation")).await;
    assert_eq!(presentation["result"]["session_id"], session_id);
    assert_eq!(presentation["result"]["model_label"], json!("fake"));
    assert_eq!(presentation["result"]["context"]["kind"], json!("unknown"));
    assert!(presentation["result"]["context"]["tokens"].is_null());
    assert!(presentation["result"]["cost_usd"].is_null());
    assert!(presentation["result"]["using_subscription"].is_null());
    assert!(presentation["result"]["git_branch"].is_null());
    assert!(presentation["result"]["last_loop"].is_null());

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    assert_eq!(history["result"]["total"], 0);

    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "hello rpc"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    assert!(sent["result"]["accepted_at"].is_string());
    assert!(sent["result"]["started"]["input"]["accepted_at"].is_string());
    let turn = sent["result"]["started"]["turn"].clone();
    assert_eq!(turn["session_id"], session_id);
    assert!(turn["loop_id"].as_str().unwrap().starts_with("lup_"));

    // Deferred wait must not block other requests.
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let ping = harness.call_ok("agent.ping", Some(json!({}))).await;
    assert_eq!(ping["result"]["version"], env!("CARGO_PKG_VERSION"));

    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited["result"]["persistence"], json!("persisted"));
    assert_eq!(waited["result"]["turn"]["loop_id"], turn["loop_id"]);

    let presentation_after = harness
        .call_ok(
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        presentation_after["result"]["last_loop"]["loop_id"],
        turn["loop_id"]
    );
    assert!(presentation_after["result"]["last_loop"]["started_at"].is_string());
    assert!(presentation_after["result"]["last_loop"]["finished_at"].is_string());

    let history2 = harness
        .call_ok(
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    assert_eq!(history2["result"]["total"], 2);
    assert!(history2["result"]["next_offset"].is_null());

    harness.shutdown().await;
}

#[tokio::test]
async fn push_update_and_cancel_work_while_wait_is_pending() {
    let mut fixture = RpcFixture::new(
        "wait-concurrent",
        [Step::Pending],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let sent = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "hello"})),
        )
        .await;
    let turn = sent["result"]["started"]["turn"].clone();

    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;

    // push while wait pending
    let pushed = harness
        .call_ok(
            "input.push",
            Some(json!({
                "session_id": session_id,
                "mode": "steer",
                "text": "stay on track"
            })),
        )
        .await;
    assert!(pushed["result"]["started"].is_null());
    assert!(pushed["result"]["accepted_at"].is_string());
    assert_eq!(
        pushed["result"]["queue"]["items"][0]["text"],
        "stay on track"
    );

    // session.update while wait pending
    let update = harness
        .call_ok(
            "session.update",
            Some(json!({"session_id": session_id, "reasoning": "high"})),
        )
        .await;
    assert!(update["result"]["active_revision"].is_number());
    assert_eq!(update["result"]["session"]["reasoning"], json!("high"));

    // cancel while wait pending
    let cancel = harness
        .call_ok("turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(cancel["result"]["cancelled"], json!(true));

    harness.shutdown().await;
}

#[tokio::test]
async fn session_update_requires_an_unknown_field_free_shape_and_model_change() {
    let mut fixture = RpcFixture::new(
        "update",
        [Step::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let noop = harness
        .call("session.update", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(noop["error"]["code"], json!(-32602));

    let missing = harness
        .call(
            "session.update",
            Some(json!({"session_id": session_id, "model": "nope"})),
        )
        .await;
    assert_eq!(missing["error"]["code"], json!(-32009));

    harness.shutdown().await;
}
