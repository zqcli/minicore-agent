use super::*;

#[tokio::test]
async fn extended_reasoning_round_trips_through_rpc_and_reopen() {
    let (agent, base, workspace) =
        test_agent("extended-reasoning", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);

    harness
        .send(
            json!("create-xhigh"),
            "session.create",
            Some(json!({"workspace": workspace, "reasoning": "xhigh"})),
        )
        .await;
    let created = harness.response(json!("create-xhigh")).await;
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

    harness
        .send(
            json!("reopen"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let reopened = harness.response(json!("reopen")).await;
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
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_is_a_separate_rpc_and_persists_title_without_config_revision() {
    let (agent, base, workspace) = test_agent(
        "rename-basic",
        [ModelScript::Text("done")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "  Unicode 名称  "})),
        )
        .await;
    let renamed = harness.response(json!("rename")).await;
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

    harness
        .send(
            json!("reopen"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let reopened = harness.response(json!("reopen")).await;
    assert_eq!(
        reopened["result"]["session"]["title"],
        json!("Unicode 名称")
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_supports_unloaded_sessions_and_clears_title() {
    let (agent, base, workspace) = test_agent("rename-unloaded", [], &[], ApprovalMode::Auto).await;
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

    harness
        .send(
            json!("rename-unloaded"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "  Unloaded  "})),
        )
        .await;
    let renamed = harness.response(json!("rename-unloaded")).await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Unloaded"));
    assert_eq!(renamed["result"]["session"]["loaded"], json!(false));

    harness
        .send(
            json!("clear"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": " \t "})),
        )
        .await;
    let cleared = harness.response(json!("clear")).await;
    assert!(cleared["result"]["session"]["title"].is_null());

    harness
        .send(
            json!("open"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let reopened = harness.response(json!("open")).await;
    assert!(reopened["result"]["session"]["title"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_rejects_invalid_titles_and_unknown_fields() {
    let (agent, base, workspace) = test_agent("rename-invalid", [], &[], ApprovalMode::Auto).await;
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
        assert_eq!(response["error"]["code"], json!(-32602));
        assert_eq!(response["error"]["data"]["kind"], json!("invalid_params"));
        assert!(!response.to_string().contains("bad"));
    }

    harness
        .send(
            json!("unknown"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "ok", "extra": true})),
        )
        .await;
    let unknown = harness.response(json!("unknown")).await;
    assert_eq!(unknown["error"]["code"], json!(-32602));

    let missing_id = SessionId::new().unwrap();
    harness
        .send(
            json!("missing"),
            "session.rename",
            Some(json!({
                "session_id": missing_id,
                "title": "missing"
            })),
        )
        .await;
    let missing = harness.response(json!("missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32001));
    assert_eq!(missing["error"]["data"]["kind"], json!("session_not_found"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_write_failure_does_not_change_loaded_state() {
    let (agent, base, workspace) =
        test_agent("rename-write-failure", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::RECORD_WRITE_FAILURES.arm(typed_id, ());

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "not persisted"})),
        )
        .await;
    let failure = harness.response(json!("rename")).await;
    assert_eq!(failure["error"]["code"], json!(-32011));
    assert_eq!(failure["error"]["data"]["kind"], json!("store_error"));
    assert!(!failure.to_string().contains("not persisted"));

    harness
        .send(json!("list"), "session.list", Some(json!({})))
        .await;
    let listed = harness.response(json!("list")).await;
    assert_eq!(
        listed["result"]["sessions"][0]["title"],
        json!("rpc session")
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_is_allowed_while_a_loop_is_busy() {
    let (agent, base, workspace) = test_agent(
        "rename-busy-rpc",
        [ModelScript::Block],
        &[],
        ApprovalMode::Auto,
    )
    .await;
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
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_is_allowed_for_a_blocked_session() {
    let (agent, base, workspace) = test_agent(
        "rename-blocked-rpc",
        [ModelScript::Text("done")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::APPEND_FAILURES.arm(typed_id, true);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "block"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
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
    remove_base(&base).await;
}

#[tokio::test]
async fn create_open_history_and_send_wait_deferred() {
    let (agent, base, workspace) = test_agent(
        "flow",
        [ModelScript::Text("complete")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
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
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello rpc"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    assert!(sent["result"]["accepted_at"].is_string());
    let turn = sent["result"]["turn"].clone();
    assert_eq!(turn["session_id"], session_id);
    assert!(turn["loop_id"].as_str().unwrap().starts_with("lup_"));

    // Deferred wait must not block other requests.
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    let ping = harness.response(json!("ping")).await;
    assert_eq!(ping["result"]["version"], env!("CARGO_PKG_VERSION"));

    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited["result"]["persistence"], json!("persisted"));
    assert_eq!(waited["result"]["turn"]["loop_id"], turn["loop_id"]);

    harness
        .send(
            json!("presentation-after"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let presentation_after = harness.response(json!("presentation-after")).await;
    assert_eq!(
        presentation_after["result"]["last_loop"]["loop_id"],
        turn["loop_id"]
    );
    assert!(presentation_after["result"]["last_loop"]["started_at"].is_string());
    assert!(presentation_after["result"]["last_loop"]["finished_at"].is_string());

    harness
        .send(
            json!("history2"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history2 = harness.response(json!("history2")).await;
    assert_eq!(history2["result"]["total"], 2);
    assert!(history2["result"]["next_offset"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn steer_and_update_and_cancel_work_while_wait_is_pending() {
    let (agent, base, workspace) = test_agent(
        "wait-concurrent",
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
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();

    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;

    // steer while wait pending
    harness
        .send(
            json!("steer"),
            "turn.steer",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "text": "stay on track"
            })),
        )
        .await;
    let steer = harness.response(json!("steer")).await;
    assert_eq!(steer["result"]["ok"], json!(true));
    assert!(steer["result"]["accepted_at"].is_string());

    // session.update while wait pending
    harness
        .send(
            json!("update"),
            "session.update",
            Some(json!({"session_id": session_id, "reasoning": "high"})),
        )
        .await;
    let update = harness.response(json!("update")).await;
    assert!(update["result"]["active_revision"].is_number());
    assert_eq!(update["result"]["session"]["reasoning"], json!("high"));

    // cancel while wait pending
    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    let cancel = harness.response(json!("cancel")).await;
    assert_eq!(cancel["result"]["cancelled"], json!(true));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_update_requires_an_unknown_field_free_shape_and_model_change() {
    let (agent, base, workspace) = test_agent(
        "update",
        [ModelScript::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("noop"),
            "session.update",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let noop = harness.response(json!("noop")).await;
    assert_eq!(noop["error"]["code"], json!(-32602));

    harness
        .send(
            json!("missing-model"),
            "session.update",
            Some(json!({"session_id": session_id, "model": "nope"})),
        )
        .await;
    let missing = harness.response(json!("missing-model")).await;
    assert_eq!(missing["error"]["code"], json!(-32009));

    harness.shutdown().await;
    remove_base(&base).await;
}
