use super::*;

fn push_params(session_id: &Value, mode: &str, text: &str) -> Option<Value> {
    Some(json!({"session_id": session_id, "mode": mode, "text": text}))
}

fn texts(queue: &Value) -> Vec<String> {
    queue["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["text"].as_str().unwrap().to_owned())
        .collect()
}

/// Waits until the gated model call has started, bounded so a lost wake-up
/// fails instead of hanging.
async fn started(gate: &Gate) {
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.wait_started())
        .await
        .expect("the gated model call must start");
}

/// A session whose first model call waits on the returned gate.
async fn gated_session(
    label: &str,
    extra: impl IntoIterator<Item = Step>,
) -> (RpcFixture, RpcHarness, Value, Arc<Gate>) {
    let gate = Arc::new(Gate::new());
    let mut scripts = vec![Step::gated_text(gate.clone(), "one")];
    scripts.extend(extra);
    let mut fixture = RpcFixture::new(label, scripts, &["read"], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    (fixture, harness, session_id, gate)
}

#[tokio::test]
async fn an_idle_push_answers_with_the_started_turn_and_its_input() {
    let mut fixture = RpcFixture::new("push-idle", [Step::Text("done")], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let pushed = harness
        .call_ok("input.push", push_params(&session_id, "steer", "hello"))
        .await;
    let result = &pushed["result"];
    let id = result["id"].as_str().unwrap();
    assert!(id.starts_with("inp_") && id.len() == 36, "{id}");
    assert!(result["accepted_at"].is_string());
    assert_eq!(result["started"]["turn"]["session_id"], session_id);
    assert!(
        result["started"]["turn"]["loop_id"]
            .as_str()
            .unwrap()
            .starts_with("lup_")
    );
    assert_eq!(result["started"]["input"]["id"], id);
    assert_eq!(result["started"]["input"]["mode"], "steer");
    assert_eq!(result["started"]["input"]["text"], "hello");
    assert_eq!(
        result["started"]["input"]["accepted_at"],
        result["accepted_at"]
    );
    assert_eq!(result["queue"], json!({"items": [], "paused": false}));

    let turn = result["started"]["turn"].clone();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    assert!(waited["result"]["next"].is_null());
    assert_eq!(
        waited["result"]["queue"],
        json!({"items": [], "paused": false})
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn a_queued_follow_up_becomes_the_wait_result_s_next_turn() {
    let (_fixture, mut harness, session_id, gate) =
        gated_session("push-queued", [Step::Text("two")]).await;
    let first = harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    let turn = first["result"]["started"]["turn"].clone();
    started(&gate).await;

    let queued = harness
        .call_ok(
            "input.push",
            push_params(&session_id, "follow_up", "second"),
        )
        .await;
    assert!(queued["result"]["started"].is_null());
    assert_eq!(texts(&queued["result"]["queue"]), ["second"]);
    assert_eq!(queued["result"]["queue"]["items"][0]["mode"], "follow_up");
    let listed = harness
        .call_ok("input.list", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(listed["result"], queued["result"]["queue"]);
    // Every queue change is announced; the last one carries the entry.
    let event = loop {
        let event = harness.event("input_queue").await;
        if !event["params"]["data"]["queue"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
        {
            break event;
        }
    };
    assert_eq!(event["params"]["data"]["queue"], queued["result"]["queue"]);
    assert_eq!(event["params"]["data"]["meta"]["session_id"], session_id);

    gate.release();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    let next = waited["result"]["next"].clone();
    assert_eq!(next["input"]["id"], queued["result"]["id"]);
    assert_eq!(next["input"]["text"], "second");
    assert_eq!(waited["result"]["queue"]["items"], json!([]));
    let second = harness
        .call_ok("turn.wait", Some(turn_params(&next["turn"])))
        .await;
    assert_eq!(second["result"]["outcome"]["type"], "completed");
    assert!(second["result"]["next"].is_null());
    harness.shutdown().await;
}

#[tokio::test]
async fn a_running_turn_announces_the_inputs_it_took() {
    let gate = Arc::new(Gate::new());
    let mut fixture = RpcFixture::new(
        "push-delivered",
        [
            Step::gated_tool(gate.clone(), "read", json!({"path": "a.txt"})),
            Step::Text("done"),
        ],
        &["read"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let first = harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    let turn = first["result"]["started"]["turn"].clone();
    started(&gate).await;
    let one = harness
        .call_ok("input.push", push_params(&session_id, "steer", "one"))
        .await;
    let two = harness
        .call_ok("input.push", push_params(&session_id, "steer", "two"))
        .await;
    gate.release();

    let delivered = harness.event("input_delivered").await;
    let data = &delivered["params"]["data"];
    assert_eq!(data["turn"], turn);
    assert_eq!(data["items"].as_array().unwrap().len(), 2);
    assert_eq!(data["items"][0]["id"], one["result"]["id"]);
    assert_eq!(data["items"][0]["text"], "one");
    assert_eq!(
        data["items"][0]["accepted_at"],
        one["result"]["accepted_at"]
    );
    assert_eq!(data["items"][1]["id"], two["result"]["id"]);
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert!(waited["result"]["next"].is_null());
    harness.shutdown().await;
}

#[tokio::test]
async fn remove_recall_resume_and_list_answer_with_the_queue() {
    let (_fixture, mut harness, session_id, gate) =
        gated_session("input-methods", [Step::Text("two")]).await;
    let first = harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    let turn = first["result"]["started"]["turn"].clone();
    started(&gate).await;
    let f1 = harness
        .call_ok("input.push", push_params(&session_id, "follow_up", "f1"))
        .await;
    harness
        .call_ok("input.push", push_params(&session_id, "follow_up", "f2"))
        .await;

    let removed = harness
        .call_ok(
            "input.remove",
            Some(json!({"session_id": session_id, "id": f1["result"]["id"]})),
        )
        .await;
    assert_eq!(removed["result"]["text"], "f1");
    assert_eq!(texts(&removed["result"]["queue"]), ["f2"]);
    harness
        .call_err(
            "input.remove",
            Some(json!({"session_id": session_id, "id": f1["result"]["id"]})),
            -32025,
            "input_not_queued",
        )
        .await;

    let recalled = harness
        .call_ok("input.recall", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(recalled["result"]["texts"], json!(["f2"]));
    assert_eq!(
        recalled["result"]["queue"],
        json!({"items": [], "paused": false})
    );

    // A running session has nothing to resume.
    let resumed = harness
        .call_ok("input.resume", Some(json!({"session_id": session_id})))
        .await;
    assert!(resumed["result"]["started"].is_null());

    // A cancel pauses the queue; resume starts its head.
    harness
        .call_ok("input.push", push_params(&session_id, "follow_up", "f3"))
        .await;
    let cancelled = harness
        .call_ok("turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(cancelled["result"]["cancelled"], true);
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert!(waited["result"]["next"].is_null());
    assert_eq!(waited["result"]["queue"]["paused"], true);
    assert_eq!(texts(&waited["result"]["queue"]), ["f3"]);
    gate.release();

    let resumed = harness
        .call_ok("input.resume", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(resumed["result"]["started"]["input"]["text"], "f3");
    assert_eq!(
        resumed["result"]["queue"],
        json!({"items": [], "paused": false})
    );
    let started = resumed["result"]["started"]["turn"].clone();
    let finished = harness
        .call_ok("turn.wait", Some(turn_params(&started)))
        .await;
    assert_eq!(finished["result"]["outcome"]["type"], "completed");
    harness.shutdown().await;
}

#[tokio::test]
async fn the_thirty_third_queued_input_is_refused() {
    let (_fixture, mut harness, session_id, gate) = gated_session("input-bound", []).await;
    harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    started(&gate).await;
    for index in 0..crate::sessions::MAX_QUEUED_INPUTS {
        harness
            .call_ok(
                "input.push",
                push_params(&session_id, "follow_up", &format!("f{index}")),
            )
            .await;
    }
    harness
        .call_err(
            "input.push",
            push_params(&session_id, "steer", "one too many"),
            -32024,
            "input_queue_full",
        )
        .await;
    harness.shutdown().await;
}

#[tokio::test]
async fn close_is_refused_while_inputs_are_queued() {
    let (_fixture, mut harness, session_id, gate) = gated_session("input-close", []).await;
    harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    started(&gate).await;
    harness
        .call_ok("input.push", push_params(&session_id, "follow_up", "later"))
        .await;
    harness
        .call_err(
            "session.close",
            Some(json!({"session_id": session_id})),
            -32003,
            "session_busy",
        )
        .await;
    harness
        .call_ok("input.recall", Some(json!({"session_id": session_id})))
        .await;
    harness
        .call_ok("session.close", Some(json!({"session_id": session_id})))
        .await;
    harness.shutdown().await;
}

#[tokio::test]
async fn input_requests_with_bad_shapes_or_targets_fail_with_their_codes() {
    let mut fixture = RpcFixture::new("input-errors", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    for params in [
        json!({"session_id": session_id, "mode": "later", "text": "x"}),
        json!({"session_id": session_id, "text": "x"}),
        json!({"session_id": session_id, "mode": "steer"}),
        json!({"session_id": session_id, "mode": "steer", "text": ""}),
        json!({"session_id": session_id, "mode": "steer", "text": "x", "extra": 1}),
    ] {
        harness
            .call_err("input.push", Some(params), -32602, "invalid_params")
            .await;
    }
    let unknown = json!("ses_00000000000000000000000000000009");
    harness
        .call_err(
            "input.push",
            push_params(&unknown, "steer", "x"),
            -32002,
            "session_not_loaded",
        )
        .await;
    harness
        .call_err(
            "input.remove",
            Some(json!({"session_id": session_id, "id": "inp_missing"})),
            -32025,
            "input_not_queued",
        )
        .await;
    for removed in ["turn.send", "turn.steer"] {
        harness
            .call_err(
                removed,
                Some(json!({"session_id": session_id, "text": "x"})),
                -32601,
                "method_not_found",
            )
            .await;
    }
    harness.shutdown().await;
}

#[tokio::test]
async fn a_manual_compaction_result_names_the_turn_it_started_or_none() {
    let mut fixture = RpcFixture::new("compact-next", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let result = harness
        .call_ok(
            "session.compact",
            Some(json!({"session_id": session_id, "operation_id": "compact-next"})),
        )
        .await;
    assert_eq!(result["result"]["status"], "noop");
    assert!(result["result"]["next"].is_null());
    harness.shutdown().await;
}
