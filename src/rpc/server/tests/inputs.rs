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
async fn an_idle_push_receipt_identifies_its_turn_in_the_snapshot() {
    let (_fixture, mut harness, session_id, gate) = gated_session("push-idle", []).await;
    let pushed = harness
        .call_ok("input.push", push_params(&session_id, "steer", "hello"))
        .await;
    let receipt = &pushed["result"];
    let id = receipt["id"].as_str().unwrap();
    assert!(id.starts_with("inp_") && id.len() == 36, "{id}");
    assert!(receipt["accepted_at"].is_string());
    assert_eq!(receipt["accepted"], true);
    assert!(receipt.get("started").is_none());
    assert!(receipt.get("queue").is_none());
    let turn = harness.turn_for_input(receipt).await;
    assert_eq!(turn["session_id"], session_id);
    assert!(turn["loop_id"].as_str().unwrap().starts_with("lup_"));
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session_id})))
        .await;
    let snapshot = &state["result"]["snapshot"];
    assert_eq!(snapshot["load"], receipt["load"]);
    let input = &snapshot["execution"]["input"];
    assert_eq!(input["id"], id);
    assert_eq!(input["mode"], "steer");
    assert_eq!(input["text"], "hello");
    assert_eq!(input["accepted_at"], receipt["accepted_at"]);
    assert_eq!(snapshot["queue"], json!({"items":[],"paused":false}));
    gate.release();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    assert!(waited["result"].get("next").is_none());
    assert!(waited["result"].get("queue").is_none());
    harness.shutdown().await;
}

#[tokio::test]
async fn a_queued_follow_up_is_identified_by_snapshots_after_its_predecessor() {
    let (_fixture, mut harness, session_id, gate) =
        gated_session("push-queued", [Step::Text("two")]).await;
    let first = harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    let turn = harness.turn_for_input(&first["result"]).await;
    started(&gate).await;
    let queued = harness
        .call_ok(
            "input.push",
            push_params(&session_id, "follow_up", "second"),
        )
        .await;
    assert_eq!(queued["result"]["accepted"], true);
    let listed = harness
        .call_ok("input.list", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(texts(&listed["result"]), ["second"]);
    assert_eq!(listed["result"]["items"][0]["mode"], "follow_up");
    assert_eq!(listed["result"]["items"][0]["id"], queued["result"]["id"]);
    let event = harness
        .event_matching("session_snapshot", |frame| {
            let snapshot = &frame["params"]["data"]["snapshot"];
            snapshot["load"] == queued["result"]["load"]
                && snapshot["queue"]["items"][0]["id"] == queued["result"]["id"]
        })
        .await;
    let snapshot = &event["params"]["data"]["snapshot"];
    assert_eq!(snapshot["queue"], listed["result"]);
    assert_eq!(snapshot["session_id"], session_id);
    gate.release();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    let next = harness.turn_for_input(&queued["result"]).await;
    assert_ne!(next["loop_id"], turn["loop_id"]);
    let second = harness.call_ok("turn.wait", Some(turn_params(&next))).await;
    assert_eq!(second["result"]["outcome"]["type"], "completed");
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(queue["result"], json!({"items":[],"paused":false}));
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
    let turn = harness.turn_for_input(&first["result"]).await;
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
    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    harness.shutdown().await;
}

#[tokio::test]
async fn remove_recall_and_resume_leave_the_queue_in_the_snapshot_and_list() {
    let (_fixture, mut harness, session_id, gate) =
        gated_session("input-methods", [Step::Text("two")]).await;
    let first = harness
        .call_ok("input.push", push_params(&session_id, "steer", "first"))
        .await;
    let turn = harness.turn_for_input(&first["result"]).await;
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
            Some(json!({"session_id":session_id,"id":f1["result"]["id"]})),
        )
        .await;
    assert_eq!(removed["result"], json!({"text":"f1"}));
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(texts(&queue["result"]), ["f2"]);
    harness
        .call_err(
            "input.remove",
            Some(json!({"session_id":session_id,"id":f1["result"]["id"]})),
            -32025,
            "input_not_queued",
        )
        .await;
    let recalled = harness
        .call_ok("input.recall", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(recalled["result"], json!({"texts":["f2"]}));
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(queue["result"], json!({"items":[],"paused":false}));
    let resumed = harness
        .call_ok("input.resume", Some(json!({"session_id":session_id})))
        .await;
    assert!(resumed["result"]["start_error"].is_null());
    let f3 = harness
        .call_ok("input.push", push_params(&session_id, "follow_up", "f3"))
        .await;
    let cancelled = harness
        .call_ok("turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(cancelled["result"]["cancelled"], true);
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], "cancelled");
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(queue["result"]["paused"], true);
    assert_eq!(texts(&queue["result"]), ["f3"]);
    gate.release();
    let resumed = harness
        .call_ok("input.resume", Some(json!({"session_id":session_id})))
        .await;
    assert!(resumed["result"]["start_error"].is_null());
    let started = harness.turn_for_input(&f3["result"]).await;
    let finished = harness
        .call_ok("turn.wait", Some(turn_params(&started)))
        .await;
    assert_eq!(finished["result"]["outcome"]["type"], "completed");
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id":session_id})))
        .await;
    assert_eq!(queue["result"], json!({"items":[],"paused":false}));
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
            Some(json!({"session_id":unknown,"load":"load_unknown","mode":"steer","text":"x"})),
            -32027,
            "stale_load",
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
async fn a_manual_compaction_result_reports_only_its_own_outcome() {
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
    assert!(result["result"].get("next").is_none());
    harness.shutdown().await;
}

#[tokio::test]
async fn w0_accepted_push_reports_failure_for_the_older_claimed_head() {
    let gate = Arc::new(Gate::new());
    let mut fixture = RpcFixture::new(
        "w0-accepted-start-failure",
        [
            Step::gated_text(gate.clone(), "first"),
            Step::Text("unused"),
        ],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release = ReleaseGate(gate.clone());
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let first = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "first"}),
                load.as_ref(),
            ),
        )
        .await;
    let turn = harness.turn_for_input(&first["result"]).await;
    started(&gate).await;
    let older = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "follow_up", "text": "older"}),
                load.as_ref(),
            ),
        )
        .await;
    harness
        .call_ok("turn.cancel", w0_params(turn_params(&turn), load.as_ref()))
        .await;
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["outcome"]["type"], "cancelled");
    let paused = harness
        .call_ok("input.list", Some(json!({"session_id":session})))
        .await;
    assert_eq!(paused["result"]["paused"], true);
    gate.release();

    let typed_session: SessionId = session.as_str().unwrap().parse().unwrap();
    crate::sessions::FAIL_RUNTIME_START.arm(typed_session, ());
    let pushed = harness
        .call(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "follow_up", "text": "newer"}),
                load.as_ref(),
            ),
        )
        .await;
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id": session})))
        .await;
    harness.shutdown().await;

    assert_eq!(texts(&queue["result"]), ["older", "newer"]);
    assert_eq!(queue["result"]["paused"], true);
    assert_eq!(
        pushed["result"]["accepted"],
        true,
        "an accepted push needs a success receipt; error kind was {:?}",
        pushed.pointer("/error/data/kind")
    );
    assert_eq!(pushed["result"]["id"], queue["result"]["items"][1]["id"]);
    assert_eq!(
        pushed["result"]["start_error"]["input_id"],
        older["result"]["id"]
    );
    assert!(pushed["result"]["start_error"]["kind"].is_string());
    assert!(pushed["result"].get("started").is_none());
    assert!(pushed["result"].get("queue").is_none());
}

#[tokio::test]
async fn w0_stale_load_cannot_remove_a_reused_input_id_after_reopen() {
    let old_gate = Arc::new(Gate::new());
    let new_gate = Arc::new(Gate::new());
    let mut fixture = RpcFixture::new(
        "w0-stale-load",
        [
            Step::gated_text(old_gate.clone(), "old"),
            Step::gated_text(new_gate.clone(), "new"),
        ],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release_old = ReleaseGate(old_gate.clone());
    let _release_new = ReleaseGate(new_gate.clone());
    let (session, old_load) = w0_create_session(&mut harness, &workspace).await;
    let old_receipt = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "old first"}),
                old_load.as_ref(),
            ),
        )
        .await;
    let old_turn = harness.turn_for_input(&old_receipt["result"]).await;
    started(&old_gate).await;
    let old_input = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "follow_up", "text": "old queued"}),
                old_load.as_ref(),
            ),
        )
        .await["result"]["id"]
        .clone();
    harness
        .call_ok(
            "input.recall",
            w0_params(json!({"session_id": session}), old_load.as_ref()),
        )
        .await;
    harness
        .call_ok(
            "turn.cancel",
            w0_params(turn_params(&old_turn), old_load.as_ref()),
        )
        .await;
    harness
        .call_ok("turn.wait", Some(turn_params(&old_turn)))
        .await;
    harness
        .call_ok(
            "session.close",
            w0_params(json!({"session_id": session}), old_load.as_ref()),
        )
        .await;
    old_gate.release();

    let opened = harness
        .call_ok("session.open", Some(json!({"session_id": session})))
        .await;
    let new_load = w0_load(&opened);
    if let (Some(old), Some(new)) = (&old_load, &new_load) {
        assert_ne!(old, new);
    }
    harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "new first"}),
                new_load.as_ref(),
            ),
        )
        .await;
    started(&new_gate).await;
    let new_input = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "follow_up", "text": "new queued"}),
                new_load.as_ref(),
            ),
        )
        .await["result"]["id"]
        .clone();
    assert_eq!(
        old_input, new_input,
        "the test must exercise an ID reused after reopen"
    );
    let stale = old_load.unwrap_or_else(|| json!("load_stale"));
    let removed = harness
        .call(
            "input.remove",
            Some(json!({"session_id": session, "load": stale, "id": old_input})),
        )
        .await;
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id": session})))
        .await;
    harness.shutdown().await;

    assert_eq!(queue["result"]["items"][0]["id"], new_input);
    assert_eq!(
        removed.pointer("/error/data/kind").and_then(Value::as_str),
        Some("stale_load")
    );
}

#[tokio::test]
async fn w0_claimed_input_is_observable_as_starting_before_runtime_installation() {
    let mut fixture = RpcFixture::new(
        "w0-starting-claim",
        [Step::Pending],
        &[],
        ApprovalMode::Auto,
    );
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let gate = Arc::new(Gate::new());
    let _release = ReleaseGate(gate.clone());
    super::super::OPERATION_GATES.arm(base.join("data"), gate.clone());
    harness
        .send(
            json!("starting-push"),
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "claimed"}),
                load.as_ref(),
            ),
        )
        .await;
    started(&gate).await;
    let state = harness
        .call_ok("session.state", Some(json!({"session_id": session})))
        .await;
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id": session})))
        .await;
    gate.release();
    let pushed = harness.response(json!("starting-push")).await;
    harness.shutdown().await;

    assert_eq!(queue["result"]["items"], json!([]));
    let execution = &state["result"]["snapshot"]["execution"];
    assert_eq!(
        execution["type"], "starting",
        "the claimed input disappeared from observable execution"
    );
    assert_eq!(execution["input"]["id"], pushed["result"]["id"]);
    assert_eq!(execution["input"]["text"], "claimed");
}

#[tokio::test]
async fn w0_close_cannot_orphan_an_accepted_starting_claim() {
    let mut fixture = RpcFixture::new("w0-close-claim", [Step::Pending], &[], ApprovalMode::Auto);
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let gate = Arc::new(Gate::new());
    let _release = ReleaseGate(gate.clone());
    super::super::OPERATION_GATES.arm(base.join("data"), gate.clone());
    harness
        .send(
            json!("claimed-push"),
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "claimed"}),
                load.as_ref(),
            ),
        )
        .await;
    started(&gate).await;
    harness
        .send(
            json!("close-claim"),
            "session.close",
            w0_params(json!({"session_id": session}), load.as_ref()),
        )
        .await;
    harness.send(json!("after-close"), "agent.ping", None).await;
    harness.response(json!("after-close")).await;
    gate.release();
    let pushed = harness.response(json!("claimed-push")).await;
    let close = harness.response(json!("close-claim")).await;
    assert_eq!(close["error"]["data"]["kind"], "session_busy");
    assert_eq!(pushed["result"]["accepted"], true);
    let turn = harness.turn_for_input(&pushed["result"]).await;
    assert_eq!(turn["session_id"], session);
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session})))
        .await;
    assert_eq!(
        state["result"]["snapshot"]["execution"]["input"]["id"],
        pushed["result"]["id"]
    );
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w0_successor_cannot_be_observed_before_its_predecessor_completion() {
    let first_gate = Arc::new(Gate::new());
    let second_gate = Arc::new(Gate::new());
    let publication_gate = Arc::new(Gate::new());
    let mut fixture = RpcFixture::new(
        "w0-successor-publication",
        [
            Step::gated_text(first_gate.clone(), "first"),
            Step::gated_text(second_gate.clone(), "second"),
        ],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release_first = ReleaseGate(first_gate.clone());
    let _release_second = ReleaseGate(second_gate.clone());
    let _release_publication = ReleaseGate(publication_gate.clone());
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let first = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "first"}),
                load.as_ref(),
            ),
        )
        .await;
    let turn = harness.turn_for_input(&first["result"]).await;
    started(&first_gate).await;
    let queued = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "follow_up", "text": "second"}),
                load.as_ref(),
            ),
        )
        .await;
    harness
        .send(json!("wait-first"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness
        .send(json!("wait-registered"), "agent.ping", None)
        .await;
    harness.response(json!("wait-registered")).await;
    let typed_session: SessionId = session.as_str().unwrap().parse().unwrap();
    crate::sessions::PAUSE_AFTER_SUCCESSOR_INSTALL.arm(typed_session, publication_gate.clone());
    first_gate.release();
    started(&publication_gate).await;
    started(&second_gate).await;
    let during = harness
        .call_ok("session.state", Some(json!({"session_id": session})))
        .await;
    publication_gate.release();
    let waited = harness.response(json!("wait-first")).await;
    let after_wait = harness
        .call_ok("session.state", Some(json!({"session_id": session})))
        .await;
    second_gate.release();
    harness.shutdown().await;

    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    let next = &during["result"]["snapshot"]["execution"];
    assert_eq!(next["input"]["id"], queued["result"]["id"]);
    let next_id = &next["loop_id"];
    assert!(next_id.is_string());
    assert_ne!(*next_id, turn["loop_id"]);
    for state in [&during, &after_wait] {
        let entry = state
            .pointer("/result/snapshot/completions/entries")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry["loop_id"] == turn["loop_id"])
            });
        assert!(
            entry.is_some(),
            "the successor was visible without its predecessor in the completion directory"
        );
        assert_eq!(entry.unwrap()["availability"], "stored");
    }
}

#[tokio::test]
async fn w1_all_loaded_mutations_reject_missing_and_stale_load_before_effects() {
    let gate = Arc::new(Gate::new());
    let mut fixture = RpcFixture::new(
        "w1-load-mutations",
        [Step::gated_text(gate.clone(), "answer")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release = ReleaseGate(gate.clone());
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let load = load.unwrap();
    let first = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"load":load,"mode":"steer","text":"first"})),
        )
        .await;
    let turn = harness.turn_for_input(&first["result"]).await;
    started(&gate).await;
    let queued = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"load":load,"mode":"follow_up","text":"queued"})),
        )
        .await;
    let interaction = minicore_runtime::InteractionId::new().unwrap();
    let cases = [
        (
            "input.push",
            json!({"session_id":session,"mode":"follow_up","text":"other"}),
        ),
        (
            "input.remove",
            json!({"session_id":session,"id":queued["result"]["id"]}),
        ),
        ("input.recall", json!({"session_id":session})),
        ("input.resume", json!({"session_id":session})),
        ("turn.cancel", turn_params(&turn)),
        (
            "interaction.answer",
            json!({"session_id":session,"loop_id":turn["loop_id"],"interaction_id":interaction,"answer":{"type":"approval","decision":"deny"}}),
        ),
        ("session.close", json!({"session_id":session})),
        (
            "session.compact",
            json!({"session_id":session,"operation_id":"w1-load"}),
        ),
        (
            "session.compact.cancel",
            json!({"session_id":session,"operation_id":"w1-load"}),
        ),
        (
            "session.update",
            json!({"session_id":session,"reasoning":"low"}),
        ),
        (
            "session.rename",
            json!({"session_id":session,"title":"changed"}),
        ),
    ];
    for (method, params) in cases {
        for (value, expected) in [
            (None, "missing_load"),
            (Some(json!("load_stale")), "stale_load"),
        ] {
            let response = harness
                .call_unmodified(method, w0_params(params.clone(), value.as_ref()))
                .await;
            assert_eq!(response["error"]["data"]["kind"], expected, "{method}");
            if method == "input.push" {
                assert_eq!(response["error"]["data"]["acceptance"], "rejected");
            }
        }
    }
    let queue = harness
        .call_ok("input.list", Some(json!({"session_id":session})))
        .await;
    assert_eq!(queue["result"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(queue["result"]["items"][0]["id"], queued["result"]["id"]);
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session})))
        .await;
    assert_eq!(
        state["result"]["snapshot"]["execution"]["loop_id"],
        turn["loop_id"]
    );
    harness
        .call_ok(
            "input.recall",
            Some(json!({"session_id":session,"load":load})),
        )
        .await;
    harness
        .call_ok(
            "turn.cancel",
            Some(json!({"session_id":session,"load":load,"loop_id":turn["loop_id"]})),
        )
        .await;
    harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    harness.shutdown().await;
}
