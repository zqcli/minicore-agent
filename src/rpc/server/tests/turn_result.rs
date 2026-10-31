use super::*;

#[tokio::test]
async fn compact_saved_identity_survives_history_append_and_stateless_close_fallback() {
    let body = "x".repeat(16 * 1024);
    let mut fixture = RpcFixture::new(
        "compact-stored-identity",
        [
            Step::Events(vec![
                ModelEvent::text_delta(&body).unwrap(),
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ]),
            Step::Text("second"),
        ],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    let first = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"mode":"steer","text":"first"})),
        )
        .await;
    let turn = first["result"]["started"]["turn"].clone();
    harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    let page = harness
        .call_ok(
            "turn.result",
            Some(json!({"session_id":session,"loop_id":turn["loop_id"],"max_bytes":1024})),
        )
        .await;
    let result_id = page["result"]["result_id"].clone();
    let cursor = page["result"]["next_cursor"].clone();
    assert!(cursor.is_object());
    let wrong=harness.call("turn.result",Some(json!({"session_id":session,"loop_id":turn["loop_id"],"cursor":cursor,"result_id":format!("r1:{}","0".repeat(64)),"max_bytes":1024}))).await;
    assert_eq!(wrong["error"]["data"]["kind"], "stale_result");
    let next = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"mode":"steer","text":"second"})),
        )
        .await;
    harness
        .call_ok(
            "turn.wait",
            Some(turn_params(&next["result"]["started"]["turn"])),
        )
        .await;
    let after=harness.call_ok("turn.result",Some(json!({"session_id":session,"loop_id":turn["loop_id"],"cursor":cursor,"result_id":result_id,"max_bytes":1024}))).await;
    assert_eq!(after["result"]["result_id"], result_id);
    assert_eq!(after["result"]["availability"], "stored");
    assert_eq!(after["result"]["total"], page["result"]["total"]);
    harness
        .call_ok("session.close", Some(json!({"session_id":session})))
        .await;
    let cold=harness.call_ok("turn.result",Some(json!({"session_id":session,"loop_id":turn["loop_id"],"cursor":cursor,"result_id":result_id,"max_bytes":1024}))).await;
    assert_eq!(cold["result"]["result_id"], result_id);
    let warm = &after["result"]["items"][0];
    let cold = &cold["result"]["items"][0];
    for field in ["index", "offset", "total_bytes", "encoding"] {
        assert_eq!(warm[field], cold[field]);
    }
    assert!(
        cold["data"]
            .as_str()
            .unwrap()
            .starts_with(warm["data"].as_str().unwrap())
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn turn_result_reports_pending_and_stored_availability() {
    let model_started = Arc::new(Notify::new());
    let mut fixture = RpcFixture::new(
        "turn-result-lifecycle",
        [
            block_with_signal(Arc::clone(&model_started)),
            Step::Text("stored answer"),
        ],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send-blocked"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "wait"})),
        )
        .await;
    let blocked_turn =
        harness.response(json!("send-blocked")).await["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, model_started.notified())
        .await
        .expect("blocked model request must be started before querying its result");

    harness
        .send(
            json!("pending"),
            "turn.result",
            Some(json!({
                "session_id": blocked_turn["session_id"],
                "loop_id": blocked_turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let pending = harness.response(json!("pending")).await;
    assert_eq!(pending["result"]["availability"], json!("pending"));

    harness
        .send(
            json!("cancel-blocked"),
            "turn.cancel",
            Some(turn_params(&blocked_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel-blocked")).await["result"]["cancelled"],
        json!(true)
    );
    let _ = harness
        .call("turn.wait", Some(turn_params(&blocked_turn)))
        .await;

    harness
        .send(
            json!("send-stored"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "persist"})),
        )
        .await;
    let stored_turn =
        harness.response(json!("send-stored")).await["result"]["started"]["turn"].clone();
    harness
        .send(
            json!("wait-stored"),
            "turn.wait",
            Some(turn_params(&stored_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("wait-stored")).await["result"]["persistence"],
        json!("persisted")
    );
    let _ = harness
        .call("session.close", Some(json!({"session_id": session_id})))
        .await;

    let stored = harness
        .call_ok(
            "turn.result",
            Some(json!({
                "session_id": stored_turn["session_id"],
                "loop_id": stored_turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    assert_eq!(stored["result"]["availability"], json!("stored"));
    assert_eq!(stored["result"]["persistence"], json!("persisted"));
    assert!(stored["result"]["items"].as_array().unwrap().len() >= 2);

    harness.shutdown().await;
}

#[tokio::test]
async fn turn_result_can_continue_from_live_to_stored_items() {
    let mut fixture = RpcFixture::new(
        "turn-result-live-stored-pages",
        [oversize()],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "live"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;

    let first = harness
        .call_ok(
            "turn.result",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    assert_eq!(first["result"]["availability"], json!("live"));
    assert!(first["result"]["next_cursor"].is_object());
    let result_id = first["result"]["result_id"].clone();
    assert!(result_id.is_string());

    let mut cursor = first["result"]["next_cursor"].clone();
    let mut reconstructed = BTreeMap::<usize, String>::new();
    for item in first["result"]["items"].as_array().unwrap() {
        let index = item["index"].as_u64().unwrap() as usize;
        let offset = item["offset"].as_u64().unwrap() as usize;
        let entry = reconstructed.entry(index).or_default();
        assert_eq!(entry.len(), offset);
        entry.push_str(item["data"].as_str().unwrap());
    }

    let _ = harness
        .call("session.close", Some(json!({"session_id": session_id})))
        .await;

    let mut pages = 1;
    while !cursor.is_null() {
        let request_id = json!(format!("stored-{pages}"));
        harness
            .send(
                request_id.clone(),
                "turn.result",
                Some(json!({
                    "session_id": turn["session_id"],
                    "loop_id": turn["loop_id"],
                    "cursor": cursor,
                    "limit": 100,
                    "max_bytes": 4_096,
                })),
            )
            .await;
        let page = harness.response(request_id).await;
        assert_eq!(page["result"]["availability"], json!("stored"));
        assert_eq!(page["result"]["result_id"], result_id);
        for item in page["result"]["items"].as_array().unwrap() {
            let index = item["index"].as_u64().unwrap() as usize;
            let offset = item["offset"].as_u64().unwrap() as usize;
            let entry = reconstructed.entry(index).or_default();
            assert_eq!(entry.len(), offset);
            entry.push_str(item["data"].as_str().unwrap());
            if item["complete"].as_bool().unwrap() {
                assert_eq!(entry.len(), item["total_bytes"].as_u64().unwrap() as usize);
            }
        }
        cursor = page["result"]["next_cursor"].clone();
        pages += 1;
        assert!(pages < 128, "turn result pagination did not make progress");
    }
    assert!(pages > 1);
    assert_eq!(reconstructed.len(), 2);

    harness.shutdown().await;
}

#[tokio::test]
async fn turn_result_reads_failed_live_report_without_history() {
    let mut fixture = RpcFixture::new(
        "turn-result-failed-append",
        [Step::Text("answer retained in memory")],
        &[],
        ApprovalMode::Auto,
    );
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_session_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::APPEND_FAILURES.arm(typed_session_id, true);

    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "failed persistence"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    std::fs::remove_file(history_path).unwrap();
    let result = harness
        .call_ok(
            "turn.result",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    assert_eq!(result["result"]["availability"], json!("live"));
    assert_eq!(result["result"]["persistence"], json!("failed"));
    assert_eq!(result["result"]["outcome"]["type"], json!("completed"));
    assert!(result.to_string().contains("answer retained in memory"));

    harness.shutdown().await;
}

#[tokio::test]
async fn deferred_query_capacity_is_four_with_shared_total_limit() {
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::channel(1);
    let mut server = RpcServer {
        agent: None,
        outbound_tx,
        waiters: tokio::task::JoinSet::new(),
        queries: tokio::task::JoinSet::new(),
        operations: tokio::task::JoinSet::new(),
        reservations: std::collections::HashSet::new(),
        query_cancellation: CancellationToken::new(),
    };
    for _ in 0..MAX_DEFERRED_QUERIES {
        server.queries.spawn(async { pending::<()>().await });
    }
    assert!(!server.query_capacity_available());

    server.queries.abort_all();
    while server.queries.join_next().await.is_some() {}

    for _ in 0..(MAX_DEFERRED_QUERIES - 1) {
        server.queries.spawn(async { pending::<()>().await });
    }
    for _ in 0..(MAX_DEFERRED_WAITERS - (MAX_DEFERRED_QUERIES - 1)) {
        server.waiters.spawn(async { pending::<()>().await });
    }
    assert!(!server.query_capacity_available());

    server.queries.abort_all();
    server.waiters.abort_all();
    while server.queries.join_next().await.is_some() {}
    while server.waiters.join_next().await.is_some() {}
}

fn w0_collect_chunks(
    page: &Value,
    items: &mut BTreeMap<usize, String>,
    completed: &mut BTreeSet<usize>,
) {
    for chunk in page["items"].as_array().unwrap() {
        assert_eq!(chunk["encoding"], "utf8_json");
        let index = chunk["index"].as_u64().unwrap() as usize;
        assert!(!completed.contains(&index));
        let offset = chunk["offset"].as_u64().unwrap() as usize;
        let item = items.entry(index).or_default();
        assert_eq!(item.len(), offset);
        item.push_str(chunk["data"].as_str().unwrap());
        if chunk["complete"].as_bool().unwrap() {
            assert_eq!(item.len(), chunk["total_bytes"].as_u64().unwrap() as usize);
            completed.insert(index);
        }
    }
}

#[tokio::test]
async fn w0_rolled_back_result_remains_pageable_after_resume_without_entering_model_history() {
    let first_gate = Arc::new(Gate::new());
    let second_gate = Arc::new(Gate::new());
    let observed_users = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = observed_users.clone();
    let successor_gate = second_gate.clone();
    let successor = Step::Custom(Box::new(move |request, _| {
        let users = request
            .messages()
            .iter()
            .filter_map(|message| match message {
                ModelMessage::User(text) => {
                    let text: &str = text.as_ref();
                    Some(text.to_owned())
                }
                _ => None,
            })
            .collect();
        *observed.lock().unwrap() = users;
        Box::pin(async move {
            successor_gate.pass().await;
            Ok(text_stream("saved successor"))
        })
    }));
    let chunk = "answer-a-".repeat(512);
    let first = Step::Gated(
        first_gate.clone(),
        Box::new(finished(&[&chunk, &chunk], Some(ModelFinishReason::Stop))),
    );
    let mut fixture = RpcFixture::new(
        "w0-unsaved-after-resume",
        [first, successor],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release_first = ReleaseGate(first_gate.clone());
    let _release_second = ReleaseGate(second_gate.clone());
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let typed_session: SessionId = session.as_str().unwrap().parse().unwrap();
    crate::store::APPEND_FAILURES.arm(typed_session, false);

    let pushed = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "steer", "text": "unsaved A"}),
                load.as_ref(),
            ),
        )
        .await;
    let turn = pushed["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, first_gate.wait_started())
        .await
        .unwrap();
    let queued = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id": session, "mode": "follow_up", "text": "follow-up B"}),
                load.as_ref(),
            ),
        )
        .await;
    first_gate.release();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["result"]["persistence"], "failed");
    assert!(waited["result"]["next"].is_null());
    assert_eq!(waited["result"]["queue"]["paused"], true);

    let first_page = harness
        .call_ok(
            "turn.result",
            w0_params(json!({"session_id": session, "loop_id": turn["loop_id"], "limit": 100, "max_bytes": 1024}), load.as_ref()),
        )
        .await;
    assert_eq!(first_page["result"]["availability"], "live");
    assert_eq!(first_page["result"]["persistence"], "failed");
    let total = first_page["result"]["total"].as_u64().unwrap() as usize;
    let result_id = first_page.pointer("/result/result_id").cloned();
    let mut cursor = first_page["result"]["next_cursor"].clone();
    assert!(cursor.is_object(), "the test needs a second result page");
    let mut items = BTreeMap::new();
    let mut completed = BTreeSet::new();
    w0_collect_chunks(&first_page["result"], &mut items, &mut completed);

    let resumed = harness
        .call_ok(
            "input.resume",
            w0_params(json!({"session_id": session}), load.as_ref()),
        )
        .await;
    assert_eq!(
        resumed["result"]["started"]["input"]["id"],
        queued["result"]["id"]
    );
    let successor_turn = resumed["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, second_gate.wait_started())
        .await
        .unwrap();
    assert_eq!(*observed_users.lock().unwrap(), ["follow-up B"]);

    let mut pages = 1;
    while !cursor.is_null() {
        let previous = cursor.clone();
        let mut params = json!({"session_id": session, "loop_id": turn["loop_id"], "cursor": cursor, "limit": 100, "max_bytes": 1024});
        if let Some(id) = &result_id {
            params["result_id"] = id.clone();
        }
        let page = harness
            .call("turn.result", w0_params(params, load.as_ref()))
            .await;
        if page.get("result").is_none() {
            let kind = page.pointer("/error/data/kind").cloned();
            second_gate.release();
            harness.shutdown().await;
            panic!(
                "expected the unsaved result to remain pageable after resume; error kind was {kind:?}"
            );
        }
        assert_eq!(page["result"]["availability"], "live");
        assert_eq!(page["result"]["persistence"], "failed");
        assert_eq!(page["result"]["total"].as_u64().unwrap() as usize, total);
        if let Some(id) = &result_id {
            assert_eq!(&page["result"]["result_id"], id);
        }
        w0_collect_chunks(&page["result"], &mut items, &mut completed);
        cursor = page["result"]["next_cursor"].clone();
        assert_ne!(cursor, previous);
        pages += 1;
        assert!(pages <= 64, "result pagination must make bounded progress");
    }
    assert!(result_id.as_ref().is_some_and(Value::is_string));
    assert_eq!(items.len(), total);
    assert_eq!(completed.len(), total);
    let decoded = items
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .collect::<Vec<_>>();
    let encoded = serde_json::to_string(&decoded).unwrap();
    assert!(encoded.contains("unsaved A"));
    assert!(encoded.contains(&format!("{chunk}{chunk}")));
    second_gate.release();
    harness
        .call_ok("turn.wait", Some(turn_params(&successor_turn)))
        .await;
    harness.shutdown().await;
}

#[tokio::test]
async fn w0_unknown_unstored_turn_is_explicitly_not_retained() {
    let mut fixture = RpcFixture::new("w0-not-retained", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let loop_id = minicore_runtime::LoopId::new().unwrap();
    let result = harness
        .call(
            "turn.result",
            w0_params(
                json!({"session_id": session, "loop_id": loop_id, "limit": 100, "max_bytes": 4096}),
                load.as_ref(),
            ),
        )
        .await;
    harness.shutdown().await;

    assert_eq!(
        result["result"]["availability"],
        "unavailable",
        "an unknown unretained turn must not be reported as an error or empty result; error kind was {:?}",
        result.pointer("/error/data/kind")
    );
    assert_eq!(result["result"]["unavailable_reason"], "not_retained");
    assert!(result["result"]["result_id"].is_null());
}

#[tokio::test]
async fn w1_evicted_unsaved_result_expires_between_pages_and_rejects_stale_identity() {
    let first_gate = Arc::new(Gate::new());
    let second_gate = Arc::new(Gate::new());
    let chunk = "first-answer-".repeat(512);
    let mut fixture = RpcFixture::new(
        "w1-result-expiry",
        [
            Step::Gated(
                first_gate.clone(),
                Box::new(finished(&[&chunk, &chunk], Some(ModelFinishReason::Stop))),
            ),
            Step::gated_text(second_gate.clone(), "second answer"),
        ],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let mut agent = fixture.open().await;
    let info = agent
        .create_session(crate::CreateSession {
            workspace,
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let session = json!(info.session_id);
    let load = json!(
        agent
            .loaded_session(info.session_id)
            .unwrap()
            .load()
            .to_string()
    );
    let catalog = agent
        .loaded_session(info.session_id)
        .unwrap()
        .result_catalog_for_test();
    catalog.set_limits_for_test(crate::sessions::results::ResultLimits {
        entries_per_load: 8,
        entries: 16,
        metadata_bytes: 8192,
        source_bytes_per_load: 32768,
        sources_per_load: 1,
        all_source_bytes: 32768,
        sources: 1,
    });
    let mut harness = RpcHarness::spawn(agent);
    let _release_first = ReleaseGate(first_gate.clone());
    let _release_second = ReleaseGate(second_gate.clone());
    crate::store::APPEND_FAILURES.arm(info.session_id, false);
    let first = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"load":load,"mode":"steer","text":"first"})),
        )
        .await;
    let first_turn = first["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, first_gate.wait_started())
        .await
        .unwrap();
    let queued = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"load":load,"mode":"follow_up","text":"second"})),
        )
        .await;
    first_gate.release();
    harness
        .call_ok("turn.wait", Some(turn_params(&first_turn)))
        .await;
    let first_page = harness.call_ok("turn.result", Some(json!({"session_id":session,"load":load,"loop_id":first_turn["loop_id"],"max_bytes":1024}))).await;
    let cursor = first_page["result"]["next_cursor"].clone();
    let result_id = first_page["result"]["result_id"].clone();
    assert!(cursor.is_object() && result_id.is_string());
    let stale = harness.call("turn.result", Some(json!({"session_id":session,"load":load,"loop_id":first_turn["loop_id"],"cursor":cursor,"result_id":format!("r1:{}", "0".repeat(64)),"max_bytes":1024}))).await;
    assert_eq!(stale["error"]["data"]["kind"], "stale_result");
    crate::store::APPEND_FAILURES.arm(info.session_id, false);
    let resumed = harness
        .call_ok(
            "input.resume",
            Some(json!({"session_id":session,"load":load})),
        )
        .await;
    assert_eq!(
        resumed["result"]["started"]["input"]["id"],
        queued["result"]["id"]
    );
    let second_turn = resumed["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, second_gate.wait_started())
        .await
        .unwrap();
    second_gate.release();
    harness
        .call_ok("turn.wait", Some(turn_params(&second_turn)))
        .await;
    let expired = harness.call_ok("turn.result", Some(json!({"session_id":session,"load":load,"loop_id":first_turn["loop_id"],"cursor":cursor,"result_id":result_id,"max_bytes":1024}))).await;
    assert_eq!(expired["result"]["availability"], "unavailable");
    assert_eq!(expired["result"]["unavailable_reason"], "expired");
    assert_eq!(expired["result"]["persistence"], "failed");
    harness.shutdown().await;
}

#[tokio::test]
async fn w1_ten_successive_completions_remain_discoverable_beyond_late_wait_index() {
    let gate = Arc::new(Gate::new());
    let mut scripts = vec![Step::gated_text(gate.clone(), "first")];
    scripts.extend((0..9).map(|_| Step::Text("next")));
    let mut fixture = RpcFixture::new("w1-ten-completions", scripts, &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release = ReleaseGate(gate.clone());
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let first = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id":session,"mode":"steer","text":"first"}),
                load.as_ref(),
            ),
        )
        .await;
    let mut turn = first["result"]["started"]["turn"].clone();
    let first_turn = turn.clone();
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    for index in 0..9 {
        harness.call_ok("input.push", w0_params(json!({"session_id":session,"mode":"follow_up","text":format!("queued-{index}")}), load.as_ref())).await;
    }
    gate.release();
    let mut ids = Vec::new();
    loop {
        ids.push(turn["loop_id"].clone());
        let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
        if waited["result"]["next"].is_null() {
            break;
        }
        turn = waited["result"]["next"]["turn"].clone();
        assert!(ids.len() < 11);
    }
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session})))
        .await;
    let entries = state["result"]["snapshot"]["completions"]["entries"]
        .as_array()
        .unwrap();
    assert_eq!(ids.len(), 10);
    assert_eq!(
        state["result"]["snapshot"]["completions"]["through_seq"],
        10
    );
    assert_eq!(entries.len(), 10);
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(entry["seq"], index + 1);
        assert_eq!(entry["loop_id"], ids[index]);
        assert_eq!(
            entry["next_loop_id"],
            ids.get(index + 1).cloned().unwrap_or(Value::Null)
        );
    }
    let old = harness
        .call("turn.wait", Some(turn_params(&first_turn)))
        .await;
    assert_eq!(old["error"]["data"]["kind"], "turn_not_found");
    harness.shutdown().await;
}

#[tokio::test]
async fn w1_internal_worker_failure_registers_unavailable_completion_and_releases_waiter() {
    let mut fixture = RpcFixture::new(
        "w1-internal-completion",
        [Step::Pending],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let typed_session: SessionId = session.as_str().unwrap().parse().unwrap();
    crate::sessions::PANIC_WORKERS.arm(typed_session, ());
    let pushed = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id":session,"mode":"steer","text":"first"}),
                load.as_ref(),
            ),
        )
        .await;
    let turn = pushed["result"]["started"]["turn"].clone();
    let waited = harness.call("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(waited["error"]["data"]["kind"], "internal_error");
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session})))
        .await;
    let entry = &state["result"]["snapshot"]["completions"]["entries"][0];
    assert_eq!(state["result"]["block_reason"], "internal");
    assert_eq!(entry["completion_kind"], "internal_error");
    assert_eq!(entry["availability"], "unavailable");
    assert_eq!(entry["unavailable_reason"], "internal_error");
    let result = harness
        .call_ok(
            "turn.result",
            w0_params(
                json!({"session_id":session,"loop_id":turn["loop_id"]}),
                load.as_ref(),
            ),
        )
        .await;
    assert_eq!(result["result"]["unavailable_reason"], "internal_error");
    harness
        .send(
            json!("shutdown-internal"),
            "agent.shutdown",
            Some(json!({})),
        )
        .await;
    assert_eq!(
        harness.response(json!("shutdown-internal")).await["error"]["data"]["kind"],
        "internal_error"
    );
    assert!(harness.next_frame().await.is_none());
    assert!(matches!(harness.join().await, Err(AgentError::Internal)));
}

#[tokio::test]
async fn w1_unknown_append_stays_blocked_and_uncertain_in_result_and_directory() {
    let gate = Arc::new(Gate::new());
    let mut fixture = RpcFixture::new(
        "w1-unknown-append",
        [Step::gated_text(gate.clone(), "answer")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let _release = ReleaseGate(gate.clone());
    let (session, load) = w0_create_session(&mut harness, &workspace).await;
    let typed_session: SessionId = session.as_str().unwrap().parse().unwrap();
    crate::store::APPEND_FAILURES.arm(typed_session, true);
    let pushed = harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id":session,"mode":"steer","text":"first"}),
                load.as_ref(),
            ),
        )
        .await;
    let turn = pushed["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    harness
        .call_ok(
            "input.push",
            w0_params(
                json!({"session_id":session,"mode":"follow_up","text":"second"}),
                load.as_ref(),
            ),
        )
        .await;
    gate.release();
    let waited = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert!(waited["result"]["next"].is_null());
    assert_eq!(waited["result"]["queue"]["paused"], true);
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session})))
        .await;
    assert_eq!(state["result"]["block_reason"], "persistence");
    let entry = &state["result"]["snapshot"]["completions"]["entries"][0];
    assert_eq!(entry["persistence"], "failed");
    assert_eq!(entry["persistence_uncertain"], true);
    let result = harness
        .call_ok(
            "turn.result",
            w0_params(
                json!({"session_id":session,"loop_id":turn["loop_id"]}),
                load.as_ref(),
            ),
        )
        .await;
    assert_eq!(result["result"]["persistence"], "failed");
    assert_eq!(result["result"]["persistence_uncertain"], true);
    let resumed = harness
        .call(
            "input.resume",
            w0_params(json!({"session_id":session}), load.as_ref()),
        )
        .await;
    assert_eq!(resumed["error"]["data"]["kind"], "session_blocked");
    harness.shutdown().await;
}

#[tokio::test]
async fn an_evicted_uncertain_result_does_not_reappear_from_the_active_report_or_disk() {
    let mut fixture = RpcFixture::new(
        "uncertain-expired",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let mut agent = fixture.open().await;
    let info = agent
        .create_session(crate::CreateSession {
            workspace,
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let session = agent.loaded_session(info.session_id).unwrap();
    session
        .result_catalog_for_test()
        .set_limits_for_test(crate::sessions::results::ResultLimits {
            entries_per_load: 0,
            ..Default::default()
        });
    crate::store::APPEND_FAILURES.arm(info.session_id, true);
    let pushed = agent
        .push_input(crate::PushInput {
            session_id: info.session_id,
            mode: crate::InputMode::Steer,
            text: "first".into(),
        })
        .await
        .unwrap();
    let turn = pushed.started.unwrap().turn;
    let result = tokio::time::timeout(TIMEOUT, agent.wait_turn(turn))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.persistence, crate::TurnPersistence::Failed);
    let prepared =
        crate::read::prepare_result_source(&result.report.appended, result.interrupted.clone())
            .unwrap();
    let record = crate::store::StoredLoopRecord {
        loop_id: turn.loop_id,
        outcome: crate::event::LoopOutcomeView::from_report(&result.report),
        items: result.report.appended.to_vec(),
        usage: result.report.usage,
        requests: result.report.requests,
        tool_rounds: result.report.tool_rounds,
        final_config_revision: result.report.final_config_revision,
        completed_at: "2026-10-31T00:00:00Z".into(),
        user_times: None,
        interrupted: result.interrupted.clone(),
    };
    let mut bytes = serde_json::to_vec(&record).unwrap();
    bytes.push(b'\n');
    tokio::fs::write(
        fixture
            .config
            .data_dir
            .join("sessions")
            .join(info.session_id.to_string())
            .join("history.jsonl"),
        bytes,
    )
    .await
    .unwrap();
    let load = session.load().to_string();
    let store = agent.store_handle();
    let mut harness = RpcHarness::spawn(agent);
    for cursor in [Value::Null, json!({"item":0,"offset":1})] {
        let page = harness
            .call_ok(
                "turn.result",
                Some(json!({"session_id":info.session_id,
            "load":load,"loop_id":turn.loop_id,"cursor":cursor,"result_id":prepared.result_id})),
            )
            .await;
        assert_eq!(page["result"]["availability"], "unavailable");
        assert_eq!(page["result"]["unavailable_reason"], "expired");
        assert_eq!(page["result"]["persistence"], "failed");
        assert_eq!(page["result"]["persistence_uncertain"], true);
        assert!(page["result"]["result_id"].is_null());
        assert!(page["result"]["items"].as_array().unwrap().is_empty());
    }
    let missing = harness
        .call_unmodified(
            "turn.result",
            Some(json!({"session_id":info.session_id,
        "loop_id":turn.loop_id})),
        )
        .await;
    assert_eq!(missing["error"]["data"]["kind"], "missing_load");
    harness
        .call_ok(
            "session.close",
            Some(json!({"session_id":info.session_id,"load":load})),
        )
        .await;
    let late = crate::read::turn_result(
        store,
        Some(session),
        crate::TurnResultRequest {
            turn,
            load: Some(load),
            result_id: Some(prepared.result_id),
            cursor: Some(crate::ReadCursor { item: 0, offset: 1 }),
            limit: 100,
            max_bytes: Some(4096),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        late.availability,
        crate::TurnResultAvailability::Unavailable
    );
    assert_eq!(late.persistence, Some(crate::TurnPersistence::Failed));
    assert!(late.persistence_uncertain);
    assert_eq!(
        late.unavailable_reason,
        Some(crate::UnavailableReason::Expired)
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn two_parked_result_preparations_leave_control_and_interactive_query_capacity() {
    let mut fixture = RpcFixture::new(
        "two-result-readers",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    std::fs::write(workspace.join("file.txt"), "content").unwrap();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    let pushed = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id":session,"mode":"steer","text":"first"})),
        )
        .await;
    let turn = pushed["result"]["started"]["turn"].clone();
    harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    let loop_id: minicore_runtime::LoopId = turn["loop_id"].as_str().unwrap().parse().unwrap();
    let prepared = harness
        .call_ok("turn.result", Some(turn_params(&turn)))
        .await;
    assert_eq!(prepared["result"]["total"], 2);
    let first = Arc::new(Gate::new());
    let second = Arc::new(Gate::new());
    let _release_first = ReleaseGate(first.clone());
    let _release_second = ReleaseGate(second.clone());
    crate::read::RESULT_PREPARE_GATES.arm(loop_id, first.clone());
    crate::read::RESULT_PREPARE_GATES.arm(loop_id, second.clone());
    for id in ["reader-a", "reader-b"] {
        harness
            .send(json!(id), "turn.result", Some(turn_params(&turn)))
            .await;
    }
    tokio::time::timeout(TIMEOUT, first.wait_started())
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, second.wait_started())
        .await
        .unwrap();
    let state = harness
        .call_ok("session.state", Some(json!({"session_id":session})))
        .await;
    assert_eq!(state["result"]["snapshot"]["completions"]["through_seq"], 1);
    let read = harness
        .call_ok(
            "workspace.read",
            Some(json!({"session_id":session,"path":"file.txt"})),
        )
        .await;
    assert_eq!(read["result"]["content"], "content");
    assert_eq!(read["result"]["status"], "ok");
    harness.call_ok("agent.ping", None).await;
    first.release();
    second.release();
    for id in ["reader-a", "reader-b"] {
        let page = harness.response(json!(id)).await;
        assert!(page["result"]["result_id"].is_string());
        assert_eq!(page["result"]["total"], 2);
    }
    harness.shutdown().await;
}

#[tokio::test]
async fn cold_identity_waiter_is_cancellable_and_close_cannot_retain_late_preparation() {
    let mut fixture = RpcFixture::new(
        "cancel-cold-identity",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    );
    let mut agent = fixture.open().await;
    let info = agent
        .create_session(crate::CreateSession {
            workspace: fixture.workspace.clone(),
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let session = agent.loaded_session(info.session_id).unwrap();
    let store = agent.store_handle();
    let catalog = session.result_catalog_for_test();
    let mut harness = RpcHarness::spawn(agent);
    let pushed = harness
        .call_ok(
            "input.push",
            Some(json!({
                "session_id":info.session_id,"load":session.load(),"mode":"steer","text":"first"
            })),
        )
        .await;
    let turn = pushed["result"]["started"]["turn"].clone();
    harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    let turn: crate::TurnRef = serde_json::from_value(turn).unwrap();
    let (_, source_revision) = store
        .loop_item_source(info.session_id, turn.loop_id, 100)
        .unwrap();
    let request = crate::TurnResultRequest {
        turn,
        load: Some(session.load().to_string()),
        result_id: None,
        cursor: None,
        limit: 100,
        max_bytes: Some(1024),
    };
    let gate = Arc::new(Gate::new());
    let _release = ReleaseGate(gate.clone());
    crate::read::RESULT_PREPARE_GATES.arm(turn.loop_id, gate.clone());
    let first = tokio::spawn(crate::queries::prepare_turn_result(
        store.clone(),
        Some(session.clone()),
        request.clone(),
        CancellationToken::new(),
    ));
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let mut waiter = crate::queries::prepare_turn_result(
        store.clone(),
        Some(session.clone()),
        request,
        cancellation.clone(),
    );
    tokio::select! {
        biased;
        _ = &mut waiter => panic!("cold preparation did not serialize"),
        _ = std::future::ready(()) => {}
    }
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(TIMEOUT, waiter).await.unwrap(),
        Err(AgentError::QueryLimit)
    ));
    harness
        .call_ok(
            "session.close",
            Some(json!({"session_id":info.session_id,"load":session.load()})),
        )
        .await;
    assert!(matches!(
        tokio::time::timeout(TIMEOUT, first).await.unwrap().unwrap(),
        Err(AgentError::SessionNotLoaded)
    ));
    gate.release();
    assert!(
        catalog
            .stored_identity(crate::sessions::results::StoredIdentityKey {
                load: session.load(),
                source_revision,
                loop_id: turn.loop_id,
            })
            .is_none()
    );
    drop(session);
    harness.shutdown().await;
}
