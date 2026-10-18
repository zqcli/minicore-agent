use super::*;

#[tokio::test]
async fn turn_result_reports_pending_and_stored_availability() {
    let model_started = Arc::new(Notify::new());
    let (agent, base, workspace) = test_agent(
        "turn-result-lifecycle",
        [
            ModelScript::BlockWithSignal(Arc::clone(&model_started)),
            ModelScript::Text("stored answer"),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send-blocked"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "wait"})),
        )
        .await;
    let blocked_turn = harness.response(json!("send-blocked")).await["result"]["turn"].clone();
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
    harness
        .send(
            json!("wait-blocked"),
            "turn.wait",
            Some(turn_params(&blocked_turn)),
        )
        .await;
    let _ = harness.response(json!("wait-blocked")).await;

    harness
        .send(
            json!("send-stored"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "persist"})),
        )
        .await;
    let stored_turn = harness.response(json!("send-stored")).await["result"]["turn"].clone();
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
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    harness
        .send(
            json!("stored-result"),
            "turn.result",
            Some(json!({
                "session_id": stored_turn["session_id"],
                "loop_id": stored_turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let stored = harness.response(json!("stored-result")).await;
    assert_eq!(stored["result"]["availability"], json!("stored"));
    assert_eq!(stored["result"]["persistence"], json!("persisted"));
    assert!(stored["result"]["items"].as_array().unwrap().len() >= 2);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn turn_result_can_continue_from_live_to_stored_items() {
    let (agent, base, workspace) = test_agent(
        "turn-result-live-stored-pages",
        [ModelScript::Oversize],
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
            Some(json!({"session_id": session_id, "text": "live"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    harness
        .send(
            json!("live-first"),
            "turn.result",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let first = harness.response(json!("live-first")).await;
    assert_eq!(first["result"]["availability"], json!("live"));
    assert!(first["result"]["next_cursor"].is_object());

    let mut cursor = first["result"]["next_cursor"].clone();
    let mut reconstructed = BTreeMap::<usize, String>::new();
    for item in first["result"]["items"].as_array().unwrap() {
        let index = item["index"].as_u64().unwrap() as usize;
        let offset = item["offset"].as_u64().unwrap() as usize;
        let entry = reconstructed.entry(index).or_default();
        assert_eq!(entry.len(), offset);
        entry.push_str(item["data"].as_str().unwrap());
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

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
    remove_base(&base).await;
}

#[tokio::test]
async fn turn_result_reads_failed_live_report_without_history() {
    let (agent, base, workspace) = test_agent(
        "turn-result-failed-append",
        [ModelScript::Text("answer retained in memory")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_session_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::APPEND_FAILURES.arm(typed_session_id, true);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "failed persistence"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    std::fs::remove_file(history_path).unwrap();
    harness
        .send(
            json!("result"),
            "turn.result",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let result = harness.response(json!("result")).await;
    assert_eq!(result["result"]["availability"], json!("live"));
    assert_eq!(result["result"]["persistence"], json!("failed"));
    assert_eq!(result["result"]["outcome"]["type"], json!("completed"));
    assert!(result.to_string().contains("answer retained in memory"));

    harness.shutdown().await;
    remove_base(&base).await;
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
