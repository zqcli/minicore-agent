use super::*;

/// Fragmented input must survive a `select!` cancellation of the read future.
/// A deferred waiter resolving between two chunks previously discarded the
/// already-consumed prefix, leaving the request unparseable.
#[tokio::test]
async fn fragmented_frame_survives_deferred_waiter_interleaving() {
    let probe = ConcurrencyProbe::new();
    let (agent, base, workspace) = test_agent(
        "half-frame-interleave",
        [
            ModelScript::Gate(Arc::clone(&probe)),
            ModelScript::Text("done"),
        ],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let (reader, input) = chunked_input();
    let mut harness =
        RpcHarness::spawn_with(agent, BufReader::new(reader), Box::new(input.clone()));

    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "gated turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;

    // Confirm the gated model call is actually in flight before relying on the
    // waiter branch of `select!` becoming ready.
    tokio::time::timeout(TIMEOUT, async {
        while probe.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gated model request did not start");

    // Deliver only the first half of the next request and wait until the server
    // has consumed it, so the partial frame is parked inside `read_frame`.
    let ping = json!({"jsonrpc": "2.0", "id": "after", "method": "agent.ping", "params": {}});
    let mut encoded = serde_json::to_vec(&ping).unwrap();
    encoded.push(b'\n');
    let split = encoded.len() / 2;
    input.feed(&encoded[..split]).await;
    input.wait_delivered(input.total_fed()).await;

    // Release the model gate so the deferred `turn.wait` resolves while the
    // partial frame is in flight. The waiter branch of `select!` cancels the
    // in-flight read future.
    probe.release.cancel();
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));

    input.feed(&encoded[split..]).await;
    let ping_response = harness.response(json!("after")).await;
    assert!(ping_response["result"]["version"].is_string());

    harness.shutdown().await;
    remove_base(&base).await;
}

/// Direct regression for the same cancellation: a frame split across a
/// cancelled poll must still be reassembled from the retained prefix.
#[tokio::test]
async fn read_frame_accumulates_across_a_cancelled_poll() {
    let request = br#"{"jsonrpc":"2.0","id":"split","method":"agent.ping","params":{}}"#.to_vec();
    let mut terminated = request.clone();
    terminated.push(b'\n');
    let split = terminated.len() / 2;
    let (reader, input) = chunked_input();
    let mut reader = BufReader::new(reader);
    let mut frame = Vec::new();

    let mut first = Box::pin(read_frame(&mut reader, &mut frame));
    assert!(futures_util::poll!(&mut first).is_pending());
    input.feed(&terminated[..split]).await;
    assert!(futures_util::poll!(&mut first).is_pending());
    input.wait_delivered(input.total_fed()).await;
    drop(first);
    assert_eq!(frame.as_slice(), &terminated[..split]);

    input.feed(&terminated[split..]).await;
    match tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader, &mut frame))
        .await
        .expect("fragmented frame was never completed")
        .unwrap()
    {
        Frame::Data(bytes) => assert_eq!(bytes, terminated),
        Frame::Eof | Frame::Oversized => panic!("fragmented frame was not reconstructed"),
    }
}

/// A frame that exceeds the limit only after an earlier cancellation is still
/// rejected, proving the limit covers the cumulative length.
#[tokio::test]
async fn read_frame_limit_covers_cumulative_length_after_cancellation() {
    let first = vec![b'x'; 1024];
    let second = vec![b'y'; MAX_RPC_LINE_BYTES];
    let (reader, input) = chunked_input();
    let mut reader = BufReader::new(reader);
    let mut frame = Vec::new();

    let mut first_read = Box::pin(read_frame(&mut reader, &mut frame));
    assert!(futures_util::poll!(&mut first_read).is_pending());
    input.feed(&first).await;
    assert!(futures_util::poll!(&mut first_read).is_pending());
    input.wait_delivered(input.total_fed()).await;
    drop(first_read);
    assert_eq!(frame.len(), 1024);

    input.feed(&second).await;
    match tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader, &mut frame))
        .await
        .expect("oversized frame was never observed")
        .unwrap()
    {
        Frame::Oversized => {}
        Frame::Data(bytes) => panic!("oversized frame was accepted: {} bytes", bytes.len()),
        Frame::Eof => panic!("oversized frame reported as EOF"),
    }
    assert!(frame.is_empty(), "oversized frame must release its buffer");
}

/// The deferred waiter pool is bounded. Filling it with parked `turn.wait`
/// requests rejects further `turn.wait`/`session.compact` with
/// `-32019` without launching the compaction, while ping and cancel still work.
#[tokio::test]
async fn deferred_waiter_limit_rejects_new_waiters_but_keeps_control_methods() {
    let probe = ConcurrencyProbe::new();
    let (agent, base, workspace) = test_agent(
        "waiter-limit",
        [ModelScript::Gate(Arc::clone(&probe))],
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
            Some(json!({"session_id": session_id, "text": "gated turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, async {
        while probe.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gated model request did not start");

    // Fill every waiter slot with a parked turn.wait.
    for index in 0..MAX_DEFERRED_WAITERS {
        harness
            .send(
                json!(format!("wait-{index}")),
                "turn.wait",
                Some(turn_params(&turn)),
            )
            .await;
    }

    harness
        .send(json!("wait-over"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let over = harness.response(json!("wait-over")).await;
    assert_eq!(over["error"]["code"], json!(-32019));
    assert_eq!(over["error"]["data"]["kind"], json!("resource_exhausted"));
    assert_eq!(over["error"]["data"]["retryable"], json!(true));

    // The capped compaction request is rejected before the Session-owned
    // operation starts, while the waiter slots are still full and the turn is
    // still gated.
    harness
        .send(
            json!("compact-over"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-over-limit"
            })),
        )
        .await;
    let compact = harness.response(json!("compact-over")).await;
    assert_eq!(compact["error"]["code"], json!(-32019));

    // The reader still serves control methods, and no compaction was launched.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert!(state["result"]["compaction"].is_null());
    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );

    // Releasing the model lets the parked waiters drain.
    probe.release.cancel();
    for index in 0..MAX_DEFERRED_WAITERS {
        let waited = harness.response(json!(format!("wait-{index}"))).await;
        assert_eq!(waited["result"]["outcome"]["type"], json!("cancelled"));
    }

    harness.shutdown().await;
    remove_base(&base).await;
}
