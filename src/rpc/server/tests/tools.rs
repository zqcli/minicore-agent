use super::*;

#[tokio::test]
async fn tool_read_and_output_query_recorded_facts_by_full_identity() {
    let (agent, base, workspace) = test_agent(
        "tool-data-rpc",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "read",
                arguments: json!({"path": "a.txt", "limit": 4}),
            }]),
            ModelScript::Text("done"),
        ],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    tokio::fs::write(workspace.join("a.txt"), b"alpha\nbeta")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "read a"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness.response(json!("wait")).await;

    // Derive the ToolRef from authoritative history rather than guessing.
    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let tool_result = history["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|item| {
            (item["item"]["type"] == json!("tool_result")).then(|| item["item"]["data"].clone())
        })
        .expect("tool result history item");
    let tool_ref = json!({
        "session_id": session_id,
        "loop_id": tool_result["loop_id"],
        "request_index": tool_result["request_index"],
        "tool_call_id": tool_result["tool_call_id"],
    });

    harness
        .send(
            json!("read"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_result["loop_id"],
                "request_index": tool_result["request_index"],
                "tool_call_id": tool_result["tool_call_id"],
            })),
        )
        .await;
    let read = harness.response(json!("read")).await;
    assert_eq!(read["result"]["execution"]["state"], json!("succeeded"));
    assert_eq!(read["result"]["invocation"]["name"], json!("read"));
    assert!(
        read["result"]["invocation"]["input"]["preview"]
            .as_str()
            .unwrap()
            .contains("a.txt")
    );

    harness
        .send(
            json!("output"),
            "tool.output",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_ref["loop_id"],
                "request_index": tool_ref["request_index"],
                "tool_call_id": tool_ref["tool_call_id"],
                "stream": "output",
                "offset": 0,
                "max_bytes": 8192,
            })),
        )
        .await;
    let output = harness.response(json!("output")).await;
    assert_eq!(output["result"]["stream"], json!("output"));
    assert_eq!(output["result"]["encoding"], json!("utf8"));
    assert!(
        output["result"]["data"]
            .as_str()
            .unwrap()
            .starts_with("1: alpha")
    );

    // An unknown identity is an explicit `tool_not_found`, never the nearest
    // matching name.
    harness
        .send(
            json!("missing"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_ref["loop_id"],
                "request_index": 999,
                "tool_call_id": tool_ref["tool_call_id"],
            })),
        )
        .await;
    let missing = harness.response(json!("missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32021));
    assert_eq!(missing["error"]["data"]["kind"], json!("tool_not_found"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn tool_read_exposes_approval_time_invocation_without_running() {
    let (agent, base, workspace) = test_agent(
        "tool-data-approval",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "out.txt", "content": "approved"}),
            }]),
            ModelScript::Text("done"),
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
            Some(json!({"session_id": session_id, "text": "write it"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    let interaction = harness.event("interaction_requested").await;
    let tool_call_id = interaction["params"]["data"]["interaction"]["tool_call_id"].clone();

    harness
        .send(
            json!("read"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "request_index": 0,
                "tool_call_id": tool_call_id,
            })),
        )
        .await;
    let read = harness.response(json!("read")).await;
    assert_eq!(
        read["result"]["execution"]["state"],
        json!("awaiting_policy")
    );
    assert_eq!(
        read["result"]["invocation"]["subject"]["kind"],
        json!("file")
    );
    assert_eq!(
        read["result"]["invocation"]["subject"]["path"],
        json!("out.txt")
    );
    assert!(read["result"]["execution"].get("started_at").is_none());

    // The output stream of a call that has not run is pending, not a false
    // empty complete page.
    harness
        .send(
            json!("output"),
            "tool.output",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "request_index": 0,
                "tool_call_id": interaction["params"]["data"]["interaction"]["tool_call_id"],
                "stream": "output",
                "offset": 0,
            })),
        )
        .await;
    let pending = harness.response(json!("output")).await;
    assert_eq!(pending["result"]["availability"], json!("pending"));
    assert_eq!(pending["result"]["eof"], json!(false));
    assert_eq!(pending["result"]["data"], json!(""));

    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
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
    harness.response(json!("answer")).await;
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness.response(json!("wait")).await;

    harness
        .send(
            json!("read2"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "request_index": 0,
                "tool_call_id": interaction["params"]["data"]["interaction"]["tool_call_id"],
            })),
        )
        .await;
    let finished = harness.response(json!("read2")).await;
    assert_eq!(finished["result"]["execution"]["state"], json!("succeeded"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn tool_read_deferred_query_capacity_ceiling_and_ping_availability() {
    let (agent, base, workspace) =
        test_agent("tool-read-capacity", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_val = create_and_open(&mut harness, &workspace).await;
    let session_id: SessionId = session_val.as_str().unwrap().parse().unwrap();

    let mut gates = Vec::new();
    let mut refs = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let tool_ref = crate::tool_data::ToolRef {
            session_id,
            loop_id: minicore_runtime::LoopId::new().unwrap(),
            request_index: index as u32,
            tool_call_id: minicore_runtime::ToolCallId::new(format!("gate-call-{index}")).unwrap(),
        };
        let gate = Arc::new(crate::test_support::Gate::new());
        TOOL_READ_GATES.arm(tool_ref.clone(), Arc::clone(&gate));
        gates.push(gate);
        refs.push(tool_ref.clone());

        harness
            .send(
                json!(format!("tr-{index}")),
                "tool.read",
                Some(json!({
                    "session_id": session_id,
                    "loop_id": tool_ref.loop_id,
                    "request_index": tool_ref.request_index,
                    "tool_call_id": tool_ref.tool_call_id,
                })),
            )
            .await;
    }

    for gate in &gates {
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .expect("tool read query did not start");
    }

    // 5th query must be rejected with resource_exhausted (-32019)
    let overflow_ref = crate::tool_data::ToolRef {
        session_id,
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 99,
        tool_call_id: minicore_runtime::ToolCallId::new("overflow-call").unwrap(),
    };
    harness
        .send(
            json!("tr-overflow"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": overflow_ref.loop_id,
                "request_index": overflow_ref.request_index,
                "tool_call_id": overflow_ref.tool_call_id,
            })),
        )
        .await;
    let overflow = harness.response(json!("tr-overflow")).await;
    assert_eq!(overflow["error"]["code"], json!(-32019));

    // Agent control methods (like ping) are not deferred and remain fully responsive
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Release gates, all queries finish
    for gate in &gates {
        gate.release();
    }
    for index in 0..MAX_DEFERRED_QUERIES {
        let resp = harness.response(json!(format!("tr-{index}"))).await;
        // The mock tool call does not exist, so it returns tool not found error (-32021)
        assert_eq!(resp["error"]["code"], json!(-32021));
    }

    // Capacity is freed after releasing queries
    harness
        .send(
            json!("tr-after"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": overflow_ref.loop_id,
                "request_index": overflow_ref.request_index,
                "tool_call_id": overflow_ref.tool_call_id,
            })),
        )
        .await;
    let after = harness.response(json!("tr-after")).await;
    // Query went through (not rejected with -32019)
    assert_ne!(after["error"]["code"], json!(-32019));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn tool_read_deferred_query_shutdown_cancels_within_deadline() {
    let (agent, base, workspace) =
        test_agent("tool-read-shutdown", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_val = create_and_open(&mut harness, &workspace).await;
    let session_id: SessionId = session_val.as_str().unwrap().parse().unwrap();

    let tool_ref = crate::tool_data::ToolRef {
        session_id,
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: minicore_runtime::ToolCallId::new("gate-shutdown-call").unwrap(),
    };
    let gate = Arc::new(crate::test_support::Gate::new());
    TOOL_READ_GATES.arm(tool_ref.clone(), Arc::clone(&gate));

    harness
        .send(
            json!("tr-pending"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_ref.loop_id,
                "request_index": tool_ref.request_index,
                "tool_call_id": tool_ref.tool_call_id,
            })),
        )
        .await;

    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .expect("tool read did not start");

    // Ping works during pending query
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Calling shutdown must cancel the query immediately and complete in < 2 seconds,
    // not waiting for the 10-second deadline.
    let start = std::time::Instant::now();
    harness.shutdown().await;
    assert!(start.elapsed() < std::time::Duration::from_secs(2));

    remove_base(&base).await;
}

#[tokio::test]
async fn tool_output_deferred_query_shutdown_cancellation_cleans_up() {
    let (agent, base, workspace) =
        test_agent("tool-output-shutdown", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_val = create_and_open(&mut harness, &workspace).await;
    let session_id: SessionId = session_val.as_str().unwrap().parse().unwrap();

    let tool_ref = crate::tool_data::ToolRef {
        session_id,
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: minicore_runtime::ToolCallId::new("gate-output-call").unwrap(),
    };
    let gate = Arc::new(crate::test_support::Gate::new());
    TOOL_READ_GATES.arm(tool_ref.clone(), Arc::clone(&gate));

    harness
        .send(
            json!("to-pending"),
            "tool.output",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_ref.loop_id,
                "request_index": tool_ref.request_index,
                "tool_call_id": tool_ref.tool_call_id,
                "stream": "stdout",
                "offset": 0,
            })),
        )
        .await;

    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .expect("tool output did not start");

    // Ping works during pending query
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Release gate so shutdown can cleanly drain
    gate.release();
    harness.shutdown().await;
    remove_base(&base).await;
}
