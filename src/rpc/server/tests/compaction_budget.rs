use super::*;

#[tokio::test]
async fn manual_compaction_rejects_fixed_schema_budget_before_model_call() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-fixed-budget-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = fake_model(256, [Step::Text("settled"), Step::Text("unused")]);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &["read"], ApprovalMode::Auto),
        test_models(Arc::clone(&model)),
    )
    .await
    .unwrap();
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
                "operation_id": "fixed-budget"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("budget_exceeded"));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_budget_counts_utf8_and_json_escaping() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-utf8-budget-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let model = fake_model(
        40_000,
        [
            Step::Text("settled"),
            observe(Arc::clone(&observations), "summary"),
        ],
    );
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &[], ApprovalMode::Auto),
        test_models(Arc::clone(&model)),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let text = "é🙂\n\t\\\" escaped ".repeat(2_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": text})),
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
                "operation_id": "utf8-budget"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    {
        let observations = observations.lock().unwrap();
        assert!(observations.iter().any(|call| {
            call.request.messages().iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.contains("é🙂") && text.contains('\n') && text.contains('\\'))
            })
        }));
        assert!(
            observations
                .iter()
                .all(|call| call.request.tools().is_empty())
        );
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_summarizes_complete_large_source_once() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-chunk-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut scripts = vec![Step::Text("settled")];
    scripts.extend((0..32).map(|_| observe(Arc::clone(&observations), "partial summary")));
    let model = fake_model(40_000, scripts);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &[], ApprovalMode::Auto),
        test_models(model),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let large_text = format!("large-history {}", "utf8 source ".repeat(4_000));
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": large_text})),
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
                "operation_id": "compact-chunk"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    assert!(result["result"]["before_tokens"].is_number());
    assert!(result["result"]["after_tokens"].is_number());
    assert!(
        result["result"]["after_tokens"].as_u64().unwrap()
            < result["result"]["before_tokens"].as_u64().unwrap()
    );
    {
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert!(observations.iter().all(|call| {
            call.request.tools().is_empty()
                && call.context.request_index == 0
                && !call
                    .request
                    .messages()
                    .iter()
                    .any(|message| matches!(message, ModelMessage::Tool { .. }))
        }));
        assert!(observations.iter().any(|call| {
            call.request.messages().iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.contains("MINICORE HISTORICAL SOURCE DATA"))
            })
        }));
        assert!(observations.iter().all(|call| {
            call.request.messages().iter().all(|message| {
                !matches!(message, ModelMessage::User(text) if text.contains("MINICORE SUMMARY PARTS DATA") || text.contains("chunk="))
            })
        }));
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_context_reports_effective_input_budget_when_auto_enabled() {
    let mut fixture = RpcFixture::new(
        "context-budget",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    )
    .auto_compaction();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("context"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context")).await;
    assert_eq!(
        context["result"]["budget"]["input_budget_tokens"],
        json!(16_384)
    );
    assert_eq!(
        context["result"]["budget"]["trigger_tokens"],
        json!(16_384 * 80 / 100)
    );
    assert_eq!(
        context["result"]["budget"]["target_tokens"],
        json!(16_384 * 50 / 100)
    );
    assert!(context["result"]["last_prepare_failure"].is_null());
    harness.shutdown().await;
}

#[tokio::test]
async fn session_context_omits_budget_when_auto_disabled() {
    let mut fixture = RpcFixture::new(
        "context-disabled",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("context"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context")).await;
    assert!(context["result"]["budget"]["input_budget_tokens"].is_null());
    assert!(context["result"]["budget"]["trigger_tokens"].is_null());
    assert!(context["result"]["budget"]["target_tokens"].is_null());
    harness.shutdown().await;
}

/// A locally oversized new user request still reaches the server and may
/// succeed. Admission and control RPCs do not wait for a threshold summary.
async fn oversized_current_input_server_acceptance(enabled: bool) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-uncompressible-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.compaction = CompactionConfig {
        enabled,
        trigger_percent: 80,
        target_percent: 50,
    };
    let server = crate::openai_mock::MockServer::spawn([crate::openai_mock::MockResponse::sse(&[
        json!({"type":"response.output_text.delta","delta":"accepted"}),
        json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"message","id":"msg_accepted","role":"assistant","content":[{"type":"output_text","text":"accepted"}]}]}}),
    ])]).await;
    let mut model = configured_model("provider-model", server.base_url(), "PATH");
    let ModelConfig::OpenAiResponses {
        physical_context_window,
        ..
    } = &mut model;
    *physical_context_window = 4_000;
    config.models.insert("fake".to_owned(), model);
    let agent = Agent::open(config).await.unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let oversized = "x".repeat(20_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": oversized})),
        )
        .await;
    let response = harness.response(json!("send")).await;
    let turn = response["result"]["turn"].clone();
    assert!(
        turn["loop_id"].is_string(),
        "ordinary submit must accept without admission summary"
    );
    let result = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(result["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(result["result"]["persistence"], json!("persisted"));
    let captured = server.finish().await;
    assert_eq!(captured.len(), 1);
    assert!(
        serde_json::to_string(&captured[0].json_body())
            .unwrap()
            .contains(&"x".repeat(20_000))
    );
    let context = harness
        .call_ok("session.context", Some(json!({"session_id":session_id})))
        .await;
    assert!(context["result"]["recovery"].is_null());
    // The reader still serves control methods and the completed persisted turn is idle.
    let state = harness
        .call_ok("session.state", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(state["result"]["status"], json!("idle"));
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn ordinary_submit_accepts_minimum_between_soft_trigger_and_hard_limit() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-minimum-window-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = fake_model(1_000, [Step::Text("answer")]);
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.compaction = CompactionConfig {
        enabled: true,
        trigger_percent: 80,
        target_percent: 50,
    };
    let system = minicore_runtime::value::BoundedText::new("test system prompt").unwrap();
    let text = (1..10_000)
        .map(|size| "x".repeat(size))
        .find(|candidate| {
            crate::compaction::auto::minimal_messages(&system, &[candidate.as_str()])
                .and_then(|messages| {
                    crate::compaction::auto::estimate_tokens_with_budget(
                        &crate::models::tests::DefaultProviderBudget,
                        messages,
                        &[],
                        ReasoningPreference::Auto,
                        None,
                        None,
                    )
                })
                .is_ok_and(|tokens| (801..=900).contains(&tokens))
        })
        .expect("test input must land above trigger and below hard limit");
    let agent = Agent::open_with_models(config, test_models(Arc::clone(&model)))
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let response = harness
        .call_ok(
            "turn.send",
            Some(json!({"session_id": session_id, "text": text})),
        )
        .await;
    assert!(response["result"]["turn"]["loop_id"].is_string());
    let turn = response["result"]["turn"].clone();
    let wait = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(wait["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(wait["result"]["persistence"], json!("persisted"));
    // The only call before completion is the ordinary model request. The
    // completed near-budget turn may independently start post-turn compaction.
    assert!(model.calls.load(Ordering::SeqCst) >= 1);
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn pi_first_boundary_utility_keeps_rpc_state_context_ping_and_cancel_responsive() {
    let base = std::env::temp_dir().join(format!("pi-rpc-first-{}", SessionId::new().unwrap()));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::write(workspace.join("old.txt"), "old ".repeat(10_000))
        .await
        .unwrap();
    let config = test_config(base.join("data"), &["read"], ApprovalMode::Auto);
    let seed_model = fake_model(
        16_384,
        [
            tool_calls(vec![ToolCallScript {
                name: "read",
                arguments: json!({"path":"old.txt"}),
            }]),
            Step::Text("seed done"),
        ],
    );
    let mut seed = Agent::open_with_models(config.clone(), test_models(seed_model))
        .await
        .unwrap();
    let info = seed
        .create_session(crate::agent::CreateSession {
            workspace: workspace.clone(),
            profile: String::new(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let turn = seed
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "seed old history".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        seed.wait_turn(turn).await.unwrap().report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    seed.close_session(info.session_id).await.unwrap();
    drop(seed);
    let entered = Arc::new(Notify::new());
    let model = fake_model(8_000, [block_with_signal(Arc::clone(&entered))]);
    let mut config = config;
    config.compaction = CompactionConfig {
        enabled: true,
        trigger_percent: 80,
        target_percent: 50,
    };
    let mut agent = Agent::open_with_models(config, test_models(Arc::clone(&model)))
        .await
        .unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let mut harness = RpcHarness::spawn(agent);
    harness
        .send(
            json!("new"),
            "turn.send",
            Some(json!({"session_id": info.session_id,"text":"new input exact"})),
        )
        .await;
    let accepted = tokio::time::timeout(Duration::from_secs(2), harness.response(json!("new")))
        .await
        .unwrap();
    let turn = accepted["result"]["turn"].clone();
    assert!(turn["loop_id"].is_string());
    entered.notified().await;
    for (id, method, params) in [
        (
            "state",
            "session.state",
            json!({"session_id":info.session_id}),
        ),
        (
            "context",
            "session.context",
            json!({"session_id":info.session_id}),
        ),
        ("ping", "agent.ping", json!({})),
    ] {
        harness.send(json!(id), method, Some(params)).await;
        let response = tokio::time::timeout(Duration::from_secs(2), harness.response(json!(id)))
            .await
            .unwrap();
        assert!(response.get("error").is_none(), "{response}");
        if id == "context" {
            assert!(response["result"]["automatic"]["current"].is_object());
        }
    }
    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), harness.response(json!("cancel")))
            .await
            .unwrap()
            .get("error")
            .is_none()
    );
    let result = harness.call_ok("turn.wait", Some(turn_params(&turn))).await;
    assert_eq!(result["result"]["outcome"]["type"], "cancelled");
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let settled = harness
        .call_ok(
            "session.context",
            Some(json!({"session_id":info.session_id})),
        )
        .await;
    assert!(settled["result"]["automatic"]["current"].is_null());
    assert_eq!(
        settled["result"]["automatic"]["last"]["utility_usage"]["call_count"],
        1
    );
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn turn_send_allows_server_to_accept_oversized_current_input() {
    oversized_current_input_server_acceptance(true).await;
}
#[tokio::test]
async fn auto_disabled_still_allows_server_to_accept_over_estimated_budget() {
    oversized_current_input_server_acceptance(false).await;
}
