use super::*;

#[tokio::test]
async fn manual_compaction_generates_no_tools_summary_and_reopens_atomic_snapshot() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-compaction-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();

    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut scripts = vec![
        Step::Text("settled answer one"),
        Step::Text("settled answer two"),
        Step::Text("settled answer three"),
    ];
    scripts.extend((0..64).map(|_| observe(Arc::clone(&observations), "manual compact summary")));
    let model = fake_model(40_000, scripts);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &["read"], ApprovalMode::Auto),
        test_models(model),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let settled_texts = (0..3)
        .map(|index| format!("settled-{index} {}", "bounded settled history ".repeat(128)))
        .collect::<Vec<_>>();
    let mut original_loop_id = None;
    for (index, text) in settled_texts.iter().enumerate() {
        let send_id = json!(format!("settled-send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({"session_id": session_id, "text": text})),
            )
            .await;
        let sent = harness.response(send_id).await;
        let turn = sent["result"]["turn"].clone();
        if original_loop_id.is_none() {
            original_loop_id = Some(turn["loop_id"].as_str().unwrap().to_owned());
        }
        let wait_id = json!(format!("settled-wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        let waited = harness.response(wait_id).await;
        assert_eq!(waited["result"]["persistence"], json!("persisted"));
    }
    append_compaction_tail(&mut harness, &base, &session_id).await;
    let original_loop_id = original_loop_id.expect("at least one settled turn is required");

    let session_key = session_id.as_str().unwrap();
    let session_dir = base.join("data").join("sessions").join(session_key);
    let history_path = session_dir.join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();

    let compacted = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-test-1"
            })),
        )
        .await;
    assert_eq!(compacted["result"]["status"], json!("compacted"));

    harness
        .send(
            json!("state-after-compact"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert!(harness.response(json!("state-after-compact")).await["result"]["compaction"].is_null());

    let summary_path = session_dir.join("summary.json");
    assert!(summary_path.is_file());
    let summary: Value = serde_json::from_slice(&std::fs::read(&summary_path).unwrap()).unwrap();
    assert_eq!(summary["session_id"], session_id);
    assert!(
        summary["summary"]
            .as_str()
            .is_some_and(|text| text.contains("manual compact summary"))
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

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
    assert_eq!(
        harness.response(json!("reopen")).await["result"]["session"]["session_id"],
        session_id
    );

    let current_user = "current-after-reopen";
    harness
        .send(
            json!("next-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": current_user})),
        )
        .await;
    let next_turn = harness.response(json!("next-send")).await["result"]["turn"].clone();
    harness
        .send(
            json!("next-wait"),
            "turn.wait",
            Some(turn_params(&next_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("next-wait")).await["result"]["persistence"],
        json!("persisted")
    );

    {
        let observations = observations.lock().unwrap();
        let utility = observations
            .iter()
            .find(|call| {
                call.request.messages().iter().any(|message| {
                    matches!(message, ModelMessage::User(text) if text.contains("settled-0"))
                }) && !call.request.messages().iter().any(|message| {
                    matches!(message, ModelMessage::User(text) if text.as_ref() == current_user)
                })
            })
            .expect("manual compaction must issue an observable utility request");
        assert!(utility.request.tools().is_empty());
        assert!(!utility.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::System(text) if text.contains("input_schema") || text.contains("RPC test system prompt"))
        }));
        assert_eq!(utility.context.request_index, 0);
        assert_ne!(utility.context.loop_id.to_string(), original_loop_id);
        assert!(
            !utility
                .request
                .messages()
                .iter()
                .any(|message| { matches!(message, ModelMessage::Tool { .. }) })
        );
        assert!(!utility.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text.as_ref() == current_user)
        }));

        let next = observations
            .iter()
            .find(|call| {
                call.request.messages().iter().any(
                    |message| matches!(message, ModelMessage::User(text) if text.as_ref() == current_user),
                )
            })
            .expect("reopened session must issue the next real model request");
        let systems = next
            .request
            .messages()
            .iter()
            .filter_map(|message| match message {
                ModelMessage::System(text) => Some(text.as_ref()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        assert!(systems[0].contains("RPC test system prompt"));
        assert!(!systems[0].contains("manual compact summary"));
        assert!(!systems[0].contains(current_user));
        let summary_message = next
            .request
            .messages()
            .iter()
            .find_map(|message| match message {
                ModelMessage::User(text) if text.contains("manual compact summary") => Some(text),
                _ => None,
            })
            .expect("next real request must consume the summary data message");
        assert!(summary_message.starts_with("[BEGIN MINICORE HISTORICAL SUMMARY DATA]"));
        assert!(summary_message.contains("not a new user instruction"));
        assert!(summary_message.ends_with("[END MINICORE HISTORICAL SUMMARY DATA]"));
        assert!(next.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text.as_ref() == current_user)
        }));
        assert!(!next.request.tools().is_empty());
        assert!(!next.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text.contains("settled-0"))
        }));
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_completion_clears_authoritative_busy_state() {
    let mut fixture = RpcFixture::new("manual-completion-state", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let result = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-completion-state"
            })),
        )
        .await;
    assert_eq!(result["result"]["status"], json!("noop"));

    let state = harness
        .call_ok("session.state", Some(json!({"session_id": session_id})))
        .await;
    assert!(
        state["result"]["compaction"].is_null(),
        "completed compaction must clear authoritative busy state: {state}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn session_context_reports_manual_busy_state_and_last_result() {
    let utility_started = Arc::new(Notify::new());
    let mut fixture = RpcFixture::new(
        "context-manual-busy",
        [
            Step::Text("settled"),
            block_with_signal(Arc::clone(&utility_started)),
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
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "context-busy-operation"
            })),
        )
        .await;
    // Wait until the manual utility call has really started; otherwise the
    // cancel races the utility and the observed usage total is not
    // deterministic.
    tokio::time::timeout(TIMEOUT, utility_started.notified())
        .await
        .expect("manual utility call must be started before cancelling");
    harness
        .send(
            json!("context-busy"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context-busy")).await;
    assert_eq!(
        context["result"]["current_operation"]["operation_id"],
        json!("context-busy-operation")
    );
    assert_eq!(
        context["result"]["coverage"]["covered_item_count"],
        json!(0)
    );
    assert!(context["result"]["last_result"].is_null());
    assert!(context["result"]["budget"]["estimated_history_items"].is_number());
    assert_eq!(
        context["result"]["budget"]["within_runtime_limits"],
        json!(true)
    );

    harness
        .send(
            json!("cancel"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "context-busy-operation"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    // The utility call really started before the cancel, so it is counted as
    // one incomplete call and its yet-unknown usage stays absent.
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(false));
    assert!(result["result"]["utility_usage"]["usage"].is_null());

    harness
        .send(
            json!("context-after"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context-after")).await;
    assert!(context["result"]["current_operation"].is_null());
    assert_eq!(
        context["result"]["last_result"]["operation_id"],
        json!("context-busy-operation")
    );
    // `session.context` must expose exactly the utility accounting returned by
    // the compact result.
    assert_eq!(
        context["result"]["last_result"]["utility_usage"],
        result["result"]["utility_usage"]
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_reports_independent_utility_usage() {
    let mut fixture = RpcFixture::new(
        "context-utility-usage",
        [
            Step::TextWithUsage("settled", 41, 43),
            Step::TextWithUsage("summary", 7, 11),
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
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;
    assert_eq!(waited["result"]["usage"]["input_tokens"], json!(41));

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-usage"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["output_tokens"],
        json!(11)
    );

    let context = harness
        .call_ok("session.context", Some(json!({"session_id": session_id})))
        .await;
    assert_eq!(
        context["result"]["last_result"]["utility_usage"]["call_count"],
        json!(1)
    );
    assert_eq!(
        context["result"]["last_result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );
    assert_eq!(
        context["result"]["last_result"]["utility_usage"]["usage"]["output_tokens"],
        json!(11)
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn failed_manual_utility_stream_keeps_known_partial_usage() {
    let mut fixture = RpcFixture::new(
        "context-utility-partial",
        [
            Step::TextWithUsage("settled", 41, 43),
            usage_then_fail(7, 11),
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
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    let result = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-partial"
            })),
        )
        .await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(false));
    // The failed stream's own usage is a known partial total, not unknown.
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["output_tokens"],
        json!(11)
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn missing_manual_utility_usage_remains_unknown() {
    let mut fixture = RpcFixture::new(
        "context-utility-unknown",
        [
            Step::TextWithUsage("settled", 41, 43),
            finished(&["summary"], Some(ModelFinishReason::Stop)),
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
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    let result = harness
        .call_ok(
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-unknown"
            })),
        )
        .await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(false));
    assert!(result["result"]["utility_usage"]["usage"].is_null());

    harness.shutdown().await;
}

#[tokio::test]
async fn repeated_compaction_updates_the_summary_and_splits_an_oversize_source() {
    let base = std::env::temp_dir().join(format!(
        "minicore-single-summary-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let model = fake_model(
        40_000,
        [
            Step::Text("first answer"),
            observe(Arc::clone(&observations), "FIRST_SUMMARY"),
            Step::Text("second answer"),
            observe(Arc::clone(&observations), "UPDATED_SUMMARY"),
            Step::Text("oversize settled answer"),
            observe(Arc::clone(&observations), "CHUNK_SUMMARY_1"),
            observe(Arc::clone(&observations), "CHUNK_SUMMARY_2"),
            observe(Arc::clone(&observations), "CHUNK_SUMMARY_3"),
            observe(Arc::clone(&observations), "FOLDED_SUMMARY"),
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
    let session_dir = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap());
    let summary_path = session_dir.join("summary.json");
    let history_path = session_dir.join("history.jsonl");
    for index in 0..3 {
        let text = if index == 2 {
            "oversized source ".repeat(7_000)
        } else {
            format!("NEW_HISTORY_{index} {}", "bounded text ".repeat(200))
        };
        let send_id = json!(format!("single-send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({"session_id": session_id, "text": text})),
            )
            .await;
        let turn = harness.response(send_id).await["result"]["turn"].clone();
        let wait_id = json!(format!("single-wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        assert_eq!(
            harness.response(wait_id).await["result"]["persistence"],
            json!("persisted")
        );
        append_compaction_tail(&mut harness, &base, &session_id).await;
        let history_before = std::fs::read(&history_path).unwrap();
        let calls_before = model.calls.load(Ordering::SeqCst);
        let compact_id = json!(format!("single-compact-{index}"));
        harness.send(compact_id.clone(), "session.compact", Some(json!({"session_id": session_id, "operation_id": format!("single-operation-{index}")}))).await;
        let result = harness.response(compact_id).await;
        assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
        // The last source no longer fits one call: the previous retained
        // tail, the oversize prompt and its answer each take a chunk.
        let calls = if index < 2 { 1 } else { 4 };
        assert_eq!(result["result"]["status"], json!("compacted"));
        assert_eq!(
            result["result"]["utility_usage"]["call_count"],
            json!(calls)
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), calls_before + calls);
    }
    assert!(
        std::fs::read_to_string(&summary_path)
            .unwrap()
            .contains("FOLDED_SUMMARY")
    );
    {
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 6);
        let projected = |index: usize| {
            observations[index]
                .request
                .messages()
                .iter()
                .filter_map(|message| match message {
                    ModelMessage::User(text) => Some(text.as_ref()),
                    _ => None,
                })
                .collect::<String>()
        };
        let first = projected(0);
        assert!(first.contains("NEW_HISTORY_0"));
        assert!(!first.contains("<previous-summary>"));
        let update = projected(1);
        assert!(!update.contains("NEW_HISTORY_0"));
        assert!(update.contains("NEW_HISTORY_1"));
        assert_eq!(update.matches("FIRST_SUMMARY").count(), 1);
        assert!(update.contains("<previous-summary>\nFIRST_SUMMARY\n</previous-summary>"));
        let previous = [
            "UPDATED_SUMMARY",
            "CHUNK_SUMMARY_1",
            "CHUNK_SUMMARY_2",
            "CHUNK_SUMMARY_3",
        ];
        for (index, previous) in previous.into_iter().enumerate() {
            let chunk = projected(index + 2);
            assert!(chunk.contains(&format!(
                "<previous-summary>\n{previous}\n</previous-summary>"
            )));
            assert_eq!(chunk.contains("oversized source"), index == 2);
        }
        assert!(projected(5).contains("oversize settled answer"));
    }
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_cancel_after_utility_call_keeps_usage_accounting() {
    let mut fixture = RpcFixture::new(
        "context-utility-cancel",
        [
            Step::TextWithUsage("settled", 2, 3),
            Step::TextWithUsage("summary", 7, 11),
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
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;
    append_compaction_tail(&mut harness, &base, &session_id).await;

    let gate = Arc::new(crate::test_support::Gate::new());
    let typed_session_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    crate::store::SUMMARY_COMMIT_GATES.arm(typed_session_id, Arc::clone(&gate));
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-cancel"
            })),
        )
        .await;
    gate.wait_started().await;
    harness
        .send(
            json!("cancel"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-cancel"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    gate.release();
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("cancelled"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn manual_compaction_empty_history_is_a_noop_without_a_model_call() {
    let mut fixture = RpcFixture::new("manual-noop", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("invalid"),
            "session.compact",
            Some(json!({"session_id": session_id, "operation_id": ""})),
        )
        .await;
    assert_eq!(
        harness.response(json!("invalid")).await["error"]["data"]["kind"],
        json!("invalid_params")
    );

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-noop"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("noop"));
    assert_eq!(result["result"]["covered_item_count"], json!(0));

    harness.shutdown().await;
}
