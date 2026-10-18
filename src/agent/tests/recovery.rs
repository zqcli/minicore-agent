use super::*;

#[tokio::test]
async fn test_provider_overflow_recovery_success() {
    let (data_dir, _guard) = fixture_dir("p3b2-recovery-success");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // A tool result large enough that replacing the whole exchange with the
    // labeled summary envelope strictly shrinks the provider body.
    std::fs::write(workspace.join("note.txt"), "recovery note ".repeat(400)).unwrap();

    let profile = read_profile();

    // One loop: request 0 runs the tool batch, request 1 rejects with
    // ContextOverflowNotStarted, utility summarizes the exchange, and the
    // same logical request is retried once with the clean projection.
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of prior read tool exchange"),
            ModelScript::Text("recovered answer"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);

    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "recovered answer"));
    let user_messages: Vec<_> = history
        .iter()
        .filter_map(|item| match item {
            HistoryItem::User(u) => Some(u.input.as_text().to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(user_messages, vec!["read note"]);
    // The original tool exchange stays in durable history exactly once even
    // though the retried request projected a summary instead.
    let tool_results = history
        .iter()
        .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
        .count();
    assert_eq!(tool_results, 1);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovered");
    assert!(recovery.before_tokens.is_some());
    assert!(recovery.after_tokens.is_some());
    assert!(recovery.utility_usage.is_some());
}

#[tokio::test]
async fn test_provider_overflow_stops_on_second_failure() {
    let (data_dir, _guard) = fixture_dir("p3b2-second-overflow");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("note.txt"), "recovery note ".repeat(400)).unwrap();

    let profile = read_profile();

    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of prior read exchange"),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("should not be reached"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Failed(_)
    ));
    // overflow, summary, second overflow: the retry never runs a third raw
    // start and the unused script is not consumed.
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovery_failed");
    assert_eq!(recovery.failure_kind.as_deref(), Some("context_overflow"));
    // The utility usage of the failed recovery attempt is retained.
    assert!(recovery.utility_usage.is_some());
}

#[tokio::test]
async fn emergency_projection_survives_followups_new_growth_and_durable_reopen() {
    let (data_dir, _guard) = fixture_dir("emergency-persistent-projection");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    for name in ["a", "b", "c"] {
        std::fs::write(
            workspace.join(format!("{name}.txt")),
            format!("RAW-{name} ").repeat(800),
        )
        .unwrap();
    }
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path":"a.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("SUMMARY-A: read a has already executed"),
            ModelScript::ToolCall("read", json!({"path":"b.txt"})),
            ModelScript::ToolCall("read", json!({"path":"c.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("SUMMARY-B: read b has already executed"),
            ModelScript::Text("completed after new growth"),
            ModelScript::Text("next turn"),
        ],
    );
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read three files").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    assert_eq!(
        result.report.tool_rounds, 3,
        "recovery must not redispatch completed tools"
    );
    let captured = model.requests();
    {
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 8);
        for index in [3, 4, 5, 7] {
            let encoded = serde_json::to_string(&requests[index]).unwrap();
            assert!(
                encoded.contains("SUMMARY-A"),
                "followup {index} lost its reduced base"
            );
            assert!(
                !encoded.contains("RAW-a"),
                "followup {index} restored covered raw history"
            );
        }
        assert!(
            serde_json::to_string(&requests[7])
                .unwrap()
                .contains("SUMMARY-B")
        );
        assert!(
            !serde_json::to_string(&requests[7])
                .unwrap()
                .contains("RAW-b")
        );
    }
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(
        stored
            .iter()
            .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
            .count(),
        3
    );
    assert!(serde_json::to_string(&stored).unwrap().contains("RAW-a"));
    agent.close_session(info.session_id).await.unwrap();
    assert!(
        data_dir
            .join("sessions")
            .join(info.session_id.to_string())
            .join("summary.json")
            .exists()
    );
    agent.open_session(info.session_id).await.unwrap();
    let next = send_text(&mut agent, info.session_id, "continue").await;
    wait_text(&agent, next).await;
    let requests = captured.lock().unwrap();
    let encoded = serde_json::to_string(&requests[8]).unwrap();
    assert!(encoded.contains("SUMMARY-A"));
    assert!(!encoded.contains("RAW-a"));
}

#[tokio::test]
async fn emergency_settlement_failure_is_observable_and_preserves_old_projection() {
    for unknown in [false, true] {
        let (data_dir, _guard) = fixture_dir(&format!("emergency-settlement-failure-{unknown}"));
        let workspace = data_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("a.txt"), "RAW-SETTLEMENT ".repeat(600)).unwrap();
        let model = FakeModel::with_window(
            "main",
            16_384,
            [
                ModelScript::ToolCall("read", json!({"path":"a.txt"})),
                ModelScript::ContextOverflowNotStarted,
                ModelScript::Text("summary with settled tool effects"),
                ModelScript::Text("answer"),
            ],
        );
        let mut agent = open_agent_auto(
            &data_dir,
            BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
            read_profile(),
        )
        .await;
        let info = create_session(&mut agent, &workspace).await;
        if unknown {
            crate::store::SUMMARY_UNKNOWN_WRITES.arm(info.session_id, ());
        } else {
            crate::store::SUMMARY_WRITE_FAILURES.arm(info.session_id, ());
        }
        let turn = send_text(&mut agent, info.session_id, "read a").await;
        let result = wait_text(&agent, turn).await;
        assert!(matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Completed
        ));
        assert_eq!(
            result.persistence,
            crate::sessions::TurnPersistence::Persisted
        );
        let context = agent.session_context(info.session_id).unwrap();
        assert_eq!(context.coverage.covered_item_count, 0);
        let observation = context.recovery.unwrap();
        assert_eq!(observation.outcome, "recovery_failed");
        assert_eq!(
            observation.failure_kind.as_deref(),
            Some(if unknown {
                "emergency_settlement_write_unknown"
            } else {
                "emergency_settlement_store"
            })
        );
        assert!(observation.utility_usage.is_some());
        assert!(
            agent
                .session_state(info.session_id)
                .unwrap()
                .block_reason
                .is_none()
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), 4);
        assert!(
            serde_json::to_string(&read_store_history(&data_dir, info.session_id).await)
                .unwrap()
                .contains("RAW-SETTLEMENT")
        );
        agent.close_session(info.session_id).await.unwrap();
    }
}

#[tokio::test]
async fn emergency_settlement_file_limit_is_an_explicit_failure_not_silent_raw_restore() {
    let (data_dir, _guard) = fixture_dir("emergency-settlement-size");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("a.txt"), "large raw tool ".repeat(500)).unwrap();
    // The projection itself fits the Runtime envelope, but JSON-escaping it
    // again inside the snapshot exceeds the unchanged 256 KiB file limit.
    let answer = Box::leak("\"".repeat(80_000).into_boxed_str());
    let model = FakeModel::with_window(
        "main",
        1_000_000,
        [
            ModelScript::ToolCall("read", json!({"path":"a.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("summary of read tool"),
            ModelScript::Text(answer),
        ],
    );
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read a").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context.recovery.unwrap();
    assert_eq!(recovery.outcome, "recovery_failed");
    assert_eq!(
        recovery.failure_kind.as_deref(),
        Some("emergency_settlement_snapshot_too_large")
    );
    assert_eq!(context.coverage.covered_item_count, 0);
    assert!(
        flatten_texts(&read_store_history(&data_dir, info.session_id).await)
            .iter()
            .any(|text| text == answer)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);
    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn post_turn_estimates_and_refreshes_a_fully_covered_emergency_projection() {
    let (data_dir, _guard) = fixture_dir("post-turn-refresh-emergency");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("a.txt"), "large raw tool ".repeat(500)).unwrap();
    let answer = Box::leak("final answer ".repeat(4_200).into_boxed_str());
    let gate = BlockGate::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path":"a.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("summary of read tool"),
            ModelScript::Text(answer),
            ModelScript::BlockUntilDrop(gate.clone(), Arc::clone(&dropped), "post-turn summary"),
        ],
    );
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read a").await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), agent.wait_turn(turn))
        .await
        .expect("turn.wait cannot wait for post-turn utility")
        .unwrap();
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("full emergency coverage still needs a near-budget post-turn check");
    let context = agent.session_context(info.session_id).unwrap();
    assert_eq!(context.coverage.retained_item_count, 0);
    assert_eq!(
        context.current_operation.unwrap().operation_id,
        format!("auto-{}", turn.loop_id)
    );
    agent.close_session(info.session_id).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(model.calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn emergency_projection_is_not_invalidated_by_request_boundary_settings_or_agents() {
    let (data_dir, _guard) = fixture_dir("emergency-update-keeps-source");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("a.txt"), "RAW-BEFORE-UPDATE ".repeat(500)).unwrap();
    std::fs::write(workspace.join("b.txt"), "small next result").unwrap();
    let gate = BlockGate::new();
    let model_a = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path":"a.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("SUMMARY-UPDATE: a was read already"),
            ModelScript::ToolCallAfterGate(gate.clone(), "read", json!({"path":"b.txt"})),
        ],
    );
    let model_b = FakeModel::with_window("other", 16_384, [ModelScript::Text("updated answer")]);
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read a then b").await;
    tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::Disabled),
        })
        .await
        .unwrap();
    std::fs::write(workspace.join("AGENTS.md"), "new workspace instruction").unwrap();
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    let requests = model_b.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].reasoning(), ReasoningPreference::Disabled);
    let encoded = serde_json::to_string(&requests[0]).unwrap();
    assert!(encoded.contains("new workspace instruction"));
    assert!(encoded.contains("SUMMARY-UPDATE"));
    assert!(!encoded.contains("RAW-BEFORE-UPDATE"));
    assert_eq!(result.report.tool_rounds, 2);
}

#[tokio::test]
async fn test_recovery_refuses_when_summary_cannot_shrink() {
    let (data_dir, _guard) = fixture_dir("p3b2-no-shrink");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // A tiny exchange is smaller than the labeled summary envelope, so the
    // utility can never produce a strictly smaller projection for it. The
    // single summary cannot win the budget; no reduce or second raw start
    // is attempted, and the remaining model scripts stay unused.
    std::fs::write(workspace.join("tiny.txt"), "x").unwrap();

    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "tiny.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of the tiny read exchange"),
            ModelScript::Text("S"),
            ModelScript::Text("never"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read tiny").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Failed(_)
    ));
    // Tool call, overflow and one utility call; no second raw start happens.
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovery_failed");
    assert_eq!(
        recovery.failure_kind.as_deref(),
        Some("context_uncompressible")
    );
    assert_eq!(recovery.utility_usage.unwrap().call_count, 1);
}

#[tokio::test]
async fn non_recoverable_model_failures_are_not_retried_as_recovery() {
    let cases = [
        (
            "unknown-provider-unavailable",
            minicore_runtime::model::ModelErrorKind::ProviderUnavailable,
            minicore_runtime::model::DeliveryState::Unknown,
            false,
        ),
        (
            "started-provider-unavailable",
            minicore_runtime::model::ModelErrorKind::ProviderUnavailable,
            minicore_runtime::model::DeliveryState::Started,
            false,
        ),
        (
            "unknown-context-overflow",
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::model::DeliveryState::Unknown,
            false,
        ),
        (
            "started-context-overflow",
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::model::DeliveryState::Started,
            false,
        ),
        (
            "started-mid-stream-context-overflow",
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::model::DeliveryState::Started,
            true,
        ),
    ];
    for (label, kind, delivery, mid_stream) in cases {
        let (data_dir, _guard) = fixture_dir(label);
        let workspace = data_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("note.txt"), "provider note ".repeat(200)).unwrap();

        let diagnostic = minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
            minicore_runtime::error::DiagnosticCategory::Model,
            minicore_runtime::value::BoundedText::new("injected model failure").unwrap(),
            false,
        )
        .unwrap();
        let error = match delivery {
            minicore_runtime::model::DeliveryState::Unknown => {
                ModelError::unknown(kind, diagnostic)
            }
            minicore_runtime::model::DeliveryState::Started => {
                ModelError::started(kind, diagnostic)
            }
            _ => unreachable!("only unknown and started deliveries are exercised"),
        };

        // The failing request follows a real tool exchange, so a compressible
        // recovery source exists; only `ContextOverflow + NotStarted` may use
        // it.
        let failing = if mid_stream {
            ModelScript::StreamErrorAfterText("partial text", error)
        } else {
            ModelScript::Error(error)
        };
        let model = FakeModel::with_window(
            "main",
            16_384,
            [
                ModelScript::ToolCall("read", json!({"path": "note.txt"})),
                failing,
                ModelScript::Text("must not be reached"),
            ],
        );

        let mut agent = open_agent_auto(
            &data_dir,
            BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
            read_profile(),
        )
        .await;

        let info = create_session(&mut agent, &workspace).await;

        let turn = send_text(&mut agent, info.session_id, "read note").await;
        let res = wait_text(&agent, turn).await;
        assert!(
            matches!(res.report.outcome, minicore_runtime::LoopOutcome::Failed(_)),
            "{label} must fail the turn"
        );
        // Tool request plus the injected failure; recovery never starts.
        assert_eq!(model.calls.load(Ordering::SeqCst), 2, "{label}");
        let context = agent.session_context(info.session_id).unwrap();
        assert!(context.recovery.is_none(), "{label}");
    }
}

#[tokio::test]
async fn test_driver_retry_reentry_does_not_reset_recovery_quota() {
    let (data_dir, _guard) = fixture_dir("p3b2-retry-reentry");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("note.txt"), "recovery note ".repeat(400)).unwrap();

    let profile = read_profile();

    let retryable = ModelError::not_started(
        minicore_runtime::model::ModelErrorKind::RateLimited,
        Some(std::time::Duration::from_millis(1)),
        minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::ModelUnavailable,
            minicore_runtime::error::DiagnosticCategory::Model,
            minicore_runtime::value::BoundedText::new("retry shortly").unwrap(),
            true,
        )
        .unwrap(),
    );

    // The second start fails with a Driver-retryable error, so the Driver
    // re-enters the same logical request. That retry must not grant another
    // ContextOverflow recovery even though the next attempt overflows again.
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of prior read exchange"),
            ModelScript::Error(retryable),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("never reached"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Failed(_)
    ));
    // tool call, overflow, summary, retryable start failure, re-entered
    // overflow: the unused script is never consumed by a second recovery.
    assert_eq!(model.calls.load(Ordering::SeqCst), 5);

    // Main requests keep one logical identity (loop id + request index) and
    // one deadline across the recovery retry and the Driver retry; the utility
    // runs on its own loop with its own attempt window.
    let contexts = model.contexts();
    let contexts = contexts.lock().unwrap();
    assert_eq!(contexts.len(), 5);
    let loop_id = contexts[0].loop_id;
    assert_eq!(contexts[0].request_index, 0);
    for index in [1, 3, 4] {
        assert_eq!(contexts[index].loop_id, loop_id);
        assert_eq!(contexts[index].request_index, 1);
        assert_eq!(contexts[index].deadline, contexts[1].deadline);
    }
    assert_ne!(contexts[2].loop_id, loop_id);
    assert_eq!(contexts[2].request_index, 0);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovery_failed");
    // The same reduced logical request exhausted its normal delivery retry;
    // this never grants a second summary attempt.
    assert_eq!(recovery.failure_kind.as_deref(), Some("context_overflow"));
    assert!(recovery.utility_usage.is_some());
    let requests = model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests[3], requests[4],
        "Driver retry must use the same reduced body"
    );
    for request in [&requests[3], &requests[4]] {
        let body = serde_json::to_string(request).unwrap();
        assert!(body.contains("Summary of prior read exchange"));
        assert!(!body.contains("recovery note recovery note"));
    }
}

#[tokio::test]
async fn test_model_update_before_turn_uses_new_model() {
    let (data_dir, _guard) = fixture_dir("p3b2-stale-ticket-update");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    let profile = read_profile();

    let model_a = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("not reached because stale"),
        ],
    );
    let model_b = FakeModel::with_window("other", 16_384, [ModelScript::Text("from model b")]);

    let mut models_map = BTreeMap::new();
    models_map.insert("main".to_owned(), model_config("PATH"));
    models_map.insert("other".to_owned(), model_config("PATH"));

    let config = auto_config(data_dir.to_path_buf(), models_map, profile);

    let runtime_models = BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a) as Arc<dyn Model>),
        ("other".to_owned(), Arc::clone(&model_b) as Arc<dyn Model>),
    ]);

    let mut agent = Agent::open_with_models(config, Models::from_values(runtime_models))
        .await
        .unwrap();

    let info = create_session(&mut agent, &workspace).await;

    let updated = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::Auto),
        })
        .await
        .unwrap();
    assert_eq!(updated.active_revision, None);

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let res = wait_text(&agent, turn).await;
    assert_eq!(res.report.outcome, minicore_runtime::LoopOutcome::Completed);
    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "from model b"));
}

#[tokio::test]
async fn test_failed_update_does_not_publish_a_new_recovery_binding() {
    let (data_dir, _guard) = fixture_dir("p3b2-failed-update");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("note.txt"), "note ".repeat(200)).unwrap();

    let profile = read_profile();

    // Model A serves the session and its recovery utility.
    let model_a = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("summary from a"),
            ModelScript::Text("recovered by a"),
        ],
    );
    // Model B cannot serve High reasoning, so the update fails inside the
    // config factory after the model lookup succeeded.
    let model_b = FakeModel::with_reasoning(
        "other",
        16_384,
        BTreeSet::from([ReasoningPreference::Auto]),
        [ModelScript::Text("summary from b")],
    );

    let mut models_map = BTreeMap::new();
    models_map.insert("main".to_owned(), model_config("PATH"));
    models_map.insert("other".to_owned(), model_config("PATH"));
    let config = auto_config(data_dir.to_path_buf(), models_map, profile);

    let runtime_models = BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a) as Arc<dyn Model>),
        ("other".to_owned(), Arc::clone(&model_b) as Arc<dyn Model>),
    ]);
    let mut agent = Agent::open_with_models(config, Models::from_values(runtime_models))
        .await
        .unwrap();

    let info = create_session(&mut agent, &workspace).await;
    let failed = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::High),
        })
        .await;
    assert!(failed.is_err(), "the update must fail");

    // The session keeps model A and its recovery binding: the overflow
    // recovery must be summarized by A, never by the failed update's model.
    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "recovered by a"));
    assert_eq!(model_b.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_recovery_two_sessions_isolated() {
    let (data_dir, _guard) = fixture_dir("p3b2-sessions-isolated");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("a.txt"), "content a ".repeat(400)).unwrap();

    let profile = read_profile();

    // Session A overflows inside the same loop that ran its tool batch;
    // session B must see no recovery observation and no shared ticket.
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary for A"),
            ModelScript::Text("recovered a"),
            ModelScript::Text("session b answer"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info_a = create_session(&mut agent, &workspace).await;
    let info_b = create_session(&mut agent, &workspace).await;

    let turn_a = send_text(&mut agent, info_a.session_id, "read a").await;
    let result_a = wait_text(&agent, turn_a).await;
    assert_eq!(
        result_a.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let turn_b = send_text(&mut agent, info_b.session_id, "hello from b").await;
    let result_b = wait_text(&agent, turn_b).await;
    assert_eq!(
        result_b.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let context_a = agent.session_context(info_a.session_id).unwrap();
    let recovery_a = context_a.recovery.expect("session A has recovery");
    assert_eq!(recovery_a.outcome, "recovered");

    let context_b = agent.session_context(info_b.session_id).unwrap();
    assert!(context_b.recovery.is_none());

    let history_a = read_store_history(&data_dir, info_a.session_id).await;
    let history_b = read_store_history(&data_dir, info_b.session_id).await;
    assert!(
        flatten_texts(&history_a)
            .iter()
            .any(|text| text == "recovered a")
    );
    assert!(
        !flatten_texts(&history_b)
            .iter()
            .any(|text| text == "recovered a")
    );
}
