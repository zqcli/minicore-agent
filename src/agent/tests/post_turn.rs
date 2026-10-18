use super::*;

#[tokio::test]
async fn post_turn_compacts_at_runtime_byte_trigger_with_a_large_model_window() {
    let items = (0..18)
        .flat_map(|_| {
            let mut record = synthetic_tool_loop_record(LoopId::new().unwrap());
            if let HistoryItem::ToolResult(result) = &mut record.items[2] {
                result.output = ToolOutput::new("x".repeat(90_000)).unwrap();
            }
            record.items
        })
        .collect::<Vec<_>>();
    let answer = Box::leak("a".repeat(160_000).into_boxed_str());
    let (data_dir, _guard, session_id, model, mut agent) = auto_admission_fixture_with_window(
        "post-turn-runtime-trigger",
        true,
        1_000_000,
        items,
        [
            Step::Text(answer),
            Step::Text("historical reads summarized"),
            Step::Text("next answer"),
        ],
        None,
        None,
    )
    .await;
    let state = agent.loaded_session(session_id).unwrap().compaction_state();
    state.note_prepare_failure("previous_attempt_failed");
    let turn = send_text(&mut agent, session_id, "answer").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    assert!(
        state.prepare_failure().is_none(),
        "successful Noop preparation clears a stale failure"
    );
    let compacted = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let context = agent.session_context(session_id).unwrap();
            if context.current_operation.is_none() {
                break context.last_result.unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        compacted.status,
        crate::compaction::CompactionStatus::Compacted
    );
    assert_eq!(compacted.utility_usage.unwrap().call_count, 1);
    assert!(compacted.before_tokens.unwrap() < 800_000);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert!(
        agent
            .session_context(session_id)
            .unwrap()
            .budget
            .estimated_history_bytes
            .unwrap()
            < 200_000
    );
    assert!(
        flatten_texts(&read_store_history(&data_dir, session_id).await)
            .iter()
            .any(|text| text == answer)
    );
    let next = send_text(&mut agent, session_id, "continue").await;
    assert!(matches!(
        wait_text(&agent, next).await.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    wait_post_turn_noop(&agent, session_id, next.loop_id).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    agent.close_session(session_id).await.unwrap();
}

#[tokio::test]
async fn post_turn_eligibility_excludes_failed_cancelled_disabled_and_unpersisted_turns() {
    let answer = Box::leak("large completed output ".repeat(650).into_boxed_str());
    for mode in ["failed", "cancelled", "disabled", "append_failed"] {
        let (data_dir, _guard) = fixture_dir(&format!("post-turn-ineligible-{mode}"));
        let workspace = data_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let error = minicore_runtime::model::ModelError::started(
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::error::DiagnosticSummary::new(
                minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
                minicore_runtime::error::DiagnosticCategory::Model,
                BoundedText::new("partial response failed").unwrap(),
                false,
            )
            .unwrap(),
        );
        let gate = Arc::new(Gate::new());
        let script = match mode {
            "failed" => Step::StreamErrorAfterText(answer, error),
            "cancelled" => Step::gated_text(gate.clone(), answer),
            _ => Step::Text(answer),
        };
        let model =
            ScriptedModel::with_window("main", 4_000, [script, Step::Text("must not summarize")])
                .with_pacing(std::time::Duration::from_millis(2));
        let models = BTreeMap::from([("main".to_owned(), Arc::clone(&model))]);
        let mut agent = if mode == "disabled" {
            open_agent(&data_dir, models, read_profile()).await
        } else {
            open_agent_auto(&data_dir, models, read_profile()).await
        };
        let info = create_session(&mut agent, &workspace).await;
        if mode == "append_failed" {
            crate::store::APPEND_FAILURES.arm(info.session_id, true);
        }
        let turn = send_text(&mut agent, info.session_id, "answer").await;
        if mode == "cancelled" {
            gate.wait_started().await;
            agent.cancel(turn).unwrap();
        }
        let result = wait_text(&agent, turn).await;
        if mode == "append_failed" {
            assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
        }
        let context = agent.session_context(info.session_id).unwrap();
        assert!(
            context.current_operation.is_none(),
            "ineligible {mode} reserved a summary"
        );
        assert!(context.last_result.is_none());
        assert!(
            context.recovery.is_none(),
            "partial output must never trigger recovery"
        );
        agent.close_session(info.session_id).await.unwrap();
        assert_eq!(
            model.calls.load(Ordering::SeqCst),
            1,
            "ineligible {mode} called utility"
        );
    }
}

#[tokio::test]
async fn manual_compaction_rejects_reserved_auto_namespace_but_cancel_accepts_it() {
    let (data_dir, _guard) = fixture_dir("manual-auto-namespace");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let request = CompactSession {
        session_id: info.session_id,
        operation_id: "auto-forged".to_owned(),
    };
    assert!(matches!(
        agent.compact_session(request.clone()).await,
        Err(AgentError::InvalidInput)
    ));
    assert!(!agent.cancel_compaction(request).unwrap());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_close_future_keeps_normal_worker_owned_until_second_join() {
    let (data_dir, _guard) = fixture_dir(&format!("close-cancel-owned-{}", next_id()));
    let (workspace, _guard) = workspace_file("close-cancel-owned-ws", "a.txt", b"hello");
    let model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(Gate::new());
    PAUSE_BEFORE_JOIN.arm(info.session_id, Arc::clone(&gate));

    let turn = send_text(&mut agent, info.session_id, "close later").await;
    gate.wait_started().await;
    let first = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        agent.close_session(info.session_id),
    )
    .await;
    assert!(
        first.is_err(),
        "cancelled close must still be waiting for the worker"
    );
    assert!(
        agent
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|session| session.session_id == info.session_id && session.loaded)
    );

    gate.release();
    let _ = agent.wait_turn(turn).await;
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_close_future_keeps_manual_worker_owned_until_second_join() {
    let (data_dir, _guard) = fixture_dir(&format!("compact-close-cancel-{}", next_id()));
    let (workspace, _guard) = workspace_file("compact-close-cancel-ws", "a.txt", b"hello");
    let model = ScriptedModel::with_window(
        "main",
        40_000,
        [Step::Text("settled"), Step::Text("summary")],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    let turn = send_text(&mut agent, info.session_id, &settled_text).await;
    wait_text(&agent, turn).await;
    agent.close_session(info.session_id).await.unwrap();
    let tail = synthetic_loop_record(
        LoopId::new().unwrap(),
        &"recent ".repeat(12_000),
        "RECENT_TAIL_ANSWER",
        false,
    );
    agent
        .store
        .append_loop(info.session_id, &tail)
        .await
        .unwrap();
    agent.open_session(info.session_id).await.unwrap();

    let gate = Arc::new(Gate::new());
    PAUSE_AFTER_COMPACTION_RESULT.arm(info.session_id, Arc::clone(&gate));
    let result = agent
        .compact(CompactSession {
            session_id: info.session_id,
            operation_id: "close-cancel-compaction".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        result.status,
        crate::compaction::CompactionStatus::Compacted,
        "unexpected manual compaction result: {result:?}"
    );
    gate.wait_started().await;

    let first = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        agent.close_session(info.session_id),
    )
    .await;
    assert!(
        first.is_err(),
        "cancelled close must still be waiting for compaction"
    );
    assert!(
        agent
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|session| session.session_id == info.session_id && session.loaded)
    );
    gate.release();
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn compaction_operation_ids_are_bounded_and_reset_on_reopen() {
    let (data_dir, _guard) = fixture_dir(&format!("compact-id-cap-{}", next_id()));
    let (workspace, _guard) = workspace_file("compact-id-cap-ws", "a.txt", b"hello");
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    for index in 0..4_096 {
        let result = agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: format!("bounded-{index}"),
            })
            .await
            .unwrap();
        assert_eq!(result.status, crate::compaction::CompactionStatus::Noop);
    }
    assert!(matches!(
        agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: "bounded-over-cap".to_owned(),
            })
            .await,
        Err(AgentError::InvalidInput)
    ));

    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(
        agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: "bounded-0".to_owned(),
            })
            .await
            .unwrap()
            .status,
        crate::compaction::CompactionStatus::Noop
    );
    agent.shutdown().await.unwrap();
}
