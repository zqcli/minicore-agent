use super::*;

/// A session whose history is just under the token trigger until the next answer.
async fn runtime_trigger_fixture(
    label: &str,
) -> (
    PathBuf,
    TempDir,
    SessionId,
    Arc<ScriptedModel>,
    Agent,
    &'static str,
) {
    let items = (0..8)
        .flat_map(|_| {
            let mut record = synthetic_tool_loop_record(LoopId::new().unwrap());
            if let HistoryItem::ToolResult(result) = &mut record.items[2] {
                result.output = ToolOutput::new("x".repeat(90_000)).unwrap();
            }
            record.items
        })
        .collect::<Vec<_>>();
    let answer = Box::leak("a".repeat(160_000).into_boxed_str());
    let (data_dir, guard, session_id, model, agent) = auto_admission_fixture_with_window(
        label,
        true,
        250_000,
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
    (data_dir, guard, session_id, model, agent, answer)
}

#[tokio::test]
async fn post_turn_compacts_when_the_answer_crosses_the_token_trigger() {
    let (data_dir, _guard, session_id, model, mut agent, answer) =
        runtime_trigger_fixture("post-turn-runtime-trigger").await;
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
    let compacted = wait_post_turn_result(&agent, session_id, turn.loop_id).await;
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
            AgentFixture::at(&data_dir).models(models).open().await
        } else {
            AgentFixture::at(&data_dir)
                .models(models)
                .auto()
                .open()
                .await
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
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .auto()
        .open()
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
    let fixture = AgentFixture::new("close-cancel-owned").file("a.txt", b"hello");
    let model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
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
    let fixture = AgentFixture::new("compact-close-cancel").file("a.txt", b"hello");
    let model = ScriptedModel::with_window(
        "main",
        40_000,
        [Step::Text("settled"), Step::Text("summary")],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
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
    let fixture = AgentFixture::new("compact-id-cap").file("a.txt", b"hello");
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;

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

/// The result of the post-turn check of `loop_id`, once it has one.
async fn wait_post_turn_result(
    agent: &Agent,
    session_id: SessionId,
    loop_id: LoopId,
) -> crate::compaction::CompactionResult {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let context = agent.session_context(session_id).unwrap();
            if let Some(result) = context
                .last_result
                .filter(|result| result.operation_id == format!("auto-{loop_id}"))
            {
                break result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

/// The compaction phases the session state events carried, in order.
async fn compaction_phases(mut events: crate::AgentEventStream) -> Vec<crate::CompactionPhase> {
    let mut phases = Vec::new();
    while let Some(event) = events.recv().await {
        if let AgentEvent::SessionState { state, .. } = event
            && let Some(progress) = state.compaction
            && phases.last() != Some(&progress.phase)
        {
            phases.push(progress.phase);
        }
    }
    phases
}

#[tokio::test]
async fn a_noop_post_turn_check_publishes_no_compaction_progress() {
    let (_data_dir, _guard, session_id, _model, mut agent) = auto_admission_fixture_with_window(
        "post-turn-silent-noop",
        true,
        100_000,
        vec![],
        [Step::Text("short answer")],
        None,
        None,
    )
    .await;
    let events = agent.take_events().unwrap();
    let gate = Arc::new(Gate::new());
    PAUSE_IN_POST_TURN_CHECK.arm(session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, session_id, "question").await;
    wait_text(&agent, turn).await;
    gate.wait_started().await;
    assert!(
        agent
            .session_context(session_id)
            .unwrap()
            .current_operation
            .is_none(),
        "the check must not look like a compaction"
    );
    gate.release();
    let result = wait_post_turn_result(&agent, session_id, turn.loop_id).await;
    assert_eq!(result.status, crate::compaction::CompactionStatus::Noop);
    agent.shutdown().await.unwrap();
    assert!(compaction_phases(events).await.is_empty());
}

#[tokio::test]
async fn a_send_during_the_post_turn_check_is_not_refused() {
    let (_data_dir, _guard, session_id, _model, mut agent) = auto_admission_fixture_with_window(
        "post-turn-send",
        true,
        100_000,
        vec![],
        [Step::Text("first"), Step::Text("second")],
        None,
        None,
    )
    .await;
    let gate = Arc::new(Gate::new());
    PAUSE_IN_POST_TURN_CHECK.arm(session_id, Arc::clone(&gate));
    let first = send_text(&mut agent, session_id, "one").await;
    wait_text(&agent, first).await;
    gate.wait_started().await;
    let second = send_text(&mut agent, session_id, "two").await;
    assert_completed(&*wait_text(&agent, second).await);
    wait_post_turn_noop(&agent, session_id, second.loop_id).await;
    agent.close_session(session_id).await.unwrap();
}

#[tokio::test]
async fn a_post_turn_check_over_the_threshold_compacts_and_shows_its_phases() {
    let (_data_dir, _guard, session_id, _model, mut agent, _answer) =
        runtime_trigger_fixture("post-turn-phases").await;
    let events = agent.take_events().unwrap();
    let gate = Arc::new(Gate::new());
    PAUSE_IN_POST_TURN_CHECK.arm(session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, session_id, "answer").await;
    wait_text(&agent, turn).await;
    gate.wait_started().await;
    assert!(
        agent
            .session_context(session_id)
            .unwrap()
            .current_operation
            .is_none()
    );
    gate.release();
    let result = wait_post_turn_result(&agent, session_id, turn.loop_id).await;
    assert_eq!(
        result.status,
        crate::compaction::CompactionStatus::Compacted
    );
    agent.shutdown().await.unwrap();
    assert_eq!(
        compaction_phases(events).await,
        [
            crate::CompactionPhase::Preparing,
            crate::CompactionPhase::Summarizing,
            crate::CompactionPhase::Committing,
        ]
    );
}
