use super::*;

#[tokio::test]
async fn cancel_turn_returns_cancelled_report_and_session_recovers() {
    let fixture = AgentFixture::new("cancel").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::new(
        "main",
        [
            Step::gated_text(gate.clone(), "blocked"),
            Step::Text("after cancel"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "cancel me").await;

    // Wait until the first model call is actually parked before cancelling,
    // so the first script is consumed and the second turn gets `Text`.
    gate.wait_started().await;
    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert_persisted(&result);

    // The session can run a new loop after cleanup.
    let turn = send_text(&mut agent, info.session_id, "next").await;
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
}

#[tokio::test]
async fn cancelling_wrapped_tool_preserves_runtime_outcome_and_recovery() {
    let fixture = AgentFixture::new("presentation-cancel").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("bash", json!({"command": "sleep 30"})),
            Step::Text("after cancel"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .profile(profile);
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run slowly").await;
    events.expect(std::time::Duration::from_secs(2), "tool start event must arrive", |event| {
        matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn).then_some(())
    }).await;
    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert_persisted(&result);

    // Dropping the wrapped execute future must not leave the Session busy or
    // change the next loop's model/tool ownership.
    let next = send_text(&mut agent, info.session_id, "next").await;
    let next_result = wait_text(&agent, next).await;
    assert_completed(&next_result);
}

#[tokio::test]
async fn close_active_session_cancels_and_waits() {
    let fixture = AgentFixture::new("close").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());

    let fixture = fixture.script([Step::gated_text(gate.clone(), "blocked")]);
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "do work").await;
    let completion = agent.wait_turn_receiver(turn).unwrap();
    gate.wait_started().await;

    agent.close_session(info.session_id).await.unwrap();
    let result = crate::sessions::await_turn_completion(completion)
        .await
        .unwrap();
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::Shutdown,)
    );
    assert_persisted(&result);
    assert_eq!(
        crate::event::LoopOutcomeView::from_report(&result.report),
        crate::event::LoopOutcomeView::Cancelled {
            reason: crate::event::CancelReasonView::Shutdown
        }
    );
    gate.release();
    // No orphan task: reopening and running a fresh loop keeps working.
    agent.open_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn agent_drop_cancels_an_active_model_future() {
    let fixture = AgentFixture::new("drop-cancel").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let model = ScriptedModel::new(
        "main",
        [Step::BlockUntilDrop(
            gate.clone(),
            Arc::clone(&dropped),
            "never returned",
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let _turn = send_text(&mut agent, info.session_id, "drop me").await;
    gate.wait_started().await;

    drop(agent);
    wait_until(
        std::time::Duration::from_secs(1),
        "Agent drop must cancel the active model future",
        || dropped.load(Ordering::SeqCst),
    )
    .await;
    gate.release();
}

#[tokio::test]
async fn agent_drop_cancels_a_manual_compaction_model_future() {
    let fixture = AgentFixture::new("drop-compact-cancel").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let model = ScriptedModel::with_window(
        "main",
        40_000,
        [
            Step::Text("settled"),
            Step::BlockUntilDrop(gate.clone(), Arc::clone(&dropped), "never returned"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "settled").await;
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
    let _receiver = agent
        .compact_session(CompactSession {
            session_id: info.session_id,
            operation_id: "drop-compaction".to_owned(),
        })
        .await
        .unwrap();
    gate.wait_started().await;

    drop(agent);
    wait_until(
        std::time::Duration::from_secs(1),
        "Agent drop must cancel the manual model future",
        || dropped.load(Ordering::SeqCst),
    )
    .await;
    gate.release();
}

#[tokio::test]
async fn active_shutdown_waits_for_the_session_owned_worker() {
    let fixture = AgentFixture::new("shutdown-active").file("a.txt", b"hello");
    let model_gate = Arc::new(Gate::new());
    let model = ScriptedModel::new("main", [Step::gated_text(model_gate.clone(), "blocked")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let gate = Arc::new(Gate::new());
    PAUSE_BEFORE_JOIN.arm(info.session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, info.session_id, "shutdown").await;
    let completion = agent.wait_turn_receiver(turn).unwrap();
    gate.wait_started().await;
    model_gate.wait_started().await;

    let mut shutdown = tokio::spawn(agent.shutdown());
    let pending = tokio::time::timeout(std::time::Duration::from_millis(25), &mut shutdown).await;
    assert!(pending.is_err(), "shutdown must wait for the owned worker");
    gate.release();
    assert!(shutdown.await.unwrap().is_ok());
    let result = crate::sessions::await_turn_completion(completion)
        .await
        .unwrap();
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::Shutdown,)
    );
    assert_persisted(&result);
}

#[tokio::test]
async fn close_after_post_turn_result_still_joins_the_operation_worker() {
    let fixture = AgentFixture::new("shutdown-admission").file("a.txt", b"hello");
    let model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .auto();
    let (mut agent, info) = fixture.start().await;
    let gate = Arc::new(Gate::new());
    PAUSE_AFTER_COMPACTION_RESULT.arm(info.session_id, Arc::clone(&gate));
    let session = agent.loaded_session(info.session_id).unwrap();
    let submission = session
        .submit(minicore_runtime::execution::UserInput::text("wait").unwrap())
        .await
        .unwrap();
    let LoopSubmission::Accepted(accepted) = submission;
    wait_text(&agent, accepted.turn).await;
    gate.wait_started().await;

    let mut close = tokio::spawn(async move { agent.close_session(info.session_id).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut close)
            .await
            .is_err()
    );
    gate.release();
    assert!(close.await.unwrap().is_ok());
}

#[tokio::test]
async fn post_turn_wait_is_independent_and_compaction_cancel_does_not_change_turn() {
    let gate = Arc::new(Gate::new());
    let dropped = Arc::new(AtomicBool::new(false));
    // Cross the post-turn trigger with an older prefix plus a complete
    // recent loop; the retained tail itself remains inside the hard budget.
    let answer = Box::leak("answer ".repeat(12_000).into_boxed_str());
    let (data_dir, _guard, session_id, model, mut agent) = auto_admission_fixture_with_window(
        "post-turn-independent",
        true,
        40_000,
        synthetic_loop_record(
            LoopId::new().unwrap(),
            &"older ".repeat(14_000),
            "old answer",
            false,
        )
        .items,
        [
            Step::Events(
                answer
                    .as_bytes()
                    .chunks(4_000)
                    .map(|chunk| {
                        ModelEvent::text_delta(std::str::from_utf8(chunk).unwrap()).unwrap()
                    })
                    .chain([
                        ModelEvent::Usage {
                            usage: Usage::default().with_provider_total_tokens(Some(38_000)),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ])
                    .collect(),
            ),
            Step::BlockUntilDrop(gate.clone(), Arc::clone(&dropped), "summary"),
        ],
        None,
        None,
    )
    .await;
    let turn = send_text(&mut agent, session_id, "answer").await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), agent.wait_turn(turn))
        .await
        .expect("turn.wait must not await summary utility")
        .unwrap();
    assert_persisted(&result);
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), gate.wait_started())
        .await
        .expect("post-turn summary must start");
    let context = agent.session_context(session_id).unwrap();
    let operation_id = context.current_operation.unwrap().operation_id;
    assert_eq!(operation_id, format!("auto-{}", turn.loop_id));
    assert!(matches!(
        agent
            .send(SendMessage {
                session_id,
                text: "next".to_owned()
            })
            .await,
        Err(AgentError::SessionBusy)
    ));
    assert!(
        agent
            .cancel_compaction(CompactSession {
                session_id,
                operation_id
            })
            .unwrap()
    );
    agent.close_session(session_id).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert!(
        flatten_texts(&read_store_history(&data_dir, session_id).await)
            .iter()
            .any(|text| text == answer)
    );
}
