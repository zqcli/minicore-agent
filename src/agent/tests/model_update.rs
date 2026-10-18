use super::*;

#[tokio::test]
async fn idle_model_update_affects_next_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("idle-update-{}", next_id()));
    let (workspace, _guard) = workspace_file("idle-update-ws", "a.txt", b"hello");
    let model_a = FakeModel::new("main", [ModelScript::Text("from a")]);
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let updated = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    let SessionUpdateResult {
        active_revision,
        session,
    } = updated;
    assert!(active_revision.is_none());
    assert_eq!(session.model, "other");

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&stored);
    assert!(texts.contains(&"from b".to_owned()));
    let requests = model_b.requests();
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn f2_01_idle_model_update_installs_candidate_loop_options() {
    let (data_dir, _guard) = fixture_dir(&format!("f2-01-{}", next_id()));
    let (workspace, _guard) = workspace_file("f2-01-ws", "a.txt", b"hello");
    let model_a = FakeModel::new("main", [ModelScript::Text("from a")]);
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before = session_options(&agent, info.session_id);
    let before_generation = session_config_generation(&agent, info.session_id);
    assert_eq!(before.prompt_timeout, std::time::Duration::from_secs(31));

    let updated = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_none());

    let after = session_options(&agent, info.session_id);
    assert_eq!(after.prompt_timeout, std::time::Duration::from_secs(47));
    assert_eq!(
        session_config_generation(&agent, info.session_id),
        before_generation.wrapping_add(1)
    );

    let turn = send_text(&mut agent, info.session_id, "next").await;
    wait_text(&agent, turn).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 0);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
}

#[tokio::test]
async fn f2_02_running_loop_keeps_its_request_and_uses_new_options_next_request() {
    let (data_dir, _guard) = fixture_dir(&format!("f2-02-{}", next_id()));
    let (workspace, _guard) = workspace_file("f2-02-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "main",
        [ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({"path": "a.txt", "limit": 16}),
        )],
    );
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "use the tool").await;
    gate.entered.notified().await;

    let updated = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_some());
    // The running loop keeps the options it captured at `AgentLoop::start`,
    // even though the future options now carry the candidate model's timeout.
    assert_eq!(
        crate::sessions::INSTALLED_LOOP_OPTIONS
            .get(&turn.loop_id)
            .expect("the running loop must record its install-time options")
            .prompt_timeout,
        std::time::Duration::from_secs(31)
    );
    assert_eq!(
        session_options(&agent, info.session_id).prompt_timeout,
        std::time::Duration::from_secs(47)
    );

    gate.release.notify_waiters();
    wait_text(&agent, turn).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
}

#[tokio::test]
async fn f2_03_close_and_reopen_rederives_installed_future_options() {
    let (data_dir, _guard) = fixture_dir(&format!("f2-03-{}", next_id()));
    let (workspace, _guard) = workspace_file("f2-03-ws", "a.txt", b"hello");
    let model_a = FakeModel::new("main", []);
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    let installed = session_options(&agent, info.session_id);
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let reopened = session_options(&agent, info.session_id);
    assert_eq!(reopened.prompt_timeout, installed.prompt_timeout);
    assert_eq!(reopened.max_tool_rounds, installed.max_tool_rounds);
    assert_eq!(
        agent.sessions.get(info.session_id).unwrap().record().model,
        "other"
    );

    let turn = send_text(&mut agent, info.session_id, "after reopen").await;
    // The reopened loop must derive exactly the options the pre-close update
    // installed, so a future Turn and a reopen cannot drift apart.
    let started = crate::sessions::INSTALLED_LOOP_OPTIONS
        .get(&turn.loop_id)
        .expect("the reopened loop records its install-time options");
    assert_eq!(started.prompt_timeout, installed.prompt_timeout);
    assert_eq!(started.max_tool_rounds, installed.max_tool_rounds);
    wait_text(&agent, turn).await;
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
}

#[tokio::test]
async fn f2_04_invalid_and_failed_updates_leave_future_state_unchanged() {
    let (data_dir, _guard) = fixture_dir(&format!("f2-04-{}", next_id()));
    let (workspace, _guard) = workspace_file("f2-04-ws", "a.txt", b"hello");
    let model_a = FakeModel::with_window("main", 16_000, [ModelScript::Text("still a")]);
    let model_b = FakeModel::with_reasoning(
        "other",
        4_000,
        BTreeSet::from([ReasoningPreference::Disabled]),
        [],
    );
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let original_options = session_options(&agent, info.session_id);
    let original_generation = session_config_generation(&agent, info.session_id);
    let original_budget = agent
        .session_context(info.session_id)
        .unwrap()
        .budget
        .input_budget_tokens;

    let invalid = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::High),
        })
        .await;
    assert!(matches!(invalid, Err(AgentError::InvalidSessionSettings)));
    assert_eq!(
        agent.sessions.get(info.session_id).unwrap().record().model,
        "main"
    );
    assert_eq!(
        session_options(&agent, info.session_id).prompt_timeout,
        original_options.prompt_timeout
    );
    assert_eq!(
        session_config_generation(&agent, info.session_id),
        original_generation
    );
    assert_eq!(
        agent
            .session_context(info.session_id)
            .unwrap()
            .budget
            .input_budget_tokens,
        original_budget
    );

    // A valid candidate (model `other`, explicitly `reasoning = disabled`, its
    // only supported mode) reaches the record write; the injected failure must
    // therefore surface as a Store error rather than an early settings
    // rejection.
    crate::store::RECORD_WRITE_FAILURES.arm(info.session_id, ());
    let failed = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::Disabled),
        })
        .await;
    assert!(matches!(failed, Err(AgentError::StoreUnavailable)));
    assert_eq!(
        agent.sessions.get(info.session_id).unwrap().record().model,
        "main"
    );
    assert_eq!(
        session_options(&agent, info.session_id).prompt_timeout,
        original_options.prompt_timeout
    );
    assert_eq!(
        session_config_generation(&agent, info.session_id),
        original_generation
    );
    assert_eq!(
        agent
            .session_context(info.session_id)
            .unwrap()
            .budget
            .input_budget_tokens,
        original_budget
    );

    let turn = send_text(&mut agent, info.session_id, "unchanged").await;
    wait_text(&agent, turn).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);
    assert_eq!(model_b.requests().lock().unwrap().len(), 0);
}

#[tokio::test]
async fn f2_05_sealed_race_persists_future_options_without_active_revision() {
    let (data_dir, _guard) = fixture_dir(&format!("f2-05-{}", next_id()));
    let (workspace, _guard) = workspace_file("f2-05-ws", "a.txt", b"hello");
    let model_a = FakeModel::new("main", [ModelScript::Text("from a")]);
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before_options = session_options(&agent, info.session_id);
    assert_eq!(
        before_options.prompt_timeout,
        std::time::Duration::from_secs(31)
    );
    let gate = Arc::new(Gate::new());
    PAUSE_BEFORE_JOIN.arm(info.session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, info.session_id, "finish").await;
    gate.wait_started().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if agent
                .runtime_loop_finished_for_test(info.session_id)
                .unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime must seal before the worker is joined");

    let before_generation = session_config_generation(&agent, info.session_id);
    let updated = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_none());
    assert_eq!(
        session_config_generation(&agent, info.session_id),
        before_generation.wrapping_add(1)
    );
    assert_eq!(
        session_options(&agent, info.session_id).prompt_timeout,
        std::time::Duration::from_secs(47),
        "a sealed update must persist the candidate model's options for the next loop"
    );
    assert_eq!(
        agent.sessions.get(info.session_id).unwrap().record().model,
        "other"
    );

    gate.release();
    wait_text(&agent, turn).await;
    wait_post_turn_noop(&agent, info.session_id, turn.loop_id).await;
    let next = send_text(&mut agent, info.session_id, "next").await;
    wait_text(&agent, next).await;
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
}

/// F2 sealed update with automatic compaction enabled: the candidate model's
/// context budget must be installed for the next loop even though the sealed
/// update returns no active revision. The two models use different windows so
/// the derived trigger/target budget is observably different.
#[tokio::test]
async fn f2_06_sealed_update_installs_the_new_model_context_budget() {
    let (data_dir, _guard) = fixture_dir(&format!("f2-06-{}", next_id()));
    let (workspace, _guard) = workspace_file("f2-06-ws", "a.txt", b"hello");
    let model_a = FakeModel::with_window("main", 16_000, [ModelScript::Text("from a")]);
    let model_b = FakeModel::with_window("other", 8_000, [ModelScript::Text("from b")]);
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before = agent.session_context(info.session_id).unwrap().budget;
    assert_eq!(before.input_budget_tokens, Some(16_000));
    assert_eq!(before.trigger_tokens, Some(12_800));

    // Seal the Runtime loop before the worker joins, then update.
    let gate = Arc::new(Gate::new());
    PAUSE_BEFORE_JOIN.arm(info.session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, info.session_id, "finish").await;
    gate.wait_started().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if agent
                .runtime_loop_finished_for_test(info.session_id)
                .unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime must seal before the worker is joined");

    let updated = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(
        updated.active_revision.is_none(),
        "a sealed loop cannot accept a live revision"
    );
    let after = agent.session_context(info.session_id).unwrap().budget;
    assert_eq!(
        after.input_budget_tokens,
        Some(8_000),
        "the sealed update must install the candidate model's window"
    );
    assert_eq!(after.trigger_tokens, Some(6_400));
    assert_eq!(after.target_tokens, Some(4_000));
    assert_eq!(
        session_options(&agent, info.session_id).prompt_timeout,
        std::time::Duration::from_secs(47)
    );

    gate.release();
    wait_text(&agent, turn).await;
    wait_post_turn_noop(&agent, info.session_id, turn.loop_id).await;
    let next = send_text(&mut agent, info.session_id, "next").await;
    wait_text(&agent, next).await;
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
    assert_eq!(
        agent
            .session_context(info.session_id)
            .unwrap()
            .budget
            .input_budget_tokens,
        Some(8_000)
    );
}
