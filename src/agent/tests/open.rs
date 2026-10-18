use super::*;

#[tokio::test]
async fn cancelled_open_does_not_publish_a_history_index() {
    let (data_dir, _guard) = fixture_dir(&format!("cancel-open-{}", next_id()));
    let (workspace, _guard) = workspace_file("cancel-open-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "saved").await;
    wait_text(&agent, turn).await;
    agent.close_session(info.session_id).await.unwrap();

    let gate = Arc::new(crate::test_support::Gate::new());
    crate::store::SESSION_LOAD_GATES.arm(info.session_id, Arc::clone(&gate));
    let mut opening = Box::pin(agent.open_session(info.session_id));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            result = &mut opening => panic!("open completed before the gate: {result:?}"),
            _ = gate.wait_started() => {}
        }
    })
    .await
    .unwrap();
    drop(opening);
    assert!(matches!(
        agent.session_state(info.session_id),
        Err(AgentError::SessionNotLoaded)
    ));

    let history = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let mut bytes = std::fs::read(&history).unwrap();
    bytes.insert(0, b' ');
    std::fs::write(&history, &bytes).unwrap();
    let page = agent
        .read_session(ReadSession {
            view: crate::read::ReadView::Canonical,
            ..display_read(info.session_id)
        })
        .await
        .unwrap();
    assert_eq!(page.history_revision, crate::store::hash_bytes(&bytes));
    assert_eq!(page.captured_end, bytes.len() as u64);
    agent.open_session(info.session_id).await.unwrap();
    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn reopen_rejects_missing_workspace_and_keeps_type_identity() {
    let (data_dir, _guard) = fixture_dir(&format!("ws-identity-{}", next_id()));
    let (workspace, _guard) = workspace_file("ws-identity-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "persist before close").await;
    wait_text(&agent, turn).await;
    agent.close_session(info.session_id).await.unwrap();

    // Deleting the workspace makes reopen fail cleanly instead of running
    // against a different root.
    std::fs::remove_dir_all(&workspace).unwrap();
    assert!(matches!(
        agent.open_session(info.session_id).await,
        Err(AgentError::Workspace)
    ));

    // Failed open must release its index; an unloaded read observes disk afresh.
    let history = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let mut bytes = std::fs::read(&history).unwrap();
    bytes.insert(0, b' ');
    std::fs::write(&history, &bytes).unwrap();
    let page = agent
        .read_session(ReadSession {
            view: crate::read::ReadView::Canonical,
            ..display_read(info.session_id)
        })
        .await
        .unwrap();
    assert_eq!(page.history_revision, crate::store::hash_bytes(&bytes));
    assert_eq!(page.captured_end, bytes.len() as u64);
}

#[cfg(unix)]
#[tokio::test]
async fn reopen_rejects_workspace_redirected_by_symlink() {
    use std::os::unix::fs::symlink;

    let (data_dir, _guard) = fixture_dir(&format!("ws-symlink-{}", next_id()));
    let (workspace, _guard) = workspace_file("ws-symlink-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();
    let canonical = workspace.canonicalize().unwrap();

    // Move the real directory and put a symlink at its former path that
    // redirects elsewhere: reopen must refuse to continue on the wrong root.
    let other = workspace.with_extension("moved");
    std::fs::rename(&canonical, &other).unwrap();
    symlink(&other, &workspace).unwrap();
    assert!(matches!(
        agent.open_session(info.session_id).await,
        Err(AgentError::Workspace)
    ));
}

#[tokio::test]
async fn reopen_after_profile_removal_still_works() {
    let (data_dir, _guard) = fixture_dir(&format!("ws-profile-{}", next_id()));
    let (workspace, _guard) = workspace_file("ws-profile-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();
    drop(agent);

    // A second agent whose config no longer has the "test" profile still
    // reopens the session, because the session record carries its own frozen
    // settings.
    let second_profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "other profile".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 4,
        approval: ApprovalMode::Auto,
    };
    let models = BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]);
    let models_config =
        BTreeMap::from([("main".to_owned(), model_config("MINICORE_AGENT_TEST_KEY"))]);
    let mut second_config = config(
        data_dir.to_path_buf(),
        models_config,
        second_profile.clone(),
    );
    // The second agent has no "test" profile at all.
    second_config.default_profile = "other".to_owned();
    second_config.profiles = BTreeMap::from([("other".to_owned(), second_profile)]);
    let second_models = models
        .iter()
        .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
        .collect();
    let mut second = Agent::open_with_models(second_config, Models::from_values(second_models))
        .await
        .unwrap();
    let reopened = second.open_session(info.session_id).await.unwrap();
    assert!(reopened.loaded);
    assert_eq!(reopened.session_id, info.session_id);
}

#[tokio::test]
async fn invalid_agents_md_fails_the_turn_as_a_prompt_error() {
    let (data_dir, _guard) = fixture_dir(&format!("agents-invalid-{}", next_id()));
    let (workspace, _guard) = workspace_file("agents-invalid-ws", "AGENTS.md", b"\xff\xfe");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = agent.wait_turn(turn).await.unwrap();
    match &result.report.outcome {
        minicore_runtime::LoopOutcome::Failed(failure) => {
            assert_eq!(failure.kind, minicore_runtime::LoopFailureKind::Prompt)
        }
        other => panic!("expected a prompt failure, got {other:?}"),
    }
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
}

#[tokio::test]
async fn invalid_session_json_skipped_by_list_and_fails_open() {
    let (data_dir, _guard) = fixture_dir(&format!("invalid-record-{}", next_id()));
    let (workspace, _guard) = workspace_file("invalid-record-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();

    let record_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("session.json");
    let raw = std::fs::read_to_string(&record_path).unwrap();
    let mut parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
    parsed["system_prompt"] = json!("");
    let updated = serde_json::to_string(&parsed).unwrap();
    std::fs::write(&record_path, updated).unwrap();

    let listed = agent.list_sessions().await.unwrap();
    assert!(listed.is_empty());
    assert!(matches!(
        agent.open_session(info.session_id).await,
        Err(AgentError::Store)
    ));

    agent.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// P2: structured tool invocation/execution data (spec 3.1 and 7)
// ---------------------------------------------------------------------------
