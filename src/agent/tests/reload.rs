use super::*;

#[tokio::test]
async fn i2_01_session_reuses_observation_resources_across_updates_and_reload() {
    let (data_dir, _guard) = fixture_dir(&format!("i2-01-{}", next_id()));
    let (workspace_a, _workspace_a_guard) = workspace_file("i2-01-a", "a.txt", b"a");
    let (workspace_b, _workspace_b_guard) = workspace_file("i2-01-b", "b.txt", b"b");
    let model_main =
        ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let model_other =
        ScriptedModel::new("other", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent_auto_with_configs(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_main)),
            ("other".to_owned(), Arc::clone(&model_other)),
        ]),
        BTreeMap::from([
            ("main".to_owned(), model_config("MINICORE_AGENT_TEST_KEY")),
            ("other".to_owned(), model_config("MINICORE_AGENT_TEST_KEY")),
        ]),
        read_profile(),
    )
    .await;
    let first = create_session(&mut agent, &workspace_a).await;
    let second = create_session(&mut agent, &workspace_b).await;
    let first_session = agent.loaded_session(first.session_id).unwrap();
    let second_session = agent.loaded_session(second.session_id).unwrap();
    let first_data = first_session.tool_data();
    let first_owners = first_session.command_owners();
    let first_observer = first_session.parts().observer;
    assert!(Arc::ptr_eq(&first_data, &first_observer.tool_data()));
    assert_eq!(first_observer.session_id(), first.session_id);
    assert!(!Arc::ptr_eq(&first_data, &second_session.tool_data()));
    assert!(!Arc::ptr_eq(
        &first_owners,
        &second_session.command_owners()
    ));

    agent
        .update_session(UpdateSession {
            session_id: first.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first_data, &first_session.tool_data()));
    assert!(Arc::ptr_eq(&first_owners, &first_session.command_owners()));
    assert!(Arc::ptr_eq(
        &first_observer,
        &first_session.parts().observer
    ));

    let candidate = agent.config().clone();
    let models = Models::from_values(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_main) as Arc<dyn Model>),
        (
            "other".to_owned(),
            Arc::clone(&model_other) as Arc<dyn Model>,
        ),
    ]));
    assert_eq!(
        agent
            .reload_settings_with_models(candidate, models)
            .unwrap(),
        crate::agent::ReloadResult { ok: true }
    );
    assert!(Arc::ptr_eq(&first_data, &first_session.tool_data()));
    assert!(Arc::ptr_eq(&first_owners, &first_session.command_owners()));
    assert!(Arc::ptr_eq(
        &first_observer,
        &first_session.parts().observer
    ));
}

#[tokio::test]
async fn i2_05_session_observation_resources_are_released_without_cycles() {
    let (data_dir, _guard) = fixture_dir(&format!("i2-05-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("i2-05-ws", "a.txt", b"a");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session = agent.loaded_session(info.session_id).unwrap();
    let weak_data: Weak<ToolData> = Arc::downgrade(&session.tool_data());
    let weak_owners: Weak<CommandOwners> = Arc::downgrade(&session.command_owners());
    let weak_observer: Weak<ToolObserver> = Arc::downgrade(&session.parts().observer);
    drop(session);

    agent.close_session(info.session_id).await.unwrap();
    drop(agent);
    assert!(weak_data.upgrade().is_none());
    assert!(weak_owners.upgrade().is_none());
    assert!(weak_observer.upgrade().is_none());
}

#[tokio::test]
async fn embedded_open_without_a_file_source_cannot_reload() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-no-source-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-no-source-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]),
        read_profile(),
    )
    .await;

    assert!(matches!(
        agent.reload().await,
        Err(AgentError::ReloadUnavailable)
    ));
    let _ = create_session(&mut agent, &workspace).await;
}

#[tokio::test]
async fn reload_keeps_active_loop_snapshot_and_updates_the_next_turn() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-active-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-active-ws", "a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model_a = ScriptedModel::new("main", [Step::gated_text(gate.clone(), "old")])
        .with_pacing(std::time::Duration::from_millis(2));
    let model_b = ScriptedModel::new("main", [Step::Text("new")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model_a))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let first = send_text(&mut agent, info.session_id, "first").await;
    gate.wait_started().await;

    let candidate = agent.config().clone();
    let candidate_models = Models::from_values(BTreeMap::from([(
        "main".to_owned(),
        Arc::clone(&model_b) as Arc<dyn Model>,
    )]));
    assert_eq!(
        agent
            .reload_settings_with_models(candidate, candidate_models)
            .unwrap(),
        crate::agent::ReloadResult { ok: true }
    );

    gate.release();
    wait_text(&agent, first).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);

    let second = send_text(&mut agent, info.session_id, "second").await;
    wait_text(&agent, second).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reload_does_not_change_a_running_compaction_binding() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-compaction-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-compaction-ws", "a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model_a = ScriptedModel::with_window(
        "main",
        40_000,
        [
            Step::Text("settled"),
            Step::gated_text(gate.clone(), "old summary"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let model_b = ScriptedModel::new("main", [Step::Text("new turn")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("replacement".to_owned(), Arc::clone(&model_b)),
        ]),
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

    let mut result = agent
        .compact_session(CompactSession {
            session_id: info.session_id,
            operation_id: "reload-compaction-binding".to_owned(),
        })
        .await
        .unwrap();
    gate.wait_started().await;

    let candidate = agent.config().clone();
    let candidate_models = Models::from_values(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_b) as Arc<dyn Model>),
        (
            "replacement".to_owned(),
            Arc::clone(&model_b) as Arc<dyn Model>,
        ),
    ]));
    agent
        .reload_settings_with_models(candidate, candidate_models)
        .unwrap();

    gate.release();
    let compacted = loop {
        if let Some(result) = result.borrow().clone() {
            break result;
        }
        result.changed().await.unwrap();
    };
    assert_eq!(
        compacted.status,
        crate::compaction::CompactionStatus::Compacted
    );

    let next = send_text(&mut agent, info.session_id, "after reload").await;
    wait_text(&agent, next).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 2);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn reload_command_environment_accumulates_all_prior_credential_names() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-env-merge-{}", next_id()));
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;

    let mut candidate_b = agent.config().clone();
    if let Some(ModelConfig::OpenAiResponses { api_key_env, .. }) =
        candidate_b.models.get_mut("main")
    {
        *api_key_env = "MINICORE_RELOAD_UNIT_KEY_B".to_owned();
    }
    agent
        .reload_settings_with_models(
            candidate_b,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                Arc::clone(&model) as Arc<dyn Model>,
            )])),
        )
        .unwrap();

    let mut candidate_c = agent.config().clone();
    if let Some(ModelConfig::OpenAiResponses { api_key_env, .. }) =
        candidate_c.models.get_mut("main")
    {
        *api_key_env = "MINICORE_RELOAD_UNIT_KEY_C".to_owned();
    }
    agent
        .reload_settings_with_models(
            candidate_c,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                Arc::clone(&model) as Arc<dyn Model>,
            )])),
        )
        .unwrap();

    let names = agent
        .command_environment
        .names()
        .iter()
        .map(|name| name.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "MINICORE_AGENT_TEST_KEY",
            "MINICORE_RELOAD_UNIT_KEY_B",
            "MINICORE_RELOAD_UNIT_KEY_C",
        ]
    );
}

#[tokio::test]
async fn legacy_six_tool_session_is_listable_renameable_but_not_openable() {
    let (data_dir, _data_guard) = fixture_dir(&format!("legacy-open-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("legacy-open-ws", "a.txt", b"hello");
    let session_id = SessionId::new().unwrap();
    let store = Store::open(data_dir.clone()).await.unwrap();
    let record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: Some("legacy".to_owned()),
        profile: "test".to_owned(),
        workspace,
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
        created_at: "2026-01-02T03:04:05.000Z".to_owned(),
        updated_at: "2026-01-02T03:04:05.000Z".to_owned(),
    };
    store.create_session(&record).await.unwrap();
    let mut legacy = record.clone();
    legacy.tools.push("subagent".to_owned());
    store.write_record(&legacy).await.unwrap();
    store
        .append_loop(
            session_id,
            &synthetic_loop_record(
                LoopId::new().unwrap(),
                "legacy user",
                "legacy assistant",
                false,
            ),
        )
        .await
        .unwrap();

    let session_dir = data_dir.join("sessions").join(session_id.to_string());
    let history_path = session_dir.join("history.jsonl");
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .unwrap();
        file.write_all(b"{\"partial_legacy_tail\": true").unwrap();
    }
    let summary_path = session_dir.join("summary.json");
    std::fs::write(&summary_path, b"historical summary").unwrap();
    let aux_dir = session_dir
        .join(crate::store::AUX_TOOLS_DIR)
        .join("legacy-aux");
    std::fs::create_dir_all(&aux_dir).unwrap();
    std::fs::write(
        aux_dir.join(crate::store::TOOL_RECORD_FILE),
        b"historical auxiliary record",
    )
    .unwrap();
    std::fs::write(
        aux_dir.join(crate::store::TOOL_INPUT_FILE),
        b"historical auxiliary input",
    )
    .unwrap();

    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let listed = agent.list_sessions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert!(!listed[0].loaded);

    let renamed = agent
        .rename_session(crate::agent::RenameSession {
            session_id,
            title: "renamed legacy".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(renamed.title.as_deref(), Some("renamed legacy"));
    let persisted = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(session_id)
        .await
        .unwrap();
    assert_eq!(persisted.tools, legacy.tools);
    let record_before_open = std::fs::read(session_dir.join("session.json")).unwrap();
    let history_before_open = std::fs::read(&history_path).unwrap();
    let summary_before_open = std::fs::read(&summary_path).unwrap();
    let mut aux_before_open = BTreeMap::new();
    for entry in std::fs::read_dir(&aux_dir).unwrap() {
        let entry = entry.unwrap();
        aux_before_open.insert(entry.file_name(), std::fs::read(entry.path()).unwrap());
    }

    let read = agent
        .read_session(ReadSession {
            view: crate::read::ReadView::Canonical,
            projection_revision: None,
            session_id,
            cursor: None,
            limit: 100,
            max_bytes: None,
            captured_end: None,
            history_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(read.session.session_id, session_id);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before_open);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_before_open);
    assert!(matches!(
        agent.open_session(session_id).await,
        Err(AgentError::InvalidSessionSettings)
    ));
    assert_eq!(
        std::fs::read(session_dir.join("session.json")).unwrap(),
        record_before_open
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before_open);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_before_open);
    for (name, expected_bytes) in aux_before_open {
        assert_eq!(
            std::fs::read(aux_dir.join(name)).unwrap(),
            expected_bytes,
            "auxiliary bytes must remain unchanged"
        );
    }
    assert!(agent.loaded_session(session_id).is_none());
    assert!(model.requests().lock().unwrap().is_empty());
    assert!(!agent.list_sessions().await.unwrap()[0].loaded);
}

#[tokio::test]
async fn reload_rejects_removed_subagent_profile_without_publishing_or_starting_work() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-subagent-{}", next_id()));
    let model = ScriptedModel::new("main", [Step::Text("unused")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let original = agent.list_profiles();
    let mut candidate = agent.config().clone();
    candidate.profiles.get_mut("test").unwrap().tools = vec!["subagent".to_owned()];

    assert!(matches!(
        agent.reload_settings_with_models(
            candidate,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                Arc::clone(&model) as Arc<dyn Model>,
            )])),
        ),
        Err(AgentError::Config(
            crate::config::ConfigError::InvalidProfile
        ))
    ));
    assert_eq!(agent.list_profiles(), original);
    assert!(model.requests().lock().unwrap().is_empty());
}

#[tokio::test]
async fn reload_preserves_session_snapshots_history_and_is_atomic_on_failure() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-atomic-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-atomic-ws", "a.txt", b"hello");
    let model_main = ScriptedModel::new("main", [Step::Text("main")])
        .with_pacing(std::time::Duration::from_millis(2));
    let model_other = ScriptedModel::new("other", [Step::Text("other")])
        .with_pacing(std::time::Duration::from_millis(2));
    let model_reloaded = ScriptedModel::new("main", [Step::Text("reloaded")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_main)),
            ("other".to_owned(), Arc::clone(&model_other)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let first = send_text(&mut agent, info.session_id, "before reload").await;
    wait_text(&agent, first).await;
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let record_before = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    let models_before = agent
        .list_models()
        .into_iter()
        .map(|model| model.id)
        .collect::<Vec<_>>();

    let mut candidate = agent.config().clone();
    candidate.profiles.get_mut("test").unwrap().model = "other".to_owned();
    candidate.profiles.get_mut("test").unwrap().system_prompt = "new profile prompt".to_owned();
    candidate.models.remove("main");
    let candidate_models = Models::from_values(BTreeMap::from([(
        "other".to_owned(),
        Arc::clone(&model_other) as Arc<dyn Model>,
    )]));
    assert!(matches!(
        agent.reload_settings_with_models(candidate, candidate_models),
        Err(AgentError::ModelNotFound)
    ));
    assert_eq!(
        agent
            .list_models()
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>(),
        models_before
    );
    assert_eq!(agent.list_profiles()[0].model, "main");
    assert_eq!(agent.config().default_profile, "test");
    assert_eq!(agent.list_sessions().await.unwrap()[0].model, "main");
    let record_disk_after = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&record_before).unwrap(),
        serde_json::to_vec(&record_disk_after).unwrap()
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

    let mut valid = agent.config().clone();
    valid.profiles.get_mut("test").unwrap().model = "other".to_owned();
    valid.profiles.get_mut("test").unwrap().system_prompt = "new profile prompt".to_owned();
    valid.profiles.insert(
        "new-default".to_owned(),
        Profile {
            model: "other".to_owned(),
            reasoning: ReasoningPreference::Auto,
            system_prompt: "new default prompt".to_owned(),
            tools: Vec::new(),
            max_tool_rounds: 8,
            approval: ApprovalMode::Auto,
        },
    );
    valid.default_profile = "new-default".to_owned();
    let valid_models = Models::from_values(BTreeMap::from([
        (
            "main".to_owned(),
            Arc::clone(&model_reloaded) as Arc<dyn Model>,
        ),
        (
            "other".to_owned(),
            Arc::clone(&model_other) as Arc<dyn Model>,
        ),
    ]));
    agent
        .reload_settings_with_models(valid, valid_models)
        .unwrap();
    let profiles = agent.list_profiles();
    assert!(
        profiles
            .iter()
            .any(|profile| profile.id == "new-default" && profile.model == "other")
    );
    assert!(
        profiles
            .iter()
            .any(|profile| profile.id == "test" && profile.model == "other")
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let record_after = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&record_before).unwrap(),
        serde_json::to_vec(&record_after).unwrap()
    );

    let second = send_text(&mut agent, info.session_id, "after reload").await;
    wait_text(&agent, second).await;
    {
        let requests = model_reloaded.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let system_prompt = requests[0]
            .messages()
            .iter()
            .find_map(|message| match message {
                ModelMessage::System(text) => Some(text.as_ref()),
                _ => None,
            })
            .expect("reloaded request must have a system prompt");
        assert!(system_prompt.contains("test system prompt"));
        assert!(!system_prompt.contains("new profile prompt"));
    }

    let (new_workspace, _new_workspace_guard) =
        workspace_file("reload-atomic-new-ws", "b.txt", b"new");
    let new_info = create_session(&mut agent, &new_workspace).await;
    assert_eq!(new_info.profile, "new-default");
    assert_eq!(new_info.model, "other");
    let third = send_text(&mut agent, new_info.session_id, "new session").await;
    wait_text(&agent, third).await;
    let requests = model_other.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let new_system_prompt = requests[0]
        .messages()
        .iter()
        .find_map(|message| match message {
            ModelMessage::System(text) => Some(text.as_ref()),
            _ => None,
        })
        .expect("new-session request must have a system prompt");
    assert!(new_system_prompt.contains("new default prompt"));
    assert!(!new_system_prompt.contains("test system prompt"));
}

#[tokio::test]
async fn reload_rejects_store_and_event_capacity_changes_before_swap() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-restart-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-restart-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]),
        read_profile(),
    )
    .await;
    let _ = create_session(&mut agent, &workspace).await;

    let mut data_dir_candidate = agent.config().clone();
    data_dir_candidate.data_dir = data_dir.join("other-data");
    assert!(matches!(
        agent.reload_settings_with_models(
            data_dir_candidate,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2))
                    as Arc<dyn Model>,
            )]))
        ),
        Err(AgentError::ReloadRequiresRestart)
    ));

    let mut event_capacity_candidate = agent.config().clone();
    event_capacity_candidate.event_capacity += 1;
    assert!(matches!(
        agent.reload_settings_with_models(
            event_capacity_candidate,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2))
                    as Arc<dyn Model>,
            )]))
        ),
        Err(AgentError::ReloadRequiresRestart)
    ));
    assert_eq!(agent.config().event_capacity, 256);
}

#[tokio::test]
async fn reload_preserves_a_blocked_session_without_unblocking_or_persisting() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-blocked-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-blocked-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", [Step::Text("done")])
                .with_pacing(std::time::Duration::from_millis(2)),
        )]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    crate::store::APPEND_FAILURES.arm(info.session_id, true);
    let turn = send_text(&mut agent, info.session_id, "block").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        crate::sessions::SessionStatus::Blocked
    );
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();

    let candidate_models = Models::from_values(BTreeMap::from([(
        "main".to_owned(),
        ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2))
            as Arc<dyn Model>,
    )]));
    let candidate = agent.config().clone();
    agent
        .reload_settings_with_models(candidate, candidate_models)
        .unwrap();
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        crate::sessions::SessionStatus::Blocked
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert!(matches!(
        agent
            .send(crate::agent::SendMessage {
                session_id: info.session_id,
                text: "still blocked".to_owned(),
            })
            .await,
        Err(AgentError::SessionBlocked)
    ));
}
