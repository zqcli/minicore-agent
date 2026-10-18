use super::*;

#[tokio::test]
async fn create_and_open_do_not_start_agent_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("open-no-loop-{}", next_id()));
    let (workspace, _guard) = workspace_file("open-ws", "a.txt", b"hello");
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
    assert!(agent.list_sessions().await.unwrap()[0].loaded);

    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
    assert!(state.active_loop.is_none());

    // Re-open from disk also starts no loop.
    let info2 = create_session(&mut agent, &workspace).await;
    agent.close_session(info2.session_id).await.unwrap();
    let reopened = agent.open_session(info.session_id).await.unwrap();
    let state = agent.session_state(reopened.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
}

#[tokio::test]
async fn create_and_loaded_open_emit_session_opened_once() {
    let (data_dir, _guard) = fixture_dir(&format!("session-opened-{}", next_id()));
    let (workspace, _guard) = workspace_file("session-opened-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();

    let info = create_session(&mut agent, &workspace).await;
    match events
        .recv()
        .await
        .expect("create must publish SessionOpened")
    {
        AgentEvent::SessionOpened { session, meta } => {
            assert_eq!(session, info);
            assert_eq!(meta.session_id, info.session_id);
            assert_eq!(meta.loop_id, None);
        }
        event => panic!("unexpected create event: {event:?}"),
    }

    let created_session = agent.loaded_session(info.session_id).unwrap();
    let created_data = created_session.tool_data();
    let created_presentation = created_session.presentation();
    let loaded = agent.open_session(info.session_id).await.unwrap();
    assert_eq!(loaded, info);
    let loaded_session = agent.loaded_session(info.session_id).unwrap();
    assert!(Arc::ptr_eq(&created_data, &loaded_session.tool_data()));
    assert!(Arc::ptr_eq(
        &created_presentation,
        &loaded_session.presentation()
    ));
    assert!(futures_util::poll!(pin!(events.recv())).is_pending());

    agent.close_session(info.session_id).await.unwrap();
    match events
        .recv()
        .await
        .expect("close must publish SessionClosed")
    {
        AgentEvent::SessionClosed {
            session_id: closed_id,
            meta,
        } => {
            assert_eq!(closed_id, info.session_id);
            assert_eq!(meta.session_id, info.session_id);
            assert_eq!(meta.loop_id, None);
        }
        event => panic!("unexpected close event: {event:?}"),
    }

    let reopened = agent.open_session(info.session_id).await.unwrap();
    assert_eq!(reopened, info);
    let reopened_session = agent.loaded_session(info.session_id).unwrap();
    assert!(!Arc::ptr_eq(
        &created_session.tool_data(),
        &reopened_session.tool_data()
    ));
    assert!(!Arc::ptr_eq(
        &created_session.presentation(),
        &reopened_session.presentation()
    ));
    match events
        .recv()
        .await
        .expect("reopen must publish SessionOpened")
    {
        AgentEvent::SessionOpened { session, meta } => {
            assert_eq!(session, info);
            assert_eq!(meta.session_id, info.session_id);
            assert_eq!(meta.loop_id, None);
        }
        event => panic!("unexpected reopen event: {event:?}"),
    }
    assert!(futures_util::poll!(pin!(events.recv())).is_pending());
}

#[tokio::test]
async fn create_capability_precheck_leaves_no_session_directory() {
    let (data_dir, _guard) = fixture_dir(&format!("create-capability-precheck-{}", next_id()));
    let (workspace, _workspace_guard) =
        workspace_file("create-capability-precheck-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]),
        read_profile(),
    )
    .await;
    let mut invalid_profile = read_profile();
    invalid_profile.tools = vec!["read".to_owned(), "read".to_owned()];
    agent.profiles = crate::profiles::Profiles::from_values(BTreeMap::from([(
        "test".to_owned(),
        invalid_profile,
    )]));

    let result = agent
        .create_session(CreateSession {
            workspace,
            profile: String::new(),
            model: None,
            reasoning: None,
            title: Some("must not persist".to_owned()),
        })
        .await;
    assert!(matches!(result, Err(AgentError::InvalidSessionSettings)));
    assert!(agent.list_sessions().await.unwrap().is_empty());
    assert!(
        std::fs::read_dir(data_dir.join("sessions"))
            .unwrap()
            .next()
            .is_none(),
        "capability precheck must run before creating a session directory"
    );
}

#[tokio::test]
async fn i1_create_open_and_turns_leave_branch_unknown_until_explicit_status() {
    let (data_dir, _data_guard) = fixture_dir(&format!("i1-turns-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("i1-turns-ws", "file.txt", b"contents\n");
    let init = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .arg(&workspace)
        .status()
        .expect("git must be available for the branch-cache regression");
    assert!(init.success());
    let head = std::process::Command::new("git")
        .args(["-C"])
        .arg(&workspace)
        .args(["symbolic-ref", "HEAD", "refs/heads/i1-test"])
        .status()
        .unwrap();
    assert!(head.success());

    let model = ScriptedModel::new(
        "main",
        [
            Step::Text("no tool"),
            Step::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            Step::Text("tool complete"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        None
    );
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        None
    );

    let no_tool = send_text(&mut agent, info.session_id, "no tool").await;
    wait_text(&agent, no_tool).await;
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        None
    );

    let with_tool = send_text(&mut agent, info.session_id, "read file").await;
    wait_text(&agent, with_tool).await;
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        None
    );
}

#[cfg(unix)]
#[tokio::test]
async fn i1_explicit_status_projects_and_failed_status_clears_branch_cache() {
    use crate::workspace::status::STATUS_PROGRAMS;
    use std::os::unix::fs::PermissionsExt;
    use tokio_util::sync::CancellationToken;

    let (data_dir, _data_guard) = fixture_dir(&format!("i1-status-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("i1-status-ws", "file.txt", b"contents\n");
    let init = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .arg(&workspace)
        .status()
        .expect("git must be available for the status projection regression");
    assert!(init.success());
    let head = std::process::Command::new("git")
        .args(["-C"])
        .arg(&workspace)
        .args(["symbolic-ref", "HEAD", "refs/heads/i1-status"])
        .status()
        .unwrap();
    assert!(head.success());

    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        None
    );

    let status = agent
        .workspace_status(crate::WorkspaceStatusRequest {
            session_id: info.session_id,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert!(status.complete);
    assert!(status.repo_available);
    assert_eq!(status.branch.as_deref(), Some("i1-status"));
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        Some("i1-status".to_owned())
    );

    let marker = data_dir.join("status-fails");
    let script = data_dir.join("status-wrapper");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ -f '{}' ]; then exit 1; fi\nexec git \"$@\"\n",
            marker.display()
        ),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    STATUS_PROGRAMS.arm(
        std::fs::canonicalize(&workspace).unwrap(),
        script.into_os_string(),
    );
    std::fs::write(&marker, b"fail\n").unwrap();

    let failed = agent
        .workspace_status(crate::WorkspaceStatusRequest {
            session_id: info.session_id,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert!(!failed.complete);
    assert!(!failed.repo_available);
    assert_eq!(failed.branch, None);
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .git_branch,
        None
    );

    let session = agent.loaded_session(info.session_id).unwrap();
    session.complete_status_query(&status, &CancellationToken::new());
    assert_eq!(
        session.presentation_view().git_branch,
        Some("i1-status".to_owned())
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    session.complete_status_query(&failed, &cancelled);
    assert_eq!(
        session.presentation_view().git_branch,
        Some("i1-status".to_owned())
    );

    agent.close_session(info.session_id).await.unwrap();
    session.complete_status_query(&failed, &CancellationToken::new());
    assert_eq!(
        session.presentation_view().git_branch,
        Some("i1-status".to_owned())
    );
}

#[tokio::test]
async fn rename_loaded_session_trims_clears_and_rejects_invalid_titles() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-loaded-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-loaded-ws", "a.txt", b"hello");
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    let lower_bound = crate::store::utc_timestamp().unwrap();

    let renamed = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "\u{2003} Renamed session \u{3000}".to_owned(),
        })
        .await
        .unwrap();
    assert!(renamed.loaded);
    assert_eq!(renamed.title.as_deref(), Some("Renamed session"));
    let after = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(after.title.as_deref(), Some("Renamed session"));
    let upper_bound = crate::store::utc_timestamp().unwrap();
    assert!(after.updated_at >= lower_bound);
    assert!(after.updated_at <= upper_bound);
    assert_eq!(after.profile, before.profile);
    assert_eq!(after.workspace, before.workspace);
    assert_eq!(after.model, before.model);
    assert_eq!(after.reasoning, before.reasoning);
    assert_eq!(after.system_prompt, before.system_prompt);
    assert_eq!(after.tools, before.tools);
    assert_eq!(after.max_tool_rounds, before.max_tool_rounds);
    assert_eq!(after.approval, before.approval);
    assert_eq!(after.created_at, before.created_at);

    let cleared = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: " \t ".to_owned(),
        })
        .await
        .unwrap();
    assert!(cleared.title.is_none());

    let exact_multibyte = "é".repeat(2_048);
    let exact = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: exact_multibyte,
        })
        .await
        .unwrap();
    assert_eq!(exact.title.as_deref().unwrap().len(), 4_096);
    let over_limit = format!("{}a", exact.title.unwrap());
    let result = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: over_limit,
        })
        .await;
    assert!(matches!(result, Err(AgentError::InvalidInput)));

    for title in ["bad\nname", "bad\0name", &"x".repeat(4_097)] {
        let result = agent
            .rename_session(RenameSession {
                session_id: info.session_id,
                title: title.to_string(),
            })
            .await;
        assert!(matches!(result, Err(AgentError::InvalidInput)));
    }
}

#[tokio::test]
async fn rename_unloaded_session_updates_only_the_persistent_metadata() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-unloaded-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-unloaded-ws", "a.txt", b"hello");
    let model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "keep history").await;
    wait_text(&agent, turn).await;
    let store = Store::open(data_dir.clone()).await.unwrap();
    let before = store.load_session(info.session_id).await.unwrap();
    assert!(!before.history.is_empty());
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    agent.close_session(info.session_id).await.unwrap();
    std::fs::remove_dir_all(&workspace).unwrap();
    std::fs::remove_file(&history_path).unwrap();

    let renamed = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "Unloaded title".to_owned(),
        })
        .await
        .unwrap();
    assert!(!renamed.loaded);
    assert_eq!(renamed.title.as_deref(), Some("Unloaded title"));

    let after = store.load_record(info.session_id).await.unwrap();
    assert_eq!(after.title.as_deref(), Some("Unloaded title"));
    assert_eq!(after.system_prompt, before.record.system_prompt);
    assert!(!history_path.exists());
}

#[tokio::test]
async fn rename_serializes_with_busy_completion_and_preserves_a_blocked_session() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-busy-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-busy-ws", "a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::new("main", [Step::gated_text(gate.clone(), "done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "busy").await;
    gate.wait_started().await;

    let renamed = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "While busy".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(renamed.title.as_deref(), Some("While busy"));
    gate.release();
    wait_text(&agent, turn).await;
    let record = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(record.title.as_deref(), Some("While busy"));

    let (blocked_data, _blocked_guard) = fixture_dir(&format!("rename-blocked-{}", next_id()));
    let (blocked_workspace, _blocked_workspace_guard) =
        workspace_file("rename-blocked-ws", "a.txt", b"hello");
    let blocked_model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut blocked_agent = open_agent(
        &blocked_data,
        BTreeMap::from([("main".to_owned(), blocked_model)]),
        read_profile(),
    )
    .await;
    let blocked_info = create_session(&mut blocked_agent, &blocked_workspace).await;
    crate::store::APPEND_FAILURES.arm(blocked_info.session_id, true);
    let blocked_turn = send_text(&mut blocked_agent, blocked_info.session_id, "block").await;
    let result = wait_text(&blocked_agent, blocked_turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(
        blocked_agent
            .session_state(blocked_info.session_id)
            .unwrap()
            .status,
        crate::sessions::SessionStatus::Blocked
    );
    let renamed_blocked = blocked_agent
        .rename_session(RenameSession {
            session_id: blocked_info.session_id,
            title: "Blocked title".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(renamed_blocked.title.as_deref(), Some("Blocked title"));
    assert_eq!(
        blocked_agent
            .session_state(blocked_info.session_id)
            .unwrap()
            .status,
        crate::sessions::SessionStatus::Blocked
    );
}

#[tokio::test]
async fn rename_persistence_failure_leaves_memory_and_disk_unchanged() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-failure-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-failure-ws", "a.txt", b"hello");
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    crate::store::RECORD_WRITE_FAILURES.arm(info.session_id, ());

    let result = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "Should not persist".to_owned(),
        })
        .await;
    assert!(matches!(result, Err(AgentError::StoreUnavailable)));
    let in_memory = agent.list_sessions().await.unwrap()[0].clone();
    assert_eq!(in_memory.title, info.title);
    assert_eq!(in_memory.updated_at, before.updated_at);
    let record = Store::open(data_dir)
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(record.title, info.title);
    assert_eq!(record.updated_at, before.updated_at);
}

#[tokio::test]
async fn system_prompt_file_is_loaded_once_and_session_snapshot_survives_reopen() {
    let (base, _guard) = fixture_dir(&format!("prompt-file-snapshot-{}", next_id()));
    std::fs::create_dir_all(&base).unwrap();
    let (workspace, _workspace_guard) = workspace_file("prompt-file-ws", "a.txt", b"hello");
    let prompt_path = base.join("prompt");
    let config_path = base.join("agent.toml");
    std::fs::write(&prompt_path, "file prompt\r\noriginal").unwrap();
    let text = format!(
        r#"
data_dir = {data_dir}
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = {{ file = {prompt_path} }}
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_PROMPT_FILE_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 30
"#,
        data_dir = toml_path(&base.join("data")),
        prompt_path = toml_path(&prompt_path)
    );
    std::fs::write(&config_path, text).unwrap();
    let config = AgentConfig::load(&config_path).unwrap();
    assert_eq!(
        config.profiles["test"].system_prompt,
        "file prompt\noriginal"
    );
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = Agent::open_with_models(
        config,
        Models::from_values(BTreeMap::from([(
            "main".to_owned(),
            model as Arc<dyn Model>,
        )])),
    )
    .await
    .unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let store = Store::open(base.join("data")).await.unwrap();
    assert_eq!(
        store
            .load_record(info.session_id)
            .await
            .unwrap()
            .system_prompt,
        "file prompt\noriginal"
    );

    std::fs::write(&prompt_path, "changed after Agent startup").unwrap();
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let reopened = store.load_record(info.session_id).await.unwrap();
    assert_eq!(reopened.system_prompt, "file prompt\noriginal");
}

#[test]
fn rename_request_debug_does_not_include_the_title() {
    let request = RenameSession {
        session_id: crate::ids::SessionId::new().unwrap(),
        title: "private title".to_owned(),
    };
    assert!(!format!("{request:?}").contains("private title"));
}

#[tokio::test]
async fn single_active_loop_busy() {
    let (data_dir, _guard) = fixture_dir(&format!("busy-{}", next_id()));
    let (workspace, _guard) = workspace_file("busy-ws", "a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::new("main", [Step::gated_text(gate.clone(), "first")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let first = send_text(&mut agent, info.session_id, "first").await;
    let busy = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "second".to_owned(),
        })
        .await;
    assert!(matches!(busy, Err(AgentError::SessionBusy)));

    gate.wait_started().await;
    gate.release();
    let result = wait_text(&agent, first).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    agent.wait_turn(first).await.unwrap(); // already completed: no error
}

#[tokio::test]
async fn basic_completion_persists_and_merges_history() {
    let (data_dir, _guard) = fixture_dir(&format!("basic-{}", next_id()));
    let (workspace, _guard) = workspace_file("basic-ws", "a.txt", b"hello");
    let model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "hello agent").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    // The history in memory and the durable JSONL agree.
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 2);
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 2);
    let texts = flatten_texts(&stored);
    assert!(texts.contains(&"hello agent".to_owned()));
    assert!(texts.contains(&"done".to_owned()));

    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
}
