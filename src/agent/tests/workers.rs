use super::*;

/// `workspace.status` is owned by the loaded Session: the public Agent path
/// registers an owned worker for it, dropping the awaiter leaves the worker in
/// charge of the git child, and closing the Session joins that worker only
/// after its child was stopped and reaped.
#[cfg(unix)]
#[tokio::test]
async fn session_close_detaches_a_stalled_status_worker_after_grace() {
    use crate::workspace::status::{STATUS_PROGRAMS, STATUS_REAP_GATES};
    use std::os::unix::fs::PermissionsExt;
    let (data_dir, _data_guard) = fixture_dir(&format!("status-stalled-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("status-stalled-ws", "note", b"text");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let session_id = create_session(&mut agent, &workspace).await.session_id;
    let pid_path = data_dir.join("pid");
    let script = data_dir.join("git");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho $$ > '{}'\nexec sleep 30\n",
            pid_path.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let root = std::fs::canonicalize(&workspace).unwrap();
    STATUS_PROGRAMS.arm(root.clone(), script.into_os_string());
    let gate = Arc::new(crate::test_support::Gate::new());
    STATUS_REAP_GATES.arm(root, Arc::clone(&gate));
    let session = agent.loaded_session(session_id).unwrap();
    let query = session
        .spawn_status_query(
            session.workspace(),
            crate::WorkspaceStatusRequest {
                session_id,
                max_bytes: None,
            },
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();
    let pid = wait_for_started_child(&pid_path, 0).await;
    let close = agent.close_session(session_id);
    tokio::pin!(close);
    assert!(futures_util::poll!(&mut close).is_pending());
    gate.wait_started().await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut close).await;
    let query_result = tokio::time::timeout(std::time::Duration::from_secs(2), query.wait()).await;
    gate.release();
    for _ in 0..200 {
        if !process_is_listed(pid) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(matches!(
        query_result.expect("status waiter exceeded grace"),
        Err(AgentError::QueryLimit)
    ));
    result.expect("status join exceeded grace").unwrap();
    assert!(!process_is_listed(pid));
}

#[cfg(unix)]
#[tokio::test]
async fn an_owned_status_worker_outlives_its_awaiter_until_the_session_closes() {
    use crate::workspace::status::{STATUS_PROGRAMS, STATUS_REAP_GATES};
    use std::os::unix::fs::PermissionsExt;

    let (data_dir, _data_guard) = fixture_dir(&format!("status-owner-{}", next_id()));
    let (workspace, _workspace_guard) =
        workspace_file("status-owner-ws", "note.txt", b"raw\ncontent\n");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session_id = info.session_id;

    // The public Agent path runs through the Session-owned worker.
    let plain = agent
        .workspace_status(crate::WorkspaceStatusRequest {
            session_id,
            max_bytes: None,
        })
        .await
        .expect("the public status path answers");
    assert!(!plain.repo_available);
    assert!(!plain.complete);

    // A child that reports its own process id and then holds its pipes open.
    let pid_path = data_dir.join("status-child-pids");
    let script = data_dir.join("slow-status-git");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> '{}'\nsleep 30\n",
            pid_path.display()
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

    let session = agent.loaded_session(session_id).unwrap();
    let spawn = |session: &crate::sessions::Session| {
        session
            .spawn_status_query(
                session.workspace(),
                crate::WorkspaceStatusRequest {
                    session_id,
                    max_bytes: None,
                },
                tokio_util::sync::CancellationToken::new(),
            )
            .unwrap()
    };
    let first = spawn(&session);
    let first_pid = wait_for_started_child(&pid_path, 0).await;
    assert!(
        process_is_listed(first_pid),
        "the owned child is not running"
    );
    assert_eq!(session.active_status_workers(), 1);

    // The awaiter goes away first: the Session's worker still owns the child
    // and reaps it, so nothing is left behind by a dropped waiter.
    drop(first);
    for _ in 0..200 {
        if !process_is_listed(first_pid) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        !process_is_listed(first_pid),
        "a dropped status awaiter left its owned child unreaped"
    );

    // Closing the Session is the barrier for a query whose awaiter is still
    // alive: while the owner still waits for its child, the close cannot return,
    // and it returns only after that child was actually reaped. The reap gate
    // parks the owner so both facts are observed without sleeping.
    let gate = Arc::new(crate::test_support::Gate::new());
    STATUS_REAP_GATES.arm(
        std::fs::canonicalize(&workspace).unwrap(),
        Arc::clone(&gate),
    );
    let second = spawn(&session);
    let second_pid = wait_for_started_child(&pid_path, 1).await;
    assert!(
        process_is_listed(second_pid),
        "the second owned child is not running"
    );

    let close = agent.close_session(session_id);
    let mut close = std::pin::pin!(close);
    // The first poll cancels the Session's queries, including this worker.
    assert!(futures_util::poll!(&mut close).is_pending());
    gate.wait_started().await;
    assert!(
        process_is_listed(second_pid),
        "the child was reaped before its owner was released"
    );
    gate.release();
    close.await.unwrap();
    assert!(
        !process_is_listed(second_pid),
        "the closing Session returned before its owned child was reaped"
    );
    assert!(matches!(second.wait().await, Err(AgentError::QueryLimit)));
}

/// P2a: `prepare_workspace_status` registers the Session-owned worker
/// synchronously and returns a wait-only future. If that future is never
/// polled and the Session closes immediately, the close must still cancel and
/// join the worker it registered, and the unpolled future must then observe
/// the cancellation instead of a fake success.
#[cfg(unix)]
#[tokio::test]
async fn prepared_status_future_never_polled_is_still_reclaimed_by_close() {
    use crate::workspace::status::{STATUS_PROGRAMS, STATUS_REAP_GATES};
    use std::os::unix::fs::PermissionsExt;

    let (data_dir, _data_guard) = fixture_dir(&format!("status-prepare-{}", next_id()));
    let (workspace, _workspace_guard) =
        workspace_file("status-prepare-ws", "note.txt", b"raw\ncontent\n");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session_id = info.session_id;

    // A child that reports its own process id and then holds its pipes open.
    let pid_path = data_dir.join("status-prepare-pids");
    let script = data_dir.join("slow-status-git");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> '{}'\nsleep 30\n",
            pid_path.display()
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

    let session = agent.loaded_session(session_id).unwrap();
    // Registration is synchronous: the worker exists before the future is ever
    // polled, and no poll is needed to make close observe it.
    let prepared = crate::queries::prepare_workspace_status(
        session.clone(),
        crate::WorkspaceStatusRequest {
            session_id,
            max_bytes: None,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    let pid = wait_for_started_child(&pid_path, 0).await;
    assert!(
        process_is_listed(pid),
        "the registered child is not running"
    );
    assert_eq!(session.active_status_workers(), 1);

    // The gate parks the worker at its child reap point, so the close is proven
    // to wait for that worker rather than skipping it.
    let gate = Arc::new(crate::test_support::Gate::new());
    STATUS_REAP_GATES.arm(
        std::fs::canonicalize(&workspace).unwrap(),
        Arc::clone(&gate),
    );

    let close = agent.close_session(session_id);
    let mut close = std::pin::pin!(close);
    assert!(futures_util::poll!(&mut close).is_pending());
    gate.wait_started().await;
    assert!(
        process_is_listed(pid),
        "the child was reaped before its owner was released"
    );
    gate.release();
    close.await.unwrap();
    assert!(
        !process_is_listed(pid),
        "the closing Session returned before its owned child was reaped"
    );
    assert_eq!(session.active_status_workers(), 0);
    // The unpolled future never saw a success; it observes the cancellation.
    assert!(matches!(prepared.await, Err(AgentError::SessionNotLoaded)));
}

/// P2b: `prepare_changes_diff` registers the Session-owned source worker
/// synchronously for a `workspace:` ref and returns a wait-only future. If that
/// future is never polled and the Session closes immediately, the close must
/// still cancel and join the worker it registered.
#[cfg(unix)]
#[tokio::test]
async fn prepared_workspace_diff_future_never_polled_is_still_reclaimed_by_close() {
    use crate::workspace::status::{STATUS_PROGRAMS, STATUS_REAP_GATES};
    use std::os::unix::fs::PermissionsExt;

    let (data_dir, _data_guard) = fixture_dir(&format!("diff-prepare-{}", next_id()));
    let (workspace, _workspace_guard) =
        workspace_file("diff-prepare-ws", "note.txt", b"raw\ncontent\n");
    // A real repository is needed: `diff_sources` first resolves the work tree
    // through Git before it can report a missing side.
    let init = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .arg(&workspace)
        .status()
        .expect("git must be available for the diff ownership regression");
    assert!(init.success());

    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session_id = info.session_id;

    // A real `workspace:` ref for the untracked file, taken from the live
    // status projection.
    let status = agent
        .workspace_status(crate::WorkspaceStatusRequest {
            session_id,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert!(status.repo_available);
    let (records, _, _) = crate::changes::workspace_records(session_id, &status);
    let change_ref = records
        .iter()
        .find(|record| record.path == "note.txt")
        .expect("the untracked file is listed")
        .change_ref
        .clone();

    // A git stub that records its pid and then holds its pipes open.
    let pid_path = data_dir.join("diff-prepare-pids");
    let script = data_dir.join("slow-diff-git");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> '{}'\nsleep 30\n",
            pid_path.display()
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

    let session = agent.loaded_session(session_id).unwrap();
    let prepared = crate::queries::prepare_changes_diff(
        agent.store_handle(),
        Some(session.clone()),
        crate::ChangesDiffRequest {
            session_id,
            change_ref,
            comparison: None,
            context_lines: None,
            cursor: None,
            max_bytes: None,
        },
        tokio_util::sync::CancellationToken::new(),
        crate::diff::diff_deadline(),
    )
    .unwrap();
    let pid = wait_for_started_child(&pid_path, 0).await;
    assert!(
        process_is_listed(pid),
        "the registered child is not running"
    );
    assert_eq!(session.active_status_workers(), 1);

    let gate = Arc::new(crate::test_support::Gate::new());
    STATUS_REAP_GATES.arm(
        std::fs::canonicalize(&workspace).unwrap(),
        Arc::clone(&gate),
    );

    let close = agent.close_session(session_id);
    let mut close = std::pin::pin!(close);
    assert!(futures_util::poll!(&mut close).is_pending());
    gate.wait_started().await;
    gate.release();
    close.await.unwrap();
    assert_eq!(session.active_status_workers(), 0);
    drop(prepared);
}

/// Waits until a fake git child has appended its process id, and returns it.
#[cfg(unix)]
async fn wait_for_started_child(pid_path: &Path, index: usize) -> i32 {
    for _ in 0..200 {
        if let Ok(text) = std::fs::read_to_string(pid_path) {
            if let Some(pid) = text.lines().nth(index) {
                return pid.trim().parse().unwrap();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the status query never started child {index}");
}

/// `ps` lists a process that exists, including one that is still a zombie, so
/// an empty listing is evidence that it was reaped rather than merely signalled.
#[cfg(unix)]
pub(super) fn process_is_listed(pid: i32) -> bool {
    let output = std::process::Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("pid=")
        .output()
        .expect("ps runs");
    !String::from_utf8_lossy(&output.stdout).trim().is_empty()
}

/// End-to-end Bash ownership through a real Agent Session: a running command is
/// cancelled with its loop, the stored process record matches the live
/// `tool_process` event and the `tool.read`/`tool.output` queries, and
/// `close_session` returns only after the owned command was reaped.
#[cfg(unix)]
#[tokio::test]
async fn i2_04_bash_owned_command_matches_events_queries_and_the_close_join() {
    use crate::tool_data::{CommandStatus, ToolDataStream};

    let (data_dir, _guard) = fixture_dir(&format!("bash-owned-turn-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-owned-turn-ws", "a.txt", b"hello");
    let pid_file = workspace.join("child.pid");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall(
                "bash",
                json!({"command": "echo $$ > child.pid; exec sleep 30"}),
            ),
            ModelScript::Text("cancelled"),
        ],
    );
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "run slowly").await;
    // Wait for the public ToolStarted boundary, then for the pid file, so the
    // cancel happens while a process is really owned.
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("bash events must arrive")
            .expect("event stream must remain open");
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn) {
            break;
        }
    }
    for _ in 0..500 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(pid_file.exists(), "the command really started");

    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    // Drain the live process events for this turn and keep the running and
    // terminal records, which must come from the same stored source as queries.
    let mut running = None;
    let mut terminal = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(1), events.recv()).await
    {
        if let AgentEvent::ToolProcess { data, .. } = event {
            if let Some(command) = data.command {
                match command.status {
                    CommandStatus::Running => running = Some((data.tool_ref, command)),
                    CommandStatus::Cancelled => {
                        terminal = Some((data.tool_ref, command));
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    let (owner, running) = running.expect("a running record was published before the first byte");
    let (terminal_ref, terminal) = terminal.expect("a terminal cancelled record was published");
    assert_eq!(terminal_ref, owner);
    assert!(!running.output_complete);

    let queried = read_tool(&agent, owner.clone()).await;
    let command = queried.execution.command.expect("a queried command record");
    assert_eq!(command.status, CommandStatus::Cancelled);
    assert!(command.termination_confirmed);
    assert!(command.exit_code.is_none());
    assert_eq!(command.stdout_base_offset, terminal.stdout_base_offset);
    assert_eq!(command.stdout_observed_end, terminal.stdout_observed_end);

    // `tool.output` and the process record describe the same retained range.
    let page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: owner.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(page.encoding, "base64");
    assert!(page.eof);
    assert_eq!(page.base_offset, command.stdout_base_offset);
    assert_eq!(page.observed_end, command.stdout_observed_end);

    // The cancelled loop already stopped and reaped its owned command.
    let child_pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("the command published its pid")
        .trim()
        .parse()
        .expect("a pid");
    assert!(
        !process_is_listed(child_pid),
        "the cancelled command was not reaped by its owner"
    );

    // Closing the Session is the join barrier and must return cleanly.
    agent.close_session(info.session_id).await.unwrap();
}
