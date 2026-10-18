use super::*;

/// I1 stub: create, open, and both a no-tool and a tool-bearing Turn must not
/// invoke Git at all. The status stub records every invocation, so a single
/// implicit query would leave a marker behind.
#[cfg(unix)]
#[tokio::test]
async fn i1_status_stub_proves_no_implicit_git_during_session_and_turns() {
    use crate::workspace::status::STATUS_PROGRAMS;
    use std::os::unix::fs::PermissionsExt;

    let fixture = AgentFixture::new("i1-stub").file("file.txt", b"contents\n");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    std::fs::create_dir_all(&data_dir).unwrap();
    let marker = data_dir.join("git-invocations");
    let script = data_dir.join("count-git");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf 'called\\n' >> '{}'\nexit 1\n",
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

    let model = ScriptedModel::new(
        "main",
        [
            Step::Text("no tool"),
            Step::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            Step::Text("tool complete"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    assert!(
        !marker.exists(),
        "session.create started an implicit Git query"
    );
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert!(
        !marker.exists(),
        "session.open started an implicit Git query"
    );

    let no_tool = send_text(&mut agent, info.session_id, "no tool").await;
    wait_text(&agent, no_tool).await;
    assert!(
        !marker.exists(),
        "a no-tool Turn started an implicit Git query"
    );

    let with_tool = send_text(&mut agent, info.session_id, "read file").await;
    wait_text(&agent, with_tool).await;
    assert!(
        !marker.exists(),
        "a tool-bearing Turn started an implicit Git query"
    );

    // Positive control: an explicit status query must reach the stub, proving
    // the marker mechanism really observes Git invocations rather than the
    // stub simply never running. The failing stub is fine; only the marker
    // matters here.
    let _ = agent
        .workspace_status(crate::WorkspaceStatusRequest {
            session_id: info.session_id,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert!(
        marker.exists(),
        "the explicit status query did not reach the Git stub"
    );
    agent.close_session(info.session_id).await.unwrap();
}

/// I2 release: a real Bash command's owners must be joinable and reclaimed
/// after the Turn, and a Weak handle to the observation resources must fail
/// after close/shutdown. The reclamation evidence is captured before the
/// unconditional `close_session`, so a failed assertion cannot be turned into a
/// pass by the close itself; no other process cleanup is introduced.
#[cfg(unix)]
#[tokio::test]
async fn i2_real_command_releases_owners_and_observation_resources() {
    use crate::tool_data::CommandStatus;
    use futures_util::FutureExt;
    use std::panic::{AssertUnwindSafe, resume_unwind};
    use std::sync::Weak;

    let fixture = AgentFixture::new("i2-release").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    let pid_file = workspace.join("child.pid");
    let model = ScriptedModel::new(
        "main",
        [Step::ToolCall(
            "bash",
            json!({"command": "printf 'started\\n'; echo $$ > child.pid; exec sleep 30"}),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .profile(bash_profile())
        .open()
        .await;
    let info = create_session(&mut agent, &workspace).await;
    let session = agent.loaded_session(info.session_id).unwrap();
    let weak_data: Weak<ToolData> = Arc::downgrade(&session.tool_data());
    let weak_owners: Weak<ToolOwners> = Arc::downgrade(&session.tool_owners());
    let weak_observer: Weak<ToolObserver> = Arc::downgrade(&session.parts().observer);
    drop(session);

    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    let mut child_pid = None;
    for _ in 0..500 {
        // Shell redirection can create the file before the PID is written.
        child_pid = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|text| text.trim().parse::<i32>().ok())
            .filter(|pid| *pid > 0);
        if child_pid.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let Some(child_pid) = child_pid.or_else(|| {
        std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|text| text.trim().parse::<i32>().ok())
            .filter(|pid| *pid > 0)
    }) else {
        // A command may still be running without ever publishing a pid. Close
        // the Session first so its owners join and revert the command, then
        // fail: no other process cleanup is attempted.
        let _ = agent.close_session(info.session_id).await;
        panic!("the command never published a pid");
    };

    // Cancel, wait, and capture the owner's reclamation evidence. A panic in
    // this block must not skip the close/join below, so it runs inside
    // `catch_unwind`.
    let body = AssertUnwindSafe(async {
        assert!(agent.cancel(turn).unwrap());
        let result = wait_text(&agent, turn).await;
        let cancelled = matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
        );
        let tool_ref = agent
            .loaded_session(info.session_id)
            .unwrap()
            .tool_data()
            .loop_tool_refs(info.session_id, turn.loop_id)
            .into_iter()
            .next()
            .expect("the Bash call recorded a ToolRef");
        let terminal = read_tool(&agent, tool_ref).await;
        let command = terminal.execution.command.expect("a terminal command");
        let reaped = !process_is_listed(child_pid);
        let active = agent
            .loaded_session(info.session_id)
            .unwrap()
            .tool_owners()
            .active();
        (
            cancelled,
            command.status,
            command.termination_confirmed,
            reaped,
            active,
        )
    })
    .catch_unwind()
    .await;

    // Unconditional close joins the Session's owners on every path.
    let close = agent.close_session(info.session_id).await;

    // Assert the evidence saved before the close ran.
    let (cancelled, status, termination_confirmed, reaped, active) = match body {
        Ok(evidence) => evidence,
        Err(payload) => resume_unwind(payload),
    };
    assert!(
        cancelled,
        "the cancelled Turn must report user cancellation"
    );
    assert_eq!(status, CommandStatus::Cancelled);
    assert!(termination_confirmed);
    assert!(reaped, "the cancelled command was not reaped by its owner");
    assert_eq!(active, 0);
    close.unwrap();

    drop(agent);
    assert!(weak_data.upgrade().is_none(), "ToolData was not released");
    assert!(
        weak_owners.upgrade().is_none(),
        "ToolOwners was not released"
    );
    assert!(
        weak_observer.upgrade().is_none(),
        "ToolObserver was not released"
    );
}

/// E2E-A: normal multi-turn Session with a hot model switch. The first
/// request's invocation is observed during that request, before the Turn
/// completes and while the gated Bash command is still running; Bash output and
/// a native write change are read back; a same-loop update changes the model
/// for the next request and the future options for the next Turn; close/open
/// keeps configuration and history consistent.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_a_multi_turn_hot_switch_tool_data_workflow() {
    use crate::changes::{ChangeScope, ChangesListRequest};
    use crate::diff::ChangesDiffRequest;
    use crate::tool_data::{CommandStatus, ToolDataStream, ToolExecutionState};
    use futures_util::FutureExt;
    use std::panic::{AssertUnwindSafe, resume_unwind};

    let fixture = AgentFixture::new("e2e-a").file("value.txt", b"user\n");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    let first_pid = workspace.join("first.pid");
    let release = workspace.join("release.txt");
    let model_a = ScriptedModel::new(
        "main",
        [Step::ToolCall(
            "bash",
            json!({"command": "printf 'out\\n'; printf 'err\\n' >&2; echo $$ > first.pid; while [ ! -f release.txt ]; do sleep 0.05; done"}),
        )],
    ).with_pacing(std::time::Duration::from_millis(2));
    let model_b = ScriptedModel::new(
        "other",
        [
            Step::ToolCall("write", json!({"path": "value.txt", "content": "agent\n"})),
            Step::Text("other finished"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let profile = Profile {
        tools: vec!["bash".to_owned(), "write".to_owned()],
        ..read_profile()
    };
    let fixture = fixture
        .models(BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]))
        .profile(profile)
        .model_configs(BTreeMap::from([
            (
                "main".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 31),
            ),
            (
                "other".to_owned(),
                model_config_with_timeout("MINICORE_AGENT_TEST_KEY", 47),
            ),
        ]))
        .auto();
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    assert_eq!(
        session_options(&agent, info.session_id).prompt_timeout,
        std::time::Duration::from_secs(31)
    );

    // Every failure path after the Bash command is dispatched must still close
    // the Session so its command and query owners are joined, then rethrow.
    let workflow = AssertUnwindSafe(async {
        // Turn 1: observe the first request's invocation while the gated Bash
        // command is still running.
        let turn = send_text(&mut agent, info.session_id, "run bash").await;
        let invocation = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let event = events.recv().await.expect("event stream must remain open");
                if let AgentEvent::ToolInvocation {
                    turn: event_turn,
                    data,
                    ..
                } = event
                {
                    if event_turn == turn {
                        return data;
                    }
                }
            }
        })
        .await
        .expect("the first request must publish its invocation before it finishes");
        assert_eq!(invocation.name, "bash");
        assert_eq!(invocation.tool_ref.request_index, 0);
        for _ in 0..500 {
            if first_pid.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(first_pid.exists(), "the Bash command really started");
        let live = read_tool(&agent, invocation.tool_ref.clone()).await;
        assert_eq!(live.execution.state, ToolExecutionState::Running);
        assert_eq!(
            live.execution
                .command
                .as_ref()
                .map(|command| command.status),
            Some(CommandStatus::Running)
        );
        // The pid file only proves the command started; the stdout/stderr
        // drainers may not have committed yet, so wait for the ToolRef's two
        // streams to actually reach the expected bytes before asserting.
        let stdout = wait_tool_stream_bytes(
            &agent,
            invocation.tool_ref.clone(),
            ToolDataStream::Stdout,
            b"out\n",
        )
        .await;
        let stderr = wait_tool_stream_bytes(
            &agent,
            invocation.tool_ref.clone(),
            ToolDataStream::Stderr,
            b"err\n",
        )
        .await;
        assert_eq!(stdout, b"out\n");
        assert_eq!(stderr, b"err\n");

        // Same-loop model update while the first request's command is active.
        let updated = agent
            .update_session(UpdateSession {
                session_id: info.session_id,
                model: Some("other".to_owned()),
                reasoning: None,
            })
            .await
            .unwrap();
        let active_revision = updated
            .active_revision
            .expect("the loop accepts the update");
        assert_eq!(
            crate::sessions::INSTALLED_LOOP_OPTIONS
                .get(&turn.loop_id)
                .expect("the running loop recorded install options")
                .prompt_timeout,
            std::time::Duration::from_secs(31),
            "a running loop must not be hot-modified"
        );
        assert_eq!(
            session_options(&agent, info.session_id).prompt_timeout,
            std::time::Duration::from_secs(47),
            "future options must track the candidate model"
        );
        // Let the gated Bash command finish so the Turn completes normally.
        std::fs::write(&release, b"go").unwrap();

        let first_result = wait_text(&agent, turn).await;
        // request 0: model_a Bash; request 1: model_b write; request 2: model_b
        // final text after the same-loop switch.
        assert_eq!(first_result.report.requests, 3);
        assert_eq!(first_result.report.tool_rounds, 2);
        assert_eq!(first_result.report.final_config_revision, active_revision);
        assert_persisted(&first_result);
        assert!(
            !process_is_listed(
                std::fs::read_to_string(&first_pid)
                    .expect("the command published its pid")
                    .trim()
                    .parse()
                    .expect("a pid")
            ),
            "the completed Turn must have joined its reported command"
        );

        // turn.result is readable and changes.diff covers the native write in
        // the same loop.
        let page = agent
            .turn_result(crate::TurnResultRequest {
                turn,
                cursor: None,
                limit: 100,
                max_bytes: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            page.availability,
            crate::read::TurnResultAvailability::Live | crate::read::TurnResultAvailability::Stored
        ));
        let changes = agent
            .changes_list(ChangesListRequest {
                session_id: info.session_id,
                scope: ChangeScope::Turn {
                    loop_id: turn.loop_id,
                },
                cursor: None,
                limit: 10,
                max_bytes: None,
            })
            .await
            .unwrap();
        assert_eq!(changes.records.len(), 1, "one native write in the turn");
        let diff = agent
            .changes_diff(ChangesDiffRequest {
                session_id: info.session_id,
                change_ref: changes.records[0].change_ref.clone(),
                comparison: None,
                context_lines: None,
                cursor: None,
                max_bytes: None,
            })
            .await
            .unwrap();
        assert!(!diff.hunks.is_empty());

        // Turn 2: the next Turn uses the candidate options and the new model.
        let second = send_text(&mut agent, info.session_id, "next turn").await;
        let next_options = crate::sessions::INSTALLED_LOOP_OPTIONS
            .get(&second.loop_id)
            .expect("the next loop recorded install options");
        assert_eq!(
            next_options.prompt_timeout,
            std::time::Duration::from_secs(47)
        );
        wait_text(&agent, second).await;
        // model_b already served both its scripted turn-1 requests; the third
        // call is the next Turn and confirms the switched model stayed selected.
        assert_eq!(model_b.requests().lock().unwrap().len(), 3);
        assert_eq!(model_a.requests().lock().unwrap().len(), 1);

        // Close/open keeps the record and history consistent with the update.
        let history_before = read_store_history(&data_dir, info.session_id).await;
        agent.close_session(info.session_id).await.unwrap();
        let reopened = agent.open_session(info.session_id).await.unwrap();
        assert_eq!(reopened.model, "other");
        assert_eq!(
            session_options(&agent, info.session_id).prompt_timeout,
            std::time::Duration::from_secs(47)
        );
        assert_eq!(
            read_store_history(&data_dir, info.session_id).await.len(),
            history_before.len()
        );
        agent.close_session(info.session_id).await.unwrap();
    })
    .catch_unwind()
    .await;
    if workflow.is_err() {
        // A panic may have skipped the workflow's own close; join the Session's
        // owners here so no gated command is left running. A Session already
        // closed above simply reports NotLoaded, which is fine.
        let _ = agent.close_session(info.session_id).await;
    }
    if let Err(payload) = workflow {
        resume_unwind(payload);
    }
}

/// E2E-B: a dropped event stream, a failed JSONL append, and a rejected send.
/// The authoritative `turn.wait`, `turn.result`, and `tool.read/output` remain
/// readable; a rejected new send keeps the previous result; close still shuts
/// the Session down cleanly. This workflow does not start a status query, so it
/// makes no claim about reclaiming never-started query workers.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_b_lost_events_failed_append_and_clean_close_workflow() {
    use crate::tool_data::{CommandStatus, ToolDataStream, ToolExecutionState, ToolRecordingState};
    use base64::Engine;
    use futures_util::FutureExt;
    use std::panic::{AssertUnwindSafe, resume_unwind};

    let fixture = AgentFixture::new("e2e-b").file("value.txt", b"user\n");
    // A short command that finishes on its own: the Turn's owner join still has
    // to reap it, but the test never depends on a fixed long sleep.
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall(
                "bash",
                json!({"command": "printf 'kept\\n'; printf 'stderr-line\\n' >&2"}),
            ),
            Step::ToolCall("write", json!({"path": "value.txt", "content": "agent\n"})),
            Step::Text("finished"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let profile = Profile {
        tools: vec!["bash".to_owned(), "write".to_owned()],
        ..read_profile()
    };
    // Capacity-1 event queue drops nearly every best-effort event; the query
    // path must not depend on it.
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .profile(profile)
        .options(AgentOptions {
            agent_event_capacity: 1,
            loop_event_capacity: Some(1),
        });
    let (mut agent, info) = fixture.start().await;
    crate::store::APPEND_FAILURES.arm(info.session_id, true);

    let workflow = AssertUnwindSafe(async {
        let turn = send_text(&mut agent, info.session_id, "run tools").await;
        let result = wait_text(&agent, turn).await;
        assert_completed(&result);
        assert_eq!(
            result.persistence,
            crate::sessions::TurnPersistence::Failed,
            "the injected append failure must surface as failed persistence"
        );

        // Session is Blocked; a new send is rejected without discarding the
        // result.
        let rejected = agent
            .send(crate::agent::SendMessage {
                session_id: info.session_id,
                text: "after".to_owned(),
            })
            .await;
        assert!(matches!(rejected, Err(AgentError::SessionBlocked)));
        let again = agent.wait_turn(turn).await.unwrap();
        assert_eq!(again.persistence, crate::sessions::TurnPersistence::Failed);

        // turn.result remains readable from the live failed report.
        let page = agent
            .turn_result(crate::TurnResultRequest {
                turn,
                cursor: None,
                limit: 100,
                max_bytes: None,
            })
            .await
            .unwrap();
        assert_eq!(page.availability, crate::read::TurnResultAvailability::Live);
        assert_eq!(
            page.persistence,
            Some(crate::sessions::TurnPersistence::Failed)
        );

        // tool.read and tool.output still serve the retained Bash and write
        // facts. A finished command no longer has a live pid; the exit status
        // and confirmed termination prove it was reaped by its owner.
        let refs = agent
            .loaded_session(info.session_id)
            .unwrap()
            .tool_data()
            .loop_tool_refs(info.session_id, turn.loop_id);
        assert_eq!(
            refs.len(),
            2,
            "the Bash and write calls both recorded facts"
        );
        let bash_ref = refs
            .iter()
            .find(|reference| {
                agent
                    .loaded_session(info.session_id)
                    .unwrap()
                    .tool_data()
                    .snapshot(reference)
                    .is_some_and(|data| data.name == "bash")
            })
            .expect("the Bash ToolRef is present")
            .clone();
        let bash = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let record = read_tool(&agent, bash_ref.clone()).await;
                if record.execution.recording != ToolRecordingState::MemoryOnly {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("auxiliary persistence did not finish");
        assert_eq!(bash.execution.state, ToolExecutionState::Succeeded);
        assert_eq!(bash.execution.recording, ToolRecordingState::Saved);
        let command = bash.execution.command.as_ref().expect("a command record");
        assert_eq!(command.status, CommandStatus::Exited);
        assert!(command.termination_confirmed);
        let output = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: bash_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&output.data)
                .unwrap(),
            b"kept\n"
        );
        let stderr = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: bash_ref,
                stream: ToolDataStream::Stderr,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&stderr.data)
                .unwrap(),
            b"stderr-line\n"
        );

        // Closing the blocked Session still shuts it down (no query was
        // started, so this asserts no more than the Session barrier).
        agent.close_session(info.session_id).await.unwrap();
        assert!(matches!(
            agent.session_state(info.session_id),
            Err(AgentError::SessionNotLoaded)
        ));
    })
    .catch_unwind()
    .await;
    if workflow.is_err() {
        let _ = agent.close_session(info.session_id).await;
    }
    if let Err(payload) = workflow {
        resume_unwind(payload);
    }
    agent.shutdown().await.unwrap();
}
