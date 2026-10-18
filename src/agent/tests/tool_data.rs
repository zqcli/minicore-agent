use super::*;

/// Authoritative ToolRefs derived from persisted history, so tests never guess
/// a tool-call id or request index.
pub(super) fn history_tool_refs(
    agent: &Agent,
    session_id: SessionId,
) -> Vec<crate::tool_data::ToolRef> {
    let page = agent
        .history(GetHistory {
            session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    page.items
        .iter()
        .filter_map(|item| match &item.item {
            crate::history::HistoryItemView::ToolResult(result) => {
                Some(crate::tool_data::ToolRef {
                    session_id,
                    loop_id: result.loop_id,
                    request_index: result.request_index,
                    tool_call_id: result.tool_call_id.clone(),
                })
            }
            _ => None,
        })
        .collect()
}

pub(super) async fn read_tool(
    agent: &Agent,
    tool_ref: crate::tool_data::ToolRef,
) -> crate::tool_data::ToolReadResult {
    agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref,
            max_bytes: None,
        })
        .await
        .unwrap()
}

/// Polls one process stream of `tool_ref` until it holds exactly `expected`.
/// A started process only means its two drainer tasks have begun; the bytes and
/// their ToolData commit may lag, so the caller waits on the query path instead
/// of assuming immediate visibility.
#[cfg(unix)]
pub(super) async fn wait_tool_stream_bytes(
    agent: &Agent,
    tool_ref: crate::tool_data::ToolRef,
    stream: crate::tool_data::ToolDataStream,
    expected: &[u8],
) -> Vec<u8> {
    use base64::Engine;
    let mut last = Vec::new();
    for _ in 0..500 {
        let page = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        last = base64::engine::general_purpose::STANDARD
            .decode(&page.data)
            .unwrap();
        if last == expected {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    last
}

async fn read_output(agent: &Agent, tool_ref: crate::tool_data::ToolRef) -> String {
    let mut collected = String::new();
    let mut offset = 0_u64;
    loop {
        let page = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: crate::tool_data::ToolDataStream::Output,
                offset,
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        collected.push_str(&page.data);
        offset = page.next_offset;
        if page.eof {
            break;
        }
        assert!(page.next_offset > 0, "a non-eof page must advance");
    }
    collected
}

#[tokio::test]
async fn tool_invocation_separates_requests_not_just_names() {
    use crate::tool_data::{ToolExecutionState, ToolSubject};

    let fixture = AgentFixture::new("tool-data-identity").file("a.txt", b"alpha");
    let workspace = fixture.workspace.clone();
    std::fs::write(workspace.join("b.txt"), b"beta").unwrap();
    // The same tool name runs in two different requests within one loop.
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            Step::ToolCall("read", json!({"path": "b.txt", "limit": 8})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "read both").await;
    wait_text(&agent, turn).await;

    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 2, "both reads must be recorded: {refs:?}");
    assert_ne!(refs[0].request_index, refs[1].request_index);
    let first = read_tool(&agent, refs[0].clone()).await;
    let second = read_tool(&agent, refs[1].clone()).await;
    assert_eq!(first.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(second.execution.state, ToolExecutionState::Succeeded);
    let first_subject = first.invocation.expect("first invocation").subject;
    let second_subject = second.invocation.expect("second invocation").subject;
    assert_ne!(
        first_subject, second_subject,
        "each request keeps its own subject"
    );
    assert!(matches!(first_subject, ToolSubject::File { .. }));
    assert!(matches!(second_subject, ToolSubject::File { .. }));

    // A wrong request index is a distinct unknown identity, never a fallback
    // to the most recent read of the same name.
    let guessed = crate::tool_data::ToolRef {
        request_index: refs[0].request_index + 100,
        ..refs[0].clone()
    };
    assert!(matches!(
        agent
            .tool_read(crate::tool_data::ToolReadRequest {
                display: false,
                tool_ref: guessed,
                max_bytes: None,
            })
            .await,
        Err(AgentError::ToolNotFound)
    ));
}

#[tokio::test]
async fn tool_invocation_is_isolated_across_sessions() {
    let (data_dir, _guard) = fixture_dir("tool-data-sessions");
    let (workspace_a, _guard_a) = workspace_file("tool-data-session-a", "a.txt", b"alpha");
    let base = workspace_a.parent().unwrap();
    let workspace_b = base.join("session-b");
    std::fs::create_dir_all(&workspace_b).unwrap();
    std::fs::write(workspace_b.join("b.txt"), b"beta").unwrap();
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            Step::Text("a done"),
            Step::ToolCall("read", json!({"path": "b.txt", "limit": 8})),
            Step::Text("b done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .open()
        .await;
    let session_a = create_session(&mut agent, &workspace_a).await;
    let turn_a = send_text(&mut agent, session_a.session_id, "read a").await;
    wait_text(&agent, turn_a).await;
    let session_b = create_session(&mut agent, &workspace_b).await;
    let turn_b = send_text(&mut agent, session_b.session_id, "read b").await;
    wait_text(&agent, turn_b).await;

    let ref_a = history_tool_refs(&agent, session_a.session_id);
    let ref_b = history_tool_refs(&agent, session_b.session_id);
    assert_eq!(ref_a.len(), 1);
    assert_eq!(ref_b.len(), 1);
    assert_eq!(ref_a[0].session_id, session_a.session_id);
    assert_eq!(ref_b[0].session_id, session_b.session_id);
    let invocation_a = read_tool(&agent, ref_a[0].clone())
        .await
        .invocation
        .expect("session-a invocation");
    let invocation_b = read_tool(&agent, ref_b[0].clone())
        .await
        .invocation
        .expect("session-b invocation");
    assert!(invocation_a.input.preview.contains("a.txt"));
    assert!(invocation_b.input.preview.contains("b.txt"));
    // The session is part of the identity: a lookup that keeps the loop-local
    // values but swaps the session must not resolve to the other session's
    // record. A session-insensitive key would find `ref_a` here.
    let cross_session = crate::tool_data::ToolRef {
        session_id: session_b.session_id,
        ..ref_a[0].clone()
    };
    assert!(matches!(
        agent
            .tool_read(crate::tool_data::ToolReadRequest {
                display: false,
                tool_ref: cross_session,
                max_bytes: None,
            })
            .await,
        Err(AgentError::ToolNotFound)
    ));
}

fn workspace_read_request(session_id: SessionId, path: &str) -> crate::WorkspaceReadRequest {
    crate::WorkspaceReadRequest {
        session_id,
        path: path.to_owned(),
        start_line: None,
        line_byte_offset: None,
        max_lines: None,
        max_bytes: None,
        if_revision: None,
    }
}

#[tokio::test]
async fn workspace_read_requires_a_loaded_session_and_touches_nothing_else() {
    let fixture = AgentFixture::new("workspace-read").file("note.txt", b"raw\ncontent\n");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let (mut agent, info) = fixture.start().await;
    let session_id = info.session_id;

    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let file_path = workspace.join("note.txt");
    let file_before = std::fs::read(&file_path).unwrap();

    let result = agent
        .workspace_read(workspace_read_request(session_id, "note.txt"))
        .await
        .unwrap();
    assert_eq!(result.status, crate::WorkspaceReadStatus::Ok);
    assert_eq!(result.content, "raw\ncontent\n");
    assert_eq!(result.returned_lines, 2);
    assert_eq!(result.revision.as_deref().map(str::len), Some(64));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&file_path).unwrap(), file_before);

    // A closed Session no longer locates its Workspace, and an unknown Session
    // ID is never guessed into one.
    agent.close_session(session_id).await.unwrap();
    assert!(matches!(
        agent
            .workspace_read(workspace_read_request(session_id, "note.txt"))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert!(matches!(
        agent
            .workspace_read(workspace_read_request(
                SessionId::new().unwrap(),
                "note.txt"
            ))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
}

fn workspace_files_request(session_id: SessionId) -> crate::WorkspaceFilesRequest {
    crate::WorkspaceFilesRequest {
        session_id,
        directory: None,
        recursive: None,
        query: None,
        cursor: None,
        limit: None,
        max_bytes: None,
    }
}

fn workspace_search_request(session_id: SessionId, query: &str) -> crate::WorkspaceSearchRequest {
    crate::WorkspaceSearchRequest {
        session_id,
        query: query.to_owned(),
        paths: None,
        case_sensitive: None,
        cursor: None,
        max_matches: None,
        max_bytes: None,
    }
}

#[tokio::test]
async fn workspace_files_and_search_require_a_loaded_session_and_touch_nothing_else() {
    let fixture = AgentFixture::new("workspace-scan").file("note.txt", b"raw\nneedle\n");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let (mut agent, info) = fixture.start().await;
    let session_id = info.session_id;

    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let file_path = workspace.join("note.txt");
    let file_before = std::fs::read(&file_path).unwrap();

    let files = agent
        .workspace_files(workspace_files_request(session_id))
        .await
        .unwrap();
    assert_eq!(files.directory, "");
    assert_eq!(files.entries.len(), 1);
    assert_eq!(files.entries[0].path, "note.txt");
    assert_eq!(files.entries[0].kind, crate::WorkspaceFileKind::File);
    assert!(files.scan_complete);

    let search = agent
        .workspace_search(workspace_search_request(session_id, "needle"))
        .await
        .unwrap();
    assert_eq!(search.matches.len(), 1);
    assert_eq!(search.matches[0].path, "note.txt");
    assert_eq!(search.matches[0].line_number, 2);
    assert_eq!(search.matches[0].line_text, "needle");
    assert!(search.scan_complete);

    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&file_path).unwrap(), file_before);

    // A closed Session no longer locates its Workspace, and an unknown Session
    // ID is never guessed into one.
    agent.close_session(session_id).await.unwrap();
    assert!(matches!(
        agent
            .workspace_files(workspace_files_request(session_id))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert!(matches!(
        agent
            .workspace_search(workspace_search_request(session_id, "needle"))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    let unknown = SessionId::new().unwrap();
    assert!(matches!(
        agent
            .workspace_files(workspace_files_request(unknown))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert!(matches!(
        agent
            .workspace_search(workspace_search_request(unknown, "needle"))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
}

#[tokio::test]
async fn tool_read_exposes_approval_time_data_without_running() {
    use crate::tool_data::{ToolExecutionState, ToolSubject};

    let fixture = AgentFixture::new("tool-data-policy").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["write".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Ask,
    };
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("write", json!({"path": "out.txt", "content": "written"})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .profile(profile);
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "write it").await;

    // Wait until the runtime reports the approval request, then query while
    // the tool is still blocked before execution.
    let interaction = events
        .expect(
            std::time::Duration::from_secs(2),
            "interaction event must arrive",
            |event| match event {
                AgentEvent::InteractionRequested { interaction, .. } => Some(interaction),
                _ => None,
            },
        )
        .await;
    let tool_ref = crate::tool_data::ToolRef {
        session_id: info.session_id,
        loop_id: turn.loop_id,
        request_index: 0,
        tool_call_id: interaction.tool_call_id.clone(),
    };
    let waiting = read_tool(&agent, tool_ref.clone()).await;
    assert_eq!(waiting.execution.state, ToolExecutionState::AwaitingPolicy);
    assert_ne!(
        waiting.execution.state,
        ToolExecutionState::Running,
        "policy wait must never report running"
    );
    // Approval-time data is obtainable before the tool executes.
    let waiting_invocation = waiting.invocation.expect("approval-time invocation data");
    assert_eq!(
        waiting_invocation.subject,
        ToolSubject::File {
            path: "out.txt".to_owned()
        }
    );
    assert!(waiting_invocation.input.preview.contains("written"));

    agent
        .answer(crate::agent::AnswerInteraction {
            turn,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::interaction::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::AllowOnce,
            ),
        })
        .await
        .unwrap();
    wait_text(&agent, turn).await;
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let finished = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(finished.execution.state, ToolExecutionState::Succeeded);
    let invocation = finished.invocation.expect("invocation after approval");
    assert_eq!(
        invocation.subject,
        ToolSubject::File {
            path: "out.txt".to_owned()
        }
    );
}

#[tokio::test]
async fn i2_02_tool_execution_event_matches_the_tool_read_query() {
    let fixture = AgentFixture::new("tool-data-events").file("a.txt", b"alpha\nbeta");
    let workspace = fixture.workspace.clone();
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 2})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;

    let mut invocation_event = None;
    let mut execution_event = None;
    while execution_event.is_none() {
        let event = events
            .next_event(
                std::time::Duration::from_secs(1),
                "tool data events must arrive",
            )
            .await;
        match event {
            AgentEvent::ToolInvocation { data, .. } => invocation_event = Some(data),
            AgentEvent::ToolExecution { data, .. } => execution_event = Some(data),
            AgentEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }
    let invocation_event = invocation_event.expect("tool_invocation event");
    let execution_event = execution_event.expect("tool_execution event");
    let queried = read_tool(&agent, execution_event.tool_ref.clone()).await;
    assert_eq!(execution_event.tool_ref, queried.execution.tool_ref);
    assert_eq!(execution_event.state, queried.execution.state);
    assert_eq!(execution_event.outcome, queried.execution.outcome);
    assert_eq!(invocation_event.tool_ref, execution_event.tool_ref);
    assert_eq!(
        invocation_event.subject,
        queried.invocation.expect("queried invocation").subject
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i2_06_first_request_observation_survives_runtime_start_binding() {
    use crate::tool_data::{CommandStatus, ToolDataStream, ToolExecutionState};
    use base64::Engine;

    let fixture = AgentFixture::new("startup-observation").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let model_gate = Arc::new(Gate::new());
    let model = ScriptedModel::new(
        "main",
        [Step::gated_tool(
            model_gate.clone(),
            "bash",
            json!({"command": "printf 'started\\n'; exec tail -f /dev/null"}),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let model_started = model.start_entered();
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .profile(bash_profile());
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(Gate::new());
    PAUSE_AFTER_RUNTIME_START.arm(info.session_id, Arc::clone(&gate));

    let agent = Arc::new(tokio::sync::Mutex::new(agent));
    let send_agent = Arc::clone(&agent);
    let mut send_task = tokio::spawn(async move {
        let mut agent = send_agent.lock().await;
        send_text(&mut agent, info.session_id, "run bash").await
    });

    // Keep every startup wait bounded. Always release both test gates before
    // joining the sender so a failed observation cannot strand its Session.
    let startup = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        gate.wait_started().await;
        model_started.notified().await;
        model_gate.wait_started().await;
    })
    .await;

    // The sender must still be waiting for install_loop to bind the started
    // loop. This is the observation that the startup gate is between
    // AgentLoop::start and bind_started_loop, not merely before startup.
    let startup_boundary_observed = startup.is_ok() && !send_task.is_finished();
    gate.release();
    model_gate.release();

    let send_result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut send_task).await;
    let turn = match send_result {
        Ok(Ok(turn)) => turn,
        Ok(Err(error)) => {
            let mut agent = agent.lock().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                agent.close_session(info.session_id),
            )
            .await;
            panic!("send task failed: {error}");
        }
        Err(_) => {
            send_task.abort();
            let _ = send_task.await;
            let mut agent = agent.lock().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                agent.close_session(info.session_id),
            )
            .await;
            panic!("install_loop did not leave the startup gate in time");
        }
    };

    if !startup_boundary_observed {
        let mut agent = agent.lock().await;
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agent.close_session(info.session_id),
        )
        .await;
        panic!("startup observation did not prove the bind boundary");
    }

    let mut agent = agent.lock().await;
    let body_result: Result<(), String> = async {
        let mut invocation = None;
        let mut process_ref = None;
        let mut output_chunk = None;
        let mut tool_started = false;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let event = events
                    .recv()
                    .await
                    .ok_or_else(|| "event stream closed before Bash started".to_owned())?;
                match event {
                    AgentEvent::ToolInvocation {
                        turn: event_turn,
                        data,
                        ..
                    } if event_turn == turn => {
                        invocation = Some(data);
                    }
                    AgentEvent::ToolStarted {
                        turn: event_turn, ..
                    } if event_turn == turn => {
                        tool_started = true;
                    }
                    AgentEvent::ToolProcess {
                        turn: event_turn,
                        data,
                        ..
                    } if event_turn == turn => {
                        if let Some(chunk) = data.chunk {
                            if chunk.stream == ToolDataStream::Stdout {
                                let bytes = base64::engine::general_purpose::STANDARD
                                    .decode(&chunk.data)
                                    .map_err(|error| format!("decode Bash output: {error}"))?;
                                process_ref = Some(data.tool_ref);
                                output_chunk = Some(bytes);
                            }
                        }
                    }
                    _ => {}
                }
                if invocation.is_some()
                    && tool_started
                    && process_ref.is_some()
                    && output_chunk.is_some()
                {
                    return Ok::<(), String>(());
                }
            }
        })
        .await
        .map_err(|_| "Bash startup events timed out".to_owned())??;

        let invocation = invocation.ok_or_else(|| "missing ToolInvocation event".to_owned())?;
        let tool_ref = invocation.tool_ref.clone();
        if invocation.name != "bash" {
            return Err(format!("expected Bash invocation, got {}", invocation.name));
        }
        if tool_ref.session_id != info.session_id
            || tool_ref.loop_id != turn.loop_id
            || tool_ref.request_index != 0
            || tool_ref.tool_call_id.as_str() != "bash-call-0"
        {
            return Err(format!("unexpected first Bash ToolRef: {tool_ref:?}"));
        }
        if process_ref != Some(tool_ref.clone()) {
            return Err("tool_process used a different ToolRef".to_owned());
        }

        let queried = agent
            .tool_read(crate::tool_data::ToolReadRequest {
                display: false,
                tool_ref: tool_ref.clone(),
                max_bytes: None,
            })
            .await
            .map_err(|error| format!("tool.read failed: {error}"))?;
        if queried.execution.tool_ref != tool_ref
            || queried
                .invocation
                .as_ref()
                .is_none_or(|data| data.tool_ref != tool_ref)
            || queried.execution.state != ToolExecutionState::Running
        {
            return Err("tool.read did not preserve the complete running ToolRef".to_owned());
        }
        let command = queried
            .execution
            .command
            .ok_or_else(|| "tool.read did not contain a Bash command".to_owned())?;
        if command.status != CommandStatus::Running {
            return Err(format!(
                "Bash command was not running: {:?}",
                command.status
            ));
        }

        let running_output = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .map_err(|error| format!("tool/output failed: {error}"))?;
        if running_output.tool_ref != tool_ref || running_output.stream != ToolDataStream::Stdout {
            return Err("tool/output did not preserve the complete ToolRef".to_owned());
        }
        let running_bytes = base64::engine::general_purpose::STANDARD
            .decode(&running_output.data)
            .map_err(|error| format!("decode tool/output: {error}"))?;
        if running_bytes != b"started\n" || running_output.eof {
            return Err(format!(
                "tool/output did not expose the live Bash output: {:?}, eof={}",
                running_bytes, running_output.eof
            ));
        }
        if agent
            .loaded_session(info.session_id)
            .ok_or_else(|| "Session was unloaded while Bash was running".to_owned())?
            .tool_owners()
            .active()
            == 0
        {
            return Err("Bash command owner was not active before cancellation".to_owned());
        }

        if !agent
            .cancel(turn)
            .map_err(|error| format!("cancel failed: {error}"))?
        {
            return Err("cancel did not find the active Bash turn".to_owned());
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), agent.wait_turn(turn))
            .await
            .map_err(|_| "turn.wait timed out after Bash cancellation".to_owned())?
            .map_err(|error| format!("turn.wait failed: {error}"))?;
        if result.report.outcome
            != minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
        {
            return Err(format!(
                "unexpected cancelled turn outcome: {:?}",
                result.report.outcome
            ));
        }

        let terminal = agent
            .tool_read(crate::tool_data::ToolReadRequest {
                display: false,
                tool_ref: tool_ref.clone(),
                max_bytes: None,
            })
            .await
            .map_err(|error| format!("terminal tool.read failed: {error}"))?;
        if terminal.execution.tool_ref != tool_ref
            || terminal.execution.state != ToolExecutionState::Cancelled
        {
            return Err("terminal tool.read changed the Bash ToolRef or state".to_owned());
        }
        let terminal_command = terminal
            .execution
            .command
            .ok_or_else(|| "terminal tool.read lost the Bash command".to_owned())?;
        if terminal_command.status != CommandStatus::Cancelled
            || !terminal_command.termination_confirmed
        {
            return Err("cancelled Bash command was not confirmed terminated".to_owned());
        }

        let terminal_output = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .map_err(|error| format!("terminal tool/output failed: {error}"))?;
        if terminal_output.tool_ref != tool_ref
            || (terminal_output.eof && terminal_output.data.is_empty())
        {
            return Err("terminal tool/output lost the observed Bash output".to_owned());
        }
        if agent
            .loaded_session(info.session_id)
            .ok_or_else(|| "Session was unloaded before owner join check".to_owned())?
            .tool_owners()
            .active()
            != 0
        {
            return Err("turn.wait returned before the Bash command owner joined".to_owned());
        }
        Ok(())
    }
    .await;

    let close_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.close_session(info.session_id),
    )
    .await;
    if let Err(error) = body_result {
        let _ = close_result;
        panic!("{error}");
    }
    close_result
        .expect("Session cleanup timed out")
        .expect("Session cleanup failed");
}

#[tokio::test]
async fn tool_result_raw_text_is_readable_by_offset_without_escaping() {
    let (data_dir, _guard) = fixture_dir("tool-data-offset");
    let expected = (1..=24)
        .map(|line| format!("line-{line}: 你好 café"))
        .collect::<Vec<_>>()
        .join("\n");
    let (workspace, _guard) = workspace_file("tool-data-offset-ws", "a.txt", expected.as_bytes());
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 64})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .open()
        .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);

    // `read` renders each source line with a "N: " prefix; reassembling the
    // paged original must be byte-exact, including non-ASCII and newlines.
    let expected_rendered = (1..=24)
        .map(|line| format!("{line}: line-{line}: 你好 café"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        read_output(&agent, refs[0].clone()).await,
        expected_rendered
    );
}

#[tokio::test]
async fn i2_03_tool_read_answers_without_consuming_live_events() {
    // A capacity-1 Agent event queue drops nearly every best-effort event, yet
    // the authoritative join report still reconciles terminal tool state.
    let fixture = AgentFixture::new("tool-data-lost").file("a.txt", b"hello");
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .options(AgentOptions {
            agent_event_capacity: 1,
            loop_event_capacity: Some(1),
        });
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;
    // The live stream is never consumed: events were dropped, not delivered.
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let result = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(
        result.execution.state,
        crate::tool_data::ToolExecutionState::Succeeded
    );
    assert!(
        read_output(&agent, refs[0].clone())
            .await
            .starts_with("1: hello")
    );
}

#[tokio::test]
async fn failed_tool_result_text_is_queryable_from_the_authoritative_report() {
    use crate::tool_data::{ToolDataAvailability, ToolExecutionState};

    let fixture = AgentFixture::new("tool-data-failed").file("a.txt", b"hello");
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "missing.txt"})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "read missing").await;
    wait_text(&agent, turn).await;

    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let result = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(result.execution.state, ToolExecutionState::Failed);
    assert_eq!(
        result.execution.outcome,
        Some(minicore_runtime::tools::ToolResultOutcome::Failed)
    );
    assert_eq!(
        result.execution.output_availability,
        ToolDataAvailability::Available
    );
    // The failed call's real text is recorded from the report, not left empty.
    let output = read_output(&agent, refs[0].clone()).await;
    assert_eq!(output, "tool failed");
    assert!(!output.contains("missing.txt"));
}

#[tokio::test]
async fn running_tool_output_stream_is_never_a_false_empty_eof() {
    use crate::tool_data::{ToolDataAvailability, ToolExecutionState};

    let fixture = AgentFixture::new("tool-data-pending").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;

    // Wait for the invocation event, then query while the tool may still be
    // running. The page must never claim an empty, complete (`eof`) available
    // stream: an unobserved output is `pending`.
    let data = events
        .expect(
            std::time::Duration::from_secs(2),
            "tool invocation event must arrive",
            |event| match event {
                AgentEvent::ToolInvocation { data, .. } => Some(data),
                _ => None,
            },
        )
        .await;
    let page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: data.tool_ref.clone(),
            stream: crate::tool_data::ToolDataStream::Output,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    match page.availability {
        ToolDataAvailability::Pending => {
            assert_eq!(page.observed_end, 0);
            assert!(!page.eof);
            assert!(page.data.is_empty());
        }
        ToolDataAvailability::Available => {
            assert!(page.eof);
            assert!(!page.data.is_empty());
        }
        other => panic!("unexpected early output availability: {other:?}"),
    }

    wait_text(&agent, turn).await;
    let finished = read_tool(&agent, data.tool_ref).await;
    assert_eq!(finished.execution.state, ToolExecutionState::Succeeded);
}

#[tokio::test]
async fn rejected_tool_input_never_fabricates_a_file_operation() {
    use crate::tool_data::{ToolExecutionState, ToolSubject};

    let fixture = AgentFixture::new("tool-data-invalid").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["write".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };
    // The schema requires `content`; the arguments are structurally valid JSON
    // but the tool's own parse rejects them.
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("write", json!({"path": "out.txt"})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .profile(profile);
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "write it").await;
    wait_text(&agent, turn).await;

    // The file was never created, and the recorded facts say the call failed
    // rather than claiming a write happened.
    assert!(!workspace.join("out.txt").exists());
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let result = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(result.execution.state, ToolExecutionState::Failed);
    // The requested input is recorded as requested, not as applied.
    let invocation = result.invocation.expect("requested input is recorded");
    assert_eq!(
        invocation.subject,
        ToolSubject::File {
            path: "out.txt".to_owned()
        }
    );
    assert!(invocation.input.preview.contains("out.txt"));
    assert!(!invocation.input.preview.contains("content"));
}

// ---------------------------------------------------------------------------
// P3b2: Provider Replay Budget And ContextOverflow Recovery Tests
// ---------------------------------------------------------------------------
