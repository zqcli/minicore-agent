use super::*;

/// Two turns across model change: the first Bash command is cancelled, the
/// second model reuses the same tool call id ("bash-call-0") under a new loop,
/// the second Bash command actually runs without mixing up stdout/stderr/queries,
/// and close_session reaps the active second process before returning.
#[cfg(unix)]
#[tokio::test]
async fn bash_two_turns_reuse_tool_call_id_across_loops_and_close_reaps_active() {
    use crate::tool_data::{CommandStatus, ToolDataStream};
    use base64::Engine;

    let fixture = AgentFixture::new("bash-two-turns").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let first_pid_file = workspace.join("first.pid");
    let second_pid_file = workspace.join("second.pid");

    let model_a = ScriptedModel::new(
        "main",
        [
            Step::ToolCall(
                "bash",
                json!({"command": "echo $$ > first.pid; exec sleep 30"}),
            ),
            Step::Text("first turn cancelled"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));

    let model_b = ScriptedModel::new(
        "other",
        [
            Step::ToolCall(
                "bash",
                json!({
                    "command": "printf 'turn2-stdout\\n'; printf 'turn2-stderr\\n' >&2; echo $$ > second.pid; exec sleep 30"
                }),
            ),
            Step::Text("second turn done"),
        ],
    ).with_pacing(std::time::Duration::from_millis(2));

    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };

    let fixture = fixture
        .models(BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]))
        .profile(profile);
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;

    // --- Turn 1 ---
    let turn_1 = send_text(&mut agent, info.session_id, "first run slowly").await;
    loop {
        let event = events
            .next_event(std::time::Duration::from_secs(5), "bash events must arrive")
            .await;
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn_1)
        {
            break;
        }
    }
    for _ in 0..500 {
        if first_pid_file.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(first_pid_file.exists(), "first command started");
    let first_pid: i32 = std::fs::read_to_string(&first_pid_file)
        .expect("read first pid")
        .trim()
        .parse()
        .expect("parse first pid");
    assert!(process_is_listed(first_pid));

    assert!(agent.cancel(turn_1).unwrap());
    let result_1 = wait_text(&agent, turn_1).await;
    assert_eq!(
        result_1.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert!(
        !process_is_listed(first_pid),
        "first process reaped after turn 1 cancelled"
    );

    // Drain turn 1 events to capture turn 1 tool_ref
    let mut turn_1_ref = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(300), events.recv()).await
    {
        if let AgentEvent::ToolProcess { turn, data, .. } = event
            && turn == turn_1
        {
            turn_1_ref = Some(data.tool_ref);
        }
    }
    let tool_ref_1 = turn_1_ref.expect("turn 1 produced tool process event");
    assert_eq!(tool_ref_1.tool_call_id.as_str(), "bash-call-0");

    // --- Switch model to model_b ("other") ---
    agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();

    // --- Turn 2 ---
    let turn_2 = send_text(
        &mut agent,
        info.session_id,
        "second run with same tool call id",
    )
    .await;
    loop {
        let event = events
            .next_event(std::time::Duration::from_secs(5), "bash events must arrive")
            .await;
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn_2)
        {
            break;
        }
    }
    for _ in 0..500 {
        if second_pid_file.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(second_pid_file.exists(), "second command started");
    let second_pid: i32 = std::fs::read_to_string(&second_pid_file)
        .expect("read second pid")
        .trim()
        .parse()
        .expect("parse second pid");
    assert!(process_is_listed(second_pid));

    // Drain turn 2 events until we get tool_ref for turn 2 and chunks
    let mut turn_2_ref = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), events.recv()).await
    {
        if let AgentEvent::ToolProcess { turn, data, .. } = event
            && turn == turn_2
        {
            turn_2_ref = Some(data.tool_ref);
        }
    }
    let tool_ref_2 = turn_2_ref.expect("turn 2 produced tool process event");

    // Both calls used the exact same tool_call_id ("bash-call-0"), but different loops!
    assert_eq!(tool_ref_2.tool_call_id.as_str(), "bash-call-0");
    assert_ne!(
        tool_ref_1.loop_id, tool_ref_2.loop_id,
        "turns must have distinct loops"
    );

    // Queries for tool_ref_2 must return turn 2's distinct stdout/stderr and not mix with turn 1
    let stdout_page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref_2.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    let stdout_bytes = base64::engine::general_purpose::STANDARD
        .decode(&stdout_page.data)
        .unwrap();
    let stdout_str = String::from_utf8_lossy(&stdout_bytes);
    assert!(
        stdout_str.contains("turn2-stdout"),
        "second bash stdout must contain 'turn2-stdout', got: {stdout_str}"
    );

    let stderr_page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref_2.clone(),
            stream: ToolDataStream::Stderr,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    let stderr_bytes = base64::engine::general_purpose::STANDARD
        .decode(&stderr_page.data)
        .unwrap();
    let stderr_str = String::from_utf8_lossy(&stderr_bytes);
    assert!(
        stderr_str.contains("turn2-stderr"),
        "second bash stderr must contain 'turn2-stderr', got: {stderr_str}"
    );

    let queried_2 = read_tool(&agent, tool_ref_2.clone()).await;
    assert_eq!(queried_2.execution.tool_ref, tool_ref_2);
    let cmd_2 = queried_2.execution.command.expect("command record exists");
    assert_eq!(cmd_2.status, CommandStatus::Running);

    // The second command is STILL running (active). Calling close_session must reap it before returning.
    assert!(
        process_is_listed(second_pid),
        "second process must still be running before close_session"
    );
    agent.close_session(info.session_id).await.unwrap();
    assert!(
        !process_is_listed(second_pid),
        "active second command was not reaped by close_session join barrier"
    );
}

/// Proves that when auxiliary tool persistence fails, the core loop outcome is
/// still completed, main history is persisted, the bash process is reaped,
/// and no duplicate tool execution occurs.
#[cfg(unix)]
#[tokio::test]
async fn bash_turn_auxiliary_write_failure_preserves_main_outcome_and_reap() {
    let fixture = AgentFixture::new("bash-aux-fail").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let pid_file = workspace.join("child.pid");

    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall(
                "bash",
                json!({"command": "echo $$ > child.pid; printf 'done' > output.txt"}),
            ),
            Step::Text("all done"),
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
    let (mut agent, info) = fixture.start().await;

    // Inject auxiliary write failure on this session
    crate::store::AUX_WRITE_FAILURES.arm(info.session_id, ());

    let turn = send_text(&mut agent, info.session_id, "run with aux failure").await;
    let result = wait_text(&agent, turn).await;

    // Core loop outcome must be Completed and core history Persisted
    assert_completed(&result);
    assert_persisted(&result);

    // Wait for the process to exit and confirm output
    for _ in 0..500 {
        if workspace.join("output.txt").exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(workspace.join("output.txt").exists());

    if pid_file.exists() {
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(!process_is_listed(pid), "bash process was reaped");
    }

    // In-memory tool records are preserved and marked Failed, core outcome intact
    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    assert!(!tool_refs.is_empty());
    for tool_ref in tool_refs {
        let read_res = agent
            .tool_read(crate::tool_data::ToolReadRequest {
                display: false,
                tool_ref: tool_ref.clone(),
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        assert_eq!(
            read_res.execution.recording,
            crate::tool_data::ToolRecordingState::Failed
        );
        assert_eq!(
            read_res.execution.outcome,
            Some(minicore_runtime::tools::ToolResultOutcome::Success)
        );
    }

    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn bash_turn_auxiliary_write_success_marks_recording_saved_and_persists_to_disk() {
    let fixture = AgentFixture::new("bash-aux-success").file("a.txt", b"hello");

    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall(
                "bash",
                json!({"command": "printf 'persisted cold output' > output.txt"}),
            ),
            Step::Text("all done"),
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
    let (mut agent, info) = fixture.start().await;

    let turn = send_text(&mut agent, info.session_id, "run with aux success").await;
    let result = wait_text(&agent, turn).await;
    agent
        .loaded_session(info.session_id)
        .unwrap()
        .cleanup_finished()
        .await
        .unwrap();

    assert_completed(&result);
    assert_persisted(&result);

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    assert!(!tool_refs.is_empty());
    let tool_ref = &tool_refs[0];

    // 1. In-memory record is marked Saved
    let live_read = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(
        live_read.execution.recording,
        crate::tool_data::ToolRecordingState::Saved
    );

    // 2. Real cold read from Store disk
    let store = agent.store_handle();
    let recovered = store
        .read_tool_record(tool_ref)
        .await
        .unwrap()
        .expect("persisted tool record must exist on disk");

    let cold_read = recovered.project_read(tool_ref, 4096).unwrap();
    assert_eq!(
        cold_read.execution.recording,
        crate::tool_data::ToolRecordingState::Saved
    );
    assert_eq!(cold_read.execution.name, "bash");

    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn session_close_while_aux_write_held_still_joins_owned_worker() {
    let fixture = AgentFixture::new("bash-aux-close").file("a.txt", b"hello");

    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("bash", json!({"command": "echo done > output.txt"})),
            Step::Text("finished"),
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
    let (mut agent, info) = fixture.start().await;

    let gate = Arc::new(crate::test_support::Gate::new());
    crate::store::AUX_COMMIT_GATES.arm(info.session_id, Arc::clone(&gate));

    let turn = send_text(&mut agent, info.session_id, "run with aux notify").await;

    // 1. Prove that execution has reached the aux write phase under lock
    gate.wait_started().await;

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), wait_text(&agent, turn))
        .await
        .expect("turn completion must not wait for auxiliary persistence");
    assert_persisted(&result);

    // 2. Poll close session while aux write is held: must remain pending on owned worker join
    let store = agent.store_handle();
    let close_task = tokio::spawn(async move { agent.close_session(info.session_id).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !close_task.is_finished(),
        "close must be waiting for active loop worker"
    );
    assert!(store.has_history_index(info.session_id));

    // 3. Release gate to let aux write complete
    gate.release();

    // 4. Worker joins cleanly and close completes
    let close_res = close_task.await.unwrap();
    assert!(close_res.is_ok());
    assert!(!store.has_history_index(info.session_id));
}

#[tokio::test]
async fn deadline_lock_timeout_marks_records_failed_without_alloc() {
    let fixture = AgentFixture::new("bash-aux-deadline").file("a.txt", b"hello");

    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("bash", json!({"command": "printf 'fast'"})),
            Step::Text("done"),
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
    let (mut agent, info) = fixture.start().await;

    let session = agent.loaded_session(info.session_id).unwrap();
    let loop_id = minicore_runtime::LoopId::new().unwrap();
    let tool_ref = crate::tool_data::ToolRef {
        session_id: info.session_id,
        loop_id,
        request_index: 0,
        tool_call_id: minicore_runtime::ToolCallId::new("call-dl-timeout").unwrap(),
    };

    session.tool_data().note_requested(&tool_ref, "bash");
    session.tool_data().mark_running(&tool_ref);
    session.tool_data().note_result(&tool_ref, "hi\n");
    session.tool_data().finish_and_snapshot(
        &tool_ref,
        minicore_runtime::tools::ToolResultOutcome::Success,
    );

    // Initial state is MemoryOnly
    let initial_read = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(
        initial_read.execution.recording,
        crate::tool_data::ToolRecordingState::MemoryOnly
    );

    // Call persist_loop_tool_records with an already-expired deadline
    let store = agent.store_handle();
    let expired_deadline = std::time::Instant::now() - std::time::Duration::from_millis(10);

    let results = store
        .persist_loop_tool_records(
            std::slice::from_ref(&tool_ref),
            session.tool_data().as_ref(),
            expired_deadline,
        )
        .await;

    assert_eq!(results.len(), 1);
    assert!(results[0].1.is_err());

    agent.close_session(info.session_id).await.unwrap();
}
