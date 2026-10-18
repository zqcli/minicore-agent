use super::*;

#[cfg(unix)]
#[tokio::test]
async fn bash_dual_stream_cold_read_closure_after_restart_without_session_loaded() {
    use crate::tool_data::{CommandStatus, ToolDataStream, ToolExecutionState, ToolRecordingState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-cold-read-closure-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-cold-read-closure-ws", "marker.txt", b"");

    let script = r"i=0; while [ $i -lt 512 ]; do printf '\316\273\000\377\033[31mX\033[0m'; printf '\347\225\214\033[2K\r' >&2; i=$((i+1)); done";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    let result = wait_text(&agent, turn).await;
    agent
        .loaded_session(info.session_id)
        .unwrap()
        .cleanup_finished()
        .await
        .unwrap();
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    assert_eq!(tool_refs.len(), 1);
    let tool_ref = tool_refs[0].clone();

    // Verify in-memory state before closing
    let live_read = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(live_read.execution.recording, ToolRecordingState::Saved);
    assert_eq!(live_read.execution.state, ToolExecutionState::Succeeded);

    // Close session and drop agent instance to simulate full restart
    agent.close_session(info.session_id).await.unwrap();
    drop(agent);

    // Re-open fresh agent without loading the session
    let agent2 = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        bash_profile(),
    )
    .await;
    assert!(agent2.loaded_session(info.session_id).is_none());

    // 1. tool_read cold projection
    let cold_read = agent2
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(cold_read.execution.tool_ref, tool_ref);
    assert_eq!(cold_read.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(cold_read.execution.recording, ToolRecordingState::Saved);

    let command = cold_read
        .execution
        .command
        .expect("command record preserved on cold read");
    assert_eq!(command.status, CommandStatus::Exited);
    assert_eq!(command.exit_code, Some(0));
    assert!(command.termination_confirmed);
    assert!(command.output_complete);
    assert!(command.stdout_observed_end > 0);
    assert!(command.stderr_observed_end > 0);

    for (stream, expected) in [
        (
            ToolDataStream::Stdout,
            b"\xce\xbb\x00\xff\x1b[31mX\x1b[0m".repeat(512),
        ),
        (ToolDataStream::Stderr, b"\xe7\x95\x8c\x1b[2K\r".repeat(512)),
    ] {
        let mut bytes = Vec::new();
        let mut offset = 0_u64;
        let mut pages = 0;
        loop {
            let page = agent2
                .tool_output(crate::tool_data::ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream,
                    offset,
                    max_bytes: Some(1024),
                })
                .await
                .unwrap();
            assert_eq!(page.encoding, "base64");
            assert!(serde_json::to_vec(&page).unwrap().len() <= 1024);
            assert_eq!(page.base_offset, offset);
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&page.data)
                .unwrap();
            assert_eq!(page.next_offset - offset, decoded.len() as u64);
            bytes.extend_from_slice(&decoded);
            offset = page.next_offset;
            pages += 1;
            if page.eof {
                break;
            }
        }
        assert!(pages > 1);
        assert_eq!(bytes, expected);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cold_read_is_strictly_read_only_and_survives_missing_workspace_and_partial_history() {
    use crate::tool_data::{ToolDataStream, ToolExecutionState, ToolRecordingState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-readonly-guard-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-readonly-guard-ws", "marker.txt", b"");

    let script = "echo hello-cold-read";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    agent.close_session(info.session_id).await.unwrap();
    drop(agent);

    // Tamper with environment to prove cold-read requires NO valid workspace
    std::fs::remove_dir_all(&workspace).unwrap();
    assert!(!workspace.exists());

    // Tamper with history.jsonl by appending a trailing truncated JSON fragment
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let initial_history_len = std::fs::metadata(&history_path).unwrap().len();
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .unwrap();
        file.write_all(b"{\"partial_corrupt_line\": true").unwrap();
    }
    let tampered_history_len = std::fs::metadata(&history_path).unwrap().len();
    assert!(tampered_history_len > initial_history_len);

    // Record tool aux directory contents using real store layout
    let hash = crate::store::tool_ref_hash(&tool_ref);
    let aux_dir = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join(crate::store::AUX_TOOLS_DIR)
        .join(&hash);
    let mut before_aux_files = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&aux_dir).unwrap() {
        let entry = entry.unwrap();
        let bytes = std::fs::read(entry.path()).unwrap();
        before_aux_files.insert(entry.file_name(), bytes);
    }
    assert!(!before_aux_files.is_empty());

    // Re-open agent without loading the session
    let agent2 = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        bash_profile(),
    )
    .await;

    // Cold read must succeed seamlessly
    let cold_read = agent2
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(cold_read.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(cold_read.execution.recording, ToolRecordingState::Saved);

    let page = agent2
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&page.data)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&decoded), "hello-cold-read\n");

    // Strictly read-only: history.jsonl length must NOT be rewritten or repaired
    assert_eq!(
        std::fs::metadata(&history_path).unwrap().len(),
        tampered_history_len,
        "history.jsonl must remain byte-identical without repair side effects"
    );

    // Strictly read-only: aux directory files must remain byte-identical
    for (name, expected_bytes) in before_aux_files {
        let current = std::fs::read(aux_dir.join(name)).unwrap();
        assert_eq!(current, expected_bytes, "aux files must not be altered");
    }
}

#[tokio::test]
async fn cold_read_missing_aux_or_unknown_tool_maps_to_tool_not_found() {
    let (data_dir, _guard) = fixture_dir(&format!("p5b2-not-found-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-not-found-ws", "marker.txt", b"");

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();

    let non_existent_ref = crate::tool_data::ToolRef {
        session_id: info.session_id,
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: minicore_runtime::ToolCallId::new("call-missing").unwrap(),
    };

    let read_err = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: non_existent_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(read_err, AgentError::ToolNotFound));

    let output_err = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: non_existent_ref,
            stream: crate::tool_data::ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(output_err, AgentError::ToolNotFound));
}

#[cfg(unix)]
#[tokio::test]
async fn warm_record_served_immediately_without_disk_or_gate() {
    use crate::read::TOOL_READ_GATES;
    use crate::tool_data::{ToolDataStream, ToolExecutionState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-warm-gate-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-warm-gate-ws", "marker.txt", b"");

    let script = "printf 'warm-content\n'";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;
    agent
        .loaded_session(info.session_id)
        .unwrap()
        .cleanup_finished()
        .await
        .unwrap();

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    // 1. Install a gate on the cold path for this tool_ref.
    let gate = Arc::new(crate::test_support::Gate::new());
    TOOL_READ_GATES.arm(tool_ref.clone(), Arc::clone(&gate));

    // 2. Corrupt disk record to ensure disk read would fail if attempted
    let hash = crate::store::tool_ref_hash(&tool_ref);
    let record_file = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join(crate::store::AUX_TOOLS_DIR)
        .join(&hash)
        .join(crate::store::TOOL_RECORD_FILE);
    std::fs::write(&record_file, b"corrupted-not-json").unwrap();

    // 3. Warm tool_read must return immediately without blocking on gate or failing from corrupted disk!
    let read_res = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        agent.tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        }),
    )
    .await
    .expect("warm read must not block on disk gate")
    .unwrap();
    assert_eq!(read_res.execution.state, ToolExecutionState::Succeeded);

    // 4. Warm tool_output must also return immediately
    let output_page = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        agent.tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        }),
    )
    .await
    .expect("warm output must not block on disk gate")
    .unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&output_page.data)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&decoded), "warm-content\n");
}

#[cfg(unix)]
#[tokio::test]
async fn evicted_stream_restored_via_narrow_merge_from_disk() {
    use crate::tool_data::{ToolDataAvailability, ToolDataStream, ToolRecordingState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-evicted-restore-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-evicted-restore-ws", "marker.txt", b"");

    let script = "printf 'persisted-stream-data\n'";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;
    agent
        .loaded_session(info.session_id)
        .unwrap()
        .cleanup_finished()
        .await
        .unwrap();

    let session = agent.loaded_session(info.session_id).unwrap();
    let tool_refs = session
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    // Verify initially saved
    let read_1 = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(read_1.execution.recording, ToolRecordingState::Saved);

    // Push 9 records with 1 MiB chunk each to exceed MAX_TOOL_TOTAL_BYTES (8 MiB) and trigger eviction
    let filler_loop = minicore_runtime::LoopId::new().unwrap();
    for i in 1..=9 {
        let filler = crate::tool_data::ToolRef {
            session_id: info.session_id,
            loop_id: filler_loop,
            request_index: i,
            tool_call_id: minicore_runtime::ToolCallId::new(format!("filler-{i}")).unwrap(),
        };
        session.tool_data().note_requested(&filler, "bash");
        session.tool_data().note_stream_chunk(
            &filler,
            ToolDataStream::Stdout,
            &vec![b'x'; 1024 * 1024],
        );
    }

    // Direct memory check on session shows availability is Expired
    let mem_output = session
        .tool_data()
        .output(
            &crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: None,
            },
            4096,
        )
        .unwrap();
    assert_eq!(mem_output.availability, ToolDataAvailability::Expired);

    // But querying through Agent::tool_output uses narrow merge with disk!
    let merged_output = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(merged_output.availability, ToolDataAvailability::Available);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&merged_output.data)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&decoded), "persisted-stream-data\n");
}

#[cfg(unix)]
#[tokio::test]
async fn both_sources_unavailable_preserves_known_offsets() {
    use crate::tool_data::{ToolDataAvailability, ToolDataStream};

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-unavailable-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-unavailable-ws", "marker.txt", b"");

    let script = "printf 'should-be-unavailable\n'";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;
    agent
        .loaded_session(info.session_id)
        .unwrap()
        .cleanup_finished()
        .await
        .unwrap();

    let session = agent.loaded_session(info.session_id).unwrap();
    let tool_refs = session
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    // Push 9 records with 1 MiB chunk each to evict stdout
    let filler_loop = minicore_runtime::LoopId::new().unwrap();
    for i in 1..=9 {
        let filler = crate::tool_data::ToolRef {
            session_id: info.session_id,
            loop_id: filler_loop,
            request_index: i,
            tool_call_id: minicore_runtime::ToolCallId::new(format!("filler-{i}")).unwrap(),
        };
        session.tool_data().note_requested(&filler, "bash");
        session.tool_data().note_stream_chunk(
            &filler,
            ToolDataStream::Stdout,
            &vec![b'x'; 1024 * 1024],
        );
    }

    // Corrupt disk aux record so disk cannot restore it
    let hash = crate::store::tool_ref_hash(&tool_ref);
    let record_file = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join(crate::store::AUX_TOOLS_DIR)
        .join(&hash)
        .join(crate::store::TOOL_RECORD_FILE);
    std::fs::write(&record_file, b"corrupted-record").unwrap();

    // Query tool_output: both sources cannot provide data, availability is Expired/Unavailable,
    // but observed_end retains the observed range (not 0!)
    let page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        page.availability,
        ToolDataAvailability::Expired | ToolDataAvailability::Unavailable
    ));
    assert_eq!(page.observed_end, b"should-be-unavailable\n".len() as u64);
    assert!(page.data.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn reader_drop_or_timeout_does_not_cancel_running_bash() {
    use crate::tool_data::ToolDataStream;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-reader-drop-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-reader-drop-ws", "marker.txt", b"");

    let pid_file = workspace.join("child.pid");
    let script = format!("echo $$ > '{}'; sleep 5", pid_file.display());
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "sleep bash").await;

    // Wait for the tool invocation to start running
    let tool_ref = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap();
        if let AgentEvent::ToolInvocation { data, .. } = event {
            break data.tool_ref;
        }
    };

    // Wait until child PID is written
    while !pid_file.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Query tool_output with an immediately expiring timeout to simulate reader drop/timeout
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(1),
        agent.tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        }),
    )
    .await;

    // The reader dropping/timing out MUST NOT kill the running process!
    assert!(
        process_is_listed(pid),
        "child bash process must still be running after reader timeout"
    );

    // Clean up
    let _ = wait_text(&agent, turn).await;
}

// ---------------------------------------------------------------------------
// P7: closeout integration acceptance
// ---------------------------------------------------------------------------
