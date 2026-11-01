use super::*;

#[tokio::test]
async fn display_history_keeps_tool_bodies_queryable_without_embedding_raw_arguments() {
    let fixture = AgentFixture::new("view").file("file.txt", b"contents");
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;

    let page = history_page(&agent, info.session_id, 100).await;
    let serialized = page
        .items
        .iter()
        .map(|item| item.data.as_str())
        .collect::<String>();
    // A path is an explicitly permitted local-display detail; the raw
    // invocation object and unrelated argument keys are not.
    assert!(serialized.contains("file.txt"));
    assert!(!serialized.contains("arguments"));
    assert!(!serialized.contains("\"path\""));
    assert!(!serialized.contains("\"limit\""));
    // Safe fields remain visible.
    assert!(serialized.contains("read"));
    assert!(!serialized.contains("contents"));
    let item: serde_json::Value = serde_json::from_str(&page.items[1].data).unwrap();
    let output = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: serde_json::from_value(item["tool_summaries"][0]["tool_ref"].clone())
                .unwrap(),
            stream: crate::tool_data::ToolDataStream::Output,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(output.data, "1: contents");
    assert!(output.eof);
}

#[tokio::test]
async fn two_sessions_write_independently() {
    let (data_dir, _guard) = fixture_dir("multi-session");
    let (workspace_a, _ga) = workspace_file("multi-session-a", "a.txt", b"a");
    let model = ScriptedModel::new("main", [Step::Text("a1"), Step::Text("a2")])
        .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .open()
        .await;
    let session_x = create_session(&mut agent, &workspace_a).await;
    let (workspace_b, _gb) = workspace_file("multi-session-b", "b.txt", b"b");
    let session_y = create_session(&mut agent, &workspace_b).await;

    let turn_x = send_text(&mut agent, session_x.session_id, "x").await;
    let turn_y = send_text(&mut agent, session_y.session_id, "y").await;
    let rx = wait_text(&agent, turn_x).await;
    let ry = wait_text(&agent, turn_y).await;
    assert_persisted(&rx);
    assert_persisted(&ry);
    let hx = history_page(&agent, session_x.session_id, 100).await;
    let hy = history_page(&agent, session_y.session_id, 100).await;
    assert_eq!(hx.total, 2);
    assert_eq!(hy.total, 2);
}

#[tokio::test]
async fn send_is_rejected_for_unloaded_session() {
    let fixture = AgentFixture::new("unloaded")
        .file("a.txt", b"hello")
        .models(BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]));
    let (mut agent, info) = fixture.start().await;
    agent.close_session(info.session_id).await.unwrap();

    let error = agent
        .send_for_test(info.session_id, "hello".to_owned())
        .await;
    assert!(matches!(error, Err(AgentError::SessionNotLoaded)));
}

#[tokio::test]
async fn delete_rejects_loaded_session_and_removes_unloaded() {
    let fixture = AgentFixture::new("delete")
        .file("a.txt", b"hello")
        .models(BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2)),
        )]));
    let (mut agent, info) = fixture.start().await;
    assert!(matches!(
        agent.delete_session(info.session_id).await,
        Err(AgentError::SessionAlreadyLoaded)
    ));
    agent.close_session(info.session_id).await.unwrap();
    agent.delete_session(info.session_id).await.unwrap();
    assert!(agent.list_sessions().await.unwrap().is_empty());
}

#[tokio::test]
async fn persisted_history_after_failed_loop_is_still_durable() {
    let fixture = AgentFixture::new("failed").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let gate = Arc::new(Gate::new());

    let fixture = fixture.script([Step::gated_text(gate.clone(), "partial")]);
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;
    gate.wait_started().await;
    gate.release();
    let result = wait_text(&agent, turn).await;
    assert_persisted(&result);
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert!(!stored.is_empty());
}

#[tokio::test]
async fn event_stream_delivers_body_and_completion_snapshots() {
    let fixture = AgentFixture::new("event-stream").script([Step::Text("done")]);
    let (mut agent, info) = fixture.start().await;
    let mut events = agent.take_events().unwrap();
    let drain = tokio::spawn(async move {
        let (mut request, mut output, mut completed) = (false, false, false);
        while let Some(event) = events.recv().await {
            match event {
                AgentEvent::RequestStarted { .. } => request = true,
                AgentEvent::OutputDelta {
                    channel: OutputChannel::Text,
                    ..
                } => output = true,
                AgentEvent::SessionSnapshot { snapshot, .. } => {
                    completed |= snapshot.completions.through_seq == 1
                }
                _ => {}
            }
        }
        (request, output, completed)
    });
    let turn = send_text(&mut agent, info.session_id, "hello").await;
    assert_persisted(&agent.wait_turn(turn).await.unwrap());
    agent.shutdown().await.unwrap();
    assert_eq!(drain.await.unwrap(), (true, true, true));
}

#[tokio::test]
async fn every_runtime_tool_and_control_event_keeps_its_exact_load_across_reopen() {
    let fixture = AgentFixture::new("event-loads")
        .file("file.txt", b"content")
        .script([
            Step::ToolCall("read", json!({"path":"file.txt"})),
            Step::Text("first"),
            Step::ToolCall("read", json!({"path":"file.txt"})),
            Step::Text("second"),
        ]);
    let (mut agent, info) = fixture.start().await;
    let mut events = agent.take_events().unwrap();
    let drain = tokio::spawn(async move {
        let mut received = Vec::new();
        while let Some(event) = events.recv().await {
            assert!(received.len() < 512);
            received.push(event);
        }
        received
    });
    let first_load = agent.loaded_session(info.session_id).unwrap().load();
    let first = send_text(&mut agent, info.session_id, "read first").await;
    wait_text(&agent, first).await;
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let second_load = agent.loaded_session(info.session_id).unwrap().load();
    assert_ne!(first_load, second_load);
    let second = send_text(&mut agent, info.session_id, "read second").await;
    wait_text(&agent, second).await;
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
    let events = tokio::time::timeout(std::time::Duration::from_secs(5), drain)
        .await
        .unwrap()
        .unwrap();
    let mut tool_loads = BTreeSet::new();
    let mut completed = BTreeSet::new();
    for event in events {
        let meta = *event.meta();
        assert_eq!(meta.session_id, info.session_id);
        assert!(meta.load == first_load || meta.load == second_load);
        if let Some(loop_id) = meta.loop_id {
            assert_eq!(
                meta.load,
                if loop_id == first.loop_id {
                    first_load
                } else {
                    assert_eq!(loop_id, second.loop_id);
                    second_load
                }
            );
        }
        let value = serde_json::to_value(&event).unwrap();
        if let Some(turn) = value.pointer("/data/turn") {
            assert_eq!(turn["session_id"], json!(meta.session_id));
            assert_eq!(turn["loop_id"], json!(meta.loop_id.unwrap()));
        }
        match event {
            AgentEvent::SessionSnapshot { snapshot, .. } => {
                assert_eq!(snapshot.load, meta.load);
                assert_eq!(snapshot.session_id, meta.session_id);
                completed.extend(
                    snapshot
                        .completions
                        .entries
                        .iter()
                        .map(|entry| entry.loop_id),
                );
            }
            AgentEvent::ToolInvocation { .. }
            | AgentEvent::ToolExecution { .. }
            | AgentEvent::ToolPresentation { .. } => {
                tool_loads.insert(meta.load);
            }
            _ => {}
        }
    }
    assert_eq!(tool_loads, BTreeSet::from([first_load, second_load]));
    assert_eq!(completed, BTreeSet::from([first.loop_id, second.loop_id]));
}

#[tokio::test]
async fn join_first_drains_queued_finished_without_duplicate_agent_completion() {
    let fixture = AgentFixture::new("join-first").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();

    let fixture = fixture.script([Step::Text("done")]).options(AgentOptions {
        agent_event_capacity: 256,
        loop_event_capacity: Some(32),
    });
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(Gate::new());
    PAUSE_BEFORE_JOIN.arm(info.session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, info.session_id, "join first").await;

    gate.wait_started().await;
    wait_until(
        std::time::Duration::from_secs(1),
        "runtime must finish while the Agent worker is paused",
        || {
            agent
                .runtime_loop_finished_for_test(info.session_id)
                .unwrap()
        },
    )
    .await;

    let drain = tokio::spawn(async move {
        let mut completed_seq = 0;
        while let Some(event) = events.recv().await {
            if let AgentEvent::SessionSnapshot { snapshot, .. } = event {
                completed_seq = completed_seq.max(snapshot.completions.through_seq);
            }
        }
        completed_seq
    });
    gate.release();

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("join-first drain must not wait for channel closure")
        .unwrap();
    assert_completed(&result);
    agent.shutdown().await.unwrap();
    assert_eq!(drain.await.unwrap(), 1);
}

#[tokio::test]
async fn queued_finished_drops_are_accounted_without_runtime_completion_mapping() {
    let fixture = AgentFixture::new("finished-drops").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    let model = ScriptedModel::new("main", [Step::Text("unused")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    // Control snapshots are published on their own reliable lane; this test
    // asserts only that a dropped Runtime `Finished` maps to no Agent event.
    while let Ok(event) =
        tokio::time::timeout(std::time::Duration::from_secs(1), events.recv()).await
    {
        if let Some(event) = event
            && !matches!(
                event,
                AgentEvent::SessionSnapshot { .. } | AgentEvent::SessionOpened { .. }
            )
        {
            panic!("unexpected event before the assertion: {event:?}");
        }
    }
    let loop_id = minicore_runtime::LoopId::new().unwrap();

    agent
        .forward_runtime_event_for_test(
            info.session_id,
            minicore_runtime::LoopEventEnvelope {
                dropped_before: 7,
                event: minicore_runtime::LoopEvent::Finished {
                    loop_id,
                    outcome: minicore_runtime::LoopOutcomeSummary::Completed,
                },
            },
        )
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), events.recv(),)
            .await
            .is_err()
    );

    let turn = TurnRef {
        session_id: info.session_id,
        loop_id,
    };
    agent.event_sink.try_send(AgentEvent::OutputDelta {
        turn,
        request_index: 0,
        channel: OutputChannel::Text,
        delta: "done".into(),
        meta: crate::event::EventMeta {
            session_id: turn.session_id,
            load: crate::test_support::load_id(),
            loop_id: Some(turn.loop_id),
            dropped_before: 0,
        },
    });
    let Some(AgentEvent::OutputDelta { meta, .. }) = events.recv().await else {
        panic!("expected the output event");
    };
    assert_eq!(meta.dropped_before, 7);
}

#[tokio::test]
async fn loop_event_drops_surface_in_agent_events_but_turn_result_is_authoritative() {
    let fixture = AgentFixture::new("loop-drops").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    // A back-to-back delta burst saturates the runtime's best-effort loop
    // event channel (capacity 1) during the runner's synchronous tail, so the
    // runtime deterministically drops events and reports them on the next
    // delivered envelope.
    let model = ScriptedModel::new(
        "main",
        [
            Step::BurstTextThenTool("x", 300, "read", json!({"path": "a.txt", "limit": 32})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .options(AgentOptions {
            agent_event_capacity: 256,
            loop_event_capacity: Some(1),
        });
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "burst").await;

    let drain = tokio::spawn(async move {
        let mut saw_drops = false;
        let mut saw_tool_finished = false;
        loop {
            let Some(event) = events.recv().await else {
                break;
            };
            let meta = event.meta();
            saw_drops |= meta.dropped_before > 0;
            saw_tool_finished |= matches!(event, AgentEvent::ToolFinished { .. });
        }
        (saw_drops, saw_tool_finished)
    });

    let result = agent.wait_turn(turn).await.unwrap();
    assert_persisted(&result);
    assert_completed(&result);
    agent.shutdown().await.unwrap();
    // The rejected loop events are accounted for on a delivered Agent event
    // (here the tool round's `tool_finished`) instead of vanishing silently.
    let (saw_drops, _saw_tool_finished) = drain.await.unwrap();
    assert!(
        saw_drops,
        "runtime loop drops must surface as dropped_before on an Agent event"
    );
    // user + assistant(tool call) + tool result + final assistant text
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 4);
}

#[tokio::test]
async fn full_agent_event_channel_keeps_wait_and_history_authoritative() {
    let fixture = AgentFixture::new("full-channel").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();

    let fixture = fixture
        .script([Step::Text("one"), Step::Text("two")])
        .options(AgentOptions {
            agent_event_capacity: 1,
            loop_event_capacity: None,
        });
    let mut agent = fixture.open().await;
    // Never read the event stream: the Agent event channel stays full and
    // best-effort body events must not block or disturb the authoritative
    // result.
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "first").await;
    let result = agent.wait_turn(turn).await.unwrap();
    assert_persisted(&result);

    let page = history_page(&agent, info.session_id, 100).await;
    assert_eq!(page.total, 2);
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 2);
}

#[tokio::test]
async fn immediate_send_after_completion_does_not_report_busy() {
    let fixture = AgentFixture::new("reopen-race").file("a.txt", b"hello");
    let model = ScriptedModel::new(
        "main",
        [Step::Text("one"), Step::Text("two"), Step::Text("three")],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    // Once a turn's agent-level completion is published, an immediate next
    // send must never observe a SessionBusy window.
    for expected in ["one", "two", "three"] {
        let turn = send_text(&mut agent, info.session_id, expected).await;
        let result = agent.wait_turn(turn).await.unwrap();
        assert_completed(&result);
        assert_persisted(&result);
    }
    let page = history_page(&agent, info.session_id, 100).await;
    assert_eq!(page.total, 6);
}
