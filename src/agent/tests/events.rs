use super::*;

#[tokio::test]
async fn history_view_exposes_whitelisted_tool_detail_without_raw_arguments() {
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

    let page = history_page(&agent, info.session_id, 100);
    let serialized = serde_json::to_string(&page).unwrap();
    // A path is an explicitly permitted local-display detail; the raw
    // invocation object and unrelated argument keys are not.
    assert!(serialized.contains("file.txt"));
    assert!(!serialized.contains("arguments"));
    assert!(!serialized.contains("\"path\""));
    assert!(!serialized.contains("\"limit\""));
    // Safe fields remain visible.
    assert!(serialized.contains("read"));
    assert!(serialized.contains("contents"));
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
    let hx = history_page(&agent, session_x.session_id, 100);
    let hy = history_page(&agent, session_y.session_id, 100);
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
async fn event_stream_delivers_lifecycle_events() {
    let fixture = AgentFixture::new("events").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();

    let fixture = fixture.script([Step::Text("final")]);
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;

    // Drain the agent event stream concurrently with the turn so the
    // best-effort event queue never overflows.
    let drain = tokio::spawn(async move {
        let mut seen_turn_started = false;
        let mut seen_request_started = false;
        let mut seen_output = false;
        let mut seen_finished = false;
        loop {
            let Some(event) = events.recv().await else {
                break;
            };
            match event {
                AgentEvent::TurnStarted {
                    turn: event_turn, ..
                } => {
                    assert_eq!(event_turn, turn);
                    seen_turn_started = true;
                }
                AgentEvent::RequestStarted { .. } => seen_request_started = true,
                AgentEvent::OutputDelta {
                    channel: OutputChannel::Text,
                    ..
                } => seen_output = true,
                AgentEvent::TurnFinished { .. } => {
                    seen_finished = true;
                    break;
                }
                _ => {}
            }
        }
        (
            seen_turn_started,
            seen_request_started,
            seen_output,
            seen_finished,
        )
    });

    let result = agent.wait_turn(turn).await.unwrap();
    assert_persisted(&result);
    let (seen_turn_started, seen_request_started, seen_output, seen_finished) =
        drain.await.unwrap();
    assert!(seen_turn_started);
    assert!(seen_request_started);
    assert!(seen_output);
    assert!(seen_finished);
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
        let mut turn_finished = 0;
        while let Some(event) = events.recv().await {
            if matches!(event, AgentEvent::TurnFinished { .. }) {
                turn_finished += 1;
            }
        }
        turn_finished
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
    let _ = events.recv().await;
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
    agent.event_sink.try_send(AgentEvent::TurnFinished {
        turn,
        outcome: crate::event::LoopOutcomeView::Completed,
        persistence: crate::sessions::TurnPersistence::Persisted,
        next: None,
        queue: crate::sessions::Queue::default(),
        meta: crate::event::EventMeta {
            session_id: turn.session_id,
            loop_id: Some(turn.loop_id),
            dropped_before: 0,
        },
    });
    let Some(AgentEvent::TurnFinished { meta, .. }) = events.recv().await else {
        panic!("expected the Agent completion event");
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
            let AgentEvent::TurnFinished { .. } = &event else {
                let meta = event.meta();
                if meta.dropped_before > 0 {
                    saw_drops = true;
                }
                if let AgentEvent::ToolFinished { .. } = &event {
                    saw_tool_finished = true;
                }
                continue;
            };
            break;
        }
        (saw_drops, saw_tool_finished)
    });

    let result = agent.wait_turn(turn).await.unwrap();
    assert_persisted(&result);
    assert_completed(&result);
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
    // best-effort events (including a dropped TurnFinished) must not block or
    // disturb the authoritative result.
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "first").await;
    let result = agent.wait_turn(turn).await.unwrap();
    assert_persisted(&result);

    let page = history_page(&agent, info.session_id, 100);
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
    let page = history_page(&agent, info.session_id, 100);
    assert_eq!(page.total, 6);
}
