use super::*;

#[tokio::test]
async fn reopened_legacy_round_limit_allows_forty_tool_rounds() {
    let (data_dir, _data_guard) = fixture_dir(&format!("legacy-rounds-{}", next_id()));
    let (workspace, _workspace_guard) =
        workspace_file("legacy-rounds-ws", "file.txt", b"file contents");
    let session_id = SessionId::new().unwrap();
    let record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: None,
        profile: "test".to_owned(),
        workspace,
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 32,
        approval: ApprovalMode::Auto,
        created_at: "2026-01-02T03:04:05.000Z".to_owned(),
        updated_at: "2026-01-02T03:04:05.000Z".to_owned(),
    };
    let store = Store::open(data_dir.clone()).await.unwrap();
    store.create_session(&record).await.unwrap();
    assert_eq!(
        store
            .load_session(session_id)
            .await
            .unwrap()
            .record
            .max_tool_rounds,
        32
    );
    drop(store);

    let mut scripts: Vec<_> = (0..40)
        .map(|_| Step::ToolCall("read", json!({"path": "file.txt", "limit": 32})))
        .collect();
    scripts.push(Step::Text("completed after forty tool rounds"));
    let model =
        ScriptedModel::new("main", scripts).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        Profile {
            max_tool_rounds: 32,
            ..read_profile()
        },
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    assert_eq!(session_options(&agent, session_id).max_tool_rounds, 0);

    let turn = send_text(&mut agent, session_id, "read forty times then finish").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(result.report.tool_rounds, 40);
    assert_eq!(result.report.requests, 41);
    assert_eq!(model.requests().lock().unwrap().len(), 41);
    let stored = read_store_history(&data_dir, session_id).await;
    assert_eq!(
        stored
            .iter()
            .filter(|item| matches!(
                item,
                HistoryItem::ToolResult(result) if result.outcome == ToolResultOutcome::Success
            ))
            .count(),
        40
    );
    assert_eq!(
        flatten_texts(&stored).last().unwrap(),
        "completed after forty tool rounds"
    );
}

#[tokio::test]
async fn tool_loop_runs_read_and_persists_one_loop_record() {
    let (data_dir, _guard) = fixture_dir(&format!("tool-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-ws", "file.txt", b"file contents");
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            Step::Text("final answer"),
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

    let turn = send_text(&mut agent, info.session_id, "read the file").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(result.report.requests, 2);
    assert_eq!(result.report.tool_rounds, 1);

    let stored = read_store_history(&data_dir, info.session_id).await;
    // User, Assistant(tool call), ToolResult, Assistant(text)
    assert_eq!(stored.len(), 4);
    let tool_results = stored
        .iter()
        .filter_map(|item| match item {
            HistoryItem::ToolResult(result) => {
                Some((result.outcome, result.output.content().as_str().to_owned()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tool_results,
        vec![(ToolResultOutcome::Success, "1: file contents".to_owned())]
    );

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let crate::history::HistoryItemView::Assistant(assistant) = &page.items[1].item else {
        panic!("expected the first assistant history item");
    };
    assert_eq!(assistant.parts.as_ref().unwrap().len(), 1);
    let display = assistant.tool_calls[0]
        .display
        .as_ref()
        .expect("history tool display");
    assert_eq!(display.detail, "file.txt:1-32");
    assert_eq!(display.hidden_line_count, Some(1));
}

#[tokio::test]
async fn live_tool_presentation_and_result_keep_runtime_identity() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-live-{}", next_id()));
    let (workspace, _guard) = workspace_file("presentation-live-ws", "file.txt", b"file contents");
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let mut presentation = None;
    let mut tool_result = None;
    for _ in 0..32 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("live Agent event must arrive")
            .expect("event stream must remain open");
        match event {
            AgentEvent::ToolPresentation {
                turn: event_turn,
                request_index,
                tool_call_id,
                display,
                ..
            } => presentation = Some((event_turn, request_index, tool_call_id, display)),
            AgentEvent::ToolFinished {
                turn: event_turn,
                request_index,
                tool_call_id,
                result,
                ..
            } => tool_result = Some((event_turn, request_index, tool_call_id, result)),
            AgentEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }

    let (event_turn, request_index, tool_call_id, display) =
        presentation.expect("tool presentation event");
    assert_eq!(event_turn, turn);
    assert_eq!(request_index, 0);
    assert_eq!(display.detail, "file.txt:1-32");
    assert_eq!(display.hidden_line_count, Some(1));
    let (result_turn, result_request_index, result_tool_call_id, result) =
        tool_result.expect("tool result event");
    assert_eq!(result_turn, turn);
    assert_eq!(result_request_index, 0);
    assert_eq!(result_tool_call_id, tool_call_id);
    assert_eq!(result.content.as_deref(), Some("1: file contents"));
    assert!(!result.content_truncated);
}

#[tokio::test]
async fn failed_tool_presentation_exposes_safe_error_and_history_fallback() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-failed-{}", next_id()));
    let (workspace, _guard) =
        workspace_file("presentation-failed-ws", "file.txt", b"file contents");
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("read", json!({"path": "missing-secret.txt"})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read the missing file").await;
    wait_text(&agent, turn).await;

    let mut finished = None;
    while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
        .await
        .expect("failed tool events must arrive")
    {
        if let AgentEvent::ToolFinished { result, .. } = event {
            finished = Some(result);
            break;
        }
    }
    let result = finished.expect("failed tool must emit ToolFinished");
    assert_eq!(result.outcome, ToolResultOutcome::Failed);
    assert_eq!(result.content.as_deref(), Some("tool execution failed"));
    assert!(
        !result
            .content
            .as_deref()
            .unwrap_or_default()
            .contains("missing-secret.txt")
    );

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let tool_result = page
        .items
        .iter()
        .find_map(|item| match &item.item {
            crate::history::HistoryItemView::ToolResult(result) => Some(result),
            _ => None,
        })
        .expect("failed tool must be present in history");
    assert_eq!(tool_result.outcome, ToolResultOutcome::Failed);
    assert_eq!(tool_result.content, "tool failed");
    assert!(!tool_result.content.contains("missing-secret.txt"));
}

#[tokio::test]
async fn reported_context_tracks_last_request_reopen_compaction_and_model_change() {
    use crate::presentation::ContextKind;
    let (data_dir, _guard) = fixture_dir(&format!("reported-context-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("reported-context-ws", "a.txt", b"hello");
    let model = ScriptedModel::with_window(
        "provider/main",
        40_000,
        [
            Step::TextWithUsage("first", 80, 20),
            Step::TextWithUsage("second", 180, 20),
            Step::Text("short summary"),
            Step::TextWithUsage("after compact", 40, 10),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), model),
            (
                "other".to_owned(),
                ScriptedModel::new("provider/other", [])
                    .with_pacing(std::time::Duration::from_millis(2)),
            ),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let context = |agent: &Agent| agent.session_presentation(info.session_id).unwrap().context;
    assert_eq!(context(&agent).kind, ContextKind::Unknown);
    let turn = send_text(&mut agent, info.session_id, &"history ".repeat(256)).await;
    wait_text(&agent, turn).await;
    assert_eq!(context(&agent).tokens, Some(100));
    assert_eq!(context(&agent).window, Some(40_000));
    assert_eq!(context(&agent).percent, Some(0.25));
    let turn = send_text(&mut agent, info.session_id, "next").await;
    wait_text(&agent, turn).await;
    assert_eq!(context(&agent).tokens, Some(200)); // Not cumulative 300.
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(context(&agent).tokens, Some(200));
    agent.close_session(info.session_id).await.unwrap();
    let mut tail = synthetic_loop_record(
        LoopId::new().unwrap(),
        &"recent ".repeat(12_000),
        "unused",
        false,
    );
    // This retained loop has no completed model request. After compaction no
    // matching historical Usage remains to restore, preserving the test's
    // unknown-context assertion without relying on full-history compaction.
    tail.items.truncate(1);
    tail.requests = 0;
    tail.usage = Usage::default();
    agent
        .store
        .append_loop(info.session_id, &tail)
        .await
        .unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let compact = agent
        .compact(CompactSession {
            session_id: info.session_id,
            operation_id: "reported-context-compact".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        compact.status,
        crate::compaction::CompactionStatus::Compacted
    );
    assert_eq!(context(&agent).kind, ContextKind::Unknown);
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(context(&agent).kind, ContextKind::Unknown);
    let turn = send_text(&mut agent, info.session_id, "after compact").await;
    wait_text(&agent, turn).await;
    assert_eq!(context(&agent).tokens, Some(50));
    agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert_eq!(context(&agent).kind, ContextKind::Unknown);
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(context(&agent).kind, ContextKind::Unknown);
}

#[tokio::test]
async fn request_usage_events_carry_real_per_request_usage_across_two_sessions() {
    let (data_dir, _guard) = fixture_dir(&format!("usage-two-sessions-{}", next_id()));
    let (workspace_a, _guard_a) = workspace_file("usage-session-a", "a.txt", b"a");
    let (workspace_b, _guard_b) = workspace_file("usage-session-b", "b.txt", b"b");
    let model = ScriptedModel::new(
        "provider/model-id",
        [
            Step::TextWithUsage("a answer", 10, 20),
            Step::TextWithUsage("b answer", 300, 400),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let session_a = create_session(&mut agent, &workspace_a).await;
    let session_b = create_session(&mut agent, &workspace_b).await;
    let turn_a = send_text(&mut agent, session_a.session_id, "a").await;
    wait_text(&agent, turn_a).await;
    let turn_b = send_text(&mut agent, session_b.session_id, "b").await;
    wait_text(&agent, turn_b).await;

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("request_usage event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::RequestUsage {
            turn,
            request_index,
            usage,
            ..
        } = event
        {
            seen.push((
                turn,
                request_index,
                usage.input_tokens(),
                usage.output_tokens(),
            ));
        }
    }
    assert!(
        seen.contains(&(turn_a, 0, Some(10), Some(20))),
        "seen: {seen:?}"
    );
    assert!(
        seen.contains(&(turn_b, 0, Some(300), Some(400))),
        "seen: {seen:?}"
    );
}

#[tokio::test]
async fn request_usage_is_keyed_per_request_index_within_one_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("usage-requests-{}", next_id()));
    let (workspace, _guard) = workspace_file("usage-requests-ws", "a.txt", b"file contents");
    // Request 0 finishes with a tool call (its Usage is (1,1,0)); the tool
    // runs; request 1 reports a distinct usage (5,6,0). Both must surface as
    // separate RequestUsage events under the same loop with distinct indexes.
    let model = ScriptedModel::new(
        "deep",
        [
            Step::ToolCall("read", json!({"path": "a.txt", "limit": 32})),
            Step::TextWithUsage("final", 5, 6),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;

    let mut by_index = std::collections::BTreeMap::new();
    while by_index.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("request_usage event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::RequestUsage {
            turn: event_turn,
            request_index,
            usage,
            ..
        } = event
        {
            assert_eq!(event_turn, turn, "usage must stay on the same turn");
            by_index.insert(request_index, (usage.input_tokens(), usage.output_tokens()));
        }
    }
    assert_eq!(by_index.get(&0), Some(&(Some(1), Some(1))));
    assert_eq!(by_index.get(&1), Some(&(Some(5), Some(6))));
    let _ = format!("{:?}", by_index); // Debug formatting must not panic (no sensitive fields)
}

#[tokio::test]
async fn shared_model_keeps_live_presentation_identity_per_session() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-sessions-{}", next_id()));
    let (workspace_a, _guard_a) = workspace_file("presentation-session-a", "a.txt", b"a");
    let (workspace_b, _guard_b) = workspace_file("presentation-session-b", "b.txt", b"b");
    let model = ScriptedModel::new(
        "provider/model-id",
        [
            Step::ToolCall("read", json!({"path": "a.txt"})),
            Step::Text("a done"),
            Step::ToolCall("read", json!({"path": "b.txt"})),
            Step::Text("b done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let session_a = create_session(&mut agent, &workspace_a).await;
    let session_b = create_session(&mut agent, &workspace_b).await;
    let turn_a = send_text(&mut agent, session_a.session_id, "read a").await;
    wait_text(&agent, turn_a).await;
    let turn_b = send_text(&mut agent, session_b.session_id, "read b").await;
    wait_text(&agent, turn_b).await;

    let mut presentations = Vec::new();
    while presentations.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("presentation event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::ToolPresentation {
            turn,
            request_index,
            display,
            ..
        } = event
        {
            presentations.push((turn, request_index, display.detail));
        }
    }
    assert!(presentations.contains(&(turn_a, 0, "a.txt".to_owned())));
    assert!(presentations.contains(&(turn_b, 0, "b.txt".to_owned())));
    assert_eq!(
        agent
            .session_presentation(session_a.session_id)
            .unwrap()
            .model_label
            .as_deref(),
        Some("main")
    );
}

#[tokio::test]
async fn project_prompt_reloads_agents_between_requests_in_one_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("dynamic-agents-{}", next_id()));
    let (workspace, _guard) = workspace_file("dynamic-agents-ws", "AGENTS.md", b"before\n");
    std::fs::write(workspace.join("a.txt"), b"file contents").unwrap();
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::new(
        "main",
        [
            Step::gated_tool(gate.clone(), "read", json!({"path": "a.txt", "limit": 32})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;

    gate.wait_started().await;
    std::fs::write(workspace.join("AGENTS.md"), b"after\n").unwrap();
    gate.release();
    wait_text(&agent, turn).await;

    let requests = model.requests();
    let requests = requests.lock().unwrap();
    let systems = requests
        .iter()
        .map(|request| {
            request
                .messages()
                .iter()
                .find_map(|message| match message {
                    ModelMessage::System(text) => Some(text.clone()),
                    _ => None,
                })
                .expect("each request has a system message")
        })
        .collect::<Vec<_>>();
    assert_eq!(systems.len(), 2);
    assert!(systems[0].contains("before"));
    assert!(!systems[0].contains("after"));
    assert!(systems[1].contains("after"));
    assert!(!systems[1].contains("before"));
}

#[tokio::test]
async fn multi_turn_creates_new_loop_each_time_and_history_grows() {
    let (data_dir, _guard) = fixture_dir(&format!("multi-{}", next_id()));
    let (workspace, _guard) = workspace_file("multi-ws", "a.txt", b"hello");
    let model = ScriptedModel::new(
        "main",
        [Step::Text("one"), Step::Text("two"), Step::Text("three")],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let mut loop_ids = Vec::new();
    for text in ["one", "two", "three"] {
        let turn = send_text(&mut agent, info.session_id, text).await;
        loop_ids.push(turn.loop_id);
        let result = wait_text(&agent, turn).await;
        assert_eq!(
            result.persistence,
            crate::sessions::TurnPersistence::Persisted
        );
    }
    assert_ne!(loop_ids[0], loop_ids[1]);
    assert_ne!(loop_ids[1], loop_ids[2]);

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    // Three user prompts + three assistant texts.
    assert_eq!(page.total, 6);
    assert!(page.next_offset.is_none());
}
