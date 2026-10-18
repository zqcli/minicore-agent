use super::*;

#[tokio::test]
async fn running_model_update_keeps_current_request_and_reports_revision() {
    let fixture = AgentFixture::new("running-update").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model_a = ScriptedModel::new("main", [Step::gated_text(gate.clone(), "from a")])
        .with_pacing(std::time::Duration::from_millis(2));
    let model_b = ScriptedModel::new("other", [Step::Text("from b")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a)),
        ("other".to_owned(), Arc::clone(&model_b)),
    ]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;

    let updated = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_some());

    gate.wait_started().await;
    gate.release();
    let result = wait_text(&agent, turn).await;
    assert_persisted(&result);
    // The current (unblocked) request used model A.
    let requests_a = model_a.requests();
    assert_eq!(requests_a.lock().unwrap().len(), 1);
    // Next turn uses the new model.
    let turn = send_text(&mut agent, info.session_id, "next").await;
    let result = wait_text(&agent, turn).await;
    assert_persisted(&result);
    let requests_b = model_b.requests();
    assert_eq!(requests_b.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn running_model_update_applies_to_next_request_in_same_loop() {
    let fixture = AgentFixture::new("same-loop-update").file("a.txt", b"hello from a.txt");
    let data_dir = fixture.data_dir.clone();
    let gate = Arc::new(Gate::new());
    let model_a = ScriptedModel::new(
        "main",
        [Step::gated_tool(
            gate.clone(),
            "read",
            json!({"path": "a.txt", "limit": 32}),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let model_b = ScriptedModel::new("other", [Step::Text("from model b")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a)),
        ("other".to_owned(), Arc::clone(&model_b)),
    ]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "read then answer").await;

    gate.wait_started().await;

    let updated = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    let active_revision = updated
        .active_revision
        .expect("running loop must accept update");

    gate.release();
    let result = wait_text(&agent, turn).await;

    // Single turn / loop identity preserved.
    assert_eq!(result.turn.loop_id, turn.loop_id);
    assert_completed(&result);
    assert_eq!(result.report.requests, 2);
    assert_eq!(result.report.tool_rounds, 1);
    assert_eq!(result.report.final_config_revision, active_revision);
    assert_persisted(&result);

    // Both models were called exactly once in this single loop.
    assert_eq!(model_a.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model_b.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);

    // History contains:
    // [0] User message
    // [1] Assistant from Model A with read tool call (request_index = 0)
    // [2] ToolResult from read tool
    // [3] Final Assistant from Model B with text (request_index = 1)
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 4);
    assert!(matches!(&stored[0], HistoryItem::User(_)));

    let HistoryItem::Assistant(ref assistant_a) = stored[1] else {
        panic!("expected Assistant history item at index 1");
    };
    assert_eq!(assistant_a.model.as_str(), "main");
    assert_eq!(assistant_a.request_index, 0);
    let has_tool_call = assistant_a.content.iter().any(|part| {
        matches!(
            part,
            minicore_runtime::model::AssistantPart::ToolCall(call) if call.name().as_str() == "read"
        )
    });
    assert!(
        has_tool_call,
        "Request 0 must produce read tool call from Model A"
    );

    let HistoryItem::ToolResult(ref tool_result) = stored[2] else {
        panic!("expected ToolResult history item at index 2");
    };
    assert_eq!(tool_result.outcome, ToolResultOutcome::Success);

    let HistoryItem::Assistant(ref assistant_b) = stored[3] else {
        panic!("expected Assistant history item at index 3");
    };
    assert_eq!(assistant_b.model.as_str(), "other");
    assert_eq!(assistant_b.request_index, 1);
    let has_final_text = assistant_b.content.iter().any(|part| {
        matches!(
            part,
            minicore_runtime::model::AssistantPart::Text(text) if text.as_ref() == "from model b"
        )
    });
    assert!(
        has_final_text,
        "Request 1 must produce final text from Model B"
    );

    let page = history_page(&agent, info.session_id, 100);
    assert_eq!(page.total, 4);
    let crate::history::HistoryItemView::Assistant(ref final_view) = page.items[3].item else {
        panic!("expected AssistantHistoryView at index 3");
    };
    assert_eq!(final_view.model, "other");
    assert_eq!(final_view.request_index, 1);
    assert_eq!(final_view.text, "from model b");

    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn model_swap_keeps_request_identity_for_live_tool_display() {
    let fixture = AgentFixture::new("presentation-model-swap").file("a.txt", b"a");
    let workspace = fixture.workspace.clone();
    std::fs::write(workspace.join("b.txt"), b"b").unwrap();
    let gate = Arc::new(Gate::new());
    let model_a = ScriptedModel::new(
        "provider/model-a",
        [Step::gated_tool(
            gate.clone(),
            "read",
            json!({"path": "a.txt"}),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let model_b = ScriptedModel::new(
        "provider/model-b",
        [
            Step::ToolCall("read", json!({"path": "b.txt"})),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a)),
        ("other".to_owned(), Arc::clone(&model_b)),
    ]));
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read both").await;
    gate.wait_started().await;
    agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    gate.release();
    wait_text(&agent, turn).await;

    let mut presentations = Vec::new();
    while presentations.len() < 2 {
        let event = events
            .next_event(
                std::time::Duration::from_secs(1),
                "model-swap presentation event must arrive",
            )
            .await;
        if let AgentEvent::ToolPresentation {
            turn: event_turn,
            request_index,
            display,
            ..
        } = event
        {
            presentations.push((event_turn, request_index, display.detail));
        }
    }
    assert!(presentations.contains(&(turn, 0, "a.txt".to_owned())));
    assert!(presentations.contains(&(turn, 1, "b.txt".to_owned())));
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .model_label
            .as_deref(),
        Some("other")
    );
}

#[tokio::test]
async fn invalid_update_does_not_write_or_change_memory() {
    let fixture = AgentFixture::new("invalid-update").file("a.txt", b"hello");
    let model_a = ScriptedModel::new("main", [Step::Text("from a")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model_a))]));
    let (mut agent, info) = fixture.start().await;

    let error = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("missing".to_owned()),
            reasoning: None,
        })
        .await;
    assert!(matches!(error, Err(AgentError::ModelNotFound)));

    let error = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: None,
            reasoning: None,
        })
        .await;
    assert!(matches!(error, Err(AgentError::InvalidArguments)));

    // Disk record still references the original model.
    let info2 = agent.list_sessions().await.unwrap();
    assert_eq!(info2[0].model, "main");
}
