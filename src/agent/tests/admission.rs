use super::*;

#[tokio::test]
async fn a_history_over_the_old_byte_limit_is_neither_compacted_nor_refused() {
    // 26 loops of about 100 KB: over 2 MiB counted by `estimate_history_bytes`,
    // but about 650,000 tokens against a trigger of 950,000.
    let items = (0..26)
        .flat_map(|_| {
            let mut record = synthetic_tool_loop_record(LoopId::new().unwrap());
            if let HistoryItem::ToolResult(result) = &mut record.items[2] {
                result.output = ToolOutput::new("x".repeat(100_000)).unwrap();
            }
            record.items
        })
        .collect::<Vec<_>>();
    assert!(minicore_runtime::history::estimate_history_bytes(&items) > 2 * 1024 * 1024);
    let (data_dir, _guard, session_id, model, mut agent) = auto_admission_fixture(
        "old-byte-limit",
        true,
        items,
        [Step::Text("answer")],
        None,
        None,
    )
    .await;
    let turn = send_text(&mut agent, session_id, "next").await;
    assert!(
        agent
            .session_context(session_id)
            .unwrap()
            .current_operation
            .is_none()
    );
    assert_completed(&*wait_text(&agent, turn).await);
    settle_turn(&agent, session_id).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(
        !data_dir
            .join("sessions")
            .join(session_id.to_string())
            .join("summary.json")
            .exists()
    );
}

#[tokio::test]
async fn newest_exchange_reaches_assistant_verbatim_even_above_threshold() {
    let (data_dir, _guard) = fixture_dir("append-only-steer");
    let content = "ordinary result ".repeat(900);
    let (workspace, _workspace_guard) =
        workspace_file("append-only-steer-ws", "a.txt", content.as_bytes());
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::with_window(
        "main",
        4_000,
        [
            Step::gated_tool(gate.clone(), "read", json!({"path": "a.txt"})),
            Step::Text("answer"),
            Step::Text("completed history summary"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .auto()
        .open()
        .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read then steer").await;
    gate.wait_started().await;
    agent
        .steer(super::SteerMessage {
            turn,
            text: "keep the answer concise".to_owned(),
        })
        .unwrap();
    assert!(
        agent
            .session_context(info.session_id)
            .unwrap()
            .current_operation
            .is_none()
    );
    gate.release();
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    agent
        .loaded_session(info.session_id)
        .unwrap()
        .cleanup_finished()
        .await
        .ok();
    agent.close_session(info.session_id).await.unwrap();
    let captured = model.requests();
    {
        let requests = captured.lock().unwrap();
        assert!(!requests[0].tools().is_empty());
        assert!(
            !requests[1].tools().is_empty(),
            "newest tool result must first reach the assistant verbatim"
        );
        assert!(
            requests[1]
                .messages()
                .iter()
                .any(|message| matches!(message,
        ModelMessage::Tool { output, .. } if output.content().as_str().contains(&content)))
        );
        assert!(
            requests[1]
                .messages()
                .iter()
                .any(|message| matches!(message,
        ModelMessage::User(text) if text.as_ref() == "keep the answer concise"))
        );
        assert!(
            !requests[1]
                .messages()
                .iter()
                .any(|message| matches!(message,
        ModelMessage::User(text) if text.contains("HISTORICAL SUMMARY DATA")))
        );
    }
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(
        stored
            .iter()
            .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
            .count(),
        1
    );
}
