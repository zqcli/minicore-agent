use super::*;

#[tokio::test]
async fn valid_summary_allows_history_over_runtime_item_limit_to_start() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|index| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(format!("old-{index}"))
                    .unwrap(),
            })
        })
        .collect();
    assert_runtime_limit_behavior(items, true).await;
}

#[tokio::test]
async fn valid_summary_allows_history_over_runtime_byte_limit_to_start() {
    let loop_id = LoopId::new().unwrap();
    let limits = minicore_runtime::LoopOptions::default_checked()
        .unwrap()
        .limits;
    let chunk_len = limits.max_model_text_bytes / 2;
    let item_count = limits.max_history_bytes / chunk_len + 1;
    let items = (0..item_count)
        .map(|index| {
            HistoryItem::Assistant(AssistantHistory {
                provider_replay: None,
                loop_id,
                request_index: u32::try_from(index).unwrap(),
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::Text("x".repeat(chunk_len).into())],
                finish_reason: ModelFinishReason::Stop,
                usage: Usage::default(),
            })
        })
        .collect();
    assert_runtime_limit_behavior(items, true).await;
}

#[tokio::test]
async fn over_runtime_history_without_valid_summary_is_rejected() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|index| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(format!("old-{index}"))
                    .unwrap(),
            })
        })
        .collect();
    assert_runtime_limit_behavior(items, false).await;
}

/// Builds a stored session whose history exceeds the runtime item limit, then
/// opens it with automatic compaction enabled or disabled.
#[tokio::test]
async fn automatic_submit_rejects_runtime_overlimit_without_startup_summary() {
    let (data_dir, _guard, session_id, model, mut agent) =
        auto_history_fixture(&format!("auto-append-only-{}", next_id()), true).await;
    let path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let original = std::fs::read(&path).unwrap();
    let context = agent.session_context(session_id).unwrap();
    assert!(context.current_operation.is_none());
    assert!(context.automatic.current.is_none());
    assert!(context.automatic.last.is_none());
    assert!(matches!(
        agent
            .send(SendMessage {
                session_id,
                text: "next".to_owned()
            })
            .await,
        Err(AgentError::HistoryTooLarge)
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(path).unwrap(), original);
}

#[tokio::test]
async fn newest_exchange_reaches_assistant_verbatim_even_above_threshold() {
    let (data_dir, _guard) = fixture_dir(&format!("append-only-steer-{}", next_id()));
    let content = "ordinary result ".repeat(900);
    let (workspace, _workspace_guard) =
        workspace_file("append-only-steer-ws", "a.txt", content.as_bytes());
    let gate = BlockGate::new();
    let model = FakeModel::with_window(
        "main",
        4_000,
        [
            ModelScript::ToolCallAfterGate(gate.clone(), "read", json!({"path": "a.txt"})),
            ModelScript::Text("answer"),
            ModelScript::Text("completed history summary"),
        ],
    );
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read then steer").await;
    gate.entered.notified().await;
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
    gate.release.notify_waiters();
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

#[tokio::test]
async fn disabled_auto_keeps_over_runtime_history_rejected() {
    let (data_dir, _guard, session_id, model, mut agent) =
        auto_history_fixture(&format!("auto-disabled-{}", next_id()), false).await;
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.budget.input_budget_tokens, None);
    assert_eq!(context.budget.trigger_tokens, None);
    assert_eq!(context.budget.target_tokens, None);
    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let before = std::fs::read(&history_path).unwrap();
    assert!(matches!(
        agent
            .send(super::SendMessage {
                session_id,
                text: "should be rejected".to_owned(),
            })
            .await,
        Err(AgentError::HistoryTooLarge)
    ));
    assert_eq!(model.requests().lock().unwrap().len(), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), before);
    assert!(
        !data_dir
            .join("sessions")
            .join(session_id.to_string())
            .join("summary.json")
            .exists()
    );
}
