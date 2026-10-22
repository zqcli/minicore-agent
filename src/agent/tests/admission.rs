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
async fn over_runtime_history_without_a_summary_is_compacted_before_the_turn() {
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

#[tokio::test]
async fn a_send_over_the_runtime_limits_compacts_first_then_runs_the_turn() {
    let (data_dir, _guard, session_id, model, mut agent) =
        auto_history_fixture("auto-append-only", true).await;
    let path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let original = std::fs::read(&path).unwrap();
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.budget.within_runtime_limits, Some(false));
    let turn = send_text(&mut agent, session_id, "next").await;
    // Read before the turn ends, after which a post-turn compaction reports.
    let context = agent.session_context(session_id).unwrap();
    let compaction = context.last_result.unwrap();
    assert_eq!(
        compaction.origin,
        crate::compaction::CompactionOrigin::Automatic
    );
    assert_eq!(
        compaction.status,
        crate::compaction::CompactionStatus::Compacted
    );
    assert_eq!(compaction.covered_item_count, 4_097);
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let requests = model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let turn_request = requests[1]
        .messages()
        .iter()
        .filter_map(|message| match message {
            ModelMessage::User(text) => Some(text.as_ref()),
            _ => None,
        })
        .collect::<String>();
    assert!(turn_request.contains("folded summary") && turn_request.contains("next"));
    assert!(std::fs::read(path).unwrap().starts_with(&original));
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

#[tokio::test]
async fn a_failed_compaction_before_a_turn_rejects_the_send_and_keeps_history() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|_| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("x").unwrap(),
            })
        })
        .collect();
    let failure = ModelError::permanent(
        minicore_runtime::model::ModelErrorKind::InvalidRequest,
        minicore_runtime::model::DeliveryState::NotStarted,
        minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::ModelUnavailable,
            minicore_runtime::error::DiagnosticCategory::Model,
            BoundedText::new("synthetic failure").unwrap(),
            false,
        )
        .unwrap(),
    );
    let (data_dir, _guard, session_id, model, mut agent) = auto_admission_fixture(
        "auto-disabled",
        false,
        items,
        [Step::Fail(failure)],
        None,
        None,
    )
    .await;
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
    assert_eq!(model.requests().lock().unwrap().len(), 1);
    assert_eq!(std::fs::read(&history_path).unwrap(), before);
    let compaction = agent
        .session_context(session_id)
        .unwrap()
        .last_result
        .unwrap();
    assert_eq!(
        compaction.origin,
        crate::compaction::CompactionOrigin::Automatic
    );
    assert_eq!(compaction.failure_kind.as_deref(), Some("model_failure"));
    assert!(
        !data_dir
            .join("sessions")
            .join(session_id.to_string())
            .join("summary.json")
            .exists()
    );
}
