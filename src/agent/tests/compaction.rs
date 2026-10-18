use super::*;

pub(super) fn pi_history_exchange(loop_id: LoopId, index: u32, bytes: usize) -> Vec<HistoryItem> {
    let call_id = ToolCallId::new(format!("pi-read-{index}")).unwrap();
    vec![
        HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: index,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::ToolCall(
                ToolCall::new(
                    call_id.clone(),
                    "read".parse().unwrap(),
                    json!({"path":"old.txt"}),
                    0,
                )
                .unwrap(),
            )],
            provider_replay: None,
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default(),
        }),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index: index,
            call_id,
            tool_name: "read".parse().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("old ".repeat(bytes / 4)).unwrap(),
        }),
    ]
}

#[tokio::test]
async fn pi_in_run_consolidates_older_exchange_once_and_keeps_newest_and_steer() {
    let (data_dir, _guard) = fixture_dir("pi-in-run");
    let content = "read-result ".repeat(900);
    let (workspace, _workspace_guard) =
        workspace_file("pi-in-run-workspace", "a.txt", content.as_bytes());
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::with_window(
        "main",
        5_000,
        [
            Step::gated_tool(gate.clone(), "read", json!({"path":"a.txt"})),
            Step::ToolCall("read", json!({"path":"a.txt"})),
            Step::Text("First read was completed; keep its findings."),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".into(), Arc::clone(&model))]))
        .auto()
        .open()
        .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(
        &mut agent,
        info.session_id,
        "read twice; preserve original input",
    )
    .await;
    gate.wait_started().await;
    agent
        .steer(super::SteerMessage {
            turn,
            text: "preserve this steer exactly".into(),
        })
        .unwrap();
    gate.release();
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
    assert_eq!(
        result.report.requests, 3,
        "utility is not an ordinary request"
    );
    wait_post_turn_noop(&agent, info.session_id, turn.loop_id).await;
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(
            requests[2].tools().is_empty(),
            "summary precedes the third assistant request"
        );
        assert!(!requests[3].tools().is_empty());
        for value in [
            "read twice; preserve original input",
            "preserve this steer exactly",
        ] {
            assert!(requests[3].messages().iter().any(
                |message| matches!(message, ModelMessage::User(text) if text.as_ref() == value)
            ));
        }
        assert_eq!(
            requests[3]
                .messages()
                .iter()
                .filter(|message| matches!(message, ModelMessage::Tool { .. }))
                .count(),
            1
        );
        assert!(requests[3].messages().iter().any(|message| matches!(message, ModelMessage::Tool { output, .. } if output.content().as_str().contains(&content))));
    }
    let context = agent.session_context(info.session_id).unwrap();
    assert!(context.automatic.current.is_none());
    assert_eq!(
        context
            .automatic
            .last
            .unwrap()
            .utility_usage
            .unwrap()
            .call_count,
        1
    );
    let history = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(
        history
            .iter()
            .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
            .count(),
        2,
        "completed tools are never replayed"
    );
}

#[tokio::test]
async fn pi_first_boundary_is_accepted_cancellable_and_preserves_raw_input() {
    let gate = Arc::new(Gate::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let (_data, _guard, session_id, model, mut agent) = auto_admission_fixture_with_window(
        "pi-first-cancel",
        true,
        8_000,
        pi_history_exchange(LoopId::new().unwrap(), 0, 40_000),
        [Step::BlockUntilDrop(
            gate.clone(),
            Arc::clone(&dropped),
            "not reached",
        )],
        None,
        None,
    )
    .await;
    let turn = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        send_text(&mut agent, session_id, "new current input remains exact"),
    )
    .await
    .unwrap();
    gate.wait_started().await;
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.automatic.current.unwrap().request_index, Some(0));
    assert!(context.current_operation.is_none());
    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(_)
    ));
    assert_eq!(
        result.report.requests, 0,
        "no ordinary model request was issued"
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(result.report.appended.iter().any(|item| matches!(item, HistoryItem::User(user) if user.input.as_text() == "new current input remains exact")));
    let view = agent.session_context(session_id).unwrap().automatic;
    assert!(view.current.is_none());
    assert_eq!(view.last.unwrap().utility_usage.unwrap().call_count, 1);
}

#[tokio::test]
async fn pi_reopen_smaller_model_refreshes_fully_covered_summary_then_sends_new_input() {
    let large_summary: &str = Box::leak("old summary ".repeat(3_000).into_boxed_str());
    let (data, _guard, session_id, _model, mut agent) = auto_admission_fixture_with_window(
        "pi-summary-reopen",
        true,
        20_000,
        pi_history_exchange(LoopId::new().unwrap(), 0, 70_000),
        [Step::Text(large_summary)],
        None,
        None,
    )
    .await;
    // Seed an old-format, fully covered v1 snapshot directly. Ordinary
    // compact now retains a recent tail; reopening legacy full coverage must
    // still exercise the original threshold refresh behavior.
    let history = read_store_history(&data, session_id).await;
    let source = agent
        .store
        .capture_history_anchor(session_id, &history)
        .await
        .unwrap()
        .unwrap();
    let record = agent.loaded_session(session_id).unwrap().record();
    let bytes = crate::compaction::encode_snapshot(
        session_id,
        &record,
        &source,
        &BoundedText::new(large_summary).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        agent
            .store
            .commit_summary(
                session_id,
                &source,
                &history,
                &bytes,
                std::time::Instant::now() + std::time::Duration::from_secs(10),
                || true
            )
            .await
            .unwrap(),
        crate::store::SummaryCommit::Committed
    ));
    agent.close_session(session_id).await.unwrap();
    drop(agent);
    let smaller = ScriptedModel::with_window(
        "main",
        10_000,
        [
            Step::Text("Refreshed historical facts."),
            Step::Text("answer"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data)
        .models(BTreeMap::from([("main".into(), Arc::clone(&smaller))]))
        .auto()
        .open()
        .await;
    agent.open_session(session_id).await.unwrap();
    let turn = send_text(&mut agent, session_id, "new after reopening exact").await;
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
    wait_post_turn_noop(&agent, session_id, turn.loop_id).await;
    {
        let requests = smaller.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].tools().is_empty());
        assert!(requests[1].messages().iter().any(
        |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "new after reopening exact")
    ));
        assert!(
            serde_json::to_string(requests[1].messages())
                .unwrap()
                .contains("Refreshed historical facts.")
        );
        assert!(
            !serde_json::to_string(requests[1].messages())
                .unwrap()
                .contains("old summary old summary")
        );
    }
    agent.close_session(session_id).await.unwrap();
    agent.open_session(session_id).await.unwrap();
    assert!(
        agent
            .session_context(session_id)
            .unwrap()
            .coverage
            .covered_item_count
            > 0
    );
}

#[tokio::test]
async fn pi_steer_and_model_settings_during_utility_rebuild_without_second_summary() {
    let gate = Arc::new(Gate::new());
    let (_data, _guard, session_id, model, mut agent) = auto_admission_fixture_with_window(
        "pi-stale-prepare",
        true,
        8_000,
        pi_history_exchange(LoopId::new().unwrap(), 0, 40_000),
        [
            Step::gated_text(gate.clone(), "historical work summary"),
            Step::Text("new settings answer"),
        ],
        None,
        None,
    )
    .await;
    let turn = send_text(&mut agent, session_id, "original prompt exact").await;
    gate.wait_started().await;
    agent
        .steer(super::SteerMessage {
            turn,
            text: "steer while summary blocked".into(),
        })
        .unwrap();
    agent
        .update_session(UpdateSession {
            session_id,
            model: None,
            reasoning: Some(ReasoningPreference::Low),
        })
        .await
        .unwrap();
    gate.release();
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
    assert_eq!(result.report.requests, 1);
    wait_post_turn_noop(&agent, session_id, turn.loop_id).await;
    let requests = model.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "one utility plus one current-settings ordinary request"
    );
    assert!(requests[0].tools().is_empty());
    assert_eq!(requests[1].reasoning(), ReasoningPreference::Low);
    for expected in ["original prompt exact", "steer while summary blocked"] {
        assert!(requests[1].messages().iter().any(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == expected)
        ));
    }
    let contexts = model.contexts.lock().unwrap();
    assert_eq!(contexts[1].request_index, 0);
    assert!(
        agent
            .session_context(session_id)
            .unwrap()
            .automatic
            .current
            .is_none()
    );
}

#[tokio::test]
async fn pi_consolidated_overflow_and_transport_retry_use_same_reduced_body_without_tools_replay() {
    let (data, _guard) = fixture_dir("pi-consolidated-retry");
    let (workspace, _workspace_guard) = workspace_file(
        "pi-consolidated-retry-ws",
        "a.txt",
        "read-result ".repeat(900).as_bytes(),
    );
    let first_summary = Box::leak("prior read facts retained. ".repeat(90).into_boxed_str());
    let retryable = ModelError::not_started(
        minicore_runtime::model::ModelErrorKind::RateLimited,
        Some(std::time::Duration::from_millis(1)),
        minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::ModelUnavailable,
            minicore_runtime::error::DiagnosticCategory::Model,
            BoundedText::new("retry shortly").unwrap(),
            true,
        )
        .unwrap(),
    );
    let model = ScriptedModel::with_window(
        "main",
        5_000,
        [
            Step::ToolCall("read", json!({"path":"a.txt"})),
            Step::ToolCall("read", json!({"path":"a.txt"})),
            Step::Text(first_summary),
            Step::ContextOverflowNotStarted,
            Step::Text("Recovered historical facts"),
            Step::Fail(retryable),
            Step::Text("completed"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data)
        .models(BTreeMap::from([("main".into(), Arc::clone(&model))]))
        .auto()
        .open()
        .await;
    let session_id = create_session(&mut agent, &workspace).await.session_id;
    let turn = send_text(&mut agent, session_id, "new user exact").await;
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
    assert_eq!(result.report.requests, 3);
    assert_eq!(result.report.tool_rounds, 2);
    wait_post_turn_noop(&agent, session_id, turn.loop_id).await;
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        assert!(requests[2].tools().is_empty() && requests[4].tools().is_empty());
        assert_eq!(
            requests[3]
                .messages()
                .iter()
                .filter(|message| matches!(message, ModelMessage::Tool { .. }))
                .count(),
            1,
            "threshold keeps the newest exchange before the real capacity rejection"
        );
        assert_eq!(
            serde_json::to_vec(&requests[5]).unwrap(),
            serde_json::to_vec(&requests[6]).unwrap()
        );
        assert!(
            serde_json::to_string(&requests[6])
                .unwrap()
                .contains("Recovered historical facts")
        );
        assert!(
            !requests[6]
                .messages()
                .iter()
                .any(|message| matches!(message, ModelMessage::Tool { .. }))
        );
        assert!(requests[6].messages().iter().any(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "new user exact")
        ));
    }
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(
        context
            .automatic
            .last
            .unwrap()
            .utility_usage
            .unwrap()
            .call_count,
        1
    );
    assert_eq!(
        context.recovery.unwrap().utility_usage.unwrap().call_count,
        1
    );
    assert_eq!(
        read_store_history(&data, session_id)
            .await
            .iter()
            .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
            .count(),
        2
    );
}
