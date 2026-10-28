use super::*;

fn history(tokens: Option<u64>) -> Vec<HistoryItem> {
    (0..3)
        .flat_map(|_| {
            let mut exchange = pi_history_exchange(LoopId::new().unwrap(), 0, 20_000);
            if let HistoryItem::Assistant(assistant) = &mut exchange[0] {
                assistant.usage = Usage::default().with_provider_total_tokens(tokens);
            }
            exchange
        })
        .collect()
}

fn reported(agent: &Agent, id: SessionId, history: &[HistoryItem]) -> Option<u64> {
    let state = agent.loaded_session(id).unwrap().compaction_state();
    let projection = state.project(history);
    let suffix = projection.as_ref().map_or(history, |value| value.suffix);
    state.reported_context(suffix.iter())
}

#[tokio::test]
async fn reopened_first_boundary_uses_only_latest_report_even_with_large_tool_results() {
    for tokens in [Some(100), Some(6_400), None] {
        let mut items = history(Some(20_000));
        if let HistoryItem::Assistant(assistant) = &mut items[4] {
            assistant.usage = Usage::default().with_provider_total_tokens(tokens);
        }
        let should_compact = tokens == Some(6_400);
        let scripts = if should_compact {
            vec![Step::Text("summary"), Step::TextWithUsage("done", 100, 1)]
        } else {
            vec![Step::TextWithUsage("done", 100, 1)]
        };
        let (data, _guard, id, model, mut agent) = auto_admission_fixture_with_window(
            "reported-reopen",
            true,
            8_000,
            items,
            scripts,
            None,
            None,
        )
        .await;
        agent.close_session(id).await.unwrap();
        agent.open_session(id).await.unwrap();
        let context = agent.session_context(id).unwrap();
        assert!(context.budget.estimated_history_bytes / 4 > 6_400);
        assert_eq!(
            reported(&agent, id, &read_store_history(&data, id).await),
            tokens
        );
        let turn = send_text(&mut agent, id, "continue").await;
        assert_completed(&*wait_text(&agent, turn).await);
        wait_post_turn_noop(&agent, id, turn.loop_id).await;
        assert_eq!(model.calls(), if should_compact { 2 } else { 1 });
        let context = agent.session_context(id).unwrap();
        assert_eq!(context.automatic.last.is_some(), should_compact);
        if should_compact {
            assert_eq!(context.automatic.last.unwrap().before_tokens, Some(6_400));
        } else {
            assert!(
                !data
                    .join("sessions")
                    .join(id.to_string())
                    .join("summary.json")
                    .exists()
            );
            assert_eq!(
                model.requests.lock().unwrap()[0]
                    .messages()
                    .iter()
                    .filter(|item| matches!(item, ModelMessage::Tool { .. }))
                    .count(),
                3
            );
        }
        agent.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn manual_post_turn_and_boundary_compaction_supersede_usage_across_reopen() {
    for origin in ["manual", "post-turn", "boundary"] {
        let gate = Arc::new(Gate::new());
        let scripts = match origin {
            "manual" => vec![Step::Text("summary"), Step::TextWithUsage("fresh", 100, 1)],
            "post-turn" => vec![
                Step::TextWithUsage("answer", 6_399, 1),
                Step::Text("summary"),
                Step::TextWithUsage("fresh", 100, 1),
            ],
            _ => vec![
                Step::Text("summary"),
                Step::gated_text(Arc::clone(&gate), "not returned"),
                Step::TextWithUsage("fresh", 100, 1),
            ],
        };
        let (data, _guard, id, model, mut agent) = auto_admission_fixture_with_window(
            origin,
            true,
            8_000,
            history(Some(if origin == "post-turn" { 100 } else { 6_400 })),
            scripts,
            None,
            None,
        )
        .await;
        if origin == "manual" {
            let result = agent
                .compact(CompactSession {
                    session_id: id,
                    operation_id: "manual-test".into(),
                })
                .await
                .unwrap();
            assert_eq!(
                result.status,
                crate::compaction::CompactionStatus::Compacted
            );
        } else {
            let turn = send_text(&mut agent, id, "continue").await;
            if origin == "boundary" {
                gate.wait_started().await;
                assert_eq!(
                    agent
                        .session_context(id)
                        .unwrap()
                        .automatic
                        .last
                        .unwrap()
                        .outcome,
                    "compacted"
                );
                assert!(agent.cancel(turn).unwrap());
                wait_text(&agent, turn).await;
            } else {
                assert_completed(&*wait_text(&agent, turn).await);
                let result =
                    super::post_turn::wait_post_turn_result(&agent, id, turn.loop_id).await;
                assert_eq!(
                    result.status,
                    crate::compaction::CompactionStatus::Compacted
                );
            }
        }
        agent.close_session(id).await.unwrap();
        let snapshot: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                data.join("sessions")
                    .join(id.to_string())
                    .join("summary.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(snapshot["superseded_usage"].is_array());
        agent.open_session(id).await.unwrap();
        assert_eq!(
            reported(&agent, id, &read_store_history(&data, id).await),
            None,
            "{origin}"
        );
        let calls = model.calls();
        let turn = send_text(&mut agent, id, "fresh response").await;
        assert_completed(&*wait_text(&agent, turn).await);
        wait_post_turn_noop(&agent, id, turn.loop_id).await;
        assert_eq!(model.calls(), calls + 1, "{origin}");
        assert!(agent.session_context(id).unwrap().automatic.last.is_none());
        assert_eq!(
            reported(&agent, id, &read_store_history(&data, id).await),
            Some(101)
        );
        agent.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn legacy_summary_uses_source_coverage_and_invalid_supersession_is_rejected_on_load() {
    let (data, _guard, id, _model, mut agent) = auto_admission_fixture_with_window(
        "legacy-reported",
        true,
        8_000,
        history(Some(6_400)),
        [Step::Text("summary")],
        None,
        None,
    )
    .await;
    let result = agent
        .compact(CompactSession {
            session_id: id,
            operation_id: "legacy-source".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        result.status,
        crate::compaction::CompactionStatus::Compacted
    );
    assert!(result.retained_item_count > 0);
    agent.close_session(id).await.unwrap();
    let path = data
        .join("sessions")
        .join(id.to_string())
        .join("summary.json");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot.as_object_mut().unwrap().remove("superseded_usage");
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    agent.open_session(id).await.unwrap();
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .coverage
            .covered_item_count,
        result.covered_item_count
    );
    assert_eq!(
        reported(&agent, id, &read_store_history(&data, id).await),
        Some(6_400)
    );
    agent.close_session(id).await.unwrap();
    snapshot["superseded_usage"] = json!([LoopId::new().unwrap(), 0]);
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    agent.open_session(id).await.unwrap();
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .coverage
            .covered_item_count,
        0
    );
    assert_eq!(
        reported(&agent, id, &read_store_history(&data, id).await),
        Some(6_400)
    );
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn switching_model_keeps_the_report_from_the_previous_model() {
    let (data, _guard, id, main, mut agent) = auto_admission_fixture_with_window(
        "reported-model-update",
        true,
        40_000,
        history(Some(6_400)),
        [],
        None,
        None,
    )
    .await;
    let smaller = ScriptedModel::with_window(
        "other",
        8_000,
        [Step::Text("summary"), Step::TextWithUsage("done", 100, 1)],
    );
    let mut config = agent.config.clone();
    config
        .models
        .insert("other".into(), model_config("MINICORE_AGENT_TEST_KEY"));
    agent
        .reload_settings_with_models(
            config,
            Models::from_values(BTreeMap::from([
                ("main".into(), main as Arc<dyn Model>),
                ("other".into(), smaller.clone() as Arc<dyn Model>),
            ])),
        )
        .unwrap();
    agent
        .update_session(UpdateSession {
            session_id: id,
            model: Some("other".into()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert_eq!(
        reported(&agent, id, &read_store_history(&data, id).await),
        Some(6_400)
    );
    assert_eq!(
        agent.session_context(id).unwrap().budget.trigger_tokens,
        Some(6_400)
    );
    let turn = send_text(&mut agent, id, "continue").await;
    assert_completed(&*wait_text(&agent, turn).await);
    wait_post_turn_noop(&agent, id, turn.loop_id).await;
    assert_eq!(smaller.calls(), 2);
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .automatic
            .last
            .unwrap()
            .before_tokens,
        Some(6_400)
    );
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn model_and_provider_reload_change_only_the_trigger_denominator() {
    let (_data, _guard, id, _model, mut agent) = auto_admission_fixture_with_window(
        "reported-model-switch",
        true,
        40_000,
        history(Some(6_400)),
        [],
        None,
        None,
    )
    .await;
    let smaller = ScriptedModel::with_window(
        "main",
        8_000,
        [Step::Text("summary"), Step::TextWithUsage("done", 100, 1)],
    );
    agent
        .reload_settings_with_models(
            agent.config.clone(),
            Models::from_values(BTreeMap::from([(
                "main".into(),
                smaller.clone() as Arc<dyn Model>,
            )])),
        )
        .unwrap();
    assert_eq!(
        agent.session_context(id).unwrap().budget.trigger_tokens,
        Some(6_400)
    );
    let turn = send_text(&mut agent, id, "continue").await;
    assert_completed(&*wait_text(&agent, turn).await);
    wait_post_turn_noop(&agent, id, turn.loop_id).await;
    assert_eq!(smaller.calls(), 2);
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .automatic
            .last
            .unwrap()
            .before_tokens,
        Some(6_400)
    );
    agent.shutdown().await.unwrap();
}
