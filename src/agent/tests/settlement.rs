use super::*;

fn settled_projection_model_error() -> ModelError {
    ModelError::permanent(
        minicore_runtime::model::ModelErrorKind::InvalidRequest,
        minicore_runtime::model::DeliveryState::NotStarted,
        minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::ModelUnavailable,
            minicore_runtime::error::DiagnosticCategory::Model,
            BoundedText::new("synthetic failure").unwrap(),
            false,
        )
        .unwrap(),
    )
}

async fn settled_projection_fixture(
    label: &str,
    window: u64,
    terminal: Step,
) -> (PathBuf, TempDir, SessionId, Arc<ScriptedModel>, Agent) {
    let old = LoopId::new().unwrap();
    let fixture = auto_admission_fixture_with_window(
        label,
        true,
        window,
        (0..3)
            .flat_map(|index| pi_history_exchange(old, index, 100_000))
            .collect(),
        [
            Step::Text("FOLDED-OLD-SOURCE"),
            Step::ToolCall("read", json!({"path":"tail.txt"})),
            terminal,
        ],
        None,
        None,
    )
    .await;
    let (data, guard, id, model, mut agent) = fixture;
    std::fs::write(
        data.join("workspace/tail.txt"),
        "EXACT-UNREAD-TAIL ".repeat(6_000),
    )
    .unwrap();
    let mut config = agent.config.clone();
    config.compaction = CompactionConfig::default();
    agent
        .reload_settings_with_models(config, agent.models.clone())
        .unwrap();
    (data, guard, id, model, agent)
}

async fn complete_display_summary(agent: &Agent, id: SessionId) -> String {
    let mut request = display_read(id);
    request.limit = 1;
    request.max_bytes = Some(4096);
    let mut body = String::new();
    loop {
        let page = agent.read_session(request.clone()).await.unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= 4096);
        assert_eq!(page.items.len(), 1);
        body.push_str(&page.items[0].data);
        if page.items[0].complete {
            break;
        }
        continue_display_read(&mut request, &page);
    }
    serde_json::from_str::<serde_json::Value>(&body).unwrap()["item"]["data"]["content"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn settled_projection_above_64k_below_trigger_persists_and_cold_reads_without_extra_utility()
{
    let (data, _guard, id, model, mut agent) =
        settled_projection_fixture("wide-projection-cold", 50_000, Step::Text("done")).await;
    let turn = send_text(&mut agent, id, "EXACT-CURRENT-USER").await;
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
    wait_post_turn_noop(&agent, id, turn.loop_id).await;
    let history = read_store_history(&data, id).await;
    let context = agent.session_context(id).unwrap();
    assert_eq!(context.coverage.covered_item_count, history.len());
    assert_eq!(context.coverage.covered_loop_count, 2);
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        3,
        "below95% must not add a summary call"
    );
    let path = data
        .join("sessions")
        .join(id.to_string())
        .join("summary.json");
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() <= crate::store::MAX_SUMMARY_FILE_BYTES);
    let snapshot: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let content = snapshot["summary"].as_str().unwrap();
    assert!(content.len() > 64 * 1024);
    assert!(content.contains("FOLDED-OLD-SOURCE"));
    assert!(content.contains("EXACT-CURRENT-USER"));
    assert!(content.contains(&"EXACT-UNREAD-TAIL ".repeat(6_000)));
    assert!(!content.contains("old old old"));
    assert_eq!(complete_display_summary(&agent, id).await, content);
    agent.close_session(id).await.unwrap();
    assert_eq!(complete_display_summary(&agent, id).await, content);
    assert!(agent.loaded_session(id).is_none());
    agent.open_session(id).await.unwrap();
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .coverage
            .covered_item_count,
        history.len()
    );
    assert_eq!(complete_display_summary(&agent, id).await, content);
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    let next = send_text(&mut agent, id, "resume").await;
    let resumed = wait_text(&agent, next).await;
    assert_completed(&resumed);
    assert_eq!(resumed.report.requests, 1);
    wait_post_turn_noop(&agent, id, next.loop_id).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);
    let requests = model.requests.lock().unwrap();
    assert!(
        !requests[3]
            .messages()
            .iter()
            .any(|message| matches!(message, ModelMessage::Tool { .. })),
        "saved projection is historical User data, not expanded tool messages"
    );
    assert!(
        requests[3]
            .messages()
            .iter()
            .any(|message| matches!(message, ModelMessage::User(text) if text.contains(content)))
    );
}

#[tokio::test]
async fn settled_projection_failed_and_cancelled_persisted_turns_keep_large_projection() {
    for cancelled in [false, true] {
        let gate = Arc::new(Gate::new());
        let terminal = if cancelled {
            Step::gated_text(gate.clone(), "not reached")
        } else {
            Step::Fail(settled_projection_model_error())
        };
        let (data, _guard, id, model, mut agent) = settled_projection_fixture(
            &format!("wide-projection-terminal-{cancelled}"),
            50_000,
            terminal,
        )
        .await;
        let turn = send_text(&mut agent, id, "EXACT-TERMINAL-USER").await;
        if cancelled {
            gate.wait_started().await;
            assert!(agent.cancel(turn).unwrap());
        }
        let result = wait_text(&agent, turn).await;
        assert_persisted(&result);
        agent.join_turn_task_for_test(id).await;
        if cancelled {
            assert!(matches!(
                result.report.outcome,
                minicore_runtime::LoopOutcome::Cancelled(_)
            ));
        } else {
            assert!(matches!(
                result.report.outcome,
                minicore_runtime::LoopOutcome::Failed(_)
            ));
        }
        assert!(agent.session_context(id).unwrap().last_result.is_none());
        assert_eq!(model.calls.load(Ordering::SeqCst), 3);
        let history = read_store_history(&data, id).await;
        assert_eq!(
            agent
                .session_context(id)
                .unwrap()
                .coverage
                .covered_item_count,
            history.len()
        );
        agent.close_session(id).await.unwrap();
        let cold = complete_display_summary(&agent, id).await;
        assert!(cold.len() > 64 * 1024 && cold.contains("EXACT-TERMINAL-USER"));
        agent.open_session(id).await.unwrap();
        assert_eq!(
            agent
                .session_context(id)
                .unwrap()
                .coverage
                .covered_item_count,
            history.len()
        );
    }
}

#[tokio::test]
async fn settled_projection_post_turn_summarizes_promoted_source_and_keeps_it_on_utility_failure() {
    for fail in [false, true] {
        let (data, _guard, id, model, mut agent) = settled_projection_fixture(
            &format!("wide-projection-post-{fail}"),
            28_500,
            Step::Text("done"),
        )
        .await;
        model.scripts.lock().unwrap().push_back(if fail {
            Step::Fail(settled_projection_model_error())
        } else {
            Step::Text("FINAL-SEMANTIC-SUMMARY")
        });
        let turn = send_text(&mut agent, id, "current user").await;
        let result = wait_text(&agent, turn).await;
        assert_completed(&result);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let context = agent.session_context(id).unwrap();
                if context.current_operation.is_none() && context.last_result.is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let context = agent.session_context(id).unwrap();
        let history = read_store_history(&data, id).await;
        assert_eq!(context.coverage.covered_item_count, history.len());
        let post = context.last_result.unwrap();
        assert_eq!(
            post.status,
            if fail {
                crate::compaction::CompactionStatus::Failed
            } else {
                crate::compaction::CompactionStatus::Compacted
            },
            "{post:?}"
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), 4);
        {
            let requests = model.requests.lock().unwrap();
            let utility = serde_json::to_string(&requests[3]).unwrap();
            assert!(utility.contains("FOLDED-OLD-SOURCE"));
            assert!(
                !utility.contains("old old old"),
                "covered raw source cannot resurrect"
            );
        }
        agent.close_session(id).await.unwrap();
        let saved = complete_display_summary(&agent, id).await;
        if fail {
            assert!(saved.len() > 64 * 1024 && saved.contains("EXACT-UNREAD-TAIL"));
        } else {
            assert_eq!(saved, "FINAL-SEMANTIC-SUMMARY");
        }
    }
}

#[tokio::test]
async fn settled_projection_post_cancel_and_append_failure_preserve_their_committed_boundaries() {
    let gate = Arc::new(Gate::new());
    let (data, _guard, id, model, mut agent) =
        settled_projection_fixture("wide-projection-post-cancel", 28_500, Step::Text("done")).await;
    model
        .scripts
        .lock()
        .unwrap()
        .push_back(Step::gated_text(gate.clone(), "not reached"));
    let turn = send_text(&mut agent, id, "current user").await;
    let result = wait_text(&agent, turn).await;
    assert_completed(&result);
    gate.wait_started().await;
    let path = data
        .join("sessions")
        .join(id.to_string())
        .join("summary.json");
    let promoted = std::fs::read(&path).unwrap();
    assert!(promoted.len() > 64 * 1024);
    assert!(
        agent
            .cancel_compaction(CompactSession {
                session_id: id,
                operation_id: format!("auto-{}", turn.loop_id)
            })
            .unwrap()
    );
    agent.close_session(id).await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), promoted);
    assert!(
        complete_display_summary(&agent, id)
            .await
            .contains("EXACT-UNREAD-TAIL")
    );

    let (data, _guard, id, _model, mut agent) =
        settled_projection_fixture("wide-projection-append-fail", 50_000, Step::Text("done")).await;
    let history = data
        .join("sessions")
        .join(id.to_string())
        .join("history.jsonl");
    let original = std::fs::read(&history).unwrap();
    crate::store::APPEND_FAILURES.arm(id, false);
    let turn = send_text(&mut agent, id, "must not promote unpersisted input").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(std::fs::read(history).unwrap(), original);
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .coverage
            .covered_item_count,
        0
    );
    assert!(
        !data
            .join("sessions")
            .join(id.to_string())
            .join("summary.json")
            .exists()
    );
    assert!(agent.session_state(id).unwrap().block_reason.is_none());
}

#[tokio::test]
async fn settled_projection_write_failures_qualify_threshold_result_without_erasing_usage() {
    for unknown in [false, true] {
        let (data, _guard, id, model, mut agent) = settled_projection_fixture(
            &format!("wide-projection-write-fail-{unknown}"),
            50_000,
            Step::Fail(settled_projection_model_error()),
        )
        .await;
        let history = data
            .join("sessions")
            .join(id.to_string())
            .join("history.jsonl");
        let prefix = std::fs::read(&history).unwrap();
        if unknown {
            crate::store::SUMMARY_UNKNOWN_WRITES.arm(id, ());
        } else {
            crate::store::SUMMARY_WRITE_FAILURES.arm(id, ());
        }
        let turn = send_text(&mut agent, id, "user survives settlement failure").await;
        let result = wait_text(&agent, turn).await;
        assert_persisted(&result);
        agent.join_turn_task_for_test(id).await;
        assert!(matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Failed(_)
        ));
        let context = agent.session_context(id).unwrap();
        assert_eq!(context.coverage.covered_item_count, 0);
        let last = context.automatic.last.unwrap();
        assert_eq!(
            last.outcome,
            if unknown {
                "compacted_settlement_unknown"
            } else {
                "compacted_settlement_failed"
            }
        );
        assert_ne!(
            last.outcome, "compacted",
            "exact-match success accounting must not treat durability failure as unqualified success"
        );
        assert_eq!(last.loop_id, Some(turn.loop_id));
        assert_eq!(last.request_index, Some(0));
        assert_eq!(last.utility_usage.unwrap().call_count, 1);
        assert!(last.after_tokens < last.before_tokens);
        assert_eq!(model.calls.load(Ordering::SeqCst), 3);
        assert!(std::fs::read(history).unwrap().starts_with(&prefix));
        assert!(agent.session_state(id).unwrap().block_reason.is_none());
        agent.close_session(id).await.unwrap();
    }
}

#[tokio::test]
async fn close_after_completion_joins_settlement_and_keeps_the_snapshot() {
    let (data, _guard, id, _model, mut agent) =
        settled_projection_fixture("wide-projection-close", 50_000, Step::Text("done")).await;
    let session = agent.loaded_session(id).unwrap().clone();
    let gate = Arc::new(crate::test_support::Gate::new());
    crate::store::SUMMARY_COMMIT_GATES.arm(id, Arc::clone(&gate));
    let _turn = send_text(&mut agent, id, "persist this user despite close race").await;
    gate.wait_started().await;
    {
        let close = agent.close_session(id);
        tokio::pin!(close);
        tokio::select! {
            biased;
            result = &mut close => panic!("close must still own the gated settlement: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        gate.release();
        close.await.unwrap();
    }
    assert!(
        data.join("sessions")
            .join(id.to_string())
            .join("summary.json")
            .exists()
    );
    let history = read_store_history(&data, id).await;
    assert!(history.iter().any(|item| matches!(item, HistoryItem::User(user) if user.input.as_text() == "persist this user despite close race")));
    assert!(session.context().coverage.covered_item_count > 0);
    assert_eq!(
        session.context().automatic.last.unwrap().outcome,
        "compacted"
    );
}
