use super::*;
use crate::compaction::{CompactionResult, CompactionStatus};

async fn compact(agent: &mut Agent, id: SessionId, operation: &str) -> CompactionResult {
    let mut receiver = agent
        .compact_session(CompactSession {
            session_id: id,
            operation_id: operation.to_owned(),
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

fn exchange(name: &str) -> Vec<HistoryItem> {
    let mut record = synthetic_tool_loop_record(LoopId::new().unwrap());
    let call_id = ToolCallId::new(format!("retained-{name}")).unwrap();
    if let HistoryItem::Assistant(assistant) = &mut record.items[1] {
        assistant.content = vec![AssistantPart::ToolCall(
            ToolCall::new(
                call_id.clone(),
                name.parse().unwrap(),
                json!({"path":"tail.txt"}),
                0,
            )
            .unwrap(),
        )];
    }
    if let HistoryItem::ToolResult(result) = &mut record.items[2] {
        result.call_id = call_id;
        result.tool_name = name.parse().unwrap();
    }
    record.items
}

#[tokio::test]
async fn default_compact_retains_answers_tools_and_real_next_request_after_cold_reopen() {
    let mut history = Vec::new();
    for index in 0..5 {
        history.extend(
            synthetic_loop_record(
                LoopId::new().unwrap(),
                &format!("USER_{index}"),
                &format!("ANSWER_{index} {}", "x".repeat(32_000)),
                false,
            )
            .items,
        );
    }
    for name in ["read", "edit", "bash"] {
        history.extend(exchange(name));
    }
    history.extend(
        synthetic_loop_record(
            LoopId::new().unwrap(),
            "latest short question",
            "LATEST_SHORT_ANSWER",
            false,
        )
        .items,
    );
    let original_count = history.len();
    let (data, _guard, id, model, mut agent) = auto_admission_fixture_with_window(
        &format!("compact-tail-next-{}", next_id()),
        false,
        100_000,
        history,
        [
            ModelScript::Text("OLD_PREFIX_SUMMARY"),
            ModelScript::Text("NEXT_ANSWER"),
            ModelScript::Text("UPDATED_PREFIX_SUMMARY"),
        ],
        None,
        None,
    )
    .await;
    let directory = data.join("sessions").join(id.to_string());
    let path = directory.join("history.jsonl");
    let before = std::fs::read(&path).unwrap();
    let result = compact(&mut agent, id, "tail-first").await;
    assert_eq!(result.status, CompactionStatus::Compacted, "{result:?}");
    assert_eq!(result.covered_loop_count, 2);
    assert_eq!(result.covered_item_count, 4);
    assert_eq!(result.retained_item_count, original_count - 4);
    assert!(result.after_tokens.unwrap() > 20_000);
    assert!(result.after_tokens < result.before_tokens);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let snapshot_bytes = std::fs::read(directory.join("summary.json")).unwrap();
    let snapshot: serde_json::Value = serde_json::from_slice(&snapshot_bytes).unwrap();
    assert_eq!(snapshot["format_version"], 1);
    assert_eq!(snapshot["source"]["covered_item_count"], 4);
    assert!(snapshot["source"]["prefix_bytes"].as_u64().unwrap() < before.len() as u64);
    let agents_path = data.join("workspace/AGENTS.md");
    std::fs::write(&agents_path, [0xff, 0xfe]).unwrap();
    let again = compact(&mut agent, id, "tail-repeat").await;
    std::fs::remove_file(agents_path).unwrap();
    assert_eq!(again.status, CompactionStatus::Noop);
    assert_eq!(again.covered_item_count, 4);
    assert_eq!(again.retained_item_count, original_count - 4);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read(directory.join("summary.json")).unwrap(),
        snapshot_bytes
    );
    let hot = agent.read_session(display_read(id)).await.unwrap();
    assert!(hot.next_cursor.is_none());
    assert_eq!(
        hot.items
            .iter()
            .filter(|item| item.data.contains("LATEST_SHORT_ANSWER"))
            .count(),
        1,
        "the actual display projection keeps the latest copyable assistant answer"
    );
    assert_eq!(hot.projection.as_ref().unwrap().covered_item_count, 4);
    agent.close_session(id).await.unwrap();
    let cold = agent.read_session(display_read(id)).await.unwrap();
    assert!(!cold.session.loaded);
    assert_eq!(hot.items, cold.items);
    assert_eq!(hot.projection, cold.projection);
    agent.open_session(id).await.unwrap();
    assert_eq!(
        agent
            .session_context(id)
            .unwrap()
            .coverage
            .covered_item_count,
        4
    );
    let turn = send_text(&mut agent, id, "NEXT_REAL_USER").await;
    let next = wait_text(&agent, turn).await;
    assert_eq!(
        next.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        use sha2::Digest as _;
        println!(
            "COMPACT_REPLAY_EVIDENCE {}",
            json!({
                "compaction": &result,
                "snapshot": &snapshot,
                "display_projection": &hot.projection,
                "history_sha256": crate::store::digest_hex(sha2::Sha256::new().chain_update(&before)),
                "utility_request": &requests[0],
                "next_request": &requests[1],
                "next_turn_requests": next.report.requests,
            })
        );
        let utility = serde_json::to_string(&requests[0]).unwrap();
        assert!(utility.contains("ANSWER_0") && utility.contains("ANSWER_1"));
        assert!(!utility.contains("ANSWER_2") && !utility.contains("LATEST_SHORT_ANSWER"));
        assert!(requests[0].tools().is_empty());
        let actual = serde_json::to_string(&requests[1]).unwrap();
        for marker in [
            "OLD_PREFIX_SUMMARY",
            "ANSWER_2",
            "ANSWER_3",
            "ANSWER_4",
            "LATEST_SHORT_ANSWER",
            "NEXT_REAL_USER",
        ] {
            assert_eq!(
                actual.matches(marker).count(),
                1,
                "{marker} must occur exactly once"
            );
        }
        assert!(!actual.contains("ANSWER_0") && !actual.contains("ANSWER_1"));
        assert_eq!(
            requests[1]
                .messages()
                .iter()
                .filter(|message| matches!(message, ModelMessage::Tool { .. }))
                .count(),
            3
        );
        for name in ["read", "edit", "bash"] {
            assert_eq!(
                actual.matches(&format!("retained-{name}")).count(),
                2,
                "call and result stay paired"
            );
        }
    }
    agent.close_session(id).await.unwrap();
    let appended = synthetic_loop_record(
        LoopId::new().unwrap(),
        &"recent ".repeat(12_000),
        "NEW_LATEST_ANSWER",
        false,
    );
    agent.store.append_loop(id, &appended).await.unwrap();
    agent.open_session(id).await.unwrap();
    let raw_before_update = std::fs::read(&path).unwrap();
    let updated = compact(&mut agent, id, "tail-new-append").await;
    assert_eq!(updated.status, CompactionStatus::Compacted, "{updated:?}");
    assert_eq!(updated.covered_item_count, original_count + 2);
    assert_eq!(updated.retained_item_count, 2);
    assert_eq!(std::fs::read(&path).unwrap(), raw_before_update);
    let requests = model.requests.lock().unwrap();
    let source = serde_json::to_string(requests.last().unwrap()).unwrap();
    assert_eq!(source.matches("OLD_PREFIX_SUMMARY").count(), 1);
    assert!(!source.contains("ANSWER_0") && !source.contains("ANSWER_1"));
    assert!(source.contains("NEXT_ANSWER"));
    assert!(!source.contains("NEW_LATEST_ANSWER"));
}

#[tokio::test]
async fn short_and_single_giant_loop_are_manual_noops_without_utility() {
    for bytes in [0, 12, 100_000] {
        let items = if bytes == 0 {
            vec![]
        } else {
            synthetic_loop_record(
                LoopId::new().unwrap(),
                &"x".repeat(bytes),
                "latest answer",
                false,
            )
            .items
        };
        let count = items.len();
        let (data, _guard, id, model, mut agent) = auto_admission_fixture_with_window(
            &format!("compact-tail-noop-{bytes}-{}", next_id()),
            false,
            100_000,
            items,
            [],
            None,
            None,
        )
        .await;
        let path = data
            .join("sessions")
            .join(id.to_string())
            .join("history.jsonl");
        let original = std::fs::read(&path).unwrap();
        let result = compact(&mut agent, id, "noop-tail").await;
        assert_eq!(result.status, CompactionStatus::Noop, "{result:?}");
        assert_eq!(result.retained_item_count, count);
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!path.parent().unwrap().join("summary.json").exists());
    }
}

#[tokio::test]
async fn automatic_no_safe_prefix_fails_only_when_trigger_is_reached() {
    for (window, expected) in [
        (100_000, CompactionStatus::Noop),
        (4_000, CompactionStatus::Failed),
    ] {
        let answer = Box::leak("answer ".repeat(1_800).into_boxed_str());
        let (data, _guard, id, model, mut agent) = auto_admission_fixture_with_window(
            &format!("compact-tail-auto-{window}-{}", next_id()),
            true,
            window,
            vec![],
            [ModelScript::Text(answer)],
            None,
            None,
        )
        .await;
        let turn = send_text(&mut agent, id, "question").await;
        wait_text(&agent, turn).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let context = agent.session_context(id).unwrap();
                if let Some(result) = context
                    .last_result
                    .filter(|_| context.current_operation.is_none())
                {
                    break result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(result.status, expected, "{result:?}");
        if expected == CompactionStatus::Failed {
            assert_eq!(result.failure_kind.as_deref(), Some("no_progress"));
        }
        assert!(result.utility_usage.is_none());
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
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
    }
}
