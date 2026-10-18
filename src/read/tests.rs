use serde::Serialize;

use super::*;
use crate::store::StoredLoopOutcome;
use minicore_runtime::execution::ConfigRevision;

#[test]
fn display_projection_strips_bodies_and_only_uses_the_captured_prefix() {
    use minicore_runtime::history::{AssistantHistory, ToolResultHistory};
    use minicore_runtime::model::{
        AssistantPart, ModelFinishReason, ReasoningPreference, ToolCall,
    };
    use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
    let session_id = SessionId::new().unwrap();
    let loop_id = LoopId::new().unwrap();
    let call_id = minicore_runtime::ToolCallId::new("display-call").unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "write".parse().unwrap(),
        serde_json::json!({
            "path": "file.txt", "content": "private body\nsecond", "secret": "raw secret"
        }),
        0,
    )
    .unwrap();
    let mut history = vec![HistoryItem::Assistant(AssistantHistory {
        loop_id,
        request_index: 0,
        model: "main".parse().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: vec![
            AssistantPart::Text("visible".to_owned()),
            AssistantPart::ToolCall(call),
        ],
        provider_replay: None,
        finish_reason: ModelFinishReason::ToolCalls,
        usage: Usage::new(1, 2, 0),
    })];
    let times = vec![None];
    let encode = |history: &[HistoryItem], end: usize| {
        encoded_display_items(session_id, 0, &history[..1], &times, &history[..end])
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1
    };
    let original = encode(&history, 1);
    assert!(!original.contains("private body"));
    assert!(!original.contains("raw secret"));
    assert!(original.contains("visible"));
    history.push(HistoryItem::ToolResult(ToolResultHistory {
        loop_id,
        request_index: 0,
        call_id,
        tool_name: "write".parse().unwrap(),
        outcome: ToolResultOutcome::Success,
        output: ToolOutput::new("result outside pin").unwrap(),
    }));
    assert_eq!(encode(&history, 1), original);
    assert_ne!(encode(&history, 2), original);
    let complete: serde_json::Value = serde_json::from_str(&encode(&history, 2)).unwrap();
    assert_eq!(
        complete["tool_summaries"][0]["display"]["input_line_count"],
        2
    );
    assert_eq!(complete["tool_summaries"][0]["output_line_count"], 1);
    assert_eq!(complete["tool_summaries"][0]["count_state"], "exact");
    assert!(complete["tool_summaries"][0]["input_availability"].is_null());
}

#[test]
fn covered_usage_preserves_known_partial_totals_without_filling_unknown_with_zero() {
    let mut covered = CoveredUsage::default();
    assert_eq!(covered.usage.input_tokens(), None);
    covered.add(
        LoopId::new().unwrap(),
        Usage::from_optional(Some(7), None, Some(0)),
    );
    let last = LoopId::new().unwrap();
    covered.add(last, Usage::from_optional(Some(3), Some(5), Some(0)));
    assert_eq!(covered.usage.input_tokens(), Some(10));
    assert_eq!(covered.usage.output_tokens(), Some(5));
    assert!(covered.partial);
    assert_eq!(covered.loop_count, 2);
    assert_eq!(covered.last_loop_id, Some(last));
}

#[derive(Clone, Serialize)]
struct TestPage {
    metadata: String,
    items: Vec<ReadItemChunk>,
    next_cursor: Option<ReadCursor>,
    total: usize,
}

fn encode_page(
    metadata: &str,
    items: &[ReadItemChunk],
    next_cursor: Option<ReadCursor>,
    total: usize,
) -> Vec<u8> {
    serde_json::to_vec(&TestPage {
        metadata: metadata.to_owned(),
        items: items.to_vec(),
        next_cursor,
        total,
    })
    .unwrap()
}

#[tokio::test]
async fn fragments_reconstruct_escaped_unicode_json_without_loss() {
    let text = "é🙂\n\t\\\" escaped ".repeat(256);
    let json = serde_json::to_string(&serde_json::json!({ "text": text })).unwrap();
    let metadata = "fixed metadata with a non-trivial prefix";
    let cancellation = CancellationToken::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let budget = 4 * 1024;
    assert!(json.len() > budget);
    let mut cursor = ReadCursor::start();
    let mut fragments = Vec::new();
    let mut fragment_count = 0;
    loop {
        let (page, next) = pack_items(
            std::iter::once(Ok::<_, AgentError>((0, json.clone()))),
            cursor,
            1,
            budget,
            &cancellation,
            deadline,
            |items, next_cursor| Ok(encode_page(metadata, items, next_cursor, 1)),
        )
        .await
        .unwrap();
        let encoded_len = encode_page(metadata, &page, next, 1).len();
        assert!(encoded_len <= budget);
        fragment_count += page.len();
        fragments.extend(page.into_iter().map(|chunk| chunk.data));
        let Some(next) = next else {
            break;
        };
        cursor = next;
    }
    assert!(fragment_count > 1);
    let reconstructed = fragments.into_iter().collect::<String>();
    assert_eq!(reconstructed, json);
}

#[tokio::test]
async fn page_budget_includes_metadata_and_rejects_too_small_pages() {
    let json = "{\"text\":\"small\"}".to_owned();
    let metadata = "metadata";
    let cancellation = CancellationToken::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let one_chunk = make_chunk(0, 0, &json, 1);
    let budget = encode_page(
        metadata,
        &[one_chunk],
        Some(ReadCursor { item: 0, offset: 1 }),
        1,
    )
    .len();
    let (items, next) = pack_items(
        std::iter::once(Ok::<_, AgentError>((0, json.clone()))),
        ReadCursor::start(),
        1,
        budget,
        &cancellation,
        deadline,
        |items, next_cursor| Ok(encode_page(metadata, items, next_cursor, 1)),
    )
    .await
    .unwrap();
    assert!(encode_page(metadata, &items, next, 1).len() <= budget);

    let error = pack_items(
        std::iter::once(Ok::<_, AgentError>((0, json))),
        ReadCursor::start(),
        1,
        1,
        &cancellation,
        deadline,
        |items, next_cursor| Ok(encode_page(metadata, items, next_cursor, 1)),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::InvalidArguments));
}

#[tokio::test]
async fn cursor_must_end_on_a_utf8_boundary() {
    let cancellation = CancellationToken::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let error = pack_items(
        std::iter::once(Ok::<_, AgentError>((0, "é".to_owned()))),
        ReadCursor { item: 0, offset: 1 },
        1,
        256,
        &cancellation,
        deadline,
        |items, next_cursor| Ok(encode_page("metadata", items, next_cursor, 1)),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::InvalidArguments));
}

fn user_item(loop_id: LoopId, text: &str) -> HistoryItem {
    HistoryItem::User(minicore_runtime::history::UserHistory {
        loop_id,
        kind: minicore_runtime::history::UserMessageKind::Prompt,
        input: minicore_runtime::execution::UserInput::text(text).unwrap(),
    })
}

#[tokio::test]
async fn wide_tool_round_counts_reach_event_and_read_wire_views() {
    for tool_rounds in [65_535_u64, 65_536, 70_000] {
        let loop_id = LoopId::new().unwrap();
        let turn = TurnRef {
            session_id: SessionId::new().unwrap(),
            loop_id,
        };
        let live = Arc::new(crate::sessions::TurnResult {
            turn,
            report: Arc::new(minicore_runtime::LoopReport {
                loop_id,
                outcome: minicore_runtime::LoopOutcome::Completed,
                appended: Arc::from([]),
                usage: Usage::default(),
                requests: 1,
                tool_rounds,
                final_config_revision: ConfigRevision::INITIAL,
            }),
            persistence: TurnPersistence::Persisted,
        });
        let event = crate::event::TurnResultView::from_turn_result(&live);
        assert_eq!(
            serde_json::to_value(event).unwrap()["tool_rounds"].as_u64(),
            Some(tool_rounds)
        );
        let stored = StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items: Vec::new(),
            usage: Usage::default(),
            requests: 1,
            tool_rounds,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: "2026-10-03T00:00:00Z".to_owned(),
            user_times: None,
        };
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let live_page = live_turn_page(
            turn,
            live,
            ReadCursor::start(),
            100,
            4096,
            &cancellation,
            deadline,
        )
        .await
        .unwrap();
        let stored_page = stored_turn_page(
            turn,
            stored,
            ReadCursor::start(),
            100,
            4096,
            &cancellation,
            deadline,
        )
        .await
        .unwrap();
        for page in [live_page, stored_page] {
            assert_eq!(page.tool_rounds, Some(tool_rounds));
            assert_eq!(
                serde_json::to_value(page).unwrap()["tool_rounds"].as_u64(),
                Some(tool_rounds)
            );
        }
        let summary = ReadTurnSummary::from_stored(&StoredTurnSummary {
            item_start: 0,
            item_end: 0,
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            usage: Usage::default(),
            requests: 1,
            tool_rounds,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: "2026-10-03T00:00:00Z".to_owned(),
        });
        assert_eq!(
            serde_json::to_value(summary).unwrap()["tool_rounds"].as_u64(),
            Some(tool_rounds)
        );
    }
}

#[tokio::test]
async fn live_and_stored_turn_pages_preserve_metadata_and_encoded_items() {
    const MAX_BYTES: usize = 512;
    const COMPLETED_AT: &str = "2026-01-02T03:04:05.000Z";

    let loop_id = LoopId::new().unwrap();
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id,
    };
    let multi_items: Vec<HistoryItem> = (0..6)
        .map(|index| user_item(loop_id, &format!("turn item {index} é🙂\n\t\\\"")))
        .collect();
    let long_text = "é🙂\n\t\\\"".repeat(256);
    let cases = [
        ("multi-item", multi_items),
        ("long-item", vec![user_item(loop_id, &long_text)]),
        ("empty-items", Vec::new()),
    ];
    let cancellation = CancellationToken::new();

    for (label, items) in cases {
        let sanitized = sanitize_history(&items).unwrap();
        let expected: Vec<String> = sanitized
            .iter()
            .map(|item| {
                serde_json::to_string(&ReadItemEnvelope {
                    item,
                    timestamp: None,
                })
                .unwrap()
            })
            .collect();
        let live = Arc::new(crate::sessions::TurnResult {
            turn,
            report: Arc::new(minicore_runtime::LoopReport {
                loop_id,
                outcome: minicore_runtime::LoopOutcome::Completed,
                appended: items.clone().into(),
                usage: Usage::default(),
                requests: 1,
                tool_rounds: 0,
                final_config_revision: ConfigRevision::INITIAL,
            }),
            persistence: crate::sessions::TurnPersistence::Failed,
        });
        let stored = StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items,
            usage: Usage::default(),
            requests: 1,
            tool_rounds: 0,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: COMPLETED_AT.to_owned(),
            user_times: None,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

        for live_side in [true, false] {
            let side = if live_side { "live" } else { "stored" };
            let (availability, persistence, completed_at) = if live_side {
                (
                    TurnResultAvailability::Live,
                    Some(crate::sessions::TurnPersistence::Failed),
                    None,
                )
            } else {
                (
                    TurnResultAvailability::Stored,
                    Some(crate::sessions::TurnPersistence::Persisted),
                    Some(COMPLETED_AT),
                )
            };
            let mut cursor = ReadCursor::start();
            let mut reconstructed = vec![String::new(); expected.len()];
            let mut page_count = 0;
            loop {
                let page = if live_side {
                    live_turn_page(
                        turn,
                        Arc::clone(&live),
                        cursor,
                        100,
                        MAX_BYTES,
                        &cancellation,
                        deadline,
                    )
                    .await
                    .unwrap()
                } else {
                    stored_turn_page(
                        turn,
                        stored.clone(),
                        cursor,
                        100,
                        MAX_BYTES,
                        &cancellation,
                        deadline,
                    )
                    .await
                    .unwrap()
                };
                assert!(
                    encoded_len(&page).unwrap() <= MAX_BYTES,
                    "{label} {side} page over budget"
                );
                assert_eq!(
                    (
                        page.turn,
                        page.availability,
                        page.outcome.clone(),
                        page.persistence,
                        page.usage,
                        page.requests,
                        page.tool_rounds,
                        page.final_config_revision,
                        page.completed_at.as_deref(),
                        page.total,
                    ),
                    (
                        turn,
                        availability,
                        Some(LoopOutcomeView::Completed),
                        persistence,
                        Some(Usage::default()),
                        Some(1),
                        Some(0),
                        Some(ConfigRevision::INITIAL),
                        completed_at,
                        expected.len(),
                    ),
                    "{label} {side} metadata"
                );
                for chunk in &page.items {
                    let expected_item = &expected[chunk.index];
                    assert_eq!(chunk.encoding, JSON_ENCODING);
                    assert_eq!(chunk.total_bytes, expected_item.len());
                    assert_eq!(chunk.offset, reconstructed[chunk.index].len());
                    let end = chunk.offset.saturating_add(chunk.data.len());
                    assert!(
                        end <= expected_item.len()
                            && expected_item.is_char_boundary(chunk.offset)
                            && expected_item.is_char_boundary(end)
                    );
                    assert_eq!(chunk.data.as_str(), &expected_item[chunk.offset..end]);
                    reconstructed[chunk.index].push_str(&chunk.data);
                    assert_eq!(chunk.complete, end == expected_item.len());
                }
                let next_cursor = page.next_cursor;
                page_count += 1;
                match next_cursor {
                    Some(next) => cursor = next,
                    None => break,
                }
            }
            assert_eq!(reconstructed, expected, "{label} {side} item data");
            if expected.is_empty() {
                assert_eq!(page_count, 1, "{label} {side} empty page");
            } else {
                assert!(page_count > 1, "{label} {side} must span pages");
            }
        }
    }
}
