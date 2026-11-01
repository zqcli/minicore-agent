use serde::Serialize;

use super::*;

#[test]
fn prefix_escape_sizes_match_serde_for_controls_unicode_and_item_json() {
    let text = (0u32..=255).filter_map(char::from_u32).collect::<String>() + "🙂\"\\\n";
    let sizes = json_prefix_lengths(&text);
    for offset in 0..=text.len() {
        if text.is_char_boundary(offset) {
            assert_eq!(
                sizes.bytes[offset] + 2,
                serde_json::to_vec(&text[..offset]).unwrap().len()
            );
        }
    }
    let json = serde_json::to_string(&serde_json::json!({"text":text})).unwrap();
    let sizes = json_prefix_lengths(&json);
    assert_eq!(
        sizes.bytes[json.len()] + 2,
        serde_json::to_vec(&json).unwrap().len()
    );
}
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
            AssistantPart::Text("visible".into()),
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
            |_, _, next_cursor| Ok(encode_page(metadata, &[], next_cursor, 1).len()),
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
        |_, _, next_cursor| Ok(encode_page(metadata, &[], next_cursor, 1).len()),
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
        |_, _, next_cursor| Ok(encode_page(metadata, &[], next_cursor, 1).len()),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::InvalidArguments));
}

#[tokio::test]
async fn incremental_page_sizes_match_exact_json_budgets_across_many_chunks() {
    let bodies: Vec<_> = (0..100)
        .map(|index| {
            serde_json::to_string(&format!("{index}: {}", "é🙂\n\t\\\"".repeat(64))).unwrap()
        })
        .collect();
    let complete: Vec<_> = bodies
        .iter()
        .enumerate()
        .map(|(index, json)| make_chunk(index, 0, json, json.len()))
        .collect();
    let total = bodies.len();
    let metadata = "metadata with \"quotes\" and Unicode 🙂";
    let exact = encode_page(metadata, &complete, None, total).len();
    for budget in [4096, exact - 1, exact] {
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut cursor = ReadCursor::start();
        let mut reconstructed = vec![String::new(); total];
        let mut pages = 0;
        loop {
            let (page, next) = pack_items(
                bodies.iter().cloned().enumerate().skip(cursor.item).map(Ok),
                cursor,
                total,
                budget,
                &cancellation,
                deadline,
                |_, _, next| Ok(encode_page(metadata, &[], next, total).len()),
            )
            .await
            .unwrap();
            assert!(encode_page(metadata, &page, next, total).len() <= budget);
            for chunk in page {
                assert_eq!(chunk.offset, reconstructed[chunk.index].len());
                reconstructed[chunk.index].push_str(&chunk.data);
            }
            pages += 1;
            let Some(next) = next else {
                break;
            };
            assert_ne!(cursor, next);
            cursor = next;
        }
        assert_eq!(reconstructed, bodies);
        assert_eq!(pages == 1, budget == exact);
    }
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
        |_, _, next_cursor| Ok(encode_page("metadata", &[], next_cursor, 1).len()),
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
async fn interrupted_partial_reaches_stored_and_live_turn_pages_and_record_summaries() {
    let loop_id = LoopId::new().unwrap();
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id,
    };
    let interrupted = InterruptedRequest {
        stop_reason: PartialStopReason::Aborted,
        request_index: 4,
        model: "main".to_owned(),
        reasoning: "hmm".to_owned(),
        text: "half".to_owned(),
    };
    let outcome = StoredLoopOutcome::Cancelled {
        reason: crate::event::CancelReasonView::User,
    };
    let stored = StoredLoopRecord {
        loop_id,
        outcome: outcome.clone(),
        items: vec![user_item(loop_id, "question")],
        usage: Usage::default(),
        requests: 4,
        tool_rounds: 3,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: "2026-10-03T00:00:00Z".to_owned(),
        user_times: None,
        interrupted: Some(interrupted.clone()),
    };
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    let stored_page = stored_turn_page(
        &result_request(turn, ReadCursor::start(), 100, 4096),
        stored,
        &result_limits(&cancellation, deadline),
    )
    .await
    .unwrap();
    assert_eq!(stored_page.interrupted.as_ref(), Some(&interrupted));
    assert_eq!(
        serde_json::to_value(&stored_page).unwrap()["interrupted"],
        serde_json::json!({
            "stop_reason": "aborted", "request_index": 4, "model": "main",
            "reasoning": "hmm", "text": "half"
        })
    );

    let live = Arc::new(crate::sessions::TurnResult {
        turn,
        report: Arc::new(minicore_runtime::LoopReport {
            loop_id,
            outcome: minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User),
            appended: Arc::from([]),
            usage: Usage::default(),
            requests: 4,
            tool_rounds: 3,
            final_config_revision: ConfigRevision::INITIAL,
        }),
        persistence: TurnPersistence::Failed,
        interrupted: Some(interrupted.clone()),
        next: None,
    });
    let live_page = live_turn_page(
        &result_request(turn, ReadCursor::start(), 100, 4096),
        live,
        &result_limits(&cancellation, deadline),
    )
    .await
    .unwrap();
    assert_eq!(live_page.interrupted.as_ref(), Some(&interrupted));

    let mut summary = StoredTurnSummary {
        item_start: 0,
        item_end: 1,
        loop_id,
        outcome,
        usage: Usage::default(),
        requests: 4,
        tool_rounds: 3,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: "2026-10-03T00:00:00Z".to_owned(),
        interrupted: Some(interrupted.clone()),
    };
    let value = serde_json::to_value(ReadTurnSummary::from_stored(&summary)).unwrap();
    assert_eq!(value["interrupted"]["text"], "half");
    summary.interrupted = None;
    let value = serde_json::to_value(ReadTurnSummary::from_stored(&summary)).unwrap();
    assert!(value.get("interrupted").is_none());
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
            interrupted: None,
            next: None,
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
            interrupted: None,
        };
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let live_page = live_turn_page(
            &result_request(turn, ReadCursor::start(), 100, 4096),
            live,
            &result_limits(&cancellation, deadline),
        )
        .await
        .unwrap();
        let stored_page = stored_turn_page(
            &result_request(turn, ReadCursor::start(), 100, 4096),
            stored,
            &result_limits(&cancellation, deadline),
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
            interrupted: None,
        });
        assert_eq!(
            serde_json::to_value(summary).unwrap()["tool_rounds"].as_u64(),
            Some(tool_rounds)
        );
    }
}

#[tokio::test]
async fn live_and_stored_turn_pages_preserve_metadata_and_encoded_items() {
    const MAX_BYTES: usize = 1024;
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
            interrupted: None,
            next: None,
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
            interrupted: None,
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
                        &result_request(turn, cursor, 100, MAX_BYTES),
                        Arc::clone(&live),
                        &result_limits(&cancellation, deadline),
                    )
                    .await
                    .unwrap()
                } else {
                    stored_turn_page(
                        &result_request(turn, cursor, 100, MAX_BYTES),
                        stored.clone(),
                        &result_limits(&cancellation, deadline),
                    )
                    .await
                    .unwrap()
                };
                assert!(
                    encoded_len(&page) <= MAX_BYTES,
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

#[tokio::test]
async fn stored_pages_preserve_failed_cancelled_and_unknown_outcomes_even_when_empty() {
    let loop_id = LoopId::new().unwrap();
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id,
    };
    let cancellation = CancellationToken::new();
    for outcome in [
        LoopOutcomeView::Unknown,
        LoopOutcomeView::Cancelled {
            reason: crate::event::CancelReasonView::User,
        },
        LoopOutcomeView::Failed {
            kind: "test_failure".into(),
            model_error: None,
        },
    ] {
        let record = StoredLoopRecord {
            loop_id,
            outcome: outcome.clone(),
            items: Vec::new(),
            usage: Usage::default(),
            requests: 0,
            tool_rounds: 0,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: "2026-10-31T00:00:00Z".into(),
            user_times: None,
            interrupted: None,
        };
        let page = stored_turn_page(
            &result_request(turn, ReadCursor::start(), 100, 4096),
            record,
            &result_limits(&cancellation, Instant::now() + Duration::from_secs(5)),
        )
        .await
        .unwrap();
        assert_eq!(page.outcome, Some(outcome));
        assert_eq!(page.persistence, Some(TurnPersistence::Persisted));
        assert_eq!(page.availability, TurnResultAvailability::Stored);
        assert!(page.result_id.is_some());
        assert!(page.items.is_empty());
        assert_eq!(page.total, 0);
        assert_eq!(page.next_cursor, None);
    }
}

#[test]
fn exact_source_reservation_admits_a_result_at_the_small_byte_limit() {
    let loop_id = LoopId::new().unwrap();
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id,
    };
    let items = vec![user_item(loop_id, &"\u{1}".repeat(8192))];
    let metadata = crate::event::TurnResultView {
        turn,
        outcome: LoopOutcomeView::Completed,
        usage: Usage::default(),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        persistence: TurnPersistence::Failed,
    };
    let bytes = result_source_upper_bound(&items, None, &metadata).unwrap();
    let catalog = crate::sessions::ResultCatalog::new();
    catalog.set_limits_for_test(crate::sessions::results::ResultLimits {
        source_bytes_per_load: bytes,
        all_source_bytes: bytes,
        ..Default::default()
    });
    let load = crate::test_support::load_id();
    catalog.register(load);
    let charge = catalog.reserve(load, bytes).unwrap();
    let source = encode_result_source(&items, None, metadata).unwrap();
    let canonical = prepare_result_source(&items, None).unwrap();
    assert_eq!(source.result_id, canonical.result_id);
    assert_eq!(source.items, canonical.items);
    let reservation = catalog.finish_reservation(source, charge).unwrap();
    assert!(matches!(
        catalog.reserve(load, 1),
        Err(crate::sessions::UnavailableReason::ResourceLimit)
    ));
    drop(reservation);
    assert!(catalog.reserve(load, bytes).is_ok());
}

#[test]
fn source_admission_counts_metadata_string_capacity_not_just_encoded_length() {
    let loop_id = LoopId::new().unwrap();
    let items = vec![user_item(loop_id, "question")];
    let mut metadata = crate::event::TurnResultView {
        turn: TurnRef {
            session_id: SessionId::new().unwrap(),
            loop_id,
        },
        outcome: LoopOutcomeView::Completed,
        usage: Usage::default(),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        persistence: TurnPersistence::Failed,
    };
    let base = result_source_upper_bound(&items, None, &metadata).unwrap();
    let mut kind = String::with_capacity(4096);
    kind.push_str("failed");
    metadata.outcome = LoopOutcomeView::Failed {
        kind,
        model_error: None,
    };
    let catalog = crate::sessions::ResultCatalog::new();
    catalog.set_limits_for_test(crate::sessions::results::ResultLimits {
        source_bytes_per_load: base + 1024,
        ..Default::default()
    });
    let load = crate::test_support::load_id();
    catalog.register(load);
    assert!(matches!(
        catalog.reserve(
            load,
            result_source_upper_bound(&items, None, &metadata).unwrap()
        ),
        Err(crate::sessions::UnavailableReason::TooLarge)
    ));
}

#[tokio::test]
async fn borrowed_result_ranges_match_canonical_cleanup_including_opaque_reasoning() {
    use minicore_runtime::history::{AssistantHistory, ToolResultHistory};
    use minicore_runtime::model::{
        AssistantPart, ModelFinishReason, ReasoningContent, ReasoningPreference, ToolCall,
    };
    let loop_id = LoopId::new().unwrap();
    let call_id = minicore_runtime::ToolCallId::new("result-call").unwrap();
    let opaque = ReasoningContent::new(None, None, Some("private".into()), None).unwrap();
    let assistant = AssistantHistory {
        loop_id,
        request_index: 0,
        model: "main".parse().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: vec![
            AssistantPart::Reasoning(opaque.clone()),
            AssistantPart::Text("answer".into()),
            AssistantPart::Reasoning(
                ReasoningContent::new(
                    Some("visible".into()),
                    Some("summary".into()),
                    Some("private".into()),
                    Some("signature".into()),
                )
                .unwrap(),
            ),
            AssistantPart::ToolCall(
                ToolCall::new(
                    call_id.clone(),
                    "read".parse().unwrap(),
                    serde_json::json!({"path":"file.txt"}),
                    0,
                )
                .unwrap(),
            ),
        ],
        provider_replay: None,
        finish_reason: ModelFinishReason::ToolCalls,
        usage: Usage::new(1, 2, 0),
    };
    let items = vec![
        user_item(loop_id, "question"),
        HistoryItem::Assistant(assistant.clone()),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index: 0,
            call_id,
            tool_name: "read".parse().unwrap(),
            outcome: minicore_runtime::tools::ToolResultOutcome::Success,
            output: minicore_runtime::tools::ToolOutput::new("result").unwrap(),
        }),
        HistoryItem::Assistant(AssistantHistory {
            content: vec![AssistantPart::Reasoning(opaque)],
            ..assistant
        }),
    ];
    let expected = prepare_result_source(&items, None).unwrap();
    assert_eq!(expected.items.len(), 3);
    let items: Arc<[HistoryItem]> = items.into();
    for index in 0..expected.items.len() {
        let page = prepare_turn_items(
            Arc::clone(&items),
            None,
            ReadCursor {
                item: index,
                offset: 0,
            },
            1,
            4096,
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(page.total, expected.items.len());
        assert_eq!(page.result_id, expected.result_id);
        assert_eq!(page.encoded, [(index, expected.items[index].clone())]);
        assert!(!page.encoded[0].1.contains("private"));
        assert!(!page.encoded[0].1.contains("signature\":\""));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_long_result_readers_verify_every_page_and_stable_identity() {
    async fn read(record: StoredLoopRecord, turn: TurnRef) -> (usize, u128, u128) {
        let started = Instant::now();
        let expected = prepare_result_source(&record.items, None).unwrap();
        let mut reconstructed = vec![String::new(); expected.items.len()];
        let mut cursor = ReadCursor::start();
        let mut pages = 0;
        let mut max_ms = 0;
        loop {
            let start = Instant::now();
            let page = stored_turn_page(
                &TurnResultRequest {
                    result_id: Some(expected.result_id.clone()),
                    ..result_request(turn, cursor, 4, MAX_READ_MAX_BYTES)
                },
                record.clone(),
                &result_limits(&CancellationToken::new(), Instant::now() + READ_DEADLINE),
            )
            .await
            .unwrap();
            max_ms = max_ms.max(start.elapsed().as_millis());
            assert_eq!(page.total, expected.items.len());
            assert_eq!(page.result_id.as_deref(), Some(expected.result_id.as_str()));
            assert!(encoded_len(&page) <= MAX_READ_MAX_BYTES);
            for chunk in page.items {
                assert_eq!(chunk.offset, reconstructed[chunk.index].len());
                reconstructed[chunk.index].push_str(&chunk.data);
                if chunk.complete {
                    assert_eq!(reconstructed[chunk.index].len(), chunk.total_bytes);
                }
            }
            pages += 1;
            assert!(pages < 64);
            match page.next_cursor {
                Some(next) => {
                    assert!((next.item, next.offset) > (cursor.item, cursor.offset));
                    cursor = next;
                }
                None => break,
            }
        }
        assert_eq!(reconstructed, expected.items);
        (pages, started.elapsed().as_millis(), max_ms)
    }
    let loop_id = LoopId::new().unwrap();
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id,
    };
    let record = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: (0..32)
            .map(|_| user_item(loop_id, &"x".repeat(256 * 1024)))
            .collect(),
        usage: Usage::default(),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: "2026-10-31T00:00:00Z".into(),
        user_times: None,
        interrupted: None,
    };
    let prefix = prepare_turn_items(
        record.items.clone().into(),
        None,
        ReadCursor::start(),
        100,
        MAX_READ_MAX_BYTES,
        CancellationToken::new(),
        Instant::now() + READ_DEADLINE,
    )
    .await
    .unwrap();
    assert_eq!(prefix.total, 32);
    assert!(!prefix.encoded.is_empty() && prefix.encoded.len() <= 4);
    assert!(
        prefix
            .encoded
            .iter()
            .map(|(_, item)| item.capacity())
            .sum::<usize>()
            < 2 * MAX_READ_MAX_BYTES
    );
    drop(prefix);
    let (a, b) = tokio::time::timeout(Duration::from_secs(90), async {
        tokio::join!(read(record.clone(), turn), read(record, turn))
    })
    .await
    .unwrap();
    println!(
        "two_result_readers bytes_per_turn=8388608 pages={}/{} total_ms={}/{} max_page_ms={}/{}",
        a.0, b.0, a.1, b.1, a.2, b.2
    );
}

fn result_request(
    turn: TurnRef,
    cursor: ReadCursor,
    limit: usize,
    max_bytes: usize,
) -> TurnResultRequest {
    TurnResultRequest {
        turn,
        load: None,
        result_id: None,
        cursor: Some(cursor),
        limit,
        max_bytes: Some(max_bytes),
    }
}

fn result_limits(cancellation: &CancellationToken, deadline: Instant) -> HistoryScanLimits {
    let mut limits = scan_limits(cancellation.clone());
    limits.deadline = deadline;
    limits
}
