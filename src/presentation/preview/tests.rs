use super::*;
use crate::event::AgentEventSink;
use crate::ids::SessionId;
use futures_util::{StreamExt, stream};
use minicore_runtime::LoopId;
use minicore_runtime::model::{DeliveryState, ModelErrorKind, ModelFinishReason};
use tokio::sync::mpsc;

fn parse_chunks(name: &str, source: &str, chunked: bool) -> PreviewParser {
    let mut parser = PreviewParser::new(name);
    let mut bytes = 0;
    if chunked {
        for ch in source.chars() {
            parser.feed(&ch.to_string(), &mut bytes);
        }
    } else {
        parser.feed(source, &mut bytes);
    }
    assert_eq!(bytes, parser.retained);
    assert_eq!(bytes, display_capacity(&parser));
    parser
}

fn fixture(capacity: usize) -> (PreviewObserver, mpsc::Receiver<AgentEvent>) {
    let (sender, receiver) = mpsc::channel(capacity);
    let presentation = Presentation::new(
        SessionId::new().unwrap(),
        AgentEventSink::new(sender),
        crate::compaction::CompactionState::new(),
    );
    (
        PreviewObserver::new(
            presentation,
            RequestKey {
                loop_id: LoopId::new().unwrap(),
                request_index: 3,
            },
            Some(7),
        ),
        receiver,
    )
}
fn start(id: &str, name: &str) -> Result<ModelEvent, ModelError> {
    Ok(ModelEvent::ToolCallStart {
        tool_call_id: ToolCallId::new(id).unwrap(),
        tool_name: name.parse().unwrap(),
    })
}
fn delta(id: &str, source: &str) -> Result<ModelEvent, ModelError> {
    Ok(ModelEvent::tool_call_arguments_delta(ToolCallId::new(id).unwrap(), source).unwrap())
}
fn end(id: &str) -> Result<ModelEvent, ModelError> {
    Ok(ModelEvent::ToolCallEnd {
        tool_call_id: ToolCallId::new(id).unwrap(),
    })
}
fn finish() -> Result<ModelEvent, ModelError> {
    Ok(ModelEvent::Finish {
        reason: ModelFinishReason::ToolCalls,
    })
}
fn error() -> Result<ModelEvent, ModelError> {
    Err(ModelError::permanent(
        ModelErrorKind::StreamInterrupted,
        DeliveryState::Started,
        minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
            minicore_runtime::error::DiagnosticCategory::Model,
            minicore_runtime::value::BoundedText::new("synthetic stream error").unwrap(),
            false,
        ),
    ))
}
fn drain(receiver: &mut mpsc::Receiver<AgentEvent>) -> Vec<AgentEvent> {
    let mut values = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        values.push(event);
    }
    values
}
fn state(event: &AgentEvent) -> ToolArgumentsPreviewState {
    match event {
        AgentEvent::ToolArgumentsPreview { state, .. } => *state,
        _ => panic!("unexpected event"),
    }
}

#[test]
fn incremental_string_escape_and_surrogate_decode_never_emits_half_scalar() {
    let source = r#"{"path":"src/\u4e2d.txt","content":"e\u0301\n\uD83D\uDC69\uD83C\uDFFD\u200D\uD83D\uDCBB\t\\\""}"#;
    let parser = parse_chunks("write", source, true);
    assert!(parser.complete());
    assert_eq!(parser.path.as_deref(), Some("src/中.txt"));
    assert_eq!(parser.content.as_deref(), Some("e\u{301}\n👩🏽‍💻\t\\\""));
    assert_eq!(parser.scanned, source.chars().count());
    let mut parser = PreviewParser::new("write");
    let mut bytes = 0;
    parser.feed(r#"{"content":"x\uD83D"#, &mut bytes);
    assert_eq!(parser.content.as_deref(), Some("x"));
    parser.feed(r#"\uDC69"}"#, &mut bytes);
    assert_eq!(parser.content.as_deref(), Some("x👩"));
    assert!(parser.complete());
}

#[test]
fn only_top_level_whitelist_is_captured_and_unknown_fields_are_not_retained() {
    let source = r#"{"secret":"NEVER DISPLAY","nested":{"path":"wrong","content":"wrong"},"array":[{"path":"wrong"},1,true,false,null,-2.5e-8],"path":"right","content":"yes"}"#;
    let parser = parse_chunks("write", source, true);
    assert!(parser.complete());
    assert_eq!(parser.path.as_deref(), Some("right"));
    assert_eq!(parser.content.as_deref(), Some("yes"));
    assert_eq!(parser.retained, display_capacity(&parser));
    assert_eq!(
        parser.path.as_ref().unwrap().len() + parser.content.as_ref().unwrap().len(),
        8
    );
    let edit = parse_chunks(
        "edit",
        r#"{"path":"a","old_text":"old","new_text":"new","content":"unknown"}"#,
        true,
    );
    assert!(edit.complete());
    assert_eq!(edit.display().expanded_input, None);
    let huge = format!(
        r#"{{"{}": "{}", "path":"ok"}}"#,
        "k".repeat(100_000),
        "v".repeat(1_000_000)
    );
    let parser = parse_chunks("read", &huge, true);
    assert!(parser.complete());
    assert_eq!(parser.retained, display_capacity(&parser));
    assert_eq!(parser.path.as_ref().unwrap().len(), 2);
    assert!(parser.key.capacity() <= 32);
    assert_eq!(parser.scanned, huge.len());
}

#[test]
fn invalid_prefixes_duplicate_fields_wrong_types_and_surrogates_are_conservative() {
    for source in [
        r#"{"path":"safe","path":"ambiguous"}"#,
        r#"{"path":"safe","file_path":"ambiguous"}"#,
        r#"{"content":"before\qafter"}"#,
        r#"{"path":"\uDC00"}"#,
        r#"{"path":"\uD800x"}"#,
        r#"{"path":"\uD800\u0041"}"#,
        r#"{"path":"\u00zz"}"#,
        r#"{"path":34}"#,
        r#"{"content":null}"#,
        r#"{"content":{"secret":"not a string"}}"#,
        r#"{"path":"safe",}"#,
        r#"{"path":"safe","nested":[1,]}"#,
        r#"{"path":"safe"}x"#,
        r#"[{"path":"nested"}]"#,
        r#"{"path":"safe","number":01}"#,
        "{\"path\":\"literal\u{1b}\"}",
    ] {
        let parser = parse_chunks("write", source, true);
        assert!(parser.invalid, "invalid fixture was accepted");
        assert!(!parser.complete());
        assert_eq!(parser.retained, 0);
        assert_eq!(parser.display().detail, "...");
        assert_eq!(parser.display().expanded_input, None);
    }
}

#[test]
fn incomplete_strings_and_numbers_are_not_mistaken_for_completion() {
    for source in [
        "",
        "{",
        r#"{"path":"par"#,
        r#"{"path":"x\"#,
        r#"{"path":"x\uD8"#,
        r#"{"path":"x\uD83D\u"#,
        r#"{"path":"x", "offset":12"#,
    ] {
        let parser = parse_chunks("read", source, true);
        assert!(!parser.complete());
        assert!(!parser.invalid);
        assert_eq!(parser.offset, None);
    }
    let mut parser = PreviewParser::new("read");
    let mut bytes = 0;
    parser.feed(r#"{"path":"a","offset":12"#, &mut bytes);
    assert_eq!(parser.display().detail, "a");
    parser.feed(r#", "limit":3}"#, &mut bytes);
    assert!(parser.complete());
    assert_eq!(parser.display().detail, "a:12-14");
    for bad in [
        "-1",
        "0",
        "1.5",
        "1e2",
        "18446744073709551616",
        "true",
        "null",
        "\"2\"",
    ] {
        assert!(parse_chunks("read", &format!(r#"{{"path":"a","offset":{bad}}}"#), true).invalid);
    }
}

#[test]
fn display_caps_apply_after_sanitization_and_preserve_utf8_prefixes() {
    let source = format!(
        r#"{{"path":"{}","content":"{}"}}"#,
        "中".repeat(400),
        r"\u001b\n👩🏽‍💻".repeat(20_000)
    );
    let parser = parse_chunks("write", &source, true);
    assert!(parser.complete());
    assert!(parser.path_cut && parser.body_cut);
    assert!(parser.retained <= MAX_CALL_BYTES);
    let display = parser.display();
    assert!(display.truncated && display.body_truncated);
    assert!(display.detail.len() <= MAX_DETAIL_BYTES);
    assert!(!display.expanded_input.as_ref().unwrap().contains('\u{1b}'));
    assert_eq!(
        display.input_line_count,
        display.expanded_input.as_deref().map(count_lines)
    );
}

#[test]
fn aggregate_cap_and_depth_are_bounded_without_retaining_raw_json() {
    let mut bytes = 0;
    let mut parsers = Vec::new();
    for _ in 0..MAX_CALLS {
        let mut parser = PreviewParser::new("write");
        parser.feed(r#"{"content":""#, &mut bytes);
        parser.feed(&"x".repeat(MAX_CALL_BYTES * 2), &mut bytes);
        parser.feed(r#""}"#, &mut bytes);
        assert!(parser.complete());
        assert!(parser.retained <= MAX_CALL_BYTES);
        assert!(bytes <= MAX_STREAM_BYTES);
        parsers.push(parser);
    }
    assert_eq!(bytes, MAX_STREAM_BYTES);
    assert!(parsers.iter().all(|parser| parser.body_cut));
    let source = format!(
        "{{\"ignored\":{}0{}}}",
        "[".repeat(MAX_DEPTH + 1),
        "]".repeat(MAX_DEPTH + 1)
    );
    let parser = parse_chunks("read", &source, true);
    assert!(parser.invalid);
    assert!(parser.stack.capacity() <= MAX_DEPTH);
}

#[test]
fn generated_snapshot_is_self_contained_and_debug_is_redacted() {
    let (mut observer, mut receiver) = fixture(16);
    let now = Instant::now();
    for item in [
        start("c", "write"),
        delta("c", r#"{"path":"SECRET_PATH","content":"SECRET_BODY"}"#),
        end("c"),
    ] {
        observer.observe(&item, now);
    }
    let events = drain(&mut receiver);
    let last = events.last().unwrap();
    assert_eq!(state(last), ToolArgumentsPreviewState::Generated);
    let wire = serde_json::to_value(last).unwrap();
    assert_eq!(wire["type"], "tool_arguments_preview");
    assert_eq!(wire["data"]["attempt"], 7);
    assert_eq!(wire["data"]["revision"], 3);
    assert_eq!(wire["data"]["display"]["detail"], "SECRET_PATH");
    assert_eq!(wire["data"]["display"]["expanded_input"], "SECRET_BODY");
    assert_eq!(wire["data"]["partial"], false);
    assert_eq!(wire["data"]["request_index"], 3);
    assert_eq!(
        wire["data"]["turn"]["session_id"],
        wire["data"]["meta"]["session_id"]
    );
    assert_eq!(
        wire["data"]["turn"]["loop_id"],
        wire["data"]["meta"]["loop_id"]
    );
    let debug = format!("{last:?}");
    assert!(!debug.contains("SECRET_PATH") && !debug.contains("SECRET_BODY"));
}

#[test]
fn one_byte_deltas_are_linear_and_throttled_before_clone_and_queue() {
    let (mut observer, mut receiver) = fixture(64);
    let now = Instant::now();
    observer.observe(&start("c", "write"), now);
    let source = format!(r#"{{"path":"abc","content":"{}"}}"#, "x".repeat(120_000));
    for ch in source.chars() {
        observer.observe(&delta("c", &ch.to_string()), now);
    }
    assert_eq!(observer.calls[0].parser.scanned, source.len());
    // Start + one first-path exception, not a snapshot/parse of each prefix.
    assert_eq!(receiver.len(), 2);
    observer.observe(&end("c"), now);
    let events = drain(&mut receiver);
    assert_eq!(events.len(), 3);
    if let AgentEvent::ToolArgumentsPreview { display, .. } = &events[2] {
        assert_eq!(display.expanded_input.as_ref().unwrap().len(), 120_000);
    }
}

#[test]
fn throttle_is_stream_wide_and_unknown_fields_do_not_repeat_snapshots() {
    let (mut observer, mut receiver) = fixture(128);
    let now = Instant::now();
    for id in ["a", "b"] {
        observer.observe(&start(id, "write"), now);
    }
    observer.observe(&delta("a", r#"{"content":"a"#), now + UPDATE_INTERVAL);
    observer.observe(&delta("b", r#"{"content":"b"#), now + UPDATE_INTERVAL);
    assert_eq!(receiver.len(), 3);
    observer.observe(&delta("b", "c"), now + UPDATE_INTERVAL * 2);
    assert_eq!(receiver.len(), 4);
    observer.observe(&delta("a", r#"","ignored":""#), now + UPDATE_INTERVAL * 3);
    let count = receiver.len();
    observer.observe(&delta("a", "irrelevant"), now + UPDATE_INTERVAL * 4);
    assert_eq!(receiver.len(), count);
    drain(&mut receiver);
}

#[test]
fn call_count_is_bounded_and_unlisted_tools_never_emit_raw_arguments() {
    let (mut observer, mut receiver) = fixture(128);
    let now = Instant::now();
    observer.observe(&start("bash", "bash"), now);
    observer.observe(&delta("bash", r#"{"command":"secret"}"#), now);
    observer.observe(&start("custom", "custom"), now);
    for i in 0..100 {
        observer.observe(&start(&format!("c-{i}"), "write"), now);
    }
    assert_eq!(observer.calls.len(), MAX_CALLS);
    assert_eq!(drain(&mut receiver).len(), MAX_CALLS);
}

#[tokio::test]
async fn stream_passthrough_preserves_all_items_and_only_finish_plus_eof_retains_cards() {
    let (observer, mut receiver) = fixture(32);
    let expected = vec![
        start("c", "write"),
        delta("c", r#"{"path":"a","content":"x"}"#),
        end("c"),
        finish(),
    ];
    let stream = PreviewStream::new(
        Box::pin(stream::iter(expected.clone())),
        observer.presentation.clone(),
        observer.key,
        Some(8),
        None,
        1000,
    );
    let actual: Vec<_> = stream.collect().await;
    assert_eq!(actual, expected);
    assert!(
        drain(&mut receiver)
            .iter()
            .all(|event| state(event) != ToolArgumentsPreviewState::Discarded)
    );
}

#[tokio::test]
async fn unfinished_eof_error_post_finish_error_extra_event_and_drop_discard() {
    for ending in [
        vec![],
        vec![error()],
        vec![finish(), error()],
        vec![finish(), Ok(ModelEvent::text_delta("extra").unwrap())],
    ] {
        let (observer, mut receiver) = fixture(32);
        let mut expected = vec![
            start("c", "write"),
            delta("c", r#"{"path":"a","content":"x"}"#),
            end("c"),
        ];
        expected.extend(ending);
        let stream = PreviewStream::new(
            Box::pin(stream::iter(expected.clone())),
            observer.presentation.clone(),
            observer.key,
            Some(8),
            None,
            1000,
        );
        assert_eq!(stream.collect::<Vec<_>>().await, expected);
        assert_eq!(
            state(drain(&mut receiver).last().unwrap()),
            ToolArgumentsPreviewState::Discarded
        );
    }
    for consume_finish in [false, true] {
        let (observer, mut receiver) = fixture(32);
        let items = [
            start("c", "write"),
            delta("c", r#"{"path":"a","content":"x"}"#),
            end("c"),
            finish(),
        ];
        let mut stream = PreviewStream::new(
            Box::pin(stream::iter(items)),
            observer.presentation.clone(),
            observer.key,
            Some(8),
            None,
            1000,
        );
        for _ in 0..if consume_finish { 4 } else { 2 } {
            assert!(stream.next().await.is_some());
        }
        drop(stream); // Simulates cancellation/deadline/assembler early rejection.
        assert_eq!(
            state(drain(&mut receiver).last().unwrap()),
            ToolArgumentsPreviewState::Discarded
        );
    }
}

#[test]
fn queue_drop_does_not_backpressure_and_later_snapshot_recovers() {
    let (mut observer, mut receiver) = fixture(1);
    let now = Instant::now();
    observer.observe(&start("c", "write"), now);
    observer.observe(&delta("c", r#"{"path":"path","content":"body"}"#), now);
    assert_eq!(receiver.len(), 1);
    drain(&mut receiver);
    observer.observe(&end("c"), now);
    match receiver.try_recv().unwrap() {
        AgentEvent::ToolArgumentsPreview {
            display,
            revision,
            meta,
            state,
            ..
        } => {
            assert_eq!(revision, 3);
            assert_eq!(state, ToolArgumentsPreviewState::Generated);
            assert_eq!(display.expanded_input.as_deref(), Some("body"));
            assert!(meta.dropped_before > 0);
        }
        _ => panic!("unexpected event"),
    }
}

#[test]
fn attempts_are_session_monotonic_and_exhaustion_disables_only_preview() {
    let (observer, _receiver) = fixture(8);
    let presentation = observer.presentation.clone();
    assert_eq!(presentation.next_preview_attempt(), Some(1));
    presentation.reset_before_loop_start();
    assert_eq!(presentation.next_preview_attempt(), Some(2));
    presentation
        .preview_attempt
        .store(u64::MAX - 1, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(presentation.next_preview_attempt(), Some(u64::MAX));
    assert_eq!(presentation.next_preview_attempt(), None);
    assert_eq!(presentation.next_preview_attempt(), None);
}

fn display_capacity(parser: &PreviewParser) -> usize {
    parser.path.as_ref().map_or(0, String::capacity)
        + parser.content.as_ref().map_or(0, String::capacity)
}

#[test]
fn five_non_power_of_two_bodies_charge_actual_capacity_within_stream_budget() {
    let mut charged = 0;
    let mut parsers = Vec::new();
    for _ in 0..5 {
        let mut parser = PreviewParser::new("write");
        parser.feed(r#"{"content":""#, &mut charged);
        parser.feed(&"x".repeat(100_000), &mut charged);
        parser.feed(r#""}"#, &mut charged);
        assert!(parser.complete());
        assert!(display_capacity(&parser) <= MAX_CALL_BYTES);
        assert_eq!(parser.retained, display_capacity(&parser));
        parsers.push(parser);
        assert_eq!(charged, parsers.iter().map(display_capacity).sum::<usize>());
        assert!(charged <= MAX_STREAM_BYTES);
    }
    assert_eq!(charged, MAX_STREAM_BYTES);
    assert_eq!(parsers.iter().filter(|parser| parser.body_cut).count(), 1);
    assert!(parsers[4].display().body_truncated);
}

#[test]
fn capacity_limits_hold_after_every_unicode_scalar_across_multiple_calls() {
    let mut charged = 0;
    let mut parsers = Vec::new();
    // Odd-sized paths leave non-power-of-two room for multi-byte content.
    // The last call reaches the aggregate ceiling with an incomplete scalar's
    // worth of free bytes; retaining a scalar is always all-or-nothing.
    for index in 0..MAX_CALLS {
        let mut parser = PreviewParser::new("write");
        let source = format!(
            r#"{{"path":"p{}","content":"{}"}}"#,
            "中".repeat(index + 1),
            "👩🏽‍💻é".repeat(8_000)
        );
        let previous: usize = parsers.iter().map(display_capacity).sum();
        for ch in source.chars() {
            parser.feed(&ch.to_string(), &mut charged);
            assert_eq!(parser.retained, display_capacity(&parser));
            assert!(parser.retained <= MAX_CALL_BYTES);
            assert_eq!(charged, previous + parser.retained);
            assert!(charged <= MAX_STREAM_BYTES);
        }
        assert!(parser.complete());
        if let Some(body) = parser.content.as_deref() {
            assert!("👩🏽‍💻é".repeat(8_000).starts_with(body));
        }
        parsers.push(parser);
    }
    assert!(parsers.iter().any(|parser| parser.body_cut));
    assert!(charged <= MAX_STREAM_BYTES);
}

#[test]
fn exact_capacity_ceiling_truncates_without_splitting_utf8_and_fail_releases_charge() {
    let mut charged = 0;
    let mut parsers = Vec::new();
    for _ in 0..4 {
        let mut parser = PreviewParser::new("write");
        parser.feed(r#"{"content":""#, &mut charged);
        parser.feed(&"x".repeat(MAX_CALL_BYTES - 3), &mut charged);
        // 3 spare bytes cannot hold this 4-byte scalar. The buffer stays at
        // its already charged ceiling, without growth or malformed UTF-8.
        parser.feed("😀", &mut charged);
        parser.feed(r#""}"#, &mut charged);
        assert_eq!(display_capacity(&parser), MAX_CALL_BYTES);
        assert_eq!(parser.content.as_ref().unwrap().len(), MAX_CALL_BYTES - 3);
        assert!(parser.body_cut);
        parsers.push(parser);
    }
    assert_eq!(charged, MAX_STREAM_BYTES);
    // An invalid prefix releases actual capacity, including its unused tail.
    parsers[0].feed("invalid after object", &mut charged);
    assert!(parsers[0].invalid);
    assert_eq!(parsers[0].retained, 0);
    assert_eq!(charged, MAX_STREAM_BYTES - MAX_CALL_BYTES);
    let mut replacement = PreviewParser::new("write");
    replacement.feed(r#"{"content":""#, &mut charged);
    replacement.feed(&"é".repeat(MAX_CALL_BYTES / 2), &mut charged);
    replacement.feed(r#""}"#, &mut charged);
    assert!(replacement.complete());
    assert!(!replacement.body_cut);
    assert_eq!(display_capacity(&replacement), MAX_CALL_BYTES);
    assert_eq!(charged, MAX_STREAM_BYTES);
}
