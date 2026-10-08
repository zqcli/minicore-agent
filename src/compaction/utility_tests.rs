use super::*;
use std::sync::Mutex;
use std::time::Duration;

use minicore_runtime::execution::UserInput;
use minicore_runtime::history::{
    AssistantHistory, SummaryHistory, ToolResultHistory, UserHistory, UserMessageKind,
};
use minicore_runtime::model::{
    ModelDescriptor, ModelStartFuture, ModelStream, ProviderReplay, ReasoningContent, ToolCall,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
use serde_json::json;

// This is an input contract fixture, not a model-quality oracle.
const REPORT_STAGE_3: &str = include_str!("fixtures/report-stage-3.txt");

struct Fake {
    descriptor: ModelDescriptor,
    requests: Mutex<Vec<ModelRequest>>,
    events: Mutex<Vec<ModelEvent>>,
}

impl Fake {
    fn new(text: &str) -> Arc<Self> {
        Self::with_events(vec![
            ModelEvent::text_delta(text).unwrap(),
            usage_event(),
            ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            },
        ])
    }

    fn with_events(events: Vec<ModelEvent>) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ModelDescriptor::new(
                "fixture".parse().unwrap(),
                100_000,
                [ReasoningPreference::Auto].into(),
                true,
            )
            .unwrap(),
            requests: Mutex::new(Vec::new()),
            events: Mutex::new(events),
        })
    }

    fn calls(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Model for Fake {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }
    fn start(&self, request: ModelRequest, _: ModelCallContext) -> ModelStartFuture<'_> {
        self.requests.lock().unwrap().push(request);
        let events = std::mem::take(&mut *self.events.lock().unwrap());
        Box::pin(async {
            let stream: ModelStream =
                Box::pin(futures_util::stream::iter(events.into_iter().map(Ok)));
            Ok(stream)
        })
    }
}

fn usage_event() -> ModelEvent {
    ModelEvent::Usage {
        usage: Usage::new(11, 7, 3),
    }
}

fn user_item(text: &str) -> HistoryItem {
    HistoryItem::User(UserHistory {
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        kind: UserMessageKind::Prompt,
        input: UserInput::text(text).unwrap(),
    })
}

fn assistant(parts: Vec<AssistantPart>) -> HistoryItem {
    HistoryItem::Assistant(AssistantHistory {
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        model: "fixture".parse().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: parts,
        provider_replay: None,
        finish_reason: ModelFinishReason::Stop,
        usage: Usage::new(1, 2, 3),
    })
}

fn tool_result(text: &str) -> HistoryItem {
    HistoryItem::ToolResult(ToolResultHistory {
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        call_id: "call_write".parse().unwrap(),
        tool_name: "write".parse().unwrap(),
        outcome: ToolResultOutcome::Failed,
        output: ToolOutput::new(text).unwrap(),
    })
}

fn input(model: &Arc<Fake>, history: Vec<HistoryItem>) -> CompactionInput {
    CompactionInput {
        model: model.clone(),
        reasoning: ReasoningPreference::Auto,
        history: history.into(),
        previous_summary: None,
        previous_covered_item_count: 0,
        project_instructions: BoundedText::new("PROJECT_ONLY_NORMAL_REQUEST").unwrap(),
        tool_schemas: Vec::new(),
        hard_tokens: 100_000,
        safe_before_estimate: false,
        operation_deadline: Instant::now() + Duration::from_secs(30),
        live_usage: None,
    }
}

fn user_text(message: &ModelMessage) -> &str {
    match message {
        ModelMessage::User(text) => text,
        _ => panic!("expected user text"),
    }
}

fn projected(input: &CompactionInput) -> String {
    user_text(&source_message(input, &CancellationToken::new()).unwrap()).to_owned()
}

#[tokio::test]
async fn pi_initial_prompt_preserves_stage_three_source_and_disables_tools() {
    let model = Fake::new("## Goal\nContinue Stage 3");
    let input = input(&model, vec![user_item(REPORT_STAGE_3)]);
    let generated = generate_summary(&input, &CancellationToken::new())
        .await
        .ok()
        .unwrap();
    assert_eq!(model.calls(), 1);
    let requests = model.requests.lock().unwrap();
    let request = &requests[0];
    assert!(request.tools().is_empty());
    assert_eq!(request.messages().len(), 2);
    let ModelMessage::System(system) = &request.messages()[0] else {
        panic!("system")
    };
    assert!(system.contains("Do NOT continue the conversation."));
    assert!(system.contains("Tools are disabled."));
    assert!(!system.contains("PROJECT_ONLY_NORMAL_REQUEST"));
    let text = user_text(&request.messages()[1]);
    assert!(text.contains(REPORT_STAGE_3));
    assert!(text.starts_with(SOURCE_PREFIX));
    assert!(text.ends_with(PI_INITIAL_PROMPT));
    assert!(!text.contains("chunk="));
    for heading in [
        "## Goal",
        "## Constraints & Preferences",
        "### Done",
        "### In Progress",
        "### Blocked",
        "## Key Decisions",
        "## Next Steps",
        "## Critical Context",
    ] {
        assert!(text.contains(heading));
    }
    let usage = generated.utility_usage.unwrap();
    assert_eq!(usage.call_count, 1);
    assert!(usage.complete);
    assert_eq!(usage.usage.unwrap().input_tokens(), Some(11));
}

#[tokio::test]
async fn update_contains_previous_once_and_only_uncovered_history_in_one_call() {
    let model = Fake::new("updated summary");
    let previous = format!("PREVIOUS_SUMMARY {}", REPORT_STAGE_3);
    let mut input = input(
        &model,
        vec![
            user_item("COVERED_DO_NOT_REPLAY"),
            user_item("NEW_PENDING_REQUEST"),
        ],
    );
    input.previous_summary = Some(BoundedText::new(&previous).unwrap());
    input.previous_covered_item_count = 1;
    generate_summary(&input, &CancellationToken::new())
        .await
        .ok()
        .unwrap();
    assert_eq!(model.calls(), 1);
    let text = projected(&input);
    assert_eq!(text.matches("PREVIOUS_SUMMARY").count(), 1);
    assert!(text.contains(&format!(
        "<previous-summary>\n{previous}\n</previous-summary>"
    )));
    assert!(text.contains("NEW_PENDING_REQUEST"));
    assert!(!text.contains("COVERED_DO_NOT_REPLAY"));
    assert!(text.ends_with(PI_UPDATE_PROMPT));
}

#[test]
fn projection_keeps_visible_reasoning_arguments_and_embedded_summary_only() {
    let model = Fake::new("summary");
    let thinking = "thinking é🙂".repeat(400);
    let arguments = json!({"path":"src/example.rs", "content":"arg text ".repeat(800)});
    let mut item = assistant(vec![
        AssistantPart::Text("VISIBLE_ANSWER".to_owned()),
        AssistantPart::Reasoning(
            ReasoningContent::new(
                Some(thinking.clone()),
                Some("VISIBLE_REASONING_SUMMARY".to_owned()),
                Some("OPAQUE_ENCRYPTED".to_owned()),
                Some("OPAQUE_SIGNATURE".to_owned()),
            )
            .unwrap(),
        ),
        AssistantPart::ToolCall(
            ToolCall::new(
                "call_write".parse().unwrap(),
                "write".parse().unwrap(),
                arguments.clone(),
                0,
            )
            .unwrap(),
        ),
        AssistantPart::Reasoning(
            ReasoningContent::new(None, None, Some("ONLY_OPAQUE".to_owned()), None).unwrap(),
        ),
    ]);
    if let HistoryItem::Assistant(value) = &mut item {
        value.provider_replay =
            Some(ProviderReplay::new("test-v1", json!({"marker":"OPAQUE_REPLAY"})).unwrap());
    }
    let input = input(
        &model,
        vec![
            user_item("USER_FULL"),
            item,
            HistoryItem::Summary(SummaryHistory {
                content: BoundedText::new("EMBEDDED_SUMMARY").unwrap(),
            }),
        ],
    );
    let before = serde_json::to_vec(input.history.as_ref()).unwrap();
    let text = projected(&input);
    for retained in [
        "USER_FULL",
        "VISIBLE_ANSWER",
        &thinking,
        "VISIBLE_REASONING_SUMMARY",
        "EMBEDDED_SUMMARY",
        &serde_json::to_string(&arguments).unwrap(),
    ] {
        assert!(text.contains(retained));
    }
    for omitted in [
        "OPAQUE_ENCRYPTED",
        "OPAQUE_SIGNATURE",
        "ONLY_OPAQUE",
        "OPAQUE_REPLAY",
        "request_index",
        "loop_id",
        "usage",
    ] {
        assert!(!text.contains(omitted));
    }
    assert_eq!(before, serde_json::to_vec(input.history.as_ref()).unwrap());
}

#[test]
fn tool_result_head_limit_is_unicode_safe_and_preserves_identity_and_outcome() {
    let model = Fake::new("summary");
    for (text, omitted) in [
        ("a".repeat(2000), 0),
        (format!("{}😀TAIL", "中".repeat(1999)), 4),
        (format!("{}z", "🙂".repeat(2000)), 1),
    ] {
        let input = input(&model, vec![tool_result(&text)]);
        let projection = projected(&input);
        assert!(
            projection
                .contains("[Tool result: name=write, call_id=call_write, outcome=\"failed\"]: ")
        );
        let head: String = text.chars().take(2000).collect();
        assert!(projection.contains(&head));
        if omitted == 0 {
            assert!(!projection.contains("characters truncated"));
        } else {
            assert!(projection.contains(&format!("[... {omitted} more characters truncated]")));
        }
        assert!(!projection.contains("TAIL"));
        let HistoryItem::ToolResult(original) = &input.history[0] else {
            unreachable!()
        };
        assert_eq!(original.output.content().as_str(), text);
    }
}

#[tokio::test]
async fn complete_escaped_request_exact_budget_and_one_byte_over() {
    let model = Fake::new("short summary");
    let mut source = "é🙂\n\"\\".repeat(1000);
    let mut exact = input(&model, vec![user_item(&source)]);
    let bytes = loop {
        exact.history = vec![user_item(&source)].into();
        let fixed = FixedPrompt::new(&exact).unwrap();
        let bytes = fixed
            .utility_request_bytes(&source_message(&exact, &CancellationToken::new()).unwrap())
            .unwrap();
        if bytes % 4 == 0 {
            break bytes;
        }
        source.push('x');
    };
    exact.hard_tokens = (bytes / 4) as u64;
    generate_summary(&exact, &CancellationToken::new())
        .await
        .ok()
        .unwrap();
    assert_eq!(model.calls(), 1);
    let over_model = Fake::new("unused");
    let mut over = exact.clone();
    over.model = over_model.clone();
    over.history = vec![user_item(&(source + "x"))].into();
    let err = generate_summary(&over, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.error, UtilityError::Budget);
    assert!(err.utility_usage.is_none());
    assert_eq!(over_model.calls(), 0);
}

#[tokio::test]
async fn oversize_history_fails_before_call_without_discarding_content() {
    let model = Fake::new("unused");
    let mut input = input(
        &model,
        vec![
            user_item(&"u".repeat(140_000)),
            user_item(&"v".repeat(140_000)),
        ],
    );
    input.previous_summary = Some(BoundedText::new("old summary").unwrap());
    let before = serde_json::to_vec(input.history.as_ref()).unwrap();
    let err = generate_summary(&input, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.error, UtilityError::TooLarge);
    assert!(err.utility_usage.is_none());
    assert_eq!(model.calls(), 0);
    assert_eq!(input.previous_summary.unwrap().as_str(), "old summary");
    assert_eq!(before, serde_json::to_vec(input.history.as_ref()).unwrap());
}

#[tokio::test]
async fn large_tool_arguments_are_not_trimmed_to_force_admission() {
    let model = Fake::new("unused");
    let argument = "write-content".repeat(4000);
    let items = (0..6)
        .map(|i| {
            assistant(vec![AssistantPart::ToolCall(
                ToolCall::new(
                    format!("call_{i}").parse().unwrap(),
                    "write".parse().unwrap(),
                    json!({"content": argument}),
                    0,
                )
                .unwrap(),
            )])
        })
        .collect();
    let input = input(&model, items);
    let err = generate_summary(&input, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.error, UtilityError::TooLarge);
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn normal_request_floor_remains_without_repeating_it_in_summary_prompt() {
    let model = Fake::new("unused");
    let mut input = input(&model, vec![user_item("historical request")]);
    input.project_instructions = BoundedText::new("fixed ".repeat(2000)).unwrap();
    input.hard_tokens = 1000;
    let err = generate_summary(&input, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.error, UtilityError::Budget);
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn fitting_shrunk_summary_above_old_half_window_target_is_not_reduced_again() {
    let model = Fake::new(&"s".repeat(10_000));
    let mut input = input(&model, vec![user_item(&"u".repeat(12_000))]);
    input.hard_tokens = 4000;
    let generated = generate_summary(&input, &CancellationToken::new())
        .await
        .ok()
        .unwrap();
    assert!(generated.after_tokens > input.hard_tokens / 2);
    assert!(generated.after_tokens < generated.before_tokens);
    assert!(generated.after_tokens <= input.hard_tokens);
    assert_eq!(model.calls(), 1);
}

#[tokio::test]
async fn non_shrinking_summary_fails_without_a_second_call() {
    let model = Fake::new(&"s".repeat(12_000));
    let input = input(&model, vec![user_item(&"u".repeat(4000))]);
    let err = generate_summary(&input, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.error, UtilityError::NoProgress);
    assert_eq!(model.calls(), 1);
    assert!(!err.utility_usage.unwrap().complete);
}

#[tokio::test]
async fn invalid_outputs_fail_once_and_keep_observed_usage() {
    let cases = vec![
        (
            vec![
                usage_event(),
                ModelEvent::text_delta("partial").unwrap(),
                ModelEvent::Finish {
                    reason: ModelFinishReason::Length,
                },
            ],
            UtilityError::InvalidResponse,
        ),
        (
            vec![
                usage_event(),
                ModelEvent::ToolCallStart {
                    tool_call_id: "call_bad".parse().unwrap(),
                    tool_name: "read".parse().unwrap(),
                },
            ],
            UtilityError::ToolCall,
        ),
        (
            vec![
                usage_event(),
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ],
            UtilityError::NoProgress,
        ),
        (
            vec![
                usage_event(),
                ModelEvent::text_delta("missing finish").unwrap(),
            ],
            UtilityError::InvalidResponse,
        ),
        (
            std::iter::once(usage_event())
                .chain((0..17).map(|_| ModelEvent::text_delta("x".repeat(4096)).unwrap()))
                .collect(),
            UtilityError::TooLarge,
        ),
    ];
    for (events, expected) in cases {
        let model = Fake::with_events(events);
        let input = input(&model, vec![user_item(REPORT_STAGE_3)]);
        let err = generate_summary(&input, &CancellationToken::new())
            .await
            .err()
            .unwrap();
        assert_eq!(err.error, expected);
        assert_eq!(model.calls(), 1);
        let usage = err.utility_usage.unwrap();
        assert_eq!(usage.call_count, 1);
        assert!(!usage.complete);
        assert_eq!(usage.usage.unwrap().input_tokens(), Some(11));
    }
}

#[tokio::test]
async fn cancellation_and_deadline_fail_before_model_call() {
    let model = Fake::new("unused");
    let mut input = input(&model, vec![user_item(REPORT_STAGE_3)]);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        generate_summary(&input, &cancellation)
            .await
            .err()
            .unwrap()
            .error,
        UtilityError::Cancelled
    );
    input.operation_deadline = Instant::now();
    assert_eq!(
        generate_summary(&input, &CancellationToken::new())
            .await
            .err()
            .unwrap()
            .error,
        UtilityError::Timeout
    );
    assert_eq!(model.calls(), 0);
}

#[test]
fn source_writer_checks_bounds_and_interrupts_between_writes() {
    let cancellation = CancellationToken::new();
    let mut writer = SourceWriter::new(&cancellation, Instant::now() + Duration::from_secs(30));
    writer.append(&"x".repeat(BoundedText::MAX_BYTES)).unwrap();
    assert_eq!(writer.bytes.len(), BoundedText::MAX_BYTES);
    assert_eq!(writer.append("x"), Err(UtilityError::TooLarge));
    assert_eq!(writer.bytes.len(), BoundedText::MAX_BYTES);
    let mut writer = SourceWriter::new(&cancellation, Instant::now() + Duration::from_secs(30));
    writer.append("first").unwrap();
    cancellation.cancel();
    assert_eq!(writer.append("second"), Err(UtilityError::Cancelled));
    let cancellation = CancellationToken::new();
    let mut writer = SourceWriter::new(&cancellation, Instant::now() + Duration::from_secs(30));
    writer.append("first").unwrap();
    writer.deadline = Instant::now();
    assert_eq!(writer.append("second"), Err(UtilityError::Timeout));
}

#[test]
fn invalid_covered_prefix_is_rejected() {
    let model = Fake::new("unused");
    let mut input = input(&model, vec![user_item("source")]);
    input.previous_covered_item_count = 2;
    assert!(matches!(
        source_message(&input, &CancellationToken::new()),
        Err(UtilityError::InvalidResponse)
    ));
}

#[test]
fn recent_tail_selection_is_bounded_and_never_restores_covered_items() {
    let cancel = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let choose = |history: &[HistoryItem], covered| {
        recent_tail_start(history, covered, &cancel, deadline).unwrap()
    };
    assert_eq!(choose(&[], 0), 0);
    assert_eq!(choose(&[user_item("short")], 0), 0);
    assert_eq!(choose(&[user_item(&"x".repeat(100_000))], 0), 0);
    let history = vec![
        user_item("covered never resurrects"),
        user_item("old candidate"),
        user_item(&"x".repeat(41_000)),
        user_item(&"y".repeat(41_000)),
    ];
    assert_eq!(choose(&history, 0), 2);
    assert_eq!(choose(&history, 3), 3);
    assert_eq!(choose(&[user_item(&"é\n\"".repeat(20_000))], 0), 0);
    cancel.cancel();
    assert_eq!(
        recent_tail_start(&history, 0, &cancel, deadline),
        Err(UtilityError::Cancelled)
    );
    assert_eq!(
        recent_tail_start(&history, 0, &CancellationToken::new(), Instant::now()),
        Err(UtilityError::Timeout)
    );
}

#[tokio::test]
async fn tail_generation_keeps_source_before_and_after_ranges_distinct() {
    let model = Fake::new("small updated summary");
    let mut input = input(
        &model,
        vec![
            user_item("COVERED_RAW_NEVER_REPLAY"),
            user_item(&format!("NEW_PREFIX {}", "old details ".repeat(2000))),
            user_item(&format!("TAIL_USER {}", "recent text ".repeat(8000))),
            assistant(vec![AssistantPart::Text("TAIL_ANSWER".into())]),
        ],
    );
    input.previous_summary = Some(BoundedText::new("PREVIOUS_SUMMARY").unwrap());
    input.previous_covered_item_count = 1;
    let history_bytes = serde_json::to_vec(input.history.as_ref()).unwrap();
    let generated = generate_summary_with_tail(&input, 2, &CancellationToken::new())
        .await
        .ok()
        .unwrap();
    let calls = model.requests.lock().unwrap();
    let source = user_text(&calls[0].messages()[1]);
    assert_eq!(source.matches("PREVIOUS_SUMMARY").count(), 1);
    assert!(source.contains("NEW_PREFIX"));
    assert!(!source.contains("COVERED_RAW_NEVER_REPLAY"));
    assert!(!source.contains("TAIL_USER"));
    assert!(!source.contains("TAIL_ANSWER"));
    assert_eq!(
        generated.before_tokens,
        estimate_before_tokens(&input).unwrap()
    );
    assert_eq!(
        generated.after_tokens,
        estimate_after_tokens_with_tail(&input, &generated.content, &input.history[2..]).unwrap()
    );
    println!(
        "COMPACT_BUDGET_EVIDENCE {}",
        json!({
            "before_tokens": generated.before_tokens,
            "after_tokens": generated.after_tokens,
            "utility_request": &calls[0],
            "before_request": normal_request(&input, input.previous_summary.as_ref(), &input.history[1..]).unwrap(),
            "after_request": normal_request(&input, Some(&generated.content), &input.history[2..]).unwrap(),
        })
    );
    assert!(generated.after_tokens > 20_000);
    assert!(generated.after_tokens < generated.before_tokens);
    assert_eq!(
        serde_json::to_vec(input.history.as_ref()).unwrap(),
        history_bytes
    );
}

#[tokio::test]
async fn retained_tail_and_fixed_request_budget_fail_before_utility() {
    let model = Fake::new("unused");
    let mut input = input(
        &model,
        vec![
            user_item(&"old ".repeat(2000)),
            user_item(&"tail ".repeat(20_000)),
        ],
    );
    input.hard_tokens = 20_000;
    let error = generate_summary_with_tail(&input, 1, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.error, UtilityError::Budget);
    assert_eq!(model.calls(), 0);
    assert!(error.utility_usage.is_none());
    input.hard_tokens = 30_000;
    input.project_instructions = BoundedText::new("fixed ".repeat(10_000)).unwrap();
    let error = generate_summary_with_tail(&input, 1, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.error, UtilityError::Budget);
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn complete_after_budget_counts_escaped_summary_with_tail() {
    let model = Fake::new(&"\"".repeat(6000));
    let mut input = input(
        &model,
        vec![
            user_item(&"old ".repeat(6000)),
            user_item(&"tail ".repeat(16_000)),
        ],
    );
    let floor = estimate_after_tokens_with_tail(
        &input,
        &BoundedText::new("x").unwrap(),
        &input.history[1..],
    )
    .unwrap();
    input.hard_tokens = floor + 2000;
    let error = generate_summary_with_tail(&input, 1, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.error, UtilityError::NoProgress);
    assert_eq!(model.calls(), 1);
}

#[tokio::test]
async fn summary_larger_than_new_prefix_is_not_progress_despite_large_tail() {
    let model = Fake::new(&"summary bigger than prefix ".repeat(100));
    let input = input(
        &model,
        vec![
            user_item("short old prefix"),
            user_item(&"tail ".repeat(20_000)),
        ],
    );
    let error = generate_summary_with_tail(&input, 1, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.error, UtilityError::NoProgress);
    assert_eq!(model.calls(), 1);
}

#[test]
fn recent_tail_target_includes_exact_twenty_thousand_token_boundary() {
    let probe = user_item("x");
    let overhead = serde_json::to_vec(&probe).unwrap().len() - 1;
    let exact = user_item(&"x".repeat(80_000 - overhead));
    assert_eq!(serde_json::to_vec(&exact).unwrap().len(), 80_000);
    let below = user_item(&"x".repeat(79_996 - overhead));
    let choose = |last| {
        recent_tail_start(
            &[user_item("older"), last],
            0,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
    };
    assert_eq!(choose(exact), 1);
    assert_eq!(choose(below), 0);
}

#[tokio::test]
async fn retained_tail_fits_without_schemas_but_fixed_schemas_prevent_utility() {
    let model = Fake::new("summary");
    let mut input = input(
        &model,
        vec![
            user_item(&"old detail ".repeat(1000)),
            user_item(&"tail ".repeat(20_000)),
        ],
    );
    let floor = estimate_after_tokens_with_tail(
        &input,
        &BoundedText::new("x").unwrap(),
        &input.history[1..],
    )
    .unwrap();
    input.hard_tokens = floor + 128;
    assert!(summary_budget_with_tail(&input, &input.history[1..]).is_ok());
    let accepted = generate_summary_with_tail(&input, 1, &CancellationToken::new())
        .await
        .ok()
        .unwrap();
    assert!(accepted.after_tokens <= input.hard_tokens);
    assert_eq!(model.calls(), 1);
    input.tool_schemas.push(ToolSpec::new("read".parse().unwrap(), "schema budget fixture", json!({
        "type":"object",
        "properties":{"path":{"type":"string", "description":"schema detail ".repeat(1000)}},
        "required":["path"], "additionalProperties":false,
    })).unwrap());
    let error = generate_summary_with_tail(&input, 1, &CancellationToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.error, UtilityError::Budget);
    assert!(error.utility_usage.is_none());
    assert_eq!(
        model.calls(),
        1,
        "schemas must reject before another utility call"
    );
}
