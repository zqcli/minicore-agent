use super::*;
use crate::test_support::{ScriptedModel, Step};
use minicore_runtime::ToolCallId;
use minicore_runtime::execution::UserInput;
use minicore_runtime::history::{
    AssistantHistory, ToolResultHistory, UserHistory, UserMessageKind,
};
use minicore_runtime::model::{
    AssistantPart, Model, ModelEvent, ModelFinishReason, ModelStream, ToolCall, Usage,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

fn summary_model(window: u64, output: &str, gate: Option<Arc<Notify>>) -> Arc<ScriptedModel> {
    let events = vec![
        ModelEvent::text_delta(output).unwrap(),
        ModelEvent::Usage {
            usage: Usage::new(7, 3, 0)
                .with_cache_read_tokens(Some(0))
                .with_cache_write_tokens(Some(0)),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::Stop,
        },
    ];
    ScriptedModel::with_reasoning("main", window, [ReasoningPreference::Auto].into(), [])
        .with_fallback(move || match &gate {
            Some(gate) => {
                let gate = Arc::clone(gate);
                Step::Custom(Box::new(move |_, _| {
                    Box::pin(async move {
                        gate.notify_one();
                        std::future::pending().await
                    })
                }))
            }
            None => Step::Events(events.clone()),
        })
}

fn partial_usage_model(gate: Arc<Notify>) -> Arc<ScriptedModel> {
    ScriptedModel::with_reasoning("main", 8_000, [ReasoningPreference::Auto].into(), [])
        .with_fallback(move || {
            let gate = Arc::clone(&gate);
            Step::Custom(Box::new(move |_, _| {
                Box::pin(async move {
                    use futures_util::StreamExt;
                    let stream = futures_util::stream::iter(vec![Ok(ModelEvent::Usage {
                        usage: Usage::new(7, 3, 0).with_provider_total_tokens(Some(10)),
                    })])
                    .chain(futures_util::stream::once(async move {
                        gate.notify_one();
                        std::future::pending::<
                            Result<ModelEvent, minicore_runtime::model::ModelError>,
                        >()
                        .await
                    }));
                    Ok(Box::pin(stream) as ModelStream)
                })
            }))
        })
}

fn provider_timeout_model() -> Arc<ScriptedModel> {
    use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
    use minicore_runtime::model::{DeliveryState, ModelError, ModelErrorKind};
    let summary = DiagnosticSummary::new(
        DiagnosticCode::ModelTimeout,
        DiagnosticCategory::Model,
        BoundedText::new("utility timed out").unwrap(),
        false,
    )
    .unwrap();
    ScriptedModel::with_reasoning(
        "main",
        8_000,
        [ReasoningPreference::Auto].into(),
        [Step::Fail(ModelError::permanent(
            ModelErrorKind::Timeout,
            DeliveryState::NotStarted,
            summary,
        ))],
    )
}

fn context(model: &Arc<ScriptedModel>) -> AutoContext {
    AutoContext {
        binding: crate::compaction::auto::AutoContextBinding {
            model: Arc::clone(model) as Arc<dyn Model>,
            budget: Arc::new(crate::models::tests::DefaultProviderBudget),
            policy: super::super::CompactionPolicy {
                enabled: true,
                trigger_percent: 95,
                target_percent: 50,
            },
        },
        state: CompactionState::new(),
    }
}
fn user(loop_id: LoopId, text: &str, steer: bool) -> HistoryItem {
    HistoryItem::User(UserHistory {
        loop_id,
        kind: if steer {
            UserMessageKind::Steering
        } else {
            UserMessageKind::Prompt
        },
        input: UserInput::text(text).unwrap(),
    })
}
fn text(loop_id: LoopId, request_index: u32, value: &str) -> HistoryItem {
    HistoryItem::Assistant(AssistantHistory {
        loop_id,
        request_index,
        model: "main".parse().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: vec![AssistantPart::Text(value.into())],
        provider_replay: None,
        finish_reason: ModelFinishReason::Stop,
        usage: Usage::default().with_provider_total_tokens(Some(200_000)),
    })
}
fn exchange(loop_id: LoopId, index: u32, bytes: usize) -> Vec<HistoryItem> {
    let call_id = ToolCallId::new(format!("read-{index}")).unwrap();
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
                    json!({"path":"large.txt"}),
                    0,
                )
                .unwrap(),
            )],
            provider_replay: None,
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default().with_provider_total_tokens(Some(200_000)),
        }),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index: index,
            call_id,
            tool_name: "read".parse().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("x".repeat(bytes)).unwrap(),
        }),
    ]
}
#[allow(clippy::too_many_arguments)]
fn input<'a>(
    auto: &'a AutoContext,
    system: &'a BoundedText,
    summary: Option<&'a BoundedText>,
    base: &'a [HistoryItem],
    appended: &'a [HistoryItem],
    loop_id: LoopId,
    index: u32,
    cancel: &'a CancellationToken,
) -> ProjectionInput<'a> {
    ProjectionInput {
        auto,
        system,
        summary,
        base,
        appended,
        loop_id,
        request_index: index,
        cancellation: cancel,
        deadline: Instant::now() + Duration::from_secs(10),
        tools: &[],
        reasoning: ReasoningPreference::Auto,
        limits: ModelLimits::default(),
        rejected_body_bytes: None,
    }
}

#[tokio::test]
async fn low_reported_usage_does_not_compact_large_history_without_an_in_memory_anchor() {
    let model = summary_model(8_000, "summary", None);
    let auto = context(&model);
    let mut base = exchange(LoopId::new().unwrap(), 0, 80_000);
    if let HistoryItem::Assistant(assistant) = &mut base[0] {
        assistant.usage = Usage::default().with_provider_total_tokens(Some(100));
    }
    let current = LoopId::new().unwrap();
    let appended = vec![user(current, "continue", false)];
    let system = BoundedText::new("system").unwrap();
    let cancel = CancellationToken::new();
    let result = consolidate(
        input(&auto, &system, None, &base, &appended, current, 0, &cancel),
        false,
    )
    .await
    .unwrap();
    assert!(
        !result.compacted,
        "reported usage below trigger must not compact"
    );
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn a_large_unmeasured_tool_result_does_not_trigger_a_mid_turn_compaction() {
    let model = summary_model(8_000, "unused", None);
    let auto = context(&model);
    let current = LoopId::new().unwrap();
    let mut appended = vec![user(current, "read", false)];
    appended.extend(exchange(current, 0, 80_000));
    if let HistoryItem::Assistant(assistant) = &mut appended[1] {
        assistant.usage = Usage::default().with_provider_total_tokens(Some(100));
    }
    let system = BoundedText::new("system").unwrap();
    let cancel = CancellationToken::new();
    let result = consolidate(
        input(&auto, &system, None, &[], &appended, current, 1, &cancel),
        false,
    )
    .await
    .unwrap();
    assert!(!result.compacted);
    assert_eq!(model.calls(), 0);
    assert!(result.request.messages().iter().any(|message| matches!(message, ModelMessage::Tool { output, .. } if output.content().byte_len() == 80_000)));
}

#[tokio::test]
async fn default_ninety_five_projects_large_tool_history_without_raw_ticket_cap() {
    let model = summary_model(
        172_000,
        "All historical reads completed; preserve their results.",
        None,
    );
    let auto = context(&model);
    let system = BoundedText::new("system").unwrap();
    let old = LoopId::new().unwrap();
    let current = LoopId::new().unwrap();
    let base = (0..6)
        .flat_map(|index| exchange(old, index, 115_000))
        .collect::<Vec<_>>();
    let appended = vec![user(current, "new input exact ✓", false)];
    let cancellation = CancellationToken::new();
    let result = consolidate(
        input(
            &auto,
            &system,
            None,
            &base,
            &appended,
            current,
            0,
            &cancellation,
        ),
        false,
    )
    .await
    .unwrap()
    .into_compacted()
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(result.request.messages().iter().any(
        |message| matches!(message, ModelMessage::User(value) if value.as_ref() == "new input exact ✓")
    ));
    assert!(
        !result
            .request
            .messages()
            .iter()
            .any(|message| matches!(message, ModelMessage::Tool { .. }))
    );
    assert_eq!(result.usage.unwrap().call_count, 1);
    let requests = model.requests.lock().unwrap();
    assert!(requests[0].tools().is_empty());
    assert!(serde_json::to_vec(&requests[0].messages()).unwrap().len() < 30_000);
    assert_eq!(
        auto.state.automatic_view().last.unwrap().outcome,
        "compacted"
    );
    assert!(auto.state.automatic_view().current.is_none());
}

#[tokio::test]
async fn successful_reduction_waits_for_a_new_report_even_when_its_estimate_stays_high() {
    let model = summary_model(8_000, &"s".repeat(30_600), None);
    let auto = context(&model);
    let system = BoundedText::new("s").unwrap();
    let old = LoopId::new().unwrap();
    let current = LoopId::new().unwrap();
    let base = (0..350)
        .map(|index| text(old, index, "old fact"))
        .collect::<Vec<_>>();
    let appended = vec![user(current, "original", false)];
    let cancel = CancellationToken::new();
    let result = consolidate(
        input(&auto, &system, None, &base, &appended, current, 0, &cancel),
        false,
    )
    .await
    .unwrap()
    .into_compacted()
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let after = auto
        .budget
        .estimate_request_tokens(&result.request, None, None)
        .unwrap();
    assert!(
        after
            >= auto
                .policy
                .budget(model.descriptor().context_window)
                .trigger_tokens,
        "an over-trigger estimate must not substitute for superseded usage"
    );
    let again = input(&auto, &system, None, &base, &appended, current, 1, &cancel);
    assert!(
        consolidate(again, false)
            .await
            .unwrap()
            .into_compacted()
            .is_none()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn in_run_preserves_newest_exchange_and_every_current_user_verbatim() {
    let model = summary_model(9_000, "Older tool results summarized.", None);
    let auto = context(&model);
    let system = BoundedText::new("system").unwrap();
    let current = LoopId::new().unwrap();
    let cancel = CancellationToken::new();
    let mut appended = vec![user(current, "keep exact A", false)];
    appended.extend(exchange(current, 0, 25_000));
    appended.push(user(current, "steer exact B", true));
    appended.extend(exchange(current, 1, 15_000));
    let result = consolidate(
        input(&auto, &system, None, &[], &appended, current, 2, &cancel),
        false,
    )
    .await
    .unwrap()
    .into_compacted()
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(super::super::recovery::validate_clean_tool_exchanges(
        result.request.messages()
    ));
    for expected in ["keep exact A", "steer exact B"] {
        assert!(result.request.messages().iter().any(
            |message| matches!(message, ModelMessage::User(value) if value.as_ref() == expected)
        ));
    }
    let outputs = result
        .request
        .messages()
        .iter()
        .filter_map(|message| match message {
            ModelMessage::Tool { output, .. } => Some(output.content().as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(outputs, vec!["x".repeat(15_000)]);
    let source = serde_json::to_string(model.requests.lock().unwrap()[0].messages()).unwrap();
    assert!(!source.contains("keep exact A") && !source.contains("steer exact B"));
}

#[tokio::test]
async fn summary_only_projection_does_not_trigger_without_reported_usage() {
    let model = summary_model(12_000, "Refreshed old summary", None);
    let mut auto = context(&model);
    auto.binding.policy.trigger_percent = 60;
    let system = BoundedText::new("s").unwrap();
    let summary = BoundedText::new("old summary ".repeat(2_600)).unwrap();
    let current = LoopId::new().unwrap();
    let appended = vec![user(current, "new", false)];
    let cancel = CancellationToken::new();
    let result = consolidate(
        input(
            &auto,
            &system,
            Some(&summary),
            &[],
            &appended,
            current,
            0,
            &cancel,
        ),
        false,
    )
    .await
    .unwrap();
    assert!(!result.compacted);
    assert_eq!(model.calls(), 0);
    assert!(
        serde_json::to_string(result.request.messages())
            .unwrap()
            .contains("old summary old summary")
    );
}

#[tokio::test]
async fn irreducible_floor_and_empty_source_do_not_spend_utility() {
    let model = summary_model(4_000, "summary", None);
    let auto = context(&model);
    let system = BoundedText::new("s").unwrap();
    let current = LoopId::new().unwrap();
    let cancel = CancellationToken::new();
    let mut appended = vec![user(current, &"u".repeat(17_000), false)];
    appended.extend(exchange(current, 0, 4_000));
    appended.extend(exchange(current, 1, 4_000));
    assert!(
        consolidate(
            input(&auto, &system, None, &[], &appended, current, 2, &cancel),
            false
        )
        .await
        .unwrap()
        .into_compacted()
        .is_none()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(
        consolidate(
            input(
                &auto,
                &system,
                None,
                &[],
                &appended[..1],
                current,
                0,
                &cancel
            ),
            false
        )
        .await
        .unwrap()
        .into_compacted()
        .is_none()
    );
}

#[tokio::test]
async fn dropped_preparation_clears_current_before_any_following_event() {
    let entered = Arc::new(Notify::new());
    let model = summary_model(8_000, "unused", Some(Arc::clone(&entered)));
    let auto = context(&model);
    let system = BoundedText::new("s").unwrap();
    let current = LoopId::new().unwrap();
    let base = exchange(LoopId::new().unwrap(), 0, 40_000);
    let appended = vec![user(current, "new", false)];
    let cancel = CancellationToken::new();
    let mut pending = Box::pin(consolidate(
        input(&auto, &system, None, &base, &appended, current, 0, &cancel),
        false,
    ));
    tokio::select! { _ = entered.notified() => {}, _ = &mut pending => panic!("must be pending") }
    assert!(auto.state.automatic_view().current.is_some());
    cancel.cancel();
    drop(pending);
    let observation = auto.state.automatic_view();
    assert!(observation.current.is_none());
    let last = observation.last.unwrap();
    assert_eq!(last.outcome, "cancelled");
    assert_eq!(last.utility_usage.unwrap().call_count, 1);
}

#[tokio::test]
async fn no_progress_preserves_projection_and_accounts_single_call() {
    let model = summary_model(15_000, &"g".repeat(55_000), None);
    let mut auto = context(&model);
    auto.binding.policy.trigger_percent = 50;
    let system = BoundedText::new("s").unwrap();
    let current = LoopId::new().unwrap();
    let old = LoopId::new().unwrap();
    let base = exchange(old, 0, 40_000);
    let appended = vec![user(current, "new", false)];
    let cancel = CancellationToken::new();
    let error = consolidate(
        input(&auto, &system, None, &base, &appended, current, 0, &cancel),
        false,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.error, UtilityError::NoProgress);
    assert_eq!(error.usage.unwrap().call_count, 1);
    assert!(auto.state.threshold.lock().unwrap().consolidated.is_none());
    assert!(auto.state.automatic_view().current.is_none());
    assert!(
        consolidate(
            input(&auto, &system, None, &base, &appended, current, 1, &cancel),
            false
        )
        .await
        .unwrap()
        .into_compacted()
        .is_none()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn latest_usage_is_unknown_when_missing_or_superseded_but_zero_is_known() {
    let state = CompactionState::new();
    let owner = LoopId::new().unwrap();
    let mut history = vec![text(owner, 0, "old")];
    assert_eq!(state.reported_context(history.iter()), Some(200_000));
    let mut latest = text(owner, 1, "new");
    if let HistoryItem::Assistant(assistant) = &mut latest {
        assistant.usage = Usage::default();
    }
    history.push(latest);
    assert_eq!(state.reported_context(history.iter()), None);
    if let HistoryItem::Assistant(assistant) = &mut history[1] {
        assistant.usage = Usage::default().with_provider_total_tokens(Some(0));
    }
    assert_eq!(state.reported_context(history.iter()), Some(0));
    *state.superseded_usage.lock().unwrap() = Some((owner, 1));
    assert_eq!(state.reported_context(history.iter()), None);
    history.push(text(owner, 2, "fresh"));
    assert_eq!(state.reported_context(history.iter()), Some(200_000));
}

#[tokio::test]
async fn empty_system_and_empty_old_history_are_a_clean_first_boundary_noop() {
    let model = summary_model(4_000, "unused", None);
    let auto = context(&model);
    let system = BoundedText::new("").unwrap();
    let current = LoopId::new().unwrap();
    let appended = vec![user(current, "new input", false)];
    let cancel = CancellationToken::new();
    let result = consolidate(
        input(&auto, &system, None, &[], &appended, current, 0, &cancel),
        false,
    )
    .await
    .unwrap();
    assert!(!result.compacted);
    assert_eq!(result.request.messages().len(), 1);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(auto.state.automatic_view().last.is_none());
}

#[tokio::test]
async fn recovery_uses_captured_rejected_body_ceiling_before_install() {
    let model = summary_model(15_000, "summary", None);
    let auto = context(&model);
    let system = BoundedText::new("s").unwrap();
    let current = LoopId::new().unwrap();
    let base = exchange(LoopId::new().unwrap(), 0, 40_000);
    let appended = vec![user(current, "u", false)];
    let cancel = CancellationToken::new();
    let mut attempt = input(&auto, &system, None, &base, &appended, current, 0, &cancel);
    attempt.rejected_body_bytes = Some(0);
    assert_eq!(
        consolidate(attempt, true).await.err().unwrap().error,
        UtilityError::NoProgress
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let mut attempt = input(&auto, &system, None, &base, &appended, current, 0, &cancel);
    attempt.rejected_body_bytes = Some(1);
    let failure = consolidate(attempt, true).await.err().unwrap();
    assert_eq!(failure.error, UtilityError::NoProgress);
    assert_eq!(failure.usage.unwrap().call_count, 1);
    assert!(auto.state.threshold.lock().unwrap().consolidated.is_none());
}

#[tokio::test]
async fn dropped_preparation_preserves_live_partial_utility_usage() {
    let entered = Arc::new(Notify::new());
    let model = partial_usage_model(Arc::clone(&entered));
    let auto = context(&model);
    let system = BoundedText::new("s").unwrap();
    let current = LoopId::new().unwrap();
    let base = exchange(LoopId::new().unwrap(), 0, 40_000);
    let appended = vec![user(current, "new", false)];
    let cancel = CancellationToken::new();
    let mut pending = Box::pin(consolidate(
        input(&auto, &system, None, &base, &appended, current, 0, &cancel),
        false,
    ));
    tokio::select! { _ = entered.notified() => {}, _ = &mut pending => panic!("expected partial stream to remain open") }
    drop(pending);
    let view = auto.state.automatic_view();
    assert!(view.current.is_none());
    let usage = view.last.unwrap().utility_usage.unwrap();
    assert_eq!(usage.call_count, 1);
    assert!(!usage.complete);
    assert_eq!(usage.usage.unwrap().provider_total_tokens(), Some(10));
}

#[test]
fn settlement_qualifier_is_idempotent_and_preserves_failed_attempt_identity() {
    let model = summary_model(8_000, "summary", None);
    let auto = context(&model);
    let loop_id = LoopId::new().unwrap();
    let original = AutomaticCompactionObservation {
        operation_id: "auto-observed".into(),
        loop_id: Some(loop_id),
        request_index: Some(3),
        before_tokens: Some(8_000),
        after_tokens: None,
        utility_before_tokens: None,
        utility_after_tokens: None,
        hard_tokens: 8_000,
        trigger_tokens: 7_600,
        target_tokens: 4_000,
        utility_usage: Some(CompactionUtilityUsage {
            call_count: 1,
            complete: false,
            usage: None,
        }),
        outcome: "no_progress".into(),
    };
    auto.state.threshold.lock().unwrap().observation.last = Some(original.clone());
    auto.state
        .note_settlement_failure(LoopId::new().unwrap(), false);
    assert_eq!(auto.state.automatic_view().last, Some(original.clone()));
    auto.state.note_settlement_failure(loop_id, false);
    auto.state.note_settlement_failure(loop_id, false);
    let mut expected = original;
    expected.outcome = "no_progress_settlement_failed".into();
    assert_eq!(auto.state.automatic_view().last, Some(expected));
}

impl ProjectionResult {
    fn into_compacted(self) -> Option<Self> {
        self.compacted.then_some(self)
    }
}

#[tokio::test]
async fn utility_timeout_preserves_a_fitting_request_but_not_an_overfull_one() {
    for (fits, provider_timeout) in [(true, false), (false, false), (true, true), (false, true)] {
        let model = if provider_timeout {
            provider_timeout_model()
        } else {
            summary_model(8_000, "unused", Some(Arc::new(Notify::new())))
        };
        let mut auto = context(&model);
        auto.binding.policy.trigger_percent = 50;
        let current = LoopId::new().unwrap();
        let base = exchange(
            LoopId::new().unwrap(),
            0,
            if fits { 20_000 } else { 40_000 },
        );
        let appended = vec![user(current, "keep", false)];
        let system = BoundedText::new("system").unwrap();
        let cancel = CancellationToken::new();
        let mut request = input(&auto, &system, None, &base, &appended, current, 0, &cancel);
        request.deadline = Instant::now() + Duration::from_millis(200);
        let result = consolidate(request, false).await;
        if fits {
            let result = result.unwrap();
            assert!(!result.compacted);
            assert!(result.request.messages().iter().any(|message| matches!(message, ModelMessage::Tool { output, .. } if output.content().byte_len() == 20_000)));
        } else {
            assert_eq!(result.err().unwrap().error, UtilityError::Timeout);
        }
        let observation = auto.state.automatic_view().last.unwrap();
        assert_eq!(observation.outcome, "timeout");
        assert_eq!(observation.utility_usage.unwrap().call_count, 1);
        assert_eq!(model.calls(), 1);
    }
}

#[tokio::test]
async fn cancellation_releases_source_for_retry_and_usage_accumulates_per_turn() {
    let gate = Arc::new(Notify::new());
    let model = partial_usage_model(Arc::clone(&gate));
    let auto = context(&model);
    let current = LoopId::new().unwrap();
    let base = exchange(LoopId::new().unwrap(), 0, 40_000);
    let appended = vec![user(current, "keep", false)];
    let system = BoundedText::new("system").unwrap();
    for index in 0..2 {
        let cancel = CancellationToken::new();
        let future = consolidate(
            input(
                &auto, &system, None, &base, &appended, current, index, &cancel,
            ),
            false,
        );
        tokio::pin!(future);
        tokio::select! {
            _ = gate.notified() => {},
            result = &mut future => panic!("utility did not start: {:?}", result.err()),
        }
        cancel.cancel();
        assert_eq!(future.await.err().unwrap().error, UtilityError::Cancelled);
        let usage = auto
            .state
            .automatic_view()
            .last
            .unwrap()
            .utility_usage
            .unwrap();
        assert_eq!(usage.call_count, index + 1);
        assert_eq!(
            usage.usage.unwrap().provider_total_tokens(),
            Some(10 * u64::from(index + 1))
        );
        assert!(!usage.complete);
    }
}
