use super::*;
use minicore_runtime::ToolCallId;
use minicore_runtime::execution::UserInput;
use minicore_runtime::history::{
    AssistantHistory, ToolResultHistory, UserHistory, UserMessageKind,
};
use minicore_runtime::model::{
    AssistantPart, Model, ModelCallContext, ModelEvent, ModelFinishReason, ModelStartFuture,
    ModelStream, ToolCall, Usage,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

struct Stub {
    descriptor: ModelDescriptor,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    output: String,
    gate: Option<Arc<Notify>>,
    partial_usage: bool,
}
impl Stub {
    fn new(window: u64, output: &str, gate: Option<Arc<Notify>>) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ModelDescriptor::new(
                "main".parse().unwrap(),
                window,
                BTreeSet::from([ReasoningPreference::Auto]),
                true,
            )
            .unwrap(),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            output: output.into(),
            gate,
            partial_usage: false,
        })
    }
}
impl Model for Stub {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }
    fn start(&self, request: ModelRequest, _: ModelCallContext) -> ModelStartFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        Box::pin(async move {
            if self.partial_usage {
                use futures_util::StreamExt;
                let gate = Arc::clone(self.gate.as_ref().unwrap());
                let stream = futures_util::stream::iter(vec![Ok(ModelEvent::Usage {
                    usage: Usage::new(7, 3, 0).with_provider_total_tokens(Some(10)),
                })])
                .chain(futures_util::stream::once(async move {
                    gate.notify_one();
                    std::future::pending::<Result<ModelEvent, minicore_runtime::model::ModelError>>(
                    )
                    .await
                }));
                return Ok(Box::pin(stream) as ModelStream);
            }
            if let Some(gate) = &self.gate {
                gate.notify_one();
                std::future::pending::<()>().await;
            }
            let events = vec![
                Ok(ModelEvent::text_delta(self.output.clone()).unwrap()),
                Ok(ModelEvent::Usage {
                    usage: Usage::new(7, 3, 0)
                        .with_cache_read_tokens(Some(0))
                        .with_cache_write_tokens(Some(0)),
                }),
                Ok(ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                }),
            ];
            Ok(Box::pin(futures_util::stream::iter(events)) as ModelStream)
        })
    }
}
fn context(model: &Arc<Stub>) -> AutoContext {
    AutoContext {
        model: Arc::clone(model) as Arc<dyn Model>,
        budget: Arc::new(crate::models::DefaultProviderBudget),
        policy: super::super::CompactionPolicy {
            enabled: true,
            trigger_percent: 95,
            target_percent: 50,
        },
        max_prompt_messages: 4096,
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
        usage: Usage::default(),
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
            usage: Usage::default(),
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
async fn default_ninety_five_projects_large_tool_history_without_raw_ticket_cap() {
    let model = Stub::new(
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
    assert!(!super::super::recovery_source_is_safe(
        &system,
        None,
        &base,
        &appended,
        &[]
    ));
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
        |message| matches!(message, ModelMessage::User(value) if value == "new input exact ✓")
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
async fn many_small_sources_use_one_summary_and_generated_output_does_not_change_source_identity() {
    let model = Stub::new(8_000, &"s".repeat(30_600), None);
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
        "fixture must remain over the unchanged threshold to test source deduplication"
    );
    let again = input(&auto, &system, None, &base, &appended, current, 1, &cancel);
    again.auto.state.threshold.lock().unwrap().issued = None;
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
    let model = Stub::new(9_000, "Older tool results summarized.", None);
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
        assert!(
            result
                .request
                .messages()
                .iter()
                .any(|message| matches!(message, ModelMessage::User(value) if value == expected))
        );
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
async fn summary_only_projection_can_refresh_after_reopen_or_smaller_model() {
    let model = Stub::new(12_000, "Refreshed old summary", None);
    let mut auto = context(&model);
    auto.policy.trigger_percent = 60;
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
    .unwrap()
    .into_compacted()
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(
        serde_json::to_string(result.request.messages())
            .unwrap()
            .contains("Refreshed old summary")
    );
    let settled = auto
        .state
        .settled_emergency_summary(current, &[], &appended, Some(&summary))
        .unwrap()
        .unwrap();
    assert!(settled.as_str().contains("Refreshed old summary"));
    assert!(!settled.as_str().contains("old summary old summary"));
}

#[tokio::test]
async fn irreducible_floor_and_empty_source_do_not_spend_utility() {
    let model = Stub::new(4_000, "summary", None);
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
    let model = Stub::new(8_000, "unused", Some(Arc::clone(&entered)));
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
    let model = Stub::new(15_000, &"g".repeat(55_000), None);
    let mut auto = context(&model);
    auto.policy.trigger_percent = 50;
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
fn usage_anchor_requires_matching_framing_and_projection_generation() {
    let model = Stub::new(8_000, "summary", None);
    let auto = context(&model);
    let current = LoopId::new().unwrap();
    let request = ModelRequest::new(
        vec![
            ModelMessage::system("s").unwrap(),
            ModelMessage::user("u").unwrap(),
        ],
        vec![],
        ModelLimits::default(),
        ReasoningPreference::Auto,
    )
    .unwrap();
    auto.state
        .note_issued_projection(current, 0, model.descriptor(), &request, &*auto.budget);
    let mut assistant = text(current, 0, "answer");
    if let HistoryItem::Assistant(value) = &mut assistant {
        value.usage = Usage::default().with_provider_total_tokens(Some(7_999));
    }
    assert_eq!(
        auto.state.threshold_estimate(
            model.descriptor(),
            &request,
            &[&assistant],
            123,
            &*auto.budget
        ),
        7_999
    );
    let other_binding = Arc::new(crate::models::DefaultProviderBudget);
    assert_eq!(
        auto.state.threshold_estimate(
            model.descriptor(),
            &request,
            &[&assistant],
            123,
            &*other_binding
        ),
        123,
        "same model label from a different provider binding must not reuse usage"
    );
    auto.state.settings.lock().unwrap().summary_generation += 1;
    assert_eq!(
        auto.state.threshold_estimate(
            model.descriptor(),
            &request,
            &[&assistant],
            123,
            &*auto.budget
        ),
        123
    );
    auto.state
        .note_issued_projection(current, 0, model.descriptor(), &request, &*auto.budget);
    let changed = ModelRequest::new(
        vec![
            ModelMessage::system("changed AGENTS").unwrap(),
            ModelMessage::user("u").unwrap(),
        ],
        vec![],
        ModelLimits::default(),
        ReasoningPreference::Auto,
    )
    .unwrap();
    assert_eq!(
        auto.state.threshold_estimate(
            model.descriptor(),
            &changed,
            &[&assistant],
            123,
            &*auto.budget
        ),
        123
    );
}

#[tokio::test]
async fn empty_system_and_empty_old_history_are_a_clean_first_boundary_noop() {
    let model = Stub::new(4_000, "unused", None);
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
    let model = Stub::new(15_000, "summary", None);
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
    let mut model = Stub::new(8_000, "unused", Some(Arc::clone(&entered)));
    Arc::get_mut(&mut model).unwrap().partial_usage = true;
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
    let model = Stub::new(8_000, "summary", None);
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
