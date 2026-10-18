use super::*;
use crate::compaction::CompactionPolicy;
use crate::compaction::{auto::compressible_ranges, auto_compose};
use crate::models::tests::DefaultProviderBudget;
use crate::test_support::model::context_overflow;
use crate::test_support::{ScriptedModel, Step};
use minicore_runtime::ToolCallId;
use minicore_runtime::execution::UserInput;
use minicore_runtime::history::{
    AssistantHistory, ToolResultHistory, UserHistory, UserMessageKind,
};
use minicore_runtime::model::{ModelEvent, ModelFinishReason, ToolCall, Usage};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn raw_model(steps: impl IntoIterator<Item = Step>) -> Arc<ScriptedModel> {
    ScriptedModel::with_reasoning("main", 16_384, [ReasoningPreference::Auto].into(), steps)
}

fn summary_model(text: &str) -> Arc<ScriptedModel> {
    let events = summary_events(text);
    raw_model([]).with_fallback(move || Step::Events(events.clone()))
}

fn summary_events(text: &str) -> Vec<ModelEvent> {
    vec![
        ModelEvent::text_delta(text).unwrap(),
        ModelEvent::Finish {
            reason: ModelFinishReason::Stop,
        },
    ]
}

fn test_request() -> ModelRequest {
    ModelRequest::new(
        vec![ModelMessage::user("hello").unwrap()],
        Vec::new(),
        ModelLimits::default(),
        ReasoningPreference::Auto,
    )
    .unwrap()
}

async fn wait_for_calls(calls: &AtomicUsize, target: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while calls.load(Ordering::SeqCst) < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "raw model was never started"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn pre_start_cancellation_is_reported_not_started() {
    let raw = raw_model([Step::Pending]);
    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        CompactionState::new(),
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let context = ModelCallContext::new(
        LoopId::new().unwrap(),
        0,
        cancellation,
        Instant::now() + Duration::from_secs(5),
    );
    let error = match tokio::time::timeout(
        Duration::from_millis(500),
        compacting.start(test_request(), context),
    )
    .await
    .expect("pre-start cancellation must resolve")
    {
        Ok(_) => panic!("cancelled request must not start"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::Cancelled);
    assert_eq!(error.delivery(), DeliveryState::NotStarted);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pre_start_expired_deadline_is_reported_not_started() {
    let raw = raw_model([Step::Pending]);
    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        CompactionState::new(),
    );
    let context = ModelCallContext::new(
        LoopId::new().unwrap(),
        0,
        CancellationToken::new(),
        Instant::now(),
    );
    let error = match tokio::time::timeout(
        Duration::from_millis(500),
        compacting.start(test_request(), context),
    )
    .await
    .expect("pre-start deadline must resolve")
    {
        Ok(_) => panic!("expired request must not start"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::Timeout);
    assert_eq!(error.delivery(), DeliveryState::NotStarted);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn in_flight_cancellation_is_reported_unknown() {
    let raw = raw_model([Step::Pending]);
    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        CompactionState::new(),
    );
    let cancellation = CancellationToken::new();
    let context = ModelCallContext::new(
        LoopId::new().unwrap(),
        0,
        cancellation.clone(),
        Instant::now() + Duration::from_secs(30),
    );
    let calls = Arc::clone(&raw.calls);
    let start = compacting.start(test_request(), context);
    tokio::pin!(start);
    let (result, ()) = tokio::join!(start.as_mut(), async {
        wait_for_calls(&calls, 1).await;
        cancellation.cancel();
    });
    let error = match result {
        Ok(_) => panic!("an interrupted in-flight start must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::Cancelled);
    assert_eq!(error.delivery(), DeliveryState::Unknown);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn in_flight_deadline_is_reported_unknown() {
    let raw = raw_model([Step::Pending]);
    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        CompactionState::new(),
    );
    let context = ModelCallContext::new(
        LoopId::new().unwrap(),
        0,
        CancellationToken::new(),
        Instant::now() + Duration::from_millis(150),
    );
    let calls = Arc::clone(&raw.calls);
    let start = compacting.start(test_request(), context);
    tokio::pin!(start);
    let (result, ()) = tokio::join!(start.as_mut(), wait_for_calls(&calls, 1));
    let error = match result {
        Ok(_) => panic!("an expired in-flight start must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::Timeout);
    assert_eq!(error.delivery(), DeliveryState::Unknown);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn compacting_model_returns_structured_overflow_without_a_ticket() {
    let raw = raw_model([]).with_fallback(|| Step::ContextOverflowNotStarted);
    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        CompactionState::new(),
    );
    let context = ModelCallContext::new(
        LoopId::new().unwrap(),
        0,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    );
    let error = match compacting.start(test_request(), context).await {
        Ok(_) => panic!("overflow without a ticket must not be recovered"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::ContextOverflow);
    assert_eq!(error.delivery(), DeliveryState::NotStarted);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

fn big_tool_exchange(loop_id: LoopId, repeat: usize) -> (Vec<HistoryItem>, Vec<HistoryItem>) {
    let call_id = ToolCallId::new("call_big").unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "read".parse().unwrap(),
        json!({"path": "big.txt"}),
        0,
    )
    .unwrap();
    let base = vec![
        HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
            loop_id,
            request_index: 0,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::ToolCall(call)],
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default(),
        }),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index: 0,
            call_id,
            tool_name: "read".parse().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("big ".repeat(repeat)).unwrap(),
        }),
    ];
    let appended = vec![HistoryItem::User(UserHistory {
        loop_id,
        kind: UserMessageKind::Prompt,
        input: UserInput::text("read big").unwrap(),
    })];
    (base, appended)
}

fn big_request() -> ModelRequest {
    ModelRequest::new(
        vec![
            ModelMessage::system("system").unwrap(),
            ModelMessage::user("u".repeat(8_000)).unwrap(),
        ],
        Vec::new(),
        ModelLimits::default(),
        ReasoningPreference::Auto,
    )
    .unwrap()
}

fn recovery_auto(state: &Arc<CompactionState>, model: Arc<ScriptedModel>) -> AutoContext {
    AutoContext {
        binding: crate::compaction::auto::AutoContextBinding {
            model: Arc::clone(&model) as Arc<dyn Model>,
            budget: Arc::new(DefaultProviderBudget),
            policy: CompactionPolicy {
                enabled: true,
                trigger_percent: 80,
                target_percent: 50,
            },
            limits: minicore_runtime::LoopLimits {
                max_prompt_messages: usize::MAX,
                ..Default::default()
            },
        },
        state: Arc::clone(state),
    }
}

fn register_recovery_test_ticket(
    state: &CompactionState,
    raw: &ScriptedModel,
    auto: &AutoContext,
    loop_id: LoopId,
    request: &ModelRequest,
    base: Vec<HistoryItem>,
    appended: Vec<HistoryItem>,
) {
    let settings = state.request_settings();
    let content_hash = compute_content_hash(&crate::compaction::recovery::TicketFraming {
        loop_id,
        request_index: 0,
        messages: request.messages(),
        tools: request.tools(),
        model_descriptor: raw.descriptor(),
        reasoning: request.reasoning(),
        config_generation: settings.config_generation,
        summary_generation: settings.summary_generation,
    })
    .unwrap();
    state.register_recovery_ticket(
        loop_id,
        0,
        Some(ActiveRecoveryTicket {
            loop_id,
            request_index: 0,
            content_hash,
            ticket_hash: None,
            original_body_bytes: 0,
            system: BoundedText::new("system").unwrap(),
            summary: None,
            base,
            appended,
            tools: request.tools().to_vec(),
            limits: *request.limits(),
            reasoning: request.reasoning(),
            auto_binding: Some(auto.binding()),
        }),
    );
}

#[tokio::test]
async fn second_start_in_flight_cancellation_is_unknown_and_keeps_utility_usage() {
    let loop_id = LoopId::new().unwrap();
    let raw = raw_model([Step::ContextOverflowNotStarted, Step::Pending]);
    let utility = summary_model("compact summary");
    let state = CompactionState::new();
    let auto = recovery_auto(&state, utility);
    state.note_settings_installed();
    let request = big_request();
    let (base, appended) = big_tool_exchange(loop_id, 20_000);
    register_recovery_test_ticket(&state, &raw, &auto, loop_id, &request, base, appended);

    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        Arc::clone(&state),
    );
    let cancellation = CancellationToken::new();
    let context = ModelCallContext::new(
        loop_id,
        0,
        cancellation.clone(),
        Instant::now() + Duration::from_secs(30),
    );
    let calls = Arc::clone(&raw.calls);
    let start = compacting.start(request, context);
    tokio::pin!(start);
    let (result, ()) = tokio::join!(start.as_mut(), async {
        wait_for_calls(&calls, 2).await;
        cancellation.cancel();
    });
    let error = match result {
        Ok(_) => panic!("an interrupted recovery retry must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::Cancelled);
    assert_eq!(error.delivery(), DeliveryState::Unknown);
    let observation = state.recovery_observation().unwrap();
    assert_eq!(observation.outcome, "recovery_failed");
    assert_eq!(observation.failure_kind.as_deref(), Some("cancelled"));
    assert!(observation.utility_usage.is_some());
}

#[tokio::test]
async fn second_start_in_flight_deadline_is_unknown_and_keeps_utility_usage() {
    let loop_id = LoopId::new().unwrap();
    let raw = raw_model([Step::ContextOverflowNotStarted, Step::Pending]);
    let utility = summary_model("compact summary");
    let state = CompactionState::new();
    let auto = recovery_auto(&state, utility);
    state.note_settings_installed();
    let request = big_request();
    let (base, appended) = big_tool_exchange(loop_id, 20_000);
    register_recovery_test_ticket(&state, &raw, &auto, loop_id, &request, base, appended);

    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        Arc::clone(&state),
    );
    let context = ModelCallContext::new(
        loop_id,
        0,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(1),
    );
    let calls = Arc::clone(&raw.calls);
    let start = compacting.start(request, context);
    tokio::pin!(start);
    let (result, ()) = tokio::join!(start.as_mut(), wait_for_calls(&calls, 2));
    let error = match result {
        Ok(_) => panic!("an expired recovery retry must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::Timeout);
    assert_eq!(error.delivery(), DeliveryState::Unknown);
    let observation = state.recovery_observation().unwrap();
    assert_eq!(observation.outcome, "recovery_failed");
    assert_eq!(observation.failure_kind.as_deref(), Some("timeout"));
    assert!(observation.utility_usage.is_some());
}

#[tokio::test]
async fn settings_change_before_start_blocks_recovery() {
    let loop_id = LoopId::new().unwrap();
    let raw = raw_model([]).with_fallback(|| Step::ContextOverflowNotStarted);
    let utility_a = summary_model("compact summary");
    let utility_b = summary_model("newer summary");
    let state = CompactionState::new();
    let auto_a = recovery_auto(&state, Arc::clone(&utility_a));
    state.note_settings_installed();
    let request = big_request();
    let (base, appended) = big_tool_exchange(loop_id, 20_000);
    register_recovery_test_ticket(&state, &raw, &auto_a, loop_id, &request, base, appended);

    // A settings update is installed between preparation and start.
    state.note_settings_installed();

    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        Arc::clone(&state),
    );
    let context = ModelCallContext::new(
        loop_id,
        0,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    );
    let error = match compacting.start(request, context).await {
        Ok(_) => panic!("a stale settings snapshot must not recover"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::ContextOverflow);
    assert_eq!(error.delivery(), DeliveryState::NotStarted);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    assert_eq!(utility_a.calls.load(Ordering::SeqCst), 0);
    assert_eq!(utility_b.calls.load(Ordering::SeqCst), 0);
    assert!(state.recovery_observation().is_none());
}

#[tokio::test]
async fn settings_change_during_raw_start_blocks_recovery() {
    let loop_id = LoopId::new().unwrap();
    let utility_a = summary_model("compact summary");
    let utility_b = summary_model("newer summary");
    let state = CompactionState::new();
    let auto_a = recovery_auto(&state, Arc::clone(&utility_a));
    state.note_settings_installed();
    let request = big_request();
    let (base, appended) = big_tool_exchange(loop_id, 20_000);

    // The raw start installs a new configuration before failing, which
    // models an update landing while the request is in flight. The claim
    // must read the current generations and refuse the stale ticket.
    let switch_state = Arc::clone(&state);
    let hook = Arc::new(move || {
        switch_state.note_settings_installed();
    }) as Arc<dyn Fn() + Send + Sync>;
    let raw = raw_model([Step::Custom(Box::new(move |_, _| {
        hook();
        Box::pin(async { Err(context_overflow()) })
    }))]);
    register_recovery_test_ticket(&state, &raw, &auto_a, loop_id, &request, base, appended);

    let compacting = CompactingModel::new(
        Arc::clone(&raw) as Arc<dyn Model>,
        Arc::new(DefaultProviderBudget),
        Arc::clone(&state),
    );
    let context = ModelCallContext::new(
        loop_id,
        0,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    );
    let error = match compacting.start(request, context).await {
        Ok(_) => panic!("a mid-start settings change must not recover"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ModelErrorKind::ContextOverflow);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    assert_eq!(utility_a.calls.load(Ordering::SeqCst), 0);
    assert_eq!(utility_b.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn recovery_source_cap_counts_structure_and_closes_over_cap() {
    let loop_id = LoopId::new().unwrap();
    let system = BoundedText::new("system").unwrap();
    let (base, appended) = big_tool_exchange(loop_id, 2_000);
    assert!(recovery_source_is_safe(
        &system,
        None,
        &base,
        &appended,
        &[]
    ));

    // Thousands of empty-content items carry only structure and
    // identifiers; a text-only estimate would score them near zero.
    let empty: Vec<HistoryItem> = (0..20_000)
        .map(|_| {
            HistoryItem::Assistant(AssistantHistory {
                provider_replay: None,
                loop_id,
                request_index: 0,
                model: "main".parse().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: Vec::new(),
                finish_reason: ModelFinishReason::Stop,
                usage: Usage::default(),
            })
        })
        .collect();
    assert!(!recovery_source_is_safe(&system, None, &empty, &[], &[]));

    // The retained system and durable summary count as well.
    let large = BoundedText::new("s".repeat(BoundedText::MAX_BYTES)).unwrap();
    assert!(!recovery_source_is_safe(
        &large,
        Some(&large),
        &[],
        &[],
        &[]
    ));
}

#[tokio::test]
async fn reconstruction_refuses_when_the_retry_body_is_not_strictly_smaller() {
    let loop_id = LoopId::new().unwrap();
    let (base, appended) = big_tool_exchange(loop_id, 20_000);
    let ticket = ActiveRecoveryTicket {
        loop_id,
        request_index: 1,
        content_hash: [0; 32],
        ticket_hash: Some([0; 32]),
        original_body_bytes: usize::MAX,
        system: BoundedText::new("system").unwrap(),
        summary: None,
        base,
        appended,
        tools: Vec::new(),
        limits: ModelLimits::default(),
        reasoning: ReasoningPreference::Auto,
        auto_binding: None,
    };
    let utility = summary_model("compact summary");
    let auto = AutoContext {
        binding: crate::compaction::auto::AutoContextBinding {
            model: Arc::clone(&utility) as Arc<dyn Model>,
            budget: Arc::new(DefaultProviderBudget),
            policy: CompactionPolicy {
                enabled: true,
                trigger_percent: 80,
                target_percent: 50,
            },
            limits: minicore_runtime::LoopLimits {
                max_prompt_messages: usize::MAX,
                ..Default::default()
            },
        },
        state: CompactionState::new(),
    };
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    let (result, usage) = reconstruct_for_recovery(
        &ticket,
        &auto,
        &DefaultProviderBudget,
        &cancellation,
        deadline,
    )
    .await;
    assert!(usage.is_some());
    let reduced = match result {
        Ok(reduced) => reduced,
        Err(error) => panic!("a large exchange must reconstruct: {error:?}"),
    };
    let reduced_bytes = DefaultProviderBudget
        .estimate_request_bytes(&reduced, Some(loop_id), Some(1))
        .unwrap();

    // A recorded original body at or below the reduced size (or an
    // unverifiable zero) must refuse the retry rather than resend it.
    let mut tight = ticket.clone();
    tight.original_body_bytes = reduced_bytes;
    let (result, _usage) = reconstruct_for_recovery(
        &tight,
        &auto,
        &DefaultProviderBudget,
        &cancellation,
        deadline,
    )
    .await;
    assert!(matches!(
        result,
        Err(RecoveryReconstructionError::Uncompressible)
    ));

    let mut unverifiable = ticket;
    unverifiable.original_body_bytes = 0;
    let (result, _usage) = reconstruct_for_recovery(
        &unverifiable,
        &auto,
        &DefaultProviderBudget,
        &cancellation,
        deadline,
    )
    .await;
    assert!(matches!(
        result,
        Err(RecoveryReconstructionError::Uncompressible)
    ));
}

#[tokio::test]
async fn reconstruction_accounts_for_each_independent_group_once() {
    let loop_id = LoopId::new().unwrap();
    let (mut base, appended) = big_tool_exchange(loop_id, 20_000);
    let (mut second, _) = big_tool_exchange(loop_id, 20_000);
    let second_id = ToolCallId::new("call_second").unwrap();
    if let HistoryItem::Assistant(assistant) = &mut second[0] {
        assistant.content = vec![AssistantPart::ToolCall(
            ToolCall::new(
                second_id.clone(),
                "read".parse().unwrap(),
                json!({"path":"second.txt"}),
                0,
            )
            .unwrap(),
        )];
    }
    if let HistoryItem::ToolResult(result) = &mut second[1] {
        result.call_id = second_id;
    }
    base.extend(second);
    let original_history = serde_json::to_vec(&base).unwrap();
    let mut ticket = ActiveRecoveryTicket {
        loop_id,
        request_index: 1,
        content_hash: [0; 32],
        ticket_hash: Some([0; 32]),
        original_body_bytes: 0,
        system: BoundedText::new("system").unwrap(),
        summary: None,
        base,
        appended,
        tools: Vec::new(),
        limits: ModelLimits::default(),
        reasoning: ReasoningPreference::Auto,
        auto_binding: None,
    };
    let (mut fixed, history) =
        auto_compose(&ticket.system, None, &ticket.base, &ticket.appended).unwrap();
    fixed.extend(history);
    let original = ModelRequest::new(
        fixed,
        Vec::new(),
        ModelLimits::default(),
        ReasoningPreference::Auto,
    )
    .unwrap();
    ticket.original_body_bytes = DefaultProviderBudget
        .estimate_request_bytes(&original, None, None)
        .unwrap();
    let utility = summary_model("compact summary");
    let state = CompactionState::new();
    let auto = recovery_auto(&state, Arc::clone(&utility));
    // Either raw 80 KiB result alone exceeds the 64 KiB input window, so
    // a successful reconstruction necessarily replaces both safe groups.
    let (result, usage) = reconstruct_for_recovery(
        &ticket,
        &auto,
        &DefaultProviderBudget,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    )
    .await;
    let request = result.expect("both complete exchanges can shrink");
    assert_eq!(utility.calls.load(Ordering::SeqCst), 2);
    let usage = usage.unwrap();
    assert_eq!(usage.call_count, 2);
    assert!(
        !usage.complete,
        "the fake supplies no usage, which stays unknown"
    );
    assert!(usage.usage.is_none());
    assert!(validate_clean_tool_exchanges(request.messages()));
    assert_eq!(
        request
            .messages()
            .iter()
            .filter(
                |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "read big")
            )
            .count(),
        1
    );
    let bytes = DefaultProviderBudget
        .estimate_request_bytes(&request, None, None)
        .unwrap();
    assert!(bytes < ticket.original_body_bytes);
    assert!(bytes.div_ceil(4) as u64 <= utility.descriptor().context_window);
    assert_eq!(serde_json::to_vec(&ticket.base).unwrap(), original_history);
}

#[test]
fn test_clean_tool_pairs_validation() {
    let call_id_1 = ToolCallId::new("call_1").unwrap();
    let call_1 = ToolCall::new(
        call_id_1.clone(),
        "read".parse().unwrap(),
        json!({"path": "a.txt"}),
        0,
    )
    .unwrap();

    let call_id_2 = ToolCallId::new("call_2").unwrap();
    let call_2 = ToolCall::new(
        call_id_2.clone(),
        "write".parse().unwrap(),
        json!({"path": "b.txt"}),
        1,
    )
    .unwrap();

    // Valid: multiple tool calls followed by matching tool results out of order
    let valid_out_of_order = vec![
        ModelMessage::user("hi").unwrap(),
        ModelMessage::assistant(vec![
            AssistantPart::ToolCall(call_1.clone()),
            AssistantPart::ToolCall(call_2.clone()),
        ])
        .unwrap(),
        ModelMessage::tool_with_outcome(
            call_id_2.clone(),
            ToolOutput::new("done 2").unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
        ModelMessage::tool_with_outcome(
            call_id_1.clone(),
            ToolOutput::new("done 1").unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
    ];
    assert!(validate_clean_tool_exchanges(&valid_out_of_order));

    // Invalid: duplicate call ID in one assistant message. The Runtime
    // constructor already rejects this shape, so build the public enum
    // variant directly to exercise the validator itself.
    let dup_call = vec![
        ModelMessage::Assistant(vec![
            AssistantPart::ToolCall(call_1.clone()),
            AssistantPart::ToolCall(call_1.clone()),
        ]),
        ModelMessage::tool_with_outcome(
            call_id_1.clone(),
            ToolOutput::new("done 1").unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
    ];
    assert!(!validate_clean_tool_exchanges(&dup_call));

    // Invalid: missing one result
    let missing_result = vec![
        ModelMessage::assistant(vec![
            AssistantPart::ToolCall(call_1.clone()),
            AssistantPart::ToolCall(call_2.clone()),
        ])
        .unwrap(),
        ModelMessage::tool_with_outcome(
            call_id_1.clone(),
            ToolOutput::new("done 1").unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
    ];
    assert!(!validate_clean_tool_exchanges(&missing_result));

    // Invalid: orphaned result
    let orphan = vec![
        ModelMessage::user("hi").unwrap(),
        ModelMessage::tool_with_outcome(
            call_id_1,
            ToolOutput::new("done 1").unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
    ];
    assert!(!validate_clean_tool_exchanges(&orphan));
}

fn prose_ticket(sizes: &[usize], current_bytes: usize) -> ActiveRecoveryTicket {
    let loop_id = LoopId::new().unwrap();
    let old = LoopId::new().unwrap();
    let user = |owner, size| {
        HistoryItem::User(UserHistory {
            loop_id: owner,
            kind: UserMessageKind::Prompt,
            input: UserInput::text("x".repeat(size)).unwrap(),
        })
    };
    let mut ticket = ActiveRecoveryTicket {
        loop_id,
        request_index: 1,
        content_hash: [0; 32],
        ticket_hash: None,
        original_body_bytes: 0,
        system: BoundedText::new("system").unwrap(),
        summary: None,
        base: sizes.iter().map(|size| user(old, *size)).collect(),
        appended: vec![user(loop_id, current_bytes)],
        tools: vec![],
        limits: ModelLimits::default(),
        reasoning: ReasoningPreference::Auto,
        auto_binding: None,
    };
    let (mut messages, history) =
        auto_compose(&ticket.system, None, &ticket.base, &ticket.appended).unwrap();
    messages.extend(history);
    let request = ModelRequest::new(messages, vec![], ticket.limits, ticket.reasoning).unwrap();
    ticket.original_body_bytes = DefaultProviderBudget
        .estimate_request_bytes(&request, None, None)
        .unwrap();
    ticket
}

async fn reconstruct(
    ticket: &ActiveRecoveryTicket,
    auto: &AutoContext,
) -> (
    Result<ModelRequest, RecoveryReconstructionError>,
    Option<CompactionUtilityUsage>,
) {
    reconstruct_for_recovery(
        ticket,
        auto,
        &DefaultProviderBudget,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
}

#[tokio::test]
async fn recovery_selects_largest_bytes_and_stops_at_soft_trigger() {
    let ticket = prose_ticket(&[1_000, 8_000, 2_000], 100);
    let utility = summary_model("summary");
    let state = CompactionState::new();
    let auto = recovery_auto(&state, Arc::clone(&utility));
    let request = reconstruct(&ticket, &auto).await.0.unwrap();
    assert_eq!(utility.calls.load(Ordering::SeqCst), 1);
    let items = ticket
        .base
        .iter()
        .chain(&ticket.appended)
        .collect::<Vec<_>>();
    let keys = compressible_ranges(ticket.base.len(), &items, ticket.loop_id).unwrap();
    let folded = state.emergency_groups(
        ticket.loop_id,
        &keys.iter().map(|(key, _)| key.clone()).collect(),
    );
    assert_eq!(folded.keys().map(|key| key.start).collect::<Vec<_>>(), [1]);
    assert!(
        DefaultProviderBudget
            .estimate_request_tokens(&request, None, None)
            .unwrap()
            <= auto
                .policy
                .budget(auto.model.descriptor().context_window)
                .trigger_tokens
    );
}

#[tokio::test]
async fn recovery_skips_nonshrinking_group_and_accounts_for_both_calls() {
    let ticket = prose_ticket(&[2_000, 1_000], 100);
    let utility = raw_model(
        vec!["s".repeat(3_000), "summary".into()]
            .into_iter()
            .map(|text| Step::Events(summary_events(&text))),
    );
    let state = CompactionState::new();
    let auto = recovery_auto(&state, Arc::clone(&utility));
    let (result, usage) = reconstruct(&ticket, &auto).await;
    let request = result.unwrap();
    assert_eq!(utility.calls.load(Ordering::SeqCst), 2);
    assert_eq!(usage.unwrap().call_count, 2);
    assert!(
        request
            .messages()
            .iter()
            .any(|message| matches!(message, ModelMessage::User(text) if text.len() == 2_000))
    );
    assert!(
        !request
            .messages()
            .iter()
            .any(|message| matches!(message, ModelMessage::User(text) if text.len() == 1_000))
    );
}

#[tokio::test]
async fn recovery_continues_below_hard_limit_and_falls_back_only_after_exhaustion() {
    for (current_bytes, expected_calls) in [(100, 2), (55_000, 2)] {
        let ticket = prose_ticket(&[60_000, 55_000], current_bytes);
        let utility = summary_model("summary");
        let auto = recovery_auto(&CompactionState::new(), Arc::clone(&utility));
        let request = reconstruct(&ticket, &auto).await.0.unwrap();
        let tokens = DefaultProviderBudget
            .estimate_request_tokens(&request, None, None)
            .unwrap();
        let budget = auto.policy.budget(auto.model.descriptor().context_window);
        assert_eq!(utility.calls.load(Ordering::SeqCst), expected_calls);
        assert!(tokens <= budget.hard_tokens);
        assert_eq!(tokens > budget.trigger_tokens, current_bytes == 55_000);
        assert!(
            DefaultProviderBudget
                .estimate_request_bytes(&request, None, None)
                .unwrap()
                < ticket.original_body_bytes
        );
    }
}
