use super::*;

#[test]
fn settlement_folds_raw_offsets_before_removing_opaque_only_assistants() {
    use minicore_runtime::history::{
        AssistantHistory, ToolResultHistory, UserHistory, UserMessageKind,
    };
    use minicore_runtime::model::{
        AssistantPart, ModelFinishReason, ReasoningContent, ToolCall, Usage,
    };
    use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
    let loop_id = LoopId::new().unwrap();
    let assistant = |content| {
        HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
            loop_id,
            request_index: 0,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content,
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::default(),
        })
    };
    let exchange = |id: &str, output: &str| {
        let call_id = minicore_runtime::ToolCallId::new(id).unwrap();
        let call = ToolCall::new(
            call_id.clone(),
            "read".parse().unwrap(),
            serde_json::json!({"path":"a.txt"}),
            0,
        )
        .unwrap();
        [
            assistant(vec![AssistantPart::ToolCall(call)]),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index: 0,
                call_id,
                tool_name: "read".parse().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new(output).unwrap(),
            }),
        ]
    };
    let mut appended = vec![
        HistoryItem::User(UserHistory {
            loop_id,
            kind: UserMessageKind::Prompt,
            input: minicore_runtime::execution::UserInput::text("original task").unwrap(),
        }),
        assistant(vec![AssistantPart::Reasoning(
            ReasoningContent::new(None, None, Some("encrypted-only".to_owned()), None).unwrap(),
        )]),
    ];
    appended.extend(exchange("folded-call", "RAW-FOLDED"));
    appended.extend(exchange("kept-call", "RAW-KEPT"));
    appended.push(assistant(vec![AssistantPart::Text("final answer".into())]));
    let items = appended.iter().collect::<Vec<_>>();
    let key = auto::compressible_ranges(0, &items, loop_id).unwrap()[0]
        .0
        .clone();
    assert_eq!((key.start, key.end), (2, 4));
    let state = CompactionState::new();
    assert!(state.install_emergency_groups(
        loop_id,
        BTreeMap::from([(key, BoundedText::new("FOLDED SUMMARY").unwrap())])
    ));
    let summary = state
        .settled_emergency_summary(loop_id, &[], &appended, None)
        .unwrap()
        .unwrap();
    let projected: Vec<ModelMessage> = serde_json::from_str(summary.as_str()).unwrap();
    assert!(recovery::validate_clean_tool_exchanges(&projected));
    assert!(summary.as_str().contains("FOLDED SUMMARY"));
    assert!(summary.as_str().contains("RAW-KEPT"));
    assert!(summary.as_str().contains("final answer"));
    assert!(!summary.as_str().contains("RAW-FOLDED"));
    assert!(!summary.as_str().contains("encrypted-only"));
}

#[test]
fn snapshot_projection_bound_is_separate_from_generated_summary_limit() {
    use minicore_runtime::model::{AssistantPart, ProviderReplay, ReasoningContent, ToolCall};
    use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
    let literal = "encrypted signature provider_replay are historical words ".repeat(1_400);
    let plain = serde_json::to_string(&vec![ModelMessage::user(&literal).unwrap()]).unwrap();
    assert!(plain.len() > MAX_SUMMARY_CONTENT_BYTES);
    assert!(validate_summary_content(&plain).is_none());
    assert!(validate_snapshot_content(&plain).is_some());
    assert!(validate_snapshot_content(&"plain semantic text ".repeat(4_000)).is_none());
    assert!(validate_snapshot_content(&format!("[{}", "x".repeat(70_000))).is_none());

    let call_id = minicore_runtime::ToolCallId::new("historical-call").unwrap();
    let call = ToolCall::new(call_id.clone(), "read".parse().unwrap(),
            serde_json::json!({"encrypted":"literal", "signature":"literal", "provider_replay":"literal"}), 0).unwrap();
    let pair = vec![
        ModelMessage::assistant(vec![AssistantPart::ToolCall(call)]).unwrap(),
        ModelMessage::tool_with_outcome(
            call_id,
            ToolOutput::new(&literal).unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
    ];
    assert!(validate_snapshot_content(&serde_json::to_string(&pair).unwrap()).is_some());
    assert!(validate_snapshot_content(&serde_json::to_string(&pair[1..]).unwrap()).is_none());
    for part in [
        AssistantPart::Reasoning(
            ReasoningContent::new(Some(literal.clone()), None, Some("opaque".into()), None)
                .unwrap(),
        ),
        AssistantPart::Reasoning(
            ReasoningContent::new(Some(literal.clone()), None, None, Some("signature".into()))
                .unwrap(),
        ),
    ] {
        let value =
            serde_json::to_string(&vec![ModelMessage::assistant(vec![part]).unwrap()]).unwrap();
        assert!(validate_snapshot_content(&value).is_none());
    }
    let replay = ProviderReplay::new("fixture", serde_json::json!({"opaque":"value"})).unwrap();
    let value = serde_json::to_string(&vec![
        ModelMessage::assistant_with_provider_replay(
            vec![AssistantPart::Text(literal.into())],
            Some(replay),
        )
        .unwrap(),
    ])
    .unwrap();
    assert!(validate_snapshot_content(&value).is_none());
}

#[test]
fn snapshot_projection_must_fit_runtime_envelope_before_file_encoding() {
    let overhead = serde_json::to_string(&vec![ModelMessage::user("x").unwrap()])
        .unwrap()
        .len()
        - 1;
    let value = serde_json::to_string(&vec![
        ModelMessage::user("x".repeat(BoundedText::MAX_BYTES - overhead)).unwrap(),
    ])
    .unwrap();
    assert_eq!(value.len(), BoundedText::MAX_BYTES);
    let content = BoundedText::new(&value).unwrap();
    assert!(summary_data_message(&content).is_err());
    assert!(validate_snapshot_content(&value).is_none());
}

#[test]
fn emergency_groups_keep_source_identity_and_bounded_installation() {
    let state = CompactionState::new();
    let loop_id = LoopId::new().unwrap();
    let key = EphemeralGroupKey {
        start: 0,
        end: 2,
        source_hash: [1; 32],
    };
    assert!(state.cache_ephemeral(loop_id, key.clone(), BoundedText::new("summary").unwrap()));
    state.note_settings_installed();
    assert!(
        state
            .emergency_groups(loop_id, &BTreeSet::from([key.clone()]))
            .contains_key(&key)
    );
    assert!(state.emergency_groups(loop_id, &BTreeSet::new()).is_empty());
    let groups = (0..=MAX_EPHEMERAL_GROUPS)
        .map(|i| {
            (
                EphemeralGroupKey {
                    start: i,
                    end: i + 1,
                    source_hash: [0; 32],
                },
                BoundedText::new("summary").unwrap(),
            )
        })
        .collect();
    assert!(!state.install_emergency_groups(loop_id, groups));
    assert!(
        state
            .emergency_groups(loop_id, &BTreeSet::from([key.clone()]))
            .contains_key(&key)
    );
}

fn ticket_descriptor() -> minicore_runtime::model::ModelDescriptor {
    minicore_runtime::model::ModelDescriptor::new(
        "main".parse().unwrap(),
        4096,
        std::collections::BTreeSet::from([ReasoningPreference::Auto]),
        true,
    )
    .unwrap()
}

fn ticket_request(
    limits: minicore_runtime::model::ModelLimits,
) -> minicore_runtime::model::ModelRequest {
    minicore_runtime::model::ModelRequest::new(
        vec![ModelMessage::user("hello").unwrap()],
        Vec::new(),
        limits,
        ReasoningPreference::Auto,
    )
    .unwrap()
}

fn ticket_content_hash(
    loop_id: LoopId,
    request_index: u32,
    request: &minicore_runtime::model::ModelRequest,
    descriptor: &minicore_runtime::model::ModelDescriptor,
) -> [u8; 32] {
    super::recovery::compute_content_hash(&crate::compaction::recovery::TicketFraming {
        loop_id,
        request_index,
        messages: request.messages(),
        tools: request.tools(),
        model_descriptor: descriptor,
        reasoning: request.reasoning(),
        config_generation: 0,
        summary_generation: 0,
    })
    .unwrap()
}

fn ticket_full_hash(
    loop_id: LoopId,
    request_index: u32,
    request: &minicore_runtime::model::ModelRequest,
    descriptor: &minicore_runtime::model::ModelDescriptor,
    config_generation: u64,
    summary_generation: u64,
) -> [u8; 32] {
    super::recovery::compute_ticket_hash(
        &crate::compaction::recovery::TicketFraming {
            loop_id,
            request_index,
            messages: request.messages(),
            tools: request.tools(),
            model_descriptor: descriptor,
            reasoning: request.reasoning(),
            config_generation,
            summary_generation,
        },
        request.limits(),
    )
    .unwrap()
}

fn register_test_ticket(
    state: &CompactionState,
    loop_id: LoopId,
    request_index: u32,
    request: &minicore_runtime::model::ModelRequest,
    descriptor: &minicore_runtime::model::ModelDescriptor,
    content_hash: [u8; 32],
) {
    state.register_recovery_ticket(
        loop_id,
        request_index,
        Some(ActiveRecoveryTicket {
            loop_id,
            request_index,
            content_hash,
            ticket_hash: Some(ticket_full_hash(
                loop_id,
                request_index,
                request,
                descriptor,
                0,
                0,
            )),
            original_body_bytes: 128,
            system: BoundedText::new("system").unwrap(),
            summary: None,
            base: Vec::new(),
            appended: Vec::new(),
            tools: request.tools().to_vec(),
            limits: *request.limits(),
            reasoning: request.reasoning(),
            auto_binding: None,
        }),
    );
}

fn claim_ticket(
    state: &CompactionState,
    loop_id: LoopId,
    request_index: u32,
    request: &minicore_runtime::model::ModelRequest,
    descriptor: &minicore_runtime::model::ModelDescriptor,
) -> Result<ActiveRecoveryTicket, super::recovery::TicketClaimError> {
    state.claim_recovery_ticket(loop_id, request_index, request, descriptor)
}

#[test]
fn recovery_ticket_claims_once_per_logical_request() {
    let state = CompactionState::new();
    let loop_id = LoopId::new().unwrap();
    let descriptor = ticket_descriptor();
    let request = ticket_request(minicore_runtime::model::ModelLimits::default());
    let content_hash = ticket_content_hash(loop_id, 0, &request, &descriptor);
    register_test_ticket(&state, loop_id, 0, &request, &descriptor, content_hash);

    let claimed = claim_ticket(&state, loop_id, 0, &request, &descriptor).unwrap();
    assert_eq!(claimed.content_hash, content_hash);
    assert!(matches!(
        claim_ticket(&state, loop_id, 0, &request, &descriptor),
        Err(super::recovery::TicketClaimError::AlreadyConsumed)
    ));
}

#[test]
fn recovery_ticket_rejects_stale_generations_and_content() {
    let loop_id = LoopId::new().unwrap();
    let descriptor = ticket_descriptor();
    let request = ticket_request(minicore_runtime::model::ModelLimits::default());
    let content_hash = ticket_content_hash(loop_id, 0, &request, &descriptor);

    let config_state = CompactionState::new();
    register_test_ticket(
        &config_state,
        loop_id,
        0,
        &request,
        &descriptor,
        content_hash,
    );
    config_state.note_settings_installed();
    assert!(matches!(
        claim_ticket(&config_state, loop_id, 0, &request, &descriptor),
        Err(super::recovery::TicketClaimError::StaleTicket)
    ));

    let summary_state = CompactionState::new();
    register_test_ticket(
        &summary_state,
        loop_id,
        0,
        &request,
        &descriptor,
        content_hash,
    );
    summary_state.cache_ephemeral(
        loop_id,
        EphemeralGroupKey {
            start: 0,
            end: 1,
            source_hash: [7; 32],
        },
        BoundedText::new("ephemeral").unwrap(),
    );
    assert!(matches!(
        claim_ticket(&summary_state, loop_id, 0, &request, &descriptor),
        Err(super::recovery::TicketClaimError::StaleTicket)
    ));

    let content_state = CompactionState::new();
    register_test_ticket(
        &content_state,
        loop_id,
        0,
        &request,
        &descriptor,
        content_hash,
    );
    let other =
        ticket_request(minicore_runtime::model::ModelLimits::new(Some(1000), Some(128)).unwrap());
    assert!(matches!(
        claim_ticket(&content_state, loop_id, 0, &other, &descriptor),
        Err(super::recovery::TicketClaimError::StaleTicket)
    ));
}

#[test]
fn recovery_ticket_binding_covers_actual_limits_and_content() {
    let loop_id = LoopId::new().unwrap();
    let descriptor = ticket_descriptor();
    let planned = ticket_request(minicore_runtime::model::ModelLimits::default());
    let content_hash = ticket_content_hash(loop_id, 0, &planned, &descriptor);
    let actual_limits = minicore_runtime::model::ModelLimits::new(Some(2000), Some(256)).unwrap();
    let actual = ticket_request(actual_limits);

    // A binding whose content does not match the prepared request drops
    // the ticket instead of re-anchoring it to different content.
    let dropped_state = CompactionState::new();
    register_test_ticket(
        &dropped_state,
        loop_id,
        0,
        &planned,
        &descriptor,
        content_hash,
    );
    dropped_state.bind_actual_request_ticket(
        loop_id,
        0,
        [9; 32],
        ticket_full_hash(loop_id, 0, &actual, &descriptor, 0, 0),
        *actual.limits(),
        256,
    );
    assert!(matches!(
        claim_ticket(&dropped_state, loop_id, 0, &actual, &descriptor),
        Err(super::recovery::TicketClaimError::NoTicket)
    ));

    // A matching binding records the real limits, and claiming requires
    // those exact limits rather than the preparation placeholder.
    let bound_state = CompactionState::new();
    register_test_ticket(
        &bound_state,
        loop_id,
        0,
        &planned,
        &descriptor,
        content_hash,
    );
    bound_state.bind_actual_request_ticket(
        loop_id,
        0,
        content_hash,
        ticket_full_hash(loop_id, 0, &actual, &descriptor, 0, 0),
        *actual.limits(),
        256,
    );
    assert!(matches!(
        claim_ticket(&bound_state, loop_id, 0, &planned, &descriptor),
        Err(super::recovery::TicketClaimError::StaleTicket)
    ));
    let bound = claim_ticket(&bound_state, loop_id, 0, &actual, &descriptor).unwrap();
    assert_eq!(bound.original_body_bytes, 256);
    assert_eq!(bound.limits, *actual.limits());
}

#[test]
fn recovery_high_water_blocks_reentry_and_older_indices() {
    let state = CompactionState::new();
    let loop_a = LoopId::new().unwrap();
    let descriptor = ticket_descriptor();
    let request = ticket_request(minicore_runtime::model::ModelLimits::default());

    register_test_ticket(
        &state,
        loop_a,
        12,
        &request,
        &descriptor,
        ticket_content_hash(loop_a, 12, &request, &descriptor),
    );
    let claimed = claim_ticket(&state, loop_a, 12, &request, &descriptor).unwrap();
    assert_eq!(claimed.request_index, 12);

    // A Driver re-entry of the same logical request is refused.
    assert!(matches!(
        claim_ticket(&state, loop_a, 12, &request, &descriptor),
        Err(super::recovery::TicketClaimError::AlreadyConsumed)
    ));

    // Every older index stays consumed as well: the high-water mark has no
    // fixed-capacity list that could evict an entry.
    for older in [3u32, 9, 11] {
        register_test_ticket(
            &state,
            loop_a,
            older,
            &request,
            &descriptor,
            ticket_content_hash(loop_a, older, &request, &descriptor),
        );
        assert!(matches!(
            claim_ticket(&state, loop_a, older, &request, &descriptor),
            Err(super::recovery::TicketClaimError::AlreadyConsumed)
        ));
    }

    // A new loop starts with fresh quota, and a late cleanup from the old
    // loop cannot clear the new loop's high-water mark.
    let loop_b = LoopId::new().unwrap();
    register_test_ticket(
        &state,
        loop_b,
        0,
        &request,
        &descriptor,
        ticket_content_hash(loop_b, 0, &request, &descriptor),
    );
    assert!(claim_ticket(&state, loop_b, 0, &request, &descriptor).is_ok());
    state.clear_ephemeral(loop_a);
    assert!(matches!(
        claim_ticket(&state, loop_b, 0, &request, &descriptor),
        Err(super::recovery::TicketClaimError::AlreadyConsumed)
    ));
}

impl CompactionState {
    pub(crate) fn ephemeral(&self, loop_id: LoopId) -> Option<EphemeralSummary> {
        self.ephemeral
            .lock()
            .unwrap()
            .as_ref()
            .filter(|summary| summary.loop_id == Some(loop_id))
            .cloned()
    }

    pub(crate) fn cache_ephemeral(
        &self,
        loop_id: LoopId,
        key: EphemeralGroupKey,
        summary: BoundedText,
    ) -> bool {
        let mut groups = self
            .ephemeral(loop_id)
            .map(|entry| entry.groups)
            .unwrap_or_default();
        groups.insert(key, summary);
        self.install_emergency_groups(loop_id, groups)
    }
}
