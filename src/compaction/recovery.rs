use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{
    AssistantPart, DeliveryState, Model, ModelCallContext, ModelDescriptor, ModelError,
    ModelErrorKind, ModelLimits, ModelMessage, ModelRequest, ModelStartFuture, ReasoningPreference,
};
use minicore_runtime::tools::ToolSpec;
use minicore_runtime::value::BoundedText;

use super::auto::{
    AutoContext, AutoContextBinding, GroupSummaryRequest, compressible_ranges, current_user_texts,
    estimate_tokens_with_budget, fold_history, merge_utility_usage, minimal_messages,
    summarize_group,
};
use super::{CONTEXT_UNCOMPRESSIBLE, CompactionState, auto_compose};
use crate::compaction::CompactionUtilityUsage;
use crate::models::ProviderBudget;

const MAX_RECOVERY_SOURCE_BYTES: usize = 512 * 1024;

/// Immutable ticket metadata prepared during prompt preparation and strictly
/// bound to one logical model request. `content_hash` is fixed when the prompt
/// is prepared; `ticket_hash` is bound once the Runtime-built request (with
/// its actual `ModelLimits`) reaches `Model::start`.
#[derive(Clone)]
pub(crate) struct ActiveRecoveryTicket {
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
    pub(crate) content_hash: [u8; 32],
    pub(crate) ticket_hash: Option<[u8; 32]>,
    pub(crate) original_body_bytes: usize,
    pub(crate) original_tokens: u64,
    pub(crate) system: BoundedText,
    pub(crate) summary: Option<BoundedText>,
    pub(crate) base: Vec<HistoryItem>,
    pub(crate) appended: Vec<HistoryItem>,
    pub(crate) tools: Vec<ToolSpec>,
    pub(crate) limits: ModelLimits,
    pub(crate) reasoning: ReasoningPreference,
    /// Utility binding captured when the Runtime-built request was bound. The
    /// recovery uses this binding, not the current settings, so a later
    /// settings update cannot re-pair the ticket with a newer utility model.
    pub(crate) auto_binding: Option<AutoContextBinding>,
}

/// Bounds the retained recovery source before any `HistoryItem` is cloned. The
/// count is a capped serde write of exactly the fields the ticket retains
/// (system, durable summary, history items, tool schemas), so structural
/// framing, identifiers, model/usage/reasoning fields, and empty items all
/// count. Serialization stops as soon as the cap is crossed and never builds
/// an intermediate buffer.
pub(crate) fn recovery_source_is_safe(
    system: &BoundedText,
    summary: Option<&BoundedText>,
    base: &[HistoryItem],
    appended: &[HistoryItem],
    tools: &[ToolSpec],
) -> bool {
    let mut writer = CappedWriter {
        bytes: 0,
        cap: MAX_RECOVERY_SOURCE_BYTES,
    };
    serde_json::to_writer(&mut writer, system).is_ok()
        && serde_json::to_writer(&mut writer, &summary).is_ok()
        && serde_json::to_writer(&mut writer, base).is_ok()
        && serde_json::to_writer(&mut writer, appended).is_ok()
        && serde_json::to_writer(&mut writer, tools).is_ok()
}

struct CappedWriter {
    bytes: usize,
    cap: usize,
}

impl std::io::Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self.bytes.saturating_add(bytes.len());
        if next > self.cap {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "recovery source exceeds cap",
            ));
        }
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Hashes every request input except `ModelLimits`. Preparation uses this to
/// fix the request content it planned; the start boundary recomputes it to
/// prove the actual request still matches before binding real limits.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_content_hash(
    loop_id: LoopId,
    request_index: u32,
    messages: &[ModelMessage],
    tools: &[ToolSpec],
    model_descriptor: &ModelDescriptor,
    reasoning: ReasoningPreference,
    config_generation: u64,
    summary_generation: u64,
) -> Result<[u8; 32], ModelError> {
    let mut hasher = Sha256::new();
    hash_ticket_inputs(
        &mut hasher,
        loop_id,
        request_index,
        messages,
        tools,
        reasoning,
        None,
        model_descriptor,
        config_generation,
        summary_generation,
    )?;
    Ok(hasher.finalize().into())
}

/// Computes the strict binding hash for one recovery ticket, capturing full
/// request framing including the actual `ModelLimits`, tool schemas,
/// descriptor capabilities, generations, and loop/request identity.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_ticket_hash(
    loop_id: LoopId,
    request_index: u32,
    messages: &[ModelMessage],
    tools: &[ToolSpec],
    limits: &ModelLimits,
    model_descriptor: &ModelDescriptor,
    reasoning: ReasoningPreference,
    config_generation: u64,
    summary_generation: u64,
) -> Result<[u8; 32], ModelError> {
    let mut hasher = Sha256::new();
    hash_ticket_inputs(
        &mut hasher,
        loop_id,
        request_index,
        messages,
        tools,
        reasoning,
        Some(limits),
        model_descriptor,
        config_generation,
        summary_generation,
    )?;
    Ok(hasher.finalize().into())
}

#[allow(clippy::too_many_arguments)]
fn hash_ticket_inputs(
    hasher: &mut Sha256,
    loop_id: LoopId,
    request_index: u32,
    messages: &[ModelMessage],
    tools: &[ToolSpec],
    reasoning: ReasoningPreference,
    limits: Option<&ModelLimits>,
    model_descriptor: &ModelDescriptor,
    config_generation: u64,
    summary_generation: u64,
) -> Result<(), ModelError> {
    hasher.update(loop_id.to_string().as_bytes());
    hasher.update(request_index.to_le_bytes());
    let messages_json = serde_json::to_vec(messages)
        .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&messages_json);
    let tools_json =
        serde_json::to_vec(tools).map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&tools_json);
    if let Some(limits) = limits {
        let limits_json = serde_json::to_vec(limits)
            .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
        hasher.update(&limits_json);
    }
    hasher.update(model_descriptor.model_ref.as_str().as_bytes());
    hasher.update(model_descriptor.context_window.to_le_bytes());
    hasher.update([model_descriptor.supports_tools as u8]);
    let reasoning_support = serde_json::to_vec(&model_descriptor.supported_reasoning)
        .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&reasoning_support);
    let reasoning_json = serde_json::to_vec(&reasoning)
        .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&reasoning_json);
    hasher.update(config_generation.to_le_bytes());
    hasher.update(summary_generation.to_le_bytes());
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoveryObservation {
    pub loop_id: LoopId,
    pub request_index: u32,
    pub before_tokens: Option<u64>,
    pub after_tokens: Option<u64>,
    pub utility_usage: Option<CompactionUtilityUsage>,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TicketClaimError {
    AlreadyConsumed,
    NoTicket,
    IndexMismatch,
    StaleTicket,
}

/// Verifies that all assistant tool calls in the reconstructed messages are
/// followed by their exact matching tool results. Multiple tool calls may complete
/// in any order, but their IDs must be unique and match exactly without orphaned calls or results.
pub(crate) fn validate_clean_tool_exchanges(messages: &[ModelMessage]) -> bool {
    let mut index = 0usize;
    while index < messages.len() {
        if let ModelMessage::Assistant(parts) | ModelMessage::AssistantWithReplay { parts, .. } =
            &messages[index]
        {
            let tool_calls = parts
                .iter()
                .filter_map(AssistantPart::as_tool_call)
                .collect::<Vec<_>>();
            if !tool_calls.is_empty() {
                let mut call_ids = BTreeSet::new();
                for call in &tool_calls {
                    if !call_ids.insert(call.tool_call_id().clone()) {
                        // Duplicate tool call id in same assistant message is invalid.
                        return false;
                    }
                }
                let mut end = index + 1;
                let mut result_ids = BTreeSet::new();
                while end < messages.len() {
                    if let ModelMessage::Tool { tool_call_id, .. } = &messages[end] {
                        if !result_ids.insert(tool_call_id.clone()) {
                            // Duplicate tool result id is invalid.
                            return false;
                        }
                        end += 1;
                        if result_ids.len() == call_ids.len() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if result_ids != call_ids {
                    // Mismatched set of tool call IDs and result IDs.
                    return false;
                }
                index = end;
                continue;
            }
        }
        if matches!(messages[index], ModelMessage::Tool { .. }) {
            // Orphaned tool result without a leading assistant tool call.
            return false;
        }
        index += 1;
    }
    true
}

#[derive(Debug)]
pub(crate) enum RecoveryReconstructionError {
    Cancelled,
    Timeout,
    Uncompressible,
    InvalidToolExchanges,
    Other(String),
}

/// Folds the retained sources exactly as the provider will see them and
/// measures the real normalized request. Returns `None` when the projection
/// cannot be represented as a valid request within the message ceiling.
#[allow(clippy::too_many_arguments)]
fn measure_reconstruction(
    ticket: &ActiveRecoveryTicket,
    auto: &AutoContext,
    budget: &dyn ProviderBudget,
    fixed: &[ModelMessage],
    history_messages: &[ModelMessage],
    loop_id: LoopId,
    request_index: u32,
    folded: &BTreeMap<super::EphemeralGroupKey, BoundedText>,
) -> Option<(ModelRequest, u64, usize)> {
    let messages = fold_history(
        fixed,
        history_messages,
        &ticket.base,
        &ticket.appended,
        loop_id,
        folded,
        None,
    )
    .ok()?;
    if messages.len() > auto.max_prompt_messages || !validate_clean_tool_exchanges(&messages) {
        return None;
    }
    let request = ModelRequest::new(
        messages,
        ticket.tools.clone(),
        ticket.limits,
        ticket.reasoning,
    )
    .ok()?;
    let tokens = budget
        .estimate_request_tokens(&request, Some(loop_id), Some(request_index))
        .ok()?;
    let bytes = budget
        .estimate_request_bytes(&request, Some(loop_id), Some(request_index))
        .ok()?;
    Some((request, tokens, bytes))
}

/// Performs clean reconstruction by folding all remaining compressible groups
/// and verifying that every tool exchange is valid and complete.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconstruct_for_recovery(
    ticket: &ActiveRecoveryTicket,
    auto: &AutoContext,
    budget: &dyn ProviderBudget,
    cancellation: &CancellationToken,
    deadline: Instant,
    loop_id: LoopId,
    request_index: u32,
    existing_usage: Option<CompactionUtilityUsage>,
) -> (
    Result<ModelRequest, RecoveryReconstructionError>,
    Option<CompactionUtilityUsage>,
) {
    let mut utility_usage = existing_usage;

    if cancellation.is_cancelled() {
        return (Err(RecoveryReconstructionError::Cancelled), utility_usage);
    }
    if Instant::now() >= deadline {
        return (Err(RecoveryReconstructionError::Timeout), utility_usage);
    }

    // No baseline body means the retry can never be proven smaller; refuse
    // before spending utility calls on a summary with no comparison point.
    if ticket.original_body_bytes == 0 {
        return (
            Err(RecoveryReconstructionError::Uncompressible),
            utility_usage,
        );
    }

    let hard_tokens = auto.model.descriptor().context_window;
    let users = current_user_texts(&ticket.base, &ticket.appended, loop_id);
    let minimal = match minimal_messages(&ticket.system, &users) {
        Ok(min) => min,
        Err(err) => {
            return (
                Err(RecoveryReconstructionError::Other(format!(
                    "minimal messages: {:?}",
                    err.kind()
                ))),
                utility_usage,
            );
        }
    };
    let minimal_tokens = match estimate_tokens_with_budget(
        budget,
        minimal,
        &ticket.tools,
        ticket.reasoning,
        Some(loop_id),
        Some(request_index),
    ) {
        Ok(tok) => tok,
        Err(_) => {
            return (
                Err(RecoveryReconstructionError::Uncompressible),
                utility_usage,
            );
        }
    };
    // The irreducible system + current User/Steer + tool schema floor is
    // checked before any utility call: summarization cannot remove it, and a
    // floor that already consumes the whole window leaves no room for a
    // summary at all.
    if minimal_tokens >= hard_tokens {
        return (
            Err(RecoveryReconstructionError::Uncompressible),
            utility_usage,
        );
    }
    let items: Vec<&HistoryItem> = ticket.base.iter().chain(ticket.appended.iter()).collect();
    let ranges = match compressible_ranges(ticket.base.len(), &items, loop_id) {
        Ok(r) => r,
        Err(err) => {
            return (
                Err(RecoveryReconstructionError::Other(format!(
                    "ranges: {:?}",
                    err.kind()
                ))),
                utility_usage,
            );
        }
    };

    let valid = ranges.iter().map(|(key, _)| key.clone()).collect();
    if auto
        .state
        .consolidated(loop_id, ticket.summary.as_ref(), &valid)
        .is_some()
    {
        let result = super::threshold::consolidate(
            super::threshold::ProjectionInput {
                auto,
                system: &ticket.system,
                summary: ticket.summary.as_ref(),
                base: &ticket.base,
                appended: &ticket.appended,
                tools: &ticket.tools,
                reasoning: ticket.reasoning,
                limits: ticket.limits,
                loop_id,
                request_index,
                deadline,
                cancellation,
                rejected_body_bytes: Some(ticket.original_body_bytes),
            },
            true,
        )
        .await;
        return match result {
            Ok(result) if result.compacted => {
                merge_utility_usage(&mut utility_usage, result.usage);
                (Ok(result.request), utility_usage)
            }
            Ok(_) => (
                Err(RecoveryReconstructionError::Uncompressible),
                utility_usage,
            ),
            Err(error) => {
                merge_utility_usage(&mut utility_usage, error.usage);
                let error = match error.error {
                    super::UtilityError::Cancelled => RecoveryReconstructionError::Cancelled,
                    super::UtilityError::Timeout => RecoveryReconstructionError::Timeout,
                    other => RecoveryReconstructionError::Other(other.kind().into()),
                };
                (Err(error), utility_usage)
            }
        };
    }

    // If there is no compressible group, recovery cannot shrink anything without dropping user constraints.
    if ranges.is_empty() {
        return (
            Err(RecoveryReconstructionError::Uncompressible),
            utility_usage,
        );
    }

    let (fixed, history_messages) = match auto_compose(
        &ticket.system,
        ticket.summary.as_ref(),
        &ticket.base,
        &ticket.appended,
    ) {
        Ok(comp) => comp,
        Err(err) => {
            return (
                Err(RecoveryReconstructionError::Other(format!(
                    "auto compose: {:?}",
                    err.kind()
                ))),
                utility_usage,
            );
        }
    };

    let valid_keys = ranges.iter().map(|(key, _)| key.clone()).collect();
    let mut folded = auto.state.emergency_groups(loop_id, &valid_keys);

    // A retry is only worthwhile when the real, provider-normalized body is
    // strictly smaller than the body that failed and stays inside the hard
    // window and message ceiling. Utility token deltas are not a shrink proof:
    // their accounting excludes opaque replay and the summary wrapper, and it
    // ignores summaries retained from earlier requests. Fold the real sources
    // and measure the actual request every time a group is replaced.
    let mut candidates = ranges;
    candidates.sort_by(|(left, _), (right, _)| {
        (right.end - right.start)
            .cmp(&(left.end - left.start))
            .then(left.start.cmp(&right.start))
    });

    // Existing groups are already in the rejected request. A fresh rejection
    // must reduce a new source group, not win merely by dropping opaque replay.

    for (key, _) in candidates {
        if folded.contains_key(&key) {
            continue;
        }
        if folded.len() >= super::MAX_EPHEMERAL_GROUPS {
            return (
                Err(RecoveryReconstructionError::Uncompressible),
                utility_usage,
            );
        }
        if cancellation.is_cancelled() {
            return (Err(RecoveryReconstructionError::Cancelled), utility_usage);
        }
        if Instant::now() >= deadline {
            return (Err(RecoveryReconstructionError::Timeout), utility_usage);
        }
        let start = key.start;
        let end = key.end;
        let group: Vec<HistoryItem> = ticket
            .base
            .iter()
            .chain(ticket.appended.iter())
            .skip(start)
            .take(end - start)
            .cloned()
            .collect();
        if group.is_empty() {
            continue;
        }
        let generation = match summarize_group(
            GroupSummaryRequest {
                model: Arc::clone(&auto.model),
                reasoning: ticket.reasoning,
                system: &ticket.system,
                tools: ticket.tools.clone(),
                group,
                hard_tokens,
                deadline,
            },
            cancellation,
        )
        .await
        {
            Ok(generation) => generation,
            Err(error) => {
                merge_utility_usage(&mut utility_usage, error.utility_usage);
                match error.error {
                    super::utility::UtilityError::Cancelled => {
                        return (Err(RecoveryReconstructionError::Cancelled), utility_usage);
                    }
                    super::utility::UtilityError::Timeout => {
                        return (Err(RecoveryReconstructionError::Timeout), utility_usage);
                    }
                    other => {
                        return (
                            Err(RecoveryReconstructionError::Other(other.kind().to_owned())),
                            utility_usage,
                        );
                    }
                }
            }
        };

        merge_utility_usage(&mut utility_usage, generation.utility_usage);
        folded.insert(key, generation.content);

        if let Some((request, tokens, bytes)) = measure_reconstruction(
            ticket,
            auto,
            budget,
            &fixed,
            &history_messages,
            loop_id,
            request_index,
            &folded,
        ) {
            if tokens <= hard_tokens && bytes < ticket.original_body_bytes {
                if cancellation.is_cancelled() {
                    return (Err(RecoveryReconstructionError::Cancelled), utility_usage);
                }
                if Instant::now() >= deadline {
                    return (Err(RecoveryReconstructionError::Timeout), utility_usage);
                }
                if !auto.state.install_emergency_groups(loop_id, folded) {
                    return (
                        Err(RecoveryReconstructionError::Uncompressible),
                        utility_usage,
                    );
                }
                return (Ok(request), utility_usage);
            }
        }
    }

    // Nothing retained or re-derived fits the window while being strictly
    // smaller than the failed body. Keep the structural failure explicit
    // instead of retrying an unchanged request.
    match fold_history(
        &fixed,
        &history_messages,
        &ticket.base,
        &ticket.appended,
        loop_id,
        &folded,
        None,
    ) {
        Ok(messages) if !validate_clean_tool_exchanges(&messages) => {
            return (
                Err(RecoveryReconstructionError::InvalidToolExchanges),
                utility_usage,
            );
        }
        Err(err) => {
            return (
                Err(RecoveryReconstructionError::Other(format!(
                    "fold history: {:?}",
                    err.kind()
                ))),
                utility_usage,
            );
        }
        Ok(_) => {}
    }
    (
        Err(RecoveryReconstructionError::Uncompressible),
        utility_usage,
    )
}

/// Model wrapper that intercepts structured `ContextOverflow + NotStarted` from
/// raw `Model::start` and performs a one-shot bounded recovery within the same
/// logical request.
pub(crate) struct CompactingModel {
    inner: Arc<dyn Model>,
    budget: Arc<dyn ProviderBudget>,
    state: Arc<CompactionState>,
}

impl CompactingModel {
    pub(crate) fn new(
        inner: Arc<dyn Model>,
        budget: Arc<dyn ProviderBudget>,
        state: Arc<CompactionState>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            budget,
            state,
        })
    }

    async fn start_inner(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> Result<minicore_runtime::model::ModelStream, ModelError> {
        let loop_id = context.loop_id;
        let request_index = context.request_index;

        // A normal delivery retry re-enters with the original immutable
        // Runtime request, not a freshly prepared projection. Match its exact
        // framing, then continue using the already reduced request without
        // granting another recovery opportunity.
        let original_hash = compute_ticket_hash(
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            request.limits(),
            self.inner.descriptor(),
            request.reasoning(),
            0,
            0,
        )?;
        if let Some(reduced) = self
            .state
            .recovered_request(loop_id, request_index, original_hash)
            .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?
        {
            if context.cancellation.is_cancelled() {
                return Err(local_model_error(ModelErrorKind::Cancelled));
            }
            if Instant::now() >= context.deadline {
                return Err(local_model_error(ModelErrorKind::Timeout));
            }
            let cancellation = context.cancellation.clone();
            let deadline = tokio::time::Instant::from_std(context.deadline);
            self.state.note_issued_projection(
                loop_id,
                request_index,
                self.inner.descriptor(),
                &reduced,
                &*self.budget,
            );
            let result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(unknown_model_error(ModelErrorKind::Cancelled)),
                _ = tokio::time::sleep_until(deadline) => Err(unknown_model_error(ModelErrorKind::Timeout)),
                result = self.inner.start(reduced.clone(), context) => result,
            };
            if let Some(observation) = self.state.recovery_observation().filter(|observation| {
                observation.loop_id == loop_id && observation.request_index == request_index
            }) {
                match &result {
                    Ok(_) => self.state.record_recovery_success(
                        loop_id,
                        request_index,
                        observation.before_tokens,
                        self.budget
                            .estimate_request_tokens(&reduced, Some(loop_id), Some(request_index))
                            .ok(),
                        observation.utility_usage,
                    ),
                    Err(error) => self.state.record_recovery_failure(
                        loop_id,
                        request_index,
                        observation.before_tokens,
                        observation.utility_usage,
                        model_error_kind_str(error.kind()),
                    ),
                }
            }
            return result;
        }

        // Capture actual before-start bytes and tokens snapshot before invocation.
        // If the provider call fails, replay may be cleared, so snapshotting here
        // accurately preserves original body and opaque continuation sizing.
        let (original_body_bytes, original_tokens) = self
            .budget
            .estimate_request_bytes(&request, Some(loop_id), Some(request_index))
            .map(|bytes| (bytes, bytes.div_ceil(4) as u64))
            .unwrap_or((0, 0));
        if original_body_bytes > 0 {
            self.state.note_request_estimate(original_tokens);
        }

        // Bind the Runtime-built request, including its actual `ModelLimits`,
        // to the preparation-time content hash. A content change under the
        // same logical index, or any serialization failure, drops the ticket
        // instead of re-anchoring it to different request content. The
        // generation pair is read as one snapshot; the utility binding itself
        // stays the one the preparing provider captured in the ticket.
        let settings = self.state.request_settings();
        let content_hash = compute_content_hash(
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            self.inner.descriptor(),
            request.reasoning(),
            settings.config_generation,
            settings.summary_generation,
        );
        let ticket_hash = compute_ticket_hash(
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            request.limits(),
            self.inner.descriptor(),
            request.reasoning(),
            settings.config_generation,
            settings.summary_generation,
        );
        match (content_hash, ticket_hash) {
            (Ok(content_hash), Ok(ticket_hash)) => self.state.bind_actual_request_ticket(
                loop_id,
                request_index,
                content_hash,
                ticket_hash,
                *request.limits(),
                original_body_bytes,
                original_tokens,
            ),
            _ => self.state.discard_recovery_ticket(loop_id, request_index),
        }

        let cancellation = context.cancellation.clone();
        let deadline = tokio::time::Instant::from_std(context.deadline);

        // Nothing has been sent before this point, so an already cancelled or
        // expired request is truthfully `NotStarted`. Creating the raw future
        // can already run synchronous side effects (for example clearing an
        // OpenAI continuation), so any interruption after this boundary can
        // only report `Unknown`.
        if context.cancellation.is_cancelled() {
            return Err(local_model_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            return Err(local_model_error(ModelErrorKind::Timeout));
        }

        self.state.note_issued_projection(
            loop_id,
            request_index,
            self.inner.descriptor(),
            &request,
            &*self.budget,
        );
        let first_result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(unknown_model_error(ModelErrorKind::Cancelled));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(unknown_model_error(ModelErrorKind::Timeout));
            }
            res = self.inner.start(request.clone(), context.clone()) => res,
        };

        let Err(error) = first_result else {
            return first_result;
        };

        // Strictly structured context rejection before output started.
        // Never guess from message substrings; never retry started or unknown deliveries.
        if error.kind() != ModelErrorKind::ContextOverflow
            || error.delivery() != DeliveryState::NotStarted
        {
            return Err(error);
        }

        // Check and claim ticket. If already consumed or stale, do not recover.
        let ticket = match self.state.claim_recovery_ticket(
            loop_id,
            request_index,
            &request,
            self.inner.descriptor(),
        ) {
            Ok(t) => t,
            Err(_) => {
                return Err(error);
            }
        };

        // The recovery utility binding is the one the preparing provider used.
        // It is captured in the ticket at preparation time and never replaced
        // by a later global settings read; without a binding, fail closed.
        let auto = match ticket.auto_binding.clone() {
            Some(binding) => {
                super::auto::AutoContext::from_binding(&binding, Arc::clone(&self.state))
            }
            None => {
                return Err(error);
            }
        };

        if context.cancellation.is_cancelled() {
            self.state.record_recovery_failure(
                loop_id,
                request_index,
                Some(original_tokens),
                None,
                "cancelled",
            );
            return Err(local_model_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            self.state.record_recovery_failure(
                loop_id,
                request_index,
                Some(original_tokens),
                None,
                "timeout",
            );
            return Err(local_model_error(ModelErrorKind::Timeout));
        }

        self.state
            .record_recovery_in_progress(loop_id, request_index, Some(original_tokens));

        let (reconstruct_res, utility_usage) = reconstruct_for_recovery(
            &ticket,
            &auto,
            &*self.budget,
            &context.cancellation,
            context.deadline,
            loop_id,
            request_index,
            None,
        )
        .await;

        let reduced_request = match reconstruct_res {
            Ok(req) => req,
            Err(RecoveryReconstructionError::Cancelled) => {
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    "cancelled",
                );
                return Err(local_model_error(ModelErrorKind::Cancelled));
            }
            Err(RecoveryReconstructionError::Timeout) => {
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    "timeout",
                );
                return Err(local_model_error(ModelErrorKind::Timeout));
            }
            Err(RecoveryReconstructionError::Uncompressible) => {
                self.state.note_prepare_failure(CONTEXT_UNCOMPRESSIBLE);
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    CONTEXT_UNCOMPRESSIBLE,
                );
                return Err(error);
            }
            Err(RecoveryReconstructionError::InvalidToolExchanges) => {
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    "invalid_tool_exchanges",
                );
                return Err(error);
            }
            Err(RecoveryReconstructionError::Other(kind)) => {
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    &kind,
                );
                return Err(error);
            }
        };

        if context.cancellation.is_cancelled() {
            self.state.record_recovery_failure(
                loop_id,
                request_index,
                Some(original_tokens),
                utility_usage,
                "cancelled",
            );
            return Err(local_model_error(ModelErrorKind::Cancelled));
        }
        if Instant::now() >= context.deadline {
            self.state.record_recovery_failure(
                loop_id,
                request_index,
                Some(original_tokens),
                utility_usage,
                "timeout",
            );
            return Err(local_model_error(ModelErrorKind::Timeout));
        }

        let after_tokens = self
            .budget
            .estimate_request_tokens(&reduced_request, Some(loop_id), Some(request_index))
            .ok();

        self.state.retain_recovered_request(
            loop_id,
            request_index,
            original_hash,
            reduced_request.clone(),
        );

        // Second start call. The pre-start checks above already covered the
        // not-yet-sent case; once this future exists an interruption can only
        // report `Unknown` delivery.
        self.state.note_issued_projection(
            loop_id,
            request_index,
            self.inner.descriptor(),
            &reduced_request,
            &*self.budget,
        );
        let second_result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    "cancelled",
                );
                return Err(unknown_model_error(ModelErrorKind::Cancelled));
            }
            _ = tokio::time::sleep_until(deadline) => {
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    "timeout",
                );
                return Err(unknown_model_error(ModelErrorKind::Timeout));
            }
            res = self.inner.start(reduced_request, context) => res,
        };

        match second_result {
            Ok(stream) => {
                self.state.record_recovery_success(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    after_tokens,
                    utility_usage,
                );
                Ok(stream)
            }
            Err(second_error) => {
                let kind_str = model_error_kind_str(second_error.kind());
                self.state.record_recovery_failure(
                    loop_id,
                    request_index,
                    Some(original_tokens),
                    utility_usage,
                    kind_str,
                );
                Err(second_error)
            }
        }
    }
}

pub(crate) fn model_error_kind_str(kind: ModelErrorKind) -> &'static str {
    match kind {
        ModelErrorKind::Cancelled => "cancelled",
        ModelErrorKind::Timeout => "timeout",
        ModelErrorKind::InvalidRequest => "invalid_request",
        ModelErrorKind::ContextOverflow => "context_overflow",
        ModelErrorKind::AuthMissing => "auth_missing",
        ModelErrorKind::AuthRejected => "auth_rejected",
        ModelErrorKind::RateLimited => "rate_limited",
        ModelErrorKind::QuotaExceeded => "quota_exceeded",
        ModelErrorKind::TransportUnavailable => "transport_unavailable",
        ModelErrorKind::ProviderUnavailable => "provider_unavailable",
        ModelErrorKind::Unavailable => "unavailable",
        ModelErrorKind::InvalidProviderResponse => "invalid_provider_response",
        ModelErrorKind::IncompleteResponse => "incomplete_response",
        ModelErrorKind::StreamInterrupted => "stream_interrupted",
        ModelErrorKind::RequestOutcomeUnknown => "request_outcome_unknown",
        ModelErrorKind::UnexpectedToolCall => "unexpected_tool_call",
        ModelErrorKind::Panicked => "panicked",
        ModelErrorKind::Internal => "internal",
    }
}

fn model_error_diagnostic(kind: ModelErrorKind) -> minicore_runtime::error::DiagnosticSummary {
    let (code, message) = match kind {
        ModelErrorKind::Cancelled => (
            minicore_runtime::error::DiagnosticCode::RuntimeTerminated,
            "model request cancelled",
        ),
        ModelErrorKind::Timeout => (
            minicore_runtime::error::DiagnosticCode::ModelTimeout,
            "model request timed out",
        ),
        _ => (
            minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
            "model request invalid",
        ),
    };
    minicore_runtime::error::DiagnosticSummary::new(
        code,
        minicore_runtime::error::DiagnosticCategory::Model,
        BoundedText::new(message).expect("static diagnostic text"),
        false,
    )
}

/// Local failure raised before any raw model work was started.
fn local_model_error(kind: ModelErrorKind) -> ModelError {
    ModelError::permanent(
        kind,
        DeliveryState::NotStarted,
        model_error_diagnostic(kind),
    )
}

/// Interruption after the raw start future existed: the provider may already
/// have received the request, so delivery cannot be claimed as `NotStarted`.
fn unknown_model_error(kind: ModelErrorKind) -> ModelError {
    ModelError::unknown(kind, model_error_diagnostic(kind))
}

impl Model for CompactingModel {
    fn descriptor(&self) -> &ModelDescriptor {
        self.inner.descriptor()
    }

    fn start<'a>(
        &'a self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelStartFuture<'a> {
        Box::pin(async move { self.start_inner(request, context).await })
    }
}

#[cfg(test)]
mod tests;
