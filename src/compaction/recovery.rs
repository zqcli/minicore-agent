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
        if let ModelMessage::Assistant(parts) = &messages[index] {
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
    // The utility target is a whole-request budget in its own accounting, so
    // it must leave room above the irreducible floor and below the hard
    // ceiling; otherwise the utility would chase an unreachable target and
    // burn calls without ever producing a usable summary.
    let target_tokens = auto
        .policy
        .budget(hard_tokens)
        .target_tokens
        .max(minimal_tokens.saturating_add(1))
        .min(hard_tokens.saturating_sub(1))
        .max(1);

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

    let mut folded: BTreeMap<super::EphemeralGroupKey, BoundedText> = auto
        .state
        .ephemeral(loop_id)
        .map(|summary| summary.groups)
        .unwrap_or_default();

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

    // Retained summaries may already satisfy the shrink gate; reuse them
    // without a utility call. An empty cache must still fold at least one real
    // group: dropping opaque replay alone is not a semantic reduction.
    if !folded.is_empty() {
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
                return (Ok(request), utility_usage);
            }
        }
    }

    for (key, _) in candidates {
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
                target_tokens,
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
        auto.state
            .cache_ephemeral(loop_id, key.clone(), generation.content.clone());
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

        // Capture actual before-start bytes and tokens snapshot before invocation.
        // If the provider call fails, replay may be cleared, so snapshotting here
        // accurately preserves original body and opaque continuation sizing.
        let (original_body_bytes, original_tokens) = self
            .budget
            .estimate_request_bytes(&request, Some(loop_id), Some(request_index))
            .map(|bytes| (bytes, bytes.div_ceil(4) as u64))
            .unwrap_or((0, 0));

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

        // Second start call. The pre-start checks above already covered the
        // not-yet-sent case; once this future exists an interruption can only
        // report `Unknown` delivery.
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
mod tests {
    use super::*;
    use crate::compaction::CompactionPolicy;
    use crate::models::DefaultProviderBudget;
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

    #[derive(Clone, Copy)]
    enum RawBehavior {
        Never,
        ContextOverflow,
        OverflowThenPending,
        Summary(&'static str),
    }

    struct RawTestModel {
        descriptor: ModelDescriptor,
        behavior: RawBehavior,
        calls: Arc<AtomicUsize>,
        before_start: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl RawTestModel {
        fn new(behavior: RawBehavior) -> Arc<Self> {
            Self::with_hook(behavior, None)
        }

        fn with_hook(
            behavior: RawBehavior,
            before_start: Option<Arc<dyn Fn() + Send + Sync>>,
        ) -> Arc<Self> {
            let descriptor = ModelDescriptor::new(
                "main".parse().unwrap(),
                16_384,
                std::collections::BTreeSet::from([ReasoningPreference::Auto]),
                true,
            )
            .unwrap();
            Arc::new(Self {
                descriptor,
                behavior,
                calls: Arc::new(AtomicUsize::new(0)),
                before_start,
            })
        }
    }

    impl Model for RawTestModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn start(
            &self,
            _request: ModelRequest,
            _context: ModelCallContext,
        ) -> ModelStartFuture<'_> {
            if let Some(hook) = &self.before_start {
                hook();
            }
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                RawBehavior::Never => Box::pin(std::future::pending()),
                RawBehavior::OverflowThenPending if call > 0 => Box::pin(std::future::pending()),
                RawBehavior::OverflowThenPending | RawBehavior::ContextOverflow => {
                    Box::pin(async {
                        let diagnostic = minicore_runtime::error::DiagnosticSummary::new(
                            minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
                            minicore_runtime::error::DiagnosticCategory::Model,
                            BoundedText::new("context window exceeded").unwrap(),
                            false,
                        );
                        Err(ModelError::permanent(
                            ModelErrorKind::ContextOverflow,
                            DeliveryState::NotStarted,
                            diagnostic,
                        ))
                    })
                }
                RawBehavior::Summary(text) => Box::pin(async move {
                    let events = vec![
                        Ok(ModelEvent::text_delta(text).unwrap()),
                        Ok(ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        }),
                    ];
                    let stream: minicore_runtime::model::ModelStream =
                        Box::pin(futures_util::stream::iter(events));
                    Ok(stream)
                }),
            }
        }
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
        let raw = RawTestModel::new(RawBehavior::Never);
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
        let raw = RawTestModel::new(RawBehavior::Never);
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
        let raw = RawTestModel::new(RawBehavior::Never);
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
        let raw = RawTestModel::new(RawBehavior::Never);
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
        let raw = RawTestModel::new(RawBehavior::ContextOverflow);
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

    fn recovery_auto(state: &Arc<CompactionState>, model: Arc<RawTestModel>) -> AutoContext {
        AutoContext {
            model: Arc::clone(&model) as Arc<dyn Model>,
            budget: Arc::new(DefaultProviderBudget),
            policy: CompactionPolicy {
                enabled: true,
                trigger_percent: 80,
                target_percent: 50,
            },
            max_prompt_messages: usize::MAX,
            state: Arc::clone(state),
        }
    }

    fn register_recovery_test_ticket(
        state: &CompactionState,
        raw: &RawTestModel,
        auto: &AutoContext,
        loop_id: LoopId,
        request: &ModelRequest,
        base: Vec<HistoryItem>,
        appended: Vec<HistoryItem>,
    ) {
        let settings = state.request_settings();
        let content_hash = compute_content_hash(
            loop_id,
            0,
            request.messages(),
            request.tools(),
            raw.descriptor(),
            request.reasoning(),
            settings.config_generation,
            settings.summary_generation,
        )
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
                original_tokens: 0,
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
        let raw = RawTestModel::new(RawBehavior::OverflowThenPending);
        let utility = RawTestModel::new(RawBehavior::Summary("compact summary"));
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
        let raw = RawTestModel::new(RawBehavior::OverflowThenPending);
        let utility = RawTestModel::new(RawBehavior::Summary("compact summary"));
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
        let raw = RawTestModel::new(RawBehavior::ContextOverflow);
        let utility_a = RawTestModel::new(RawBehavior::Summary("compact summary"));
        let utility_b = RawTestModel::new(RawBehavior::Summary("newer summary"));
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
        let utility_a = RawTestModel::new(RawBehavior::Summary("compact summary"));
        let utility_b = RawTestModel::new(RawBehavior::Summary("newer summary"));
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
        let raw = RawTestModel::with_hook(RawBehavior::ContextOverflow, Some(hook));
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
            original_tokens: 0,
            system: BoundedText::new("system").unwrap(),
            summary: None,
            base,
            appended,
            tools: Vec::new(),
            limits: ModelLimits::default(),
            reasoning: ReasoningPreference::Auto,
            auto_binding: None,
        };
        let utility = RawTestModel::new(RawBehavior::Summary("compact summary"));
        let auto = AutoContext {
            model: Arc::clone(&utility) as Arc<dyn Model>,
            budget: Arc::new(DefaultProviderBudget),
            policy: CompactionPolicy {
                enabled: true,
                trigger_percent: 80,
                target_percent: 50,
            },
            max_prompt_messages: usize::MAX,
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
            loop_id,
            1,
            None,
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
            loop_id,
            1,
            None,
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
            loop_id,
            1,
            None,
        )
        .await;
        assert!(matches!(
            result,
            Err(RecoveryReconstructionError::Uncompressible)
        ));
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
}
