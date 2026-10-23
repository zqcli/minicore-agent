use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{
    AssistantPart, DeliveryState, Model, ModelCallContext, ModelDescriptor, ModelError,
    ModelErrorKind, ModelMessage, ModelRequest, ModelStartFuture,
};
use minicore_runtime::value::BoundedText;

use super::auto::{
    AutoContext, GroupSummaryRequest, current_user_texts, estimate_tokens_with_budget,
    fold_history_with_ranges, merge_utility_usage, minimal_messages, summarize_group,
};
use super::{CONTEXT_UNCOMPRESSIBLE, CompactionState};
use crate::compaction::CompactionUtilityUsage;
use crate::models::ProviderBudget;

mod ticket;
pub(crate) use ticket::{
    ActiveRecoveryTicket, TicketClaimError, TicketFraming, compute_content_hash,
    compute_ticket_hash,
};

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

impl RecoveryObservation {
    pub(super) fn new(
        loop_id: LoopId,
        request_index: u32,
        before_tokens: Option<u64>,
        outcome: &str,
    ) -> Self {
        Self {
            loop_id,
            request_index,
            before_tokens,
            after_tokens: None,
            utility_usage: None,
            outcome: outcome.into(),
            failure_kind: None,
        }
    }
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

impl RecoveryReconstructionError {
    fn kind(&self) -> &str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Uncompressible => CONTEXT_UNCOMPRESSIBLE,
            Self::InvalidToolExchanges => "invalid_tool_exchanges",
            Self::Other(kind) => kind,
        }
    }
}

impl From<super::UtilityError> for RecoveryReconstructionError {
    fn from(error: super::UtilityError) -> Self {
        match error {
            super::UtilityError::Cancelled => Self::Cancelled,
            super::UtilityError::Timeout => Self::Timeout,
            other => Self::Other(other.kind().into()),
        }
    }
}

/// Folds the retained sources exactly as the provider will see them and
/// measures the real normalized request. Returns `None` when the projection
/// cannot be represented as a valid request.
fn measure_reconstruction(
    ticket: &ActiveRecoveryTicket,
    budget: &dyn ProviderBudget,
    projection: &super::auto::Projection<'_>,
    folded: &BTreeMap<super::EphemeralGroupKey, BoundedText>,
) -> Option<(ModelRequest, u64, usize)> {
    let messages = fold_history_with_ranges(
        &projection.fixed,
        &projection.history,
        &projection.ranges,
        folded,
        None,
    )
    .ok()?;
    let loop_id = ticket.loop_id;
    let request_index = ticket.request_index;
    if !validate_clean_tool_exchanges(&messages) {
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
pub(crate) async fn reconstruct_for_recovery(
    ticket: &ActiveRecoveryTicket,
    auto: &AutoContext,
    budget: &dyn ProviderBudget,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> (
    Result<ModelRequest, RecoveryReconstructionError>,
    Option<CompactionUtilityUsage>,
) {
    let mut utility_usage = None;
    let result = reconstruct_inner(
        ticket,
        auto,
        budget,
        cancellation,
        deadline,
        &mut utility_usage,
    )
    .await;
    (result, utility_usage)
}

async fn reconstruct_inner(
    ticket: &ActiveRecoveryTicket,
    auto: &AutoContext,
    budget: &dyn ProviderBudget,
    cancellation: &CancellationToken,
    deadline: Instant,
    utility_usage: &mut Option<CompactionUtilityUsage>,
) -> Result<ModelRequest, RecoveryReconstructionError> {
    let loop_id = ticket.loop_id;
    let request_index = ticket.request_index;
    super::utility::check_active(cancellation, deadline)?;

    // No baseline body means the retry can never be proven smaller; refuse
    // before spending utility calls on a summary with no comparison point.
    if ticket.original_body_bytes == 0 {
        return Err(RecoveryReconstructionError::Uncompressible);
    }

    let limits = auto.policy.budget(auto.model.descriptor().context_window);
    let hard_tokens = limits.hard_tokens;
    let users = current_user_texts(&ticket.base, &ticket.appended, loop_id);
    let minimal = match minimal_messages(&ticket.system, &users) {
        Ok(min) => min,
        Err(err) => {
            return Err(RecoveryReconstructionError::Other(format!(
                "minimal messages: {:?}",
                err.kind()
            )));
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
            return Err(RecoveryReconstructionError::Uncompressible);
        }
    };
    // The irreducible system + current User/Steer + tool schema floor is
    // checked before any utility call: summarization cannot remove it, and a
    // floor that already consumes the whole window leaves no room for a
    // summary at all.
    if minimal_tokens >= hard_tokens {
        return Err(RecoveryReconstructionError::Uncompressible);
    }
    let mut projection = super::auto::Projection::new(
        &auto.state,
        &ticket.system,
        ticket.summary.as_ref(),
        &ticket.base,
        &ticket.appended,
        loop_id,
    )?;
    if projection.consolidated.is_some() {
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
                merge_utility_usage(utility_usage, result.usage);
                Ok(result.request)
            }
            Ok(_) => Err(RecoveryReconstructionError::Uncompressible),
            Err(error) => {
                merge_utility_usage(utility_usage, error.usage);
                Err(error.error.into())
            }
        };
    }

    let mut folded = std::mem::take(&mut projection.folded);

    // A retry is only worthwhile when the real, provider-normalized body is
    // strictly smaller than the body that failed and stays inside the hard
    // window and message ceiling. Utility token deltas are not a shrink proof:
    // their accounting excludes opaque replay and the summary wrapper, and it
    // ignores summaries retained from earlier requests. Fold the real sources
    // and measure the actual request every time a group is replaced.
    let mut candidates = projection.ranges.iter().collect::<Vec<_>>();
    candidates.sort_by(|(left, left_bytes), (right, right_bytes)| {
        right_bytes
            .cmp(left_bytes)
            .then(left.start.cmp(&right.start))
    });
    let mut best = None;

    // Existing groups are already in the rejected request. A fresh rejection
    // must reduce a new source group, not win merely by dropping opaque replay.

    for (key, _) in candidates {
        if folded.contains_key(key) {
            continue;
        }
        if folded.len() >= super::MAX_EPHEMERAL_GROUPS {
            break;
        }
        super::utility::check_active(cancellation, deadline)?;
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
                merge_utility_usage(utility_usage, error.utility_usage);
                if matches!(error.error, super::UtilityError::NoProgress) {
                    continue;
                }
                return Err(error.error.into());
            }
        };

        merge_utility_usage(utility_usage, generation.utility_usage);
        folded.insert(key.clone(), generation.content);

        if let Some((request, tokens, bytes)) =
            measure_reconstruction(ticket, budget, &projection, &folded)
            && tokens <= hard_tokens
            && bytes < ticket.original_body_bytes
            && folded.values().map(BoundedText::byte_len).sum::<usize>()
                <= super::MAX_EPHEMERAL_BYTES
            && (tokens <= limits.trigger_tokens
                || best
                    .as_ref()
                    .is_none_or(|(_, best_bytes, _)| bytes < *best_bytes))
        {
            best = Some((request, bytes, folded.clone()));
            if tokens <= limits.trigger_tokens {
                break;
            }
        }
    }

    super::utility::check_active(cancellation, deadline)?;
    if let Some((request, _, groups)) = best {
        if auto.state.install_emergency_groups(loop_id, groups) {
            return Ok(request);
        }
        return Err(RecoveryReconstructionError::Uncompressible);
    }

    // Nothing retained or re-derived fits the window while being strictly
    // smaller than the failed body. Keep the structural failure explicit
    // instead of retrying an unchanged request.
    match fold_history_with_ranges(
        &projection.fixed,
        &projection.history,
        &projection.ranges,
        &folded,
        None,
    ) {
        Ok(messages) if !validate_clean_tool_exchanges(&messages) => {
            return Err(RecoveryReconstructionError::InvalidToolExchanges);
        }
        Err(err) => {
            return Err(RecoveryReconstructionError::Other(format!(
                "fold history: {:?}",
                err.kind()
            )));
        }
        Ok(_) => {}
    }
    Err(RecoveryReconstructionError::Uncompressible)
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
        let framing = TicketFraming {
            loop_id,
            request_index,
            messages: request.messages(),
            tools: request.tools(),
            model_descriptor: self.inner.descriptor(),
            reasoning: request.reasoning(),
            config_generation: 0,
            summary_generation: 0,
        };
        let original_hash = compute_ticket_hash(&framing, request.limits())?;
        if let Some(reduced) = self
            .state
            .recovered_request(loop_id, request_index, original_hash)
            .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?
        {
            check_start(&context)?;
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
        let framing = TicketFraming {
            config_generation: settings.config_generation,
            summary_generation: settings.summary_generation,
            ..framing
        };
        let content_hash = compute_content_hash(&framing);
        let ticket_hash = compute_ticket_hash(&framing, request.limits());
        match (content_hash, ticket_hash) {
            (Ok(content_hash), Ok(ticket_hash)) => self.state.bind_actual_request_ticket(
                loop_id,
                request_index,
                content_hash,
                ticket_hash,
                *request.limits(),
                original_body_bytes,
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
        check_start(&context)?;

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
            // A started request is never recovered, so its source copy is not kept.
            self.state.discard_recovery_ticket(loop_id, request_index);
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

        let fail = |kind: &str, usage, error| {
            self.state.record_recovery_failure(
                loop_id,
                request_index,
                Some(original_tokens),
                usage,
                kind,
            );
            error
        };
        check_start(&context)
            .map_err(|error| fail(model_error_kind_str(error.kind()), None, error))?;

        self.state
            .record_recovery_in_progress(loop_id, request_index, Some(original_tokens));

        let (reconstruct_res, utility_usage) = reconstruct_for_recovery(
            &ticket,
            &auto,
            &*self.budget,
            &context.cancellation,
            context.deadline,
        )
        .await;

        let reduced_request = match reconstruct_res {
            Ok(req) => req,
            Err(failure) => {
                if matches!(failure, RecoveryReconstructionError::Uncompressible) {
                    self.state.note_prepare_failure(CONTEXT_UNCOMPRESSIBLE);
                }
                let returned = match &failure {
                    RecoveryReconstructionError::Cancelled => {
                        local_model_error(ModelErrorKind::Cancelled)
                    }
                    RecoveryReconstructionError::Timeout => {
                        local_model_error(ModelErrorKind::Timeout)
                    }
                    _ => error,
                };
                return Err(fail(failure.kind(), utility_usage, returned));
            }
        };
        check_start(&context).map_err(|error| {
            fail(
                model_error_kind_str(error.kind()),
                utility_usage.clone(),
                error,
            )
        })?;

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
            _ = cancellation.cancelled() => Err(unknown_model_error(ModelErrorKind::Cancelled)),
            _ = tokio::time::sleep_until(deadline) => Err(unknown_model_error(ModelErrorKind::Timeout)),
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
            Err(second_error) => Err(fail(
                model_error_kind_str(second_error.kind()),
                utility_usage,
                second_error,
            )),
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
    .expect("valid diagnostic text")
}

fn check_start(context: &ModelCallContext) -> Result<(), ModelError> {
    if context.cancellation.is_cancelled() {
        Err(local_model_error(ModelErrorKind::Cancelled))
    } else if Instant::now() >= context.deadline {
        Err(local_model_error(ModelErrorKind::Timeout))
    } else {
        Ok(())
    }
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
