use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::Serialize;

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{ModelMessage, ModelValueError, ReasoningPreference, Usage};
use minicore_runtime::value::BoundedText;

use crate::ids::SessionId;
use crate::store::{HistoryPrefix, MAX_SUMMARY_FILE_BYTES, SessionRecord, Store};

pub(crate) mod projection;
pub(crate) use projection as auto;
mod recovery;
mod snapshot;
pub(crate) mod threshold;
mod utility;
use snapshot::validate_snapshot_content;
pub(crate) use snapshot::{
    display_snapshot_candidate, encode_snapshot, load_state, validate_summary_content,
};

pub(crate) use projection::AutoContext;
pub(crate) use projection::compose as auto_compose;
pub use recovery::RecoveryObservation;
pub(crate) use recovery::{
    ActiveRecoveryTicket, CompactingModel, TicketFraming, compute_content_hash,
};
pub(crate) use utility::{
    CompactionInput, UtilityError, generate_summary, generate_summary_with_tail, recent_tail_start,
};
/// Failure kind recorded when request preparation cannot reduce a context
/// without dropping user constraints or replaying tools.
pub(crate) const CONTEXT_UNCOMPRESSIBLE: &str = "context_uncompressible";
const MAX_EPHEMERAL_GROUPS: usize = 64;
const MAX_EPHEMERAL_BYTES: usize = 512 * 1024;

/// Agent-global policy for request-boundary/post-turn compaction and bounded
/// upstream capacity recovery. Manual compaction remains available regardless.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CompactionPolicy {
    pub(crate) enabled: bool,
    pub(crate) trigger_percent: u8,
    pub(crate) target_percent: u8,
}

impl CompactionPolicy {
    pub(crate) fn history_triggered(
        self,
        items: usize,
        bytes: usize,
        limits: &minicore_runtime::LoopLimits,
    ) -> bool {
        let reached = |value: usize, limit: usize| {
            value as u128 * 100 >= limit as u128 * u128::from(self.trigger_percent)
        };
        reached(items, limits.max_history_items) || reached(bytes, limits.max_history_bytes)
    }

    pub(crate) const fn budget(self, context_window: u64) -> InputBudget {
        InputBudget {
            hard_tokens: context_window,
            trigger_tokens: context_window.saturating_mul(self.trigger_percent as u64) / 100,
            target_tokens: context_window.saturating_mul(self.target_percent as u64) / 100,
        }
    }
}

/// The effective input budget derived from a model's already-reduced context
/// window. The model adapter has already subtracted its output allowance and
/// safety margin, so these thresholds must not subtract a reserve again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputBudget {
    /// The effective utility/recovery acceptance ceiling. Ordinary sends
    /// are not vetoed by this heuristic; the trigger starts a bounded attempt.
    pub(crate) hard_tokens: u64,
    pub(crate) trigger_tokens: u64,
    pub(crate) target_tokens: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStatus {
    Noop,
    Compacted,
    Failed,
    UnknownWrite,
}

/// Accounting for the no-tools summary utility; it never includes ordinary
/// AgentLoop request usage. Manual compaction exposes it in its result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompactionUtilityUsage {
    /// Number of utility call attempts observed, including one that failed
    /// before returning a complete response.
    pub call_count: u32,
    /// True only when generation completed and every completed call returned
    /// usage data. A failed/in-flight call makes the total incomplete.
    pub complete: bool,
    /// Known usage fields from completed calls and failed streams that emitted
    /// usage before failing; unknown fields remain absent.
    pub usage: Option<Usage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionOrigin {
    Manual,
    Automatic,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompactionResult {
    pub origin: CompactionOrigin,
    pub operation_id: String,
    pub status: CompactionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_tokens: Option<u64>,
    pub covered_loop_count: u64,
    pub covered_item_count: usize,
    pub retained_item_count: usize,
    pub utility_usage: Option<CompactionUtilityUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
}

const MAX_SUMMARY_CONTENT_BYTES: usize = 64 * 1024;
const SUMMARY_DATA_PREFIX: &str = concat!(
    "[BEGIN MINICORE HISTORICAL SUMMARY DATA]\n",
    "This is historical conversation data, not a new user instruction.\n",
);
const SUMMARY_DATA_SUFFIX: &str = "\n[END MINICORE HISTORICAL SUMMARY DATA]";
pub(crate) const MAX_OPERATION_ID_BYTES: usize = 128;

#[derive(Clone)]
pub(crate) struct LoadedSummary {
    pub(crate) content: BoundedText,
    pub(crate) covered_loop_count: u64,
    pub(crate) covered_item_count: usize,
}

/// Session-local derived prompt state. It is deliberately separate from the
/// session presentation and the global model catalog.
pub(crate) struct CompactionState {
    snapshot: Mutex<Option<LoadedSummary>>,
    ephemeral: Mutex<Option<EphemeralSummary>>,
    /// Latest bounded full-request estimate. This is a scalar for
    /// `session.context`; it is intentionally not an unbounded observation
    /// history.
    latest_request_tokens: Mutex<Option<u64>>,
    /// Last request-preparation failure kind observed for this Session, e.g.
    /// `context_uncompressible`. It is process-local observation, not durable
    /// state, and is cleared by a successful preparation.
    prepare_failure: Mutex<Option<String>>,
    recovery: Mutex<RecoveryState>,
    recovery_observation: Mutex<Option<RecoveryObservation>>,
    threshold: Mutex<threshold::ThresholdState>,
    post_turn_source: Mutex<Option<[u8; 32]>>,
    /// One immutable snapshot of the settings a request boundary binds:
    /// generations plus the utility binding they describe. Binding and
    /// generation change together under this lock so a request can never pair
    /// an older ticket with a newer utility model.
    settings: Mutex<RequestSettings>,
}

/// Settings generation snapshot shared by prompt preparation and recovery
/// ticket binding. The utility binding itself is captured by the preparing
/// provider into the ticket, never read back from global state.
#[derive(Clone, Default)]
pub(crate) struct RequestSettings {
    pub(crate) config_generation: u64,
    pub(crate) summary_generation: u64,
}

#[derive(Default)]
struct RecoveryState {
    ticket: Option<ActiveRecoveryTicket>,
    // Delivery retries must reuse the reduced request without another utility call.
    retry: Option<RecoveryRetry>,
    // Monotonic within a loop: consuming a ticket also excludes older requests.
    high_water: Option<(LoopId, u32)>,
}

struct RecoveryRetry {
    loop_id: LoopId,
    request_index: u32,
    original_hash: [u8; 32],
    request: minicore_runtime::model::ModelRequest,
}

/// Bounded turn-local emergency reductions of complete source groups. Source
/// hashes must match on every projection; settled history is promoted through
/// the existing derived snapshot, never by rewriting the transcript.
#[derive(Clone, Default)]
pub(crate) struct EphemeralSummary {
    pub(crate) loop_id: Option<LoopId>,
    /// Ephemeral summary keyed by the owning loop's source range and content
    /// hash. The range is stable while the loop is live and the hash prevents
    /// reuse after the source history changes.
    pub(crate) groups: BTreeMap<EphemeralGroupKey, BoundedText>,
}

/// Stable identity for one ephemeral source group. The hash prevents a
/// summary from being reused when the same range contains different history.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct EphemeralGroupKey {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) source_hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AutomaticCompactionObservation {
    pub operation_id: String,
    pub loop_id: Option<LoopId>,
    pub request_index: Option<u32>,
    pub before_tokens: Option<u64>,
    pub after_tokens: Option<u64>,
    pub utility_before_tokens: Option<u64>,
    pub utility_after_tokens: Option<u64>,
    pub hard_tokens: u64,
    pub trigger_tokens: u64,
    pub target_tokens: u64,
    pub utility_usage: Option<CompactionUtilityUsage>,
    pub outcome: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AutomaticCompactionView {
    pub current: Option<AutomaticCompactionObservation>,
    pub last: Option<AutomaticCompactionObservation>,
}

pub(crate) struct HistoryProjection<'a> {
    pub(crate) summary: BoundedText,
    pub(crate) suffix: &'a [HistoryItem],
}

impl CompactionState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            snapshot: Mutex::new(None),
            ephemeral: Mutex::new(None),
            latest_request_tokens: Mutex::new(None),
            prepare_failure: Mutex::new(None),
            recovery: Mutex::new(RecoveryState::default()),
            recovery_observation: Mutex::new(None),
            threshold: Mutex::new(threshold::ThresholdState::default()),
            post_turn_source: Mutex::new(None),
            settings: Mutex::new(RequestSettings::default()),
        })
    }

    fn install(&self, summary: LoadedSummary) {
        *self.snapshot.lock().unwrap() = Some(summary);
        let mut settings = self.settings.lock().unwrap();
        settings.summary_generation = settings.summary_generation.wrapping_add(1);
    }

    pub(crate) fn publish(
        &self,
        content: BoundedText,
        covered_loop_count: u64,
        covered_item_count: usize,
    ) {
        self.install(LoadedSummary {
            content,
            covered_loop_count,
            covered_item_count,
        });
    }

    pub(crate) fn display_snapshot(&self) -> Option<LoadedSummary> {
        self.snapshot.lock().unwrap().clone()
    }

    pub(crate) fn coverage(&self) -> Option<(u64, usize)> {
        self.snapshot
            .lock()
            .unwrap()
            .as_ref()
            .map(|summary| (summary.covered_loop_count, summary.covered_item_count))
    }

    pub(crate) fn project<'a>(&self, base: &'a [HistoryItem]) -> Option<HistoryProjection<'a>> {
        let summary = self.snapshot.lock().unwrap().clone()?;
        if summary.covered_item_count > base.len() {
            return None;
        }
        Some(HistoryProjection {
            summary: summary.content,
            suffix: &base[summary.covered_item_count..],
        })
    }

    /// Returns only source-identical reductions installed by a successful
    /// emergency reconstruction. Configuration changes affect future request
    /// framing, not already covered source history.
    pub(crate) fn emergency_groups(
        &self,
        loop_id: LoopId,
        valid_keys: &BTreeSet<EphemeralGroupKey>,
    ) -> BTreeMap<EphemeralGroupKey, BoundedText> {
        let slot = self.ephemeral.lock().unwrap();
        slot.as_ref()
            .filter(|summary| summary.loop_id == Some(loop_id))
            .map(|summary| {
                summary
                    .groups
                    .iter()
                    .filter(|(key, _)| valid_keys.contains(*key))
                    .map(|(key, content)| (key.clone(), content.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn install_emergency_groups(
        &self,
        loop_id: LoopId,
        groups: BTreeMap<EphemeralGroupKey, BoundedText>,
    ) -> bool {
        if groups.len() > MAX_EPHEMERAL_GROUPS
            || groups.values().map(BoundedText::byte_len).sum::<usize>() > MAX_EPHEMERAL_BYTES
        {
            return false;
        }
        let mut slot = self.ephemeral.lock().unwrap();
        if slot
            .as_ref()
            .and_then(|summary| summary.loop_id)
            .is_some_and(|owner| owner != loop_id)
        {
            return false;
        }
        *slot = Some(EphemeralSummary {
            loop_id: Some(loop_id),
            groups,
        });
        let mut settings = self.settings.lock().unwrap();
        settings.summary_generation = settings.summary_generation.wrapping_add(1);
        true
    }

    /// Promote a settled turn's effective history without another model call.
    /// Source ranges are validated against the raw Runtime report first; only
    /// sanitized messages are encoded into the historical snapshot data.
    pub(crate) fn settled_emergency_summary(
        &self,
        loop_id: LoopId,
        base: &[HistoryItem],
        appended: &[HistoryItem],
        previous: Option<&BoundedText>,
    ) -> Result<Option<BoundedText>, ()> {
        let items = base.iter().chain(appended).collect::<Vec<_>>();
        let ranges = auto::compressible_ranges(base.len(), &items, loop_id).map_err(|_| ())?;
        let valid = ranges.iter().map(|(key, _)| key.clone()).collect();
        let groups = self.emergency_groups(loop_id, &valid);
        let consolidated = self.consolidated(loop_id, previous, &valid);
        if groups.is_empty() && consolidated.is_none() {
            return Ok(None);
        }
        let mut messages = Vec::new();
        if let Some(summary) = consolidated
            .as_ref()
            .map(|value| &value.content)
            .or(previous)
        {
            messages.push(summary_data_message(summary).map_err(|_| ())?);
        }
        let mut index = 0;
        while index < items.len() {
            if let Some(key) = consolidated
                .as_ref()
                .and_then(|value| value.covered.iter().find(|key| key.start == index))
            {
                index = key.end;
                continue;
            }
            if let Some((key, _)) = ranges
                .iter()
                .find(|(key, _)| key.start == index)
                .filter(|(key, _)| groups.contains_key(key))
            {
                messages.push(summary_data_message(&groups[key]).map_err(|_| ())?);
                index = key.end;
            } else {
                // Fold by raw source indexes, then sanitize uncovered items.
                // Sanitization can remove opaque-only assistants, so indexing
                // a pre-sanitized vector with raw group offsets is unsafe.
                let sanitized =
                    crate::history::sanitize_history(std::slice::from_ref(items[index]))
                        .map_err(|_| ())?;
                for item in sanitized.iter() {
                    messages.push(utility::history_message(item).map_err(|_| ())?);
                }
                index += 1;
            }
        }
        let content = serde_json::to_string(&messages).map_err(|_| ())?;
        validate_snapshot_content(&content).map(Some).ok_or(())
    }

    pub(crate) fn clear_ephemeral(&self, loop_id: LoopId) {
        let mut projection_removed = self.clear_threshold(loop_id);
        let mut ephemeral = self.ephemeral.lock().unwrap();
        if ephemeral
            .as_ref()
            .is_some_and(|summary| summary.loop_id == Some(loop_id))
        {
            // The settled snapshot transition owns any cross-turn reduction.
            // Never carry range indexes into a different turn.
            projection_removed |= ephemeral
                .as_ref()
                .is_some_and(|value| !value.groups.is_empty());
            *ephemeral = None;
        }
        if projection_removed {
            let mut settings = self.settings.lock().unwrap();
            settings.summary_generation = settings.summary_generation.wrapping_add(1);
        }
        let mut recovery = self.recovery.lock().unwrap();
        let RecoveryState {
            high_water,
            ticket: slot,
            retry,
        } = &mut *recovery;
        if high_water
            .as_ref()
            .is_some_and(|(candidate, _)| *candidate == loop_id)
        {
            *high_water = None;
        }
        if slot.as_ref().is_some_and(|t| t.loop_id == loop_id) {
            *slot = None;
        }
        if retry
            .as_ref()
            .is_some_and(|request| request.loop_id == loop_id)
        {
            *retry = None;
        }
    }

    pub(crate) fn recovered_request(
        &self,
        loop_id: LoopId,
        request_index: u32,
        original_hash: [u8; 32],
    ) -> Result<Option<minicore_runtime::model::ModelRequest>, ()> {
        let mut recovery = self.recovery.lock().unwrap();
        let slot = &mut recovery.retry;
        let Some(retry) = slot.as_ref() else {
            return Ok(None);
        };
        if retry.loop_id != loop_id || retry.request_index != request_index {
            *slot = None;
            return Ok(None);
        }
        if retry.original_hash != original_hash {
            return Err(());
        }
        Ok(Some(retry.request.clone()))
    }

    pub(crate) fn retain_recovered_request(
        &self,
        loop_id: LoopId,
        request_index: u32,
        original_hash: [u8; 32],
        request: minicore_runtime::model::ModelRequest,
    ) {
        self.recovery.lock().unwrap().retry = Some(RecoveryRetry {
            loop_id,
            request_index,
            original_hash,
            request,
        });
    }

    /// Configuration changes invalidate pending request tickets, not already
    /// installed emergency reductions. Their source identity remains valid
    /// while request framing and hard budgets are recomputed.
    pub(crate) fn note_settings_installed(&self) {
        let mut settings = self.settings.lock().unwrap();
        settings.config_generation = settings.config_generation.wrapping_add(1);
    }

    /// Returns the generations and utility binding as one consistent snapshot.
    pub(crate) fn request_settings(&self) -> RequestSettings {
        self.settings.lock().unwrap().clone()
    }

    /// Replaces the pending recovery ticket for `(loop_id, request_index)`.
    /// `None` clears it, so a preparation that no longer has a bounded source
    /// cannot leave an older ticket behind for the same logical request.
    pub(crate) fn register_recovery_ticket(
        &self,
        loop_id: LoopId,
        request_index: u32,
        ticket: Option<ActiveRecoveryTicket>,
    ) {
        let mut recovery = self.recovery.lock().unwrap();
        let RecoveryState {
            high_water,
            ticket: slot,
            ..
        } = &mut *recovery;
        if high_water
            .as_ref()
            .is_some_and(|(candidate, index)| *candidate == loop_id && request_index <= *index)
        {
            *slot = None;
            return;
        }
        *slot = ticket;
    }

    /// Drops the pending ticket only when it belongs to this logical request.
    /// Used when the request could not be bound (serialization failure or a
    /// content change after preparation) so recovery fails closed.
    pub(crate) fn discard_recovery_ticket(&self, loop_id: LoopId, request_index: u32) {
        let mut recovery = self.recovery.lock().unwrap();
        let slot = &mut recovery.ticket;
        if slot.as_ref().is_some_and(|ticket| {
            ticket.loop_id == loop_id && ticket.request_index == request_index
        }) {
            *slot = None;
        }
    }

    pub(crate) fn bind_actual_request_ticket(
        &self,
        loop_id: LoopId,
        request_index: u32,
        content_hash: [u8; 32],
        ticket_hash: [u8; 32],
        limits: minicore_runtime::model::ModelLimits,
        original_body_bytes: usize,
    ) {
        let mut recovery = self.recovery.lock().unwrap();
        let slot = &mut recovery.ticket;
        let Some(ticket) = slot.as_ref() else {
            return;
        };
        if ticket.loop_id != loop_id || ticket.request_index != request_index {
            return;
        }
        // Preparation hashed the planned messages with placeholder limits. A
        // different content under the same logical index means the ticket
        // sources no longer describe the request that is actually starting.
        if ticket.content_hash != content_hash {
            *slot = None;
            return;
        }
        let ticket = slot.as_mut().expect("checked above");
        ticket.ticket_hash = Some(ticket_hash);
        ticket.limits = limits;
        ticket.original_body_bytes = original_body_bytes;
    }

    pub(crate) fn claim_recovery_ticket(
        &self,
        loop_id: LoopId,
        request_index: u32,
        request: &minicore_runtime::model::ModelRequest,
        model_descriptor: &minicore_runtime::model::ModelDescriptor,
    ) -> Result<ActiveRecoveryTicket, recovery::TicketClaimError> {
        let mut recovery = self.recovery.lock().unwrap();
        let RecoveryState {
            high_water,
            ticket: slot,
            ..
        } = &mut *recovery;
        if high_water
            .as_ref()
            .is_some_and(|(candidate, index)| *candidate == loop_id && request_index <= *index)
        {
            return Err(recovery::TicketClaimError::AlreadyConsumed);
        }
        let Some(ticket) = slot.as_ref() else {
            return Err(recovery::TicketClaimError::NoTicket);
        };
        if ticket.loop_id != loop_id || ticket.request_index != request_index {
            return Err(recovery::TicketClaimError::IndexMismatch);
        }
        // Settings are read fresh here, not from the pre-start snapshot: a
        // config or summary change during the raw start must invalidate the
        // ticket instead of silently recovering with stale sources.
        let settings = self.request_settings();
        let framing = recovery::TicketFraming {
            loop_id,
            request_index,
            messages: request.messages(),
            tools: request.tools(),
            model_descriptor,
            reasoning: request.reasoning(),
            config_generation: settings.config_generation,
            summary_generation: settings.summary_generation,
        };
        let content_hash = recovery::compute_content_hash(&framing)
            .map_err(|_| recovery::TicketClaimError::StaleTicket)?;
        if content_hash != ticket.content_hash {
            return Err(recovery::TicketClaimError::StaleTicket);
        }
        let Some(bound_hash) = ticket.ticket_hash else {
            return Err(recovery::TicketClaimError::StaleTicket);
        };
        let ticket_hash = recovery::compute_ticket_hash(&framing, request.limits())
            .map_err(|_| recovery::TicketClaimError::StaleTicket)?;
        if ticket_hash != bound_hash {
            return Err(recovery::TicketClaimError::StaleTicket);
        }
        *high_water = Some((loop_id, request_index));
        Ok(slot.take().expect("ticket checked above"))
    }

    pub(crate) fn record_recovery_in_progress(
        &self,
        loop_id: LoopId,
        request_index: u32,
        before_tokens: Option<u64>,
    ) {
        *self.recovery_observation.lock().unwrap() = Some(RecoveryObservation::new(
            loop_id,
            request_index,
            before_tokens,
            "recovering",
        ));
    }

    pub(crate) fn record_recovery_success(
        &self,
        loop_id: LoopId,
        request_index: u32,
        before_tokens: Option<u64>,
        after_tokens: Option<u64>,
        utility_usage: Option<CompactionUtilityUsage>,
    ) {
        *self.recovery_observation.lock().unwrap() = Some(RecoveryObservation {
            after_tokens,
            utility_usage,
            ..RecoveryObservation::new(loop_id, request_index, before_tokens, "recovered")
        });
    }

    pub(crate) fn record_recovery_failure(
        &self,
        loop_id: LoopId,
        request_index: u32,
        before_tokens: Option<u64>,
        utility_usage: Option<CompactionUtilityUsage>,
        failure_kind: &str,
    ) {
        *self.recovery_observation.lock().unwrap() = Some(RecoveryObservation {
            utility_usage,
            failure_kind: Some(failure_kind.to_owned()),
            ..RecoveryObservation::new(loop_id, request_index, before_tokens, "recovery_failed")
        });
    }

    pub(crate) fn recovery_observation(&self) -> Option<RecoveryObservation> {
        self.recovery_observation.lock().unwrap().clone()
    }

    pub(crate) fn note_request_estimate(&self, tokens: u64) {
        *self.latest_request_tokens.lock().unwrap() = Some(tokens);
    }

    pub(crate) fn latest_request_tokens(&self) -> Option<u64> {
        *self.latest_request_tokens.lock().unwrap()
    }

    pub(crate) fn post_turn_source(&self) -> Option<[u8; 32]> {
        *self.post_turn_source.lock().unwrap()
    }

    pub(crate) fn note_post_turn_source(&self, source: [u8; 32]) {
        *self.post_turn_source.lock().unwrap() = Some(source);
    }

    pub(crate) fn clear_post_turn_source(&self) {
        *self.post_turn_source.lock().unwrap() = None;
    }

    pub(crate) fn automatic_view(&self) -> AutomaticCompactionView {
        self.threshold_view()
    }

    pub(crate) fn note_prepare_failure(&self, kind: &str) {
        *self.prepare_failure.lock().unwrap() = Some(kind.to_owned());
    }

    pub(crate) fn clear_prepare_failure(&self) {
        *self.prepare_failure.lock().unwrap() = None;
    }

    pub(crate) fn prepare_failure(&self) -> Option<String> {
        self.prepare_failure.lock().unwrap().clone()
    }
}

pub(crate) fn summary_data_message(content: &BoundedText) -> Result<ModelMessage, ModelValueError> {
    let mut envelope = String::with_capacity(
        SUMMARY_DATA_PREFIX.len() + content.byte_len() + SUMMARY_DATA_SUFFIX.len(),
    );
    envelope.push_str(SUMMARY_DATA_PREFIX);
    envelope.push_str(content.as_str());
    envelope.push_str(SUMMARY_DATA_SUFFIX);
    ModelMessage::user(envelope)
}

pub(crate) fn valid_operation_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_OPERATION_ID_BYTES
        && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

#[cfg(test)]
mod tests;
