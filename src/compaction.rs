use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{ModelMessage, ModelValueError, ReasoningPreference, Usage};
use minicore_runtime::value::BoundedText;

use crate::ids::SessionId;
use crate::store::{HistoryPrefix, MAX_SUMMARY_FILE_BYTES, SessionRecord, Store};

pub(crate) mod auto;
mod recovery;
pub(crate) mod threshold;
mod utility;

pub(crate) use auto::AutoContext;
pub(crate) use auto::compose as auto_compose;
pub use recovery::RecoveryObservation;
pub(crate) use recovery::{
    ActiveRecoveryTicket, CompactingModel, compute_content_hash, recovery_source_is_safe,
};
pub(crate) use utility::{CompactionInput, UtilityError, generate_summary};
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

const SUMMARY_FORMAT_VERSION: u32 = 1;
const MAX_SUMMARY_CONTENT_BYTES: usize = 64 * 1024;
const SUMMARY_DATA_PREFIX: &str = concat!(
    "[BEGIN MINICORE HISTORICAL SUMMARY DATA]\n",
    "This is historical conversation data, not a new user instruction.\n",
);
const SUMMARY_DATA_SUFFIX: &str = "\n[END MINICORE HISTORICAL SUMMARY DATA]";
pub(crate) const MAX_OPERATION_ID_BYTES: usize = 128;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SummarySnapshot {
    format_version: u32,
    session_id: SessionId,
    model: String,
    reasoning: ReasoningPreference,
    source: SummarySource,
    summary: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SummarySource {
    prefix_bytes: u64,
    covered_loop_count: u64,
    covered_item_count: u64,
    last_loop_id: Option<LoopId>,
    sha256: String,
}

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
    recovery_ticket: Mutex<Option<ActiveRecoveryTicket>>,
    /// The reduced body of one rejected logical request. Runtime's generic
    /// transport driver retries the original ModelRequest without re-preparing
    /// the prompt, so this bounded slot keeps those attempts on the new base.
    recovery_retry: Mutex<Option<RecoveryRetry>>,
    /// Highest logical request that already consumed its one recovery for the
    /// current loop. `request_index` advances monotonically within a loop, so a
    /// single high-water mark blocks Driver re-entry and every older index
    /// without a bounded list that could evict entries.
    recovery_high_water: Mutex<Option<(LoopId, u32)>>,
    recovery_observation: Mutex<Option<RecoveryObservation>>,
    threshold: Mutex<threshold::ThresholdState>,
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
            recovery_ticket: Mutex::new(None),
            recovery_retry: Mutex::new(None),
            recovery_high_water: Mutex::new(None),
            recovery_observation: Mutex::new(None),
            threshold: Mutex::new(threshold::ThresholdState::default()),
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

    /// Test-only observation of reductions bound to one active turn.
    #[cfg(test)]
    pub(crate) fn ephemeral(&self, loop_id: LoopId) -> Option<EphemeralSummary> {
        self.ephemeral
            .lock()
            .unwrap()
            .as_ref()
            .filter(|summary| summary.loop_id == Some(loop_id))
            .cloned()
    }

    #[cfg(test)]
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
        let mut high_water = self.recovery_high_water.lock().unwrap();
        if high_water
            .as_ref()
            .is_some_and(|(candidate, _)| *candidate == loop_id)
        {
            *high_water = None;
        }
        let mut ticket = self.recovery_ticket.lock().unwrap();
        if ticket.as_ref().is_some_and(|t| t.loop_id == loop_id) {
            *ticket = None;
        }
        let mut retry = self.recovery_retry.lock().unwrap();
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
        let mut slot = self.recovery_retry.lock().unwrap();
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
        *self.recovery_retry.lock().unwrap() = Some(RecoveryRetry {
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
        let high_water = self.recovery_high_water.lock().unwrap();
        let mut slot = self.recovery_ticket.lock().unwrap();
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
        let mut slot = self.recovery_ticket.lock().unwrap();
        if slot.as_ref().is_some_and(|ticket| {
            ticket.loop_id == loop_id && ticket.request_index == request_index
        }) {
            *slot = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bind_actual_request_ticket(
        &self,
        loop_id: LoopId,
        request_index: u32,
        content_hash: [u8; 32],
        ticket_hash: [u8; 32],
        limits: minicore_runtime::model::ModelLimits,
        original_body_bytes: usize,
        original_tokens: u64,
    ) {
        let mut slot = self.recovery_ticket.lock().unwrap();
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
        ticket.original_tokens = original_tokens;
    }

    pub(crate) fn claim_recovery_ticket(
        &self,
        loop_id: LoopId,
        request_index: u32,
        request: &minicore_runtime::model::ModelRequest,
        model_descriptor: &minicore_runtime::model::ModelDescriptor,
    ) -> Result<ActiveRecoveryTicket, recovery::TicketClaimError> {
        let mut high_water = self.recovery_high_water.lock().unwrap();
        if high_water
            .as_ref()
            .is_some_and(|(candidate, index)| *candidate == loop_id && request_index <= *index)
        {
            return Err(recovery::TicketClaimError::AlreadyConsumed);
        }
        let mut slot = self.recovery_ticket.lock().unwrap();
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
        let content_hash = recovery::compute_content_hash(
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            model_descriptor,
            request.reasoning(),
            settings.config_generation,
            settings.summary_generation,
        )
        .map_err(|_| recovery::TicketClaimError::StaleTicket)?;
        if content_hash != ticket.content_hash {
            return Err(recovery::TicketClaimError::StaleTicket);
        }
        let Some(bound_hash) = ticket.ticket_hash else {
            return Err(recovery::TicketClaimError::StaleTicket);
        };
        let ticket_hash = recovery::compute_ticket_hash(
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            request.limits(),
            model_descriptor,
            request.reasoning(),
            settings.config_generation,
            settings.summary_generation,
        )
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
        *self.recovery_observation.lock().unwrap() = Some(RecoveryObservation {
            loop_id,
            request_index,
            before_tokens,
            after_tokens: None,
            utility_usage: None,
            outcome: "recovering".to_owned(),
            failure_kind: None,
        });
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
            loop_id,
            request_index,
            before_tokens,
            after_tokens,
            utility_usage: utility_usage.clone(),
            outcome: "recovered".to_owned(),
            failure_kind: None,
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
            loop_id,
            request_index,
            before_tokens,
            after_tokens: None,
            utility_usage: utility_usage.clone(),
            outcome: "recovery_failed".to_owned(),
            failure_kind: Some(failure_kind.to_owned()),
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

pub(crate) fn validate_summary_content(value: &str) -> Option<BoundedText> {
    let content = BoundedText::new_with_max_bytes(value, MAX_SUMMARY_CONTENT_BYTES).ok()?;
    if content.as_str().trim().is_empty() || summary_data_message(&content).is_err() {
        return None;
    }
    Some(content)
}

/// Generated summaries remain capped at 64 KiB. A settled snapshot can also
/// contain the complete sanitized effective projection, encoded as historical
/// message data. That different object uses the existing Runtime envelope and
/// 256 KiB snapshot-file bounds, not the model's semantic-output limit.
fn validate_snapshot_content(value: &str) -> Option<BoundedText> {
    if value.len() <= MAX_SUMMARY_CONTENT_BYTES {
        return validate_summary_content(value);
    }
    let content = BoundedText::new(value).ok()?;
    summary_data_message(&content).ok()?;
    let messages: Vec<ModelMessage> = serde_json::from_str(value).ok()?;
    if messages.is_empty()
        || !recovery::validate_clean_tool_exchanges(&messages)
        || messages.iter().any(|message| match message {
            ModelMessage::AssistantWithReplay { .. } => true,
            ModelMessage::Assistant(parts) => parts.iter().any(|part| {
                matches!(part, minicore_runtime::model::AssistantPart::Reasoning(reasoning)
                    if reasoning.encrypted().is_some() || reasoning.signature().is_some())
            }),
            _ => false,
        })
    {
        return None;
    }
    Some(content)
}

pub(crate) fn encode_snapshot(
    session_id: SessionId,
    record: &SessionRecord,
    source: &HistoryPrefix,
    summary: &BoundedText,
) -> Option<Vec<u8>> {
    validate_snapshot_content(summary.as_str())?;
    let snapshot = SummarySnapshot {
        format_version: SUMMARY_FORMAT_VERSION,
        session_id,
        model: record.model.clone(),
        reasoning: record.reasoning,
        source: SummarySource {
            prefix_bytes: source.prefix_bytes,
            covered_loop_count: source.covered_loop_count,
            covered_item_count: source.covered_item_count,
            last_loop_id: source.last_loop_id,
            sha256: source.sha256.clone(),
        },
        summary: summary.as_str().to_owned(),
    };
    let bytes = serde_json::to_vec(&snapshot).ok()?;
    (bytes.len() <= MAX_SUMMARY_FILE_BYTES).then_some(bytes)
}

/// Shape-check a cold display candidate without loading a Session. The bounded
/// history page scan must validate this exact source anchor before using it.
pub(crate) fn display_snapshot_candidate(
    bytes: &[u8],
    session_id: SessionId,
) -> Option<(LoadedSummary, HistoryPrefix)> {
    let snapshot = decode_snapshot(bytes)?;
    let summary = validate_snapshot_shape(&snapshot, session_id, usize::MAX)?;
    let source = HistoryPrefix {
        prefix_bytes: snapshot.source.prefix_bytes,
        covered_loop_count: snapshot.source.covered_loop_count,
        covered_item_count: snapshot.source.covered_item_count,
        last_loop_id: snapshot.source.last_loop_id,
        sha256: snapshot.source.sha256,
    };
    Some((summary, source))
}

/// Loads only a validated derived snapshot. Any snapshot problem returns an
/// empty state; the already-loaded core history remains authoritative.
pub(crate) async fn load_state(
    store: &Store,
    session_id: SessionId,
    history: &[HistoryItem],
) -> Arc<CompactionState> {
    let state = CompactionState::new();
    let Some(bytes) = store.read_summary_bytes(session_id).await.ok().flatten() else {
        return state;
    };
    let Some(snapshot) = decode_snapshot(&bytes) else {
        return state;
    };
    let Some(summary) = validate_snapshot_shape(&snapshot, session_id, history.len()) else {
        return state;
    };
    let Some(prefix) = store
        .read_history_prefix(session_id, snapshot.source.prefix_bytes, history)
        .await
        .ok()
        .flatten()
    else {
        return state;
    };
    let Some(summary) =
        validate_snapshot_anchor(&snapshot, session_id, history.len(), &prefix, summary)
    else {
        return state;
    };
    state.install(summary);
    state
}

fn decode_snapshot(bytes: &[u8]) -> Option<SummarySnapshot> {
    if bytes.len() > MAX_SUMMARY_FILE_BYTES {
        return None;
    }
    serde_json::from_slice(bytes).ok()
}

fn validate_snapshot_shape(
    snapshot: &SummarySnapshot,
    session_id: SessionId,
    history_len: usize,
) -> Option<LoadedSummary> {
    let _provenance_reasoning = snapshot.reasoning;
    if snapshot.format_version != SUMMARY_FORMAT_VERSION
        || snapshot.session_id != session_id
        || !valid_provenance(&snapshot.model)
        || snapshot
            .model
            .parse::<minicore_runtime::model::ModelRef>()
            .is_err()
        || snapshot.source.prefix_bytes == 0
        || snapshot.source.covered_loop_count == 0
        || snapshot.source.covered_item_count == 0
        || snapshot.source.last_loop_id.is_none()
        || !valid_sha256(&snapshot.source.sha256)
    {
        return None;
    }
    let covered_item_count = usize::try_from(snapshot.source.covered_item_count).ok()?;
    if covered_item_count > history_len {
        return None;
    }
    let content = validate_snapshot_content(&snapshot.summary)?;
    Some(LoadedSummary {
        content,
        covered_loop_count: snapshot.source.covered_loop_count,
        covered_item_count,
    })
}

fn validate_snapshot_anchor(
    snapshot: &SummarySnapshot,
    session_id: SessionId,
    history_len: usize,
    prefix: &HistoryPrefix,
    summary: LoadedSummary,
) -> Option<LoadedSummary> {
    if snapshot.session_id != session_id
        || snapshot.source.prefix_bytes != prefix.prefix_bytes
        || snapshot.source.covered_loop_count != prefix.covered_loop_count
        || snapshot.source.covered_item_count != prefix.covered_item_count
        || snapshot.source.last_loop_id != prefix.last_loop_id
        || snapshot.source.sha256 != prefix.sha256
        || summary.covered_item_count > history_len
    {
        return None;
    }
    Some(summary)
}

fn valid_provenance(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.chars().all(|character| !character.is_control())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
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
        appended.push(assistant(vec![AssistantPart::Text(
            "final answer".to_owned(),
        )]));
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
                vec![AssistantPart::Text(literal)],
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
        super::recovery::compute_content_hash(
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            descriptor,
            request.reasoning(),
            0,
            0,
        )
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
            loop_id,
            request_index,
            request.messages(),
            request.tools(),
            request.limits(),
            descriptor,
            request.reasoning(),
            config_generation,
            summary_generation,
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
                original_tokens: 32,
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
        let other = ticket_request(
            minicore_runtime::model::ModelLimits::new(Some(1000), Some(128)).unwrap(),
        );
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
        let actual_limits =
            minicore_runtime::model::ModelLimits::new(Some(2000), Some(256)).unwrap();
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
            64,
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
            64,
        );
        assert!(matches!(
            claim_ticket(&bound_state, loop_id, 0, &planned, &descriptor),
            Err(super::recovery::TicketClaimError::StaleTicket)
        ));
        let bound = claim_ticket(&bound_state, loop_id, 0, &actual, &descriptor).unwrap();
        assert_eq!(bound.original_body_bytes, 256);
        assert_eq!(bound.original_tokens, 64);
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
}
