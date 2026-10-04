//! Request-boundary compaction. Derived projection only; never rewrites history.
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{
    ModelDescriptor, ModelLimits, ModelMessage, ModelRequest, ReasoningPreference,
};
use minicore_runtime::tools::ToolSpec;
use minicore_runtime::value::BoundedText;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::auto::{AutoContext, compressible_ranges, fold_history_with_ranges};
use super::utility::{self, CompactionInput, SourcePart, UtilityError};
use super::{
    AutomaticCompactionObservation, AutomaticCompactionView, CompactionState,
    CompactionUtilityUsage, EphemeralGroupKey,
};

#[derive(Clone)]
pub(crate) struct ConsolidatedSummary {
    pub(crate) loop_id: LoopId,
    pub(crate) original_summary_hash: [u8; 32],
    pub(crate) covered: BTreeSet<EphemeralGroupKey>,
    pub(crate) content: BoundedText,
}

#[derive(Clone)]
struct IssuedProjection {
    loop_id: LoopId,
    request_index: u32,
    summary_generation: u64,
    framing: [u8; 32],
    provider_identity: usize,
}

#[derive(Default)]
pub(super) struct ThresholdState {
    consolidated: Option<ConsolidatedSummary>,
    attempted: Option<(LoopId, u32)>,
    source: Option<[u8; 32]>,
    issued: Option<IssuedProjection>,
    observation: AutomaticCompactionView,
}

struct HashWriter(Sha256);
impl std::io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn source_hash(value: &impl Serialize) -> Result<[u8; 32], UtilityError> {
    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| UtilityError::Serialization)?;
    Ok(writer.0.finalize().into())
}

fn framing_hash(
    descriptor: &ModelDescriptor,
    request: &ModelRequest,
    config_generation: u64,
) -> Result<[u8; 32], UtilityError> {
    let system = request
        .messages()
        .iter()
        .filter(|item| matches!(item, ModelMessage::System(_)))
        .collect::<Vec<_>>();
    source_hash(&(
        descriptor.model_ref.as_str(),
        descriptor.context_window,
        descriptor.supports_tools,
        &descriptor.supported_reasoning,
        request.reasoning(),
        system,
        request.tools(),
        config_generation,
    ))
}

impl CompactionState {
    pub(crate) fn consolidated(
        &self,
        loop_id: LoopId,
        summary: Option<&BoundedText>,
        valid: &BTreeSet<EphemeralGroupKey>,
    ) -> Option<ConsolidatedSummary> {
        let hash = source_hash(&summary).ok()?;
        self.threshold
            .lock()
            .unwrap()
            .consolidated
            .as_ref()
            .filter(|entry| {
                entry.loop_id == loop_id
                    && entry.original_summary_hash == hash
                    && entry.covered.is_subset(valid)
            })
            .cloned()
    }

    fn install_consolidated(&self, value: ConsolidatedSummary) -> bool {
        if value.covered.len() > 4096 || value.content.byte_len() > super::MAX_SUMMARY_CONTENT_BYTES
        {
            return false;
        }
        let mut state = self.threshold.lock().unwrap();
        if state
            .consolidated
            .as_ref()
            .is_some_and(|old| old.loop_id != value.loop_id)
        {
            return false;
        }
        state.consolidated = Some(value);
        drop(state);
        let mut settings = self.settings.lock().unwrap();
        settings.summary_generation = settings.summary_generation.wrapping_add(1);
        true
    }

    pub(super) fn clear_threshold(&self, loop_id: LoopId) -> bool {
        let mut state = self.threshold.lock().unwrap();
        let removed = state
            .consolidated
            .as_ref()
            .is_some_and(|entry| entry.loop_id == loop_id);
        if removed {
            state.consolidated = None;
        }
        if state.attempted.is_some_and(|(owner, _)| owner == loop_id) {
            state.attempted = None;
        }
        removed
    }

    pub(crate) fn note_normalized_projection(
        &self,
        raw: &[HistoryItem],
        normalized: &[HistoryItem],
    ) {
        if raw != normalized {
            self.threshold.lock().unwrap().issued = None;
        }
    }

    pub(crate) fn note_issued_projection(
        &self,
        loop_id: LoopId,
        request_index: u32,
        descriptor: &ModelDescriptor,
        request: &ModelRequest,
        budget: &dyn crate::models::ProviderBudget,
    ) {
        let settings = self.request_settings();
        let issued = framing_hash(descriptor, request, settings.config_generation)
            .ok()
            .map(|framing| IssuedProjection {
                loop_id,
                request_index,
                summary_generation: settings.summary_generation,
                framing,
                provider_identity: budget as *const dyn crate::models::ProviderBudget as *const ()
                    as usize,
            });
        self.threshold.lock().unwrap().issued = issued;
    }

    pub(crate) fn threshold_estimate(
        &self,
        descriptor: &ModelDescriptor,
        request: &ModelRequest,
        history: &[&HistoryItem],
        fallback: u64,
        budget: &dyn crate::models::ProviderBudget,
    ) -> u64 {
        let settings = self.request_settings();
        let issued = self.threshold.lock().unwrap().issued.clone();
        let Some(issued) = issued.filter(|anchor| {
            anchor.summary_generation == settings.summary_generation
                && anchor.provider_identity
                    == budget as *const dyn crate::models::ProviderBudget as *const () as usize
        }) else {
            return fallback;
        };
        if framing_hash(descriptor, request, settings.config_generation).ok()
            != Some(issued.framing)
        {
            return fallback;
        }
        let Some((index, usage)) =
            history
                .iter()
                .enumerate()
                .rev()
                .find_map(|(index, item)| match item {
                    HistoryItem::Assistant(assistant)
                        if assistant.loop_id == issued.loop_id
                            && assistant.request_index == issued.request_index =>
                    {
                        crate::presentation::context_tokens(assistant.usage)
                            .filter(|value| *value > 0)
                            .map(|usage| (index, usage))
                    }
                    _ => None,
                })
        else {
            return fallback;
        };
        let mut tokens = usage;
        for item in &history[index + 1..] {
            let Ok(message) = utility::history_message(item) else {
                return fallback;
            };
            let Ok(bytes) = serde_json::to_vec(&message) else {
                return fallback;
            };
            let Some(next) = tokens.checked_add(bytes.len().div_ceil(4) as u64) else {
                return fallback;
            };
            tokens = next;
        }
        tokens
    }

    /// Qualify durability without erasing the original attempt or its usage.
    /// In particular, an unknown write is not a confirmed rollback.
    pub(crate) fn note_settlement_failure(&self, loop_id: LoopId, unknown: bool) {
        let mut state = self.threshold.lock().unwrap();
        if let Some(last) = state
            .observation
            .last
            .as_mut()
            .filter(|last| last.loop_id == Some(loop_id))
        {
            if !last.outcome.ends_with("_settlement_failed")
                && !last.outcome.ends_with("_settlement_unknown")
            {
                last.outcome.push_str(if unknown {
                    "_settlement_unknown"
                } else {
                    "_settlement_failed"
                });
            }
        }
    }

    pub(super) fn threshold_view(&self) -> AutomaticCompactionView {
        self.threshold.lock().unwrap().observation.clone()
    }
}

pub(crate) struct ProjectionInput<'a> {
    pub(crate) auto: &'a AutoContext,
    pub(crate) system: &'a BoundedText,
    pub(crate) summary: Option<&'a BoundedText>,
    pub(crate) base: &'a [HistoryItem],
    pub(crate) appended: &'a [HistoryItem],
    pub(crate) tools: &'a [ToolSpec],
    pub(crate) reasoning: ReasoningPreference,
    pub(crate) limits: ModelLimits,
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: &'a CancellationToken,
    pub(crate) rejected_body_bytes: Option<usize>,
}

pub(crate) struct ProjectionResult {
    pub(crate) request: ModelRequest,
    pub(crate) usage: Option<CompactionUtilityUsage>,
    pub(crate) compacted: bool,
}

#[cfg(test)]
impl ProjectionResult {
    fn into_compacted(self) -> Option<Self> {
        self.compacted.then_some(self)
    }
}

#[derive(Debug)]
pub(crate) struct ProjectionFailure {
    pub(crate) error: UtilityError,
    pub(crate) usage: Option<CompactionUtilityUsage>,
}
impl From<UtilityError> for ProjectionFailure {
    fn from(error: UtilityError) -> Self {
        Self { error, usage: None }
    }
}

/// `recovery` is used only after the existing explicit upstream/ticket gate.
/// Threshold may retain the newest exchange; recovery can compress that exchange.
pub(crate) async fn consolidate(
    input: ProjectionInput<'_>,
    recovery: bool,
) -> Result<ProjectionResult, ProjectionFailure> {
    let auto = input.auto;
    let state = &auto.state;
    let items = input.base.iter().chain(input.appended).collect::<Vec<_>>();
    let ranges = compressible_ranges(input.base.len(), &items, input.loop_id)?;
    let valid = ranges.iter().map(|(key, _)| key.clone()).collect();
    let old = state.consolidated(input.loop_id, input.summary, &valid);
    let folded = state.emergency_groups(input.loop_id, &valid);
    let (fixed, history) =
        super::auto_compose(input.system, input.summary, input.base, input.appended)?;
    let messages = fold_history_with_ranges(
        &fixed,
        &history,
        items.len(),
        &ranges,
        &folded,
        old.as_ref(),
    )?;
    let make = |messages| {
        ModelRequest::new(
            messages,
            input.tools.to_vec(),
            input.limits,
            input.reasoning,
        )
        .map_err(super::auto::invalid)
    };
    let before_request = make(messages)?;
    let before_bytes = auto
        .budget
        .estimate_request_bytes(
            &before_request,
            Some(input.loop_id),
            Some(input.request_index),
        )
        .map_err(|_| UtilityError::Serialization)?;
    let before = before_bytes.div_ceil(4) as u64;
    let budget = auto.policy.budget(auto.model.descriptor().context_window);
    if !recovery && input.request_index == 0 && input.base.is_empty() && input.summary.is_none() {
        return Ok(ProjectionResult {
            request: before_request,
            usage: None,
            compacted: false,
        });
    }
    let trigger_before = if input.request_index == 0 && !recovery {
        // The first Runtime boundary contains only new User/Steer items.
        // They stay raw but do not participate in the pre-user threshold.
        let old_len = before_request
            .messages()
            .len()
            .saturating_sub(input.appended.len());
        let first = make(before_request.messages()[..old_len].to_vec())?;
        let fallback = auto
            .budget
            .estimate_request_tokens(&first, None, None)
            .map_err(|_| UtilityError::Serialization)?;
        state.threshold_estimate(
            auto.model.descriptor(),
            &first,
            &input.base.iter().collect::<Vec<_>>(),
            fallback,
            &*auto.budget,
        )
    } else {
        state.threshold_estimate(
            auto.model.descriptor(),
            &before_request,
            &items,
            before,
            &*auto.budget,
        )
    };
    if !recovery && trigger_before < budget.trigger_tokens {
        return Ok(ProjectionResult {
            request: before_request,
            usage: None,
            compacted: false,
        });
    }

    // The newest result must first reach the assistant verbatim. Earlier,
    // already consumed complete exchanges can enter the historical summary.
    let newest = (!recovery && input.request_index > 0)
        .then(|| {
            ranges
                .iter()
                .rev()
                .find(|(key, _)| {
                    key.start >= input.base.len()
                        && matches!(items[key.start], HistoryItem::Assistant(_))
                })
                .map(|(key, _)| key.clone())
        })
        .flatten();
    let mut covered = old
        .as_ref()
        .map(|old| old.covered.clone())
        .unwrap_or_default();
    for (key, _) in &ranges {
        if Some(key) != newest.as_ref() {
            covered.insert(key.clone());
        }
    }
    if covered.is_empty() && input.summary.is_none() && old.is_none() {
        return Ok(ProjectionResult {
            request: before_request,
            usage: None,
            compacted: false,
        });
    }
    if covered.len() > 4096 {
        return Err(UtilityError::TooLarge.into());
    }
    if recovery && input.rejected_body_bytes.is_none_or(|bytes| bytes == 0) {
        return Err(UtilityError::NoProgress.into());
    }
    let original_summary_hash = source_hash(&input.summary)?;
    // This identity excludes generated output/generations, so our own success
    // never manufactures new source for the next check.
    let source = source_hash(&(
        original_summary_hash,
        &covered,
        auto.model.descriptor().model_ref.as_str(),
        auto.model.descriptor().context_window,
        input.system,
        input.tools,
        input.reasoning,
        budget.trigger_tokens,
        Arc::as_ptr(&auto.budget) as *const () as usize,
    ))?;
    if !recovery {
        let threshold = state.threshold.lock().unwrap();
        if threshold
            .attempted
            .is_some_and(|(owner, index)| owner == input.loop_id && input.request_index <= index)
            || threshold.source == Some(source)
        {
            return Ok(ProjectionResult {
                request: before_request,
                usage: None,
                compacted: false,
            });
        }
        drop(threshold);
        let mut floor = Vec::new();
        if !input.system.is_empty() {
            floor.push(
                ModelMessage::system(input.system.as_str().to_owned())
                    .map_err(super::auto::invalid)?,
            );
        }
        for (index, message) in history.iter().enumerate() {
            if !covered
                .iter()
                .any(|key| key.start <= index && index < key.end)
                && !(input.request_index == 0 && index >= input.base.len())
            {
                floor.push(message.clone());
            }
        }
        if !floor.is_empty()
            && auto
                .budget
                .estimate_request_tokens(&make(floor)?, None, None)
                .map_err(|_| UtilityError::Serialization)?
                >= budget.trigger_tokens
        {
            return Ok(ProjectionResult {
                request: before_request,
                usage: None,
                compacted: false,
            });
        }
        let mut threshold = state.threshold.lock().unwrap();
        threshold.attempted = Some((input.loop_id, input.request_index));
        threshold.source = Some(source);
    }
    if input.cancellation.is_cancelled() {
        return Err(UtilityError::Cancelled.into());
    }
    if Instant::now() >= input.deadline {
        return Err(UtilityError::Timeout.into());
    }
    let live_usage = Arc::new(std::sync::Mutex::new(None));
    let mut observation = ObservationGuard::new(
        state,
        &input,
        trigger_before,
        recovery,
        Arc::clone(&live_usage),
    );
    let previous = old.as_ref().map(|old| &old.content).or(input.summary);
    let mut parts = Vec::new();
    for (key, _) in &ranges {
        if !covered.contains(key) || old.as_ref().is_some_and(|old| old.covered.contains(key)) {
            continue;
        }
        if let Some(summary) = folded.get(key) {
            parts.push(SourcePart::Summary(summary));
        } else {
            parts.extend(
                items[key.start..key.end]
                    .iter()
                    .map(|item| SourcePart::Item(item)),
            );
        }
    }
    let source_message =
        utility::projected_source_message(previous, parts, input.cancellation, input.deadline)?;
    let utility_input = CompactionInput {
        model: Arc::clone(&auto.model),
        reasoning: input.reasoning,
        history: Arc::from([]),
        previous_summary: None,
        previous_covered_item_count: 0,
        project_instructions: input.system.clone(),
        tool_schemas: input.tools.to_vec(),
        hard_tokens: budget.hard_tokens,
        safe_before_estimate: false,
        operation_deadline: input.deadline,
        live_usage: Some(live_usage),
    };
    let generated = match utility::generate_projected_summary(
        &utility_input,
        source_message,
        input.cancellation,
    )
    .await
    {
        Ok(generated) => generated,
        Err(error) => {
            observation.finish(error.error.kind(), None, error.utility_usage.clone());
            return Err(ProjectionFailure {
                error: error.error,
                usage: error.utility_usage,
            });
        }
    };
    let validated = (|| {
        let consolidated = ConsolidatedSummary {
            loop_id: input.loop_id,
            original_summary_hash,
            covered,
            content: generated.content,
        };
        let request = make(fold_history_with_ranges(
            &fixed,
            &history,
            items.len(),
            &ranges,
            &folded,
            Some(&consolidated),
        )?)?;
        let after_bytes = auto
            .budget
            .estimate_request_bytes(&request, Some(input.loop_id), Some(input.request_index))
            .map_err(|_| UtilityError::Serialization)?;
        let after = after_bytes.div_ceil(4) as u64;
        if after_bytes >= before_bytes
            || (recovery
                && (after > budget.hard_tokens
                    || input
                        .rejected_body_bytes
                        .is_none_or(|original| after_bytes >= original)))
            || request.messages().len() > auto.max_prompt_messages
            || !super::recovery::validate_clean_tool_exchanges(request.messages())
        {
            return Err(UtilityError::NoProgress);
        }
        if input.cancellation.is_cancelled() {
            return Err(UtilityError::Cancelled);
        }
        if Instant::now() >= input.deadline {
            return Err(UtilityError::Timeout);
        }
        if !state.install_consolidated(consolidated) {
            return Err(UtilityError::TooLarge);
        }
        Ok((request, after))
    })();
    let (request, after) = match validated {
        Ok(value) => value,
        Err(error) => {
            observation.finish(error.kind(), None, generated.utility_usage.clone());
            return Err(ProjectionFailure {
                error,
                usage: generated.utility_usage,
            });
        }
    };
    observation.finish("compacted", Some(after), generated.utility_usage.clone());
    Ok(ProjectionResult {
        request,
        usage: generated.utility_usage,
        compacted: true,
    })
}

/// Runtime may drop preparation on cancel/deadline/panic. No cached current
/// observation can outlive that future, including before terminal events.
struct ObservationGuard<'a> {
    state: &'a CompactionState,
    cancellation: &'a CancellationToken,
    deadline: Instant,
    operation_id: String,
    live_usage: Arc<std::sync::Mutex<Option<CompactionUtilityUsage>>>,
    armed: bool,
}
impl<'a> ObservationGuard<'a> {
    fn new(
        state: &'a CompactionState,
        input: &ProjectionInput<'a>,
        before: u64,
        recovery: bool,
        live_usage: Arc<std::sync::Mutex<Option<CompactionUtilityUsage>>>,
    ) -> Self {
        let operation_id = format!("auto-boundary-{}-{}", input.loop_id, input.request_index);
        if !recovery {
            let budget = input
                .auto
                .policy
                .budget(input.auto.model.descriptor().context_window);
            state.threshold.lock().unwrap().observation.current =
                Some(AutomaticCompactionObservation {
                    operation_id: operation_id.clone(),
                    loop_id: Some(input.loop_id),
                    request_index: Some(input.request_index),
                    before_tokens: Some(before),
                    after_tokens: None,
                    utility_before_tokens: None,
                    utility_after_tokens: None,
                    hard_tokens: budget.hard_tokens,
                    trigger_tokens: budget.trigger_tokens,
                    target_tokens: budget.target_tokens,
                    utility_usage: None,
                    outcome: "summarizing".into(),
                });
        }
        Self {
            state,
            cancellation: input.cancellation,
            deadline: input.deadline,
            operation_id,
            live_usage,
            armed: !recovery,
        }
    }
    fn finish(&mut self, outcome: &str, after: Option<u64>, usage: Option<CompactionUtilityUsage>) {
        if !self.armed {
            return;
        }
        let mut state = self.state.threshold.lock().unwrap();
        if state
            .observation
            .current
            .as_ref()
            .is_some_and(|entry| entry.operation_id == self.operation_id)
        {
            if let Some(mut current) = state.observation.current.take() {
                current.outcome = outcome.to_owned();
                current.after_tokens = after;
                current.utility_usage = usage.or_else(|| self.live_usage.lock().unwrap().clone());
                state.observation.last = Some(current);
            }
        }
        self.armed = false;
    }
}
impl Drop for ObservationGuard<'_> {
    fn drop(&mut self) {
        let outcome = if self.cancellation.is_cancelled() {
            "cancelled"
        } else if Instant::now() >= self.deadline {
            "timeout"
        } else {
            "failed"
        };
        self.finish(outcome, None, None);
    }
}

#[cfg(test)]
#[path = "threshold_tests.rs"]
mod tests;
