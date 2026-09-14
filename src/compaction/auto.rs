use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{
    Model, ModelLimits, ModelMessage, ModelRequest, ModelValueError, ReasoningPreference,
};
use minicore_runtime::tools::ToolSpec;
use minicore_runtime::value::BoundedText;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::utility::{
    self, CompactionInput, SummaryGenerationError, UtilityError, generate_summary,
};
use super::{
    AutomaticCompactionObservation, CompactionPolicy, CompactionState, EphemeralGroupKey,
    InputBudget, summary_data_message,
};

/// Immutable request-time automatic-compaction context. It holds the raw
/// (unwrapped) model used for the no-tools utility, the active policy, and the
/// Session-local summary state. The effective budget is derived per request
/// from the descriptor in `PromptRequest`, so a model hot-swap re-derives it
/// while the durable and ephemeral summaries stay valid.
#[derive(Clone)]
pub(crate) struct AutoContext {
    pub(crate) model: Arc<dyn Model>,
    pub(crate) budget: Arc<dyn crate::models::ProviderBudget>,
    pub(crate) policy: CompactionPolicy,
    pub(crate) max_prompt_messages: usize,
    pub(crate) state: Arc<CompactionState>,
}

/// The `AutoContext` fields that do not point back at `CompactionState`. The
/// Session keeps the full context; the state stores only this binding so
/// `CompactionState -> auto binding -> CompactionState` cannot form a strong
/// reference cycle. The recovery wrapper rebuilds a full `AutoContext` for
/// one reconstruction with the state it already owns.
#[derive(Clone)]
pub(crate) struct AutoContextBinding {
    pub(crate) model: Arc<dyn Model>,
    pub(crate) budget: Arc<dyn crate::models::ProviderBudget>,
    pub(crate) policy: CompactionPolicy,
    pub(crate) max_prompt_messages: usize,
}

impl AutoContext {
    pub(crate) fn binding(&self) -> AutoContextBinding {
        AutoContextBinding {
            model: Arc::clone(&self.model),
            budget: Arc::clone(&self.budget),
            policy: self.policy,
            max_prompt_messages: self.max_prompt_messages,
        }
    }

    pub(crate) fn from_binding(binding: &AutoContextBinding, state: Arc<CompactionState>) -> Self {
        Self {
            model: Arc::clone(&binding.model),
            budget: Arc::clone(&binding.budget),
            policy: binding.policy,
            max_prompt_messages: binding.max_prompt_messages,
            state,
        }
    }
}

/// The result of one request-time compaction decision. Every failure is
/// explicit; a truncated or fabricated summary is never returned as success.
#[derive(Debug)]
pub(crate) enum PlanError {
    /// The irreducible request alone exceeds the budget, so no summary can
    /// make it fit without dropping user constraints.
    Uncompressible,
    Cancelled,
    Utility(UtilityError),
}

/// One complete tool exchange group: an Assistant message carrying ToolCalls
/// followed by exactly the ToolResults for those calls. A group with any
/// pending call is never reported.
fn complete_group_ranges(items: &[&HistoryItem]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut index = 0usize;
    while index < items.len() {
        let Some(HistoryItem::Assistant(assistant)) = items.get(index) else {
            index += 1;
            continue;
        };
        let call_ids: Vec<_> = assistant
            .content
            .iter()
            .filter_map(|part| part.as_tool_call().map(|call| call.tool_call_id().clone()))
            .collect();
        if call_ids.is_empty()
            || call_ids
                .iter()
                .enumerate()
                .any(|(position, call_id)| call_ids[..position].contains(call_id))
        {
            index += 1;
            continue;
        }
        let mut end = index + 1;
        let mut matched = BTreeSet::new();
        while matched.len() < call_ids.len() {
            match items.get(end) {
                Some(HistoryItem::ToolResult(result))
                    if result.loop_id == assistant.loop_id
                        && result.request_index == assistant.request_index
                        && call_ids.contains(&result.call_id)
                        && matched.insert(result.call_id.clone()) =>
                {
                    end += 1;
                }
                _ => break,
            }
        }
        // Do not treat an exchange as complete when another ToolResult is
        // still adjacent: that would leave an orphan result after replacing
        // the group and would make the resulting ModelRequest invalid.
        if matched.len() == call_ids.len()
            && !matches!(items.get(end), Some(HistoryItem::ToolResult(_)))
        {
            ranges.push((index, end));
            index = end;
        } else {
            index += 1;
        }
    }
    ranges
}

pub(crate) fn startup_history_is_request_safe(items: &[HistoryItem]) -> bool {
    let items: Vec<&HistoryItem> = items.iter().collect();
    let ranges = complete_group_ranges(&items);
    let mut index = 0usize;
    while index < items.len() {
        if matches!(items[index], HistoryItem::ToolResult(_)) {
            return false;
        }
        if let HistoryItem::Assistant(assistant) = items[index] {
            let has_tool_calls = assistant
                .content
                .iter()
                .any(|part| part.as_tool_call().is_some());
            if has_tool_calls {
                let Some((start, end)) = ranges.iter().find(|(start, _)| *start == index) else {
                    return false;
                };
                debug_assert_eq!(*start, index);
                index = *end;
                continue;
            }
        }
        index += 1;
    }
    true
}

fn group_key(
    items: &[&HistoryItem],
    start: usize,
    end: usize,
) -> Result<EphemeralGroupKey, UtilityError> {
    let source = serde_json::to_vec(&items[start..end]).map_err(|_| UtilityError::Serialization)?;
    Ok(EphemeralGroupKey {
        start,
        end,
        source_hash: Sha256::digest(source).into(),
    })
}

/// Returns complete tool exchanges and ordinary settled base items that may
/// be summarized. Current-loop User/Steer messages, ToolResults without a
/// complete exchange, and incomplete tool calls are deliberately excluded.
pub(crate) fn compressible_ranges(
    base_len: usize,
    items: &[&HistoryItem],
    loop_id: LoopId,
) -> Result<Vec<(EphemeralGroupKey, usize)>, UtilityError> {
    let tool_ranges = complete_group_ranges(items);
    let mut result = Vec::new();
    let mut index = 0usize;
    while index < items.len() {
        if let Some((_, end)) = tool_ranges.iter().find(|(start, _)| *start == index) {
            result.push((
                group_key(items, index, *end)?,
                group_bytes(items, (index, *end)),
            ));
            index = *end;
            continue;
        }
        let settled_base = index < base_len;
        let eligible = settled_base
            && !matches!(items[index], HistoryItem::ToolResult(_))
            && !matches!(
                items[index],
                HistoryItem::Assistant(assistant)
                    if assistant
                        .content
                        .iter()
                        .any(|part| part.as_tool_call().is_some())
            )
            && !matches!(
                items[index],
                HistoryItem::User(user) if user.loop_id == loop_id
            );
        if eligible {
            result.push((
                group_key(items, index, index + 1)?,
                group_bytes(items, (index, index + 1)),
            ));
        }
        index += 1;
    }
    Ok(result)
}

/// A cheap byte estimate used only to choose the largest groups first. It is
/// not a token count and never authorizes a claim about provider usage.
fn group_bytes(items: &[&HistoryItem], range: (usize, usize)) -> usize {
    items[range.0..range.1]
        .iter()
        .map(|item| match item {
            HistoryItem::User(user) => user.input.as_text().len(),
            HistoryItem::Assistant(assistant) => assistant
                .content
                .iter()
                .map(|part| match part {
                    minicore_runtime::model::AssistantPart::Text(text) => text.len(),
                    minicore_runtime::model::AssistantPart::Reasoning(reasoning) => reasoning
                        .text()
                        .map_or(0, str::len)
                        .saturating_add(reasoning.summary().map_or(0, str::len)),
                    minicore_runtime::model::AssistantPart::ToolCall(call) => {
                        call.name().as_str().len()
                            + serde_json::to_vec(call.arguments()).map_or(0, |value| value.len())
                    }
                })
                .sum(),
            HistoryItem::ToolResult(result) => result.output.content().byte_len(),
            HistoryItem::Summary(summary) => summary.content.byte_len(),
        })
        .sum()
}

/// The current loop's initial Prompt and every applied Steer, in history
/// order. These are preserved verbatim and never folded.
pub(crate) fn current_user_texts<'a>(
    base: &'a [HistoryItem],
    appended: &'a [HistoryItem],
    loop_id: LoopId,
) -> Vec<&'a str> {
    base.iter()
        .chain(appended.iter())
        .filter_map(|item| match item {
            HistoryItem::User(user) if user.loop_id == loop_id => Some(user.input.as_text()),
            _ => None,
        })
        .collect()
}

/// Replaces each folded complete group in the projected history messages with
/// one labeled historical summary data message. `fixed` (system and durable
/// summary) is preserved exactly; `history` has exactly one entry per
/// base-then-appended item.
pub(crate) fn fold_history(
    fixed: &[ModelMessage],
    history: &[ModelMessage],
    base: &[HistoryItem],
    appended: &[HistoryItem],
    loop_id: LoopId,
    folded: &BTreeMap<EphemeralGroupKey, BoundedText>,
) -> Result<Vec<ModelMessage>, UtilityError> {
    let items: Vec<&HistoryItem> = base.iter().chain(appended.iter()).collect();
    if history.len() != items.len() {
        return Err(UtilityError::InvalidResponse);
    }
    let ranges = compressible_ranges(base.len(), &items, loop_id)?;
    let mut result = Vec::with_capacity(fixed.len() + history.len());
    result.extend_from_slice(fixed);
    let mut index = 0usize;
    while index < items.len() {
        let Some((key, end)) = ranges
            .iter()
            .find(|(key, _)| key.start == index)
            .map(|(key, _)| (key, key.end))
        else {
            result.push(history[index].clone());
            index += 1;
            continue;
        };
        if let Some(summary) = folded.get(key) {
            result.push(summary_data_message(summary).map_err(invalid)?);
            index = end;
        } else {
            result.push(history[index].clone());
            index += 1;
        }
    }
    Ok(result)
}

/// The irreducible request: only the merged system text, the current loop's
/// Prompt/Steer messages, and the active tool schemas. History, summaries, and
/// tool exchanges are excluded, so exceeding this proves compaction cannot
/// help without dropping user constraints.
pub(crate) fn minimal_messages(
    system: &BoundedText,
    current_users: &[&str],
) -> Result<Vec<ModelMessage>, UtilityError> {
    let mut messages = Vec::with_capacity(current_users.len() + 1);
    if !system.is_empty() {
        messages.push(ModelMessage::system(system.as_str().to_owned()).map_err(invalid)?);
    }
    for text in current_users {
        messages.push(ModelMessage::user((*text).to_owned()).map_err(invalid)?);
    }
    Ok(messages)
}

/// Splits a projected request into its fixed prefix (system and durable
/// summary) and its per-item history messages. The history vector has exactly
/// one entry per base-then-appended item, so the same index space is used when
/// folding groups.
pub(crate) fn compose(
    system: &BoundedText,
    summary: Option<&BoundedText>,
    base: &[HistoryItem],
    appended: &[HistoryItem],
) -> Result<(Vec<ModelMessage>, Vec<ModelMessage>), UtilityError> {
    let mut fixed = Vec::with_capacity(2);
    if !system.is_empty() {
        fixed.push(ModelMessage::system(system.as_str().to_owned()).map_err(invalid)?);
    }
    if let Some(summary) = summary {
        fixed.push(summary_data_message(summary).map_err(invalid)?);
    }
    let mut history = Vec::with_capacity(base.len() + appended.len());
    for item in base.iter().chain(appended.iter()) {
        history.push(utility::history_message(item)?);
    }
    Ok((fixed, history))
}

/// Estimates a prospective startup request: the same projection plus the
/// pending current User input, exactly as the loop's first prepare will see it.
pub(crate) fn estimate_startup(
    system: &BoundedText,
    summary: Option<&BoundedText>,
    base: &[HistoryItem],
    current_input: &str,
    tools: &[ToolSpec],
    reasoning: ReasoningPreference,
) -> Result<u64, UtilityError> {
    // This is deliberately a structural-limit-safe estimate. Startup may be
    // invoked specifically because the raw history is too large or contains
    // an exchange that cannot be represented as one ModelRequest. Serializing
    // the bounded source items here lets the utility replace that history
    // before any full ModelRequest is constructed.
    let mut bytes = 128usize
        .saturating_add(system.byte_len())
        .saturating_add(summary.map_or(0, BoundedText::byte_len))
        .saturating_add(current_input.len())
        .saturating_add(
            serde_json::to_vec(tools)
                .map_err(|_| UtilityError::Serialization)?
                .len(),
        )
        .saturating_add(
            serde_json::to_vec(&reasoning)
                .map_err(|_| UtilityError::Serialization)?
                .len(),
        );
    for item in base {
        bytes = bytes.saturating_add(
            serde_json::to_vec(item)
                .map_err(|_| UtilityError::Serialization)?
                .len(),
        );
    }
    Ok(u64::try_from(bytes).unwrap_or(u64::MAX).div_ceil(4))
}

pub(crate) fn estimate_startup_exact(
    system: &BoundedText,
    summary: Option<&BoundedText>,
    base: &[HistoryItem],
    current_input: &str,
    tools: &[ToolSpec],
    reasoning: ReasoningPreference,
    budget: &dyn crate::models::ProviderBudget,
) -> Result<u64, UtilityError> {
    let (mut fixed, history) = compose(system, summary, base, &[])?;
    fixed.extend(history);
    if !current_input.is_empty() {
        fixed.push(ModelMessage::user(current_input.to_owned()).map_err(invalid)?);
    }
    estimate_tokens_with_budget(budget, fixed, tools, reasoning, None, None)
}

/// Estimates the irreducible startup minimum: system text, the pending User
/// input, and tool schemas only.
pub(crate) fn estimate_minimal(
    system: &BoundedText,
    current_inputs: &[&str],
    tools: &[ToolSpec],
    reasoning: ReasoningPreference,
    budget: &dyn crate::models::ProviderBudget,
) -> Result<u64, UtilityError> {
    let messages = minimal_messages(system, current_inputs)?;
    estimate_tokens_with_budget(budget, messages, tools, reasoning, None, None)
}

pub(crate) fn estimate_tokens_with_budget(
    budget: &dyn crate::models::ProviderBudget,
    messages: Vec<ModelMessage>,
    tools: &[ToolSpec],
    reasoning: ReasoningPreference,
    loop_id: Option<LoopId>,
    request_index: Option<u32>,
) -> Result<u64, UtilityError> {
    let request = ModelRequest::new(messages, tools.to_vec(), ModelLimits::default(), reasoning)
        .map_err(invalid)?;
    budget
        .estimate_request_tokens(&request, loop_id, request_index)
        .map_err(|_| UtilityError::Serialization)
}

/// Applies the automatic policy to one prepared request. It returns the exact
/// provider messages, folding complete tool exchanges or settled base items
/// into reusable ephemeral summaries. The trigger starts an attempt; only the
/// hard ceiling determines whether the result is acceptable.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn plan(
    fixed: &[ModelMessage],
    history: Vec<ModelMessage>,
    system: &BoundedText,
    base: &[HistoryItem],
    appended: &[HistoryItem],
    tools: &[ToolSpec],
    reasoning: ReasoningPreference,
    budget: InputBudget,
    auto: &AutoContext,
    deadline: Instant,
    cancellation: &CancellationToken,
    loop_id: LoopId,
    request_index: u32,
) -> Result<Vec<ModelMessage>, PlanError> {
    let operation_id = format!("auto-{loop_id}-{request_index}");
    let mut observation = PlanObservation::new(
        Arc::clone(&auto.state),
        operation_id,
        loop_id,
        request_index,
        budget,
    );
    let mut folded: BTreeMap<EphemeralGroupKey, BoundedText> = auto
        .state
        .ephemeral(loop_id)
        .map(|summary| summary.groups)
        .unwrap_or_default();
    let mut current =
        fold_history(fixed, &history, base, appended, loop_id, &folded).map_err(|error| {
            observation.finish("invalid_history", None);
            PlanError::Utility(error)
        })?;
    let mut tokens = match estimate_tokens_with_budget(
        &*auto.budget,
        current.clone(),
        tools,
        reasoning,
        Some(loop_id),
        Some(request_index),
    ) {
        Ok(tokens) => tokens,
        Err(_) => {
            observation.finish("context_uncompressible", None);
            return Err(PlanError::Uncompressible);
        }
    };
    observation.set_estimate(tokens);
    if cancellation.is_cancelled() {
        observation.finish("cancelled", Some(tokens));
        return Err(PlanError::Cancelled);
    }
    if Instant::now() >= deadline {
        observation.finish("timeout", Some(tokens));
        return Err(PlanError::Utility(UtilityError::Timeout));
    }
    if tokens <= budget.trigger_tokens && current.len() <= auto.max_prompt_messages {
        observation.finish("fit", Some(tokens));
        return Ok(current);
    }
    let users = current_user_texts(base, appended, loop_id);
    let minimal = minimal_messages(system, &users).map_err(PlanError::Utility)?;
    let minimal_message_count = minimal.len();
    let minimal_tokens = estimate_tokens_with_budget(
        &*auto.budget,
        minimal,
        tools,
        reasoning,
        Some(loop_id),
        Some(request_index),
    )
    .map_err(|error| {
        observation.finish("invalid_minimum", Some(tokens));
        PlanError::Utility(error)
    })?;
    if minimal_tokens > budget.hard_tokens || minimal_message_count > auto.max_prompt_messages {
        observation.finish("context_uncompressible", Some(tokens));
        return Err(PlanError::Uncompressible);
    }
    let candidates = {
        let items: Vec<&HistoryItem> = base.iter().chain(appended.iter()).collect();
        let mut candidates: Vec<(EphemeralGroupKey, usize)> =
            compressible_ranges(base.len(), &items, loop_id)
                .map_err(|error| {
                    observation.finish("invalid_history", Some(tokens));
                    PlanError::Utility(error)
                })?
                .into_iter()
                .collect();
        candidates.sort_by(|left, right| {
            folded
                .contains_key(&left.0)
                .cmp(&folded.contains_key(&right.0))
                .then(right.1.cmp(&left.1))
                .then(left.0.start.cmp(&right.0.start))
        });
        candidates
    };
    if candidates.is_empty() {
        if tokens <= budget.hard_tokens {
            observation.finish("fit_over_trigger", Some(tokens));
            return Ok(current);
        }
        observation.finish("context_uncompressible", Some(tokens));
        return Err(PlanError::Uncompressible);
    }
    let mut compacted = false;
    for (key, _) in candidates {
        if folded.contains_key(&key) && tokens <= budget.hard_tokens {
            // A source-identical cached summary is already valid for this
            // model. Do not spend utility calls merely because the soft
            // target is lower; rederive it only when a model update makes the
            // cached projection cross the hard boundary.
            continue;
        }
        if tokens <= budget.target_tokens && current.len() <= auto.max_prompt_messages {
            break;
        }
        if cancellation.is_cancelled() {
            observation.finish("cancelled", Some(tokens));
            return Err(PlanError::Cancelled);
        }
        if Instant::now() >= deadline {
            observation.finish("timeout", Some(tokens));
            return Err(PlanError::Utility(UtilityError::Timeout));
        }
        let start = key.start;
        let end = key.end;
        let group: Vec<HistoryItem> = base
            .iter()
            .chain(appended.iter())
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
                reasoning,
                system,
                tools: tools.to_vec(),
                group,
                target_tokens: budget.target_tokens,
                hard_tokens: budget.hard_tokens,
                deadline,
            },
            cancellation,
        )
        .await
        {
            Ok(generation) => generation,
            Err(error) => {
                observation.add_usage(error.utility_usage);
                let failure = error.error;
                match failure {
                    UtilityError::Cancelled => {
                        observation.finish("cancelled", Some(tokens));
                        return Err(PlanError::Cancelled);
                    }
                    UtilityError::Timeout => {
                        observation.finish("timeout", Some(tokens));
                        return Err(PlanError::Utility(UtilityError::Timeout));
                    }
                    failure
                        if raw_fit_fallback_allowed(failure)
                            && tokens <= budget.hard_tokens
                            && current.len() <= auto.max_prompt_messages =>
                    {
                        // A valid raw request is a conservative fallback only
                        // for compression inability, never for cancellation,
                        // timeout, or a model/protocol failure.
                        observation.finish("fit_over_trigger", Some(tokens));
                        return Ok(current);
                    }
                    failure => {
                        observation.finish(failure.kind(), Some(tokens));
                        return Err(PlanError::Utility(failure));
                    }
                }
            }
        };
        observation.set_utility_estimate(generation.before_tokens, generation.after_tokens);
        observation.add_usage(generation.utility_usage.clone());
        auto.state
            .cache_ephemeral(loop_id, key.clone(), generation.content.clone());
        folded.insert(key, generation.content);
        compacted = true;
        current =
            fold_history(fixed, &history, base, appended, loop_id, &folded).map_err(|error| {
                observation.finish("invalid_history", Some(tokens));
                PlanError::Utility(error)
            })?;
        tokens = match estimate_tokens_with_budget(
            &*auto.budget,
            current.clone(),
            tools,
            reasoning,
            Some(loop_id),
            Some(request_index),
        ) {
            Ok(tokens) => tokens,
            Err(_) => {
                observation.finish("context_uncompressible", Some(tokens));
                return Err(PlanError::Uncompressible);
            }
        };
        observation.set_estimate(tokens);
    }
    if tokens > budget.hard_tokens || current.len() > auto.max_prompt_messages {
        observation.finish("context_uncompressible", Some(tokens));
        return Err(PlanError::Uncompressible);
    }
    if cancellation.is_cancelled() {
        observation.finish("cancelled", Some(tokens));
        return Err(PlanError::Cancelled);
    }
    if Instant::now() >= deadline {
        observation.finish("timeout", Some(tokens));
        return Err(PlanError::Utility(UtilityError::Timeout));
    }
    observation.finish(
        if compacted {
            "compacted"
        } else {
            "fit_over_trigger"
        },
        Some(tokens),
    );
    Ok(current)
}

struct PlanObservation {
    state: Arc<CompactionState>,
    operation_id: String,
    observation: AutomaticCompactionObservation,
    finished: bool,
}

impl PlanObservation {
    fn new(
        state: Arc<CompactionState>,
        operation_id: String,
        loop_id: LoopId,
        request_index: u32,
        budget: InputBudget,
    ) -> Self {
        let observation = AutomaticCompactionObservation {
            operation_id: operation_id.clone(),
            loop_id: Some(loop_id),
            request_index: Some(request_index),
            before_tokens: None,
            after_tokens: None,
            utility_before_tokens: None,
            utility_after_tokens: None,
            hard_tokens: budget.hard_tokens,
            trigger_tokens: budget.trigger_tokens,
            target_tokens: budget.target_tokens,
            utility_usage: None,
            outcome: "preparing".to_owned(),
        };
        state.begin_automatic(observation.clone());
        Self {
            state,
            operation_id,
            observation,
            finished: false,
        }
    }

    fn set_estimate(&mut self, tokens: u64) {
        self.state.note_request_estimate(tokens);
        self.observation.before_tokens.get_or_insert(tokens);
        self.observation.after_tokens = Some(tokens);
        self.state.update_automatic(&self.operation_id, |current| {
            current.before_tokens.get_or_insert(tokens);
            current.after_tokens = Some(tokens);
        });
    }

    fn add_usage(&mut self, usage: Option<super::CompactionUtilityUsage>) {
        merge_utility_usage(&mut self.observation.utility_usage, usage);
        let utility_usage = self.observation.utility_usage.clone();
        self.state.update_automatic(&self.operation_id, |current| {
            current.utility_usage = utility_usage;
        });
    }

    fn set_utility_estimate(&mut self, before_tokens: u64, after_tokens: u64) {
        self.observation
            .utility_before_tokens
            .get_or_insert(before_tokens);
        self.observation.utility_after_tokens = Some(after_tokens);
        self.state.update_automatic(&self.operation_id, |current| {
            current.utility_before_tokens.get_or_insert(before_tokens);
            current.utility_after_tokens = Some(after_tokens);
        });
    }

    fn finish(&mut self, outcome: &str, after_tokens: Option<u64>) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.state.finish_automatic(
            &self.operation_id,
            outcome,
            after_tokens,
            self.observation.utility_usage.clone(),
        );
    }
}

impl Drop for PlanObservation {
    fn drop(&mut self) {
        if !self.finished {
            self.finish("aborted", self.observation.after_tokens);
        }
    }
}

pub(crate) fn merge_utility_usage(
    total: &mut Option<super::CompactionUtilityUsage>,
    next: Option<super::CompactionUtilityUsage>,
) {
    let Some(next) = next else {
        return;
    };
    let Some(current) = total.as_mut() else {
        *total = Some(next);
        return;
    };
    current.call_count = current.call_count.saturating_add(next.call_count);
    let left = current.usage.take();
    let next_complete = next.complete;
    let right = next.usage;
    let overflow = matches!((&left, &right), (Some(left), Some(right)) if utility::usage_sum_overflow(left, right));
    current.complete &= next_complete && !overflow;
    current.usage = match (left, right) {
        (Some(left), Some(right)) => Some(utility::sum_usage(left, right)),
        _ => None,
    };
}

pub(crate) struct GroupSummaryRequest<'a> {
    pub(crate) model: Arc<dyn Model>,
    pub(crate) reasoning: ReasoningPreference,
    pub(crate) system: &'a BoundedText,
    pub(crate) tools: Vec<ToolSpec>,
    pub(crate) group: Vec<HistoryItem>,
    pub(crate) hard_tokens: u64,
    pub(crate) target_tokens: u64,
    pub(crate) deadline: Instant,
}

/// Generates one ephemeral semantic summary for one safe settled/tool group
/// with the raw no-tools utility identity. Empty, oversized,
/// no-progress, or timeout outcomes propagate as clear errors rather than a
/// truncated success.
pub(crate) async fn summarize_group(
    request: GroupSummaryRequest<'_>,
    cancellation: &CancellationToken,
) -> Result<utility::SummaryGeneration, SummaryGenerationError> {
    if cancellation.is_cancelled() {
        return Err(SummaryGenerationError {
            error: UtilityError::Cancelled,
            utility_usage: None,
        });
    }
    let input = CompactionInput {
        model: Arc::clone(&request.model),
        reasoning: request.reasoning,
        history: request.group.into(),
        previous_summary: None,
        previous_covered_item_count: 0,
        project_instructions: request.system.clone(),
        tool_schemas: request.tools,
        hard_tokens: request.hard_tokens,
        target_tokens: request.target_tokens,
        safe_before_estimate: false,
        operation_deadline: request.deadline,
    };
    let mut on_merge = || {};
    generate_summary(&input, cancellation, &mut on_merge).await
}

fn raw_fit_fallback_allowed(error: UtilityError) -> bool {
    matches!(
        error,
        UtilityError::NoProgress | UtilityError::TooLarge | UtilityError::Budget
    )
}

pub(crate) fn invalid(_: ModelValueError) -> UtilityError {
    UtilityError::InvalidResponse
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use futures_util::stream;
    use minicore_runtime::ToolCallId;
    use minicore_runtime::execution::UserInput;
    use minicore_runtime::history::{
        AssistantHistory, SummaryHistory, ToolResultHistory, UserHistory, UserMessageKind,
    };
    use minicore_runtime::model::{
        AssistantPart, ModelCallContext, ModelDescriptor, ModelEvent, ModelFinishReason,
        ModelRequest, ModelStartFuture, ModelStream, ToolCall, Usage,
    };
    use minicore_runtime::tools::{ToolOutput, ToolResultOutcome, ToolSpec};

    use super::*;

    const UTILITY_TEXT: &str = "compact summary body";

    struct RecordingModel {
        descriptor: ModelDescriptor,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl RecordingModel {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                descriptor: ModelDescriptor::new(
                    "main".parse().unwrap(),
                    20_000,
                    [ReasoningPreference::Auto].into_iter().collect(),
                    true,
                )
                .unwrap(),
                requests: Mutex::new(Vec::new()),
            })
        }

        fn utility_calls(&self) -> usize {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.tools().is_empty())
                .count()
        }
    }

    impl Model for RecordingModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn start(&self, request: ModelRequest, _context: ModelCallContext) -> ModelStartFuture<'_> {
            self.requests.lock().unwrap().push(request);
            Box::pin(async move {
                let events: Vec<Result<ModelEvent, minicore_runtime::model::ModelError>> = vec![
                    Ok(ModelEvent::text_delta(UTILITY_TEXT).unwrap()),
                    Ok(ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    }),
                    Ok(ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    }),
                ];
                Ok(Box::pin(stream::iter(events)) as ModelStream)
            })
        }
    }

    fn assistant_text(loop_id: LoopId, index: u32, text: String) -> HistoryItem {
        HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: index,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::Text(text)],
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::default(),
        })
    }

    fn tool_group(loop_id: LoopId, index: u32, text: String) -> Vec<HistoryItem> {
        let call_id = ToolCallId::new(format!("call-{index}")).unwrap();
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
                        serde_json::json!({"path": "x"}),
                        0,
                    )
                    .unwrap(),
                )],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::default(),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index: index,
                call_id,
                tool_name: "read".parse().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new(text).unwrap(),
            }),
        ]
    }

    fn user(loop_id: LoopId, kind: UserMessageKind, text: &str) -> HistoryItem {
        HistoryItem::User(UserHistory {
            loop_id,
            kind,
            input: UserInput::text(text).unwrap(),
        })
    }

    fn context() -> (AutoContext, Arc<RecordingModel>, BoundedText) {
        let model = RecordingModel::new();
        let state = CompactionState::new();
        let context = AutoContext {
            model: Arc::clone(&model) as Arc<dyn Model>,
            budget: Arc::new(crate::models::DefaultProviderBudget),
            policy: CompactionPolicy {
                enabled: true,
                trigger_percent: 80,
                target_percent: 50,
            },
            max_prompt_messages: usize::MAX,
            state,
        };
        (context, model, BoundedText::new("system").unwrap())
    }

    async fn run_plan(
        context: &AutoContext,
        system: &BoundedText,
        base: &[HistoryItem],
        appended: &[HistoryItem],
        loop_id: LoopId,
        budget: InputBudget,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ModelMessage>, PlanError> {
        let (fixed, history) = compose(system, None, base, appended).unwrap();
        plan(
            &fixed,
            history,
            system,
            base,
            appended,
            &[],
            ReasoningPreference::Auto,
            budget,
            context,
            Instant::now() + Duration::from_secs(30),
            cancellation,
            loop_id,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn below_trigger_keeps_history_verbatim() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        let base = vec![assistant_text(loop_id, 0, "small".to_owned())];
        let messages = run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: u64::MAX,
                trigger_tokens: u64::MAX,
                target_tokens: 0,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(model.utility_calls(), 0);
        assert!(messages.iter().any(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text == "small")))
        }));
    }

    #[tokio::test]
    async fn over_trigger_folds_a_complete_tool_group_and_reuses_it() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        let mut base = Vec::new();
        for index in 0..3 {
            base.extend(tool_group(loop_id, index, "x".repeat(8_000)));
        }
        let budget = InputBudget {
            hard_tokens: 20_000,
            trigger_tokens: 2_000,
            target_tokens: 1_000,
        };
        let messages = run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            budget,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        // Each folded group is one labeled historical data message.
        let summaries = messages
            .iter()
            .filter(|message| {
                matches!(message, ModelMessage::User(text) if text.contains(UTILITY_TEXT))
            })
            .count();
        assert_eq!(summaries, 3);
        assert_eq!(model.utility_calls(), 3);
        // A second plan for the same loop reuses the cached group summaries.
        run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            budget,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let cached = context.state.ephemeral(loop_id).unwrap();
        assert_eq!(cached.groups.len(), 3);
        assert_eq!(model.utility_calls(), 3);
    }

    #[tokio::test]
    async fn settled_base_items_can_fold_without_folding_current_users() {
        let old_loop = LoopId::new().unwrap();
        let current_loop = LoopId::new().unwrap();
        let (context, model, system) = context();
        let base = vec![
            user(old_loop, UserMessageKind::Prompt, &"old user ".repeat(800)),
            assistant_text(old_loop, 0, "old answer ".repeat(800)),
        ];
        let appended = vec![
            user(current_loop, UserMessageKind::Prompt, "current prompt"),
            user(current_loop, UserMessageKind::Steering, "current steer"),
        ];
        let messages = run_plan(
            &context,
            &system,
            &base,
            &appended,
            current_loop,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 2_000,
                target_tokens: 1_000,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(model.utility_calls() > 0);
        assert!(messages.iter().any(
            |message| matches!(message, ModelMessage::User(text) if text == "current prompt")
        ));
        assert!(
            messages.iter().any(
                |message| matches!(message, ModelMessage::User(text) if text == "current steer")
            )
        );
    }

    #[tokio::test]
    async fn changing_a_cached_source_range_forces_a_new_summary() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        let first = vec![assistant_text(loop_id, 0, "first ".repeat(1_500))];
        run_plan(
            &context,
            &system,
            &first,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 2_000,
                target_tokens: 1_000,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(model.utility_calls(), 1);

        let changed = vec![assistant_text(loop_id, 0, "changed ".repeat(1_500))];
        run_plan(
            &context,
            &system,
            &changed,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 2_000,
                target_tokens: 1_000,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(model.utility_calls(), 2);
    }

    #[tokio::test]
    async fn incomplete_tool_group_is_never_folded() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        // A tool call whose result is still pending, plus a large assistant
        // item so the request exceeds the trigger.
        let mut base = tool_group(loop_id, 0, "x".repeat(8_000));
        base.pop();
        base.push(assistant_text(loop_id, 1, "y".repeat(8_000)));
        let result = run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: 2_000,
                trigger_tokens: 2_000,
                target_tokens: 1_000,
            },
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(result, Err(PlanError::Uncompressible)));
        assert_eq!(model.utility_calls(), 0);
    }

    #[tokio::test]
    async fn current_user_and_steer_are_preserved() {
        let loop_id = LoopId::new().unwrap();
        let (context, _model, system) = context();
        let base = vec![
            user(loop_id, UserMessageKind::Prompt, "current prompt"),
            user(loop_id, UserMessageKind::Steering, "steer one"),
            assistant_text(loop_id, 0, "x".repeat(8_000)),
        ];
        let messages = run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: u64::MAX,
                trigger_tokens: u64::MAX,
                target_tokens: 0,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(messages.iter().any(
            |message| matches!(message, ModelMessage::User(text) if text == "current prompt")
        ));
        assert!(
            messages
                .iter()
                .any(|message| matches!(message, ModelMessage::User(text) if text == "steer one"))
        );
    }

    #[tokio::test]
    async fn trigger_is_not_a_failure_when_empty_history_still_fits_hard_limit() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        let messages = run_plan(
            &context,
            &system,
            &[],
            &[user(loop_id, UserMessageKind::Prompt, "current")],
            loop_id,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 0,
                target_tokens: 0,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(model.utility_calls(), 0);
        assert!(
            messages
                .iter()
                .any(|message| matches!(message, ModelMessage::User(text) if text == "current"))
        );
    }

    #[tokio::test]
    async fn an_unreachable_target_is_accepted_after_the_request_fits_hard_limit() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        let messages = run_plan(
            &context,
            &system,
            &[assistant_text(loop_id, 0, "history ".repeat(1_500))],
            &[],
            loop_id,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 2_000,
                target_tokens: 0,
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!messages.is_empty());
        assert!(model.utility_calls() >= 1);
        let last = context.state.automatic_view().last.unwrap();
        assert_eq!(last.target_tokens, 0);
        assert!(last.after_tokens.unwrap() <= last.hard_tokens);
    }

    #[tokio::test]
    async fn uncompressible_minimum_fails_without_calling_the_model() {
        let loop_id = LoopId::new().unwrap();
        let (context, model, system) = context();
        let base = vec![user(loop_id, UserMessageKind::Prompt, "current prompt")];
        let result = run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: 0,
                trigger_tokens: 0,
                target_tokens: 0,
            },
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(result, Err(PlanError::Uncompressible)));
        assert_eq!(model.utility_calls(), 0);
    }

    #[tokio::test]
    async fn cancelled_plan_reports_cancelled() {
        let loop_id = LoopId::new().unwrap();
        let (context, _model, system) = context();
        let mut base = Vec::new();
        for index in 0..2 {
            base.extend(tool_group(loop_id, index, "x".repeat(8_000)));
        }
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = run_plan(
            &context,
            &system,
            &base,
            &[],
            loop_id,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 2_000,
                target_tokens: 1_000,
            },
            &cancellation,
        )
        .await;
        assert!(matches!(result, Err(PlanError::Cancelled)));
    }

    #[tokio::test]
    async fn expired_deadline_fails_with_timeout() {
        let loop_id = LoopId::new().unwrap();
        let (context, _model, system) = context();
        let mut base = Vec::new();
        for index in 0..2 {
            base.extend(tool_group(loop_id, index, "x".repeat(8_000)));
        }
        let (fixed, history) = compose(&system, None, &base, &[]).unwrap();
        let result = plan(
            &fixed,
            history,
            &system,
            &base,
            &[],
            &[],
            ReasoningPreference::Auto,
            InputBudget {
                hard_tokens: 20_000,
                trigger_tokens: 2_000,
                target_tokens: 1_000,
            },
            &context,
            Instant::now() - Duration::from_secs(1),
            &CancellationToken::new(),
            loop_id,
            0,
        )
        .await;
        assert!(matches!(
            result,
            Err(PlanError::Utility(UtilityError::Timeout))
        ));
    }

    #[test]
    fn minimal_estimate_excludes_history_and_summary() {
        let system = BoundedText::new("system").unwrap();
        let tokens = estimate_minimal(
            &system,
            &["current"],
            &[] as &[ToolSpec],
            ReasoningPreference::Auto,
            &crate::models::DefaultProviderBudget,
        )
        .unwrap();
        assert!(tokens > 0);
    }

    #[test]
    fn durable_summary_stays_a_data_message() {
        let system = BoundedText::new("system").unwrap();
        let summary = BoundedText::new("durable summary").unwrap();
        let (fixed, _history) = compose(
            &system,
            Some(&summary),
            &[HistoryItem::Summary(SummaryHistory {
                content: BoundedText::new("runtime summary").unwrap(),
            })],
            &[],
        )
        .unwrap();
        assert!(matches!(fixed[0], ModelMessage::System(_)));
        assert!(matches!(fixed[1], ModelMessage::User(_)));
    }
}
