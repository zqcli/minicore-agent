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
use super::{CompactionPolicy, CompactionState, EphemeralGroupKey, summary_data_message};

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
        safe_before_estimate: false,
        operation_deadline: request.deadline,
    };
    generate_summary(&input, cancellation).await
}

pub(crate) fn invalid(_: ModelValueError) -> UtilityError {
    UtilityError::InvalidResponse
}
