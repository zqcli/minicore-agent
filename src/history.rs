use std::sync::Arc;

use serde::Serialize;

use minicore_runtime::history::{
    AssistantHistory, HistoryItem, SummaryHistory, ToolResultHistory, UserHistory, UserMessageKind,
};
use minicore_runtime::model::{
    AssistantPart, ModelFinishReason, ReasoningContent, ReasoningPreference, Usage,
};
use minicore_runtime::tools::ToolResultOutcome;
use minicore_runtime::{LoopId, ToolCallId};

use crate::error::AgentError;
use crate::ids::SessionId;

const MAX_HISTORY_LIMIT: usize = 100;

/// Cleans a loop's persisted history so provider-opaque continuation data
/// never reaches the Agent JSONL, the in-memory session history, RPC views,
/// logs, or the next user turn.
///
/// - User, ToolResult, and Summary items pass through unchanged.
/// - Assistant reasoning parts keep only `text`/`summary`; `encrypted` and
///   `signature` are dropped. A reasoning part that carried only opaque
///   fields is removed entirely.
/// - An Assistant item whose content becomes empty is removed.
///
/// ToolCall arguments are preserved: the runtime `DefaultPromptProvider`
/// reconstructs the next request from Assistant ToolCall + ToolResult pairs.
pub(crate) fn sanitize_history(items: &[HistoryItem]) -> Result<Arc<[HistoryItem]>, AgentError> {
    let mut sanitized = Vec::with_capacity(items.len());
    for item in items {
        match item {
            HistoryItem::User(user) => sanitized.push(HistoryItem::User(user.clone())),
            HistoryItem::ToolResult(result) => {
                sanitized.push(HistoryItem::ToolResult(result.clone()));
            }
            HistoryItem::Summary(summary) => sanitized.push(HistoryItem::Summary(summary.clone())),
            HistoryItem::Assistant(assistant) => {
                if let Some(assistant) = sanitize_assistant(assistant)? {
                    sanitized.push(HistoryItem::Assistant(assistant));
                }
            }
        }
    }
    Ok(sanitized.into())
}

fn sanitize_assistant(
    assistant: &AssistantHistory,
) -> Result<Option<AssistantHistory>, AgentError> {
    let mut content = Vec::with_capacity(assistant.content.len());
    for part in &assistant.content {
        match part {
            AssistantPart::Text(text) => content.push(AssistantPart::Text(text.clone())),
            AssistantPart::ToolCall(call) => content.push(AssistantPart::ToolCall(call.clone())),
            AssistantPart::Reasoning(reasoning) => {
                let text = reasoning.text().map(str::to_owned);
                let summary = reasoning.summary().map(str::to_owned);
                if text.is_none() && summary.is_none() {
                    // This reasoning part only carried opaque continuation data.
                    continue;
                }
                let cleaned = ReasoningContent::new(text, summary, None, None)
                    .map_err(|_| AgentError::Internal)?;
                content.push(AssistantPart::Reasoning(cleaned));
            }
        }
    }
    if content.is_empty() {
        return Ok(None);
    }
    Ok(Some(AssistantHistory {
        loop_id: assistant.loop_id,
        request_index: assistant.request_index,
        model: assistant.model.clone(),
        reasoning: assistant.reasoning,
        content,
        finish_reason: assistant.finish_reason,
        usage: assistant.usage,
    }))
}

/// Request for one paginated slice of a session's stored history.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetHistory {
    pub session_id: SessionId,
    /// Zero-based index of the first item to read.
    pub offset: usize,
    /// Number of items to return, bounded to `1..=100`.
    pub limit: usize,
}

impl GetHistory {
    pub(crate) fn validate(&self) -> Result<(), AgentError> {
        if (1..=MAX_HISTORY_LIMIT).contains(&self.limit) {
            Ok(())
        } else {
            Err(AgentError::InvalidArguments)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HistoryPage {
    pub items: Vec<IndexedHistoryItem>,
    /// `None` means this page is complete.
    pub next_offset: Option<usize>,
    pub total: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexedHistoryItem {
    pub index: usize,
    pub item: HistoryItemView,
}

/// Redacted view of one stored `HistoryItem`.
///
/// Tool-call arguments and opaque reasoning are never exposed here; read the
/// authoritative view through the safe fields only.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum HistoryItemView {
    User(UserHistoryView),
    Assistant(AssistantHistoryView),
    ToolResult(ToolResultHistoryView),
    Summary(SummaryHistoryView),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UserHistoryView {
    pub loop_id: LoopId,
    pub kind: UserMessageKind,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AssistantHistoryView {
    pub loop_id: LoopId,
    pub request_index: u32,
    pub model: String,
    pub reasoning_level: ReasoningPreference,
    pub text: String,
    pub reasoning: String,
    pub tool_calls: Vec<ToolCallView>,
    pub usage: Usage,
    pub finish_reason: ModelFinishReason,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolCallView {
    pub tool_call_id: ToolCallId,
    pub name: String,
    pub call_index: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolResultHistoryView {
    pub loop_id: LoopId,
    pub request_index: u32,
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub outcome: ToolResultOutcome,
    pub content: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SummaryHistoryView {
    pub content: String,
}

impl From<&HistoryItem> for HistoryItemView {
    fn from(item: &HistoryItem) -> Self {
        match item {
            HistoryItem::User(user) => Self::User(UserHistoryView::from(user)),
            HistoryItem::Assistant(assistant) => {
                Self::Assistant(AssistantHistoryView::from(assistant))
            }
            HistoryItem::ToolResult(result) => {
                Self::ToolResult(ToolResultHistoryView::from(result))
            }
            HistoryItem::Summary(summary) => Self::Summary(SummaryHistoryView::from(summary)),
        }
    }
}

impl From<&UserHistory> for UserHistoryView {
    fn from(user: &UserHistory) -> Self {
        Self {
            loop_id: user.loop_id,
            kind: user.kind,
            text: user.input.as_text().to_owned(),
        }
    }
}

impl From<&AssistantHistory> for AssistantHistoryView {
    fn from(assistant: &AssistantHistory) -> Self {
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut tool_calls = Vec::new();
        for part in &assistant.content {
            match part {
                AssistantPart::Text(part) => text.push_str(part),
                AssistantPart::Reasoning(part) => {
                    if let Some(text) = part.text() {
                        reasoning.push_str(text);
                    }
                    if let Some(summary) = part.summary() {
                        reasoning.push_str(summary);
                    }
                }
                AssistantPart::ToolCall(call) => tool_calls.push(ToolCallView {
                    tool_call_id: call.tool_call_id().clone(),
                    name: call.name().as_str().to_owned(),
                    call_index: call.call_index(),
                }),
            }
        }
        Self {
            loop_id: assistant.loop_id,
            request_index: assistant.request_index,
            model: assistant.model.as_str().to_owned(),
            reasoning_level: assistant.reasoning,
            text,
            reasoning,
            tool_calls,
            usage: assistant.usage,
            finish_reason: assistant.finish_reason,
        }
    }
}

impl From<&ToolResultHistory> for ToolResultHistoryView {
    fn from(result: &ToolResultHistory) -> Self {
        Self {
            loop_id: result.loop_id,
            request_index: result.request_index,
            tool_call_id: result.call_id.clone(),
            tool_name: result.tool_name.as_str().to_owned(),
            outcome: result.outcome,
            content: result.output.content().as_str().to_owned(),
        }
    }
}

impl From<&SummaryHistory> for SummaryHistoryView {
    fn from(summary: &SummaryHistory) -> Self {
        Self {
            content: summary.content.as_str().to_owned(),
        }
    }
}

/// Pages a session's in-memory history. History only appends while a session
/// is loaded, so item indexes are stable within one loaded lifetime.
pub(crate) fn page_history(history: &[HistoryItem], offset: usize, limit: usize) -> HistoryPage {
    let total = history.len();
    let end = offset.saturating_add(limit).min(total);
    let mut items = Vec::with_capacity(end.saturating_sub(offset));
    for (index, item) in history.iter().enumerate().skip(offset).take(limit) {
        items.push(IndexedHistoryItem {
            index,
            item: HistoryItemView::from(item),
        });
    }
    let next_offset = if end < total { Some(end) } else { None };
    HistoryPage {
        items,
        next_offset,
        total,
    }
}
