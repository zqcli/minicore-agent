#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt;
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

#[cfg(test)]
thread_local! {
    static TOOL_RESULT_SCAN_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_tool_result_scan_count() {
    TOOL_RESULT_SCAN_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn tool_result_scan_count() -> usize {
    TOOL_RESULT_SCAN_COUNT.with(Cell::get)
}

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
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum HistoryItemView {
    User(UserHistoryView),
    Assistant(AssistantHistoryView),
    ToolResult(ToolResultHistoryView),
    Summary(SummaryHistoryView),
}

impl fmt::Debug for HistoryItemView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User(view) => formatter.debug_tuple("User").field(view).finish(),
            Self::Assistant(view) => formatter.debug_tuple("Assistant").field(view).finish(),
            Self::ToolResult(view) => formatter.debug_tuple("ToolResult").field(view).finish(),
            Self::Summary(view) => formatter.debug_tuple("Summary").field(view).finish(),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct UserHistoryView {
    pub loop_id: LoopId,
    pub kind: UserMessageKind,
    pub text: String,
    /// RFC3339 acceptance time recorded by the Agent (new sessions); `None`
    /// for pre-`user_times` history — never back-filled with `now()`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

impl fmt::Debug for UserHistoryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserHistoryView")
            .field("loop_id", &self.loop_id)
            .field("kind", &self.kind)
            .field("text_bytes", &self.text.len())
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
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
    /// Visible assistant parts in their original order (already sanitized);
    /// the flattened `text`/`reasoning` fields remain for old clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<crate::presentation::AssistantDisplayPart>>,
}

impl fmt::Debug for AssistantHistoryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssistantHistoryView")
            .field("loop_id", &self.loop_id)
            .field("request_index", &self.request_index)
            .field("model", &self.model)
            .field("reasoning_level", &self.reasoning_level)
            .field("text_bytes", &self.text.len())
            .field("reasoning_bytes", &self.reasoning.len())
            .field("tool_call_count", &self.tool_calls.len())
            .field("usage", &self.usage)
            .field("finish_reason", &self.finish_reason)
            .field("part_count", &self.parts.as_ref().map(Vec::len))
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolCallView {
    pub tool_call_id: ToolCallId,
    pub name: String,
    pub call_index: u32,
    /// Whitelisted display regenerated from the stored arguments/results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<crate::presentation::ToolDisplay>,
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolResultHistoryView {
    pub loop_id: LoopId,
    pub request_index: u32,
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub outcome: ToolResultOutcome,
    pub content: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub content_truncated: bool,
}

impl fmt::Debug for ToolResultHistoryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResultHistoryView")
            .field("loop_id", &self.loop_id)
            .field("request_index", &self.request_index)
            .field("tool_call_id", &self.tool_call_id)
            .field("tool_name", &self.tool_name)
            .field("outcome", &self.outcome)
            .field("content_len", &self.content.len())
            .field("content_truncated", &self.content_truncated)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct SummaryHistoryView {
    pub content: String,
}

impl fmt::Debug for SummaryHistoryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SummaryHistoryView")
            .field("content_len", &self.content.len())
            .finish()
    }
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
            timestamp: None,
        }
    }
}

impl From<&AssistantHistory> for AssistantHistoryView {
    fn from(assistant: &AssistantHistory) -> Self {
        assistant_history_view(assistant, &std::collections::HashMap::new())
    }
}

impl From<&ToolResultHistory> for ToolResultHistoryView {
    fn from(result: &ToolResultHistory) -> Self {
        let (content, content_truncated) = crate::presentation::bounded_result_content(
            result.output.content().as_str(),
            crate::presentation::MAX_RESULT_DISPLAY_BYTES,
        );
        Self {
            loop_id: result.loop_id,
            request_index: result.request_index,
            tool_call_id: result.call_id.clone(),
            tool_name: result.tool_name.as_str().to_owned(),
            outcome: result.outcome,
            content,
            content_truncated,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
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
/// `user_times` maps `(loop_id, occurrence)` to the persisted RFC3339
/// acceptance time for each stored User item (old records simply have no
/// entry). Tool-call displays are regenerated from stored args + results with
/// the same whitelist formatter the live path uses.
pub(crate) fn page_history(
    history: &[HistoryItem],
    offset: usize,
    limit: usize,
    user_times: &std::collections::HashMap<(LoopId, usize), String>,
) -> HistoryPage {
    let total = history.len();
    let page_start = offset.min(total);
    let end = offset.saturating_add(limit).min(total);
    let tool_results = collect_tool_results(history, page_start, end);
    let mut items = Vec::with_capacity(end.saturating_sub(offset));
    // Count occurrences before the requested page as well. A page boundary
    // must not make the second identical User item look like occurrence zero.
    let mut user_occurrences: std::collections::HashMap<LoopId, usize> =
        std::collections::HashMap::new();
    for item in history.iter().take(offset) {
        if let HistoryItem::User(user) = item {
            *user_occurrences.entry(user.loop_id).or_default() += 1;
        }
    }
    for (index, item) in history.iter().enumerate().skip(offset).take(limit) {
        items.push(IndexedHistoryItem {
            index,
            item: match item {
                HistoryItem::User(user) => {
                    let occurrence = user_occurrences.entry(user.loop_id).or_insert(0);
                    let timestamp = user_times.get(&(user.loop_id, *occurrence)).cloned();
                    *occurrence += 1;
                    HistoryItemView::User(UserHistoryView {
                        loop_id: user.loop_id,
                        kind: user.kind,
                        text: user.input.as_text().to_owned(),
                        timestamp,
                    })
                }
                HistoryItem::Assistant(assistant) => {
                    HistoryItemView::Assistant(assistant_history_view(assistant, &tool_results))
                }
                HistoryItem::ToolResult(result) => {
                    HistoryItemView::ToolResult(ToolResultHistoryView::from(result))
                }
                HistoryItem::Summary(summary) => {
                    HistoryItemView::Summary(SummaryHistoryView::from(summary))
                }
            },
        });
    }
    let next_offset = if end < total { Some(end) } else { None };
    HistoryPage {
        items,
        next_offset,
        total,
    }
}

/// Maps only ToolResults needed by the current page. Runtime-produced history
/// appends an Assistant followed immediately by its ToolResult batch, so a
/// page needs its own items plus the contiguous ToolResult suffix just beyond
/// the right edge. Best-effort: a view never needs the result to render.
fn collect_tool_results(
    history: &[HistoryItem],
    page_start: usize,
    page_end: usize,
) -> HashMap<(LoopId, u32, ToolCallId), (String, bool)> {
    let keys = history
        .iter()
        .skip(page_start)
        .take(page_end.saturating_sub(page_start))
        .filter_map(|item| match item {
            HistoryItem::Assistant(assistant) => Some(assistant),
            _ => None,
        })
        .flat_map(|assistant| {
            let loop_id = assistant.loop_id;
            let request_index = assistant.request_index;
            assistant.content.iter().filter_map(move |part| {
                part.as_tool_call()
                    .map(|call| (loop_id, request_index, call.tool_call_id()))
            })
        })
        .collect::<HashSet<(LoopId, u32, &ToolCallId)>>();
    if keys.is_empty() {
        return HashMap::new();
    }

    let mut results = HashMap::new();
    for item in history
        .iter()
        .skip(page_start)
        .take(page_end.saturating_sub(page_start))
    {
        let HistoryItem::ToolResult(result) = item else {
            continue;
        };
        add_tool_result(&mut results, &keys, result);
    }
    if results.len() < keys.len() {
        for item in history.iter().skip(page_end) {
            let HistoryItem::ToolResult(result) = item else {
                break;
            };
            add_tool_result(&mut results, &keys, result);
            if results.len() == keys.len() {
                break;
            }
        }
    }
    results
}

fn add_tool_result(
    results: &mut HashMap<(LoopId, u32, ToolCallId), (String, bool)>,
    keys: &HashSet<(LoopId, u32, &ToolCallId)>,
    result: &ToolResultHistory,
) {
    #[cfg(test)]
    TOOL_RESULT_SCAN_COUNT.with(|count| count.set(count.get() + 1));
    if !keys.contains(&(result.loop_id, result.request_index, &result.call_id)) {
        return;
    }
    let key = (result.loop_id, result.request_index, result.call_id.clone());
    let (content, truncated) = crate::presentation::bounded_result_content(
        result.output.content().as_str(),
        crate::presentation::MAX_RESULT_DISPLAY_BYTES,
    );
    if !content.is_empty() {
        results.insert(key, (content, truncated));
    }
}

fn assistant_history_view(
    assistant: &AssistantHistory,
    tool_results: &std::collections::HashMap<(LoopId, u32, ToolCallId), (String, bool)>,
) -> AssistantHistoryView {
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
            AssistantPart::ToolCall(call) => {
                let display = crate::presentation::build_tool_display(
                    call.name().as_str(),
                    Some(call.arguments()),
                    tool_results
                        .get(&(
                            assistant.loop_id,
                            assistant.request_index,
                            call.tool_call_id().clone(),
                        ))
                        .map(|(content, _)| content.as_str()),
                );
                tool_calls.push(ToolCallView {
                    tool_call_id: call.tool_call_id().clone(),
                    name: call.name().as_str().to_owned(),
                    call_index: call.call_index(),
                    display: Some(display),
                });
            }
        }
    }
    AssistantHistoryView {
        loop_id: assistant.loop_id,
        request_index: assistant.request_index,
        model: assistant.model.as_str().to_owned(),
        reasoning_level: assistant.reasoning,
        text,
        reasoning,
        tool_calls,
        parts: Some(crate::presentation::assistant_display_parts(
            &assistant.content,
        )),
        usage: assistant.usage,
        finish_reason: assistant.finish_reason,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use minicore_runtime::history::{AssistantHistory, ToolResultHistory};
    use minicore_runtime::model::{AssistantPart, ModelFinishReason, ModelRef, Usage};
    use minicore_runtime::tools::{ToolName, ToolOutput, ToolResultOutcome};

    fn tool_pair(
        loop_id: LoopId,
        request_index: u32,
        call_id: &str,
        output: &str,
    ) -> (HistoryItem, HistoryItem) {
        let call_id = call_id.parse::<ToolCallId>().unwrap();
        let call = minicore_runtime::model::ToolCall::new(
            call_id.clone(),
            "bash".parse::<ToolName>().unwrap(),
            serde_json::json!({"command": call_id.as_str()}),
            0,
        )
        .unwrap();
        (
            HistoryItem::Assistant(AssistantHistory {
                loop_id,
                request_index,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(call)],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::default(),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index,
                call_id,
                tool_name: "bash".parse::<ToolName>().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new(output).unwrap(),
            }),
        )
    }

    fn tool_batch(loop_id: LoopId, request_index: u32) -> (HistoryItem, Vec<HistoryItem>) {
        let call_ids = ["batch-one", "batch-two"];
        let calls = call_ids
            .iter()
            .enumerate()
            .map(|(call_index, call_id)| {
                let call_id = call_id.parse::<ToolCallId>().unwrap();
                minicore_runtime::model::ToolCall::new(
                    call_id.clone(),
                    "bash".parse::<ToolName>().unwrap(),
                    serde_json::json!({"command": call_id.as_str()}),
                    call_index as u32,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let results = [
            ("batch-one", "first batch result"),
            ("batch-two", "second batch result"),
        ]
        .into_iter()
        .map(|(call_id, output)| {
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index,
                call_id: call_id.parse().unwrap(),
                tool_name: "bash".parse::<ToolName>().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new(output).unwrap(),
            })
        })
        .collect();
        (
            HistoryItem::Assistant(AssistantHistory {
                loop_id,
                request_index,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: calls.into_iter().map(AssistantPart::ToolCall).collect(),
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::default(),
            }),
            results,
        )
    }

    #[test]
    fn page_history_does_not_process_unrelated_tool_results() {
        let mut history = Vec::new();
        for index in 0..128 {
            let (assistant, result) = tool_pair(
                LoopId::new().unwrap(),
                0,
                &format!("old-call-{index}"),
                "old result",
            );
            history.extend([assistant, result]);
        }
        let current_assistant_index = history.len();
        let (assistant, result) =
            tool_pair(LoopId::new().unwrap(), 0, "current-call", "current result");
        history.extend([assistant, result]);

        reset_tool_result_scan_count();
        let page = page_history(&history, current_assistant_index, 1, &HashMap::new());

        assert_eq!(tool_result_scan_count(), 1);
        let HistoryItemView::Assistant(assistant) = &page.items[0].item else {
            panic!("expected the current Assistant item");
        };
        assert_eq!(
            assistant.tool_calls[0].display.as_ref().unwrap().detail,
            "$ current-call"
        );
    }

    #[test]
    fn page_history_skips_result_collection_without_page_tool_calls() {
        let loop_id = LoopId::new().unwrap();
        let mut history = vec![HistoryItem::User(UserHistory {
            loop_id,
            kind: UserMessageKind::Prompt,
            input: minicore_runtime::execution::UserInput::text("plain").unwrap(),
        })];
        for index in 0..128 {
            let (assistant, result) = tool_pair(
                LoopId::new().unwrap(),
                0,
                &format!("old-call-{index}"),
                "old result",
            );
            history.extend([assistant, result]);
        }

        reset_tool_result_scan_count();
        let page = page_history(&history, 0, 1, &HashMap::new());

        assert_eq!(tool_result_scan_count(), 0);
        assert!(matches!(page.items[0].item, HistoryItemView::User(_)));
    }

    #[test]
    fn page_history_keeps_missing_and_cross_identity_results_distinct() {
        let loop_id = LoopId::new().unwrap();
        let (assistant_one, result_one) = tool_pair(loop_id, 0, "same-call", "first result");
        let (assistant_two, result_two) =
            tool_pair(loop_id, 1, "same-call", "second result\nextra");
        let missing_loop = LoopId::new().unwrap();
        let (missing_assistant, _) = tool_pair(missing_loop, 0, "missing-call", "unused");

        let history = vec![
            assistant_one,
            result_one,
            assistant_two,
            result_two,
            missing_assistant,
        ];
        let page = page_history(&history, 0, 5, &HashMap::new());

        let HistoryItemView::Assistant(first) = &page.items[0].item else {
            panic!("expected first Assistant item");
        };
        let HistoryItemView::Assistant(second) = &page.items[2].item else {
            panic!("expected second Assistant item");
        };
        let HistoryItemView::Assistant(missing) = &page.items[4].item else {
            panic!("expected missing-result Assistant item");
        };
        assert_eq!(
            first.tool_calls[0]
                .display
                .as_ref()
                .unwrap()
                .hidden_line_count,
            Some(4)
        );
        assert_eq!(
            second.tool_calls[0]
                .display
                .as_ref()
                .unwrap()
                .hidden_line_count,
            Some(5)
        );
        assert_eq!(
            missing.tool_calls[0]
                .display
                .as_ref()
                .unwrap()
                .hidden_line_count,
            Some(3)
        );
    }

    #[test]
    fn page_history_finds_a_tool_batch_beyond_the_page_boundary() {
        let loop_id = LoopId::new().unwrap();
        let (assistant, results) = tool_batch(loop_id, 0);
        let mut history = vec![assistant];
        history.extend(results);

        reset_tool_result_scan_count();
        let page = page_history(&history, 0, 1, &HashMap::new());

        assert_eq!(tool_result_scan_count(), 2);
        let HistoryItemView::Assistant(assistant) = &page.items[0].item else {
            panic!("expected Assistant item");
        };
        assert_eq!(assistant.tool_calls.len(), 2);
        assert_eq!(assistant.tool_calls[0].call_index, 0);
        assert_eq!(assistant.tool_calls[1].call_index, 1);
        assert_eq!(
            assistant.tool_calls[0].display.as_ref().unwrap().detail,
            "$ batch-one"
        );
        assert_eq!(
            assistant.tool_calls[1].display.as_ref().unwrap().detail,
            "$ batch-two"
        );
    }

    #[test]
    fn history_tool_display_results_use_the_full_tool_identity() {
        let call_id: ToolCallId = "same-call".parse().unwrap();
        let call_one = minicore_runtime::model::ToolCall::new(
            call_id.clone(),
            "bash".parse::<ToolName>().unwrap(),
            serde_json::json!({"command": "first"}),
            0,
        )
        .unwrap();
        let call_two = minicore_runtime::model::ToolCall::new(
            call_id.clone(),
            "bash".parse::<ToolName>().unwrap(),
            serde_json::json!({"command": "second"}),
            0,
        )
        .unwrap();
        let loop_one: LoopId = "lup_00000000000000000000000000000001".parse().unwrap();
        let loop_two: LoopId = "lup_00000000000000000000000000000002".parse().unwrap();
        let history = vec![
            HistoryItem::Assistant(AssistantHistory {
                loop_id: loop_one,
                request_index: 0,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(call_one)],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::default(),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id: loop_one,
                request_index: 0,
                call_id: call_id.clone(),
                tool_name: "bash".parse::<ToolName>().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new("first result").unwrap(),
            }),
            HistoryItem::Assistant(AssistantHistory {
                loop_id: loop_two,
                request_index: 0,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(call_two)],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::default(),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id: loop_two,
                request_index: 0,
                call_id,
                tool_name: "bash".parse::<ToolName>().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new("second result").unwrap(),
            }),
        ];
        let results = collect_tool_results(&history, 0, history.len());
        let one = match &history[0] {
            HistoryItem::Assistant(assistant) => assistant_history_view(assistant, &results),
            _ => unreachable!(),
        };
        let two = match &history[2] {
            HistoryItem::Assistant(assistant) => assistant_history_view(assistant, &results),
            _ => unreachable!(),
        };
        assert_eq!(
            one.tool_calls[0].display.as_ref().unwrap().detail,
            "$ first"
        );
        assert_eq!(
            two.tool_calls[0].display.as_ref().unwrap().detail,
            "$ second"
        );
        assert_eq!(
            one.tool_calls[0]
                .display
                .as_ref()
                .unwrap()
                .hidden_line_count,
            Some(4)
        );
        assert_eq!(
            two.tool_calls[0]
                .display
                .as_ref()
                .unwrap()
                .hidden_line_count,
            Some(4)
        );
    }

    #[test]
    fn history_view_debug_redacts_user_and_tool_content() {
        let user = UserHistoryView {
            loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
            kind: UserMessageKind::Prompt,
            text: "do not log this prompt".to_owned(),
            timestamp: Some("2026-09-05T14:05:06Z".to_owned()),
        };
        let tool = ToolResultHistoryView {
            loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
            request_index: 0,
            tool_call_id: "call".parse().unwrap(),
            tool_name: "bash".to_owned(),
            outcome: ToolResultOutcome::Success,
            content: "do not log this result".to_owned(),
            content_truncated: false,
        };
        let debug = format!("{user:?} {tool:?}");
        assert!(!debug.contains("do not log this prompt"));
        assert!(!debug.contains("do not log this result"));
        assert!(debug.contains("text_bytes"));
        assert!(debug.contains("content_len"));
    }
}
