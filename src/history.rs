use std::sync::Arc;

use minicore_runtime::history::{AssistantHistory, HistoryItem};
use minicore_runtime::model::{AssistantPart, ReasoningContent};

use crate::error::AgentError;

/// Public/prose normalization strips opaque provider replay and legacy opaque
/// reasoning. Durable normalization uses the same canonical cleanup while
/// retaining validated replay in the existing history pipeline.
pub(crate) fn sanitize_history(items: &[HistoryItem]) -> Result<Arc<[HistoryItem]>, AgentError> {
    normalize(items, false)
}

pub(crate) fn normalize_history(items: &[HistoryItem]) -> Result<Arc<[HistoryItem]>, AgentError> {
    normalize(items, true)
}

fn normalize(items: &[HistoryItem], durable: bool) -> Result<Arc<[HistoryItem]>, AgentError> {
    #[cfg(feature = "e2e-observe")]
    let mut observation = crate::e2e_observe::Span::new(if durable {
        crate::e2e_observe::Work::NormalizeDurable
    } else {
        crate::e2e_observe::Work::NormalizePublic
    });
    let mut sanitized = Vec::with_capacity(items.len());
    for item in items {
        #[cfg(feature = "e2e-observe")]
        observation.add(1, 0);
        match item {
            HistoryItem::User(user) => sanitized.push(HistoryItem::User(user.clone())),
            HistoryItem::ToolResult(result) => {
                sanitized.push(HistoryItem::ToolResult(result.clone()));
            }
            HistoryItem::Summary(summary) => sanitized.push(HistoryItem::Summary(summary.clone())),
            HistoryItem::Assistant(assistant) => {
                if let Some(assistant) = sanitize_assistant(assistant, durable)? {
                    sanitized.push(HistoryItem::Assistant(assistant));
                }
            }
        }
    }
    Ok(sanitized.into())
}

fn sanitize_assistant(
    assistant: &AssistantHistory,
    durable: bool,
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
    let provider_replay = if durable {
        if let Some(replay) = &assistant.provider_replay {
            crate::models::validate_history_replay(replay, &content)
                .map_err(|_| AgentError::Internal)?;
        }
        assistant.provider_replay.clone()
    } else {
        None
    };
    if content.is_empty() && provider_replay.is_none() {
        return Ok(None);
    }
    Ok(Some(AssistantHistory {
        provider_replay,
        loop_id: assistant.loop_id,
        request_index: assistant.request_index,
        model: assistant.model.clone(),
        reasoning: assistant.reasoning,
        content,
        finish_reason: assistant.finish_reason,
        usage: assistant.usage,
    }))
}
