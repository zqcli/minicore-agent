use super::*;

pub(super) fn capture_output_item(
    state: &mut StreamState,
    output_index: u32,
    item: &Value,
) -> Result<(), ()> {
    replay::validate_item(item)?;
    if state.output_evidence.len() >= MAX_REPLAY_ITEMS_PER_RESPONSE
        || state
            .output_evidence
            .insert(output_index, replay::ItemEvidence::new(item)?)
            .is_some()
        || item
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| !state.output_ids.insert(id.to_owned()))
    {
        return Err(());
    }
    if item_type(item) == Some("message") {
        state.refusal_seen |= item["content"]
            .as_array()
            .ok_or(())?
            .iter()
            .any(|part| item_type(part) == Some("refusal"));
    }
    if let Some(output_items) = state.output_items.as_mut() {
        state.output_item_bytes += crate::budget::encoded_len(item);
        if state.output_item_bytes > MAX_REPLAY_BYTES {
            state.output_items = None;
        } else {
            output_items.insert(output_index, item.clone());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct TerminalEvent {
    pub(super) response: ProviderResponse,
}

#[derive(Deserialize)]
pub(super) struct ProviderResponse {
    status: Option<String>,
    usage: Option<ProviderUsage>,
    incomplete_details: Option<IncompleteDetails>,
    output: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub(super) struct ProviderUsage {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: Option<u64>,
    input_tokens_details: Option<InputTokenDetails>,
    output_tokens_details: Option<OutputTokenDetails>,
}

#[derive(Deserialize)]
struct InputTokenDetails {
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct OutputTokenDetails {
    reasoning_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct IncompleteDetails {
    reason: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) enum TerminalKind {
    Completed,
    Incomplete,
}

pub(super) fn finish_response(
    state: &mut StreamState,
    response: ProviderResponse,
    kind: TerminalKind,
) -> Result<(), ()> {
    if state.terminal_seen || state.tools.values().any(|tool| !tool.ended) {
        return Err(());
    }
    let expected_status = match kind {
        TerminalKind::Completed => "completed",
        TerminalKind::Incomplete => "incomplete",
    };
    if response
        .status
        .as_deref()
        .is_some_and(|status| status != expected_status)
    {
        return Err(());
    }
    let usage = response.usage.map(provider_usage).transpose()?;
    state.finalize_output_items(response.output)?;
    let text = if let Some(output) = &state.output_items {
        state.refusal_seen |= output
            .values()
            .filter(|item| item_type(item) == Some("message"))
            .filter_map(|item| item.get("content").and_then(Value::as_array))
            .flatten()
            .any(|part| item_type(part) == Some("refusal"));
        replay::visible_text(output.values())?
    } else {
        state
            .output_evidence
            .values()
            .map(|item| item.text.as_str())
            .collect()
    };
    if !text.is_empty() && text != state.text {
        let suffix = text.strip_prefix(&state.text).ok_or(())?.to_owned();
        queue_text(state, suffix, false);
    }
    let reason = if matches!(kind, TerminalKind::Incomplete) {
        match response
            .incomplete_details
            .and_then(|details| details.reason)
        {
            Some(reason) if matches!(reason.as_str(), "max_output_tokens" | "max_tokens") => {
                ModelFinishReason::Length
            }
            Some(reason) if reason == "content_filter" => ModelFinishReason::ContentFiltered,
            Some(reason) if reason == "refusal" => ModelFinishReason::Refused,
            _ => ModelFinishReason::Unknown,
        }
    } else if state.refusal_seen {
        ModelFinishReason::Refused
    } else if !state.tools.is_empty() {
        ModelFinishReason::ToolCalls
    } else {
        ModelFinishReason::Stop
    };
    if let Some(event) = state.replay_event()? {
        state.queue(event);
    }
    state.terminal_seen = true;
    if let Some(trace) = state.trace {
        tracing::debug!(
            loop_id = %trace.loop_id,
            request_index = trace.request_index,
            finish_reason = ?reason,
            "provider request terminal"
        );
    }
    if let Some(usage) = usage {
        state.queue(ModelEvent::Usage { usage });
    }
    state.queue(ModelEvent::Finish { reason });
    state.done = true;
    Ok(())
}

pub(super) fn provider_usage(usage: ProviderUsage) -> Result<Usage, ()> {
    let cached = usage
        .input_tokens_details
        .as_ref()
        .and_then(|details| details.cached_tokens);
    let cache_write = usage
        .input_tokens_details
        .and_then(|details| details.cache_write_tokens);
    let cached_and_written = cached
        .unwrap_or(0)
        .checked_add(cache_write.unwrap_or(0))
        .ok_or(())?;
    let input = usage
        .input_tokens
        .checked_sub(cached_and_written)
        .ok_or(())?;
    let reasoning = usage
        .output_tokens_details
        .and_then(|details| details.reasoning_tokens);
    let output = usage
        .output_tokens
        .checked_sub(reasoning.unwrap_or(0))
        .ok_or(())?;
    if let Some(total) = usage.total_tokens
        && usage.input_tokens.checked_add(usage.output_tokens) != Some(total)
    {
        return Err(());
    }
    // OpenAI reports cached tokens only; a missing cache-write count is none.
    Ok(
        Usage::from_optional(cached.map(|_| input), reasoning.map(|_| output), reasoning)
            .with_cache_read_tokens(cached)
            .with_cache_write_tokens(cache_write)
            .with_provider_total_tokens(usage.total_tokens),
    )
}
