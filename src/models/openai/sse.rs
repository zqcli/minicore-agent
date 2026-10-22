use super::*;

#[derive(Default)]
pub(super) struct SseParser {
    line: Vec<u8>,
    frame_data: Vec<u8>,
    frame_has_data: bool,
    pending_cr: bool,
    frames: VecDeque<Vec<u8>>,
    queued_frame_bytes: usize,
}

impl SseParser {
    pub(super) fn feed(&mut self, chunk: &[u8]) -> Result<(), ()> {
        for &byte in chunk {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => {
                    self.pending_cr = true;
                    self.end_line()?;
                }
                b'\n' => self.end_line()?,
                _ => {
                    if self.line.len() >= MAX_SSE_LINE_BYTES {
                        return Err(());
                    }
                    self.line.push(byte);
                }
            }
        }
        Ok(())
    }

    fn end_line(&mut self) -> Result<(), ()> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            if self.frame_has_data {
                if self.frame_data.last() == Some(&b'\n') {
                    self.frame_data.pop();
                }
                let queued = self.queued_frame_bytes + self.frame_data.len();
                if queued > MAX_SSE_FRAME_BYTES || self.frames.len() >= MAX_QUEUED_SSE_FRAMES {
                    return Err(());
                }
                self.queued_frame_bytes = queued;
                self.frames.push_back(std::mem::take(&mut self.frame_data));
            }
            self.frame_data.clear();
            self.frame_has_data = false;
            return Ok(());
        }
        if line.first() == Some(&b':') {
            return Ok(());
        }
        if let Some(data) = line.strip_prefix(b"data:") {
            let data = data.strip_prefix(b" ").unwrap_or(data);
            let new_len = self.frame_data.len() + data.len() + 1;
            if new_len > MAX_SSE_FRAME_BYTES {
                return Err(());
            }
            self.frame_data.extend_from_slice(data);
            self.frame_data.push(b'\n');
            self.frame_has_data = true;
        }
        Ok(())
    }

    pub(super) fn pop_frame(&mut self) -> Option<Vec<u8>> {
        let frame = self.frames.pop_front()?;
        self.queued_frame_bytes -= frame.len();
        Some(frame)
    }
}

pub(super) fn handle_frame(state: &mut StreamState, frame: &[u8]) -> Result<(), ()> {
    if frame == b"[DONE]" {
        let error = stream_error(
            ModelErrorKind::IncompleteResponse,
            state.provider_event_seen,
        );
        state.fail(error);
        return Ok(());
    }
    let value: Value = serde_json::from_slice(frame).map_err(|_| ())?;
    let event_type = value.get("type").and_then(Value::as_str).ok_or(())?;
    state.provider_event_seen = true;
    state.output_seen |= event_type.starts_with("response.output_")
        || event_type.starts_with("response.content_part.")
        || event_type.starts_with("response.reasoning_")
        || event_type.starts_with("response.refusal.")
        || event_type.starts_with("response.function_call_");
    match event_type {
        "response.output_text.delta" => {
            let event: DeltaEvent = from_value(value)?;
            queue_text(state, event.delta, false);
        }
        "response.refusal.delta" => {
            let event: DeltaEvent = from_value(value)?;
            state.refusal_seen |= !event.delta.is_empty();
            queue_text(state, event.delta, false);
        }
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            let event: ReasoningSummaryDeltaEvent = from_value(value)?;
            queue_reasoning_delta(state, event);
            state.reasoning_seen = true;
        }
        // The name OpenAI used before `reasoning_text`, still sent by some
        // proxies; its delta was not always text, and only text is shown.
        "response.reasoning.delta" => {
            if value.get("delta").is_some_and(Value::is_string) {
                let event: ReasoningSummaryDeltaEvent = from_value(value)?;
                queue_reasoning_delta(state, event);
                state.reasoning_seen = true;
            }
        }
        // Explicit summary-part lifecycle: some providers split ONE reasoning
        // item into several summary parts and only signal the part via these
        // events (the delta frames carry no index). Each event is a real part
        // boundary, so a following delta (even index-less) starts a new part
        // while deltas within the same part keep concatenating.
        "response.reasoning_summary_part.added" | "response.reasoning_summary_part.done" => {
            state.reasoning_parts.mark_item_boundary();
        }
        "response.output_item.added" => {
            let event: OutputItemEvent = from_value(value)?;
            if item_type(&event.item) == Some("function_call") {
                let item: FunctionCallItem = from_value(event.item)?;
                start_tool(state, event.output_index, item)?;
            } else if item_type(&event.item) == Some("reasoning") {
                state.reasoning_parts.mark_item_boundary();
            }
        }
        "response.function_call_arguments.delta" => {
            let event: ArgumentsDeltaEvent = from_value(value)?;
            append_tool_arguments(state, event.output_index, &event.delta)?;
        }
        "response.function_call_arguments.done" => {
            let event: ArgumentsDoneEvent = from_value(value)?;
            finish_tool_arguments(state, event.output_index, &event.arguments)?;
        }
        "response.output_item.done" => {
            let event: OutputItemEvent = from_value(value)?;
            capture_output_item(state, event.output_index, &event.item)?;
            if item_type(&event.item) == Some("function_call") {
                let item: FunctionCallItem = from_value(event.item)?;
                finish_tool_item(state, event.output_index, item)?;
            } else if item_type(&event.item) == Some("reasoning") {
                // A provider that streamed no reasoning text may still return
                // it in the finished item.
                if !state.reasoning_seen {
                    queue_item_reasoning(state, event.output_index, &event.item);
                }
                state.reasoning_parts.mark_item_boundary();
            }
        }
        "response.completed" => {
            let event: TerminalEvent = from_value(value)?;
            finish_response(state, event.response, TerminalKind::Completed)?;
        }
        "response.incomplete" => {
            let event: TerminalEvent = from_value(value)?;
            finish_response(state, event.response, TerminalKind::Incomplete)?;
        }
        "response.failed" | "error" => {
            state.terminal_seen = true;
            let error = value
                .pointer("/response/error")
                .or_else(|| value.get("error"))
                .unwrap_or(&value);
            let kind = ["code", "type"]
                .into_iter()
                .filter_map(|field| error.get(field).and_then(Value::as_str))
                .find_map(|code| match code {
                    "context_length_exceeded" | "context_window_exceeded" | "context_overflow" => {
                        Some(ModelErrorKind::ContextOverflow)
                    }
                    "insufficient_quota" | "quota_exceeded" => Some(ModelErrorKind::QuotaExceeded),
                    "rate_limit_exceeded" => Some(ModelErrorKind::RateLimited),
                    _ => None,
                })
                .unwrap_or(ModelErrorKind::ProviderUnavailable);
            let failed_output = value
                .pointer("/response/output")
                .and_then(Value::as_array)
                .is_some_and(|output| !output.is_empty());
            state.fail(if state.output_seen || failed_output {
                started_error(kind)
            } else if kind == ModelErrorKind::RateLimited {
                retryable_error(kind, None)
            } else {
                local_error(kind)
            });
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn item_type(value: &Value) -> Option<&str> {
    value.get("type").and_then(Value::as_str)
}

fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, ()> {
    serde_json::from_value(value).map_err(|_| ())
}

#[derive(Deserialize)]
struct DeltaEvent {
    delta: String,
}

#[derive(Deserialize)]
struct OutputItemEvent {
    output_index: u32,
    item: Value,
}

#[derive(Deserialize)]
struct ArgumentsDeltaEvent {
    output_index: u32,
    delta: String,
}

#[derive(Deserialize)]
struct ArgumentsDoneEvent {
    output_index: u32,
    arguments: String,
}
