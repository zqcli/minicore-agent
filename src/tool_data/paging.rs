//! Byte-bounded tool reads and raw-output pages.
use super::record::{ToolRecord, stream_availability};
use super::types::{
    ToolDataAvailability, ToolDataStream, ToolInputSummary, ToolInvocationData, ToolOutputPage,
    ToolOutputRequest, ToolReadResult, ToolRef,
};
use super::{INPUT_ENCODING, MAX_INVOCATION_PREVIEW_BYTES, STREAM_ENCODING};
use crate::budget::{encoded_len, encoded_result};
use crate::error::AgentError;
use crate::text::truncate;
use base64::Engine as _;

impl ToolRecord {
    pub(crate) fn project_read(
        &self,
        tool_ref: &ToolRef,
        max_bytes: usize,
    ) -> Result<ToolReadResult, AgentError> {
        let execution = self.execution_data(tool_ref);
        let invocation = (self.input_seen || self.input_expired).then(|| {
            let (preview, cut) = truncate(&self.input, MAX_INVOCATION_PREVIEW_BYTES);
            ToolInvocationData {
                tool_ref: tool_ref.clone(),
                name: self.name.clone(),
                subject: self.subject.clone(),
                subject_truncated: self.subject_truncated,
                input: ToolInputSummary {
                    total_bytes: self.input_total,
                    preview: preview.to_owned(),
                    truncated: cut
                        || self.input_truncated
                        || self.input_expired
                        || self.input_corrupt,
                    encoding: INPUT_ENCODING,
                },
            }
        });
        let mut result = ToolReadResult {
            display: None,
            invocation,
            execution,
        };
        if encoded_len(&result) > max_bytes {
            let Some(source) = result.invocation.as_ref().map(|i| i.input.preview.clone()) else {
                return Err(AgentError::InvalidArguments);
            };
            let mut template = result.clone();
            if let Some(invocation) = template.invocation.as_mut() {
                invocation.input.preview.clear();
                invocation.input.truncated = true;
            }
            let template_len = encoded_len(&template);
            if template_len > max_bytes {
                return Err(AgentError::InvalidArguments);
            }
            // The template already encodes the empty preview's two quotes.
            let available = max_bytes.saturating_sub(template_len).saturating_add(2);
            let (preview, _) = fit_encoded_string(&source, available);
            if let Some(invocation) = result.invocation.as_mut() {
                invocation.input.preview = preview;
                invocation.input.truncated = true;
            }
        }
        Ok(result)
    }

    pub(crate) fn project_display_read(
        &self,
        tool_ref: &ToolRef,
        max_bytes: usize,
    ) -> Result<ToolReadResult, AgentError> {
        let arguments = (!self.input_truncated && !self.input_expired && !self.input_corrupt)
            .then(|| serde_json::from_str(&self.input).ok())
            .flatten();
        let mut display =
            crate::presentation::build_tool_card_display(&self.name, arguments.as_ref());
        if arguments.is_none()
            && matches!(
                self.name.as_str(),
                "write" | "edit" | "patch" | "apply_patch" | "bash"
            )
        {
            display.body_truncated = true;
        }
        let mut result = ToolReadResult {
            display: Some(display),
            invocation: None,
            execution: self.execution_data(tool_ref),
        };
        if encoded_len(&result) > max_bytes {
            let source = result
                .display
                .as_mut()
                .and_then(|display| display.expanded_input.take())
                .ok_or(AgentError::InvalidArguments)?;
            let display = result.display.as_mut().ok_or(AgentError::Internal)?;
            display.expanded_input = Some(String::new());
            display.truncated = true;
            display.body_truncated = true;
            let template_len = encoded_len(&result);
            if template_len > max_bytes {
                return Err(AgentError::InvalidArguments);
            }
            let available = max_bytes.saturating_sub(template_len).saturating_add(2);
            let (body, _) = fit_encoded_string(&source, available);
            let display = result.display.as_mut().ok_or(AgentError::Internal)?;
            display.input_line_count = Some(crate::presentation::count_lines(&body));
            display.hidden_line_count = display.input_line_count;
            display.expanded_input = Some(body);
        }
        Ok(result)
    }

    pub(crate) fn project_output(
        &self,
        request: &ToolOutputRequest,
        max_bytes: usize,
    ) -> Result<ToolOutputPage, AgentError> {
        let stream = request.stream;
        if stream.is_process() {
            return output_process(self, request, max_bytes);
        }
        let (content, observed, seen, expired, truncated) = match stream {
            ToolDataStream::Input => (
                self.input.as_str(),
                self.input_total,
                self.input_seen,
                self.input_expired,
                self.input_truncated,
            ),
            ToolDataStream::Output => (
                self.result.as_str(),
                self.result_total,
                self.result_seen,
                self.result_expired,
                self.result_truncated,
            ),
            ToolDataStream::Stdout | ToolDataStream::Stderr => {
                return output_process(self, request, max_bytes);
            }
        };
        let availability = stream_availability(
            seen,
            expired,
            self.state.is_terminal(),
            content.len(),
            observed,
            truncated,
        );
        let base = ToolOutputPage {
            tool_ref: request.tool_ref.clone(),
            stream,
            encoding: stream.encoding(),
            base_offset: 0,
            next_offset: 0,
            observed_end: observed as u64,
            eof: false,
            truncated: false,
            availability,
            data: String::new(),
        };
        if expired {
            return encoded_result(
                ToolOutputPage {
                    base_offset: observed as u64,
                    next_offset: observed as u64,
                    eof: true,
                    truncated: true,
                    ..base
                },
                max_bytes,
            );
        }
        if !seen {
            if request.offset != 0 {
                return Err(AgentError::InvalidArguments);
            }
            return encoded_result(
                ToolOutputPage {
                    eof: availability != ToolDataAvailability::Pending,
                    ..base
                },
                max_bytes,
            );
        }
        if request.offset > observed as u64 {
            return Err(AgentError::InvalidArguments);
        }
        let offset = usize::try_from(request.offset).unwrap_or(usize::MAX);
        let retained = content.len();
        if offset > retained {
            return encoded_result(
                ToolOutputPage {
                    base_offset: request.offset,
                    next_offset: observed as u64,
                    eof: true,
                    truncated: true,
                    availability: ToolDataAvailability::Partial,
                    ..base
                },
                max_bytes,
            );
        }
        if !content.is_char_boundary(offset) {
            return Err(AgentError::InvalidArguments);
        }
        let template = ToolOutputPage {
            base_offset: offset as u64,
            next_offset: u64::MAX,
            ..base.clone()
        };
        let template_len = encoded_len(&template);
        if template_len > max_bytes {
            return Err(AgentError::InvalidArguments);
        }
        let available = max_bytes.saturating_sub(template_len).saturating_add(2);
        let (data, _cut) = fit_encoded_string(&content[offset..], available);
        if data.is_empty() && offset < retained {
            return Err(AgentError::InvalidArguments);
        }
        let next = offset.saturating_add(data.len());
        let lossy = retained < observed || truncated;
        let eof = next >= retained;
        encoded_result(
            ToolOutputPage {
                base_offset: offset as u64,
                next_offset: next as u64,
                eof,
                truncated: lossy,
                data,
                ..base
            },
            max_bytes,
        )
    }
}

/// Pages one raw process stream. Offsets are raw byte offsets, `data` is
/// base64 so any byte sequence survives unchanged, and `eof` is true only when
/// the stream really ended at or before the returned `next_offset`.
fn output_process(
    record: &ToolRecord,
    request: &ToolOutputRequest,
    max_bytes: usize,
) -> Result<ToolOutputPage, AgentError> {
    let stream = request.stream;
    let Some(window) = record.stream(stream) else {
        return Err(AgentError::InvalidArguments);
    };
    let availability = record.stream_availability(stream);
    // A page may report an end of output only when the owner's own decision is
    // final for this stream: the owner marked it ended/cut, or it published a
    // terminal command record. A Runtime-terminal tool call is not such a
    // signal, because a dropped tool future can leave the owner still draining.
    let stream_final = window.complete || record.stream_final();
    let base = ToolOutputPage {
        tool_ref: request.tool_ref.clone(),
        stream,
        encoding: STREAM_ENCODING,
        base_offset: window.start_offset,
        next_offset: window.observed_end,
        observed_end: window.observed_end,
        eof: stream_final,
        truncated: window.truncated,
        availability,
        data: String::new(),
    };
    if window.corrupt {
        return encoded_result(
            ToolOutputPage {
                eof: stream_final,
                truncated: true,
                availability: ToolDataAvailability::Unavailable,
                ..base
            },
            max_bytes,
        );
    }
    if request.offset > window.observed_end {
        return Err(AgentError::InvalidArguments);
    }
    if window.expired {
        // The window was freed; the observed end is the only honest anchor.
        return encoded_result(
            ToolOutputPage {
                truncated: true,
                ..base
            },
            max_bytes,
        );
    }
    // A stream the owner has not read a single byte from has no content at all;
    // a non-zero offset is invalid rather than a fabricated loss.
    if !window.seen && request.offset != 0 {
        return Err(AgentError::InvalidArguments);
    }
    let Some((first, second)) = window.page(request.offset) else {
        // The requested bytes were dropped from the retained window: report the
        // loss and where the window starts now so the next page can read the tail.
        let has_retained = !window.bytes.is_empty();
        return encoded_result(
            ToolOutputPage {
                base_offset: window.start_offset,
                next_offset: if has_retained {
                    window.start_offset
                } else {
                    window.observed_end
                },
                eof: stream_final && !has_retained,
                truncated: true,
                availability: ToolDataAvailability::Partial,
                ..base
            },
            max_bytes,
        );
    };
    if first.is_empty() && second.is_empty() {
        // Nothing is retained at or after this offset; the frame reports the
        // real end without inventing content.
        return encoded_result(
            ToolOutputPage {
                base_offset: request.offset,
                next_offset: request.offset,
                eof: stream_final && request.offset >= window.observed_end,
                ..base
            },
            max_bytes,
        );
    }
    let template = ToolOutputPage {
        base_offset: request.offset,
        next_offset: u64::MAX,
        ..base.clone()
    };
    let template_len = encoded_len(&template);
    if template_len > max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    let available = max_bytes.saturating_sub(template_len).saturating_add(2);
    let take = raw_bytes_for_base64(available).min(first.len() + second.len());
    if take == 0 {
        return Err(AgentError::InvalidArguments);
    }
    let bytes = if take <= first.len() {
        std::borrow::Cow::Borrowed(&first[..take])
    } else {
        let mut bytes = Vec::with_capacity(take);
        bytes.extend_from_slice(first);
        bytes.extend_from_slice(&second[..take - first.len()]);
        std::borrow::Cow::Owned(bytes)
    };
    let next = request.offset.saturating_add(take as u64);
    encoded_result(
        ToolOutputPage {
            base_offset: request.offset,
            next_offset: next,
            eof: stream_final && next >= window.observed_end,
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
            ..base
        },
        max_bytes,
    )
}

/// The largest raw byte count whose base64 JSON string fits `available` bytes.
fn raw_bytes_for_base64(available: usize) -> usize {
    available.saturating_sub(2) / 4 * 3
}

/// Largest char-boundary prefix whose JSON string encoding fits `available`
/// bytes, including the surrounding quotes. `encoded_len(prefix(k))` is monotonic
/// in the character count `k`, so a plain binary search over character counts
/// is correct, including a first multibyte character whose boundary is farther
/// than one byte step from the previous one.
pub(super) fn fit_encoded_string(value: &str, available: usize) -> (String, bool) {
    if value.is_empty() || encoded_len("") > available {
        return (String::new(), !value.is_empty());
    }
    if encoded_len(value) <= available {
        return (value.to_owned(), false);
    }
    let char_count = value.chars().count();
    let prefix = |count: usize| -> &str {
        match value.char_indices().nth(count) {
            Some((index, _)) => &value[..index],
            None => value,
        }
    };
    let mut low = 0usize; // empty prefix fits
    let mut high = char_count; // full value does not fit
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if encoded_len(prefix(middle)) <= available {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let end = prefix(low).len();
    (value[..end].to_owned(), low < char_count)
}
