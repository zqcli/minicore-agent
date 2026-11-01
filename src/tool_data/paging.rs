//! Byte-bounded tool reads and raw-output pages.
use super::read::{ToolBlob, ToolSummary};
use super::record::{ToolRecord, stream_availability};
use super::types::{
    ToolDataAvailability, ToolDataStream, ToolInputSummary, ToolInvocationData, ToolOutputPage,
    ToolOutputRequest, ToolReadResult,
};
use super::{INPUT_ENCODING, MAX_INVOCATION_PREVIEW_BYTES};
use crate::budget::{encoded_len, encoded_result, json_string_prefix};
use crate::error::AgentError;
use crate::store::StoredToolRecord;
use crate::text::truncate;
use base64::Engine as _;

impl ToolSummary {
    pub(crate) fn project_read(&self, max_bytes: usize) -> Result<ToolReadResult, AgentError> {
        let execution = self.metadata.execution.clone();
        let invocation = (self.metadata.input_seen
            || self.metadata.execution.input_availability == ToolDataAvailability::Expired)
            .then(|| {
                let (preview, cut) = truncate(&self.input, MAX_INVOCATION_PREVIEW_BYTES);
                ToolInvocationData {
                    tool_ref: self.metadata.execution.tool_ref.clone(),
                    name: self.metadata.execution.name.clone(),
                    subject: self.subject.clone(),
                    subject_truncated: self.subject_truncated,
                    input: ToolInputSummary {
                        total_bytes: self.metadata.execution.input_bytes,
                        preview: preview.to_owned(),
                        truncated: cut
                            || self.metadata.execution.input_truncated
                            || (self.metadata.execution.input_availability
                                == ToolDataAvailability::Expired)
                            || (self.metadata.execution.input_availability
                                == ToolDataAvailability::Unavailable),
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
            let preview = json_string_prefix(&source, available).to_owned();
            if let Some(invocation) = result.invocation.as_mut() {
                invocation.input.preview = preview;
                invocation.input.truncated = true;
            }
        }
        Ok(result)
    }

    pub(crate) fn project_display_read(
        &self,
        max_bytes: usize,
    ) -> Result<ToolReadResult, AgentError> {
        let arguments = (!self.metadata.execution.input_truncated
            && !(self.metadata.execution.input_availability == ToolDataAvailability::Expired)
            && !(self.metadata.execution.input_availability == ToolDataAvailability::Unavailable))
            .then(|| serde_json::from_str(&self.input).ok())
            .flatten();
        let mut display = crate::presentation::build_tool_card_display(
            &self.metadata.execution.name,
            arguments.as_ref(),
        );
        if arguments.is_none()
            && matches!(
                self.metadata.execution.name.as_str(),
                "write" | "edit" | "patch" | "apply_patch" | "bash"
            )
        {
            display.body_truncated = true;
        }
        let mut result = ToolReadResult {
            display: Some(display),
            invocation: None,
            execution: self.metadata.execution.clone(),
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
            let body = json_string_prefix(&source, available).to_owned();
            let display = result.display.as_mut().ok_or(AgentError::Internal)?;
            display.input_line_count = Some(crate::presentation::count_lines(&body));
            display.hidden_line_count = display.input_line_count;
            display.expanded_input = Some(body);
        }
        Ok(result)
    }
}

/// A borrowed view of one observed byte channel.
pub(super) struct ToolOutputView<'a> {
    bytes: [&'a [u8]; 2],
    base: u64,
    observed: u64,
    seen: bool,
    known: bool,
    complete: bool,
    truncated: bool,
    pub(super) availability: ToolDataAvailability,
    pub(super) corrupt: bool,
}

impl ToolRecord {
    pub(super) fn output_view(&self, stream: ToolDataStream) -> ToolOutputView<'_> {
        if let Some(window) = self.stream(stream) {
            let (first, second) = window.bytes.as_slices();
            return ToolOutputView {
                bytes: [first, second],
                base: window.start_offset,
                observed: window.observed_end,
                seen: window.seen,
                known: window.seen || window.complete,
                complete: window.complete || self.stream_final(),
                truncated: window.truncated,
                availability: self.stream_availability(stream),
                corrupt: window.corrupt,
            };
        }
        let (text, observed, seen, expired, truncated, corrupt) = match stream {
            ToolDataStream::Input => (
                &self.input,
                self.input_total,
                self.input_seen,
                self.input_expired,
                self.input_truncated,
                self.input_corrupt,
            ),
            ToolDataStream::Output => (
                &self.result,
                self.result_total,
                self.result_seen,
                self.result_expired,
                self.result_truncated,
                self.result_corrupt,
            ),
            _ => unreachable!(),
        };
        ToolOutputView {
            bytes: [text.as_bytes(), &[]],
            base: 0,
            observed: observed as u64,
            seen,
            known: seen,
            complete: self.state.is_terminal(),
            truncated,
            availability: if corrupt {
                ToolDataAvailability::Unavailable
            } else {
                stream_availability(
                    seen,
                    expired,
                    self.state.is_terminal(),
                    text.len(),
                    observed,
                    truncated,
                )
            },
            corrupt,
        }
    }
}

impl<'a> ToolOutputView<'a> {
    pub(super) fn from_stored(
        record: &StoredToolRecord,
        stream: ToolDataStream,
        blob: &'a ToolBlob,
    ) -> Self {
        let (bytes, corrupt) = blob;
        let bytes = bytes.as_deref().unwrap_or_default();
        let terminal = record.command.as_ref().map_or_else(
            || record.state.is_terminal(),
            |command| command.status.is_terminal(),
        );
        if stream.is_process() {
            let window = if stream == ToolDataStream::Stdout {
                &record.stdout
            } else {
                &record.stderr
            };
            let availability = if *corrupt {
                ToolDataAvailability::Unavailable
            } else if window.expired {
                ToolDataAvailability::Expired
            } else if window.truncated {
                ToolDataAvailability::Partial
            } else if window.seen || window.complete {
                ToolDataAvailability::Available
            } else if terminal {
                ToolDataAvailability::Unavailable
            } else {
                ToolDataAvailability::Pending
            };
            Self {
                bytes: [bytes, &[]],
                base: window.start_offset,
                observed: window.observed_end,
                seen: window.seen,
                known: window.seen || window.complete,
                complete: window.complete || terminal,
                truncated: window.truncated,
                availability,
                corrupt: *corrupt,
            }
        } else {
            let (seen, expired, total, retained, truncated) = match stream {
                ToolDataStream::Input => (
                    record.input.seen,
                    record.input.expired,
                    record.input.total_bytes,
                    record.input.file_bytes,
                    record.input.truncated,
                ),
                ToolDataStream::Output => (
                    record.result.seen,
                    record.result.expired,
                    record.result.total_bytes,
                    record.result.file_bytes,
                    record.result.truncated,
                ),
                _ => unreachable!(),
            };
            let corrupt = *corrupt || std::str::from_utf8(bytes).is_err();
            Self {
                bytes: [bytes, &[]],
                base: 0,
                observed: total as u64,
                seen,
                known: seen,
                complete: record.state.is_terminal(),
                truncated,
                availability: if corrupt {
                    ToolDataAvailability::Unavailable
                } else {
                    stream_availability(
                        seen,
                        expired,
                        record.state.is_terminal(),
                        retained,
                        total,
                        truncated,
                    )
                },
                corrupt,
            }
        }
    }

    pub(super) fn known(&self) -> bool {
        self.known
    }

    pub(super) fn project(
        &self,
        request: &ToolOutputRequest,
        max_bytes: usize,
    ) -> Result<ToolOutputPage, AgentError> {
        let process = request.stream.is_process();
        let retained = self.bytes[0].len() + self.bytes[1].len();
        let end = self.base + retained as u64;
        let base = ToolOutputPage {
            tool_ref: request.tool_ref.clone(),
            stream: request.stream,
            encoding: request.stream.encoding(),
            base_offset: self.base,
            next_offset: self.observed,
            observed_end: self.observed,
            eof: self.complete,
            truncated: self.truncated || (!process && end < self.observed),
            availability: self.availability,
            data: String::new(),
        };
        if self.corrupt || self.availability == ToolDataAvailability::Expired {
            if process && !self.corrupt && request.offset > self.observed {
                return Err(AgentError::InvalidArguments);
            }
            return encoded_result(
                ToolOutputPage {
                    base_offset: if self.corrupt {
                        self.base
                    } else {
                        self.observed
                    },
                    eof: self.complete || !process,
                    truncated: true,
                    ..base
                },
                max_bytes,
            );
        }
        if request.offset > self.observed || (!self.seen && request.offset != 0) {
            return Err(AgentError::InvalidArguments);
        }
        if request.offset < self.base {
            return encoded_result(
                ToolOutputPage {
                    next_offset: if retained == 0 {
                        self.observed
                    } else {
                        self.base
                    },
                    eof: self.complete && retained == 0,
                    truncated: true,
                    availability: ToolDataAvailability::Partial,
                    ..base
                },
                max_bytes,
            );
        }
        if request.offset > end {
            return encoded_result(
                ToolOutputPage {
                    base_offset: request.offset,
                    eof: true,
                    truncated: true,
                    availability: ToolDataAvailability::Partial,
                    ..base
                },
                max_bytes,
            );
        }
        let offset = (request.offset - self.base) as usize;
        let (first, second) = if offset < self.bytes[0].len() {
            (&self.bytes[0][offset..], self.bytes[1])
        } else {
            (&self.bytes[1][offset - self.bytes[0].len()..], &[][..])
        };
        if first.is_empty() && second.is_empty() {
            return encoded_result(
                ToolOutputPage {
                    base_offset: request.offset,
                    next_offset: request.offset,
                    eof: if process {
                        self.complete && request.offset >= self.observed
                    } else {
                        self.availability != ToolDataAvailability::Pending
                    },
                    ..base
                },
                max_bytes,
            );
        }
        let template = ToolOutputPage {
            base_offset: request.offset,
            next_offset: u64::MAX,
            eof: false,
            ..base.clone()
        };
        let envelope = encoded_len(&template);
        if envelope > max_bytes {
            return Err(AgentError::InvalidArguments);
        }
        let available = max_bytes - envelope + 2;
        let (data, take) = if process {
            let take = (available.saturating_sub(2) / 4 * 3).min(first.len() + second.len());
            let bytes = if take <= first.len() {
                std::borrow::Cow::Borrowed(&first[..take])
            } else {
                let mut bytes = Vec::with_capacity(take);
                bytes.extend_from_slice(first);
                bytes.extend_from_slice(&second[..take - first.len()]);
                std::borrow::Cow::Owned(bytes)
            };
            (
                base64::engine::general_purpose::STANDARD.encode(bytes),
                take,
            )
        } else {
            let text = std::str::from_utf8(first).map_err(|_| AgentError::InvalidArguments)?;
            let prefix = json_string_prefix(text, available);
            (prefix.to_owned(), prefix.len())
        };
        if take == 0 {
            return Err(AgentError::InvalidArguments);
        }
        let next = request.offset + take as u64;
        encoded_result(
            ToolOutputPage {
                base_offset: request.offset,
                next_offset: next,
                data,
                eof: if process {
                    self.complete && next >= self.observed
                } else {
                    next >= end
                },
                ..base
            },
            max_bytes,
        )
    }
}
