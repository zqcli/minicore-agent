//! Disposable, bounded observation of input generation. Nothing in this module
//! constructs a ToolInvocation, touches a workspace, or accepts an input on
//! behalf of the Runtime assembler. Unknown fields never become display data.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::Stream;
use minicore_runtime::ToolCallId;
use minicore_runtime::model::{ModelError, ModelEvent, ModelStream};
use serde::Serialize;

use super::{MAX_DETAIL_BYTES, Presentation, ToolDisplay, count_lines, single_line};
use crate::event::{AgentEvent, EventMeta};
use crate::sessions::TurnRef;
use crate::tools::observe::RequestKey;

mod parser;
use super::format_read_range;
use parser::PreviewParser;

const MAX_CALLS: usize = 16;
const MAX_CALL_BYTES: usize = 128 * 1024;
const MAX_STREAM_BYTES: usize = 512 * 1024;
const MAX_DEPTH: usize = 64;
const UPDATE_INTERVAL: Duration = Duration::from_nanos(33_333_334);

#[cfg(test)]
static PANIC_PREVIEWS: crate::test_support::Registry<RequestKey> =
    crate::test_support::Registry::new();

/// Input generation status, deliberately independent of tool execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolArgumentsPreviewState {
    /// Arguments are still being generated; no input is validated.
    Generating,
    /// The provider ended this argument stream; validation is still separate.
    Generated,
    /// Discard this provisional display (including after cancellation/drop).
    Discarded,
}

pub(super) struct PreviewStream {
    inner: ModelStream,
    preview: PreviewObserver,
    context_generation: Option<u64>,
    physical_window: u64,
}

impl PreviewStream {
    pub(super) fn new(
        inner: ModelStream,
        presentation: Arc<Presentation>,
        key: RequestKey,
        attempt: Option<u64>,
        context_generation: Option<u64>,
        physical_window: u64,
    ) -> Self {
        Self {
            inner,
            preview: PreviewObserver::new(presentation, key, attempt),
            context_generation,
            physical_window,
        }
    }
}

impl Stream for PreviewStream {
    type Item = Result<ModelEvent, ModelError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let next = this.inner.as_mut().poll_next(cx);
        match &next {
            Poll::Ready(Some(item)) => {
                let (presentation, key) = (&this.preview.presentation, this.preview.key);
                match item {
                    Ok(ModelEvent::Usage { usage }) => presentation.note_request_usage(
                        key,
                        *usage,
                        this.context_generation,
                        this.physical_window,
                    ),
                    Ok(ModelEvent::TextDelta { delta }) => {
                        presentation.note_streamed(key, false, delta.as_str());
                    }
                    Ok(ModelEvent::ReasoningDelta { delta }) => {
                        presentation.note_streamed(key, true, delta.as_str());
                    }
                    _ => {}
                }
                this.preview
                    .protect(|preview| preview.observe(item, Instant::now()));
            }
            Poll::Ready(None) => this.preview.protect(PreviewObserver::eof),
            Poll::Pending => {}
        }
        // Return the very same item, including errors and malformed ordering.
        next
    }
}

struct PreviewObserver {
    presentation: Arc<Presentation>,
    key: RequestKey,
    attempt: Option<u64>,
    calls: Vec<PreviewCall>,
    retained_bytes: usize,
    last_update: Option<Instant>,
    saw_finish: bool,
    normal_eof: bool,
    discarded: bool,
}

struct PreviewCall {
    id: ToolCallId,
    name: String,
    parser: PreviewParser,
    ended: bool,
    revision: u64,
    emitted_change: u64,
    first_path_sent: bool,
}

impl PreviewObserver {
    fn protect(&mut self, update: impl FnOnce(&mut Self)) {
        if self.discarded {
            return;
        }
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            #[cfg(test)]
            tests::panic_preview(self.key);
            update(self);
        }))
        .is_err()
        {
            // Preview failures must not reach Runtime's model-stream boundary.
            // Clients also clear orphaned previews at authoritative turn termination.
            self.discarded = true;
            self.calls.clear();
            self.retained_bytes = 0;
        }
    }

    fn new(presentation: Arc<Presentation>, key: RequestKey, attempt: Option<u64>) -> Self {
        Self {
            presentation,
            key,
            attempt,
            calls: Vec::new(),
            retained_bytes: 0,
            last_update: None,
            saw_finish: false,
            normal_eof: false,
            discarded: false,
        }
    }

    fn observe(&mut self, item: &Result<ModelEvent, ModelError>, now: Instant) {
        if self.attempt.is_none() || self.discarded {
            return;
        }
        if self.saw_finish || item.is_err() {
            self.discard();
            return;
        }
        match item {
            Ok(ModelEvent::ToolCallStart {
                tool_call_id,
                tool_name,
            }) => {
                if self.calls.iter().any(|call| &call.id == tool_call_id) {
                    self.discard();
                    return;
                }
                let name = tool_name.as_str();
                if !matches!(name, "read" | "write" | "edit") || self.calls.len() >= MAX_CALLS {
                    return;
                }
                self.calls.push(PreviewCall {
                    id: tool_call_id.clone(),
                    name: name.to_owned(),
                    parser: PreviewParser::new(name),
                    ended: false,
                    revision: 0,
                    emitted_change: 0,
                    first_path_sent: false,
                });
                self.emit(
                    self.calls.len() - 1,
                    ToolArgumentsPreviewState::Generating,
                    now,
                );
            }
            Ok(ModelEvent::ToolCallArgumentsDelta {
                tool_call_id,
                delta,
            }) => {
                let Some(index) = self.calls.iter().position(|call| &call.id == tool_call_id)
                else {
                    return;
                };
                let call = &mut self.calls[index];
                if call.ended {
                    self.discard();
                    return;
                }
                call.parser.feed(delta.as_str(), &mut self.retained_bytes);
                let first_path = !call.first_path_sent
                    && call
                        .parser
                        .path
                        .as_ref()
                        .is_some_and(|path| !path.is_empty());
                let due = self.last_update.is_none_or(|last| {
                    now.saturating_duration_since(last) >= call.parser.update_interval()
                });
                if first_path || (due && call.parser.changes != call.emitted_change) {
                    call.first_path_sent |= first_path;
                    self.emit(index, ToolArgumentsPreviewState::Generating, now);
                }
            }
            Ok(ModelEvent::ToolCallEnd { tool_call_id }) => {
                if let Some(index) = self.calls.iter().position(|call| &call.id == tool_call_id) {
                    if self.calls[index].ended {
                        self.discard();
                        return;
                    }
                    self.calls[index].ended = true;
                    // Forced final replacement snapshot, even if no delta was
                    // due or all preceding snapshots were dropped.
                    self.emit(index, ToolArgumentsPreviewState::Generated, now);
                }
            }
            Ok(ModelEvent::Finish { .. }) => self.saw_finish = true,
            _ => {}
        }
    }

    fn emit(&mut self, index: usize, state: ToolArgumentsPreviewState, now: Instant) {
        let call = &mut self.calls[index];
        call.revision += 1;
        call.emitted_change = call.parser.changes;
        let (display, partial) = if state == ToolArgumentsPreviewState::Discarded {
            (empty_display(), true)
        } else {
            (
                call.parser.display(),
                call.parser.invalid
                    || call.parser.truncated()
                    || (call.ended && !call.parser.complete()),
            )
        };
        let turn = TurnRef {
            session_id: self.presentation.session_id,
            loop_id: self.key.loop_id,
        };
        let _ = self
            .presentation
            .events
            .try_send(AgentEvent::ToolArgumentsPreview {
                turn,
                request_index: self.key.request_index,
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                attempt: self.attempt.unwrap_or(0),
                revision: call.revision,
                state,
                partial,
                display,
                meta: EventMeta::for_turn(turn),
            });
        self.last_update = Some(now);
    }

    fn eof(&mut self) {
        if self.normal_eof {
            return;
        }
        if self.saw_finish && !self.discarded {
            self.normal_eof = true;
        } else {
            self.discard();
        }
    }

    fn discard(&mut self) {
        if self.discarded {
            return;
        }
        self.discarded = true;
        for index in 0..self.calls.len() {
            self.emit(index, ToolArgumentsPreviewState::Discarded, Instant::now());
        }
    }
}

impl Drop for PreviewObserver {
    fn drop(&mut self) {
        if !self.normal_eof {
            self.protect(Self::discard);
        }
    }
}

fn empty_display() -> ToolDisplay {
    ToolDisplay {
        detail: "...".to_owned(),
        expanded_input: None,
        input_line_count: None,
        hidden_line_count: None,
        truncated: false,
        body_truncated: false,
    }
}

#[cfg(test)]
mod tests;
