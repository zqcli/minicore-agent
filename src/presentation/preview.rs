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

const MAX_CALLS: usize = 16;
const MAX_CALL_BYTES: usize = 128 * 1024;
const MAX_STREAM_BYTES: usize = 512 * 1024;
const MAX_DEPTH: usize = 64;
const UPDATE_INTERVAL: Duration = Duration::from_nanos(33_333_334);

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
                if let Ok(ModelEvent::Usage { usage }) = item {
                    this.preview.presentation.note_request_usage(
                        this.preview.key,
                        *usage,
                        this.context_generation,
                        this.physical_window,
                    );
                }
                this.preview.observe(item, Instant::now());
            }
            Poll::Ready(None) => this.preview.eof(),
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
                let due = self
                    .last_update
                    .is_none_or(|last| now.saturating_duration_since(last) >= UPDATE_INTERVAL);
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
        let _ = self
            .presentation
            .events
            .try_send(AgentEvent::ToolArgumentsPreview {
                turn: TurnRef {
                    session_id: self.presentation.session_id,
                    loop_id: self.key.loop_id,
                },
                request_index: self.key.request_index,
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                attempt: self.attempt.unwrap_or(0),
                revision: call.revision,
                state,
                partial,
                display,
                meta: EventMeta {
                    session_id: self.presentation.session_id,
                    loop_id: Some(self.key.loop_id),
                    dropped_before: 0,
                },
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
            self.discard();
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum Field {
    Ignore,
    Path,
    Content,
    Offset,
    Limit,
}
impl Field {
    fn bit(self) -> u8 {
        match self {
            Self::Ignore => 0,
            Self::Path => 1,
            Self::Content => 2,
            Self::Offset => 4,
            Self::Limit => 8,
        }
    }
}
#[derive(Clone, Copy)]
enum Frame {
    Object(ObjectState),
    Array(ArrayState),
}
#[derive(Clone, Copy)]
enum ObjectState {
    KeyOrEnd,
    Key,
    Colon,
    Value,
    CommaOrEnd,
}
#[derive(Clone, Copy)]
enum ArrayState {
    ValueOrEnd,
    Value,
    CommaOrEnd,
}
#[derive(Clone, Copy)]
enum Capture {
    Key,
    Ignore,
    Field(Field),
}
#[derive(Clone, Copy)]
enum Escape {
    None,
    Slash,
    Unicode(u8, u16),
    LowSlash(u16),
    LowU(u16),
    LowUnicode(u16, u8, u16),
}
#[derive(Clone, Copy)]
enum NumberState {
    Minus,
    Zero,
    Integer,
    Dot,
    Fraction,
    Exp,
    ExpSign,
    ExpDigits,
}
impl NumberState {
    fn complete(self) -> bool {
        matches!(
            self,
            Self::Zero | Self::Integer | Self::Fraction | Self::ExpDigits
        )
    }
}
enum Token {
    String {
        capture: Capture,
        escape: Escape,
    },
    Number {
        state: NumberState,
        field: Field,
        digits: String,
    },
    Literal {
        expected: &'static str,
        index: usize,
    },
}

/// Small streaming JSON recognizer, not an input validator. Each input scalar
/// is visited once (number terminators twice); irrelevant values are scanned
/// without retaining their bytes. Keys and nesting have fixed bounds. Only
/// display-whitelisted fields are captured, after JSON escape decoding.
/// Deliberately no Debug: this state can hold user/model source text.
struct PreviewParser {
    write: bool,
    read: bool,
    started: bool,
    done: bool,
    stack: Vec<Frame>,
    token: Option<Token>,
    key: String,
    key_overflow: bool,
    field: Field,
    seen: u8,
    path: Option<String>,
    content: Option<String>,
    offset: Option<u64>,
    limit: Option<u64>,
    retained: usize,
    path_cut: bool,
    body_cut: bool,
    invalid: bool,
    changes: u64,
    #[cfg(test)]
    scanned: usize,
}

impl PreviewParser {
    fn new(name: &str) -> Self {
        Self {
            write: name == "write",
            read: name == "read",
            started: false,
            done: false,
            stack: Vec::new(),
            token: None,
            key: String::new(),
            key_overflow: false,
            field: Field::Ignore,
            seen: 0,
            path: None,
            content: None,
            offset: None,
            limit: None,
            retained: 0,
            path_cut: false,
            body_cut: false,
            invalid: false,
            changes: 0,
            #[cfg(test)]
            scanned: 0,
        }
    }

    fn complete(&self) -> bool {
        self.done && !self.invalid
    }
    fn truncated(&self) -> bool {
        self.path_cut || self.body_cut
    }

    fn fail(&mut self, stream_bytes: &mut usize) {
        if self.invalid {
            return;
        }
        self.invalid = true;
        self.changes += 1;
        *stream_bytes = stream_bytes.saturating_sub(self.retained);
        self.retained = 0;
        self.path = None;
        self.content = None;
        self.offset = None;
        self.limit = None;
        self.token = None;
        self.key.clear();
        self.stack.clear();
    }

    fn feed(&mut self, delta: &str, stream_bytes: &mut usize) {
        for ch in delta.chars() {
            if self.invalid {
                break;
            }
            #[cfg(test)]
            {
                self.scanned += 1;
            }
            if let Some(token) = self.token.take() {
                if self.token_char(token, ch, stream_bytes) {
                    continue;
                }
                if self.invalid {
                    break;
                }
            }
            self.structural(ch, stream_bytes);
        }
    }

    /// Returns false only for a number's unconsumed terminating delimiter.
    fn token_char(&mut self, token: Token, ch: char, bytes: &mut usize) -> bool {
        match token {
            Token::String { capture, escape } => {
                let mut decoded = None;
                let next = match escape {
                    Escape::None => match ch {
                        '"' => {
                            if matches!(capture, Capture::Key) {
                                self.finish_key(bytes);
                            } else if matches!(
                                self.stack.last(),
                                Some(Frame::Object(ObjectState::KeyOrEnd | ObjectState::Key))
                            ) {
                                *self.stack.last_mut().unwrap() = Frame::Object(ObjectState::Colon);
                            }
                            return true;
                        }
                        '\\' => Escape::Slash,
                        c if c < '\u{20}' => {
                            self.fail(bytes);
                            return true;
                        }
                        c => {
                            decoded = Some(c);
                            Escape::None
                        }
                    },
                    Escape::Slash => match ch {
                        '"' | '\\' | '/' => {
                            decoded = Some(ch);
                            Escape::None
                        }
                        'b' => {
                            decoded = Some('\u{8}');
                            Escape::None
                        }
                        'f' => {
                            decoded = Some('\u{c}');
                            Escape::None
                        }
                        'n' => {
                            decoded = Some('\n');
                            Escape::None
                        }
                        'r' => {
                            decoded = Some('\r');
                            Escape::None
                        }
                        't' => {
                            decoded = Some('\t');
                            Escape::None
                        }
                        'u' => Escape::Unicode(0, 0),
                        _ => {
                            self.fail(bytes);
                            return true;
                        }
                    },
                    Escape::Unicode(digits, value) => {
                        let Some(hex) = ch.to_digit(16) else {
                            self.fail(bytes);
                            return true;
                        };
                        let value = (value << 4) | hex as u16;
                        if digits < 3 {
                            Escape::Unicode(digits + 1, value)
                        } else if (0xd800..=0xdbff).contains(&value) {
                            Escape::LowSlash(value)
                        } else if (0xdc00..=0xdfff).contains(&value) {
                            self.fail(bytes);
                            return true;
                        } else {
                            decoded = char::from_u32(u32::from(value));
                            Escape::None
                        }
                    }
                    Escape::LowSlash(high) if ch == '\\' => Escape::LowU(high),
                    Escape::LowU(high) if ch == 'u' => Escape::LowUnicode(high, 0, 0),
                    Escape::LowUnicode(high, digits, value) => {
                        let Some(hex) = ch.to_digit(16) else {
                            self.fail(bytes);
                            return true;
                        };
                        let value = (value << 4) | hex as u16;
                        if digits < 3 {
                            Escape::LowUnicode(high, digits + 1, value)
                        } else if (0xdc00..=0xdfff).contains(&value) {
                            decoded = char::from_u32(
                                0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(value)
                                    - 0xdc00,
                            );
                            Escape::None
                        } else {
                            self.fail(bytes);
                            return true;
                        }
                    }
                    _ => {
                        self.fail(bytes);
                        return true;
                    }
                };
                if let Some(ch) = decoded {
                    self.capture(capture, ch, bytes);
                }
                self.token = Some(Token::String {
                    capture,
                    escape: next,
                });
                true
            }
            Token::Literal { expected, index } => {
                if expected.as_bytes().get(index).copied().map(char::from) != Some(ch) {
                    self.fail(bytes);
                } else if index + 1 < expected.len() {
                    self.token = Some(Token::Literal {
                        expected,
                        index: index + 1,
                    });
                }
                true
            }
            Token::Number {
                state,
                field,
                mut digits,
            } => {
                use NumberState::*;
                let next = match (state, ch) {
                    (Minus, '0') => Some(Zero),
                    (Minus, '1'..='9') => Some(Integer),
                    (Integer, '0'..='9') => Some(Integer),
                    (Zero | Integer, '.') => Some(Dot),
                    (Zero | Integer | Fraction, 'e' | 'E') => Some(Exp),
                    (Dot | Fraction, '0'..='9') => Some(Fraction),
                    (Exp, '+' | '-') => Some(ExpSign),
                    (Exp | ExpSign | ExpDigits, '0'..='9') => Some(ExpDigits),
                    _ => None,
                };
                if let Some(state) = next {
                    if field != Field::Ignore {
                        if digits.len() >= 20 {
                            self.fail(bytes);
                            return true;
                        }
                        digits.push(ch);
                    }
                    self.token = Some(Token::Number {
                        state,
                        field,
                        digits,
                    });
                    true
                } else if state.complete() && (json_space(ch) || matches!(ch, ',' | ']' | '}')) {
                    if field != Field::Ignore {
                        match digits.parse::<u64>() {
                            Ok(value) if value > 0 => {
                                if field == Field::Offset {
                                    self.offset = Some(value);
                                } else {
                                    self.limit = Some(value);
                                }
                                self.changes += 1;
                            }
                            _ => self.fail(bytes),
                        }
                    }
                    false
                } else {
                    self.fail(bytes);
                    true
                }
            }
        }
    }

    fn capture(&mut self, capture: Capture, ch: char, stream_bytes: &mut usize) {
        match capture {
            Capture::Key => {
                if self.key.len() + ch.len_utf8() <= 32 && !self.key_overflow {
                    self.key.push(ch);
                } else {
                    self.key_overflow = true;
                }
            }
            Capture::Field(field @ (Field::Path | Field::Content)) => {
                let body = field == Field::Content;
                if (body && self.body_cut) || (!body && self.path_cut) {
                    return;
                }
                // Sanitize as we retain, so display bytes (including escaped
                // controls) are covered by the same aggregate memory budget.
                let mut utf8 = [0u8; 4];
                let escaped;
                let value: &str = if !body && matches!(ch, '\r' | '\n' | '\t') {
                    " "
                } else if ch.is_control() && !(body && matches!(ch, '\n' | '\t')) {
                    escaped = ch.escape_default().to_string();
                    &escaped
                } else {
                    ch.encode_utf8(&mut utf8)
                };
                let field_len = if body {
                    self.content.as_ref().map_or(0, String::len)
                } else {
                    self.path.as_ref().map_or(0, String::len)
                };
                let field_cap = if body {
                    MAX_CALL_BYTES
                } else {
                    MAX_DETAIL_BYTES
                };
                if field_len + value.len() > field_cap
                    || self.retained + value.len() > MAX_CALL_BYTES
                    || *stream_bytes + value.len() > MAX_STREAM_BYTES
                {
                    if body {
                        self.body_cut = true;
                    } else {
                        self.path_cut = true;
                    }
                    self.changes += 1;
                    return;
                }
                if body {
                    self.content.as_mut().unwrap().push_str(value);
                } else {
                    self.path.as_mut().unwrap().push_str(value);
                }
                self.retained += value.len();
                *stream_bytes += value.len();
                self.changes += 1;
            }
            _ => {}
        }
    }

    fn finish_key(&mut self, bytes: &mut usize) {
        self.field = if self.key_overflow {
            Field::Ignore
        } else {
            match self.key.as_str() {
                "path" | "file_path" => Field::Path,
                "content" if self.write => Field::Content,
                "offset" if self.read => Field::Offset,
                "limit" if self.read => Field::Limit,
                _ => Field::Ignore,
            }
        };
        if self.seen & self.field.bit() != 0 {
            self.fail(bytes);
            return;
        }
        self.seen |= self.field.bit();
        self.key.clear();
        *self.stack.last_mut().unwrap() = Frame::Object(ObjectState::Colon);
    }

    fn structural(&mut self, ch: char, bytes: &mut usize) {
        if json_space(ch) {
            return;
        }
        if !self.started {
            self.started = true;
            if ch != '{' {
                self.fail(bytes);
                return;
            }
            self.stack.push(Frame::Object(ObjectState::KeyOrEnd));
            return;
        }
        if self.done {
            self.fail(bytes);
            return;
        }
        match self.stack.last().copied() {
            Some(Frame::Object(ObjectState::KeyOrEnd | ObjectState::Key)) => {
                if ch == '}'
                    && matches!(
                        self.stack.last(),
                        Some(Frame::Object(ObjectState::KeyOrEnd))
                    )
                {
                    self.close();
                } else if ch == '"' {
                    self.key.clear();
                    self.key_overflow = false;
                    self.token = Some(Token::String {
                        capture: if self.stack.len() == 1 {
                            Capture::Key
                        } else {
                            Capture::Ignore
                        },
                        escape: Escape::None,
                    });
                } else {
                    self.fail(bytes);
                }
            }
            Some(Frame::Object(ObjectState::Colon)) => {
                if ch == ':' {
                    *self.stack.last_mut().unwrap() = Frame::Object(ObjectState::Value);
                } else {
                    self.fail(bytes);
                }
            }
            Some(Frame::Object(ObjectState::CommaOrEnd)) => match ch {
                ',' => *self.stack.last_mut().unwrap() = Frame::Object(ObjectState::Key),
                '}' => self.close(),
                _ => self.fail(bytes),
            },
            Some(Frame::Array(ArrayState::CommaOrEnd)) => match ch {
                ',' => *self.stack.last_mut().unwrap() = Frame::Array(ArrayState::Value),
                ']' => self.close(),
                _ => self.fail(bytes),
            },
            Some(Frame::Array(ArrayState::ValueOrEnd)) if ch == ']' => self.close(),
            Some(
                Frame::Object(ObjectState::Value)
                | Frame::Array(ArrayState::ValueOrEnd | ArrayState::Value),
            ) => {
                let field = if self.stack.len() == 1 {
                    self.field
                } else {
                    Field::Ignore
                };
                if (matches!(field, Field::Path | Field::Content) && ch != '"')
                    || (matches!(field, Field::Offset | Field::Limit)
                        && !ch.is_ascii_digit()
                        && ch != '-')
                {
                    self.fail(bytes);
                    return;
                }
                let parent = self.stack.last_mut().unwrap();
                *parent = match parent {
                    Frame::Object(_) => Frame::Object(ObjectState::CommaOrEnd),
                    Frame::Array(_) => Frame::Array(ArrayState::CommaOrEnd),
                };
                match ch {
                    '{' | '[' => {
                        if self.stack.len() >= MAX_DEPTH {
                            self.fail(bytes);
                            return;
                        }
                        self.stack.push(if ch == '{' {
                            Frame::Object(ObjectState::KeyOrEnd)
                        } else {
                            Frame::Array(ArrayState::ValueOrEnd)
                        });
                    }
                    '"' => {
                        if field == Field::Path {
                            self.path = Some(String::new());
                            self.changes += 1;
                        } else if field == Field::Content {
                            self.content = Some(String::new());
                            self.changes += 1;
                        }
                        self.token = Some(Token::String {
                            capture: Capture::Field(field),
                            escape: Escape::None,
                        });
                    }
                    '-' | '0'..='9' => {
                        self.token = Some(Token::Number {
                            state: match ch {
                                '-' => NumberState::Minus,
                                '0' => NumberState::Zero,
                                _ => NumberState::Integer,
                            },
                            field,
                            digits: if field == Field::Ignore {
                                String::new()
                            } else {
                                ch.to_string()
                            },
                        })
                    }
                    't' => {
                        self.token = Some(Token::Literal {
                            expected: "true",
                            index: 1,
                        })
                    }
                    'f' => {
                        self.token = Some(Token::Literal {
                            expected: "false",
                            index: 1,
                        })
                    }
                    'n' => {
                        self.token = Some(Token::Literal {
                            expected: "null",
                            index: 1,
                        })
                    }
                    _ => self.fail(bytes),
                }
            }
            None => self.fail(bytes),
        }
    }

    fn close(&mut self) {
        self.stack.pop();
        if self.stack.is_empty() {
            self.done = true;
        }
    }

    fn display(&self) -> ToolDisplay {
        let mut detail = self.path.clone().unwrap_or_else(|| "...".to_owned());
        if self.offset.is_some() || self.limit.is_some() {
            let offset = self.offset.unwrap_or(1);
            detail.push_str(&format!(":{offset}"));
            if let Some(limit) = self.limit {
                detail.push_str(&format!(
                    "-{}",
                    offset.saturating_add(limit).saturating_sub(1)
                ));
            }
        }
        let (detail, detail_cut) = single_line(&detail, MAX_DETAIL_BYTES);
        let rows = self.content.as_deref().map(count_lines);
        ToolDisplay {
            detail,
            expanded_input: self.content.clone(),
            input_line_count: rows,
            hidden_line_count: rows.filter(|count| *count > 0),
            truncated: self.truncated() || detail_cut,
            body_truncated: self.body_cut,
        }
    }
}

fn json_space(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\r' | '\n')
}

#[cfg(test)]
mod tests;
