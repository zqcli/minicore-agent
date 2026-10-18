use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Field {
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
pub(super) enum Frame {
    Object(ObjectState),
    Array(ArrayState),
}
#[derive(Clone, Copy)]
pub(super) enum ObjectState {
    KeyOrEnd,
    Key,
    Colon,
    Value,
    CommaOrEnd,
}
#[derive(Clone, Copy)]
pub(super) enum ArrayState {
    ValueOrEnd,
    Value,
    CommaOrEnd,
}
#[derive(Clone, Copy)]
pub(super) enum Capture {
    Key,
    Ignore,
    Field(Field),
}
#[derive(Clone, Copy)]
pub(super) enum Escape {
    None,
    Slash,
    Unicode(u8, u16),
    LowSlash(u16),
    LowU(u16),
    LowUnicode(u16, u8, u16),
}

impl Escape {
    fn decode(self, ch: char) -> Option<(Self, Option<char>)> {
        let decoded = match self {
            Self::None => match ch {
                '\\' => return Some((Self::Slash, None)),
                c if c < '\u{20}' => return None,
                c => c,
            },
            Self::Slash => match ch {
                'u' => return Some((Self::Unicode(0, 0), None)),
                '"' | '\\' | '/' => ch,
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                _ => return None,
            },
            Self::LowSlash(high) if ch == '\\' => return Some((Self::LowU(high), None)),
            Self::LowU(high) if ch == 'u' => return Some((Self::LowUnicode(high, 0, 0), None)),
            Self::Unicode(digits, value) | Self::LowUnicode(_, digits, value) => {
                let high = match self {
                    Self::LowUnicode(high, _, _) => Some(high),
                    _ => None,
                };
                let value = (value << 4) | ch.to_digit(16)? as u16;
                if digits < 3 {
                    return Some((
                        match high {
                            Some(high) => Self::LowUnicode(high, digits + 1, value),
                            None => Self::Unicode(digits + 1, value),
                        },
                        None,
                    ));
                }
                match (high, value) {
                    (Some(high), 0xdc00..=0xdfff) => char::from_u32(
                        0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(value) - 0xdc00,
                    )?,
                    (Some(_), _) | (None, 0xdc00..=0xdfff) => return None,
                    (None, 0xd800..=0xdbff) => return Some((Self::LowSlash(value), None)),
                    (None, _) => char::from_u32(u32::from(value))?,
                }
            }
            _ => return None,
        };
        Some((Self::None, Some(decoded)))
    }
}
#[derive(Clone, Copy)]
pub(super) enum NumberState {
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
    pub(super) fn complete(self) -> bool {
        matches!(
            self,
            Self::Zero | Self::Integer | Self::Fraction | Self::ExpDigits
        )
    }
}
pub(super) enum Token {
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
pub(super) struct PreviewParser {
    pub(super) write: bool,
    pub(super) read: bool,
    pub(super) started: bool,
    pub(super) done: bool,
    pub(super) stack: Vec<Frame>,
    pub(super) token: Option<Token>,
    pub(super) key: String,
    pub(super) key_overflow: bool,
    pub(super) field: Field,
    pub(super) seen: u8,
    pub(super) path: Option<String>,
    pub(super) content: Option<String>,
    pub(super) offset: Option<u64>,
    pub(super) limit: Option<u64>,
    /// Actual capacity charged for path/content buffers, including spare bytes.
    pub(super) retained: usize,
    pub(super) path_cut: bool,
    pub(super) body_cut: bool,
    pub(super) invalid: bool,
    pub(super) changes: u64,
}

impl PreviewParser {
    pub(super) fn new(name: &str) -> Self {
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
        }
    }

    pub(super) fn complete(&self) -> bool {
        self.done && !self.invalid
    }
    pub(super) fn truncated(&self) -> bool {
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

    pub(super) fn feed(&mut self, delta: &str, stream_bytes: &mut usize) {
        for ch in delta.chars() {
            if self.invalid {
                break;
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
                if matches!(escape, Escape::None) && ch == '"' {
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
                let Some((next, decoded)) = escape.decode(ch) else {
                    self.fail(bytes);
                    return true;
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
                if !self.append_display(body, value, stream_bytes) {
                    if body {
                        self.body_cut = true;
                    } else {
                        self.path_cut = true;
                    }
                }
                self.changes += 1;
            }
            _ => {}
        }
    }

    /// Geometric growth with an explicit capacity ceiling. String::push_str
    /// normally doubles capacity, so charging only the appended byte length
    /// would exceed the advertised per-call/stream memory limits. Reserving
    /// exactly at the few growth boundaries remains amortized linear; there
    /// is no per-character shrink/reallocation or cumulative JSON reparse.
    fn append_display(&mut self, body: bool, value: &str, stream_bytes: &mut usize) -> bool {
        let text = if body {
            self.content.as_mut().unwrap()
        } else {
            self.path.as_mut().unwrap()
        };
        let previous_capacity = text.capacity();
        let other_call_capacity = self.retained - previous_capacity;
        let other_stream_capacity = *stream_bytes - previous_capacity;
        let capacity_limit = if body {
            MAX_CALL_BYTES
        } else {
            MAX_DETAIL_BYTES
        }
        .min(MAX_CALL_BYTES.saturating_sub(other_call_capacity))
        .min(MAX_STREAM_BYTES.saturating_sub(other_stream_capacity));
        let needed = text.len() + value.len();
        if needed > capacity_limit {
            return false;
        }
        if needed > previous_capacity {
            let requested_capacity = previous_capacity
                .saturating_mul(2)
                .max(8)
                .max(needed)
                .min(capacity_limit);
            let reserved = text
                .try_reserve_exact(requested_capacity - text.len())
                .is_ok();
            let actual_capacity = text.capacity();
            // Allocators are permitted to give reserve_exact more space than
            // requested. Never retain that excess or undercount it. On this
            // uncommon path, drop the speculative field instead of trying a
            // per-character shrink or affecting the original model stream.
            if actual_capacity > capacity_limit {
                *text = String::new();
                self.retained = other_call_capacity;
                *stream_bytes = other_stream_capacity;
                return false;
            }
            self.retained = other_call_capacity + actual_capacity;
            *stream_bytes = other_stream_capacity + actual_capacity;
            if !reserved {
                return false;
            }
        }
        // The explicit reservation above guarantees this append cannot grow.
        debug_assert!(needed <= text.capacity());
        text.push_str(value);
        true
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
                            self.path.get_or_insert_with(String::new);
                            self.changes += 1;
                        } else if field == Field::Content {
                            if self.path.is_none() {
                                self.path = Some(String::new());
                                // Charge a small path reservation before a content-first body
                                // consumes the per-call or stream allocation budget.
                                let reserve =
                                    " ".repeat(32.min(MAX_STREAM_BYTES.saturating_sub(*bytes)));
                                self.append_display(false, &reserve, bytes);
                                self.path.as_mut().unwrap().clear();
                            }
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
                    't' | 'f' | 'n' => {
                        self.token = Some(Token::Literal {
                            expected: match ch {
                                't' => "true",
                                'f' => "false",
                                _ => "null",
                            },
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

    pub(super) fn update_interval(&self) -> Duration {
        // Full snapshots target at most 128 KiB/s, retaining 30 Hz for small cards.
        let bytes = self.path.as_ref().map_or(0, String::len)
            + self.content.as_ref().map_or(0, String::len);
        UPDATE_INTERVAL.max(Duration::from_secs_f64(bytes as f64 / (128.0 * 1024.0)))
    }

    pub(super) fn display(&self) -> ToolDisplay {
        let mut detail = self
            .path
            .as_ref()
            .filter(|path| !path.is_empty())
            .cloned()
            .unwrap_or_else(|| "...".to_owned());
        detail.push_str(&super::format_read_range(self.offset, self.limit));
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
