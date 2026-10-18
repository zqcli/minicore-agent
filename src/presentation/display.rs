use super::*;

/// Display text limits. Aligned with the existing per-argument/output caps so
/// the expanded view can never promise rows that the Agent cannot show.
pub(crate) const MAX_DETAIL_BYTES: usize = 512;
pub(crate) const MAX_EXPANDED_INPUT_BYTES: usize = 512 * 1024;
pub(crate) const MAX_RESULT_DISPLAY_BYTES: usize = 512 * 1024;

/// Whitelisted, single-line detail plus bounded expandable input for one tool
/// card. This is the only Tool-detail formatter; live events and history
/// regeneration share it.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolDisplay {
    /// Single visual line (command/path/range/generic summary).
    pub detail: String,
    /// Bounded input body the TUI may show when expanded: write content, edit
    /// old/new text, apply_patch patch. Never the raw invocation object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expanded_input: Option<String>,
    /// Logical line count of the actual rendered input body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_line_count: Option<usize>,
    /// Displayable hidden rows this card would collapse: the source input
    /// rows of whitelisted expanded input plus result rows actually available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden_line_count: Option<usize>,
    /// The source above the display cap was truncated; the TUI must not
    /// promise more expandable rows than shown.
    #[serde(default)]
    pub truncated: bool,
    /// Expanded body reached its display cap; shortened details do not count.
    #[serde(default)]
    pub body_truncated: bool,
}

impl fmt::Debug for ToolDisplay {
    /// Redacted: identity, lengths, and counts only. Raw detail or input text
    /// must never reach a log or an error Debug dump.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolDisplay")
            .field("detail_bytes", &self.detail.len())
            .field(
                "expanded_input_bytes",
                &self.expanded_input.as_ref().map(|input| input.len()),
            )
            .field("input_line_count", &self.input_line_count)
            .field("hidden_line_count", &self.hidden_line_count)
            .field("truncated", &self.truncated)
            .finish()
    }
}

/// Visible, sanitized assistant parts in their original order. Only plain
/// text, visible reasoning text, and tool-call identity are allowed; opaque
/// reasoning and encrypted/signature fields were already dropped by
/// `sanitize_history`.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum AssistantDisplayPart {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
    },
    ToolCall {
        tool_call_id: ToolCallId,
        name: String,
    },
}

/// Visible, sanitized assistant parts in their original order, as a display
/// list. Only plain text, visible reasoning text, and tool-call identity are
/// allowed; the content was already sanitized before this point.
pub(crate) fn assistant_display_parts(
    content: &[minicore_runtime::model::AssistantPart],
) -> Vec<AssistantDisplayPart> {
    let mut parts = Vec::with_capacity(content.len());
    for part in content {
        match part {
            minicore_runtime::model::AssistantPart::Text(text) => {
                parts.push(AssistantDisplayPart::Text {
                    text: text.to_string(),
                });
            }
            minicore_runtime::model::AssistantPart::Reasoning(reasoning) => {
                let mut text = String::new();
                if let Some(reasoning_text) = reasoning.text() {
                    text.push_str(reasoning_text);
                }
                if let Some(summary) = reasoning.summary() {
                    text.push_str(summary);
                }
                if !text.is_empty() {
                    parts.push(AssistantDisplayPart::Reasoning { text });
                }
            }
            minicore_runtime::model::AssistantPart::ToolCall(call) => {
                parts.push(AssistantDisplayPart::ToolCall {
                    tool_call_id: call.tool_call_id().clone(),
                    name: call.name().as_str().to_owned(),
                });
            }
        }
    }
    parts
}

impl fmt::Debug for AssistantDisplayPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { text } => formatter
                .debug_struct("AssistantDisplayPart::Text")
                .field("text_bytes", &text.len())
                .finish(),
            Self::Reasoning { text } => formatter
                .debug_struct("AssistantDisplayPart::Reasoning")
                .field("text_bytes", &text.len())
                .finish(),
            Self::ToolCall { tool_call_id, name } => formatter
                .debug_struct("AssistantDisplayPart::ToolCall")
                .field("tool_call_id", tool_call_id)
                .field("name", name)
                .finish(),
        }
    }
}

pub(super) fn safe_tool_error_text(error: minicore_runtime::tools::ToolError) -> &'static str {
    match error {
        minicore_runtime::tools::ToolError::Cancelled => "tool execution was cancelled",
        minicore_runtime::tools::ToolError::Failed => "tool execution failed",
        minicore_runtime::tools::ToolError::TimedOut => "tool execution timed out",
        minicore_runtime::tools::ToolError::Panicked => "tool operation panicked",
        minicore_runtime::tools::ToolError::InvalidInvocation => "tool invocation is invalid",
        minicore_runtime::tools::ToolError::Internal => "tool operation failed internally",
    }
}

// ---------------------------------------------------------------------------
// The single whitelist Tool-detail formatter (live and history)
// ---------------------------------------------------------------------------

pub(crate) fn count_lines(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        text.matches('\n').count() + 1
    }
}

/// Bounds a string at a char boundary; returns (copy, truncated).
fn bounded(value: &str, max: usize) -> (String, bool) {
    let (prefix, truncated) = crate::text::truncate(value, max);
    (prefix.to_owned(), truncated)
}

/// Removes terminal control effects from display-only text. Newlines and tabs
/// remain meaningful in expanded code bodies; single-line details flatten
/// them. The runtime invocation is never passed through this function.
fn sanitize_display_text(value: &str, multiline: bool) -> String {
    let mut text = String::with_capacity(value.len());
    for character in value.chars() {
        if multiline && matches!(character, '\n' | '\t') {
            text.push(character);
        } else if !multiline && matches!(character, '\r' | '\n' | '\t') {
            text.push(' ');
        } else if character.is_control() {
            text.extend(character.escape_default());
        } else {
            text.push(character);
        }
    }
    text
}

pub(crate) fn sanitize_multiline(value: &str) -> String {
    sanitize_display_text(value, true)
}

/// One-line detail: control whitespace is flattened like the reference
/// `collapsedSimpleLine` and the result is bounded.
pub(super) fn single_line(value: &str, max: usize) -> (String, bool) {
    let sanitized = sanitize_display_text(value, false);
    bounded(&sanitized, max)
}

pub(super) struct DisplayBody {
    pub(super) text: Option<String>,
    bytes: usize,
    lines: usize,
    limit: usize,
    pub(super) truncated: bool,
}

impl DisplayBody {
    fn new(limit: usize, retain: bool) -> Self {
        Self {
            text: retain.then(String::new),
            bytes: 0,
            lines: 0,
            limit,
            truncated: false,
        }
    }

    fn push(&mut self, value: &str) {
        if self.truncated {
            return;
        }
        let (prefix, cut) = crate::text::truncate(value, self.limit - self.bytes);
        self.lines +=
            prefix.matches('\n').count() + usize::from(self.bytes == 0 && !prefix.is_empty());
        self.bytes += prefix.len();
        if let Some(text) = &mut self.text {
            text.push_str(prefix);
        }
        self.truncated = cut;
    }

    fn sanitized(&mut self, value: &str) {
        for ch in value.chars() {
            if self.truncated {
                break;
            }
            if ch.is_control() && !matches!(ch, '\n' | '\t') {
                for escaped in ch.escape_default() {
                    self.push(escaped.encode_utf8(&mut [0; 4]));
                }
            } else {
                self.push(ch.encode_utf8(&mut [0; 4]));
            }
        }
    }

    fn from_text(value: &str, limit: usize, retain: bool) -> Self {
        let mut body = Self::new(limit, retain);
        body.sanitized(value);
        body
    }
}

/// A display-only line diff with an explicit format marker. Bound both the
/// diff inputs and emitted text; fixed complexity limits keep projection
/// deterministic and prevent expensive alignments. No raw argument object
/// is ever exposed through this whitelist.
pub(super) fn edit_display_body(old: &str, new: &str, retain: bool) -> DisplayBody {
    use similar::{Algorithm, ChangeTag, TextDiff};
    let (old, old_cut) = crate::text::truncate(old, MAX_EXPANDED_INPUT_BYTES);
    let (new, new_cut) = crate::text::truncate(new, MAX_EXPANDED_INPUT_BYTES);
    let mut body = DisplayBody::new(MAX_EXPANDED_INPUT_BYTES, retain);
    body.push("--- before\n+++ after\n@@\n");
    let mut row = |prefix: &str, value: &str| {
        body.push(prefix);
        body.sanitized(value.trim_end_matches('\n'));
        body.push("\n");
        body.truncated
    };
    // Keep the same deterministic alignment for retained and count-only cards.
    if old.len() + new.len() > 64 * 1024 || count_lines(old) + count_lines(new) > 2_000 {
        for (prefix, value) in [("-", old), ("+", new)] {
            for line in value.split_inclusive('\n') {
                if row(prefix, line) {
                    return body;
                }
            }
        }
    } else {
        let diff = TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_lines(old, new);
        for op in diff.ops() {
            for change in diff.iter_changes(op) {
                let prefix = match change.tag() {
                    ChangeTag::Equal => " ",
                    ChangeTag::Delete => "-",
                    ChangeTag::Insert => "+",
                };
                if row(prefix, change.value()) {
                    return body;
                }
            }
        }
    }
    body.truncated |= old_cut || new_cut;
    body
}

pub(super) fn path_arg(args: &serde_json::Value, home: &str) -> Option<String> {
    let raw = args
        .get("path")
        .or_else(|| args.get("file_path"))
        .and_then(serde_json::Value::as_str);
    raw.map(|path| {
        let home_match = if home.is_empty() {
            false
        } else if home == "/" {
            path.starts_with('/')
        } else {
            path == home
                || path
                    .strip_prefix(home)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        };
        if home_match {
            if home == "/" {
                if path == "/" {
                    "~".to_owned()
                } else {
                    format!("~{path}")
                }
            } else {
                let suffix = path.strip_prefix(home).unwrap_or(path);
                format!("~{suffix}")
            }
        } else {
            path.to_owned()
        }
    })
}

fn read_range(args: &serde_json::Value) -> String {
    format_read_range(
        args.get("offset").and_then(serde_json::Value::as_u64),
        args.get("limit").and_then(serde_json::Value::as_u64),
    )
}

pub(super) fn format_read_range(offset_value: Option<u64>, limit_value: Option<u64>) -> String {
    if offset_value.is_none() && limit_value.is_none() {
        return String::new();
    }
    let offset = offset_value.unwrap_or(1);
    match limit_value {
        Some(limit) if limit > 0 => format!(
            ":{offset}-{}",
            offset.saturating_add(limit).saturating_sub(1)
        ),
        _ => format!(":{offset}"),
    }
}

/// Adds displayable result rows to a display produced without a result. The
/// initial hidden count already contains the source input rows, following the
/// reference `executionHiddenLineCount` semantics.
pub(super) fn apply_result_to_display(display: &mut ToolDisplay, result_text: Option<&str>) {
    let result_rows = result_text
        .filter(|text| !text.is_empty())
        .map(count_lines)
        .unwrap_or(0);
    let input_rows = display.hidden_line_count.unwrap_or(0);
    let hidden = input_rows + result_rows;
    display.hidden_line_count = (hidden > 0).then_some(hidden);
}

/// Builds the whitelisted ToolDisplay for one tool invocation. `result_text`
/// is the (bounded) result output when known; the same function regenerates
/// history displays from stored args/result so live and history agree.
pub(crate) fn build_tool_display(
    name: &str,
    args: Option<&serde_json::Value>,
    result_text: Option<&str>,
) -> ToolDisplay {
    build_tool_display_inner(name, args, result_text, true)
}

fn build_tool_display_inner(
    name: &str,
    args: Option<&serde_json::Value>,
    result_text: Option<&str>,
    retain: bool,
) -> ToolDisplay {
    let empty_arguments = serde_json::Value::Object(serde_json::Map::new());
    let arguments = args.unwrap_or(&empty_arguments);
    let home = std::env::var("HOME").unwrap_or_default();

    let (detail, mut truncated) = match name {
        "bash" => {
            let command = arguments
                .get("command")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("...");
            let (command, cut) = single_line(command, MAX_DETAIL_BYTES.saturating_sub(2));
            (format!("$ {command}"), cut)
        }
        "read" => {
            let path = path_arg(arguments, &home).unwrap_or_else(|| "...".to_owned());
            let detail_source = format!("{path}{}", read_range(arguments));
            single_line(&detail_source, MAX_DETAIL_BYTES)
        }
        "write" | "edit" => {
            let path = path_arg(arguments, &home).unwrap_or_else(|| "...".to_owned());
            single_line(&path, MAX_DETAIL_BYTES)
        }
        // Unknown tools deliberately expose only their identity. In
        // particular, patterns, paths, and arbitrary JSON arguments do not
        // escape the whitelist through the generic branch.
        _ => single_line(&format!("tool {name}"), MAX_DETAIL_BYTES),
    };

    let (bounded_result, result_truncated) = result_text
        .map(sanitize_multiline)
        .map(|result| bounded(&result, MAX_RESULT_DISPLAY_BYTES))
        .map_or((None, false), |(result, truncated)| {
            (Some(result), truncated)
        });
    truncated |= result_truncated;

    let detail_truncated = truncated;
    let body = if name == "edit" {
        match (
            arguments
                .get("old_text")
                .and_then(serde_json::Value::as_str),
            arguments
                .get("new_text")
                .and_then(serde_json::Value::as_str),
        ) {
            (Some(old), Some(new)) => Some(edit_display_body(old, new, retain)),
            (body, None) | (None, body) => {
                body.map(|body| DisplayBody::from_text(body, MAX_EXPANDED_INPUT_BYTES, retain))
            }
        }
    } else {
        let field = match name {
            "bash" => "command",
            "write" => "content",
            "apply_patch" | "patch" => "patch",
            _ => "",
        };
        (!field.is_empty())
            .then(|| arguments.get(field))
            .flatten()
            .and_then(serde_json::Value::as_str)
            .map(|body| DisplayBody::from_text(body, MAX_EXPANDED_INPUT_BYTES, retain))
    };
    let input_line_count = body.as_ref().map(|body| body.lines);
    let input_rows = input_line_count.unwrap_or(0);
    let truncated = body.as_ref().is_some_and(|body| body.truncated);
    let expanded_input = body.and_then(|body| body.text);

    let mut display = ToolDisplay {
        detail,
        expanded_input,
        input_line_count,
        hidden_line_count: (input_rows > 0).then_some(input_rows),
        body_truncated: truncated || result_truncated,
        truncated: detail_truncated || truncated,
    };
    apply_result_to_display(&mut display, bounded_result.as_deref());
    display
}

/// The compact card target is a command's first physical line or an explicit
/// patch path. Body formatting remains shared with live/legacy displays.
pub(crate) fn build_tool_card_display(name: &str, args: Option<&serde_json::Value>) -> ToolDisplay {
    tool_card(name, args, true)
}

pub(crate) fn tool_card_counts(name: &str, args: Option<&serde_json::Value>) -> ToolDisplay {
    tool_card(name, args, false)
}

fn tool_card(name: &str, args: Option<&serde_json::Value>, retain: bool) -> ToolDisplay {
    let mut display = build_tool_display_inner(name, args, None, retain);
    let field = match name {
        "bash" => "command",
        "apply_patch" | "patch" => "path",
        _ => return display,
    };
    let source = args
        .and_then(|args| args.get(field))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(name)
        .lines()
        .next()
        .unwrap_or("");
    let (detail, cut) = single_line(source, MAX_DETAIL_BYTES);
    display.detail = detail;
    display.truncated = display.body_truncated || cut;
    display
}

pub(crate) fn result_line_count(value: &str) -> (usize, bool) {
    let body = DisplayBody::from_text(value, MAX_RESULT_DISPLAY_BYTES, false);
    (body.lines, body.truncated)
}

pub(super) fn bounded_display_copy(value: &str, max: usize) -> Option<(String, bool)> {
    if value.is_empty() {
        return None;
    }
    Some(bounded_result_content(value, max))
}

/// Sanitizes and bounds a real tool result for both live events and history
/// views. Empty results remain representable as an empty string plus a false
/// truncation bit; callers decide whether to omit that value from JSON.
pub(crate) fn bounded_result_content(value: &str, max: usize) -> (String, bool) {
    let sanitized = sanitize_multiline(value);
    bounded(&sanitized, max)
}
