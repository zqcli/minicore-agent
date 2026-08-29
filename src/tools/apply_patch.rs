use std::path::{Component, Path};
use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::{Workspace, WorkspaceError};

use super::{
    MAX_PATCH_BYTES, escape_control_characters, map_workspace_error, precheck_control,
    run_controlled, wait_for_test_io,
};

const TOOL_NAME: &str = "apply_patch";

pub(super) struct ApplyPatchTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyPatchInput {
    path: String,
    patch: String,
}

impl ApplyPatchTool {
    pub(super) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("apply_patch is a valid tool name"),
            "Apply one complete unified text patch to one existing workspace file.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative existing file path."
                    },
                    "patch": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Single-file unified text patch, limited to 512 KiB."
                    }
                },
                "required": ["path", "patch"],
                "additionalProperties": false
            }),
        )
        .expect("static apply_patch tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for ApplyPatchTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'a> {
        Box::pin(async move {
            precheck_control(&context)?;
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input: ApplyPatchInput = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            if input.patch.is_empty()
                || input.patch.len() > MAX_PATCH_BYTES
                || input.patch.contains('\0')
            {
                return Err(ToolError::InvalidInvocation);
            }
            self.workspace
                .validate_write_path(&input.path)
                .map_err(map_workspace_error)?;
            let path_display = escape_control_characters(&input.path);

            run_controlled(&context, async {
                wait_for_test_io(TOOL_NAME, &input.path).await;
                let source = self
                    .workspace
                    .read_text(&input.path, MAX_PATCH_BYTES)
                    .await
                    .map_err(map_source_error)?;
                let result = apply_single_file_patch(&input.path, &source, &input.patch)?;
                if result.len() > MAX_PATCH_BYTES {
                    return Err(ToolError::InvalidInvocation);
                }
                let output = ToolOutput::new(format!(
                    "patched {} bytes to {} bytes at {path_display}",
                    source.len(),
                    result.len()
                ))
                .map_err(|_| ToolError::Internal)?;
                self.workspace
                    .write_atomic(&input.path, result.as_bytes())
                    .await
                    .map_err(map_workspace_error)?;
                Ok(ToolExecutionOutcome::Completed(output))
            })
            .await
        })
    }
}

fn apply_single_file_patch(path: &str, source: &str, patch: &str) -> Result<String, ToolError> {
    apply_single_file_patch_with_stats(path, source, patch).map(|(result, _)| result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LineEnding {
    None,
    Lf,
    Crlf,
}

impl LineEnding {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Lf => "\n",
            Self::Crlf => "\r\n",
        }
    }
}

#[derive(Clone, Copy)]
struct SourceLine<'a> {
    text: &'a str,
    ending: LineEnding,
}

#[derive(Clone, Copy)]
enum PatchLineKind {
    Context,
    Remove,
    Add,
}

#[derive(Clone, Copy)]
struct PatchContent<'a> {
    kind: PatchLineKind,
    text: &'a str,
    old_no_newline: bool,
    new_no_newline: bool,
}

#[derive(Clone, Copy)]
struct HunkRange {
    index: usize,
    count: usize,
    end: usize,
}

#[derive(Default)]
struct ApplyStats {
    source_lines: usize,
    source_line_visits: usize,
    patch_lines: usize,
    patch_content_visits: usize,
}

impl ApplyStats {
    fn total_steps(&self) -> usize {
        self.source_line_visits
            .saturating_add(self.patch_content_visits)
    }
}

struct ResultBuilder {
    value: String,
    line_count: usize,
    previous_had_no_newline: bool,
}

impl ResultBuilder {
    fn new(source_len: usize) -> Self {
        Self {
            value: String::with_capacity(source_len.min(MAX_PATCH_BYTES)),
            line_count: 0,
            previous_had_no_newline: false,
        }
    }

    fn append(&mut self, text: &str, ending: LineEnding) -> Result<(), ToolError> {
        if self.previous_had_no_newline {
            return Err(ToolError::Failed);
        }
        let added = text
            .len()
            .checked_add(ending.as_str().len())
            .ok_or(ToolError::InvalidInvocation)?;
        let result_len = self
            .value
            .len()
            .checked_add(added)
            .ok_or(ToolError::InvalidInvocation)?;
        if result_len > MAX_PATCH_BYTES {
            return Err(ToolError::InvalidInvocation);
        }
        self.value.push_str(text);
        self.value.push_str(ending.as_str());
        self.line_count = self
            .line_count
            .checked_add(1)
            .ok_or(ToolError::InvalidInvocation)?;
        self.previous_had_no_newline = ending == LineEnding::None;
        Ok(())
    }
}

struct PatchLines<'a> {
    lines: std::str::SplitInclusive<'a, char>,
    peeked: Option<&'a str>,
    steps: usize,
}

impl<'a> PatchLines<'a> {
    fn new(patch: &'a str) -> Self {
        Self {
            lines: patch.split_inclusive('\n'),
            peeked: None,
            steps: 0,
        }
    }

    fn peek(&mut self) -> Option<&'a str> {
        if self.peeked.is_none() {
            self.peeked = self.lines.next();
        }
        self.peeked
    }

    fn next(&mut self) -> Option<&'a str> {
        let line = self.peeked.take().or_else(|| self.lines.next())?;
        self.steps = self.steps.saturating_add(1);
        Some(line)
    }
}

fn apply_single_file_patch_with_stats(
    path: &str,
    source: &str,
    patch: &str,
) -> Result<(String, ApplyStats), ToolError> {
    let source_lines = split_source_lines(source);
    let preferred_ending = preferred_line_ending(&source_lines);
    let mut stats = ApplyStats {
        source_lines: source_lines.len(),
        ..ApplyStats::default()
    };
    let mut parser = PatchLines::new(patch);
    let first = parser.next().ok_or(ToolError::InvalidInvocation)?;
    let mut next_hunk = if strip_transport_ending(first).starts_with("--- ") {
        let original = parse_header_path(first, "--- ")?;
        let modified =
            parse_header_path(parser.next().ok_or(ToolError::InvalidInvocation)?, "+++ ")?;
        validate_header_path(&original, path, "a/")?;
        validate_header_path(&modified, path, "b/")?;
        parser.next().ok_or(ToolError::InvalidInvocation)?
    } else {
        first
    };

    let mut source_cursor = 0usize;
    let mut builder = ResultBuilder::new(source.len());
    let mut hunk_count = 0usize;
    let mut previous_old_end = 0usize;
    let mut previous_new_end = 0usize;

    loop {
        let (old_range, new_range) = parse_hunk_header(strip_transport_ending(next_hunk))?;
        if old_range.index < previous_old_end || new_range.index < previous_new_end {
            return Err(ToolError::InvalidInvocation);
        }
        previous_old_end = old_range.end;
        previous_new_end = new_range.end;

        let contents = parse_hunk_contents(&mut parser, old_range.count, new_range.count)?;
        while source_cursor < old_range.index {
            append_source_line(&source_lines, &mut source_cursor, &mut builder, &mut stats)?;
        }
        if source_cursor != old_range.index || builder.line_count != new_range.index {
            return Err(ToolError::Failed);
        }
        for content in contents.iter().copied() {
            apply_patch_content(
                content,
                &source_lines,
                &mut source_cursor,
                &mut builder,
                preferred_ending,
                &mut stats,
            )?;
            stats.patch_content_visits = stats
                .patch_content_visits
                .checked_add(1)
                .ok_or(ToolError::Internal)?;
        }
        if source_cursor != old_range.end || builder.line_count != new_range.end {
            return Err(ToolError::Failed);
        }
        hunk_count = hunk_count
            .checked_add(1)
            .ok_or(ToolError::InvalidInvocation)?;

        match parser.next() {
            Some(line) if strip_transport_ending(line).starts_with("@@ ") => {
                next_hunk = line;
            }
            Some(_) => return Err(ToolError::InvalidInvocation),
            None => break,
        }
    }

    if hunk_count == 0 {
        return Err(ToolError::InvalidInvocation);
    }
    while source_cursor < source_lines.len() {
        append_source_line(&source_lines, &mut source_cursor, &mut builder, &mut stats)?;
    }
    stats.patch_lines = parser.steps;
    if stats.source_line_visits != stats.source_lines {
        return Err(ToolError::Internal);
    }
    Ok((builder.value, stats))
}

fn parse_hunk_contents<'a>(
    parser: &mut PatchLines<'a>,
    expected_old: usize,
    expected_new: usize,
) -> Result<Vec<PatchContent<'a>>, ToolError> {
    let mut contents = Vec::new();
    let mut old_count = 0usize;
    let mut new_count = 0usize;
    while old_count < expected_old || new_count < expected_new {
        let raw = parser.next().ok_or(ToolError::InvalidInvocation)?;
        let line = strip_transport_ending(raw);
        if line == "\\ No newline at end of file" {
            return Err(ToolError::InvalidInvocation);
        }
        let mut content = parse_patch_content(line)?;
        match content.kind {
            PatchLineKind::Context => {
                old_count = old_count
                    .checked_add(1)
                    .ok_or(ToolError::InvalidInvocation)?;
                new_count = new_count
                    .checked_add(1)
                    .ok_or(ToolError::InvalidInvocation)?;
            }
            PatchLineKind::Remove => {
                old_count = old_count
                    .checked_add(1)
                    .ok_or(ToolError::InvalidInvocation)?;
            }
            PatchLineKind::Add => {
                new_count = new_count
                    .checked_add(1)
                    .ok_or(ToolError::InvalidInvocation)?;
            }
        }
        if old_count > expected_old || new_count > expected_new {
            return Err(ToolError::InvalidInvocation);
        }
        if parser
            .peek()
            .is_some_and(|line| strip_transport_ending(line) == "\\ No newline at end of file")
        {
            parser.next();
            mark_no_newline(&mut content)?;
            if parser
                .peek()
                .is_some_and(|line| strip_transport_ending(line) == "\\ No newline at end of file")
            {
                return Err(ToolError::InvalidInvocation);
            }
        }
        contents.push(content);
    }
    if contents.is_empty() {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(contents)
}

fn split_source_lines(source: &str) -> Vec<SourceLine<'_>> {
    let bytes = source.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte != b'\n' {
            continue;
        }
        let (text_end, ending) = if index > start && bytes[index - 1] == b'\r' {
            (index - 1, LineEnding::Crlf)
        } else {
            (index, LineEnding::Lf)
        };
        lines.push(SourceLine {
            text: &source[start..text_end],
            ending,
        });
        start = index + 1;
    }
    if start < source.len() {
        lines.push(SourceLine {
            text: &source[start..],
            ending: LineEnding::None,
        });
    }
    lines
}

fn preferred_line_ending(lines: &[SourceLine<'_>]) -> LineEnding {
    let mut lf = 0usize;
    let mut crlf = 0usize;
    let mut first = None;
    for line in lines {
        match line.ending {
            LineEnding::Lf => {
                lf = lf.saturating_add(1);
                first.get_or_insert(LineEnding::Lf);
            }
            LineEnding::Crlf => {
                crlf = crlf.saturating_add(1);
                first.get_or_insert(LineEnding::Crlf);
            }
            LineEnding::None => {}
        }
    }
    match crlf.cmp(&lf) {
        std::cmp::Ordering::Greater => LineEnding::Crlf,
        std::cmp::Ordering::Less => LineEnding::Lf,
        std::cmp::Ordering::Equal => first.unwrap_or(LineEnding::Lf),
    }
}

fn append_source_line(
    source: &[SourceLine<'_>],
    cursor: &mut usize,
    builder: &mut ResultBuilder,
    stats: &mut ApplyStats,
) -> Result<(), ToolError> {
    let line = *source.get(*cursor).ok_or(ToolError::Failed)?;
    builder.append(line.text, line.ending)?;
    *cursor = cursor.checked_add(1).ok_or(ToolError::Internal)?;
    stats.source_line_visits = stats
        .source_line_visits
        .checked_add(1)
        .ok_or(ToolError::Internal)?;
    Ok(())
}

fn apply_patch_content(
    content: PatchContent<'_>,
    source: &[SourceLine<'_>],
    cursor: &mut usize,
    builder: &mut ResultBuilder,
    preferred_ending: LineEnding,
    stats: &mut ApplyStats,
) -> Result<(), ToolError> {
    match content.kind {
        PatchLineKind::Context => {
            let line = matching_source_line(content, source, *cursor)?;
            builder.append(line.text, line.ending)?;
            *cursor = cursor.checked_add(1).ok_or(ToolError::Internal)?;
            stats.source_line_visits = stats
                .source_line_visits
                .checked_add(1)
                .ok_or(ToolError::Internal)?;
        }
        PatchLineKind::Remove => {
            matching_source_line(content, source, *cursor)?;
            *cursor = cursor.checked_add(1).ok_or(ToolError::Internal)?;
            stats.source_line_visits = stats
                .source_line_visits
                .checked_add(1)
                .ok_or(ToolError::Internal)?;
        }
        PatchLineKind::Add => {
            let ending = if content.new_no_newline {
                LineEnding::None
            } else {
                preferred_ending
            };
            builder.append(content.text, ending)?;
        }
    }
    Ok(())
}

fn matching_source_line<'a>(
    content: PatchContent<'_>,
    source: &'a [SourceLine<'a>],
    cursor: usize,
) -> Result<SourceLine<'a>, ToolError> {
    let line = *source.get(cursor).ok_or(ToolError::Failed)?;
    let has_no_newline = line.ending == LineEnding::None;
    if line.text != content.text || has_no_newline != content.old_no_newline {
        return Err(ToolError::Failed);
    }
    Ok(line)
}

fn parse_patch_content(line: &str) -> Result<PatchContent<'_>, ToolError> {
    let (kind, text) = if let Some(text) = line.strip_prefix(' ') {
        (PatchLineKind::Context, text)
    } else if let Some(text) = line.strip_prefix('-') {
        (PatchLineKind::Remove, text)
    } else if let Some(text) = line.strip_prefix('+') {
        (PatchLineKind::Add, text)
    } else {
        return Err(ToolError::InvalidInvocation);
    };
    Ok(PatchContent {
        kind,
        text,
        old_no_newline: false,
        new_no_newline: false,
    })
}

fn mark_no_newline(content: &mut PatchContent<'_>) -> Result<(), ToolError> {
    if content.old_no_newline || content.new_no_newline {
        return Err(ToolError::InvalidInvocation);
    }
    match content.kind {
        PatchLineKind::Context => {
            content.old_no_newline = true;
            content.new_no_newline = true;
        }
        PatchLineKind::Remove => content.old_no_newline = true,
        PatchLineKind::Add => content.new_no_newline = true,
    }
    Ok(())
}

fn parse_hunk_header(header: &str) -> Result<(HunkRange, HunkRange), ToolError> {
    let header = header
        .strip_prefix("@@ ")
        .ok_or(ToolError::InvalidInvocation)?;
    let closing = header.find(" @@").ok_or(ToolError::InvalidInvocation)?;
    let ranges = &header[..closing];
    let suffix = &header[closing + 3..];
    if suffix.contains('\0') || (!suffix.is_empty() && !suffix.starts_with(' ')) {
        return Err(ToolError::InvalidInvocation);
    }
    let (old, new) = ranges.split_once(' ').ok_or(ToolError::InvalidInvocation)?;
    if old.contains(' ') || new.contains(' ') {
        return Err(ToolError::InvalidInvocation);
    }
    Ok((parse_hunk_range(old, '-')?, parse_hunk_range(new, '+')?))
}

fn parse_hunk_range(range: &str, prefix: char) -> Result<HunkRange, ToolError> {
    let range = range
        .strip_prefix(prefix)
        .ok_or(ToolError::InvalidInvocation)?;
    let (start, count) = range.split_once(',').unwrap_or((range, "1"));
    if !is_decimal(start) || !is_decimal(count) {
        return Err(ToolError::InvalidInvocation);
    }
    let start = start
        .parse::<usize>()
        .map_err(|_| ToolError::InvalidInvocation)?;
    let count = count
        .parse::<usize>()
        .map_err(|_| ToolError::InvalidInvocation)?;
    if start == 0 && count != 0 {
        return Err(ToolError::InvalidInvocation);
    }
    let index = if count == 0 {
        start
    } else {
        start.checked_sub(1).ok_or(ToolError::InvalidInvocation)?
    };
    let end = index
        .checked_add(count)
        .ok_or(ToolError::InvalidInvocation)?;
    start
        .checked_add(count)
        .ok_or(ToolError::InvalidInvocation)?;
    Ok(HunkRange { index, count, end })
}

fn is_decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_header_path(line: &str, prefix: &str) -> Result<String, ToolError> {
    let line = strip_transport_ending(line);
    let value = line
        .strip_prefix(prefix)
        .ok_or(ToolError::InvalidInvocation)?;
    let path = if value.starts_with('"') {
        decode_quoted_path(value)?
    } else {
        value
            .split_once('\t')
            .map_or(value, |(path, _)| path)
            .to_owned()
    };
    if path.is_empty() || path.contains('\0') {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(path)
}

fn decode_quoted_path(value: &str) -> Result<String, ToolError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 1usize;
    let mut closed = false;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                closed = true;
                break;
            }
            b'\\' => {
                index += 1;
                let escaped = *bytes.get(index).ok_or(ToolError::InvalidInvocation)?;
                match escaped {
                    b'\\' | b'"' => {
                        decoded.push(escaped);
                        index += 1;
                    }
                    b'a' | b'b' | b't' | b'n' | b'v' | b'f' | b'r' => {
                        decoded.push(match escaped {
                            b'a' => 0x07,
                            b'b' => 0x08,
                            b't' => b'\t',
                            b'n' => b'\n',
                            b'v' => 0x0b,
                            b'f' => 0x0c,
                            b'r' => b'\r',
                            _ => unreachable!(),
                        });
                        index += 1;
                    }
                    b'0'..=b'7' => {
                        let mut value = 0u16;
                        let mut digits = 0usize;
                        while digits < 3
                            && index < bytes.len()
                            && matches!(bytes[index], b'0'..=b'7')
                        {
                            value = value
                                .checked_mul(8)
                                .and_then(|value| value.checked_add(u16::from(bytes[index] - b'0')))
                                .ok_or(ToolError::InvalidInvocation)?;
                            index += 1;
                            digits += 1;
                        }
                        decoded
                            .push(u8::try_from(value).map_err(|_| ToolError::InvalidInvocation)?);
                    }
                    _ => return Err(ToolError::InvalidInvocation),
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    if !closed || (index < bytes.len() && bytes[index] != b'\t') {
        return Err(ToolError::InvalidInvocation);
    }
    let path = String::from_utf8(decoded).map_err(|_| ToolError::InvalidInvocation)?;
    if path.contains('\0') {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(path)
}

fn validate_header_path(header: &str, tool_path: &str, prefix: &str) -> Result<(), ToolError> {
    let candidate = if header == tool_path {
        header
    } else {
        header.strip_prefix(prefix).unwrap_or(header)
    };
    let path = Path::new(candidate);
    if candidate != tool_path
        || candidate.is_empty()
        || candidate.contains('\0')
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(())
}

fn strip_transport_ending(line: &str) -> &str {
    let Some(line) = line.strip_suffix('\n') else {
        return line;
    };
    line.strip_suffix('\r').unwrap_or(line)
}

fn map_source_error(error: WorkspaceError) -> ToolError {
    match error {
        WorkspaceError::InvalidPath | WorkspaceError::Escape => ToolError::InvalidInvocation,
        WorkspaceError::NotFound
        | WorkspaceError::NotFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::Unavailable
        | WorkspaceError::UnknownOutcome
        | WorkspaceError::TooLarge
        | WorkspaceError::Binary => ToolError::Failed,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::ids::{SessionInstanceId, ToolCallId, TurnId};
    use minicore_runtime::tools::{
        Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::tools::{ToolIoGate, block_next_io};
    use crate::workspace::{fail_next_before_rename, fail_next_directory_sync};

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, ApplyPatchTool) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-apply-patch-tool-{label}-{}",
            minicore_runtime::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let tool = ApplyPatchTool::new(Arc::clone(&workspace));
        (base, workspace, tool)
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            instance_id: "ins_00000000000000000000000000000001"
                .parse::<SessionInstanceId>()
                .unwrap(),
            turn_id: "trn_00000000000000000000000000000001"
                .parse::<TurnId>()
                .unwrap(),
            tool_call_id: ToolCallId::new("apply-patch-call").unwrap(),
            tool_name: TOOL_NAME.parse().unwrap(),
            arguments,
        }
    }

    fn context(cancellation: CancellationToken, deadline: Instant) -> ToolContext {
        ToolContext {
            cancellation,
            deadline,
            progress: ToolProgressSink::default(),
        }
    }

    async fn execute(tool: &ApplyPatchTool, arguments: Value) -> Result<String, ToolError> {
        match tool
            .execute(
                invocation(arguments),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5),
                ),
            )
            .await?
        {
            ToolExecutionOutcome::Completed(output) => Ok(output.content().as_str().to_owned()),
            ToolExecutionOutcome::RequestInput(_) => {
                panic!("apply_patch must not request input")
            }
        }
    }

    async fn cleanup(base: &Path) {
        let _ = tokio::fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn schema_is_strict_and_headerless_patch_is_applied() {
        let (base, _, tool) = fixture("headerless").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value.txt"), "one\ntwo\nthree\n")
            .await
            .unwrap();
        let schema = tool.spec().input_schema();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], json!(["path", "patch"]));
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["patch"]["minLength"], 1);
        for arguments in [
            json!({"path": "value.txt"}),
            json!({"path": "value.txt", "patch": "@@ -1 +1 @@\n one\n", "extra": true}),
        ] {
            assert_eq!(
                tool.execute(
                    invocation(arguments),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::InvalidInvocation)
            );
        }

        let patch = "@@ -1,3 +1,3 @@\n one\n-two\n+TWO\n three\n";
        assert_eq!(
            execute(&tool, json!({"path": "value.txt", "patch": patch}))
                .await
                .unwrap(),
            "patched 14 bytes to 14 bytes at value.txt"
        );
        assert_eq!(
            tokio::fs::read_to_string(root.join("value.txt"))
                .await
                .unwrap(),
            "one\nTWO\nthree\n"
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn standard_matching_headers_and_multiple_hunks_apply_unicode_changes() {
        let (base, _, tool) = fixture("standard-multi-hunk").await;
        let root = base.join("root");
        let source = "alpha\nbeta\nmiddle\ngamma\ndelta\n";
        let expected = "alpha\nβeta\nmiddle\ngamma\n世界\n";
        tokio::fs::write(root.join("value.txt"), source)
            .await
            .unwrap();
        let patch = "--- a/value.txt\t2026-01-01\n+++ b/value.txt\t2026-01-02\n@@ -1,2 +1,2 @@\n alpha\n-beta\n+βeta\n@@ -4,2 +4,2 @@\n gamma\n-delta\n+世界\n";

        assert_eq!(
            execute(&tool, json!({"path": "value.txt", "patch": patch}))
                .await
                .unwrap(),
            format!(
                "patched {} bytes to {} bytes at value.txt",
                source.len(),
                expected.len()
            )
        );
        assert_eq!(
            tokio::fs::read_to_string(root.join("value.txt"))
                .await
                .unwrap(),
            expected
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn headerless_and_standard_hunks_accept_section_headings() {
        let (base, _, tool) = fixture("section-headings").await;
        let root = base.join("root");
        tokio::fs::write(root.join("headerless.txt"), "old\n")
            .await
            .unwrap();
        let headerless = "@@ -1 +1 @@ fn execute() @@ nested\n-old\n+new\n";
        execute(
            &tool,
            json!({"path": "headerless.txt", "patch": headerless}),
        )
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(root.join("headerless.txt"))
                .await
                .unwrap(),
            "new\n"
        );

        tokio::fs::write(root.join("standard.txt"), "old\n")
            .await
            .unwrap();
        let standard =
            "--- a/standard.txt\n+++ b/standard.txt\n@@ -1 +1 @@   fn execute()\n-old\n+new\n";
        execute(&tool, json!({"path": "standard.txt", "patch": standard}))
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(root.join("standard.txt"))
                .await
                .unwrap(),
            "new\n"
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn context_failure_and_partial_second_hunk_leave_the_source_unchanged() {
        let (base, _, tool) = fixture("hunk-failure").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        let source = "one\ntwo\nthree\nfour\n";
        tokio::fs::write(&target, source).await.unwrap();

        for patch in [
            "@@ -1,2 +1,2 @@\n one\n-mismatch\n+TWO\n",
            "@@ -1,2 +1,2 @@\n one\n-two\n+TWO\n@@ -3,2 +3,2 @@\n three\n-mismatch\n+FOUR\n",
        ] {
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": "value.txt", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::Failed)
            );
            assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
        }
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn hunk_content_at_the_wrong_declared_position_does_not_fuzzy_apply() {
        let (base, _, tool) = fixture("exact-position").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        let source = "zero\none\ntarget\n";
        tokio::fs::write(&target, source).await.unwrap();
        let patch = "@@ -1 +1 @@\n-target\n+changed\n";

        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn hunk_ranges_are_strict_ordered_and_support_start_zero_insertions() {
        let (base, _, tool) = fixture("strict-ranges").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        tokio::fs::write(&target, "tail\n").await.unwrap();
        assert_eq!(
            execute(
                &tool,
                json!({"path": "value.txt", "patch": "@@ -0,0 +1 @@\n+head\n"})
            )
            .await
            .unwrap(),
            "patched 5 bytes to 10 bytes at value.txt"
        );
        assert_eq!(
            tokio::fs::read_to_string(&target).await.unwrap(),
            "head\ntail\n"
        );

        let invalid = [
            "@@ -0 +1 @@\n-tail\n+TAIL\n",
            "@@ -1,2 +1 @@\n-tail\n+TAIL\n",
            "@@ -1 +1\n-tail\n+TAIL\n",
            "@@@ -1 +1 @@\n-tail\n+TAIL\n",
            "@@ -x +1 @@ fn execute()\n-tail\n+TAIL\n",
            "@@ -1 +1 @@fn execute()\n-tail\n+TAIL\n",
            "@@ -1 +1 @@@\n-tail\n+TAIL\n",
            "@@ -1,0 +1,0 @@\n",
            "@@ -184467440737095516160,0 +1,0 @@\n+x\n",
            "@@ -2 +2 @@\n-b\n+B\n@@ -1 +1 @@\n-a\n+A\n",
            "@@ -1,2 +1,2 @@\n a\n b\n@@ -2 +2 @@\n-b\n+B\n",
        ];
        for patch in invalid {
            tokio::fs::write(&target, "a\nb\n").await.unwrap();
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": "value.txt", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::InvalidInvocation),
                "patch should be rejected as invalid: {patch:?}"
            );
            assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "a\nb\n");
        }

        tokio::fs::write(&target, "a\n").await.unwrap();
        let wrong_new_position = "@@ -1 +2 @@\n-a\n+A\n";
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": wrong_new_position})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "a\n");
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn c_quoted_headers_decode_tab_quote_backslash_and_octal_utf8() {
        let (base, _, tool) = fixture("quoted-headers").await;
        let root = base.join("root");
        let cases = [
            (
                "tab\tname.txt",
                r#""a/tab\tname.txt""#,
                r#""b/tab\tname.txt""#,
            ),
            (
                "quote\"name.txt",
                r#""a/quote\"name.txt""#,
                r#""b/quote\"name.txt""#,
            ),
            (
                "slash\\name.txt",
                r#""a/slash\\name.txt""#,
                r#""b/slash\\name.txt""#,
            ),
            ("é.txt", r#""a/\303\251.txt""#, r#""b/\303\251.txt""#),
        ];
        for (path, old_header, new_header) in cases {
            tokio::fs::write(root.join(path), "old\n").await.unwrap();
            let patch = format!(
                "--- {old_header}\told timestamp\n+++ {new_header}\tnew timestamp\n@@ -1 +1 @@\n-old\n+new\n"
            );
            execute(&tool, json!({"path": path, "patch": patch}))
                .await
                .unwrap();
            assert_eq!(
                tokio::fs::read_to_string(root.join(path)).await.unwrap(),
                "new\n"
            );
        }

        tokio::fs::write(root.join("value.txt"), "old\n")
            .await
            .unwrap();
        let invalid_headers = [
            r#""/absolute""#,
            r#""a/../value.txt""#,
            r#""a/value\x.txt""#,
            r#""a/\377.txt""#,
            r#""a/\000value.txt""#,
            r#""a/value.txt"junk"#,
            r#""a/value.txt"#,
        ];
        for old_header in invalid_headers {
            let patch = format!("--- {old_header}\n+++ \"b/value.txt\"\n@@ -1 +1 @@\n-old\n+new\n");
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": "value.txt", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::InvalidInvocation)
            );
            assert_eq!(
                tokio::fs::read_to_string(root.join("value.txt"))
                    .await
                    .unwrap(),
                "old\n"
            );
        }
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn newline_markers_are_side_specific_and_preserve_crlf_final_newline_state() {
        let (base, _, tool) = fixture("newline-markers").await;
        let root = base.join("root");
        let target = root.join("value.txt");

        tokio::fs::write(&target, b"keep\r\nold").await.unwrap();
        let old_only = "@@ -2 +2 @@\r\n-old\r\n\\ No newline at end of file\r\n+new\r\n";
        execute(&tool, json!({"path": "value.txt", "patch": old_only}))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"keep\r\nnew\r\n");

        tokio::fs::write(&target, b"keep\r\nold\r\n").await.unwrap();
        let new_only = "--- a/value.txt\r\n+++ b/value.txt\r\n@@ -2 +2 @@\r\n-old\r\n+new\r\n\\ No newline at end of file\r\n";
        execute(&tool, json!({"path": "value.txt", "patch": new_only}))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"keep\r\nnew");

        tokio::fs::write(&target, b"keep\r\nlast").await.unwrap();
        let context_patch = "@@ -2 +2 @@\r\n last\r\n\\ No newline at end of file\r\n";
        execute(&tool, json!({"path": "value.txt", "patch": context_patch}))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"keep\r\nlast");

        for patch in [
            "\\ No newline at end of file\n@@ -1 +1 @@\n-old\n+new\n",
            "@@ -1 +1 @@\n-old\n\\ No newline at end of file\n\\ No newline at end of file\n+new\n",
        ] {
            tokio::fs::write(&target, b"old\n").await.unwrap();
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": "value.txt", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::InvalidInvocation)
            );
            assert_eq!(tokio::fs::read(&target).await.unwrap(), b"old\n");
        }

        tokio::fs::write(&target, b"old\n").await.unwrap();
        let wrong_old_marker = "@@ -1 +1 @@\n-old\n\\ No newline at end of file\n+new\n";
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": wrong_old_marker})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"old\n");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn mixed_source_endings_are_preserved_outside_changes_and_additions_use_majority() {
        let (base, _, tool) = fixture("mixed-endings").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value.txt"), b"one\r\ntwo\nthree\r\n")
            .await
            .unwrap();
        let patch = "@@ -2 +2 @@\n-two\n+TWO\n";
        execute(&tool, json!({"path": "value.txt", "patch": patch}))
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(root.join("value.txt")).await.unwrap(),
            b"one\r\nTWO\r\nthree\r\n"
        );
        cleanup(&base).await;
    }

    #[test]
    fn thousands_of_hunks_use_single_forward_source_and_patch_steps() {
        let mut source = String::new();
        let mut replacement_patch = String::new();
        let mut expected = String::new();
        for index in 0..3_000usize {
            source.push_str(&format!("v{index:04}\n"));
            expected.push_str(&format!("V{index:04}\n"));
            let line = index + 1;
            replacement_patch.push_str(&format!(
                "@@ -{line} +{line} @@\n-v{index:04}\n+V{index:04}\n"
            ));
        }
        assert!(replacement_patch.len() <= MAX_PATCH_BYTES);
        let (result, stats) =
            apply_single_file_patch_with_stats("value.txt", &source, &replacement_patch).unwrap();
        assert_eq!(result, expected);
        assert_eq!(stats.source_line_visits, stats.source_lines);
        assert_eq!(stats.patch_lines, replacement_patch.lines().count());
        assert!(stats.total_steps() <= stats.source_lines + replacement_patch.lines().count());

        let mut insertion_patch = String::new();
        let mut inserted = String::new();
        for index in 0..3_000usize {
            let new_line = index + 1;
            insertion_patch.push_str(&format!("@@ -0,0 +{new_line} @@\n+i{index:04}\n"));
            inserted.push_str(&format!("i{index:04}\n"));
        }
        assert!(insertion_patch.len() <= MAX_PATCH_BYTES);
        let (result, stats) =
            apply_single_file_patch_with_stats("value.txt", "", &insertion_patch).unwrap();
        assert_eq!(result, inserted);
        assert_eq!(stats.source_line_visits, 0);
        assert_eq!(stats.patch_lines, insertion_patch.lines().count());
        assert!(stats.total_steps() <= insertion_patch.lines().count());

        let source = "x".repeat(MAX_PATCH_BYTES - 8_000);
        let mut over_limit_patch = String::new();
        for index in 0..5_000usize {
            let new_line = index + 1;
            over_limit_patch.push_str(&format!("@@ -0,0 +{new_line} @@\n+x\n"));
        }
        assert!(over_limit_patch.len() <= MAX_PATCH_BYTES);
        assert_eq!(
            apply_single_file_patch("value.txt", &source, &over_limit_patch),
            Err(ToolError::InvalidInvocation)
        );
    }

    #[tokio::test]
    async fn unsafe_or_multi_file_patch_shapes_are_invalid_and_do_not_write() {
        let (base, _, tool) = fixture("invalid-shapes").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        let source = "old\n";
        tokio::fs::write(&target, source).await.unwrap();
        let invalid = [
            "--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-old\n+new\n--- a/other.txt\n+++ b/other.txt\n@@ -1 +1 @@\n-other\n+OTHER\n",
            "--- a/value.txt\n+++ b/renamed.txt\n@@ -1 +1 @@\n-old\n+new\n",
            "--- /dev/null\n+++ b/value.txt\n@@ -0,0 +1 @@\n+new\n",
            "--- a/value.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n",
            "diff --git a/value.txt b/value.txt\nrename from value.txt\nrename to renamed.txt\n",
            "diff --git a/value.txt b/value.txt\nGIT binary patch\nliteral 4\n",
            "--- a/value.txt\n+++ b/value.txt\nindex 1111111..2222222 100644\n@@ -1 +1 @@\n-old\n+new\n",
            "--- a/value.txt\n+++ b/value.txt\n",
            "--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-old\n+new\ntrailing junk\n",
            "@@ -1 +1 @@\n-old\n+new\n--- a/other.txt\n+++ b/other.txt\n",
        ];
        for patch in invalid {
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": "value.txt", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::InvalidInvocation),
                "patch shape should be rejected"
            );
            assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
            assert!(
                tokio::fs::symlink_metadata(root.join("other.txt"))
                    .await
                    .is_err()
            );
        }

        let mismatched = "--- a/other.txt\n+++ b/other.txt\n@@ -1 +1 @@\n-old\n+new\n";
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": mismatched})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn patch_source_and_result_limits_are_enforced_without_writing() {
        let (base, _, tool) = fixture("limits").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        tokio::fs::write(&target, "old\n").await.unwrap();

        for patch in [
            String::new(),
            "x".repeat(MAX_PATCH_BYTES + 1),
            "@@ -1 +1 @@\n-old\n+new\0\n".to_owned(),
        ] {
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": "value.txt", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::InvalidInvocation)
            );
        }
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "old\n");

        tokio::fs::write(root.join("too-large"), "a".repeat(MAX_PATCH_BYTES + 1))
            .await
            .unwrap();
        tokio::fs::write(root.join("binary"), [0xff, 0xfe])
            .await
            .unwrap();
        tokio::fs::write(root.join("nul"), b"text\0value")
            .await
            .unwrap();
        tokio::fs::create_dir(root.join("directory")).await.unwrap();
        let patch = "@@ -1 +1 @@\n-a\n+b\n";
        for path in ["too-large", "binary", "nul", "directory", "missing"] {
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": path, "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::Failed)
            );
        }

        let at_limit = format!("a\n{}", "x".repeat(MAX_PATCH_BYTES - 2));
        tokio::fs::write(&target, &at_limit).await.unwrap();
        let too_large_result = "@@ -1 +1,2 @@\n a\n+b\n";
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": too_large_result})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), at_limit);

        let at_limit_patch = "@@ -1 +1 @@\n-a\n+b\n";
        assert_eq!(
            execute(&tool, json!({"path": "value.txt", "patch": at_limit_patch}))
                .await
                .unwrap(),
            format!("patched {MAX_PATCH_BYTES} bytes to {MAX_PATCH_BYTES} bytes at value.txt")
        );
        assert_eq!(
            tokio::fs::metadata(&target).await.unwrap().len(),
            MAX_PATCH_BYTES as u64
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn matching_crlf_patch_preserves_crlf_content() {
        let (base, _, tool) = fixture("crlf").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value.txt"), b"one\r\ntwo\r\n")
            .await
            .unwrap();
        let patch = "@@ -1,2 +1,2 @@\r\n one\r\n-two\r\n+TWO\r\n";
        execute(&tool, json!({"path": "value.txt", "patch": patch}))
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(root.join("value.txt")).await.unwrap(),
            b"one\r\nTWO\r\n"
        );
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn paths_are_contained_final_symlinks_are_rejected_and_output_is_single_line() {
        use std::os::unix::fs::symlink;

        let (base, _, tool) = fixture("paths").await;
        let root = base.join("root");
        let control_path = "control-你-\r-\n-\t-\u{1b}.txt";
        tokio::fs::write(root.join(control_path), "old\n")
            .await
            .unwrap();
        let patch = "@@ -1 +1 @@\n-old\n+new\n";
        let output = execute(&tool, json!({"path": control_path, "patch": patch}))
            .await
            .unwrap();
        assert_eq!(
            output,
            "patched 4 bytes to 4 bytes at control-你-\\r-\\n-\\t-\\u{1b}.txt"
        );
        assert_eq!(output.lines().count(), 1);
        assert!(!output.chars().any(char::is_control));

        tokio::fs::write(root.join("real"), "old\n").await.unwrap();
        symlink("real", root.join("link")).unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "link", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(
            tokio::fs::read_to_string(root.join("real")).await.unwrap(),
            "old\n"
        );
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "../escape", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn cancellation_and_deadline_before_commit_do_not_modify_the_source() {
        let (base, _, tool) = fixture("control").await;
        let root = base.join("root");
        tokio::fs::write(root.join("cancelled"), "old\n")
            .await
            .unwrap();
        tokio::fs::write(root.join("timed-out"), "old\n")
            .await
            .unwrap();
        let tool = Arc::new(tool);
        let patch = "@@ -1 +1 @@\n-old\n+new\n";

        let cancellation = CancellationToken::new();
        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "cancelled"));
        block_next_io(Arc::clone(&gate));
        let cancelled_tool = Arc::clone(&tool);
        let cancelled_context = context(
            cancellation.clone(),
            Instant::now() + Duration::from_secs(5),
        );
        let cancelled = tokio::spawn(async move {
            cancelled_tool
                .execute(
                    invocation(json!({"path": "cancelled", "patch": patch})),
                    cancelled_context,
                )
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        cancellation.cancel();
        assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));
        assert_eq!(
            tokio::fs::read_to_string(root.join("cancelled"))
                .await
                .unwrap(),
            "old\n"
        );

        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "timed-out"));
        block_next_io(Arc::clone(&gate));
        let deadline_tool = Arc::clone(&tool);
        let deadline = tokio::spawn(async move {
            deadline_tool
                .execute(
                    invocation(json!({"path": "timed-out", "patch": patch})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_millis(25),
                    ),
                )
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        assert_eq!(deadline.await.unwrap(), Err(ToolError::TimedOut));
        assert_eq!(
            tokio::fs::read_to_string(root.join("timed-out"))
                .await
                .unwrap(),
            "old\n"
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn atomic_failures_and_unknown_outcomes_never_report_success() {
        let (base, workspace, tool) = fixture("atomic-errors").await;
        let root = base.join("root");
        let target = root.join("value");
        tokio::fs::write(&target, "old\n").await.unwrap();
        let resolved = workspace.resolve_for_write("value").await.unwrap();
        let patch = "@@ -1 +1 @@\n-old\n+new\n";

        fail_next_before_rename(resolved.clone());
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "old\n");

        fail_next_directory_sync(resolved.parent().unwrap().to_path_buf());
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "new\n");
        cleanup(&base).await;
    }
}
