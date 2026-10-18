use std::collections::HashMap;
use std::path::{Component, Path};
use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::Workspace;
use crate::tool_data::{ToolData, ToolRef};

use super::file_change::{commit_text, read_source_before};
use super::{
    MAX_PATCH_BYTES, emit_phase, escape_control_characters, map_workspace_error, run_controlled,
};

const TOOL_NAME: &str = "apply_patch";
const CODEX_BEGIN_PATCH: &str = "*** Begin Patch";
const CODEX_END_PATCH: &str = "*** End Patch";
const CODEX_UPDATE_FILE: &str = "*** Update File: ";
const CODEX_END_OF_FILE: &str = "*** End of File";
const NO_NEWLINE_AT_END: &str = "\\ No newline at end of file";
// Bound substring compatibility matching across all Codex anchor and hunk queries;
// this deliberately does not claim an unbounded implementation of every Codex locator.
const MAX_CODEX_MATCH_VISITS: usize = MAX_PATCH_BYTES.saturating_mul(8);

pub(crate) struct ApplyPatchTool {
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
    pub(crate) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("apply_patch is a valid tool name"),
            "Apply one complete patch to one existing workspace file. Standard unified diffs are single-file only; Codex envelopes must contain exactly one *** Update File operation. Add File, Delete File, Move to, and multifile envelopes are rejected.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative path of the one existing file; a Codex Update File path must match it."
                    },
                    "patch": {
                        "type": "string",
                        "minLength": 1,
                        "description": concat!(
                            "Single-file standard unified diff or one-file Codex *** Begin Patch envelope. ",
                            "Example: *** Begin Patch\n",
                            "*** Update File: value.txt\n@@\n-old\n+new\n*** End Patch. ",
                            "Add/Delete/Move/multifile forms are rejected; limited to 512 KiB."
                        )
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

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        self.execute_bound(invocation, context, None, None)
    }
}

impl ApplyPatchTool {
    pub(crate) fn execute_bound(
        &self,
        invocation: ToolInvocation,
        context: ToolContext,
        tool_ref: Option<ToolRef>,
        tool_data: Option<Arc<ToolData>>,
    ) -> ToolFuture<'_> {
        Box::pin(async move {
            let input: ApplyPatchInput = super::decode_input(&self.spec, &invocation, &context)?;
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
                #[cfg(test)]
                if let Some(gate) = super::IO_GATES.take(&(TOOL_NAME, input.path.clone())) {
                    gate.pass().await;
                }
                emit_phase(&context, "reading");
                let (source, before) = match read_source_before(
                    &self.workspace,
                    &input.path,
                    MAX_PATCH_BYTES,
                    tool_ref.is_some() && tool_data.is_some(),
                )
                .await
                {
                    Ok(source) => source,
                    Err(error) => return super::file_change::source_failure(error),
                };
                emit_phase(&context, "matching");
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
                emit_phase(&context, "committing");
                commit_text(
                    &self.workspace,
                    tool_ref.as_ref(),
                    tool_data.as_deref(),
                    &input.path,
                    before,
                    result.as_bytes(),
                )
                .await?;
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
    context_new_ending: Option<LineEnding>,
}

#[derive(Clone, Copy)]
struct HunkRange {
    index: usize,
    count: usize,
    end: usize,
}

enum CodexLocator<'a> {
    Range { old: HunkRange, new: HunkRange },
    Anchor(Option<&'a str>),
}

struct CodexHunk<'a> {
    locator: CodexLocator<'a>,
    contents: Vec<PatchContent<'a>>,
    end_of_file: bool,
}

struct ResolvedHunk<'a> {
    old: HunkRange,
    new: HunkRange,
    contents: Vec<PatchContent<'a>>,
}

struct CodexSource<'source, 'text> {
    lines: &'source [SourceLine<'text>],
    index: &'source HashMap<&'text str, Vec<usize>>,
}

#[allow(dead_code)]
#[derive(Default)]
struct ApplyStats {
    source_lines: usize,
    source_line_visits: usize,
    patch_lines: usize,
    patch_content_visits: usize,
    codex_match_visits: usize,
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
        self.line_count = add(self.line_count, 1)?;
        self.previous_had_no_newline = ending == LineEnding::None;
        Ok(())
    }
}

fn apply_single_file_patch_with_stats(
    path: &str,
    source: &str,
    patch: &str,
) -> Result<(String, ApplyStats), ToolError> {
    if patch.is_empty() || patch.len() > MAX_PATCH_BYTES || patch.contains('\0') {
        return Err(ToolError::InvalidInvocation);
    }
    let first = patch
        .split_inclusive('\n')
        .next()
        .ok_or(ToolError::InvalidInvocation)?;
    if strip_transport_ending(first) == CODEX_BEGIN_PATCH {
        return apply_codex_patch_with_stats(path, source, patch);
    }
    apply_unified_patch_with_stats(path, source, patch)
}

fn apply_codex_patch_with_stats(
    path: &str,
    source: &str,
    patch: &str,
) -> Result<(String, ApplyStats), ToolError> {
    let source_lines = split_source_lines(source);
    let (hunks, patch_lines, codex_match_visits) =
        parse_and_resolve_codex_patch(path, patch, &source_lines)?;
    apply_resolved_hunks(
        source,
        &source_lines,
        &hunks,
        patch_lines,
        codex_match_visits,
    )
}

fn apply_unified_patch_with_stats(
    path: &str,
    source: &str,
    patch: &str,
) -> Result<(String, ApplyStats), ToolError> {
    let source_lines = split_source_lines(source);
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

    let mut resolved = Vec::new();
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
        resolved.push(ResolvedHunk {
            old: old_range,
            new: new_range,
            contents,
        });
        hunk_count = add(hunk_count, 1)?;

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
    apply_resolved_hunks(source, &source_lines, &resolved, parser.steps, 0)
}

fn apply_resolved_hunks(
    source: &str,
    source_lines: &[SourceLine<'_>],
    hunks: &[ResolvedHunk<'_>],
    patch_lines: usize,
    codex_match_visits: usize,
) -> Result<(String, ApplyStats), ToolError> {
    let preferred_ending = preferred_line_ending(source_lines);
    let mut stats = ApplyStats {
        source_lines: source_lines.len(),
        patch_lines,
        codex_match_visits,
        ..ApplyStats::default()
    };
    let mut source_cursor = 0usize;
    let mut builder = ResultBuilder::new(source.len());
    let mut previous_old_end = 0usize;
    let mut previous_new_end = 0usize;

    for hunk in hunks {
        if hunk.old.index < previous_old_end || hunk.new.index < previous_new_end {
            return Err(ToolError::InvalidInvocation);
        }
        previous_old_end = hunk.old.end;
        previous_new_end = hunk.new.end;
        while source_cursor < hunk.old.index {
            append_source_line(source_lines, &mut source_cursor, &mut builder, &mut stats)?;
        }
        if source_cursor != hunk.old.index || builder.line_count != hunk.new.index {
            return Err(ToolError::Failed);
        }
        for content in hunk.contents.iter().copied() {
            apply_patch_content(
                content,
                source_lines,
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
        if source_cursor != hunk.old.end || builder.line_count != hunk.new.end {
            return Err(ToolError::Failed);
        }
    }
    while source_cursor < source_lines.len() {
        append_source_line(source_lines, &mut source_cursor, &mut builder, &mut stats)?;
    }
    if stats.source_line_visits != stats.source_lines {
        return Err(ToolError::Internal);
    }
    Ok((builder.value, stats))
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

fn parse_and_resolve_codex_patch<'a>(
    path: &str,
    patch: &'a str,
    source: &[SourceLine<'a>],
) -> Result<(Vec<ResolvedHunk<'a>>, usize, usize), ToolError> {
    let (codex_hunks, patch_lines) = parse_codex_patch(path, patch)?;
    let total_hunks = codex_hunks.len();
    let mut resolved = Vec::with_capacity(total_hunks);
    let mut source_cursor = 0usize;
    let mut output_cursor = 0usize;
    let mut previous_start = None;
    let mut match_visits = 0usize;
    let preferred_ending = preferred_line_ending(source);
    let source_index = codex_source_index(source);
    let codex_source = CodexSource {
        lines: source,
        index: &source_index,
    };

    for (index, hunk) in codex_hunks.into_iter().enumerate() {
        if hunk.end_of_file && index + 1 != total_hunks {
            return Err(ToolError::InvalidInvocation);
        }
        let header_only = hunk.contents.is_empty();
        let old_contents = hunk
            .contents
            .iter()
            .copied()
            .filter(|content| !matches!(content.kind, PatchLineKind::Add))
            .collect::<Vec<_>>();
        let (old_count, new_count) = codex_content_counts(&hunk.contents)?;
        let start = if header_only {
            let CodexLocator::Anchor(Some(anchor)) = &hunk.locator else {
                return Err(ToolError::InvalidInvocation);
            };
            find_unique_codex_anchor(source, source_cursor, anchor, &mut match_visits)?
                .ok_or(ToolError::Failed)?
        } else {
            locate_codex_hunk(
                &hunk.locator,
                &old_contents,
                (old_count, new_count),
                hunk.end_of_file,
                &codex_source,
                source_cursor,
                &mut match_visits,
            )?
        };
        if start < source_cursor || (!header_only && previous_start == Some(start)) {
            return Err(ToolError::InvalidInvocation);
        }
        if let CodexLocator::Range { new, .. } = &hunk.locator {
            let skipped = sub(start, source_cursor)?;
            let expected_new = add(output_cursor, skipped)?;
            if new.index != expected_new {
                return Err(ToolError::InvalidInvocation);
            }
        }
        let skipped = sub(start, source_cursor)?;
        output_cursor = add(output_cursor, skipped)?;
        source_cursor = start;
        if header_only {
            continue;
        }

        let old_end = add(start, old_count)?;
        let mut contents = hunk.contents;
        mark_codex_source_endings(&mut contents, source, start, preferred_ending)?;
        if index + 1 == total_hunks
            && source
                .last()
                .is_some_and(|line| line.ending == LineEnding::None)
            && old_end == source.len()
        {
            mark_codex_result_no_newline(&mut contents, new_count);
        }

        let old = make_hunk_range(start, old_count)?;
        let new = make_hunk_range(output_cursor, new_count)?;
        resolved.push(ResolvedHunk { old, new, contents });
        source_cursor = old_end;
        output_cursor = add(output_cursor, new_count)?;
        previous_start = Some(start);
    }

    Ok((resolved, patch_lines, match_visits))
}

fn parse_codex_patch<'a>(
    path: &str,
    patch: &'a str,
) -> Result<(Vec<CodexHunk<'a>>, usize), ToolError> {
    let mut parser = PatchLines::new(patch);
    let begin = parser.next().ok_or(ToolError::InvalidInvocation)?;
    if strip_transport_ending(begin) != CODEX_BEGIN_PATCH {
        return Err(ToolError::InvalidInvocation);
    }
    let update = parser.next().ok_or(ToolError::InvalidInvocation)?;
    let header_path = strip_transport_ending(update)
        .strip_prefix(CODEX_UPDATE_FILE)
        .ok_or(ToolError::InvalidInvocation)?;
    validate_header_path(header_path, path, "")?;

    let mut hunks = Vec::new();
    loop {
        let raw = parser.next().ok_or(ToolError::InvalidInvocation)?;
        let line = strip_transport_ending(raw);
        if line == CODEX_END_PATCH {
            if hunks
                .last()
                .is_none_or(|hunk: &CodexHunk<'_>| hunk.contents.is_empty())
                || parser.next().is_some()
            {
                return Err(ToolError::InvalidInvocation);
            }
            return Ok((hunks, parser.steps));
        }
        if !line.starts_with("@@") {
            return Err(ToolError::InvalidInvocation);
        }
        let locator = parse_codex_locator(line)?;
        let mut contents = Vec::new();
        let mut end_of_file = false;
        loop {
            let raw = parser.peek().ok_or(ToolError::InvalidInvocation)?;
            let line = strip_transport_ending(raw);
            if line == CODEX_END_PATCH || line.starts_with("@@") {
                break;
            }
            if line == CODEX_END_OF_FILE {
                parser.next();
                end_of_file = true;
                break;
            }
            if line.starts_with("***") || line == NO_NEWLINE_AT_END {
                return Err(ToolError::InvalidInvocation);
            }
            let raw = parser.next().ok_or(ToolError::InvalidInvocation)?;
            let line = strip_transport_ending(raw);
            let mut content = parse_patch_content(line)?;
            consume_no_newline(&mut parser, &mut content)?;
            contents.push(content);
        }
        if contents.is_empty()
            && (end_of_file || !matches!(&locator, CodexLocator::Anchor(Some(_))))
        {
            return Err(ToolError::InvalidInvocation);
        }
        hunks.push(CodexHunk {
            locator,
            contents,
            end_of_file,
        });
    }
}

fn parse_codex_locator(line: &str) -> Result<CodexLocator<'_>, ToolError> {
    let rest = line
        .strip_prefix("@@")
        .ok_or(ToolError::InvalidInvocation)?;
    let header = rest.trim();
    if header.is_empty() {
        return Ok(CodexLocator::Anchor(None));
    }
    if header.starts_with('-') {
        let (old, new) = parse_hunk_header(line)?;
        return Ok(CodexLocator::Range { old, new });
    }
    if header.contains('\0') {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(CodexLocator::Anchor(Some(header)))
}

fn codex_content_counts(contents: &[PatchContent<'_>]) -> Result<(usize, usize), ToolError> {
    let mut old_count = 0usize;
    let mut new_count = 0usize;
    for content in contents {
        let (old, new) = content.kind.delta();
        old_count = add(old_count, old)?;
        new_count = add(new_count, new)?;
    }
    Ok((old_count, new_count))
}

fn locate_codex_hunk(
    locator: &CodexLocator<'_>,
    old_contents: &[PatchContent<'_>],
    (old_count, new_count): (usize, usize),
    end_of_file: bool,
    codex_source: &CodexSource<'_, '_>,
    search_start: usize,
    match_visits: &mut usize,
) -> Result<usize, ToolError> {
    let source = codex_source.lines;
    match locator {
        CodexLocator::Range { old, new } => {
            if old.count != old_count || new.count != new_count {
                return Err(ToolError::InvalidInvocation);
            }
            let end = add(old.index, old.count)?;
            if end > source.len() || (end_of_file && end != source.len()) {
                return Err(ToolError::Failed);
            }
            if !codex_contents_match_at(old_contents, source, old.index) {
                return Err(ToolError::Failed);
            }
            Ok(old.index)
        }
        CodexLocator::Anchor(anchor) => {
            if end_of_file {
                let start = source
                    .len()
                    .checked_sub(old_count)
                    .ok_or(ToolError::Failed)?;
                if !codex_contents_match_at(old_contents, source, start) {
                    return Err(ToolError::Failed);
                }
                return Ok(start);
            }
            if old_count == 0 {
                let Some(anchor) = anchor else {
                    return Err(ToolError::Failed);
                };
                let Some(anchor_line) =
                    find_unique_codex_anchor(source, search_start, anchor, match_visits)?
                else {
                    return Err(ToolError::Failed);
                };
                return anchor_line
                    .checked_add(1)
                    .ok_or(ToolError::InvalidInvocation);
            }

            let match_start = if let Some(anchor) = anchor {
                let Some(anchor_line) =
                    find_unique_codex_anchor(source, search_start, anchor, match_visits)?
                else {
                    return Err(ToolError::Failed);
                };
                anchor_line
            } else {
                search_start
            };
            let Some(start) = find_unique_codex_match(
                old_contents,
                source,
                codex_source.index,
                match_start,
                match_visits,
            )?
            else {
                return Err(ToolError::Failed);
            };
            Ok(start)
        }
    }
}

fn find_unique_codex_anchor(
    source: &[SourceLine<'_>],
    start: usize,
    anchor: &str,
    visits: &mut usize,
) -> Result<Option<usize>, ToolError> {
    let mut found = None;
    for (index, line) in source.iter().enumerate().skip(start) {
        *visits = visits.checked_add(1).ok_or(ToolError::Internal)?;
        if *visits > MAX_CODEX_MATCH_VISITS {
            return Err(ToolError::InvalidInvocation);
        }
        if line.text.contains(anchor) {
            if found.is_some() {
                return Err(ToolError::InvalidInvocation);
            }
            found = Some(index);
        }
    }
    Ok(found)
}

fn codex_source_index<'a>(source: &[SourceLine<'a>]) -> HashMap<&'a str, Vec<usize>> {
    let mut index = HashMap::new();
    for (line_index, line) in source.iter().enumerate() {
        index
            .entry(line.text)
            .or_insert_with(Vec::new)
            .push(line_index);
    }
    index
}

fn find_unique_codex_match<'a>(
    pattern: &[PatchContent<'a>],
    source: &[SourceLine<'a>],
    source_index: &HashMap<&'a str, Vec<usize>>,
    start: usize,
    visits: &mut usize,
) -> Result<Option<usize>, ToolError> {
    if pattern.is_empty() {
        return Ok(None);
    }
    let mut no_newline_offset = None;
    for (offset, content) in pattern.iter().enumerate() {
        if content.old_no_newline && no_newline_offset.replace(offset).is_some() {
            return Err(ToolError::InvalidInvocation);
        }
    }

    let positions = source_index
        .get(pattern[0].text)
        .map_or(&[][..], Vec::as_slice);
    let first = positions
        .binary_search(&start)
        .unwrap_or_else(|index| index);
    let candidates = &positions[first..];
    if candidates.is_empty() {
        return Ok(None);
    }
    if candidates.len() == 1 {
        *visits = visits.checked_add(1).ok_or(ToolError::Internal)?;
        if *visits > MAX_CODEX_MATCH_VISITS {
            return Err(ToolError::InvalidInvocation);
        }
        return Ok(codex_contents_match_at(pattern, source, candidates[0]).then_some(candidates[0]));
    }

    let prefix = codex_pattern_prefix(pattern);
    let mut matched = 0usize;
    let mut found = None;
    for (index, line) in source.iter().enumerate().skip(start) {
        *visits = visits.checked_add(1).ok_or(ToolError::Internal)?;
        if *visits > MAX_CODEX_MATCH_VISITS {
            return Err(ToolError::InvalidInvocation);
        }
        while matched > 0 && pattern[matched].text != line.text {
            matched = prefix[matched - 1];
        }
        if pattern[matched].text == line.text {
            matched += 1;
        }
        if matched == pattern.len() {
            let candidate = index + 1 - pattern.len();
            if no_newline_offset.is_none_or(|offset| {
                source
                    .get(candidate + offset)
                    .is_some_and(|line| line.ending == LineEnding::None)
            }) {
                if found.is_some() {
                    return Err(ToolError::InvalidInvocation);
                }
                found = Some(candidate);
            }
            matched = prefix[matched - 1];
        }
    }
    Ok(found)
}

fn codex_pattern_prefix(pattern: &[PatchContent<'_>]) -> Vec<usize> {
    let mut prefix = vec![0usize; pattern.len()];
    let mut matched = 0usize;
    for index in 1..pattern.len() {
        while matched > 0 && pattern[matched].text != pattern[index].text {
            matched = prefix[matched - 1];
        }
        if pattern[matched].text == pattern[index].text {
            matched += 1;
        }
        prefix[index] = matched;
    }
    prefix
}

fn codex_contents_match_at(
    contents: &[PatchContent<'_>],
    source: &[SourceLine<'_>],
    start: usize,
) -> bool {
    for (source_index, content) in (start..).zip(contents.iter()) {
        let Some(line) = source.get(source_index) else {
            return false;
        };
        if line.text != content.text || (content.old_no_newline && line.ending != LineEnding::None)
        {
            return false;
        }
    }
    true
}

fn mark_codex_source_endings(
    contents: &mut [PatchContent<'_>],
    source: &[SourceLine<'_>],
    start: usize,
    preferred_ending: LineEnding,
) -> Result<(), ToolError> {
    let mut source_lines = source.iter().skip(start);
    for index in 0..contents.len() {
        if matches!(contents[index].kind, PatchLineKind::Add) {
            continue;
        }
        let line = source_lines.next().ok_or(ToolError::Failed)?;
        if line.ending == LineEnding::None {
            contents[index].old_no_newline = true;
            if matches!(contents[index].kind, PatchLineKind::Context)
                && !contents[index].new_no_newline
                && contents[index + 1..]
                    .iter()
                    .any(|content| !matches!(content.kind, PatchLineKind::Remove))
            {
                contents[index].context_new_ending = Some(preferred_ending);
            }
        }
    }
    Ok(())
}

fn mark_codex_result_no_newline(contents: &mut [PatchContent<'_>], new_count: usize) {
    if new_count == 0 {
        return;
    }
    if let Some(content) = contents
        .iter_mut()
        .rev()
        .find(|content| !matches!(content.kind, PatchLineKind::Remove))
    {
        content.new_no_newline = true;
    }
}

fn make_hunk_range(index: usize, count: usize) -> Result<HunkRange, ToolError> {
    let end = add(index, count)?;
    Ok(HunkRange { index, count, end })
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
        if line == NO_NEWLINE_AT_END {
            return Err(ToolError::InvalidInvocation);
        }
        let mut content = parse_patch_content(line)?;
        let (old, new) = content.kind.delta();
        old_count = add(old_count, old)?;
        new_count = add(new_count, new)?;
        if old_count > expected_old || new_count > expected_new {
            return Err(ToolError::InvalidInvocation);
        }
        consume_no_newline(parser, &mut content)?;
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
            let ending = content.context_new_ending.unwrap_or(line.ending);
            builder.append(line.text, ending)?;
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
        context_new_ending: None,
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
    let index = if count == 0 { start } else { sub(start, 1)? };
    let end = add(index, count)?;
    add(start, count)?;
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

fn add(left: usize, right: usize) -> Result<usize, ToolError> {
    left.checked_add(right).ok_or(ToolError::InvalidInvocation)
}
fn sub(left: usize, right: usize) -> Result<usize, ToolError> {
    left.checked_sub(right).ok_or(ToolError::InvalidInvocation)
}
impl PatchLineKind {
    fn delta(self) -> (usize, usize) {
        match self {
            Self::Context => (1, 1),
            Self::Remove => (1, 0),
            Self::Add => (0, 1),
        }
    }
}
fn consume_no_newline(
    parser: &mut PatchLines<'_>,
    content: &mut PatchContent<'_>,
) -> Result<(), ToolError> {
    if parser
        .peek()
        .is_some_and(|line| strip_transport_ending(line) == NO_NEWLINE_AT_END)
    {
        parser.next();
        mark_no_newline(content)?;
        if parser
            .peek()
            .is_some_and(|line| strip_transport_ending(line) == NO_NEWLINE_AT_END)
        {
            return Err(ToolError::InvalidInvocation);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
