use super::MAX_PATCH_BYTES;
use super::codex::{CODEX_BEGIN_PATCH, apply_codex_patch_with_stats};
use super::unified::{apply_unified_patch_with_stats, strip_transport_ending};
use minicore_runtime::tools::ToolError;

pub(super) fn apply_single_file_patch(
    path: &str,
    source: &str,
    patch: &str,
) -> Result<String, ToolError> {
    apply_single_file_patch_with_stats(path, source, patch).map(|(result, _)| result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LineEnding {
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
pub(super) struct SourceLine<'a> {
    pub(super) text: &'a str,
    pub(super) ending: LineEnding,
}

#[derive(Clone, Copy)]
pub(super) enum PatchLineKind {
    Context,
    Remove,
    Add,
}

#[derive(Clone, Copy)]
pub(super) struct PatchContent<'a> {
    pub(super) kind: PatchLineKind,
    pub(super) text: &'a str,
    pub(super) old_no_newline: bool,
    pub(super) new_no_newline: bool,
    pub(super) context_new_ending: Option<LineEnding>,
}

#[derive(Clone, Copy)]
pub(super) struct HunkRange {
    pub(super) index: usize,
    pub(super) count: usize,
    pub(super) end: usize,
}

pub(super) struct ResolvedHunk<'a> {
    pub(super) old: HunkRange,
    pub(super) new: HunkRange,
    pub(super) contents: Vec<PatchContent<'a>>,
}

#[allow(dead_code)]
#[derive(Default)]
pub(super) struct ApplyStats {
    pub(super) source_lines: usize,
    pub(super) source_line_visits: usize,
    pub(super) patch_lines: usize,
    pub(super) patch_content_visits: usize,
    pub(super) codex_match_visits: usize,
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

pub(super) fn apply_single_file_patch_with_stats(
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

pub(super) fn apply_resolved_hunks(
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

pub(super) fn split_source_lines(source: &str) -> Vec<SourceLine<'_>> {
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

pub(super) fn preferred_line_ending(lines: &[SourceLine<'_>]) -> LineEnding {
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

pub(super) fn add(left: usize, right: usize) -> Result<usize, ToolError> {
    left.checked_add(right).ok_or(ToolError::InvalidInvocation)
}
pub(super) fn sub(left: usize, right: usize) -> Result<usize, ToolError> {
    left.checked_sub(right).ok_or(ToolError::InvalidInvocation)
}
impl PatchLineKind {
    pub(super) fn delta(self) -> (usize, usize) {
        match self {
            Self::Context => (1, 1),
            Self::Remove => (1, 0),
            Self::Add => (0, 1),
        }
    }
}
