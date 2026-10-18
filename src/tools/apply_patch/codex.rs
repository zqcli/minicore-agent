use super::MAX_PATCH_BYTES;
use super::apply::{
    ApplyStats, HunkRange, LineEnding, PatchContent, PatchLineKind, ResolvedHunk, SourceLine, add,
    apply_resolved_hunks, preferred_line_ending, split_source_lines, sub,
};
use super::unified::{
    NO_NEWLINE_AT_END, PatchLines, consume_no_newline, make_hunk_range, parse_hunk_header,
    parse_patch_content, strip_transport_ending, validate_header_path,
};
use minicore_runtime::tools::ToolError;
use std::collections::HashMap;

pub(super) const CODEX_BEGIN_PATCH: &str = "*** Begin Patch";
const CODEX_END_PATCH: &str = "*** End Patch";
const CODEX_UPDATE_FILE: &str = "*** Update File: ";
const CODEX_END_OF_FILE: &str = "*** End of File";
// Bound substring compatibility matching across all Codex anchor and hunk queries;
// this deliberately does not claim an unbounded implementation of every Codex locator.
const MAX_CODEX_MATCH_VISITS: usize = MAX_PATCH_BYTES.saturating_mul(8);

enum CodexLocator<'a> {
    Range { old: HunkRange, new: HunkRange },
    Anchor(Option<&'a str>),
}

struct CodexHunk<'a> {
    locator: CodexLocator<'a>,
    contents: Vec<PatchContent<'a>>,
    end_of_file: bool,
}

struct CodexSource<'source, 'text> {
    lines: &'source [SourceLine<'text>],
    index: &'source HashMap<&'text str, Vec<usize>>,
}

pub(super) fn apply_codex_patch_with_stats(
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
            {
                return Err(ToolError::InvalidInvocation);
            }
            parser.finish()?;
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
