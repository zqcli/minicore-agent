use super::apply::{
    ApplyStats, HunkRange, PatchContent, PatchLineKind, ResolvedHunk, add, apply_resolved_hunks,
    split_source_lines, sub,
};
use minicore_runtime::tools::ToolError;
use std::path::{Component, Path};

pub(super) const NO_NEWLINE_AT_END: &str = "\\ No newline at end of file";

pub(super) fn apply_unified_patch_with_stats(
    path: &str,
    source: &str,
    patch: &str,
) -> Result<(String, ApplyStats), ToolError> {
    let source_lines = split_source_lines(source);
    let mut parser = PatchLines::new(patch);
    let mut first = parser.next().ok_or(ToolError::InvalidInvocation)?;
    if first.starts_with("diff --git ") || first.starts_with("index ") {
        if first.starts_with("diff --git ") {
            first = parser.next().ok_or(ToolError::InvalidInvocation)?;
        }
        if first.starts_with("index ") {
            first = parser.next().ok_or(ToolError::InvalidInvocation)?;
        }
        // Git metadata is informational; the ---/+++ paths still authorize
        // the single file this patch may change.
        if !first.starts_with("--- ") {
            return Err(ToolError::InvalidInvocation);
        }
    }
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
        match parser.next() {
            Some(line) if strip_transport_ending(line).starts_with("@@ ") => {
                next_hunk = line;
            }
            Some(line) if strip_transport_ending(line).is_empty() => {
                parser.finish()?;
                break;
            }
            Some(_) => return Err(ToolError::InvalidInvocation),
            None => break,
        }
    }
    apply_resolved_hunks(source, &source_lines, &resolved, parser.steps, 0)
}

pub(super) struct PatchLines<'a> {
    lines: std::str::SplitInclusive<'a, char>,
    peeked: Option<&'a str>,
    pub(super) steps: usize,
}

impl<'a> PatchLines<'a> {
    pub(super) fn new(patch: &'a str) -> Self {
        Self {
            lines: patch.split_inclusive('\n'),
            peeked: None,
            steps: 0,
        }
    }

    pub(super) fn peek(&mut self) -> Option<&'a str> {
        if self.peeked.is_none() {
            self.peeked = self.lines.next();
        }
        self.peeked
    }

    pub(super) fn finish(&mut self) -> Result<(), ToolError> {
        while let Some(line) = self.next() {
            if !strip_transport_ending(line).is_empty() {
                return Err(ToolError::InvalidInvocation);
            }
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Option<&'a str> {
        let line = self.peeked.take().or_else(|| self.lines.next())?;
        self.steps = self.steps.saturating_add(1);
        Some(line)
    }
}

pub(super) fn make_hunk_range(index: usize, count: usize) -> Result<HunkRange, ToolError> {
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

pub(super) fn parse_patch_content(line: &str) -> Result<PatchContent<'_>, ToolError> {
    let (kind, text) = if line.is_empty() {
        (PatchLineKind::Context, "")
    } else if let Some(text) = line.strip_prefix(' ') {
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

pub(super) fn parse_hunk_header(header: &str) -> Result<(HunkRange, HunkRange), ToolError> {
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

pub(super) fn validate_header_path(
    header: &str,
    tool_path: &str,
    prefix: &str,
) -> Result<(), ToolError> {
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

pub(super) fn strip_transport_ending(line: &str) -> &str {
    let Some(line) = line.strip_suffix('\n') else {
        return line;
    };
    line.strip_suffix('\r').unwrap_or(line)
}

pub(super) fn consume_no_newline(
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
