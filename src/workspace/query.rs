//! Bounded, session-scoped reads of Workspace files for read-only clients.
//!
//! One read resolves a workspace-relative regular file, performs one bounded
//! read of its bytes, and returns the raw text without display line numbers.
//! The binary check, the whole-file revision, and the returned page all
//! describe those same bytes, so a response can never combine an old hash with
//! newer content. Nothing here writes history, starts a model call, or mutates
//! the file.

use crate::workspace::scan::{DEFAULT_RESULT_BYTES, validate_result_bytes};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::ids::SessionId;
use crate::tools::{DEFAULT_READ_LIMIT, MAX_READ_BYTES, MAX_READ_LINES};
use crate::workspace::Workspace;

/// Wall-clock budget for one workspace read, matching the other read queries.
pub(crate) const WORKSPACE_READ_DEADLINE: Duration = Duration::from_secs(10);
/// Same line window as the read Tool.
const DEFAULT_MAX_LINES: u32 = DEFAULT_READ_LIMIT as u32;
const MAX_MAX_LINES: u32 = MAX_READ_LINES as u32;
/// A result budget smaller than this cannot hold any useful envelope plus
/// content. `workspace.files` and `workspace.search` share the same range.
/// A request path longer than this is rejected before any query slot or
/// serialization work. The generic workspace path checks stay uncapped.
pub(crate) const MAX_PATH_BYTES: usize = 4096;

/// One bounded read of a workspace file.
///
/// `path` is a workspace-relative path of at most 4096 bytes. `start_line` is
/// one-based. `line_byte_offset` is a UTF-8 byte offset inside that line and
/// continues a previously truncated line; it requires `start_line`, must fall
/// on a character boundary inside the line, and cannot exceed the whole-file
/// bound. `max_bytes` is an encoded result budget like `session.read`'s, not a
/// content length. `if_revision` makes the read conditional on the whole-file
/// revision of the bytes this query reads and reports `changed` instead of
/// silently returning a newer file.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceReadRequest {
    pub session_id: SessionId,
    pub path: String,
    #[serde(default)]
    pub start_line: Option<u32>,
    #[serde(default)]
    pub line_byte_offset: Option<u32>,
    #[serde(default)]
    pub max_lines: Option<u32>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub if_revision: Option<String>,
}

impl WorkspaceReadRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        // Cheap lexical and bound checks need no filesystem access, so an
        // invalid request is rejected before the caller reserves a query slot
        // or serializes anything.
        if self.path.len() > MAX_PATH_BYTES {
            return Err(AgentError::InvalidArguments);
        }
        crate::workspace::validate_relative_path(&self.path)
            .map_err(|_| AgentError::InvalidArguments)?;
        if self.start_line == Some(0) {
            return Err(AgentError::InvalidArguments);
        }
        if self.line_byte_offset.is_some() && self.start_line.is_none() {
            return Err(AgentError::InvalidArguments);
        }
        // The whole file is read into one bounded buffer, so a larger offset
        // can never fall inside it.
        if self
            .line_byte_offset
            .is_some_and(|offset| offset > MAX_READ_BYTES as u32)
        {
            return Err(AgentError::InvalidArguments);
        }
        if self
            .max_lines
            .is_some_and(|max_lines| max_lines == 0 || max_lines > MAX_MAX_LINES)
        {
            return Err(AgentError::InvalidArguments);
        }
        validate_result_bytes(self.max_bytes)?;
        if self
            .if_revision
            .as_deref()
            .is_some_and(|value| !crate::read::valid_revision(value))
        {
            return Err(AgentError::InvalidArguments);
        }
        Ok(())
    }
}

/// Raw content of one page plus the metadata a frontend needs to continue.
/// `revision` is the whole-file SHA-256 of the bytes this query read, present
/// only when they form one consistent whole file.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct WorkspaceReadResult {
    pub path: String,
    pub content: String,
    pub start_line: u32,
    pub returned_lines: u32,
    pub revision: Option<String>,
    /// The returned content does not reach the end of the requested range.
    pub truncated: bool,
    /// The page ends inside a line; `next_range` continues it at the byte
    /// offset after the returned content.
    pub line_truncated: bool,
    pub next_range: Option<WorkspaceReadRange>,
    pub encoding: WorkspaceReadEncoding,
    pub status: WorkspaceReadStatus,
    pub file_bytes: u64,
    pub file_modified_unix_ms: Option<u64>,
}

impl fmt::Debug for WorkspaceReadResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceReadResult")
            .field("path_bytes", &self.path.len())
            .field("content_bytes", &self.content.len())
            .field("start_line", &self.start_line)
            .field("returned_lines", &self.returned_lines)
            .field("revision", &self.revision)
            .field("truncated", &self.truncated)
            .field("line_truncated", &self.line_truncated)
            .field("next_range", &self.next_range)
            .field("encoding", &self.encoding)
            .field("status", &self.status)
            .field("file_bytes", &self.file_bytes)
            .field("file_modified_unix_ms", &self.file_modified_unix_ms)
            .finish()
    }
}

impl fmt::Debug for WorkspaceReadRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceReadRequest")
            .field("session_id", &self.session_id)
            .field("path_bytes", &self.path.len())
            .field("start_line", &self.start_line)
            .field("line_byte_offset", &self.line_byte_offset)
            .field("max_lines", &self.max_lines)
            .field("max_bytes", &self.max_bytes)
            .field("if_revision", &self.if_revision)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceReadStatus {
    /// `content` holds the requested page.
    Ok,
    /// The whole file is not valid UTF-8 text (or contains NUL); its bytes are
    /// not returned.
    Binary,
    /// The bytes did not form one consistent whole file (it changed while it
    /// was read) or `if_revision` no longer matches; `content` is empty.
    Changed,
    /// The file is larger than the whole-file bound; `content` is empty and no
    /// partial revision or preview is returned.
    TooLarge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceReadEncoding {
    Utf8,
    Unknown,
}

/// The next page a client may request. `start_line` is one-based and
/// `line_byte_offset` is a UTF-8 byte offset inside that line.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WorkspaceReadRange {
    pub start_line: u32,
    pub line_byte_offset: u32,
}

/// Reads one page of a workspace file.
pub(crate) async fn read(
    workspace: Arc<Workspace>,
    request: WorkspaceReadRequest,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
) -> Result<WorkspaceReadResult, AgentError> {
    let deadline = Instant::now()
        .checked_add(WORKSPACE_READ_DEADLINE)
        .unwrap_or_else(Instant::now);
    read_until(
        workspace,
        request,
        session_cancellation,
        shutdown_cancellation,
        deadline,
    )
    .await
}

pub(crate) async fn read_until(
    workspace: Arc<Workspace>,
    request: WorkspaceReadRequest,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
    deadline: Instant,
) -> Result<WorkspaceReadResult, AgentError> {
    request.validate()?;
    let WorkspaceReadRequest {
        session_id: _,
        path,
        start_line,
        line_byte_offset,
        max_lines,
        max_bytes,
        if_revision,
    } = request;
    let start_line = start_line.unwrap_or(1);
    let offset = line_byte_offset.unwrap_or(0);
    let max_lines = max_lines.unwrap_or(DEFAULT_MAX_LINES);
    let max_bytes = max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);

    // The budget covers metadata and JSON escaping, so a budget that cannot
    // even hold the envelope is an argument error before any IO.
    let envelope = envelope_bytes(&path, max_bytes)?;
    let content_budget = max_bytes - envelope;

    #[cfg(test)]
    if let Some(gate) = READ_GATES.take(&path) {
        with_query_budget(
            gate.pass(),
            &session_cancellation,
            &shutdown_cancellation,
            deadline,
        )
        .await?;
    }

    let capture = with_query_budget(
        workspace.capture_file(&path, MAX_READ_BYTES),
        &session_cancellation,
        &shutdown_cancellation,
        deadline,
    )
    .await?;
    let (bytes, stamp, stable, oversized) = match capture {
        crate::workspace::FileCapture::Complete(snapshot) => {
            (snapshot.bytes, snapshot.stamp, snapshot.stable, false)
        }
        crate::workspace::FileCapture::TooLarge(stamp) => (Vec::new(), stamp, false, true),
        crate::workspace::FileCapture::Missing => return Err(AgentError::Workspace),
        crate::workspace::FileCapture::Error(error) => return Err(map_workspace_error(error)),
    };
    // A stable stamp alone cannot prove a full read (e.g. a procfs file).
    let consistent = stable && bytes.len() as u64 == stamp.len();
    let revision = consistent.then(|| revision_of(&bytes));

    let mut result = WorkspaceReadResult {
        path,
        content: String::new(),
        start_line,
        returned_lines: 0,
        revision,
        truncated: false,
        line_truncated: false,
        next_range: None,
        encoding: WorkspaceReadEncoding::Unknown,
        status: WorkspaceReadStatus::Ok,
        file_bytes: stamp.len(),
        file_modified_unix_ms: stamp.modified_unix_ms(),
    };

    if oversized {
        result.status = WorkspaceReadStatus::TooLarge;
        return Ok(result);
    }
    if !consistent {
        result.status = WorkspaceReadStatus::Changed;
        return Ok(result);
    }
    if let Some(expected) = if_revision.as_deref() {
        let matches = result
            .revision
            .as_deref()
            .is_some_and(|current| current.eq_ignore_ascii_case(expected));
        if !matches {
            result.status = WorkspaceReadStatus::Changed;
            return Ok(result);
        }
    }
    if bytes.contains(&0) || std::str::from_utf8(&bytes).is_err() {
        result.status = WorkspaceReadStatus::Binary;
        return Ok(result);
    }
    let text = std::str::from_utf8(&bytes).expect("validated above");
    result.encoding = WorkspaceReadEncoding::Utf8;

    let Some(line_start) = line_start_offset(text, start_line) else {
        // The requested line does not exist. An offset would address a byte
        // that cannot be there.
        if offset != 0 {
            return Err(AgentError::InvalidArguments);
        }
        return Ok(result);
    };
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let raw_len = line_raw_end(text, line_start) - line_start;
    if offset >= raw_len {
        return Err(AgentError::InvalidArguments);
    }
    let from = line_start + offset;
    if !text.is_char_boundary(from) {
        return Err(AgentError::InvalidArguments);
    }
    let page = select_page(text, from, start_line, offset, max_lines, content_budget)?;
    result.content = page.content;
    result.returned_lines = page.lines;
    result.line_truncated = page.line_truncated;
    result.truncated = page.next.is_some();
    result.next_range = page.next;
    Ok(result)
}

/// Runs one IO step under the query's cancellation and deadline. Each step
/// awaits inside this select, so a pending open, read, or metadata call cannot
/// outlive the Session that owns the Workspace, RPC shutdown, or the deadline.
async fn with_query_budget<F, T>(
    future: F,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, AgentError>
where
    F: Future<Output = T>,
{
    tokio::select! {
        biased;
        _ = session_cancellation.cancelled() => Err(AgentError::QueryLimit),
        _ = shutdown_cancellation.cancelled() => Err(AgentError::QueryLimit),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(AgentError::QueryLimit)
        }
        value = future => Ok(value),
    }
}

/// Serialized size of the result envelope with an empty content, using the
/// widest possible variable fields so the measured value bounds the real
/// response. A budget that cannot hold even this envelope is an argument error.
fn envelope_bytes(path: &str, max_bytes: usize) -> Result<usize, AgentError> {
    let probe = WorkspaceReadResult {
        path: path.to_owned(),
        content: String::new(),
        start_line: u32::MAX,
        returned_lines: u32::MAX,
        revision: Some("f".repeat(64)),
        truncated: false,
        line_truncated: false,
        next_range: Some(WorkspaceReadRange {
            start_line: u32::MAX,
            line_byte_offset: u32::MAX,
        }),
        encoding: WorkspaceReadEncoding::Unknown,
        status: WorkspaceReadStatus::TooLarge,
        file_bytes: u64::MAX,
        file_modified_unix_ms: Some(u64::MAX),
    };
    crate::workspace::scan::envelope_size(&probe, max_bytes)
}

fn revision_of(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    crate::store::digest_hex(hasher)
}

/// Exact serde_json escaped length of `text`, without the surrounding quotes.
fn json_escaped_len(text: &str) -> usize {
    text.chars().map(escaped_char_len).sum()
}

fn escaped_char_len(character: char) -> usize {
    match character {
        '"' | '\\' | '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
        character if (character as u32) < 0x20 => 6,
        character => character.len_utf8(),
    }
}

/// Longest character-boundary prefix of `text` whose escaped length fits.
fn char_prefix_within_budget(text: &str, budget: usize) -> usize {
    let mut used = 0usize;
    let mut end = 0usize;
    for (index, character) in text.char_indices() {
        let cost = escaped_char_len(character);
        if used + cost > budget {
            break;
        }
        used += cost;
        end = index + character.len_utf8();
    }
    end
}

/// Byte offset of the first byte of the one-based `line`, or `None` when the
/// buffer has no such line. A trailing newline does not create an extra line.
fn line_start_offset(buffer: &str, line: u32) -> Option<usize> {
    if buffer.is_empty() {
        return None;
    }
    if line <= 1 {
        return Some(0);
    }
    let mut current = 1u32;
    for (index, byte) in buffer.bytes().enumerate() {
        if byte != b'\n' {
            continue;
        }
        current += 1;
        if current == line {
            let start = index + 1;
            return (start < buffer.len()).then_some(start);
        }
    }
    None
}

/// Raw end (exclusive) of the line starting at `start`, including its newline
/// when it has one.
fn line_raw_end(buffer: &str, start: usize) -> usize {
    match buffer[start..].find('\n') {
        Some(offset) => start + offset + 1,
        None => buffer.len(),
    }
}

struct Page {
    content: String,
    lines: u32,
    line_truncated: bool,
    next: Option<WorkspaceReadRange>,
}

/// Selects whole lines, and when necessary a character-boundary prefix of the
/// final line, that fit `content_budget` encoded bytes. A cut inside a line is
/// continued through its byte offset instead of being skipped, so reassembled
/// pages are byte-exact.
fn select_page(
    text: &str,
    from: usize,
    start_line: u32,
    offset: usize,
    max_lines: u32,
    content_budget: usize,
) -> Result<Page, AgentError> {
    let mut content = String::new();
    let mut escaped = 0usize;
    let mut lines = 0u32;
    let mut line_offset = offset;
    let mut position = from;
    let mut line_truncated = false;
    let mut next = None;
    while position < text.len() {
        if lines >= max_lines {
            next = Some(WorkspaceReadRange {
                start_line: start_line.saturating_add(lines),
                line_byte_offset: 0,
            });
            break;
        }
        let line_end = line_raw_end(text, position);
        let segment = &text[position..line_end];
        let segment_escaped = json_escaped_len(segment);
        let remaining = content_budget.saturating_sub(escaped);
        if segment_escaped <= remaining {
            content.push_str(segment);
            escaped += segment_escaped;
            lines += 1;
            position = line_end;
            line_offset = 0;
            continue;
        }
        let take = char_prefix_within_budget(segment, remaining);
        if take == 0 {
            if lines == 0 {
                // The requested budget cannot hold even one character.
                return Err(AgentError::InvalidArguments);
            }
            next = Some(WorkspaceReadRange {
                start_line: start_line.saturating_add(lines),
                line_byte_offset: 0,
            });
            break;
        }
        content.push_str(&segment[..take]);
        lines += 1;
        line_truncated = true;
        next = Some(WorkspaceReadRange {
            start_line: start_line.saturating_add(lines - 1),
            line_byte_offset: u32::try_from(line_offset + take).unwrap_or(u32::MAX),
        });
        break;
    }
    Ok(Page {
        content,
        lines,
        line_truncated,
        next,
    })
}

fn map_workspace_error(error: crate::workspace::WorkspaceError) -> AgentError {
    match error {
        crate::workspace::WorkspaceError::InvalidPath => AgentError::InvalidArguments,
        _ => AgentError::Workspace,
    }
}

/// Holds the next read of a path after validation so a test can observe a
/// genuinely pending query.
#[cfg(test)]
pub(crate) static READ_GATES: crate::test_support::Registry<
    String,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

#[cfg(test)]
mod tests;
