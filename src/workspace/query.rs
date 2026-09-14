//! Bounded, session-scoped reads of Workspace files for read-only clients.
//!
//! One read resolves a workspace-relative regular file, performs one bounded
//! read of its bytes, and returns the raw text without display line numbers.
//! The binary check, the whole-file revision, and the returned page all
//! describe those same bytes, so a response can never combine an old hash with
//! newer content. Nothing here writes history, starts a model call, or mutates
//! the file.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
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
pub(crate) const MIN_RESULT_BYTES: usize = 1024;
pub(crate) const DEFAULT_RESULT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_RESULT_BYTES: usize = 256 * 1024;
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
        if self
            .max_bytes
            .is_some_and(|max_bytes| !(MIN_RESULT_BYTES..=MAX_RESULT_BYTES).contains(&max_bytes))
        {
            return Err(AgentError::InvalidArguments);
        }
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
    take_and_pause_workspace_read(
        &path,
        &session_cancellation,
        &shutdown_cancellation,
        deadline,
    )
    .await?;

    let mut file = with_query_budget(
        workspace.open_regular_file(&path),
        &session_cancellation,
        &shutdown_cancellation,
        deadline,
    )
    .await?
    .map_err(map_workspace_error)?;
    let before = with_query_budget(
        file.metadata(),
        &session_cancellation,
        &shutdown_cancellation,
        deadline,
    )
    .await?
    .map_err(|_| AgentError::Workspace)?;
    let capacity = usize::try_from(before.len().min(MAX_READ_BYTES as u64)).unwrap_or(0) + 1;
    let mut bytes = Vec::with_capacity(capacity);
    let limit = MAX_READ_BYTES as u64 + 1;
    with_query_budget(
        async { (&mut file).take(limit).read_to_end(&mut bytes).await },
        &session_cancellation,
        &shutdown_cancellation,
        deadline,
    )
    .await?
    .map_err(|_| AgentError::Workspace)?;
    let after = with_query_budget(
        file.metadata(),
        &session_cancellation,
        &shutdown_cancellation,
        deadline,
    )
    .await?
    .map_err(|_| AgentError::Workspace)?;

    // A file larger than the whole-file bound has no preview and no revision.
    // The bounded read cannot exceed the bound plus one byte.
    let oversized = bytes.len() > MAX_READ_BYTES;
    // A response is only trustworthy when the bytes form one consistent whole
    // file: same size before and after, and no modification in between. The
    // full modification time is compared so a same-millisecond rewrite is not
    // hidden; this is still a live observation, never a filesystem snapshot.
    let consistent = !oversized
        && u64::try_from(bytes.len()).ok() == Some(after.len())
        && after.len() == before.len()
        && modified_time(&before) == modified_time(&after);
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
        file_bytes: after.len(),
        file_modified_unix_ms: modified_unix_ms(&after),
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
    let size = serde_json::to_vec(&probe)
        .map_err(|_| AgentError::Internal)?
        .len();
    if size >= max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    Ok(size)
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

fn modified_time(metadata: &std::fs::Metadata) -> Option<SystemTime> {
    metadata.modified().ok()
}

fn modified_unix_ms(metadata: &std::fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn map_workspace_error(error: crate::workspace::WorkspaceError) -> AgentError {
    match error {
        crate::workspace::WorkspaceError::InvalidPath => AgentError::InvalidArguments,
        _ => AgentError::Workspace,
    }
}

#[cfg(test)]
type WorkspaceReadGates = Vec<(String, Arc<WorkspaceReadGate>)>;

#[cfg(test)]
static WORKSPACE_READ_GATES: OnceLock<Mutex<WorkspaceReadGates>> = OnceLock::new();

/// Test seam that holds the next query for `path` after validation, so a test
/// can observe a genuinely pending read without a sleep.
#[cfg(test)]
pub(crate) struct WorkspaceReadGate {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl WorkspaceReadGate {
    pub(crate) fn new() -> Self {
        Self {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
pub(crate) fn gate_next_workspace_read(path: &str, gate: Arc<WorkspaceReadGate>) {
    WORKSPACE_READ_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((path.to_owned(), gate));
}

#[cfg(test)]
fn take_workspace_read_gate(path: &str) -> Option<Arc<WorkspaceReadGate>> {
    let mut gates = WORKSPACE_READ_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    let index = gates.iter().position(|(candidate, _)| candidate == path)?;
    Some(gates.remove(index).1)
}

#[cfg(test)]
async fn take_and_pause_workspace_read(
    path: &str,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), AgentError> {
    let Some(gate) = take_workspace_read_gate(path) else {
        return Ok(());
    };
    gate.started.notify_one();
    with_query_budget(
        gate.release.notified(),
        session_cancellation,
        shutdown_cancellation,
        deadline,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        root: std::path::PathBuf,
        workspace: Arc<Workspace>,
    }

    async fn fixture(label: &str) -> Fixture {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-query-{label}-{}",
            SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());
        Fixture { root, workspace }
    }

    async fn write(fixture: &Fixture, name: &str, bytes: impl AsRef<[u8]>) {
        tokio::fs::write(fixture.root.join(name), bytes)
            .await
            .unwrap();
    }

    async fn query(
        fixture: &Fixture,
        request: WorkspaceReadRequest,
    ) -> Result<WorkspaceReadResult, AgentError> {
        read(
            Arc::clone(&fixture.workspace),
            request,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
    }

    fn read_request(path: &str) -> WorkspaceReadRequest {
        WorkspaceReadRequest {
            session_id: SessionId::new().unwrap(),
            path: path.to_owned(),
            start_line: None,
            line_byte_offset: None,
            max_lines: None,
            max_bytes: None,
            if_revision: None,
        }
    }

    fn digest(bytes: &[u8]) -> String {
        revision_of(bytes)
    }

    /// Pages until the file ends and returns the reassembled content and the
    /// revision every page reported. Every page must fit its encoded budget.
    async fn collect_pages(
        fixture: &Fixture,
        mut request: WorkspaceReadRequest,
    ) -> (String, String) {
        let budget = request.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
        let mut content = Vec::new();
        let mut revision: Option<String> = None;
        for _ in 0..4096 {
            let result = query(fixture, request.clone()).await.unwrap();
            assert_eq!(result.status, WorkspaceReadStatus::Ok, "page status");
            let encoded = serde_json::to_vec(&result).unwrap().len();
            assert!(encoded <= budget, "encoded page {encoded} exceeds {budget}");
            assert!(result.content.is_empty() || result.returned_lines >= 1);
            match &revision {
                Some(expected) => assert_eq!(result.revision.as_deref(), Some(expected.as_str())),
                None => revision = result.revision.clone(),
            }
            content.extend_from_slice(result.content.as_bytes());
            match result.next_range {
                Some(next) => {
                    let line = request.start_line.unwrap_or(1);
                    let offset = request.line_byte_offset.unwrap_or(0);
                    assert!(
                        next.start_line > line
                            || (next.start_line == line && next.line_byte_offset > offset),
                        "pagination must advance"
                    );
                    request.start_line = Some(next.start_line);
                    request.line_byte_offset = Some(next.line_byte_offset);
                }
                None => {
                    return (
                        String::from_utf8(content).unwrap(),
                        revision.expect("a complete read carries a revision"),
                    );
                }
            }
        }
        panic!("pagination did not finish");
    }

    #[test]
    fn escaped_length_matches_the_json_encoding() {
        let mut sample = String::new();
        for value in 0u32..0x300 {
            if let Some(character) = char::from_u32(value) {
                sample.push(character);
            }
        }
        sample.push('💥');
        sample.push('\u{2028}');
        for end in sample
            .char_indices()
            .map(|(index, character)| index + character.len_utf8())
            .chain(std::iter::once(sample.len()))
        {
            let encoded = serde_json::to_string(&sample[..end]).unwrap();
            assert_eq!(
                json_escaped_len(&sample[..end]) + 2,
                encoded.len(),
                "escaped length of {end} bytes"
            );
        }
    }

    #[tokio::test]
    async fn reads_a_whole_file_with_a_byte_exact_revision() {
        let fixture = fixture("whole").await;
        let bytes = b"alpha\nbeta\ngamma\n";
        write(&fixture, "note.txt", bytes).await;

        let mut request = read_request("note.txt");
        request.max_bytes = Some(4096);
        let result = query(&fixture, request).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok);
        assert_eq!(result.content, "alpha\nbeta\ngamma\n");
        assert_eq!(result.start_line, 1);
        assert_eq!(result.returned_lines, 3);
        assert!(!result.truncated);
        assert!(!result.line_truncated);
        assert!(result.next_range.is_none());
        assert_eq!(result.encoding, WorkspaceReadEncoding::Utf8);
        assert_eq!(result.revision.as_deref(), Some(digest(bytes).as_str()));
        assert_eq!(
            result.revision.as_deref(),
            Some(digest(result.content.as_bytes()).as_str())
        );
        assert_eq!(result.file_bytes, bytes.len() as u64);
        assert!(result.file_modified_unix_ms.is_some());

        let mut ranged = read_request("note.txt");
        ranged.start_line = Some(2);
        ranged.max_lines = Some(1);
        let result = query(&fixture, ranged).await.unwrap();
        assert_eq!(result.content, "beta\n");
        assert_eq!(result.start_line, 2);
        assert_eq!(result.returned_lines, 1);
        assert!(result.truncated);
        assert_eq!(
            result.next_range,
            Some(WorkspaceReadRange {
                start_line: 3,
                line_byte_offset: 0,
            })
        );
    }

    #[tokio::test]
    async fn pagination_is_lossless_for_long_unicode_and_control_lines() {
        let fixture = fixture("lossless").await;
        let mut file = Vec::new();
        file.extend_from_slice(b"short\n");
        file.extend_from_slice(
            "h\u{e9}llo w\u{f6}rld \u{2014} unicode "
                .repeat(80)
                .as_bytes(),
        );
        file.extend_from_slice(b"\n");
        file.extend_from_slice("control\u{1}\u{2}\t\u{7f} chars\n".as_bytes());
        file.resize(file.len() + 4096, b'x');
        file.extend_from_slice(b"\r\n");
        file.extend_from_slice(b"tail without newline");
        write(&fixture, "mixed.txt", &file).await;

        let mut whole = read_request("mixed.txt");
        whole.max_bytes = Some(MAX_RESULT_BYTES);
        let result = query(&fixture, whole).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok);
        assert_eq!(result.content.as_bytes(), file.as_slice());
        assert!(result.next_range.is_none());
        assert_eq!(result.revision.as_deref(), Some(digest(&file).as_str()));

        let mut request = read_request("mixed.txt");
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let (reassembled, revision) = collect_pages(&fixture, request).await;
        assert_eq!(reassembled.as_bytes(), file.as_slice());
        assert_eq!(revision, digest(&file));
    }

    #[tokio::test]
    async fn line_byte_offset_continues_inside_a_long_line() {
        let fixture = fixture("long-line").await;
        let long = "abc\u{e9}\u{4e2d}\u{1f600}".repeat(2000);
        let file = format!("{long}\nsecond\n");
        write(&fixture, "long.txt", file.as_bytes()).await;

        let mut request = read_request("long.txt");
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let (reassembled, revision) = collect_pages(&fixture, request.clone()).await;
        assert_eq!(reassembled, file);
        assert_eq!(revision, digest(file.as_bytes()));

        let first = query(&fixture, request.clone()).await.unwrap();
        assert_eq!(first.start_line, 1);
        assert!(first.line_truncated);
        let next = first.next_range.expect("a long line continues");
        assert_eq!(next.start_line, 1);
        assert!(next.line_byte_offset > 0);
        request.start_line = Some(next.start_line);
        request.line_byte_offset = Some(next.line_byte_offset);
        let second = query(&fixture, request).await.unwrap();
        assert_eq!(second.start_line, 1);
        assert!(second.line_truncated);
        let mut combined = first.content.clone();
        combined.push_str(&second.content);
        assert!(file.starts_with(&combined));
        assert!(!second.content.is_empty());
    }

    #[tokio::test]
    async fn empty_trailing_newline_and_beyond_eof_ranges_are_explicit() {
        let fixture = fixture("edges").await;

        write(&fixture, "empty.txt", b"").await;
        let result = query(&fixture, read_request("empty.txt")).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok);
        assert_eq!(result.content, "");
        assert_eq!(result.returned_lines, 0);
        assert!(!result.truncated);
        assert!(result.next_range.is_none());
        assert_eq!(result.revision.as_deref(), Some(digest(b"").as_str()));

        write(&fixture, "one.txt", b"a\n").await;
        let result = query(&fixture, read_request("one.txt")).await.unwrap();
        assert_eq!(result.content, "a\n");
        assert_eq!(result.returned_lines, 1);
        assert!(result.next_range.is_none());

        write(&fixture, "blank.txt", b"a\n\n").await;
        let result = query(&fixture, read_request("blank.txt")).await.unwrap();
        assert_eq!(result.content, "a\n\n");
        assert_eq!(result.returned_lines, 2);
        assert!(result.next_range.is_none());

        let mut beyond = read_request("one.txt");
        beyond.start_line = Some(9);
        let result = query(&fixture, beyond).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok);
        assert_eq!(result.content, "");
        assert_eq!(result.returned_lines, 0);
        assert!(result.next_range.is_none());
    }

    #[tokio::test]
    async fn binary_files_are_reported_without_content() {
        let fixture = fixture("binary").await;

        write(&fixture, "nul.txt", b"text\0more").await;
        let result = query(&fixture, read_request("nul.txt")).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Binary);
        assert_eq!(result.content, "");
        assert_eq!(result.returned_lines, 0);
        assert_eq!(result.encoding, WorkspaceReadEncoding::Unknown);
        assert!(result.next_range.is_none());
        assert!(result.revision.is_some());

        write(
            &fixture,
            "invalid.txt",
            [b'f', b'o', b'o', b'\n', 0xff, b'\n'],
        )
        .await;
        let result = query(&fixture, read_request("invalid.txt")).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Binary);
        assert_eq!(result.content, "");
    }

    #[tokio::test]
    async fn files_at_or_over_the_whole_file_bound_are_explicit() {
        let fixture = fixture("bound").await;

        let exact = b"a".repeat(MAX_READ_BYTES);
        write(&fixture, "exact.txt", &exact).await;
        let mut request = read_request("exact.txt");
        request.max_bytes = Some(MAX_RESULT_BYTES);
        let result = query(&fixture, request).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok);
        assert_eq!(result.file_bytes, MAX_READ_BYTES as u64);
        assert_eq!(result.revision.as_deref(), Some(digest(&exact).as_str()));
        assert!(result.next_range.is_some());

        let mut over = b"a".repeat(MAX_READ_BYTES);
        over.push(b'b');
        write(&fixture, "over.txt", &over).await;
        let mut request = read_request("over.txt");
        request.max_bytes = Some(MAX_RESULT_BYTES);
        let result = query(&fixture, request).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::TooLarge);
        assert_eq!(result.content, "");
        assert!(result.revision.is_none());
        assert!(result.next_range.is_none());
        assert_eq!(result.file_bytes, over.len() as u64);
    }

    #[tokio::test]
    async fn revision_mismatch_reports_changed_without_content() {
        let fixture = fixture("revision").await;
        write(&fixture, "small.txt", b"small\n").await;
        let first = query(&fixture, read_request("small.txt")).await.unwrap();
        let revision = first.revision.clone().unwrap();
        assert_eq!(revision, digest(b"small\n"));

        let mut matching = read_request("small.txt");
        matching.if_revision = Some(revision.to_uppercase());
        let result = query(&fixture, matching).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok);
        assert_eq!(result.content, "small\n");

        let mut stale = read_request("small.txt");
        stale.if_revision = Some("0".repeat(64));
        let result = query(&fixture, stale).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Changed);
        assert_eq!(result.content, "");
        assert!(!result.truncated);
        assert!(result.next_range.is_none());
        assert_eq!(result.revision.as_deref(), Some(revision.as_str()));
    }

    #[tokio::test]
    async fn validation_rejects_invalid_paths_ranges_and_offsets() {
        let fixture = fixture("validation").await;
        write(&fixture, "file.txt", b"abc\n").await;

        for path in ["", ".", "..", "../escape", "/etc/hosts", "a/../../b"] {
            let request = read_request(path);
            assert!(
                matches!(request.validate(), Err(AgentError::InvalidArguments)),
                "{path:?} must be rejected without IO"
            );
        }
        let request = read_request("nul\0name");
        assert!(matches!(
            request.validate(),
            Err(AgentError::InvalidArguments)
        ));

        let mut request = read_request("file.txt");
        request.start_line = Some(0);
        assert!(request.validate().is_err());
        let mut request = read_request("file.txt");
        request.line_byte_offset = Some(4);
        assert!(request.validate().is_err());
        let mut request = read_request("file.txt");
        request.max_lines = Some(0);
        assert!(request.validate().is_err());
        let mut request = read_request("file.txt");
        request.max_lines = Some(MAX_MAX_LINES + 1);
        assert!(request.validate().is_err());
        let mut request = read_request("file.txt");
        request.max_bytes = Some(MIN_RESULT_BYTES - 1);
        assert!(request.validate().is_err());
        let mut request = read_request("file.txt");
        request.max_bytes = Some(MAX_RESULT_BYTES + 1);
        assert!(request.validate().is_err());
        let mut request = read_request("file.txt");
        request.if_revision = Some("not-a-revision".to_owned());
        assert!(request.validate().is_err());

        // The path byte cap and the offset bound are lexical: both fail before
        // any file is opened or any query slot is reserved, and the bounds
        // themselves are still accepted.
        let request = read_request(&"a".repeat(MAX_PATH_BYTES));
        assert!(request.validate().is_ok());
        let request = read_request(&"a".repeat(MAX_PATH_BYTES + 1));
        assert!(matches!(
            request.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut request = read_request("file.txt");
        request.start_line = Some(1);
        request.line_byte_offset = Some(MAX_READ_BYTES as u32);
        assert!(request.validate().is_ok());
        request.line_byte_offset = Some(MAX_READ_BYTES as u32 + 1);
        assert!(matches!(
            request.validate(),
            Err(AgentError::InvalidArguments)
        ));

        // A budget that cannot even hold the metadata envelope is an argument
        // error, not a truncation.
        let mut request = read_request(&"a".repeat(900));
        request.max_bytes = Some(MIN_RESULT_BYTES);
        assert!(matches!(
            query(&fixture, request).await,
            Err(AgentError::InvalidArguments)
        ));

        // Offsets must fall inside the line and on a character boundary.
        let mut request = read_request("file.txt");
        request.start_line = Some(1);
        request.line_byte_offset = Some(4);
        assert!(matches!(
            query(&fixture, request).await,
            Err(AgentError::InvalidArguments)
        ));
        let mut request = read_request("file.txt");
        request.start_line = Some(1);
        request.line_byte_offset = Some(5);
        assert!(matches!(
            query(&fixture, request).await,
            Err(AgentError::InvalidArguments)
        ));
        write(&fixture, "wide.txt", "\u{e9}\n").await;
        let mut request = read_request("wide.txt");
        request.start_line = Some(1);
        request.line_byte_offset = Some(1);
        assert!(matches!(
            query(&fixture, request).await,
            Err(AgentError::InvalidArguments)
        ));
        let mut request = read_request("file.txt");
        request.start_line = Some(9);
        request.line_byte_offset = Some(1);
        assert!(matches!(
            query(&fixture, request).await,
            Err(AgentError::InvalidArguments)
        ));

        assert!(matches!(
            query(&fixture, read_request("missing.txt")).await,
            Err(AgentError::Workspace)
        ));
        tokio::fs::create_dir(fixture.root.join("dir"))
            .await
            .unwrap();
        assert!(matches!(
            query(&fixture, read_request("dir")).await,
            Err(AgentError::Workspace)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn special_files_and_a_post_check_swap_do_not_block() {
        let fixture = fixture("special").await;
        write(&fixture, "static.txt", b"data\n").await;
        let fifo = fixture.root.join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());
        let static_result = tokio::time::timeout(
            Duration::from_secs(2),
            query(&fixture, read_request("pipe")),
        )
        .await;
        assert!(
            matches!(static_result, Ok(Err(AgentError::Workspace))),
            "a static FIFO is rejected by metadata"
        );

        write(&fixture, "swap.txt", b"data\n").await;
        let target = std::fs::canonicalize(fixture.root.join("swap.txt")).unwrap();
        crate::workspace::swap_next_open_with_fifo(target);
        let swapped = tokio::time::timeout(
            Duration::from_secs(2),
            query(&fixture, read_request("swap.txt")),
        )
        .await;
        assert!(
            matches!(swapped, Ok(Err(AgentError::Workspace))),
            "a FIFO swapped in after the metadata check must not block the open"
        );
    }

    #[tokio::test]
    async fn deadline_and_cancellation_stop_a_pending_query() {
        let fixture = fixture("cancel").await;
        write(&fixture, "wait.txt", b"data\n").await;

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let result = read(
            Arc::clone(&fixture.workspace),
            read_request("wait.txt"),
            cancelled,
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(result, Err(AgentError::QueryLimit)));

        // Cancelling the owning Session stops a started query.
        let gate = Arc::new(WorkspaceReadGate::new());
        gate_next_workspace_read("wait.txt", Arc::clone(&gate));
        let session = CancellationToken::new();
        let pending = tokio::spawn({
            let workspace = Arc::clone(&fixture.workspace);
            let session = session.clone();
            async move {
                read(
                    workspace,
                    read_request("wait.txt"),
                    session,
                    CancellationToken::new(),
                )
                .await
            }
        });
        gate.wait_started().await;
        session.cancel();
        assert!(matches!(
            pending.await.unwrap(),
            Err(AgentError::QueryLimit)
        ));

        // RPC shutdown cancels the query the same way.
        let gate = Arc::new(WorkspaceReadGate::new());
        gate_next_workspace_read("wait.txt", Arc::clone(&gate));
        let shutdown = CancellationToken::new();
        let pending = tokio::spawn({
            let workspace = Arc::clone(&fixture.workspace);
            let shutdown = shutdown.clone();
            async move {
                read(
                    workspace,
                    read_request("wait.txt"),
                    CancellationToken::new(),
                    shutdown,
                )
                .await
            }
        });
        gate.wait_started().await;
        shutdown.cancel();
        assert!(matches!(
            pending.await.unwrap(),
            Err(AgentError::QueryLimit)
        ));

        // The deadline wraps the pending step itself.
        let gate = Arc::new(WorkspaceReadGate::new());
        gate_next_workspace_read("wait.txt", Arc::clone(&gate));
        let pending = tokio::spawn({
            let workspace = Arc::clone(&fixture.workspace);
            async move {
                read_until(
                    workspace,
                    read_request("wait.txt"),
                    CancellationToken::new(),
                    CancellationToken::new(),
                    Instant::now() + Duration::from_millis(20),
                )
                .await
            }
        });
        gate.wait_started().await;
        assert!(matches!(
            pending.await.unwrap(),
            Err(AgentError::QueryLimit)
        ));

        let gate = Arc::new(WorkspaceReadGate::new());
        gate_next_workspace_read("wait.txt", Arc::clone(&gate));
        let pending = tokio::spawn({
            let workspace = Arc::clone(&fixture.workspace);
            async move {
                read(
                    workspace,
                    read_request("wait.txt"),
                    CancellationToken::new(),
                    CancellationToken::new(),
                )
                .await
            }
        });
        gate.wait_started().await;
        gate.release();
        let result = pending.await.unwrap().unwrap();
        assert_eq!(result.content, "data\n");
    }

    #[tokio::test]
    async fn with_query_budget_wraps_a_pending_step() {
        let deadline = Instant::now() + Duration::from_secs(30);

        let session = CancellationToken::new();
        let shutdown = CancellationToken::new();
        session.cancel();
        let pending =
            with_query_budget(std::future::pending::<()>(), &session, &shutdown, deadline);
        assert!(matches!(pending.await, Err(AgentError::QueryLimit)));

        let session = CancellationToken::new();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let pending =
            with_query_budget(std::future::pending::<()>(), &session, &shutdown, deadline);
        assert!(matches!(pending.await, Err(AgentError::QueryLimit)));

        let session = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let pending = with_query_budget(
            std::future::pending::<()>(),
            &session,
            &shutdown,
            Instant::now() + Duration::from_millis(10),
        );
        assert!(matches!(pending.await, Err(AgentError::QueryLimit)));

        let session = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let ready = with_query_budget(std::future::ready(7u8), &session, &shutdown, deadline);
        assert_eq!(ready.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn query_leaves_the_file_bytes_unchanged() {
        let fixture = fixture("read-only").await;
        write(&fixture, "keep.txt", b"keep\n").await;
        let before = tokio::fs::read(fixture.root.join("keep.txt"))
            .await
            .unwrap();
        query(&fixture, read_request("keep.txt")).await.unwrap();
        let after = tokio::fs::read(fixture.root.join("keep.txt"))
            .await
            .unwrap();
        assert_eq!(before, after);
    }
}
