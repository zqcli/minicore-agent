//! `workspace.search`: one bounded page of literal matches in workspace files.
//!
//! Search shares the ignore-aware traversal, bounds, and result budget of
//! `workspace.files`. The query is always literal: a case-insensitive search
//! escapes the query and matches it with a case-folding expression over the
//! original line text, so reported byte ranges always fall on UTF-8 boundaries
//! of the returned slice. A line that does not fit the page is returned as a
//! bounded slice that still contains its matches, with the slice's own offset
//! inside the original line, and a page that cannot hold the next match stops
//! before it so the next page re-finds it. Nothing here writes history, starts a
//! model call, or mutates the file.

use std::fmt;
use std::io::Read;
use std::sync::Arc;

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::ids::SessionId;
use crate::tools::MAX_READ_BYTES;
use crate::workspace::Workspace;
use crate::workspace::query::{
    DEFAULT_RESULT_BYTES, MAX_PATH_BYTES, MAX_RESULT_BYTES, MIN_RESULT_BYTES,
};
use crate::workspace::scan::{
    MAX_SEARCH_PATHS, READ_CHUNK_BYTES, SCOPE_HEX_LEN, ScanBudget, Visit, WalkOptions,
    WorkspaceFileKind, WorkspaceScanConsistency, WorkspaceScanStop, encoded_len,
    floor_char_boundary, normalized_roots, now_unix_ms, run_scan, scope_digest, valid_query_text,
    valid_scope, walk_scope,
};

const DEFAULT_MAX_MATCHES: u32 = 100;
const MAX_MAX_MATCHES: u32 = 1000;
/// Left context kept before the first match of a line that does not fit.
const LINE_CONTEXT_BYTES: usize = 64;

/// One bounded literal search request. `query` is a single line and is never
/// interpreted as a regular expression. `paths` limits the search to those
/// workspace-relative files or directories and defaults to the whole
/// Workspace; the list is normalized, de-duplicated, and rejected when it
/// overlaps. `cursor` continues a previous page.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSearchRequest {
    pub session_id: SessionId,
    pub query: String,
    #[serde(default)]
    pub paths: Option<Vec<String>>,
    #[serde(default)]
    pub case_sensitive: Option<bool>,
    #[serde(default)]
    pub cursor: Option<WorkspaceSearchCursor>,
    #[serde(default)]
    pub max_matches: Option<u32>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl WorkspaceSearchRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        if self.query.is_empty() || !valid_query_text(&self.query) {
            return Err(AgentError::InvalidArguments);
        }
        if let Some(paths) = &self.paths {
            if paths.len() > MAX_SEARCH_PATHS {
                return Err(AgentError::InvalidArguments);
            }
            for path in paths {
                if path.len() > MAX_PATH_BYTES {
                    return Err(AgentError::InvalidArguments);
                }
            }
        }
        let roots = normalized_roots(self.paths.as_deref())?;
        if self
            .max_matches
            .is_some_and(|max_matches| !(1..=MAX_MAX_MATCHES).contains(&max_matches))
        {
            return Err(AgentError::InvalidArguments);
        }
        if self
            .max_bytes
            .is_some_and(|max_bytes| !(MIN_RESULT_BYTES..=MAX_RESULT_BYTES).contains(&max_bytes))
        {
            return Err(AgentError::InvalidArguments);
        }
        if let Some(cursor) = &self.cursor {
            // The scope is computed from this request alone, so a cursor from a
            // different session, query, case mode, or root list is rejected
            // before any query slot is reserved.
            if !valid_scope(&cursor.scope)
                || cursor.scope != self.scope(&roots)
                || usize::try_from(cursor.path_index).unwrap_or(usize::MAX) >= roots.len()
                || cursor.line_byte_offset > MAX_READ_BYTES as u32
            {
                return Err(AgentError::InvalidArguments);
            }
        }
        Ok(())
    }

    fn case_sensitive(&self) -> bool {
        self.case_sensitive.unwrap_or(false)
    }

    fn max_matches(&self) -> u32 {
        self.max_matches.unwrap_or(DEFAULT_MAX_MATCHES)
    }

    fn max_bytes(&self) -> usize {
        self.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES)
    }

    /// Binds a cursor to this method, session, query, case mode, and normalized
    /// root list. Page size may change between pages.
    fn scope(&self, roots: &[String]) -> String {
        let session = self.session_id.to_string();
        let mut parts = Vec::with_capacity(roots.len() + 4);
        parts.push("workspace.search");
        parts.push(session.as_str());
        parts.push(self.query.as_str());
        parts.push(if self.case_sensitive() { "1" } else { "0" });
        for root in roots {
            parts.push(root.as_str());
        }
        scope_digest(&parts)
    }
}

impl fmt::Debug for WorkspaceSearchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceSearchRequest")
            .field("session_id", &self.session_id)
            .field("query_bytes", &self.query.len())
            .field("paths", &self.paths.as_ref().map(Vec::len))
            .field("case_sensitive", &self.case_sensitive())
            .field("cursor", &self.cursor)
            .field("max_matches", &self.max_matches)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

/// Where a later page continues: the index into the request's `paths`, the raw
/// traversal entry ordinal inside it (rule-excluded entries included), and the
/// position inside that entry when the page stopped in the middle of a file. `line`/`line_byte_offset` are
/// ignored when `line` is zero, which continues from the entry start.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSearchCursor {
    pub path_index: u32,
    pub entry: u64,
    pub line: u32,
    pub line_byte_offset: u32,
    pub scope: String,
}

/// Half-open UTF-8 byte range inside `WorkspaceSearchMatch::line_text`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WorkspaceByteRange {
    pub start: u32,
    pub end: u32,
}

/// One matching line, or one bounded slice of a matching line. `line_text` is
/// raw line text without its `\n` or `\r\n` terminator, starting at
/// `line_text_byte_offset` inside the original line, and `match_byte_ranges`
/// are byte offsets inside `line_text`. `line_truncated` marks a slice that
/// does not cover the whole line; `workspace.read` pages such a line in full.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct WorkspaceSearchMatch {
    pub path: String,
    pub line_number: u32,
    pub line_text_byte_offset: u32,
    pub match_byte_ranges: Vec<WorkspaceByteRange>,
    pub line_text: String,
    pub line_truncated: bool,
}

impl fmt::Debug for WorkspaceSearchMatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceSearchMatch")
            .field("path_bytes", &self.path.len())
            .field("line_number", &self.line_number)
            .field("line_text_byte_offset", &self.line_text_byte_offset)
            .field("ranges", &self.match_byte_ranges.len())
            .field("line_bytes", &self.line_text.len())
            .field("line_truncated", &self.line_truncated)
            .finish()
    }
}

/// One page of a live literal search.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct WorkspaceSearchResult {
    pub matches: Vec<WorkspaceSearchMatch>,
    /// Present when the scan stopped with an exact continuation point.
    pub next_cursor: Option<WorkspaceSearchCursor>,
    /// This response is a partial view: the traversal was cut by a bound, a
    /// file could not be read, or a match could not be represented.
    pub truncated: bool,
    /// The traversal reached the end of the scope without a read failure or an
    /// unrepresentable match. Files that are unsupported by design (binary,
    /// oversized, non-UTF-8, special, or outside the boundary) and roots
    /// excluded by the rules are counted in `skipped_files` without clearing
    /// it: the scope was enumerated, but not every file was searched.
    pub scan_complete: bool,
    /// What ended the scan. `page` and `bytes` continue from the cursor; `end`,
    /// `depth`, `entries`, and `rules` never do.
    pub stopped_by: WorkspaceScanStop,
    /// Visited files that were not searched: unsupported by design (over the
    /// whole-file bound, binary, not valid UTF-8, special, or outside the
    /// Workspace boundary), not valid UTF-8 in their path, unreadable, or
    /// holding a match that could not fit the result budget, plus explicitly
    /// requested roots excluded by the rules. Rule-excluded descendants are not
    /// counted.
    pub skipped_files: u64,
    pub consistency: WorkspaceScanConsistency,
    pub observed_at_unix_ms: u64,
}

impl fmt::Debug for WorkspaceSearchResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceSearchResult")
            .field("matches", &self.matches.len())
            .field("next_cursor", &self.next_cursor)
            .field("truncated", &self.truncated)
            .field("scan_complete", &self.scan_complete)
            .field("stopped_by", &self.stopped_by)
            .field("skipped_files", &self.skipped_files)
            .field("consistency", &self.consistency)
            .field("observed_at_unix_ms", &self.observed_at_unix_ms)
            .finish()
    }
}

/// Literal matcher: exact text, or an escaped case-folding regex over the
/// original line so reported ranges stay on UTF-8 boundaries.
enum Matcher {
    Literal(String),
    Folded(Regex),
}

impl Matcher {
    fn new(query: &str, case_sensitive: bool) -> Result<Self, AgentError> {
        if case_sensitive {
            return Ok(Self::Literal(query.to_owned()));
        }
        RegexBuilder::new(&regex::escape(query))
            .case_insensitive(true)
            .build()
            .map(Self::Folded)
            .map_err(|_| AgentError::Internal)
    }
}

/// Whether the range scan should keep going.
enum RangeFlow {
    Next,
    Stop,
}

/// Walks the literal matches of one line from `from` on, in byte order.
fn scan_ranges(
    matcher: &Matcher,
    line: &str,
    from: usize,
    mut visit: impl FnMut(usize, usize) -> Result<RangeFlow, AgentError>,
) -> Result<(), AgentError> {
    if from > line.len() {
        return Ok(());
    }
    match matcher {
        Matcher::Literal(needle) => {
            for (start, found) in line[from..].match_indices(needle.as_str()) {
                let start = from + start;
                if matches!(visit(start, start + found.len())?, RangeFlow::Stop) {
                    return Ok(());
                }
            }
        }
        Matcher::Folded(regex) => {
            for found in regex.find_iter(line) {
                if found.start() < from {
                    continue;
                }
                if matches!(visit(found.start(), found.end())?, RangeFlow::Stop) {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

struct SearchPlan {
    roots: Vec<String>,
    query: String,
    case_sensitive: bool,
    max_matches: u64,
    max_bytes: usize,
    scope: String,
    start_path: usize,
    start_entry: u64,
    resume: Option<(u32, u32)>,
}

struct SearchPage<'a> {
    matcher: &'a Matcher,
    content_budget: usize,
    used: usize,
    remaining: u64,
    matches: Vec<WorkspaceSearchMatch>,
    skipped_files: u64,
    /// Matches dropped because the page could not represent them.
    dropped: u64,
    /// Whether the file being scanned dropped a match.
    file_dropped: bool,
}

/// A record under construction for one line, and its exact encoded size.
struct PendingLine {
    ranges: Vec<(usize, usize)>,
    record: WorkspaceSearchMatch,
    encoded: usize,
}

enum FileScan {
    Done,
    /// The page stops inside this file; the cursor resumes at `line`/`offset`.
    StopInside {
        line: u32,
        offset: u32,
    },
    /// The scan byte ceiling was reached before this file was read.
    StopBytes,
}

enum ReadFile {
    Text(String),
    /// A known unsupported object: over the whole-file bound, binary, not valid
    /// UTF-8, or outside the Workspace boundary. The scope was still
    /// enumerated, but the file was not searched.
    Unsupported,
    /// The file could not be opened, typed, or read. The scan is incomplete.
    Failed,
    /// The scan byte ceiling was reached while reading.
    StopBytes,
}

/// Searches one bounded page of literal matches.
pub(crate) async fn search(
    workspace: Arc<Workspace>,
    request: WorkspaceSearchRequest,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
) -> Result<WorkspaceSearchResult, AgentError> {
    request.validate()?;
    let roots = normalized_roots(request.paths.as_deref())?;
    let scope = request.scope(&roots);
    let (start_path, start_entry, resume) = match &request.cursor {
        Some(cursor) => (
            usize::try_from(cursor.path_index).unwrap_or(usize::MAX),
            cursor.entry,
            if cursor.line == 0 {
                None
            } else {
                Some((cursor.line, cursor.line_byte_offset))
            },
        ),
        None => (0, 0, None),
    };
    let plan = SearchPlan {
        roots,
        query: request.query.clone(),
        case_sensitive: request.case_sensitive(),
        max_matches: u64::from(request.max_matches()),
        max_bytes: request.max_bytes(),
        scope,
        start_path,
        start_entry,
        resume,
    };
    run_scan(
        workspace,
        session_cancellation,
        shutdown_cancellation,
        move |workspace, budget| scan_search(workspace, &plan, budget),
    )
    .await
}

fn scan_search(
    workspace: &Workspace,
    plan: &SearchPlan,
    budget: &mut ScanBudget,
) -> Result<WorkspaceSearchResult, AgentError> {
    let envelope = envelope_bytes(plan.max_bytes)?;
    let matcher = Matcher::new(&plan.query, plan.case_sensitive)?;
    let mut page = SearchPage {
        matcher: &matcher,
        content_budget: plan.max_bytes - envelope,
        used: 0,
        remaining: plan.max_matches,
        matches: Vec::new(),
        skipped_files: 0,
        dropped: 0,
        file_dropped: false,
    };
    let mut reason = WorkspaceScanStop::End;
    let mut next: Option<WorkspaceSearchCursor> = None;
    let mut unrepresentable = 0u64;
    let mut unreadable = 0u64;
    let mut excluded = 0u64;
    for (path_index, root) in plan.roots.iter().enumerate() {
        if path_index < plan.start_path {
            continue;
        }
        let entry_start = if path_index == plan.start_path {
            plan.start_entry
        } else {
            0
        };
        let mut resume = if path_index == plan.start_path {
            plan.resume
        } else {
            None
        };
        let outcome = walk_scope(
            workspace,
            root,
            WalkOptions {
                recursive: true,
                want_size: false,
                directory_only: false,
            },
            entry_start,
            budget,
            |entry, budget| {
                if entry.kind == WorkspaceFileKind::Directory {
                    return Ok(Visit::Next);
                }
                if page.remaining == 0 {
                    return Ok(Visit::StopBefore(WorkspaceScanStop::Page));
                }
                match scan_file(workspace, &entry.relative, resume.take(), &mut page, budget)? {
                    FileScan::Done => Ok(Visit::Next),
                    FileScan::StopInside { line, offset } => Ok(Visit::StopInside {
                        reason: WorkspaceScanStop::Page,
                        line,
                        offset,
                    }),
                    FileScan::StopBytes => Ok(Visit::StopBefore(WorkspaceScanStop::Bytes)),
                }
            },
        )?;
        unrepresentable += outcome.unrepresentable;
        unreadable += outcome.unreadable;
        excluded += outcome.excluded;
        match outcome.next {
            Some(entry) => {
                reason = outcome.reason;
                next = Some(WorkspaceSearchCursor {
                    path_index: u32::try_from(path_index).unwrap_or(u32::MAX),
                    entry,
                    line: outcome.resume.map(|(line, _)| line).unwrap_or(0),
                    line_byte_offset: outcome.resume.map(|(_, offset)| offset).unwrap_or(0),
                    scope: plan.scope.clone(),
                });
                break;
            }
            None => {
                if outcome.reason != WorkspaceScanStop::End {
                    reason = outcome.reason;
                    break;
                }
            }
        }
    }
    let dropped = page.dropped + unrepresentable + unreadable;
    Ok(WorkspaceSearchResult {
        matches: page.matches,
        next_cursor: next,
        truncated: reason != WorkspaceScanStop::End || dropped > 0,
        scan_complete: reason == WorkspaceScanStop::End && dropped == 0,
        stopped_by: reason,
        skipped_files: page.skipped_files + unrepresentable + unreadable + excluded,
        consistency: WorkspaceScanConsistency::Live,
        observed_at_unix_ms: now_unix_ms(),
    })
}

fn scan_file(
    workspace: &Workspace,
    relative: &str,
    resume: Option<(u32, u32)>,
    page: &mut SearchPage<'_>,
    budget: &mut ScanBudget,
) -> Result<FileScan, AgentError> {
    let text = match read_searchable_file(workspace, relative, budget)? {
        ReadFile::Text(text) => text,
        ReadFile::Unsupported => {
            page.skipped_files += 1;
            return Ok(FileScan::Done);
        }
        ReadFile::Failed => {
            page.skipped_files += 1;
            page.dropped += 1;
            return Ok(FileScan::Done);
        }
        ReadFile::StopBytes => return Ok(FileScan::StopBytes),
    };
    let resume_line = resume.map(|(line, _)| line).unwrap_or(0);
    let resume_offset = resume.map(|(_, offset)| offset).unwrap_or(0);
    page.file_dropped = false;
    let mut line_number = 1u32;
    let mut stop = None;
    for segment in text.split_inclusive('\n') {
        budget.check()?;
        if budget.time_expired() {
            break;
        }
        let terminated = segment.ends_with('\n');
        let raw = if terminated {
            &segment[..segment.len() - 1]
        } else {
            segment
        };
        let line = if terminated {
            raw.strip_suffix('\r').unwrap_or(raw)
        } else {
            raw
        };
        if line_number >= resume_line.max(1) {
            let from = if line_number == resume_line {
                let offset = usize::try_from(resume_offset).unwrap_or(usize::MAX);
                floor_char_boundary(line, offset.min(line.len()))
            } else {
                0
            };
            stop = visit_line(page, relative, line_number, line, from, budget)?;
            if stop.is_some() {
                break;
            }
        }
        // A test timeout placed after the first committed result must keep it
        // and end the scan with a deadline instead of a cursor.
        #[cfg(test)]
        if !page.matches.is_empty()
            && crate::workspace::scan::scan_expiry_after_result(workspace.root())
        {
            budget.expire_now();
        }
        line_number = line_number.saturating_add(1);
    }
    if page.file_dropped {
        page.skipped_files += 1;
    }
    match stop {
        Some((line, offset)) => Ok(FileScan::StopInside { line, offset }),
        None => Ok(FileScan::Done),
    }
}

/// Visits one line, appending at most one record per call. Returns the position
/// where the page must be resumed when it stopped inside this line.
fn visit_line(
    page: &mut SearchPage<'_>,
    path: &str,
    line_number: u32,
    line: &str,
    from: usize,
    budget: &mut ScanBudget,
) -> Result<Option<(u32, u32)>, AgentError> {
    let matcher = page.matcher;
    let mut pending: Option<PendingLine> = None;
    let mut stop: Option<(u32, u32)> = None;
    scan_ranges(matcher, line, from, |start, end| {
        // Long lines and pages that build many records still stop when the
        // query is cancelled or the deadline passes.
        budget.check()?;
        if budget.time_expired() {
            // Keep the matches already committed for this line and let the walk
            // report the deadline; a timeout never hands out a page cursor.
            return Ok(RangeFlow::Stop);
        }
        let accepted = pending
            .as_ref()
            .map(|pending| pending.ranges.len())
            .unwrap_or(0);
        if u64::try_from(accepted).unwrap_or(u64::MAX) >= page.remaining {
            // The page may not return another match: stop before this one so
            // the next page re-finds it.
            stop = Some((line_number, u32::try_from(start).unwrap_or(u32::MAX)));
            return Ok(RangeFlow::Stop);
        }
        let mut ranges = pending
            .as_ref()
            .map(|pending| pending.ranges.clone())
            .unwrap_or_default();
        ranges.push((start, end));
        match build_record(page, path, line_number, line, &ranges)? {
            Some(record) => {
                pending = Some(record);
                Ok(RangeFlow::Next)
            }
            None if page.matches.is_empty() && pending.is_none() => {
                // Nothing has been returned for this page yet, so skipping and
                // counting the match keeps the page moving and marks the result
                // incomplete instead of stalling or pretending to be complete.
                page.dropped += 1;
                page.file_dropped = true;
                Ok(RangeFlow::Next)
            }
            None => {
                stop = Some((line_number, u32::try_from(start).unwrap_or(u32::MAX)));
                Ok(RangeFlow::Stop)
            }
        }
    })?;
    if let Some(pending) = pending {
        commit(page, pending);
    }
    Ok(stop)
}

/// Builds the record for one line and the given absolute ranges, preferring the
/// whole line and falling back to a bounded slice that still contains every
/// range. Returns `None` when even that cannot fit the page.
fn build_record(
    page: &SearchPage<'_>,
    path: &str,
    line_number: u32,
    line: &str,
    ranges: &[(usize, usize)],
) -> Result<Option<PendingLine>, AgentError> {
    let (Some(first), Some(last)) = (ranges.first(), ranges.last()) else {
        return Ok(None);
    };
    if let Some(record) = build_candidate(page, path, line_number, line, ranges, 0, line.len())? {
        return Ok(Some(record));
    }
    let start = floor_char_boundary(line, first.0.saturating_sub(LINE_CONTEXT_BYTES));
    let end = floor_char_boundary(line, last.1.min(line.len()));
    if start >= end {
        return Ok(None);
    }
    build_candidate(page, path, line_number, line, ranges, start, end)
}

fn build_candidate(
    page: &SearchPage<'_>,
    path: &str,
    line_number: u32,
    line: &str,
    ranges: &[(usize, usize)],
    start: usize,
    end: usize,
) -> Result<Option<PendingLine>, AgentError> {
    let raw = end.saturating_sub(start);
    if raw > page.content_budget.saturating_sub(page.used) {
        // Escaping only grows, so a slice longer than the remaining budget
        // cannot fit whatever the rest of the record costs.
        return Ok(None);
    }
    let mut match_byte_ranges = Vec::with_capacity(ranges.len());
    for (range_start, range_end) in ranges {
        if *range_start < start || *range_end > end {
            return Ok(None);
        }
        match_byte_ranges.push(WorkspaceByteRange {
            start: u32::try_from(range_start - start).unwrap_or(u32::MAX),
            end: u32::try_from(range_end - start).unwrap_or(u32::MAX),
        });
    }
    let record = WorkspaceSearchMatch {
        path: path.to_owned(),
        line_number,
        line_text_byte_offset: u32::try_from(start).unwrap_or(u32::MAX),
        match_byte_ranges,
        line_text: line[start..end].to_owned(),
        line_truncated: start > 0 || end < line.len(),
    };
    let encoded = encoded_len(&record)?;
    if page.used + encoded + 1 > page.content_budget {
        return Ok(None);
    }
    Ok(Some(PendingLine {
        ranges: ranges.to_vec(),
        record,
        encoded,
    }))
}

/// Adds a built record to the page and charges its matches.
fn commit(page: &mut SearchPage<'_>, pending: PendingLine) {
    page.used += pending.encoded + 1;
    page.remaining = page
        .remaining
        .saturating_sub(u64::try_from(pending.record.match_byte_ranges.len()).unwrap_or(u64::MAX));
    page.matches.push(pending.record);
}

/// Reads one searchable file through the same Workspace boundary as
/// `workspace.read`, in chunks that charge the scan byte budget, check
/// cancellation and the deadline, and keep the whole 16 MiB ceiling honest:
/// bytes that were read are counted even when the file is then skipped.
fn read_searchable_file(
    workspace: &Workspace,
    relative: &str,
    budget: &mut ScanBudget,
) -> Result<ReadFile, AgentError> {
    let mut file = match workspace.open_regular_file_sync(relative) {
        Ok(file) => file,
        Err(error) => {
            return Ok(match error {
                // A special file or a path outside the boundary is a known
                // unsupported object, not a failed read.
                crate::workspace::WorkspaceError::NotFile
                | crate::workspace::WorkspaceError::NotDirectory
                | crate::workspace::WorkspaceError::Escape
                | crate::workspace::WorkspaceError::InvalidPath => ReadFile::Unsupported,
                _ => ReadFile::Failed,
            });
        }
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return Ok(ReadFile::Failed),
    };
    if !metadata.is_file() {
        return Ok(ReadFile::Unsupported);
    }
    if metadata.len() > MAX_READ_BYTES as u64 {
        return Ok(ReadFile::Unsupported);
    }
    let capacity = usize::try_from(metadata.len().min(MAX_READ_BYTES as u64)).unwrap_or(0) + 1;
    let mut bytes = Vec::with_capacity(capacity);
    let mut chunk = [0u8; READ_CHUNK_BYTES];
    loop {
        budget.check()?;
        if budget.time_expired() {
            // Stop reading and leave this file without new matches; matches
            // already collected from other files stay in the partial result.
            return Ok(ReadFile::StopBytes);
        }
        let read = match file.read(&mut chunk) {
            Ok(read) => read,
            Err(_) => return Ok(ReadFile::Failed),
        };
        if read == 0 {
            break;
        }
        if !budget.byte_available(u64::try_from(read).unwrap_or(u64::MAX)) {
            return Ok(ReadFile::StopBytes);
        }
        if bytes.len() + read > MAX_READ_BYTES {
            return Ok(ReadFile::Unsupported);
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    if bytes.contains(&0) || std::str::from_utf8(&bytes).is_err() {
        return Ok(ReadFile::Unsupported);
    }
    Ok(ReadFile::Text(
        String::from_utf8(bytes).expect("validated above"),
    ))
}

/// Serialized size of the result envelope with no matches, using the widest
/// possible variable fields so the measured value bounds the real response.
fn envelope_bytes(max_bytes: usize) -> Result<usize, AgentError> {
    let probe = WorkspaceSearchResult {
        matches: Vec::new(),
        next_cursor: Some(WorkspaceSearchCursor {
            path_index: u32::MAX,
            entry: u64::MAX,
            line: u32::MAX,
            line_byte_offset: u32::MAX,
            scope: "f".repeat(SCOPE_HEX_LEN),
        }),
        truncated: true,
        scan_complete: false,
        // The widest stop name keeps this bound valid for every response.
        stopped_by: WorkspaceScanStop::Deadline,
        skipped_files: u64::MAX,
        consistency: WorkspaceScanConsistency::Live,
        observed_at_unix_ms: u64::MAX,
    };
    let size = encoded_len(&probe)?;
    if size >= max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::scan::{
        ScanLimits, expire_scan_after_result, set_scan_deadline, set_scan_limits,
    };

    /// One collected match: path, line number, and absolute line byte ranges.
    type MatchedLine = (String, u32, Vec<(u64, u64)>);

    struct Fixture {
        session: SessionId,
        workspace: Arc<Workspace>,
    }

    async fn fixture(label: &str) -> Fixture {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-search-{label}-{}",
            SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        Fixture {
            session: SessionId::new().unwrap(),
            workspace,
        }
    }

    fn write(fixture: &Fixture, path: &str, bytes: impl AsRef<[u8]>) {
        let full = fixture.workspace.root().join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, bytes).unwrap();
    }

    fn request(fixture: &Fixture, query: &str) -> WorkspaceSearchRequest {
        WorkspaceSearchRequest {
            session_id: fixture.session,
            query: query.to_owned(),
            paths: None,
            case_sensitive: None,
            cursor: None,
            max_matches: None,
            max_bytes: None,
        }
    }

    async fn run(fixture: &Fixture, request: WorkspaceSearchRequest) -> WorkspaceSearchResult {
        search(
            Arc::clone(&fixture.workspace),
            request,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    /// Absolute offsets of one record's ranges inside its original line.
    fn absolute_ranges(record: &WorkspaceSearchMatch) -> Vec<(u64, u64)> {
        record
            .match_byte_ranges
            .iter()
            .map(|range| {
                (
                    u64::from(record.line_text_byte_offset) + u64::from(range.start),
                    u64::from(record.line_text_byte_offset) + u64::from(range.end),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn literal_queries_ignore_regex_metacharacters() {
        let fixture = fixture("literal").await;
        write(&fixture, "a.txt", b"a.b\naxb\na+b\n");

        let result = run(&fixture, request(&fixture, "a.b")).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].line_number, 1);
        assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
        assert!(!result.matches[0].line_truncated);
        assert_eq!(result.consistency, WorkspaceScanConsistency::Live);
        assert!(result.scan_complete);
        assert_eq!(result.stopped_by, WorkspaceScanStop::End);

        let result = run(&fixture, request(&fixture, "a+b")).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].line_number, 3);
        assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
    }

    #[tokio::test]
    async fn case_insensitive_matches_keep_original_byte_ranges() {
        let fixture = fixture("case").await;
        write(&fixture, "cafe.txt", "CAFÉ\ncafé\nplain\n".as_bytes());

        let result = run(&fixture, request(&fixture, "café")).await;
        assert_eq!(result.matches.len(), 2);
        for record in &result.matches {
            assert_eq!(record.match_byte_ranges.len(), 1);
            let range = record.match_byte_ranges[0];
            let slice = &record.line_text[range.start as usize..range.end as usize];
            assert_eq!(slice.to_lowercase(), "café");
        }
        assert_eq!(result.matches[0].line_number, 1);
        assert_eq!(result.matches[1].line_number, 2);

        let mut sensitive = request(&fixture, "café");
        sensitive.case_sensitive = Some(true);
        let result = run(&fixture, sensitive).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].line_number, 2);
    }

    #[tokio::test]
    async fn crlf_terminators_are_not_part_of_the_reported_line() {
        let fixture = fixture("crlf").await;
        write(&fixture, "crlf.txt", b"one\r\ntwo\r\nthree");

        let result = run(&fixture, request(&fixture, "two")).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].line_number, 2);
        assert_eq!(result.matches[0].line_text, "two");
        assert_eq!(result.matches[0].line_text_byte_offset, 0);
        assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
        assert!(!result.matches[0].line_truncated);

        let result = run(&fixture, request(&fixture, "three")).await;
        assert_eq!(result.matches[0].line_number, 3);
        assert_eq!(result.matches[0].line_text, "three");

        let newline = request(&fixture, "two\n");
        assert!(matches!(
            newline.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let carriage = request(&fixture, "two\r");
        assert!(matches!(
            carriage.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let empty = request(&fixture, "");
        assert!(matches!(
            empty.validate(),
            Err(AgentError::InvalidArguments)
        ));
    }

    #[tokio::test]
    async fn binary_invalid_utf8_and_oversized_files_are_skipped() {
        let fixture = fixture("skip").await;
        write(&fixture, "nul.txt", b"hit\0more");
        write(&fixture, "invalid.txt", [b'h', b'i', b't', b'\n', 0xff]);
        write(&fixture, "huge.txt", b"hit".repeat(MAX_READ_BYTES / 3 + 8));
        write(&fixture, "ok.txt", b"hit\n");

        let result = run(&fixture, request(&fixture, "hit")).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].path, "ok.txt");
        assert_eq!(result.skipped_files, 3);
        assert!(result.scan_complete);
    }

    #[tokio::test]
    async fn a_match_after_a_long_prefix_is_returned_as_a_bounded_slice() {
        let fixture = fixture("long-line").await;
        let mut line = "x".repeat(300 * 1024);
        line.push_str("needle\n");
        write(&fixture, "long.txt", line.as_bytes());

        let result = run(&fixture, request(&fixture, "needle")).await;
        assert_eq!(result.matches.len(), 1);
        let record = &result.matches[0];
        assert!(record.line_truncated);
        assert!(record.line_text_byte_offset > 0);
        assert!(record.line_text.ends_with("needle"));
        assert_eq!(absolute_ranges(record), vec![(300 * 1024, 300 * 1024 + 6)]);
        assert_eq!(result.stopped_by, WorkspaceScanStop::End);
        assert!(result.scan_complete);
    }

    #[tokio::test]
    async fn a_line_prefix_that_fits_keeps_its_matches_within_the_budget() {
        let fixture = fixture("long-prefix").await;
        let mut line = String::from("needle ");
        line.push_str(&"x".repeat(300 * 1024));
        line.push('\n');
        write(&fixture, "long.txt", line.as_bytes());

        let mut request = request(&fixture, "needle");
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let result = run(&fixture, request).await;
        assert_eq!(result.matches.len(), 1);
        let record = &result.matches[0];
        assert!(record.line_truncated);
        assert!(!record.line_text.is_empty());
        assert!(record.line_text.len() < line.len());
        assert_eq!(absolute_ranges(record), vec![(0, 6)]);
        assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
    }

    #[tokio::test]
    async fn a_record_that_cannot_fit_even_an_empty_page_is_reported_incomplete() {
        let fixture = fixture("unrepresentable").await;
        // The path alone is longer than an empty page can hold, so even a
        // bounded slice of the matching line cannot be represented.
        let deep = (0..9)
            .map(|index| format!("level-{index:02}-{}", "x".repeat(70)))
            .collect::<Vec<_>>()
            .join("/");
        write(&fixture, &format!("{deep}/quotes.txt"), b"needle\n");

        let mut request = request(&fixture, "needle");
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let result = run(&fixture, request).await;
        assert!(result.matches.is_empty());
        assert!(result.skipped_files >= 1);
        assert!(result.truncated);
        assert!(!result.scan_complete);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn escaped_matches_stay_inside_the_result_budget() {
        let fixture = fixture("escaping").await;
        let mut line = "\"".repeat(4096);
        line.push_str("needle\n");
        write(&fixture, "quotes.txt", line.as_bytes());

        let mut request = request(&fixture, "needle");
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let result = run(&fixture, request).await;
        assert_eq!(result.matches.len(), 1);
        let record = &result.matches[0];
        assert_eq!(absolute_ranges(record), vec![(4096, 4102)]);
        assert_eq!(record.line_text_byte_offset, 4096 - 64);
        assert_eq!(record.line_text.len(), 64 + 6);
        assert!(record.line_truncated);
        assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
    }

    #[tokio::test]
    async fn max_matches_splits_a_line_and_the_cursor_resumes_inside_it() {
        let fixture = fixture("split").await;
        write(&fixture, "many.txt", b"hit hit hit hit hit\n");

        let mut request = request(&fixture, "hit");
        request.max_matches = Some(2);
        let mut all = Vec::new();
        let mut pages = 0;
        loop {
            let result = run(&fixture, request.clone()).await;
            pages += 1;
            assert!(pages < 8, "split pagination did not terminate");
            assert!(
                result
                    .matches
                    .iter()
                    .all(|record| !record.line_text.is_empty())
            );
            for record in &result.matches {
                all.extend(absolute_ranges(record));
            }
            match result.next_cursor {
                Some(cursor) => request.cursor = Some(cursor),
                None => break,
            }
        }
        assert_eq!(all, vec![(0, 3), (4, 7), (8, 11), (12, 15), (16, 19)]);
        // Two matches per page: the first two pages are cut by `max_matches`,
        // and the third finds the last match and reports the end of the scope.
        assert_eq!(pages, 3);
    }

    #[tokio::test]
    async fn a_byte_budget_pages_matches_without_loss_or_duplication() {
        let fixture = fixture("byte-paging").await;
        let mut first_line = String::new();
        for _ in 0..12 {
            first_line.push_str("needle ");
        }
        first_line.push('\n');
        write(&fixture, "a.txt", first_line.as_bytes());
        write(&fixture, "b.txt", b"needle here\n");
        write(&fixture, "c.txt", b"needle\n");

        let mut request = request(&fixture, "needle");
        request.max_bytes = Some(MIN_RESULT_BYTES);
        request.max_matches = Some(20);
        let mut collected: Vec<MatchedLine> = Vec::new();
        let mut pages = 0;
        loop {
            let result = run(&fixture, request.clone()).await;
            pages += 1;
            assert!(pages < 32, "byte paging did not terminate");
            let encoded = serde_json::to_vec(&result).unwrap().len();
            assert!(encoded <= MIN_RESULT_BYTES, "page {encoded} exceeds budget");
            for record in &result.matches {
                assert!(
                    !record.match_byte_ranges.is_empty(),
                    "a record always carries at least one range"
                );
                for (start, end) in absolute_ranges(record) {
                    let from = start - u64::from(record.line_text_byte_offset);
                    let to = end - u64::from(record.line_text_byte_offset);
                    let slice = &record.line_text[from as usize..to as usize];
                    assert_eq!(slice.to_lowercase(), "needle");
                }
                if let Some(existing) = collected
                    .iter_mut()
                    .find(|(path, line, _)| *path == record.path && *line == record.line_number)
                {
                    existing.2.extend(absolute_ranges(record));
                } else {
                    collected.push((
                        record.path.clone(),
                        record.line_number,
                        absolute_ranges(record),
                    ));
                }
            }
            match result.next_cursor {
                Some(cursor) => request.cursor = Some(cursor),
                None => break,
            }
        }
        assert!(pages > 1, "the small budget must page");
        collected.sort();
        let mut expected = vec![
            (
                "a.txt".to_owned(),
                1,
                (0..12).map(|index| (index * 7, index * 7 + 6)).collect(),
            ),
            ("b.txt".to_owned(), 1, vec![(0, 6)]),
            ("c.txt".to_owned(), 1, vec![(0, 6)]),
        ];
        expected.sort();
        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn a_deadline_after_a_match_keeps_it_without_a_cursor() {
        let fixture = fixture("deadline-partial").await;
        write(&fixture, "one.txt", b"hit\n");
        write(&fixture, "two.txt", b"hit again\n");
        // The timeout lands as soon as the first match has been committed: the
        // second file is never searched.
        expire_scan_after_result(fixture.workspace.root().to_path_buf());

        let result = run(&fixture, request(&fixture, "hit")).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
        assert_eq!(result.stopped_by, WorkspaceScanStop::Deadline);
        assert!(result.truncated);
        assert!(!result.scan_complete);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn an_expired_budget_returns_an_empty_partial_instead_of_an_end() {
        let fixture = fixture("deadline-empty").await;
        write(&fixture, "one.txt", b"hit\n");
        set_scan_deadline(
            fixture.workspace.root().to_path_buf(),
            std::time::Duration::ZERO,
        );

        let result = run(&fixture, request(&fixture, "hit")).await;
        assert!(result.matches.is_empty());
        assert_eq!(result.stopped_by, WorkspaceScanStop::Deadline);
        assert!(result.truncated);
        assert!(!result.scan_complete);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn paging_across_roots_uses_each_root_ordinal() {
        let fixture = fixture("root-paging").await;
        write(&fixture, "one/.gitignore", b"skip.txt\n");
        write(&fixture, "one/nested/deep.txt", b"hit deep\n");
        write(&fixture, "one/skip.txt", b"hit skip\n");
        write(&fixture, "one/top.txt", b"hit top\n");
        write(&fixture, "two/deep/deep.txt", b"hit two\n");
        write(&fixture, "two/other.txt", b"hit other\n");

        let mut request = request(&fixture, "hit");
        request.paths = Some(vec!["one".to_owned(), "two".to_owned()]);
        request.max_matches = Some(1);
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let mut collected: Vec<MatchedLine> = Vec::new();
        for _ in 0..32 {
            let result = run(&fixture, request.clone()).await;
            let encoded = serde_json::to_vec(&result).unwrap().len();
            assert!(encoded <= MIN_RESULT_BYTES, "page {encoded} exceeds budget");
            for record in &result.matches {
                assert_eq!(absolute_ranges(record).len(), 1);
                collected.push((
                    record.path.clone(),
                    record.line_number,
                    absolute_ranges(record),
                ));
            }
            match result.next_cursor {
                Some(cursor) => request.cursor = Some(cursor),
                None => {
                    assert!(result.scan_complete);
                    break;
                }
            }
        }
        collected.sort();
        let mut expected: Vec<MatchedLine> = vec![
            ("one/nested/deep.txt".to_owned(), 1, vec![(0, 3)]),
            ("one/top.txt".to_owned(), 1, vec![(0, 3)]),
            ("two/deep/deep.txt".to_owned(), 1, vec![(0, 3)]),
            ("two/other.txt".to_owned(), 1, vec![(0, 3)]),
        ];
        expected.sort();
        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn paths_restrict_the_search_and_must_resolve() {
        let fixture = fixture("paths").await;
        write(&fixture, "one/hit.txt", b"hit\n");
        write(&fixture, "two/hit.txt", b"hit\n");

        let mut restricted = request(&fixture, "hit");
        restricted.paths = Some(vec!["two".to_owned()]);
        let result = run(&fixture, restricted).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].path, "two/hit.txt");

        let mut single = request(&fixture, "hit");
        single.paths = Some(vec!["one/hit.txt".to_owned()]);
        let result = run(&fixture, single).await;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].path, "one/hit.txt");

        let mut missing = request(&fixture, "hit");
        missing.paths = Some(vec!["missing".to_owned()]);
        let error = search(
            Arc::clone(&fixture.workspace),
            missing,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::Workspace));

        let mut escaping = request(&fixture, "hit");
        escaping.paths = Some(vec!["../escape".to_owned()]);
        assert!(matches!(
            escaping.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut many = request(&fixture, "hit");
        many.paths = Some(
            (0..(MAX_SEARCH_PATHS + 1))
                .map(|index| format!("p{index}"))
                .collect(),
        );
        assert!(matches!(many.validate(), Err(AgentError::InvalidArguments)));
        let mut budget = request(&fixture, "hit");
        budget.max_bytes = Some(MIN_RESULT_BYTES - 1);
        assert!(matches!(
            budget.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut matches = request(&fixture, "hit");
        matches.max_matches = Some(MAX_MAX_MATCHES + 1);
        assert!(matches!(
            matches.validate(),
            Err(AgentError::InvalidArguments)
        ));
    }

    #[tokio::test]
    async fn duplicate_roots_are_merged_and_overlapping_roots_are_rejected() {
        let fixture = fixture("roots").await;
        write(&fixture, "sub/hit.txt", b"hit\n");

        let mut duplicate = request(&fixture, "hit");
        duplicate.paths = Some(vec!["sub".to_owned(), "sub/".to_owned()]);
        assert!(duplicate.validate().is_ok());
        let result = run(&fixture, duplicate).await;
        assert_eq!(result.matches.len(), 1);

        for paths in [
            vec!["sub".to_owned(), "sub/hit.txt".to_owned()],
            vec![String::new(), "sub".to_owned()],
        ] {
            let mut overlapping = request(&fixture, "hit");
            overlapping.paths = Some(paths);
            assert!(matches!(
                overlapping.validate(),
                Err(AgentError::InvalidArguments)
            ));
        }
    }

    #[tokio::test]
    async fn explicit_roots_cannot_bypass_the_rules() {
        let fixture = fixture("explicit-rules").await;
        write(&fixture, ".gitignore", b"secret.txt\n");
        write(&fixture, "secret.txt", b"hit secret\n");
        write(&fixture, "kept.txt", b"hit kept\n");

        let ignored = run(&fixture, request(&fixture, "hit")).await;
        assert_eq!(ignored.matches.len(), 1);
        assert_eq!(ignored.matches[0].path, "kept.txt");

        let mut explicit = request(&fixture, "hit");
        explicit.paths = Some(vec!["secret.txt".to_owned()]);
        let result = run(&fixture, explicit).await;
        assert!(result.matches.is_empty());
        assert_eq!(result.skipped_files, 1);
        assert!(result.scan_complete);
    }

    #[tokio::test]
    async fn scan_ceiling_reports_zero_matches_and_continues() {
        let fixture = fixture("ceiling").await;
        write(&fixture, "a.txt", b"hit one\n");
        write(&fixture, "b.txt", b"hit two\n");
        set_scan_limits(
            fixture.workspace.root().to_path_buf(),
            ScanLimits {
                entries: 100,
                bytes: 6,
                depth: 64,
                rule_files: 8,
                rule_bytes: 1 << 20,
            },
        );
        let mut request = request(&fixture, "hit");
        let first = run(&fixture, request.clone()).await;
        assert!(first.matches.is_empty());
        assert!(!first.scan_complete);
        assert!(first.truncated);
        assert_eq!(first.stopped_by, WorkspaceScanStop::Bytes);
        let cursor = first.next_cursor.clone().expect("the scan continues");

        set_scan_limits(
            fixture.workspace.root().to_path_buf(),
            ScanLimits {
                entries: 100,
                bytes: 1 << 20,
                depth: 64,
                rule_files: 8,
                rule_bytes: 1 << 20,
            },
        );
        request.cursor = Some(cursor);
        let second = run(&fixture, request).await;
        assert_eq!(second.matches.len(), 2);
        assert!(second.scan_complete);
    }

    #[tokio::test]
    async fn a_cursor_from_another_query_is_rejected() {
        let fixture = fixture("cursor").await;
        write(&fixture, "a.txt", b"hit\n");
        write(&fixture, "b.txt", b"hit\n");

        let mut request = request(&fixture, "hit");
        request.max_matches = Some(1);
        let first = run(&fixture, request.clone()).await;
        let cursor = first.next_cursor.clone().expect("a cut page continues");

        for mismatched in [
            {
                let mut mismatched = request.clone();
                mismatched.query = "other".to_owned();
                mismatched
            },
            {
                let mut mismatched = request.clone();
                mismatched.case_sensitive = Some(true);
                mismatched
            },
            {
                let mut mismatched = request.clone();
                mismatched.paths = Some(vec!["a.txt".to_owned()]);
                mismatched
            },
            {
                let mut mismatched = request.clone();
                mismatched.session_id = SessionId::new().unwrap();
                mismatched
            },
        ] {
            let mut mismatched = mismatched;
            mismatched.cursor = Some(cursor.clone());
            assert!(matches!(
                mismatched.validate(),
                Err(AgentError::InvalidArguments)
            ));
            let error = search(
                Arc::clone(&fixture.workspace),
                mismatched,
                CancellationToken::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, AgentError::InvalidArguments));
        }

        let mut wrong_index = request.clone();
        wrong_index.cursor = Some(WorkspaceSearchCursor {
            path_index: 3,
            ..cursor
        });
        assert!(matches!(
            wrong_index.validate(),
            Err(AgentError::InvalidArguments)
        ));
    }

    #[tokio::test]
    async fn a_cancelled_session_stops_the_scan() {
        let fixture = fixture("cancel").await;
        write(&fixture, "a.txt", b"hit\n");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = search(
            Arc::clone(&fixture.workspace),
            request(&fixture, "hit"),
            cancelled,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::QueryLimit));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn special_files_and_escaping_symlinks_are_skipped_without_blocking() {
        use std::os::unix::fs::symlink;
        use std::time::Duration;

        let fixture = fixture("special").await;
        write(&fixture, "ok.txt", b"hit\n");
        let outside = std::env::temp_dir().join(format!(
            "minicore-workspace-search-outside-{}",
            SessionId::new().unwrap()
        ));
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), b"hit secret\n").unwrap();
        let root = fixture.workspace.root().to_path_buf();
        symlink(outside.join("secret.txt"), root.join("escape.txt")).unwrap();
        symlink(root.join("ok.txt"), root.join("link.txt")).unwrap();
        let fifo = root.join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            run(&fixture, request(&fixture, "hit")),
        )
        .await
        .expect("a special file must not block the search");
        let mut paths: Vec<String> = result
            .matches
            .iter()
            .map(|record| record.path.clone())
            .collect();
        paths.sort();
        assert_eq!(paths, vec!["link.txt", "ok.txt"]);
        assert_eq!(result.skipped_files, 2);
        let _ = std::fs::remove_dir_all(&outside);
    }
}
