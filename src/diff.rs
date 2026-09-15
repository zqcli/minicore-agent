//! Bounded, read-only review diffs for one retained native Tool change.
//!
//! One query resolves a `tool:` change reference to its retained `FileChange`,
//! compares the actual before/after snapshots with a bounded line diff, and
//! pages the structured result by raw UTF-8 byte offsets. It never writes
//! History, calls a model, mutates a file, or runs an external diff program.
//! A missing before snapshot is a real addition; an unknown, expired, or
//! corrupt snapshot is explicitly unavailable rather than an empty file.

use std::cmp::min;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use similar::{Algorithm, ChangeTag, TextDiff};
use tokio_util::sync::CancellationToken;

use crate::changes::{
    ChangeCommitState, ChangeCoverage, ChangeKind, ChangeOrigin, ChangeRevision, FileChange,
    stored_tool_change_ref, tool_change_ref,
};
use crate::error::AgentError;
use crate::error::StoreError;
use crate::ids::SessionId;
use crate::store::Store;
use crate::tool_data::{ToolData, ToolRef};

/// Wall-clock budget for one diff query. The CPU comparison gets a smaller
/// budget from this value.
pub(crate) const CHANGE_DIFF_DEADLINE: Duration = Duration::from_secs(10);
/// Upper bound on the comparison itself. `similar`'s deadline is approximate
/// and has no expiry flag, so a result produced at or past it is reported as
/// truncated rather than as the optimal diff.
const CHANGE_DIFF_CPU_BUDGET: Duration = Duration::from_millis(500);
/// Combined line ceiling for both sides. Past it the comparison is not started.
const MAX_DIFF_LINES: usize = 100_000;
const DEFAULT_CONTEXT_LINES: usize = 3;
const MAX_CONTEXT_LINES: usize = 1_000;
const DEFAULT_DIFF_MAX_BYTES: usize = 64 * 1024;
/// The minimum must hold the result envelope plus a bounded continuation
/// cursor; a smaller budget is answered with a clear invalid-arguments
/// rejection rather than a page that could never continue.
const MIN_DIFF_MAX_BYTES: usize = 2048;
const MAX_DIFF_MAX_BYTES: usize = 256 * 1024;
const MAX_CHANGE_REF_BYTES: usize = 128;

pub(crate) fn diff_deadline() -> Instant {
    Instant::now()
        .checked_add(CHANGE_DIFF_DEADLINE)
        .unwrap_or_else(Instant::now)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffComparison {
    /// The tool's captured before and after snapshots of one file.
    ToolBeforeAfter,
    HeadToIndex,
    IndexToWorktree,
    HeadToWorktree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffLineKind {
    Context,
    Added,
    Removed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffAvailability {
    /// Every page of the comparison is present.
    Available,
    /// Some pages, lines, or context were cut by a bound or the CPU budget.
    Partial,
    /// At least one side is not text; no hunks are produced.
    Binary,
    /// The snapshots are unknown, expired, corrupt, or otherwise unreadable.
    Unavailable,
}

/// One page fragment of one logical diff line.
///
/// `line_byte_offset` is the offset of `text` inside the full logical line,
/// which is `line_byte_len` bytes. `line_complete` is false when the line was
/// split across pages. Concatenating fragments in order at increasing offsets
/// reproduces the exact original bytes, including CRLF and a missing final
/// newline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DiffLine {
    pub kind: DiffLineKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_index: Option<usize>,
    pub line_byte_offset: usize,
    pub line_byte_len: usize,
    pub text: String,
    pub line_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DiffHunk {
    pub old_start: usize,
    pub old_count: usize,
    pub new_start: usize,
    pub new_count: usize,
    pub lines: Vec<DiffLine>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffCursor {
    pub session_id: SessionId,
    pub change_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_ref: Option<ToolRef>,
    pub ops_fingerprint: String,
    pub context_lines: usize,
    pub hunk_index: usize,
    pub line_index: usize,
    pub line_byte_offset: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangesDiffRequest {
    pub session_id: SessionId,
    pub change_ref: String,
    #[serde(default)]
    pub context_lines: Option<usize>,
    #[serde(default)]
    pub cursor: Option<DiffCursor>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl ChangesDiffRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        if self.change_ref.is_empty() || self.change_ref.len() > MAX_CHANGE_REF_BYTES {
            return Err(AgentError::InvalidArguments);
        }
        let well_formed = self
            .change_ref
            .strip_prefix("tool:")
            .is_some_and(valid_sha256)
            || self
                .change_ref
                .strip_prefix("workspace:")
                .is_some_and(valid_sha256);
        if !well_formed {
            return Err(AgentError::InvalidArguments);
        }
        if self
            .context_lines
            .is_some_and(|value| value > MAX_CONTEXT_LINES)
        {
            return Err(AgentError::InvalidArguments);
        }
        if self
            .max_bytes
            .is_some_and(|value| !(MIN_DIFF_MAX_BYTES..=MAX_DIFF_MAX_BYTES).contains(&value))
        {
            return Err(AgentError::InvalidArguments);
        }
        if let Some(cursor) = &self.cursor {
            let context = self.context_lines.unwrap_or(DEFAULT_CONTEXT_LINES);
            if cursor.session_id != self.session_id
                || cursor.change_ref != self.change_ref
                || cursor.context_lines != context
                || !valid_sha256(&cursor.ops_fingerprint)
                || cursor
                    .tool_ref
                    .as_ref()
                    .is_some_and(|tool_ref| tool_ref.session_id != self.session_id)
            {
                return Err(AgentError::InvalidArguments);
            }
        }
        Ok(())
    }

    pub(crate) fn max_bytes(&self) -> usize {
        self.max_bytes.unwrap_or(DEFAULT_DIFF_MAX_BYTES)
    }

    pub(crate) fn context_lines(&self) -> usize {
        self.context_lines.unwrap_or(DEFAULT_CONTEXT_LINES)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct DiffResult {
    pub change_ref: String,
    pub path: String,
    pub kind: ChangeKind,
    pub origin: ChangeOrigin,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_ref: Option<ToolRef>,
    pub comparison: DiffComparison,
    pub base_version: ChangeRevision,
    pub target_version: ChangeRevision,
    pub commit_state: ChangeCommitState,
    pub coverage: ChangeCoverage,
    pub binary: bool,
    pub stale: bool,
    pub availability: DiffAvailability,
    pub hunks: Vec<DiffHunk>,
    pub complete: bool,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DiffCursor>,
}

impl fmt::Debug for DiffResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiffResult")
            .field("change_ref", &self.change_ref)
            .field("path_bytes", &self.path.len())
            .field("kind", &self.kind)
            .field("origin", &self.origin)
            .field("comparison", &self.comparison)
            .field("binary", &self.binary)
            .field("stale", &self.stale)
            .field("availability", &self.availability)
            .field("hunks", &self.hunks.len())
            .field("complete", &self.complete)
            .field("truncated", &self.truncated)
            .field("next_cursor", &self.next_cursor.is_some())
            .finish()
    }
}

/// The bounded, owned result of one CPU comparison. It carries only numeric
/// line positions, so it can cross the blocking-worker boundary without
/// retaining either source buffer.
pub(crate) enum DiffOutcome {
    Binary,
    Plan(DiffPlan),
}

pub(crate) struct DiffPlan {
    pub(crate) hunks: Vec<HunkPlan>,
    pub(crate) fingerprint: String,
    pub(crate) truncated: bool,
}

pub(crate) struct HunkPlan {
    pub(crate) old_start: usize,
    pub(crate) old_count: usize,
    pub(crate) new_start: usize,
    pub(crate) new_count: usize,
    pub(crate) lines: Vec<LinePlan>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum LineSource {
    Before,
    After,
}

pub(crate) struct LinePlan {
    pub(crate) kind: DiffLineKind,
    pub(crate) old_index: Option<usize>,
    pub(crate) new_index: Option<usize>,
    pub(crate) source: LineSource,
    pub(crate) byte_start: usize,
    pub(crate) byte_len: usize,
}

/// Runs one bounded line comparison on a blocking thread. It checks its
/// cancellation and deadline before doing any work, and reports truncation
/// rather than claiming an optimal diff after `similar`'s approximate deadline.
pub(crate) fn plan_diff(
    before: &[u8],
    after: &[u8],
    context_lines: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<DiffOutcome, AgentError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        return Err(AgentError::QueryLimit);
    }
    #[cfg(test)]
    block_on_diff_gate(before, after, cancellation);
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        return Err(AgentError::QueryLimit);
    }
    let before_text = match std::str::from_utf8(before) {
        Ok(text) => text,
        Err(_) => return Ok(DiffOutcome::Binary),
    };
    let after_text = match std::str::from_utf8(after) {
        Ok(text) => text,
        Err(_) => return Ok(DiffOutcome::Binary),
    };
    if before.contains(&0u8) || after.contains(&0u8) {
        return Ok(DiffOutcome::Binary);
    }
    let (before_lines, before_count) = split_lines(before_text, MAX_DIFF_LINES);
    if before_count > MAX_DIFF_LINES {
        return Ok(DiffOutcome::Plan(DiffPlan {
            hunks: Vec::new(),
            fingerprint: crate::store::hash_bytes(b"diff-lines-over-limit"),
            truncated: true,
        }));
    }
    let (after_lines, after_count) = split_lines(after_text, MAX_DIFF_LINES - before_count);
    if after_count > MAX_DIFF_LINES - before_count {
        return Ok(DiffOutcome::Plan(DiffPlan {
            hunks: Vec::new(),
            fingerprint: crate::store::hash_bytes(b"diff-lines-over-limit"),
            truncated: true,
        }));
    }
    let cpu_deadline = min(
        Instant::now()
            .checked_add(CHANGE_DIFF_CPU_BUDGET)
            .unwrap_or(deadline),
        deadline,
    );
    let mut config = TextDiff::configure();
    config.algorithm(Algorithm::Myers).deadline(cpu_deadline);
    let diff = config.diff_lines(before_text, after_text);
    let groups = diff.grouped_ops(context_lines);
    let mut hunks = Vec::new();
    let mut truncated = false;
    for group in groups {
        if cancellation.is_cancelled() {
            return Err(AgentError::QueryLimit);
        }
        if Instant::now() >= cpu_deadline {
            truncated = true;
            break;
        }
        // The grouped ops are contiguous, so the hunk's line spans are exactly
        // the union of their canonical old/new ranges.
        let mut old_start = usize::MAX;
        let mut old_end = 0usize;
        let mut new_start = usize::MAX;
        let mut new_end = 0usize;
        let mut lines: Vec<LinePlan> = Vec::new();
        let mut scanned = 0usize;
        for op in &group {
            let old_range = op.old_range();
            let new_range = op.new_range();
            old_start = old_start.min(old_range.start);
            old_end = old_end.max(old_range.end);
            new_start = new_start.min(new_range.start);
            new_end = new_end.max(new_range.end);
            for change in diff.iter_changes(op) {
                scanned += 1;
                // A single very large hunk must still observe cancellation and
                // the CPU deadline, not only at the hunk boundary.
                if scanned % 256 == 0 {
                    if cancellation.is_cancelled() {
                        return Err(AgentError::QueryLimit);
                    }
                    if Instant::now() >= cpu_deadline {
                        truncated = true;
                        break;
                    }
                }
                let (kind, source, old_index, new_index) = match change.tag() {
                    ChangeTag::Equal => (
                        DiffLineKind::Context,
                        LineSource::After,
                        change.old_index(),
                        change.new_index(),
                    ),
                    ChangeTag::Delete => (
                        DiffLineKind::Removed,
                        LineSource::Before,
                        change.old_index(),
                        None,
                    ),
                    ChangeTag::Insert => (
                        DiffLineKind::Added,
                        LineSource::After,
                        None,
                        change.new_index(),
                    ),
                };
                let table = match source {
                    LineSource::Before => &before_lines,
                    LineSource::After => &after_lines,
                };
                let index = match source {
                    LineSource::Before => old_index,
                    LineSource::After => new_index,
                };
                let Some((start, len)) = index.and_then(|index| table.get(index).copied()) else {
                    continue;
                };
                lines.push(LinePlan {
                    kind,
                    old_index,
                    new_index,
                    source,
                    byte_start: start,
                    byte_len: len,
                });
            }
            if truncated {
                break;
            }
        }
        if lines.is_empty() {
            continue;
        }
        let old_start = if old_start == usize::MAX {
            0
        } else {
            old_start
        };
        let new_start = if new_start == usize::MAX {
            0
        } else {
            new_start
        };
        hunks.push(HunkPlan {
            old_start,
            old_count: old_end.saturating_sub(old_start),
            new_start,
            new_count: new_end.saturating_sub(new_start),
            lines,
        });
    }
    if Instant::now() >= cpu_deadline {
        truncated = true;
    }
    Ok(DiffOutcome::Plan(DiffPlan {
        fingerprint: plan_fingerprint(context_lines, &hunks),
        hunks,
        truncated,
    }))
}

fn plan_fingerprint(context_lines: usize, hunks: &[HunkPlan]) -> String {
    // Hash the plan field by field with a stable, explicit encoding instead of
    // building a second multi-megabyte canonical `Vec` and its JSON bytes.
    let mut hasher = Sha256::new();
    hasher.update((context_lines as u64).to_le_bytes());
    hasher.update((hunks.len() as u64).to_le_bytes());
    for hunk in hunks {
        hasher.update((hunk.old_start as u64).to_le_bytes());
        hasher.update((hunk.old_count as u64).to_le_bytes());
        hasher.update((hunk.new_start as u64).to_le_bytes());
        hasher.update((hunk.new_count as u64).to_le_bytes());
        hasher.update((hunk.lines.len() as u64).to_le_bytes());
        for line in &hunk.lines {
            hasher.update([match line.kind {
                DiffLineKind::Context => 0u8,
                DiffLineKind::Added => 1,
                DiffLineKind::Removed => 2,
            }]);
            hasher.update(option_index(line.old_index));
            hasher.update(option_index(line.new_index));
            hasher.update((line.byte_start as u64).to_le_bytes());
            hasher.update((line.byte_len as u64).to_le_bytes());
        }
    }
    crate::store::digest_hex(hasher)
}

fn option_index(value: Option<usize>) -> [u8; 9] {
    match value {
        Some(index) => {
            let mut bytes = [0u8; 9];
            bytes[0] = 1;
            bytes[1..].copy_from_slice(&(index as u64).to_le_bytes());
            bytes
        }
        None => [0u8; 9],
    }
}

/// Splits `text` into `(byte_start, byte_len)` logical lines.
///
/// The scan matches the same line endings `similar` uses: `\r\n`, a bare `\n`,
/// and a bare `\r`. A trailing `\r` stays part of its line just as a trailing
/// `\n` does, so line indices and raw bytes stay aligned with the diff. The
/// scan stops once `limit` lines were produced and reports that the text has
/// more, so a huge side is never fully materialized.
fn split_lines(text: &str, limit: usize) -> (Vec<(usize, usize)>, usize) {
    let mut lines = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\n' {
            index += 1;
            lines.push((start, index - start));
            start = index;
            if lines.len() > limit {
                let count = lines.len();
                return (lines, count);
            }
        } else if byte == b'\r' {
            index += 1;
            if bytes.get(index) == Some(&b'\n') {
                index += 1;
            }
            lines.push((start, index - start));
            start = index;
            if lines.len() > limit {
                let count = lines.len();
                return (lines, count);
            }
        } else {
            index += 1;
        }
    }
    if start < bytes.len() {
        lines.push((start, bytes.len() - start));
    }
    let count = lines.len();
    (lines, count)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    if index >= text.len() {
        return text.len();
    }
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// Resolves one `tool:` change reference to its retained `FileChange`.
///
/// A warm in-memory change answers without disk I/O when it is complete. A warm
/// change that lost or corrupted one side is only upgraded from disk when the
/// disk metadata is byte-identical, and then only the unavailable side is
/// replaced, so a valid warm half is never overwritten and a different revision
/// is never substituted. Without usable warm metadata, the same bounded
/// metadata scan used by `changes.list` locates the record, and only that one
/// record's snapshots are read.
pub(crate) async fn resolve_tool_change(
    store: &Store,
    warm: Option<&Arc<ToolData>>,
    session_id: SessionId,
    change_ref: &str,
    cursor_tool_ref: Option<&ToolRef>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<(ToolRef, FileChange)>, AgentError> {
    // A cursor tool reference is only a lookup hint. It is verified against the
    // session and the resolved change reference, so a forged or stale hint can
    // never select a different change.
    if let Some(tool_ref) = cursor_tool_ref {
        if tool_ref.session_id != session_id {
            return Err(AgentError::InvalidArguments);
        }
        if let Some(resolved) = resolve_one(
            store,
            warm,
            session_id,
            change_ref,
            tool_ref,
            deadline,
            cancellation,
        )
        .await?
        {
            return Ok(Some(resolved));
        }
    }
    // Warm candidates are matched on metadata only, so unrelated before/after
    // buffers are not cloned to discover that they do not match.
    if let Some(data) = warm {
        for record in data.file_change_records(session_id, None) {
            if record.change_ref != change_ref {
                continue;
            }
            let Some(tool_ref) = record.tool_ref else {
                continue;
            };
            if let Some(resolved) = resolve_one(
                store,
                warm,
                session_id,
                change_ref,
                &tool_ref,
                deadline,
                cancellation,
            )
            .await?
            {
                return Ok(Some(resolved));
            }
        }
    }
    let scan = store
        .list_tool_changes(session_id, None, cancellation, deadline)
        .await
        .map_err(crate::sessions::map_store_error)?;
    for (tool_ref, stored) in &scan.records {
        if stored_tool_change_ref(tool_ref, stored) != change_ref {
            continue;
        }
        // The scan matched on stored metadata; the second read re-verifies the
        // change reference under the current bytes. A record that moved under
        // us, or an unreadable one, is reported as unavailable rather than
        // substituted. Only a real budget/cancellation failure propagates.
        match store
            .read_file_change_snapshots(session_id, tool_ref, deadline, cancellation)
            .await
        {
            Ok(Some(disk)) if tool_change_ref(tool_ref, &disk) == change_ref => {
                return Ok(Some((tool_ref.clone(), disk)));
            }
            Err(StoreError::QueryLimit) => return Err(AgentError::QueryLimit),
            Ok(_) | Err(_) => {
                return Ok(Some((
                    tool_ref.clone(),
                    FileChange::from_stored(stored.clone(), None, false, None, false),
                )));
            }
        }
    }
    // An incomplete scan cannot prove the reference is absent. Reporting
    // `ToolNotFound` here would be a false negative.
    if !scan.complete || scan.skipped {
        return Err(AgentError::Store);
    }
    Ok(None)
}

/// Resolves one known `tool_ref`, verifying the change reference before use.
/// The warm side owns the metadata; a disk read only supplements a missing or
/// corrupt side under byte-identical stored metadata.
async fn resolve_one(
    store: &Store,
    warm: Option<&Arc<ToolData>>,
    session_id: SessionId,
    change_ref: &str,
    tool_ref: &ToolRef,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<(ToolRef, FileChange)>, AgentError> {
    if let Some(warm_change) = warm.and_then(|data| data.file_change(tool_ref)) {
        if tool_change_ref(tool_ref, &warm_change) == change_ref {
            if warm_change.details_available() {
                return Ok(Some((tool_ref.clone(), warm_change)));
            }
            // A corrupt or unreadable disk record never overwrites the warm
            // metadata; only its missing/corrupt sides may be filled. A real
            // budget failure still propagates.
            match store
                .read_file_change_snapshots(session_id, tool_ref, deadline, cancellation)
                .await
            {
                Ok(Some(disk)) if disk.stored() == warm_change.stored() => {
                    return Ok(Some((tool_ref.clone(), merge_sides(warm_change, disk))));
                }
                Err(StoreError::QueryLimit) => return Err(AgentError::QueryLimit),
                Ok(_) | Err(_) => {}
            }
            return Ok(Some((tool_ref.clone(), warm_change)));
        }
        return Ok(None);
    }
    let disk = store
        .read_file_change_snapshots(session_id, tool_ref, deadline, cancellation)
        .await;
    let disk = match disk {
        Ok(disk) => disk,
        Err(StoreError::QueryLimit) => return Err(AgentError::QueryLimit),
        Err(_) => None,
    };
    match disk {
        Some(disk) if tool_change_ref(tool_ref, &disk) == change_ref => {
            Ok(Some((tool_ref.clone(), disk)))
        }
        _ => Ok(None),
    }
}

/// Fills each unavailable side from `disk` while keeping every valid warm side.
/// Both `FileChange` values carry byte-identical stored metadata, so the merge
/// never changes the change reference.
fn merge_sides(mut warm: FileChange, disk: FileChange) -> FileChange {
    // Cache both disk-side availability flags before moving any field out of
    // `disk`, so the second check does not borrow a partially moved value.
    let disk_before = disk.before_available();
    let disk_after = disk.after_available();
    if !warm.before_available() && disk_before {
        warm.before_bytes = disk.before_bytes;
        warm.before_corrupt = disk.before_corrupt;
    }
    if !warm.after_available() && disk_after {
        warm.after_bytes = disk.after_bytes;
        warm.after_corrupt = disk.after_corrupt;
    }
    warm
}

pub(crate) async fn changes_diff(
    store: Store,
    warm: Option<Arc<ToolData>>,
    request: ChangesDiffRequest,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<DiffResult, AgentError> {
    request.validate()?;
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        return Err(AgentError::QueryLimit);
    }
    let context_lines = request.context_lines();
    let max_bytes = request.max_bytes();
    if request.change_ref.starts_with("workspace:") {
        return encoded_result(workspace_unavailable(&request), max_bytes);
    }
    let Some((tool_ref, mut change)) = resolve_tool_change(
        &store,
        warm.as_ref(),
        request.session_id,
        &request.change_ref,
        request
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.tool_ref.as_ref()),
        deadline,
        &cancellation,
    )
    .await?
    else {
        return Err(AgentError::ToolNotFound);
    };
    if tool_change_ref(&tool_ref, &change) != request.change_ref {
        return Err(AgentError::ToolNotFound);
    }
    let base = DiffResult {
        change_ref: request.change_ref.clone(),
        path: change.path.clone(),
        kind: change.kind.clone(),
        origin: ChangeOrigin::Tool,
        tool_ref: Some(tool_ref),
        comparison: DiffComparison::ToolBeforeAfter,
        base_version: change.before.clone(),
        target_version: change.after.clone(),
        commit_state: change.commit_state,
        coverage: change.coverage,
        binary: false,
        stale: false,
        availability: DiffAvailability::Unavailable,
        hunks: Vec::new(),
        complete: false,
        truncated: false,
        next_cursor: None,
    };
    if !change.details_available() {
        return encoded_result(base, max_bytes);
    }
    let Some(before_bytes) = side_bytes(&change.before, change.before_bytes.take()) else {
        return encoded_result(base, max_bytes);
    };
    let Some(after_bytes) = side_bytes(&change.after, change.after_bytes.take()) else {
        return encoded_result(base, max_bytes);
    };
    let query = store
        .spawn_diff_query(
            request.session_id,
            Arc::clone(&before_bytes),
            Arc::clone(&after_bytes),
            context_lines,
            deadline,
        )
        .map_err(crate::sessions::map_store_error)?;
    let outcome = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(AgentError::QueryLimit),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            return Err(AgentError::QueryLimit)
        }
        result = query.wait() => result?,
    };
    match outcome {
        DiffOutcome::Binary => encoded_result(
            DiffResult {
                binary: true,
                availability: DiffAvailability::Binary,
                complete: true,
                ..clone_base(&base)
            },
            max_bytes,
        ),
        DiffOutcome::Plan(plan) => {
            if let Some(cursor) = &request.cursor {
                if cursor.ops_fingerprint != plan.fingerprint {
                    return encoded_result(
                        DiffResult {
                            stale: true,
                            availability: DiffAvailability::Available,
                            ..clone_base(&base)
                        },
                        max_bytes,
                    );
                }
            }
            let start = request
                .cursor
                .as_ref()
                .map(|cursor| {
                    (
                        cursor.hunk_index,
                        cursor.line_index,
                        cursor.line_byte_offset,
                    )
                })
                .unwrap_or((0, 0, 0));
            let (hunks, next_cursor, complete) = build_page(
                &request,
                &base,
                &plan,
                &before_bytes,
                &after_bytes,
                context_lines,
                start,
                max_bytes,
            )?;
            let availability = if plan.truncated || !complete {
                DiffAvailability::Partial
            } else {
                DiffAvailability::Available
            };
            encoded_result(
                DiffResult {
                    hunks,
                    next_cursor,
                    complete: complete && !plan.truncated,
                    truncated: plan.truncated,
                    availability,
                    ..clone_base(&base)
                },
                max_bytes,
            )
        }
    }
}

fn workspace_unavailable(request: &ChangesDiffRequest) -> DiffResult {
    DiffResult {
        change_ref: request.change_ref.clone(),
        path: String::new(),
        kind: ChangeKind::Unknown,
        origin: ChangeOrigin::WorkspaceUnknown,
        tool_ref: None,
        comparison: DiffComparison::HeadToWorktree,
        base_version: ChangeRevision::Unknown,
        target_version: ChangeRevision::Unknown,
        commit_state: ChangeCommitState::Unknown,
        coverage: ChangeCoverage::Unavailable,
        binary: false,
        stale: false,
        availability: DiffAvailability::Unavailable,
        hunks: Vec::new(),
        complete: false,
        truncated: false,
        next_cursor: None,
    }
}

fn clone_base(base: &DiffResult) -> DiffResult {
    DiffResult {
        change_ref: base.change_ref.clone(),
        path: base.path.clone(),
        kind: base.kind.clone(),
        origin: base.origin.clone(),
        tool_ref: base.tool_ref.clone(),
        comparison: base.comparison,
        base_version: base.base_version.clone(),
        target_version: base.target_version.clone(),
        commit_state: base.commit_state,
        coverage: base.coverage,
        binary: false,
        stale: false,
        availability: DiffAvailability::Unavailable,
        hunks: Vec::new(),
        complete: false,
        truncated: false,
        next_cursor: None,
    }
}

fn side_bytes(revision: &ChangeRevision, bytes: Option<Vec<u8>>) -> Option<Arc<[u8]>> {
    match revision {
        ChangeRevision::Missing => Some(Arc::from(&[][..])),
        // Move the owned snapshot into the shared buffer instead of copying it
        // through a temporary `Vec` first.
        ChangeRevision::Content { .. } => bytes.map(Arc::from),
        ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => None,
    }
}

/// A running byte accountant for the `hunks` array. Building the page only ever
/// serializes the current hunk, so paging a large diff does not re-encode all
/// previous records per candidate.
struct PageMeter {
    base_empty: usize,
    completed: Vec<usize>,
    current: Option<(usize, usize, usize)>,
    max_bytes: usize,
}

impl PageMeter {
    fn fits_with(&self, line_json_len: usize) -> bool {
        let current = self.current.map(|(open, comp, lines)| {
            let comma = if lines > 0 { 1 } else { 0 };
            (open, comp + comma + line_json_len, lines + 1)
        });
        let mut sum: usize = self.completed.iter().sum();
        let mut count = self.completed.len();
        if let Some((open, comp, _)) = current {
            sum = sum
                .saturating_add(open)
                .saturating_add(2)
                .saturating_add(comp);
            count += 1;
        }
        let content = if count == 0 {
            0
        } else {
            sum.saturating_add(count - 1)
        };
        self.base_empty.saturating_add(content) <= self.max_bytes
    }

    fn commit_line(&mut self, line_json_len: usize) {
        if let Some((open, comp, lines)) = self.current {
            let comma = if lines > 0 { 1 } else { 0 };
            self.current = Some((open, comp + comma + line_json_len, lines + 1));
        }
    }

    fn open_hunk(&mut self, full_open: usize) {
        self.current = Some((full_open, 0, 0));
    }

    fn finish_hunk(&mut self) {
        if let Some((open, comp, _)) = self.current.take() {
            self.completed.push(open + 2 + comp);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_page(
    request: &ChangesDiffRequest,
    base: &DiffResult,
    plan: &DiffPlan,
    before: &[u8],
    after: &[u8],
    context_lines: usize,
    start: (usize, usize, usize),
    max_bytes: usize,
) -> Result<(Vec<DiffHunk>, Option<DiffCursor>, bool), AgentError> {
    let mut template = clone_base(base);
    template.hunks = Vec::new();
    template.next_cursor = Some(page_cursor(
        request,
        base,
        plan,
        context_lines,
        plan.hunks.len(),
        plan.hunks
            .iter()
            .map(|hunk| hunk.lines.len())
            .max()
            .unwrap_or(0),
        before.len().max(after.len()),
    ));
    let base_empty = crate::workspace::scan::encoded_len(&template)?;
    let mut meter = PageMeter {
        base_empty,
        completed: Vec::new(),
        current: None,
        max_bytes,
    };
    let mut page: Vec<DiffHunk> = Vec::new();
    let (mut hi, mut li, mut offset) = start;
    // The only canonical end is the tuple past the last hunk. Any other tuple
    // that points outside a hunk or an inner line is rejected rather than
    // silently treated as complete.
    if hi > plan.hunks.len()
        || (hi == plan.hunks.len() && (li != 0 || offset != 0))
        || plan
            .hunks
            .get(hi)
            .is_some_and(|hunk| li >= hunk.lines.len())
    {
        return Err(AgentError::InvalidArguments);
    }
    let mut next_cursor = None;
    let mut complete = true;
    'outer: while hi < plan.hunks.len() {
        let hunk = &plan.hunks[hi];
        if li >= hunk.lines.len() {
            meter.finish_hunk();
            hi += 1;
            li = 0;
            offset = 0;
            continue;
        }
        let line = &hunk.lines[li];
        // An offset at the end of a line means that line is already complete;
        // normalize to the start of the next line instead of re-emitting it.
        if offset == line.byte_len {
            li += 1;
            offset = 0;
            continue;
        }
        if meter.current.is_none() {
            meter.open_hunk(hunk_open_len(hunk)?);
            page.push(DiffHunk {
                old_start: hunk.old_start,
                old_count: hunk.old_count,
                new_start: hunk.new_start,
                new_count: hunk.new_count,
                lines: Vec::new(),
            });
        }
        let source = match line.source {
            LineSource::Before => before,
            LineSource::After => after,
        };
        let end = line.byte_start.saturating_add(line.byte_len);
        let line_bytes = source
            .get(line.byte_start..end)
            .ok_or(AgentError::Internal)?;
        let remaining = line_bytes
            .get(offset..)
            .ok_or(AgentError::InvalidArguments)?;
        // A non-zero offset must land on a UTF-8 boundary of the real line.
        // Validate the full line first, so an interior offset is reported as
        // invalid arguments rather than as corrupted internal source bytes.
        if offset > 0 {
            let full = std::str::from_utf8(line_bytes).map_err(|_| AgentError::Internal)?;
            if !full.is_char_boundary(offset) {
                return Err(AgentError::InvalidArguments);
            }
        }
        let text = std::str::from_utf8(remaining).map_err(|_| AgentError::Internal)?;
        let mut take = text.len();
        let mut chosen: Option<(usize, bool)> = None;
        while take > 0 {
            let part = floor_char_boundary(text, take);
            if part == 0 {
                break;
            }
            let line_complete = part == text.len();
            let fragment = DiffLine {
                kind: line.kind,
                old_index: line.old_index,
                new_index: line.new_index,
                line_byte_offset: offset,
                line_byte_len: line.byte_len,
                text: text[..part].to_owned(),
                line_complete,
            };
            let encoded = crate::workspace::scan::encoded_len(&fragment)?;
            if meter.fits_with(encoded) {
                chosen = Some((part, line_complete));
                break;
            }
            take = part / 2;
        }
        let Some((part, line_complete)) = chosen else {
            remove_empty_tail(&mut page);
            if page.is_empty() {
                return Err(AgentError::InvalidArguments);
            }
            complete = false;
            break 'outer;
        };
        let fragment = DiffLine {
            kind: line.kind,
            old_index: line.old_index,
            new_index: line.new_index,
            line_byte_offset: offset,
            line_byte_len: line.byte_len,
            text: text[..part].to_owned(),
            line_complete,
        };
        let encoded = crate::workspace::scan::encoded_len(&fragment)?;
        meter.commit_line(encoded);
        page.last_mut()
            .expect("the current hunk is open")
            .lines
            .push(fragment);
        if line_complete {
            offset = 0;
            li += 1;
        } else {
            offset = offset.saturating_add(part);
            complete = false;
            next_cursor = Some(page_cursor(
                request,
                base,
                plan,
                context_lines,
                hi,
                li,
                offset,
            ));
            break 'outer;
        }
    }
    if hi < plan.hunks.len() {
        complete = false;
    }
    if complete {
        next_cursor = None;
    } else if next_cursor.is_none() {
        next_cursor = Some(page_cursor(
            request,
            base,
            plan,
            context_lines,
            hi,
            li,
            offset,
        ));
    }
    // Final safety net: the measured page plus any continuation cursor must
    // never exceed the budget, even for an unusually long ToolRef.
    let mut check = clone_base(base);
    check.hunks = page.clone();
    check.next_cursor = next_cursor.clone();
    if crate::workspace::scan::encoded_len(&check)? > max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    Ok((page, next_cursor, complete))
}

fn remove_empty_tail(page: &mut Vec<DiffHunk>) {
    while page.last().is_some_and(|hunk| hunk.lines.is_empty()) {
        page.pop();
    }
}

#[allow(clippy::too_many_arguments)]
fn page_cursor(
    request: &ChangesDiffRequest,
    _base: &DiffResult,
    plan: &DiffPlan,
    context_lines: usize,
    hunk_index: usize,
    line_index: usize,
    line_byte_offset: usize,
) -> DiffCursor {
    DiffCursor {
        session_id: request.session_id,
        change_ref: request.change_ref.clone(),
        tool_ref: None,
        ops_fingerprint: plan.fingerprint.clone(),
        context_lines,
        hunk_index,
        line_index,
        line_byte_offset,
    }
}

fn hunk_open_len(hunk: &HunkPlan) -> Result<usize, AgentError> {
    let empty = DiffHunk {
        old_start: hunk.old_start,
        old_count: hunk.old_count,
        new_start: hunk.new_start,
        new_count: hunk.new_count,
        lines: Vec::new(),
    };
    let encoded = crate::workspace::scan::encoded_len(&empty)?;
    Ok(encoded.saturating_sub(2))
}

fn encoded_result(result: DiffResult, max_bytes: usize) -> Result<DiffResult, AgentError> {
    if crate::workspace::scan::encoded_len(&result)? > max_bytes {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(result)
    }
}

#[cfg(test)]
pub(crate) struct DiffGate {
    pub(crate) entered: tokio::sync::Notify,
    released: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
}

#[cfg(test)]
impl DiffGate {
    pub(crate) fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            released: std::sync::Mutex::new(false),
            release: std::sync::Condvar::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

#[cfg(test)]
type DiffGateEntry = (String, Arc<DiffGate>);
#[cfg(test)]
static DIFF_GATES: std::sync::OnceLock<std::sync::Mutex<Vec<DiffGateEntry>>> =
    std::sync::OnceLock::new();

/// Registers a gate for the comparison of these exact bytes, so a parallel test
/// comparing different content cannot consume it.
#[cfg(test)]
pub(crate) fn gate_next_diff(before: &[u8], after: &[u8], gate: Arc<DiffGate>) {
    let key = comparison_key(before, after);
    DIFF_GATES
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((key, gate));
}

#[cfg(test)]
fn comparison_key(before: &[u8], after: &[u8]) -> String {
    let mut bytes = Vec::with_capacity(before.len() + after.len() + 16);
    bytes.extend_from_slice(&(before.len() as u64).to_le_bytes());
    bytes.extend_from_slice(before);
    bytes.extend_from_slice(&(after.len() as u64).to_le_bytes());
    bytes.extend_from_slice(after);
    crate::store::hash_bytes(&bytes)
}

/// Blocks one CPU comparison until its gate is released or the comparison is
/// cancelled. It polls with a short timeout so a cancelled owner still stops it.
#[cfg(test)]
fn block_on_diff_gate(before: &[u8], after: &[u8], cancellation: &CancellationToken) {
    let key = comparison_key(before, after);
    let Some(gate) = DIFF_GATES.get().and_then(|mutex| {
        let mut gates = mutex.lock().unwrap();
        let position = gates.iter().position(|(candidate, _)| candidate == &key)?;
        Some(gates.remove(position).1)
    }) else {
        return;
    };
    gate.entered.notify_one();
    let mut released = gate.released.lock().unwrap();
    while !*released && !cancellation.is_cancelled() {
        let (guard, _) = gate
            .release
            .wait_timeout(released, Duration::from_millis(5))
            .unwrap();
        released = guard;
    }
}

#[cfg(test)]
mod tests {
    use minicore_runtime::LoopId;
    use minicore_runtime::ToolCallId;

    use super::*;
    use crate::changes::{
        ChangeCommitState, ChangeCoverage, ChangeKind, FileChange, content_revision,
    };
    use crate::tool_data::ToolData;

    fn session() -> SessionId {
        SessionId::new().unwrap()
    }

    fn tool_ref(session_id: SessionId) -> ToolRef {
        ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-call").unwrap(),
        }
    }

    fn change(path: &str, before: &[u8], after: &[u8]) -> FileChange {
        FileChange {
            path: path.to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(before),
            after: content_revision(after),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(before.to_vec()),
            after_bytes: Some(after.to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        }
    }

    fn plan(before: &[u8], after: &[u8], context: usize) -> DiffPlan {
        match plan_diff(
            before,
            after,
            context,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap()
        {
            DiffOutcome::Plan(plan) => plan,
            DiffOutcome::Binary => panic!("expected a text plan"),
        }
    }

    /// Reconstructs the exact bytes each plan line refers to, so a test can
    /// prove the plan points at the real before/after line offsets.
    fn reconstructed(plan: &DiffPlan, before: &[u8], after: &[u8]) -> Vec<(DiffLineKind, Vec<u8>)> {
        let mut out = Vec::new();
        for hunk in &plan.hunks {
            for line in &hunk.lines {
                let source = match line.source {
                    LineSource::Before => before,
                    LineSource::After => after,
                };
                let end = line.byte_start + line.byte_len;
                out.push((line.kind, source[line.byte_start..end].to_vec()));
            }
        }
        out
    }

    #[test]
    fn line_splitting_matches_similar_mixed_endings_and_counts_before_allocating() {
        // `similar` tokenizes `\r\n`, a bare `\n`, and a bare `\r` as line ends;
        // the table must agree so indices and raw bytes stay aligned.
        let (crlf, crlf_count) = split_lines("a\r\nb\r\n", 100);
        assert_eq!(crlf, vec![(0, 3), (3, 3)]);
        assert_eq!(crlf_count, 2);
        let (mixed, mixed_count) = split_lines("a\rb\nc\r\nd", 100);
        assert_eq!(mixed, vec![(0, 2), (2, 2), (4, 3), (7, 1)]);
        assert_eq!(mixed_count, 4);
        let (blank, blank_count) = split_lines("a\n\nb\n", 100);
        assert_eq!(blank, vec![(0, 2), (2, 1), (3, 2)]);
        assert_eq!(blank_count, 3);
        let (no_final, no_final_count) = split_lines("a\nb", 100);
        assert_eq!(no_final, vec![(0, 2), (2, 1)]);
        assert_eq!(no_final_count, 2);
        let (empty, empty_count) = split_lines("", 100);
        assert!(empty.is_empty());
        assert_eq!(empty_count, 0);
        // The scan stops early and reports the ceiling breach instead of
        // materializing every line.
        let (bounded, bounded_count) = split_lines("x\ny\nz\n", 2);
        assert_eq!(bounded.len(), 3);
        assert_eq!(bounded_count, 3);
    }

    #[test]
    fn bare_cr_and_mixed_endings_plan_the_real_bytes() {
        let before = b"a\rb\nc\r\nd";
        let after = b"a\rB\nc\r\nD";
        let plan = plan(before, after, 1);
        let lines = reconstructed(&plan, before, after);
        assert!(
            lines
                .iter()
                .any(|(kind, bytes)| { *kind == DiffLineKind::Removed && bytes == b"b\n" })
        );
        assert!(
            lines
                .iter()
                .any(|(kind, bytes)| { *kind == DiffLineKind::Added && bytes == b"B\n" })
        );
        assert!(
            lines
                .iter()
                .any(|(kind, bytes)| { *kind == DiffLineKind::Removed && bytes == b"d" })
        );
        assert!(
            lines
                .iter()
                .any(|(kind, bytes)| { *kind == DiffLineKind::Added && bytes == b"D" })
        );
    }

    #[test]
    fn textual_plan_marks_added_and_removed_lines() {
        let plan = plan(b"one\ntwo\nthree\n", b"one\nTWO\nthree\n", 1);
        assert!(!plan.truncated);
        assert_eq!(plan.hunks.len(), 1);
        let kinds: Vec<_> = plan.hunks[0].lines.iter().map(|line| line.kind).collect();
        assert!(kinds.contains(&DiffLineKind::Removed));
        assert!(kinds.contains(&DiffLineKind::Added));
        assert_eq!(plan.fingerprint.len(), 64);
    }

    #[test]
    fn a_side_over_the_line_ceiling_is_truncated_not_compared() {
        // 100_001 short lines on one side trips the ceiling before the diff is
        // started; the plan reports truncation with no hunks.
        let mut before = Vec::new();
        for index in 0..100_001 {
            before.extend_from_slice(format!("{index}\n").as_bytes());
        }
        let plan = plan(&before, b"after\n", 0);
        assert!(plan.truncated);
        assert!(plan.hunks.is_empty());
    }

    #[test]
    fn binary_and_invalid_utf8_are_reported_without_replacement_characters() {
        let before = [0x00u8, 0x01, 0x02];
        let after = [0x03u8, 0xff];
        assert!(matches!(
            plan_diff(
                &before,
                &after,
                3,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap(),
            DiffOutcome::Binary
        ));
        let invalid = [0xffu8, 0xfe];
        assert!(matches!(
            plan_diff(
                b"text",
                &invalid,
                3,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap(),
            DiffOutcome::Binary
        ));
    }

    #[test]
    fn an_expired_deadline_is_a_query_limit_not_an_empty_diff() {
        assert!(matches!(
            plan_diff(
                b"a\n",
                b"b\n",
                3,
                &CancellationToken::new(),
                Instant::now() - Duration::from_millis(1),
            ),
            Err(AgentError::QueryLimit)
        ));
    }

    #[test]
    fn a_cancelled_worker_stops_before_any_comparison() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(matches!(
            plan_diff(
                b"a\n",
                b"b\n",
                3,
                &cancellation,
                Instant::now() + Duration::from_secs(5),
            ),
            Err(AgentError::QueryLimit)
        ));
    }

    fn page_base(tool_ref: Option<ToolRef>) -> DiffResult {
        DiffResult {
            change_ref: format!("tool:{}", "a".repeat(64)),
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            origin: ChangeOrigin::Tool,
            tool_ref,
            comparison: DiffComparison::ToolBeforeAfter,
            base_version: content_revision(b"x"),
            target_version: content_revision(b"y"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            binary: false,
            stale: false,
            availability: DiffAvailability::Available,
            hunks: Vec::new(),
            complete: false,
            truncated: false,
            next_cursor: None,
        }
    }

    /// Pages one plan with `max_bytes`, returning the total line bytes and the
    /// final completeness flag. Every page is asserted to stay within the
    /// budget, every fragment contiguous inside its logical line, and every
    /// completed line byte-identical to its real before/after source line.
    fn page_all(
        base: &DiffResult,
        plan: &DiffPlan,
        before: &[u8],
        after: &[u8],
        context: usize,
        max_bytes: usize,
    ) -> (Vec<u8>, bool, usize) {
        let (before_lines, _) = split_lines(
            std::str::from_utf8(before).expect("test source is UTF-8"),
            MAX_DIFF_LINES,
        );
        let (after_lines, _) = split_lines(
            std::str::from_utf8(after).expect("test source is UTF-8"),
            MAX_DIFF_LINES,
        );
        let request = ChangesDiffRequest {
            session_id: base.session_id_for_test(),
            change_ref: base.change_ref.clone(),
            context_lines: Some(context),
            cursor: None,
            max_bytes: Some(max_bytes),
        };
        let mut rebuilt = Vec::new();
        let mut line_buf = Vec::new();
        let mut line_offset = 0usize;
        let mut start = (0usize, 0usize, 0usize);
        let mut cursor = None;
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(pages < 100_000, "paging did not terminate");
            let request = ChangesDiffRequest {
                cursor: cursor.clone(),
                ..request.clone()
            };
            let (hunks, next, done) = build_page(
                &request, base, plan, before, after, context, start, max_bytes,
            )
            .unwrap();
            // The measured page must respect the budget on every step.
            let mut check = clone_base(base);
            check.hunks = hunks.clone();
            check.next_cursor = next.clone();
            assert!(crate::workspace::scan::encoded_len(&check).unwrap() <= max_bytes);
            for hunk in &hunks {
                for line in &hunk.lines {
                    assert_eq!(line.line_byte_offset, line_offset);
                    line_buf.extend_from_slice(line.text.as_bytes());
                    line_offset += line.text.len();
                    rebuilt.extend_from_slice(line.text.as_bytes());
                    if line.line_complete {
                        let expected = match (line.old_index, line.new_index) {
                            (Some(index), None) => slice(before, &before_lines, index),
                            (_, Some(index)) => slice(after, &after_lines, index),
                            (None, None) => panic!("a line has no index"),
                        };
                        assert_eq!(line_buf, expected);
                        assert_eq!(line_buf.len(), line.line_byte_len);
                        line_buf.clear();
                        line_offset = 0;
                    }
                }
            }
            if done {
                break;
            }
            let next = next.expect("an incomplete page has a cursor");
            start = (next.hunk_index, next.line_index, next.line_byte_offset);
            cursor = Some(next);
        }
        (rebuilt, true, pages)
    }

    fn slice(source: &[u8], lines: &[(usize, usize)], index: usize) -> Vec<u8> {
        let (start, len) = lines[index];
        source[start..start + len].to_vec()
    }

    impl DiffResult {
        fn session_id_for_test(&self) -> SessionId {
            match &self.tool_ref {
                Some(tool_ref) => tool_ref.session_id,
                None => session(),
            }
        }
    }

    #[test]
    fn build_page_fragments_a_long_unicode_line_losslessly_at_the_minimum_budget() {
        let long = format!("{}{}\n", "a".repeat(3_000), "é🙂世界".repeat(400));
        let base = page_base(None);
        let plan = plan(b"", long.as_bytes(), 1);
        assert_eq!(plan.hunks.len(), 1);
        let before = Vec::new();
        let after = long.as_bytes();
        let (rebuilt, complete, pages) =
            page_all(&base, &plan, &before, after, 1, MIN_DIFF_MAX_BYTES);
        assert!(complete);
        assert!(pages > 1, "a long line must span pages");
        assert_eq!(rebuilt, after);
    }

    #[test]
    fn build_page_preserves_crlf_and_a_missing_final_newline_across_pages() {
        let before = b"one\r\ntwo\r\nthree";
        let after = b"one\r\nTWO\r\nthree";
        let base = page_base(None);
        let plan = plan(before, after, 1);
        let (rebuilt, complete, _) = page_all(&base, &plan, before, after, 1, MIN_DIFF_MAX_BYTES);
        assert!(complete);
        // Reconstructed bytes are exactly the real source line text, including
        // the CRLF terminators and the missing final newline on `three`.
        assert!(rebuilt.windows(2).any(|w| w == b"\r\n"));
        assert!(rebuilt.ends_with(b"three"));
    }

    #[test]
    fn build_page_preserves_a_bare_cr_line_ending_across_pages() {
        let before = b"aaa\rbbb\r";
        let after = b"aaa\rBBB\r";
        let base = page_base(None);
        let plan = plan(before, after, 0);
        let (rebuilt, complete, _) = page_all(&base, &plan, before, after, 0, 4096);
        assert!(complete);
        // Context 0 keeps only the changed lines, and each bare-CR line is
        // reconstructed with its terminator attached.
        assert_eq!(rebuilt, b"bbb\rBBB\r");
    }

    #[test]
    fn build_page_rejects_a_forged_start_tuple() {
        let before = b"a\nb\nc\n";
        let after = b"a\nB\nc\n";
        let base = page_base(None);
        let plan = plan(before, after, 0);
        let request = ChangesDiffRequest {
            session_id: base.session_id_for_test(),
            change_ref: base.change_ref.clone(),
            context_lines: Some(0),
            cursor: None,
            max_bytes: Some(MIN_DIFF_MAX_BYTES),
        };
        let lines = plan.hunks[0].lines.len();
        // Past the last hunk with a non-canonical tail is not a valid end.
        assert!(matches!(
            build_page(
                &request,
                &base,
                &plan,
                before,
                after,
                0,
                (plan.hunks.len(), 1, 0),
                MIN_DIFF_MAX_BYTES,
            ),
            Err(AgentError::InvalidArguments)
        ));
        // A line index past the hunk's lines is rejected, not skipped.
        assert!(matches!(
            build_page(
                &request,
                &base,
                &plan,
                before,
                after,
                0,
                (0, lines + 5, 0),
                MIN_DIFF_MAX_BYTES,
            ),
            Err(AgentError::InvalidArguments)
        ));
        // A byte offset past the line is rejected.
        let byte_len = plan.hunks[0].lines[0].byte_len;
        assert!(matches!(
            build_page(
                &request,
                &base,
                &plan,
                before,
                after,
                0,
                (0, 0, byte_len + 1),
                MIN_DIFF_MAX_BYTES,
            ),
            Err(AgentError::InvalidArguments)
        ));
        // The canonical end tuple is accepted and yields an empty complete page.
        let (hunks, cursor, complete) = build_page(
            &request,
            &base,
            &plan,
            before,
            after,
            0,
            (plan.hunks.len(), 0, 0),
            MIN_DIFF_MAX_BYTES,
        )
        .unwrap();
        assert!(hunks.is_empty());
        assert!(cursor.is_none());
        assert!(complete);
    }

    #[test]
    fn build_page_rejects_a_non_utf8_boundary_offset() {
        // "é" is two bytes, so offset 1 lands inside the first character.
        let before = b"\xc3\xa9\n";
        let after = b"x\n";
        let base = page_base(None);
        let plan = plan(before, after, 0);
        let request = ChangesDiffRequest {
            session_id: base.session_id_for_test(),
            change_ref: base.change_ref.clone(),
            context_lines: Some(0),
            cursor: None,
            max_bytes: Some(MIN_DIFF_MAX_BYTES),
        };
        let removed = plan.hunks[0]
            .lines
            .iter()
            .position(|line| line.source == LineSource::Before)
            .expect("a removed line exists");
        assert!(matches!(
            build_page(
                &request,
                &base,
                &plan,
                before,
                after,
                0,
                (0, removed, 1),
                MIN_DIFF_MAX_BYTES,
            ),
            Err(AgentError::InvalidArguments)
        ));
    }

    #[test]
    fn cursor_bound_page_refuses_a_tool_ref_that_cannot_fit() {
        let tool_call_id = ToolCallId::new("long-call").unwrap();
        let session_id = session();
        let long_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id,
        };
        let mut base = page_base(Some(long_ref.clone()));
        base.path = "x".repeat(4_000);
        let before = b"a\n";
        let after = b"a\nb\n";
        let plan = plan(before, after, 0);
        let request = ChangesDiffRequest {
            session_id,
            change_ref: base.change_ref.clone(),
            context_lines: Some(0),
            cursor: None,
            max_bytes: Some(MIN_DIFF_MAX_BYTES),
        };
        // A path this long cannot fit the minimum budget, so the final
        // encoded-size check refuses the page instead of overrunning it.
        assert!(matches!(
            build_page(
                &request,
                &base,
                &plan,
                before,
                after,
                0,
                (0, 0, 0),
                MIN_DIFF_MAX_BYTES,
            ),
            Err(AgentError::InvalidArguments)
        ));
        // A short ToolRef fits and pages normally.
        let short_base = page_base(Some(ToolRef {
            session_id,
            loop_id: long_ref.loop_id,
            request_index: 0,
            tool_call_id: ToolCallId::new("short").unwrap(),
        }));
        let (rebuilt, complete, _) =
            page_all(&short_base, &plan, before, after, 0, MIN_DIFF_MAX_BYTES);
        assert!(complete);
        assert!(rebuilt.ends_with(b"\n"));
    }

    #[test]
    fn tool_data_candidates_and_lookup_are_identity_scoped() {
        let session_id = session();
        let tool_ref = tool_ref(session_id);
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(&tool_ref, change("value.txt", b"user\n", b"agent\n"));
        assert!(data.file_change(&tool_ref).is_some());
        let candidates = data.file_change_records(session_id, None);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].tool_ref.as_ref(), Some(&tool_ref));
        assert_eq!(
            candidates[0].change_ref,
            crate::changes::tool_change_ref(&tool_ref, &change("value.txt", b"user\n", b"agent\n"))
        );
        let other = session();
        assert!(data.file_change_records(other, None).is_empty());
    }
}
