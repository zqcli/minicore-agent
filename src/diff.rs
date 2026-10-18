//! Bounded, read-only review diffs for one retained native Tool change.
//!
//! One query resolves a `tool:` change reference to its retained `FileChange`,
//! compares the actual before/after snapshots with a bounded line diff, and
//! pages the structured result by raw UTF-8 byte offsets. It never writes
//! History, calls a model, mutates a file, or runs an external diff program.
//! A missing before snapshot is a real addition; an unknown, expired, or
//! corrupt snapshot is explicitly unavailable rather than an empty file.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use similar::{Algorithm, ChangeTag, DiffOp, capture_diff, group_diff_ops};
use tokio_util::sync::CancellationToken;

use crate::budget::{check_budget, encoded_len, encoded_result};
use crate::changes::{
    ChangeCommitState, ChangeCoverage, ChangeKind, ChangeOrigin, ChangeRevision, FileChange,
    stored_tool_change_ref, tool_change_ref,
};
use crate::error::AgentError;
use crate::error::StoreError;
use crate::ids::SessionId;
use crate::store::Store;
use crate::text::floor_char_boundary;
use crate::tool_data::{ToolData, ToolRef};

/// Wall-clock budget for one diff query.
pub(crate) const CHANGE_DIFF_DEADLINE: Duration = Duration::from_secs(10);
/// Bounds Myers' quadratic worst case after trimming equal ends. Larger
/// middles use a linear replacement plan without omitting any source lines.
const MAX_MYERS_LINES: usize = 2_048;
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
const MAX_CHANGE_REF_BYTES: usize = 16384;

pub(crate) fn diff_deadline() -> Instant {
    Instant::now()
        .checked_add(CHANGE_DIFF_DEADLINE)
        .unwrap_or_else(Instant::now)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    /// Some pages, lines, or context were cut by a bound.
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
    pub comparison: Option<DiffComparison>,
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
            || crate::changes::parse_workspace_ref(&self.change_ref, self.session_id).is_ok();
        if !well_formed {
            return Err(AgentError::InvalidArguments);
        }
        if self.comparison.is_some_and(|comparison| {
            (self.change_ref.starts_with("tool:") && comparison != DiffComparison::ToolBeforeAfter)
                || (self.change_ref.starts_with("workspace:")
                    && comparison == DiffComparison::ToolBeforeAfter)
        }) {
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
    /// Workspace comparisons explicitly refresh list-time observations.
    pub versions_refreshed: bool,
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

/// Builds a deterministic plan on a blocking thread. Query expiry cancels
/// the result, never changes the alignment or returns a time-dependent prefix.
pub(crate) fn plan_diff(
    before: &[u8],
    after: &[u8],
    context_lines: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<DiffOutcome, AgentError> {
    check_budget(cancellation, deadline)?;
    #[cfg(test)]
    tests::block_on_diff_gate(before, after, cancellation, deadline)?;
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
    let before_tokens = before_lines
        .iter()
        .map(|&(start, len)| &before_text[start..start + len])
        .collect::<Vec<_>>();
    let after_tokens = after_lines
        .iter()
        .map(|&(start, len)| &after_text[start..start + len])
        .collect::<Vec<_>>();
    let prefix = before_tokens
        .iter()
        .zip(&after_tokens)
        .take_while(|(a, b)| a == b)
        .count();
    let suffix = before_tokens[prefix..]
        .iter()
        .rev()
        .zip(after_tokens[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let old_end = before_count - suffix;
    let new_end = after_count - suffix;
    let replace = old_end - prefix + new_end - prefix > MAX_MYERS_LINES;
    let mut ops = Vec::new();
    if prefix > 0 {
        ops.push(DiffOp::Equal {
            old_index: 0,
            new_index: 0,
            len: prefix,
        });
    }
    if replace {
        ops.push(DiffOp::Replace {
            old_index: prefix,
            old_len: old_end - prefix,
            new_index: prefix,
            new_len: new_end - prefix,
        });
    } else {
        ops.extend(capture_diff(
            Algorithm::Myers,
            &before_tokens,
            prefix..old_end,
            &after_tokens,
            prefix..new_end,
        ));
    }
    if suffix > 0 {
        ops.push(DiffOp::Equal {
            old_index: old_end,
            new_index: new_end,
            len: suffix,
        });
    }
    let groups = group_diff_ops(ops, context_lines);
    let mut hunks = Vec::new();
    for group in groups {
        check_budget(cancellation, deadline)?;
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
            for change in op.iter_changes(&before_tokens, &after_tokens) {
                scanned += 1;
                if scanned % 256 == 0 {
                    check_budget(cancellation, deadline)?;
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
                let (start, len) = table[index.expect("diff change has a source index")];
                lines.push(LinePlan {
                    kind,
                    old_index,
                    new_index,
                    source,
                    byte_start: start,
                    byte_len: len,
                });
            }
        }
        if lines.is_empty() {
            continue;
        }
        hunks.push(HunkPlan {
            old_start,
            old_count: old_end - old_start,
            new_start,
            new_count: new_end - new_start,
            lines,
        });
    }
    check_budget(cancellation, deadline)?;
    Ok(DiffOutcome::Plan(DiffPlan {
        fingerprint: plan_fingerprint(context_lines, replace, &hunks),
        hunks,
        truncated: false,
    }))
}

fn plan_fingerprint(context_lines: usize, replace: bool, hunks: &[HunkPlan]) -> String {
    // Hash the plan field by field with a stable, explicit encoding instead of
    // building a second multi-megabyte canonical `Vec` and its JSON bytes.
    let mut hasher = Sha256::new();
    hasher.update([u8::from(replace)]);
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
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<(ToolRef, FileChange)>, AgentError> {
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
    if scan.skipped {
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
    check_budget(&cancellation, deadline)?;
    let max_bytes = request.max_bytes();
    if request.change_ref.starts_with("workspace:") {
        return encoded_result(workspace_unavailable(&request), max_bytes);
    }
    let Some((tool_ref, mut change)) = resolve_tool_change(
        &store,
        warm.as_ref(),
        request.session_id,
        &request.change_ref,
        deadline,
        &cancellation,
    )
    .await?
    else {
        return Err(AgentError::ToolNotFound);
    };
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
        versions_refreshed: false,
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
    compare_sources(
        store,
        request,
        base,
        before_bytes,
        after_bytes,
        cancellation,
        deadline,
    )
    .await
}

type DiffBytes = (Arc<[u8]>, Arc<[u8]>);

pub(crate) struct DiffSources {
    pub base: DiffResult,
    pub bytes: Option<DiffBytes>,
}

pub(crate) async fn workspace_diff(
    store: Store,
    request: ChangesDiffRequest,
    sources: DiffSources,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<DiffResult, AgentError> {
    match sources.bytes {
        Some((before, after)) => {
            compare_sources(
                store,
                request,
                sources.base,
                before,
                after,
                cancellation,
                deadline,
            )
            .await
        }
        None => encoded_result(sources.base, request.max_bytes()),
    }
}

async fn compare_sources(
    store: Store,
    request: ChangesDiffRequest,
    base: DiffResult,
    before_bytes: Arc<[u8]>,
    after_bytes: Arc<[u8]>,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<DiffResult, AgentError> {
    let context_lines = request.context_lines();
    let max_bytes = request.max_bytes();
    let query = store
        .spawn_cancellable_diff_query(
            request.session_id,
            Arc::clone(&before_bytes),
            Arc::clone(&after_bytes),
            context_lines,
            deadline,
            &cancellation,
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
                stale: request.cursor.is_some(),
                availability: DiffAvailability::Binary,
                complete: true,
                ..base
            },
            max_bytes,
        ),
        DiffOutcome::Plan(mut plan) => {
            if base.versions_refreshed {
                plan.fingerprint = crate::store::hash_bytes(
                    &serde_json::to_vec(&(
                        &plan.fingerprint,
                        &base.base_version,
                        &base.target_version,
                        base.comparison,
                    ))
                    .map_err(|_| AgentError::RpcSerialization)?,
                );
            }
            if let Some(cursor) = &request.cursor {
                if cursor.ops_fingerprint != plan.fingerprint {
                    return encoded_result(
                        DiffResult {
                            stale: true,
                            availability: DiffAvailability::Available,
                            ..base
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
                    ..base
                },
                max_bytes,
            )
        }
    }
}

pub(crate) fn workspace_unavailable(request: &ChangesDiffRequest) -> DiffResult {
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
        versions_refreshed: true,
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
    completed_bytes: usize,
    completed_hunks: usize,
    current: Option<(usize, usize, usize)>,
    max_bytes: usize,
}

impl PageMeter {
    fn fits_with(&self, line_json_len: usize) -> bool {
        let current = self.current.map(|(open, comp, lines)| {
            let comma = if lines > 0 { 1 } else { 0 };
            (open, comp + comma + line_json_len, lines + 1)
        });
        let mut sum = self.completed_bytes;
        let mut count = self.completed_hunks;
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
            self.completed_bytes += open + 2 + comp;
            self.completed_hunks += 1;
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
    let mut template = base.clone();
    template.next_cursor = Some(page_cursor(
        request,
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
    let base_empty = encoded_len(&template);
    let mut meter = PageMeter {
        base_empty,
        completed_bytes: 0,
        completed_hunks: 0,
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
            meter.open_hunk(hunk_open_len(hunk));
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
            let encoded = encoded_len(&fragment);
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
        let encoded = encoded_len(&fragment);
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
            next_cursor = Some(page_cursor(request, plan, context_lines, hi, li, offset));
            break 'outer;
        }
    }
    if hi < plan.hunks.len() {
        complete = false;
    }
    if complete {
        next_cursor = None;
    } else if next_cursor.is_none() {
        next_cursor = Some(page_cursor(request, plan, context_lines, hi, li, offset));
    }
    Ok((page, next_cursor, complete))
}

fn remove_empty_tail(page: &mut Vec<DiffHunk>) {
    while page.last().is_some_and(|hunk| hunk.lines.is_empty()) {
        page.pop();
    }
}

fn page_cursor(
    request: &ChangesDiffRequest,
    plan: &DiffPlan,
    context_lines: usize,
    hunk_index: usize,
    line_index: usize,
    line_byte_offset: usize,
) -> DiffCursor {
    DiffCursor {
        session_id: request.session_id,
        change_ref: request.change_ref.clone(),
        ops_fingerprint: plan.fingerprint.clone(),
        context_lines,
        hunk_index,
        line_index,
        line_byte_offset,
    }
}

fn hunk_open_len(hunk: &HunkPlan) -> usize {
    let empty = DiffHunk {
        old_start: hunk.old_start,
        old_count: hunk.old_count,
        new_start: hunk.new_start,
        new_count: hunk.new_count,
        lines: Vec::new(),
    };
    let encoded = encoded_len(&empty);
    encoded - 2
}

#[cfg(test)]
pub(crate) static DIFF_GATES: crate::test_support::Registry<
    String,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

#[cfg(test)]
pub(crate) mod tests;
