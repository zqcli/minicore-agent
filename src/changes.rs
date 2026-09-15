use std::fmt;
#[cfg(test)]
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use minicore_runtime::LoopId;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::store::Store;
use crate::tool_data::{ToolData, ToolRef};
use crate::workspace::status::{
    WorkspaceStatusEntry, WorkspaceStatusEntryKind, WorkspaceStatusResult,
};

pub(crate) const CHANGE_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const MAX_CHANGE_SNAPSHOT_BYTES: usize = 512 * 1024;
pub(crate) const CHANGE_METADATA_BYTES: usize = 512;
const DEFAULT_CHANGE_LIMIT: usize = 100;
const MAX_CHANGE_LIMIT: usize = 1_000;
const DEFAULT_CHANGE_MAX_BYTES: usize = 64 * 1024;
const MIN_CHANGE_MAX_BYTES: usize = 1_024;
const MAX_CHANGE_MAX_BYTES: usize = 256 * 1024;

pub(crate) fn change_deadline() -> Instant {
    Instant::now()
        .checked_add(CHANGE_DEADLINE)
        .unwrap_or_else(Instant::now)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    Conflict,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOrigin {
    Tool,
    WorkspaceUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangeRevision {
    Missing,
    Content {
        sha256: String,
        bytes: usize,
    },
    Metadata {
        bytes: u64,
        modified_unix_ms: Option<u64>,
    },
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeCommitState {
    NotCommitted,
    Applied,
    Conflict,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeCoverage {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeListConsistency {
    Live,
    Cold,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeListWarning {
    NoRepository,
    StatusIncomplete,
    DetailsUnavailable,
    RecordsSkipped,
    StaleCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeRecord {
    pub change_ref: String,
    pub path: String,
    pub kind: ChangeKind,
    pub origin: ChangeOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_ref: Option<ToolRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_path: Option<String>,
    pub before: ChangeRevision,
    pub after: ChangeRevision,
    pub commit_state: ChangeCommitState,
    pub details_available: bool,
    pub coverage: ChangeCoverage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangeScope {
    Workspace,
    Session,
    Turn { loop_id: LoopId },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeCursor {
    pub session_id: crate::ids::SessionId,
    pub scope: ChangeScope,
    pub offset: usize,
    #[serde(default)]
    pub observation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangesListRequest {
    pub session_id: crate::ids::SessionId,
    pub scope: ChangeScope,
    #[serde(default)]
    pub cursor: Option<ChangeCursor>,
    #[serde(default = "default_change_limit")]
    pub limit: usize,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl ChangesListRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        if !(1..=MAX_CHANGE_LIMIT).contains(&self.limit) {
            return Err(AgentError::InvalidArguments);
        }
        if self
            .max_bytes
            .is_some_and(|value| !(MIN_CHANGE_MAX_BYTES..=MAX_CHANGE_MAX_BYTES).contains(&value))
        {
            return Err(AgentError::InvalidArguments);
        }
        if let Some(cursor) = &self.cursor {
            if cursor.session_id != self.session_id || cursor.scope != self.scope {
                return Err(AgentError::InvalidArguments);
            }
            if cursor.offset > 0 && cursor.observation.is_none() {
                return Err(AgentError::InvalidArguments);
            }
            if cursor.observation.as_deref().is_some_and(|value| {
                value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
            }) {
                return Err(AgentError::InvalidArguments);
            }
        }
        Ok(())
    }

    pub(crate) fn max_bytes(&self) -> usize {
        self.max_bytes.unwrap_or(DEFAULT_CHANGE_MAX_BYTES)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangesListResult {
    pub session_id: crate::ids::SessionId,
    pub scope: ChangeScope,
    pub records: Vec<ChangeRecord>,
    pub next_cursor: Option<ChangeCursor>,
    pub total: usize,
    pub complete: bool,
    pub stale: bool,
    pub consistency: ChangeListConsistency,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at_unix_ms: Option<u64>,
    pub warnings: Vec<ChangeListWarning>,
}

/// The metadata retained inside one native file-tool record. Snapshot bytes are
/// deliberately kept outside this serializable value and go to before.bin and
/// after.bin when the existing auxiliary budgets allow it.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredFileChange {
    pub(crate) path: String,
    pub(crate) kind: ChangeKind,
    pub(crate) before: ChangeRevision,
    pub(crate) after: ChangeRevision,
    pub(crate) commit_state: ChangeCommitState,
    pub(crate) coverage: ChangeCoverage,
    pub(crate) before_captured: bool,
    pub(crate) after_captured: bool,
}

impl fmt::Debug for StoredFileChange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredFileChange")
            .field("path_bytes", &self.path.len())
            .field("kind", &self.kind)
            .field("before", &self.before)
            .field("after", &self.after)
            .field("commit_state", &self.commit_state)
            .field("coverage", &self.coverage)
            .field("before_captured", &self.before_captured)
            .field("after_captured", &self.after_captured)
            .finish()
    }
}

/// The in-memory form adds the bounded raw snapshots and their independent
/// corruption flags. Metadata remains queryable when either blob is evicted.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct FileChange {
    pub(crate) path: String,
    pub(crate) kind: ChangeKind,
    pub(crate) before: ChangeRevision,
    pub(crate) after: ChangeRevision,
    pub(crate) commit_state: ChangeCommitState,
    pub(crate) coverage: ChangeCoverage,
    pub(crate) before_captured: bool,
    pub(crate) after_captured: bool,
    pub(crate) before_bytes: Option<Vec<u8>>,
    pub(crate) after_bytes: Option<Vec<u8>>,
    pub(crate) before_corrupt: bool,
    pub(crate) after_corrupt: bool,
}

impl FileChange {
    /// True when the before side is captured and its retained bytes still match
    /// the recorded revision. Used to decide whether a disk read may fill a
    /// missing or corrupt side without ever overwriting a valid warm one.
    pub(crate) fn before_available(&self) -> bool {
        self.before_captured
            && !self.before_corrupt
            && revision_matches_snapshot(&self.before, self.before_bytes.as_deref(), true)
    }

    pub(crate) fn after_available(&self) -> bool {
        self.after_captured
            && !self.after_corrupt
            && revision_matches_snapshot(&self.after, self.after_bytes.as_deref(), false)
    }

    pub(crate) fn details_available(&self) -> bool {
        self.before_available() && self.after_available()
    }

    pub(crate) fn needs_stored(&self) -> bool {
        (self.before_captured
            && matches!(&self.before, ChangeRevision::Content { .. })
            && self.before_bytes.is_none()
            && !self.before_corrupt)
            || (self.after_captured
                && matches!(&self.after, ChangeRevision::Content { .. })
                && self.after_bytes.is_none()
                && !self.after_corrupt)
            || self.before_corrupt
            || self.after_corrupt
    }

    pub(crate) fn stored(&self) -> StoredFileChange {
        StoredFileChange {
            path: self.path.clone(),
            kind: self.kind.clone(),
            before: self.before.clone(),
            after: self.after.clone(),
            commit_state: self.commit_state,
            coverage: self.coverage,
            before_captured: self.before_captured,
            after_captured: self.after_captured,
        }
    }

    pub(crate) fn from_stored(
        stored: StoredFileChange,
        before_bytes: Option<Vec<u8>>,
        before_corrupt: bool,
        after_bytes: Option<Vec<u8>>,
        after_corrupt: bool,
    ) -> Self {
        Self {
            path: stored.path,
            kind: stored.kind,
            before: stored.before,
            after: stored.after,
            commit_state: stored.commit_state,
            coverage: stored.coverage,
            before_captured: stored.before_captured,
            after_captured: stored.after_captured,
            before_bytes,
            after_bytes,
            before_corrupt,
            after_corrupt,
        }
    }

    pub(crate) fn record(&self, tool_ref: &ToolRef) -> ChangeRecord {
        ChangeRecord {
            change_ref: tool_change_ref(tool_ref, self),
            path: self.path.clone(),
            kind: self.kind.clone(),
            origin: ChangeOrigin::Tool,
            tool_ref: Some(tool_ref.clone()),
            original_path: None,
            before: self.before.clone(),
            after: self.after.clone(),
            commit_state: self.commit_state,
            details_available: self.details_available(),
            coverage: self.coverage,
        }
    }
}

fn revision_matches_snapshot(
    revision: &ChangeRevision,
    bytes: Option<&[u8]>,
    allow_missing: bool,
) -> bool {
    match (revision, bytes) {
        (ChangeRevision::Missing, None) if allow_missing => true,
        (
            ChangeRevision::Content {
                sha256,
                bytes: size,
            },
            Some(value),
        ) => *size == value.len() && crate::store::hash_bytes(value).as_str() == sha256.as_str(),
        _ => false,
    }
}

impl StoredFileChange {
    pub(crate) fn record(&self, tool_ref: &ToolRef, details_available: bool) -> ChangeRecord {
        ChangeRecord {
            change_ref: stored_tool_change_ref(tool_ref, self),
            path: self.path.clone(),
            kind: self.kind.clone(),
            origin: ChangeOrigin::Tool,
            tool_ref: Some(tool_ref.clone()),
            original_path: None,
            before: self.before.clone(),
            after: self.after.clone(),
            commit_state: self.commit_state,
            details_available,
            coverage: self.coverage,
        }
    }
}

pub(crate) fn content_revision(bytes: &[u8]) -> ChangeRevision {
    ChangeRevision::Content {
        sha256: crate::store::hash_bytes(bytes),
        bytes: bytes.len(),
    }
}

pub(crate) fn metadata_revision(bytes: u64, modified_unix_ms: Option<u64>) -> ChangeRevision {
    ChangeRevision::Metadata {
        bytes,
        modified_unix_ms,
    }
}

pub(crate) fn tool_change_ref(tool_ref: &ToolRef, change: &FileChange) -> String {
    stored_tool_change_ref(tool_ref, &change.stored())
}

pub(crate) fn stored_tool_change_ref(tool_ref: &ToolRef, change: &StoredFileChange) -> String {
    let bytes = serde_json::to_vec(&(tool_ref, change))
        .expect("serializing a bounded change reference cannot fail");
    format!("tool:{}", crate::store::hash_bytes(&bytes))
}

fn workspace_change_ref(
    session_id: crate::ids::SessionId,
    head_oid: &str,
    entry: &WorkspaceStatusEntry,
) -> String {
    let bytes = serde_json::to_vec(&(session_id, head_oid, entry))
        .expect("serializing a bounded workspace reference cannot fail");
    use base64::Engine;
    format!(
        "workspace:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

pub(crate) fn parse_workspace_ref(
    value: &str,
    session_id: crate::SessionId,
) -> Result<(String, WorkspaceStatusEntry), AgentError> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(
            value
                .strip_prefix("workspace:")
                .ok_or(AgentError::InvalidArguments)?,
        )
        .map_err(|_| AgentError::InvalidArguments)?;
    let (session, head, entry): (crate::SessionId, String, WorkspaceStatusEntry) =
        serde_json::from_slice(&bytes).map_err(|_| AgentError::InvalidArguments)?;
    if session != session_id
        || (!head.is_empty()
            && !([40, 64].contains(&head.len())
                && head.bytes().all(|byte| byte.is_ascii_hexdigit())))
    {
        return Err(AgentError::InvalidArguments);
    }
    for path in std::iter::once(&entry.path).chain(entry.original_path.iter()) {
        if path.len() > 4096 {
            return Err(AgentError::InvalidArguments);
        }
        crate::workspace::validate_relative_path(path).map_err(|_| AgentError::InvalidArguments)?;
    }
    Ok((head, entry))
}

fn workspace_observation(status: &WorkspaceStatusResult) -> String {
    #[derive(Serialize)]
    struct Observation<'a> {
        repo_available: bool,
        head_oid: &'a Option<String>,
        branch: &'a Option<String>,
        detached: bool,
        staged: u64,
        unstaged: u64,
        untracked: u64,
        conflicted: u64,
        entries: &'a [WorkspaceStatusEntry],
        skipped_paths: u64,
        complete: bool,
        warnings: &'a [crate::workspace::status::WorkspaceStatusWarning],
    }
    let value = Observation {
        repo_available: status.repo_available,
        head_oid: &status.head_oid,
        branch: &status.branch,
        detached: status.detached,
        staged: status.staged,
        unstaged: status.unstaged,
        untracked: status.untracked,
        conflicted: status.conflicted,
        entries: &status.entries,
        skipped_paths: status.skipped_paths,
        complete: status.complete,
        warnings: &status.warnings,
    };
    let bytes =
        serde_json::to_vec(&value).expect("serializing a workspace observation cannot fail");
    crate::store::hash_bytes(&bytes)
}

pub(crate) fn workspace_records(
    session_id: crate::ids::SessionId,
    status: &WorkspaceStatusResult,
) -> (Vec<ChangeRecord>, String, Vec<ChangeListWarning>) {
    let observation = workspace_observation(status);
    let mut warnings = Vec::new();
    if !status.repo_available {
        warnings.push(ChangeListWarning::NoRepository);
    }
    if !status.complete {
        warnings.push(ChangeListWarning::StatusIncomplete);
    }
    if status.skipped_paths > 0 {
        push_warning(&mut warnings, ChangeListWarning::RecordsSkipped);
    }
    let records = status
        .entries
        .iter()
        .map(|entry| ChangeRecord {
            change_ref: workspace_change_ref(
                session_id,
                status.head_oid.as_deref().unwrap_or(""),
                entry,
            ),
            path: entry.path.clone(),
            kind: workspace_kind(entry),
            origin: ChangeOrigin::WorkspaceUnknown,
            tool_ref: None,
            original_path: entry.original_path.clone(),
            before: ChangeRevision::Unknown,
            after: ChangeRevision::Unknown,
            commit_state: ChangeCommitState::Unknown,
            details_available: false,
            coverage: ChangeCoverage::Unavailable,
        })
        .collect();
    (records, observation, warnings)
}

fn workspace_kind(entry: &WorkspaceStatusEntry) -> ChangeKind {
    match entry.kind {
        WorkspaceStatusEntryKind::Renamed => ChangeKind::Renamed,
        WorkspaceStatusEntryKind::Unmerged => ChangeKind::Conflict,
        WorkspaceStatusEntryKind::Untracked => ChangeKind::Added,
        WorkspaceStatusEntryKind::Ordinary => {
            let statuses = [
                entry.index_status.as_deref(),
                entry.worktree_status.as_deref(),
            ];
            if statuses.iter().flatten().any(|status| *status == "A") {
                ChangeKind::Added
            } else if statuses.iter().flatten().any(|status| *status == "D") {
                ChangeKind::Deleted
            } else {
                ChangeKind::Modified
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn page_records(
    request: &ChangesListRequest,
    mut records: Vec<ChangeRecord>,
    observation: String,
    consistency: ChangeListConsistency,
    observed_at_unix_ms: Option<u64>,
    mut warnings: Vec<ChangeListWarning>,
    complete: bool,
    cancellation: &CancellationToken,
    deadline: Instant,
    page_end: Option<usize>,
) -> Result<ChangesListResult, AgentError> {
    request.validate()?;
    if records.iter().any(|record| !record.details_available) {
        push_warning(&mut warnings, ChangeListWarning::DetailsUnavailable);
    }
    records.sort_by(|left, right| left.change_ref.cmp(&right.change_ref));
    let total = records.len();
    let cursor = request.cursor.as_ref();
    let offset = cursor.map_or(0, |cursor| cursor.offset);
    if cursor
        .and_then(|cursor| cursor.observation.as_deref())
        .is_some_and(|value| value != observation)
    {
        push_warning(&mut warnings, ChangeListWarning::StaleCursor);
        let result = ChangesListResult {
            session_id: request.session_id,
            scope: request.scope.clone(),
            records: Vec::new(),
            next_cursor: None,
            total,
            complete: false,
            stale: true,
            consistency,
            observed_at_unix_ms,
            warnings,
        };
        return encoded_result(result, request.max_bytes());
    }
    if offset > total {
        return Err(AgentError::InvalidArguments);
    }
    let end = plan_page_end(
        request,
        &records,
        &observation,
        consistency,
        observed_at_unix_ms,
        &warnings,
        page_end,
        (cancellation, deadline),
    )
    .await?;
    check_budget(cancellation, deadline)?;
    let result = ChangesListResult {
        session_id: request.session_id,
        scope: request.scope.clone(),
        records: records[offset..end].to_vec(),
        next_cursor: (end < total).then_some(ChangeCursor {
            session_id: request.session_id,
            scope: request.scope.clone(),
            offset: end,
            observation: Some(observation),
        }),
        total,
        complete,
        stale: false,
        consistency,
        observed_at_unix_ms,
        warnings,
    };
    encoded_result(result, request.max_bytes())
}

/// The one pagination plan. It greedily selects the longest record prefix that
/// fits the encoded byte budget under worst-case header/warnings, never
/// re-serializing an owned page clone per candidate. `page_end`, when present,
/// is an already-reserved upper bound from an earlier planning pass; because the
/// reservation is a superset and `complete: false` is the longer encoding, the
/// final plan is capped by it and every already-verified record stays selected.
#[allow(clippy::too_many_arguments)]
async fn plan_page_end(
    request: &ChangesListRequest,
    records: &[ChangeRecord],
    observation: &str,
    consistency: ChangeListConsistency,
    observed_at_unix_ms: Option<u64>,
    warnings: &[ChangeListWarning],
    page_end: Option<usize>,
    reservation: (&CancellationToken, Instant),
) -> Result<usize, AgentError> {
    request.validate()?;
    // Blob verification can add `RecordsSkipped` and flip `complete` to false
    // after the pre-plan, while `DetailsUnavailable` only ever disappears. Plan
    // against the reserved superset so the final page can only shrink and no
    // already-hashed record falls outside the byte budget.
    let mut reserved = warnings.to_vec();
    push_warning(&mut reserved, ChangeListWarning::DetailsUnavailable);
    push_warning(&mut reserved, ChangeListWarning::RecordsSkipped);
    let total = records.len();
    let offset = request.cursor.as_ref().map_or(0, |cursor| cursor.offset);
    if offset > total {
        return Err(AgentError::InvalidArguments);
    }
    let bound = page_end.unwrap_or(total).min(total).max(offset);
    let mut end = offset;
    for _ in records[offset..bound].iter().take(request.limit) {
        let (cancellation, deadline) = reservation;
        check_budget(cancellation, deadline)?;
        end = end.saturating_add(1);
        let candidate = CandidateResult {
            session_id: request.session_id,
            scope: &request.scope,
            records: &records[offset..end],
            next_cursor: (end < total).then_some(ChangeCursor {
                session_id: request.session_id,
                scope: request.scope.clone(),
                offset: end,
                observation: Some(observation.to_owned()),
            }),
            total,
            // `false` is the longer encoding, so this stays an upper bound even
            // when blob verification flips `complete` after the pre-plan.
            complete: false,
            stale: false,
            consistency,
            observed_at_unix_ms,
            warnings: &reserved,
        };
        if serde_json::to_vec(&candidate)
            .map_err(|_| AgentError::RpcSerialization)?
            .len()
            > request.max_bytes()
        {
            end = end.saturating_sub(1);
            break;
        }
        tokio::task::yield_now().await;
    }
    if end == offset && offset < total {
        return Err(AgentError::InvalidArguments);
    }
    Ok(end)
}

/// Borrowed mirror of `ChangesListResult` used only to measure an encoded page
/// without cloning the records it already contains.
#[derive(Serialize)]
struct CandidateResult<'a> {
    session_id: crate::ids::SessionId,
    scope: &'a ChangeScope,
    records: &'a [ChangeRecord],
    next_cursor: Option<ChangeCursor>,
    total: usize,
    complete: bool,
    stale: bool,
    consistency: ChangeListConsistency,
    #[serde(skip_serializing_if = "Option::is_none")]
    observed_at_unix_ms: Option<u64>,
    warnings: &'a [ChangeListWarning],
}

pub(crate) async fn list_tool_changes(
    store: Store,
    warm: Option<std::sync::Arc<ToolData>>,
    request: ChangesListRequest,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<ChangesListResult, AgentError> {
    request.validate()?;
    check_budget(&cancellation, deadline)?;
    #[cfg(test)]
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(AgentError::QueryLimit),
        _ = check_change_list_gate(request.session_id) => {}
    }
    let loop_id = match &request.scope {
        ChangeScope::Turn { loop_id } => Some(*loop_id),
        ChangeScope::Workspace | ChangeScope::Session => None,
    };
    let warm_records = warm
        .as_ref()
        .map(|data| data.file_change_records(request.session_id, loop_id))
        .unwrap_or_default();
    let scan = match store
        .list_tool_changes(request.session_id, loop_id, &cancellation, deadline)
        .await
    {
        Ok(scan) => scan,
        Err(error) => {
            let mapped = crate::sessions::map_store_error(error);
            if matches!(&mapped, AgentError::QueryLimit) || warm_records.is_empty() {
                return Err(mapped);
            }
            crate::store::ToolChangeScan::unavailable()
        }
    };

    let mut records = warm_records;
    let mut warnings = Vec::new();
    let mut complete = scan.complete;
    if scan.skipped {
        push_warning(&mut warnings, ChangeListWarning::RecordsSkipped);
    }
    for (tool_ref, change) in &scan.records {
        if let Some(record) = records
            .iter_mut()
            .find(|record| record.tool_ref.as_ref() == Some(tool_ref))
        {
            if !compatible_change(record, tool_ref, change) {
                complete = false;
                push_warning(&mut warnings, ChangeListWarning::RecordsSkipped);
            }
            continue;
        }
        records.push(change.record(tool_ref, false));
    }
    records.sort_by(|left, right| left.change_ref.cmp(&right.change_ref));
    let observation = tool_observation(&records, &scan.observation, complete);
    if request
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.observation.as_deref())
        .is_some_and(|cursor| cursor != observation)
    {
        return page_records(
            &request,
            records,
            observation,
            if warm.is_some() {
                ChangeListConsistency::Live
            } else {
                ChangeListConsistency::Cold
            },
            None,
            warnings,
            complete,
            &cancellation,
            deadline,
            None,
        )
        .await;
    }
    let observation_complete = complete;
    let consistency = if warm.is_some() {
        ChangeListConsistency::Live
    } else {
        ChangeListConsistency::Cold
    };
    let page_end = plan_page_end(
        &request,
        &records,
        &observation,
        consistency,
        None,
        &warnings,
        None,
        (&cancellation, deadline),
    )
    .await?;
    let offset = request.cursor.as_ref().map_or(0, |cursor| cursor.offset);
    let mut blob_budget = crate::store::ChangeBlobBudget {
        used_bytes: 0,
        exhausted: false,
    };
    for record in records
        .iter_mut()
        .skip(offset)
        .take(page_end.saturating_sub(offset))
    {
        if record.details_available {
            continue;
        }
        let Some(tool_ref) = record.tool_ref.clone() else {
            continue;
        };
        let Some((_, change)) = scan
            .records
            .iter()
            .find(|(candidate, _)| candidate == &tool_ref)
        else {
            continue;
        };
        if !compatible_change(record, &tool_ref, change) {
            continue;
        }
        let available = store
            .change_blobs_available(
                request.session_id,
                &tool_ref,
                change,
                &cancellation,
                deadline,
                &mut blob_budget,
            )
            .await
            .map_err(crate::sessions::map_store_error)?;
        record.details_available = available;
        if blob_budget.exhausted {
            complete = false;
            push_warning(&mut warnings, ChangeListWarning::RecordsSkipped);
            break;
        }
    }
    let observation = tool_observation(&records, &scan.observation, observation_complete);
    page_records(
        &request,
        records,
        observation,
        consistency,
        None,
        warnings,
        complete,
        &cancellation,
        deadline,
        Some(page_end),
    )
    .await
}

pub(crate) async fn list_workspace_changes(
    request: ChangesListRequest,
    status: WorkspaceStatusResult,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<ChangesListResult, AgentError> {
    request.validate()?;
    if !matches!(&request.scope, ChangeScope::Workspace) {
        return Err(AgentError::InvalidArguments);
    }
    check_budget(&cancellation, deadline)?;
    let (records, observation, warnings) = workspace_records(request.session_id, &status);
    page_records(
        &request,
        records,
        observation,
        ChangeListConsistency::Live,
        Some(status.observed_at_unix_ms),
        warnings,
        status.complete,
        &cancellation,
        deadline,
        None,
    )
    .await
}

fn compatible_change(record: &ChangeRecord, tool_ref: &ToolRef, change: &StoredFileChange) -> bool {
    record.origin == ChangeOrigin::Tool
        && record.tool_ref.as_ref() == Some(tool_ref)
        && record.change_ref == stored_tool_change_ref(tool_ref, change)
        && record.path == change.path
        && record.kind == change.kind
        && record.before == change.before
        && record.after == change.after
        && record.commit_state == change.commit_state
        && record.coverage == change.coverage
}

fn tool_observation(records: &[ChangeRecord], scan_observation: &str, complete: bool) -> String {
    let mut observations = records
        .iter()
        .map(|record| (record.change_ref.as_str(), record.coverage))
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| left.0.cmp(right.0));
    let bytes = serde_json::to_vec(&(scan_observation, complete, observations))
        .expect("serializing change observations cannot fail");
    crate::store::hash_bytes(&bytes)
}

fn encoded_result(
    result: ChangesListResult,
    max_bytes: usize,
) -> Result<ChangesListResult, AgentError> {
    if serde_json::to_vec(&result)
        .map_err(|_| AgentError::RpcSerialization)?
        .len()
        > max_bytes
    {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(result)
    }
}

fn push_warning(warnings: &mut Vec<ChangeListWarning>, warning: ChangeListWarning) {
    if !warnings.contains(&warning) {
        warnings.push(warning);
    }
}

fn check_budget(cancellation: &CancellationToken, deadline: Instant) -> Result<(), AgentError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(AgentError::QueryLimit)
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) struct ChangeListGate {
    entered: tokio::sync::Notify,
}

#[cfg(test)]
impl ChangeListGate {
    pub(crate) fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.entered.notified().await;
    }
}

#[cfg(test)]
type ChangeListGateEntry = (crate::ids::SessionId, Arc<ChangeListGate>);
#[cfg(test)]
static CHANGE_LIST_GATES: OnceLock<Mutex<Vec<ChangeListGateEntry>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn gate_next_change_list(session_id: crate::ids::SessionId, gate: Arc<ChangeListGate>) {
    let mutex = CHANGE_LIST_GATES.get_or_init(|| Mutex::new(Vec::new()));
    mutex.lock().unwrap().push((session_id, gate));
}

/// Holds one `changes.list` query until its owning caller or Session cancels it,
/// so tests can observe the shared deferred-query pool without a fake request.
#[cfg(test)]
async fn check_change_list_gate(session_id: crate::ids::SessionId) {
    let gate = {
        let Some(mutex) = CHANGE_LIST_GATES.get() else {
            return;
        };
        let mut entries = mutex.lock().unwrap();
        let Some(position) = entries.iter().position(|(id, _)| *id == session_id) else {
            return;
        };
        entries.remove(position).1
    };
    gate.entered.notify_one();
    std::future::pending::<()>().await;
}

fn default_change_limit() -> usize {
    DEFAULT_CHANGE_LIMIT
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::workspace::status::{WorkspaceStatusEntry, WorkspaceStatusEntryKind};

    fn session() -> crate::ids::SessionId {
        "ses_00000000000000000000000000000001".parse().unwrap()
    }

    fn status_entry(path: &str) -> WorkspaceStatusEntry {
        WorkspaceStatusEntry {
            path: path.to_owned(),
            kind: WorkspaceStatusEntryKind::Ordinary,
            index_status: Some("M".to_owned()),
            worktree_status: Some(".".to_owned()),
            original_path: None,
        }
    }

    #[test]
    fn revisions_keep_missing_empty_and_unknown_distinct() {
        let empty = content_revision(b"");
        assert_ne!(ChangeRevision::Missing, empty);
        assert_ne!(empty, ChangeRevision::Unknown);
    }

    #[test]
    fn scopes_use_string_units_and_an_explicit_turn_object() {
        assert_eq!(
            serde_json::to_value(ChangeScope::Workspace).unwrap(),
            json!("workspace")
        );
        assert_eq!(
            serde_json::to_value(ChangeScope::Session).unwrap(),
            json!("session")
        );
        let turn = ChangeScope::Turn {
            loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
        };
        assert_eq!(
            serde_json::to_value(turn).unwrap(),
            json!({"turn": {"loop_id": "lup_00000000000000000000000000000001"}})
        );
    }

    #[tokio::test]
    async fn pages_honor_limit_and_reject_a_changed_observation_as_stale() {
        let status = WorkspaceStatusResult {
            repo_available: true,
            head_oid: None,
            branch: None,
            detached: false,
            staged: 2,
            unstaged: 0,
            untracked: 0,
            conflicted: 0,
            entries: vec![status_entry("a.txt"), status_entry("b.txt")],
            skipped_paths: 0,
            complete: true,
            warnings: Vec::new(),
            consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
            observed_at_unix_ms: 1,
        };
        let (records, observation, warnings) = workspace_records(session(), &status);
        let request = ChangesListRequest {
            session_id: session(),
            scope: ChangeScope::Workspace,
            cursor: None,
            limit: 1,
            max_bytes: Some(64 * 1024),
        };
        let first = page_records(
            &request,
            records.clone(),
            observation.clone(),
            ChangeListConsistency::Live,
            Some(1),
            warnings.clone(),
            true,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .await
        .unwrap();
        assert_eq!(first.records.len(), 1);
        let cursor = first.next_cursor.unwrap();
        let continued = page_records(
            &ChangesListRequest {
                cursor: Some(cursor),
                ..request.clone()
            },
            records,
            "0".repeat(64),
            ChangeListConsistency::Live,
            Some(2),
            warnings,
            true,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .await
        .unwrap();
        assert!(continued.stale);
        assert!(continued.records.is_empty());
        assert!(continued.warnings.contains(&ChangeListWarning::StaleCursor));
    }

    #[tokio::test]
    async fn incomplete_pages_keep_a_cursor_for_retained_records() {
        let status = WorkspaceStatusResult {
            repo_available: true,
            head_oid: None,
            branch: None,
            detached: false,
            staged: 0,
            unstaged: 0,
            untracked: 0,
            conflicted: 0,
            entries: vec![status_entry("a.txt"), status_entry("b.txt")],
            skipped_paths: 1,
            complete: false,
            warnings: Vec::new(),
            consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
            observed_at_unix_ms: 1,
        };
        let (records, observation, warnings) = workspace_records(session(), &status);
        let result = page_records(
            &ChangesListRequest {
                session_id: session(),
                scope: ChangeScope::Workspace,
                cursor: None,
                limit: 1,
                max_bytes: Some(64 * 1024),
            },
            records,
            observation,
            ChangeListConsistency::Live,
            Some(1),
            warnings,
            false,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .await
        .unwrap();
        assert!(!result.complete);
        assert!(result.next_cursor.is_some());
    }

    #[tokio::test]
    async fn blob_verification_warnings_cannot_push_a_selected_record_past_the_budget() {
        // Two records whose details are initially unavailable. Choosing the page
        // end must already reserve the `RecordsSkipped` warning that blob
        // verification can add, so a later `complete = false` cannot grow the
        // encoded page past `max_bytes` and drop an already-selected record.
        let long_a = format!("{}.txt", "a".repeat(400));
        let long_b = format!("{}.txt", "b".repeat(400));
        let status = WorkspaceStatusResult {
            repo_available: true,
            head_oid: None,
            branch: None,
            detached: false,
            staged: 0,
            unstaged: 0,
            untracked: 0,
            conflicted: 0,
            entries: vec![status_entry(&long_a), status_entry(&long_b)],
            skipped_paths: 0,
            complete: true,
            warnings: Vec::new(),
            consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
            observed_at_unix_ms: 1,
        };
        let (records, observation, _) = workspace_records(session(), &status);
        assert!(records.iter().all(|record| !record.details_available));
        let request = ChangesListRequest {
            session_id: session(),
            scope: ChangeScope::Workspace,
            cursor: None,
            limit: 10,
            max_bytes: Some(64 * 1024),
        };
        // The best case the planner must be able to reach: both records present
        // with both blob-verification warnings reserved.
        let best = page_records(
            &request,
            records.clone(),
            observation.clone(),
            ChangeListConsistency::Live,
            Some(1),
            Vec::new(),
            false,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(best.records.len(), 2);
        let max_bytes = serde_json::to_vec(&best).unwrap().len() - 1;
        let result = page_records(
            &ChangesListRequest {
                max_bytes: Some(max_bytes),
                ..request
            },
            records,
            observation,
            ChangeListConsistency::Live,
            Some(1),
            Vec::new(),
            false,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            Some(2),
        )
        .await
        .expect("the request stays answerable under the real encoded budget");
        assert_eq!(result.records.len(), 1);
        assert!(result.next_cursor.is_some());
        assert!(serde_json::to_vec(&result).unwrap().len() <= max_bytes);
    }

    #[test]
    fn retained_snapshot_integrity_controls_detail_availability() {
        let mut change = FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"before"),
            after: content_revision(b"after"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"before".to_vec()),
            after_bytes: Some(b"after".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        };
        assert!(change.details_available());
        change.after_bytes = Some(b"tampered".to_vec());
        assert!(!change.details_available());
    }

    #[test]
    fn a_different_disk_revision_cannot_upgrade_a_warm_tool_record() {
        let tool_ref = ToolRef {
            session_id: session(),
            loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
            request_index: 3,
            tool_call_id: "call-3".parse().unwrap(),
        };
        let warm_change = StoredFileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"before"),
            after: content_revision(b"after"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
        };
        let mut disk_change = warm_change.clone();
        disk_change.after = content_revision(b"different");
        let warm_record = warm_change.record(&tool_ref, true);
        assert!(!compatible_change(&warm_record, &tool_ref, &disk_change));
    }

    #[test]
    fn workspace_records_never_attribute_a_tool() {
        let status = WorkspaceStatusResult {
            repo_available: true,
            head_oid: None,
            branch: None,
            detached: false,
            staged: 1,
            unstaged: 0,
            untracked: 0,
            conflicted: 0,
            entries: vec![status_entry("value.txt")],
            skipped_paths: 0,
            complete: true,
            warnings: Vec::new(),
            consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
            observed_at_unix_ms: 1,
        };
        let (records, _, _) = workspace_records(session(), &status);
        assert_eq!(records[0].origin, ChangeOrigin::WorkspaceUnknown);
        assert!(records[0].tool_ref.is_none());
        assert_eq!(&records[0].before, &ChangeRevision::Unknown);
        assert_eq!(&records[0].after, &ChangeRevision::Unknown);
    }
}
