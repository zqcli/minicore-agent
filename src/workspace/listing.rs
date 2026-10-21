//! `workspace.files`: one bounded page of workspace directory entries.
//!
//! A listing is a live observation of the ignore-filtered tree, never a
//! filesystem snapshot. It shares its traversal with `workspace.search`, never
//! follows symlinked directories, and reports every bound it hit instead of
//! inventing a global total.

use crate::workspace::page::{
    DEFAULT_RESULT_BYTES, MAX_PATH_BYTES, SCOPE_HEX_LEN, display_path, now_unix_ms, scope_digest,
    valid_scope, validate_result_bytes,
};
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::budget::encoded_len;
use crate::error::AgentError;
use crate::ids::SessionId;
use crate::workspace::Workspace;

use crate::workspace::scan::{
    Visit, WalkOptions, WorkspaceFileKind, WorkspaceScanConsistency, WorkspaceScanStop, run_scan,
    walk_scope,
};

const DEFAULT_LIMIT: u32 = 200;
const MAX_LIMIT: u32 = 1000;

/// One bounded listing request. `directory` defaults to the Workspace root,
/// must name a directory, and `recursive` defaults to one level. `query`
/// filters entry paths only; it never looks at file contents. `cursor`
/// continues a previous page.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceFilesRequest {
    pub session_id: SessionId,
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default)]
    pub recursive: Option<bool>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub cursor: Option<WorkspaceListCursor>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl WorkspaceFilesRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        if let Some(directory) = self.directory.as_deref()
            && !directory.is_empty()
        {
            if directory.len() > MAX_PATH_BYTES {
                return Err(AgentError::InvalidArguments);
            }
            crate::workspace::validate_relative_path(directory)
                .map_err(|_| AgentError::InvalidArguments)?;
        }
        // "." and ".." are rejected above; this is the normalized form the
        // traversal and the cursor scope are built from.
        let directory = display_path(
            &crate::workspace::normalize_relative(self.directory(), true)
                .map_err(|_| AgentError::InvalidArguments)?,
        );
        if let Some(query) = self.query.as_deref()
            && !crate::workspace::page::valid_query_text(query)
        {
            return Err(AgentError::InvalidArguments);
        }
        if self
            .limit
            .is_some_and(|limit| !(1..=MAX_LIMIT).contains(&limit))
        {
            return Err(AgentError::InvalidArguments);
        }
        validate_result_bytes(self.max_bytes)?;
        if let Some(cursor) = &self.cursor {
            // The scope is computed from this request alone, so a cursor from a
            // different session, directory, recursion mode, or path filter is
            // rejected before any query slot is reserved.
            if !valid_scope(&cursor.scope)
                || cursor.scope != self.scope(&directory)
                || cursor.path.len() > MAX_PATH_BYTES
                || crate::workspace::normalize_relative(&cursor.path, true).is_err()
            {
                return Err(AgentError::InvalidArguments);
            }
        }
        Ok(())
    }

    fn directory(&self) -> &str {
        self.directory.as_deref().unwrap_or("")
    }

    fn recursive(&self) -> bool {
        self.recursive.unwrap_or(false)
    }

    fn limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT)
    }

    fn max_bytes(&self) -> usize {
        self.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES)
    }

    /// Binds a cursor to this method, session, and traversal parameters. Page
    /// size may change between pages.
    fn scope(&self, directory: &str) -> String {
        scope_digest(&[
            "workspace.files",
            &self.session_id.to_string(),
            directory,
            if self.recursive() { "1" } else { "0" },
            self.query.as_deref().unwrap_or(""),
        ])
    }
}

impl fmt::Debug for WorkspaceFilesRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceFilesRequest")
            .field("session_id", &self.session_id)
            .field("directory_bytes", &self.directory().len())
            .field("recursive", &self.recursive())
            .field("query_bytes", &self.query.as_deref().unwrap_or("").len())
            .field("cursor", &self.cursor)
            .field("limit", &self.limit)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

/// Last processed relative path in sorted depth-first order. `scope` binds it
/// to the session and traversal parameters; entries behind it are not replayed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceListCursor {
    pub path: String,
    pub scope: String,
}

/// One workspace entry. `path` is relative to the Workspace root and `size` is
/// present for regular files only.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct WorkspaceFileEntry {
    pub path: String,
    pub kind: WorkspaceFileKind,
    pub size: Option<u64>,
}

impl fmt::Debug for WorkspaceFileEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceFileEntry")
            .field("path_bytes", &self.path.len())
            .field("kind", &self.kind)
            .field("size", &self.size)
            .finish()
    }
}

/// One page of a live listing.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct WorkspaceFilesResult {
    pub directory: String,
    pub entries: Vec<WorkspaceFileEntry>,
    /// Present when the scan stopped with an exact continuation point.
    pub next_cursor: Option<WorkspaceListCursor>,
    /// This response is a partial view: the traversal was cut by a bound, or an
    /// entry could not be read or represented.
    pub truncated: bool,
    /// The traversal reached the end of the scope without a read failure, an
    /// unrepresentable path, or an entry the budget could not hold. Entries
    /// filtered out by `query` and roots excluded by the rules do not clear it.
    pub scan_complete: bool,
    /// What ended the scan. `end`, `depth`, `rules`, and `deadline` do not continue.
    pub stopped_by: WorkspaceScanStop,
    /// Visited entries that were not returned: entries filtered out by
    /// `query`, entries whose path is not valid UTF-8, entries that could not
    /// be read, explicitly requested roots excluded by the rules, and single
    /// entries that cannot fit the result budget. Rule-excluded descendants are
    /// not counted; they are simply not part of the visible tree.
    pub skipped_count: u64,
    pub consistency: WorkspaceScanConsistency,
    pub observed_at_unix_ms: u64,
}

impl fmt::Debug for WorkspaceFilesResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceFilesResult")
            .field("directory_bytes", &self.directory.len())
            .field("entries", &self.entries.len())
            .field("next_cursor", &self.next_cursor)
            .field("truncated", &self.truncated)
            .field("scan_complete", &self.scan_complete)
            .field("stopped_by", &self.stopped_by)
            .field("skipped_count", &self.skipped_count)
            .field("consistency", &self.consistency)
            .field("observed_at_unix_ms", &self.observed_at_unix_ms)
            .finish()
    }
}

struct FilesPlan {
    directory: String,
    recursive: bool,
    query: Option<String>,
    limit: usize,
    max_bytes: usize,
    scope: String,
    start_path: Option<String>,
}

/// Lists one bounded page of workspace entries.
pub(crate) async fn files(
    workspace: Arc<Workspace>,
    request: WorkspaceFilesRequest,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
) -> Result<WorkspaceFilesResult, AgentError> {
    request.validate()?;
    let directory = display_path(
        &crate::workspace::normalize_relative(request.directory(), true)
            .map_err(|_| AgentError::InvalidArguments)?,
    );
    let scope = request.scope(&directory);
    let start_path = request.cursor.as_ref().map(|cursor| cursor.path.clone());
    let plan = FilesPlan {
        directory,
        recursive: request.recursive(),
        query: request.query.clone().filter(|query| !query.is_empty()),
        limit: usize::try_from(request.limit()).unwrap_or(usize::MAX),
        max_bytes: request.max_bytes(),
        start_path,
        scope,
    };
    run_scan(
        workspace,
        session_cancellation,
        shutdown_cancellation,
        move |workspace, budget| scan_files(workspace, &plan, budget),
    )
    .await
}

fn scan_files(
    workspace: &Workspace,
    plan: &FilesPlan,
    budget: &mut crate::workspace::scan::ScanBudget,
) -> Result<WorkspaceFilesResult, AgentError> {
    let envelope = envelope_bytes(&plan.directory, plan.max_bytes)?;
    let content_budget = plan.max_bytes - envelope;
    let mut result = WorkspaceFilesResult {
        directory: plan.directory.clone(),
        entries: Vec::new(),
        next_cursor: None,
        truncated: false,
        scan_complete: true,
        stopped_by: WorkspaceScanStop::End,
        skipped_count: 0,
        consistency: WorkspaceScanConsistency::Live,
        observed_at_unix_ms: now_unix_ms(),
    };
    let mut used = 0usize;
    let mut dropped = 0u64;
    let outcome = walk_scope(
        workspace,
        &plan.directory,
        WalkOptions {
            recursive: plan.recursive,
            want_size: true,
            directory_only: true,
            resume_file: false,
        },
        plan.start_path.as_deref(),
        budget,
        |entry, budget| {
            if !budget.byte_available(u64::try_from(entry.relative.len()).unwrap_or(u64::MAX)) {
                return Ok(Visit::StopBefore(WorkspaceScanStop::Bytes));
            }
            if let Some(query) = plan.query.as_deref()
                && !entry.relative.contains(query)
            {
                result.skipped_count += 1;
                return Ok(Visit::Next);
            }
            let record = WorkspaceFileEntry {
                path: entry.relative.clone(),
                kind: entry.kind,
                size: entry.size,
            };
            let encoded = encoded_len(&record);
            if used + encoded + 1 + encoded_len(&entry.relative) > content_budget {
                if result.entries.is_empty() {
                    // The budget cannot hold this entry even alone, so the page
                    // skips and counts it instead of stalling, and the response
                    // is reported as incomplete.
                    result.skipped_count += 1;
                    dropped += 1;
                    return Ok(Visit::Next);
                }
                // Stop before the entry so the next page re-examines it. A
                // stop after it would silently drop it.
                return Ok(Visit::StopBefore(WorkspaceScanStop::Page));
            }
            used += encoded + 1;
            result.entries.push(record);
            if result.entries.len() >= plan.limit {
                return Ok(Visit::Stop(WorkspaceScanStop::Page));
            }
            Ok(Visit::Next)
        },
    )?;
    // Each skipped entry is counted exactly once: the skips the visitor made
    // above, then the walk's unrepresentable and unreadable entries and the
    // excluded roots. `dropped` only decides completeness below.
    let dropped = dropped + outcome.unrepresentable + outcome.unreadable;
    result.skipped_count += outcome.unrepresentable + outcome.unreadable + outcome.excluded;
    result.stopped_by = outcome.reason;
    // The envelope reserves an empty cursor path; charge the actual key too.
    // A key that cannot fit cannot be advertised as a usable continuation.
    result.next_cursor = outcome
        .next
        .filter(|path| {
            path.len() <= MAX_PATH_BYTES && encoded_len(path) - 2 <= content_budget - used
        })
        .map(|path| WorkspaceListCursor {
            path,
            scope: plan.scope.clone(),
        });
    result.truncated = outcome.reason != WorkspaceScanStop::End || dropped > 0;
    result.scan_complete = outcome.reason == WorkspaceScanStop::End && dropped == 0;
    Ok(result)
}

/// Serialized size of the result envelope with no entries, using the widest
/// possible variable fields so the measured value bounds the real response.
fn envelope_bytes(directory: &str, max_bytes: usize) -> Result<usize, AgentError> {
    let probe = WorkspaceFilesResult {
        directory: directory.to_owned(),
        entries: Vec::new(),
        next_cursor: Some(WorkspaceListCursor {
            path: String::new(),
            scope: "f".repeat(SCOPE_HEX_LEN),
        }),
        truncated: true,
        scan_complete: false,
        // The widest stop name keeps this bound valid for every response.
        stopped_by: WorkspaceScanStop::Deadline,
        skipped_count: u64::MAX,
        consistency: WorkspaceScanConsistency::Live,
        observed_at_unix_ms: u64::MAX,
    };
    crate::workspace::page::envelope_size(&probe, max_bytes)
}

#[cfg(test)]
mod tests;
