//! `workspace.files`: one bounded page of workspace directory entries.
//!
//! A listing is a live observation of the ignore-filtered tree, never a
//! filesystem snapshot. It shares its traversal with `workspace.search`, never
//! follows symlinked directories, and reports every bound it hit instead of
//! inventing a global total.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::ids::SessionId;
use crate::workspace::Workspace;
use crate::workspace::query::{
    DEFAULT_RESULT_BYTES, MAX_PATH_BYTES, MAX_RESULT_BYTES, MIN_RESULT_BYTES,
};
use crate::workspace::scan::{
    SCOPE_HEX_LEN, Visit, WalkOptions, WorkspaceFileKind, WorkspaceScanConsistency,
    WorkspaceScanStop, display_path, encoded_len, now_unix_ms, run_scan, scope_digest, valid_scope,
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
        if let Some(directory) = self.directory.as_deref() {
            if !directory.is_empty() {
                if directory.len() > MAX_PATH_BYTES {
                    return Err(AgentError::InvalidArguments);
                }
                crate::workspace::validate_relative_path(directory)
                    .map_err(|_| AgentError::InvalidArguments)?;
            }
        }
        // "." and ".." are rejected above; this is the normalized form the
        // traversal and the cursor scope are built from.
        let directory = display_path(
            &crate::workspace::normalize_relative(self.directory(), true)
                .map_err(|_| AgentError::InvalidArguments)?,
        );
        if let Some(query) = self.query.as_deref() {
            if !crate::workspace::scan::valid_query_text(query) {
                return Err(AgentError::InvalidArguments);
            }
        }
        if self
            .limit
            .is_some_and(|limit| !(1..=MAX_LIMIT).contains(&limit))
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
            // different session, directory, recursion mode, or path filter is
            // rejected before any query slot is reserved.
            if !valid_scope(&cursor.scope) || cursor.scope != self.scope(&directory) {
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

/// Where a later page continues. `entry` is the ordinal of the first raw
/// traversal entry (rule-excluded entries included) the request must examine
/// again; `scope` binds the cursor to the
/// session and traversal parameters that produced it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceListCursor {
    pub entry: u64,
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
    /// What ended the scan. `end`, `depth`, `rules`, and `entries` never
    /// continue, so an incomplete scan without a cursor is always explicit.
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
    start_entry: u64,
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
    let start_entry = match &request.cursor {
        Some(cursor) => cursor.entry,
        None => 0,
    };
    let plan = FilesPlan {
        directory,
        recursive: request.recursive(),
        query: request.query.clone().filter(|query| !query.is_empty()),
        limit: usize::try_from(request.limit()).unwrap_or(usize::MAX),
        max_bytes: request.max_bytes(),
        start_entry,
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
        },
        plan.start_entry,
        budget,
        |entry, budget| {
            if !budget.byte_available(u64::try_from(entry.relative.len()).unwrap_or(u64::MAX)) {
                return Ok(Visit::StopBefore(WorkspaceScanStop::Bytes));
            }
            if let Some(query) = plan.query.as_deref() {
                if !entry.relative.contains(query) {
                    result.skipped_count += 1;
                    return Ok(Visit::Next);
                }
            }
            let record = WorkspaceFileEntry {
                path: entry.relative.clone(),
                kind: entry.kind,
                size: entry.size,
            };
            let encoded = encoded_len(&record)?;
            if used + encoded + 1 > content_budget {
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
    result.next_cursor = outcome.next.map(|entry| WorkspaceListCursor {
        entry,
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
            entry: u64::MAX,
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
        ScanHold, ScanLimits, hold_next_scan, set_scan_deadline, set_scan_limits,
    };

    struct Fixture {
        session: SessionId,
        workspace: Arc<Workspace>,
    }

    async fn fixture(label: &str) -> Fixture {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-files-{label}-{}",
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

    async fn write(fixture: &Fixture, path: &str, bytes: impl AsRef<[u8]>) {
        let full = fixture.workspace.root().join(path);
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(full, bytes).await.unwrap();
    }

    fn request(fixture: &Fixture) -> WorkspaceFilesRequest {
        WorkspaceFilesRequest {
            session_id: fixture.session,
            directory: None,
            recursive: None,
            query: None,
            cursor: None,
            limit: None,
            max_bytes: None,
        }
    }

    async fn list(fixture: &Fixture, request: WorkspaceFilesRequest) -> WorkspaceFilesResult {
        files(
            Arc::clone(&fixture.workspace),
            request,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    fn paths(result: &WorkspaceFilesResult) -> Vec<String> {
        let mut paths: Vec<String> = result
            .entries
            .iter()
            .map(|entry| entry.path.clone())
            .collect();
        paths.sort();
        paths
    }

    async fn collect_all(
        fixture: &Fixture,
        mut request: WorkspaceFilesRequest,
    ) -> (Vec<String>, WorkspaceScanStop, bool) {
        let budget = request.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
        let mut all = Vec::new();
        for _ in 0..64 {
            let result = list(fixture, request.clone()).await;
            let encoded = serde_json::to_vec(&result).unwrap().len();
            assert!(encoded <= budget, "encoded page {encoded} exceeds {budget}");
            all.extend(result.entries.iter().map(|entry| entry.path.clone()));
            match result.next_cursor {
                Some(cursor) => {
                    assert!(!result.scan_complete);
                    request.cursor = Some(cursor);
                }
                None => return (all, result.stopped_by, result.scan_complete),
            }
        }
        panic!("listing did not finish");
    }

    #[tokio::test]
    async fn lists_one_level_by_default_and_recurses_on_request() {
        let fixture = fixture("levels").await;
        write(&fixture, "a.txt", b"a").await;
        write(&fixture, "sub/b.txt", b"bb").await;
        write(&fixture, "sub/deep/c.txt", b"ccc").await;
        write(&fixture, "other/d.txt", b"d").await;

        let result = list(&fixture, request(&fixture)).await;
        assert_eq!(result.directory, "");
        assert_eq!(paths(&result), vec!["a.txt", "other", "sub"]);
        assert!(result.scan_complete);
        assert_eq!(result.stopped_by, WorkspaceScanStop::End);
        assert!(!result.truncated);
        assert_eq!(result.skipped_count, 0);
        assert_eq!(result.consistency, WorkspaceScanConsistency::Live);
        assert!(result.observed_at_unix_ms > 0);
        let file = result
            .entries
            .iter()
            .find(|entry| entry.path == "a.txt")
            .unwrap();
        assert_eq!(file.kind, WorkspaceFileKind::File);
        assert_eq!(file.size, Some(1));
        let directory = result
            .entries
            .iter()
            .find(|entry| entry.path == "sub")
            .unwrap();
        assert_eq!(directory.kind, WorkspaceFileKind::Directory);
        assert_eq!(directory.size, None);

        let mut recursive = request(&fixture);
        recursive.recursive = Some(true);
        let result = list(&fixture, recursive.clone()).await;
        assert_eq!(
            paths(&result),
            vec![
                "a.txt",
                "other",
                "other/d.txt",
                "sub",
                "sub/b.txt",
                "sub/deep",
                "sub/deep/c.txt"
            ]
        );
        assert!(result.scan_complete);

        let mut nested = request(&fixture);
        nested.directory = Some("sub".to_owned());
        nested.recursive = Some(true);
        let result = list(&fixture, nested).await;
        assert_eq!(
            paths(&result),
            vec!["sub/b.txt", "sub/deep", "sub/deep/c.txt"]
        );
        assert_eq!(result.directory, "sub");
    }

    #[tokio::test]
    async fn ignore_rules_and_git_metadata_are_respected() {
        let fixture = fixture("ignore").await;
        write(&fixture, ".gitignore", b"ignored.txt\ntarget/\n").await;
        write(&fixture, ".ignore", b"also.txt\n").await;
        write(&fixture, ".git/HEAD", b"ref: refs/heads/main\n").await;
        write(&fixture, ".git/objects/junk", b"junk").await;
        write(&fixture, "kept.txt", b"kept").await;
        write(&fixture, "ignored.txt", b"ignored").await;
        write(&fixture, "also.txt", b"ignored").await;
        write(&fixture, "target/build.log", b"log").await;
        write(&fixture, "sub/nested.txt", b"nested").await;
        write(&fixture, "sub/.gitignore", b"nested.txt\n").await;

        let mut recursive = request(&fixture);
        recursive.recursive = Some(true);
        let result = list(&fixture, recursive).await;
        assert_eq!(
            paths(&result),
            vec![".gitignore", ".ignore", "kept.txt", "sub", "sub/.gitignore"]
        );
        assert!(result.scan_complete);
    }

    #[tokio::test]
    async fn gitignore_applies_without_a_git_repository() {
        let fixture = fixture("non-repo").await;
        write(&fixture, ".gitignore", b"skipped.txt\n").await;
        write(&fixture, "kept.txt", b"kept").await;
        write(&fixture, "skipped.txt", b"skipped").await;

        let result = list(&fixture, request(&fixture)).await;
        assert_eq!(paths(&result), vec![".gitignore", "kept.txt"]);
        assert!(result.scan_complete);
    }

    #[tokio::test]
    async fn query_filters_entry_paths_and_never_file_contents() {
        let fixture = fixture("query").await;
        write(&fixture, "needle.txt", b"nothing here").await;
        write(&fixture, "other.txt", b"needle in content").await;
        write(&fixture, "needle/sub/x.txt", b"x").await;
        write(&fixture, "plain/y.txt", b"y").await;

        let mut filtered = request(&fixture);
        filtered.query = Some("needle".to_owned());
        filtered.recursive = Some(true);
        let result = list(&fixture, filtered).await;
        assert_eq!(
            paths(&result),
            vec!["needle", "needle.txt", "needle/sub", "needle/sub/x.txt"]
        );
        assert!(result.skipped_count > 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_are_listed_but_never_followed_or_escaped() {
        use std::os::unix::fs::symlink;

        let outside = std::env::temp_dir().join(format!(
            "minicore-workspace-files-outside-{}",
            SessionId::new().unwrap()
        ));
        tokio::fs::create_dir_all(&outside).await.unwrap();
        tokio::fs::write(outside.join("secret.txt"), b"outside")
            .await
            .unwrap();
        let fixture = fixture("symlink").await;
        write(&fixture, "inside/real.txt", b"real").await;
        let root = fixture.workspace.root().to_path_buf();
        symlink(outside.join("secret.txt"), root.join("escape.txt")).unwrap();
        symlink(outside.clone(), root.join("escape-dir")).unwrap();
        symlink(root.join("inside/real.txt"), root.join("link.txt")).unwrap();
        symlink(root.join("inside"), root.join("inside-link")).unwrap();

        let mut recursive = request(&fixture);
        recursive.recursive = Some(true);
        let result = list(&fixture, recursive).await;
        assert_eq!(
            paths(&result),
            vec![
                "escape-dir",
                "escape.txt",
                "inside",
                "inside-link",
                "inside/real.txt",
                "link.txt"
            ]
        );
        let linked = result
            .entries
            .iter()
            .find(|entry| entry.path == "escape-dir")
            .unwrap();
        assert_eq!(linked.kind, WorkspaceFileKind::Symlink);
        assert_eq!(linked.size, None);

        // A requested directory that resolves outside the workspace, and a
        // requested root that is itself a symlinked directory, are rejected
        // instead of being walked.
        for directory in ["escape-dir", "inside-link"] {
            let mut escaped = request(&fixture);
            escaped.directory = Some(directory.to_owned());
            let error = files(
                Arc::clone(&fixture.workspace),
                escaped,
                CancellationToken::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, AgentError::Workspace), "{directory}");
        }
        let _ = tokio::fs::remove_dir_all(&outside).await;
    }

    #[tokio::test]
    async fn an_expired_budget_returns_no_entries_and_no_cursor() {
        let fixture = fixture("deadline-empty").await;
        write(&fixture, "one.txt", b"x").await;
        set_scan_deadline(
            fixture.workspace.root().to_path_buf(),
            std::time::Duration::ZERO,
        );

        let result = list(&fixture, request(&fixture)).await;
        assert!(result.entries.is_empty());
        assert_eq!(result.stopped_by, WorkspaceScanStop::Deadline);
        assert!(result.truncated);
        assert!(!result.scan_complete);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn pagination_is_lossless_and_every_page_fits_its_budget() {
        let fixture = fixture("paging").await;
        let mut expected = Vec::new();
        for index in 0..40 {
            let path = format!("dir-{:02}/file-{index:02}.txt", index % 5);
            write(&fixture, &path, b"x").await;
            expected.push(path);
        }
        write(&fixture, "dir-00/.gitignore", b"nothing\n").await;
        expected.push("dir-00/.gitignore".to_owned());
        for index in 0..5 {
            expected.push(format!("dir-{index:02}"));
        }
        expected.sort();

        let mut recursive = request(&fixture);
        recursive.recursive = Some(true);
        recursive.limit = Some(7);
        recursive.max_bytes = Some(2048);
        let (collected, stopped_by, complete) = collect_all(&fixture, recursive).await;
        let mut collected = collected;
        collected.sort();
        assert_eq!(stopped_by, WorkspaceScanStop::End);
        assert!(complete);
        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn one_entry_pages_replay_directories_and_ignored_entries() {
        let fixture = fixture("dfs-paging").await;
        write(&fixture, ".gitignore", b"skip.txt\n").await;
        write(&fixture, "a/one.txt", b"1").await;
        write(&fixture, "a/skip.txt", b"2").await;
        write(&fixture, "a/b/two.txt", b"3").await;
        write(&fixture, "a/b/three.txt", b"4").await;
        write(&fixture, "c/four.txt", b"5").await;
        write(&fixture, "top.txt", b"6").await;

        let mut request = request(&fixture);
        request.recursive = Some(true);
        request.limit = Some(1);
        request.max_bytes = Some(MIN_RESULT_BYTES);
        let mut cursor = None;
        let mut all: Vec<String> = Vec::new();
        for _ in 0..64 {
            let mut page = request.clone();
            page.cursor = cursor.clone();
            let result = list(&fixture, page).await;
            assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
            all.extend(result.entries.iter().map(|entry| entry.path.clone()));
            match result.next_cursor {
                Some(next) => {
                    assert!(!result.scan_complete);
                    cursor = Some(next);
                }
                None => {
                    assert!(result.scan_complete);
                    assert_eq!(result.stopped_by, WorkspaceScanStop::End);
                    break;
                }
            }
        }
        // No entry is returned twice, and a directory is always returned
        // before the entries inside it, which is only true when a resumed page
        // descends into the directories it positioned past.
        let mut unique = all.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), all.len(), "an entry was returned twice");
        for (index, path) in all.iter().enumerate() {
            for earlier in &all[..index] {
                assert!(
                    !earlier.starts_with(&format!("{path}/")),
                    "{earlier} was returned before its directory {path}"
                );
            }
        }
        let mut collected = all;
        collected.sort();
        assert_eq!(
            collected,
            vec![
                ".gitignore",
                "a",
                "a/b",
                "a/b/three.txt",
                "a/b/two.txt",
                "a/one.txt",
                "c",
                "c/four.txt",
                "top.txt"
            ]
        );
    }

    #[tokio::test]
    async fn entry_and_depth_ceilings_are_reported_not_hidden() {
        let ceilings = fixture("ceilings").await;
        for index in 0..8 {
            write(&ceilings, &format!("file-{index}.txt"), b"x").await;
        }
        set_scan_limits(
            ceilings.workspace.root().to_path_buf(),
            ScanLimits {
                entries: 3,
                bytes: 1 << 20,
                depth: 64,
                rule_files: 8,
                rule_bytes: 1 << 20,
            },
        );
        let mut recursive = request(&ceilings);
        recursive.recursive = Some(true);
        let first = list(&ceilings, recursive.clone()).await;
        assert_eq!(first.stopped_by, WorkspaceScanStop::Entries);
        assert_eq!(first.entries.len(), 3);
        assert!(first.truncated);
        assert!(!first.scan_complete);
        // A resumed request would spend its whole budget reaching the same
        // ceiling, so the stop is explicit and has no continuation.
        assert!(first.next_cursor.is_none());
        let (collected, _, complete) = collect_all(&ceilings, recursive).await;
        assert_eq!(collected.len(), 3);
        assert!(!complete);

        let deep = fixture("depth").await;
        write(&deep, "a/b/c/d/e.txt", b"deep").await;
        set_scan_limits(
            deep.workspace.root().to_path_buf(),
            ScanLimits {
                entries: 1000,
                bytes: 1 << 20,
                depth: 2,
                rule_files: 8,
                rule_bytes: 1 << 20,
            },
        );
        let mut recursive = request(&deep);
        recursive.recursive = Some(true);
        let result = list(&deep, recursive).await;
        // The depth ceiling descends two levels below the requested root.
        assert_eq!(paths(&result), vec!["a", "a/b"]);
        assert_eq!(result.stopped_by, WorkspaceScanStop::Depth);
        assert!(result.truncated);
        assert!(!result.scan_complete);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn result_budget_and_validation_are_enforced() {
        let fixture = fixture("budget").await;
        for index in 0..20 {
            write(
                &fixture,
                &format!("a-rather-long-name-for-budget-{index:02}.txt"),
                b"x",
            )
            .await;
        }
        let mut tight = request(&fixture);
        tight.limit = Some(20);
        tight.max_bytes = Some(MIN_RESULT_BYTES);
        let result = list(&fixture, tight.clone()).await;
        assert!(result.truncated);
        assert_eq!(result.stopped_by, WorkspaceScanStop::Page);
        assert!(!result.entries.is_empty());
        assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
        let (collected, stopped_by, complete) = collect_all(&fixture, tight).await;
        let mut collected = collected;
        collected.sort();
        let mut expected: Vec<String> = (0..20)
            .map(|index| format!("a-rather-long-name-for-budget-{index:02}.txt"))
            .collect();
        expected.sort();
        assert_eq!(stopped_by, WorkspaceScanStop::End);
        assert!(complete);
        assert_eq!(collected, expected);

        for directory in ["", ".", "..", "../escape", "/etc", "a/../../b"] {
            let mut request = request(&fixture);
            request.directory = Some(directory.to_owned());
            if directory.is_empty() {
                assert!(request.validate().is_ok(), "{directory:?}");
            } else {
                assert!(matches!(
                    request.validate(),
                    Err(AgentError::InvalidArguments)
                ));
            }
        }
        let mut long = request(&fixture);
        long.directory = Some("a".repeat(MAX_PATH_BYTES + 1));
        assert!(matches!(long.validate(), Err(AgentError::InvalidArguments)));
        let mut limit = request(&fixture);
        limit.limit = Some(0);
        assert!(matches!(
            limit.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut limit = request(&fixture);
        limit.limit = Some(MAX_LIMIT + 1);
        assert!(matches!(
            limit.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut budget = request(&fixture);
        budget.max_bytes = Some(MIN_RESULT_BYTES - 1);
        assert!(matches!(
            budget.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut budget = request(&fixture);
        budget.max_bytes = Some(MAX_RESULT_BYTES + 1);
        assert!(matches!(
            budget.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut query = request(&fixture);
        query.query = Some("two\nlines".to_owned());
        assert!(matches!(
            query.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let mut cursor = request(&fixture);
        cursor.cursor = Some(WorkspaceListCursor {
            entry: 0,
            scope: "not-a-scope".to_owned(),
        });
        assert!(matches!(
            cursor.validate(),
            Err(AgentError::InvalidArguments)
        ));
    }

    #[tokio::test]
    async fn an_entry_that_cannot_fit_an_empty_page_is_skipped_and_incomplete() {
        let fixture = fixture("unrepresentable").await;
        // Every entry of this directory has a path longer than an empty page
        // can hold, so each one is skipped and counted.
        let deep = (0..8)
            .map(|index| format!("level-{index:02}-{}", "x".repeat(58)))
            .collect::<Vec<_>>()
            .join("/");
        write(&fixture, &format!("{deep}/one.txt"), b"x").await;
        write(&fixture, &format!("{deep}/two.txt"), b"x").await;
        let mut tight = request(&fixture);
        tight.directory = Some(deep);
        tight.recursive = Some(true);
        tight.max_bytes = Some(MIN_RESULT_BYTES);
        let result = list(&fixture, tight).await;
        assert!(result.entries.is_empty());
        assert_eq!(result.skipped_count, 2);
        assert!(result.truncated);
        assert!(!result.scan_complete);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn a_cursor_from_another_request_is_rejected() {
        let fixture = fixture("cursor-mismatch").await;
        write(&fixture, "a.txt", b"a").await;
        write(&fixture, "b.txt", b"b").await;
        let mut request = request(&fixture);
        request.limit = Some(1);
        let first = list(&fixture, request.clone()).await;
        let cursor = first.next_cursor.expect("a cut page continues");

        let mut mismatched = request.clone();
        mismatched.cursor = Some(cursor.clone());
        mismatched.query = Some("b".to_owned());
        let error = files(
            Arc::clone(&fixture.workspace),
            mismatched,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::InvalidArguments));

        // The cursor is still valid for the request that produced it. The
        // ordinal rule is only exact while the tree is unchanged, so this page
        // is a fresh live observation that may reflect the deletion.
        tokio::fs::remove_file(fixture.workspace.root().join("a.txt"))
            .await
            .unwrap();
        let mut resumed = request;
        resumed.cursor = Some(cursor);
        let second = list(&fixture, resumed).await;
        assert_eq!(second.consistency, WorkspaceScanConsistency::Live);
        assert!(paths(&second).iter().all(|path| path != "a.txt"));
    }

    #[tokio::test]
    async fn a_held_scan_finishes_after_release() {
        let fixture = fixture("hold").await;
        write(&fixture, "a.txt", b"a").await;
        let hold = Arc::new(ScanHold::new());
        hold_next_scan(fixture.workspace.root().to_path_buf(), Arc::clone(&hold));
        let pending = tokio::spawn({
            let workspace = Arc::clone(&fixture.workspace);
            let request = request(&fixture);
            async move {
                files(
                    workspace,
                    request,
                    CancellationToken::new(),
                    CancellationToken::new(),
                )
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), hold.wait_started())
            .await
            .expect("the held scan did not start");
        hold.release();
        let result = pending.await.unwrap().unwrap();
        assert_eq!(paths(&result), vec!["a.txt"]);
        assert!(result.scan_complete);
    }

    #[tokio::test]
    async fn a_cancelled_session_stops_the_scan() {
        let fixture = fixture("cancel").await;
        write(&fixture, "a.txt", b"a").await;
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = files(
            Arc::clone(&fixture.workspace),
            request(&fixture),
            cancelled,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::QueryLimit));
    }
}
