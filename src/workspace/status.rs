//! Bounded, session-scoped observation of the Workspace's Git status.
//!
//! One query runs at most three fixed git commands without a shell: a
//! `rev-parse --show-toplevel` that locates the work tree, at most one
//! `rev-parse --is-bare-repository` that classifies a workspace without a work
//! tree, and one `status --porcelain=v2 -z --branch` whose output is parsed
//! from bytes. Nothing here writes history, calls a model, stages, fetches, or
//! touches the network. The result is an observation only: it makes no claim
//! about who changed a file, and it never waits for a Turn to complete.
//!
//! A repository that does not exist, a Workspace that is not a work tree, an
//! unborn `HEAD`, a detached `HEAD`, and merge conflicts are ordinary states
//! that are reported explicitly rather than as an internal error or as an
//! otherwise clean workspace. A workspace git cannot read for an unexplained
//! reason is reported as a failed observation instead, never as a definite
//! missing repository. When the workspace is a subdirectory of a larger
//! repository, only paths inside the workspace are counted or returned.
//!
//! Submodule internals are not observed: the comparison uses the commits
//! recorded in the superproject, so a gitlink recorded in the index that
//! differs from the committed one is an ordinary entry, while a submodule's own
//! work tree is never scanned for modifications or untracked files, is never
//! recursed into, and never runs a monitor or hook configured inside it. That is
//! a documented boundary rather than a silent gap.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::ids::SessionId;
use crate::workspace::Workspace;
use crate::workspace::query::{
    DEFAULT_RESULT_BYTES, MAX_PATH_BYTES, MAX_RESULT_BYTES, MIN_RESULT_BYTES,
};
use crate::workspace::scan::{
    READ_CHUNK_BYTES, WorkspaceScanConsistency, encoded_len, now_unix_ms,
};

mod diff;
pub(crate) use diff::diff_sources;
#[cfg(test)]
mod diff_tests;

/// Wall-clock budget for one workspace status query, matching the other
/// workspace queries. It is checked between bounded operations, so one blocking
/// read on a stalled remote filesystem can outlast it.
pub(crate) const WORKSPACE_STATUS_DEADLINE: Duration = Duration::from_secs(10);
/// Captured stdout. Further output stops the child, so a single query never
/// buffers more than this.
const MAX_STATUS_STDOUT_BYTES: usize = 1024 * 1024;
/// Standard error is counted, never captured. Past this many bytes the child is
/// stopped: a git flooding diagnostics is not answering the query, and letting
/// it run to the deadline would discard without bound.
const MAX_STATUS_STDERR_BYTES: u64 = 64 * 1024;
/// Branch names are filesystem-bound; a longer value cannot be a ref git
/// printed, so it is omitted and reported as a malformed header instead of
/// failing the whole query.
const MAX_STATUS_BRANCH_BYTES: usize = 1024;
/// Read size for the stderr drain loop.
const STATUS_STDERR_CHUNK_BYTES: usize = 8192;
/// Parsed records one query accepts. The summary counters cover exactly the
/// records a query accepted.
const MAX_STATUS_RECORDS: u64 = 100_000;

/// One observation of the Workspace's Git status.
///
/// `max_bytes` is an encoded result budget like the other workspace queries,
/// not a limit on git output. `repo_available` is false when git is missing,
/// when git conclusively reports a workspace without a work tree, and for any
/// repository state git refused to resolve; `complete` separates those definite
/// answers from the incomplete ones. `head_oid` is absent for an unborn `HEAD`;
/// `branch` is absent for a detached `HEAD`. `complete` is true only when the
/// observation was neither cut short nor partly unrepresentable; `warnings`
/// names every such condition and never echoes a path git printed.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStatusRequest {
    pub session_id: SessionId,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl WorkspaceStatusRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        // Cheap and filesystem-free, so an invalid budget is rejected before a
        // query slot is reserved.
        if self
            .max_bytes
            .is_some_and(|max_bytes| !(MIN_RESULT_BYTES..=MAX_RESULT_BYTES).contains(&max_bytes))
        {
            return Err(AgentError::InvalidArguments);
        }
        Ok(())
    }
}

/// A condition that limits or qualifies one observation. These are codes only:
/// nothing from git's stdout diagnostics or stderr, and no path outside the
/// workspace, is ever reported.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceStatusWarning {
    /// The git executable could not be started.
    GitUnavailable,
    /// A repository state could not be observed: a work tree was found but its
    /// status failed, or git refused to resolve a work tree for a reason other
    /// than a definite bare repository or git directory.
    StatusFailed,
    /// A stdout, stderr, record, or encoded-result bound cut the observation
    /// short.
    OutputTruncated,
    /// The deadline passed before the observation finished.
    Deadline,
    /// Entries were skipped because a path was not valid UTF-8 or was too long.
    SkippedPaths,
    /// The Workspace is a subdirectory of a larger repository; changes outside
    /// it are neither counted nor returned.
    NestedRepository,
}

/// Which kind of porcelain record produced an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceStatusEntryKind {
    /// An ordinary changed entry, staged or unstaged.
    Ordinary,
    /// A rename or copy between `HEAD` and the index.
    Renamed,
    /// An unmerged entry.
    Unmerged,
    /// An untracked entry. Git does not expand untracked directories here.
    Untracked,
}

/// One changed path inside the Workspace, relative to the Workspace root.
///
/// `index_status` and `worktree_status` are git's own `XY` codes; they are
/// absent for untracked entries. `original_path` is the previous name of a
/// rename or copy.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStatusEntry {
    pub path: String,
    pub kind: WorkspaceStatusEntryKind,
    pub index_status: Option<String>,
    pub worktree_status: Option<String>,
    pub original_path: Option<String>,
}

impl fmt::Debug for WorkspaceStatusEntry {
    // Diagnostics report lengths and codes, never a workspace path.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceStatusEntry")
            .field("path_bytes", &self.path.len())
            .field("kind", &self.kind)
            .field("index_status", &self.index_status)
            .field("worktree_status", &self.worktree_status)
            .field(
                "original_path_bytes",
                &self.original_path.as_ref().map(String::len),
            )
            .finish()
    }
}

/// Git status of the Workspace at one moment, together with the bounds that
/// limited the observation.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct WorkspaceStatusResult {
    pub repo_available: bool,
    pub head_oid: Option<String>,
    pub branch: Option<String>,
    pub detached: bool,
    /// Entries with an index status: staged changes.
    pub staged: u64,
    /// Entries with a worktree status: unstaged changes.
    pub unstaged: u64,
    /// Untracked entries, exactly as git reports them.
    pub untracked: u64,
    /// Unmerged entries, counted only as conflicts.
    pub conflicted: u64,
    /// A prefix of the changed entries. The counters above cover the whole
    /// observation, which may be longer than this list when the encoded budget
    /// cut it short.
    pub entries: Vec<WorkspaceStatusEntry>,
    /// Entries skipped because their path could not be represented.
    pub skipped_paths: u64,
    pub complete: bool,
    pub warnings: Vec<WorkspaceStatusWarning>,
    pub consistency: WorkspaceScanConsistency,
    pub observed_at_unix_ms: u64,
}

impl fmt::Debug for WorkspaceStatusResult {
    // Diagnostics report counts, lengths, and codes, never a workspace path, a
    // branch name, or an object id.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceStatusResult")
            .field("repo_available", &self.repo_available)
            .field("head_oid_bytes", &self.head_oid.as_ref().map(String::len))
            .field("branch_bytes", &self.branch.as_ref().map(String::len))
            .field("detached", &self.detached)
            .field("staged", &self.staged)
            .field("unstaged", &self.unstaged)
            .field("untracked", &self.untracked)
            .field("conflicted", &self.conflicted)
            .field("entries", &self.entries.len())
            .field("skipped_paths", &self.skipped_paths)
            .field("complete", &self.complete)
            .field("warnings", &self.warnings)
            .field("consistency", &self.consistency)
            .field("observed_at_unix_ms", &self.observed_at_unix_ms)
            .finish()
    }
}

/// Caps one query enforces. Tests may lower them.
#[derive(Clone, Copy)]
pub(crate) struct StatusCaps {
    pub(crate) stdout_bytes: usize,
    pub(crate) records: u64,
}

impl StatusCaps {
    const fn standard() -> Self {
        Self {
            stdout_bytes: MAX_STATUS_STDOUT_BYTES,
            records: MAX_STATUS_RECORDS,
        }
    }
}

/// Captured result of one git invocation. Standard error is counted and
/// discarded, so no git diagnostic can reach a response, a log, or an error.
struct GitOutput {
    stdout: Vec<u8>,
    exit_code: Option<i32>,
    spawn_failed: bool,
    /// A stdout or stderr bound stopped the child before it finished.
    truncated: bool,
    /// The deadline passed; the child was stopped.
    timed_out: bool,
    /// Reading a pipe or waiting for the child failed. The observation is
    /// incomplete even though no bound cut it short.
    io_failed: bool,
}

impl GitOutput {
    fn empty() -> Self {
        Self {
            stdout: Vec::new(),
            exit_code: None,
            spawn_failed: false,
            truncated: false,
            timed_out: false,
            io_failed: false,
        }
    }

    fn spawn_failed() -> Self {
        Self {
            spawn_failed: true,
            ..Self::empty()
        }
    }

    fn expired() -> Self {
        Self {
            timed_out: true,
            ..Self::empty()
        }
    }

    fn succeeded(&self) -> bool {
        self.exit_code == Some(0)
    }

    /// True when the child did not answer with success and no bound or
    /// cancellation explains it. A signal death has no exit code and counts as
    /// a failure, never as an empty answer.
    fn quietly_failed(&self) -> bool {
        !self.spawn_failed && !self.timed_out && !self.truncated && !self.succeeded()
    }
}

/// Everything one git invocation needs besides its arguments.
struct GitRun<'a> {
    program: &'a OsStr,
    root: &'a Path,
    env: &'a [(OsString, OsString)],
    caps: StatusCaps,
    deadline: Instant,
    session_cancellation: &'a CancellationToken,
    shutdown_cancellation: &'a CancellationToken,
    child_cancellation: &'a CancellationToken,
}

impl GitRun<'_> {
    fn cancelled(&self) -> bool {
        self.session_cancellation.is_cancelled()
            || self.shutdown_cancellation.is_cancelled()
            || self.child_cancellation.is_cancelled()
    }

    fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
}

/// Flags a run's readers publish to the race that owns the child.
#[derive(Default)]
struct GitFlags {
    stdout_capped: AtomicBool,
    stderr_capped: AtomicBool,
    io_failed: AtomicBool,
    stderr_bytes: AtomicU64,
}

/// What one parsing pass over the porcelain stream found.
#[derive(Default)]
struct Observation {
    head_oid: Option<String>,
    branch: Option<String>,
    detached: bool,
    saw_oid_header: bool,
    saw_head_header: bool,
    staged: u64,
    unstaged: u64,
    untracked: u64,
    conflicted: u64,
    entries: Vec<WorkspaceStatusEntry>,
    skipped_paths: u64,
    records: u64,
    truncated: bool,
    malformed: u64,
}

impl Observation {
    /// `--branch` prints both headers together. One without the other means the
    /// stream was cut by something this parser cannot account for, so the
    /// answer is not treated as complete.
    fn branch_headers_complete(&self) -> bool {
        self.saw_oid_header && self.saw_head_header
    }
}

/// Observes the Workspace's Git status through fixed, shell-free git arguments.
#[cfg(test)]
pub(crate) async fn status(
    workspace: &Workspace,
    request: &WorkspaceStatusRequest,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    child_cancellation: &CancellationToken,
) -> Result<WorkspaceStatusResult, AgentError> {
    let deadline = status_deadline_at(workspace.root());
    status_with_deadline(
        workspace,
        request,
        session_cancellation,
        shutdown_cancellation,
        child_cancellation,
        deadline,
    )
    .await
}

pub(crate) async fn status_with_deadline(
    workspace: &Workspace,
    request: &WorkspaceStatusRequest,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    child_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<WorkspaceStatusResult, AgentError> {
    let root = workspace.root().to_path_buf();
    let caps = status_caps(&root);
    let program = status_program(&root);
    let env = status_env(&root);
    let observed_at_unix_ms = now_unix_ms();
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
    let run = GitRun {
        program: program.as_os_str(),
        root: &root,
        env: &env,
        caps,
        deadline,
        session_cancellation,
        shutdown_cancellation,
        child_cancellation,
    };

    let mut warnings = Vec::new();
    let mut complete = true;

    // The work tree root bounds every path this query may report and is also
    // the directory git runs from, so porcelain paths are unambiguous.
    let probe = run_git(&run, &os_args(&["rev-parse", "--show-toplevel"])).await?;
    if probe.spawn_failed {
        // Git is not installed or not executable: an explicit unavailable
        // state, never a clean workspace.
        push_warning(&mut warnings, WorkspaceStatusWarning::GitUnavailable);
        return assemble(
            false,
            Observation::default(),
            warnings,
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    }
    if probe.timed_out {
        push_warning(&mut warnings, WorkspaceStatusWarning::Deadline);
        return assemble(
            false,
            Observation::default(),
            warnings,
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    }
    if probe.truncated || probe.io_failed {
        // The repository state was not observed, so it is reported as unknown
        // instead of as a definite answer.
        push_warning(
            &mut warnings,
            if probe.truncated {
                WorkspaceStatusWarning::OutputTruncated
            } else {
                WorkspaceStatusWarning::StatusFailed
            },
        );
        return assemble(
            false,
            Observation::default(),
            warnings,
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    }
    let toplevel = if probe.succeeded() {
        repository_root(&probe.stdout)
    } else {
        None
    };
    if probe.succeeded() && toplevel.is_none() {
        // Git answered, but the answer is not a usable work tree root.
        push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        return assemble(
            false,
            Observation::default(),
            warnings,
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    }
    let Some(toplevel) = toplevel else {
        // Without a work tree root, only a definite answer from git may be
        // reported as a complete `repo_available: false`. A refusal such as a
        // dubious ownership check, a corrupt configuration, or a permission
        // problem stays an unknown failure.
        let classify = run_git(&run, &os_args(&["rev-parse", "--is-bare-repository"])).await?;
        if classify.spawn_failed {
            push_warning(&mut warnings, WorkspaceStatusWarning::GitUnavailable);
        } else if classify.timed_out {
            push_warning(&mut warnings, WorkspaceStatusWarning::Deadline);
        } else if classify.truncated {
            push_warning(&mut warnings, WorkspaceStatusWarning::OutputTruncated);
        } else if classify.io_failed {
            push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        } else if classify.succeeded()
            && matches!(
                String::from_utf8_lossy(&classify.stdout).trim(),
                "true" | "false"
            )
        {
            // `true` is a bare repository, `false` means the workspace is
            // inside a git directory but not in a work tree. Both are definite
            // answers with no status to report.
            return assemble(
                false,
                Observation::default(),
                warnings,
                true,
                max_bytes,
                observed_at_unix_ms,
            );
        } else {
            push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        }
        return assemble(
            false,
            Observation::default(),
            warnings,
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    };
    let Some(pathspec) = workspace_pathspec(workspace.root(), &toplevel) else {
        // The work tree root cannot be related to the workspace, so no
        // porcelain path could be attributed safely.
        return assemble(
            false,
            Observation::default(),
            vec![WorkspaceStatusWarning::StatusFailed],
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    };
    if pathspec != OsStr::new(".") {
        push_warning(&mut warnings, WorkspaceStatusWarning::NestedRepository);
    }

    // Fixed machine-readable arguments. `--literal-pathspecs` and
    // `--no-optional-locks` are git options and come before the subcommand;
    // the pathspec is passed as one argument and never assembled by a shell.
    let mut args: Vec<OsString> = os_args(&[
        "--literal-pathspecs",
        "status",
        "--porcelain=v2",
        "-z",
        "--branch",
        "--no-ahead-behind",
        // Submodule internals are not observed: the recorded commit is compared,
        // and the submodule's work tree is never scanned and never recursed into.
        "--ignore-submodules=dirty",
        "--untracked-files=normal",
        "--",
    ]);
    args.push(pathspec);
    let run = GitRun {
        root: &toplevel,
        ..run
    };
    let out = run_git(&run, &args).await?;
    if out.spawn_failed {
        push_warning(&mut warnings, WorkspaceStatusWarning::GitUnavailable);
        return assemble(
            true,
            Observation::default(),
            warnings,
            false,
            max_bytes,
            observed_at_unix_ms,
        );
    }
    let observation = observe(&out.stdout, &toplevel, workspace.root(), caps);
    if out.timed_out {
        push_warning(&mut warnings, WorkspaceStatusWarning::Deadline);
        complete = false;
    }
    if out.truncated || observation.truncated {
        push_warning(&mut warnings, WorkspaceStatusWarning::OutputTruncated);
        complete = false;
    }
    if observation.skipped_paths > 0 {
        push_warning(&mut warnings, WorkspaceStatusWarning::SkippedPaths);
        complete = false;
    }
    let failed = observation.malformed > 0 || out.io_failed || out.quietly_failed();
    if failed {
        // A work tree exists but its status could not be read: report the
        // repository without pretending it is clean.
        push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        complete = false;
    } else if !observation.branch_headers_complete() && !out.truncated && !out.timed_out {
        // `--branch` prints both headers together, so a missing one means the
        // answer is not the complete one it appears to be.
        push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        complete = false;
    }
    assemble(
        true,
        observation,
        warnings,
        complete,
        max_bytes,
        observed_at_unix_ms,
    )
}

fn push_warning(warnings: &mut Vec<WorkspaceStatusWarning>, warning: WorkspaceStatusWarning) {
    if !warnings.contains(&warning) {
        warnings.push(warning);
    }
}

/// Builds the encoded result within `max_bytes`, keeping entries only while
/// they fit. The envelope it reserves room for is the widest form of this
/// result (every warning this query can carry and an incomplete answer), so
/// appending a warning after the last entry cannot push the encoded result past
/// the budget.
fn assemble(
    repo_available: bool,
    observation: Observation,
    warnings: Vec<WorkspaceStatusWarning>,
    complete: bool,
    max_bytes: usize,
    observed_at_unix_ms: u64,
) -> Result<WorkspaceStatusResult, AgentError> {
    let mut result = WorkspaceStatusResult {
        repo_available,
        head_oid: observation.head_oid,
        branch: observation.branch,
        detached: observation.detached,
        staged: observation.staged,
        unstaged: observation.unstaged,
        untracked: observation.untracked,
        conflicted: observation.conflicted,
        entries: Vec::new(),
        skipped_paths: observation.skipped_paths,
        complete,
        warnings,
        consistency: WorkspaceScanConsistency::Live,
        observed_at_unix_ms,
    };
    let mut envelope = result.clone();
    envelope.entries = Vec::new();
    envelope.complete = false;
    envelope.warnings = vec![
        WorkspaceStatusWarning::GitUnavailable,
        WorkspaceStatusWarning::StatusFailed,
        WorkspaceStatusWarning::OutputTruncated,
        WorkspaceStatusWarning::Deadline,
        WorkspaceStatusWarning::SkippedPaths,
        WorkspaceStatusWarning::NestedRepository,
    ];
    let envelope = encoded_len(&envelope)?;
    if envelope > max_bytes {
        // A valid budget that cannot hold even the summary is answered as a
        // failed observation whose summary fits, never as an invalid budget.
        result.head_oid = None;
        result.branch = None;
        result.entries = Vec::new();
        result.complete = false;
        result.warnings = vec![WorkspaceStatusWarning::StatusFailed];
        return Ok(result);
    }
    let mut used = envelope;
    for entry in observation.entries {
        let encoded = encoded_len(&entry)?;
        if used + encoded + 1 > max_bytes {
            result.complete = false;
            push_warning(
                &mut result.warnings,
                WorkspaceStatusWarning::OutputTruncated,
            );
            break;
        }
        used += encoded + 1;
        result.entries.push(entry);
    }
    Ok(result)
}

/// Parses one porcelain v2 (`-z`) stream, keeping only paths inside the
/// workspace. `toplevel` is where git was run, so every reported path is
/// relative to it.
fn observe(stdout: &[u8], toplevel: &Path, workspace_root: &Path, caps: StatusCaps) -> Observation {
    let mut found = Observation::default();
    // A record cut in half by the output ceiling or the deadline is not a
    // record: parse only up to the last NUL, so no truncated path is reported.
    let stream: &[u8] = match stdout.iter().rposition(|byte| *byte == 0) {
        Some(last) => &stdout[..=last],
        None => &[],
    };
    let mut chunks = stream.split(|byte| *byte == 0);
    while let Some(chunk) = chunks.next() {
        if chunk.is_empty() {
            continue;
        }
        if found.records >= caps.records {
            found.truncated = true;
            break;
        }
        match chunk.first().copied() {
            Some(b'#') => header(chunk, &mut found),
            Some(b'1') => {
                let Some(fields) = split_fields(chunk, 9) else {
                    found.malformed += 1;
                    continue;
                };
                // A record whose codes are not git's documented status
                // characters is not an entry this parser can report.
                let Some(statuses) = xy(fields[1]) else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, _)) =
                    map_entry(fields[8], None, toplevel, workspace_root, &mut found)
                {
                    found.records += 1;
                    count(&mut found, statuses);
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Ordinary,
                        index_status: Some(statuses.0.to_string()),
                        worktree_status: Some(statuses.1.to_string()),
                        original_path: None,
                    });
                }
            }
            Some(b'2') => {
                // A rename record is followed by a second NUL-terminated path.
                // The chunk is consumed before the record is validated, so a
                // malformed record cannot shift the stream.
                let original = chunks.next();
                let Some(fields) = split_fields(chunk, 10) else {
                    found.malformed += 1;
                    continue;
                };
                let statuses = xy(fields[1]);
                let Some(original) = original.filter(|original| !original.is_empty()) else {
                    // Without the previous name this is not a rename, and a
                    // half record never becomes a complete rename entry.
                    found.malformed += 1;
                    continue;
                };
                let Some(statuses) = statuses else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, original_path)) = map_entry(
                    fields[9],
                    Some(original),
                    toplevel,
                    workspace_root,
                    &mut found,
                ) {
                    found.records += 1;
                    count(&mut found, statuses);
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Renamed,
                        index_status: Some(statuses.0.to_string()),
                        worktree_status: Some(statuses.1.to_string()),
                        original_path,
                    });
                }
            }
            Some(b'u') => {
                let Some(fields) = split_fields(chunk, 11) else {
                    found.malformed += 1;
                    continue;
                };
                let Some(statuses) = xy(fields[1]) else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, _)) =
                    map_entry(fields[10], None, toplevel, workspace_root, &mut found)
                {
                    found.records += 1;
                    // A conflicted path is counted as a conflict, not as a
                    // staged or unstaged change.
                    found.conflicted += 1;
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Unmerged,
                        index_status: Some(statuses.0.to_string()),
                        worktree_status: Some(statuses.1.to_string()),
                        original_path: None,
                    });
                }
            }
            Some(b'?') => {
                let Some(fields) = split_fields(chunk, 2) else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, _)) =
                    map_entry(fields[1], None, toplevel, workspace_root, &mut found)
                {
                    found.records += 1;
                    found.untracked += 1;
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Untracked,
                        index_status: None,
                        worktree_status: None,
                        original_path: None,
                    });
                }
            }
            // Ignored entries are never requested; anything else is not a
            // record this parser knows.
            Some(b'!') => {}
            _ => found.malformed += 1,
        }
    }
    found
}

/// Counts one ordinary or renamed entry in the summary buckets.
fn count(found: &mut Observation, (x, y): (char, char)) {
    if x != '.' {
        found.staged += 1;
    }
    if y != '.' {
        found.unstaged += 1;
    }
}

/// The status characters git documents for a porcelain v2 `XY` field.
const STATUS_CODES: [char; 8] = ['.', 'M', 'T', 'A', 'D', 'R', 'C', 'U'];

/// The two status characters of an ordinary, renamed, or unmerged record.
/// Anything outside git's documented codes is not a record this parser accepts.
fn xy(field: &[u8]) -> Option<(char, char)> {
    let text = std::str::from_utf8(field).ok()?;
    let mut chars = text.chars();
    let x = chars.next()?;
    let y = chars.next()?;
    if chars.next().is_some() || !STATUS_CODES.contains(&x) || !STATUS_CODES.contains(&y) {
        return None;
    }
    Some((x, y))
}

/// A full object id: 40 hex digits (SHA-1) or 64 (SHA-256).
fn valid_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn header(chunk: &[u8], found: &mut Observation) {
    let Ok(text) = std::str::from_utf8(chunk) else {
        found.malformed += 1;
        return;
    };
    let Some(rest) = text.strip_prefix("# ") else {
        found.malformed += 1;
        return;
    };
    let Some((key, value)) = rest.split_once(' ') else {
        found.malformed += 1;
        return;
    };
    match key {
        "branch.oid" => {
            found.saw_oid_header = true;
            found.head_oid = match value {
                "(initial)" => None,
                value if valid_oid(value) => Some(value.to_owned()),
                // Only a hash git could have printed or the unborn marker is
                // accepted; anything else stays absent and malformed.
                _ => {
                    found.malformed += 1;
                    None
                }
            };
        }
        "branch.head" => {
            found.saw_head_header = true;
            if value == "(detached)" {
                found.detached = true;
                found.branch = None;
            } else if value.is_empty() || value.len() > MAX_STATUS_BRANCH_BYTES {
                found.malformed += 1;
            } else {
                found.branch = Some(value.to_owned());
            }
        }
        // `branch.upstream` and `branch.ab` are read but not reported.
        _ => {}
    }
}

/// Splits `fields` space-separated porcelain fields, so the last one keeps any
/// spaces the path contains.
fn split_fields(chunk: &[u8], fields: usize) -> Option<Vec<&[u8]>> {
    let parts: Vec<&[u8]> = chunk.splitn(fields, |byte| *byte == b' ').collect();
    if parts.len() != fields {
        return None;
    }
    Some(parts)
}

/// Maps one reported path into workspace-relative form, skipping or filtering
/// it. Returns the new path and, for renames, the previous one.
fn map_entry(
    path: &[u8],
    original: Option<&[u8]>,
    toplevel: &Path,
    workspace_root: &Path,
    found: &mut Observation,
) -> Option<(String, Option<String>)> {
    let mapped = match relative_to_workspace(path, toplevel, workspace_root) {
        Mapped::Inside(path) => path,
        Mapped::Outside => return None,
        Mapped::Skipped => {
            found.skipped_paths += 1;
            return None;
        }
    };
    let original_path = match original {
        None => None,
        Some(original) => match relative_to_workspace(original, toplevel, workspace_root) {
            Mapped::Inside(path) => Some(path),
            // A rename whose previous name cannot be represented or lies
            // outside the workspace keeps the entry and reports the current
            // name only.
            Mapped::Outside => None,
            Mapped::Skipped => {
                found.skipped_paths += 1;
                None
            }
        },
    };
    Some((mapped, original_path))
}

enum Mapped {
    Inside(String),
    Outside,
    Skipped,
}

/// Converts one git path, relative to the work tree root, into a
/// workspace-relative path. Paths outside the workspace are filtered out and
/// never counted; paths that cannot be represented are reported as skipped.
fn relative_to_workspace(path: &[u8], toplevel: &Path, workspace_root: &Path) -> Mapped {
    if path.len() > MAX_PATH_BYTES {
        return Mapped::Skipped;
    }
    let Ok(text) = std::str::from_utf8(path) else {
        return Mapped::Skipped;
    };
    let full = if Path::new(text).is_absolute() {
        PathBuf::from(text)
    } else {
        toplevel.join(text)
    };
    let mut normal = PathBuf::new();
    for component in full.components() {
        match component {
            Component::Prefix(prefix) => normal.push(prefix.as_os_str()),
            Component::RootDir => normal.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normal.pop() {
                    return Mapped::Skipped;
                }
            }
            Component::Normal(name) => normal.push(name),
        }
    }
    let Ok(stripped) = normal.strip_prefix(workspace_root) else {
        return Mapped::Outside;
    };
    let mut relative = String::new();
    for component in stripped.components() {
        let Some(name) = component.as_os_str().to_str() else {
            return Mapped::Skipped;
        };
        if !relative.is_empty() {
            relative.push('/');
        }
        relative.push_str(name);
    }
    Mapped::Inside(relative)
}

/// The path git is given to limit status to the workspace itself. `.` is used
/// when the workspace is the work tree root.
fn workspace_pathspec(workspace_root: &Path, toplevel: &Path) -> Option<OsString> {
    let stripped = workspace_root.strip_prefix(toplevel).ok()?;
    let mut spec = OsString::new();
    for component in stripped.components() {
        let name = component.as_os_str();
        if name.is_empty() {
            continue;
        }
        if !spec.is_empty() {
            spec.push("/");
        }
        spec.push(name);
    }
    if spec.is_empty() {
        spec.push(".");
    }
    Some(spec)
}

fn repository_root(stdout: &[u8]) -> Option<PathBuf> {
    let text = std::str::from_utf8(stdout).ok()?;
    let path = text.strip_suffix('\n').unwrap_or(text);
    if path.is_empty() || path.contains('\n') {
        return None;
    }
    // The work tree root must be a real directory that can be canonicalized
    // and compared with the canonical workspace root (for example `/tmp`
    // against `/private/tmp`). An answer that fails that check is not a root
    // every porcelain path can be attributed to.
    let canonical = std::fs::canonicalize(path).ok()?;
    canonical.is_dir().then_some(canonical)
}

fn os_args(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}

/// Runs one git command with fixed, shell-free arguments. A cancelled or
/// already-expired query never starts a process. The child is owned by this
/// future: cancellation, the deadline, a stdout or stderr bound, and every wait
/// stop and reap it, and the wait after its pipes close is under the same
/// budget. `kill_on_drop` stays only as a last-resort backstop while the
/// runtime is being torn down; it is not what reaps a query.
async fn run_git(run: &GitRun<'_>, args: &[OsString]) -> Result<GitOutput, AgentError> {
    if run.cancelled() {
        return Err(AgentError::QueryLimit);
    }
    if run.expired() {
        return Ok(GitOutput::expired());
    }
    let mut command = Command::new(run.program);
    command
        .arg("-C")
        .arg(run.root)
        .arg("--no-optional-locks")
        .arg("--no-pager")
        // Fixed configuration, so neither a user's global configuration nor a
        // repository setting can start a file system monitor, recurse into
        // submodules, colour the output, or change how renames are detected.
        .arg("-c")
        .arg("core.fsmonitor=false")
        .arg("-c")
        .arg("submodule.recurse=false")
        .arg("-c")
        .arg("color.ui=false")
        .arg("-c")
        .arg("core.quotePath=false")
        .arg("-c")
        .arg("diff.renames=true")
        .arg("-c")
        .arg("status.renames=true")
        .arg("-c")
        .arg("status.relativePaths=false")
        .args(args);
    // Every inherited `GIT_*` name is dropped before the child starts, so no
    // `GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_CONFIG_*`, or
    // `GIT_TRACE*` value can redirect or instrument the query.
    command.env_clear();
    for (name, value) in run.env {
        command.env(name, value);
    }
    // System and user configuration are disabled explicitly, so no inherited
    // `HOME`, `XDG_CONFIG_HOME`, or `GIT_CONFIG_COUNT` can point git at a
    // configuration that includes another file or starts an external program.
    command.env("GIT_CONFIG_NOSYSTEM", "1");
    command.env("GIT_CONFIG_GLOBAL", null_device());
    command.env("GIT_OPTIONAL_LOCKS", "0");
    command.env("GIT_NO_LAZY_FETCH", "1");
    command.env("GIT_NO_REPLACE_OBJECTS", "1");
    // A query never inherits the Agent's RPC input pipe.
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.kill_on_drop(true);

    let Ok(mut child) = command.spawn() else {
        return Ok(GitOutput::spawn_failed());
    };
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    // The captured bytes and the reader flags live outside the reader futures,
    // so whatever a race endpoint saw stays available to the caller.
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let flags = Arc::new(GitFlags::default());
    // A reader that reaches a bound notifies the race below. A `Notify` never
    // fires because a reader ended: only an actual bound ends the run early.
    let stdout_bound = Arc::new(tokio::sync::Notify::new());
    let stderr_bound = Arc::new(tokio::sync::Notify::new());

    let capture = {
        let captured = Arc::clone(&captured);
        let flags = Arc::clone(&flags);
        let bound = Arc::clone(&stdout_bound);
        async move {
            let mut chunk = vec![0u8; READ_CHUNK_BYTES];
            loop {
                match stdout.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(read) => {
                        // The chunk that crosses the ceiling contributes its
                        // prefix: the observation keeps every byte that fits.
                        let mut buffer = captured.lock().unwrap();
                        let remaining = run.caps.stdout_bytes.saturating_sub(buffer.len());
                        let keep = remaining.min(read);
                        buffer.extend_from_slice(&chunk[..keep]);
                        drop(buffer);
                        if keep < read {
                            flags.stdout_capped.store(true, Ordering::Release);
                            bound.notify_one();
                            break;
                        }
                    }
                    Err(_) => {
                        // A read failure is not an end of output.
                        flags.io_failed.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        }
    };
    // Standard error is counted and discarded: a full stderr pipe must never
    // block the child, and nothing git prints there may reach the caller. Past
    // the count bound the child is stopped instead of draining without bound.
    let drain = {
        let flags = Arc::clone(&flags);
        let bound = Arc::clone(&stderr_bound);
        async move {
            let mut chunk = [0u8; STATUS_STDERR_CHUNK_BYTES];
            loop {
                match stderr.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(read) => {
                        let total = flags.stderr_bytes.fetch_add(read as u64, Ordering::AcqRel)
                            + read as u64;
                        if total > MAX_STATUS_STDERR_BYTES {
                            flags.stderr_capped.store(true, Ordering::Release);
                            bound.notify_one();
                            break;
                        }
                    }
                    Err(_) => {
                        flags.io_failed.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        }
    };

    let mut timed_out = false;
    let waited = tokio::select! {
        biased;
        _ = run.session_cancellation.cancelled() => Stop::Cancelled,
        _ = run.shutdown_cancellation.cancelled() => Stop::Cancelled,
        _ = run.child_cancellation.cancelled() => Stop::Cancelled,
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(run.deadline)) => {
            timed_out = true;
            Stop::Expired
        }
        _ = stdout_bound.notified() => Stop::Bounded,
        _ = stderr_bound.notified() => Stop::Bounded,
        _ = futures_util::future::join(capture, drain) => Stop::PipesClosed,
    };
    let mut truncated =
        flags.stdout_capped.load(Ordering::Acquire) || flags.stderr_capped.load(Ordering::Acquire);
    let mut io_failed = flags.io_failed.load(Ordering::Acquire);
    let mut exit_code = None;
    match waited {
        Stop::PipesClosed => {
            // The pipes are closed, but the child still owns its exit status:
            // wait for it under the same budget instead of waiting without one.
            let status = tokio::select! {
                biased;
                _ = run.session_cancellation.cancelled() => Stop::Cancelled,
                _ = run.shutdown_cancellation.cancelled() => Stop::Cancelled,
                _ = run.child_cancellation.cancelled() => Stop::Cancelled,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(run.deadline)) => {
                    Stop::Expired
                }
                status = child.wait() => {
                    match status {
                        Ok(status) => {
                            exit_code = status.code();
                            Stop::Reaped
                        }
                        Err(_) => {
                            io_failed = true;
                            Stop::Reaped
                        }
                    }
                }
            };
            match status {
                Stop::Reaped => {}
                Stop::Cancelled => {
                    if !stop_child(&mut child, run).await {
                        // The child was asked to stop and was not reaped: the
                        // query fails instead of reporting a cancellation it
                        // cannot back.
                        return Err(AgentError::Internal);
                    }
                    return Err(AgentError::QueryLimit);
                }
                Stop::Expired => {
                    timed_out = true;
                    if !stop_child(&mut child, run).await {
                        io_failed = true;
                    }
                }
                Stop::PipesClosed | Stop::Bounded => {
                    unreachable!("the wait branch only stops by the budget")
                }
            }
        }
        Stop::Cancelled => {
            if !stop_child(&mut child, run).await {
                return Err(AgentError::Internal);
            }
            return Err(AgentError::QueryLimit);
        }
        Stop::Expired => {
            timed_out = true;
            if !stop_child(&mut child, run).await {
                io_failed = true;
            }
        }
        Stop::Bounded => {
            truncated = true;
            if !stop_child(&mut child, run).await {
                io_failed = true;
            }
        }
        Stop::Reaped => {}
    }
    let stdout = std::mem::take(&mut *captured.lock().unwrap());
    Ok(GitOutput {
        stdout,
        exit_code,
        spawn_failed: false,
        truncated,
        timed_out,
        io_failed,
    })
}

/// Why one git invocation's race ended.
#[derive(Clone, Copy)]
enum Stop {
    /// Both pipes reached end of file; the child may still be running.
    PipesClosed,
    /// The query was cancelled.
    Cancelled,
    /// The deadline passed.
    Expired,
    /// A stdout or stderr bound was reached.
    Bounded,
    /// The child exited and was reaped.
    Reaped,
}

/// Stops the owned child and reaps it. The owner waits until the operating
/// system reports the child, so a stuck system can delay the query past its
/// deadline; that delay is documented and is never replaced by a latency-based
/// guess at termination. Returns false when the kill and the wait could not
/// both be confirmed, which the caller reports as a failed observation.
async fn stop_child(child: &mut tokio::process::Child, run: &GitRun<'_>) -> bool {
    let _ = run;
    // A Unix test may hold this point to observe the owner while its child is
    // still unreaped; the hold never appears outside Unix tests.
    #[cfg(all(test, unix))]
    if let Some(gate) = status_reap_gate(run.root) {
        gate.hold().await;
    }
    let killed = child.start_kill();
    if killed.is_err() {
        // An error here can simply mean the child already exited; only the exit
        // status settles that, so the wait below is what confirms it.
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
    }
    child.wait().await.is_ok()
}

/// A platform null file, so git's user configuration is explicitly empty.
fn null_device() -> &'static OsStr {
    #[cfg(windows)]
    {
        OsStr::new("NUL")
    }
    #[cfg(not(windows))]
    {
        OsStr::new("/dev/null")
    }
}

#[cfg(test)]
static STATUS_CAPS: OnceLock<Mutex<Vec<(PathBuf, StatusCaps)>>> = OnceLock::new();
#[cfg(test)]
static STATUS_DEADLINES: OnceLock<Mutex<Vec<(PathBuf, Duration)>>> = OnceLock::new();
#[cfg(test)]
static STATUS_PROGRAMS: OnceLock<Mutex<Vec<(PathBuf, OsString)>>> = OnceLock::new();
#[cfg(test)]
type StatusEnvs = Mutex<Vec<(PathBuf, Vec<(OsString, OsString)>)>>;
#[cfg(all(test, unix))]
type StatusReapGates = Mutex<Vec<(PathBuf, Arc<StatusReapGate>)>>;
#[cfg(test)]
static STATUS_ENVS: OnceLock<StatusEnvs> = OnceLock::new();
#[cfg(all(test, unix))]
static STATUS_REAP_GATES: OnceLock<StatusReapGates> = OnceLock::new();

/// A test seam that parks one owned worker at the moment it would stop its
/// child, so a test can observe the owner while the process is still unreaped
/// and then release it without sleeping. It never exists outside Unix tests.
#[cfg(all(test, unix))]
pub(crate) struct StatusReapGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(all(test, unix))]
impl StatusReapGate {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })
    }

    /// Signals that a worker reached this point and waits for the release.
    async fn hold(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }

    /// Waits until a worker reached this point.
    pub(crate) async fn entered(&self) {
        self.entered.notified().await;
    }

    /// Lets the parked worker stop and reap its child.
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(all(test, unix))]
fn status_reap_gate(root: &Path) -> Option<Arc<StatusReapGate>> {
    lookup(&STATUS_REAP_GATES, root, Arc::clone)
}

/// Parks one owned worker at the moment it would stop its child.
#[cfg(all(test, unix))]
pub(crate) fn set_status_reap_gate(root: PathBuf, gate: Arc<StatusReapGate>) {
    STATUS_REAP_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((root, gate));
}

fn status_caps(root: &Path) -> StatusCaps {
    #[cfg(test)]
    if let Some(caps) = lookup(&STATUS_CAPS, root, |caps| *caps) {
        return caps;
    }
    let _ = root;
    StatusCaps::standard()
}

pub(crate) fn status_deadline_at(root: &Path) -> Instant {
    Instant::now()
        .checked_add(status_deadline(root))
        .unwrap_or_else(Instant::now)
}

fn status_deadline(root: &Path) -> Duration {
    #[cfg(test)]
    if let Some(deadline) = lookup(&STATUS_DEADLINES, root, |deadline| *deadline) {
        return deadline;
    }
    let _ = root;
    WORKSPACE_STATUS_DEADLINE
}

fn status_program(root: &Path) -> OsString {
    #[cfg(test)]
    if let Some(program) = lookup(&STATUS_PROGRAMS, root, |program| program.clone()) {
        return program;
    }
    let _ = root;
    OsString::from("git")
}

/// The environment a git child receives: every inherited `GIT_*` name is
/// dropped, so no `GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_CONFIG_*`,
/// or `GIT_TRACE*` value can redirect or instrument the query. Values are
/// filtered by name and never recorded.
fn status_env(root: &Path) -> Vec<(OsString, OsString)> {
    #[cfg(test)]
    let inherited: Vec<(OsString, OsString)> =
        lookup(&STATUS_ENVS, root, Clone::clone).unwrap_or_else(|| std::env::vars_os().collect());
    #[cfg(not(test))]
    let inherited: Vec<(OsString, OsString)> = std::env::vars_os().collect();
    let _ = root;
    // The comparison is ASCII case-insensitive on every platform: Windows
    // environment names are case-insensitive, so `git_dir` must be dropped
    // exactly like `GIT_DIR`. Values are filtered by name and never recorded.
    inherited
        .into_iter()
        .filter(|(name, _)| {
            let name = name.to_string_lossy();
            !name
                .as_bytes()
                .get(..4)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"GIT_"))
        })
        .collect()
}

#[cfg(test)]
fn lookup<T: Clone>(
    entries: &OnceLock<Mutex<Vec<(PathBuf, T)>>>,
    root: &Path,
    take: impl Fn(&T) -> T,
) -> Option<T> {
    entries.get().and_then(|entries| {
        entries
            .lock()
            .unwrap()
            .iter()
            .find(|(path, _)| path.as_path() == root)
            .map(|(_, value)| take(value))
    })
}

/// Lowers the query caps for one workspace, so a test can reach the output and
/// record ceilings without producing a megabyte of output.
#[cfg(test)]
pub(crate) fn set_status_caps(root: PathBuf, stdout_bytes: usize, records: u64) {
    STATUS_CAPS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((
            root,
            StatusCaps {
                stdout_bytes,
                records,
            },
        ));
}

/// Shortens the query deadline for one workspace, so a timeout can be observed
/// without waiting for real time to pass.
#[cfg(test)]
pub(crate) fn set_status_deadline(root: PathBuf, deadline: Duration) {
    STATUS_DEADLINES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((root, deadline));
}

/// Replaces the git executable for one workspace.
#[cfg(test)]
pub(crate) fn set_status_program(root: PathBuf, program: PathBuf) {
    STATUS_PROGRAMS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((root, program.into_os_string()));
}

/// Replaces the inherited environment for one workspace, so a test can prove
/// that a `GIT_*` variable from the parent never reaches the git child.
#[cfg(test)]
pub(crate) fn set_status_env(root: PathBuf, env: Vec<(OsString, OsString)>) {
    STATUS_ENVS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((root, env));
}

#[cfg(test)]
mod tests;
