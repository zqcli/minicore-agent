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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
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
#[derive(Clone, Eq, PartialEq, Serialize)]
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
pub(crate) async fn status(
    workspace: &Workspace,
    request: &WorkspaceStatusRequest,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    child_cancellation: &CancellationToken,
) -> Result<WorkspaceStatusResult, AgentError> {
    let root = workspace.root().to_path_buf();
    let deadline = Instant::now()
        .checked_add(status_deadline(&root))
        .unwrap_or_else(Instant::now);
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
mod tests {
    use super::*;
    use std::process::{Command as StdCommand, Output};
    use std::sync::Arc;

    struct Fixture {
        base: PathBuf,
        root: PathBuf,
        workspace: Arc<Workspace>,
    }

    /// A workspace directory that is not inside any repository.
    async fn plain_fixture(label: &str) -> Fixture {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-status-{label}-{}",
            SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());
        Fixture {
            base,
            root,
            workspace,
        }
    }

    /// A workspace that is the root of its own freshly initialized repository.
    async fn repo_fixture(label: &str) -> Fixture {
        let fixture = plain_fixture(label).await;
        git(&fixture.root, &["init", "--quiet"]);
        fixture
    }

    /// Runs one fixture-local git command. Identity is passed per command, so
    /// no global configuration is ever read or written.
    fn git_output(root: &Path, args: &[&str]) -> Output {
        StdCommand::new("git")
            .arg("-C")
            .arg(root)
            .arg("-c")
            .arg("user.name=minicore-test")
            .arg("-c")
            .arg("user.email=minicore-test@example.invalid")
            .args(args)
            .output()
            .expect("git must be available for workspace status tests")
    }

    fn git(root: &Path, args: &[&str]) -> Output {
        let output = git_output(root, args);
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn write_file(root: &Path, path: &str, bytes: &[u8]) {
        let full = root.join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, bytes).unwrap();
    }

    async fn observe(workspace: &Workspace) -> WorkspaceStatusResult {
        request(workspace, None).await.unwrap()
    }

    async fn request(
        workspace: &Workspace,
        max_bytes: Option<usize>,
    ) -> Result<WorkspaceStatusResult, AgentError> {
        let request = WorkspaceStatusRequest {
            session_id: SessionId::new().unwrap(),
            max_bytes,
        };
        status(
            workspace,
            &request,
            &CancellationToken::new(),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await
    }

    fn cleanup(fixture: &Fixture) {
        let _ = std::fs::remove_dir_all(&fixture.base);
    }

    #[cfg(unix)]
    fn executable(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, contents).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[tokio::test]
    async fn a_clean_repository_is_observed_completely() {
        let fixture = repo_fixture("clean").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(
            result
                .head_oid
                .as_deref()
                .is_some_and(|oid| oid.len() == 40)
        );
        assert!(result.branch.is_some());
        assert!(!result.detached);
        assert_eq!(
            (
                result.staged,
                result.unstaged,
                result.untracked,
                result.conflicted
            ),
            (0, 0, 0, 0)
        );
        assert!(result.entries.is_empty());
        assert!(result.complete);
        assert_eq!(result.skipped_paths, 0);
        assert!(result.warnings.is_empty(), "{:?}", result.warnings);
        assert_eq!(result.consistency, WorkspaceScanConsistency::Live);
        assert!(result.observed_at_unix_ms > 0);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn staged_unstaged_and_untracked_entries_are_counted() {
        let fixture = repo_fixture("changes").await;
        write_file(&fixture.root, "staged.txt", b"one\n");
        write_file(&fixture.root, "unstaged.txt", b"two\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        write_file(&fixture.root, "staged.txt", b"one changed\n");
        git(&fixture.root, &["add", "staged.txt"]);
        write_file(&fixture.root, "unstaged.txt", b"two changed\n");
        write_file(&fixture.root, "untracked.txt", b"three\n");

        let result = observe(&fixture.workspace).await;
        assert_eq!(result.staged, 1);
        assert_eq!(result.unstaged, 1);
        assert_eq!(result.untracked, 1);
        assert_eq!(result.conflicted, 0);
        assert!(result.complete);
        assert!(result.warnings.is_empty(), "{:?}", result.warnings);

        let staged = result
            .entries
            .iter()
            .find(|entry| entry.path == "staged.txt")
            .expect("staged entry");
        assert_eq!(staged.kind, WorkspaceStatusEntryKind::Ordinary);
        assert_eq!(staged.index_status.as_deref(), Some("M"));
        assert_eq!(staged.worktree_status.as_deref(), Some("."));
        assert_eq!(staged.original_path, None);

        let unstaged = result
            .entries
            .iter()
            .find(|entry| entry.path == "unstaged.txt")
            .expect("unstaged entry");
        assert_eq!(unstaged.index_status.as_deref(), Some("."));
        assert_eq!(unstaged.worktree_status.as_deref(), Some("M"));

        let untracked = result
            .entries
            .iter()
            .find(|entry| entry.path == "untracked.txt")
            .expect("untracked entry");
        assert_eq!(untracked.kind, WorkspaceStatusEntryKind::Untracked);
        assert_eq!(untracked.index_status, None);
        assert_eq!(untracked.worktree_status, None);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn an_unborn_head_reports_a_branch_without_an_oid() {
        let fixture = repo_fixture("unborn").await;

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert_eq!(result.head_oid, None);
        assert!(result.branch.is_some());
        assert!(!result.detached);
        assert!(result.complete);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_detached_head_reports_detached_without_a_branch() {
        let fixture = repo_fixture("detached").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        git(&fixture.root, &["checkout", "--quiet", "--detach"]);

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(result.detached);
        assert_eq!(result.branch, None);
        assert!(result.head_oid.is_some());
        assert!(result.complete);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_conflict_is_reported_as_unmerged() {
        let fixture = repo_fixture("conflict").await;
        write_file(&fixture.root, "shared.txt", b"base\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "base"]);
        git(&fixture.root, &["checkout", "--quiet", "-b", "other"]);
        write_file(&fixture.root, "shared.txt", b"other\n");
        git(&fixture.root, &["commit", "--quiet", "-am", "other"]);
        git(&fixture.root, &["checkout", "--quiet", "-"]);
        write_file(&fixture.root, "shared.txt", b"current\n");
        git(&fixture.root, &["commit", "--quiet", "-am", "current"]);
        let merge = git_output(&fixture.root, &["merge", "other"]);
        assert!(
            !merge.status.success(),
            "the merge was expected to conflict"
        );

        let result = observe(&fixture.workspace).await;
        assert_eq!(result.conflicted, 1);
        assert!(result.complete);
        let entry = result
            .entries
            .iter()
            .find(|entry| entry.kind == WorkspaceStatusEntryKind::Unmerged)
            .expect("unmerged entry");
        assert_eq!(entry.path, "shared.txt");
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_rename_keeps_both_paths() {
        let fixture = repo_fixture("rename").await;
        write_file(&fixture.root, "before.txt", b"content\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        std::fs::rename(
            fixture.root.join("before.txt"),
            fixture.root.join("after.txt"),
        )
        .unwrap();
        git(&fixture.root, &["add", "-A"]);

        let result = observe(&fixture.workspace).await;
        assert!(result.complete);
        let entry = result
            .entries
            .iter()
            .find(|entry| entry.kind == WorkspaceStatusEntryKind::Renamed)
            .expect("a rename record");
        assert_eq!(entry.path, "after.txt");
        assert_eq!(entry.original_path.as_deref(), Some("before.txt"));
        assert_eq!(entry.index_status.as_deref(), Some("R"));
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_non_repository_is_reported_conservatively() {
        let fixture = plain_fixture("nonrepo").await;

        // Git refuses to resolve a work tree, and a refusal is not proof that
        // no repository exists: the answer stays incomplete instead of
        // claiming a definite missing repository.
        let result = observe(&fixture.workspace).await;
        assert!(!result.repo_available);
        assert_eq!(result.head_oid, None);
        assert_eq!(result.branch, None);
        assert!(!result.detached);
        assert!(result.entries.is_empty());
        assert!(!result.complete);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_bare_repository_is_a_complete_answer_without_a_work_tree() {
        let fixture = plain_fixture("bare").await;
        git(&fixture.root, &["init", "--quiet", "--bare"]);

        // `--is-bare-repository` answers with `true`: a definite state without
        // a work tree to observe.
        let result = observe(&fixture.workspace).await;
        assert!(!result.repo_available);
        assert!(result.entries.is_empty());
        assert!(result.complete);
        assert!(result.warnings.is_empty(), "{:?}", result.warnings);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_missing_git_executable_is_reported_as_unavailable() {
        let fixture = plain_fixture("missing-git").await;
        set_status_program(
            fixture.workspace.root().to_path_buf(),
            fixture.base.join("missing-git"),
        );

        let result = observe(&fixture.workspace).await;
        assert!(!result.repo_available);
        assert!(!result.complete);
        assert_eq!(
            result.warnings,
            vec![WorkspaceStatusWarning::GitUnavailable]
        );
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_nested_workspace_reports_only_paths_inside_it() {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-status-nested-{}",
            SessionId::new().unwrap()
        ));
        let repository = base.join("repo");
        let root = repository.join("sub");
        std::fs::create_dir_all(&root).unwrap();
        git(&repository, &["init", "--quiet"]);
        write_file(&repository, "outside.txt", b"outside\n");
        write_file(&repository, "outside-only.txt", b"outside\n");
        write_file(&root, "tracked.txt", b"tracked\n");
        write_file(&root, "removed.txt", b"removed\n");
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "initial"]);
        // A change outside the workspace that stays outside, a move that
        // crosses into it, a staged deletion whose file is already gone, and an
        // untracked file: only the paths inside may appear.
        write_file(&repository, "outside-only.txt", b"changed outside\n");
        git(&repository, &["mv", "outside.txt", "sub/moved.txt"]);
        std::fs::remove_file(root.join("removed.txt")).unwrap();
        git(&repository, &["add", "-A", "--", "sub/removed.txt"]);
        write_file(&root, "inside.txt", b"inside\n");
        let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());

        let result = observe(&workspace).await;
        assert!(result.repo_available);
        assert!(result.complete);
        assert!(
            result
                .warnings
                .contains(&WorkspaceStatusWarning::NestedRepository)
        );
        let mut paths: Vec<&str> = result
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        // The contract does not order tracked and untracked entries together,
        // so the set is compared, not the emission order.
        paths.sort_unstable();
        assert_eq!(paths, vec!["inside.txt", "moved.txt", "removed.txt"]);
        let moved = result
            .entries
            .iter()
            .find(|entry| entry.path == "moved.txt")
            .expect("the moved entry");
        assert_eq!(moved.original_path, None);
        assert_eq!(
            (
                result.staged,
                result.unstaged,
                result.untracked,
                result.conflicted
            ),
            (2, 0, 1, 0)
        );
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(
            !serialized.contains("outside"),
            "a path outside the workspace leaked into the result"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    /// A staged gitlink change is a commit recorded in the superproject, so it
    /// is reported and counted as staged. The submodule's own work tree is never
    /// scanned and never recursed into, and no monitor or hook configured inside
    /// it runs.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_staged_gitlink_is_counted_without_entering_the_submodule() {
        let fixture = repo_fixture("gitlink").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        let first = git_output(&fixture.root, &["rev-parse", "HEAD"]);
        let first = String::from_utf8(first.stdout).unwrap().trim().to_owned();
        // A gitlink in the index and in a commit, without a network access and
        // without any submodule fetch.
        git(
            &fixture.root,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                "160000",
                first.as_str(),
                "sub",
            ],
        );
        git(
            &fixture.root,
            &["commit", "--quiet", "-m", "record gitlink"],
        );
        write_file(&fixture.root, "second.txt", b"second\n");
        // Only the new file is staged: adding everything would record the
        // absence of the gitlink's (still missing) work tree directory and drop
        // the gitlink from HEAD, which this fixture must keep.
        git(&fixture.root, &["add", "--", "second.txt"]);
        git(&fixture.root, &["commit", "--quiet", "-m", "second"]);
        // The recorded gitlink is still in HEAD, so the change below is a
        // recorded modification rather than an addition.
        let recorded = git_output(&fixture.root, &["ls-tree", "HEAD", "--", "sub"]);
        assert!(
            String::from_utf8(recorded.stdout)
                .unwrap()
                .starts_with("160000 commit"),
            "the fixture lost its recorded gitlink"
        );
        let second = git_output(&fixture.root, &["rev-parse", "HEAD"]);
        let second = String::from_utf8(second.stdout).unwrap().trim().to_owned();
        git(
            &fixture.root,
            &[
                "update-index",
                "--cacheinfo",
                "160000",
                second.as_str(),
                "sub",
            ],
        );

        // A real repository in the submodule's place, with an untracked file
        // and traps that would leave a marker if anything inside it ran.
        let submodule = fixture.root.join("sub");
        std::fs::create_dir_all(&submodule).unwrap();
        git(&submodule, &["init", "--quiet"]);
        write_file(&submodule, "untracked.txt", b"x");
        let fsmonitor_marker = fixture.base.join("submodule-fsmonitor-ran");
        let fsmonitor = fixture.base.join("submodule-fsmonitor");
        executable(
            &fsmonitor,
            &format!("#!/bin/sh\n: > '{}'\n", fsmonitor_marker.display()),
        );
        let hook_marker = fixture.base.join("submodule-hook-ran");
        let hooks = fixture.base.join("submodule-hooks");
        std::fs::create_dir_all(&hooks).unwrap();
        executable(
            &hooks.join("post-index-change"),
            &format!("#!/bin/sh\n: > '{}'\n", hook_marker.display()),
        );
        git(
            &submodule,
            &["config", "core.fsmonitor", fsmonitor.to_str().unwrap()],
        );
        git(
            &submodule,
            &["config", "core.hooksPath", hooks.to_str().unwrap()],
        );

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(result.complete, "{:?}", result.warnings);
        let entry = result
            .entries
            .iter()
            .find(|entry| entry.path == "sub")
            .expect("the gitlink entry");
        assert_eq!(entry.kind, WorkspaceStatusEntryKind::Ordinary);
        assert_eq!(entry.index_status.as_deref(), Some("M"));
        assert_eq!(result.staged, 1);
        // Nothing inside the submodule is reported: the untracked file there
        // does not appear and its work tree is not walked.
        assert_eq!(result.untracked, 0, "{:?}", result);
        assert!(
            !result
                .entries
                .iter()
                .any(|entry| entry.path.starts_with("sub/")),
            "a submodule path was reported"
        );
        assert!(
            !fsmonitor_marker.exists(),
            "a submodule file system monitor ran"
        );
        assert!(!hook_marker.exists(), "a submodule hook ran");
        cleanup(&fixture);
    }

    /// A rename whose previous name lies outside the workspace keeps the entry
    /// and reports the current name only: the previous name is never returned.
    #[tokio::test]
    async fn a_rename_from_outside_the_workspace_never_reports_its_previous_name() {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-status-cross-rename-{}",
            SessionId::new().unwrap()
        ));
        let repository = base.join("repo");
        let root = repository.join("sub");
        std::fs::create_dir_all(&root).unwrap();
        let workspace = Workspace::open(root.clone()).await.unwrap();
        // The workspace root is canonical, so the stand-in work tree root must
        // be too for the prefix check this test exercises.
        let repository = std::fs::canonicalize(&repository).unwrap();
        let mut found = Observation::default();

        let mapped = super::map_entry(
            b"sub/inside.txt",
            Some(b"outside.txt"),
            &repository,
            workspace.root(),
            &mut found,
        );
        assert_eq!(mapped, Some(("inside.txt".to_owned(), None)));
        assert_eq!(found.skipped_paths, 0);
        // An outside path is filtered before it can even be counted.
        let mut found = Observation::default();
        let mapped = super::map_entry(
            b"outside.txt",
            None,
            &repository,
            workspace.root(),
            &mut found,
        );
        assert_eq!(mapped, None);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn odd_paths_survive_the_nul_parser() {
        let fixture = repo_fixture("odd-paths").await;
        write_file(&fixture.root, "with space.txt", b"x");
        write_file(&fixture.root, "-leading-dash.txt", b"x");
        #[cfg(unix)]
        write_file(&fixture.root, "line\nbreak.txt", b"x");

        let result = observe(&fixture.workspace).await;
        assert!(result.complete);
        let mut paths: Vec<&str> = result
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        paths.sort_unstable();
        let mut expected = vec!["-leading-dash.txt", "with space.txt"];
        #[cfg(unix)]
        expected.push("line\nbreak.txt");
        expected.sort_unstable();
        assert_eq!(paths, expected);
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_non_utf8_path_is_skipped_and_marked_incomplete() {
        use std::os::unix::ffi::OsStrExt;
        let fixture = repo_fixture("non-utf8").await;
        let name = std::ffi::OsStr::from_bytes(b"bad-\xff\xfe.txt");
        std::fs::write(fixture.root.join(name), b"x").unwrap();

        let result = observe(&fixture.workspace).await;
        assert_eq!(result.skipped_paths, 1);
        assert!(
            result
                .warnings
                .contains(&WorkspaceStatusWarning::SkippedPaths)
        );
        assert!(!result.complete);
        assert!(result.entries.is_empty());
        assert_eq!(result.untracked, 0);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn the_stdout_ceiling_stops_the_query() {
        let fixture = repo_fixture("stdout-cap").await;
        for index in 0..40 {
            write_file(
                &fixture.root,
                &format!("a-rather-long-untracked-name-{index:02}.txt"),
                b"x",
            );
        }
        set_status_caps(
            fixture.workspace.root().to_path_buf(),
            512,
            MAX_STATUS_RECORDS,
        );

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(!result.complete);
        assert_eq!(
            result.warnings,
            vec![WorkspaceStatusWarning::OutputTruncated]
        );
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn the_record_ceiling_stops_the_query() {
        let fixture = repo_fixture("record-cap").await;
        for index in 0..3 {
            write_file(&fixture.root, &format!("file-{index}.txt"), b"x");
        }
        set_status_caps(
            fixture.workspace.root().to_path_buf(),
            MAX_STATUS_STDOUT_BYTES,
            1,
        );

        let result = observe(&fixture.workspace).await;
        assert_eq!(result.untracked, 1);
        assert!(!result.complete);
        assert_eq!(
            result.warnings,
            vec![WorkspaceStatusWarning::OutputTruncated]
        );
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn the_encoded_budget_truncates_entries_without_losing_the_summary() {
        let fixture = repo_fixture("encoded-budget").await;
        for index in 0..40 {
            write_file(
                &fixture.root,
                &format!("a-rather-long-untracked-name-{index:02}.txt"),
                b"x",
            );
        }

        let result = request(&fixture.workspace, Some(MIN_RESULT_BYTES))
            .await
            .unwrap();
        assert_eq!(result.untracked, 40);
        assert!(result.entries.len() < 40);
        assert!(!result.complete);
        assert_eq!(
            result.warnings,
            vec![WorkspaceStatusWarning::OutputTruncated]
        );
        assert!(encoded_len(&result).unwrap() <= MIN_RESULT_BYTES);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn an_expired_deadline_reports_an_unavailable_observation() {
        let fixture = repo_fixture("deadline").await;
        set_status_deadline(fixture.workspace.root().to_path_buf(), Duration::ZERO);

        let result = observe(&fixture.workspace).await;
        assert!(!result.repo_available);
        assert!(!result.complete);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::Deadline]);
        assert!(result.entries.is_empty());
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_cancelled_query_stops_and_reaps_the_owned_child() {
        let fixture = plain_fixture("cancel").await;
        let started = fixture.base.join("started");
        let script = fixture.base.join("slow-git");
        // The child reports that it started and then holds its pipes open long
        // enough that only a stopped process can return the query promptly.
        executable(
            &script,
            &format!("#!/bin/sh\n: > '{}'\nsleep 3\n", started.display()),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);

        let session = CancellationToken::new();
        let request = WorkspaceStatusRequest {
            session_id: SessionId::new().unwrap(),
            max_bytes: None,
        };
        let pending = tokio::spawn({
            let workspace = Arc::clone(&fixture.workspace);
            let session = session.clone();
            async move {
                status(
                    &workspace,
                    &request,
                    &session,
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                )
                .await
            }
        });
        for _ in 0..200 {
            if started.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(started.exists(), "the query never started its child");
        session.cancel();
        let cancelled_at = Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("a cancelled query did not return")
            .expect("the query task panicked")
            .unwrap_err();
        assert!(matches!(error, AgentError::QueryLimit));
        // Returning before the script's sleep ends is only possible because the
        // owned child was stopped and reaped instead of being left behind.
        assert!(
            cancelled_at.elapsed() < Duration::from_secs(2),
            "a cancelled query did not stop its git child"
        );
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_stderr_flood_stops_the_owned_child() {
        let fixture = plain_fixture("stderr").await;
        let survived = fixture.base.join("flood-finished");
        let script = fixture.base.join("loud-git");
        // A little stdout for the work tree probe, then far more standard error
        // than the count bound allows: a query must stop the child instead of
        // discarding without bound until the deadline. The trailing marker
        // proves the child was stopped while it still had output to write.
        executable(
            &script,
            &format!(
                "#!/bin/sh\ncase \"$*\" in\n  *rev-parse*) printf '%s\\n' '{}' ;;\nesac\ndd if=/dev/zero bs=1024 count=4096 1>&2 2>/dev/null\n: > '{}'\n",
                fixture.root.display(),
                survived.display()
            ),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);

        let queried_at = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(5), observe(&fixture.workspace))
            .await
            .expect("a loud child stalled the query");
        assert!(!result.warnings.contains(&WorkspaceStatusWarning::Deadline));
        assert!(
            result
                .warnings
                .contains(&WorkspaceStatusWarning::OutputTruncated)
        );
        assert!(!result.complete);
        assert!(
            !survived.exists(),
            "a flooding child was allowed to run to completion"
        );
        assert!(queried_at.elapsed() < Duration::from_secs(5));
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn inherited_git_environment_variables_never_reach_git() {
        let fixture = repo_fixture("env").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        write_file(&fixture.root, "untracked.txt", b"x");
        let trace = fixture.base.join("git-trace.log");
        let mut env: Vec<(OsString, OsString)> = Vec::new();
        if let Some(path) = std::env::var_os("PATH") {
            env.push(("PATH".into(), path));
        }
        env.push((
            "GIT_DIR".into(),
            fixture.base.join("elsewhere").into_os_string(),
        ));
        env.push((
            "GIT_WORK_TREE".into(),
            fixture.base.clone().into_os_string(),
        ));
        env.push((
            "GIT_INDEX_FILE".into(),
            fixture.base.join("index").into_os_string(),
        ));
        env.push(("GIT_CONFIG_COUNT".into(), "1".into()));
        env.push(("GIT_CONFIG_KEY_0".into(), "core.fsmonitor".into()));
        env.push((
            "GIT_CONFIG_VALUE_0".into(),
            fixture.base.join("fsmonitor").into_os_string(),
        ));
        env.push(("GIT_TRACE".into(), trace.clone().into_os_string()));
        set_status_env(fixture.workspace.root().to_path_buf(), env);

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert_eq!(result.untracked, 1);
        assert!(result.complete);
        assert!(!trace.exists(), "a GIT_* variable reached the git child");
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_configured_fsmonitor_never_runs() {
        let fixture = repo_fixture("fsmonitor").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        write_file(&fixture.root, "untracked.txt", b"x");
        let marker = fixture.base.join("fsmonitor-ran");
        let script = fixture.base.join("fake-fsmonitor");
        executable(&script, &format!("#!/bin/sh\n: > '{}'\n", marker.display()));
        // A repository-local setting only: no global configuration is touched.
        git(
            &fixture.root,
            &["config", "core.fsmonitor", script.to_str().unwrap()],
        );
        let index = fixture.root.join(".git/index");
        let before = std::fs::read(&index).unwrap();
        let before_modified = std::fs::metadata(&index).unwrap().modified().unwrap();

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(result.complete);
        assert!(
            !marker.exists(),
            "a configured fsmonitor ran during a status query"
        );
        assert_eq!(std::fs::read(&index).unwrap(), before);
        assert_eq!(
            std::fs::metadata(&index).unwrap().modified().unwrap(),
            before_modified
        );
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn status_leaves_the_workspace_files_unchanged() {
        let fixture = repo_fixture("read-only").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        write_file(&fixture.root, "untracked.txt", b"x");
        let file = fixture.root.join("tracked.txt");
        let before = std::fs::read(&file).unwrap();
        let before_modified = std::fs::metadata(&file).unwrap().modified().unwrap();

        let _ = observe(&fixture.workspace).await;

        assert_eq!(std::fs::read(&file).unwrap(), before);
        assert_eq!(
            std::fs::metadata(&file).unwrap().modified().unwrap(),
            before_modified
        );
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_child_that_closes_its_pipes_is_still_reaped_by_the_deadline() {
        let fixture = plain_fixture("pipe-close").await;
        let started = fixture.base.join("started");
        let script = fixture.base.join("closing-git");
        // The child closes both of its pipes and keeps running. Reaching the
        // deadline is only possible because the wait for the child is under the
        // same budget as the reads.
        executable(
            &script,
            &format!(
                "#!/bin/sh\nexec >&- 2>&-\n: > '{}'\nsleep 30\n",
                started.display()
            ),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);
        set_status_deadline(
            fixture.workspace.root().to_path_buf(),
            Duration::from_millis(300),
        );

        let result = tokio::time::timeout(Duration::from_secs(5), observe(&fixture.workspace))
            .await
            .expect("a child that closed its pipes stalled the query");
        assert!(started.exists(), "the query never started its child");
        assert!(!result.repo_available);
        assert!(!result.complete);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::Deadline]);
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_chunk_that_crosses_the_stdout_ceiling_keeps_its_prefix() {
        let fixture = plain_fixture("crossing").await;
        // One write far past the ceiling: the parseable prefix must survive
        // instead of the whole chunk being discarded.
        let payload: Vec<u8> = (0..200)
            .flat_map(|index| format!("? file-{index:03}.txt\0").into_bytes())
            .collect();
        let payload_path = fixture.base.join("payload");
        std::fs::write(&payload_path, &payload).unwrap();
        let script = fixture.base.join("chatty-git");
        executable(
            &script,
            &format!(
                "#!/bin/sh\ncase \"$*\" in\n  *rev-parse*) printf '%s\\n' '{}' ;;\n  *status*) cat '{}' ;;\nesac\n",
                fixture.root.display(),
                payload_path.display()
            ),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);
        set_status_caps(
            fixture.workspace.root().to_path_buf(),
            1024,
            MAX_STATUS_RECORDS,
        );

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(!result.complete);
        assert_eq!(
            result.warnings,
            vec![WorkspaceStatusWarning::OutputTruncated]
        );
        assert!(
            result.untracked > 0,
            "the prefix of the chunk that crossed the ceiling was discarded"
        );
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_signal_death_is_a_failed_observation() {
        let fixture = plain_fixture("signalled").await;
        let script = fixture.base.join("killed-git");
        // The work tree probe answers, but the status command dies from a
        // signal: an empty answer is never reported as a clean workspace. Only
        // the standalone `status` argument dies; the fixed `-c` values that
        // contain the word status must not be mistaken for the subcommand.
        executable(
            &script,
            &format!(
                "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = status ]; then\n    kill -9 $$\n  fi\ndone\nprintf '%s\\n' '{}'\n",
                fixture.root.display()
            ),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert!(!result.complete);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_unusable_repository_root_is_a_failed_observation() {
        let fixture = plain_fixture("bad-root").await;
        let script = fixture.base.join("fake-git");
        // The probe answers with a path that is not a directory, so no porcelain
        // path could be attributed to a work tree root.
        executable(
            &script,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' '{}'\n",
                fixture.base.join("nowhere").display()
            ),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);

        let result = observe(&fixture.workspace).await;
        assert!(!result.repo_available);
        assert!(!result.complete);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_cancelled_budget_never_starts_a_process() {
        let fixture = plain_fixture("pre-cancel").await;
        let started = fixture.base.join("started");
        let script = fixture.base.join("marking-git");
        executable(
            &script,
            &format!("#!/bin/sh\n: > '{}'\n", started.display()),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);
        let session = CancellationToken::new();
        session.cancel();
        let request = WorkspaceStatusRequest {
            session_id: SessionId::new().unwrap(),
            max_bytes: None,
        };

        // Cancellation is checked before a process is started, so a cancelled
        // query cannot leave a child behind.
        assert!(matches!(
            status(
                &fixture.workspace,
                &request,
                &session,
                &CancellationToken::new(),
                &CancellationToken::new(),
            )
            .await,
            Err(AgentError::QueryLimit)
        ));
        assert!(!started.exists(), "a cancelled query started a git process");
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_expired_budget_never_starts_a_process() {
        let fixture = plain_fixture("pre-expired").await;
        let started = fixture.base.join("started");
        let script = fixture.base.join("marking-git");
        executable(
            &script,
            &format!("#!/bin/sh\n: > '{}'\n", started.display()),
        );
        set_status_program(fixture.workspace.root().to_path_buf(), script);
        set_status_deadline(fixture.workspace.root().to_path_buf(), Duration::ZERO);

        let result = observe(&fixture.workspace).await;
        assert!(!started.exists(), "an expired query started a git process");
        assert!(!result.repo_available);
        assert!(!result.complete);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::Deadline]);
        cleanup(&fixture);
    }

    #[tokio::test]
    async fn a_stream_git_could_not_have_produced_is_not_a_complete_answer() {
        let fixture = repo_fixture("stream-safety").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        let mut stream = String::new();
        stream.push_str("# branch.oid 1111111111111111111111111111111111111111\0");
        stream.push_str("# branch.head main\0");
        // Codes outside git's documented set.
        stream.push_str(
            "1 ZZ N... 100644 100644 100644 1111111111111111111111111111111111111111 1111111111111111111111111111111111111111 tracked.txt\0",
        );
        // An object id that is not a hash.
        stream.push_str("# branch.oid zzzz\0");
        // A branch name beyond the bounded header size.
        stream.push_str(&format!(
            "# branch.head {}\0",
            "a".repeat(MAX_STATUS_BRANCH_BYTES + 1)
        ));
        // A rename record whose second path chunk never arrived.
        stream.push_str(
            "2 R. N... 100644 100644 100644 1111111111111111111111111111111111111111 1111111111111111111111111111111111111111 R100 tracked.txt\0",
        );

        let found = super::observe(
            stream.as_bytes(),
            &fixture.root,
            fixture.workspace.root(),
            StatusCaps::standard(),
        );
        assert!(found.entries.is_empty(), "{:?}", found.entries);
        assert_eq!(found.malformed, 4);
        assert_eq!(found.head_oid, None);
        assert!(found.branch_headers_complete());
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn global_and_system_configuration_is_never_read() {
        let fixture = repo_fixture("global-config").await;
        write_file(&fixture.root, "tracked.txt", b"tracked\n");
        git(&fixture.root, &["add", "."]);
        git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
        write_file(&fixture.root, "untracked.txt", b"x");
        // A home directory whose configuration would both include another file
        // and start an external monitor, plus explicit system and command-line
        // overrides: none of it may reach the child or run.
        let home = fixture.base.join("home");
        std::fs::create_dir_all(home.join(".config/git")).unwrap();
        let marker = fixture.base.join("global-fsmonitor-ran");
        let monitor = fixture.base.join("global-fsmonitor");
        executable(
            &monitor,
            &format!("#!/bin/sh\n: > '{}'\n", marker.display()),
        );
        let included = fixture.base.join("included.config");
        std::fs::write(
            &included,
            format!("[core]\n\tfsmonitor = {}\n", monitor.display()),
        )
        .unwrap();
        std::fs::write(
            home.join(".gitconfig"),
            format!("[include]\n\tpath = {}\n", included.display()),
        )
        .unwrap();
        std::fs::write(
            home.join(".config/git/config"),
            format!("[core]\n\tfsmonitor = {}\n", monitor.display()),
        )
        .unwrap();
        let system = fixture.base.join("system.config");
        std::fs::write(
            &system,
            format!("[core]\n\tfsmonitor = {}\n", monitor.display()),
        )
        .unwrap();
        let mut env: Vec<(OsString, OsString)> = Vec::new();
        if let Some(path) = std::env::var_os("PATH") {
            env.push(("PATH".into(), path));
        }
        env.push(("HOME".into(), home.into_os_string()));
        env.push((
            "XDG_CONFIG_HOME".into(),
            fixture.base.join("xdg").into_os_string(),
        ));
        env.push((
            "GIT_CONFIG_GLOBAL".into(),
            fixture.base.join("override.config").into_os_string(),
        ));
        env.push(("GIT_CONFIG_SYSTEM".into(), system.into_os_string()));
        env.push(("GIT_CONFIG_COUNT".into(), "1".into()));
        env.push(("GIT_CONFIG_KEY_0".into(), "core.fsmonitor".into()));
        env.push((
            "GIT_CONFIG_VALUE_0".into(),
            monitor.clone().into_os_string(),
        ));
        set_status_env(fixture.workspace.root().to_path_buf(), env);

        let result = observe(&fixture.workspace).await;
        assert!(result.repo_available);
        assert_eq!(result.untracked, 1);
        assert!(result.complete);
        assert!(
            !marker.exists(),
            "a global configuration started an external monitor"
        );
        cleanup(&fixture);
    }

    /// A branch name long enough that reserving its room would exceed a minimal
    /// encoded budget: the answer is a status failure that fits, not an invalid
    /// budget, and the caller never sees an argument error for a valid budget.
    #[test]
    fn a_long_branch_is_omitted_instead_of_failing_the_budget() {
        let observation = Observation {
            branch: Some("b".repeat(MAX_STATUS_BRANCH_BYTES)),
            saw_oid_header: true,
            saw_head_header: true,
            ..Observation::default()
        };

        let result = assemble(true, observation, Vec::new(), true, MIN_RESULT_BYTES, 0)
            .expect("a valid budget is never an argument error");
        assert!(result.repo_available);
        assert!(!result.complete);
        assert_eq!(result.branch, None);
        assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
        assert!(encoded_len(&result).unwrap() <= MIN_RESULT_BYTES);
    }

    #[tokio::test]
    async fn every_cut_point_stays_within_the_encoded_budget() {
        let fixture = repo_fixture("budget-boundary").await;
        for index in 0..30 {
            write_file(&fixture.root, &format!("f-{index:02}.txt"), b"x");
        }
        let full = observe(&fixture.workspace).await;
        assert_eq!(full.untracked, 30);
        assert!(encoded_len(&full).unwrap() > MIN_RESULT_BYTES);

        for budget in MIN_RESULT_BYTES..MIN_RESULT_BYTES + 64 {
            let result = request(&fixture.workspace, Some(budget)).await.unwrap();
            assert!(
                encoded_len(&result).unwrap() <= budget,
                "the result exceeded its {budget} byte budget"
            );
            assert_eq!(result.untracked, 30);
            if result.entries.len() < 30 {
                assert!(!result.complete);
                assert!(
                    result
                        .warnings
                        .contains(&WorkspaceStatusWarning::OutputTruncated)
                );
            }
        }
        cleanup(&fixture);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn json_escapes_are_counted_against_the_budget() {
        let fixture = repo_fixture("budget-escapes").await;
        for index in 0..8 {
            write_file(
                &fixture.root,
                &format!("a \"quoted\" \\ backslash-{index:02}.txt"),
                b"x",
            );
        }

        for budget in MIN_RESULT_BYTES..MIN_RESULT_BYTES + 64 {
            let result = request(&fixture.workspace, Some(budget)).await.unwrap();
            assert!(
                encoded_len(&result).unwrap() <= budget,
                "an escaped entry exceeded its {budget} byte budget"
            );
            assert_eq!(result.untracked, 8);
        }
        cleanup(&fixture);
    }
}
