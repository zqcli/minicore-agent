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

use crate::workspace::page::{DEFAULT_RESULT_BYTES, now_unix_ms, validate_result_bytes};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::budget::encoded_len;
use crate::error::AgentError;
use crate::ids::SessionId;
use crate::workspace::Workspace;

use crate::workspace::scan::WorkspaceScanConsistency;

mod git;
mod porcelain;
#[cfg(all(test, unix))]
pub(crate) use git::STATUS_REAP_GATES;
pub(crate) use git::status_deadline_at;
use git::{
    GitOutput, GitRun, MAX_STATUS_STDOUT_BYTES, os_args, repository_root, run_git, status_caps,
    status_env, status_program,
};
#[cfg(test)]
pub(crate) use git::{STATUS_CAPS, STATUS_DEADLINES, STATUS_ENVS, STATUS_PROGRAMS};
#[cfg(test)]
use porcelain::{MAX_STATUS_BRANCH_BYTES, map_entry};
use porcelain::{MAX_STATUS_RECORDS, Observation, observe, workspace_pathspec};

mod diff;
pub(crate) use diff::diff_sources;
#[cfg(test)]
mod diff_tests;

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
        validate_result_bytes(self.max_bytes)?;
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
    let unavailable = |warnings, complete| {
        assemble(
            false,
            Observation::default(),
            warnings,
            complete,
            max_bytes,
            observed_at_unix_ms,
        )
    };
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
    if let Some(warning) = failure_warning(&probe) {
        push_warning(&mut warnings, warning);
        return unavailable(warnings, false);
    }
    let toplevel = if probe.succeeded() {
        repository_root(&probe.stdout).await
    } else {
        None
    };
    if probe.succeeded() && toplevel.is_none() {
        // Git answered, but the answer is not a usable work tree root.
        push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        return unavailable(warnings, false);
    }
    let Some(toplevel) = toplevel else {
        // Without a work tree root, only a definite answer from git may be
        // reported as a complete `repo_available: false`. A refusal such as a
        // dubious ownership check, a corrupt configuration, or a permission
        // problem stays an unknown failure.
        let classify = run_git(&run, &os_args(&["rev-parse", "--is-bare-repository"])).await?;
        if let Some(warning) = failure_warning(&classify) {
            push_warning(&mut warnings, warning);
        } else if classify.succeeded()
            && matches!(
                String::from_utf8_lossy(&classify.stdout).trim(),
                "true" | "false"
            )
        {
            // `true` is a bare repository, `false` means the workspace is
            // inside a git directory but not in a work tree. Both are definite
            // answers with no status to report.
            return unavailable(warnings, true);
        } else {
            push_warning(&mut warnings, WorkspaceStatusWarning::StatusFailed);
        }
        return unavailable(warnings, false);
    };
    let Some(pathspec) = workspace_pathspec(workspace.root(), &toplevel) else {
        // The work tree root cannot be related to the workspace, so no
        // porcelain path could be attributed safely.
        return unavailable(vec![WorkspaceStatusWarning::StatusFailed], false);
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

fn failure_warning(output: &GitOutput) -> Option<WorkspaceStatusWarning> {
    if output.spawn_failed {
        Some(WorkspaceStatusWarning::GitUnavailable)
    } else if output.timed_out {
        Some(WorkspaceStatusWarning::Deadline)
    } else if output.truncated {
        Some(WorkspaceStatusWarning::OutputTruncated)
    } else if output.io_failed {
        Some(WorkspaceStatusWarning::StatusFailed)
    } else {
        None
    }
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
    let envelope = encoded_len(&envelope);
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
        let encoded = encoded_len(&entry);
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

#[cfg(test)]
pub(crate) mod tests;
