//! Fixed Git commands, bounded capture, cancellation and child reaping.

use super::StatusCaps;
use crate::error::AgentError;
use crate::workspace::scan::READ_CHUNK_BYTES;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Wall-clock budget for one workspace status query, matching the other
/// workspace queries. It is checked between bounded operations, so one blocking
/// read on a stalled remote filesystem can outlast it.
pub(crate) const WORKSPACE_STATUS_DEADLINE: Duration = Duration::from_secs(10);
/// Captured stdout. Further output stops the child, so a single query never
/// buffers more than this.
pub(super) const MAX_STATUS_STDOUT_BYTES: usize = 1024 * 1024;
/// Standard error is counted, never captured. Past this many bytes the child is
/// stopped: a git flooding diagnostics is not answering the query, and letting
/// it run to the deadline would discard without bound.
const MAX_STATUS_STDERR_BYTES: u64 = 64 * 1024;
/// Read size for the stderr drain loop.
const STATUS_STDERR_CHUNK_BYTES: usize = 8192;
/// Captured result of one git invocation. Standard error is counted and
/// discarded, so no git diagnostic can reach a response, a log, or an error.
pub(super) struct GitOutput {
    pub(super) stdout: Vec<u8>,
    pub(super) exit_code: Option<i32>,
    pub(super) spawn_failed: bool,
    /// A stdout or stderr bound stopped the child before it finished.
    pub(super) truncated: bool,
    /// The deadline passed; the child was stopped.
    pub(super) timed_out: bool,
    /// Reading a pipe or waiting for the child failed. The observation is
    /// incomplete even though no bound cut it short.
    pub(super) io_failed: bool,
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

    pub(super) fn succeeded(&self) -> bool {
        self.exit_code == Some(0)
    }

    /// True when the child did not answer with success and no bound or
    /// cancellation explains it. A signal death has no exit code and counts as
    /// a failure, never as an empty answer.
    pub(super) fn quietly_failed(&self) -> bool {
        !self.spawn_failed && !self.timed_out && !self.truncated && !self.succeeded()
    }
}

/// Everything one git invocation needs besides its arguments.
pub(super) struct GitRun<'a> {
    pub(super) program: &'a OsStr,
    pub(super) root: &'a Path,
    pub(super) env: &'a [(OsString, OsString)],
    pub(super) caps: StatusCaps,
    pub(super) deadline: Instant,
    pub(super) session_cancellation: &'a CancellationToken,
    pub(super) shutdown_cancellation: &'a CancellationToken,
    pub(super) child_cancellation: &'a CancellationToken,
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

pub(super) async fn repository_root(stdout: &[u8]) -> Option<PathBuf> {
    let text = std::str::from_utf8(stdout).ok()?;
    let path = text.strip_suffix('\n').unwrap_or(text);
    if path.is_empty() || path.contains('\n') {
        return None;
    }
    // The work tree root must be a real directory that can be canonicalized
    // and compared with the canonical workspace root (for example `/tmp`
    // against `/private/tmp`). An answer that fails that check is not a root
    // every porcelain path can be attributed to.
    let canonical = tokio::fs::canonicalize(path).await.ok()?;
    tokio::fs::metadata(&canonical)
        .await
        .ok()?
        .is_dir()
        .then_some(canonical)
}

pub(super) fn os_args(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}

/// Runs one git command with fixed, shell-free arguments. A cancelled or
/// already-expired query never starts a process. The child is owned by this
/// future: cancellation, the deadline, a stdout or stderr bound, and every wait
/// stop and reap it, and the wait after its pipes close is under the same
/// budget. `kill_on_drop` stays only as a last-resort backstop while the
/// runtime is being torn down; it is not what reaps a query.
pub(super) async fn run_git(run: &GitRun<'_>, args: &[OsString]) -> Result<GitOutput, AgentError> {
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
        .args([
            "-c",
            "core.fsmonitor=false",
            "-c",
            "submodule.recurse=false",
            "-c",
            "color.ui=false",
            "-c",
            "core.quotePath=false",
            "-c",
            "diff.renames=true",
            "-c",
            "status.renames=true",
            "-c",
            "status.relativePaths=false",
        ])
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
        _ = run.session_cancellation.cancelled() => Some(Stop::Cancelled),
        _ = run.shutdown_cancellation.cancelled() => Some(Stop::Cancelled),
        _ = run.child_cancellation.cancelled() => Some(Stop::Cancelled),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(run.deadline)) => {
            timed_out = true;
            Some(Stop::Expired)
        }
        _ = stdout_bound.notified() => Some(Stop::Bounded),
        _ = stderr_bound.notified() => Some(Stop::Bounded),
        _ = futures_util::future::join(capture, drain) => None,
    };
    let mut truncated =
        flags.stdout_capped.load(Ordering::Acquire) || flags.stderr_capped.load(Ordering::Acquire);
    let mut io_failed = flags.io_failed.load(Ordering::Acquire);
    let mut exit_code = None;
    let waited = match waited {
        Some(stop) => stop,
        None => tokio::select! {
                biased;
                _ = run.session_cancellation.cancelled() => Stop::Cancelled,
                _ = run.shutdown_cancellation.cancelled() => Stop::Cancelled,
                _ = run.child_cancellation.cancelled() => Stop::Cancelled,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(run.deadline)) => {
                    Stop::Expired
                }
                status = child.wait() => {
                    match status {
                        Ok(status) => exit_code = status.code(),
                        Err(_) => io_failed = true,
                    }
                    Stop::Reaped
                }
        },
    };
    match waited {
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
    /// The query was cancelled.
    Cancelled,
    /// The deadline passed.
    Expired,
    /// A stdout or stderr bound was reached.
    Bounded,
    /// The child exited and was reaped.
    Reaped,
}

/// Stops the child and waits for a bounded reap grace. A timeout is an
/// unconfirmed termination, never evidence that the child exited.
async fn stop_child(child: &mut tokio::process::Child, run: &GitRun<'_>) -> bool {
    let _ = run;
    // A Unix test may hold this point to observe the owner while its child is
    // still unreaped; the hold never appears outside Unix tests.
    #[cfg(all(test, unix))]
    if let Some(gate) = STATUS_REAP_GATES.take(&run.root.to_path_buf()) {
        gate.pass().await;
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
    match tokio::time::timeout(crate::workspace::WORKSPACE_CANCEL_GRACE, child.wait()).await {
        Ok(result) => result.is_ok(),
        Err(_) => {
            tracing::warn!("detaching workspace status child: reap exceeded cancellation grace");
            false
        }
    }
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
pub(crate) static STATUS_CAPS: crate::test_support::Registry<PathBuf, StatusCaps> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static STATUS_DEADLINES: crate::test_support::Registry<PathBuf, Duration> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static STATUS_PROGRAMS: crate::test_support::Registry<PathBuf, OsString> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static STATUS_ENVS: crate::test_support::Registry<PathBuf, Vec<(OsString, OsString)>> =
    crate::test_support::Registry::new();
#[cfg(all(test, unix))]
pub(crate) static STATUS_REAP_GATES: crate::test_support::Registry<
    PathBuf,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

pub(super) fn status_caps(root: &Path) -> StatusCaps {
    #[cfg(test)]
    if let Some(caps) = STATUS_CAPS.get(&root.to_path_buf()) {
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
    if let Some(deadline) = STATUS_DEADLINES.get(&root.to_path_buf()) {
        return deadline;
    }
    let _ = root;
    WORKSPACE_STATUS_DEADLINE
}

pub(super) fn status_program(root: &Path) -> OsString {
    #[cfg(test)]
    if let Some(program) = STATUS_PROGRAMS.get(&root.to_path_buf()) {
        return program;
    }
    let _ = root;
    OsString::from("git")
}

/// The environment a git child receives: every inherited `GIT_*` name is
/// dropped, so no `GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_CONFIG_*`,
/// or `GIT_TRACE*` value can redirect or instrument the query. Values are
/// filtered by name and never recorded.
pub(super) fn status_env(root: &Path) -> Vec<(OsString, OsString)> {
    let inherited: Vec<(OsString, OsString)> = std::env::vars_os().collect();
    #[cfg(test)]
    let inherited = STATUS_ENVS.get(&root.to_path_buf()).unwrap_or(inherited);
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
