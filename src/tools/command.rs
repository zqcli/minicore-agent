//! Owned command workers for Bash (spec §8).
//!
//! One command has exactly one owner worker, registered under the complete
//! `ToolRef` of the call that started it. The Runtime may drop the tool future
//! at any time (turn cancellation, an outer deadline); the worker keeps the
//! child until it is stopped and reaped, so a dropped future never decides
//! cleanup. Turn completion, Session close, and shutdown are the joins that
//! decide when an owner is really gone.
//!
//! Each worker drains both pipes in parallel and hands every accepted chunk to
//! one sink, which writes the authoritative bounded window before publishing a
//! best-effort event. Offsets are raw byte offsets for both streams, and the
//! two streams are independent: no total order between them is invented.
//!
//! The controlled scope is a Unix process group or a Windows job object. A
//! `kill_on_drop` flag is a backstop only; termination is reported as confirmed
//! only when the controlled scope itself was observed to be gone, never from a
//! request to stop or from elapsed time.

use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitStatus;
use std::process::Stdio;
#[cfg(all(test, unix))]
use std::sync::OnceLock;
#[cfg(windows)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use minicore_runtime::LoopId;
#[cfg(any(test, windows))]
use minicore_runtime::ToolCallId;
use minicore_runtime::tools::ToolError;
use process_wrap::tokio::{KillOnDrop, TokioChildWrapper, TokioCommandWrap};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::tool_data::{
    CommandResult, CommandStatus, MAX_TOOL_CHUNK_BYTES, ToolDataStream, ToolRef, ToolStreamNotice,
};

use super::CommandEnvironment;

#[cfg(windows)]
static NEXT_PIPE_ID: AtomicU64 = AtomicU64::new(1);

/// Maximum concurrently registered command owners for one owner boundary. A
/// command in flight holds a process and two pipes, so this is bounded like the
/// Session tool-record store rather than exposed as user configuration.
pub(crate) const MAX_COMMAND_OWNERS: usize = 1024;

/// Short grace for a leader that exited while descendants still hold a pipe.
const LEADER_DRAIN_GRACE: Duration = Duration::from_millis(250);
/// Bounded drain after the controlled scope was stopped. A process that escaped
/// the scope is not promised, so this never becomes an open-ended wait.
const STOPPED_DRAIN_GRACE: Duration = Duration::from_millis(500);
/// Bounded probes used to confirm a Unix process group holds no process.
#[cfg(unix)]
const GROUP_CONFIRM_PROBES: u32 = 10;
#[cfg(unix)]
const GROUP_CONFIRM_INTERVAL: Duration = Duration::from_millis(20);

/// Receives accepted process bytes and process facts while a command runs.
///
/// The implementation writes the authoritative bounded window first and only
/// then publishes a best-effort event, so an event and a later `tool.output`
/// page can never disagree about the same bytes.
pub(crate) trait CommandStreamSink: Send + Sync {
    fn push_chunk(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
        chunk: &[u8],
    ) -> Option<ToolStreamNotice>;
    /// Stores the current process record. It never changes the Runtime outcome.
    fn note_command(&self, tool_ref: &ToolRef, result: &CommandResult);
    /// Marks cancellation as requested, before the terminal record.
    fn mark_cancelling(&self, tool_ref: &ToolRef);
    /// The retained range of one stream, as `(base_offset, observed_end)`.
    fn stream_range(&self, tool_ref: &ToolRef, stream: ToolDataStream) -> Option<(u64, u64)>;
    /// Marks a real end of output for one stream, never a failed read.
    fn note_stream_end(&self, tool_ref: &ToolRef, stream: ToolDataStream);
    /// Marks that the owner stopped observing one stream before a real end of
    /// output: the record is final, but bytes may be missing.
    fn note_stream_cut(&self, tool_ref: &ToolRef, stream: ToolDataStream);
}

/// Reads one captured stream. Boxed so the Windows named-pipe capture and the
/// inherited OS pipes share one owner-side type.
pub(crate) type OutputReader = Box<dyn AsyncRead + Unpin + Send>;

/// Narrow handle a tool needs to run owned commands: the registry they join
/// and the sink their bytes go to. Identity is never resolved here; the caller
/// that reached the real `Tool::execute` boundary passes the complete
/// `ToolRef` explicitly.
pub(crate) struct CommandBinding {
    owners: Arc<CommandOwners>,
    sink: Arc<dyn CommandStreamSink>,
}

impl CommandBinding {
    pub(crate) fn new(owners: Arc<CommandOwners>, sink: Arc<dyn CommandStreamSink>) -> Self {
        Self { owners, sink }
    }

    pub(crate) fn owners(&self) -> &Arc<CommandOwners> {
        &self.owners
    }

    pub(crate) fn sink(&self) -> Arc<dyn CommandStreamSink> {
        Arc::clone(&self.sink)
    }
}

/// How the child gets its pipes and how the owner reads them. The default uses
/// inherited OS pipes; Windows keeps its named-pipe capture so a grandchild
/// that inherited a write handle cannot wedge the owner's read side.
#[derive(Default)]
pub(crate) enum Capture {
    #[default]
    Inherited,
    #[cfg(windows)]
    NamedPipes(NamedPipeCapture),
}

impl Capture {
    fn configure(&mut self, command: &mut tokio::process::Command) {
        match self {
            Capture::Inherited => {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
            }
            #[cfg(windows)]
            Capture::NamedPipes(capture) => capture.configure(command),
        }
    }

    fn take_readers(
        &mut self,
        child: &mut Box<dyn TokioChildWrapper>,
    ) -> io::Result<(OutputReader, OutputReader)> {
        match self {
            Capture::Inherited => {
                let stdout = child.inner_mut().stdout.take();
                let stderr = child.inner_mut().stderr.take();
                match (stdout, stderr) {
                    (Some(stdout), Some(stderr)) => Ok((Box::new(stdout), Box::new(stderr))),
                    _ => Err(io::Error::other("captured stdio is unavailable")),
                }
            }
            #[cfg(windows)]
            Capture::NamedPipes(capture) => capture.take_readers(),
        }
    }
}

/// Windows capture through named pipes. The server side is owned here, so the
/// owner reads a pipe rather than the child's inherited handle. The server and
/// the client are stored separately: configuring the command consumes only the
/// client, and the server stays owned until the readers are taken.
#[cfg(windows)]
pub(crate) struct NamedPipeCapture {
    stdout: Option<PipeEnds>,
    stderr: Option<PipeEnds>,
}

#[cfg(windows)]
struct PipeEnds {
    server: tokio::net::windows::named_pipe::NamedPipeServer,
    client: Option<std::fs::File>,
}

#[cfg(windows)]
impl NamedPipeCapture {
    pub(crate) async fn new(tool_call_id: &ToolCallId) -> io::Result<Self> {
        let (stdout_server, stdout_client) = create_capture_pipe(tool_call_id, "stdout")?;
        let (stderr_server, stderr_client) = create_capture_pipe(tool_call_id, "stderr")?;
        tokio::try_join!(stdout_server.connect(), stderr_server.connect())?;
        Ok(Self {
            stdout: Some(PipeEnds {
                server: stdout_server,
                client: Some(stdout_client),
            }),
            stderr: Some(PipeEnds {
                server: stderr_server,
                client: Some(stderr_client),
            }),
        })
    }

    /// Consumes only the client end. The server stays owned so the readers can
    /// still be taken after the child was spawned.
    fn configure(&mut self, command: &mut tokio::process::Command) {
        if let Some(client) = self.stdout.as_mut().and_then(|ends| ends.client.take()) {
            command.stdout(Stdio::from(client));
        }
        if let Some(client) = self.stderr.as_mut().and_then(|ends| ends.client.take()) {
            command.stderr(Stdio::from(client));
        }
    }

    fn take_readers(&mut self) -> io::Result<(OutputReader, OutputReader)> {
        match (self.stdout.take(), self.stderr.take()) {
            (Some(stdout), Some(stderr)) => Ok((Box::new(stdout.server), Box::new(stderr.server))),
            _ => Err(io::Error::other("captured named pipes are unavailable")),
        }
    }
}

#[cfg(windows)]
fn create_capture_pipe(
    tool_call_id: &ToolCallId,
    stream: &str,
) -> io::Result<(
    tokio::net::windows::named_pipe::NamedPipeServer,
    std::fs::File,
)> {
    use tokio::net::windows::named_pipe::{PipeMode, ServerOptions};

    let pipe_id = NEXT_PIPE_ID.fetch_add(1, Ordering::Relaxed);
    let tool_call_id = safe_pipe_component(tool_call_id.as_str());
    let pipe_name = format!(
        r"\\.\pipe\minicore-agent-{}-{}-{pipe_id}-{stream}",
        std::process::id(),
        tool_call_id,
    );
    let server = ServerOptions::new()
        .pipe_mode(PipeMode::Byte)
        .access_outbound(false)
        .first_pipe_instance(true)
        .create(&pipe_name)?;
    let client = std::fs::OpenOptions::new().write(true).open(&pipe_name)?;
    Ok((server, client))
}

#[cfg(windows)]
fn safe_pipe_component(value: &str) -> String {
    value
        .bytes()
        .take(48)
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') {
                char::from(byte)
            } else {
                '_'
            }
        })
        .collect()
}

/// Everything one owned command needs before it starts.
pub(crate) struct CommandRequest {
    /// The complete Runtime identity, present whenever the call came from a
    /// real loop. A standalone tool without a captured identity has none, and
    /// no `ToolRef` is fabricated for it.
    pub(crate) tool_ref: Option<ToolRef>,
    /// Free-form shell text, passed to the shell as one argument.
    pub(crate) command: String,
    pub(crate) cwd: PathBuf,
    /// Earliest of the tool input timeout, the tool context, and the turn.
    pub(crate) deadline: Instant,
    pub(crate) cancellation: CancellationToken,
    pub(crate) environment: CommandEnvironment,
    /// Retained prefix per stream for the model-facing result. This is a
    /// separate, smaller budget than the streaming window.
    pub(crate) prefix_limit: usize,
    pub(crate) streams: Option<Arc<dyn CommandStreamSink>>,
    /// Platform capture for this command; Windows command owners pass their
    /// named-pipe capture, everything else uses inherited pipes.
    pub(crate) capture: Capture,
}

/// What one finished command leaves behind: the structured record plus the
/// bounded model-facing prefix of each stream.
pub(crate) struct CommandOutcome {
    pub(crate) result: CommandResult,
    /// Retained model-facing prefix of each stream, kept separately from the
    /// streaming window.
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) stdout_truncated: bool,
    pub(crate) stderr_truncated: bool,
}

/// A running command as seen by its caller. Dropping it requests cancellation
/// of that command only; the owner keeps running and is joined by its registry.
pub(crate) struct CommandWorker {
    completion: watch::Receiver<Option<Arc<CommandOutcome>>>,
    cancel: CancellationToken,
}

impl CommandWorker {
    /// Waits for the owner to finish. The runtime may drop this future; the
    /// owner is unaffected and stays registered until it really ends.
    pub(crate) async fn completion(&mut self) -> Arc<CommandOutcome> {
        loop {
            if let Some(outcome) = self.completion.borrow().clone() {
                return outcome;
            }
            if self.completion.changed().await.is_err() {
                // A registered owner always publishes; if that ever failed, the
                // command is reported as failed instead of as a clean exit.
                return Arc::new(CommandOutcome {
                    result: terminal_record(CommandStatus::Failed, None, None, false, None, None),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                });
            }
        }
    }
}

impl Drop for CommandWorker {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Owned command workers for one owner boundary: a Session or standalone
/// owner. At most one worker exists per complete `ToolRef`, and never more than
/// one for the same call.
pub(crate) struct CommandOwners {
    inner: Mutex<OwnersInner>,
}

#[derive(Default)]
struct OwnersInner {
    closing: bool,
    next_local: u64,
    workers: Vec<Arc<OwnedWorker>>,
}

struct OwnedWorker {
    key: OwnerKey,
    /// The owner's own stop request. A join barrier cancels it before it
    /// waits, so a still-running owner is stopped even when the tool future
    /// that started it was not dropped through `CommandWorker::drop`.
    cancel: CancellationToken,
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl OwnedWorker {
    fn new(key: OwnerKey, cancel: CancellationToken, handle: JoinHandle<()>) -> Self {
        Self {
            key,
            cancel,
            join: tokio::sync::Mutex::new(Some(handle)),
        }
    }

    fn is_finished(&self) -> bool {
        match self.join.try_lock() {
            Ok(slot) => slot.as_ref().is_none_or(|handle| handle.is_finished()),
            Err(_) => false,
        }
    }

    async fn join(&self) {
        let mut slot = self.join.lock().await;
        let Some(handle) = slot.as_mut() else {
            return;
        };
        let _ = Pin::new(handle).await;
        slot.take();
    }
}

/// Registry identity of one owned command. A real call is always keyed by its
/// complete `ToolRef`; only a tool executed without a Runtime identity uses the
/// process-local handle, which is never mistaken for one.
#[derive(Clone, Eq, PartialEq)]
enum OwnerKey {
    Tool(ToolRef),
    Local(u64),
}

impl CommandOwners {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(OwnersInner::default()),
        })
    }

    /// Registers and starts one owner. A duplicate identity, a closing owner,
    /// or a full registry is refused instead of being guessed around. The
    /// registry is bounded like the Session tool-record store, and finished
    /// owners are reaped on every registration so a long-lived Session cannot
    /// accumulate handles without limit. A standalone tool without a Runtime
    /// identity is still registered (under its local key), so its explicit join
    /// always reaches it.
    pub(crate) fn start(&self, request: CommandRequest) -> Result<CommandWorker, ToolError> {
        let (sender, receiver) = watch::channel(None);
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let mut inner = self.inner.lock().unwrap();
        // Reap finished handles first, then refuse rather than exceed the cap.
        inner.workers.retain(|worker| !worker.is_finished());
        if inner.workers.len() >= MAX_COMMAND_OWNERS {
            return Err(ToolError::Internal);
        }
        let key = match &request.tool_ref {
            Some(tool_ref) => OwnerKey::Tool(tool_ref.clone()),
            None => {
                inner.next_local = inner.next_local.saturating_add(1);
                OwnerKey::Local(inner.next_local)
            }
        };
        if inner.closing || inner.workers.iter().any(|worker| worker.key == key) {
            return Err(ToolError::Internal);
        }
        let handle = tokio::spawn(async move {
            let outcome = execute(request, &worker_cancel).await;
            let _ = sender.send(Some(Arc::new(outcome)));
        });
        inner
            .workers
            .push(Arc::new(OwnedWorker::new(key, cancel.clone(), handle)));
        Ok(CommandWorker {
            completion: receiver,
            cancel,
        })
    }

    /// Waits for every owner started by one loop. The owner's stop request is
    /// issued first for every matching worker, so a dropped tool future cannot
    /// leave a command behind and turn completion is a real join barrier.
    pub(crate) async fn join_loop(&self, loop_id: LoopId) {
        let workers = {
            let inner = self.inner.lock().unwrap();
            inner
                .workers
                .iter()
                .filter(|worker| {
                    matches!(
                        &worker.key,
                        OwnerKey::Tool(tool_ref) if tool_ref.loop_id == loop_id
                    )
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for worker in &workers {
            worker.cancel.cancel();
        }
        for worker in &workers {
            worker.join().await;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.workers.retain(|worker| !worker.is_finished());
    }

    /// Refuses new owners, requests every owner to stop, and waits for every
    /// owner to really exit and reap.
    pub(crate) async fn join_all(&self) {
        let workers = {
            let mut inner = self.inner.lock().unwrap();
            inner.closing = true;
            inner.workers.clone()
        };
        for worker in &workers {
            worker.cancel.cancel();
        }
        for worker in &workers {
            worker.join().await;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.workers.retain(|worker| !worker.is_finished());
    }

    /// Test-only evidence that no owner is still running.
    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .workers
            .iter()
            .filter(|worker| !worker.is_finished())
            .count()
    }

    #[cfg(test)]
    pub(crate) fn add_held_worker(
        &self,
        loop_id: LoopId,
    ) -> (CancellationToken, tokio::sync::oneshot::Sender<()>) {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let cancel = CancellationToken::new();
        let tool_ref = crate::tool_data::ToolRef {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            loop_id,
            request_index: 0,
            tool_call_id: ToolCallId::new(format!("held-{loop_id}")).unwrap(),
        };
        let handle = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let mut inner = self.inner.lock().unwrap();
        inner.workers.push(Arc::new(OwnedWorker::new(
            OwnerKey::Tool(tool_ref),
            cancel.clone(),
            handle,
        )));
        (cancel, release_tx)
    }
}

/// Per-stream accumulator for the model-facing prefix. It is deliberately
/// small: the streaming window lives in the sink and both budgets are counted
/// separately.
#[derive(Default)]
struct StreamState {
    prefix: Vec<u8>,
    truncated: bool,
    complete: bool,
    /// The owner stopped observing before a real end of output.
    cut: bool,
}

type StreamHandle = Arc<Mutex<StreamState>>;

async fn execute(mut request: CommandRequest, cancel: &CancellationToken) -> CommandOutcome {
    let stdout_state: StreamHandle = Arc::new(Mutex::new(StreamState::default()));
    let stderr_state: StreamHandle = Arc::new(Mutex::new(StreamState::default()));
    // A task that was already cancelled or timed out while queued must not run
    // its side effects at all. This is checked before any capture or spawn: no
    // process is started, and the record honestly says nothing ran.
    if request.cancellation.is_cancelled() || cancel.is_cancelled() {
        let result = never_started_record(CommandStatus::Cancelled);
        return finish(&request, result, &stdout_state, &stderr_state);
    }
    if Instant::now() >= request.deadline {
        let result = never_started_record(CommandStatus::TimedOut);
        return finish(&request, result, &stdout_state, &stderr_state);
    }
    let mut capture = std::mem::take(&mut request.capture);
    let mut child = match spawn_child(&request, &mut capture) {
        Ok(child) => child,
        Err(_) => {
            // No process token was ever obtained, so no controlled scope was
            // observed and nothing can be confirmed about one.
            let result = terminal_record(CommandStatus::SpawnFailed, None, None, false, None, None);
            return finish(&request, result, &stdout_state, &stderr_state);
        }
    };
    let leader = child.inner_mut().id();
    let Ok((mut stdout, mut stderr)) = capture.take_readers(&mut child) else {
        let result = fail_owned(&request, &mut child, leader, &stdout_state, &stderr_state).await;
        return finish(&request, result, &stdout_state, &stderr_state);
    };
    // The process is really owned and drained from here on; publish that fact
    // before the first byte, so `tool.read` can distinguish a running command
    // from a call that has not started one.
    announce_start(&request);

    let mut drains: Pin<Box<dyn Future<Output = ()> + Send + '_>> = {
        let tool_ref = request.tool_ref.clone();
        let sink = request.streams.clone();
        let limit = request.prefix_limit;
        let stdout_state = Arc::clone(&stdout_state);
        let stderr_state = Arc::clone(&stderr_state);
        Box::pin(async move {
            tokio::join!(
                drain_stream(
                    &mut stdout,
                    sink.clone(),
                    tool_ref.clone(),
                    ToolDataStream::Stdout,
                    stdout_state,
                    limit,
                ),
                drain_stream(
                    &mut stderr,
                    sink,
                    tool_ref.clone(),
                    ToolDataStream::Stderr,
                    stderr_state,
                    limit,
                ),
            );
        })
    };

    let mut cancel_requested = false;
    let mut deadline_expired = false;
    let mut leader_status: Option<io::Result<ExitStatus>> = None;
    // A completed `drains` future must never be polled again (an async block
    // panics when resumed after completion), so completion is tracked instead
    // of being re-checked by polling the boxed future.
    let mut drains_done = false;
    tokio::select! {
        biased;
        _ = request.cancellation.cancelled() => {
            // The Runtime cancels this token for both a user cancel and a
            // deadline, so the deadline instant decides which one happened.
            if Instant::now() >= request.deadline {
                deadline_expired = true;
            } else {
                cancel_requested = true;
            }
        }
        // The owner's own stop token fires when the tool future is dropped or a
        // join barrier asks for it.
        _ = cancel.cancelled() => {
            if !request.cancellation.is_cancelled() && Instant::now() >= request.deadline {
                deadline_expired = true;
            } else if !request.cancellation.is_cancelled() {
                cancel_requested = true;
            }
        }
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(request.deadline)) => {
            deadline_expired = true;
        }
        status = child.inner_mut().wait() => leader_status = Some(status),
        _ = &mut drains => drains_done = true,
    }

    let mut stopped_scope = false;
    // Set when the drain was cut short although the leader had already exited:
    // output that a stopped descendant could still have written is reported as
    // incomplete instead of as a clean end of output.
    let mut pipes_cut = false;
    if cancel_requested || deadline_expired {
        announce_stop(&request, cancel_requested);
        stop_scope(&mut child, leader);
        stopped_scope = true;
    } else if leader_status.is_none() {
        // The pipes closed first, but the leader still owns its exit: wait for
        // it under the same budget instead of waiting without one.
        let waited = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => None,
            _ = cancel.cancelled() => None,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(request.deadline)) => None,
            status = child.inner_mut().wait() => Some(status),
        };
        match waited {
            Some(status) => leader_status = Some(status),
            None => {
                // The same deadline-instant rule as the main wait: an owner
                // stop after the deadline is a timeout, not a cancellation.
                if Instant::now() >= request.deadline {
                    deadline_expired = true;
                } else {
                    cancel_requested = true;
                }
                announce_stop(&request, cancel_requested);
                stop_scope(&mut child, leader);
                stopped_scope = true;
            }
        }
    } else {
        // The leader exited while descendants may still hold its pipes: drain
        // under a short grace that the remaining deadline can only shorten,
        // then stop the controlled scope rather than waiting without a bound.
        // The grace is shortened by the remaining budget: when the deadline cut
        // the drain, that is a timeout; when the grace itself ran out, the
        // leader really exited inside its budget and only the descendant's
        // output is missing.
        let remaining = remaining_until(request.deadline);
        let deadline_cut = remaining <= LEADER_DRAIN_GRACE;
        let grace = LEADER_DRAIN_GRACE.min(remaining);
        if !drain_within(&mut drains, grace, cancel, &mut drains_done).await {
            if deadline_cut || Instant::now() >= request.deadline {
                deadline_expired = true;
            } else if request.cancellation.is_cancelled() || cancel.is_cancelled() {
                cancel_requested = true;
                announce_stop(&request, true);
            }
            stop_scope(&mut child, leader);
            stopped_scope = true;
            pipes_cut = true;
        }
    }

    if stopped_scope {
        // The pipes end when the controlled scope is gone; a process that
        // escaped the scope is not promised, so this stays bounded. A bounded
        // final drain is still attempted: the stop token is normally already
        // cancelled here, and giving up on the buffers immediately would lose
        // bytes a stopped process already wrote.
        pipes_cut |= !drain_final(&mut drains, STOPPED_DRAIN_GRACE, &mut drains_done).await;
    }
    // A leader can exit and leave group/job members behind even when they hold
    // no pipe (for example `sleep 60 >/dev/null &`). Those members must be gone
    // before the owner finishes; only a process that actively left the scope is
    // outside the promise.
    if leader_status.is_some() && !stopped_scope && !scope_gone(leader).await {
        stop_scope(&mut child, leader);
        stopped_scope = true;
        pipes_cut |= !drain_final(&mut drains, STOPPED_DRAIN_GRACE, &mut drains_done).await;
    }

    // The one full reap; its future is never dropped mid-flight.
    let waited = wrapper_wait(&mut child, leader).await;
    let status = match waited {
        Ok(status) => Some(status),
        Err(_) => match &leader_status {
            Some(Ok(status)) => Some(*status),
            _ => None,
        },
    };
    let exit_code = status.as_ref().and_then(|status| status.code());
    let signal = status.as_ref().and_then(exit_signal);
    #[cfg(unix)]
    let termination_confirmed = waited.is_ok() && confirmed_group_gone(leader).await;
    #[cfg(windows)]
    let termination_confirmed = {
        // The job-object wait returns only once the job holds no process.
        let _ = leader;
        waited.is_ok()
    };

    let command_status = if cancel_requested {
        CommandStatus::Cancelled
    } else if deadline_expired {
        CommandStatus::TimedOut
    } else if status.is_none() {
        CommandStatus::Failed
    } else {
        CommandStatus::Exited
    };
    // A read abandoned by a stop is not an end of output: the streams are
    // reported cut so `eof` becomes true without claiming the bytes were all
    // observed.
    if stopped_scope || pipes_cut || !streams_closed(&stdout_state, &stderr_state) {
        note_cut(&request, &stdout_state, &stderr_state, pipes_cut);
    }

    let (stdout_range, stderr_range) = stream_ranges(&request);
    let result = terminal_record(
        command_status,
        exit_code,
        signal,
        termination_confirmed,
        stdout_range,
        stderr_range,
    );
    let result = with_output_flags(
        result,
        &stdout_state.lock().unwrap(),
        &stderr_state.lock().unwrap(),
        pipes_cut,
    );
    finish(&request, result, &stdout_state, &stderr_state)
}

/// Stores the terminal record, then takes the model-facing prefixes. The
/// stored record is written before this returns, so a later query and the tool
/// result describe the same command.
fn finish(
    request: &CommandRequest,
    result: CommandResult,
    stdout: &StreamHandle,
    stderr: &StreamHandle,
) -> CommandOutcome {
    record(request, &result);
    let stdout_truncated = {
        let state = stdout.lock().unwrap();
        state.truncated || state.cut
    };
    let stderr_truncated = {
        let state = stderr.lock().unwrap();
        state.truncated || state.cut
    };
    CommandOutcome {
        result,
        stdout: take_prefix(stdout),
        stderr: take_prefix(stderr),
        stdout_truncated,
        stderr_truncated,
    }
}

/// The earliest remaining budget, never negative.
fn remaining_until(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Records the requested stop before the terminal record is available. Only a
/// cancellation is recorded: a deadline is not a cancellation request, and its
/// terminal `timed_out` record follows within the same owner.
fn announce_start(request: &CommandRequest) {
    let (Some(sink), Some(tool_ref)) = (&request.streams, &request.tool_ref) else {
        return;
    };
    let (stdout_range, stderr_range) = stream_ranges(request);
    let (stdout_base, stdout_end) = stdout_range.unwrap_or((0, 0));
    let (stderr_base, stderr_end) = stderr_range.unwrap_or((0, 0));
    sink.note_command(
        tool_ref,
        &CommandResult {
            status: CommandStatus::Running,
            exit_code: None,
            signal: None,
            termination_confirmed: false,
            stdout_base_offset: stdout_base,
            stdout_observed_end: stdout_end,
            stderr_base_offset: stderr_base,
            stderr_observed_end: stderr_end,
            output_complete: false,
            output_truncated: false,
        },
    );
}

/// Records the requested stop before the terminal record is available.
fn announce_stop(request: &CommandRequest, cancel_requested: bool) {
    if !cancel_requested {
        return;
    }
    let (Some(sink), Some(tool_ref)) = (&request.streams, &request.tool_ref) else {
        return;
    };
    sink.mark_cancelling(tool_ref);
    let (stdout_range, stderr_range) = stream_ranges(request);
    let (stdout_base, stdout_end) = stdout_range.unwrap_or((0, 0));
    let (stderr_base, stderr_end) = stderr_range.unwrap_or((0, 0));
    sink.note_command(
        tool_ref,
        &CommandResult {
            status: CommandStatus::Cancelling,
            exit_code: None,
            signal: None,
            termination_confirmed: false,
            stdout_base_offset: stdout_base,
            stdout_observed_end: stdout_end,
            stderr_base_offset: stderr_base,
            stderr_observed_end: stderr_end,
            output_complete: false,
            output_truncated: false,
        },
    );
}

/// Spawns the shell command inside the controlled scope. A Windows job object
/// is created before the process resumes, so there is no escape window.
fn spawn_child(
    request: &CommandRequest,
    capture: &mut Capture,
) -> io::Result<Box<dyn TokioChildWrapper>> {
    let mut command = shell_command(&request.command);
    request.environment.apply(&mut command);
    command.current_dir(&request.cwd).stdin(Stdio::null());
    capture.configure(&mut command);
    let mut wrapped = TokioCommandWrap::from(command);
    #[cfg(unix)]
    wrapped.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    wrapped.wrap(process_wrap::tokio::JobObject);
    // A backstop only, never the evidence that cleanup finished.
    wrapped.wrap(KillOnDrop);
    wrapped.spawn()
}

#[cfg(unix)]
fn shell_command(command: &str) -> tokio::process::Command {
    let mut process = tokio::process::Command::new("/bin/sh");
    process.arg("-lc").arg(command);
    process
}

#[cfg(windows)]
fn shell_command(command: &str) -> tokio::process::Command {
    let mut process = tokio::process::Command::new("powershell.exe");
    process
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-Command")
        .arg(command);
    process
}

/// Reads one pipe to a real end of output. Every accepted chunk goes to the
/// authoritative window first, then to the best-effort event.
async fn drain_stream<R>(
    reader: &mut R,
    sink: Option<Arc<dyn CommandStreamSink>>,
    tool_ref: Option<ToolRef>,
    stream: ToolDataStream,
    state: StreamHandle,
    limit: usize,
) where
    R: AsyncRead + Unpin + Send,
{
    let mut buffer = vec![0u8; MAX_TOOL_CHUNK_BYTES];
    // Only a real EOF is an end of output. A read error stops the loop but
    // leaves the stream incomplete, so a later page reports it as cut instead
    // of claiming the bytes ended.
    let mut ended = false;
    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(0) => {
                ended = true;
                if let (Some(sink), Some(tool_ref)) = (&sink, &tool_ref) {
                    sink.note_stream_end(tool_ref, stream);
                }
                break;
            }
            Ok(read) => read,
            Err(_) => break,
        };
        let chunk = &buffer[..read];
        if let (Some(sink), Some(tool_ref)) = (&sink, &tool_ref) {
            sink.push_chunk(tool_ref, stream, chunk);
        }
        let mut state = state.lock().unwrap();
        let remaining = limit.saturating_sub(state.prefix.len());
        let keep = remaining.min(chunk.len());
        state.prefix.extend_from_slice(&chunk[..keep]);
        if keep < chunk.len() {
            state.truncated = true;
        }
    }
    if ended {
        state.lock().unwrap().complete = true;
    }
}

/// Awaits the drains for at most `grace`. True only for a real end of output.
/// `done` records that the boxed future already completed, so it is never
/// polled a second time.
async fn drain_within(
    drains: &mut Pin<Box<dyn Future<Output = ()> + Send + '_>>,
    grace: Duration,
    cancel: &CancellationToken,
    done: &mut bool,
) -> bool {
    if *done {
        return true;
    }
    let completed = tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(grace) => false,
        _ = &mut *drains => true,
    };
    if completed {
        *done = true;
    }
    completed
}

/// The final drain after the controlled scope was stopped. It does not observe
/// a cancel token: the owner's stop token is normally already cancelled at this
/// point, and abandoning the buffers immediately would lose the bytes a killed
/// process already wrote. It stays bounded, so a pipe held by a process that
/// escaped the scope still cannot hang the owner.
async fn drain_final(
    drains: &mut Pin<Box<dyn Future<Output = ()> + Send + '_>>,
    grace: Duration,
    done: &mut bool,
) -> bool {
    if *done {
        return true;
    }
    let completed = tokio::select! {
        biased;
        _ = tokio::time::sleep(grace) => false,
        _ = &mut *drains => true,
    };
    if completed {
        *done = true;
    }
    completed
}

/// Joins the leader through the wrapper, which is where the process group or
/// job object is reaped. This is the one full reap the owner must always
/// perform; its future is never dropped mid-flight. The injected test failure
/// still runs the real wait and reports an error afterwards, so the process is
/// reaped while termination stays honestly unconfirmed.
async fn wrapper_wait(
    child: &mut Box<dyn TokioChildWrapper>,
    _leader: Option<u32>,
) -> io::Result<ExitStatus> {
    let result = Box::into_pin(child.wait()).await;
    #[cfg(all(test, unix))]
    if take_termination_failure(_leader, TerminationFailure::Wait) {
        let _ = &result;
        return Err(io::Error::other("injected wait failure"));
    }
    result
}

/// Asks the wrapper to stop the leader. A failure is returned rather than
/// discarded, so the caller can fall back instead of skipping cleanup.
fn start_kill(child: &mut Box<dyn TokioChildWrapper>, _leader: Option<u32>) -> io::Result<()> {
    #[cfg(all(test, unix))]
    if take_termination_failure(_leader, TerminationFailure::StartKill) {
        return Err(io::Error::other("injected start_kill failure"));
    }
    child.start_kill()
}

/// Test-only injection of a stop/reap failure, keyed by leader pid. The owner
/// then reports an explicit failure instead of claiming termination.
#[cfg(all(test, unix))]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum TerminationFailure {
    StartKill,
    Wait,
}

#[cfg(all(test, unix))]
static TERMINATION_FAILURES: OnceLock<Mutex<Vec<(u32, TerminationFailure)>>> = OnceLock::new();

#[cfg(all(test, unix))]
pub(crate) fn inject_termination_failure(pid: u32, failure: TerminationFailure) {
    TERMINATION_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((pid, failure));
}

#[cfg(all(test, unix))]
fn take_termination_failure(pid: Option<u32>, failure: TerminationFailure) -> bool {
    let Some(pid) = pid else {
        return false;
    };
    let mut failures = TERMINATION_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    let Some(position) = failures
        .iter()
        .position(|candidate| *candidate == (pid, failure))
    else {
        return false;
    };
    failures.remove(position);
    true
}

/// Stops the controlled scope: the leader first, then the whole group/job so no
/// descendant survives the owner. Every step is attempted even when an earlier
/// one failed, and a failed wrapper stop falls back to killing the leader
/// directly, so a stop error never skips real cleanup. The returned facts are
/// unknown-capable: a caller must not report a confirmed termination from a
/// request that may not have reached the OS.
fn stop_scope(child: &mut Box<dyn TokioChildWrapper>, leader: Option<u32>) {
    let leader_stopped = start_kill(child, leader).is_ok();
    #[cfg(unix)]
    if let Some(pid) = leader {
        let _ = signal_group(pid, nix::sys::signal::Signal::SIGKILL);
    }
    #[cfg(windows)]
    let _ = leader;
    if !leader_stopped {
        // The wrapper stop failed; still stop the leader directly.
        let _ = child.inner_mut().start_kill();
    }
}

/// One bounded observation of the controlled scope. `true` means every member
/// was observed gone. On Windows the job cannot be probed cheaply, so the scope
/// is treated as live and stopped; the wrapper's own wait then confirms the end.
/// Unix retries a few times so a leader that just exited is not mistaken for a
/// live group, and a still-live group is never reported as gone.
async fn scope_gone(leader: Option<u32>) -> bool {
    #[cfg(unix)]
    {
        let Some(pid) = leader else {
            return true;
        };
        for _ in 0..GROUP_CONFIRM_PROBES {
            if matches!(probe_group(pid), Ok(true)) {
                return true;
            }
            tokio::time::sleep(GROUP_CONFIRM_INTERVAL).await;
        }
        matches!(probe_group(pid), Ok(true))
    }
    #[cfg(windows)]
    {
        let _ = leader;
        false
    }
}

/// A command whose own handling failed: the process record says so instead of
/// claiming a clean exit.
async fn fail_owned(
    request: &CommandRequest,
    child: &mut Box<dyn TokioChildWrapper>,
    leader: Option<u32>,
    stdout: &StreamHandle,
    stderr: &StreamHandle,
) -> CommandResult {
    stop_scope(child, leader);
    let _ = wrapper_wait(child, leader).await;
    // A command whose own handling failed has no honest end of output: the
    // streams are final but cut, so a page never reports them as pending. The
    // caller records the terminal record once through `finish`.
    note_cut(request, stdout, stderr, true);
    let (stdout_range, stderr_range) = stream_ranges(request);
    let result = terminal_record(
        CommandStatus::Failed,
        None,
        None,
        false,
        stdout_range,
        stderr_range,
    );
    with_output_flags(
        result,
        &stdout.lock().unwrap(),
        &stderr.lock().unwrap(),
        true,
    )
}

fn with_output_flags(
    mut result: CommandResult,
    stdout: &StreamState,
    stderr: &StreamState,
    pipes_cut: bool,
) -> CommandResult {
    result.output_complete =
        !pipes_cut && !stdout.cut && !stderr.cut && stdout.complete && stderr.complete;
    result.output_truncated = !result.output_complete;
    result
}

fn take_prefix(state: &StreamHandle) -> Vec<u8> {
    let mut state = state.lock().unwrap();
    std::mem::take(&mut state.prefix)
}

/// True only when both streams reached a real end of output.
fn streams_closed(stdout: &StreamHandle, stderr: &StreamHandle) -> bool {
    stdout.lock().unwrap().complete && stderr.lock().unwrap().complete
}

/// Marks streams whose observation ended without a real end of output, through
/// the same sink that owns the window. Such a stream is final (`eof` becomes
/// true) and truncated, never a clean end. `force` marks every stream when the
/// drain was cut short even if the pipes closed later. The decision is made
/// under the state lock but the sink is called without it, so no lock ordering
/// between the stream state and the shared store is ever established.
fn note_cut(request: &CommandRequest, stdout: &StreamHandle, stderr: &StreamHandle, force: bool) {
    let (Some(sink), Some(tool_ref)) = (&request.streams, &request.tool_ref) else {
        return;
    };
    let mut cut = Vec::new();
    for (state, stream) in [
        (stdout, ToolDataStream::Stdout),
        (stderr, ToolDataStream::Stderr),
    ] {
        let mut state = state.lock().unwrap();
        if !state.complete || force {
            state.cut = true;
            cut.push(stream);
        }
    }
    for stream in cut {
        sink.note_stream_cut(tool_ref, stream);
    }
}

/// A record for a command that never owned a process: it was cancelled or its
/// deadline passed while the task was still queued. No stream was observed and
/// nothing was lost, so both flags stay false while the terminal command record
/// makes the streams report `unavailable` (never observed) rather than pending.
fn never_started_record(status: CommandStatus) -> CommandResult {
    debug_assert!(status.is_terminal());
    terminal_record(status, None, None, false, None, None)
}

fn terminal_record(
    status: CommandStatus,
    exit_code: Option<i32>,
    signal: Option<i32>,
    termination_confirmed: bool,
    stdout_range: Option<(u64, u64)>,
    stderr_range: Option<(u64, u64)>,
) -> CommandResult {
    let (stdout_base, stdout_end) = stdout_range.unwrap_or((0, 0));
    let (stderr_base, stderr_end) = stderr_range.unwrap_or((0, 0));
    CommandResult {
        status,
        exit_code,
        signal,
        termination_confirmed,
        stdout_base_offset: stdout_base,
        stdout_observed_end: stdout_end,
        stderr_base_offset: stderr_base,
        stderr_observed_end: stderr_end,
        output_complete: false,
        output_truncated: false,
    }
}

/// Stores the process record through the same sink that owns the windows.
fn record(request: &CommandRequest, result: &CommandResult) {
    if let (Some(sink), Some(tool_ref)) = (&request.streams, &request.tool_ref) {
        sink.note_command(tool_ref, result);
    }
}

type StreamRange = Option<(u64, u64)>;

fn stream_ranges(request: &CommandRequest) -> (StreamRange, StreamRange) {
    let (Some(sink), Some(tool_ref)) = (&request.streams, &request.tool_ref) else {
        return (None, None);
    };
    (
        sink.stream_range(tool_ref, ToolDataStream::Stdout),
        sink.stream_range(tool_ref, ToolDataStream::Stderr),
    )
}

/// Confirms the Unix process group holds no process. `ESRCH` is the only
/// confirmation; any other error stays unknown instead of being reported as a
/// termination. A group that still exists is a backgrounded descendant outside
/// the promised sandbox, not an unconfirmed leader.
#[cfg(unix)]
async fn confirmed_group_gone(leader: Option<u32>) -> bool {
    let Some(pid) = leader else {
        return false;
    };
    for _ in 0..GROUP_CONFIRM_PROBES {
        match probe_group(pid) {
            Ok(true) => return true,
            Ok(false) => tokio::time::sleep(GROUP_CONFIRM_INTERVAL).await,
            Err(_) => return false,
        }
    }
    matches!(probe_group(pid), Ok(true))
}

/// Observes whether a process group still exists without sending a signal.
#[cfg(unix)]
fn probe_group(pid: u32) -> Result<bool, nix::errno::Errno> {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    match killpg(Pid::from_raw(pid as i32), None) {
        Ok(()) => Ok(false),
        Err(nix::errno::Errno::ESRCH) => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: nix::sys::signal::Signal) -> Result<(), nix::errno::Errno> {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    killpg(Pid::from_raw(pid as i32), signal)
}

#[cfg(unix)]
fn exit_signal(status: &ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(windows)]
fn exit_signal(_status: &ExitStatus) -> Option<i32> {
    None
}

#[cfg(test)]
mod tests;
