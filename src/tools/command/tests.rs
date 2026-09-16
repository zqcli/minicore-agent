use std::path::Path;

use tokio::sync::Notify;

use super::*;
use crate::ids::SessionId;
use crate::tool_data::ToolData;

/// Sink that records what an owner published, so tests can assert the
/// structured facts without a Session.
struct RecordingSink {
    data: Arc<ToolData>,
    chunks: Mutex<Vec<(ToolDataStream, u64, u64)>>,
    commands: Mutex<Vec<CommandResult>>,
    notify: Notify,
}

impl RecordingSink {
    fn new(data: Arc<ToolData>) -> Arc<Self> {
        Arc::new(Self {
            data,
            chunks: Mutex::new(Vec::new()),
            commands: Mutex::new(Vec::new()),
            notify: Notify::new(),
        })
    }

    #[cfg(unix)]
    async fn wait_for_chunk(&self) {
        for _ in 0..100 {
            if !self.chunks.lock().unwrap().is_empty() {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_millis(20), self.notify.notified()).await;
        }
        panic!("no accepted chunk was published");
    }

    fn commands(&self) -> Vec<CommandResult> {
        self.commands.lock().unwrap().clone()
    }
}

impl CommandStreamSink for RecordingSink {
    fn push_chunk(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
        chunk: &[u8],
    ) -> Option<ToolStreamNotice> {
        let notice = self.data.note_stream_chunk(tool_ref, stream, chunk)?;
        self.chunks
            .lock()
            .unwrap()
            .push((stream, notice.base_offset, notice.next_offset));
        self.notify.notify_waiters();
        Some(notice)
    }

    fn note_command(&self, tool_ref: &ToolRef, result: &CommandResult) {
        let _ = self.data.note_command(tool_ref, result.clone());
        self.commands.lock().unwrap().push(result.clone());
    }

    fn mark_cancelling(&self, tool_ref: &ToolRef) {
        self.data.mark_cancelling(tool_ref);
    }

    fn stream_range(&self, tool_ref: &ToolRef, stream: ToolDataStream) -> Option<(u64, u64)> {
        self.data.stream_range(tool_ref, stream)
    }

    fn note_stream_end(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        self.data.note_stream_end(tool_ref, stream);
    }

    fn note_stream_cut(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        self.data.note_stream_cut(tool_ref, stream);
    }
}

fn session() -> SessionId {
    let mut bytes = [0_u8; 16];
    bytes[15] = 7;
    format!(
        "ses_{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .unwrap()
}

fn loop_id() -> LoopId {
    let mut bytes = [0_u8; 16];
    bytes[15] = 9;
    format!(
        "lup_{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .unwrap()
}

fn tool_ref(call: &str) -> ToolRef {
    ToolRef {
        session_id: session(),
        loop_id: loop_id(),
        request_index: 1,
        tool_call_id: ToolCallId::new(call).unwrap(),
    }
}

fn fixture(label: &str) -> std::path::PathBuf {
    let root =
        std::env::temp_dir().join(format!("minicore-command-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn request(
    dir: &Path,
    call: &str,
    command: &str,
    deadline_ms: u64,
    sink: Option<Arc<dyn CommandStreamSink>>,
) -> CommandRequest {
    CommandRequest {
        tool_ref: Some(tool_ref(call)),
        command: command.to_owned(),
        cwd: dir.to_path_buf(),
        deadline: Instant::now() + Duration::from_millis(deadline_ms),
        cancellation: CancellationToken::new(),
        environment: CommandEnvironment::new(std::iter::empty::<std::ffi::OsString>()),
        prefix_limit: 4096,
        streams: sink,
        capture: Capture::Inherited,
    }
}

#[cfg(unix)]
async fn read_pid(path: &Path) -> u32 {
    for _ in 0..200 {
        if let Ok(value) = tokio::fs::read_to_string(path).await {
            if let Ok(pid) = value.trim().parse() {
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("fixture process did not publish its PID");
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -0 {pid} 2>/dev/null"))
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(unix)]
async fn wait_for_exit(pid: u32) {
    for _ in 0..300 {
        if !process_exists(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -KILL {pid} 2>/dev/null"))
        .status();
    panic!("process {pid} outlived its owner");
}

#[tokio::test]
async fn a_second_call_with_the_same_identity_is_refused() {
    let owners = CommandOwners::new();
    let dir = fixture("duplicate-identity");
    let sink = RecordingSink::new(Arc::new(ToolData::new()));
    let data: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let _first = owners
        .start(request(
            &dir,
            "call-1",
            "exit 0",
            5_000,
            Some(Arc::clone(&data)),
        ))
        .unwrap();
    assert!(
        owners
            .start(request(&dir, "call-1", "exit 0", 5_000, Some(data)))
            .is_err()
    );
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_chunk_is_stored_before_it_is_observed_and_while_the_command_runs() {
    let owners = CommandOwners::new();
    let dir = fixture("live-chunk");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-live");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-live",
        "printf 'one'; sleep 0.3; printf 'two' >&2",
        10_000,
        Some(streams),
    );
    let mut worker = owners.start(request).unwrap();
    sink.wait_for_chunk().await;
    // The window is already authoritative while the command still runs.
    let (base, end) = data
        .stream_range(&tool, ToolDataStream::Stdout)
        .expect("a retained stdout window");
    assert_eq!(base, 0);
    assert!(end > 0);
    // No terminal record exists yet: the bytes are visible before the exit,
    // and the running process fact was published before the first byte.
    assert!(
        !sink
            .commands()
            .iter()
            .any(|command| command.status.is_terminal())
    );
    assert_eq!(
        sink.commands().first().map(|command| command.status),
        Some(CommandStatus::Running)
    );
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Exited);
    assert_eq!(outcome.result.exit_code, Some(0));
    assert_eq!(
        data.stream_range(&tool, ToolDataStream::Stdout),
        Some((0, 3))
    );
    assert_eq!(
        data.stream_range(&tool, ToolDataStream::Stderr),
        Some((0, 3))
    );
    assert!(outcome.result.output_complete);
    assert!(!outcome.result.output_truncated);
    assert_eq!(
        sink.commands().last().map(|c| c.status),
        Some(CommandStatus::Exited)
    );
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_records_cancelling_then_a_confirmed_group_end() {
    let owners = CommandOwners::new();
    let dir = fixture("cancel-owner");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-cancel");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-cancel",
        "echo $$ > child.pid; exec sleep 30",
        30_000,
        Some(streams),
    );
    let cancellation = request.cancellation.clone();
    let mut worker = owners.start(request).unwrap();
    let pid = read_pid(&dir.join("child.pid")).await;
    assert!(process_exists(pid));
    cancellation.cancel();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Cancelled);
    let statuses = sink
        .commands()
        .iter()
        .map(|command| command.status)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            CommandStatus::Running,
            CommandStatus::Cancelling,
            CommandStatus::Cancelled
        ]
    );
    assert!(outcome.result.termination_confirmed);
    wait_for_exit(pid).await;
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn an_injected_reap_failure_never_claims_confirmed_termination() {
    let owners = CommandOwners::new();
    let dir = fixture("reap-failure");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-reap");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-reap",
        "echo $$ > child.pid; exec sleep 30",
        30_000,
        Some(streams),
    );
    let cancellation = request.cancellation.clone();
    let mut worker = owners.start(request).unwrap();
    let pid = read_pid(&dir.join("child.pid")).await;
    inject_termination_failure(pid, TerminationFailure::Wait);
    cancellation.cancel();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Cancelled);
    assert!(
        !outcome.result.termination_confirmed,
        "a failed reap was reported as confirmed termination"
    );
    wait_for_exit(pid).await;
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_pipe_holding_grandchild_is_stopped_after_a_bounded_grace() {
    let owners = CommandOwners::new();
    let dir = fixture("held-pipe");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-pipe");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-pipe",
        "sleep 30 & echo $! > grandchild.pid; exit 0",
        30_000,
        Some(streams),
    );
    let started = Instant::now();
    let mut worker = owners.start(request).unwrap();
    let grandchild = read_pid(&dir.join("grandchild.pid")).await;
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker.completion())
        .await
        .expect("the owner waited for a descendant to close its pipe");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(outcome.result.status, CommandStatus::Exited);
    assert_eq!(outcome.result.exit_code, Some(0));
    assert!(
        !outcome.result.output_complete,
        "output from a stopped descendant was reported as complete"
    );
    wait_for_exit(grandchild).await;
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn the_window_keeps_the_tail_with_raw_offsets() {
    let owners = CommandOwners::new();
    let dir = fixture("tail-window");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-tail");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-tail",
        "head -c 1200000 /dev/zero | tr '\\0' 'x'",
        30_000,
        Some(streams),
    );
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Exited);
    let (base, end) = data
        .stream_range(&tool, ToolDataStream::Stdout)
        .expect("a retained stdout window");
    assert_eq!(end, 1_200_000);
    assert!(base > 0, "the window kept more than its bound");
    assert!(base < end);
    // Reading from an offset that was dropped reports the loss instead of
    // inventing content, and the retained tail stays readable by offset.
    let missing = data
        .output(
            &crate::tool_data::ToolOutputRequest {
                tool_ref: tool.clone(),
                stream: ToolDataStream::Stdout,
                offset: base - 1,
                max_bytes: None,
            },
            64 * 1024,
        )
        .unwrap();
    assert!(missing.truncated);
    let tail = data
        .output(
            &crate::tool_data::ToolOutputRequest {
                tool_ref: tool,
                stream: ToolDataStream::Stdout,
                offset: base,
                max_bytes: None,
            },
            64 * 1024,
        )
        .unwrap();
    assert_eq!(tail.base_offset, base);
    assert!(!tail.data.is_empty());
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_queued_owner_that_was_already_cancelled_never_runs_its_script() {
    let owners = CommandOwners::new();
    let dir = fixture("queued-cancel");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-queued");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-queued",
        "echo ran > side-effect.txt",
        30_000,
        Some(streams),
    );
    // The call is already cancelled before the worker gets to run.
    request.cancellation.cancel();
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Cancelled);
    // Windows reports the job termination code while Unix reports a
    // signal with no exit code. Neither may be presented as success 0.
    assert_ne!(outcome.result.exit_code, Some(0));
    assert!(
        !dir.join("side-effect.txt").exists(),
        "a cancelled queued command still ran its side effects"
    );
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_queued_owner_past_its_deadline_never_runs_its_script() {
    let owners = CommandOwners::new();
    let dir = fixture("queued-deadline");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-queued-late");
    data.note_requested(&tool, "bash");
    let mut request = request(
        &dir,
        "call-queued-late",
        "echo ran > side-effect.txt",
        0,
        Some(streams),
    );
    // The deadline already passed while the task was queued.
    request.deadline = Instant::now() - Duration::from_millis(1);
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::TimedOut);
    assert!(!dir.join("side-effect.txt").exists());
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_background_group_member_is_stopped_before_the_turn_join_returns() {
    let owners = CommandOwners::new();
    let dir = fixture("background-group");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-bg");
    data.note_requested(&tool, "bash");
    // The leader exits normally and the background member keeps no pipe and
    // writes nothing: only the group is left alive. It must still be gone
    // before the loop join returns, otherwise a turn completes while
    // `sleep 60` keeps running in the Session's process group.
    let request = request(
        &dir,
        "call-bg",
        "sleep 60 >/dev/null 2>&1 & echo $! > bg.pid; exit 0",
        30_000,
        Some(streams),
    );
    let loop_key = request.tool_ref.as_ref().unwrap().loop_id;
    let mut worker = owners.start(request).unwrap();
    let background = read_pid(&dir.join("bg.pid")).await;
    assert!(process_exists(background));
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Exited);
    assert_eq!(outcome.result.exit_code, Some(0));
    // Linux reports an empty group as ESRCH. Other Unix kernels can return
    // a less specific result after cleanup, which remains honestly
    // unconfirmed even though the PID observation below proves this member
    // is gone.
    #[cfg(target_os = "linux")]
    assert!(outcome.result.termination_confirmed);
    assert!(
        !process_exists(background),
        "a background group member outlived its owner"
    );
    owners.join_loop(loop_key).await;
    assert_eq!(owners.active(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn a_loop_join_leaves_another_loops_owner_running() {
    let owners = CommandOwners::new();
    let dir = fixture("loop-identity");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let first = tool_ref("call-first");
    let second = ToolRef {
        loop_id: LoopId::new().unwrap(),
        ..first.clone()
    };
    data.note_requested(&first, "bash");
    data.note_requested(&second, "bash");
    let mut first_request = request(
        &dir,
        "call-first",
        "echo $$ > first.pid; exec sleep 30",
        30_000,
        Some(Arc::clone(&streams)),
    );
    first_request.tool_ref = Some(first.clone());
    let mut second_request = request(
        &dir,
        "call-second",
        "echo $$ > second.pid; exec sleep 30",
        30_000,
        Some(streams),
    );
    second_request.tool_ref = Some(second.clone());
    let mut first_worker = owners.start(first_request).unwrap();
    let mut second_worker = owners.start(second_request).unwrap();
    let first_pid = read_pid(&dir.join("first.pid")).await;
    let second_pid = read_pid(&dir.join("second.pid")).await;

    // Joining one loop must not stop or reap the other loop's command.
    owners.join_loop(first.loop_id).await;
    assert_eq!(
        first_worker.completion().await.result.status,
        CommandStatus::Cancelled
    );
    wait_for_exit(first_pid).await;
    assert!(
        process_exists(second_pid),
        "joining one loop stopped another loop's command"
    );
    assert_eq!(owners.active(), 1);

    owners.join_all().await;
    assert_eq!(
        second_worker.completion().await.result.status,
        CommandStatus::Cancelled
    );
    wait_for_exit(second_pid).await;
    assert_eq!(owners.active(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn a_turn_join_stops_and_reaps_a_command_whose_future_was_dropped() {
    let owners = CommandOwners::new();
    let dir = fixture("join-dropped-future");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-join");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-join",
        "echo $$ > child.pid; exec sleep 30",
        30_000,
        Some(streams),
    );
    let loop_key = request.tool_ref.as_ref().unwrap().loop_id;
    let worker = owners.start(request).unwrap();
    let pid = read_pid(&dir.join("child.pid")).await;
    assert!(process_exists(pid));
    // Dropping the worker requests cancellation, but the owner must still
    // reap; the loop join is the barrier that proves it.
    drop(worker);
    owners.join_loop(loop_key).await;
    assert_eq!(owners.active(), 0);
    wait_for_exit(pid).await;
    let outcome = sink
        .commands()
        .last()
        .cloned()
        .expect("the owner published a terminal record");
    assert!(outcome.status.is_terminal());
    assert_eq!(outcome.status, CommandStatus::Cancelled);
}

#[cfg(unix)]
#[tokio::test]
async fn a_joined_owner_reaps_both_the_leader_and_its_group_member() {
    let owners = CommandOwners::new();
    let dir = fixture("join-group");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-group");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-group",
        "sleep 30 & echo $! > grandchild.pid; echo $$ > child.pid; wait",
        30_000,
        Some(streams),
    );
    let cancellation = request.cancellation.clone();
    let mut worker = owners.start(request).unwrap();
    let leader = read_pid(&dir.join("child.pid")).await;
    let grandchild = read_pid(&dir.join("grandchild.pid")).await;
    assert!(process_exists(leader));
    assert!(process_exists(grandchild));
    cancellation.cancel();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Cancelled);
    // Both members of the controlled scope are really gone, not just the
    // leader whose exit the wrapper caches.
    wait_for_exit(leader).await;
    wait_for_exit(grandchild).await;
    owners.join_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_drain_cut_before_the_scope_ends_reports_truncated_eof_pages() {
    let owners = CommandOwners::new();
    let dir = fixture("cut-stream");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-cut");
    data.note_requested(&tool, "bash");
    // The leader exits while a descendant keeps the pipes open: the drain is
    // cut short by the bounded grace and the stopped scope, so the output
    // must be reported incomplete rather than as a clean end.
    let request = request(
        &dir,
        "call-cut",
        "printf 'partial'; sleep 30 & echo $! > grandchild.pid; exit 0",
        30_000,
        Some(streams),
    );
    let mut worker = owners.start(request).unwrap();
    let grandchild = read_pid(&dir.join("grandchild.pid")).await;
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker.completion())
        .await
        .expect("the owner waited for a descendant to close its pipe");
    assert!(!outcome.result.output_complete);
    // A cut stream is final and reports its loss instead of staying open.
    let page = data
        .output(
            &crate::tool_data::ToolOutputRequest {
                tool_ref: tool.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: None,
            },
            64 * 1024,
        )
        .unwrap();
    assert!(page.eof);
    assert!(page.truncated);
    assert_eq!(
        page.availability,
        crate::tool_data::ToolDataAvailability::Partial
    );
    wait_for_exit(grandchild).await;
    owners.join_all().await;
}

#[tokio::test]
async fn a_deadline_cut_is_recorded_as_timed_out_not_cancelled() {
    let owners = CommandOwners::new();
    let dir = fixture("deadline-class");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-deadline");
    data.note_requested(&tool, "bash");
    // The command's own deadline is reached while it sleeps; no user
    // cancellation is ever requested for this token.
    let request = request(&dir, "call-deadline", "exec sleep 30", 150, Some(streams));
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert_eq!(
        outcome.result.status,
        CommandStatus::TimedOut,
        "a reached deadline must not be reported as a cancellation"
    );
    assert_ne!(outcome.result.exit_code, Some(0));
    assert_eq!(
        sink.commands().last().map(|command| command.status),
        Some(CommandStatus::TimedOut)
    );
    owners.join_all().await;
}

#[tokio::test]
async fn a_zero_byte_stream_that_really_ended_is_available_not_pending() {
    let owners = CommandOwners::new();
    let dir = fixture("empty-stream");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-empty");
    data.note_requested(&tool, "bash");
    let request = request(&dir, "call-empty", "exit 0", 5_000, Some(streams));
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert!(outcome.result.output_complete);
    for stream in [ToolDataStream::Stdout, ToolDataStream::Stderr] {
        let page = data
            .output(
                &crate::tool_data::ToolOutputRequest {
                    tool_ref: tool.clone(),
                    stream,
                    offset: 0,
                    max_bytes: None,
                },
                64 * 1024,
            )
            .unwrap();
        assert!(page.eof);
        assert!(!page.truncated);
        assert_eq!(
            page.availability,
            crate::tool_data::ToolDataAvailability::Available
        );
        assert!(page.data.is_empty());
    }
    owners.join_all().await;
}

#[tokio::test]
async fn a_spawn_failure_never_claims_a_confirmed_termination() {
    let owners = CommandOwners::new();
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-no-confirm");
    data.note_requested(&tool, "bash");
    let request = request(
        Path::new("/minicore-agent-missing-directory"),
        "call-no-confirm",
        "exit 0",
        2_000,
        Some(streams),
    );
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::SpawnFailed);
    assert!(
        !outcome.result.termination_confirmed,
        "a spawn failure has no controlled scope to confirm"
    );
}

#[tokio::test]
async fn a_spawn_failure_is_reported_without_a_process() {
    let owners = CommandOwners::new();
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-missing");
    data.note_requested(&tool, "bash");
    let request = request(
        Path::new("/minicore-agent-missing-directory"),
        "call-missing",
        "exit 0",
        2_000,
        Some(streams),
    );
    let mut worker = owners.start(request).unwrap();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::SpawnFailed);
    assert!(outcome.result.exit_code.is_none());
    assert_eq!(sink.commands().len(), 1);
    // The call is terminal from the owner's point of view, but the tool
    // record only becomes terminal when the Runtime report is reconciled;
    // until then its streams stay pending rather than claiming an end.
    for stream in [ToolDataStream::Stdout, ToolDataStream::Stderr] {
        let page = data
            .output(
                &crate::tool_data::ToolOutputRequest {
                    tool_ref: tool.clone(),
                    stream,
                    offset: 0,
                    max_bytes: None,
                },
                64 * 1024,
            )
            .unwrap();
        assert!(page.data.is_empty());
    }
}

#[cfg(windows)]
#[tokio::test]
async fn windows_cancellation_reports_a_confirmed_job_end() {
    let owners = CommandOwners::new();
    let dir = fixture("windows-cancel");
    let data = Arc::new(ToolData::new());
    let sink = RecordingSink::new(Arc::clone(&data));
    let streams: Arc<dyn CommandStreamSink> = Arc::clone(&sink) as Arc<dyn CommandStreamSink>;
    let tool = tool_ref("call-windows");
    data.note_requested(&tool, "bash");
    let request = request(
        &dir,
        "call-windows",
        "Start-Sleep -Seconds 30",
        30_000,
        Some(streams),
    );
    let cancellation = request.cancellation.clone();
    let mut worker = owners.start(request).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancellation.cancel();
    let outcome = worker.completion().await;
    assert_eq!(outcome.result.status, CommandStatus::Cancelled);
    assert!(
        outcome.result.termination_confirmed,
        "the job object has to be joined before termination is reported"
    );
    owners.join_all().await;
}

/// The named-pipe capture must keep the server end after configuring the
/// command and hand it to the owner as the reader; consuming the whole pair
/// during `configure` would drop the server and make every Windows capture
/// read nothing.
#[cfg(windows)]
#[tokio::test]
async fn windows_capture_keeps_the_server_end_as_the_reader() {
    let tool = tool_ref("call-pipe-capture");
    let mut capture = NamedPipeCapture::new(&tool.tool_call_id)
        .await
        .expect("the named pipes are created");
    // Configure consumes only the client ends and leaves the servers owned.
    let mut command = tokio::process::Command::new("cmd.exe");
    capture.configure(&mut command);
    // The server ends are still owned, so the readers can be taken.
    let readers = capture
        .take_readers()
        .expect("the server ends must survive configure");
    drop(readers);
}

#[tokio::test]
async fn join_future_drop_preserves_handle_for_subsequent_join() {
    let owners = CommandOwners::new();
    let loop_key = LoopId::new().unwrap();
    let (_cancel, release) = owners.add_held_worker(loop_key);
    assert_eq!(owners.active(), 1);

    // First join begins and is polled once, then dropped before the worker finishes.
    let mut first_join = Box::pin(owners.join_loop(loop_key));
    let finished = tokio::select! {
        _ = &mut first_join => true,
        _ = tokio::time::sleep(Duration::from_millis(20)) => false,
    };
    assert!(!finished, "join 1 must not complete while worker is held");
    drop(first_join);

    // The worker remains tracked in the registry with its handle preserved.
    assert_eq!(owners.active(), 1);

    // A second join is started; it must continue to wait rather than returning early.
    let mut second_join = Box::pin(owners.join_loop(loop_key));
    let finished2 = tokio::select! {
        _ = &mut second_join => true,
        _ = tokio::time::sleep(Duration::from_millis(20)) => false,
    };
    assert!(!finished2, "join 2 must wait for the held worker");

    // Release the worker; the second join completes and leaves the registry empty.
    let _ = release.send(());
    second_join.await;
    assert_eq!(owners.active(), 0);
}

#[tokio::test]
async fn concurrent_joins_both_wait_for_worker_completion_and_leave_registry_empty() {
    let owners = CommandOwners::new();
    let loop_key = LoopId::new().unwrap();
    let (_cancel, release) = owners.add_held_worker(loop_key);
    assert_eq!(owners.active(), 1);

    let owners_a = Arc::clone(&owners);
    let join_a = tokio::spawn(async move {
        owners_a.join_loop(loop_key).await;
    });
    let owners_b = Arc::clone(&owners);
    let join_b = tokio::spawn(async move {
        owners_b.join_loop(loop_key).await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!join_a.is_finished(), "join A must wait for worker");
    assert!(!join_b.is_finished(), "join B must wait for worker");

    // Release the worker: both joins finish, neither earlier than release.
    let _ = release.send(());
    join_a.await.unwrap();
    join_b.await.unwrap();
    assert_eq!(owners.active(), 0);
}

#[tokio::test]
async fn unfinished_held_worker_occupies_registry_capacity() {
    let owners = CommandOwners::new();
    let loop_key = LoopId::new().unwrap();
    let (_cancel, release) = owners.add_held_worker(loop_key);
    assert_eq!(owners.active(), 1);

    // Duplicate registration for same identity is refused.
    let dir = fixture("held-worker-cap");
    let tool_ref = crate::tool_data::ToolRef {
        session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
        loop_id: loop_key,
        request_index: 0,
        tool_call_id: ToolCallId::new(format!("held-{loop_key}")).unwrap(),
    };
    let mut req = request(&dir, "held-cap", "exit 0", 5000, None);
    req.tool_ref = Some(tool_ref);
    assert!(owners.start(req).is_err());

    let _ = release.send(());
    owners.join_all().await;
    assert_eq!(owners.active(), 0);
}
