use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::Mutex;
use std::time::{Duration, Instant};

use minicore_runtime::ToolCallId;
use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use super::*;

#[test]
fn omission_counts_raw_bytes_even_when_invalid_utf8_expands() {
    let capture = StreamCapture {
        bytes: vec![0xff; 100],
        truncated: false,
        omitted: 0,
    };
    let output = format_stream(&capture, TRUNCATION_OVERHEAD + 12);
    assert!(output.contains("96 bytes omitted"));
    assert_eq!(output.matches('\u{fffd}').count(), 4);
}

#[test]
fn model_output_shares_the_budget_and_keeps_head_and_tail() {
    let empty = StreamCapture {
        bytes: Vec::new(),
        truncated: false,
        omitted: 0,
    };
    let small = StreamCapture {
        bytes: vec![b'x'; FORMATTED_OUTPUT_LIMIT * 3 / 4],
        truncated: false,
        omitted: 0,
    };
    let output = format_output(Some(0), &small, &empty);
    assert!(!output.contains(TRUNCATED));
    assert_eq!(
        output.bytes().filter(|byte| *byte == b'x').count(),
        small.bytes.len() + 1
    );
    let large = StreamCapture {
        bytes: format!("HEAD{}TAIL", "x".repeat(FORMATTED_OUTPUT_LIMIT * 2)).into_bytes(),
        truncated: false,
        omitted: 0,
    };
    let output = format_output(Some(0), &large, &empty);
    assert!(output.contains("HEAD"));
    assert!(output.contains("TAIL"));
    assert!(output.contains("bytes omitted"));
    assert!(output.len() <= FORMATTED_OUTPUT_LIMIT);
}
#[cfg(unix)]
use crate::tools::command::tests::TerminationFailure;

const TEST_MODEL_KEY: &str = "MINICORE_TEST_MODEL_KEY";
const TEST_MODEL_SECRET: &str = "MINICORE-TEST-MODEL-SECRET";
const TEST_PUBLIC_KEY: &str = "MINICORE_TEST_PUBLIC";
const TEST_PUBLIC_VALUE: &str = "minicore-test-public-value";

/// Records the identity and bytes an owned command published, so a test can
/// assert which `ToolRef` a bound tool used without a Session.
#[cfg(unix)]
struct IdentitySink {
    data: Arc<crate::tool_data::ToolData>,
    owned: Mutex<Vec<crate::tool_data::ToolRef>>,
}

#[cfg(unix)]
impl IdentitySink {
    fn new(data: Arc<crate::tool_data::ToolData>) -> Arc<Self> {
        Arc::new(Self {
            data,
            owned: Mutex::new(Vec::new()),
        })
    }

    fn first_owned(&self) -> Option<crate::tool_data::ToolRef> {
        self.owned.lock().unwrap().first().cloned()
    }
}

#[cfg(unix)]
impl crate::tools::command::CommandStreamSink for IdentitySink {
    fn push_chunk(
        &self,
        tool_ref: &crate::tool_data::ToolRef,
        stream: crate::tool_data::ToolDataStream,
        chunk: &[u8],
    ) -> Option<crate::tool_data::ToolStreamNotice> {
        self.owned.lock().unwrap().push(tool_ref.clone());
        self.data.note_stream_chunk(tool_ref, stream, chunk)
    }

    fn note_command(
        &self,
        tool_ref: &crate::tool_data::ToolRef,
        result: &crate::tool_data::CommandResult,
    ) {
        self.owned.lock().unwrap().push(tool_ref.clone());
        let _ = self.data.note_command(tool_ref, result.clone());
    }

    fn mark_cancelling(&self, tool_ref: &crate::tool_data::ToolRef) {
        self.data.mark_cancelling(tool_ref);
    }

    fn stream_range(
        &self,
        tool_ref: &crate::tool_data::ToolRef,
        stream: crate::tool_data::ToolDataStream,
    ) -> Option<(u64, u64)> {
        self.data.stream_range(tool_ref, stream)
    }

    fn note_stream_end(
        &self,
        tool_ref: &crate::tool_data::ToolRef,
        stream: crate::tool_data::ToolDataStream,
    ) {
        self.data.note_stream_end(tool_ref, stream);
    }

    fn note_stream_cut(
        &self,
        tool_ref: &crate::tool_data::ToolRef,
        stream: crate::tool_data::ToolDataStream,
    ) {
        self.data.note_stream_cut(tool_ref, stream);
    }
}

fn empty_command_environment() -> CommandEnvironment {
    CommandEnvironment::new(std::iter::empty::<OsString>())
}

#[cfg(unix)]
fn environment_test_command(name: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.arg("-lc").arg(format!("printf '%s' \"${name}\""));
    command
}

#[cfg(windows)]
fn environment_test_command(name: &str) -> Command {
    let mut command = Command::new("powershell.exe");
    command
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-Command")
        .arg(format!("[Console]::Out.Write([string]$env:{name})"));
    command
}

async fn command_stdout(command: &mut Command) -> String {
    let output = command.output().await.unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn command_environment_filters_sorts_and_deduplicates_removed_names() {
    let environment = CommandEnvironment::new([
        OsString::from("MINICORE_ZETA_MODEL_KEY"),
        OsString::new(),
        OsString::from("MINICORE_ALPHA_MODEL_KEY"),
        OsString::from("MINICORE_ZETA_MODEL_KEY"),
        OsString::from("MINICORE_ALPHA_MODEL_KEY"),
    ]);
    let expected = [
        OsString::from("MINICORE_ALPHA_MODEL_KEY"),
        OsString::from("MINICORE_ZETA_MODEL_KEY"),
    ];

    assert_eq!(environment.removed.as_ref(), expected.as_slice());
}

#[cfg(unix)]
#[tokio::test]
async fn unix_command_environment_removes_configured_credentials() {
    let environment = CommandEnvironment::new([OsString::from(TEST_MODEL_KEY)]);
    let mut command = environment_test_command(TEST_MODEL_KEY);
    command.env(TEST_MODEL_KEY, TEST_MODEL_SECRET);
    environment.apply(&mut command);

    let stdout = command_stdout(&mut command).await;
    assert!(stdout.is_empty());
    assert!(!stdout.contains(TEST_MODEL_SECRET));
}

#[cfg(windows)]
#[tokio::test]
async fn windows_command_environment_removes_configured_credentials() {
    let environment = CommandEnvironment::new([OsString::from(TEST_MODEL_KEY)]);
    let mut command = environment_test_command(TEST_MODEL_KEY);
    command.env(TEST_MODEL_KEY, TEST_MODEL_SECRET);
    environment.apply(&mut command);

    let stdout = command_stdout(&mut command).await;
    assert!(stdout.is_empty());
    assert!(!stdout.contains(TEST_MODEL_SECRET));
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn command_environment_preserves_unremoved_public_environment() {
    let environment = CommandEnvironment::new([OsString::from(TEST_MODEL_KEY)]);
    let mut command = environment_test_command(TEST_PUBLIC_KEY);
    command.env(TEST_PUBLIC_KEY, TEST_PUBLIC_VALUE);
    environment.apply(&mut command);

    assert_eq!(command_stdout(&mut command).await, TEST_PUBLIC_VALUE);
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn command_environment_sets_the_agent_marker() {
    let environment = CommandEnvironment::new(std::iter::empty::<OsString>());
    let mut command = environment_test_command("MINICORE_AGENT");
    environment.apply(&mut command);

    assert_eq!(command_stdout(&mut command).await, "1");
}

async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, BashTool) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-bash-tool-{label}-{}",
        crate::ids::SessionId::new().unwrap()
    ));
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root).await.unwrap());
    let tool = BashTool::new(Arc::clone(&workspace), empty_command_environment());
    (base, workspace, tool)
}

fn invocation(arguments: Value) -> ToolInvocation {
    ToolInvocation {
        tool_call_id: ToolCallId::new("bash-call").unwrap(),
        tool_name: TOOL_NAME.parse().unwrap(),
        arguments,
    }
}

fn context(cancellation: CancellationToken, deadline: Instant) -> ToolContext {
    ToolContext {
        cancellation,
        deadline,
        progress: ToolProgressSink::default(),
    }
}

async fn execute(tool: &BashTool, arguments: Value) -> Result<String, ToolError> {
    match tool
        .execute(
            invocation(arguments),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await?
    {
        ToolExecutionOutcome::Completed(output) => Ok(output.content().as_str().to_owned()),
        ToolExecutionOutcome::RequestInput(_) => panic!("bash must not request input"),
    }
}

#[cfg(unix)]
async fn execute_with_context(
    tool: &BashTool,
    arguments: Value,
    tool_context: ToolContext,
) -> Result<String, ToolError> {
    match tool.execute(invocation(arguments), tool_context).await? {
        ToolExecutionOutcome::Completed(output) => Ok(output.content().as_str().to_owned()),
        ToolExecutionOutcome::RequestInput(_) => panic!("bash must not request input"),
    }
}

async fn cleanup(base: &Path) {
    let _ = tokio::fs::remove_dir_all(base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn schema_is_strict_and_success_has_stable_labels() {
    let (base, _, tool) = fixture("success").await;
    let schema = tool.spec().input_schema();
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["command"]));
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["timeout_seconds"]["minimum"], 1);
    assert_eq!(
        schema["properties"]["timeout_seconds"]["maximum"],
        MAX_COMMAND_TIMEOUT
    );

    assert_eq!(
        execute(&tool, json!({"command": "printf hello"}))
            .await
            .unwrap(),
        "exit_code: 0\nstdout:\nhello\nstderr:\n"
    );
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn nonzero_exit_stdout_and_stderr_are_a_completed_outcome() {
    let (base, _, tool) = fixture("nonzero").await;
    assert_eq!(
        execute(
            &tool,
            json!({"command": "printf out; printf err >&2; exit 7"})
        )
        .await
        .unwrap(),
        "exit_code: 7\nstdout:\nout\nstderr:\nerr"
    );
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn cwd_and_agent_environment_are_set_inside_the_workspace() {
    let (base, _, tool) = fixture("cwd-env").await;
    let root = base.join("root");
    tokio::fs::create_dir(root.join("nested")).await.unwrap();
    let output = execute(
        &tool,
        json!({
            "command": "printf '%s\\n%s' \"$MINICORE_AGENT\" \"$PWD\"",
            "cwd": "nested"
        }),
    )
    .await
    .unwrap();
    assert!(output.contains("stdout:\n1\n"));
    assert!(output.contains(root.join("nested").to_str().unwrap()));
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_inputs_and_workspace_paths_fail_with_stable_categories() {
    use std::os::unix::fs::symlink;

    let (base, _, tool) = fixture("invalid").await;
    let root = base.join("root");
    let outside = base.join("outside");
    tokio::fs::create_dir(&outside).await.unwrap();
    symlink(&outside, root.join("escape")).unwrap();

    for arguments in [
        json!({}),
        json!({"command": ""}),
        json!({"command": "bad\0command"}),
        json!({"command": "true", "timeout_seconds": 0}),
        json!({"command": "true", "timeout_seconds": MAX_COMMAND_TIMEOUT + 1}),
        json!({"command": "true", "extra": true}),
        json!({"command": "true", "cwd": "../outside"}),
        json!({"command": "true", "cwd": outside.to_str().unwrap()}),
        json!({"command": "true", "cwd": "escape"}),
    ] {
        assert_eq!(
            tool.execute(
                invocation(arguments),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
    }
    assert_eq!(
        tool.execute(
            invocation(json!({"command": "true", "cwd": "missing"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn simultaneous_large_streams_are_drained_bounded_and_marked() {
    let (base, _, tool) = fixture("large-output").await;
    let command = "i=0; while [ \"$i\" -lt 70000 ]; do printf 'stdout-%08d\\n' \"$i\"; printf 'stderr-%08d\\n' \"$i\" >&2; i=$((i+1)); done";
    let output = execute_with_context(
        &tool,
        json!({"command": command, "timeout_seconds": 30}),
        context(
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(30),
        ),
    )
    .await
    .unwrap();
    assert!(output.starts_with("exit_code: 0\nstdout:\n"));
    assert!(output.contains("\nstderr:\n"));
    assert_eq!(output.matches(TRUNCATED).count(), 2);
    assert!(output.len() <= MAX_COMMAND_OUTPUT);
    assert!(output.len() <= MAX_TEXT_BYTES);
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_utf8_ansi_nul_and_controls_are_sanitized() {
    let (base, _, tool) = fixture("sanitize").await;
    let output = execute(
        &tool,
        json!({
            "command": "printf '\\377A\\000B\\033[31mC\\rD\\tE\\n'; printf '\\033err\\000\\376' >&2"
        }),
    )
    .await
    .unwrap();
    assert!(output.contains('�'));
    assert!(output.contains("\\u{0}"));
    assert!(output.contains("\\u{1b}"));
    assert!(output.contains("\\r"));
    assert!(!output.contains('\0'));
    assert!(!output.contains('\u{1b}'));
    assert!(!output.contains('\r'));
    assert!(
        output
            .chars()
            .all(|character| !character.is_control() || matches!(character, '\n' | '\t'))
    );
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn timeout_kills_and_reaps_the_direct_child() {
    let (base, _, tool) = fixture("timeout-kill").await;
    let pid_file = base.join("root/child.pid");
    assert_eq!(
        tool.execute(
            invocation(json!({
                "command": "echo $$ > child.pid; exec sleep 30",
                "timeout_seconds": 1
            })),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::TimedOut)
    );
    let pid = read_pid(&pid_file).await;
    wait_for_process_exit(pid).await;
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_kills_and_reaps_the_direct_child() {
    let (base, _, tool) = fixture("cancel-kill").await;
    let pid_file = base.join("root/child.pid");
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        tool.execute(
            invocation(json!({
                "command": "echo $$ > child.pid; exec sleep 30",
                "timeout_seconds": 30
            })),
            context(task_cancellation, Instant::now() + Duration::from_secs(30)),
        )
        .await
    });
    let pid = read_pid(&pid_file).await;
    cancellation.cancel();
    assert_eq!(task.await.unwrap(), Err(ToolError::Cancelled));
    wait_for_process_exit(pid).await;
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_dropped_execute_future_still_joins_its_owned_command() {
    let (base, _, tool) = fixture("abort-kill").await;
    let tool = Arc::new(tool);
    let pid_file = base.join("root/child.pid");
    let runner = Arc::clone(&tool);
    let task = tokio::spawn(async move {
        runner
            .execute(
                invocation(json!({
                    "command": "echo $$ > child.pid; exec sleep 30",
                    "timeout_seconds": 30
                })),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(30),
                ),
            )
            .await
    });
    let pid = read_pid(&pid_file).await;
    assert!(process_exists(pid));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // The dropped future only requested cancellation; the owner still has
    // to stop and reap the process, and this join is the barrier.
    tool.join_owned_commands().await;
    assert_eq!(tool.active_owned_commands(), 0);
    assert!(!process_exists(pid), "owned command outlived its join");
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn earliest_context_or_input_deadline_wins() {
    let (base, _, tool) = fixture("deadline-order").await;
    let started = Instant::now();
    assert_eq!(
        tool.execute(
            invocation(json!({
                "command": "exec sleep 30",
                "timeout_seconds": 30
            })),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_millis(100),
            )
        )
        .await,
        Err(ToolError::TimedOut)
    );
    assert!(started.elapsed() < Duration::from_secs(2));

    let started = Instant::now();
    assert_eq!(
        tool.execute(
            invocation(json!({
                "command": "exec sleep 30",
                "timeout_seconds": 1
            })),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
        )
        .await,
        Err(ToolError::TimedOut)
    );
    assert!(started.elapsed() < Duration::from_secs(4));
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn exited_child_with_inherited_pipes_obeys_input_and_context_deadlines() {
    // The command itself finishes immediately; only a descendant keeps the
    // pipe open. Stopping the drain at its bounded grace is a normal end
    // with incomplete output, not a fabricated timeout: the command's own
    // deadline was never reached.
    assert_grandchild_held_pipe_drain_is_bounded(
        "held-pipe-drain",
        1,
        Instant::now() + Duration::from_secs(5),
    )
    .await;
    // The context deadline really is reached while the drain is bounded, so
    // this one is a timeout.
    assert_grandchild_held_pipe_timeout(
        "held-pipe-context",
        30,
        Instant::now() + Duration::from_millis(100),
    )
    .await;
}

#[cfg(windows)]
#[tokio::test]
async fn windows_timeout_drops_named_pipe_readers_held_by_grandchild() {
    let (base, workspace, tool) = fixture("windows-held-pipe-timeout").await;
    let follow_up = BashTool::new(workspace, empty_command_environment());
    let pid_file = base.join("root/grandchild.pid");
    let started = Instant::now();
    let mut task = tokio::spawn(async move {
        tool.execute(
            invocation(json!({
                "command": windows_grandchild_command(true),
                "timeout_seconds": 10
            })),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(15),
            ),
        )
        .await
    });
    let pid = read_pid_or_task(&pid_file, &mut task).await;
    let mut guard = WindowsProcessGuard::new(pid);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap(),
        Err(ToolError::TimedOut)
    );
    assert!(started.elapsed() < Duration::from_secs(15));
    // The job object is the controlled scope: its grandchild is stopped
    // with the leader instead of holding the capture open.
    wait_for_process_exit(pid).await;
    assert_windows_follow_up(&follow_up).await;
    guard.terminate().await;
    cleanup(&base).await;
}

#[cfg(windows)]
#[tokio::test]
async fn windows_cancellation_drops_named_pipe_readers_held_by_grandchild() {
    let (base, workspace, tool) = fixture("windows-held-pipe-cancel").await;
    let follow_up = BashTool::new(workspace, empty_command_environment());
    let pid_file = base.join("root/grandchild.pid");
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move {
        tool.execute(
            invocation(json!({
                "command": windows_grandchild_command(false),
                "timeout_seconds": 30
            })),
            context(task_cancellation, Instant::now() + Duration::from_secs(30)),
        )
        .await
    });
    let pid = read_pid_or_task(&pid_file, &mut task).await;
    let mut guard = WindowsProcessGuard::new(pid);
    cancellation.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap(),
        Err(ToolError::Cancelled)
    );
    wait_for_process_exit(pid).await;
    assert_windows_follow_up(&follow_up).await;
    guard.terminate().await;
    cleanup(&base).await;
}

#[cfg(windows)]
#[test]
fn windows_future_drop_cancels_capture_and_runtime_shutdown_is_bounded() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (base, workspace, tool) = fixture("windows-held-pipe-drop").await;
        let follow_up = BashTool::new(workspace, empty_command_environment());
        let pid_file = base.join("root/grandchild.pid");
        let mut task = tokio::spawn(async move {
            tool.execute(
                invocation(json!({
                    "command": windows_grandchild_command(true),
                    "timeout_seconds": 30
                })),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(30),
                ),
            )
            .await
        });
        let pid = read_pid_or_task(&pid_file, &mut task).await;
        let mut guard = WindowsProcessGuard::new(pid);
        task.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert_windows_follow_up(&follow_up).await;
        guard.terminate().await;
        cleanup(&base).await;
    });
    let shutdown_started = Instant::now();
    runtime.shutdown_timeout(Duration::from_secs(2));
    assert!(shutdown_started.elapsed() < Duration::from_secs(3));
}

#[cfg(unix)]
#[tokio::test]
async fn an_injected_stop_failure_still_stops_without_claiming_success() {
    assert_termination_failure_is_honest("start-kill-failure", TerminationFailure::StartKill).await;
}

#[cfg(unix)]
#[tokio::test]
async fn an_injected_reap_failure_still_stops_without_claiming_success() {
    assert_termination_failure_is_honest("wait-failure", TerminationFailure::Wait).await;
}

#[cfg(unix)]
#[tokio::test]
async fn signal_exit_uses_unavailable_exit_code() {
    let (base, _, tool) = fixture("signal-exit").await;
    let output = execute(&tool, json!({"command": "kill -TERM $$"}))
        .await
        .unwrap();
    assert!(output.starts_with("exit_code: unavailable\nstdout:\n"));
    assert!(output.contains("\nstderr:\n"));
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn unicode_and_binary_bytes_are_reported_exactly() {
    let (base, _, tool) = fixture("raw-bytes").await;
    // A multi-byte character split across two writes plus invalid UTF-8
    // must not change the byte length the model sees as the recorded exit
    // is still a completed command.
    let output = execute(
        &tool,
        json!({"command": r"printf '\303'; printf '\251'; printf '\377'"}),
    )
    .await
    .unwrap();
    assert!(output.starts_with("exit_code: 0\nstdout:\n"));
    assert!(output.contains('\u{fffd}'));
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_nonzero_exit_is_a_completed_result_not_an_error() {
    let (base, _, tool) = fixture("nonzero-completed").await;
    let result = tool
        .execute(
            invocation(json!({"command": "exit 3"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await;
    assert!(matches!(result, Ok(ToolExecutionOutcome::Completed(_))));
    cleanup(&base).await;
}

#[cfg(unix)]
async fn assert_grandchild_held_pipe_drain_is_bounded(
    label: &str,
    timeout_seconds: u64,
    context_deadline: Instant,
) {
    let (base, _, tool) = fixture(label).await;
    let pid_file = base.join("root/grandchild.pid");
    let started = Instant::now();
    let task = tokio::spawn(async move {
        tool.execute(
            invocation(json!({
                "command": "sleep 30 & echo $! > grandchild.pid; exit 0",
                "timeout_seconds": timeout_seconds
            })),
            context(CancellationToken::new(), context_deadline),
        )
        .await
    });
    let pid = read_pid(&pid_file).await;
    let mut guard = UnixProcessGuard::new(pid);
    let result = tokio::time::timeout(Duration::from_secs(3), task).await;
    let elapsed = started.elapsed();
    match result.unwrap().unwrap() {
        Ok(ToolExecutionOutcome::Completed(output)) => {
            // The leader really exited; only its descendant's output is
            // missing, so the result is completed and explicitly truncated.
            assert!(output.content().as_str().starts_with("exit_code: 0\n"));
            assert!(output.content().as_str().contains(TRUNCATED));
        }
        Ok(ToolExecutionOutcome::RequestInput(_)) => {
            panic!("bash must not request input")
        }
        Err(error) => panic!("a bounded drain stop must stay a completed result: {error}"),
    }
    assert!(elapsed < Duration::from_secs(2));
    // The bounded drain still stops the whole controlled group.
    wait_for_process_exit(pid).await;
    guard.terminate().await;
    cleanup(&base).await;
}

#[cfg(unix)]
async fn assert_grandchild_held_pipe_timeout(
    label: &str,
    timeout_seconds: u64,
    context_deadline: Instant,
) {
    let (base, _, tool) = fixture(label).await;
    let pid_file = base.join("root/grandchild.pid");
    let started = Instant::now();
    let task = tokio::spawn(async move {
        tool.execute(
            invocation(json!({
                "command": "sleep 30 & echo $! > grandchild.pid; exit 0",
                "timeout_seconds": timeout_seconds
            })),
            context(CancellationToken::new(), context_deadline),
        )
        .await
    });
    let pid = read_pid(&pid_file).await;
    let mut guard = UnixProcessGuard::new(pid);
    let result = tokio::time::timeout(Duration::from_secs(3), task).await;
    let elapsed = started.elapsed();
    assert_eq!(
        result.unwrap().unwrap(),
        Err(ToolError::TimedOut),
        "collector waited for inherited pipe EOF"
    );
    assert!(elapsed < Duration::from_secs(2));
    // A leader that exited while a descendant held its pipe is followed by
    // a bounded grace, then the whole controlled group is stopped.
    wait_for_process_exit(pid).await;
    guard.terminate().await;
    cleanup(&base).await;
}

#[cfg(unix)]
async fn assert_termination_failure_is_honest(label: &str, failure: TerminationFailure) {
    let (base, _, tool) = fixture(label).await;
    let pid_file = base.join("root/child.pid");
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move {
        tool.execute(
            invocation(json!({
                "command": "echo $$ > child.pid; exec sleep 30",
                "timeout_seconds": 30
            })),
            context(task_cancellation, Instant::now() + Duration::from_secs(30)),
        )
        .await
    });
    let pid = read_pid(&pid_file).await;
    let mut guard = UnixProcessGuard::new(pid);
    crate::tools::command::TERMINATION_FAILURES.arm((Some(pid), failure), ());
    let started = Instant::now();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
    let observed = match result {
        Ok(Ok(Err(error))) => Some(error),
        _ => None,
    };
    if !task.is_finished() {
        task.abort();
        let _ = task.await;
    }
    // Whatever the injected failure, the controlled group is still stopped
    // and the tool reports the cancellation it was asked for, never a
    // fabricated termination.
    wait_for_process_exit(pid).await;
    guard.disarm();
    cleanup(&base).await;
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(observed, Some(ToolError::Cancelled));
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
    panic!("test process did not publish its PID");
}

#[cfg(windows)]
fn report_finished_pid_task(
    result: Result<Result<ToolExecutionOutcome, ToolError>, tokio::task::JoinError>,
) -> ! {
    match result {
        Ok(Ok(ToolExecutionOutcome::Completed(_))) => {
            panic!("tool returned Completed before publishing its PID")
        }
        Ok(Ok(ToolExecutionOutcome::RequestInput(_))) => {
            panic!("tool returned RequestInput before publishing its PID")
        }
        Ok(Err(error)) => panic!("tool failed before publishing its PID: {error}"),
        Err(error) => panic!("tool task ended before publishing its PID: {error}"),
    }
}

#[cfg(windows)]
async fn read_pid_or_task(
    path: &Path,
    task: &mut tokio::task::JoinHandle<Result<ToolExecutionOutcome, ToolError>>,
) -> u32 {
    const READY_TIMEOUT: Duration = Duration::from_secs(15);
    let wait = async {
        loop {
            if let Ok(value) = tokio::fs::read_to_string(path).await {
                if let Ok(pid) = value.trim().parse() {
                    return pid;
                }
            }
            tokio::select! {
                result = &mut *task => report_finished_pid_task(result),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    };
    match tokio::time::timeout(READY_TIMEOUT, wait).await {
        Ok(pid) => pid,
        Err(_) => {
            task.abort();
            match (&mut *task).await {
                Err(error) if error.is_cancelled() => {
                    panic!("test process did not publish its PID within {READY_TIMEOUT:?}")
                }
                result => report_finished_pid_task(result),
            }
        }
    }
}

#[cfg(unix)]
async fn wait_for_process_exit(pid: u32) {
    for _ in 0..200 {
        if !process_exists(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("direct child PID {pid} remained alive");
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
struct UnixProcessGuard {
    pid: Option<u32>,
}

#[cfg(unix)]
impl UnixProcessGuard {
    fn new(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    async fn terminate(&mut self) {
        let Some(pid) = self.pid else {
            return;
        };
        kill_process(pid);
        wait_for_process_exit(pid).await;
        self.pid = None;
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

#[cfg(unix)]
impl Drop for UnixProcessGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            kill_process(pid);
        }
    }
}

#[cfg(unix)]
fn kill_process(pid: u32) {
    let _ = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -KILL {pid} 2>/dev/null"))
        .status();
}

#[cfg(windows)]
fn windows_grandchild_command(parent_waits: bool) -> String {
    let parent_tail = if parent_waits {
        "; Start-Sleep -Seconds 30"
    } else {
        ""
    };
    format!(
        "$child = Start-Process -FilePath powershell.exe -NoNewWindow -PassThru \
             -ArgumentList @('-NoProfile', '-NonInteractive', '-Command', \
             'Start-Sleep -Seconds 30'); Set-Content -NoNewline -Path \
             grandchild.pid -Value $child.Id{parent_tail}"
    )
}

/// The identity a Bash call runs under is the one passed at the real
/// execute boundary, so a standalone tool (no captured identity) still owns
/// its command and a bound tool records under the exact `ToolRef` it was
/// given -- never under a name, a path, or "the newest call".
#[cfg(unix)]
#[tokio::test]
async fn execute_bound_records_under_the_captured_identity() {
    let (base, workspace, _) = fixture("bound-identity").await;
    let owners = crate::tools::command::ToolOwners::new();
    let data = Arc::new(crate::tool_data::ToolData::new());
    let sink = IdentitySink::new(Arc::clone(&data));
    let streams: Arc<dyn crate::tools::command::CommandStreamSink> =
        Arc::clone(&sink) as Arc<dyn crate::tools::command::CommandStreamSink>;
    let binding = crate::tools::command::CommandBinding::new(Arc::clone(&owners), streams);
    let bound = BashTool::with_binding(workspace, empty_command_environment(), Some(binding));
    let tool_ref = crate::tool_data::ToolRef {
        session_id: crate::ids::SessionId::new().unwrap(),
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 2,
        tool_call_id: ToolCallId::new("bound-call").unwrap(),
    };
    data.note_requested(&tool_ref, "bash");
    let result = bound
        .execute_bound(
            invocation(json!({"command": "printf bound"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
            Some(tool_ref.clone()),
        )
        .await;
    assert!(matches!(result, Ok(ToolExecutionOutcome::Completed(_))));
    // The stream bytes were stored under the captured identity.
    assert_eq!(
        data.stream_range(&tool_ref, crate::tool_data::ToolDataStream::Stdout),
        Some((0, 5))
    );
    let owned = sink.first_owned();
    assert_eq!(owned.as_ref(), Some(&tool_ref));
    owners.join_all().await;
    cleanup(&base).await;
}

/// A standalone tool with no captured identity still owns and joins its
/// command instead of falling outside every barrier.
#[cfg(unix)]
#[tokio::test]
async fn a_standalone_call_still_owns_and_joins_its_command() {
    let (base, _, tool) = fixture("standalone-owner").await;
    let pid_file = base.join("root/child.pid");
    let tool = Arc::new(tool);
    let runner = Arc::clone(&tool);
    let task = tokio::spawn(async move {
        runner
            .execute_bound(
                invocation(json!({
                    "command": "echo $$ > child.pid; exec sleep 30",
                    "timeout_seconds": 30
                })),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(30),
                ),
                None,
            )
            .await
    });
    let pid = read_pid(&pid_file).await;
    assert!(process_exists(pid));
    task.abort();
    let _ = task.await;
    tool.join_owned_commands().await;
    assert_eq!(tool.active_owned_commands(), 0);
    assert!(
        !process_exists(pid),
        "a standalone command outlived its join"
    );
    cleanup(&base).await;
}

#[cfg(windows)]
async fn assert_windows_follow_up(tool: &BashTool) {
    let output = tokio::time::timeout(
        Duration::from_secs(2),
        execute(tool, json!({"command": "Write-Output follow-up"})),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(output.starts_with("exit_code: 0\nstdout:\nfollow-up"));
    assert!(output.contains("\nstderr:\n"));
}

#[cfg(windows)]
async fn wait_for_process_exit(pid: u32) {
    for _ in 0..200 {
        if !process_exists(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("test process PID {pid} remained alive");
}

#[cfg(windows)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-Command")
        .arg(format!(
            "if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 0 }} \
                 else {{ exit 1 }}"
        ))
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
struct WindowsProcessGuard {
    pid: Option<u32>,
}

#[cfg(windows)]
impl WindowsProcessGuard {
    fn new(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    async fn terminate(&mut self) {
        let Some(pid) = self.pid else {
            return;
        };
        kill_process(pid);
        wait_for_process_exit(pid).await;
        self.pid = None;
    }
}

#[cfg(windows)]
impl Drop for WindowsProcessGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            kill_process(pid);
        }
    }
}

#[cfg(windows)]
fn kill_process(pid: u32) {
    let _ = std::process::Command::new("taskkill")
        .arg("/PID")
        .arg(pid.to_string())
        .arg("/F")
        .status();
}

impl CommandEnvironment {
    #[cfg(test)]
    pub(crate) fn names(&self) -> &[OsString] {
        &self.removed
    }
}
impl BashTool {
    #[cfg(test)]
    pub(super) fn new(workspace: Arc<Workspace>, environment: CommandEnvironment) -> Self {
        Self::with_binding(workspace, environment, None)
    }
    #[cfg(all(test, unix))]
    pub(super) async fn join_owned_commands(&self) {
        self.owners.join_all().await;
    }
    #[cfg(all(test, unix))]
    pub(super) fn active_owned_commands(&self) -> usize {
        self.owners.active()
    }
}
