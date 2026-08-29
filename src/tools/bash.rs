use std::io;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
#[cfg(windows)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(all(test, unix))]
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use minicore_runtime::value::MAX_TEXT_BYTES;
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

use crate::{Workspace, WorkspaceError};

use super::{DEFAULT_COMMAND_TIMEOUT, MAX_COMMAND_OUTPUT, MAX_COMMAND_TIMEOUT, precheck_control};

const TOOL_NAME: &str = "bash";
const STREAM_CAPTURE_LIMIT: usize = MAX_COMMAND_OUTPUT / 2;
const TRUNCATED: &str = "[truncated]";
const FORMATTED_OUTPUT_LIMIT: usize = if MAX_COMMAND_OUTPUT < MAX_TEXT_BYTES {
    MAX_COMMAND_OUTPUT
} else {
    MAX_TEXT_BYTES
};

#[cfg(windows)]
static NEXT_PIPE_ID: AtomicU64 = AtomicU64::new(1);

type OutputReader = Box<dyn AsyncRead + Unpin + Send>;

struct SpawnedCommand {
    child: Child,
    stdout: OutputReader,
    stderr: OutputReader,
}

pub(super) struct BashTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BashInput {
    command: String,
    #[serde(default = "default_cwd")]
    cwd: String,
    #[serde(default = "default_timeout")]
    timeout_seconds: u64,
}

impl BashTool {
    pub(super) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("bash is a valid tool name"),
            "Run one bounded shell command in a workspace directory.",
            json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Shell command to execute."
                    },
                    "cwd": {
                        "type": "string",
                        "default": ".",
                        "description": "Workspace-relative working directory."
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_COMMAND_TIMEOUT,
                        "default": DEFAULT_COMMAND_TIMEOUT,
                        "description": "Command timeout in seconds."
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        )
        .expect("static bash tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for BashTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'a> {
        Box::pin(async move {
            precheck_control(&context)?;
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input: BashInput = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            if input.command.is_empty()
                || input.command.contains('\0')
                || !(1..=MAX_COMMAND_TIMEOUT).contains(&input.timeout_seconds)
            {
                return Err(ToolError::InvalidInvocation);
            }
            let input_deadline = Instant::now()
                .checked_add(Duration::from_secs(input.timeout_seconds))
                .ok_or(ToolError::InvalidInvocation)?;
            let deadline = context.deadline.min(input_deadline);
            let cwd = resolve_cwd(&self.workspace, &input.cwd, &context, deadline).await?;
            let output = run_command(&input.command, &cwd, &invocation, &context, deadline).await?;
            let output = ToolOutput::new(output).map_err(|_| ToolError::Internal)?;
            Ok(ToolExecutionOutcome::Completed(output))
        })
    }
}

async fn resolve_cwd(
    workspace: &Workspace,
    cwd: &str,
    context: &ToolContext,
    deadline: Instant,
) -> Result<std::path::PathBuf, ToolError> {
    let resolve = workspace.resolve_directory(cwd);
    tokio::pin!(resolve);
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(ToolError::Cancelled),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ToolError::TimedOut)
        }
        result = &mut resolve => result.map_err(map_cwd_error),
    }
}

async fn run_command(
    command: &str,
    cwd: &std::path::Path,
    invocation: &ToolInvocation,
    context: &ToolContext,
    deadline: Instant,
) -> Result<String, ToolError> {
    if context.cancellation.is_cancelled() {
        return Err(ToolError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(ToolError::TimedOut);
    }

    let spawned = spawn_with_output_capture(command, cwd, invocation).await?;
    let mut child = spawned.child;
    let stdout = spawned.stdout;
    let stderr = spawned.stderr;
    let collectors = async { tokio::try_join!(capture_stream(stdout), capture_stream(stderr)) };
    tokio::pin!(collectors);
    let mut status = None;
    let mut captures = None;

    loop {
        if status.is_some() && captures.is_some() {
            break;
        }
        let event = tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => ProcessEvent::Cancelled,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                ProcessEvent::TimedOut
            }
            result = child.wait(), if status.is_none() => ProcessEvent::Exited(result),
            result = &mut collectors, if captures.is_none() => ProcessEvent::Output(result),
        };
        match event {
            ProcessEvent::Cancelled => {
                terminate_and_reap(&mut child).await?;
                return Err(ToolError::Cancelled);
            }
            ProcessEvent::TimedOut => {
                terminate_and_reap(&mut child).await?;
                return Err(ToolError::TimedOut);
            }
            ProcessEvent::Exited(Ok(exit_status)) => status = Some(exit_status),
            ProcessEvent::Output(Ok(output)) => captures = Some(output),
            ProcessEvent::Exited(Err(_)) | ProcessEvent::Output(Err(_)) => {
                terminate_and_reap(&mut child).await?;
                return Err(ToolError::Internal);
            }
        }
    }

    let (stdout, stderr) = captures.expect("output capture is complete");
    Ok(format_output(
        status.expect("process status is complete"),
        &stdout,
        &stderr,
    ))
}

#[cfg(unix)]
async fn spawn_with_output_capture(
    command: &str,
    cwd: &std::path::Path,
    _invocation: &ToolInvocation,
) -> Result<SpawnedCommand, ToolError> {
    let mut process = shell_command(command);
    process
        .current_dir(cwd)
        .env("MINICORE_AGENT", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process.spawn().map_err(|_| ToolError::Failed)?;
    drop(process);
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            terminate_and_reap(&mut child).await?;
            return Err(ToolError::Internal);
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            terminate_and_reap(&mut child).await?;
            return Err(ToolError::Internal);
        }
    };
    Ok(SpawnedCommand {
        child,
        stdout: Box::new(stdout),
        stderr: Box::new(stderr),
    })
}

#[cfg(windows)]
async fn spawn_with_output_capture(
    command: &str,
    cwd: &std::path::Path,
    invocation: &ToolInvocation,
) -> Result<SpawnedCommand, ToolError> {
    let (stdout_server, stdout_client) =
        create_capture_pipe(invocation, "stdout").map_err(|_| ToolError::Failed)?;
    let (stderr_server, stderr_client) =
        create_capture_pipe(invocation, "stderr").map_err(|_| ToolError::Failed)?;
    tokio::try_join!(stdout_server.connect(), stderr_server.connect())
        .map_err(|_| ToolError::Failed)?;

    let mut process = shell_command(command);
    process
        .current_dir(cwd)
        .env("MINICORE_AGENT", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_client))
        .stderr(Stdio::from(stderr_client))
        .kill_on_drop(true);
    let child = process.spawn().map_err(|_| ToolError::Failed)?;
    drop(process);
    Ok(SpawnedCommand {
        child,
        stdout: Box::new(stdout_server),
        stderr: Box::new(stderr_server),
    })
}

#[cfg(windows)]
fn create_capture_pipe(
    invocation: &ToolInvocation,
    stream: &str,
) -> io::Result<(
    tokio::net::windows::named_pipe::NamedPipeServer,
    std::fs::File,
)> {
    use tokio::net::windows::named_pipe::{PipeMode, ServerOptions};

    let pipe_id = NEXT_PIPE_ID.fetch_add(1, Ordering::Relaxed);
    let tool_call_id = safe_pipe_component(invocation.tool_call_id().as_str());
    let pipe_name = format!(
        r"\\.\pipe\minicore-agent-{}-{}-{}-{pipe_id}-{stream}",
        std::process::id(),
        invocation.session_id(),
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

#[cfg(unix)]
fn shell_command(command: &str) -> Command {
    let mut process = Command::new("/bin/sh");
    process.arg("-lc").arg(command);
    process
}

#[cfg(windows)]
fn shell_command(command: &str) -> Command {
    let mut process = Command::new("powershell.exe");
    process
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-Command")
        .arg(command);
    process
}

enum ProcessEvent {
    Cancelled,
    TimedOut,
    Exited(io::Result<ExitStatus>),
    Output(io::Result<(StreamCapture, StreamCapture)>),
}

struct StreamCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn capture_stream<R>(mut reader: R) -> io::Result<StreamCapture>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(STREAM_CAPTURE_LIMIT.min(8 * 1024));
    let mut truncated = false;
    let mut buffer = [0u8; 8 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = STREAM_CAPTURE_LIMIT.saturating_sub(bytes.len());
        let retained = remaining.min(read);
        bytes.extend_from_slice(&buffer[..retained]);
        truncated |= retained < read;
    }
    Ok(StreamCapture { bytes, truncated })
}

async fn terminate_and_reap(child: &mut Child) -> Result<(), ToolError> {
    if child.try_wait().map_err(|_| ToolError::Internal)?.is_some() {
        return Ok(());
    }
    start_kill(child).map_err(|_| ToolError::Internal)?;
    wait_after_kill(child)
        .await
        .map_err(|_| ToolError::Internal)?;
    Ok(())
}

fn start_kill(child: &mut Child) -> io::Result<()> {
    #[cfg(all(test, unix))]
    if take_termination_failure(child, TerminationFailure::StartKill) {
        return Err(io::Error::other("injected start_kill failure"));
    }
    child.start_kill()
}

async fn wait_after_kill(child: &mut Child) -> io::Result<ExitStatus> {
    #[cfg(all(test, unix))]
    if take_termination_failure(child, TerminationFailure::Wait) {
        return Err(io::Error::other("injected wait failure"));
    }
    child.wait().await
}

#[cfg(all(test, unix))]
#[derive(Clone, Copy, Eq, PartialEq)]
enum TerminationFailure {
    StartKill,
    Wait,
}

#[cfg(all(test, unix))]
static TERMINATION_FAILURES: OnceLock<Mutex<Vec<(u32, TerminationFailure)>>> = OnceLock::new();

#[cfg(all(test, unix))]
fn inject_termination_failure(pid: u32, failure: TerminationFailure) {
    TERMINATION_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((pid, failure));
}

#[cfg(all(test, unix))]
fn take_termination_failure(child: &Child, failure: TerminationFailure) -> bool {
    let Some(pid) = child.id() else {
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

fn format_output(status: ExitStatus, stdout: &StreamCapture, stderr: &StreamCapture) -> String {
    let exit_code = status
        .code()
        .map_or_else(|| "unavailable".to_owned(), |code| code.to_string());
    let prefix = format!("exit_code: {exit_code}\nstdout:\n");
    let stderr_label = "stderr:\n";
    let fixed = prefix
        .len()
        .saturating_add(stderr_label.len())
        .saturating_add(1);
    let available = FORMATTED_OUTPUT_LIMIT.saturating_sub(fixed);
    let stdout_budget = available / 2;
    let stderr_budget = available.saturating_sub(stdout_budget);
    let stdout = format_stream(stdout, stdout_budget);
    let stderr = format_stream(stderr, stderr_budget);
    let mut output = String::with_capacity(
        FORMATTED_OUTPUT_LIMIT
            .min(prefix.len() + stdout.len() + stderr_label.len() + stderr.len() + 1),
    );
    output.push_str(&prefix);
    output.push_str(&stdout);
    if !output.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(stderr_label);
    output.push_str(&stderr);
    debug_assert!(output.len() <= FORMATTED_OUTPUT_LIMIT);
    output
}

fn format_stream(capture: &StreamCapture, budget: usize) -> String {
    let value = String::from_utf8_lossy(&capture.bytes);
    let sanitized_len = value.chars().fold(0usize, |length, character| {
        length.saturating_add(sanitized_character_len(character))
    });
    let truncated = capture.truncated || sanitized_len > budget;
    let marker_extra = TRUNCATED.len().saturating_add(1);
    let content_budget = if truncated {
        budget.saturating_sub(marker_extra)
    } else {
        budget
    };
    let mut output = String::with_capacity(budget.min(sanitized_len.saturating_add(marker_extra)));
    for character in value.chars() {
        let length = sanitized_character_len(character);
        if output.len().saturating_add(length) > content_budget {
            break;
        }
        push_sanitized_character(&mut output, character);
    }
    if truncated {
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str(TRUNCATED);
    }
    debug_assert!(output.len() <= budget);
    output
}

fn sanitized_character_len(character: char) -> usize {
    if !character.is_control() || matches!(character, '\n' | '\t') {
        character.len_utf8()
    } else {
        character
            .escape_default()
            .map(|escaped| escaped.len_utf8())
            .sum()
    }
}

fn push_sanitized_character(output: &mut String, character: char) {
    if !character.is_control() || matches!(character, '\n' | '\t') {
        output.push(character);
    } else {
        output.extend(character.escape_default());
    }
}

fn map_cwd_error(error: WorkspaceError) -> ToolError {
    match error {
        WorkspaceError::InvalidPath | WorkspaceError::Escape => ToolError::InvalidInvocation,
        WorkspaceError::NotFound
        | WorkspaceError::NotFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::Unavailable
        | WorkspaceError::UnknownOutcome
        | WorkspaceError::TooLarge
        | WorkspaceError::Binary => ToolError::Failed,
    }
}

fn default_cwd() -> String {
    ".".to_owned()
}

const fn default_timeout() -> u64 {
    DEFAULT_COMMAND_TIMEOUT
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::ids::{SessionInstanceId, ToolCallId, TurnId};
    use minicore_runtime::tools::{
        Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, BashTool) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-bash-tool-{label}-{}",
            minicore_runtime::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let tool = BashTool::new(Arc::clone(&workspace));
        (base, workspace, tool)
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            instance_id: "ins_00000000000000000000000000000001"
                .parse::<SessionInstanceId>()
                .unwrap(),
            turn_id: "trn_00000000000000000000000000000001"
                .parse::<TurnId>()
                .unwrap(),
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
    async fn aborting_the_execute_future_triggers_kill_on_drop() {
        let (base, _, tool) = fixture("abort-kill").await;
        let pid_file = base.join("root/child.pid");
        let task = tokio::spawn(async move {
            tool.execute(
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
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        wait_for_process_exit(pid).await;
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
        assert_grandchild_held_pipe_timeout(
            "held-pipe-input",
            1,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
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
        let follow_up = BashTool::new(workspace);
        let pid_file = base.join("root/grandchild.pid");
        let started = Instant::now();
        let task = tokio::spawn(async move {
            tool.execute(
                invocation(json!({
                    "command": windows_grandchild_command(false),
                    "timeout_seconds": 1
                })),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5),
                ),
            )
            .await
        });
        let pid = read_pid(&pid_file).await;
        let mut guard = WindowsProcessGuard::new(pid);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap(),
            Err(ToolError::TimedOut)
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(process_exists(pid));
        assert_windows_follow_up(&follow_up).await;
        guard.terminate().await;
        cleanup(&base).await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_cancellation_drops_named_pipe_readers_held_by_grandchild() {
        let (base, workspace, tool) = fixture("windows-held-pipe-cancel").await;
        let follow_up = BashTool::new(workspace);
        let pid_file = base.join("root/grandchild.pid");
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            tool.execute(
                invocation(json!({
                    "command": windows_grandchild_command(false),
                    "timeout_seconds": 30
                })),
                context(task_cancellation, Instant::now() + Duration::from_secs(30)),
            )
            .await
        });
        let pid = read_pid(&pid_file).await;
        let mut guard = WindowsProcessGuard::new(pid);
        cancellation.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap(),
            Err(ToolError::Cancelled)
        );
        assert!(process_exists(pid));
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
            let follow_up = BashTool::new(workspace);
            let pid_file = base.join("root/grandchild.pid");
            let task = tokio::spawn(async move {
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
            let pid = read_pid(&pid_file).await;
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
    async fn start_kill_failure_is_internal_without_waiting_for_natural_exit() {
        assert_termination_failure_is_internal("start-kill-failure", TerminationFailure::StartKill)
            .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wait_failure_is_internal_and_child_drop_finishes_cleanup() {
        assert_termination_failure_is_internal("wait-failure", TerminationFailure::Wait).await;
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
        assert!(process_exists(pid));
        guard.terminate().await;
        cleanup(&base).await;
    }

    #[cfg(unix)]
    async fn assert_termination_failure_is_internal(label: &str, failure: TerminationFailure) {
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
        inject_termination_failure(pid, failure);
        let started = Instant::now();
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_millis(500), &mut task).await;
        let observed = match result {
            Ok(Ok(Err(error))) => Some(error),
            _ => None,
        };
        if !task.is_finished() {
            task.abort();
            let _ = task.await;
        }
        wait_for_process_exit(pid).await;
        guard.disarm();
        cleanup(&base).await;
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(observed, Some(ToolError::Internal));
    }

    #[cfg(any(unix, windows))]
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
}
