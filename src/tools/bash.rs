use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::Workspace;
use crate::tool_data::{CommandStatus, ToolRef};

use super::command::{Capture, CommandBinding, CommandOutcome, CommandRequest, ToolOwners};
use super::truncate::{Tail, format_size, share, truncate_tail};
use super::{
    DEFAULT_COMMAND_TIMEOUT, MAX_COMMAND_OUTPUT, MAX_COMMAND_TIMEOUT, MAX_OUTPUT_BYTES,
    MAX_OUTPUT_LINES,
};

const TOOL_NAME: &str = "bash";
const INCOMPLETE: &str = "[output may be incomplete]";

#[derive(Clone)]
pub(crate) struct CommandEnvironment {
    removed: Arc<[OsString]>,
}

impl CommandEnvironment {
    pub(crate) fn new(names: impl IntoIterator<Item = OsString>) -> Self {
        let mut removed = names
            .into_iter()
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        removed.sort();
        removed.dedup();
        Self {
            removed: removed.into(),
        }
    }

    pub(crate) fn extended(&self, names: impl IntoIterator<Item = OsString>) -> Self {
        Self::new(self.removed.iter().cloned().chain(names))
    }

    pub(crate) fn apply(&self, command: &mut Command) {
        for name in self.removed.iter() {
            command.env_remove(name);
        }
        command.env("MINICORE_AGENT", "1");
    }
}

pub(crate) struct BashTool {
    workspace: Arc<Workspace>,
    environment: CommandEnvironment,
    /// Owner boundary for every command this tool starts. A tool built without
    /// a presentation still owns its commands instead of relying on the
    /// Runtime dropping a future.
    owners: Arc<ToolOwners>,
    /// Narrow presentation binding: complete identity of the current call and
    /// the sink its streams go to. `None` for a standalone tool.
    binding: Option<CommandBinding>,
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
    pub(crate) fn with_binding(
        workspace: Arc<Workspace>,
        environment: CommandEnvironment,
        binding: Option<CommandBinding>,
    ) -> Self {
        let owners = binding
            .as_ref()
            .map_or_else(ToolOwners::new, |binding| Arc::clone(binding.owners()));
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("bash is a valid tool name"),
            "Run one bounded shell command in a workspace directory. Returns the exit code, \
             stdout and stderr. Together the streams are cut to their last 2000 lines or \
             50KB, whichever is hit first; the full output of a cut stream is saved to a \
             temp file named in the notice.",
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
        Self {
            workspace,
            environment,
            owners,
            binding,
            spec,
        }
    }
}

impl Tool for BashTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        // A standalone Bash tool has no captured identity; it still owns its
        // commands and can be joined explicitly.
        self.execute_bound(invocation, context, None)
    }
}

impl BashTool {
    /// Executes one Bash call whose complete `ToolRef` was captured at the real
    /// `Tool::execute` boundary of its wrapper. The identity is passed in
    /// explicitly and is never recovered from a live table, so a new loop that
    /// reuses a call id can never inherit a stale one.
    pub(crate) fn execute_bound(
        &self,
        invocation: ToolInvocation,
        context: ToolContext,
        tool_ref: Option<ToolRef>,
    ) -> ToolFuture<'_> {
        Box::pin(async move {
            let input: BashInput = super::decode_input(&self.spec, &invocation, &context)?;
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
            let capture = command_capture(&invocation).await?;
            let request = CommandRequest {
                tool_ref,
                command: input.command,
                cwd,
                deadline,
                cancellation: context.cancellation.clone(),
                environment: self.environment.clone(),
                capture_limit: MAX_COMMAND_OUTPUT,
                streams: self.binding.as_ref().map(|binding| binding.sink()),
                capture,
            };
            // The owner keeps the child even if this future is dropped; the
            // Runtime result is taken from the owner's terminal facts only.
            let mut worker = self
                .owners
                .start(request)
                .map_err(|_| ToolError::Internal)?;
            super::emit_phase(&context, "running");
            let outcome = worker.completion().await;
            outcome_to_result(&outcome).await
        })
    }
}

#[cfg(unix)]
async fn command_capture(_invocation: &ToolInvocation) -> Result<Capture, ToolError> {
    Ok(Capture::Inherited)
}

/// Windows keeps its named-pipe capture: the owner reads a pipe it created
/// rather than the child's inherited handle.
#[cfg(windows)]
async fn command_capture(invocation: &ToolInvocation) -> Result<Capture, ToolError> {
    super::command::NamedPipeCapture::new(invocation.tool_call_id())
        .await
        .map(Capture::NamedPipes)
        .map_err(|_| ToolError::Failed)
}

/// Maps the owner's terminal facts to the Runtime result. A non-zero exit is a
/// completed outcome, never an error, and a requested stop is only reported
/// after the owner really joined the process. The process record is the single
/// source of truth here: a command that really exited is reported as completed
/// even when a cancellation happened to arrive while the pipes were closing.
async fn outcome_to_result(outcome: &CommandOutcome) -> Result<ToolExecutionOutcome, ToolError> {
    match outcome.result.status {
        CommandStatus::SpawnFailed => return Err(ToolError::Failed),
        CommandStatus::Failed => return Err(ToolError::Internal),
        CommandStatus::Cancelled => return Err(ToolError::Cancelled),
        CommandStatus::TimedOut => return Err(ToolError::TimedOut),
        CommandStatus::Running | CommandStatus::Cancelling => return Err(ToolError::Internal),
        CommandStatus::Exited => {}
    }
    let stdout = Stream::new("stdout", &outcome.stdout, outcome.stdout_omitted);
    let stderr = Stream::new("stderr", &outcome.stderr, outcome.stderr_omitted);
    let lines = share(stdout.lines(), stderr.lines(), MAX_OUTPUT_LINES);
    let bytes = share(stdout.text.len(), stderr.text.len(), MAX_OUTPUT_BYTES);
    // A completed result whose output was cut short (a stopped descendant or a
    // bounded drain) must not look like a clean, complete output.
    let incomplete = !outcome.result.output_complete;
    let stdout = stdout.render((lines.0, bytes.0), incomplete).await;
    let stderr = stderr.render((lines.1, bytes.1), incomplete).await;
    let output = format_output(outcome.result.exit_code, &stdout, &stderr);
    let output = ToolOutput::new(output).map_err(|_| ToolError::Internal)?;
    Ok(ToolExecutionOutcome::Completed(output))
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
        result = &mut resolve => result.map_err(super::map_source_error),
    }
}

/// One captured stream and the sanitized text the model sees of it.
struct Stream<'a> {
    name: &'static str,
    captured: &'a [u8],
    /// Bytes the capture dropped before `captured`.
    omitted: u64,
    text: String,
}

impl<'a> Stream<'a> {
    fn new(name: &'static str, captured: &'a [u8], omitted: u64) -> Self {
        Self {
            name,
            captured,
            omitted,
            text: sanitized(captured),
        }
    }

    fn lines(&self) -> usize {
        self.text.split_terminator('\n').count()
    }

    /// The text, or its tail followed by a notice that names the saved output.
    async fn render(&self, (max_lines, max_bytes): (usize, usize), incomplete: bool) -> String {
        let mut section = match truncate_tail(&self.text, max_lines, max_bytes) {
            None => self.text.clone(),
            Some(tail) => {
                let path = save_output(self.captured, self.name).await;
                let notice = notice(&tail, path.as_deref(), self.omitted > 0);
                format!("{}\n\n{notice}", tail.text)
            }
        };
        if incomplete {
            if !section.is_empty() && !section.ends_with('\n') {
                section.push('\n');
            }
            section.push_str(INCOMPLETE);
        }
        section
    }
}

fn format_output(exit_code: Option<i32>, stdout: &str, stderr: &str) -> String {
    let exit_code = exit_code.map_or_else(|| "unavailable".to_owned(), |code| code.to_string());
    let mut output = format!("exit_code: {exit_code}\nstdout:\n{stdout}");
    if !output.ends_with('\n') {
        output.push('\n');
    }
    output.push_str("stderr:\n");
    output.push_str(stderr);
    output
}

fn notice(tail: &Tail, path: Option<&Path>, capture_lost: bool) -> String {
    let end = tail.total_lines;
    let start = end - tail.kept_lines + 1;
    let mut notice = if tail.partial {
        format!(
            "[Showing last {} of line {end} (line is {})",
            format_size(tail.text.len()),
            format_size(tail.last_line_bytes)
        )
    } else if tail.by_bytes {
        format!(
            "[Showing lines {start}-{end} of {end} ({} limit)",
            format_size(MAX_OUTPUT_BYTES)
        )
    } else {
        format!("[Showing lines {start}-{end} of {end}")
    };
    notice.push('.');
    let label = if capture_lost {
        notice.push_str(&format!(
            " Output beyond the last {} was not captured.",
            format_size(MAX_COMMAND_OUTPUT)
        ));
        "Captured output"
    } else {
        "Full output"
    };
    if let Some(path) = path {
        notice.push_str(&format!(" {label}: {}", path.display()));
    }
    notice.push(']');
    notice
}

/// Writes the captured bytes to a new private file in the OS temp directory.
/// `None` when that fails, so a notice never names a file that does not exist.
async fn save_output(bytes: &[u8], stream: &str) -> Option<PathBuf> {
    let mut id = [0; 8];
    getrandom::fill(&mut id).ok()?;
    let path = std::env::temp_dir().join(format!(
        "minicore-bash-{:016x}-{stream}.log",
        u64::from_le_bytes(id)
    ));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&path).await.ok()?;
    if file.write_all(bytes).await.is_err() || file.flush().await.is_err() {
        drop(file);
        let _ = tokio::fs::remove_file(&path).await;
        return None;
    }
    Some(path)
}

fn sanitized(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        for character in chunk.valid().chars() {
            if !character.is_control() || matches!(character, '\n' | '\t') {
                output.push(character);
            } else {
                output.extend(character.escape_default());
            }
        }
        if !chunk.invalid().is_empty() {
            output.push('\u{fffd}');
        }
    }
    output
}

fn default_cwd() -> String {
    ".".to_owned()
}

const fn default_timeout() -> u64 {
    DEFAULT_COMMAND_TIMEOUT
}

#[cfg(test)]
mod tests;
