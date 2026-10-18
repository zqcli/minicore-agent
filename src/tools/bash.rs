use std::ffi::OsString;
use std::sync::Arc;
use std::time::{Duration, Instant};

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use minicore_runtime::value::MAX_TEXT_BYTES;
use serde::Deserialize;
use serde_json::json;
use tokio::process::Command;

use crate::tool_data::{CommandStatus, ToolRef};
use crate::{Workspace, WorkspaceError};

use super::command::{Capture, CommandBinding, CommandOutcome, CommandOwners, CommandRequest};
use super::{DEFAULT_COMMAND_TIMEOUT, MAX_COMMAND_OUTPUT, MAX_COMMAND_TIMEOUT, precheck_control};

/// Bounded model-facing prefix of one stream, kept separately from the
/// streaming window in `tool_data`.
struct StreamCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

const TOOL_NAME: &str = "bash";
const STREAM_CAPTURE_LIMIT: usize = MAX_COMMAND_OUTPUT / 2;
const TRUNCATED: &str = "[truncated]";
const FORMATTED_OUTPUT_LIMIT: usize = if MAX_COMMAND_OUTPUT < MAX_TEXT_BYTES {
    MAX_COMMAND_OUTPUT
} else {
    MAX_TEXT_BYTES
};

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

    #[cfg(test)]
    pub(crate) fn names(&self) -> &[OsString] {
        &self.removed
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
    owners: Arc<CommandOwners>,
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
    #[cfg(test)]
    pub(super) fn new(workspace: Arc<Workspace>, environment: CommandEnvironment) -> Self {
        Self::with_binding(workspace, environment, None)
    }

    pub(crate) fn with_binding(
        workspace: Arc<Workspace>,
        environment: CommandEnvironment,
        binding: Option<CommandBinding>,
    ) -> Self {
        let owners = binding
            .as_ref()
            .map_or_else(CommandOwners::new, |binding| Arc::clone(binding.owners()));
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
        Self {
            workspace,
            environment,
            owners,
            binding,
            spec,
        }
    }

    /// Waits for every command this tool owns. A standalone tool has no
    /// Session-level join, so its tests use this.
    #[cfg(all(test, unix))]
    pub(super) async fn join_owned_commands(&self) {
        self.owners.join_all().await;
    }

    #[cfg(all(test, unix))]
    pub(super) fn active_owned_commands(&self) -> usize {
        self.owners.active()
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
            let capture = command_capture(&invocation).await?;
            let request = CommandRequest {
                tool_ref,
                command: input.command,
                cwd,
                deadline,
                cancellation: context.cancellation.clone(),
                environment: self.environment.clone(),
                prefix_limit: STREAM_CAPTURE_LIMIT,
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
            outcome_to_result(&outcome)
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
fn outcome_to_result(outcome: &CommandOutcome) -> Result<ToolExecutionOutcome, ToolError> {
    match outcome.result.status {
        CommandStatus::SpawnFailed => return Err(ToolError::Failed),
        CommandStatus::Failed => return Err(ToolError::Internal),
        CommandStatus::Cancelled => return Err(ToolError::Cancelled),
        CommandStatus::TimedOut => return Err(ToolError::TimedOut),
        CommandStatus::Running | CommandStatus::Cancelling => return Err(ToolError::Internal),
        CommandStatus::Exited => {}
    }
    // A completed result whose output was cut short (a stopped descendant, a
    // bounded drain, or an evicted window) is still marked truncated for the
    // model: otherwise the tool text would look like a clean, complete output.
    let incomplete = !outcome.result.output_complete;
    let stdout = StreamCapture {
        bytes: outcome.stdout.clone(),
        truncated: outcome.stdout_truncated || incomplete,
    };
    let stderr = StreamCapture {
        bytes: outcome.stderr.clone(),
        truncated: outcome.stderr_truncated || incomplete,
    };
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
        result = &mut resolve => result.map_err(map_cwd_error),
    }
}

fn format_output(exit_code: Option<i32>, stdout: &StreamCapture, stderr: &StreamCapture) -> String {
    let exit_code = exit_code.map_or_else(|| "unavailable".to_owned(), |code| code.to_string());
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
mod tests;
