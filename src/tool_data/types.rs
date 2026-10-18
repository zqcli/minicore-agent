//! Public tool facts and query wire types.
use super::{INPUT_ENCODING, OUTPUT_ENCODING, STREAM_ENCODING};
use crate::error::AgentError;
use crate::ids::SessionId;
use minicore_runtime::tools::ToolResultOutcome;
use minicore_runtime::{LoopId, ToolCallId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// One model-facing byte channel. `Input` and `Output` carry the request JSON
/// and the model-facing result text; `Stdout` and `Stderr` carry raw process
/// bytes, so their offsets are raw byte offsets and their content is base64.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDataStream {
    Input,
    Output,
    Stdout,
    Stderr,
}

impl ToolDataStream {
    /// Encoding of this stream's `data` text. `input` is canonical JSON,
    /// `output` is recorded UTF-8 text, and the two process streams are raw
    /// bytes carried as base64 with raw-byte offsets.
    pub(crate) const fn encoding(self) -> &'static str {
        match self {
            Self::Input => INPUT_ENCODING,
            Self::Output => OUTPUT_ENCODING,
            Self::Stdout | Self::Stderr => STREAM_ENCODING,
        }
    }

    pub(crate) const fn is_process(self) -> bool {
        matches!(self, Self::Stdout | Self::Stderr)
    }
}

/// Observed availability of one stream's raw bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDataAvailability {
    /// The stream has not been observed yet and may still produce bytes (for
    /// example the output stream of a still-running call). No content exists
    /// to page.
    Pending,
    /// The call is terminal but its bytes were never observed in this process.
    /// The stream is ended, yet no content was recorded; this is distinct from
    /// a real empty result.
    Unavailable,
    /// Metadata and all observed bytes are retained.
    Available,
    /// A prefix is retained; the remainder was never kept.
    Partial,
    /// Raw bytes were evicted under the Session budget.
    Expired,
}

/// Known real execution stages. A free-form `ToolContext.progress` message is
/// only recorded when it maps to one of these; unknown text is ignored so no
/// arbitrary runtime content can reach the structured record or its `Debug`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPhase {
    Reading,
    Writing,
    Matching,
    Committing,
    Running,
}

impl ToolPhase {
    pub(crate) fn from_wire(message: &str) -> Option<Self> {
        match message {
            "reading" => Some(Self::Reading),
            "writing" => Some(Self::Writing),
            "matching" => Some(Self::Matching),
            "committing" => Some(Self::Committing),
            "running" => Some(Self::Running),
            _ => None,
        }
    }
}

/// The lifecycle state of one tool call. Policy waiting is never `Running`:
/// the tool has not been invoked before the policy decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionState {
    Requested,
    AwaitingPolicy,
    Running,
    /// Cancellation was requested and the owner has not reported a terminal
    /// state yet. Not terminal: the real cleanup completion is still pending.
    Cancelling,
    Succeeded,
    Failed,
    Denied,
    Cancelled,
    InputProvided,
}

impl ToolExecutionState {
    pub(crate) const fn from_outcome(outcome: ToolResultOutcome) -> Self {
        match outcome {
            ToolResultOutcome::Success => Self::Succeeded,
            ToolResultOutcome::Failed => Self::Failed,
            ToolResultOutcome::Denied => Self::Denied,
            ToolResultOutcome::Cancelled => Self::Cancelled,
            ToolResultOutcome::InputProvided => Self::InputProvided,
        }
    }

    pub(crate) const fn matches_outcome(self, outcome: ToolResultOutcome) -> bool {
        match outcome {
            ToolResultOutcome::Success => matches!(self, Self::Succeeded),
            ToolResultOutcome::Failed => matches!(self, Self::Failed),
            ToolResultOutcome::Denied => matches!(self, Self::Denied),
            ToolResultOutcome::Cancelled => matches!(self, Self::Cancelled),
            ToolResultOutcome::InputProvided => matches!(self, Self::InputProvided),
        }
    }

    pub(crate) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Denied | Self::Cancelled | Self::InputProvided
        )
    }
}

/// Complete, non-guessable identity of one tool call.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRef {
    pub session_id: SessionId,
    pub loop_id: LoopId,
    pub request_index: u32,
    pub tool_call_id: ToolCallId,
}

/// The target a tool call was directed at. Raw text, not a display line.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolSubject {
    File { path: String },
    Command { script: String, cwd: String },
    Other,
}

impl fmt::Debug for ToolSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File { path } => formatter
                .debug_struct("ToolSubject::File")
                .field("path_bytes", &path.len())
                .finish(),
            Self::Command { script, cwd } => formatter
                .debug_struct("ToolSubject::Command")
                .field("script_bytes", &script.len())
                .field("cwd_bytes", &cwd.len())
                .finish(),
            Self::Other => formatter.write_str("ToolSubject::Other"),
        }
    }
}

/// Bounded raw input preview plus the true observed length.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolInputSummary {
    pub total_bytes: usize,
    pub preview: String,
    pub truncated: bool,
    pub encoding: &'static str,
}

impl fmt::Debug for ToolInputSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInputSummary")
            .field("total_bytes", &self.total_bytes)
            .field("preview_bytes", &self.preview.len())
            .field("truncated", &self.truncated)
            .field("encoding", &self.encoding)
            .finish()
    }
}

/// Structured invocation facts. Published once the validated request reaches
/// the policy boundary, before any approval decision and before the tool's own
/// parse/validation runs. The raw arguments are therefore *requested* input,
/// not proof that the tool accepted them.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolInvocationData {
    pub tool_ref: ToolRef,
    pub name: String,
    pub subject: ToolSubject,
    pub subject_truncated: bool,
    pub input: ToolInputSummary,
}

impl fmt::Debug for ToolInvocationData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInvocationData")
            .field("tool_ref", &self.tool_ref)
            .field("name", &self.name)
            .field("subject", &self.subject)
            .field("subject_truncated", &self.subject_truncated)
            .field("input", &self.input)
            .finish()
    }
}

/// The lifecycle state of one owned command. `Cancelling` records that
/// cancellation was requested; it is not a terminal state, and it is never
/// reported as one before the owner actually observed termination.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Running,
    Cancelling,
    Exited,
    Cancelled,
    TimedOut,
    SpawnFailed,
    Failed,
}

impl CommandStatus {
    pub(crate) const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Cancelling)
    }
}

/// Structured facts about one owned command, kept separately from the Runtime
/// tool outcome: a non-zero exit is a fact, not an RPC error, and a cancelled
/// command is not a successful one.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandResult {
    pub status: CommandStatus,
    /// Only set when an exit code was really observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Terminating signal, when the platform reported one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// True only when real evidence that the controlled group/job is gone was
    /// observed. A dropped handle or a requested kill is not evidence.
    pub termination_confirmed: bool,
    pub stdout_base_offset: u64,
    pub stdout_observed_end: u64,
    pub stderr_base_offset: u64,
    pub stderr_observed_end: u64,
    /// True when both streams reached a real end of output.
    pub output_complete: bool,
    /// True when bytes were dropped or the observation was cut short.
    pub output_truncated: bool,
}

impl fmt::Debug for CommandResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandResult")
            .field("status", &self.status)
            .field("exit_code", &self.exit_code)
            .field("signal", &self.signal)
            .field("termination_confirmed", &self.termination_confirmed)
            .field("stdout_base_offset", &self.stdout_base_offset)
            .field("stdout_observed_end", &self.stdout_observed_end)
            .field("stderr_base_offset", &self.stderr_base_offset)
            .field("stderr_observed_end", &self.stderr_observed_end)
            .field("output_complete", &self.output_complete)
            .field("output_truncated", &self.output_truncated)
            .finish()
    }
}

/// What one accepted chunk changed, so a live consumer and a later `tool.output`
/// page describe the same bytes without re-reading anything.
#[derive(Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct ToolStreamNotice {
    pub(crate) stream: ToolDataStream,
    /// Raw offset of the first byte of this chunk.
    pub(crate) base_offset: u64,
    pub(crate) next_offset: u64,
    pub(crate) observed_end: u64,
    /// True when this chunk pushed earlier bytes out of the window.
    pub(crate) dropped: bool,
    pub(crate) expired: bool,
}

impl fmt::Debug for ToolStreamNotice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolStreamNotice")
            .field("stream", &self.stream)
            .field("base_offset", &self.base_offset)
            .field("next_offset", &self.next_offset)
            .field("observed_end", &self.observed_end)
            .field("dropped", &self.dropped)
            .field("expired", &self.expired)
            .finish()
    }
}

/// One accepted chunk as a live event. The bytes were already stored in the
/// authoritative window, so a consumer that misses this event loses a
/// notification only, never the data itself.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolProcessChunk {
    pub stream: ToolDataStream,
    /// Always `base64` for process streams: the payload is raw bytes and the
    /// offsets count raw bytes, never base64 positions.
    pub encoding: &'static str,
    /// Base64 of exactly the bytes this notice describes; raw offsets are in
    /// `base_offset`/`next_offset` and never count base64 positions.
    pub data: String,
    pub base_offset: u64,
    pub next_offset: u64,
    pub observed_end: u64,
    pub dropped: bool,
    pub expired: bool,
}

impl fmt::Debug for ToolProcessChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProcessChunk")
            .field("stream", &self.stream)
            .field("encoding", &self.encoding)
            .field("base_offset", &self.base_offset)
            .field("next_offset", &self.next_offset)
            .field("observed_end", &self.observed_end)
            .field("dropped", &self.dropped)
            .field("expired", &self.expired)
            .field("data_bytes", &self.data.len())
            .finish()
    }
}

/// Live process facts for one owned command. At least one part is present; the
/// Runtime outcome is never taken from here.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolProcessData {
    pub tool_ref: ToolRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk: Option<ToolProcessChunk>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandResult>,
}

impl fmt::Debug for ToolProcessData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProcessData")
            .field("tool_ref", &self.tool_ref)
            .field("chunk", &self.chunk)
            .field("command", &self.command)
            .finish()
    }
}

/// Durable storage state for one completed tool record's auxiliary files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecordingState {
    /// In-memory facts only, not yet or not eligible to be written to disk.
    MemoryOnly,
    /// Successfully persisted to the auxiliary store.
    Saved,
    /// Persistence failed or timed out; memory records remain intact.
    Failed,
}

/// One tool call's process record. `command` is absent until a command was
/// really owned, so a policy wait is never reported as a running process.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolExecutionData {
    /// Logical rows of the retained model-facing output, never stdout+stderr.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_line_count: Option<usize>,
    pub tool_ref: ToolRef,
    pub name: String,
    pub state: ToolExecutionState,
    /// Last real, whitelisted execution stage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<ToolPhase>,
    /// Set when the tool actually started running, never at the policy
    /// boundary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// The authoritative Runtime `ToolResultOutcome` once terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<ToolResultOutcome>,
    /// Per-stream availability: evicting the input bytes must not make a later
    /// result unqueryable.
    pub input_availability: ToolDataAvailability,
    pub output_availability: ToolDataAvailability,
    pub input_bytes: usize,
    pub result_bytes: usize,
    pub input_truncated: bool,
    pub result_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandResult>,
    pub recording: ToolRecordingState,
}

impl fmt::Debug for ToolExecutionData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExecutionData")
            .field("tool_ref", &self.tool_ref)
            .field("name", &self.name)
            .field("state", &self.state)
            .field("phase", &self.phase)
            .field("started_at", &self.started_at)
            .field("finished_at", &self.finished_at)
            .field("outcome", &self.outcome)
            .field("input_availability", &self.input_availability)
            .field("output_availability", &self.output_availability)
            .field("input_bytes", &self.input_bytes)
            .field("result_bytes", &self.result_bytes)
            .field("input_truncated", &self.input_truncated)
            .field("result_truncated", &self.result_truncated)
            .field("command", &self.command)
            .field("recording", &self.recording)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolReadRequest {
    pub tool_ref: ToolRef,
    #[serde(default)]
    pub display: bool,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl ToolReadRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        crate::read::validate_max_bytes(self.max_bytes)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOutputRequest {
    pub tool_ref: ToolRef,
    pub stream: ToolDataStream,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl ToolOutputRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        crate::read::validate_max_bytes(self.max_bytes)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolReadResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<crate::presentation::ToolDisplay>,
    /// Absent until the invocation boundary was actually reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation: Option<ToolInvocationData>,
    pub execution: ToolExecutionData,
}

impl fmt::Debug for ToolReadResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolReadResult")
            .field("invocation", &self.invocation)
            .field("execution", &self.execution)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolOutputPage {
    pub tool_ref: ToolRef,
    pub stream: ToolDataStream,
    pub encoding: &'static str,
    pub base_offset: u64,
    pub next_offset: u64,
    pub observed_end: u64,
    /// True only when this stream can no longer yield bytes: it is fully
    /// delivered, or its retained window was evicted/truncated.
    pub eof: bool,
    pub truncated: bool,
    pub availability: ToolDataAvailability,
    pub data: String,
}

impl fmt::Debug for ToolOutputPage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolOutputPage")
            .field("tool_ref", &self.tool_ref)
            .field("stream", &self.stream)
            .field("encoding", &self.encoding)
            .field("base_offset", &self.base_offset)
            .field("next_offset", &self.next_offset)
            .field("observed_end", &self.observed_end)
            .field("eof", &self.eof)
            .field("truncated", &self.truncated)
            .field("availability", &self.availability)
            .field("data_bytes", &self.data.len())
            .finish()
    }
}
