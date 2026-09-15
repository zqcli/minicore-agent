//! Structured tool invocation and execution facts (spec §3.1 and §7).
//!
//! Identity is the complete [`ToolRef`]: session, loop, request, and tool-call
//! id. A tool name, path, command, PID, or "most recent call" is never used as
//! a key, because all of those can repeat.
//!
//! Raw input JSON and raw result text are data, not log text. They are
//! bounded, never rewritten through `escape_default`, and never appear in
//! `Debug` or `tracing`. Records live in memory for one loaded Session under a
//! fixed record/byte budget; queries report the availability and retention
//! they actually observed instead of claiming a complete object.
//!
//! This module owns the model-facing per-tool facts plus bounded native file
//! change metadata. Bash stdout/stderr streaming, process ownership, and
//! durable auxiliary files retain their own real consumers from P5.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Mutex;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use minicore_runtime::history::HistoryItem;
use minicore_runtime::tools::{ToolInvocation, ToolResultOutcome};
use minicore_runtime::{LoopId, ToolCallId};

use crate::changes::{CHANGE_METADATA_BYTES, ChangeRecord, FileChange};
use crate::error::AgentError;
use crate::ids::SessionId;
use crate::store::{
    StoredInputSummary, StoredResultSummary, StoredStreamWindow, StoredToolRecord,
    TOOL_RECORD_FORMAT_VERSION,
};

/// Maximum retained records for one loaded Session.
pub(crate) const MAX_TOOL_RECORDS: usize = 1024;
/// Maximum retained bytes (raw data plus bounded metadata) per Session.
pub(crate) const MAX_TOOL_TOTAL_BYTES: usize = 8 * 1024 * 1024;
/// Retained raw input per tool call. Runtime already validates arguments
/// against `MAX_JSON_BYTES`, so this is a defensive copy bound.
pub(crate) const MAX_TOOL_INPUT_BYTES: usize = 64 * 1024;
/// Retained raw result text per tool call.
pub(crate) const MAX_TOOL_RESULT_BYTES: usize = 256 * 1024;
/// Preview returned by `tool.read`.
pub(crate) const MAX_INVOCATION_PREVIEW_BYTES: usize = 8 * 1024;
/// Preview carried by the best-effort `tool_invocation` event.
pub(crate) const EVENT_INPUT_PREVIEW_BYTES: usize = 2 * 1024;
/// Retained subject text (path/script/cwd) per tool call.
pub(crate) const MAX_SUBJECT_BYTES: usize = 4 * 1024;
/// Retained tail window per process stream. This is the window a client pages
/// through; the model-facing result prefix has its own separate budget.
pub(crate) const MAX_TOOL_STREAM_BYTES: usize = 1024 * 1024;
/// Largest raw chunk a process reader hands to the store in one call.
pub(crate) const MAX_TOOL_CHUNK_BYTES: usize = 32 * 1024;

const INPUT_ENCODING: &str = "utf8_json";
const OUTPUT_ENCODING: &str = "utf8";
const STREAM_ENCODING: &str = "base64";
/// Bounded size one retained `CommandResult` contributes to the Session
/// budget, so metadata is counted rather than assumed free.
const COMMAND_METADATA_BYTES: usize = 128;

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

/// A bounded tail window of one raw process stream. `start_offset` is the raw
/// offset of the first retained byte, so eviction moves `base_offset` forward
/// instead of renumbering bytes; `observed_end` counts every byte the owner
/// really read, whether or not it is still retained.
#[derive(Clone, Default)]
pub(crate) struct StreamWindow {
    bytes: Vec<u8>,
    start_offset: u64,
    observed_end: u64,
    seen: bool,
    /// The stream reached a real end of output.
    complete: bool,
    /// Bytes were dropped, either by the window or by an eviction.
    truncated: bool,
    /// The retained bytes were freed under the Session budget.
    expired: bool,
    /// The backing blob was corrupt or missing on disk during cold read.
    corrupt: bool,
}

/// What one pushed chunk changed, measured against the raw stream.
struct StreamPush {
    base_offset: u64,
    next_offset: u64,
    dropped: bool,
}

impl StreamWindow {
    /// Appends one chunk and evicts the oldest bytes beyond the window.
    fn push(&mut self, chunk: &[u8]) -> StreamPush {
        self.seen = true;
        if self.bytes.is_empty() {
            // Nothing is retained (first byte, or retention was evicted), so
            // the retained window starts where this chunk starts.
            self.start_offset = self.observed_end;
        }
        let base_offset = self.observed_end;
        self.observed_end = self.observed_end.saturating_add(chunk.len() as u64);
        if chunk.is_empty() {
            return StreamPush {
                base_offset,
                next_offset: self.observed_end,
                dropped: false,
            };
        }
        self.expired = false;
        let old_len = self.bytes.len();
        let total = old_len.saturating_add(chunk.len());
        let dropped;
        if total <= MAX_TOOL_STREAM_BYTES {
            dropped = false;
            if total > self.bytes.capacity() {
                let target = (self.bytes.capacity().saturating_mul(2))
                    .max(total)
                    .min(MAX_TOOL_STREAM_BYTES);
                let additional = target - old_len;
                self.bytes.reserve_exact(additional);
            }
            self.bytes.extend_from_slice(chunk);
        } else {
            let excess = total - MAX_TOOL_STREAM_BYTES;
            self.start_offset = self.start_offset.saturating_add(excess as u64);
            self.truncated = true;
            dropped = true;
            let old_dropped = excess.min(old_len);
            let chunk_dropped = excess - old_dropped;
            if old_dropped == old_len {
                self.bytes.clear();
            } else if old_dropped > 0 {
                self.bytes.drain(..old_dropped);
            }
            let chunk_slice = &chunk[chunk_dropped..];
            if self.bytes.capacity() < MAX_TOOL_STREAM_BYTES {
                let additional = MAX_TOOL_STREAM_BYTES - self.bytes.len();
                self.bytes.reserve_exact(additional);
            }
            self.bytes.extend_from_slice(chunk_slice);
        }
        StreamPush {
            base_offset,
            next_offset: self.observed_end,
            dropped,
        }
    }

    fn retained(&self) -> usize {
        if self.expired {
            0
        } else {
            self.bytes.capacity()
        }
    }

    /// The retained range as `(base_offset, observed_end)`, even when nothing
    /// is retained or the window was evicted. The observed end never moves
    /// backwards, so a live command record can report progress while running.
    fn live_range(&self) -> (u64, u64) {
        (self.start_offset, self.observed_end)
    }

    /// The retained bytes from one raw offset, when that offset is still
    /// inside the window.
    fn page(&self, offset: u64) -> Option<&[u8]> {
        if self.expired || offset < self.start_offset {
            return None;
        }
        let index = usize::try_from(offset - self.start_offset).ok()?;
        self.bytes.get(index..)
    }

    fn evict(&mut self) {
        self.bytes = Vec::new();
        self.start_offset = self.observed_end;
        self.expired = true;
        self.truncated = true;
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

#[derive(Clone)]
pub(crate) struct ToolRecord {
    name: String,
    subject: ToolSubject,
    subject_truncated: bool,
    input: String,
    input_total: usize,
    input_seen: bool,
    input_truncated: bool,
    input_expired: bool,
    result: String,
    result_total: usize,
    result_seen: bool,
    result_truncated: bool,
    result_expired: bool,
    state: ToolExecutionState,
    phase: Option<ToolPhase>,
    started_at: Option<String>,
    finished_at: Option<String>,
    outcome: Option<ToolResultOutcome>,
    /// Bounded tail windows of the process streams; empty until a command
    /// really owned a process.
    stdout: StreamWindow,
    stderr: StreamWindow,
    /// The structured process record; absent until a command was owned.
    command: Option<CommandResult>,
    /// One native file mutation, if the bound tool reached its mutation seam.
    file_change: Option<FileChange>,
    input_corrupt: bool,
    result_corrupt: bool,
    recording: ToolRecordingState,
}

impl ToolRecord {
    pub(crate) fn has_corrupt_streams(&self) -> bool {
        self.input_corrupt || self.result_corrupt || self.stdout.corrupt || self.stderr.corrupt
    }

    pub(crate) fn is_terminal(&self) -> bool {
        if !self.state.is_terminal() {
            return false;
        }
        if let Some(cmd) = &self.command {
            if !cmd.status.is_terminal() {
                return false;
            }
        }
        true
    }

    pub(crate) fn needs_stored(&self) -> bool {
        self.is_terminal()
            && (self.input_expired
                || self.result_expired
                || self.stdout.expired
                || self.stderr.expired
                || !self.input_seen
                || !self.result_seen
                || self
                    .file_change
                    .as_ref()
                    .is_some_and(FileChange::needs_stored)
                || self.has_corrupt_streams())
    }

    pub(crate) fn merge_stored(&mut self, stored: ToolRecord) -> bool {
        // 1. Conflict checks: refuse merge on any identity or observed range conflict.
        if self.name != stored.name {
            return false;
        }
        if self.state.is_terminal() && stored.state.is_terminal() && self.state != stored.state {
            return false;
        }
        if self.outcome.is_some() && stored.outcome.is_some() && self.outcome != stored.outcome {
            return false;
        }
        if self.input_seen && stored.input_seen && self.input_total != stored.input_total {
            return false;
        }
        if self.result_seen && stored.result_seen && self.result_total != stored.result_total {
            return false;
        }
        let known = |stream: &StreamWindow| {
            stream.seen || stream.complete || stream.expired || stream.observed_end > 0
        };
        for (memory, disk) in [
            (&self.stdout, &stored.stdout),
            (&self.stderr, &stored.stderr),
        ] {
            if known(memory) && known(disk) && memory.observed_end != disk.observed_end {
                return false;
            }
        }
        if self
            .command
            .as_ref()
            .zip(stored.command.as_ref())
            .is_some_and(|(memory, disk)| {
                (
                    memory.status,
                    memory.exit_code,
                    memory.signal,
                    memory.termination_confirmed,
                    memory.output_complete,
                ) != (
                    disk.status,
                    disk.exit_code,
                    disk.signal,
                    disk.termination_confirmed,
                    disk.output_complete,
                )
            })
        {
            return false;
        }
        if self
            .file_change
            .as_ref()
            .zip(stored.file_change.as_ref())
            .is_some_and(|(memory, disk)| memory.stored() != disk.stored())
        {
            return false;
        }

        if !self.input_seen && self.started_at.is_none() {
            self.phase = self.phase.or(stored.phase);
            self.started_at = stored.started_at;
            if stored.finished_at.is_some() {
                self.finished_at = stored.finished_at;
            }
        }

        // 2. Input
        if !self.input_seen {
            if stored.input_seen {
                self.input = stored.input;
                self.input_total = stored.input_total;
                self.input_seen = true;
                self.input_truncated = stored.input_truncated;
                self.input_expired = stored.input_expired;
                self.input_corrupt = stored.input_corrupt;
                self.subject = stored.subject;
                self.subject_truncated = stored.subject_truncated;
            }
        } else if self.input_expired || self.input_corrupt {
            if stored.input_seen && !stored.input_expired && !stored.input_corrupt {
                self.input = stored.input;
                self.input_total = stored.input_total;
                self.input_truncated = stored.input_truncated;
                self.input_expired = false;
                self.input_corrupt = false;
            } else if stored.input_corrupt {
                self.input_corrupt = true;
                self.input_total = stored.input_total;
            }
        }

        // 3. Result
        if !self.result_seen {
            if stored.result_seen {
                self.result = stored.result;
                self.result_total = stored.result_total;
                self.result_seen = true;
                self.result_truncated = stored.result_truncated;
                self.result_expired = stored.result_expired;
                self.result_corrupt = stored.result_corrupt;
            }
        } else if self.result_expired || self.result_corrupt {
            if stored.result_seen && !stored.result_expired && !stored.result_corrupt {
                self.result = stored.result;
                self.result_total = stored.result_total;
                self.result_truncated = stored.result_truncated;
                self.result_expired = false;
                self.result_corrupt = false;
            } else if stored.result_corrupt {
                self.result_corrupt = true;
                self.result_total = stored.result_total;
            }
        }

        // 4. Process streams (stdout / stderr)
        let stdout_unknown =
            !self.stdout.seen && !self.stdout.complete && self.stdout.observed_end == 0;
        if stdout_unknown {
            self.stdout = stored.stdout;
        } else if self.stdout.expired || self.stdout.corrupt {
            if (stored.stdout.seen || stored.stdout.complete)
                && !stored.stdout.expired
                && !stored.stdout.corrupt
            {
                self.stdout = stored.stdout;
            } else if stored.stdout.corrupt {
                self.stdout.corrupt = true;
                if stored.stdout.observed_end > 0 {
                    self.stdout.observed_end = stored.stdout.observed_end;
                    self.stdout.start_offset = stored.stdout.start_offset;
                }
            }
        } else if !self.stdout.complete && stored.stdout.complete {
            self.stdout.complete = true;
        }

        let stderr_unknown =
            !self.stderr.seen && !self.stderr.complete && self.stderr.observed_end == 0;
        if stderr_unknown {
            self.stderr = stored.stderr;
        } else if self.stderr.expired || self.stderr.corrupt {
            if (stored.stderr.seen || stored.stderr.complete)
                && !stored.stderr.expired
                && !stored.stderr.corrupt
            {
                self.stderr = stored.stderr;
            } else if stored.stderr.corrupt {
                self.stderr.corrupt = true;
                if stored.stderr.observed_end > 0 {
                    self.stderr.observed_end = stored.stderr.observed_end;
                    self.stderr.start_offset = stored.stderr.start_offset;
                }
            }
        } else if !self.stderr.complete && stored.stderr.complete {
            self.stderr.complete = true;
        }

        // 5. Command record
        if self.command.is_none() {
            if let Some(cmd) = stored.command {
                self.command = Some(self.live_command(cmd));
            }
        } else if let Some(cmd) = self.command.take() {
            self.command = Some(self.live_command(cmd));
        }

        // 6. File change snapshots. Metadata from memory wins; disk can only
        // fill an evicted or corrupt blob for the same immutable change.
        if self.file_change.is_none() {
            self.file_change = stored.file_change;
        } else if let (Some(memory), Some(mut disk)) =
            (self.file_change.as_mut(), stored.file_change)
        {
            if memory.before_bytes.is_none() && !memory.before_corrupt {
                memory.before_bytes = disk.before_bytes.take();
                memory.before_corrupt = disk.before_corrupt;
            }
            if memory.after_bytes.is_none() && !memory.after_corrupt {
                memory.after_bytes = disk.after_bytes.take();
                memory.after_corrupt = disk.after_corrupt;
            }
        }

        // 7. Recording state
        if self.recording == ToolRecordingState::MemoryOnly
            && stored.recording != ToolRecordingState::MemoryOnly
        {
            self.recording = stored.recording;
        }

        true
    }

    fn new(name: String) -> Self {
        Self {
            name,
            subject: ToolSubject::Other,
            subject_truncated: false,
            input: String::new(),
            input_total: 0,
            input_seen: false,
            input_truncated: false,
            input_expired: false,
            result: String::new(),
            result_total: 0,
            result_seen: false,
            result_truncated: false,
            result_expired: false,
            state: ToolExecutionState::Requested,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: None,
            stdout: StreamWindow::default(),
            stderr: StreamWindow::default(),
            command: None,
            file_change: None,
            input_corrupt: false,
            result_corrupt: false,
            recording: ToolRecordingState::MemoryOnly,
        }
    }

    fn stream(&self, stream: ToolDataStream) -> Option<&StreamWindow> {
        match stream {
            ToolDataStream::Stdout => Some(&self.stdout),
            ToolDataStream::Stderr => Some(&self.stderr),
            ToolDataStream::Input | ToolDataStream::Output => None,
        }
    }

    fn stream_mut(&mut self, stream: ToolDataStream) -> Option<&mut StreamWindow> {
        match stream {
            ToolDataStream::Stdout => Some(&mut self.stdout),
            ToolDataStream::Stderr => Some(&mut self.stderr),
            ToolDataStream::Input | ToolDataStream::Output => None,
        }
    }

    /// Counts retained raw bytes and bounded metadata, so the Session budget
    /// matches what is actually held in memory.
    fn retained_bytes(&self) -> usize {
        let command = if self.command.is_some() {
            COMMAND_METADATA_BYTES
        } else {
            0
        };
        let file_change = self.file_change.as_ref().map_or(0, |change| {
            CHANGE_METADATA_BYTES
                .saturating_add(change.path.len())
                .saturating_add(
                    change
                        .before_bytes
                        .as_ref()
                        .map_or(0, |bytes| bytes.capacity())
                        .saturating_add(
                            change
                                .after_bytes
                                .as_ref()
                                .map_or(0, |bytes| bytes.capacity()),
                        ),
                )
        });
        self.name
            .len()
            .saturating_add(subject_len(&self.subject))
            .saturating_add(self.input.len())
            .saturating_add(self.result.len())
            .saturating_add(self.stdout.retained())
            .saturating_add(self.stderr.retained())
            .saturating_add(file_change)
            .saturating_add(command)
    }

    fn input_availability(&self) -> ToolDataAvailability {
        if self.input_corrupt {
            return ToolDataAvailability::Unavailable;
        }
        stream_availability(
            self.input_seen,
            self.input_expired,
            self.state.is_terminal(),
            self.input.len(),
            self.input_total,
            self.input_truncated,
        )
    }

    fn output_availability(&self) -> ToolDataAvailability {
        if self.result_corrupt {
            return ToolDataAvailability::Unavailable;
        }
        stream_availability(
            self.result_seen,
            self.result_expired,
            self.state.is_terminal(),
            self.result.len(),
            self.result_total,
            self.result_truncated,
        )
    }

    /// Frees the retained input bytes and marks the stream expired. Capacity is
    /// dropped, not just logically cleared.
    fn evict_input(&mut self) {
        self.input = String::new();
        self.input_expired = true;
    }

    fn evict_result(&mut self) {
        self.result = String::new();
        self.result_expired = true;
    }

    /// Stream availability for a process stream. A running stream is `Pending`
    /// until its first byte, and never claims an end of output it has not seen.
    /// A stream the owner really ended is `Available` even with zero bytes (a
    /// real empty end); one that was cut short is `Partial`.
    fn stream_availability(&self, stream: ToolDataStream) -> ToolDataAvailability {
        let Some(window) = self.stream(stream) else {
            return ToolDataAvailability::Unavailable;
        };
        if window.corrupt {
            return ToolDataAvailability::Unavailable;
        }
        if window.expired {
            return ToolDataAvailability::Expired;
        }
        if window.truncated {
            return ToolDataAvailability::Partial;
        }
        if window.seen || window.complete {
            return ToolDataAvailability::Available;
        }
        if self.stream_final() {
            ToolDataAvailability::Unavailable
        } else {
            ToolDataAvailability::Pending
        }
    }

    /// True when no more bytes can arrive for either process stream. A Runtime
    /// terminal outcome alone is not enough: a dropped tool future can leave
    /// the owner still draining, so the owner's own terminal command record (or
    /// the absence of any command record on a terminal call) decides.
    fn stream_final(&self) -> bool {
        match &self.command {
            Some(command) => command.status.is_terminal(),
            None => self.state.is_terminal(),
        }
    }

    fn execution_data(&self, tool_ref: &ToolRef) -> ToolExecutionData {
        ToolExecutionData {
            tool_ref: tool_ref.clone(),
            name: self.name.clone(),
            state: self.state,
            phase: self.phase,
            started_at: self.started_at.clone(),
            finished_at: self.finished_at.clone(),
            outcome: self.outcome,
            input_availability: self.input_availability(),
            output_availability: self.output_availability(),
            input_bytes: self.input_total,
            result_bytes: self.result_total,
            input_truncated: self.input_truncated || self.input.len() < self.input_total,
            result_truncated: self.result_truncated || self.result.len() < self.result_total,
            command: self
                .command
                .clone()
                .map(|command| self.live_command(command)),
            recording: self.recording,
        }
    }

    /// Overlays the ranges and retention flags of the *current* stream windows
    /// on the stored command record. A record stored at start or at the end
    /// must not make a running command report empty ranges while
    /// `tool.output` already returns bytes, and a complete EOF must not hide
    /// that the retained tail was evicted.
    fn live_command(&self, mut command: CommandResult) -> CommandResult {
        let (stdout_base, stdout_end) = self.stdout.live_range();
        let (stderr_base, stderr_end) = self.stderr.live_range();
        command.stdout_base_offset = stdout_base;
        command.stdout_observed_end = stdout_end;
        command.stderr_base_offset = stderr_base;
        command.stderr_observed_end = stderr_end;
        // Real end of output and complete retention are different facts:
        // `output_complete` is the owner's end-of-observation decision, while
        // `output_truncated` also covers a dropped tail window.
        command.output_truncated = command.output_truncated
            || self.stdout.truncated
            || self.stderr.truncated
            || self.stdout.expired
            || self.stderr.expired;
        command
    }

    pub(crate) fn project_read(
        &self,
        tool_ref: &ToolRef,
        max_bytes: usize,
    ) -> Result<ToolReadResult, AgentError> {
        let execution = self.execution_data(tool_ref);
        let invocation = (self.input_seen || self.input_expired).then(|| {
            let (preview, cut) = truncate_prefix(&self.input, MAX_INVOCATION_PREVIEW_BYTES);
            ToolInvocationData {
                tool_ref: tool_ref.clone(),
                name: self.name.clone(),
                subject: self.subject.clone(),
                subject_truncated: self.subject_truncated,
                input: ToolInputSummary {
                    total_bytes: self.input_total,
                    preview,
                    truncated: cut
                        || self.input_truncated
                        || self.input_expired
                        || self.input_corrupt,
                    encoding: INPUT_ENCODING,
                },
            }
        });
        let mut result = ToolReadResult {
            invocation,
            execution,
        };
        if encoded_len(&result)? > max_bytes {
            let Some(source) = result.invocation.as_ref().map(|i| i.input.preview.clone()) else {
                return Err(AgentError::InvalidArguments);
            };
            let mut template = result.clone();
            if let Some(invocation) = template.invocation.as_mut() {
                invocation.input.preview.clear();
                invocation.input.truncated = true;
            }
            let template_len = encoded_len(&template)?;
            if template_len > max_bytes {
                return Err(AgentError::InvalidArguments);
            }
            // The template already encodes the empty preview's two quotes.
            let available = max_bytes.saturating_sub(template_len).saturating_add(2);
            let (preview, _) = fit_encoded_string(&source, available);
            if let Some(invocation) = result.invocation.as_mut() {
                invocation.input.preview = preview;
                invocation.input.truncated = true;
            }
        }
        Ok(result)
    }

    pub(crate) fn project_output(
        &self,
        request: &ToolOutputRequest,
        max_bytes: usize,
    ) -> Result<ToolOutputPage, AgentError> {
        let stream = request.stream;
        if stream.is_process() {
            return output_process(self, request, max_bytes);
        }
        let (content, observed, seen, expired, truncated) = match stream {
            ToolDataStream::Input => (
                self.input.as_str(),
                self.input_total,
                self.input_seen,
                self.input_expired,
                self.input_truncated,
            ),
            ToolDataStream::Output => (
                self.result.as_str(),
                self.result_total,
                self.result_seen,
                self.result_expired,
                self.result_truncated,
            ),
            ToolDataStream::Stdout | ToolDataStream::Stderr => {
                return output_process(self, request, max_bytes);
            }
        };
        let availability = stream_availability(
            seen,
            expired,
            self.state.is_terminal(),
            content.len(),
            observed,
            truncated,
        );
        let base = ToolOutputPage {
            tool_ref: request.tool_ref.clone(),
            stream,
            encoding: stream.encoding(),
            base_offset: 0,
            next_offset: 0,
            observed_end: observed as u64,
            eof: false,
            truncated: false,
            availability,
            data: String::new(),
        };
        if expired {
            return bounded_page(
                ToolOutputPage {
                    base_offset: observed as u64,
                    next_offset: observed as u64,
                    eof: true,
                    truncated: true,
                    ..base
                },
                max_bytes,
            );
        }
        if !seen {
            if request.offset != 0 {
                return Err(AgentError::InvalidArguments);
            }
            return bounded_page(
                ToolOutputPage {
                    eof: availability != ToolDataAvailability::Pending,
                    ..base
                },
                max_bytes,
            );
        }
        if request.offset > observed as u64 {
            return Err(AgentError::InvalidArguments);
        }
        let offset = usize::try_from(request.offset).unwrap_or(usize::MAX);
        let retained = content.len();
        if offset > retained {
            return bounded_page(
                ToolOutputPage {
                    base_offset: request.offset,
                    next_offset: observed as u64,
                    eof: true,
                    truncated: true,
                    availability: ToolDataAvailability::Partial,
                    ..base
                },
                max_bytes,
            );
        }
        if !content.is_char_boundary(offset) {
            return Err(AgentError::InvalidArguments);
        }
        let template = ToolOutputPage {
            base_offset: offset as u64,
            next_offset: u64::MAX,
            ..base.clone()
        };
        let template_len = encoded_len(&template)?;
        if template_len > max_bytes {
            return Err(AgentError::InvalidArguments);
        }
        let available = max_bytes.saturating_sub(template_len).saturating_add(2);
        let (data, _cut) = fit_encoded_string(&content[offset..], available);
        if data.is_empty() && offset < retained {
            return Err(AgentError::InvalidArguments);
        }
        let next = offset.saturating_add(data.len());
        let lossy = retained < observed || truncated;
        let eof = next >= retained;
        bounded_page(
            ToolOutputPage {
                base_offset: offset as u64,
                next_offset: next as u64,
                eof,
                truncated: lossy,
                data,
                ..base
            },
            max_bytes,
        )
    }

    pub(crate) fn from_stored(
        stored: StoredToolRecord,
        (input_bytes, input_corrupt): (Option<Vec<u8>>, bool),
        (result_bytes, result_corrupt): (Option<Vec<u8>>, bool),
        (stdout_bytes, stdout_corrupt): (Option<Vec<u8>>, bool),
        (stderr_bytes, stderr_corrupt): (Option<Vec<u8>>, bool),
        (file_change_before, file_change_before_corrupt): (Option<Vec<u8>>, bool),
        (file_change_after, file_change_after_corrupt): (Option<Vec<u8>>, bool),
    ) -> Self {
        let mut input_corrupt = input_corrupt;
        let input = match input_bytes {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => {
                    input_corrupt = true;
                    String::new()
                }
            },
            None => String::new(),
        };
        let mut result_corrupt = result_corrupt;
        let result = match result_bytes {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => {
                    result_corrupt = true;
                    String::new()
                }
            },
            None => String::new(),
        };
        let stdout = StreamWindow {
            bytes: stdout_bytes.unwrap_or_default(),
            start_offset: stored.stdout.start_offset,
            observed_end: stored.stdout.observed_end,
            seen: stored.stdout.seen,
            complete: stored.stdout.complete,
            truncated: stored.stdout.truncated,
            expired: stored.stdout.expired,
            corrupt: stdout_corrupt,
        };
        let stderr = StreamWindow {
            bytes: stderr_bytes.unwrap_or_default(),
            start_offset: stored.stderr.start_offset,
            observed_end: stored.stderr.observed_end,
            seen: stored.stderr.seen,
            complete: stored.stderr.complete,
            truncated: stored.stderr.truncated,
            expired: stored.stderr.expired,
            corrupt: stderr_corrupt,
        };
        let file_change = stored.file_change.map(|change| {
            FileChange::from_stored(
                change,
                file_change_before,
                file_change_before_corrupt,
                file_change_after,
                file_change_after_corrupt,
            )
        });
        Self {
            name: stored.name,
            subject: stored.subject,
            subject_truncated: stored.subject_truncated,
            input,
            input_total: stored.input.total_bytes,
            input_seen: stored.input.seen,
            input_truncated: stored.input.truncated,
            input_expired: stored.input.expired,
            input_corrupt,
            result,
            result_total: stored.result.total_bytes,
            result_seen: stored.result.seen,
            result_truncated: stored.result.truncated,
            result_expired: stored.result.expired,
            result_corrupt,
            state: stored.state,
            phase: stored.phase,
            started_at: stored.started_at,
            finished_at: stored.finished_at,
            outcome: stored.outcome,
            stdout,
            stderr,
            command: stored.command,
            file_change,
            recording: ToolRecordingState::Saved,
        }
    }
}

/// A snapshot of one completed tool call's metadata and retained buffers,
/// bounded per tool to <= 3 MiB for durable storage.
#[derive(Clone)]
pub(crate) struct ToolPersistenceSnapshot {
    pub(crate) tool_ref: ToolRef,
    pub(crate) record: StoredToolRecord,
    pub(crate) input_bytes: Option<Vec<u8>>,
    pub(crate) result_bytes: Option<Vec<u8>>,
    pub(crate) stdout_bytes: Option<Vec<u8>>,
    pub(crate) stderr_bytes: Option<Vec<u8>>,
    pub(crate) file_change_before: Option<Vec<u8>>,
    pub(crate) file_change_after: Option<Vec<u8>>,
}

impl fmt::Debug for ToolPersistenceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolPersistenceSnapshot")
            .field("tool_ref", &self.tool_ref)
            .field("record", &self.record)
            .field(
                "input_bytes_len",
                &self.input_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "result_bytes_len",
                &self.result_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "stdout_bytes_len",
                &self.stdout_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "stderr_bytes_len",
                &self.stderr_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "file_change_before_len",
                &self.file_change_before.as_ref().map(|b| b.len()),
            )
            .field(
                "file_change_after_len",
                &self.file_change_after.as_ref().map(|b| b.len()),
            )
            .finish()
    }
}

fn subject_len(subject: &ToolSubject) -> usize {
    match subject {
        ToolSubject::File { path } => path.len(),
        ToolSubject::Command { script, cwd } => script.len().saturating_add(cwd.len()),
        ToolSubject::Other => 0,
    }
}

fn stream_availability(
    seen: bool,
    expired: bool,
    terminal: bool,
    retained: usize,
    total: usize,
    truncated: bool,
) -> ToolDataAvailability {
    if expired {
        ToolDataAvailability::Expired
    } else if seen {
        if truncated || retained < total {
            ToolDataAvailability::Partial
        } else {
            ToolDataAvailability::Available
        }
    } else if terminal {
        ToolDataAvailability::Unavailable
    } else {
        ToolDataAvailability::Pending
    }
}

#[derive(Default)]
struct ToolDataInner {
    records: HashMap<ToolRef, ToolRecord>,
    /// Insertion order; the front is the oldest record.
    order: VecDeque<ToolRef>,
    total_bytes: usize,
}

impl ToolDataInner {
    fn track(&mut self, tool_ref: &ToolRef, name: &str) {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.records.entry(tool_ref.clone())
        {
            let record = ToolRecord::new(name.to_owned());
            self.total_bytes = self.total_bytes.saturating_add(record.retained_bytes());
            entry.insert(record);
            self.order.push_back(tool_ref.clone());
        }
    }

    /// Resizes one record's retained bytes while keeping the Session byte
    /// budget exact.
    fn resize(&mut self, tool_ref: &ToolRef, resize: impl FnOnce(&mut ToolRecord)) {
        let before = self
            .records
            .get(tool_ref)
            .map_or(0, ToolRecord::retained_bytes);
        if let Some(record) = self.records.get_mut(tool_ref) {
            resize(record);
        }
        let after = self
            .records
            .get(tool_ref)
            .map_or(0, ToolRecord::retained_bytes);
        self.total_bytes = self
            .total_bytes
            .saturating_sub(before)
            .saturating_add(after);
    }

    /// Evicts the oldest still-retained stream bytes. Input is evicted before
    /// result, so a result that arrives after its input was evicted stays
    /// queryable. Returns false when only metadata remains.
    fn evict_some_bytes(&mut self) -> bool {
        let input_victim = self
            .order
            .iter()
            .find(|tool_ref| {
                self.records.get(*tool_ref).is_some_and(|record| {
                    !record.input_expired && (!record.input.is_empty() || record.input_total > 0)
                })
            })
            .cloned();
        if let Some(victim) = input_victim {
            let freed = self
                .records
                .get(&victim)
                .map_or(0, |record| record.input.len());
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                record.evict_input();
            }
            return true;
        }
        let victim = self
            .order
            .iter()
            .find(|tool_ref| {
                self.records.get(*tool_ref).is_some_and(|record| {
                    !record.result_expired && (!record.result.is_empty() || record.result_total > 0)
                })
            })
            .cloned();
        if let Some(victim) = victim {
            let freed = self
                .records
                .get(&victim)
                .map_or(0, |record| record.result.len());
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                record.evict_result();
            }
            return true;
        }
        let victim = self.order.iter().find_map(|tool_ref| {
            let record = self.records.get(tool_ref)?;
            let change = record.file_change.as_ref()?;
            let retained = change
                .before_bytes
                .as_ref()
                .map_or(0, |bytes| bytes.capacity())
                .saturating_add(
                    change
                        .after_bytes
                        .as_ref()
                        .map_or(0, |bytes| bytes.capacity()),
                );
            (retained > 0).then(|| tool_ref.clone())
        });
        if let Some(victim) = victim {
            let freed = self.records.get(&victim).map_or(0, |record| {
                record.file_change.as_ref().map_or(0, |change| {
                    change
                        .before_bytes
                        .as_ref()
                        .map_or(0, |bytes| bytes.capacity())
                        .saturating_add(
                            change
                                .after_bytes
                                .as_ref()
                                .map_or(0, |bytes| bytes.capacity()),
                        )
                })
            });
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                if let Some(change) = record.file_change.as_mut() {
                    change.before_bytes = None;
                    change.after_bytes = None;
                }
            }
            return true;
        }
        // Process windows are evicted last, oldest record first, so a live
        // stream keeps its newest bytes as long as any budget remains.
        let victim = self.order.iter().find_map(|tool_ref| {
            let record = self.records.get(tool_ref)?;
            [ToolDataStream::Stdout, ToolDataStream::Stderr]
                .into_iter()
                .find(|stream| {
                    record.stream(*stream).is_some_and(|window| {
                        !window.expired && (!window.bytes.is_empty() || window.observed_end > 0)
                    })
                })
                .map(|stream| (tool_ref.clone(), stream))
        });
        if let Some((victim, stream)) = victim {
            let freed = self
                .records
                .get(&victim)
                .and_then(|record| record.stream(stream))
                .map_or(0, StreamWindow::retained);
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                if let Some(window) = record.stream_mut(stream) {
                    window.evict();
                }
            }
            return true;
        }
        false
    }

    /// Removes the oldest record (preferring a terminal one) to bound the
    /// record count or a metadata-only byte overrun.
    fn remove_oldest(&mut self) -> bool {
        let victim = self
            .order
            .iter()
            .find(|tool_ref| {
                self.records
                    .get(*tool_ref)
                    .is_some_and(|record| record.state.is_terminal())
            })
            .cloned()
            .or_else(|| self.order.front().cloned());
        let Some(victim) = victim else {
            return false;
        };
        if let Some(record) = self.records.remove(&victim) {
            self.total_bytes = self.total_bytes.saturating_sub(record.retained_bytes());
        }
        self.order.retain(|tool_ref| tool_ref != &victim);
        true
    }

    fn enforce_limits(&mut self) {
        while self.total_bytes > MAX_TOOL_TOTAL_BYTES {
            if !self.evict_some_bytes() && !self.remove_oldest() {
                break;
            }
        }
        while self.records.len() > MAX_TOOL_RECORDS {
            if !self.remove_oldest() {
                break;
            }
        }
        debug_assert_eq!(
            self.total_bytes,
            self.records
                .values()
                .map(ToolRecord::retained_bytes)
                .sum::<usize>(),
            "tool data byte accounting drifted"
        );
    }
}

/// Per-Session bounded store of tool invocation/execution facts.
pub(crate) struct ToolData {
    inner: Mutex<ToolDataInner>,
}

impl ToolData {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(ToolDataInner::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ToolDataInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns an isolated copy of one tool call's record from memory if present.
    pub(crate) fn get_record(&self, tool_ref: &ToolRef) -> Option<ToolRecord> {
        let inner = self.lock();
        inner.records.get(tool_ref).cloned()
    }

    /// Records that the Runtime accepted a tool call, before any policy
    /// decision. Arguments are not available at this boundary.
    pub(crate) fn note_requested(&self, tool_ref: &ToolRef, name: &str) {
        let mut inner = self.lock();
        inner.track(tool_ref, name);
        inner.enforce_limits();
    }

    /// Publishes the requested invocation exactly once per tool call and
    /// returns the event payload on the first publish. This runs at the policy
    /// boundary, before the approval decision and before the tool parses its
    /// own input; the recorded arguments are what the model requested.
    pub(crate) fn note_invocation(
        &self,
        tool_ref: &ToolRef,
        invocation: &ToolInvocation,
    ) -> Option<ToolInvocationData> {
        let name = invocation.tool_name().as_str().to_owned();
        let raw = serde_json::to_string(invocation.arguments()).unwrap_or_else(|_| "{}".to_owned());
        let (subject, subject_truncated) = subject_for(&name, invocation.arguments());
        let (input, input_total, input_truncated) = bounded(&raw, MAX_TOOL_INPUT_BYTES);

        let mut inner = self.lock();
        inner.track(tool_ref, &name);
        let already_published = inner
            .records
            .get(tool_ref)
            .is_some_and(|record| record.input_seen);
        inner.resize(tool_ref, |record| {
            if record.input_seen {
                return;
            }
            record.subject = subject;
            record.subject_truncated = subject_truncated;
            record.input = input;
            record.input_total = input_total;
            record.input_seen = true;
            record.input_truncated = input_truncated;
            record.input_expired = false;
        });
        inner.enforce_limits();
        if already_published {
            return None;
        }
        let record = inner.records.get(tool_ref)?;
        Some(ToolInvocationData {
            tool_ref: tool_ref.clone(),
            name: record.name.clone(),
            subject: record.subject.clone(),
            subject_truncated: record.subject_truncated,
            input: ToolInputSummary {
                total_bytes: record.input_total,
                preview: truncate_prefix(&record.input, EVENT_INPUT_PREVIEW_BYTES).0,
                truncated: record.input.len() > EVENT_INPUT_PREVIEW_BYTES || record.input_truncated,
                encoding: INPUT_ENCODING,
            },
        })
    }

    /// Marks a call as awaiting a policy/approval decision. Never `Running`:
    /// the tool has not been invoked before the decision.
    pub(crate) fn mark_awaiting_policy(&self, tool_ref: &ToolRef) {
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            if record.state == ToolExecutionState::Requested {
                record.state = ToolExecutionState::AwaitingPolicy;
            }
        }
    }

    /// Marks a call as actually executing and stamps the real start time.
    pub(crate) fn mark_running(&self, tool_ref: &ToolRef) {
        let started_at = crate::store::utc_timestamp().ok();
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            if !record.state.is_terminal() {
                record.state = ToolExecutionState::Running;
            }
            if record.started_at.is_none() {
                record.started_at = started_at;
            }
        }
    }

    /// Records a real execution stage. Unknown progress text is ignored.
    pub(crate) fn note_phase(&self, tool_ref: &ToolRef, phase: &str) {
        let Some(phase) = ToolPhase::from_wire(phase) else {
            return;
        };
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            record.phase = Some(phase);
        }
    }

    /// Retains the raw result text observed by the wrapper. The authoritative
    /// terminal state still comes from the Runtime report.
    pub(crate) fn note_result(&self, tool_ref: &ToolRef, content: &str) {
        let (result, result_total, result_truncated) = bounded(content, MAX_TOOL_RESULT_BYTES);
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            record.result = result;
            record.result_total = result_total;
            record.result_seen = true;
            record.result_truncated = result_truncated;
            record.result_expired = false;
        });
        inner.enforce_limits();
    }

    /// Marks that cancellation was requested for a still-running call. Never
    /// terminal: the owner reports the real end separately.
    pub(crate) fn mark_cancelling(&self, tool_ref: &ToolRef) {
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            if !record.state.is_terminal() {
                record.state = ToolExecutionState::Cancelling;
            }
        }
    }

    /// Appends one accepted chunk of one process stream. The retained window is
    /// authoritative: live events and every later `tool.output` page describe
    /// these same bytes, so nothing is re-read from a file system. The notice
    /// is built after the Session budget ran, so it reports the state a
    /// consumer will actually observe.
    pub(crate) fn note_stream_chunk(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
        chunk: &[u8],
    ) -> Option<ToolStreamNotice> {
        let mut inner = self.lock();
        if !inner.records.contains_key(tool_ref) {
            return None;
        }
        let mut pushed = None;
        inner.resize(tool_ref, |record| {
            let Some(window) = record.stream_mut(stream) else {
                return;
            };
            pushed = Some(window.push(chunk));
        });
        let pushed = pushed?;
        inner.enforce_limits();
        let window = inner.records.get(tool_ref)?.stream(stream)?;
        Some(ToolStreamNotice {
            stream,
            base_offset: pushed.base_offset,
            next_offset: pushed.next_offset,
            observed_end: window.observed_end,
            // True when the retained window no longer covers every observed
            // byte, so a live consumer knows it has to page `tool.output` for
            // the authoritative range instead of stitching this event alone.
            dropped: pushed.dropped || window.truncated,
            expired: window.expired,
        })
    }

    /// Marks a real end of output for one stream, never a failed read.
    pub(crate) fn note_stream_end(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            if let Some(window) = record.stream_mut(stream) {
                window.complete = true;
            }
        });
    }

    /// Marks an owner-observed end that may be missing bytes: the observation
    /// stopped before a real end of output. `eof` becomes true (the record is
    /// final) and the stream is truncated, never reported as a clean end.
    pub(crate) fn note_stream_cut(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            if let Some(window) = record.stream_mut(stream) {
                window.complete = true;
                window.truncated = true;
            }
        });
    }

    /// Stores the current process record without touching the Runtime outcome.
    /// The ranges and retention flags are overlaid from the current windows and
    /// the built snapshot is returned *after* the Session budget ran, so an
    /// event cannot publish a stale result clone that disagrees with a later
    /// `tool.read`.
    pub(crate) fn note_command(
        &self,
        tool_ref: &ToolRef,
        command: CommandResult,
    ) -> Option<ToolExecutionData> {
        let mut inner = self.lock();
        if !inner.records.contains_key(tool_ref) {
            return None;
        }
        inner.resize(tool_ref, |record| {
            record.command = Some(command);
        });
        inner.enforce_limits();
        inner
            .records
            .get(tool_ref)
            .map(|record| record.execution_data(tool_ref))
    }

    /// Marks durable recording state for one tool call and returns the updated snapshot.
    pub(crate) fn note_recording(
        &self,
        tool_ref: &ToolRef,
        recording: ToolRecordingState,
    ) -> Option<ToolExecutionData> {
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            record.recording = recording;
            Some(record.execution_data(tool_ref))
        } else {
            None
        }
    }

    /// Commits one native file-tool fact at the real mutation boundary. A
    /// duplicate identity is never overwritten with a guessed later value.
    pub(crate) fn note_file_change(&self, tool_ref: &ToolRef, change: FileChange) {
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            if record.file_change.is_none() {
                record.file_change = Some(change);
            }
        });
        inner.enforce_limits();
    }

    /// The retained range of one process stream, as
    /// `(base_offset, observed_end)`, so the process record and `tool.output`
    /// describe the same bytes.
    pub(crate) fn stream_range(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
    ) -> Option<(u64, u64)> {
        let inner = self.lock();
        let window = inner.records.get(tool_ref)?.stream(stream)?;
        Some((window.start_offset, window.observed_end))
    }

    /// Current structured snapshot for a live event. It is not an outcome:
    /// only `finish_and_snapshot` records the authoritative terminal state.
    pub(crate) fn snapshot(&self, tool_ref: &ToolRef) -> Option<ToolExecutionData> {
        let inner = self.lock();
        inner
            .records
            .get(tool_ref)
            .map(|record| record.execution_data(tool_ref))
    }

    /// Records the authoritative terminal outcome and returns the same
    /// snapshot `tool.read` would return, so the live event and the query
    /// cannot disagree.
    pub(crate) fn finish_and_snapshot(
        &self,
        tool_ref: &ToolRef,
        outcome: ToolResultOutcome,
    ) -> Option<ToolExecutionData> {
        let mut inner = self.lock();
        let finished_at = inner.records.get(tool_ref).and_then(|record| {
            if record.finished_at.is_some() {
                None
            } else {
                crate::store::utc_timestamp().ok()
            }
        });
        let record = inner.records.get_mut(tool_ref)?;
        record.state = ToolExecutionState::from_outcome(outcome);
        record.outcome = Some(outcome);
        if record.finished_at.is_none() {
            record.finished_at = finished_at;
        }
        Some(record.execution_data(tool_ref))
    }

    /// Reconciles terminal state from the joined loop report, which is
    /// authoritative. State/outcome are always aligned. Result content is
    /// captured whenever it was not already observed and not evicted, for
    /// every outcome, including failed/denied calls. Evicted content is never
    /// resurrected as available.
    pub(crate) fn reconcile(&self, session_id: SessionId, items: &[HistoryItem]) {
        let finished_at = crate::store::utc_timestamp().ok();
        let mut inner = self.lock();
        for item in items {
            let HistoryItem::ToolResult(result) = item else {
                continue;
            };
            let tool_ref = ToolRef {
                session_id,
                loop_id: result.loop_id,
                request_index: result.request_index,
                tool_call_id: result.call_id.clone(),
            };
            inner.track(&tool_ref, result.tool_name.as_str());
            // The report is the authority for the result text the model saw;
            // replace any best-effort early wrapper copy. Never resurrect bytes
            // that were evicted.
            let captured = inner.records.get(&tool_ref).and_then(|record| {
                (!record.result_expired)
                    .then(|| bounded(result.output.content().as_str(), MAX_TOOL_RESULT_BYTES))
            });
            if let Some(record) = inner.records.get_mut(&tool_ref) {
                record.state = ToolExecutionState::from_outcome(result.outcome);
                record.outcome = Some(result.outcome);
                if record.finished_at.is_none() {
                    record.finished_at = finished_at.clone();
                }
            }
            if let Some((content, total, truncated)) = captured {
                inner.resize(&tool_ref, |record| {
                    record.result = content;
                    record.result_total = total;
                    record.result_seen = true;
                    record.result_truncated = truncated;
                });
            }
        }
        inner.enforce_limits();
    }

    #[cfg(test)]
    pub(crate) fn read(
        &self,
        request: &ToolReadRequest,
        max_bytes: usize,
    ) -> Result<ToolReadResult, AgentError> {
        let inner = self.lock();
        let Some(record) = inner.records.get(&request.tool_ref) else {
            return Err(AgentError::ToolNotFound);
        };
        record.project_read(&request.tool_ref, max_bytes)
    }

    #[cfg(test)]
    pub(crate) fn output(
        &self,
        request: &ToolOutputRequest,
        max_bytes: usize,
    ) -> Result<ToolOutputPage, AgentError> {
        let inner = self.lock();
        let Some(record) = inner.records.get(&request.tool_ref) else {
            return Err(AgentError::ToolNotFound);
        };
        record.project_output(request, max_bytes)
    }

    /// Returns all tool refs associated with a particular loop in insertion order.
    pub(crate) fn loop_tool_refs(&self, session_id: SessionId, loop_id: LoopId) -> Vec<ToolRef> {
        let inner = self.lock();
        inner
            .order
            .iter()
            .filter(|r| r.session_id == session_id && r.loop_id == loop_id)
            .cloned()
            .collect()
    }

    /// Returns only bounded file-change metadata and retained blob ownership;
    /// callers never receive the larger invocation/result snapshots here.
    pub(crate) fn file_change_records(
        &self,
        session_id: SessionId,
        loop_id: Option<LoopId>,
    ) -> Vec<ChangeRecord> {
        let inner = self.lock();
        inner
            .order
            .iter()
            .filter(|tool_ref| {
                tool_ref.session_id == session_id
                    && loop_id
                        .as_ref()
                        .is_none_or(|loop_id| &tool_ref.loop_id == loop_id)
            })
            .filter_map(|tool_ref| {
                inner
                    .records
                    .get(tool_ref)
                    .and_then(|record| record.file_change.as_ref())
                    .map(|change| change.record(tool_ref))
            })
            .collect()
    }

    /// Returns the full in-memory `FileChange` for one tool call, with its
    /// bounded before/after snapshots, when one was recorded. Callers match on
    /// `file_change_records` metadata first, so only the hit is cloned here.
    pub(crate) fn file_change(&self, tool_ref: &ToolRef) -> Option<crate::changes::FileChange> {
        let inner = self.lock();
        inner
            .records
            .get(tool_ref)
            .and_then(|record| record.file_change.as_ref())
            .cloned()
    }

    /// Exports one completed tool call's facts and retained buffers for durable persistence.
    /// Memory copying is bounded to this single record (<= 3 MiB total).
    pub(crate) fn snapshot_for_persistence(
        &self,
        tool_ref: &ToolRef,
    ) -> Option<ToolPersistenceSnapshot> {
        let inner = self.lock();
        let record = inner.records.get(tool_ref)?;

        if !record.state.is_terminal() {
            return None;
        }
        if let Some(command) = &record.command {
            if !command.status.is_terminal() {
                return None;
            }
        }

        let input_bytes = if !record.input_expired && !record.input.is_empty() {
            Some(record.input.as_bytes().to_vec())
        } else {
            None
        };
        let input_file_bytes = input_bytes.as_ref().map_or(0, |b| b.len());
        let input_file_sha256 = input_bytes.as_ref().map(|b| crate::store::hash_bytes(b));
        let input_summary = StoredInputSummary {
            total_bytes: record.input_total,
            seen: record.input_seen,
            truncated: record.input_truncated,
            expired: record.input_expired,
            file_bytes: input_file_bytes,
            file_sha256: input_file_sha256,
        };

        let result_bytes = if !record.result_expired && !record.result.is_empty() {
            Some(record.result.as_bytes().to_vec())
        } else {
            None
        };
        let result_file_bytes = result_bytes.as_ref().map_or(0, |b| b.len());
        let result_file_sha256 = result_bytes.as_ref().map(|b| crate::store::hash_bytes(b));
        let result_summary = StoredResultSummary {
            total_bytes: record.result_total,
            seen: record.result_seen,
            truncated: record.result_truncated,
            expired: record.result_expired,
            file_bytes: result_file_bytes,
            file_sha256: result_file_sha256,
        };

        let stdout_bytes = if !record.stdout.expired && !record.stdout.bytes.is_empty() {
            Some(record.stdout.bytes.clone())
        } else {
            None
        };
        let stdout_file_bytes = stdout_bytes.as_ref().map_or(0, |b| b.len());
        let stdout_file_sha256 = stdout_bytes.as_ref().map(|b| crate::store::hash_bytes(b));
        let stdout_summary = StoredStreamWindow {
            start_offset: record.stdout.start_offset,
            observed_end: record.stdout.observed_end,
            seen: record.stdout.seen,
            complete: record.stdout.complete,
            truncated: record.stdout.truncated,
            expired: record.stdout.expired,
            file_bytes: stdout_file_bytes,
            file_sha256: stdout_file_sha256,
        };

        let stderr_bytes = if !record.stderr.expired && !record.stderr.bytes.is_empty() {
            Some(record.stderr.bytes.clone())
        } else {
            None
        };
        let stderr_file_bytes = stderr_bytes.as_ref().map_or(0, |b| b.len());
        let stderr_file_sha256 = stderr_bytes.as_ref().map(|b| crate::store::hash_bytes(b));
        let stderr_summary = StoredStreamWindow {
            start_offset: record.stderr.start_offset,
            observed_end: record.stderr.observed_end,
            seen: record.stderr.seen,
            complete: record.stderr.complete,
            truncated: record.stderr.truncated,
            expired: record.stderr.expired,
            file_bytes: stderr_file_bytes,
            file_sha256: stderr_file_sha256,
        };

        let command = record.command.clone().map(|cmd| record.live_command(cmd));
        let file_change = record.file_change.as_ref().map(FileChange::stored);
        let file_change_before = record
            .file_change
            .as_ref()
            .and_then(|change| change.before_bytes.clone());
        let file_change_after = record
            .file_change
            .as_ref()
            .and_then(|change| change.after_bytes.clone());

        let stored_record = StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: record.name.clone(),
            subject: record.subject.clone(),
            subject_truncated: record.subject_truncated,
            state: record.state,
            phase: record.phase,
            started_at: record.started_at.clone(),
            finished_at: record.finished_at.clone(),
            outcome: record.outcome,
            input: input_summary,
            result: result_summary,
            stdout: stdout_summary,
            stderr: stderr_summary,
            command,
            file_change,
        };

        Some(ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: stored_record,
            input_bytes,
            result_bytes,
            stdout_bytes,
            stderr_bytes,
            file_change_before,
            file_change_after,
        })
    }
}

fn bounded_page(page: ToolOutputPage, max_bytes: usize) -> Result<ToolOutputPage, AgentError> {
    if encoded_len(&page)? > max_bytes {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(page)
    }
}

/// Pages one raw process stream. Offsets are raw byte offsets, `data` is
/// base64 so any byte sequence survives unchanged, and `eof` is true only when
/// the stream really ended at or before the returned `next_offset`.
fn output_process(
    record: &ToolRecord,
    request: &ToolOutputRequest,
    max_bytes: usize,
) -> Result<ToolOutputPage, AgentError> {
    let stream = request.stream;
    let Some(window) = record.stream(stream) else {
        return Err(AgentError::InvalidArguments);
    };
    let availability = record.stream_availability(stream);
    // A page may report an end of output only when the owner's own decision is
    // final for this stream: the owner marked it ended/cut, or it published a
    // terminal command record. A Runtime-terminal tool call is not such a
    // signal, because a dropped tool future can leave the owner still draining.
    let stream_final = window.complete || record.stream_final();
    let base = ToolOutputPage {
        tool_ref: request.tool_ref.clone(),
        stream,
        encoding: STREAM_ENCODING,
        base_offset: window.start_offset,
        next_offset: window.observed_end,
        observed_end: window.observed_end,
        eof: stream_final,
        truncated: window.truncated,
        availability,
        data: String::new(),
    };
    if window.corrupt {
        return bounded_page(
            ToolOutputPage {
                eof: stream_final,
                truncated: true,
                availability: ToolDataAvailability::Unavailable,
                ..base
            },
            max_bytes,
        );
    }
    if request.offset > window.observed_end {
        return Err(AgentError::InvalidArguments);
    }
    if window.expired {
        // The window was freed; the observed end is the only honest anchor.
        return bounded_page(
            ToolOutputPage {
                truncated: true,
                ..base
            },
            max_bytes,
        );
    }
    // A stream the owner has not read a single byte from has no content at all;
    // a non-zero offset is invalid rather than a fabricated loss.
    if !window.seen && request.offset != 0 {
        return Err(AgentError::InvalidArguments);
    }
    let Some(bytes) = window.page(request.offset) else {
        // The requested bytes were dropped from the retained window: report the
        // loss and where the window starts now so the next page can read the tail.
        let has_retained = !window.bytes.is_empty();
        return bounded_page(
            ToolOutputPage {
                base_offset: window.start_offset,
                next_offset: if has_retained {
                    window.start_offset
                } else {
                    window.observed_end
                },
                eof: stream_final && !has_retained,
                truncated: true,
                availability: ToolDataAvailability::Partial,
                ..base
            },
            max_bytes,
        );
    };
    if bytes.is_empty() {
        // Nothing is retained at or after this offset; the frame reports the
        // real end without inventing content.
        return bounded_page(
            ToolOutputPage {
                base_offset: request.offset,
                next_offset: request.offset,
                eof: stream_final && request.offset >= window.observed_end,
                ..base
            },
            max_bytes,
        );
    }
    let template = ToolOutputPage {
        base_offset: request.offset,
        next_offset: u64::MAX,
        ..base.clone()
    };
    let template_len = encoded_len(&template)?;
    if template_len > max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    let available = max_bytes.saturating_sub(template_len).saturating_add(2);
    let take = raw_bytes_for_base64(available).min(bytes.len());
    if take == 0 {
        return Err(AgentError::InvalidArguments);
    }
    let next = request.offset.saturating_add(take as u64);
    bounded_page(
        ToolOutputPage {
            base_offset: request.offset,
            next_offset: next,
            eof: stream_final && next >= window.observed_end,
            data: base64::engine::general_purpose::STANDARD.encode(&bytes[..take]),
            ..base
        },
        max_bytes,
    )
}

/// The largest raw byte count whose base64 JSON string fits `available` bytes.
fn raw_bytes_for_base64(available: usize) -> usize {
    available.saturating_sub(2) / 4 * 3
}

/// Structured subject extraction for the known native tools. Unknown tools
/// expose only `Other`; their raw arguments stay readable through
/// `tool.output(Input, ...)`.
fn subject_for(name: &str, arguments: &Value) -> (ToolSubject, bool) {
    match name {
        "read" | "write" | "edit" | "apply_patch" => {
            let path = string_arg(arguments, &["path", "file_path"]);
            let (path, truncated) = truncate_prefix(&path, MAX_SUBJECT_BYTES);
            (ToolSubject::File { path }, truncated)
        }
        "bash" => {
            let script = string_arg(arguments, &["command"]);
            let cwd = arguments.get("cwd").and_then(Value::as_str).unwrap_or(".");
            let (script, script_truncated) = truncate_prefix(&script, MAX_SUBJECT_BYTES);
            let (cwd, cwd_truncated) = truncate_prefix(cwd, MAX_SUBJECT_BYTES);
            (
                ToolSubject::Command { script, cwd },
                script_truncated || cwd_truncated,
            )
        }
        _ => (ToolSubject::Other, false),
    }
}

fn string_arg(arguments: &Value, keys: &[&str]) -> String {
    for key in keys {
        if let Some(value) = arguments.get(*key).and_then(Value::as_str) {
            return value.to_owned();
        }
    }
    String::new()
}

fn bounded(value: &str, max_bytes: usize) -> (String, usize, bool) {
    let total = value.len();
    if total <= max_bytes {
        (value.to_owned(), total, false)
    } else {
        let end = previous_boundary(value, max_bytes);
        (value[..end].to_owned(), total, true)
    }
}

fn truncate_prefix(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        (value.to_owned(), false)
    } else {
        let end = previous_boundary(value, max_bytes);
        (value[..end].to_owned(), true)
    }
}

/// Largest char-boundary prefix whose JSON string encoding fits `available`
/// bytes, including the surrounding quotes. `json_len(prefix(k))` is monotonic
/// in the character count `k`, so a plain binary search over character counts
/// is correct, including a first multibyte character whose boundary is farther
/// than one byte step from the previous one.
fn fit_encoded_string(value: &str, available: usize) -> (String, bool) {
    if value.is_empty() || json_len("") > available {
        return (String::new(), !value.is_empty());
    }
    if json_len(value) <= available {
        return (value.to_owned(), false);
    }
    let char_count = value.chars().count();
    let prefix = |count: usize| -> &str {
        match value.char_indices().nth(count) {
            Some((index, _)) => &value[..index],
            None => value,
        }
    };
    let mut low = 0usize; // empty prefix fits
    let mut high = char_count; // full value does not fit
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if json_len(prefix(middle)) <= available {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let end = prefix(low).len();
    (value[..end].to_owned(), low < char_count)
}

fn json_len(value: &str) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

fn previous_boundary(value: &str, mut offset: usize) -> usize {
    offset = offset.min(value.len());
    while offset > 0 && !value.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn encoded_len<T: Serialize>(value: &T) -> Result<usize, AgentError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| AgentError::RpcSerialization)
}

#[cfg(test)]
mod tests {
    #[test]
    fn historical_memory_only_record_needs_stored_details() {
        let mut record = super::ToolRecord::new("bash".to_owned());
        record.state = super::ToolExecutionState::Succeeded;
        record.outcome = Some(super::ToolResultOutcome::Success);
        record.result_seen = true;
        assert!(record.needs_stored());
    }

    #[test]
    fn complete_empty_streams_do_not_need_stored_details() {
        let mut record = super::ToolRecord::new("bash".to_owned());
        record.state = super::ToolExecutionState::Succeeded;
        record.outcome = Some(super::ToolResultOutcome::Success);
        record.input_seen = true;
        record.result_seen = true;
        record.recording = super::ToolRecordingState::Saved;
        record.stdout.complete = true;
        record.stderr.complete = true;
        record.command = Some(super::CommandResult {
            status: super::CommandStatus::Exited,
            exit_code: Some(0),
            signal: None,
            termination_confirmed: true,
            stdout_base_offset: 0,
            stdout_observed_end: 0,
            stderr_base_offset: 0,
            stderr_observed_end: 0,
            output_complete: true,
            output_truncated: false,
        });
        assert!(!record.needs_stored());
    }

    use base64::Engine as _;
    use serde_json::json;

    use crate::changes::FileChange;

    use super::*;

    fn session(value: u8) -> SessionId {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
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

    fn loop_id(value: u8) -> LoopId {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
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

    fn make_tool_ref(session_id: SessionId, loop_key: LoopId, request: u32, call: &str) -> ToolRef {
        ToolRef {
            session_id,
            loop_id: loop_key,
            request_index: request,
            tool_call_id: ToolCallId::new(call).unwrap(),
        }
    }

    fn invocation(call: &str, name: &str, arguments: Value) -> ToolInvocation {
        ToolInvocation {
            tool_call_id: ToolCallId::new(call).unwrap(),
            tool_name: name.parse().unwrap(),
            arguments,
        }
    }

    fn read_request(tool_ref: ToolRef, max_bytes: Option<usize>) -> ToolReadRequest {
        ToolReadRequest {
            tool_ref,
            max_bytes,
        }
    }

    fn output_request(
        tool_ref: ToolRef,
        stream: ToolDataStream,
        offset: u64,
        max_bytes: Option<usize>,
    ) -> ToolOutputRequest {
        ToolOutputRequest {
            tool_ref,
            stream,
            offset,
            max_bytes,
        }
    }

    fn invocation_of(
        data: &ToolData,
        tool_ref: &ToolRef,
        call: &str,
        name: &str,
        arguments: Value,
    ) {
        let _ = data.note_invocation(tool_ref, &invocation(call, name, arguments));
    }

    #[test]
    fn session_window_bytes_are_counted_into_the_real_budget() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "window-budget");
        data.note_requested(&tool_ref, "bash");
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &vec![b'x'; 512 * 1024]);
        assert_accounting(&data);
        // Many windows cannot exceed the Session budget just because each one
        // is individually bounded.
        for index in 0..16u32 {
            let filler = make_tool_ref(session(1), loop_id(1), index + 1, "filler");
            data.note_requested(&filler, "bash");
            data.note_stream_chunk(&filler, ToolDataStream::Stdout, &vec![b'y'; 512 * 1024]);
            assert_accounting(&data);
        }
        let inner = data.lock();
        assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
        // The oldest window was freed rather than silently retained beyond the
        // Session budget.
        assert!(inner.records[&tool_ref].stdout.expired);
    }

    #[test]
    fn multi_round_push_and_multi_stream_capacity_accounting_stays_within_budget() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "capacity-test");
        data.note_requested(&tool_ref, "bash");

        // Push 200 rounds of 8 KiB chunks (~1.6 MiB total) to test repeated draining
        // does not inflate backing capacity beyond 1 MiB.
        let chunk = vec![b'c'; 8 * 1024];
        for _ in 0..200 {
            data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &chunk);
            assert_accounting(&data);
        }
        {
            let inner = data.lock();
            let window = inner.records[&tool_ref].stdout.bytes.as_slice();
            assert_eq!(window.len(), MAX_TOOL_STREAM_BYTES);
            let capacity = inner.records[&tool_ref].stdout.bytes.capacity();
            assert!(
                capacity <= MAX_TOOL_STREAM_BYTES,
                "single stream capacity {capacity} exceeded bound {MAX_TOOL_STREAM_BYTES}"
            );
        }

        // Test oversized chunk: 2.5 MiB in one push must not allocate > 1 MiB
        let oversized = vec![b'z'; 2_500_000];
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stderr, &oversized);
        assert_accounting(&data);
        {
            let inner = data.lock();
            let cap = inner.records[&tool_ref].stderr.bytes.capacity();
            assert!(
                cap <= MAX_TOOL_STREAM_BYTES,
                "oversized stream capacity {cap} exceeded bound {MAX_TOOL_STREAM_BYTES}"
            );
            assert_eq!(
                inner.records[&tool_ref].stderr.bytes.len(),
                MAX_TOOL_STREAM_BYTES
            );
            assert_eq!(
                inner.records[&tool_ref].stderr.start_offset,
                (2_500_000 - MAX_TOOL_STREAM_BYTES) as u64
            );
        }

        // Multiple streams across multiple records: verify total capacity accounting <= 8 MiB
        for index in 1..=12 {
            let other = make_tool_ref(session(1), loop_id(1), index, &format!("other-{index}"));
            data.note_requested(&other, "bash");
            // Push multi-round chunks into both stdout and stderr
            for _ in 0..16 {
                data.note_stream_chunk(&other, ToolDataStream::Stdout, &vec![b'o'; 64 * 1024]);
                data.note_stream_chunk(&other, ToolDataStream::Stderr, &vec![b'e'; 64 * 1024]);
            }
            assert_accounting(&data);
        }
        let inner = data.lock();
        assert!(
            inner.total_bytes <= MAX_TOOL_TOTAL_BYTES,
            "total capacity accounting {} exceeded {}",
            inner.total_bytes,
            MAX_TOOL_TOTAL_BYTES
        );
    }

    #[test]
    fn paging_from_stale_offset_zero_recovers_retained_tail_for_running_and_terminal() {
        use base64::Engine;

        let data = ToolData::new();

        // 1. Running stream test with binary data
        let running_ref = make_tool_ref(session(10), loop_id(10), 0, "running-tail");
        data.note_requested(&running_ref, "bash");
        // Push 1.4 MiB of binary data
        let binary_payload: Vec<u8> = (0..1_400_000u32).map(|i| (i % 251) as u8).collect();
        for chunk in binary_payload.chunks(128 * 1024) {
            data.note_stream_chunk(&running_ref, ToolDataStream::Stdout, chunk);
        }
        let (base_offset, observed_end) = data
            .stream_range(&running_ref, ToolDataStream::Stdout)
            .unwrap();
        assert_eq!(observed_end, 1_400_000);
        assert_eq!(base_offset, 1_400_000 - MAX_TOOL_STREAM_BYTES as u64);

        // Client queries from stale offset 0
        let page_budget = 4096;
        let notice = data
            .output(
                &output_request(running_ref.clone(), ToolDataStream::Stdout, 0, None),
                page_budget,
            )
            .unwrap();
        assert!(notice.data.is_empty());
        assert_eq!(notice.base_offset, base_offset);
        assert_eq!(notice.next_offset, base_offset);
        assert!(!notice.eof, "running stream must not report eof");
        assert!(notice.truncated);
        assert_eq!(notice.availability, ToolDataAvailability::Partial);

        // Resume from notice.next_offset and reconstruct retained tail
        let mut offset = notice.next_offset;
        let mut reconstructed_binary = Vec::new();
        while offset < observed_end {
            let page = data
                .output(
                    &output_request(running_ref.clone(), ToolDataStream::Stdout, offset, None),
                    page_budget,
                )
                .unwrap();
            let page_json_len = serde_json::to_vec(&page).unwrap().len();
            assert!(
                page_json_len <= page_budget,
                "page encoded size {page_json_len} exceeded budget {page_budget}"
            );
            assert!(!page.eof, "running stream must not report eof before end");
            let chunk_bytes = base64::engine::general_purpose::STANDARD
                .decode(&page.data)
                .unwrap();
            reconstructed_binary.extend_from_slice(&chunk_bytes);
            assert!(page.next_offset > offset);
            offset = page.next_offset;
        }
        assert_eq!(offset, observed_end);
        let expected_tail = &binary_payload[base_offset as usize..];
        assert_eq!(reconstructed_binary.as_slice(), expected_tail);

        // 2. Terminal stream test with UTF-8 data
        let terminal_ref = make_tool_ref(session(11), loop_id(11), 0, "terminal-tail");
        data.note_requested(&terminal_ref, "bash");
        let line = "Line content for UTF-8 test with unicode: 你好，世界！\n";
        let mut utf8_payload = String::new();
        while utf8_payload.len() < 1_300_000 {
            utf8_payload.push_str(line);
        }
        let utf8_bytes = utf8_payload.as_bytes();
        for chunk in utf8_bytes.chunks(128 * 1024) {
            data.note_stream_chunk(&terminal_ref, ToolDataStream::Stdout, chunk);
        }
        data.note_stream_end(&terminal_ref, ToolDataStream::Stdout);
        let (t_base, t_end) = data
            .stream_range(&terminal_ref, ToolDataStream::Stdout)
            .unwrap();
        assert_eq!(t_end, utf8_bytes.len() as u64);
        assert_eq!(t_base, (utf8_bytes.len() - MAX_TOOL_STREAM_BYTES) as u64);

        // Query from stale offset 0
        let notice = data
            .output(
                &output_request(terminal_ref.clone(), ToolDataStream::Stdout, 0, None),
                page_budget,
            )
            .unwrap();
        assert!(notice.data.is_empty());
        assert_eq!(notice.base_offset, t_base);
        assert_eq!(notice.next_offset, t_base);
        assert!(
            !notice.eof,
            "stale offset 0 notice must not report eof when tail is retained"
        );
        assert!(notice.truncated);

        // Page until eof == true
        let mut offset = notice.next_offset;
        let mut reconstructed_utf8 = Vec::new();
        let mut saw_eof = false;
        while !saw_eof {
            let page = data
                .output(
                    &output_request(terminal_ref.clone(), ToolDataStream::Stdout, offset, None),
                    page_budget,
                )
                .unwrap();
            let page_json_len = serde_json::to_vec(&page).unwrap().len();
            assert!(
                page_json_len <= page_budget,
                "page encoded size {page_json_len} exceeded budget {page_budget}"
            );
            let chunk_bytes = base64::engine::general_purpose::STANDARD
                .decode(&page.data)
                .unwrap();
            reconstructed_utf8.extend_from_slice(&chunk_bytes);
            saw_eof = page.eof;
            assert!(page.next_offset > offset || page.eof);
            offset = page.next_offset;
        }
        assert_eq!(offset, t_end);
        let expected_utf8_tail = &utf8_bytes[t_base as usize..];
        assert_eq!(reconstructed_utf8.as_slice(), expected_utf8_tail);
    }

    #[test]
    fn an_abandoned_stream_reports_truncated_and_eof_not_an_endless_page() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(2), loop_id(2), 0, "cut");
        data.note_requested(&tool_ref, "bash");
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, b"partial");
        // The owner stopped observing before a real end of output.
        data.note_stream_cut(&tool_ref, ToolDataStream::Stdout);
        let request = ToolOutputRequest {
            tool_ref,
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        };
        let page = data.output(&request, 64 * 1024).unwrap();
        assert!(page.eof, "a cut stream is final");
        assert!(page.truncated, "a cut stream never claims a clean end");
        assert_eq!(page.availability, ToolDataAvailability::Partial);
    }

    #[test]
    fn offset_pages_never_return_overlapping_or_duplicate_bytes() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(5), loop_id(5), 0, "paging");
        data.note_requested(&tool_ref, "bash");
        // Large enough that a 4 KiB budget really forces several pages while
        // staying well above the page frame's own JSON size.
        let payload: Vec<u8> = (0..8192u32).map(|byte| byte as u8).collect();
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &payload);
        data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
        let mut offset = 0u64;
        let mut assembled = Vec::new();
        let mut pages = 0usize;
        while offset < payload.len() as u64 {
            pages += 1;
            assert!(pages < 64, "paging did not make progress");
            let page = data
                .output(
                    &ToolOutputRequest {
                        tool_ref: tool_ref.clone(),
                        stream: ToolDataStream::Stdout,
                        offset,
                        max_bytes: None,
                    },
                    4096,
                )
                .unwrap();
            assert!(page.next_offset > offset, "a page must make progress");
            assembled.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(&page.data)
                    .unwrap(),
            );
            offset = page.next_offset;
        }
        assert!(pages > 1, "the budget did not force pagination");
        assert_eq!(assembled, payload);
    }

    #[test]
    fn a_running_command_reports_live_ranges_and_retention_truncation() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(7), loop_id(7), 0, "live-ranges");
        data.note_requested(&tool_ref, "bash");
        // The owner publishes its opening record before any byte exists.
        data.note_command(
            &tool_ref,
            CommandResult {
                status: CommandStatus::Running,
                exit_code: None,
                signal: None,
                termination_confirmed: false,
                stdout_base_offset: 0,
                stdout_observed_end: 0,
                stderr_base_offset: 0,
                stderr_observed_end: 0,
                output_complete: false,
                output_truncated: false,
            },
        );
        // Bytes arrive afterwards; the stored record is not rewritten.
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, b"hello");
        let snapshot = data.snapshot(&tool_ref).unwrap();
        let command = snapshot.command.expect("a running command record");
        assert_eq!(command.status, CommandStatus::Running);
        assert_eq!(
            (command.stdout_base_offset, command.stdout_observed_end),
            (0, 5),
            "tool.read must see the live window range while streaming"
        );
        // The stream query reports the same range from the same source.
        assert_eq!(
            data.stream_range(&tool_ref, ToolDataStream::Stdout),
            Some((0, 5))
        );
        assert!(!command.output_truncated);
    }

    #[test]
    fn a_full_eof_with_a_dropped_tail_window_reports_truncated() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(8), loop_id(8), 0, "tail-loss");
        data.note_requested(&tool_ref, "bash");
        // More than one 1 MiB window, so the oldest tail bytes were dropped.
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &vec![b'x'; 1_200_000]);
        data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
        let command = CommandResult {
            status: CommandStatus::Exited,
            exit_code: Some(0),
            signal: None,
            termination_confirmed: true,
            stdout_base_offset: 0,
            stdout_observed_end: 0,
            stderr_base_offset: 0,
            stderr_observed_end: 0,
            output_complete: true,
            output_truncated: false,
        };
        let snapshot = data.note_command(&tool_ref, command).unwrap();
        let command = snapshot.command.unwrap();
        assert!(command.output_complete, "the stream really ended");
        assert!(
            command.output_truncated,
            "a dropped tail window must still be reported as truncation"
        );
        // The reported range is the retained window, and the observed end is
        // the real stream end, not the retained length.
        assert_eq!(command.stdout_observed_end, 1_200_000);
        assert!(command.stdout_base_offset > 0);
        assert_eq!(
            (command.stdout_base_offset, command.stdout_observed_end),
            data.stream_range(&tool_ref, ToolDataStream::Stdout)
                .unwrap()
        );
    }

    #[test]
    fn a_terminal_runtime_outcome_alone_never_closes_a_live_process_stream() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(6), loop_id(6), 0, "owner-finality");
        data.note_requested(&tool_ref, "bash");
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, b"still-running");
        // The owner published only a running command record.
        data.note_command(
            &tool_ref,
            CommandResult {
                status: CommandStatus::Running,
                exit_code: None,
                signal: None,
                termination_confirmed: false,
                stdout_base_offset: 0,
                stdout_observed_end: 13,
                stderr_base_offset: 0,
                stderr_observed_end: 0,
                output_complete: false,
                output_truncated: false,
            },
        );
        // The Runtime outcome is terminal while the owner has not finished.
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Cancelled)
            .unwrap();
        let request = output_request(tool_ref.clone(), ToolDataStream::Stdout, 0, None);
        let page = data.output(&request, 64 * 1024).unwrap();
        assert!(
            !page.eof,
            "a Runtime terminal outcome alone must not close a live stream"
        );
        assert_eq!(page.availability, ToolDataAvailability::Available);

        // Only the owner's terminal record closes it.
        data.note_command(
            &tool_ref,
            CommandResult {
                status: CommandStatus::Cancelled,
                exit_code: None,
                signal: None,
                termination_confirmed: true,
                stdout_base_offset: 0,
                stdout_observed_end: 13,
                stderr_base_offset: 0,
                stderr_observed_end: 0,
                output_complete: true,
                output_truncated: false,
            },
        );
        data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
        let page = data.output(&request, 64 * 1024).unwrap();
        assert!(page.eof);
    }

    #[test]
    fn process_stream_pages_are_base64_with_raw_offsets() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(3), loop_id(3), 1, "call-stream");
        data.note_requested(&tool_ref, "bash");
        let payload = b"caf\xc3\xa9-\xff\x1b[31m";
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &payload[..5]);
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &payload[5..]);
        let request = ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        };
        let page = data.output(&request, 64 * 1024).unwrap();
        assert_eq!(page.encoding, STREAM_ENCODING);
        assert_eq!(page.base_offset, 0);
        assert_eq!(page.next_offset, payload.len() as u64);
        assert!(
            !page.eof,
            "a running stream must never report an end of output"
        );
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&page.data)
                .unwrap(),
            payload.to_vec()
        );
        // A page cut by the budget stays honest about the next raw offset.
        // The JSON frame has a real minimum size, so a budget that cannot hold
        // the frame is rejected instead of silently returning an empty page.
        assert!(data.output(&request, 64).is_err());
        // Only the owner can end the stream, and then eof is real.
        data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
        let ended = data.output(&request, 64 * 1024).unwrap();
        assert!(ended.eof);
        assert_eq!(ended.next_offset, payload.len() as u64);
    }

    #[test]
    fn an_evicted_stream_window_anchors_at_the_observed_end() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(4), loop_id(4), 1, "call-evict");
        data.note_requested(&tool_ref, "bash");
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &vec![b'x'; 1_200_000]);
        let (base, end) = data
            .stream_range(&tool_ref, ToolDataStream::Stdout)
            .unwrap();
        assert_eq!(end, 1_200_000);
        assert!(base > 0, "the window retained more than its bound");
        let request = ToolOutputRequest {
            tool_ref,
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        };
        let page = data.output(&request, 64 * 1024).unwrap();
        assert!(page.truncated);
        assert_eq!(page.base_offset, base);
        assert_eq!(page.availability, ToolDataAvailability::Partial);
        {
            let mut inner = data.lock();
            inner.resize(&request.tool_ref, |record| record.stdout.evict());
        }
        let expired = data.output(&request, 64 * 1024).unwrap();
        assert_eq!(expired.availability, ToolDataAvailability::Expired);
        assert_eq!(expired.next_offset, end);
        assert!(!expired.eof);
        let past_end = ToolOutputRequest {
            offset: end + 1,
            ..request
        };
        assert!(matches!(
            data.output(&past_end, 64 * 1024),
            Err(AgentError::InvalidArguments)
        ));
    }

    #[test]
    fn full_identity_separates_sessions_loops_requests_and_calls() {
        let data = ToolData::new();
        let first = make_tool_ref(session(1), loop_id(1), 0, "call-a");
        let second = make_tool_ref(session(2), loop_id(1), 0, "call-a");
        let third = make_tool_ref(session(1), loop_id(2), 0, "call-a");
        let fourth = make_tool_ref(session(1), loop_id(1), 1, "call-a");
        let fifth = make_tool_ref(session(1), loop_id(1), 0, "call-b");
        for other in [&first, &second, &third, &fourth, &fifth] {
            data.note_requested(other, "read");
        }
        let result = data.read(&read_request(first.clone(), None), 4096).unwrap();
        assert_eq!(result.execution.tool_ref, first);
        for other in [&second, &third, &fourth, &fifth] {
            assert_ne!(result.execution.tool_ref, *other);
            assert!(data.read(&read_request(other.clone(), None), 4096).is_ok());
        }
    }

    #[test]
    fn tool_ref_rejects_unknown_fields() {
        let value = json!({
            "session_id": session(1).to_string(),
            "loop_id": loop_id(1).to_string(),
            "request_index": 0,
            "tool_call_id": "call-a",
            "extra": "no",
        });
        assert!(serde_json::from_value::<ToolRef>(value).is_err());
    }

    #[test]
    fn states_never_report_running_while_waiting_for_policy() {
        let data = ToolData::new();
        let loop_key = loop_id(1);
        let tool_ref = make_tool_ref(session(1), loop_key, 0, "write-call");
        data.note_requested(&tool_ref, "write");
        assert_eq!(
            data.read(&read_request(tool_ref.clone(), None), 4096)
                .unwrap()
                .execution
                .state,
            ToolExecutionState::Requested
        );
        // Requested input is published at the policy boundary.
        invocation_of(
            &data,
            &tool_ref,
            "write-call",
            "write",
            json!({"path": "a.txt", "content": "hi"}),
        );
        data.mark_awaiting_policy(&tool_ref);
        let waiting = data
            .read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap();
        assert_eq!(waiting.execution.state, ToolExecutionState::AwaitingPolicy);
        let waiting_invocation = waiting.invocation.expect("requested input");
        assert!(matches!(
            waiting_invocation.subject,
            ToolSubject::File { ref path } if path == "a.txt"
        ));
        // No execution time before the tool actually runs.
        assert!(waiting.execution.started_at.is_none());

        data.mark_running(&tool_ref);
        let running = data
            .read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap();
        assert_eq!(running.execution.state, ToolExecutionState::Running);
        assert!(running.execution.started_at.is_some());

        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("tracked record finishes");
        let finished = data.read(&read_request(tool_ref, None), 4096).unwrap();
        assert_eq!(finished.execution.state, ToolExecutionState::Succeeded);
        assert_eq!(finished.execution.outcome, Some(ToolResultOutcome::Success));
        assert!(finished.execution.finished_at.is_some());
    }

    #[test]
    fn unknown_phase_text_is_ignored_and_never_stored() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "read-call");
        data.note_requested(&tool_ref, "read");
        data.mark_running(&tool_ref);
        data.note_phase(&tool_ref, "reading");
        assert_eq!(
            data.read(&read_request(tool_ref.clone(), None), 4096)
                .unwrap()
                .execution
                .phase,
            Some(ToolPhase::Reading)
        );
        data.note_phase(&tool_ref, "TOP-SECRET-ARBITRARY-PROGRESS");
        let execution = data
            .read(&read_request(tool_ref, None), 4096)
            .unwrap()
            .execution;
        assert_eq!(execution.phase, Some(ToolPhase::Reading));
        let debug = format!("{execution:?}");
        assert!(!debug.contains("TOP-SECRET"));
    }

    #[test]
    fn raw_output_is_paged_by_offset_without_escaping() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "read-call");
        data.note_requested(&tool_ref, "read");
        invocation_of(
            &data,
            &tool_ref,
            "read-call",
            "read",
            json!({"path": "é.txt"}),
        );
        let content = "1: 你好\n2: café\tend\n".repeat(64);
        data.note_result(&tool_ref, &content);
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("tracked record finishes");

        let mut collected = String::new();
        let mut offset = 0_u64;
        loop {
            let page = data
                .output(
                    &output_request(tool_ref.clone(), ToolDataStream::Output, offset, Some(512)),
                    512,
                )
                .unwrap();
            assert_eq!(page.base_offset, offset);
            assert_eq!(page.observed_end, content.len() as u64);
            collected.push_str(&page.data);
            offset = page.next_offset;
            if page.eof {
                break;
            }
        }
        assert_eq!(collected, content);
        assert!(collected.contains('\t'));
        assert!(collected.contains("你好"));
    }

    #[test]
    fn output_stream_is_pending_until_the_result_is_observed() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "bash-call");
        data.note_requested(&tool_ref, "bash");
        invocation_of(
            &data,
            &tool_ref,
            "bash-call",
            "bash",
            json!({"command": "sleep 1"}),
        );
        data.mark_running(&tool_ref);

        // A running call has no result bytes yet: not eof, not available.
        let pending = data
            .output(
                &output_request(tool_ref.clone(), ToolDataStream::Output, 0, None),
                4096,
            )
            .unwrap();
        assert_eq!(pending.availability, ToolDataAvailability::Pending);
        assert_eq!(pending.observed_end, 0);
        assert!(!pending.eof);
        assert!(!pending.truncated);
        assert!(pending.data.is_empty());

        // A non-zero offset on an unobserved stream is rejected.
        assert!(matches!(
            data.output(
                &output_request(tool_ref.clone(), ToolDataStream::Output, 1, None),
                4096
            ),
            Err(AgentError::InvalidArguments)
        ));

        data.note_result(&tool_ref, "done");
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("tracked record finishes");
        let complete = data
            .output(
                &output_request(tool_ref, ToolDataStream::Output, 0, None),
                4096,
            )
            .unwrap();
        assert_eq!(complete.availability, ToolDataAvailability::Available);
        assert!(complete.eof);
        assert_eq!(complete.data, "done");
    }

    #[test]
    fn report_reconciliation_records_failed_results_and_aligns_state() {
        use minicore_runtime::history::ToolResultHistory;
        use minicore_runtime::tools::ToolOutput;

        let data = ToolData::new();
        let session_id = session(3);
        let loop_key = loop_id(3);
        let tool_ref = make_tool_ref(session_id, loop_key, 0, "read-call");
        data.note_requested(&tool_ref, "read");
        invocation_of(
            &data,
            &tool_ref,
            "read-call",
            "read",
            json!({"path": "missing.txt"}),
        );
        data.mark_running(&tool_ref);
        // The wrapper never captured a result (for example its future was
        // dropped); only the report knows the terminal outcome.
        let item = HistoryItem::ToolResult(ToolResultHistory {
            loop_id: loop_key,
            request_index: 0,
            call_id: ToolCallId::new("read-call").unwrap(),
            tool_name: "read".parse().unwrap(),
            outcome: ToolResultOutcome::Failed,
            output: ToolOutput::new("tool failed").unwrap(),
        });
        data.reconcile(session_id, std::slice::from_ref(&item));
        let result = data
            .read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap();
        assert_eq!(result.execution.state, ToolExecutionState::Failed);
        assert_eq!(result.execution.outcome, Some(ToolResultOutcome::Failed));
        let page = data
            .output(
                &output_request(tool_ref, ToolDataStream::Output, 0, None),
                4096,
            )
            .unwrap();
        assert_eq!(page.data, "tool failed");
        assert_eq!(page.availability, ToolDataAvailability::Available);
        assert!(page.eof);
    }

    #[test]
    fn reconcile_does_not_resurrect_evicted_result_bytes() {
        use minicore_runtime::history::ToolResultHistory;
        use minicore_runtime::tools::ToolOutput;

        let data = ToolData::new();
        let session_id = session(4);
        let loop_key = loop_id(4);
        let tool_ref = make_tool_ref(session_id, loop_key, 0, "read-call");
        data.note_requested(&tool_ref, "read");
        invocation_of(
            &data,
            &tool_ref,
            "read-call",
            "read",
            json!({"path": "a.txt"}),
        );
        data.note_result(&tool_ref, &"x".repeat(MAX_TOOL_RESULT_BYTES));
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("tracked record finishes");
        // Enough distinct large result records to cross the Session byte budget
        // and evict the oldest result bytes.
        let per_record = MAX_TOOL_RESULT_BYTES;
        let needed = MAX_TOOL_TOTAL_BYTES / per_record + 2;
        for index in 1..=needed {
            let filler = make_tool_ref(
                session_id,
                loop_key,
                index as u32,
                &format!("filler-{index}"),
            );
            data.note_requested(&filler, "read");
            invocation_of(
                &data,
                &filler,
                &format!("filler-{index}"),
                "read",
                json!({"path": "f"}),
            );
            data.note_result(&filler, &"y".repeat(per_record));
            data.finish_and_snapshot(&filler, ToolResultOutcome::Success)
                .expect("tracked record finishes");
        }
        assert_eq!(
            data.read(&read_request(tool_ref.clone(), None), 4096)
                .unwrap()
                .execution
                .output_availability,
            ToolDataAvailability::Expired
        );
        let item = HistoryItem::ToolResult(ToolResultHistory {
            loop_id: loop_key,
            request_index: 0,
            call_id: ToolCallId::new("read-call").unwrap(),
            tool_name: "read".parse().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("authoritative").unwrap(),
        });
        data.reconcile(session_id, std::slice::from_ref(&item));
        let page = data
            .output(
                &output_request(tool_ref, ToolDataStream::Output, 0, None),
                4096,
            )
            .unwrap();
        assert_eq!(page.availability, ToolDataAvailability::Expired);
        assert!(page.data.is_empty());
        assert!(page.truncated);
        assert!(page.eof);
    }

    #[test]
    fn byte_budget_counts_metadata_and_frees_capacity() {
        let data = ToolData::new();
        // Large subjects are bounded metadata and must count toward the budget.
        for index in 0..64u32 {
            let tool_ref =
                make_tool_ref(session(1), loop_id(1), index, &format!("subject-{index}"));
            data.note_requested(&tool_ref, "bash");
            assert_accounting(&data);
            invocation_of(
                &data,
                &tool_ref,
                &format!("subject-{index}"),
                "bash",
                json!({"command": "c".repeat(4 * 1024)}),
            );
            assert_accounting(&data);
            data.note_result(&tool_ref, &"r".repeat(4 * 1024));
            assert_accounting(&data);
            data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
                .expect("tracked record finishes");
            assert_accounting(&data);
        }
        let inner = data.lock();
        assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
        // Metadata is genuinely counted, not just the raw input bytes.
        assert!(inner.total_bytes > 200 * 1024);
        let sum: usize = inner.records.values().map(ToolRecord::retained_bytes).sum();
        assert_eq!(sum, inner.total_bytes);
    }

    #[test]
    fn eviction_releases_backing_capacity_and_keeps_accounting_exact() {
        let data = ToolData::new();
        let session_id = session(1);
        let loop_key = loop_id(1);
        let oldest = make_tool_ref(session_id, loop_key, 0, "oldest");
        data.note_requested(&oldest, "read");
        invocation_of(&data, &oldest, "oldest", "read", json!({"path": "a.txt"}));
        data.note_result(&oldest, &"x".repeat(MAX_TOOL_RESULT_BYTES));
        data.finish_and_snapshot(&oldest, ToolResultOutcome::Success)
            .expect("tracked record finishes");
        let capacity_before = data
            .lock()
            .records
            .get(&oldest)
            .map_or(0, |record| record.result.capacity());
        assert!(capacity_before >= MAX_TOOL_RESULT_BYTES);

        let needed = MAX_TOOL_TOTAL_BYTES / MAX_TOOL_RESULT_BYTES + 2;
        for index in 1..=needed {
            let filler = make_tool_ref(
                session_id,
                loop_key,
                index as u32,
                &format!("filler-{index}"),
            );
            data.note_requested(&filler, "read");
            invocation_of(
                &data,
                &filler,
                &format!("filler-{index}"),
                "read",
                json!({"path": "f"}),
            );
            data.note_result(&filler, &"y".repeat(MAX_TOOL_RESULT_BYTES));
            data.finish_and_snapshot(&filler, ToolResultOutcome::Success)
                .expect("tracked record finishes");
            assert_accounting(&data);
        }

        let inner = data.lock();
        assert_eq!(
            inner.records[&oldest].output_availability(),
            ToolDataAvailability::Expired
        );
        // The evicted backing buffer is actually released, not logically cleared.
        assert_eq!(inner.records[&oldest].result.capacity(), 0);
        let sum: usize = inner.records.values().map(ToolRecord::retained_bytes).sum();
        assert_eq!(sum, inner.total_bytes);
        assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
    }

    fn assert_accounting(data: &ToolData) {
        let inner = data.lock();
        let sum: usize = inner.records.values().map(ToolRecord::retained_bytes).sum();
        assert_eq!(
            sum, inner.total_bytes,
            "tracked total drifted from retained records"
        );
        assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
    }

    #[test]
    fn note_requested_alone_stays_bounded() {
        let data = ToolData::new();
        for index in 0..(MAX_TOOL_RECORDS + 64) {
            let tool_ref = make_tool_ref(
                session(1),
                loop_id(1),
                index as u32,
                &format!("call-{index}"),
            );
            data.note_requested(&tool_ref, "read");
        }
        assert!(data.lock().records.len() <= MAX_TOOL_RECORDS);
    }

    #[test]
    fn response_byte_budget_includes_json_encoding() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "read-call");
        data.note_requested(&tool_ref, "read");
        invocation_of(
            &data,
            &tool_ref,
            "read-call",
            "read",
            json!({"path": "src/\"quoted\"/文件.txt", "limit": 32}),
        );
        data.note_result(&tool_ref, &"quote\" and slash\\ and 世界\n".repeat(32));
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("tracked record finishes");

        let page = data
            .output(
                &output_request(tool_ref.clone(), ToolDataStream::Output, 0, Some(1024)),
                1024,
            )
            .unwrap();
        assert!(encoded_len(&page).unwrap() <= 1024);
        assert!(!page.eof || page.next_offset == page.observed_end || page.truncated);

        let large = make_tool_ref(session(1), loop_id(1), 1, "write-call");
        data.note_requested(&large, "write");
        invocation_of(
            &data,
            &large,
            "write-call",
            "write",
            json!({"path": "a.txt", "content": "z".repeat(16 * 1024)}),
        );
        let read = data.read(&read_request(large, Some(1024)), 1024).unwrap();
        assert!(encoded_len(&read).unwrap() <= 1024);
        assert!(read.invocation.unwrap().input.truncated);

        let error = data
            .output(
                &output_request(tool_ref.clone(), ToolDataStream::Output, 0, Some(1)),
                1,
            )
            .unwrap_err();
        assert!(matches!(error, AgentError::InvalidArguments));
        let error = data.read(&read_request(tool_ref, Some(1)), 1).unwrap_err();
        assert!(matches!(error, AgentError::InvalidArguments));
    }

    #[test]
    fn fit_encoded_string_accepts_a_minimal_multibyte_budget() {
        // `"€"` is 5 encoded bytes; a budget of exactly 5 must keep it.
        assert_eq!(fit_encoded_string("€", 5).0, "€");
        assert!(!fit_encoded_string("€", 5).1);
        // One byte short cannot fit the character and returns an empty prefix.
        assert!(fit_encoded_string("€", 4).0.is_empty());
        assert!(fit_encoded_string("€", 4).1);
        // A larger value keeps the longest fitting prefix on a char boundary.
        assert_eq!(fit_encoded_string("€x", 5).0, "€");
        assert!(fit_encoded_string("€x", 5).1);
        assert_eq!(fit_encoded_string("€x", 6).0, "€x");
        assert!(!fit_encoded_string("€x", 6).1);
        assert_eq!(fit_encoded_string("a你好z", 64).0, "a你好z");
    }

    #[test]
    fn terminal_without_observed_result_is_unavailable_not_pending() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "bash-call");
        data.note_requested(&tool_ref, "bash");
        invocation_of(
            &data,
            &tool_ref,
            "bash-call",
            "bash",
            json!({"command": "echo hi"}),
        );
        data.mark_running(&tool_ref);
        // Terminal with no recorded result: the stream is ended but empty.
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Cancelled)
            .expect("tracked record finishes");
        let execution = data
            .read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap()
            .execution;
        assert_eq!(
            execution.output_availability,
            ToolDataAvailability::Unavailable
        );
        let page = data
            .output(
                &output_request(tool_ref, ToolDataStream::Output, 0, None),
                4096,
            )
            .unwrap();
        assert_eq!(page.availability, ToolDataAvailability::Unavailable);
        assert!(page.eof);
        assert!(page.data.is_empty());
    }

    #[test]
    fn unknown_reference_is_not_found() {
        let data = ToolData::new();
        let missing = make_tool_ref(session(9), loop_id(9), 9, "missing");
        assert!(matches!(
            data.read(&read_request(missing.clone(), None), 4096),
            Err(AgentError::ToolNotFound)
        ));
        assert!(matches!(
            data.output(
                &output_request(missing, ToolDataStream::Output, 0, None),
                4096
            ),
            Err(AgentError::ToolNotFound)
        ));
    }

    #[test]
    fn debug_never_exposes_raw_content() {
        let data = ToolData::new();
        let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "bash-call");
        data.note_requested(&tool_ref, "bash");
        invocation_of(
            &data,
            &tool_ref,
            "bash-call",
            "bash",
            json!({"command": "curl -H 'Authorization: TOP-SECRET' https://private.invalid"}),
        );
        data.note_result(&tool_ref, "TOP-SECRET-OUTPUT");
        let read = data
            .read(&read_request(tool_ref.clone(), None), 8192)
            .unwrap();
        let page = data
            .output(
                &output_request(tool_ref, ToolDataStream::Output, 0, None),
                8192,
            )
            .unwrap();
        for debug in [format!("{read:?}"), format!("{page:?}")] {
            assert!(!debug.contains("TOP-SECRET"));
            assert!(!debug.contains("private.invalid"));
            assert!(!debug.contains("Authorization"));
        }
    }

    #[test]
    fn merge_stored_rejects_incompatible_identity_totals_and_state() {
        let mut mem = ToolRecord::new("bash".to_owned());
        mem.state = ToolExecutionState::Succeeded;
        mem.input_seen = true;
        mem.input_total = 100;
        mem.stdout.seen = true;
        mem.stdout.observed_end = 500;

        // Incompatible tool name
        let stored_diff_name = ToolRecord::new("read".to_owned());
        assert!(!mem.merge_stored(stored_diff_name));

        // Incompatible state
        let mut stored_diff_state = ToolRecord::new("bash".to_owned());
        stored_diff_state.state = ToolExecutionState::Failed;
        assert!(!mem.merge_stored(stored_diff_state));

        // Incompatible input total
        let mut stored_diff_total = ToolRecord::new("bash".to_owned());
        stored_diff_total.state = ToolExecutionState::Succeeded;
        stored_diff_total.input_seen = true;
        stored_diff_total.input_total = 999;
        assert!(!mem.merge_stored(stored_diff_total));

        // Incompatible stdout observed end
        let mut stored_diff_end = ToolRecord::new("bash".to_owned());
        stored_diff_end.state = ToolExecutionState::Succeeded;
        stored_diff_end.input_seen = true;
        stored_diff_end.input_total = 100;
        stored_diff_end.stdout.seen = true;
        stored_diff_end.stdout.observed_end = 999;
        assert!(!mem.merge_stored(stored_diff_end));

        // Memory remains completely unmutated
        assert_eq!(mem.input_total, 100);
        assert_eq!(mem.stdout.observed_end, 500);
    }

    #[test]
    fn merge_stored_preserves_known_empty_eof_and_fills_unknown() {
        // Known empty EOF in memory cannot be replaced by non-empty stream
        let mut mem = ToolRecord::new("bash".to_owned());
        mem.state = ToolExecutionState::Succeeded;
        mem.stdout.complete = true;
        mem.stdout.observed_end = 0;

        let mut stored = ToolRecord::new("bash".to_owned());
        stored.state = ToolExecutionState::Succeeded;
        stored.stdout.observed_end = 120;
        assert!(!mem.merge_stored(stored));
        assert_eq!(mem.stdout.observed_end, 0);
        assert!(mem.stdout.complete);

        // Unknown in memory learns complete EOF even if seen is false
        let mut mem_unknown = ToolRecord::new("bash".to_owned());
        mem_unknown.state = ToolExecutionState::Succeeded;
        let mut stored_empty_eof = ToolRecord::new("bash".to_owned());
        stored_empty_eof.state = ToolExecutionState::Succeeded;
        stored_empty_eof.stdout.complete = true;
        stored_empty_eof.stdout.observed_end = 0;
        assert!(mem_unknown.merge_stored(stored_empty_eof));
        assert!(mem_unknown.stdout.complete);
        assert_eq!(mem_unknown.stdout.observed_end, 0);
    }

    #[test]
    fn merge_stored_reconciled_result_learns_input_and_command_from_disk() {
        let mut mem = ToolRecord::new("bash".to_owned());
        mem.state = ToolExecutionState::Succeeded;
        mem.result = "cmd output\n".to_owned();
        mem.result_seen = true;
        mem.result_total = 11;
        mem.finished_at = Some("2026-09-15T00:00:00Z".to_owned());
        // input and command are missing in memory
        assert!(!mem.input_seen);
        assert!(mem.command.is_none());

        let mut stored = ToolRecord::new("bash".to_owned());
        stored.state = ToolExecutionState::Succeeded;
        stored.phase = Some(ToolPhase::Running);
        stored.started_at = Some("2026-09-14T00:00:00Z".to_owned());
        stored.finished_at = Some("2026-09-14T00:00:01Z".to_owned());
        stored.result = "cmd output\n".to_owned();
        stored.result_seen = true;
        stored.result_total = 11;
        stored.input = "echo hello".to_owned();
        stored.input_seen = true;
        stored.input_total = 10;
        stored.stdout.seen = true;
        stored.stdout.bytes = b"cmd output\n".to_vec();
        stored.stdout.start_offset = 0;
        stored.stdout.observed_end = 11;
        stored.stdout.complete = true;
        stored.command = Some(CommandResult {
            status: CommandStatus::Exited,
            exit_code: Some(0),
            signal: None,
            termination_confirmed: true,
            stdout_base_offset: 0,
            stdout_observed_end: 11,
            stderr_base_offset: 0,
            stderr_observed_end: 0,
            output_complete: true,
            output_truncated: false,
        });

        assert!(mem.merge_stored(stored));
        assert!(mem.input_seen);
        assert_eq!(mem.input, "echo hello");
        assert_eq!(mem.input_total, 10);
        assert_eq!(mem.phase, Some(ToolPhase::Running));
        assert_eq!(mem.started_at.as_deref(), Some("2026-09-14T00:00:00Z"));
        assert_eq!(mem.finished_at.as_deref(), Some("2026-09-14T00:00:01Z"));
        let cmd = mem.command.expect("command restored from disk");
        assert_eq!(cmd.status, CommandStatus::Exited);
        assert_eq!(cmd.stdout_observed_end, 11);
    }

    #[test]
    fn large_file_snapshots_are_evicted_by_real_capacity() {
        let data = ToolData::new();
        let before = vec![b'b'; crate::changes::MAX_CHANGE_SNAPSHOT_BYTES];
        let after = vec![b'a'; crate::changes::MAX_CHANGE_SNAPSHOT_BYTES];
        let session_id = session(1);
        let mut refs = Vec::new();
        for index in 0..8 {
            let tool_ref = make_tool_ref(
                session_id,
                loop_id(index as u8 + 1),
                index,
                &format!("write-{index}"),
            );
            data.note_requested(&tool_ref, "write");
            data.note_file_change(
                &tool_ref,
                FileChange {
                    path: format!("file-{index}.txt"),
                    kind: crate::changes::ChangeKind::Modified,
                    before: crate::changes::content_revision(&before),
                    after: crate::changes::content_revision(&after),
                    commit_state: crate::changes::ChangeCommitState::Applied,
                    coverage: crate::changes::ChangeCoverage::Complete,
                    before_captured: true,
                    after_captured: true,
                    before_bytes: Some(before.clone()),
                    after_bytes: Some(after.clone()),
                    before_corrupt: false,
                    after_corrupt: false,
                },
            );
            refs.push(tool_ref);
        }
        let first = data.get_record(&refs[0]).unwrap();
        let first_change = first.file_change.as_ref().unwrap();
        assert!(first_change.before_bytes.is_none());
        assert!(first_change.after_bytes.is_none());
        assert_eq!(data.file_change_records(session_id, None).len(), 8);
        assert!(data.get_record(&refs[7]).unwrap().file_change.is_some());
    }
}
