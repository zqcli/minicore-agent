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

mod data;
mod paging;
mod record;
mod types;

pub(crate) use data::ToolData;
pub(crate) use record::{ToolPersistenceSnapshot, ToolRecord};
pub(crate) use types::ToolStreamNotice;
pub use types::{
    CommandResult, CommandStatus, ToolDataAvailability, ToolDataStream, ToolExecutionData,
    ToolExecutionState, ToolInputSummary, ToolInvocationData, ToolOutputPage, ToolOutputRequest,
    ToolPhase, ToolProcessChunk, ToolProcessData, ToolReadRequest, ToolReadResult,
    ToolRecordingState, ToolRef, ToolSubject,
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

#[cfg(test)]
mod tests;
