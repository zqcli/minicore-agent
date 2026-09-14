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
//! This module intentionally covers only the model-facing per-tool result
//! output. Bash stdout/stderr streaming, process ownership, and durable
//! auxiliary files arrive with their own real consumer in P5.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use minicore_runtime::history::HistoryItem;
use minicore_runtime::tools::{ToolInvocation, ToolResultOutcome};
use minicore_runtime::{LoopId, ToolCallId};

use crate::error::AgentError;
use crate::ids::SessionId;

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

const INPUT_ENCODING: &str = "utf8_json";
const OUTPUT_ENCODING: &str = "utf8";

/// One model-facing byte channel. P2 produces `Input` (requested invocation
/// JSON) and `Output` (the recorded tool result text); process streams are
/// added in P5.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDataStream {
    Input,
    Output,
}

impl ToolDataStream {
    const fn encoding(self) -> &'static str {
        match self {
            Self::Input => INPUT_ENCODING,
            Self::Output => OUTPUT_ENCODING,
        }
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
    Succeeded,
    Failed,
    Denied,
    Cancelled,
    InputProvided,
}

impl ToolExecutionState {
    const fn from_outcome(outcome: ToolResultOutcome) -> Self {
        match outcome {
            ToolResultOutcome::Success => Self::Succeeded,
            ToolResultOutcome::Failed => Self::Failed,
            ToolResultOutcome::Denied => Self::Denied,
            ToolResultOutcome::Cancelled => Self::Cancelled,
            ToolResultOutcome::InputProvided => Self::InputProvided,
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

/// Current or final execution facts for one tool call.
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

struct ToolRecord {
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
}

impl ToolRecord {
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
        }
    }

    /// Counts retained raw bytes and bounded metadata, so the Session budget
    /// matches what is actually held in memory.
    fn retained_bytes(&self) -> usize {
        self.name
            .len()
            .saturating_add(subject_len(&self.subject))
            .saturating_add(self.input.len())
            .saturating_add(self.result.len())
    }

    fn input_availability(&self) -> ToolDataAvailability {
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
        }
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

    pub(crate) fn read(
        &self,
        request: &ToolReadRequest,
        max_bytes: usize,
    ) -> Result<ToolReadResult, AgentError> {
        let inner = self.lock();
        let Some(record) = inner.records.get(&request.tool_ref) else {
            return Err(AgentError::ToolNotFound);
        };
        let execution = record.execution_data(&request.tool_ref);
        // Invocation data is the requested input; expose it whenever the
        // stream was observed or evicted, not only while bytes remain.
        let invocation = (record.input_seen || record.input_expired).then(|| {
            let (preview, cut) = truncate_prefix(&record.input, MAX_INVOCATION_PREVIEW_BYTES);
            ToolInvocationData {
                tool_ref: request.tool_ref.clone(),
                name: record.name.clone(),
                subject: record.subject.clone(),
                subject_truncated: record.subject_truncated,
                input: ToolInputSummary {
                    total_bytes: record.input_total,
                    preview,
                    truncated: cut || record.input_truncated || record.input_expired,
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

    pub(crate) fn output(
        &self,
        request: &ToolOutputRequest,
        max_bytes: usize,
    ) -> Result<ToolOutputPage, AgentError> {
        let inner = self.lock();
        let Some(record) = inner.records.get(&request.tool_ref) else {
            return Err(AgentError::ToolNotFound);
        };
        let stream = request.stream;
        let (content, observed, seen, expired, truncated) = match stream {
            ToolDataStream::Input => (
                record.input.as_str(),
                record.input_total,
                record.input_seen,
                record.input_expired,
                record.input_truncated,
            ),
            ToolDataStream::Output => (
                record.result.as_str(),
                record.result_total,
                record.result_seen,
                record.result_expired,
                record.result_truncated,
            ),
        };
        let availability = stream_availability(
            seen,
            expired,
            record.state.is_terminal(),
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
            // The window is empty and sits at the observed end.
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
            // The stream has no recorded bytes. For a terminal call that is an
            // explicit `unavailable`; for a running call it is `pending`. A
            // non-zero offset is invalid either way.
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
            // The bytes existed but fell outside the retained prefix. Report
            // the loss without moving `next_offset` backwards.
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
        // `u64::MAX` reserves the widest possible `next_offset` digits so the
        // final page can never exceed the budget after the real value lands.
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
        // `truncated` means real byte loss (the stored stream is only a
        // prefix), not page-budget pagination: an incomplete page is expressed
        // by `next_offset` with `eof: false`.
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
}

fn bounded_page(page: ToolOutputPage, max_bytes: usize) -> Result<ToolOutputPage, AgentError> {
    if encoded_len(&page)? > max_bytes {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(page)
    }
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
    use serde_json::json;

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
}
