use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader,
};
use tokio_util::sync::CancellationToken;

use minicore_runtime::LoopId;
use minicore_runtime::execution::ConfigRevision;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{ModelError, ModelErrorKind, RetryHint, Usage};
use minicore_runtime::tools::ToolResultOutcome;

use crate::changes::{ChangeRevision, StoredFileChange};
use crate::error::StoreError;
use crate::history::sanitize_history;
use crate::ids::SessionId;
use crate::models::Models;
use crate::profiles::ApprovalMode;
use crate::tool_data::{
    CommandResult, ToolExecutionState, ToolPersistenceSnapshot, ToolPhase, ToolRecord, ToolRef,
    ToolSubject,
};
use crate::tools::KNOWN_TOOL_NAMES;

mod history;
mod tool_records;
#[cfg(test)]
use tool_records::change_blob_read_cost;

pub(crate) const SESSION_FORMAT_VERSION: u32 = 1;
const LEGACY_STORED_TOOL_NAME: &str = "subagent";

const SESSIONS_DIR: &str = "sessions";
pub(crate) const SESSION_RECORD_FILE: &str = "session.json";
const HISTORY_FILE: &str = "history.jsonl";
pub(crate) const SUMMARY_FILE: &str = "summary.json";
pub(crate) const MAX_SUMMARY_FILE_BYTES: usize = 256 * 1024;
const LEGACY_MANIFEST_FILE: &str = "manifest.json";
const LEGACY_CONVERSATION_FILE: &str = "conversation.log";
const METADATA_TEMP_SUFFIX: &str = ".tmp";
const MAX_LOOP_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_SESSION_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROFILE_BYTES: usize = 256;
const MAX_WORKSPACE_BYTES: usize = 4_096;
const MAX_TITLE_BYTES: usize = 4_096;
const MAX_TIMESTAMP_BYTES: usize = 64;
const MAX_SYSTEM_PROMPT_BYTES: usize = 128 * 1024;

pub(crate) const AUX_TOOLS_DIR: &str = "tools";
pub(crate) const TOOL_RECORD_FILE: &str = "record.json";
pub(crate) const TOOL_INPUT_FILE: &str = "input.bin";
pub(crate) const TOOL_RESULT_FILE: &str = "result.bin";
pub(crate) const TOOL_STDOUT_FILE: &str = "stdout.bin";
pub(crate) const TOOL_STDERR_FILE: &str = "stderr.bin";
pub(crate) const TOOL_BEFORE_FILE: &str = "before.bin";
pub(crate) const TOOL_AFTER_FILE: &str = "after.bin";
pub(crate) const ALLOWED_AUX_FILES: [&str; 7] = [
    TOOL_RECORD_FILE,
    TOOL_INPUT_FILE,
    TOOL_RESULT_FILE,
    TOOL_STDOUT_FILE,
    TOOL_STDERR_FILE,
    TOOL_BEFORE_FILE,
    TOOL_AFTER_FILE,
];
pub(crate) const TOOL_RECORD_FORMAT_VERSION: u32 = 1;
pub(crate) const MAX_TOOL_METADATA_BYTES: usize = 64 * 1024;
pub(crate) const MAX_TOOL_INPUT_PERSIST_BYTES: usize = 64 * 1024;
pub(crate) const MAX_TOOL_RESULT_PERSIST_BYTES: usize = 256 * 1024;
pub(crate) const DEFAULT_SESSION_AUX_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_SESSION_AUX_RECORDS: usize = 1024;
pub(crate) const DEFAULT_GLOBAL_AUX_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_GLOBAL_AUX_RECORDS: usize = 8192;
pub(crate) const MAX_AUX_SCAN_ENTRIES: usize = 65536;
pub(crate) const CHANGE_SCAN_METADATA_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const CHANGE_SCAN_BLOB_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const AUX_PERSIST_DEADLINE: Duration = Duration::from_secs(10);

static NEXT_TEMP_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[cfg(test)]
static APPEND_FAILURES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
static RECORD_WRITE_FAILURES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
static SUMMARY_WRITE_FAILURES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
static AUX_WRITE_FAILURES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
static READ_CHANGE_BLOBS: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
#[cfg(test)]
static FAIL_REMOVE_TEMP_SESSIONS: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
type AuxCommitGateEntry = (SessionId, Arc<AuxCommitGate>);
#[cfg(test)]
static AUX_COMMIT_GATES: OnceLock<Mutex<Vec<AuxCommitGateEntry>>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct AuxCommitGate {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

#[cfg(test)]
impl AuxCommitGate {
    pub(crate) fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

#[cfg(test)]
pub(crate) fn register_aux_commit_gate(session_id: SessionId, gate: Arc<AuxCommitGate>) {
    let mutex = AUX_COMMIT_GATES.get_or_init(|| Mutex::new(Vec::new()));
    mutex.lock().unwrap().push((session_id, gate));
}

#[cfg(test)]
async fn wait_aux_commit_gate(session_id: SessionId) {
    let gate = {
        let Some(mutex) = AUX_COMMIT_GATES.get() else {
            return;
        };
        let mut entries = mutex.lock().unwrap();
        let Some(pos) = entries.iter().position(|(s, _)| *s == session_id) else {
            return;
        };
        entries.remove(pos).1
    };
    gate.entered.notify_one();
    gate.release.notified().await;
}

#[cfg(test)]
pub(crate) fn fail_next_remove_temp(session_id: SessionId) {
    let mutex = FAIL_REMOVE_TEMP_SESSIONS.get_or_init(|| Mutex::new(Vec::new()));
    mutex.lock().unwrap().push(session_id);
}

#[cfg(test)]
fn should_fail_remove_temp(session_id: SessionId) -> bool {
    let Some(mutex) = FAIL_REMOVE_TEMP_SESSIONS.get() else {
        return false;
    };
    let mut failures = mutex.lock().unwrap();
    if let Some(pos) = failures.iter().position(|&s| s == session_id) {
        failures.remove(pos);
        true
    } else {
        false
    }
}
#[cfg(test)]
type SummaryCommitGateEntry = (SessionId, Arc<SummaryCommitGate>);
#[cfg(test)]
static SUMMARY_COMMIT_GATES: OnceLock<Mutex<Vec<SummaryCommitGateEntry>>> = OnceLock::new();
#[cfg(test)]
static SUMMARY_UNKNOWN_WRITES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn take_read_change_blobs_under(prefix: &Path) -> Vec<PathBuf> {
    let mut entries = READ_CHANGE_BLOBS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    let mut taken = Vec::new();
    entries.retain(|path| {
        if path.starts_with(prefix) {
            taken.push(path.clone());
            false
        } else {
            true
        }
    });
    taken
}

#[cfg(test)]
pub(crate) fn fail_next_aux_write(session_id: SessionId) {
    let mutex = AUX_WRITE_FAILURES.get_or_init(|| Mutex::new(Vec::new()));
    mutex.lock().unwrap().push(session_id);
}

#[cfg(test)]
fn should_fail_aux_write(session_id: SessionId) -> bool {
    let Some(mutex) = AUX_WRITE_FAILURES.get() else {
        return false;
    };
    let mut failures = mutex.lock().unwrap();
    if let Some(pos) = failures.iter().position(|item| *item == session_id) {
        failures.remove(pos);
        true
    } else {
        false
    }
}

#[cfg(test)]
pub(crate) struct SummaryCommitGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl SummaryCommitGate {
    pub(crate) fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SummaryCommit {
    Committed,
    Rejected,
    Unknown,
}

/// Persistent per-session product state owned by the Agent. Creating a session
/// copies the profile's defaults into this record, so later profile edits (or
/// deletion) never change an existing session.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionRecord {
    pub(crate) format_version: u32,
    pub(crate) session_id: SessionId,
    pub(crate) title: Option<String>,
    pub(crate) profile: String,
    pub(crate) workspace: PathBuf,
    pub(crate) model: String,
    pub(crate) reasoning: minicore_runtime::model::ReasoningPreference,
    pub(crate) system_prompt: String,
    pub(crate) tools: Vec<String>,
    pub(crate) max_tool_rounds: u16,
    pub(crate) approval: ApprovalMode,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

impl SessionRecord {
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        let mut seen_tools = BTreeSet::new();
        let valid_tools = self.tools.len() <= KNOWN_TOOL_NAMES.len() + 1
            && self.tools.iter().all(|name| {
                (KNOWN_TOOL_NAMES.contains(&name.as_str()) || name == LEGACY_STORED_TOOL_NAME)
                    && seen_tools.insert(name.as_str())
            });

        if self.format_version != SESSION_FORMAT_VERSION
            || !valid_text(&self.profile, MAX_PROFILE_BYTES, false)
            || Models::model_ref(&self.model).is_err()
            || self.workspace.as_os_str().is_empty()
            || self.workspace.as_os_str().len() > MAX_WORKSPACE_BYTES
            || !valid_multiline_text(&self.system_prompt, MAX_SYSTEM_PROMPT_BYTES, false)
            || !(1..=1_024).contains(&self.max_tool_rounds)
            || !valid_tools
            || !valid_timestamp(&self.created_at)
            || !valid_timestamp(&self.updated_at)
            || self
                .title
                .as_deref()
                .is_some_and(|title| !valid_text(title, MAX_TITLE_BYTES, true))
        {
            return Err(StoreError::InvalidRecord);
        }
        Ok(())
    }
}

pub(crate) fn normalize_title(title: &str) -> Result<Option<String>, ()> {
    let title = title.trim();
    if title.is_empty() {
        return Ok(None);
    }
    if valid_text(title, MAX_TITLE_BYTES, false) {
        Ok(Some(title.to_owned()))
    } else {
        Err(())
    }
}

/// A loaded session: its persistent record plus a sanitized in-memory history.
pub(crate) struct StoredSession {
    pub(crate) record: SessionRecord,
    pub(crate) history: std::sync::Arc<[HistoryItem]>,
    /// `(loop_id, occurrence)` -> RFC3339 acceptance time, collected from the
    /// loop records so history views can show persisted user timestamps.
    pub(crate) user_times: std::collections::HashMap<(LoopId, usize), String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoryPrefix {
    pub(crate) prefix_bytes: u64,
    pub(crate) covered_loop_count: u64,
    pub(crate) covered_item_count: u64,
    pub(crate) last_loop_id: Option<LoopId>,
    pub(crate) sha256: String,
}

pub(crate) struct HistoryScanLimits {
    pub(crate) max_bytes: u64,
    pub(crate) max_lines: usize,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: CancellationToken,
}

pub(crate) struct HistoryReadPage {
    pub(crate) captured_end: u64,
    pub(crate) revision: String,
    pub(crate) trailing_incomplete: bool,
    pub(crate) total_items: usize,
    pub(crate) items: Vec<HistoryItem>,
    pub(crate) user_times: Vec<Option<String>>,
    pub(crate) turns: Vec<StoredTurnSummary>,
    pub(crate) turns_truncated: bool,
}

#[derive(Clone)]
pub(crate) struct StoredTurnSummary {
    pub(crate) item_start: usize,
    pub(crate) item_end: usize,
    pub(crate) loop_id: LoopId,
    pub(crate) outcome: StoredLoopOutcome,
    pub(crate) usage: Usage,
    pub(crate) requests: u32,
    pub(crate) tool_rounds: u16,
    pub(crate) final_config_revision: ConfigRevision,
    pub(crate) completed_at: String,
}

const MAX_READ_TURN_SUMMARIES: usize = 64;

/// One completed agent loop, stored as a single JSON line.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredLoopRecord {
    pub(crate) loop_id: LoopId,
    pub(crate) outcome: StoredLoopOutcome,
    pub(crate) items: Vec<HistoryItem>,
    pub(crate) usage: Usage,
    pub(crate) requests: u32,
    pub(crate) tool_rounds: u16,
    pub(crate) final_config_revision: ConfigRevision,
    pub(crate) completed_at: String,
    /// RFC3339 acceptance times aligned by User-item occurrence (Prompt first,
    /// then applied Steers). Missing in old JSONL lines, which stay readable.
    #[serde(
        default,
        deserialize_with = "deserialize_user_times",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) user_times: Option<Vec<Option<String>>>,
}

fn deserialize_user_times<'de, D>(deserializer: D) -> Result<Option<Vec<Option<String>>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let serde_json::Value::Array(values) = value else {
        return Ok(None);
    };
    Ok(Some(
        values
            .into_iter()
            .map(|value| match value {
                serde_json::Value::String(timestamp) if valid_timestamp(&timestamp) => {
                    Some(timestamp)
                }
                _ => None,
            })
            .collect(),
    ))
}

impl StoredLoopRecord {
    fn user_times_are_valid(&self) -> bool {
        let Some(times) = &self.user_times else {
            return true;
        };
        let user_count = self
            .items
            .iter()
            .filter(|item| matches!(item, HistoryItem::User(_)))
            .count();
        times.len() <= user_count
            && times
                .iter()
                .flatten()
                .all(|timestamp| valid_timestamp(timestamp))
    }

    pub(crate) fn normalized_user_times(&self) -> Option<Vec<Option<String>>> {
        let user_count = self
            .items
            .iter()
            .filter(|item| matches!(item, HistoryItem::User(_)))
            .count();
        let Some(times) = &self.user_times else {
            return None;
        };
        if times.len() > user_count {
            return None;
        }
        Some(
            times
                .iter()
                .map(|timestamp| {
                    timestamp
                        .as_deref()
                        .filter(|timestamp| valid_timestamp(timestamp))
                        .map(str::to_owned)
                })
                .collect(),
        )
    }
}

/// Stored record of how a loop ended. This is record-keeping only; it never
/// reconstructs a runtime `LoopReport`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StoredLoopOutcome {
    Completed,
    Cancelled {
        reason: StoredCancelReason,
    },
    Failed {
        kind: String,
        model_error: Option<StoredModelError>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoredCancelReason {
    User,
    OwnerDropped,
    Shutdown,
    Deadline,
}

/// Safe, redacted model failure summary. Provider bodies, diagnostic text,
/// API keys, and prompts are never persisted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredModelError {
    pub(crate) kind: String,
    pub(crate) delivery: String,
    pub(crate) retryable: bool,
    pub(crate) retry_after_millis: Option<u64>,
}

impl StoredModelError {
    pub(crate) fn from_model(error: &ModelError) -> Self {
        let retry_after_millis = match error.retry_hint() {
            RetryHint::Never => None,
            RetryHint::Retryable { retry_after } => {
                retry_after.and_then(|duration| u64::try_from(duration.as_millis()).ok())
            }
        };
        Self {
            kind: model_error_kind(error.kind()),
            delivery: delivery_state(error.delivery()),
            retryable: error.diagnostic().retryable,
            retry_after_millis,
        }
    }
}

pub(crate) fn model_error_kind(kind: ModelErrorKind) -> String {
    match kind {
        ModelErrorKind::InvalidRequest => "invalid_request".to_owned(),
        ModelErrorKind::Unavailable => "unavailable".to_owned(),
        ModelErrorKind::ProviderUnavailable => "provider_unavailable".to_owned(),
        ModelErrorKind::AuthMissing => "auth_missing".to_owned(),
        ModelErrorKind::AuthRejected => "auth_rejected".to_owned(),
        ModelErrorKind::RateLimited => "rate_limited".to_owned(),
        ModelErrorKind::QuotaExceeded => "quota_exceeded".to_owned(),
        ModelErrorKind::ContextOverflow => "context_overflow".to_owned(),
        ModelErrorKind::Timeout => "timeout".to_owned(),
        ModelErrorKind::TransportUnavailable => "transport_unavailable".to_owned(),
        ModelErrorKind::Cancelled => "cancelled".to_owned(),
        ModelErrorKind::InvalidProviderResponse => "invalid_provider_response".to_owned(),
        ModelErrorKind::IncompleteResponse => "incomplete_response".to_owned(),
        ModelErrorKind::StreamInterrupted => "stream_interrupted".to_owned(),
        ModelErrorKind::RequestOutcomeUnknown => "request_outcome_unknown".to_owned(),
        ModelErrorKind::UnexpectedToolCall => "unexpected_tool_call".to_owned(),
        ModelErrorKind::Panicked => "panicked".to_owned(),
        ModelErrorKind::Internal => "internal".to_owned(),
    }
}

pub(crate) fn delivery_state(delivery: minicore_runtime::model::DeliveryState) -> String {
    match delivery {
        minicore_runtime::model::DeliveryState::NotStarted => "not_started".to_owned(),
        minicore_runtime::model::DeliveryState::Started => "started".to_owned(),
        minicore_runtime::model::DeliveryState::Unknown => "unknown".to_owned(),
    }
}

#[derive(Default, Clone)]
pub(crate) struct Store {
    root: PathBuf,
    aux_lock: Arc<tokio::sync::Mutex<()>>,
    diff_workers: Arc<Mutex<DiffWorkers>>,
    #[cfg(test)]
    aux_limits: Option<AuxLimits>,
}

/// `changes.diff` workers run at most this many CPU comparisons at a time.
pub(crate) const MAX_DIFF_WORKERS: usize = 4;

/// Owned blocking handlers for `changes.diff`. Each handle is retained with its
/// cancellation token and session identity so the Store can always cancel and
/// join it, for a loaded Session or an unloaded one.
#[derive(Default)]
struct DiffWorkers {
    closing: bool,
    workers: Vec<Arc<OwnedDiffWorker>>,
}

impl Drop for DiffWorkers {
    fn drop(&mut self) {
        // Ordinary Agent drop is non-blocking, but every owned comparison is
        // still cancelled so its blocking handler stops promptly instead of
        // running to its own deadline.
        for worker in &self.workers {
            worker.cancel.cancel();
        }
    }
}

/// One owned `changes.diff` comparison. The join handle lives behind a tokio
/// mutex and is taken only after it really finished, so a shutdown future that
/// is dropped mid-join never detaches it and a second or concurrent join either
/// waits on the same handle or observes its completion.
struct OwnedDiffWorker {
    session_id: SessionId,
    cancel: CancellationToken,
    join: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl OwnedDiffWorker {
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
        let _ = std::pin::Pin::new(handle).await;
        slot.take();
    }
}

/// One owned `changes.diff` CPU comparison. Awaiting it observes the blocking
/// result only; dropping it cancels that comparison without losing the handle.
pub(crate) struct DiffQuery {
    receiver:
        tokio::sync::oneshot::Receiver<Result<crate::diff::DiffOutcome, crate::error::AgentError>>,
    child_cancel: CancellationToken,
}

impl DiffQuery {
    pub(crate) async fn wait(
        mut self,
    ) -> Result<crate::diff::DiffOutcome, crate::error::AgentError> {
        match (&mut self.receiver).await {
            Ok(result) => result,
            Err(_) => Err(crate::error::AgentError::Internal),
        }
    }
}

impl Drop for DiffQuery {
    fn drop(&mut self) {
        self.child_cancel.cancel();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredToolRecord {
    pub(crate) version: u32,
    pub(crate) tool_ref: ToolRef,
    pub(crate) name: String,
    pub(crate) subject: ToolSubject,
    pub(crate) subject_truncated: bool,
    pub(crate) state: ToolExecutionState,
    pub(crate) phase: Option<ToolPhase>,
    pub(crate) started_at: Option<String>,
    pub(crate) finished_at: Option<String>,
    pub(crate) outcome: Option<ToolResultOutcome>,
    pub(crate) input: StoredInputSummary,
    pub(crate) result: StoredResultSummary,
    pub(crate) stdout: StoredStreamWindow,
    pub(crate) stderr: StoredStreamWindow,
    pub(crate) command: Option<CommandResult>,
    #[serde(default)]
    pub(crate) file_change: Option<StoredFileChange>,
}

struct ReadToolRecord {
    metadata: StoredToolRecord,
    record: ToolRecord,
}

pub(crate) struct ToolChangeScan {
    pub(crate) records: Vec<(ToolRef, StoredFileChange)>,
    pub(crate) complete: bool,
    pub(crate) skipped: bool,
    #[cfg(test)]
    pub(crate) entries: usize,
    #[cfg(test)]
    pub(crate) metadata_bytes: usize,
    pub(crate) observation: String,
}

impl ToolChangeScan {
    /// The fail-closed fallback used when the durable scan is unavailable but
    /// warm in-memory records still exist. It carries no records and no budget
    /// accounting across the `cfg(test)` field boundary.
    pub(crate) fn unavailable() -> Self {
        Self {
            records: Vec::new(),
            complete: false,
            skipped: true,
            #[cfg(test)]
            entries: 0,
            #[cfg(test)]
            metadata_bytes: 0,
            observation: hash_bytes(b"change-scan-store-unavailable"),
        }
    }
}

pub(crate) struct ChangeBlobBudget {
    pub(crate) used_bytes: usize,
    pub(crate) exhausted: bool,
}

#[derive(Default)]
struct ChangeScanBudget {
    entries: usize,
    metadata_bytes: usize,
}

enum ChangeMetadataRead {
    Record(Option<Box<StoredToolRecord>>),
    BudgetExhausted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredInputSummary {
    pub(crate) total_bytes: usize,
    pub(crate) seen: bool,
    pub(crate) truncated: bool,
    pub(crate) expired: bool,
    pub(crate) file_bytes: usize,
    pub(crate) file_sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredResultSummary {
    pub(crate) total_bytes: usize,
    pub(crate) seen: bool,
    pub(crate) truncated: bool,
    pub(crate) expired: bool,
    pub(crate) file_bytes: usize,
    pub(crate) file_sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredStreamWindow {
    pub(crate) start_offset: u64,
    pub(crate) observed_end: u64,
    pub(crate) seen: bool,
    pub(crate) complete: bool,
    pub(crate) truncated: bool,
    pub(crate) expired: bool,
    pub(crate) file_bytes: usize,
    pub(crate) file_sha256: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuxLimits {
    pub(crate) session_bytes: u64,
    pub(crate) session_records: usize,
    pub(crate) global_bytes: u64,
    pub(crate) global_records: usize,
    pub(crate) max_scan_entries: usize,
}

pub(crate) const DEFAULT_AUX_LIMITS: AuxLimits = AuxLimits {
    session_bytes: DEFAULT_SESSION_AUX_BYTES,
    session_records: DEFAULT_SESSION_AUX_RECORDS,
    global_bytes: DEFAULT_GLOBAL_AUX_BYTES,
    global_records: DEFAULT_GLOBAL_AUX_RECORDS,
    max_scan_entries: MAX_AUX_SCAN_ENTRIES,
};

impl Store {
    pub(crate) async fn open(root: PathBuf) -> Result<Self, StoreError> {
        if root.as_os_str().is_empty() {
            return Err(StoreError::InvalidRoot);
        }
        // The root itself must be a real directory: never follow a `data_dir`
        // symlink indirectly through `root/sessions`.
        match path_state(&root)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => {}
            PathState::Missing => {
                fs::create_dir_all(&root)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
            }
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                return Err(StoreError::InvalidRoot);
            }
        }
        let sessions = root.join(SESSIONS_DIR);
        match path_state(&sessions)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => {}
            PathState::Missing => {
                fs::create_dir_all(&sessions)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
            }
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                return Err(StoreError::InvalidRoot);
            }
        }
        Ok(Self {
            root,
            aux_lock: Arc::new(tokio::sync::Mutex::new(())),
            diff_workers: Arc::new(Mutex::new(DiffWorkers::default())),
            #[cfg(test)]
            aux_limits: None,
        })
    }

    pub(crate) async fn list_sessions(&self) -> Result<Vec<SessionRecord>, StoreError> {
        self.require_sessions_root().await?;
        let mut directory = fs::read_dir(self.sessions_directory())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut records = Vec::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            let state = match path_state(&entry.path()).await {
                Ok(state) => state,
                Err(_) => continue,
            };
            if state != PathState::Directory {
                continue;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Ok(session_id) = name.parse::<SessionId>() else {
                continue;
            };
            match self.load_record(session_id).await {
                Ok(record) => records.push(record),
                Err(error) => {
                    tracing::warn!(
                        session_id = %session_id,
                        error_kind = error.kind(),
                        "skipping unreadable session"
                    );
                }
            }
        }
        records.sort_by_key(|record| record.session_id);
        Ok(records)
    }

    pub(crate) async fn create_session(&self, record: &SessionRecord) -> Result<(), StoreError> {
        record.validate()?;
        if record
            .tools
            .iter()
            .any(|name| name == LEGACY_STORED_TOOL_NAME)
        {
            return Err(StoreError::InvalidRecord);
        }
        self.require_sessions_root().await?;
        let directory = self.session_directory(record.session_id);
        match path_state(&directory)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Missing => {
                fs::create_dir(&directory)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
            }
            PathState::Directory => return Err(StoreError::SessionAlreadyExists),
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let result = async {
            self.write_record(record).await?;
            let directory = self.require_session_directory(record.session_id).await?;
            let history_path = directory.join(HISTORY_FILE);
            match path_state(&history_path)
                .await
                .map_err(|_| StoreError::Unavailable)?
            {
                PathState::Missing => {
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&history_path)
                        .await
                        .map_err(|_| StoreError::Unavailable)?;
                }
                PathState::RegularFile => {}
                PathState::Directory | PathState::Symlink | PathState::Other => {
                    return Err(StoreError::Corrupt);
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            self.remove_created_session_directory(&directory).await;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn load_session(
        &self,
        session_id: SessionId,
    ) -> Result<StoredSession, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let record = Self::read_record(&directory.join(SESSION_RECORD_FILE), session_id).await?;
        let (history, user_times) = self
            .load_history(&directory.join(HISTORY_FILE), session_id)
            .await?;
        Ok(StoredSession {
            record,
            history,
            user_times,
        })
    }

    /// Reads only the bounded derived snapshot file. A missing, non-regular,
    /// oversized, or unreadable derived file is ignored; core session data is
    /// never made unreadable by this optional file.
    pub(crate) async fn read_summary_bytes(
        &self,
        session_id: SessionId,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(SUMMARY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Missing | PathState::Symlink | PathState::Directory | PathState::Other => {
                return Ok(None);
            }
            PathState::RegularFile => {}
        }
        let metadata = match fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(_) => return Ok(None),
        };
        // Metadata is only a fast rejection; the bounded reader remains
        // authoritative if the file grows after this check.
        if metadata.len() > MAX_SUMMARY_FILE_BYTES as u64 {
            return Ok(None);
        }
        let file = match File::open(&path).await {
            Ok(file) => file,
            Err(_) => return Ok(None),
        };
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        if file
            .take((MAX_SUMMARY_FILE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .is_err()
        {
            return Ok(None);
        }
        if bytes.len() > MAX_SUMMARY_FILE_BYTES {
            return Ok(None);
        }
        Ok(Some(bytes))
    }

    /// Revalidates the complete settled-history anchor immediately before an
    /// atomic derived snapshot replacement. A changed source rejects the
    /// commit without touching the existing summary.
    pub(crate) async fn commit_summary(
        &self,
        session_id: SessionId,
        expected_anchor: &HistoryPrefix,
        expected_history: &[HistoryItem],
        bytes: &[u8],
        deadline: Instant,
        commit_point: impl FnOnce() -> bool + Send,
    ) -> Result<SummaryCommit, StoreError> {
        if bytes.len() > MAX_SUMMARY_FILE_BYTES {
            return Ok(SummaryCommit::Rejected);
        }
        #[cfg(test)]
        if let Some(gate) = take_summary_commit_gate(session_id) {
            gate.entered.notify_one();
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
            else {
                return Ok(SummaryCommit::Rejected);
            };
            if tokio::time::timeout(remaining, gate.release.notified())
                .await
                .is_err()
            {
                return Ok(SummaryCommit::Rejected);
            }
        }
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return Ok(SummaryCommit::Rejected);
        };
        let actual_anchor = match tokio::time::timeout(
            remaining,
            self.capture_history_anchor(session_id, expected_history),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Ok(SummaryCommit::Rejected),
        };
        let Some(actual_anchor) = actual_anchor else {
            return Ok(SummaryCommit::Rejected);
        };
        if &actual_anchor != expected_anchor {
            return Ok(SummaryCommit::Rejected);
        }
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return Ok(SummaryCommit::Rejected);
        };
        let directory =
            match tokio::time::timeout(remaining, self.require_session_directory(session_id)).await
            {
                Ok(result) => result?,
                Err(_) => return Ok(SummaryCommit::Rejected),
            };
        if !commit_point() {
            return Ok(SummaryCommit::Rejected);
        }
        #[cfg(test)]
        if should_force_unknown_summary_write(session_id) {
            return Ok(SummaryCommit::Unknown);
        }
        #[cfg(test)]
        if should_fail_summary_write(session_id) {
            return Err(StoreError::Unavailable);
        }
        let path = directory.join(SUMMARY_FILE);
        atomic_write_status(&path, bytes).await
    }

    pub(crate) async fn write_record(&self, record: &SessionRecord) -> Result<(), StoreError> {
        record.validate()?;
        let directory = self.require_session_directory(record.session_id).await?;
        #[cfg(test)]
        if should_fail_record_write(record.session_id) {
            return Err(StoreError::Unavailable);
        }
        let path = directory.join(SESSION_RECORD_FILE);
        let bytes = serde_json::to_vec(record).map_err(|_| StoreError::Corrupt)?;
        atomic_write(&path, &bytes).await
    }

    /// Starts one owned `changes.diff` CPU comparison for `session_id`.
    /// Registration and the capacity check happen under the same lock as the
    /// closing flag, so a Store that starts closing either observes this worker
    /// or refuses it. Test-only: production callers pass an admission token
    /// through `spawn_cancellable_diff_query` so a closing Session is observed.
    #[cfg(test)]
    pub(crate) fn spawn_diff_query(
        &self,
        session_id: SessionId,
        before: Arc<[u8]>,
        after: Arc<[u8]>,
        context_lines: usize,
        deadline: Instant,
    ) -> Result<DiffQuery, StoreError> {
        self.spawn_cancellable_diff_query(
            session_id,
            before,
            after,
            context_lines,
            deadline,
            &CancellationToken::new(),
        )
    }

    /// Starts one owned `changes.diff` CPU comparison for `session_id`, deriving
    /// the comparison's stop token from `admission` so a closing Session or
    /// Agent cancels it. Registration and the capacity check happen under the
    /// same lock as the closing flag, so a Store that starts closing either
    /// observes this worker or refuses it.
    pub(crate) fn spawn_cancellable_diff_query(
        &self,
        session_id: SessionId,
        before: Arc<[u8]>,
        after: Arc<[u8]>,
        context_lines: usize,
        deadline: Instant,
        admission: &CancellationToken,
    ) -> Result<DiffQuery, StoreError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let child_cancel = admission.child_token();
        let worker_cancel = child_cancel.clone();
        let mut workers = self.diff_workers.lock().unwrap();
        workers.workers.retain(|worker| !worker.is_finished());
        if workers.closing || admission.is_cancelled() || workers.workers.len() >= MAX_DIFF_WORKERS
        {
            return Err(StoreError::QueryLimit);
        }
        let handle = tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                crate::diff::plan_diff(
                    before.as_ref(),
                    after.as_ref(),
                    context_lines,
                    &worker_cancel,
                    deadline,
                )
            })
            .await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(crate::error::AgentError::Internal),
            };
            let _ = sender.send(result);
        });
        workers.workers.push(Arc::new(OwnedDiffWorker {
            session_id,
            cancel: child_cancel.clone(),
            join: tokio::sync::Mutex::new(Some(handle)),
        }));
        Ok(DiffQuery {
            receiver,
            child_cancel,
        })
    }

    /// Cancels every owned diff comparison and waits for its blocking handler
    /// to really finish. Handles stay registered until they are joined, so a
    /// second or concurrent call observes the same workers instead of an empty
    /// set. Called by the Agent shutdown barrier, so unloaded-Session diffs are
    /// covered too.
    pub(crate) async fn shutdown_diff_workers(&self) {
        let workers = {
            let mut workers = self.diff_workers.lock().unwrap();
            workers.closing = true;
            workers.workers.clone()
        };
        for worker in &workers {
            worker.cancel.cancel();
        }
        for worker in &workers {
            worker.join().await;
        }
        let mut workers = self.diff_workers.lock().unwrap();
        workers.workers.retain(|worker| !worker.is_finished());
    }

    pub(crate) fn cancel_diff_workers(&self) {
        let mut workers = self.diff_workers.lock().unwrap();
        workers.closing = true;
        for worker in &workers.workers {
            worker.cancel.cancel();
        }
    }

    /// Cancels and joins only the diff comparisons started for one closed
    /// Session, so closing a Session actually reaps its CPU workers rather than
    /// leaving them to run to the shared deadline.
    pub(crate) async fn shutdown_session_diff_workers(&self, session_id: SessionId) {
        let workers = {
            let workers = self.diff_workers.lock().unwrap();
            workers
                .workers
                .iter()
                .filter(|worker| worker.session_id == session_id)
                .cloned()
                .collect::<Vec<_>>()
        };
        for worker in &workers {
            worker.cancel.cancel();
        }
        for worker in &workers {
            worker.join().await;
        }
        let mut workers = self.diff_workers.lock().unwrap();
        workers
            .workers
            .retain(|worker| !(worker.session_id == session_id && worker.is_finished()));
    }

    /// Test-only evidence that diff worker handles are still registered (not
    /// taken and dropped), used to prove a dropped or re-entered shutdown does
    /// not detach an owned comparison.
    #[cfg(test)]
    pub(crate) fn registered_diff_workers(&self) -> usize {
        self.diff_workers.lock().unwrap().workers.len()
    }

    pub(crate) async fn delete_session(&self, session_id: SessionId) -> Result<(), StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        fs::remove_dir_all(directory)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(())
    }

    pub(crate) async fn load_record(
        &self,
        session_id: SessionId,
    ) -> Result<SessionRecord, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        Self::read_record(&directory.join(SESSION_RECORD_FILE), session_id).await
    }

    async fn read_record(path: &Path, session_id: SessionId) -> Result<SessionRecord, StoreError> {
        match path_state(path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing => {
                let directory = path.parent().unwrap_or(path);
                let legacy = legacy_format_present(directory).await?;
                return if legacy {
                    Err(StoreError::UnsupportedFormat)
                } else {
                    Err(StoreError::SessionNotFound)
                };
            }
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        // Bound the record read absolutely before handing bytes to serde.
        let metadata = fs::metadata(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if metadata.len() > MAX_SESSION_RECORD_BYTES as u64 {
            return Err(StoreError::Corrupt);
        }
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_SESSION_RECORD_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if bytes.len() > MAX_SESSION_RECORD_BYTES {
            return Err(StoreError::Corrupt);
        }
        let record: SessionRecord =
            serde_json::from_slice(&bytes).map_err(|_| StoreError::Corrupt)?;
        record.validate().map_err(|_| StoreError::Corrupt)?;
        if record.session_id != session_id || record.session_id == SessionId::default() {
            return Err(StoreError::Corrupt);
        }
        Ok(record)
    }

    fn sessions_directory(&self) -> PathBuf {
        self.root.join(SESSIONS_DIR)
    }

    fn session_directory(&self, session_id: SessionId) -> PathBuf {
        self.sessions_directory().join(session_id.to_string())
    }

    async fn require_root(&self) -> Result<(), StoreError> {
        match path_state(&self.root)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => Ok(()),
            PathState::Missing => Err(StoreError::Unavailable),
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                Err(StoreError::Corrupt)
            }
        }
    }

    async fn require_session_directory(
        &self,
        session_id: SessionId,
    ) -> Result<PathBuf, StoreError> {
        self.require_sessions_root().await?;
        let directory = self.session_directory(session_id);
        match path_state(&directory)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => {
                reject_symlink_entries(&directory)
                    .await
                    .map_err(map_io_store_error)?;
                Ok(directory)
            }
            PathState::Missing => Err(StoreError::SessionNotFound),
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                Err(StoreError::Corrupt)
            }
        }
    }

    async fn remove_created_session_directory(&self, directory: &Path) {
        if self.require_sessions_root().await.is_err() {
            return;
        }
        let Ok(PathState::Directory) = path_state(directory).await else {
            return;
        };
        if reject_symlink_entries(directory).await.is_err() {
            return;
        }
        let _ = fs::remove_dir_all(directory).await;
    }

    async fn require_sessions_root(&self) -> Result<(), StoreError> {
        self.require_root().await?;
        match path_state(&self.sessions_directory())
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => Ok(()),
            PathState::Missing => Err(StoreError::Unavailable),
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                Err(StoreError::Corrupt)
            }
        }
    }
}

pub(crate) fn digest_hex(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut value = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(value, "{byte:02x}").expect("writing a digest cannot fail");
    }
    value
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    digest_hex(hasher)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn legacy_format_present(directory: &Path) -> Result<bool, StoreError> {
    for name in [LEGACY_MANIFEST_FILE, LEGACY_CONVERSATION_FILE] {
        let path = directory.join(name);
        if matches!(
            path_state(&path)
                .await
                .map_err(|_| StoreError::Unavailable)?,
            PathState::RegularFile
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PathState {
    Missing,
    Directory,
    RegularFile,
    Symlink,
    Other,
}

async fn path_state(path: &Path) -> io::Result<PathState> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            Ok(if file_type.is_symlink() {
                PathState::Symlink
            } else if file_type.is_dir() {
                PathState::Directory
            } else if file_type.is_file() {
                PathState::RegularFile
            } else {
                PathState::Other
            })
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(PathState::Missing),
        Err(error) => Err(error),
    }
}

async fn reject_symlink_entries(directory: &Path) -> io::Result<()> {
    let mut entries = fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let is_symlink = fs::symlink_metadata(entry.path())
            .await?
            .file_type()
            .is_symlink();
        if is_symlink && entry.file_name().to_str() != Some(SUMMARY_FILE) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "symlink entry"));
        }
    }
    Ok(())
}

fn map_io_store_error(error: io::Error) -> StoreError {
    if error.kind() == io::ErrorKind::InvalidData {
        StoreError::Corrupt
    } else {
        StoreError::Unavailable
    }
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    match atomic_write_status(path, bytes).await? {
        SummaryCommit::Committed => Ok(()),
        SummaryCommit::Rejected | SummaryCommit::Unknown => Err(StoreError::Unavailable),
    }
}

async fn atomic_write_status(path: &Path, bytes: &[u8]) -> Result<SummaryCommit, StoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    match path_state(parent)
        .await
        .map_err(|_| StoreError::Unavailable)?
    {
        PathState::Directory => {}
        PathState::Symlink | PathState::RegularFile | PathState::Other => {
            return Err(StoreError::Corrupt);
        }
        PathState::Missing => return Err(StoreError::Unavailable),
    }
    reject_symlink_entries(parent)
        .await
        .map_err(map_io_store_error)?;
    match path_state(path)
        .await
        .map_err(|_| StoreError::Unavailable)?
    {
        PathState::Missing | PathState::RegularFile => {}
        PathState::Directory | PathState::Symlink | PathState::Other => {
            return Err(StoreError::Corrupt);
        }
    }
    let temp_path = unique_temp_path(path);
    match path_state(&temp_path)
        .await
        .map_err(|_| StoreError::Unavailable)?
    {
        PathState::Missing => {}
        PathState::RegularFile => return Err(StoreError::Unavailable),
        PathState::Directory | PathState::Symlink | PathState::Other => {
            return Err(StoreError::Corrupt);
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if file.write_all(bytes).await.is_err()
        || file.flush().await.is_err()
        || file.sync_all().await.is_err()
    {
        drop(file);
        let _ = fs::remove_file(&temp_path).await;
        return Err(StoreError::Unavailable);
    }
    drop(file);
    if fs::rename(&temp_path, path).await.is_err() {
        let _ = fs::remove_file(&temp_path).await;
        return Ok(SummaryCommit::Unknown);
    }
    Ok(SummaryCommit::Committed)
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("record");
    let id = NEXT_TEMP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(
            ".{name}{METADATA_TEMP_SUFFIX}-{}-{id}",
            std::process::id()
        ))
}

pub(crate) fn tool_ref_hash(tool_ref: &ToolRef) -> String {
    let mut hasher = Sha256::new();
    let json = serde_json::to_vec(tool_ref).expect("serializing ToolRef cannot fail");
    hasher.update(&json);
    digest_hex(hasher)
}

async fn safe_open_read(path: &Path) -> io::Result<File> {
    let symlink_meta = fs::symlink_metadata(path).await?;
    if symlink_meta.file_type().is_symlink() || !symlink_meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "symlink or not a regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    let file = options.open(path).await?;
    let meta = file.metadata().await?;
    if !meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    Ok(file)
}

async fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
            .await
            .map_err(|_| io::Error::other("sync_directory worker panicked"))?
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn valid_text(value: &str, maximum: usize, allow_empty: bool) -> bool {
    (allow_empty || !value.is_empty())
        && value.len() <= maximum
        && value.chars().all(|character| !character.is_control())
}

fn valid_multiline_text(value: &str, maximum: usize, allow_empty: bool) -> bool {
    (allow_empty || !value.is_empty())
        && value.len() <= maximum
        && value
            .chars()
            .all(|character| !character.is_control() || matches!(character, '\n' | '\t'))
}

fn valid_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() > MAX_TIMESTAMP_BYTES || bytes.len() < 20 {
        return false;
    }
    if bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| {
        bytes
            .get(range)
            .is_some_and(|part| part.iter().all(u8::is_ascii_digit))
    };
    if !digits(0..4)
        || !digits(5..7)
        || !digits(8..10)
        || !digits(11..13)
        || !digits(14..16)
        || !digits(17..19)
    {
        return false;
    }
    let number = |range: std::ops::Range<usize>| {
        std::str::from_utf8(&bytes[range])
            .ok()
            .and_then(|part| part.parse::<u32>().ok())
    };
    let Some(year) = number(0..4) else {
        return false;
    };
    let Some(month) = number(5..7) else {
        return false;
    };
    let Some(day) = number(8..10) else {
        return false;
    };
    let Some(hour) = number(11..13) else {
        return false;
    };
    let Some(minute) = number(14..16) else {
        return false;
    };
    let Some(second) = number(17..19) else {
        return false;
    };
    if year == 0
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return false;
    }

    let mut index = 19;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == start {
            return false;
        }
    }
    match bytes.get(index) {
        Some(b'Z') => index + 1 == bytes.len(),
        Some(b'+' | b'-') => {
            index += 1;
            if index + 5 != bytes.len()
                || bytes.get(index + 2) != Some(&b':')
                || !digits(index..index + 2)
                || !digits(index + 3..index + 5)
            {
                return false;
            }
            let offset_hour = number(index..index + 2).unwrap_or(24);
            let offset_minute = number(index + 3..index + 5).unwrap_or(60);
            offset_hour <= 23 && offset_minute <= 59
        }
        _ => false,
    }
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(crate) fn utc_timestamp() -> Result<String, StoreError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StoreError::Unavailable)?;
    Ok(format_rfc3339_millis(
        duration.as_secs(),
        duration.subsec_millis(),
    ))
}

fn format_rfc3339_millis(total_seconds: u64, millis: u32) -> String {
    const SECS_PER_DAY: i64 = 86_400;
    let days = i64::try_from(total_seconds)
        .unwrap_or(0)
        .div_euclid(SECS_PER_DAY);
    let seconds_of_day = i64::try_from(total_seconds)
        .unwrap_or(0)
        .rem_euclid(SECS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month as u32, day as u32)
}

#[cfg(test)]
pub(crate) fn fail_next_append(session_id: SessionId) {
    APPEND_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(session_id);
}

#[cfg(test)]
fn should_fail_append(session_id: SessionId) -> bool {
    let mut failures = APPEND_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| *candidate == session_id)
        .map(|position| {
            failures.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn fail_next_record_write(session_id: SessionId) {
    RECORD_WRITE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(session_id);
}

#[cfg(test)]
fn should_fail_record_write(session_id: SessionId) -> bool {
    let mut failures = RECORD_WRITE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| *candidate == session_id)
        .map(|position| {
            failures.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn fail_next_summary_write(session_id: SessionId) {
    SUMMARY_WRITE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(session_id);
}

#[cfg(test)]
fn should_fail_summary_write(session_id: SessionId) -> bool {
    let mut failures = SUMMARY_WRITE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| *candidate == session_id)
        .map(|position| {
            failures.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn gate_next_summary_commit(session_id: SessionId, gate: Arc<SummaryCommitGate>) {
    SUMMARY_COMMIT_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, gate));
}

#[cfg(test)]
fn take_summary_commit_gate(session_id: SessionId) -> Option<Arc<SummaryCommitGate>> {
    let mut gates = SUMMARY_COMMIT_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|(candidate, _)| *candidate == session_id)
        .map(|position| gates.remove(position).1)
}

#[cfg(test)]
pub(crate) fn force_unknown_summary_write(session_id: SessionId) {
    SUMMARY_UNKNOWN_WRITES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(session_id);
}

#[cfg(test)]
fn should_force_unknown_summary_write(session_id: SessionId) -> bool {
    let mut writes = SUMMARY_UNKNOWN_WRITES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    writes
        .iter()
        .position(|candidate| *candidate == session_id)
        .map(|position| {
            writes.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests;
