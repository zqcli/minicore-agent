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

pub(crate) const SESSION_FORMAT_VERSION: u32 = 1;

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
        let valid_tools = self.tools.len() <= KNOWN_TOOL_NAMES.len()
            && self.tools.iter().all(|name| {
                KNOWN_TOOL_NAMES.contains(&name.as_str()) && seen_tools.insert(name.as_str())
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

    /// Scans one raw history prefix without invoking the normal tail repair.
    /// The result exists only when the requested byte offset ends immediately
    /// after a complete StoredLoopRecord line.
    pub(crate) async fn read_history_prefix(
        &self,
        session_id: SessionId,
        prefix_bytes: u64,
        expected_history: &[HistoryItem],
    ) -> Result<Option<HistoryPrefix>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing | PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if prefix_bytes > metadata.len() {
            return Ok(None);
        }
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        scan_history_prefix(file, prefix_bytes, expected_history).await
    }

    /// Captures the complete raw history anchor without tail repair. The
    /// loaded sanitized history must account for every stored item before an
    /// anchor can be used for a derived snapshot write.
    pub(crate) async fn capture_history_anchor(
        &self,
        session_id: SessionId,
        expected_history: &[HistoryItem],
    ) -> Result<Option<HistoryPrefix>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing | PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let file = File::open(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let length = metadata.len();
        let Some(anchor) = scan_history_prefix(file, length, expected_history).await? else {
            return Ok(None);
        };
        let final_length = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
            .len();
        if final_length != length {
            return Ok(None);
        }
        if anchor.covered_item_count != u64::try_from(expected_history.len()).unwrap_or(u64::MAX) {
            return Ok(None);
        }
        Ok(Some(anchor))
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

    #[cfg(test)]
    pub(crate) fn with_aux_limits(mut self, limits: AuxLimits) -> Self {
        self.aux_limits = Some(limits);
        self
    }

    fn aux_limits(&self) -> AuxLimits {
        #[cfg(test)]
        if let Some(limits) = self.aux_limits {
            return limits;
        }
        DEFAULT_AUX_LIMITS
    }

    /// Atomically persists one completed tool call's metadata and retained
    /// raw stream windows to the auxiliary directory.
    #[cfg(test)]
    pub(crate) async fn commit_tool_record(
        &self,
        snapshot: &ToolPersistenceSnapshot,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        if should_fail_aux_write(snapshot.tool_ref.session_id) {
            return Err(StoreError::Unavailable);
        }
        let _guard = match tokio::time::timeout_at(deadline.into(), self.aux_lock.lock()).await {
            Ok(guard) => guard,
            Err(_) => return Err(StoreError::Unavailable),
        };
        self.commit_tool_record_locked(snapshot, deadline).await
    }

    async fn commit_tool_record_locked(
        &self,
        snapshot: &ToolPersistenceSnapshot,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        if Instant::now() >= deadline {
            return Err(StoreError::Unavailable);
        }
        validate_stored_tool_record(&snapshot.record, &snapshot.tool_ref)?;

        let input_len = snapshot.input_bytes.as_ref().map_or(0, |b| b.len());
        let result_len = snapshot.result_bytes.as_ref().map_or(0, |b| b.len());
        let stdout_len = snapshot.stdout_bytes.as_ref().map_or(0, |b| b.len());
        let stderr_len = snapshot.stderr_bytes.as_ref().map_or(0, |b| b.len());
        let before_len = snapshot
            .file_change_before
            .as_ref()
            .map_or(0, |bytes| bytes.len());
        let after_len = snapshot
            .file_change_after
            .as_ref()
            .map_or(0, |bytes| bytes.len());

        if input_len != snapshot.record.input.file_bytes
            || result_len != snapshot.record.result.file_bytes
            || stdout_len != snapshot.record.stdout.file_bytes
            || stderr_len != snapshot.record.stderr.file_bytes
        {
            return Err(StoreError::Corrupt);
        }

        if let Some(bytes) = &snapshot.input_bytes {
            if snapshot.record.input.file_sha256.as_deref() != Some(&hash_bytes(bytes)) {
                return Err(StoreError::Corrupt);
            }
        }
        if let Some(bytes) = &snapshot.result_bytes {
            if snapshot.record.result.file_sha256.as_deref() != Some(&hash_bytes(bytes)) {
                return Err(StoreError::Corrupt);
            }
        }
        if let Some(bytes) = &snapshot.stdout_bytes {
            if snapshot.record.stdout.file_sha256.as_deref() != Some(&hash_bytes(bytes)) {
                return Err(StoreError::Corrupt);
            }
        }
        if let Some(bytes) = &snapshot.stderr_bytes {
            if snapshot.record.stderr.file_sha256.as_deref() != Some(&hash_bytes(bytes)) {
                return Err(StoreError::Corrupt);
            }
        }
        validate_file_change_snapshot(
            snapshot.record.file_change.as_ref(),
            snapshot.file_change_before.as_deref(),
            snapshot.file_change_after.as_deref(),
        )?;
        let raw_bytes = input_len
            .saturating_add(result_len)
            .saturating_add(stdout_len)
            .saturating_add(stderr_len)
            .saturating_add(before_len)
            .saturating_add(after_len);
        if raw_bytes > 3 * 1024 * 1024 {
            return Err(StoreError::RecordTooLarge);
        }

        let session_dir = self
            .require_session_directory(snapshot.tool_ref.session_id)
            .await?;
        let session_meta = fs::symlink_metadata(&session_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if session_meta.file_type().is_symlink() || !session_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        match path_state(&tools_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory | PathState::Missing => {}
            _ => return Err(StoreError::Corrupt),
        }

        let hash = tool_ref_hash(&snapshot.tool_ref);
        let target_dir = tools_dir.join(&hash);
        if target_dir.exists() {
            let target_meta = fs::symlink_metadata(&target_dir)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            if target_meta.file_type().is_symlink() || !target_meta.is_dir() {
                return Err(StoreError::Corrupt);
            }
            match self
                .read_tool_record_details(&snapshot.tool_ref, deadline)
                .await
            {
                Ok(Some(existing)) => {
                    if existing.record.has_corrupt_streams() {
                        return Err(StoreError::Corrupt);
                    }
                    let existing_record_bytes =
                        serde_json::to_vec(&existing.metadata).map_err(|_| StoreError::Corrupt)?;
                    let incoming_record_bytes =
                        serde_json::to_vec(&snapshot.record).map_err(|_| StoreError::Corrupt)?;
                    if existing_record_bytes != incoming_record_bytes {
                        return Err(StoreError::Corrupt);
                    }
                    return Ok(());
                }
                _ => return Err(StoreError::Corrupt),
            }
        }

        let record_bytes = serde_json::to_vec(&snapshot.record).map_err(|_| StoreError::Corrupt)?;
        if record_bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Err(StoreError::RecordTooLarge);
        }

        let item_bytes = (record_bytes.len()
            + input_len
            + result_len
            + stdout_len
            + stderr_len
            + before_len
            + after_len) as u64;
        let reserve = item_bytes.saturating_mul(2);

        let limits = self.aux_limits();
        self.enforce_aux_budget_locked(snapshot.tool_ref.session_id, reserve, limits, deadline)
            .await?;

        if Instant::now() >= deadline {
            return Err(StoreError::Unavailable);
        }
        match fs::create_dir(&tools_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if !matches!(path_state(&tools_dir).await, Ok(PathState::Directory)) {
                    return Err(StoreError::Corrupt);
                }
            }
            Err(_) => return Err(StoreError::Unavailable),
        }
        let temp_dir = unique_temp_path(&target_dir);
        if Instant::now() >= deadline {
            let _ = fs::remove_dir(&tools_dir).await;
            return Err(StoreError::Unavailable);
        }
        if fs::create_dir(&temp_dir).await.is_err() {
            let _ = fs::remove_dir(&tools_dir).await;
            return Err(StoreError::Unavailable);
        }

        let write_res = async {
            if let Some(bytes) = &snapshot.input_bytes {
                if Instant::now() >= deadline {
                    return Err(StoreError::Unavailable);
                }
                write_sync_file(&temp_dir.join(TOOL_INPUT_FILE), bytes).await?;
            }
            if let Some(bytes) = &snapshot.result_bytes {
                if Instant::now() >= deadline {
                    return Err(StoreError::Unavailable);
                }
                write_sync_file(&temp_dir.join(TOOL_RESULT_FILE), bytes).await?;
            }
            if let Some(bytes) = &snapshot.stdout_bytes {
                if Instant::now() >= deadline {
                    return Err(StoreError::Unavailable);
                }
                write_sync_file(&temp_dir.join(TOOL_STDOUT_FILE), bytes).await?;
            }
            if let Some(bytes) = &snapshot.stderr_bytes {
                if Instant::now() >= deadline {
                    return Err(StoreError::Unavailable);
                }
                write_sync_file(&temp_dir.join(TOOL_STDERR_FILE), bytes).await?;
            }
            if let Some(bytes) = &snapshot.file_change_before {
                if Instant::now() >= deadline {
                    return Err(StoreError::Unavailable);
                }
                write_sync_file(&temp_dir.join(TOOL_BEFORE_FILE), bytes).await?;
            }
            if let Some(bytes) = &snapshot.file_change_after {
                if Instant::now() >= deadline {
                    return Err(StoreError::Unavailable);
                }
                write_sync_file(&temp_dir.join(TOOL_AFTER_FILE), bytes).await?;
            }
            if Instant::now() >= deadline {
                return Err(StoreError::Unavailable);
            }
            write_sync_file(&temp_dir.join(TOOL_RECORD_FILE), &record_bytes).await?;
            if Instant::now() >= deadline {
                return Err(StoreError::Unavailable);
            }
            sync_directory(&temp_dir)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            Ok::<(), StoreError>(())
        }
        .await;

        if write_res.is_err() {
            let _ = remove_aux_directory(&temp_dir).await;
            return Err(StoreError::Unavailable);
        }

        if Instant::now() >= deadline {
            let _ = remove_aux_directory(&temp_dir).await;
            return Err(StoreError::Unavailable);
        }

        if fs::rename(&temp_dir, &target_dir).await.is_err() {
            let _ = remove_aux_directory(&temp_dir).await;
            return Err(StoreError::Unavailable);
        }

        if sync_directory(&tools_dir).await.is_err() {
            return Err(StoreError::Unavailable);
        }

        Ok(())
    }

    /// Persists completed tool records of a loop under a shared lock and deadline.
    pub(crate) async fn persist_loop_tool_records(
        &self,
        tool_refs: &[ToolRef],
        tool_data: &crate::tool_data::ToolData,
        deadline: Instant,
    ) -> Vec<(ToolRef, Result<(), StoreError>)> {
        if tool_refs.is_empty() {
            return Vec::new();
        }

        #[cfg(test)]
        if let Some(first) = tool_refs.first() {
            if should_fail_aux_write(first.session_id) {
                return tool_refs
                    .iter()
                    .cloned()
                    .map(|r| (r, Err(StoreError::Unavailable)))
                    .collect();
            }
        }

        let _guard = match tokio::time::timeout_at(deadline.into(), self.aux_lock.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                return tool_refs
                    .iter()
                    .cloned()
                    .map(|r| (r, Err(StoreError::Unavailable)))
                    .collect();
            }
        };

        #[cfg(test)]
        if let Some(first) = tool_refs.first() {
            wait_aux_commit_gate(first.session_id).await;
        }

        let mut results = Vec::with_capacity(tool_refs.len().min(DEFAULT_SESSION_AUX_RECORDS));
        let mut expired = false;

        for tool_ref in tool_refs {
            if expired || Instant::now() >= deadline {
                expired = true;
                results.push((tool_ref.clone(), Err(StoreError::Unavailable)));
                continue;
            }

            let snapshot = match tool_data.snapshot_for_persistence(tool_ref) {
                Some(snap) => snap,
                None => {
                    results.push((tool_ref.clone(), Err(StoreError::Corrupt)));
                    continue;
                }
            };

            let res = self.commit_tool_record_locked(&snapshot, deadline).await;
            if matches!(res, Err(StoreError::QueryLimit)) || Instant::now() >= deadline {
                expired = true;
            }
            results.push((tool_ref.clone(), res));
        }

        results
    }

    /// Reads one stored tool record and its retained raw blobs from the auxiliary
    /// directory, returning an in-memory `ToolRecord` projection. Returns `Ok(None)`
    /// when the record or auxiliary directory does not exist.
    #[cfg(test)]
    pub(crate) async fn read_tool_record(
        &self,
        tool_ref: &ToolRef,
    ) -> Result<Option<ToolRecord>, StoreError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        self.read_tool_record_with_deadline(tool_ref, deadline)
            .await
    }

    pub(crate) async fn read_tool_record_with_deadline(
        &self,
        tool_ref: &ToolRef,
        deadline: Instant,
    ) -> Result<Option<ToolRecord>, StoreError> {
        self.read_tool_record_details(tool_ref, deadline)
            .await
            .map(|loaded| loaded.map(|loaded| loaded.record))
    }

    /// Lists only persisted file-change metadata for one Session or loop.
    /// Blob contents are deliberately not touched here; callers verify only
    /// the records that survive their encoded page budget.
    pub(crate) async fn list_tool_changes(
        &self,
        session_id: SessionId,
        loop_id: Option<LoopId>,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<ToolChangeScan, StoreError> {
        check_aux_scan(cancellation, deadline)?;
        let session_dir = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(
                deadline.into(),
                self.require_session_directory(session_id),
            ) => result.map_err(|_| StoreError::QueryLimit)??,
        };
        check_aux_scan(cancellation, deadline)?;
        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        let mut budget = ChangeScanBudget::default();
        let mut complete = true;
        let mut skipped = false;
        let mut records = Vec::new();
        match path_state(&tools_dir).await {
            Ok(PathState::Missing) => {}
            Ok(PathState::Directory) => {
                let mut entries = match fs::read_dir(&tools_dir).await {
                    Ok(entries) => entries,
                    Err(_) => {
                        return Ok(make_tool_change_scan(records, false, true, &budget));
                    }
                };
                let limits = self.aux_limits();
                loop {
                    check_aux_scan(cancellation, deadline)?;
                    // Reserve the entry ceiling before the next directory I/O, so
                    // the hard limit is never exceeded by one speculative read.
                    if !budget.entry_available(limits.max_scan_entries) {
                        complete = false;
                        skipped = true;
                        break;
                    }
                    let entry = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
                        result = tokio::time::timeout_at(
                            deadline.into(),
                            entries.next_entry(),
                        ) => match result {
                            Ok(Ok(Some(entry))) => entry,
                            Ok(Ok(None)) => break,
                            Ok(Err(_)) => {
                                complete = false;
                                skipped = true;
                                break;
                            }
                            Err(_) => return Err(StoreError::QueryLimit),
                        },
                    };
                    budget.consume_entry();
                    check_aux_scan(cancellation, deadline)?;
                    let path = entry.path();
                    let metadata = match fs::symlink_metadata(&path).await {
                        Ok(metadata) => metadata,
                        Err(_) => {
                            complete = false;
                            skipped = true;
                            continue;
                        }
                    };
                    let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                        complete = false;
                        skipped = true;
                        continue;
                    };
                    if is_valid_temp_name(&name) {
                        continue;
                    }
                    if !valid_sha256(&name)
                        || metadata.file_type().is_symlink()
                        || !metadata.is_dir()
                    {
                        complete = false;
                        skipped = true;
                        continue;
                    }
                    match self
                        .read_change_metadata_at(
                            &path,
                            session_id,
                            &name,
                            cancellation,
                            deadline,
                            &mut budget,
                        )
                        .await?
                    {
                        ChangeMetadataRead::BudgetExhausted => {
                            complete = false;
                            skipped = true;
                            break;
                        }
                        ChangeMetadataRead::Record(None) => {
                            complete = false;
                            skipped = true;
                        }
                        ChangeMetadataRead::Record(Some(stored)) => {
                            let Some(change) = stored.file_change else {
                                continue;
                            };
                            if loop_id
                                .as_ref()
                                .is_some_and(|wanted| &stored.tool_ref.loop_id != wanted)
                            {
                                continue;
                            }
                            records.push((stored.tool_ref, change));
                            if records.len() > limits.session_records {
                                records.pop();
                                complete = false;
                                skipped = true;
                                break;
                            }
                        }
                    }
                }
            }
            Ok(_) | Err(_) => {
                complete = false;
                skipped = true;
            }
        }
        Ok(make_tool_change_scan(records, complete, skipped, &budget))
    }

    async fn read_change_metadata_at(
        &self,
        path: &Path,
        session_id: SessionId,
        expected_hash: &str,
        cancellation: &CancellationToken,
        deadline: Instant,
        budget: &mut ChangeScanBudget,
    ) -> Result<ChangeMetadataRead, StoreError> {
        let mut entries = match fs::read_dir(path).await {
            Ok(entries) => entries,
            Err(_) => return Ok(ChangeMetadataRead::Record(None)),
        };
        let mut record_path = None;
        let mut malformed = false;
        let max_entries = self.aux_limits().max_scan_entries;
        loop {
            check_aux_scan(cancellation, deadline)?;
            // Same entry ceiling reservation as the outer scan, before I/O.
            if !budget.entry_available(max_entries) {
                return Ok(ChangeMetadataRead::BudgetExhausted);
            }
            let entry = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
                result = tokio::time::timeout_at(
                    deadline.into(),
                    entries.next_entry(),
                ) => match result {
                    Ok(Ok(Some(entry))) => entry,
                    Ok(Ok(None)) => break,
                    Ok(Err(_)) => return Ok(ChangeMetadataRead::Record(None)),
                    Err(_) => return Err(StoreError::QueryLimit),
                },
            };
            budget.consume_entry();
            let metadata = match fs::symlink_metadata(entry.path()).await {
                Ok(metadata) => metadata,
                Err(_) => {
                    malformed = true;
                    continue;
                }
            };
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                malformed = true;
                continue;
            };
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || !ALLOWED_AUX_FILES.contains(&name.as_str())
            {
                malformed = true;
                continue;
            }
            if name == TOOL_RECORD_FILE {
                record_path = Some(entry.path());
            }
        }
        if malformed {
            return Ok(ChangeMetadataRead::Record(None));
        }
        let Some(record_path) = record_path else {
            return Ok(ChangeMetadataRead::Record(None));
        };
        let file = match safe_open_read(&record_path).await {
            Ok(file) => file,
            Err(_) => return Ok(ChangeMetadataRead::Record(None)),
        };
        // Read at most the remaining allowance, capped by the oversized-file
        // probe. If the read fills a cap smaller than that probe the file did
        // not reach EOF, so a truncated read is never accepted as a complete
        // record and the whole read stays inside the cumulative budget.
        let probe = MAX_TOOL_METADATA_BYTES.saturating_add(1);
        let remaining = CHANGE_SCAN_METADATA_BYTES.saturating_sub(budget.metadata_bytes);
        if remaining == 0 {
            return Ok(ChangeMetadataRead::BudgetExhausted);
        }
        let cap = remaining.min(probe);
        let mut bytes = Vec::new();
        let mut reader = file.take(cap as u64);
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(
                deadline.into(),
                reader.read_to_end(&mut bytes),
            ) => result.map_err(|_| StoreError::QueryLimit)?,
        };
        if read.is_err() {
            return Ok(ChangeMetadataRead::Record(None));
        }
        check_aux_scan(cancellation, deadline)?;
        // Charge every byte actually read, including an oversized file, so the
        // cumulative budget bounds total metadata I/O rather than only the
        // records that parse successfully.
        if !budget.consume_metadata(bytes.len(), CHANGE_SCAN_METADATA_BYTES) {
            return Ok(ChangeMetadataRead::BudgetExhausted);
        }
        if bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Ok(ChangeMetadataRead::Record(None));
        }
        if bytes.len() == cap && cap < probe {
            return Ok(ChangeMetadataRead::BudgetExhausted);
        }
        let stored: StoredToolRecord = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Ok(ChangeMetadataRead::Record(None)),
        };
        if stored.tool_ref.session_id != session_id
            || tool_ref_hash(&stored.tool_ref) != expected_hash
            || validate_stored_tool_record(&stored, &stored.tool_ref).is_err()
        {
            return Ok(ChangeMetadataRead::Record(None));
        }
        Ok(ChangeMetadataRead::Record(Some(Box::new(stored))))
    }

    pub(crate) async fn change_blobs_available(
        &self,
        session_id: SessionId,
        tool_ref: &ToolRef,
        change: &StoredFileChange,
        cancellation: &CancellationToken,
        deadline: Instant,
        budget: &mut ChangeBlobBudget,
    ) -> Result<bool, StoreError> {
        check_aux_scan(cancellation, deadline)?;
        if !change.before_captured
            || !change.after_captured
            || (!matches!(
                &change.before,
                ChangeRevision::Missing | ChangeRevision::Content { .. }
            ))
            || !matches!(&change.after, ChangeRevision::Content { .. })
        {
            return Ok(false);
        }
        // Charge the worst-case read cost (expected bytes plus the one-byte
        // lookahead), not just the expected length, so the cumulative budget
        // bounds what is actually read from disk. A `Missing` before-image is
        // not opened and costs 0; a real empty `Content` still costs 1.
        let required = change_blob_read_cost(&change.before)
            .saturating_add(change_blob_read_cost(&change.after));
        if budget.used_bytes.saturating_add(required) > CHANGE_SCAN_BLOB_BYTES {
            budget.exhausted = true;
            return Ok(false);
        }
        budget.used_bytes = budget.used_bytes.saturating_add(required);
        let target_dir = self
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(tool_ref));
        let before = Self::change_blob_present(
            &target_dir.join(TOOL_BEFORE_FILE),
            &change.before,
            cancellation,
            deadline,
        )
        .await?;
        let after = Self::change_blob_present(
            &target_dir.join(TOOL_AFTER_FILE),
            &change.after,
            cancellation,
            deadline,
        )
        .await?;
        Ok(before && after)
    }

    async fn change_blob_present(
        path: &Path,
        revision: &ChangeRevision,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        if !matches!(revision, ChangeRevision::Content { .. }) {
            return Ok(true);
        }
        let (value, corrupt) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(deadline.into(), read_change_blob(path, revision)) => {
                result.map_err(|_| StoreError::QueryLimit)?
            }
        };
        check_aux_scan(cancellation, deadline)?;
        Ok(value.is_some() && !corrupt)
    }

    async fn read_tool_record_details(
        &self,
        tool_ref: &ToolRef,
        deadline: Instant,
    ) -> Result<Option<ReadToolRecord>, StoreError> {
        match tokio::time::timeout_at(deadline.into(), self.read_tool_record_inner(tool_ref)).await
        {
            Ok(res) => res,
            Err(_) => Err(StoreError::Unavailable),
        }
    }

    /// Reads only one retained record's bounded before/after snapshots. It
    /// deliberately skips the larger input/result/stream blobs, so a cold
    /// `changes.diff` never clones an unrelated tool output.
    pub(crate) async fn read_file_change_snapshots(
        &self,
        session_id: SessionId,
        tool_ref: &ToolRef,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::changes::FileChange>, StoreError> {
        if tool_ref.session_id != session_id {
            return Err(StoreError::InvalidArguments);
        }
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        let session_dir = self.session_directory(session_id);
        let session_meta = match fs::symlink_metadata(&session_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        if session_meta.file_type().is_symlink() || !session_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }
        let target_dir = session_dir
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(tool_ref));
        let record_path = target_dir.join(TOOL_RECORD_FILE);
        let file = match safe_open_read(&record_path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        let mut bytes = Vec::new();
        let mut reader = file.take((MAX_TOOL_METADATA_BYTES + 1) as u64);
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(deadline.into(), reader.read_to_end(&mut bytes)) => {
                result.map_err(|_| StoreError::QueryLimit)?
            }
        };
        if read.is_err() || bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Ok(None);
        }
        let stored: StoredToolRecord = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Ok(None),
        };
        if stored.tool_ref != *tool_ref || validate_stored_tool_record(&stored, tool_ref).is_err() {
            return Ok(None);
        }
        let Some(change) = stored.file_change else {
            return Ok(None);
        };
        let (before, before_corrupt) =
            read_change_blob(&target_dir.join(TOOL_BEFORE_FILE), &change.before).await;
        check_aux_scan(cancellation, deadline)?;
        let (after, after_corrupt) =
            read_change_blob(&target_dir.join(TOOL_AFTER_FILE), &change.after).await;
        check_aux_scan(cancellation, deadline)?;
        Ok(Some(crate::changes::FileChange::from_stored(
            change,
            before,
            before_corrupt,
            after,
            after_corrupt,
        )))
    }

    /// Starts one owned `changes.diff` CPU comparison for `session_id`.
    /// Registration and the capacity check happen under the same lock as the
    /// closing flag, so a Store that starts closing either observes this worker
    /// or refuses it.
    pub(crate) fn spawn_diff_query(
        &self,
        session_id: SessionId,
        before: Arc<[u8]>,
        after: Arc<[u8]>,
        context_lines: usize,
        deadline: Instant,
    ) -> Result<DiffQuery, StoreError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let child_cancel = CancellationToken::new();
        let worker_cancel = child_cancel.clone();
        let mut workers = self.diff_workers.lock().unwrap();
        workers.workers.retain(|worker| !worker.is_finished());
        if workers.closing || workers.workers.len() >= MAX_DIFF_WORKERS {
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

    async fn read_tool_record_inner(
        &self,
        tool_ref: &ToolRef,
    ) -> Result<Option<ReadToolRecord>, StoreError> {
        let root_meta = fs::symlink_metadata(&self.root)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if root_meta.file_type().is_symlink() || !root_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let sessions_dir = self.sessions_directory();
        let sessions_meta = fs::symlink_metadata(&sessions_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if sessions_meta.file_type().is_symlink() || !sessions_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let session_dir = self.session_directory(tool_ref.session_id);
        let session_meta = match fs::symlink_metadata(&session_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(StoreError::SessionNotFound);
            }
            Err(_) => return Err(StoreError::Unavailable),
        };
        if session_meta.file_type().is_symlink() || !session_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        let tools_meta = match fs::symlink_metadata(&tools_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        if tools_meta.file_type().is_symlink() || !tools_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let hash = tool_ref_hash(tool_ref);
        let target_dir = tools_dir.join(&hash);
        let target_meta = match fs::symlink_metadata(&target_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        if target_meta.file_type().is_symlink() || !target_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let record_path = target_dir.join(TOOL_RECORD_FILE);
        let file = match safe_open_read(&record_path).await {
            Ok(f) => f,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };

        let mut bytes = Vec::new();
        if file
            .take((MAX_TOOL_METADATA_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .is_err()
        {
            return Err(StoreError::Unavailable);
        }
        if bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Err(StoreError::Corrupt);
        }

        let stored: StoredToolRecord = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Err(StoreError::Corrupt),
        };

        validate_stored_tool_record(&stored, tool_ref)?;

        let (input_bytes, input_corrupt) = read_blob(
            &target_dir.join(TOOL_INPUT_FILE),
            stored.input.file_bytes,
            stored.input.file_sha256.as_deref(),
            MAX_TOOL_INPUT_PERSIST_BYTES,
        )
        .await;

        let (result_bytes, result_corrupt) = read_blob(
            &target_dir.join(TOOL_RESULT_FILE),
            stored.result.file_bytes,
            stored.result.file_sha256.as_deref(),
            MAX_TOOL_RESULT_PERSIST_BYTES,
        )
        .await;

        let (stdout_bytes, stdout_corrupt) = read_blob(
            &target_dir.join(TOOL_STDOUT_FILE),
            stored.stdout.file_bytes,
            stored.stdout.file_sha256.as_deref(),
            crate::tool_data::MAX_TOOL_STREAM_BYTES,
        )
        .await;

        let (stderr_bytes, stderr_corrupt) = read_blob(
            &target_dir.join(TOOL_STDERR_FILE),
            stored.stderr.file_bytes,
            stored.stderr.file_sha256.as_deref(),
            crate::tool_data::MAX_TOOL_STREAM_BYTES,
        )
        .await;

        let (file_change_before, file_change_before_corrupt) = match &stored.file_change {
            Some(change) => {
                read_change_blob(&target_dir.join(TOOL_BEFORE_FILE), &change.before).await
            }
            None => (None, false),
        };
        let (file_change_after, file_change_after_corrupt) = match &stored.file_change {
            Some(change) => {
                read_change_blob(&target_dir.join(TOOL_AFTER_FILE), &change.after).await
            }
            None => (None, false),
        };

        let record = ToolRecord::from_stored(
            stored.clone(),
            (input_bytes, input_corrupt),
            (result_bytes, result_corrupt),
            (stdout_bytes, stdout_corrupt),
            (stderr_bytes, stderr_corrupt),
            (file_change_before, file_change_before_corrupt),
            (file_change_after, file_change_after_corrupt),
        );

        Ok(Some(ReadToolRecord {
            metadata: stored,
            record,
        }))
    }

    async fn enforce_aux_budget_locked(
        &self,
        target_session: SessionId,
        reserve: u64,
        limits: AuxLimits,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        let sessions_dir = self.sessions_directory();
        let sessions_meta = fs::symlink_metadata(&sessions_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if sessions_meta.file_type().is_symlink() || !sessions_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let mut session_entries = fs::read_dir(&sessions_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;

        let mut total_scanned = 0usize;
        let mut global_bytes = 0u64;
        let mut global_records = 0usize;
        let mut session_bytes = 0u64;
        let mut session_records = 0usize;
        let mut all_entries = Vec::new();
        let mut target_entries = Vec::new();

        while let Some(ses_entry) = {
            if Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            session_entries
                .next_entry()
                .await
                .map_err(|_| StoreError::Unavailable)?
        } {
            total_scanned = total_scanned.saturating_add(1);
            if total_scanned > limits.max_scan_entries || Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            let ses_path = ses_entry.path();
            let ses_meta = fs::symlink_metadata(&ses_path)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            if ses_meta.file_type().is_symlink() {
                return Err(StoreError::Corrupt);
            }
            if !ses_meta.is_dir() {
                global_bytes = global_bytes.saturating_add(ses_meta.len());
                continue;
            }
            let file_name = ses_entry.file_name();
            let Some(name_str) = file_name.to_str() else {
                return Err(StoreError::Corrupt);
            };
            let Ok(current_ses_id) = name_str.parse::<SessionId>() else {
                return Err(StoreError::Corrupt);
            };
            let tools_dir = ses_path.join(AUX_TOOLS_DIR);
            let tools_meta = match fs::symlink_metadata(&tools_dir).await {
                Ok(m) => m,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => return Err(StoreError::Unavailable),
            };
            if tools_meta.file_type().is_symlink() || !tools_meta.is_dir() {
                return Err(StoreError::Corrupt);
            }

            let mut tool_dir_entries = fs::read_dir(&tools_dir)
                .await
                .map_err(|_| StoreError::Unavailable)?;

            while let Some(tool_entry) = {
                if Instant::now() >= deadline {
                    return Err(StoreError::QueryLimit);
                }
                tool_dir_entries
                    .next_entry()
                    .await
                    .map_err(|_| StoreError::Unavailable)?
            } {
                total_scanned = total_scanned.saturating_add(1);
                if total_scanned > limits.max_scan_entries || Instant::now() >= deadline {
                    return Err(StoreError::QueryLimit);
                }
                let tool_path = tool_entry.path();
                let tool_meta = fs::symlink_metadata(&tool_path)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
                if tool_meta.file_type().is_symlink() {
                    return Err(StoreError::Corrupt);
                }
                let tool_name = tool_entry.file_name();
                let Some(name_str) = tool_name.to_str() else {
                    return Err(StoreError::Corrupt);
                };

                if is_valid_temp_name(name_str) {
                    if !tool_meta.is_dir() {
                        return Err(StoreError::Corrupt);
                    }
                    match verify_and_scan_temp_dir(
                        &tool_path,
                        &mut total_scanned,
                        limits.max_scan_entries,
                        deadline,
                    )
                    .await?
                    {
                        Some(temp_bytes) => {
                            global_bytes = global_bytes.saturating_add(temp_bytes);
                            if current_ses_id == target_session {
                                session_bytes = session_bytes.saturating_add(temp_bytes);
                            }

                            #[cfg(test)]
                            let fail_remove = should_fail_remove_temp(current_ses_id);
                            #[cfg(not(test))]
                            let fail_remove = false;

                            if fail_remove || remove_aux_directory(&tool_path).await.is_err() {
                                return Err(StoreError::Unavailable);
                            }

                            global_bytes = global_bytes.saturating_sub(temp_bytes);
                            if current_ses_id == target_session {
                                session_bytes = session_bytes.saturating_sub(temp_bytes);
                            }
                            continue;
                        }
                        None => {
                            return Err(StoreError::Corrupt);
                        }
                    }
                }

                if !valid_sha256(name_str) || !tool_meta.is_dir() {
                    return Err(StoreError::Corrupt);
                }

                let (dir_bytes, _mtime, verified_entry) = scan_and_verify_tool_dir(
                    &tool_path,
                    current_ses_id,
                    name_str,
                    &mut total_scanned,
                    limits.max_scan_entries,
                    deadline,
                )
                .await?;

                global_bytes = global_bytes.saturating_add(dir_bytes);
                global_records = global_records.saturating_add(1);
                if current_ses_id == target_session {
                    session_bytes = session_bytes.saturating_add(dir_bytes);
                    session_records = session_records.saturating_add(1);
                    target_entries.push(verified_entry.clone());
                }
                all_entries.push(verified_entry);
            }
        }

        // Evict session-level oldest records if exceeding session bounds
        target_entries.sort_by_key(|e| e.mtime);
        while (session_bytes.saturating_add(reserve) > limits.session_bytes
            || session_records.saturating_add(1) > limits.session_records)
            && !target_entries.is_empty()
        {
            if Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            let victim = target_entries.remove(0);
            remove_aux_directory(&victim.path).await?;
            session_bytes = session_bytes.saturating_sub(victim.bytes);
            session_records = session_records.saturating_sub(1);
            global_bytes = global_bytes.saturating_sub(victim.bytes);
            global_records = global_records.saturating_sub(1);
            if let Some(pos) = all_entries.iter().position(|e| e.path == victim.path) {
                all_entries.remove(pos);
            }
        }
        if session_bytes.saturating_add(reserve) > limits.session_bytes
            || session_records.saturating_add(1) > limits.session_records
        {
            return Err(StoreError::Unavailable);
        }

        // Evict global oldest records if exceeding global bounds
        all_entries.sort_by_key(|e| e.mtime);
        while (global_bytes.saturating_add(reserve) > limits.global_bytes
            || global_records.saturating_add(1) > limits.global_records)
            && !all_entries.is_empty()
        {
            if Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            let victim = all_entries.remove(0);
            remove_aux_directory(&victim.path).await?;
            global_bytes = global_bytes.saturating_sub(victim.bytes);
            global_records = global_records.saturating_sub(1);
            if victim.session_id == target_session {
                session_bytes = session_bytes.saturating_sub(victim.bytes);
                session_records = session_records.saturating_sub(1);
            }
        }
        if global_bytes.saturating_add(reserve) > limits.global_bytes
            || global_records.saturating_add(1) > limits.global_records
        {
            return Err(StoreError::Unavailable);
        }

        Ok(())
    }

    /// Appends one completed loop as a single JSON line. On success the file
    /// content is complete; callers merge the sanitized items into memory.
    pub(crate) async fn append_loop(
        &self,
        session_id: SessionId,
        record: &StoredLoopRecord,
    ) -> Result<(), StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        #[cfg(test)]
        if should_fail_append(session_id) {
            return Err(StoreError::Unavailable);
        }
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::RegularFile => {}
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let mut bytes = if record.user_times_are_valid() {
            serde_json::to_vec(record)
        } else {
            let mut normalized = record.clone();
            normalized.user_times = record.normalized_user_times();
            serde_json::to_vec(&normalized)
        }
        .map_err(|_| StoreError::Corrupt)?;
        if bytes.len() > MAX_LOOP_RECORD_BYTES && record.user_times.is_some() {
            // Presentation metadata must not make an otherwise storable core
            // loop record cross the single-line limit.
            let mut without_metadata = record.clone();
            without_metadata.user_times = None;
            bytes = serde_json::to_vec(&without_metadata).map_err(|_| StoreError::Corrupt)?;
        }
        if bytes.len() > MAX_LOOP_RECORD_BYTES {
            return Err(StoreError::RecordTooLarge);
        }
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        file.write_all(&bytes)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        file.flush().await.map_err(|_| StoreError::Unavailable)?;
        Ok(())
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_history_page(
        &self,
        session_id: SessionId,
        item_offset: usize,
        item_limit: usize,
        visible_item_count: Option<usize>,
        captured_end: Option<u64>,
        expected_revision: Option<&str>,
        expected_history: Option<&[HistoryItem]>,
        limits: &HistoryScanLimits,
    ) -> Result<HistoryReadPage, StoreError> {
        if captured_end.is_some() != expected_revision.is_some() {
            return Err(StoreError::InvalidArguments);
        }
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let target = captured_end.unwrap_or(metadata.len());
        if target > metadata.len() {
            return Err(StoreError::HistoryChanged);
        }
        if let Some(expected) = expected_revision {
            if !valid_sha256(expected) {
                return Err(StoreError::InvalidArguments);
            }
        }
        check_history_scan(limits)?;
        if visible_item_count == Some(0) {
            if captured_end.is_some_and(|end| end != 0) {
                return Err(StoreError::HistoryChanged);
            }
            return finish_empty_history_page(0, expected_revision);
        }

        let file = File::open(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut reader = BufReader::new(file);
        let scan_target = target.min(limits.max_bytes);
        let mut remaining = scan_target;
        let mut complete_end = 0_u64;
        let mut hasher = Sha256::new();
        let mut line_count = 0_usize;
        let mut total_items = 0_usize;
        let mut items = Vec::new();
        let mut user_times = Vec::new();
        let page_end = item_offset.saturating_add(item_limit);
        let mut turns = Vec::new();
        let mut turns_truncated = false;
        let mut expected_index = 0_usize;
        let mut stopped_at_visible_cap = false;
        let mut trailing_incomplete = false;
        let retained_source_limit = limits
            .max_bytes
            .saturating_sub(MAX_LOOP_RECORD_BYTES as u64);
        let mut retained_source_bytes = 0_u64;

        loop {
            if remaining == 0 {
                break;
            }
            check_history_scan(limits)?;
            if line_count >= limits.max_lines {
                return Err(StoreError::QueryLimit);
            }
            let line = match read_bounded_line(&mut reader, &mut remaining, limits).await? {
                BoundedLine::Complete(line) => line,
                BoundedLine::Partial => {
                    if captured_end.is_some() {
                        return Err(StoreError::HistoryChanged);
                    }
                    if scan_target < target {
                        return Err(StoreError::QueryLimit);
                    }
                    trailing_incomplete = true;
                    break;
                }
                BoundedLine::End => return Err(StoreError::HistoryChanged),
            };
            if line.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord =
                serde_json::from_slice(&line).map_err(|_| StoreError::Corrupt)?;
            let normalized = sanitize_history(&record.items).map_err(|_| StoreError::Corrupt)?;
            let record_start = total_items;
            let record_end = record_start
                .checked_add(normalized.len())
                .ok_or(StoreError::Corrupt)?;
            if let Some(expected) = expected_history {
                let compared = visible_item_count
                    .map(|cap| normalized.len().min(cap.saturating_sub(record_start)))
                    .unwrap_or(normalized.len());
                for item in normalized.iter().take(compared) {
                    let Some(expected_item) = expected.get(expected_index) else {
                        return Err(StoreError::HistoryChanged);
                    };
                    if item != expected_item {
                        return Err(StoreError::HistoryChanged);
                    }
                    expected_index = expected_index.checked_add(1).ok_or(StoreError::Corrupt)?;
                }
            }
            if visible_item_count.is_some_and(|cap| record_end > cap) {
                return Err(StoreError::HistoryChanged);
            }
            if record_end > item_offset && record_start < page_end {
                retained_source_bytes = retained_source_bytes
                    .checked_add(u64::try_from(line.len()).map_err(|_| StoreError::Corrupt)?)
                    .ok_or(StoreError::QueryLimit)?;
                if retained_source_bytes > retained_source_limit {
                    return Err(StoreError::QueryLimit);
                }
            }
            let times = record.normalized_user_times().unwrap_or_default();
            let mut user_occurrence = 0_usize;
            for (index, item) in normalized.iter().enumerate() {
                let timestamp = if matches!(item, HistoryItem::User(_)) {
                    let timestamp = times.get(user_occurrence).cloned().flatten();
                    user_occurrence += 1;
                    timestamp
                } else {
                    None
                };
                if index + record_start >= item_offset && index + record_start < page_end {
                    items.push(item.clone());
                    user_times.push(timestamp);
                }
            }
            if record_end > item_offset && record_start < page_end {
                if turns.len() < MAX_READ_TURN_SUMMARIES {
                    turns.push(StoredTurnSummary {
                        item_start: record_start,
                        item_end: record_end,
                        loop_id: record.loop_id,
                        outcome: record.outcome.clone(),
                        usage: record.usage,
                        requests: record.requests,
                        tool_rounds: record.tool_rounds,
                        final_config_revision: record.final_config_revision,
                        completed_at: record.completed_at.clone(),
                    });
                } else {
                    turns_truncated = true;
                }
            }
            total_items = record_end;
            line_count += 1;
            hasher.update(&line);
            hasher.update(b"\n");
            complete_end = scan_target - remaining;
            tokio::task::yield_now().await;
            if visible_item_count.is_some_and(|cap| cap > 0 && cap == total_items) {
                stopped_at_visible_cap = true;
                break;
            }
        }

        if !stopped_at_visible_cap && scan_target < target {
            return Err(StoreError::QueryLimit);
        }
        if remaining != 0 && !stopped_at_visible_cap {
            return Err(StoreError::HistoryChanged);
        }
        if captured_end.is_some() && complete_end != target {
            return Err(StoreError::HistoryChanged);
        }
        if let Some(expected) = expected_history {
            if expected_index != total_items || total_items > expected.len() {
                return Err(StoreError::HistoryChanged);
            }
        }
        let revision = digest_hex(hasher);
        if expected_revision.is_some_and(|expected| expected != revision) {
            return Err(StoreError::HistoryChanged);
        }
        Ok(HistoryReadPage {
            captured_end: complete_end,
            revision,
            trailing_incomplete,
            total_items,
            items,
            user_times,
            turns,
            turns_truncated,
        })
    }

    pub(crate) async fn read_loop_record(
        &self,
        session_id: SessionId,
        loop_id: LoopId,
        limits: &HistoryScanLimits,
    ) -> Result<Option<StoredLoopRecord>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        check_history_scan(limits)?;
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut reader = BufReader::new(file);
        let target = metadata.len();
        let scan_target = target.min(limits.max_bytes);
        let mut remaining = scan_target;
        let mut line_count = 0_usize;
        loop {
            if remaining == 0 {
                if scan_target < target {
                    return Err(StoreError::QueryLimit);
                }
                return Ok(None);
            }
            check_history_scan(limits)?;
            if line_count >= limits.max_lines {
                return Err(StoreError::QueryLimit);
            }
            let line = match read_bounded_line(&mut reader, &mut remaining, limits).await? {
                BoundedLine::Complete(line) => line,
                BoundedLine::Partial => {
                    if scan_target < target {
                        return Err(StoreError::QueryLimit);
                    }
                    return Ok(None);
                }
                BoundedLine::End => return Err(StoreError::HistoryChanged),
            };
            if line.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord =
                serde_json::from_slice(&line).map_err(|_| StoreError::Corrupt)?;
            if record.loop_id == loop_id {
                return Ok(Some(record));
            }
            line_count += 1;
            tokio::task::yield_now().await;
        }
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

    async fn load_history(
        &self,
        path: &Path,
        session_id: SessionId,
    ) -> Result<
        (
            std::sync::Arc<[HistoryItem]>,
            std::collections::HashMap<(LoopId, usize), String>,
        ),
        StoreError,
    > {
        match path_state(path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            // A valid v0.3 record always pairs `session.json` with
            // `history.jsonl`; a missing history for a present record is
            // corrupt data, never an empty conversation.
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut reader = BufReader::new(file);
        let mut items = Vec::new();
        let mut times = std::collections::HashMap::new();
        let mut buffer = Vec::new();
        let mut last_complete_offset = 0u64;
        let mut saw_partial = false;
        loop {
            buffer.clear();
            // Bounded line assembly: once a line exceeds the ceiling, stop
            // retaining payload bytes but keep scanning to distinguish a
            // complete oversized line from an oversized final partial.
            let mut line_len = 0usize;
            let mut oversized = false;
            let mut is_complete = false;
            let reached_eof = loop {
                let chunk = reader
                    .fill_buf()
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
                if chunk.is_empty() {
                    break true;
                }
                if let Some(position) = chunk.iter().position(|byte| *byte == b'\n') {
                    if !oversized {
                        match line_len.checked_add(position) {
                            Some(total) if total <= MAX_LOOP_RECORD_BYTES => {
                                buffer.extend_from_slice(&chunk[..position]);
                                line_len = total;
                            }
                            Some(_) | None => oversized = true,
                        }
                    }
                    reader.consume(position + 1);
                    is_complete = true;
                    break false;
                }
                let length = chunk.len();
                if !oversized {
                    match line_len.checked_add(length) {
                        Some(total) if total <= MAX_LOOP_RECORD_BYTES => {
                            buffer.extend_from_slice(chunk);
                            line_len = total;
                        }
                        Some(_) | None => oversized = true,
                    }
                }
                reader.consume(length);
            };
            if reached_eof && line_len == 0 && !oversized {
                break;
            }
            if !is_complete {
                saw_partial = true;
                break;
            }
            if oversized {
                return Err(StoreError::Corrupt);
            }
            if buffer.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord =
                serde_json::from_slice(&buffer).map_err(|_| StoreError::Corrupt)?;
            let user_count = record
                .items
                .iter()
                .filter(|item| matches!(item, HistoryItem::User(_)))
                .count();
            let user_times = record
                .user_times
                .as_deref()
                .filter(|times| times.len() <= user_count)
                .unwrap_or(&[]);
            let mut user_occurrence = 0usize;
            for item in &record.items {
                if let HistoryItem::User(_) = item {
                    if let Some(Some(time)) = user_times.get(user_occurrence) {
                        times.insert((record.loop_id, user_occurrence), time.clone());
                    }
                    user_occurrence += 1;
                }
            }
            items.extend(record.items);
            last_complete_offset = reader
                .stream_position()
                .await
                .map_err(|_| StoreError::Unavailable)?;
        }
        if saw_partial {
            // The only allowed repair: truncate back to the last complete
            // line. Works for a first-segment partial too (offset 0).
            let had_partial = tail_repair(path, last_complete_offset).await?;
            if had_partial {
                tracing::warn!(session_id = %session_id, "history tail repaired");
            }
        }
        let history = sanitize_history(&items).map_err(|_| StoreError::Corrupt)?;
        Ok((history, times))
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

enum BoundedLine {
    End,
    Complete(Vec<u8>),
    Partial,
}

async fn read_bounded_line<R>(
    reader: &mut R,
    remaining: &mut u64,
    limits: &HistoryScanLimits,
) -> Result<BoundedLine, StoreError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        if *remaining == 0 {
            return if line.is_empty() {
                Ok(BoundedLine::End)
            } else {
                Ok(BoundedLine::Partial)
            };
        }
        check_history_scan(limits)?;
        let chunk = reader
            .fill_buf()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if chunk.is_empty() {
            return Ok(BoundedLine::End);
        }
        let take = chunk
            .len()
            .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
        let chunk = &chunk[..take];
        if let Some(position) = chunk.iter().position(|byte| *byte == b'\n') {
            if !oversized {
                match line.len().checked_add(position) {
                    Some(length) if length <= MAX_LOOP_RECORD_BYTES => {
                        line.extend_from_slice(&chunk[..position]);
                    }
                    Some(_) | None => oversized = true,
                }
            }
            reader.consume(position + 1);
            *remaining -= u64::try_from(position + 1).map_err(|_| StoreError::Corrupt)?;
            if oversized {
                return Err(StoreError::Corrupt);
            }
            return Ok(BoundedLine::Complete(line));
        }
        if !oversized {
            match line.len().checked_add(chunk.len()) {
                Some(length) if length <= MAX_LOOP_RECORD_BYTES => {
                    line.extend_from_slice(chunk);
                }
                Some(_) | None => oversized = true,
            }
        }
        reader.consume(take);
        *remaining -= u64::try_from(take).map_err(|_| StoreError::Corrupt)?;
        tokio::task::yield_now().await;
    }
}

fn check_history_scan(limits: &HistoryScanLimits) -> Result<(), StoreError> {
    if limits.cancellation.is_cancelled() || Instant::now() >= limits.deadline {
        Err(StoreError::QueryLimit)
    } else {
        Ok(())
    }
}

fn check_aux_scan(cancellation: &CancellationToken, deadline: Instant) -> Result<(), StoreError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(StoreError::QueryLimit)
    } else {
        Ok(())
    }
}

impl ChangeScanBudget {
    /// True when one more directory entry fits under the scan ceiling. Callers
    /// check this before the next `next_entry()` I/O, then record the consumed
    /// entry only after they really received one.
    fn entry_available(&self, max_entries: usize) -> bool {
        self.entries < max_entries
    }

    fn consume_entry(&mut self) {
        self.entries = self.entries.saturating_add(1);
    }

    fn consume_metadata(&mut self, bytes: usize, max_bytes: usize) -> bool {
        self.metadata_bytes = self.metadata_bytes.saturating_add(bytes);
        self.metadata_bytes <= max_bytes
    }
}

/// The worst-case bytes one blob verification reads, including the one-byte
/// lookahead that detects an oversized or truncated file. A `Content` blob is
/// always read with `.take(bytes + 1)`, so even a genuinely empty one costs 1;
/// a `Missing` before-image is not opened at all and costs 0.
fn change_blob_read_cost(revision: &ChangeRevision) -> usize {
    match revision {
        ChangeRevision::Content { bytes, .. } => bytes.saturating_add(1),
        ChangeRevision::Missing | ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => 0,
    }
}

fn make_tool_change_scan(
    mut records: Vec<(ToolRef, StoredFileChange)>,
    complete: bool,
    skipped: bool,
    budget: &ChangeScanBudget,
) -> ToolChangeScan {
    records.sort_by_cached_key(|entry| tool_ref_hash(&entry.0));
    let fingerprint = serde_json::to_vec(&(
        &records,
        complete,
        skipped,
        budget.entries,
        budget.metadata_bytes,
    ))
    .map(|bytes| hash_bytes(&bytes))
    .unwrap_or_else(|_| hash_bytes(b"change-scan-serialization-failed"));
    ToolChangeScan {
        records,
        complete,
        skipped,
        #[cfg(test)]
        entries: budget.entries,
        #[cfg(test)]
        metadata_bytes: budget.metadata_bytes,
        observation: fingerprint,
    }
}

fn finish_empty_history_page(
    captured_end: u64,
    expected_revision: Option<&str>,
) -> Result<HistoryReadPage, StoreError> {
    let revision = digest_hex(Sha256::new());
    if expected_revision.is_some_and(|expected| expected != revision) {
        return Err(StoreError::HistoryChanged);
    }
    Ok(HistoryReadPage {
        captured_end,
        revision,
        trailing_incomplete: false,
        total_items: 0,
        items: Vec::new(),
        user_times: Vec::new(),
        turns: Vec::new(),
        turns_truncated: false,
    })
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

async fn scan_history_prefix(
    file: File,
    prefix_bytes: u64,
    expected_history: &[HistoryItem],
) -> Result<Option<HistoryPrefix>, StoreError> {
    let mut reader = BufReader::new(file);
    let mut remaining = prefix_bytes;
    let mut hasher = Sha256::new();
    let mut line = Vec::new();
    let mut covered_loop_count = 0_u64;
    let mut covered_item_count = 0_u64;
    let mut last_loop_id = None;
    // Bind the raw scan to the already-loaded sanitized history without
    // constructing a second history-sized collection.
    let mut expected_item_index = 0_usize;

    while remaining > 0 {
        let chunk = reader
            .fill_buf()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if chunk.is_empty() {
            return Ok(None);
        }
        let take = chunk
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let bytes = &chunk[..take];
        hasher.update(bytes);

        let mut segment_start = 0;
        for (index, byte) in bytes.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            let segment = &bytes[segment_start..index];
            let line_length = line
                .len()
                .checked_add(segment.len())
                .ok_or(StoreError::Corrupt)?;
            if line_length > MAX_LOOP_RECORD_BYTES {
                return Err(StoreError::Corrupt);
            }
            line.extend_from_slice(segment);
            if line.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord =
                serde_json::from_slice(&line).map_err(|_| StoreError::Corrupt)?;
            let normalized = sanitize_history(&record.items).map_err(|_| StoreError::Corrupt)?;
            for item in normalized.iter() {
                let Some(expected) = expected_history.get(expected_item_index) else {
                    return Ok(None);
                };
                let actual_bytes = serde_json::to_vec(item).map_err(|_| StoreError::Corrupt)?;
                let expected_bytes =
                    serde_json::to_vec(expected).map_err(|_| StoreError::Corrupt)?;
                if actual_bytes != expected_bytes {
                    return Ok(None);
                }
                expected_item_index = expected_item_index
                    .checked_add(1)
                    .ok_or(StoreError::Corrupt)?;
            }
            covered_loop_count = covered_loop_count
                .checked_add(1)
                .ok_or(StoreError::Corrupt)?;
            covered_item_count = covered_item_count
                .checked_add(u64::try_from(normalized.len()).map_err(|_| StoreError::Corrupt)?)
                .ok_or(StoreError::Corrupt)?;
            last_loop_id = Some(record.loop_id);
            line.clear();
            segment_start = index + 1;
        }
        if segment_start < bytes.len() {
            let segment = &bytes[segment_start..];
            let line_length = line
                .len()
                .checked_add(segment.len())
                .ok_or(StoreError::Corrupt)?;
            if line_length > MAX_LOOP_RECORD_BYTES {
                return Err(StoreError::Corrupt);
            }
            line.extend_from_slice(segment);
        }
        reader.consume(take);
        remaining -= u64::try_from(take).map_err(|_| StoreError::Corrupt)?;
    }

    if !line.is_empty() {
        return Ok(None);
    }
    let digest = hasher.finalize();
    let mut sha256 = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(sha256, "{byte:02x}").expect("writing digest cannot fail");
    }
    Ok(Some(HistoryPrefix {
        prefix_bytes,
        covered_loop_count,
        covered_item_count,
        last_loop_id,
        sha256,
    }))
}

/// If the file ends without a newline, truncate it back to the last complete
/// line. Returns whether a repair happened.
async fn tail_repair(path: &Path, complete_offset: u64) -> Result<bool, StoreError> {
    let metadata = fs::metadata(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if metadata.len() == complete_offset {
        return Ok(false);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    file.set_len(complete_offset)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    file.flush().await.map_err(|_| StoreError::Unavailable)?;
    Ok(true)
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

#[derive(Clone)]
struct AuxDirectoryEntry {
    session_id: SessionId,
    path: PathBuf,
    mtime: std::time::SystemTime,
    bytes: u64,
}

async fn remove_aux_directory(path: &Path) -> Result<(), StoreError> {
    let parent = path.parent().ok_or(StoreError::Corrupt)?;
    if parent.file_name() != Some(std::ffi::OsStr::new(AUX_TOOLS_DIR)) {
        return Err(StoreError::Corrupt);
    }
    fs::remove_dir_all(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    match fs::remove_dir(parent).await {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        Err(_) => Err(StoreError::Unavailable),
    }
}

async fn write_sync_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if file.write_all(bytes).await.is_err()
        || file.flush().await.is_err()
        || file.sync_all().await.is_err()
    {
        return Err(StoreError::Unavailable);
    }
    Ok(())
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

fn is_valid_temp_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() < 73 || bytes[0] != b'.' {
        return false;
    }
    let hex_part = &bytes[1..65];
    if !hex_part
        .iter()
        .all(|&b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return false;
    }
    if &bytes[65..70] != b".tmp-" {
        return false;
    }
    let remainder = &bytes[70..];
    let Some(dash_pos) = remainder.iter().position(|&b| b == b'-') else {
        return false;
    };
    if dash_pos == 0 || dash_pos == remainder.len() - 1 {
        return false;
    }
    let pid_part = &remainder[..dash_pos];
    let id_part = &remainder[dash_pos + 1..];
    pid_part.iter().all(u8::is_ascii_digit) && id_part.iter().all(u8::is_ascii_digit)
}

fn validate_stored_tool_record(
    stored: &StoredToolRecord,
    tool_ref: &ToolRef,
) -> Result<(), StoreError> {
    if stored.version != TOOL_RECORD_FORMAT_VERSION {
        return Err(StoreError::UnsupportedFormat);
    }
    if stored.tool_ref != *tool_ref {
        return Err(StoreError::Corrupt);
    }
    if !stored.state.is_terminal() {
        return Err(StoreError::Corrupt);
    }
    if let Some(outcome) = stored.outcome {
        if !stored.state.matches_outcome(outcome) {
            return Err(StoreError::Corrupt);
        }
    }

    if !stored.input.seen {
        if stored.input.total_bytes != 0
            || stored.input.file_bytes != 0
            || stored.input.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        if stored.input.file_bytes > stored.input.total_bytes {
            return Err(StoreError::Corrupt);
        }
        if stored.input.expired
            && (stored.input.file_bytes != 0 || stored.input.file_sha256.is_some())
        {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.input.file_bytes > MAX_TOOL_INPUT_PERSIST_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    if !stored.result.seen {
        if stored.result.total_bytes != 0
            || stored.result.file_bytes != 0
            || stored.result.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        if stored.result.file_bytes > stored.result.total_bytes {
            return Err(StoreError::Corrupt);
        }
        if stored.result.expired
            && (stored.result.file_bytes != 0 || stored.result.file_sha256.is_some())
        {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.result.file_bytes > MAX_TOOL_RESULT_PERSIST_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    if stored.stdout.start_offset > stored.stdout.observed_end {
        return Err(StoreError::Corrupt);
    }
    if !stored.stdout.seen {
        if stored.stdout.start_offset != 0
            || stored.stdout.observed_end != 0
            || stored.stdout.file_bytes != 0
            || stored.stdout.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else if stored.stdout.expired {
        if stored.stdout.start_offset != stored.stdout.observed_end
            || stored.stdout.file_bytes != 0
            || stored.stdout.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        let diff = stored
            .stdout
            .observed_end
            .checked_sub(stored.stdout.start_offset)
            .ok_or(StoreError::Corrupt)?;
        if diff != stored.stdout.file_bytes as u64 {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.stdout.file_bytes > crate::tool_data::MAX_TOOL_STREAM_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    if stored.stderr.start_offset > stored.stderr.observed_end {
        return Err(StoreError::Corrupt);
    }
    if !stored.stderr.seen {
        if stored.stderr.start_offset != 0
            || stored.stderr.observed_end != 0
            || stored.stderr.file_bytes != 0
            || stored.stderr.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else if stored.stderr.expired {
        if stored.stderr.start_offset != stored.stderr.observed_end
            || stored.stderr.file_bytes != 0
            || stored.stderr.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        let diff = stored
            .stderr
            .observed_end
            .checked_sub(stored.stderr.start_offset)
            .ok_or(StoreError::Corrupt)?;
        if diff != stored.stderr.file_bytes as u64 {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.stderr.file_bytes > crate::tool_data::MAX_TOOL_STREAM_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    let change_bytes = stored.file_change.as_ref().map_or(0, |change| {
        let before = match &change.before {
            crate::changes::ChangeRevision::Content { bytes, .. } => *bytes,
            _ => 0,
        };
        let after = match &change.after {
            crate::changes::ChangeRevision::Content { bytes, .. } => *bytes,
            _ => 0,
        };
        before.saturating_add(after)
    });

    let total_file_bytes = stored
        .input
        .file_bytes
        .saturating_add(stored.result.file_bytes)
        .saturating_add(stored.stdout.file_bytes)
        .saturating_add(stored.stderr.file_bytes)
        .saturating_add(change_bytes);
    if total_file_bytes > 3 * 1024 * 1024 {
        return Err(StoreError::RecordTooLarge);
    }

    let check_sha256 = |bytes: usize, hash: Option<&str>| -> bool {
        if bytes == 0 {
            hash.is_none()
        } else {
            hash.is_some_and(valid_sha256)
        }
    };
    if !check_sha256(stored.input.file_bytes, stored.input.file_sha256.as_deref())
        || !check_sha256(
            stored.result.file_bytes,
            stored.result.file_sha256.as_deref(),
        )
        || !check_sha256(
            stored.stdout.file_bytes,
            stored.stdout.file_sha256.as_deref(),
        )
        || !check_sha256(
            stored.stderr.file_bytes,
            stored.stderr.file_sha256.as_deref(),
        )
    {
        return Err(StoreError::Corrupt);
    }

    if let Some(cmd) = &stored.command {
        if !cmd.status.is_terminal() {
            return Err(StoreError::Corrupt);
        }
        if cmd.stdout_base_offset != stored.stdout.start_offset
            || cmd.stdout_observed_end != stored.stdout.observed_end
            || cmd.stderr_base_offset != stored.stderr.start_offset
            || cmd.stderr_observed_end != stored.stderr.observed_end
        {
            return Err(StoreError::Corrupt);
        }
    }

    if let Some(change) = &stored.file_change {
        validate_stored_file_change(change)?;
    }

    Ok(())
}

fn validate_stored_file_change(change: &StoredFileChange) -> Result<(), StoreError> {
    crate::workspace::validate_relative_path(&change.path).map_err(|_| StoreError::Corrupt)?;
    match &change.before {
        ChangeRevision::Missing => {
            if !change.before_captured {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Content { sha256, bytes } => {
            if !change.before_captured
                || *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
                || !valid_sha256(sha256)
            {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => {
            if change.before_captured {
                return Err(StoreError::Corrupt);
            }
        }
    }
    match &change.after {
        ChangeRevision::Content { sha256, bytes } => {
            if !change.after_captured
                || *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
                || !valid_sha256(sha256)
            {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Missing | ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => {
            if change.after_captured {
                return Err(StoreError::Corrupt);
            }
        }
    }
    match change.commit_state {
        crate::changes::ChangeCommitState::Applied
        | crate::changes::ChangeCommitState::Conflict => {
            if !matches!(&change.after, ChangeRevision::Content { .. }) {
                return Err(StoreError::Corrupt);
            }
        }
        crate::changes::ChangeCommitState::NotCommitted
        | crate::changes::ChangeCommitState::Unknown => {}
    }
    Ok(())
}

fn validate_file_change_snapshot(
    stored: Option<&StoredFileChange>,
    before_bytes: Option<&[u8]>,
    after_bytes: Option<&[u8]>,
) -> Result<(), StoreError> {
    let Some(stored) = stored else {
        return if before_bytes.is_none() && after_bytes.is_none() {
            Ok(())
        } else {
            Err(StoreError::Corrupt)
        };
    };
    validate_stored_file_change(stored)?;
    if let Some(bytes) = before_bytes {
        let ChangeRevision::Content {
            sha256,
            bytes: size,
        } = &stored.before
        else {
            return Err(StoreError::Corrupt);
        };
        if !stored.before_captured
            || bytes.len() != *size
            || bytes.len() > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
            || hash_bytes(bytes) != *sha256
        {
            return Err(StoreError::Corrupt);
        }
    }
    if let Some(bytes) = after_bytes {
        let ChangeRevision::Content {
            sha256,
            bytes: size,
        } = &stored.after
        else {
            return Err(StoreError::Corrupt);
        };
        if !stored.after_captured
            || bytes.len() != *size
            || bytes.len() > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
            || hash_bytes(bytes) != *sha256
        {
            return Err(StoreError::Corrupt);
        }
    }
    Ok(())
}

async fn read_blob(
    path: &Path,
    expected_bytes: usize,
    expected_sha256: Option<&str>,
    max_cap: usize,
) -> (Option<Vec<u8>>, bool) {
    if expected_bytes == 0 {
        return (None, false);
    }
    if expected_bytes > max_cap {
        return (None, true);
    }
    let Some(expected_hash) = expected_sha256 else {
        return (None, true);
    };
    let Ok(file) = safe_open_read(path).await else {
        return (None, true);
    };
    let mut bytes = Vec::with_capacity(expected_bytes);
    if file
        .take((expected_bytes.saturating_add(1)) as u64)
        .read_to_end(&mut bytes)
        .await
        .is_err()
    {
        return (None, true);
    }
    if bytes.len() != expected_bytes {
        return (None, true);
    }
    let actual_hash = hash_bytes(&bytes);
    if actual_hash != expected_hash {
        return (None, true);
    }
    (Some(bytes), false)
}

async fn read_change_blob(path: &Path, revision: &ChangeRevision) -> (Option<Vec<u8>>, bool) {
    let ChangeRevision::Content { sha256, bytes } = revision else {
        return (None, false);
    };
    if *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES || !valid_sha256(sha256) {
        return (None, true);
    }
    #[cfg(test)]
    READ_CHANGE_BLOBS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(path.to_path_buf());
    let Ok(file) = safe_open_read(path).await else {
        return (None, true);
    };
    let mut value = Vec::with_capacity(*bytes);
    if file
        .take((*bytes).saturating_add(1) as u64)
        .read_to_end(&mut value)
        .await
        .is_err()
    {
        return (None, true);
    }
    if value.len() != *bytes || hash_bytes(&value) != *sha256 {
        return (None, true);
    }
    (Some(value), false)
}

async fn verify_and_scan_temp_dir(
    path: &Path,
    total_scanned: &mut usize,
    max_scan_entries: usize,
    deadline: Instant,
) -> Result<Option<u64>, StoreError> {
    let mut read_dir = fs::read_dir(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    let mut temp_bytes = 0u64;
    while let Some(entry) = {
        if Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        read_dir
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
    } {
        *total_scanned = total_scanned.saturating_add(1);
        if *total_scanned > max_scan_entries || Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        let meta = fs::symlink_metadata(entry.path())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if meta.file_type().is_symlink() || !meta.is_file() {
            return Ok(None);
        }
        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            return Ok(None);
        };
        if !ALLOWED_AUX_FILES.contains(&name_str) {
            return Ok(None);
        }
        temp_bytes = temp_bytes.saturating_add(meta.len());
    }
    Ok(Some(temp_bytes))
}

async fn scan_and_verify_tool_dir(
    path: &Path,
    session_id: SessionId,
    expected_hash: &str,
    total_scanned: &mut usize,
    max_scan_entries: usize,
    deadline: Instant,
) -> Result<(u64, std::time::SystemTime, AuxDirectoryEntry), StoreError> {
    let dir_meta = fs::symlink_metadata(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if dir_meta.file_type().is_symlink() || !dir_meta.is_dir() {
        return Err(StoreError::Corrupt);
    }
    let mtime = dir_meta
        .modified()
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

    let mut read_dir = fs::read_dir(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    let mut dir_bytes = 0u64;
    let mut has_record = false;

    while let Some(entry) = {
        if Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        read_dir
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
    } {
        *total_scanned = total_scanned.saturating_add(1);
        if *total_scanned > max_scan_entries || Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        let file_meta = fs::symlink_metadata(entry.path())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if file_meta.file_type().is_symlink() || !file_meta.is_file() {
            return Err(StoreError::Corrupt);
        }
        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            return Err(StoreError::Corrupt);
        };
        if !ALLOWED_AUX_FILES.contains(&name_str) {
            return Err(StoreError::Corrupt);
        }
        if name_str == TOOL_RECORD_FILE {
            has_record = true;
        }
        dir_bytes = dir_bytes.saturating_add(file_meta.len());
    }

    if !has_record {
        return Err(StoreError::Corrupt);
    }

    let record_path = path.join(TOOL_RECORD_FILE);
    let file = safe_open_read(&record_path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    let mut bytes = Vec::new();
    if file
        .take((MAX_TOOL_METADATA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .is_err()
    {
        return Err(StoreError::Unavailable);
    }
    if bytes.len() > MAX_TOOL_METADATA_BYTES {
        return Err(StoreError::Corrupt);
    }
    let stored: StoredToolRecord =
        serde_json::from_slice(&bytes).map_err(|_| StoreError::Corrupt)?;
    if stored.version != TOOL_RECORD_FORMAT_VERSION
        || stored.tool_ref.session_id != session_id
        || tool_ref_hash(&stored.tool_ref) != expected_hash
    {
        return Err(StoreError::Corrupt);
    }
    validate_stored_tool_record(&stored, &stored.tool_ref)?;

    Ok((
        dir_bytes,
        mtime,
        AuxDirectoryEntry {
            session_id,
            path: path.to_path_buf(),
            mtime,
            bytes: dir_bytes,
        },
    ))
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
mod tests {
    use base64::Engine as _;
    use minicore_runtime::ToolCallId;
    use minicore_runtime::execution::ConfigRevision;
    use minicore_runtime::history::{AssistantHistory, UserHistory};
    use minicore_runtime::model::{
        AssistantPart, ModelFinishReason, ModelRef, ReasoningPreference, ToolCall, Usage,
    };
    use minicore_runtime::tools::ToolResultOutcome;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use crate::changes::{
        ChangeCommitState, ChangeCoverage, ChangeKind, FileChange, content_revision,
    };
    use crate::error::AgentError;
    use crate::tool_data::{ToolData, ToolDataAvailability, ToolDataStream, ToolOutputRequest};

    use super::*;

    async fn fixture(label: &str) -> (PathBuf, Store, SessionId) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-store-{label}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&base).await;
        let store = Store::open(base.clone()).await.unwrap();
        let session_id = SessionId::new().unwrap();
        (base, store, session_id)
    }

    fn record(_store: &Store, session_id: SessionId) -> SessionRecord {
        let now = utc_timestamp().unwrap();
        SessionRecord {
            format_version: SESSION_FORMAT_VERSION,
            session_id,
            title: Some("title".to_owned()),
            profile: "coding".to_owned(),
            workspace: PathBuf::from("/tmp/workspace"),
            model: "main".to_owned(),
            reasoning: ReasoningPreference::Auto,
            system_prompt: "system".to_owned(),
            tools: vec!["read".to_owned(), "write".to_owned()],
            max_tool_rounds: 32,
            approval: ApprovalMode::Auto,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    fn loop_record(session_id: SessionId, text: &str) -> StoredLoopRecord {
        let _ = session_id;
        let loop_id = LoopId::new().unwrap();
        StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items: vec![HistoryItem::User(UserHistory {
                loop_id,
                kind: minicore_runtime::history::UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(text).unwrap(),
            })],
            usage: Usage::new(1, 2, 0),
            requests: 1,
            tool_rounds: 0,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: utc_timestamp().unwrap(),
            user_times: None,
        }
    }

    fn loop_record_with_users(texts: &[&str]) -> StoredLoopRecord {
        let loop_id = LoopId::new().unwrap();
        let items = texts
            .iter()
            .map(|text| {
                HistoryItem::User(UserHistory {
                    loop_id,
                    kind: minicore_runtime::history::UserMessageKind::Prompt,
                    input: minicore_runtime::execution::UserInput::text(text).unwrap(),
                })
            })
            .collect();
        StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items,
            usage: Usage::new(1, 2, 0),
            requests: 1,
            tool_rounds: 0,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: utc_timestamp().unwrap(),
            user_times: None,
        }
    }

    async fn append_raw_history_line(
        store: &Store,
        session_id: SessionId,
        value: serde_json::Value,
    ) {
        let path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        let mut file = OpenOptions::new().append(true).open(path).await.unwrap();
        file.write_all(&bytes).await.unwrap();
        file.flush().await.unwrap();
    }

    async fn append_raw_history_json_line(store: &Store, session_id: SessionId, json: &str) {
        let path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut file = OpenOptions::new().append(true).open(path).await.unwrap();
        file.write_all(json.as_bytes()).await.unwrap();
        file.write_all(b"\n").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn user_time_metadata_is_bounded_validated_and_old_records_stay_compatible() {
        let (base, store, session_id) = fixture("user-times").await;
        let session = record(&store, session_id);
        store.create_session(&session).await.unwrap();

        let mut valid = loop_record(session_id, "same");
        let loop_id = valid.loop_id;
        valid.user_times = Some(vec![Some("2026-09-05T14:05:06.007Z".to_owned())]);
        store.append_loop(session_id, &valid).await.unwrap();
        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(
            loaded.user_times.get(&(loop_id, 0)).map(String::as_str),
            Some("2026-09-05T14:05:06.007Z")
        );

        // A missing optional field remains the old JSONL compatibility path.
        let old = loop_record(session_id, "old");
        store.append_loop(session_id, &old).await.unwrap();
        let history_path = base
            .join(SESSIONS_DIR)
            .join(session_id.to_string())
            .join(HISTORY_FILE);
        let old_jsonl_before_load = fs::read(&history_path).await.unwrap();
        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 2);
        assert_eq!(
            fs::read(&history_path).await.unwrap(),
            old_jsonl_before_load
        );

        let mut invalid_timestamp = loop_record(session_id, "invalid");
        invalid_timestamp.user_times = Some(vec![Some("not-a-timestamp".to_owned())]);
        store
            .append_loop(session_id, &invalid_timestamp)
            .await
            .unwrap();

        let mut too_many = loop_record(session_id, "too many");
        too_many.user_times = Some(vec![
            Some("2026-09-05T14:05:06.007Z".to_owned()),
            Some("extra".to_owned()),
        ]);
        store.append_loop(session_id, &too_many).await.unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 4);
        assert!(
            !loaded
                .user_times
                .contains_key(&(invalid_timestamp.loop_id, 0))
        );
        assert!(!loaded.user_times.contains_key(&(too_many.loop_id, 0)));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn user_time_metadata_does_not_displace_core_at_line_limit() {
        let (base, store, session_id) = fixture("user-times-line-limit").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let mut loop_record: StoredLoopRecord =
            serde_json::from_slice(&loop_line_with_size(MAX_LOOP_RECORD_BYTES - 32)).unwrap();
        let user_count = loop_record
            .items
            .iter()
            .filter(|item| matches!(item, HistoryItem::User(_)))
            .count();
        loop_record.user_times = Some(
            (0..user_count)
                .map(|_| Some("2026-09-05T14:05:06.007Z".to_owned()))
                .collect(),
        );

        store.append_loop(session_id, &loop_record).await.unwrap();
        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), user_count);
        assert!(loaded.user_times.is_empty());
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn same_text_users_keep_occurrence_when_one_time_is_invalid() {
        let (base, store, session_id) = fixture("user-time-occurrence").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let mut loop_record = loop_record_with_users(&["same", "same"]);
        let loop_id = loop_record.loop_id;
        let valid_time = "2026-09-05T14:05:06.007Z";
        loop_record.user_times = Some(vec![
            Some("not-a-timestamp".to_owned()),
            Some(valid_time.to_owned()),
        ]);
        store.append_loop(session_id, &loop_record).await.unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        let page =
            crate::history::page_history(loaded.history.as_ref(), 0, 100, &loaded.user_times);
        let timestamps = page
            .items
            .iter()
            .map(|item| match &item.item {
                crate::history::HistoryItemView::User(user) => user.timestamp.as_deref(),
                other => panic!("unexpected history item {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![None, Some(valid_time)]);
        assert!(!loaded.user_times.contains_key(&(loop_id, 0)));
        assert_eq!(
            loaded.user_times.get(&(loop_id, 1)).map(String::as_str),
            Some(valid_time)
        );
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn malformed_user_time_json_does_not_block_history_read_or_append() {
        let (base, store, session_id) = fixture("malformed-user-times").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        store
            .append_loop(session_id, &loop_record(session_id, "before"))
            .await
            .unwrap();

        let malformed_shape = serde_json::to_value(loop_record(session_id, "object")).unwrap();
        let malformed_shape = {
            let mut value = malformed_shape;
            value["user_times"] = json!({"unexpected": "shape"});
            value
        };
        append_raw_history_line(&store, session_id, malformed_shape).await;

        let malformed_entry = serde_json::to_value(loop_record(session_id, "entry")).unwrap();
        let malformed_entry = {
            let mut value = malformed_entry;
            value["user_times"] = json!([{"unexpected": "entry"}]);
            value
        };
        append_raw_history_line(&store, session_id, malformed_entry).await;

        let loaded = store.load_session(session_id).await.unwrap();
        let texts = loaded
            .history
            .iter()
            .map(|item| match item {
                HistoryItem::User(user) => user.input.as_text().to_owned(),
                other => panic!("unexpected history item {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(texts, vec!["before", "object", "entry"]);
        assert!(loaded.user_times.is_empty());

        store
            .append_loop(session_id, &loop_record(session_id, "after"))
            .await
            .unwrap();
        assert_eq!(
            store.load_session(session_id).await.unwrap().history.len(),
            4
        );
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn core_history_corruption_is_still_rejected() {
        let (base, store, session_id) = fixture("core-history-corrupt").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let mut value = serde_json::to_value(loop_record(session_id, "core")).unwrap();
        value["items"] = json!("not-an-items-array");
        value["user_times"] = json!({"unexpected": "shape"});
        append_raw_history_line(&store, session_id, value).await;

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn duplicate_core_json_fields_are_rejected() {
        let (base, store, session_id) = fixture("duplicate-core-field").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let raw = serde_json::to_string(&loop_record(session_id, "duplicate core")).unwrap();
        let duplicate_items = raw.replacen("\"items\":", "\"items\":[],\"items\":", 1);
        append_raw_history_json_line(&store, session_id, &duplicate_items).await;

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn duplicate_user_time_json_fields_are_rejected() {
        let (base, store, session_id) = fixture("duplicate-user-times-field").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let mut loop_record = loop_record(session_id, "duplicate user times");
        loop_record.user_times = Some(vec![Some("2026-09-05T14:05:06.007Z".to_owned())]);
        let raw = serde_json::to_string(&loop_record).unwrap();
        let duplicate_user_times =
            raw.replacen("\"user_times\":", "\"user_times\":null,\"user_times\":", 1);
        append_raw_history_json_line(&store, session_id, &duplicate_user_times).await;

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn unknown_and_wrong_type_core_json_fields_are_rejected() {
        let unknown_field = {
            let mut raw =
                serde_json::to_string(&loop_record(SessionId::new().unwrap(), "unknown")).unwrap();
            raw.insert_str(raw.len() - 1, ",\"unknown_core\":true");
            raw
        };
        let wrong_type = {
            let mut raw =
                serde_json::to_string(&loop_record(SessionId::new().unwrap(), "wrong")).unwrap();
            let field = "\"items\":";
            let value_start = raw.find(field).unwrap() + field.len();
            let value_end = raw[value_start..].find("],\"usage\"").unwrap() + value_start + 1;
            raw.replace_range(value_start..value_end, "\"not-an-items-array\"");
            raw
        };

        for (label, raw) in [
            ("unknown-core-field", unknown_field),
            ("wrong-type-core-field", wrong_type),
        ] {
            let (base, store, session_id) = fixture(label).await;
            store
                .create_session(&record(&store, session_id))
                .await
                .unwrap();
            append_raw_history_json_line(&store, session_id, &raw).await;
            assert!(matches!(
                store.load_session(session_id).await,
                Err(StoreError::Corrupt)
            ));
            let _ = fs::remove_dir_all(base).await;
        }
    }

    #[test]
    fn timestamp_validation_accepts_generated_rfc3339_and_rejects_lookalikes() {
        assert!(valid_timestamp("2026-09-05T14:05:06.007Z"));
        assert!(valid_timestamp("2026-09-05T14:05:06+08:00"));
        assert!(!valid_timestamp("2026-99-99T99:99:99Z"));
        assert!(!valid_timestamp("2026-09-05T14:05:06"));
        assert!(!valid_timestamp("2026-09-05T14:05:06.badZ"));
    }

    fn loop_line_with_size(target: usize) -> Vec<u8> {
        let loop_id = LoopId::new().unwrap();
        let fixed_text = "x".repeat(255 * 1024);
        let mut items = (0..64)
            .map(|_| {
                HistoryItem::User(UserHistory {
                    loop_id,
                    kind: minicore_runtime::history::UserMessageKind::Prompt,
                    input: minicore_runtime::execution::UserInput::text(&fixed_text).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        items.push(HistoryItem::User(UserHistory {
            loop_id,
            kind: minicore_runtime::history::UserMessageKind::Prompt,
            input: minicore_runtime::execution::UserInput::text("x").unwrap(),
        }));
        let mut record = StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items,
            usage: Usage::new(1, 2, 0),
            requests: 1,
            tool_rounds: 0,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: utc_timestamp().unwrap(),
            user_times: None,
        };
        let base = serde_json::to_vec(&record).unwrap();
        let final_text_len = target
            .checked_sub(base.len())
            .and_then(|length| length.checked_add(1))
            .expect("test record must have room for its variable item");
        assert!(final_text_len <= 256 * 1024);
        let Some(HistoryItem::User(user)) = record.items.last_mut() else {
            unreachable!("test record has a final user item");
        };
        user.input =
            minicore_runtime::execution::UserInput::text("x".repeat(final_text_len)).unwrap();
        let bytes = serde_json::to_vec(&record).unwrap();
        assert_eq!(bytes.len(), target);
        bytes
    }

    #[tokio::test]
    async fn create_round_trip_and_delete() {
        let (base, store, session_id) = fixture("create").await;
        let record = record(&store, session_id);
        store.create_session(&record).await.unwrap();

        let directory = store.session_directory(session_id);
        assert!(
            fs::metadata(directory.join(SESSION_RECORD_FILE))
                .await
                .unwrap()
                .is_file()
        );
        assert!(
            fs::metadata(directory.join(HISTORY_FILE))
                .await
                .unwrap()
                .is_file()
        );

        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.record.session_id, session_id);
        assert_eq!(loaded.record.model, "main");
        assert_eq!(loaded.record.profile, "coding");
        assert!(loaded.history.is_empty());
        assert!(!store.list_sessions().await.unwrap().is_empty());

        store.delete_session(session_id).await.unwrap();
        assert!(matches!(
            store.load_record(session_id).await,
            Err(StoreError::SessionNotFound)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn extended_reasoning_values_round_trip_in_record_and_history() {
        let (base, store, session_id) = fixture("extended-reasoning").await;
        let mut session_record = record(&store, session_id);
        store.create_session(&session_record).await.unwrap();

        let values = [
            (ReasoningPreference::XHigh, "xhigh"),
            (ReasoningPreference::Max, "max"),
            (ReasoningPreference::Ultra, "ultra"),
        ];
        for (reasoning, wire) in values {
            session_record.reasoning = reasoning;
            store.write_record(&session_record).await.unwrap();
            assert_eq!(
                store.load_record(session_id).await.unwrap().reasoning,
                reasoning
            );

            let loop_id = LoopId::new().unwrap();
            store
                .append_loop(
                    session_id,
                    &StoredLoopRecord {
                        loop_id,
                        outcome: StoredLoopOutcome::Completed,
                        items: vec![HistoryItem::Assistant(AssistantHistory {
                            loop_id,
                            request_index: 0,
                            model: "main".parse().unwrap(),
                            reasoning,
                            content: vec![AssistantPart::Text(format!("answer-{wire}"))],
                            finish_reason: ModelFinishReason::Stop,
                            usage: Usage::new(1, 2, 0),
                        })],
                        usage: Usage::new(1, 2, 0),
                        requests: 1,
                        tool_rounds: 0,
                        final_config_revision: ConfigRevision::INITIAL,
                        completed_at: utc_timestamp().unwrap(),
                        user_times: None,
                    },
                )
                .await
                .unwrap();
        }

        let loaded = store.load_session(session_id).await.unwrap();
        let history_reasoning = loaded
            .history
            .iter()
            .map(|item| match item {
                HistoryItem::Assistant(assistant) => assistant.reasoning,
                other => panic!("unexpected history item {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            history_reasoning,
            values
                .iter()
                .map(|(reasoning, _)| *reasoning)
                .collect::<Vec<_>>()
        );
        let serialized = serde_json::to_string(loaded.history.as_ref()).unwrap();
        let view = crate::history::page_history(
            loaded.history.as_ref(),
            0,
            100,
            &std::collections::HashMap::new(),
        );
        let view_serialized = serde_json::to_string(&view).unwrap();
        for (_, wire) in values {
            assert!(serialized.contains(&format!("\"reasoning\":\"{wire}\"")));
            assert!(view_serialized.contains(&format!("\"reasoning_level\":\"{wire}\"")));
        }
        assert_eq!(loaded.record.reasoning, ReasoningPreference::Ultra);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn append_and_load_flatten_history_in_order() {
        let (base, store, session_id) = fixture("append").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        store
            .append_loop(session_id, &loop_record(session_id, "first"))
            .await
            .unwrap();
        store
            .append_loop(session_id, &loop_record(session_id, "second"))
            .await
            .unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        let texts = loaded
            .history
            .iter()
            .map(|item| match item {
                HistoryItem::User(user) => user.input.as_text().to_owned(),
                other => panic!("unexpected item {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(texts, vec!["first".to_owned(), "second".to_owned()]);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn final_partial_line_is_ignored_and_truncated() {
        let (base, store, session_id) = fixture("partial").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        store
            .append_loop(session_id, &loop_record(session_id, "first"))
            .await
            .unwrap();
        let history_path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await
            .unwrap();
        file.write_all(b"{\"truncated\": true").await.unwrap();
        file.flush().await.unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 1);

        // After the repair, a new append is still legal.
        store
            .append_loop(session_id, &loop_record(session_id, "second"))
            .await
            .unwrap();
        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 2);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn first_partial_line_only_is_truncated_to_empty_then_append_works() {
        let (base, store, session_id) = fixture("only-partial").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let history_path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await
            .unwrap();
        file.write_all(b"{\"truncated\": true").await.unwrap();
        file.flush().await.unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        assert!(loaded.history.is_empty());
        // The repair truncated the whole file back to zero bytes.
        assert_eq!(fs::metadata(&history_path).await.unwrap().len(), 0);

        store
            .append_loop(session_id, &loop_record(session_id, "first"))
            .await
            .unwrap();
        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 1);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn oversized_final_partial_line_is_repaired_and_appendable() {
        let (base, store, session_id) = fixture("oversized-partial").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let history_path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await
            .unwrap();
        // Even an oversized final partial is the one repairable tail case.
        file.write_all(&loop_line_with_size(MAX_LOOP_RECORD_BYTES + 1))
            .await
            .unwrap();
        file.flush().await.unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        assert!(loaded.history.is_empty());
        assert_eq!(fs::metadata(&history_path).await.unwrap().len(), 0);

        store
            .append_loop(session_id, &loop_record(session_id, "after repair"))
            .await
            .unwrap();
        let reopened = Store::open(base.clone()).await.unwrap();
        let loaded = reopened.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 1);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn legal_complete_line_at_limit_is_accepted_but_just_over_limit_is_corrupt() {
        let (base, store, session_id) = fixture("line-boundary").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let history_path = store.session_directory(session_id).join(HISTORY_FILE);

        let at_limit = loop_line_with_size(MAX_LOOP_RECORD_BYTES);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await
            .unwrap();
        file.write_all(&at_limit).await.unwrap();
        file.write_all(b"\n").await.unwrap();
        file.flush().await.unwrap();
        let loaded = store.load_session(session_id).await.unwrap();
        assert_eq!(loaded.history.len(), 65);

        let over_limit = loop_line_with_size(MAX_LOOP_RECORD_BYTES + 1);
        fs::write(&history_path, [&over_limit[..], b"\n"].concat())
            .await
            .unwrap();
        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn missing_history_file_is_corrupt_but_list_still_reads() {
        let (base, store, session_id) = fixture("missing-history").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        fs::remove_file(store.session_directory(session_id).join(HISTORY_FILE))
            .await
            .unwrap();

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        // list only reads the record and still reports the session.
        let records = store.list_sessions().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].session_id, session_id);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn oversized_session_record_is_corrupt() {
        let (base, store, session_id) = fixture("oversized-record").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let record_path = store
            .session_directory(session_id)
            .join(SESSION_RECORD_FILE);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&record_path)
            .await
            .unwrap();
        file.write_all(vec![b'{'; MAX_SESSION_RECORD_BYTES + 1].as_slice())
            .await
            .unwrap();
        file.flush().await.unwrap();

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn store_root_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let label = format!("root-symlink-{}", std::process::id());
        let root_area = std::env::temp_dir().join(label);
        let _ = fs::remove_dir_all(&root_area).await;
        let base = root_area.join("data");
        let real = root_area.join("real");
        fs::create_dir_all(&real).await.unwrap();
        symlink(&real, &base).unwrap();

        let result = Store::open(base.clone()).await;
        assert!(matches!(result, Err(StoreError::InvalidRoot)));
        let _ = fs::remove_file(&base).await;
        let _ = fs::remove_dir_all(&root_area).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn post_open_sessions_symlink_rejects_all_session_operations() {
        use std::os::unix::fs::symlink;

        let (base, store, session_id) = fixture("sessions-root-symlink").await;
        let original = record(&store, session_id);
        store.create_session(&original).await.unwrap();

        let sessions = store.sessions_directory();
        let moved_sessions = base.join("sessions-real");
        fs::rename(&sessions, &moved_sessions).await.unwrap();

        let outside = base.join("outside");
        let outside_session = outside.join(session_id.to_string());
        fs::create_dir_all(&outside_session).await.unwrap();
        fs::write(
            outside_session.join(SESSION_RECORD_FILE),
            serde_json::to_vec(&original).unwrap(),
        )
        .await
        .unwrap();
        fs::write(outside_session.join(HISTORY_FILE), b"")
            .await
            .unwrap();
        let sentinel = outside.join("sentinel");
        fs::write(&sentinel, b"outside must not change")
            .await
            .unwrap();
        let outside_record_before = fs::read(outside_session.join(SESSION_RECORD_FILE))
            .await
            .unwrap();
        let outside_history_before = fs::read(outside_session.join(HISTORY_FILE)).await.unwrap();

        symlink(&outside, &sessions).unwrap();

        assert!(matches!(
            store.load_record(session_id).await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.list_sessions().await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.write_record(&original).await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store
                .append_loop(session_id, &loop_record(session_id, "must not append"))
                .await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.delete_session(session_id).await,
            Err(StoreError::Corrupt)
        ));

        let new_session_id = SessionId::new().unwrap();
        let new_record = record(&store, new_session_id);
        assert!(matches!(
            store.create_session(&new_record).await,
            Err(StoreError::Corrupt)
        ));

        assert_eq!(
            fs::read(&sentinel).await.unwrap(),
            b"outside must not change"
        );
        assert_eq!(
            fs::read(outside_session.join(SESSION_RECORD_FILE))
                .await
                .unwrap(),
            outside_record_before
        );
        assert_eq!(
            fs::read(outside_session.join(HISTORY_FILE)).await.unwrap(),
            outside_history_before
        );
        assert!(
            fs::try_exists(moved_sessions.join(session_id.to_string()))
                .await
                .unwrap()
        );
        let _ = fs::remove_file(&sessions).await;
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn middle_corrupt_line_is_corrupt() {
        let (base, store, session_id) = fixture("corrupt").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        store
            .append_loop(session_id, &loop_record(session_id, "first"))
            .await
            .unwrap();
        let history_path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await
            .unwrap();
        file.write_all(b"{\"broken\": true}\n").await.unwrap();
        file.flush().await.unwrap();

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn oversized_line_is_corrupt() {
        let (base, store, session_id) = fixture("oversized").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let history_path = store.session_directory(session_id).join(HISTORY_FILE);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await
            .unwrap();
        file.write_all(vec![b'{'; MAX_LOOP_RECORD_BYTES + 1].as_slice())
            .await
            .unwrap();
        file.write_all(b"\n").await.unwrap();
        file.flush().await.unwrap();

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn record_too_large_append_is_rejected() {
        let (base, store, session_id) = fixture("record-too-large").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let loop_id = LoopId::new().unwrap();
        let oversized_item = HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: 0,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::Text("x".repeat(MAX_LOOP_RECORD_BYTES + 16))],
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::new(1, 2, 0),
        });
        let big = StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items: vec![oversized_item],
            usage: Usage::new(1, 2, 0),
            requests: 1,
            tool_rounds: 0,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: utc_timestamp().unwrap(),
            user_times: None,
        };
        assert!(matches!(
            store.append_loop(session_id, &big).await,
            Err(StoreError::RecordTooLarge)
        ));
        let loaded = store.load_session(session_id).await.unwrap();
        assert!(loaded.history.is_empty());
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn old_format_is_skipped_in_list_and_rejected_on_open() {
        let (base, store, session_id) = fixture("old-format").await;
        let directory = store.session_directory(session_id);
        fs::create_dir_all(&directory).await.unwrap();
        fs::write(directory.join(LEGACY_MANIFEST_FILE), b"{}")
            .await
            .unwrap();
        fs::write(directory.join(LEGACY_CONVERSATION_FILE), b"")
            .await
            .unwrap();

        assert!(store.list_sessions().await.unwrap().is_empty());
        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::UnsupportedFormat)
        ));
        // Old files are not modified.
        assert!(
            fs::metadata(directory.join(LEGACY_MANIFEST_FILE))
                .await
                .unwrap()
                .is_file()
        );
        let _ = fs::remove_dir_all(base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_session_files_are_rejected() {
        use std::os::unix::fs::symlink;

        let (base, store, session_id) = fixture("symlink").await;
        let outside = base.join("outside.json");
        fs::write(&outside, b"{}").await.unwrap();
        let directory = store.session_directory(session_id);
        fs::create_dir_all(&directory).await.unwrap();
        symlink(&outside, directory.join(SESSION_RECORD_FILE)).unwrap();

        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn unrelated_files_do_not_break_list() {
        let (base, store, session_id) = fixture("tolerance").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        fs::write(store.sessions_directory().join("not-a-session"), b"x")
            .await
            .unwrap();
        fs::create_dir(store.sessions_directory().join("bad"))
            .await
            .unwrap();

        let records = store.list_sessions().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].session_id, session_id);
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn delete_only_removes_the_target_session_directory() {
        let (base, store, session_id) = fixture("delete-only").await;
        let other = SessionId::new().unwrap();
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        store.create_session(&record(&store, other)).await.unwrap();

        store.delete_session(session_id).await.unwrap();
        assert!(store.load_session(other).await.is_ok());
        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::SessionNotFound)
        ));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn loaded_history_is_sanitized_of_opaque_reasoning() {
        let (base, store, session_id) = fixture("sanitize").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let loop_id = LoopId::new().unwrap();
        let tool = ToolCall::new(
            ToolCallId::new("call-1").unwrap(),
            "read".parse().unwrap(),
            json!({"path": "file.txt"}),
            0,
        )
        .unwrap();
        let assistant = HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: 0,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![
                AssistantPart::Text("hello".to_owned()),
                AssistantPart::Reasoning(
                    minicore_runtime::model::ReasoningContent::new(
                        Some("visible reasoning".to_owned()),
                        None,
                        Some("enc::opaque".to_owned()),
                        Some("sig::opaque".to_owned()),
                    )
                    .unwrap(),
                ),
                AssistantPart::ToolCall(tool),
            ],
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::new(1, 2, 0),
        });
        let record = StoredLoopRecord {
            loop_id,
            outcome: StoredLoopOutcome::Completed,
            items: vec![assistant.clone()],
            usage: Usage::new(1, 2, 0),
            requests: 1,
            tool_rounds: 1,
            final_config_revision: ConfigRevision::INITIAL,
            completed_at: utc_timestamp().unwrap(),
            user_times: None,
        };
        store.append_loop(session_id, &record).await.unwrap();

        let loaded = store.load_session(session_id).await.unwrap();
        let history = &loaded.history;
        let HistoryItem::Assistant(loaded) = &history[0] else {
            panic!("expected assistant item");
        };
        let serialized = serde_json::to_string(history.as_ref()).unwrap();
        assert!(!serialized.contains("enc::opaque"));
        assert!(!serialized.contains("sig::opaque"));
        assert!(!serialized.contains("opaque"));
        assert!(serialized.contains("visible reasoning"));
        assert!(
            loaded
                .content
                .iter()
                .any(|part| part.as_tool_call().is_some())
        );
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn session_record_validation_accepts_valid_multiline_prompt_and_known_tools() {
        let (base, store, session_id) = fixture("valid-record").await;
        let mut record = record(&store, session_id);
        record.system_prompt = concat!(
            "You are a helpful assistant.\n",
            "\tPlease follow instructions:\n",
            "1. Read files.\n",
            "2. Write files."
        )
        .to_owned();
        record.tools = vec!["read".to_owned(), "write".to_owned(), "bash".to_owned()];
        record.model = "provider/model-v1:beta".to_owned();
        assert!(record.validate().is_ok());
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn session_record_validation_rejects_empty_and_control_chars_in_system_prompt() {
        let (base, store, session_id) = fixture("invalid-prompt").await;
        let mut record = record(&store, session_id);

        record.system_prompt = String::new();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.system_prompt = "hello\0world".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.system_prompt = "hello\x01world".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.system_prompt = "hello\rworld".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.system_prompt = "hello\r\nworld".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.system_prompt = "hello\n\tworld".to_owned();
        assert!(record.validate().is_ok());
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn session_record_validation_rejects_unknown_and_duplicate_tools() {
        let (base, store, session_id) = fixture("invalid-tools").await;
        let mut record = record(&store, session_id);

        record.tools = vec!["read".to_owned(), "unknown_tool".to_owned()];
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.tools = vec!["read".to_owned(), "read".to_owned()];
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.tools = vec![];
        assert!(record.validate().is_ok());

        record.tools = KNOWN_TOOL_NAMES
            .iter()
            .map(|&name| name.to_owned())
            .collect();
        assert!(record.validate().is_ok());

        record.tools.push("read".to_owned());
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn session_record_validation_rejects_invalid_model_ids() {
        let (base, store, session_id) = fixture("invalid-model").await;
        let mut record = record(&store, session_id);

        record.model = String::new();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.model = "my model".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.model = "model\nid".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.model = "model@invalid".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.model = "model$invalid".to_owned();
        assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

        record.model = "main".to_owned();
        assert!(record.validate().is_ok());

        record.model = "vendor/model-name:v1.0".to_owned();
        assert!(record.validate().is_ok());
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn invalid_session_record_skipped_by_list_and_rejected_by_open() {
        let (base, store, valid_id) = fixture("corrupt-record-list-open").await;

        let valid_record = record(&store, valid_id);
        store.create_session(&valid_record).await.unwrap();

        let invalid_id = SessionId::new().unwrap();
        let invalid_dir = store.session_directory(invalid_id);
        fs::create_dir_all(&invalid_dir).await.unwrap();

        let mut invalid_record = record(&store, invalid_id);
        invalid_record.system_prompt = String::new();
        let invalid_bytes = serde_json::to_vec(&invalid_record).unwrap();
        fs::write(invalid_dir.join(SESSION_RECORD_FILE), invalid_bytes)
            .await
            .unwrap();
        fs::write(invalid_dir.join(HISTORY_FILE), b"")
            .await
            .unwrap();

        let listed = store.list_sessions().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id, valid_id);

        let open_result = store.load_session(invalid_id).await;
        assert!(matches!(open_result, Err(StoreError::Corrupt)));

        assert!(store.load_session(valid_id).await.is_ok());

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn readonly_history_scan_honors_cancel_deadline_and_byte_budget() {
        let (base, store, session_id) = fixture("readonly-scan-limits").await;
        let record = record(&store, session_id);
        store.create_session(&record).await.unwrap();
        store
            .append_loop(session_id, &loop_record(session_id, "scan me"))
            .await
            .unwrap();

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let limits = HistoryScanLimits {
            max_bytes: 64 * 1024,
            max_lines: 100,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
            cancellation: cancellation.clone(),
        };
        assert!(matches!(
            store
                .read_history_page(session_id, 0, 1, None, None, None, None, &limits,)
                .await,
            Err(StoreError::QueryLimit)
        ));

        let expired = HistoryScanLimits {
            max_bytes: 64 * 1024,
            max_lines: 100,
            deadline: std::time::Instant::now() - std::time::Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert!(matches!(
            store
                .read_history_page(session_id, 0, 1, None, None, None, None, &expired,)
                .await,
            Err(StoreError::QueryLimit)
        ));

        let capped = HistoryScanLimits {
            max_bytes: 1,
            max_lines: 100,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert!(matches!(
            store
                .read_history_page(session_id, 0, 1, None, None, None, None, &capped,)
                .await,
            Err(StoreError::QueryLimit)
        ));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn rejected_aux_budget_does_not_create_an_auxiliary_directory() {
        let (base, store, session_id) = fixture("aux-rejected-directory").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let store = store.with_aux_limits(AuxLimits {
            global_bytes: 0,
            ..DEFAULT_AUX_LIMITS
        });
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("rejected").unwrap(),
        };
        let data = crate::tool_data::ToolData::new();
        data.note_requested(&tool_ref, "read");
        data.note_result(&tool_ref, "ok");
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success);
        let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
        assert!(
            store
                .commit_tool_record(&snapshot, Instant::now() + AUX_PERSIST_DEADLINE)
                .await
                .is_err()
        );
        assert!(
            !store
                .session_directory(session_id)
                .join(AUX_TOOLS_DIR)
                .exists()
        );
        assert!(
            fs::read(store.session_directory(session_id).join(HISTORY_FILE))
                .await
                .unwrap()
                .is_empty()
        );
        fs::remove_dir_all(base).await.unwrap();
    }

    #[tokio::test]
    async fn binary_and_utf8_empty_eof_auxiliary_tool_persistence() {
        let (base, store, session_id) = fixture("aux-binary-utf8").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("bash-call-1").unwrap(),
        };

        let input_bytes = b"echo hello\n".to_vec();
        let result_bytes = b"hello\n".to_vec();
        let stdout_bytes: Vec<u8> = (0..50_000u32).map(|b| (b % 256) as u8).collect();

        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Command {
                    script: "echo hello".to_owned(),
                    cwd: "/tmp".to_owned(),
                },
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: Some(ToolPhase::Running),
                started_at: Some("2026-09-15T00:00:00Z".to_owned()),
                finished_at: Some("2026-09-15T00:00:01Z".to_owned()),
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: input_bytes.len(),
                    seen: true,
                    truncated: false,
                    expired: false,
                    file_bytes: input_bytes.len(),
                    file_sha256: Some(hash_bytes(&input_bytes)),
                },
                result: StoredResultSummary {
                    total_bytes: result_bytes.len(),
                    seen: true,
                    truncated: false,
                    expired: false,
                    file_bytes: result_bytes.len(),
                    file_sha256: Some(hash_bytes(&result_bytes)),
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: stdout_bytes.len() as u64,
                    seen: true,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: stdout_bytes.len(),
                    file_sha256: Some(hash_bytes(&stdout_bytes)),
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: true,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: Some(CommandResult {
                    status: crate::tool_data::CommandStatus::Exited,
                    exit_code: Some(0),
                    signal: None,
                    termination_confirmed: true,
                    stdout_base_offset: 0,
                    stdout_observed_end: stdout_bytes.len() as u64,
                    stderr_base_offset: 0,
                    stderr_observed_end: 0,
                    output_complete: true,
                    output_truncated: false,
                }),
                file_change: None,
            },
            input_bytes: Some(input_bytes),
            result_bytes: Some(result_bytes),
            stdout_bytes: Some(stdout_bytes.clone()),
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();

        let recovered = store
            .read_tool_record(&tool_ref)
            .await
            .unwrap()
            .expect("stored tool record must exist");

        let read_res = recovered.project_read(&tool_ref, 4096).unwrap();
        assert_eq!(read_res.execution.tool_ref, tool_ref);
        assert_eq!(read_res.execution.name, "bash");
        assert_eq!(read_res.execution.state, ToolExecutionState::Succeeded);

        let stdout_page = recovered
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 0,
                    max_bytes: Some(100_000),
                },
                100_000,
            )
            .unwrap();
        assert_eq!(stdout_page.encoding, "base64");
        assert!(stdout_page.eof);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&stdout_page.data)
            .unwrap();
        assert_eq!(decoded, stdout_bytes);

        let stderr_page = recovered
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stderr,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(stderr_page.encoding, "base64");
        assert!(stderr_page.eof);
        assert_eq!(stderr_page.observed_end, 0);
        assert!(stderr_page.data.is_empty());

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn file_change_auxiliary_round_trip_and_missing_blob_degrade_details() {
        let (base, store, session_id) = fixture("file-change-round-trip").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("write-file-change").unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(b"user\n"),
                after: content_revision(b"agent\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"user\n".to_vec()),
                after_bytes: Some(b"agent\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("file change record finishes");
        let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();

        let scan = store
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(scan.records.len(), 1);
        let mut blob_budget = ChangeBlobBudget {
            used_bytes: 0,
            exhausted: false,
        };
        assert!(
            store
                .change_blobs_available(
                    session_id,
                    &scan.records[0].0,
                    &scan.records[0].1,
                    &CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10),
                    &mut blob_budget,
                )
                .await
                .unwrap()
        );

        let target = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(&tool_ref))
            .join(TOOL_AFTER_FILE);
        fs::remove_file(&target).await.unwrap();
        let scan = store
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(scan.records.len(), 1);
        let mut blob_budget = ChangeBlobBudget {
            used_bytes: 0,
            exhausted: false,
        };
        assert!(
            !store
                .change_blobs_available(
                    session_id,
                    &scan.records[0].0,
                    &scan.records[0].1,
                    &CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10),
                    &mut blob_budget,
                )
                .await
                .unwrap()
        );
        fs::write(&target, b"bogus!").await.unwrap();
        let scan = store
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(scan.records.len(), 1);
        let mut blob_budget = ChangeBlobBudget {
            used_bytes: 0,
            exhausted: false,
        };
        assert!(
            !store
                .change_blobs_available(
                    session_id,
                    &scan.records[0].0,
                    &scan.records[0].1,
                    &CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10),
                    &mut blob_budget,
                )
                .await
                .unwrap()
        );
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn cold_change_list_pages_many_records_without_reading_other_pages_blobs() {
        let (base, store, session_id) = fixture("change-cold-pages").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        // 12 persisted native records; only the page's records are ever verified.
        let mut refs = Vec::new();
        for index in 0..12u32 {
            let tool_ref = ToolRef {
                session_id,
                loop_id: LoopId::new().unwrap(),
                request_index: index,
                tool_call_id: ToolCallId::new(format!("cold-{index}")).unwrap(),
            };
            let before = format!("before-{index}\n").into_bytes();
            let after = format!("after-{index}\n").into_bytes();
            let data = ToolData::new();
            data.note_requested(&tool_ref, "write");
            data.note_file_change(
                &tool_ref,
                FileChange {
                    path: format!("file-{index}.txt"),
                    kind: ChangeKind::Modified,
                    before: content_revision(&before),
                    after: content_revision(&after),
                    commit_state: ChangeCommitState::Applied,
                    coverage: ChangeCoverage::Complete,
                    before_captured: true,
                    after_captured: true,
                    before_bytes: Some(before),
                    after_bytes: Some(after),
                    before_corrupt: false,
                    after_corrupt: false,
                },
            );
            data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
                .expect("cold change record finishes");
            store
                .commit_tool_record(
                    &data.snapshot_for_persistence(&tool_ref).unwrap(),
                    Instant::now() + Duration::from_secs(10),
                )
                .await
                .unwrap();
            refs.push(tool_ref);
        }

        // A missing after.bin degrades only that one record's details.
        let broken_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(&refs[3]));
        fs::remove_file(broken_dir.join(TOOL_AFTER_FILE))
            .await
            .unwrap();

        let scan = store
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(scan.records.len(), 12);
        assert!(scan.complete);
        assert!(!scan.skipped);
        assert!(scan.metadata_bytes > 0);
        assert!(scan.entries >= 12);
        // Metadata-only scan: no blob is read or hashed while listing.
        let mut budget = ChangeBlobBudget {
            used_bytes: 0,
            exhausted: false,
        };
        for (tool_ref, change) in &scan.records {
            if tool_ref == &refs[3] {
                assert!(
                    !store
                        .change_blobs_available(
                            session_id,
                            tool_ref,
                            change,
                            &CancellationToken::new(),
                            Instant::now() + Duration::from_secs(10),
                            &mut budget,
                        )
                        .await
                        .unwrap()
                );
            }
        }

        // Continuation cursors keep scope identity and reject a different scope.
        let request = crate::changes::ChangesListRequest {
            session_id,
            scope: crate::changes::ChangeScope::Session,
            cursor: None,
            limit: 3,
            max_bytes: Some(64 * 1024),
        };
        // Only the records that survive a page may have their blobs read and
        // verified. Walk every page to exhaustion: each page must attempt at
        // least one read (even a missing after.bin is still attempted), and no
        // attempt may target a record outside that page. The log is scoped to
        // this fixture root so parallel tests cannot pollute or drain it.
        let page_dirs = |page: &crate::changes::ChangesListResult| {
            page.records
                .iter()
                .filter_map(|record| record.tool_ref.as_ref())
                .map(tool_ref_hash)
                .collect::<std::collections::BTreeSet<_>>()
        };
        let dir_of = |path: &Path| {
            path.parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                .unwrap()
                .to_owned()
        };
        let _ = take_read_change_blobs_under(&base);
        let mut cursor = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut pages = 0;
        let mut broken_seen = false;
        loop {
            let page = crate::changes::list_tool_changes(
                store.clone(),
                None,
                crate::changes::ChangesListRequest {
                    cursor: cursor.clone(),
                    ..request.clone()
                },
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
            assert!(!page.records.is_empty());
            let expected_dirs = page_dirs(&page);
            let attempts = take_read_change_blobs_under(&base);
            assert!(
                !attempts.is_empty(),
                "page {pages} did not attempt any blob verification"
            );
            for path in attempts {
                let dir = dir_of(&path);
                assert!(
                    expected_dirs.contains(&dir),
                    "cold listing read a blob outside its page: {dir}"
                );
            }
            for record in &page.records {
                assert!(
                    seen.insert(record.change_ref.clone()),
                    "a record was returned on two pages"
                );
                if record.tool_ref.as_ref() == Some(&refs[3]) {
                    broken_seen = true;
                    assert!(!record.details_available);
                } else {
                    assert!(record.details_available);
                }
            }
            pages += 1;
            match page.next_cursor {
                Some(next) => {
                    assert_eq!(next.session_id, session_id);
                    assert_eq!(next.scope, crate::changes::ChangeScope::Session);
                    cursor = Some(next);
                }
                None => break,
            }
        }
        assert_eq!(pages, 4, "12 records page as 3+3+3+3");
        assert_eq!(seen.len(), 12, "every record was returned exactly once");
        assert!(broken_seen, "the degraded record was still listed");

        // A workspace-scoped cursor cannot continue a session page.
        let mismatched = crate::changes::ChangesListRequest {
            scope: crate::changes::ChangeScope::Workspace,
            cursor: cursor.clone(),
            ..request.clone()
        };
        assert!(mismatched.validate().is_err());

        // Turn scope is filtered by the exact Loop ID and stays separate from
        // the session scope: each record has its own loop here.
        let turn_page = crate::changes::list_tool_changes(
            store.clone(),
            None,
            crate::changes::ChangesListRequest {
                scope: crate::changes::ChangeScope::Turn {
                    loop_id: refs[0].loop_id,
                },
                cursor: None,
                limit: 100,
                max_bytes: Some(64 * 1024),
                ..request.clone()
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(turn_page.records.len(), 1);
        assert_eq!(
            turn_page.records[0].tool_ref.as_ref().unwrap().loop_id,
            refs[0].loop_id
        );

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn corrupt_change_metadata_is_skipped_without_corrupting_the_store() {
        let (base, store, session_id) = fixture("change-metadata-corrupt").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let good_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("good").unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&good_ref, "write");
        data.note_file_change(
            &good_ref,
            FileChange {
                path: "good.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(b"before\n"),
                after: content_revision(b"after\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"before\n".to_vec()),
                after_bytes: Some(b"after\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&good_ref, ToolResultOutcome::Success)
            .expect("change record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&good_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();

        // A directory with a malformed record.json must be skipped as an
        // incomplete scan, not treated as Store corruption. Warm records still
        // list the valid one.
        let bad_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join("f".repeat(64));
        fs::create_dir_all(&bad_dir).await.unwrap();
        fs::write(bad_dir.join(TOOL_RECORD_FILE), b"{ not json")
            .await
            .unwrap();
        let valid_ref = ToolRef {
            session_id,
            loop_id: good_ref.loop_id,
            request_index: good_ref.request_index,
            tool_call_id: good_ref.tool_call_id.clone(),
        };
        let warm = ToolData::new();
        warm.note_requested(&valid_ref, "write");
        warm.note_file_change(
            &valid_ref,
            FileChange {
                path: "good.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(b"before\n"),
                after: content_revision(b"after\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"before\n".to_vec()),
                after_bytes: Some(b"after\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        let result = crate::changes::list_tool_changes(
            store.clone(),
            Some(std::sync::Arc::new(warm)),
            crate::changes::ChangesListRequest {
                session_id,
                scope: crate::changes::ChangeScope::Session,
                cursor: None,
                limit: 10,
                max_bytes: Some(64 * 1024),
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert!(
            result
                .warnings
                .contains(&crate::changes::ChangeListWarning::RecordsSkipped)
        );
        assert!(!result.complete);
        assert!(
            result
                .records
                .iter()
                .any(|record| record.path == "good.txt")
        );

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn change_scan_entry_and_metadata_budgets_reserve_before_io() {
        let (base, store, session_id) = fixture("change-scan-budget").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let mut refs = Vec::new();
        for index in 0..3u32 {
            let tool_ref = ToolRef {
                session_id,
                loop_id: LoopId::new().unwrap(),
                request_index: index,
                tool_call_id: ToolCallId::new(format!("budget-{index}")).unwrap(),
            };
            let data = ToolData::new();
            data.note_requested(&tool_ref, "write");
            data.note_file_change(
                &tool_ref,
                FileChange {
                    path: format!("budget-{index}.txt"),
                    kind: ChangeKind::Modified,
                    before: content_revision(b"before\n"),
                    after: content_revision(b"after\n"),
                    commit_state: ChangeCommitState::Applied,
                    coverage: ChangeCoverage::Complete,
                    before_captured: true,
                    after_captured: true,
                    before_bytes: Some(b"before\n".to_vec()),
                    after_bytes: Some(b"after\n".to_vec()),
                    before_corrupt: false,
                    after_corrupt: false,
                },
            );
            data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
                .expect("budget change record finishes");
            store
                .commit_tool_record(
                    &data.snapshot_for_persistence(&tool_ref).unwrap(),
                    Instant::now() + Duration::from_secs(10),
                )
                .await
                .unwrap();
            refs.push(tool_ref);
        }

        // A scan budget that admits at most one entry must stop before reading a
        // second directory entry, and must report the scan as incomplete rather
        // than silently claiming a complete one-record list.
        let limited = store.clone().with_aux_limits(AuxLimits {
            max_scan_entries: 1,
            ..DEFAULT_AUX_LIMITS
        });
        let scan = limited
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(!scan.complete);
        assert!(scan.skipped);
        assert!(
            scan.entries <= 1,
            "the entry ceiling is never exceeded (got {})",
            scan.entries
        );

        // Metadata reads stay strictly inside the cumulative allowance: an empty
        // remainder refuses before any read, and the direct unit assertions on
        // accounting stay independent of filesystem size.
        let budget = ChangeScanBudget {
            entries: 0,
            metadata_bytes: CHANGE_SCAN_METADATA_BYTES,
        };
        assert_eq!(
            CHANGE_SCAN_METADATA_BYTES.saturating_sub(budget.metadata_bytes),
            0
        );
        assert!(!budget.entry_available(0));
        assert!(budget.entry_available(1));

        // A record.json larger than the bounded metadata read is refused, never
        // parsed as a complete record: the scan reports it as skipped.
        let oversized_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join("a".repeat(64));
        fs::create_dir_all(&oversized_dir).await.unwrap();
        fs::write(
            oversized_dir.join(TOOL_RECORD_FILE),
            vec![b'x'; MAX_TOOL_METADATA_BYTES + 1],
        )
        .await
        .unwrap();
        let oversized_scan = store
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(!oversized_scan.complete);
        assert!(oversized_scan.skipped);
        assert_eq!(
            oversized_scan.records.len(),
            3,
            "only the real records list"
        );

        // Blob verification charges the +1 lookahead, not just the expected
        // bytes, and a missing before-image costs nothing.
        assert_eq!(
            change_blob_read_cost(&content_revision(b"")),
            1,
            "an empty content blob still costs its one-byte lookahead"
        );
        assert_eq!(change_blob_read_cost(&ChangeRevision::Missing), 0);
        let mut blob_budget = ChangeBlobBudget {
            used_bytes: CHANGE_SCAN_BLOB_BYTES - 1,
            exhausted: false,
        };
        let change = StoredFileChange {
            path: "x".to_owned(),
            kind: ChangeKind::Modified,
            before: ChangeRevision::Missing,
            after: content_revision(b"after\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
        };
        assert!(
            !store
                .change_blobs_available(
                    session_id,
                    &refs[0],
                    &change,
                    &CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10),
                    &mut blob_budget,
                )
                .await
                .unwrap()
        );
        assert!(blob_budget.exhausted, "the lookahead crosses the budget");

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_resolves_warm_and_cold_and_rejects_a_stale_cursor() {
        let (base, store, session_id) = fixture("diff-warm-cold").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-warm").unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(b"user dirty\n"),
                after: content_revision(b"agent clean\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"user dirty\n".to_vec()),
                after_bytes: Some(b"agent clean\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("diff record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        let change_ref = crate::changes::stored_tool_change_ref(
            &tool_ref,
            &data.file_change(&tool_ref).unwrap().stored(),
        );
        let request = crate::diff::ChangesDiffRequest {
            session_id,
            change_ref: change_ref.clone(),
            context_lines: None,
            cursor: None,
            max_bytes: None,
        };

        // Warm path: the in-memory snapshots answer without any disk read.
        let warm = crate::diff::changes_diff(
            store.clone(),
            Some(Arc::new(data)),
            request.clone(),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(warm.availability, crate::diff::DiffAvailability::Available);
        assert!(!warm.binary);
        assert_eq!(warm.base_version, content_revision(b"user dirty\n"));
        assert_eq!(warm.target_version, content_revision(b"agent clean\n"));
        assert!(
            warm.hunks
                .iter()
                .flat_map(|hunk| &hunk.lines)
                .any(|line| line.kind == crate::diff::DiffLineKind::Removed)
        );

        // Cold path: an unloaded Session still resolves through the bounded
        // metadata scan and reads only this record's snapshots.
        let cold = crate::diff::changes_diff(
            store.clone(),
            None,
            request.clone(),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(cold.availability, crate::diff::DiffAvailability::Available);
        assert_eq!(cold.change_ref, change_ref);

        // A cursor carrying a different diff fingerprint is stale, not a new diff.
        let stale = crate::diff::changes_diff(
            store.clone(),
            None,
            crate::diff::ChangesDiffRequest {
                cursor: Some(crate::diff::DiffCursor {
                    session_id,
                    change_ref: change_ref.clone(),
                    tool_ref: Some(tool_ref.clone()),
                    ops_fingerprint: "a".repeat(64),
                    context_lines: 3,
                    hunk_index: 0,
                    line_index: 0,
                    line_byte_offset: 0,
                }),
                ..request.clone()
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert!(stale.stale);
        assert!(stale.hunks.is_empty());

        // An unknown reference is not a fabricated empty diff.
        let miss = crate::diff::changes_diff(
            store.clone(),
            None,
            crate::diff::ChangesDiffRequest {
                change_ref: format!("tool:{}", "b".repeat(64)),
                ..request.clone()
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await;
        assert!(matches!(miss, Err(AgentError::ToolNotFound)));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_missing_before_is_an_addition_and_empty_content_is_not_binary() {
        let (base, store, session_id) = fixture("diff-missing-before").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-added").unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "created.txt".to_owned(),
                kind: ChangeKind::Added,
                before: crate::changes::ChangeRevision::Missing,
                after: content_revision(b"new\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: None,
                after_bytes: Some(b"new\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("diff record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        let change_ref = crate::changes::stored_tool_change_ref(
            &tool_ref,
            &data.file_change(&tool_ref).unwrap().stored(),
        );
        let result = crate::diff::changes_diff(
            store.clone(),
            None,
            crate::diff::ChangesDiffRequest {
                session_id,
                change_ref,
                context_lines: None,
                cursor: None,
                max_bytes: None,
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(result.base_version, crate::changes::ChangeRevision::Missing);
        assert!(!result.binary);
        assert_eq!(
            result.availability,
            crate::diff::DiffAvailability::Available
        );
        assert!(
            result
                .hunks
                .iter()
                .flat_map(|hunk| &hunk.lines)
                .any(|line| line.kind == crate::diff::DiffLineKind::Added)
        );

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_corrupt_or_missing_snapshots_report_unavailable_not_empty() {
        let (base, store, session_id) = fixture("diff-corrupt").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-corrupt").unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(b"before\n"),
                after: content_revision(b"after\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"before\n".to_vec()),
                after_bytes: Some(b"after\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("diff record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        let change_ref = crate::changes::stored_tool_change_ref(
            &tool_ref,
            &data.file_change(&tool_ref).unwrap().stored(),
        );
        let target = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(&tool_ref))
            .join(TOOL_AFTER_FILE);
        fs::write(&target, b"tampered bytes").await.unwrap();
        let result = crate::diff::changes_diff(
            store.clone(),
            None,
            crate::diff::ChangesDiffRequest {
                session_id,
                change_ref,
                context_lines: None,
                cursor: None,
                max_bytes: None,
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(
            result.availability,
            crate::diff::DiffAvailability::Unavailable
        );
        assert!(result.hunks.is_empty());
        assert!(!result.binary);

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_warm_half_merges_with_its_disk_counterpart_only() {
        let (base, store, session_id) = fixture("diff-half-merge").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-half").unwrap(),
        };
        let before = b"user dirty\n".to_vec();
        let after = b"agent clean\n".to_vec();
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(&before),
                after: content_revision(&after),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(before.clone()),
                after_bytes: Some(after.clone()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("diff record finishes");
        let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        let change_ref = crate::changes::stored_tool_change_ref(
            &tool_ref,
            &data.file_change(&tool_ref).unwrap().stored(),
        );

        // Warm record lost its before bytes but keeps a valid after half; the
        // matching disk record supplies the before side. The result compares
        // the real before, unaugmented by disk, and the request never fabricates
        // an unrelated half.
        let warm = ToolData::new();
        warm.note_requested(&tool_ref, "write");
        warm.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(&before),
                after: content_revision(&after),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: None,
                after_bytes: Some(after.clone()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        let warm = Arc::new(warm);
        let merged = crate::diff::resolve_tool_change(
            &store,
            Some(&warm),
            session_id,
            &change_ref,
            None,
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .expect("the warm record resolves");
        assert_eq!(merged.1.before_bytes.as_deref(), Some(before.as_slice()));
        assert_eq!(merged.1.after_bytes.as_deref(), Some(after.as_slice()));
        assert!(merged.1.details_available());

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_metadata_conflict_keeps_the_valid_warm_side() {
        let (base, store, session_id) = fixture("diff-meta-conflict").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-conflict").unwrap(),
        };
        let disk_before = b"disk before\n".to_vec();
        let disk_after = b"disk after\n".to_vec();
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(&disk_before),
                after: content_revision(&disk_after),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(disk_before.clone()),
                after_bytes: Some(disk_after.clone()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("diff record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();

        // The warm record carries a different revision than disk. Disk must not
        // be merged in: the valid warm sides stay, and the absent side stays
        // unavailable rather than taking a disk half from another revision.
        let warm_before = b"warm before\n".to_vec();
        let warm = ToolData::new();
        warm.note_requested(&tool_ref, "write");
        warm.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(&warm_before),
                after: content_revision(&disk_after),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(warm_before.clone()),
                after_bytes: None,
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        let warm_ref = crate::changes::stored_tool_change_ref(
            &tool_ref,
            &warm.file_change(&tool_ref).unwrap().stored(),
        );
        let warm = Arc::new(warm);
        let resolved = crate::diff::resolve_tool_change(
            &store,
            Some(&warm),
            session_id,
            &warm_ref,
            None,
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .expect("the warm record resolves");
        assert_eq!(
            resolved.1.before_bytes.as_deref(),
            Some(warm_before.as_slice())
        );
        assert!(resolved.1.after_bytes.is_none());
        assert!(!resolved.1.details_available());

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_cursor_hint_must_match_the_resolved_reference() {
        let (base, store, session_id) = fixture("diff-hint").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-hint").unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: "value.txt".to_owned(),
                kind: ChangeKind::Modified,
                before: content_revision(b"before\n"),
                after: content_revision(b"after\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"before\n".to_vec()),
                after_bytes: Some(b"after\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("diff record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        let change_ref = crate::changes::stored_tool_change_ref(
            &tool_ref,
            &data.file_change(&tool_ref).unwrap().stored(),
        );

        // A hint pointing at the right session but the wrong change reference
        // must not select a different change. The metadata scan is still
        // consulted, so the real record is found and verified.
        let wrong = ToolRef {
            session_id,
            loop_id: tool_ref.loop_id,
            request_index: 0,
            tool_call_id: ToolCallId::new("diff-hint-other").unwrap(),
        };
        let resolved = crate::diff::resolve_tool_change(
            &store,
            None,
            session_id,
            &change_ref,
            Some(&wrong),
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .expect("the real change is found despite the wrong hint");
        assert_eq!(resolved.0, tool_ref);
        assert_eq!(resolved.1.path, "value.txt");

        // A forged hint for a different session is rejected without a lookup.
        let other_session = SessionId::new().unwrap();
        assert!(matches!(
            crate::diff::resolve_tool_change(
                &store,
                None,
                session_id,
                &change_ref,
                Some(&ToolRef {
                    session_id: other_session,
                    ..wrong.clone()
                }),
                Instant::now() + Duration::from_secs(10),
                &CancellationToken::new(),
            )
            .await,
            Err(AgentError::InvalidArguments)
        ));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_worker_capacity_is_bounded_and_shutdown_joins() {
        let (base, store, session_id) = fixture("diff-workers").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        // Hold every worker slot with a distinct, gated comparison so capacity
        // cannot drain before the overflow attempt.
        let mut held = Vec::new();
        let mut gates = Vec::new();
        for index in 0..MAX_DIFF_WORKERS {
            let before: Arc<[u8]> = Arc::from(format!("line {index}\n").into_bytes());
            let after: Arc<[u8]> = Arc::from(format!("LINE {index}\n").into_bytes());
            let gate = Arc::new(crate::diff::DiffGate::new());
            crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
            held.push(
                store
                    .spawn_diff_query(
                        session_id,
                        Arc::clone(&before),
                        Arc::clone(&after),
                        3,
                        Instant::now() + Duration::from_secs(10),
                    )
                    .unwrap(),
            );
            gates.push(gate);
        }
        for gate in &gates {
            tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
                .await
                .expect("diff worker did not start");
        }
        assert!(matches!(
            store.spawn_diff_query(
                session_id,
                Arc::from(b"one\ntwo\n".to_vec()),
                Arc::from(b"one\nTWO\n".to_vec()),
                3,
                Instant::now() + Duration::from_secs(10),
            ),
            Err(StoreError::QueryLimit)
        ));
        for gate in &gates {
            gate.release();
        }
        for query in held {
            let _ = query.wait().await;
        }
        store.shutdown_diff_workers().await;

        // After shutdown the worker set refuses new comparisons rather than
        // launching an unowned one.
        assert!(matches!(
            store.spawn_diff_query(
                session_id,
                Arc::from(b"one\ntwo\n".to_vec()),
                Arc::from(b"one\nTWO\n".to_vec()),
                3,
                Instant::now() + Duration::from_secs(10),
            ),
            Err(StoreError::QueryLimit)
        ));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_shutdown_drop_and_concurrent_join_really_stop_the_worker() {
        let (base, store, session_id) = fixture("diff-shutdown-reentrant").await;
        let before: Arc<[u8]> = Arc::from(b"one\n".to_vec());
        let after: Arc<[u8]> = Arc::from(b"ONE\n".to_vec());
        let gate = Arc::new(crate::diff::DiffGate::new());
        crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
        let query = store
            .spawn_diff_query(
                session_id,
                Arc::clone(&before),
                Arc::clone(&after),
                3,
                Instant::now() + Duration::from_secs(30),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
            .await
            .expect("diff worker did not start");
        // Dropping the query owner only cancels the comparison; the Store still
        // owns the handle. The gate is never released, so completion depends on
        // the cancellation really reaching the blocking handler.
        drop(query);
        tokio::time::timeout(Duration::from_secs(10), store.shutdown_diff_workers())
            .await
            .expect("shutdown did not join the cancelled worker");
        assert_eq!(store.registered_diff_workers(), 0);
        let _ = fs::remove_dir_all(base).await;

        // A shutdown future dropped before it joins must not detach the handle.
        // The same owner is still registered and a later (or concurrent)
        // shutdown joins the same worker instead of returning early.
        let (base, store, session_id) = fixture("diff-shutdown-dropped").await;
        let before: Arc<[u8]> = Arc::from(b"two\n".to_vec());
        let after: Arc<[u8]> = Arc::from(b"TWO\n".to_vec());
        let gate = Arc::new(crate::diff::DiffGate::new());
        crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
        let _query = store
            .spawn_diff_query(
                session_id,
                Arc::clone(&before),
                Arc::clone(&after),
                3,
                Instant::now() + Duration::from_secs(30),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
            .await
            .expect("diff worker did not start");
        // Dropping the future before it is ever polled must not detach the
        // handle: the same owner is still registered, so a later (or
        // concurrent) shutdown joins the same worker instead of returning early.
        let dropped = store.shutdown_diff_workers();
        drop(dropped);
        assert_eq!(store.registered_diff_workers(), 1);
        let (first, second) =
            tokio::join!(store.shutdown_diff_workers(), store.shutdown_diff_workers());
        let _ = (first, second);
        assert_eq!(store.registered_diff_workers(), 0);

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn diff_session_shutdown_joins_only_that_session() {
        let (base, store, first) = fixture("diff-session-join-a").await;
        let second = SessionId::new().unwrap();
        let before_a: Arc<[u8]> = Arc::from(b"alpha\n".to_vec());
        let after_a: Arc<[u8]> = Arc::from(b"ALPHA\n".to_vec());
        let before_b: Arc<[u8]> = Arc::from(b"beta\n".to_vec());
        let after_b: Arc<[u8]> = Arc::from(b"BETA\n".to_vec());
        let gate_a = Arc::new(crate::diff::DiffGate::new());
        crate::diff::gate_next_diff(&before_a, &after_a, Arc::clone(&gate_a));
        let gate_b = Arc::new(crate::diff::DiffGate::new());
        crate::diff::gate_next_diff(&before_b, &after_b, Arc::clone(&gate_b));
        let query_a = store
            .spawn_diff_query(
                first,
                Arc::clone(&before_a),
                Arc::clone(&after_a),
                3,
                Instant::now() + Duration::from_secs(30),
            )
            .unwrap();
        let query_b = store
            .spawn_diff_query(
                second,
                Arc::clone(&before_b),
                Arc::clone(&after_b),
                3,
                Instant::now() + Duration::from_secs(30),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), gate_a.wait_started())
            .await
            .expect("first diff worker did not start");
        tokio::time::timeout(Duration::from_secs(10), gate_b.wait_started())
            .await
            .expect("second diff worker did not start");
        // The first Session's close cancels and joins only its own CPU worker;
        // the unrelated Session's comparison stays registered and running.
        tokio::time::timeout(
            Duration::from_secs(10),
            store.shutdown_session_diff_workers(first),
        )
        .await
        .expect("session shutdown did not join its worker");
        assert_eq!(store.registered_diff_workers(), 1);
        drop(query_a);
        drop(query_b);
        store.shutdown_diff_workers().await;
        assert_eq!(store.registered_diff_workers(), 0);

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn valid_temp_directory_does_not_mark_tool_change_scan_incomplete() {
        let (base, store, session_id) = fixture("temp-scan-complete").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-temp-1").unwrap(),
        };
        let valid_hash = tool_ref_hash(&tool_ref);
        let temp_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(format!(".{valid_hash}.tmp-1234-1"));
        fs::create_dir_all(&temp_dir).await.unwrap();

        let scan = store
            .list_tool_changes(
                session_id,
                None,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(scan.complete);
        assert!(!scan.skipped);
        assert!(scan.records.is_empty());
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn complete_identity_and_path_traversal_protection() {
        let (base, store, session_id) = fixture("aux-identity-path").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-1").unwrap(),
        };

        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();

        let hash = tool_ref_hash(&tool_ref);
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Check directory exists strictly under tools/
        let expected_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash);
        assert!(expected_dir.is_dir());

        // Mismatched ToolRef query on different loop or call returns None
        let other_tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-1").unwrap(),
        };
        assert!(
            store
                .read_tool_record(&other_tool_ref)
                .await
                .unwrap()
                .is_none()
        );

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn atomic_publishing_idempotent_overwrite_and_cleanup_on_failure() {
        let (base, store, session_id) = fixture("aux-atomic-idempotent").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-idempotent").unwrap(),
        };

        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "read".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        // First commit succeeds
        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();

        // Second commit of identical record succeeds idempotently
        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();

        // Injected failure cleans up temporary directory without corrupting target
        fail_next_aux_write(session_id);
        assert!(matches!(
            store
                .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
                .await,
            Err(StoreError::Unavailable)
        ));

        // Read still succeeds
        assert!(store.read_tool_record(&tool_ref).await.unwrap().is_some());

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn metadata_and_blob_corruption_handling() {
        let (base, store, session_id) = fixture("aux-corrupt").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-corrupt").unwrap(),
        };

        let stdout_bytes = b"authoritative stdout tail".to_vec();
        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: stdout_bytes.len() as u64,
                    seen: true,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: stdout_bytes.len(),
                    file_sha256: Some(hash_bytes(&stdout_bytes)),
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: Some(stdout_bytes),
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();

        // 1. Missing blob: remove stdout.bin
        let hash = tool_ref_hash(&tool_ref);
        let blob_path = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash)
            .join(TOOL_STDOUT_FILE);
        fs::remove_file(&blob_path).await.unwrap();

        let recovered = store
            .read_tool_record(&tool_ref)
            .await
            .unwrap()
            .expect("metadata should still load");
        let page = recovered
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        // Missing blob must report Unavailable and retain observed_end, not a clean empty EOF
        assert_eq!(page.availability, ToolDataAvailability::Unavailable);
        assert_eq!(page.observed_end, 25);
        assert!(page.data.is_empty());
        assert!(page.truncated);

        // 2. Hash mismatch / corrupt bytes: write wrong content
        fs::write(&blob_path, b"corrupted bytes!").await.unwrap();
        let recovered2 = store
            .read_tool_record(&tool_ref)
            .await
            .unwrap()
            .expect("metadata should still load");
        let page2 = recovered2
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(page2.availability, ToolDataAvailability::Unavailable);
        assert_eq!(page2.observed_end, 25);
        assert!(page2.data.is_empty());

        // 3. Unsupported format version
        let record_path = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash)
            .join(TOOL_RECORD_FILE);
        let mut corrupted_record = snapshot.record.clone();
        corrupted_record.version = 999;
        fs::write(&record_path, serde_json::to_vec(&corrupted_record).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            store.read_tool_record(&tool_ref).await,
            Err(StoreError::UnsupportedFormat)
        ));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn auxiliary_budget_gc_and_scan_limits() {
        let (base, store, session_id) = fixture("aux-gc-limits").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        // Set small limits: session max 2 records / 50 KiB; global max 3 records / 80 KiB
        let store = store.with_aux_limits(AuxLimits {
            session_bytes: 50 * 1024,
            session_records: 2,
            global_bytes: 80 * 1024,
            global_records: 3,
            max_scan_entries: 50,
        });

        let make_snap = |idx: u32, call_name: &'static str| {
            let tool_ref = ToolRef {
                session_id,
                loop_id: LoopId::new().unwrap(),
                request_index: idx,
                tool_call_id: ToolCallId::new(call_name).unwrap(),
            };
            ToolPersistenceSnapshot {
                tool_ref: tool_ref.clone(),
                record: StoredToolRecord {
                    version: TOOL_RECORD_FORMAT_VERSION,
                    tool_ref: tool_ref.clone(),
                    name: "bash".to_owned(),
                    subject: ToolSubject::Other,
                    subject_truncated: false,
                    state: ToolExecutionState::Succeeded,
                    phase: None,
                    started_at: None,
                    finished_at: None,
                    outcome: Some(ToolResultOutcome::Success),
                    input: StoredInputSummary {
                        total_bytes: 0,
                        seen: false,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    result: StoredResultSummary {
                        total_bytes: 0,
                        seen: false,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    stdout: StoredStreamWindow {
                        start_offset: 0,
                        observed_end: 0,
                        seen: false,
                        complete: true,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    stderr: StoredStreamWindow {
                        start_offset: 0,
                        observed_end: 0,
                        seen: false,
                        complete: true,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    command: None,
                    file_change: None,
                },
                input_bytes: None,
                result_bytes: None,
                stdout_bytes: None,
                stderr_bytes: None,
                file_change_before: None,
                file_change_after: None,
            }
        };

        let snap1 = make_snap(0, "call-gc-1");
        store
            .commit_tool_record(&snap1, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let snap2 = make_snap(1, "call-gc-2");
        store
            .commit_tool_record(&snap2, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Both records exist
        assert!(
            store
                .read_tool_record(&snap1.tool_ref)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .read_tool_record(&snap2.tool_ref)
                .await
                .unwrap()
                .is_some()
        );

        // Third record exceeds session_records (2) -> snap1 must be evicted!
        let snap3 = make_snap(2, "call-gc-3");
        store
            .commit_tool_record(&snap3, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        assert!(
            store
                .read_tool_record(&snap1.tool_ref)
                .await
                .unwrap()
                .is_none(),
            "oldest record must be evicted"
        );
        assert!(
            store
                .read_tool_record(&snap2.tool_ref)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .read_tool_record(&snap3.tool_ref)
                .await
                .unwrap()
                .is_some()
        );

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn legacy_session_without_tools_directory() {
        let (base, store, session_id) = fixture("aux-legacy").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-old").unwrap(),
        };

        // No tools directory exists yet; read returns Ok(None), not StoreError::Corrupt
        assert!(store.read_tool_record(&tool_ref).await.unwrap().is_none());

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn scan_limit_hit_in_inner_files_returns_query_limit() {
        let (base, store, session_id) = fixture("aux-inner-limit").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-scan-inner").unwrap(),
        };

        let stdout_bytes = b"testing inner scan limit".to_vec();
        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: stdout_bytes.len() as u64,
                    seen: true,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: stdout_bytes.len(),
                    file_sha256: Some(hash_bytes(&stdout_bytes)),
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: Some(stdout_bytes),
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        // First commit with normal limits
        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        // Second commit with max_scan_entries: 1 -> scanning sessions/tools/inner_files
        // will exceed scan limit inside directory scan!
        let store_restricted = store.with_aux_limits(AuxLimits {
            session_bytes: 16 * 1024 * 1024,
            session_records: 1024,
            global_bytes: 256 * 1024 * 1024,
            global_records: 8192,
            max_scan_entries: 1,
        });

        let tool_ref2 = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-scan-inner-2").unwrap(),
        };
        let mut snap2 = snapshot.clone();
        snap2.tool_ref = tool_ref2.clone();
        snap2.record.tool_ref = tool_ref2;

        let res = store_restricted
            .commit_tool_record(&snap2, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(matches!(res, Err(StoreError::QueryLimit)));

        let _ = fs::remove_dir_all(base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_session_id_dir_is_rejected_and_not_scanned() {
        use std::os::unix::fs::symlink;

        let (base, store, session_id) = fixture("aux-symlink-ses").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        // Create an outside directory
        let outside = base.join("outside_dir");
        fs::create_dir_all(&outside).await.unwrap();

        // Create a symlink named as a valid SessionId pointing to outside_dir
        let fake_ses_id = SessionId::new().unwrap();
        let symlink_path = store.sessions_directory().join(fake_ses_id.to_string());
        symlink(&outside, &symlink_path).unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-sym-ses").unwrap(),
        };
        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        // Budget enforcement scanning encounters the symlink session entry and must fail closed
        let res = store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(matches!(res, Err(StoreError::Corrupt)));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn temp_dir_removal_failure_rejects_aux_commit() {
        let (base, store, session_id) = fixture("aux-temp-fail").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-temp-fail").unwrap(),
        };

        let tools_dir = store.session_directory(session_id).join(AUX_TOOLS_DIR);
        fs::create_dir_all(&tools_dir).await.unwrap();
        let valid_hash = "0".repeat(64);
        let temp_dir = tools_dir.join(format!(".{valid_hash}.tmp-1234-1"));
        fs::create_dir(&temp_dir).await.unwrap();
        fs::write(temp_dir.join(TOOL_RECORD_FILE), b"{}")
            .await
            .unwrap();

        fail_next_remove_temp(session_id);

        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        let res = store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(matches!(res, Err(StoreError::Unavailable)));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn private_temp_directory_with_unknown_files_is_not_deleted_and_rejects_commit() {
        let (base, store, session_id) = fixture("aux-private-temp").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-private-temp").unwrap(),
        };

        let tools_dir = store.session_directory(session_id).join(AUX_TOOLS_DIR);
        fs::create_dir_all(&tools_dir).await.unwrap();

        // 1. Directory with name ".private.tmp-not-owned"
        let private_dir = tools_dir.join(".private.tmp-not-owned");
        fs::create_dir(&private_dir).await.unwrap();
        let user_file = private_dir.join("user_secret.txt");
        fs::write(&user_file, b"secret user content").await.unwrap();
        let nested_dir = private_dir.join("nested");
        fs::create_dir(&nested_dir).await.unwrap();
        fs::write(nested_dir.join("nested.txt"), b"nested content")
            .await
            .unwrap();

        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        // Budget enforcement fails closed because of unknown directory
        let res = store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(matches!(res, Err(StoreError::Corrupt)));

        // Critical safety verification: user file and nested directory are NEVER deleted!
        assert!(user_file.exists(), "user file must not be deleted");
        assert!(nested_dir.exists(), "nested directory must not be deleted");

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn legitimate_crashed_temp_directory_is_converged_and_evicted() {
        let (base, store, session_id) = fixture("aux-crashed-temp").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-crashed-temp").unwrap(),
        };

        let tools_dir = store.session_directory(session_id).join(AUX_TOOLS_DIR);
        fs::create_dir_all(&tools_dir).await.unwrap();

        // Legitimate temp directory format with only allowed aux files
        let valid_hash = "a".repeat(64);
        let crashed_temp_dir = tools_dir.join(format!(".{valid_hash}.tmp-9999-1"));
        fs::create_dir(&crashed_temp_dir).await.unwrap();
        fs::write(crashed_temp_dir.join(TOOL_RECORD_FILE), b"{}")
            .await
            .unwrap();
        fs::write(crashed_temp_dir.join(TOOL_INPUT_FILE), b"crash input")
            .await
            .unwrap();

        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        // Commit succeeds and cleans up the orphaned legitimate temp directory
        let res = store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(res.is_ok());
        assert!(
            !crashed_temp_dir.exists(),
            "orphaned temp dir converged and removed"
        );

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn oversized_metadata_file_bytes_rejected_without_alloc() {
        let (base, store, session_id) = fixture("aux-oversized-meta").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-oversized").unwrap(),
        };

        let hash = tool_ref_hash(&tool_ref);
        let target_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash);
        fs::create_dir_all(&target_dir).await.unwrap();

        // Write a malicious record claiming 100 MiB input file_bytes
        let malicious_record = StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 100 * 1024 * 1024,
                seen: true,
                truncated: false,
                expired: false,
                file_bytes: 100 * 1024 * 1024,
                file_sha256: Some("00".repeat(32)),
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        };

        fs::write(
            target_dir.join(TOOL_RECORD_FILE),
            serde_json::to_vec(&malicious_record).unwrap(),
        )
        .await
        .unwrap();

        // Must reject without allocating 100 MiB
        let res = store.read_tool_record(&tool_ref).await;
        assert!(matches!(res, Err(StoreError::RecordTooLarge)));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn invalid_metadata_ranges_and_hashes_rejected() {
        let (base, store, session_id) = fixture("aux-bad-ranges").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-bad-range").unwrap(),
        };

        let hash = tool_ref_hash(&tool_ref);
        let target_dir = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash);
        fs::create_dir_all(&target_dir).await.unwrap();

        // 1. Range start > end
        let bad_range_record = StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 50,
                observed_end: 10, // start > end
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        };
        fs::write(
            target_dir.join(TOOL_RECORD_FILE),
            serde_json::to_vec(&bad_range_record).unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            store.read_tool_record(&tool_ref).await,
            Err(StoreError::Corrupt)
        ));

        // 2. Retained len mismatch (file_bytes != observed_end - start_offset)
        let mut bad_len_record = bad_range_record.clone();
        bad_len_record.stdout.start_offset = 0;
        bad_len_record.stdout.observed_end = 10;
        bad_len_record.stdout.file_bytes = 20; // mismatch: 20 != 10
        bad_len_record.stdout.file_sha256 = Some("00".repeat(32));
        fs::write(
            target_dir.join(TOOL_RECORD_FILE),
            serde_json::to_vec(&bad_len_record).unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            store.read_tool_record(&tool_ref).await,
            Err(StoreError::Corrupt)
        ));

        // 3. Invalid sha256 format (non-hex)
        let mut bad_hash_record = bad_len_record;
        bad_hash_record.stdout.file_bytes = 10;
        bad_hash_record.stdout.file_sha256 = Some("not-a-valid-hex-hash".to_owned());
        fs::write(
            target_dir.join(TOOL_RECORD_FILE),
            serde_json::to_vec(&bad_hash_record).unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            store.read_tool_record(&tool_ref).await,
            Err(StoreError::Corrupt)
        ));

        let _ = fs::remove_dir_all(base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn single_blob_symlink_only_marks_stream_unavailable() {
        use std::os::unix::fs::symlink;

        let (base, store, session_id) = fixture("aux-blob-symlink").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-blob-symlink").unwrap(),
        };

        let stdout_bytes = b"safe output".to_vec();
        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: stdout_bytes.len() as u64,
                    seen: true,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: stdout_bytes.len(),
                    file_sha256: Some(hash_bytes(&stdout_bytes)),
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: Some(stdout_bytes),
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        // Replace stdout.bin with a symlink to an outside file
        let hash = tool_ref_hash(&tool_ref);
        let blob_path = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash)
            .join(TOOL_STDOUT_FILE);
        fs::remove_file(&blob_path).await.unwrap();
        let outside = base.join("outside_target.bin");
        fs::write(&outside, b"secret").await.unwrap();
        symlink(&outside, &blob_path).unwrap();

        // Reading the record succeeds; only stdout is Unavailable!
        let recovered = store
            .read_tool_record(&tool_ref)
            .await
            .unwrap()
            .expect("record should load");
        let page = recovered
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(page.availability, ToolDataAvailability::Unavailable);

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn idempotent_commit_with_corrupt_existing_fails() {
        let (base, store, session_id) = fixture("aux-idempotent-corrupt").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-idempotent-corrupt").unwrap(),
        };

        let stdout_bytes = b"valid content".to_vec();
        let snapshot = ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: stdout_bytes.len() as u64,
                    seen: true,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: stdout_bytes.len(),
                    file_sha256: Some(hash_bytes(&stdout_bytes)),
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: Some(stdout_bytes),
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        };

        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        // Corrupt stdout.bin
        let hash = tool_ref_hash(&tool_ref);
        let blob_path = store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(&hash)
            .join(TOOL_STDOUT_FILE);
        fs::write(&blob_path, b"corrupted bytes!").await.unwrap();

        // Idempotent retry must not return Ok(()); it must fail because published record is corrupt!
        let res = store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(matches!(res, Err(StoreError::Corrupt)));

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn concurrent_sessions_global_quota() {
        let (base, store, session_id1) = fixture("aux-concurrent-quota").await;
        let session_id2 = SessionId::new().unwrap();

        store
            .create_session(&record(&store, session_id1))
            .await
            .unwrap();
        let mut rec2 = record(&store, session_id2);
        rec2.session_id = session_id2;
        store.create_session(&rec2).await.unwrap();

        // Global limits: max 2 records total across all sessions
        let store = store.with_aux_limits(AuxLimits {
            session_bytes: 16 * 1024 * 1024,
            session_records: 1024,
            global_bytes: 256 * 1024 * 1024,
            global_records: 2,
            max_scan_entries: 65536,
        });

        let make_snap = |ses: SessionId, name: &str| {
            let tool_ref = ToolRef {
                session_id: ses,
                loop_id: LoopId::new().unwrap(),
                request_index: 0,
                tool_call_id: ToolCallId::new(name).unwrap(),
            };
            ToolPersistenceSnapshot {
                tool_ref: tool_ref.clone(),
                record: StoredToolRecord {
                    version: TOOL_RECORD_FORMAT_VERSION,
                    tool_ref: tool_ref.clone(),
                    name: "bash".to_owned(),
                    subject: ToolSubject::Other,
                    subject_truncated: false,
                    state: ToolExecutionState::Succeeded,
                    phase: None,
                    started_at: None,
                    finished_at: None,
                    outcome: Some(ToolResultOutcome::Success),
                    input: StoredInputSummary {
                        total_bytes: 0,
                        seen: false,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    result: StoredResultSummary {
                        total_bytes: 0,
                        seen: false,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    stdout: StoredStreamWindow {
                        start_offset: 0,
                        observed_end: 0,
                        seen: false,
                        complete: true,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    stderr: StoredStreamWindow {
                        start_offset: 0,
                        observed_end: 0,
                        seen: false,
                        complete: true,
                        truncated: false,
                        expired: false,
                        file_bytes: 0,
                        file_sha256: None,
                    },
                    command: None,
                    file_change: None,
                },
                input_bytes: None,
                result_bytes: None,
                stdout_bytes: None,
                stderr_bytes: None,
                file_change_before: None,
                file_change_after: None,
            }
        };

        let snap1 = make_snap(session_id1, "call-concurrent-1");
        let snap2 = make_snap(session_id2, "call-concurrent-2");

        // Concurrently commit from both sessions using tokio::spawn
        let store1 = store.clone();
        let snap1_clone = snap1.clone();
        let h1 = tokio::spawn(async move {
            store1
                .commit_tool_record(&snap1_clone, Instant::now() + Duration::from_secs(5))
                .await
        });

        let store2 = store.clone();
        let snap2_clone = snap2.clone();
        let h2 = tokio::spawn(async move {
            store2
                .commit_tool_record(&snap2_clone, Instant::now() + Duration::from_secs(5))
                .await
        });

        let (r1, r2) = tokio::join!(h1, h2);
        r1.unwrap().unwrap();
        r2.unwrap().unwrap();

        // Both records exist (total = 2)
        assert!(
            store
                .read_tool_record(&snap1.tool_ref)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .read_tool_record(&snap2.tool_ref)
                .await
                .unwrap()
                .is_some()
        );

        // Third commit exceeds global quota (2) -> oldest must be evicted
        let snap3 = make_snap(session_id1, "call-concurrent-3");
        store
            .commit_tool_record(&snap3, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        // At most 2 records survive globally
        let remaining_records = [
            store
                .read_tool_record(&snap1.tool_ref)
                .await
                .unwrap()
                .is_some(),
            store
                .read_tool_record(&snap2.tool_ref)
                .await
                .unwrap()
                .is_some(),
            store
                .read_tool_record(&snap3.tool_ref)
                .await
                .unwrap()
                .is_some(),
        ]
        .iter()
        .filter(|&&exists| exists)
        .count();

        assert_eq!(remaining_records, 2);

        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn command_record_stdout_evicted_by_pressure_snapshot_commit_and_cold_project_consistency()
     {
        let (base, store, session_id) = fixture("aux-cmd-evict").await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();

        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("call-evict-cmd").unwrap(),
        };

        let tool_data = crate::tool_data::ToolData::new();
        tool_data.note_requested(&tool_ref, "bash");
        tool_data.note_invocation(
            &tool_ref,
            &minicore_runtime::tools::ToolInvocation {
                tool_call_id: tool_ref.tool_call_id.clone(),
                tool_name: "bash".parse().unwrap(),
                arguments: serde_json::json!({"command": "echo test"}),
            },
        );
        tool_data.mark_running(&tool_ref);

        // Push stdout and stderr chunks
        let stdout_bytes = b"long stdout output that will be evicted under pressure".to_vec();
        let stderr_bytes = b"stderr retained".to_vec();
        tool_data
            .note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &stdout_bytes)
            .unwrap();
        tool_data
            .note_stream_chunk(&tool_ref, ToolDataStream::Stderr, &stderr_bytes)
            .unwrap();
        tool_data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
        tool_data.note_stream_end(&tool_ref, ToolDataStream::Stderr);

        let cmd = CommandResult {
            status: crate::tool_data::CommandStatus::Exited,
            exit_code: Some(0),
            signal: None,
            termination_confirmed: true,
            stdout_base_offset: 0,
            stdout_observed_end: stdout_bytes.len() as u64,
            stderr_base_offset: 0,
            stderr_observed_end: stderr_bytes.len() as u64,
            output_complete: true,
            output_truncated: false,
        };
        tool_data.note_command(&tool_ref, cmd);
        tool_data.finish_and_snapshot(
            &tool_ref,
            minicore_runtime::tools::ToolResultOutcome::Success,
        );

        // Simulate global/session budget pressure that evicts stdout of this record
        // (In tool_data, an eviction empties bytes and sets start_offset = observed_end)
        let evict_snap = {
            let snap = tool_data.snapshot_for_persistence(&tool_ref).unwrap();
            let mut snap_modified = snap.clone();
            snap_modified.record.stdout.expired = true;
            snap_modified.record.stdout.truncated = true;
            snap_modified.record.stdout.start_offset = snap.record.stdout.observed_end;
            snap_modified.record.stdout.file_bytes = 0;
            snap_modified.record.stdout.file_sha256 = None;
            snap_modified.stdout_bytes = None;
            // Update the command ranges using the live stream overlay principle
            if let Some(mut command) = snap_modified.record.command {
                command.stdout_base_offset = snap_modified.record.stdout.start_offset;
                command.stdout_observed_end = snap_modified.record.stdout.observed_end;
                command.output_truncated = true;
                snap_modified.record.command = Some(command);
            }
            snap_modified
        };

        // Persistence commit with strict metadata validator must succeed
        store
            .commit_tool_record(&evict_snap, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        // Cold project from disk
        let recovered = store
            .read_tool_record(&tool_ref)
            .await
            .unwrap()
            .expect("record must exist");

        // Metadata projection shows Saved
        let read_res = recovered.project_read(&tool_ref, 4096).unwrap();
        assert_eq!(
            read_res.execution.recording,
            crate::tool_data::ToolRecordingState::Saved
        );

        // Evicted stdout projection returns Expired while retaining observed_end
        let stdout_page = recovered
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(stdout_page.availability, ToolDataAvailability::Expired);
        assert_eq!(stdout_page.observed_end, stdout_bytes.len() as u64);
        assert!(stdout_page.truncated);

        // Stderr stream was not evicted and remains Available
        let stderr_page = recovered
            .project_output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stderr,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(stderr_page.availability, ToolDataAvailability::Available);
        assert!(!stderr_page.data.is_empty());

        let _ = fs::remove_dir_all(base).await;
    }
}
