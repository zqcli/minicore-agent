use std::collections::{BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
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
use minicore_runtime::model::{ModelErrorKind, Usage};
use minicore_runtime::tools::ToolResultOutcome;

use crate::changes::{ChangeRevision, StoredFileChange};
use crate::error::StoreError;
use crate::history::normalize_history;
use crate::ids::SessionId;
use crate::models::Models;
use crate::profiles::ApprovalMode;
use crate::tool_data::{
    CommandResult, ToolExecutionState, ToolPersistenceSnapshot, ToolPhase, ToolRecord, ToolRef,
    ToolSubject,
};
use crate::tools::KNOWN_TOOL_NAMES;

mod history;
pub(crate) use history::HistoryLine;
mod time;
mod tool_records;
pub(crate) use time::utc_timestamp;
use time::valid_timestamp;
use tool_records::AuxLedger;
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
const DELETING_PREFIX: &str = ".deleting-";
const MAX_LOOP_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_SESSION_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROFILE_BYTES: usize = 256;
const MAX_WORKSPACE_BYTES: usize = 4_096;
const MAX_TITLE_BYTES: usize = 4_096;
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

#[cfg(test)]
static DELETE_GATES: crate::test_support::Registry<SessionId, Arc<crate::test_support::Gate>> =
    crate::test_support::Registry::new();

#[cfg(test)]
pub(crate) static SESSION_LOAD_GATES: crate::test_support::Registry<
    SessionId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

#[cfg(test)]
pub(crate) static APPEND_FAILURES: crate::test_support::Registry<SessionId, bool> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static RECORD_WRITE_FAILURES: crate::test_support::Registry<SessionId, ()> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static SUMMARY_WRITE_FAILURES: crate::test_support::Registry<SessionId, ()> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static AUX_WRITE_FAILURES: crate::test_support::Registry<SessionId, ()> =
    crate::test_support::Registry::new();
#[cfg(test)]
static READ_CHANGE_BLOBS: crate::test_support::Registry<PathBuf, PathBuf> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static FAIL_REMOVE_TEMP_SESSIONS: crate::test_support::Registry<SessionId, ()> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static AUX_COMMIT_GATES: crate::test_support::Registry<
    SessionId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static SUMMARY_COMMIT_GATES: crate::test_support::Registry<
    SessionId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static SUMMARY_UNKNOWN_WRITES: crate::test_support::Registry<SessionId, ()> =
    crate::test_support::Registry::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SummaryCommit {
    Committed,
    Rejected,
    Unknown,
}

#[derive(Debug)]
pub(crate) enum AppendError {
    NotWritten(StoreError),
    Unknown,
}

impl From<StoreError> for AppendError {
    fn from(error: StoreError) -> Self {
        Self::NotWritten(error)
    }
}

impl AppendError {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::NotWritten(error) => error.kind(),
            Self::Unknown => "unknown",
        }
    }
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

/// Session data read from disk, including an unpublished history index.
pub(crate) struct StoredSession {
    pub(crate) record: SessionRecord,
    pub(crate) history: std::sync::Arc<[HistoryItem]>,
    pub(crate) history_index: Vec<HistoryLine>,
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

/// One bounded scan proves both the full commit gate and a complete-loop
/// prefix to summarize. The prefix never includes bytes from a later record.
pub(crate) struct CompactionAnchors {
    pub(crate) full: HistoryPrefix,
    pub(crate) prefix: Option<HistoryPrefix>,
}

pub(crate) struct HistoryScanLimits {
    pub(crate) max_bytes: u64,
    pub(crate) max_lines: usize,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: CancellationToken,
}

#[derive(Default)]
pub(crate) struct HistoryReadPage {
    pub(crate) captured_end: u64,
    pub(crate) revision: String,
    pub(crate) trailing_incomplete: bool,
    pub(crate) total_items: usize,
    pub(crate) items: Vec<HistoryItem>,
    pub(crate) user_times: Vec<Option<String>>,
    pub(crate) turns: Vec<StoredTurnSummary>,
    pub(crate) turns_truncated: bool,
    pub(crate) covered_usage: crate::read::CoveredUsage,
    /// Present only for cold display reads; never contains canonical tool bodies.
    pub(crate) display_items: Option<Vec<(usize, String)>>,
    pub(crate) summary_anchor_valid: bool,
    pub(crate) scanned_bytes: u64,
    pub(crate) scanned_lines: usize,
}

#[derive(Clone)]
pub(crate) struct StoredTurnSummary {
    pub(crate) item_start: usize,
    pub(crate) item_end: usize,
    pub(crate) loop_id: LoopId,
    pub(crate) outcome: StoredLoopOutcome,
    pub(crate) usage: Usage,
    pub(crate) requests: u32,
    pub(crate) tool_rounds: u64,
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
    pub(crate) tool_rounds: u64,
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

pub(crate) use crate::event::LoopOutcomeView as StoredLoopOutcome;

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

#[derive(Clone)]
pub(crate) struct Store {
    root: PathBuf,
    history_indexes: Arc<Mutex<HashMap<SessionId, Vec<HistoryLine>>>>,
    aux_lock: Arc<tokio::sync::Mutex<Option<AuxLedger>>>,
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
    pub(crate) observed_at: std::collections::HashMap<ToolRef, Option<String>>,
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
            observed_at: std::collections::HashMap::new(),
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
        match path_state(&root).await? {
            PathState::Directory => {}
            PathState::Missing => {
                fs::create_dir_all(&root).await.map_err(unavailable)?;
            }
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                return Err(StoreError::InvalidRoot);
            }
        }
        let sessions = root.join(SESSIONS_DIR);
        match path_state(&sessions).await? {
            PathState::Directory => {}
            PathState::Missing => {
                fs::create_dir_all(&sessions).await.map_err(unavailable)?;
            }
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                return Err(StoreError::InvalidRoot);
            }
        }
        let store = Self {
            root,
            history_indexes: Arc::default(),
            aux_lock: Arc::default(),
            diff_workers: Arc::new(Mutex::new(DiffWorkers::default())),
            #[cfg(test)]
            aux_limits: None,
        };
        store.reclaim_leftovers().await?;
        Ok(store)
    }

    async fn reclaim_leftovers(&self) -> Result<(), StoreError> {
        let sessions = self.sessions_directory();
        let mut entries = fs::read_dir(&sessions).await.map_err(unavailable)?;
        let mut removed = false;
        while let Some(entry) = entries.next_entry().await.map_err(unavailable)? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let path = entry.path();
            if !matches!(path_state(&path).await, Ok(PathState::Directory)) {
                continue;
            }
            if name
                .strip_prefix(DELETING_PREFIX)
                .is_some_and(|id| id.parse::<SessionId>().is_ok())
            {
                match fs::remove_dir_all(&path).await {
                    Ok(()) => removed = true,
                    Err(error) => {
                        unavailable(error);
                    }
                }
            } else if name.parse::<SessionId>().is_ok() {
                if let Err(error) = remove_metadata_temps(&path).await {
                    unavailable(error);
                }
            }
        }
        if removed {
            sync_directory(&sessions).await.map_err(unavailable)?;
        }
        Ok(())
    }

    pub(crate) async fn list_sessions(&self) -> Result<Vec<SessionRecord>, StoreError> {
        self.require_sessions_root().await?;
        let mut directory = fs::read_dir(self.sessions_directory())
            .await
            .map_err(unavailable)?;
        let mut records = Vec::new();
        while let Some(entry) = directory.next_entry().await.map_err(unavailable)? {
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
        match path_state(&directory).await? {
            PathState::Missing => {
                fs::create_dir(&directory).await.map_err(unavailable)?;
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
            match path_state(&history_path).await? {
                PathState::Missing => {
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&history_path)
                        .await
                        .map_err(unavailable)?;
                }
                PathState::RegularFile => {}
                PathState::Directory | PathState::Symlink | PathState::Other => {
                    return Err(StoreError::Corrupt);
                }
            }
            sync_directory(&directory).await.map_err(unavailable)?;
            sync_directory(&self.sessions_directory())
                .await
                .map_err(unavailable)?;
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
        let (history, user_times, history_index) = self.load_history(session_id).await?;
        #[cfg(test)]
        if let Some(gate) = SESSION_LOAD_GATES.take(&session_id) {
            gate.pass().await;
        }
        Ok(StoredSession {
            record,
            history,
            history_index,
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
        match path_state(&path).await? {
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
        if !tests::pass_summary_gate(session_id, deadline).await {
            return Ok(SummaryCommit::Rejected);
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
        if SUMMARY_UNKNOWN_WRITES.hit(&session_id) {
            return Ok(SummaryCommit::Unknown);
        }
        #[cfg(test)]
        if SUMMARY_WRITE_FAILURES.hit(&session_id) {
            return Err(StoreError::Unavailable);
        }
        let path = directory.join(SUMMARY_FILE);
        atomic_write_status(&path, bytes).await
    }

    pub(crate) async fn write_record(&self, record: &SessionRecord) -> Result<(), StoreError> {
        record.validate()?;
        let directory = self.require_session_directory(record.session_id).await?;
        #[cfg(test)]
        if RECORD_WRITE_FAILURES.hit(&record.session_id) {
            return Err(StoreError::Unavailable);
        }
        let path = directory.join(SESSION_RECORD_FILE);
        let bytes = serde_json::to_vec(record).map_err(corrupt)?;
        atomic_write(&path, &bytes).await
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

    pub(crate) async fn delete_session(&self, session_id: SessionId) -> Result<(), StoreError> {
        let mut ledger = self.aux_lock.lock().await;
        let directory = self.require_session_directory(session_id).await?;
        let sessions = self.sessions_directory();
        let deleting = sessions.join(format!("{DELETING_PREFIX}{session_id}"));
        if path_state(&deleting).await? != PathState::Missing {
            return Err(StoreError::Corrupt);
        }
        // Cancellation or an uncertain rename must not leave authoritative old accounting.
        let mut cached = ledger.take();
        fs::rename(directory, &deleting)
            .await
            .map_err(unavailable)?;
        self.unload_history(session_id);
        if let Some(ledger) = cached.as_mut() {
            ledger.remove_session(session_id);
        }
        *ledger = cached;
        sync_directory(&sessions).await.map_err(unavailable)?;
        #[cfg(test)]
        if let Some(gate) = DELETE_GATES.take(&session_id) {
            gate.pass().await;
        }
        if let Err(error) = fs::remove_dir_all(deleting).await {
            unavailable(error);
        }
        Ok(())
    }

    pub(crate) fn unload_history(&self, session_id: SessionId) {
        self.history_indexes.lock().unwrap().remove(&session_id);
    }

    pub(crate) fn install_history_index(&self, session_id: SessionId, index: Vec<HistoryLine>) {
        self.history_indexes
            .lock()
            .unwrap()
            .insert(session_id, index);
    }

    pub(crate) async fn load_record(
        &self,
        session_id: SessionId,
    ) -> Result<SessionRecord, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        Self::read_record(&directory.join(SESSION_RECORD_FILE), session_id).await
    }

    async fn read_record(path: &Path, session_id: SessionId) -> Result<SessionRecord, StoreError> {
        match path_state(path).await? {
            PathState::RegularFile => {}
            PathState::Missing => {
                let directory = path
                    .parent()
                    .expect("session records have a parent directory");
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
        let metadata = fs::metadata(path).await.map_err(unavailable)?;
        if metadata.len() > MAX_SESSION_RECORD_BYTES as u64 {
            return Err(StoreError::Corrupt);
        }
        let file = File::open(path).await.map_err(unavailable)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_SESSION_RECORD_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(unavailable)?;
        if bytes.len() > MAX_SESSION_RECORD_BYTES {
            return Err(StoreError::Corrupt);
        }
        let record: SessionRecord = serde_json::from_slice(&bytes).map_err(corrupt)?;
        record.validate().map_err(corrupt)?;
        if record.session_id != session_id {
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
        match path_state(&self.root).await? {
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
        match path_state(&directory).await? {
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
        match path_state(&self.sessions_directory()).await? {
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
        if matches!(path_state(&path).await?, PathState::RegularFile) {
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

async fn path_state(path: &Path) -> Result<PathState, StoreError> {
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
        Err(error) => Err(unavailable(error)),
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
    if matches!(
        error.kind(),
        io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput
    ) {
        StoreError::Corrupt
    } else {
        unavailable(error)
    }
}

fn unavailable(error: io::Error) -> StoreError {
    tracing::warn!(error_kind = ?error.kind(), "store I/O failed");
    StoreError::Unavailable
}
fn corrupt<E>(_: E) -> StoreError {
    StoreError::Corrupt
}

async fn real_dir_opt(path: &Path) -> Result<bool, StoreError> {
    match path_state(path).await? {
        PathState::Directory => Ok(true),
        PathState::Missing => Ok(false),
        _ => Err(StoreError::Corrupt),
    }
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    match atomic_write_status(path, bytes).await? {
        SummaryCommit::Committed => Ok(()),
        SummaryCommit::Rejected | SummaryCommit::Unknown => Err(StoreError::Unavailable),
    }
}

async fn atomic_write_status(path: &Path, bytes: &[u8]) -> Result<SummaryCommit, StoreError> {
    let parent = path.parent().expect("store files have a parent directory");
    match path_state(parent).await? {
        PathState::Directory => {}
        PathState::Symlink | PathState::RegularFile | PathState::Other => {
            return Err(StoreError::Corrupt);
        }
        PathState::Missing => return Err(StoreError::Unavailable),
    }
    reject_symlink_entries(parent)
        .await
        .map_err(map_io_store_error)?;
    match path_state(path).await? {
        PathState::Missing | PathState::RegularFile => {}
        PathState::Directory | PathState::Symlink | PathState::Other => {
            return Err(StoreError::Corrupt);
        }
    }
    let temp_path = unique_temp_path(path)?;
    if let Err(error) = write_sync_file(&temp_path, bytes).await {
        let _ = fs::remove_file(&temp_path).await;
        return Err(error);
    }
    if let Err(error) = fs::rename(&temp_path, path).await {
        unavailable(error);
        let _ = fs::remove_file(&temp_path).await;
        return Ok(SummaryCommit::Unknown);
    }
    if let Err(error) = sync_directory(parent).await {
        unavailable(error);
        return Ok(SummaryCommit::Unknown);
    }
    Ok(SummaryCommit::Committed)
}

fn unique_temp_path(path: &Path) -> Result<PathBuf, StoreError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("store file names are UTF-8");
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|_| StoreError::Unavailable)?;
    let id = u128::from_le_bytes(random);
    Ok(path.with_file_name(format!(".{name}.tmp-{}-{id:032x}", std::process::id())))
}

async fn write_sync_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(unavailable)?;
    file.write_all(bytes).await.map_err(unavailable)?;
    file.flush().await.map_err(unavailable)?;
    file.sync_all().await.map_err(unavailable)
}

fn valid_temp_suffix(suffix: &str) -> bool {
    let Some((pid, id)) = suffix.split_once('-') else {
        return false;
    };
    !pid.is_empty()
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && !id.is_empty()
        && (id.bytes().all(|byte| byte.is_ascii_digit())
            || (id.len() == 32
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))))
}

async fn remove_metadata_temps(directory: &Path) -> io::Result<()> {
    let mut entries = fs::read_dir(directory).await?;
    let mut removed = false;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if [".session.json.tmp-", ".summary.json.tmp-"]
            .iter()
            .any(|prefix| name.strip_prefix(prefix).is_some_and(valid_temp_suffix))
            && entry.file_type().await?.is_file()
        {
            fs::remove_file(entry.path()).await?;
            removed = true;
        }
    }
    if removed {
        sync_directory(directory).await?;
    }
    Ok(())
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

#[cfg(test)]
mod tests;
