use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

use minicore_runtime::LoopId;
use minicore_runtime::execution::ConfigRevision;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{ModelError, ModelErrorKind, RetryHint, Usage};

use crate::error::StoreError;
use crate::history::sanitize_history;
use crate::ids::SessionId;
use crate::models::Models;
use crate::profiles::ApprovalMode;
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

static NEXT_TEMP_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[cfg(test)]
static APPEND_FAILURES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
static RECORD_WRITE_FAILURES: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();

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

pub(crate) struct HistoryPrefix {
    pub(crate) prefix_bytes: u64,
    pub(crate) covered_loop_count: u64,
    pub(crate) covered_item_count: u64,
    pub(crate) last_loop_id: Option<LoopId>,
    pub(crate) sha256: String,
}

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

    fn normalized_user_times(&self) -> Option<Vec<Option<String>>> {
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
}

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
        Ok(Self { root })
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
        return Err(StoreError::Unavailable);
    }
    Ok(())
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
mod tests {
    use minicore_runtime::ToolCallId;
    use minicore_runtime::execution::ConfigRevision;
    use minicore_runtime::history::{AssistantHistory, UserHistory};
    use minicore_runtime::model::{
        AssistantPart, ModelFinishReason, ModelRef, ReasoningPreference, ToolCall, Usage,
    };
    use serde_json::json;

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
}
