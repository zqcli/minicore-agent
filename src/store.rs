use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;

use minicore_runtime::config::{SessionManifest, Timestamp};
use minicore_runtime::conversation::{ConversationEntry, ConversationSeq};
use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::ids::SessionId;
use minicore_runtime::storage::{
    AppendReceipt, ConversationPage, LogFuture, SessionLog, SessionLogError, SessionLogErrorKind,
};
use minicore_runtime::value::BoundedText;

use crate::error::StoreError;

const SESSIONS_DIR: &str = "sessions";
const SESSION_RECORD_FILE: &str = "session.json";
const MANIFEST_FILE: &str = "manifest.json";
const CONVERSATION_FILE: &str = "conversation.log";
const METADATA_TEMP_SUFFIX: &str = ".tmp";
const MAX_PROFILE_BYTES: usize = 256;
const MAX_TITLE_BYTES: usize = 4_096;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub(crate) session_id: SessionId,
    pub(crate) title: Option<String>,
    pub(crate) profile: String,
    pub(crate) workspace: PathBuf,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

impl SessionRecord {
    fn validate(&self) -> Result<(), StoreError> {
        if !valid_metadata_text(&self.profile, MAX_PROFILE_BYTES, false)
            || self.workspace.as_os_str().is_empty()
            || !valid_timestamp(&self.created_at)
            || !valid_timestamp(&self.updated_at)
            || self
                .title
                .as_deref()
                .is_some_and(|title| !valid_metadata_text(title, MAX_TITLE_BYTES, true))
        {
            return Err(StoreError::InvalidRecord);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogBatch {
    previous_head: ConversationSeq,
    new_head: ConversationSeq,
    entries: Vec<ConversationEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AtomicWriteError {
    Unavailable,
    UnknownOutcome,
}

pub struct Store {
    root: PathBuf,
}

impl Store {
    pub async fn open(root: PathBuf) -> Result<Self, StoreError> {
        if root.as_os_str().is_empty() {
            return Err(StoreError::InvalidRoot);
        }
        match fs::metadata(&root).await {
            Ok(metadata) if !metadata.is_dir() => return Err(StoreError::InvalidRoot),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&root)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
            }
            Err(_) => return Err(StoreError::Unavailable),
        }
        fs::create_dir_all(root.join(SESSIONS_DIR))
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(Self { root })
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionRecord>, StoreError> {
        let mut directory = fs::read_dir(self.sessions_directory())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut records = Vec::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|_| StoreError::Unavailable)?;
            if !file_type.is_dir() {
                return Err(StoreError::Corrupt);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| StoreError::Corrupt)?;
            let session_id = name.parse::<SessionId>().map_err(|_| StoreError::Corrupt)?;
            let record = self.load_record(session_id).await?;
            if record.session_id != session_id {
                return Err(StoreError::Corrupt);
            }
            records.push(record);
        }
        records.sort_by_key(|record| record.session_id);
        Ok(records)
    }

    pub async fn create_session(
        &self,
        record: SessionRecord,
    ) -> Result<LocalSessionLog, StoreError> {
        record.validate()?;
        let directory = self.session_directory(record.session_id);
        match fs::create_dir(&directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StoreError::SessionAlreadyExists);
            }
            Err(_) => return Err(StoreError::Unavailable),
        }
        self.write_record(&record).await?;
        Ok(LocalSessionLog::new(directory))
    }

    pub async fn load_record(&self, session_id: SessionId) -> Result<SessionRecord, StoreError> {
        self.require_session_directory(session_id).await?;
        let path = self.session_directory(session_id).join(SESSION_RECORD_FILE);
        let bytes = match fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::SessionNotFound);
            }
            Err(_) => return Err(StoreError::Unavailable),
        };
        let record =
            serde_json::from_slice::<SessionRecord>(&bytes).map_err(|_| StoreError::Corrupt)?;
        record.validate().map_err(|_| StoreError::Corrupt)?;
        if record.session_id != session_id {
            return Err(StoreError::Corrupt);
        }
        Ok(record)
    }

    pub async fn open_log(&self, session_id: SessionId) -> Result<LocalSessionLog, StoreError> {
        self.load_record(session_id).await?;
        let directory = self.require_session_directory(session_id).await?;
        LocalSessionLog::load(directory)
            .await
            .map_err(StoreError::Log)
    }

    pub async fn touch(&self, session_id: SessionId) -> Result<(), StoreError> {
        let mut record = self.load_record(session_id).await?;
        record.updated_at = utc_timestamp()?;
        self.write_record(&record).await
    }

    pub async fn delete_session(&self, session_id: SessionId) -> Result<(), StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        fs::remove_dir_all(directory).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StoreError::SessionNotFound
            } else {
                StoreError::Unavailable
            }
        })
    }

    fn sessions_directory(&self) -> PathBuf {
        self.root.join(SESSIONS_DIR)
    }

    fn session_directory(&self, session_id: SessionId) -> PathBuf {
        self.sessions_directory().join(session_id.to_string())
    }

    async fn require_session_directory(
        &self,
        session_id: SessionId,
    ) -> Result<PathBuf, StoreError> {
        let directory = self.session_directory(session_id);
        match fs::metadata(&directory).await {
            Ok(metadata) if metadata.is_dir() => Ok(directory),
            Ok(_) => Err(StoreError::Corrupt),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(StoreError::SessionNotFound)
            }
            Err(_) => Err(StoreError::Unavailable),
        }
    }

    async fn write_record(&self, record: &SessionRecord) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(record).map_err(|_| StoreError::Internal)?;
        atomic_write(
            &self
                .session_directory(record.session_id)
                .join(SESSION_RECORD_FILE),
            &bytes,
        )
        .await
        .map_err(map_atomic_store_error)
    }
}

pub struct LocalSessionLog {
    directory: PathBuf,
    manifest: Option<SessionManifest>,
    entries: Vec<ConversationEntry>,
    file: Option<File>,
    head: ConversationSeq,
    initialized: bool,
    closed: bool,
    durability_unknown: bool,
    #[cfg(test)]
    fail_after_write: bool,
}

impl LocalSessionLog {
    fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            manifest: None,
            entries: Vec::new(),
            file: None,
            head: ConversationSeq::ZERO,
            initialized: false,
            closed: false,
            durability_unknown: false,
            #[cfg(test)]
            fail_after_write: false,
        }
    }

    async fn load(directory: PathBuf) -> Result<Self, SessionLogError> {
        let expected_session_id = directory_session_id(&directory)?;
        ensure_log_directory(&directory).await?;
        let manifest = read_manifest(&directory).await?;
        if manifest.session_id != expected_session_id {
            return Err(log_error(SessionLogErrorKind::Corrupt));
        }

        let conversation_path = directory.join(CONVERSATION_FILE);
        let mut bytes = match fs::read(&conversation_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(log_error(SessionLogErrorKind::Corrupt));
            }
            Err(_) => return Err(log_error(SessionLogErrorKind::Unavailable)),
        };
        let complete_len = complete_log_length(&bytes);
        if complete_len != bytes.len() {
            truncate_log(&conversation_path, complete_len).await?;
            bytes.truncate(complete_len);
        }
        let (entries, head) = decode_batches(&bytes)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&conversation_path)
            .await
            .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;
        Ok(Self {
            directory,
            manifest: Some(manifest),
            entries,
            file: Some(file),
            head,
            initialized: true,
            closed: false,
            durability_unknown: false,
            #[cfg(test)]
            fail_after_write: false,
        })
    }

    async fn initialize_inner(
        &mut self,
        manifest: SessionManifest,
    ) -> Result<ConversationSeq, SessionLogError> {
        self.preflight_initialize()?;
        if manifest.validate_structural().is_err() {
            return Err(log_error(SessionLogErrorKind::Internal));
        }
        ensure_log_directory(&self.directory).await?;
        let manifest_path = self.directory.join(MANIFEST_FILE);
        let conversation_path = self.directory.join(CONVERSATION_FILE);
        if path_exists(&manifest_path).await? || path_exists(&conversation_path).await? {
            return Err(log_error(SessionLogErrorKind::AlreadyInitialized));
        }

        let manifest_bytes =
            serde_json::to_vec(&manifest).map_err(|_| log_error(SessionLogErrorKind::Internal))?;
        match atomic_write(&manifest_path, &manifest_bytes).await {
            Ok(()) => {}
            Err(AtomicWriteError::Unavailable) => {
                return Err(log_error(SessionLogErrorKind::Unavailable));
            }
            Err(AtomicWriteError::UnknownOutcome) => return Err(self.mark_unknown()),
        }

        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&conversation_path)
            .await
        {
            Ok(file) => file,
            Err(_) => return Err(self.mark_unknown()),
        };
        if file.flush().await.is_err() || file.sync_data().await.is_err() {
            return Err(self.mark_unknown());
        }
        drop(file);
        let file = match OpenOptions::new()
            .read(true)
            .append(true)
            .open(&conversation_path)
            .await
        {
            Ok(file) => file,
            Err(_) => return Err(self.mark_unknown()),
        };
        self.manifest = Some(manifest);
        self.entries.clear();
        self.file = Some(file);
        self.head = ConversationSeq::ZERO;
        self.initialized = true;
        Ok(self.head)
    }

    async fn append_inner(
        &mut self,
        expected_head: ConversationSeq,
        entries: Vec<ConversationEntry>,
    ) -> Result<AppendReceipt, SessionLogError> {
        self.preflight()?;
        if expected_head != self.head {
            return Err(log_error(SessionLogErrorKind::Conflict));
        }
        if entries.is_empty() {
            return Err(log_error(SessionLogErrorKind::Internal));
        }
        let new_head = validate_append_entries(expected_head, &entries)?;
        let mut bytes = serde_json::to_vec(&LogBatch {
            previous_head: expected_head,
            new_head,
            entries: entries.clone(),
        })
        .map_err(|_| log_error(SessionLogErrorKind::Internal))?;
        bytes.push(b'\n');

        let write_result = match self.file.as_mut() {
            Some(file) => file.write_all(&bytes).await,
            None => return Err(self.mark_unknown()),
        };
        if write_result.is_err() {
            return Err(self.mark_unknown());
        }
        #[cfg(test)]
        if self.fail_after_write {
            self.fail_after_write = false;
            return Err(self.mark_unknown());
        }
        let flush_result = match self.file.as_mut() {
            Some(file) => file.flush().await,
            None => return Err(self.mark_unknown()),
        };
        if flush_result.is_err() {
            return Err(self.mark_unknown());
        }
        let sync_result = match self.file.as_mut() {
            Some(file) => file.sync_data().await,
            None => return Err(self.mark_unknown()),
        };
        if sync_result.is_err() {
            return Err(self.mark_unknown());
        }

        let appended = entries.len();
        self.entries.extend(entries);
        self.head = new_head;
        Ok(AppendReceipt {
            previous_head: expected_head,
            new_head,
            appended,
        })
    }

    async fn read_page_inner(
        &mut self,
        after: Option<ConversationSeq>,
        limit: usize,
    ) -> Result<ConversationPage, SessionLogError> {
        self.preflight()?;
        let start = after.map_or(0, |cursor| {
            self.entries
                .iter()
                .position(|entry| entry.seq() > cursor)
                .unwrap_or(self.entries.len())
        });
        let end = start.saturating_add(limit).min(self.entries.len());
        let entries = self.entries[start..end].to_vec();
        let next_after = if end < self.entries.len() {
            entries.last().map(ConversationEntry::seq)
        } else {
            None
        };
        Ok(ConversationPage {
            entries,
            next_after,
            observed_head: self.head,
        })
    }

    async fn close_inner(&mut self) -> Result<(), SessionLogError> {
        if self.durability_unknown {
            return Err(log_error(SessionLogErrorKind::UnknownOutcome));
        }
        if self.closed {
            return Ok(());
        }
        let flush_result = match self.file.as_mut() {
            Some(file) => file.flush().await,
            None => Ok(()),
        };
        if flush_result.is_err() {
            self.durability_unknown = true;
            self.file.take();
            self.closed = true;
            return Err(log_error(SessionLogErrorKind::UnknownOutcome));
        }
        let sync_result = match self.file.as_mut() {
            Some(file) => file.sync_data().await,
            None => Ok(()),
        };
        if sync_result.is_err() {
            self.durability_unknown = true;
            self.file.take();
            self.closed = true;
            return Err(log_error(SessionLogErrorKind::UnknownOutcome));
        }
        self.file.take();
        self.closed = true;
        Ok(())
    }

    fn preflight_initialize(&self) -> Result<(), SessionLogError> {
        if self.durability_unknown {
            Err(log_error(SessionLogErrorKind::UnknownOutcome))
        } else if self.closed {
            Err(log_error(SessionLogErrorKind::Closed))
        } else if self.initialized {
            Err(log_error(SessionLogErrorKind::AlreadyInitialized))
        } else {
            Ok(())
        }
    }

    fn preflight(&self) -> Result<(), SessionLogError> {
        if self.durability_unknown {
            Err(log_error(SessionLogErrorKind::UnknownOutcome))
        } else if self.closed {
            Err(log_error(SessionLogErrorKind::Closed))
        } else if !self.initialized {
            Err(log_error(SessionLogErrorKind::NotInitialized))
        } else {
            Ok(())
        }
    }

    fn mark_unknown(&mut self) -> SessionLogError {
        self.durability_unknown = true;
        self.file.take();
        log_error(SessionLogErrorKind::UnknownOutcome)
    }

    #[cfg(test)]
    fn inject_unknown_after_write(&mut self) {
        self.fail_after_write = true;
    }
}

impl SessionLog for LocalSessionLog {
    fn initialize<'a>(&'a mut self, manifest: SessionManifest) -> LogFuture<'a, ConversationSeq> {
        Box::pin(async move { self.initialize_inner(manifest).await })
    }

    fn load_manifest<'a>(&'a mut self) -> LogFuture<'a, SessionManifest> {
        Box::pin(async move {
            self.preflight()?;
            self.manifest
                .clone()
                .ok_or_else(|| log_error(SessionLogErrorKind::NotInitialized))
        })
    }

    fn read_page<'a>(
        &'a mut self,
        after: Option<ConversationSeq>,
        limit: usize,
    ) -> LogFuture<'a, ConversationPage> {
        Box::pin(async move { self.read_page_inner(after, limit).await })
    }

    fn append<'a>(
        &'a mut self,
        expected_head: ConversationSeq,
        entries: Vec<ConversationEntry>,
    ) -> LogFuture<'a, AppendReceipt> {
        Box::pin(async move { self.append_inner(expected_head, entries).await })
    }

    fn close<'a>(&'a mut self) -> LogFuture<'a, ()> {
        Box::pin(async move { self.close_inner().await })
    }
}

fn valid_metadata_text(value: &str, maximum: usize, allow_empty: bool) -> bool {
    (allow_empty || !value.is_empty())
        && value.len() <= maximum
        && value.chars().all(|character| !character.is_control())
}

fn valid_timestamp(value: &str) -> bool {
    value.parse::<Timestamp>().is_ok()
}

fn utc_timestamp() -> Result<String, StoreError> {
    Timestamp::now_utc()
        .map(|timestamp| timestamp.as_str().to_owned())
        .map_err(|_| StoreError::Internal)
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    let temp_path = path.with_extension(format!(
        "{}{}",
        path.extension().and_then(OsStr::to_str).unwrap_or("tmp"),
        METADATA_TEMP_SUFFIX
    ));
    let mut file = File::create(&temp_path)
        .await
        .map_err(|_| AtomicWriteError::Unavailable)?;
    if file.write_all(bytes).await.is_err()
        || file.flush().await.is_err()
        || file.sync_all().await.is_err()
    {
        let _ = fs::remove_file(&temp_path).await;
        return Err(AtomicWriteError::UnknownOutcome);
    }
    drop(file);
    if fs::rename(&temp_path, path).await.is_err() {
        let _ = fs::remove_file(&temp_path).await;
        return Err(AtomicWriteError::UnknownOutcome);
    }
    Ok(())
}

fn map_atomic_store_error(error: AtomicWriteError) -> StoreError {
    match error {
        AtomicWriteError::Unavailable => StoreError::Unavailable,
        AtomicWriteError::UnknownOutcome => StoreError::UnknownOutcome,
    }
}

async fn path_exists(path: &Path) -> Result<bool, SessionLogError> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(log_error(SessionLogErrorKind::Unavailable)),
    }
}

async fn ensure_log_directory(directory: &Path) -> Result<(), SessionLogError> {
    match fs::metadata(directory).await {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(log_error(SessionLogErrorKind::Corrupt)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(log_error(SessionLogErrorKind::Unavailable))
        }
        Err(_) => Err(log_error(SessionLogErrorKind::Unavailable)),
    }
}

fn directory_session_id(directory: &Path) -> Result<SessionId, SessionLogError> {
    directory
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| log_error(SessionLogErrorKind::Corrupt))?
        .parse::<SessionId>()
        .map_err(|_| log_error(SessionLogErrorKind::Corrupt))
}

async fn read_manifest(directory: &Path) -> Result<SessionManifest, SessionLogError> {
    let path = directory.join(MANIFEST_FILE);
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(log_error(SessionLogErrorKind::NotInitialized));
        }
        Err(_) => return Err(log_error(SessionLogErrorKind::Unavailable)),
    };
    serde_json::from_slice(&bytes).map_err(|_| log_error(SessionLogErrorKind::Corrupt))
}

fn complete_log_length(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |position| position + 1)
}

async fn truncate_log(path: &Path, length: usize) -> Result<(), SessionLogError> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;
    if file.set_len(length as u64).await.is_err()
        || file.flush().await.is_err()
        || file.sync_data().await.is_err()
    {
        return Err(log_error(SessionLogErrorKind::UnknownOutcome));
    }
    Ok(())
}

fn decode_batches(
    bytes: &[u8],
) -> Result<(Vec<ConversationEntry>, ConversationSeq), SessionLogError> {
    if bytes.is_empty() {
        return Ok((Vec::new(), ConversationSeq::ZERO));
    }
    let payload = bytes
        .strip_suffix(b"\n")
        .ok_or_else(|| log_error(SessionLogErrorKind::Corrupt))?;
    if payload.is_empty() {
        return Err(log_error(SessionLogErrorKind::Corrupt));
    }
    let mut entries = Vec::new();
    let mut head = ConversationSeq::ZERO;
    for line in payload.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            return Err(log_error(SessionLogErrorKind::Corrupt));
        }
        let batch = serde_json::from_slice::<LogBatch>(line)
            .map_err(|_| log_error(SessionLogErrorKind::Corrupt))?;
        validate_batch(head, &batch)?;
        head = batch.new_head;
        entries.extend(batch.entries);
    }
    Ok((entries, head))
}

fn validate_batch(head: ConversationSeq, batch: &LogBatch) -> Result<(), SessionLogError> {
    if batch.entries.is_empty() || batch.previous_head != head {
        return Err(log_error(SessionLogErrorKind::Corrupt));
    }
    let expected_first = head
        .next()
        .ok_or_else(|| log_error(SessionLogErrorKind::Corrupt))?;
    if batch.entries.first().map(ConversationEntry::seq) != Some(expected_first) {
        return Err(log_error(SessionLogErrorKind::Corrupt));
    }
    for pair in batch.entries.windows(2) {
        if Some(pair[1].seq()) != pair[0].seq().next() {
            return Err(log_error(SessionLogErrorKind::Corrupt));
        }
    }
    let last = batch
        .entries
        .last()
        .map(ConversationEntry::seq)
        .ok_or_else(|| log_error(SessionLogErrorKind::Corrupt))?;
    if batch.new_head != last || batch.new_head == head {
        return Err(log_error(SessionLogErrorKind::Corrupt));
    }
    Ok(())
}

fn validate_append_entries(
    expected_head: ConversationSeq,
    entries: &[ConversationEntry],
) -> Result<ConversationSeq, SessionLogError> {
    let expected_first = expected_head
        .next()
        .ok_or_else(|| log_error(SessionLogErrorKind::Internal))?;
    if entries.first().map(ConversationEntry::seq) != Some(expected_first) {
        return Err(log_error(SessionLogErrorKind::Conflict));
    }
    for pair in entries.windows(2) {
        if Some(pair[1].seq()) != pair[0].seq().next() {
            return Err(log_error(SessionLogErrorKind::Conflict));
        }
    }
    entries
        .last()
        .map(ConversationEntry::seq)
        .ok_or_else(|| log_error(SessionLogErrorKind::Internal))
}

fn log_error(kind: SessionLogErrorKind) -> SessionLogError {
    let (code, message, retryable) = match kind {
        SessionLogErrorKind::NotInitialized => (
            DiagnosticCode::InvalidSessionManifest,
            "local session log is not initialized",
            false,
        ),
        SessionLogErrorKind::AlreadyInitialized => (
            DiagnosticCode::InvalidSessionManifest,
            "local session log is already initialized",
            false,
        ),
        SessionLogErrorKind::Conflict => (
            DiagnosticCode::LogConflict,
            "local session log append head conflicts",
            false,
        ),
        SessionLogErrorKind::Corrupt => (
            DiagnosticCode::LogCorrupt,
            "local session log is corrupt",
            false,
        ),
        SessionLogErrorKind::Unavailable => (
            DiagnosticCode::Internal,
            "local session log is unavailable",
            true,
        ),
        SessionLogErrorKind::UnknownOutcome => (
            DiagnosticCode::LogUnknownOutcome,
            "local session log mutation outcome is unknown",
            false,
        ),
        SessionLogErrorKind::Closed => (
            DiagnosticCode::SessionClosed,
            "local session log is closed",
            false,
        ),
        SessionLogErrorKind::Internal => (
            DiagnosticCode::Internal,
            "local session log failed internally",
            false,
        ),
    };
    SessionLogError::new(
        kind,
        DiagnosticSummary::new(
            code,
            DiagnosticCategory::Storage,
            BoundedText::new(message).expect("static diagnostic fits"),
            retryable,
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;

    use minicore_runtime::config::{CompactionConfig, SessionSpec};
    use minicore_runtime::conversation::{
        ConversationEntry, ConversationSeq, TurnExecutionRecord, UserInputRecord, UserMessageEntry,
    };
    use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, SessionLogErrorKind};
    use minicore_runtime::ids::{SessionId, TurnId};
    use minicore_runtime::model::{ModelRef, ReasoningPreference};
    use minicore_runtime::storage::SessionLog;
    use minicore_runtime::value::BoundedText;

    use super::{
        CONVERSATION_FILE, LocalSessionLog, LogBatch, MANIFEST_FILE, SESSION_RECORD_FILE,
        SessionRecord, Store, StoreError,
    };

    fn session_id(value: u8) -> SessionId {
        format!("ses_{value:032x}").parse().unwrap()
    }

    fn turn_id(value: u8) -> TurnId {
        format!("trn_{value:032x}").parse().unwrap()
    }

    fn spec() -> SessionSpec {
        SessionSpec::new(
            "model:v1".parse::<ModelRef>().unwrap(),
            ReasoningPreference::Auto,
            BoundedText::new("system").unwrap(),
            BTreeSet::new(),
            4,
            CompactionConfig::Disabled,
        )
        .unwrap()
    }

    fn manifest(id: SessionId) -> minicore_runtime::SessionManifest {
        minicore_runtime::SessionManifest::new(id, spec()).unwrap()
    }

    fn record(id: SessionId) -> SessionRecord {
        SessionRecord {
            session_id: id,
            title: Some(format!("Session {id}")),
            profile: "coding".to_owned(),
            workspace: "/tmp/project".into(),
            created_at: "2020-01-02T03:04:05.006Z".to_owned(),
            updated_at: "2020-01-02T03:04:05.006Z".to_owned(),
        }
    }

    fn entry(seq: u64, turn: TurnId) -> ConversationEntry {
        ConversationEntry::UserMessage(UserMessageEntry {
            seq: ConversationSeq::new(seq),
            turn_id: turn,
            input: UserInputRecord::new(BoundedText::new("hello").unwrap()).unwrap(),
            execution: TurnExecutionRecord::new(
                "model:v1".parse().unwrap(),
                ReasoningPreference::Auto,
                4,
            )
            .unwrap(),
            created_at: "2020-01-02T03:04:05.006Z".parse().unwrap(),
        })
    }

    async fn test_store(label: &str) -> (Store, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "minicore-agent-store-{label}-{}",
            SessionId::new().unwrap()
        ));
        let store = Store::open(root.clone()).await.unwrap();
        (store, root)
    }

    async fn remove_root(root: &std::path::Path) {
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    async fn initialized_log(
        store: &Store,
        id: SessionId,
    ) -> (LocalSessionLog, std::path::PathBuf) {
        let record = record(id);
        let mut log = store.create_session(record).await.unwrap();
        log.initialize(manifest(id)).await.unwrap();
        let directory = store.root.join("sessions").join(id.to_string());
        (log, directory)
    }

    #[tokio::test]
    async fn store_lifecycle_is_atomic_sorted_and_path_safe() {
        let (store, root) = test_store("lifecycle").await;
        let first = session_id(2);
        let second = session_id(1);
        let first_manifest = manifest(first);
        let mut first_log = store.create_session(record(first)).await.unwrap();
        first_log.initialize(first_manifest.clone()).await.unwrap();
        first_log.close().await.unwrap();
        let mut second_log = store.create_session(record(second)).await.unwrap();
        second_log.initialize(manifest(second)).await.unwrap();
        second_log.close().await.unwrap();

        let directory = root.join("sessions").join(first.to_string());
        assert!(
            tokio::fs::try_exists(directory.join(SESSION_RECORD_FILE))
                .await
                .unwrap()
        );
        assert!(
            tokio::fs::try_exists(directory.join(MANIFEST_FILE))
                .await
                .unwrap()
        );
        assert!(
            tokio::fs::try_exists(directory.join(CONVERSATION_FILE))
                .await
                .unwrap()
        );
        assert!(matches!(
            store.create_session(record(first)).await,
            Err(StoreError::SessionAlreadyExists)
        ));

        let records = store.list_sessions().await.unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.session_id)
                .collect::<Vec<_>>(),
            vec![second, first]
        );
        assert_eq!(store.load_record(first).await.unwrap().session_id, first);
        let old_updated_at = store.load_record(first).await.unwrap().updated_at;
        store.touch(first).await.unwrap();
        assert_ne!(
            store.load_record(first).await.unwrap().updated_at,
            old_updated_at
        );
        let mut directory_entries = tokio::fs::read_dir(&directory).await.unwrap();
        while let Some(entry) = directory_entries.next_entry().await.unwrap() {
            assert!(!entry.file_name().to_string_lossy().contains(".tmp"));
        }

        let mut reopened = store.open_log(first).await.unwrap();
        assert_eq!(reopened.load_manifest().await.unwrap(), first_manifest);
        reopened.close().await.unwrap();
        store.delete_session(second).await.unwrap();
        assert_eq!(store.list_sessions().await.unwrap().len(), 1);
        for missing in [second] {
            assert!(matches!(
                store.load_record(missing).await,
                Err(StoreError::SessionNotFound)
            ));
            assert!(matches!(
                store.open_log(missing).await,
                Err(StoreError::SessionNotFound)
            ));
            assert!(matches!(
                store.touch(missing).await,
                Err(StoreError::SessionNotFound)
            ));
            assert!(matches!(
                store.delete_session(missing).await,
                Err(StoreError::SessionNotFound)
            ));
        }
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn new_log_rejects_operations_until_initialize() {
        let (store, root) = test_store("uninitialized").await;
        let id = session_id(9);
        let mut log = store.create_session(record(id)).await.unwrap();
        assert!(matches!(
            store.open_log(id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::NotInitialized
        ));
        assert_eq!(
            log.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::NotInitialized
        );
        assert_eq!(
            log.read_page(None, 1).await.unwrap_err().kind(),
            SessionLogErrorKind::NotInitialized
        );
        assert_eq!(
            log.append(ConversationSeq::ZERO, vec![entry(1, turn_id(9))])
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::NotInitialized
        );
        log.close().await.unwrap();
        assert_eq!(
            log.initialize(manifest(id)).await.unwrap_err().kind(),
            SessionLogErrorKind::Closed
        );
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn local_log_batches_pages_conflicts_close_and_reload() {
        let (store, root) = test_store("append").await;
        let id = session_id(3);
        let (mut log, directory) = initialized_log(&store, id).await;
        let stored_manifest = log.load_manifest().await.unwrap();
        assert_eq!(stored_manifest.session_id, id);
        assert_eq!(
            log.initialize(stored_manifest).await.unwrap_err().kind(),
            SessionLogErrorKind::AlreadyInitialized
        );

        let turn = turn_id(3);
        let first = entry(1, turn);
        let second = entry(2, turn);
        let third = entry(3, turn);
        assert_eq!(
            log.append(ConversationSeq::ZERO, vec![first.clone()])
                .await
                .unwrap(),
            minicore_runtime::AppendReceipt {
                previous_head: ConversationSeq::ZERO,
                new_head: ConversationSeq::new(1),
                appended: 1,
            }
        );
        assert_eq!(
            log.append(ConversationSeq::new(1), vec![second.clone(), third.clone()])
                .await
                .unwrap(),
            minicore_runtime::AppendReceipt {
                previous_head: ConversationSeq::new(1),
                new_head: ConversationSeq::new(3),
                appended: 2,
            }
        );
        let first_page = log.read_page(None, 1).await.unwrap();
        assert_eq!(first_page.entries, vec![first.clone()]);
        assert_eq!(first_page.next_after, Some(ConversationSeq::new(1)));
        assert_eq!(first_page.observed_head, ConversationSeq::new(3));
        let second_page = log.read_page(first_page.next_after, 10).await.unwrap();
        assert_eq!(second_page.entries, vec![second.clone(), third.clone()]);
        assert_eq!(second_page.next_after, None);
        assert_eq!(
            log.read_page(Some(ConversationSeq::new(3)), 10)
                .await
                .unwrap()
                .entries,
            Vec::<ConversationEntry>::new()
        );
        assert_eq!(
            log.append(ConversationSeq::ZERO, vec![entry(4, turn)])
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Conflict
        );
        assert_eq!(
            log.append(ConversationSeq::new(3), Vec::new())
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Internal
        );
        assert_eq!(
            log.append(ConversationSeq::new(3), vec![entry(5, turn)])
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Conflict
        );

        let bytes = tokio::fs::read(directory.join(CONVERSATION_FILE))
            .await
            .unwrap();
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 2);
        let lines = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty());
        let batches = lines
            .map(|line| serde_json::from_slice::<LogBatch>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(batches[0].entries.len(), 1);
        assert_eq!(batches[1].entries.len(), 2);
        log.close().await.unwrap();
        log.close().await.unwrap();
        assert_eq!(
            log.read_page(None, 1).await.unwrap_err().kind(),
            SessionLogErrorKind::Closed
        );

        let mut reloaded = store.open_log(id).await.unwrap();
        let page = reloaded.read_page(None, 10).await.unwrap();
        assert_eq!(page.entries, vec![first, second, third]);
        reloaded.close().await.unwrap();
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn load_truncates_only_final_partial_tail() {
        let (store, root) = test_store("tail").await;
        let id = session_id(4);
        let (mut log, directory) = initialized_log(&store, id).await;
        log.append(ConversationSeq::ZERO, vec![entry(1, turn_id(4))])
            .await
            .unwrap();
        log.close().await.unwrap();
        let path = directory.join(CONVERSATION_FILE);
        let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
        file.write_all(b"{\"partial\":").await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        let mut reloaded = store.open_log(id).await.unwrap();
        assert_eq!(reloaded.read_page(None, 10).await.unwrap().entries.len(), 1);
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert!(bytes.ends_with(b"\n"));
        assert!(!bytes.ends_with(b"{\"partial\":"));
        reloaded.close().await.unwrap();
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn middle_corruption_and_invalid_batch_heads_are_rejected() {
        let (store, root) = test_store("corrupt").await;
        let id = session_id(5);
        let (mut log, directory) = initialized_log(&store, id).await;
        let turn = turn_id(5);
        log.append(ConversationSeq::ZERO, vec![entry(1, turn)])
            .await
            .unwrap();
        log.append(ConversationSeq::new(1), vec![entry(2, turn)])
            .await
            .unwrap();
        log.close().await.unwrap();
        let path = directory.join(CONVERSATION_FILE);
        let bytes = tokio::fs::read(&path).await.unwrap();
        let lines = bytes
            .split_inclusive(|byte| *byte == b'\n')
            .collect::<Vec<_>>();
        let mut corrupted = lines[0].to_vec();
        corrupted.extend_from_slice(b"not-json\n");
        corrupted.extend_from_slice(lines[1]);
        tokio::fs::write(&path, corrupted).await.unwrap();
        assert!(matches!(
            store.open_log(id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
        ));

        let id = session_id(6);
        let (mut log, directory) = initialized_log(&store, id).await;
        log.close().await.unwrap();
        let bad_batch = LogBatch {
            previous_head: ConversationSeq::ZERO,
            new_head: ConversationSeq::new(2),
            entries: vec![entry(1, turn_id(6))],
        };
        let bad_bytes = format!("{}\n", serde_json::to_string(&bad_batch).unwrap());
        tokio::fs::write(directory.join(CONVERSATION_FILE), bad_bytes)
            .await
            .unwrap();
        assert!(matches!(
            store.open_log(id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
        ));
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn unknown_append_outcome_locks_the_log_object() {
        let (store, root) = test_store("unknown").await;
        let id = session_id(7);
        let (mut log, _) = initialized_log(&store, id).await;
        log.inject_unknown_after_write();
        assert_eq!(
            log.append(ConversationSeq::ZERO, vec![entry(1, turn_id(7))])
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            log.append(ConversationSeq::ZERO, vec![entry(1, turn_id(7))])
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            log.read_page(None, 10).await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            log.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            log.close().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn invalid_directory_and_metadata_are_not_silently_ignored() {
        let (store, root) = test_store("invalid").await;
        let invalid_directory = root.join("sessions").join("not-a-session-id");
        tokio::fs::create_dir_all(&invalid_directory).await.unwrap();
        assert!(matches!(
            store.list_sessions().await,
            Err(StoreError::Corrupt)
        ));
        tokio::fs::remove_dir_all(&invalid_directory).await.unwrap();

        let id = session_id(8);
        let mut log = store.create_session(record(id)).await.unwrap();
        log.initialize(manifest(id)).await.unwrap();
        log.close().await.unwrap();
        let metadata_path = root
            .join("sessions")
            .join(id.to_string())
            .join(SESSION_RECORD_FILE);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
        metadata
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), json!(true));
        tokio::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            store.load_record(id).await,
            Err(StoreError::Corrupt)
        ));
        remove_root(&root).await;
    }

    #[test]
    fn log_error_mapping_preserves_runtime_diagnostics() {
        let expected = [
            (
                SessionLogErrorKind::NotInitialized,
                DiagnosticCode::InvalidSessionManifest,
                false,
            ),
            (
                SessionLogErrorKind::AlreadyInitialized,
                DiagnosticCode::InvalidSessionManifest,
                false,
            ),
            (
                SessionLogErrorKind::Conflict,
                DiagnosticCode::LogConflict,
                false,
            ),
            (
                SessionLogErrorKind::Corrupt,
                DiagnosticCode::LogCorrupt,
                false,
            ),
            (
                SessionLogErrorKind::Unavailable,
                DiagnosticCode::Internal,
                true,
            ),
            (
                SessionLogErrorKind::UnknownOutcome,
                DiagnosticCode::LogUnknownOutcome,
                false,
            ),
            (
                SessionLogErrorKind::Closed,
                DiagnosticCode::SessionClosed,
                false,
            ),
            (
                SessionLogErrorKind::Internal,
                DiagnosticCode::Internal,
                false,
            ),
        ];
        for (kind, code, retryable) in expected {
            let error = super::log_error(kind);
            assert_eq!(error.kind(), kind);
            assert_eq!(error.diagnostic().code, code);
            assert_eq!(error.diagnostic().category, DiagnosticCategory::Storage);
            assert_eq!(error.diagnostic().retryable, retryable);
            assert!(!error.to_string().contains("/"));
        }
    }
}
