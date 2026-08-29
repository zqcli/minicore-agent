use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use tokio::sync::Semaphore;

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

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
struct DirectorySyncFailure {
    path: PathBuf,
    remaining_successes: usize,
}

#[cfg(test)]
static DIRECTORY_SYNC_FAILURES: OnceLock<Mutex<Vec<DirectorySyncFailure>>> = OnceLock::new();

#[cfg(test)]
static ATOMIC_BEFORE_RENAME_FAILURES: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();

#[cfg(test)]
static DELETE_FAILURES: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();

#[cfg(test)]
static CLEANUP_FAILURES: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();

#[derive(Clone, Copy)]
enum TouchFailure {
    Unavailable,
    UnknownOutcome,
}

#[cfg(test)]
static TOUCH_FAILURES: OnceLock<Mutex<Vec<(SessionId, TouchFailure)>>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct TouchGate {
    session_id: SessionId,
    pub(crate) started: Arc<Semaphore>,
    pub(crate) release: Arc<Semaphore>,
    pub(crate) finished: Arc<Semaphore>,
}

#[cfg(test)]
impl TouchGate {
    pub(crate) fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            finished: Arc::new(Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
static TOUCH_GATES: OnceLock<Mutex<Vec<Arc<TouchGate>>>> = OnceLock::new();

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
    Corrupt,
    UnknownOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PathState {
    Missing,
    Directory,
    RegularFile,
    Symlink,
    Other,
}

#[derive(Debug)]
enum InitializationState {
    Missing,
    EmptyLog,
    Initialized {
        manifest: SessionManifest,
        log_bytes: Vec<u8>,
    },
    Corrupt,
}

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    pub async fn open(root: PathBuf) -> Result<Self, StoreError> {
        if root.as_os_str().is_empty() {
            return Err(StoreError::InvalidRoot);
        }
        match path_state(&root)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => {}
            PathState::Missing => {
                fs::create_dir_all(&root)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
                if sync_directory(&parent_directory(&root)).await.is_err() {
                    return Err(StoreError::Unavailable);
                }
                if sync_directory(&root).await.is_err() {
                    return Err(StoreError::Unavailable);
                }
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
                fs::create_dir(&sessions)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
                if sync_directory(&root).await.is_err() {
                    return Err(StoreError::Unavailable);
                }
                if sync_directory(&sessions).await.is_err() {
                    return Err(StoreError::Unavailable);
                }
            }
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                return Err(StoreError::InvalidRoot);
            }
        }
        let store = Self { root };
        store.require_sessions_directory().await?;
        Ok(store)
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionRecord>, StoreError> {
        self.require_sessions_directory().await?;
        let mut directory = fs::read_dir(self.sessions_directory())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut records = Vec::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            if path_state(&entry.path())
                .await
                .map_err(|_| StoreError::Unavailable)?
                != PathState::Directory
            {
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
        self.require_sessions_directory().await?;
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
        if sync_directory(&self.sessions_directory()).await.is_err() {
            let primary = StoreError::Unavailable;
            return Err(reconcile_cleanup(
                primary,
                remove_empty_session_directory(&directory, &self.sessions_directory()).await,
            ));
        }
        if let Err(primary) = self.write_record(&record).await {
            if matches!(&primary, StoreError::UnknownOutcome) {
                return Err(primary);
            }
            return Err(reconcile_cleanup(
                primary,
                remove_empty_session_directory(&directory, &self.sessions_directory()).await,
            ));
        }
        Ok(LocalSessionLog::new(directory))
    }

    pub async fn load_record(&self, session_id: SessionId) -> Result<SessionRecord, StoreError> {
        self.require_session_directory(session_id).await?;
        let path = self.session_directory(session_id).join(SESSION_RECORD_FILE);
        let bytes = match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::RegularFile => fs::read(path).await.map_err(|_| StoreError::Unavailable)?,
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
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
        let updated_at = utc_timestamp()?;
        self.touch_at(session_id, updated_at).await
    }

    pub(crate) async fn touch_at(
        &self,
        session_id: SessionId,
        updated_at: String,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        let touch_failure = take_touch_failure(session_id);
        #[cfg(not(test))]
        let touch_failure: Option<TouchFailure> = None;
        #[cfg(test)]
        let touch_gate = take_touch_gate(session_id);
        #[cfg(test)]
        if let Some(gate) = &touch_gate {
            gate.started.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        let result = if let Some(failure) = touch_failure {
            Err(match failure {
                TouchFailure::Unavailable => StoreError::Unavailable,
                TouchFailure::UnknownOutcome => StoreError::UnknownOutcome,
            })
        } else {
            async {
                let mut record = self.load_record(session_id).await?;
                record.updated_at = updated_at;
                self.write_record(&record).await
            }
            .await
        };
        #[cfg(test)]
        if let Some(gate) = touch_gate {
            gate.finished.add_permits(1);
        }
        result
    }

    pub async fn delete_session(&self, session_id: SessionId) -> Result<(), StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        #[cfg(test)]
        if should_fail_delete_after_partial(&directory) {
            let _ = fs::remove_file(directory.join(SESSION_RECORD_FILE)).await;
            return Err(StoreError::UnknownOutcome);
        }
        fs::remove_dir_all(directory)
            .await
            .map_err(|_| StoreError::UnknownOutcome)?;
        if sync_directory(&self.sessions_directory()).await.is_err() {
            return Err(StoreError::UnknownOutcome);
        }
        Ok(())
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
        match path_state(&directory)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => {
                reject_symlink_entries(&directory)
                    .await
                    .map_err(map_store_directory_error)?;
                Ok(directory)
            }
            PathState::Missing => Err(StoreError::SessionNotFound),
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                Err(StoreError::Corrupt)
            }
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

    async fn require_sessions_directory(&self) -> Result<(), StoreError> {
        match path_state(&self.sessions_directory())
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory => reject_symlink_entries(&self.sessions_directory())
                .await
                .map_err(map_store_directory_error),
            PathState::Missing => Err(StoreError::Unavailable),
            PathState::Symlink | PathState::RegularFile | PathState::Other => {
                Err(StoreError::Corrupt)
            }
        }
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
    #[cfg(test)]
    fail_close_flush: bool,
    #[cfg(test)]
    fail_close_sync: bool,
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
            #[cfg(test)]
            fail_close_flush: false,
            #[cfg(test)]
            fail_close_sync: false,
        }
    }

    async fn load(directory: PathBuf) -> Result<Self, SessionLogError> {
        let expected_session_id = directory_session_id(&directory)?;
        ensure_log_directory(&directory).await?;
        let conversation_path = directory.join(CONVERSATION_FILE);
        let (manifest, mut bytes) = match initialization_state(&directory, expected_session_id)
            .await?
        {
            InitializationState::Missing | InitializationState::EmptyLog => {
                return Err(log_error(SessionLogErrorKind::NotInitialized));
            }
            InitializationState::Initialized {
                manifest,
                log_bytes,
            } => (manifest, log_bytes),
            InitializationState::Corrupt => return Err(log_error(SessionLogErrorKind::Corrupt)),
        };
        // v0.1 has no log-size limit; loading a very large log is a known
        // memory/latency limitation of this in-memory paging implementation.
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
            #[cfg(test)]
            fail_close_flush: false,
            #[cfg(test)]
            fail_close_sync: false,
        })
    }

    async fn initialize_inner(
        &mut self,
        manifest: SessionManifest,
    ) -> Result<ConversationSeq, SessionLogError> {
        self.preflight_initialize()?;
        let expected_session_id = directory_session_id(&self.directory)?;
        if manifest.validate_structural().is_err() || manifest.session_id != expected_session_id {
            return Err(log_error(SessionLogErrorKind::Corrupt));
        }
        ensure_log_directory(&self.directory).await?;
        let manifest_path = self.directory.join(MANIFEST_FILE);
        let conversation_path = self.directory.join(CONVERSATION_FILE);
        match initialization_state(&self.directory, expected_session_id).await? {
            InitializationState::Initialized { .. } => {
                return Err(log_error(SessionLogErrorKind::AlreadyInitialized));
            }
            InitializationState::Corrupt => {
                return Err(log_error(SessionLogErrorKind::Corrupt));
            }
            InitializationState::Missing | InitializationState::EmptyLog => {}
        }

        let mut file = match path_state(&conversation_path)
            .await
            .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?
        {
            PathState::Missing => OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&conversation_path)
                .await
                .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?,
            PathState::RegularFile => OpenOptions::new()
                .read(true)
                .write(true)
                .open(&conversation_path)
                .await
                .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?,
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(log_error(SessionLogErrorKind::Corrupt));
            }
        };
        if file
            .metadata()
            .await
            .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?
            .len()
            != 0
        {
            return Err(log_error(SessionLogErrorKind::Corrupt));
        }
        if file.flush().await.is_err() || file.sync_data().await.is_err() {
            return Err(log_error(SessionLogErrorKind::Unavailable));
        }
        drop(file);
        if sync_directory(&self.directory).await.is_err() {
            return Err(log_error(SessionLogErrorKind::Unavailable));
        }

        let manifest_bytes =
            serde_json::to_vec(&manifest).map_err(|_| log_error(SessionLogErrorKind::Internal))?;
        match atomic_write(&manifest_path, &manifest_bytes).await {
            Ok(()) => {}
            Err(AtomicWriteError::Unavailable) => {
                return Err(log_error(SessionLogErrorKind::Unavailable));
            }
            Err(AtomicWriteError::Corrupt) => {
                return Err(log_error(SessionLogErrorKind::Corrupt));
            }
            Err(AtomicWriteError::UnknownOutcome) => return Err(self.mark_unknown()),
        }

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
        #[cfg(test)]
        if self.fail_close_flush {
            self.fail_close_flush = false;
            self.durability_unknown = true;
            self.file.take();
            self.closed = true;
            return Err(log_error(SessionLogErrorKind::UnknownOutcome));
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
        #[cfg(test)]
        if self.fail_close_sync {
            self.fail_close_sync = false;
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

    async fn load_manifest_inner(&mut self) -> Result<SessionManifest, SessionLogError> {
        if self.durability_unknown {
            return Err(log_error(SessionLogErrorKind::UnknownOutcome));
        }
        if self.closed {
            return Err(log_error(SessionLogErrorKind::Closed));
        }
        if self.initialized {
            return self
                .manifest
                .clone()
                .ok_or_else(|| log_error(SessionLogErrorKind::Internal));
        }
        let loaded = match Self::load(self.directory.clone()).await {
            Ok(loaded) => loaded,
            Err(error) => {
                if error.kind() == SessionLogErrorKind::UnknownOutcome {
                    self.durability_unknown = true;
                    self.file.take();
                }
                return Err(error);
            }
        };
        let manifest = loaded
            .manifest
            .clone()
            .ok_or_else(|| log_error(SessionLogErrorKind::Internal))?;
        *self = loaded;
        Ok(manifest)
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

    #[cfg(test)]
    fn inject_initialize_before_marker_sync(&self) {
        fail_next_directory_sync(&self.directory);
    }

    #[cfg(test)]
    fn inject_initialize_after_marker_sync(&self) {
        fail_directory_sync_after(&self.directory, 1);
    }

    #[cfg(test)]
    fn inject_initialize_before_marker_rename(&self) {
        fail_next_atomic_write_before_rename(&self.directory.join(MANIFEST_FILE));
    }

    #[cfg(test)]
    fn inject_close_flush_failure(&mut self) {
        self.fail_close_flush = true;
    }

    #[cfg(test)]
    fn inject_close_sync_failure(&mut self) {
        self.fail_close_sync = true;
    }
}

impl SessionLog for LocalSessionLog {
    fn initialize<'a>(&'a mut self, manifest: SessionManifest) -> LogFuture<'a, ConversationSeq> {
        Box::pin(async move { self.initialize_inner(manifest).await })
    }

    fn load_manifest<'a>(&'a mut self) -> LogFuture<'a, SessionManifest> {
        Box::pin(async move { self.load_manifest_inner().await })
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
    let parent = parent_directory(path);
    match path_state(&parent).await {
        Ok(PathState::Directory) => {}
        Ok(PathState::Symlink | PathState::RegularFile | PathState::Other) => {
            return Err(AtomicWriteError::Corrupt);
        }
        Ok(PathState::Missing) | Err(_) => return Err(AtomicWriteError::Unavailable),
    }
    reject_symlink_entries(&parent).await.map_err(|error| {
        if error.kind() == io::ErrorKind::InvalidData {
            AtomicWriteError::Corrupt
        } else {
            AtomicWriteError::Unavailable
        }
    })?;
    match path_state(path).await {
        Ok(PathState::Missing | PathState::RegularFile) => {}
        Ok(PathState::Directory | PathState::Symlink | PathState::Other) => {
            return Err(AtomicWriteError::Corrupt);
        }
        Err(_) => return Err(AtomicWriteError::Unavailable),
    }

    let temp_path = unique_temp_path(path);
    match path_state(&temp_path).await {
        Ok(PathState::Missing) => {}
        Ok(PathState::Symlink | PathState::Directory | PathState::Other) => {
            return Err(AtomicWriteError::Corrupt);
        }
        Ok(PathState::RegularFile) => return Err(AtomicWriteError::Unavailable),
        Err(_) => return Err(AtomicWriteError::Unavailable),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await
        .map_err(|_| AtomicWriteError::Unavailable)?;
    if file.write_all(bytes).await.is_err()
        || file.flush().await.is_err()
        || file.sync_all().await.is_err()
    {
        let _ = fs::remove_file(&temp_path).await;
        return Err(AtomicWriteError::Unavailable);
    }
    drop(file);
    #[cfg(test)]
    if should_fail_atomic_write_before_rename(path) {
        let _ = fs::remove_file(&temp_path).await;
        return Err(AtomicWriteError::Unavailable);
    }
    if fs::rename(&temp_path, path).await.is_err() {
        let _ = fs::remove_file(&temp_path).await;
        return Err(AtomicWriteError::Unavailable);
    }
    if sync_directory(&parent).await.is_err() {
        return Err(AtomicWriteError::UnknownOutcome);
    }
    Ok(())
}

fn map_atomic_store_error(error: AtomicWriteError) -> StoreError {
    match error {
        AtomicWriteError::Unavailable => StoreError::Unavailable,
        AtomicWriteError::Corrupt => StoreError::Corrupt,
        AtomicWriteError::UnknownOutcome => StoreError::UnknownOutcome,
    }
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("metadata");
    let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    parent_directory(path).join(format!(
        ".{name}{METADATA_TEMP_SUFFIX}-{}-{id}",
        std::process::id()
    ))
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
        if fs::symlink_metadata(entry.path())
            .await?
            .file_type()
            .is_symlink()
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "symlink entry"));
        }
    }
    Ok(())
}

fn map_store_directory_error(error: io::Error) -> StoreError {
    if error.kind() == io::ErrorKind::InvalidData {
        StoreError::Corrupt
    } else {
        StoreError::Unavailable
    }
}

fn map_log_directory_error(error: io::Error) -> SessionLogError {
    if error.kind() == io::ErrorKind::InvalidData {
        log_error(SessionLogErrorKind::Corrupt)
    } else {
        log_error(SessionLogErrorKind::Unavailable)
    }
}

fn reconcile_cleanup(primary: StoreError, cleanup: Result<(), StoreError>) -> StoreError {
    match cleanup {
        Ok(()) => primary,
        Err(StoreError::UnknownOutcome) => StoreError::UnknownOutcome,
        Err(cleanup) => StoreError::CleanupFailed {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        },
    }
}

fn parent_directory(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

async fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if should_fail_directory_sync(path) {
        return Err(io::Error::other("injected directory sync failure"));
    }

    #[cfg(unix)]
    {
        let directory = File::open(path).await?;
        directory.sync_all().await
    }
    #[cfg(not(unix))]
    {
        // Windows has no portable directory fsync API; file contents are still
        // synced, but directory-entry durability cannot be promised there.
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
fn should_fail_directory_sync(path: &Path) -> bool {
    let mut failures = DIRECTORY_SYNC_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    let Some(position) = failures.iter().position(|failure| failure.path == path) else {
        return false;
    };
    if failures[position].remaining_successes == 0 {
        failures.remove(position);
        true
    } else {
        failures[position].remaining_successes -= 1;
        false
    }
}

#[cfg(test)]
fn should_fail_atomic_write_before_rename(path: &Path) -> bool {
    let mut failures = ATOMIC_BEFORE_RENAME_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| candidate == path)
        .map(|position| {
            failures.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
fn fail_next_directory_sync(path: &Path) {
    fail_directory_sync_after(path, 0);
}

#[cfg(test)]
fn fail_directory_sync_after(path: &Path, remaining_successes: usize) {
    DIRECTORY_SYNC_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(DirectorySyncFailure {
            path: path.to_path_buf(),
            remaining_successes,
        });
}

#[cfg(test)]
fn fail_next_atomic_write_before_rename(path: &Path) {
    ATOMIC_BEFORE_RENAME_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(path.to_path_buf());
}

#[cfg(test)]
fn fail_next_delete_after_partial(path: &Path) {
    DELETE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(path.to_path_buf());
}

#[cfg(test)]
fn should_fail_delete_after_partial(path: &Path) -> bool {
    let mut failures = DELETE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| candidate == path)
        .map(|position| {
            failures.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
fn fail_next_cleanup(path: &Path) {
    CLEANUP_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(path.to_path_buf());
}

#[cfg(test)]
pub(crate) fn fail_next_touch_unavailable(session_id: SessionId) {
    TOUCH_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, TouchFailure::Unavailable));
}

#[cfg(test)]
pub(crate) fn fail_next_touch_unknown_outcome(session_id: SessionId) {
    TOUCH_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, TouchFailure::UnknownOutcome));
}

#[cfg(test)]
fn take_touch_failure(session_id: SessionId) -> Option<TouchFailure> {
    let mut failures = TOUCH_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|(candidate, _)| *candidate == session_id)
        .map(|position| failures.remove(position).1)
}

#[cfg(test)]
pub(crate) fn block_next_touch(gate: Arc<TouchGate>) {
    TOUCH_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(gate);
}

#[cfg(test)]
fn take_touch_gate(session_id: SessionId) -> Option<Arc<TouchGate>> {
    let mut gates = TOUCH_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|gate| gate.session_id == session_id)
        .map(|position| gates.remove(position))
}

#[cfg(test)]
fn should_fail_cleanup(path: &Path) -> bool {
    let mut failures = CLEANUP_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| candidate == path)
        .map(|position| {
            failures.remove(position);
            true
        })
        .unwrap_or(false)
}

async fn ensure_log_directory(directory: &Path) -> Result<(), SessionLogError> {
    match path_state(directory).await {
        Ok(PathState::Directory) => reject_symlink_entries(directory)
            .await
            .map_err(map_log_directory_error),
        Ok(PathState::Missing) | Err(_) => Err(log_error(SessionLogErrorKind::Unavailable)),
        Ok(PathState::Symlink | PathState::RegularFile | PathState::Other) => {
            Err(log_error(SessionLogErrorKind::Corrupt))
        }
    }
}

async fn remove_empty_session_directory(directory: &Path, parent: &Path) -> Result<(), StoreError> {
    #[cfg(test)]
    if should_fail_cleanup(directory) {
        return Err(StoreError::Unavailable);
    }
    match fs::remove_dir(directory).await {
        Ok(()) => {
            if sync_directory(parent).await.is_err() {
                Err(StoreError::UnknownOutcome)
            } else {
                Ok(())
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(StoreError::Unavailable),
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

async fn read_manifest_file(path: &Path) -> Result<SessionManifest, SessionLogError> {
    let bytes = fs::read(path)
        .await
        .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;
    let manifest = serde_json::from_slice::<SessionManifest>(&bytes)
        .map_err(|_| log_error(SessionLogErrorKind::Corrupt))?;
    if manifest.validate_structural().is_err() {
        return Err(log_error(SessionLogErrorKind::Corrupt));
    }
    Ok(manifest)
}

async fn initialization_state(
    directory: &Path,
    expected_session_id: SessionId,
) -> Result<InitializationState, SessionLogError> {
    let manifest_path = directory.join(MANIFEST_FILE);
    let conversation_path = directory.join(CONVERSATION_FILE);
    let manifest_state = path_state(&manifest_path)
        .await
        .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;
    let conversation_state = path_state(&conversation_path)
        .await
        .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;

    match manifest_state {
        PathState::Missing => match conversation_state {
            PathState::Missing => Ok(InitializationState::Missing),
            PathState::RegularFile => {
                let bytes = fs::read(&conversation_path)
                    .await
                    .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;
                if bytes.is_empty() {
                    Ok(InitializationState::EmptyLog)
                } else {
                    Ok(InitializationState::Corrupt)
                }
            }
            PathState::Directory | PathState::Symlink | PathState::Other => {
                Ok(InitializationState::Corrupt)
            }
        },
        PathState::RegularFile => {
            let manifest = read_manifest_file(&manifest_path).await?;
            if manifest.validate_structural().is_err() || manifest.session_id != expected_session_id
            {
                return Ok(InitializationState::Corrupt);
            }
            match conversation_state {
                PathState::RegularFile => {
                    // Validate only the complete prefix here; `load` owns the
                    // one final partial-tail truncation pass.
                    let bytes = fs::read(&conversation_path)
                        .await
                        .map_err(|_| log_error(SessionLogErrorKind::Unavailable))?;
                    let complete_len = complete_log_length(&bytes);
                    if decode_batches(&bytes[..complete_len]).is_err() {
                        Ok(InitializationState::Corrupt)
                    } else {
                        Ok(InitializationState::Initialized {
                            manifest,
                            log_bytes: bytes,
                        })
                    }
                }
                PathState::Missing
                | PathState::Directory
                | PathState::Symlink
                | PathState::Other => Ok(InitializationState::Corrupt),
            }
        }
        PathState::Directory | PathState::Symlink | PathState::Other => {
            Ok(InitializationState::Corrupt)
        }
    }
}

fn complete_log_length(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |position| position + 1)
}

async fn truncate_log(path: &Path, length: usize) -> Result<(), SessionLogError> {
    match path_state(path).await {
        Ok(PathState::RegularFile) => {}
        Ok(PathState::Missing) => return Err(log_error(SessionLogErrorKind::Unavailable)),
        Ok(PathState::Directory | PathState::Symlink | PathState::Other) => {
            return Err(log_error(SessionLogErrorKind::Corrupt));
        }
        Err(_) => return Err(log_error(SessionLogErrorKind::Unavailable)),
    }
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
    if sync_directory(&parent_directory(path)).await.is_err() {
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
    use std::path::{Path, PathBuf};

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

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
        SessionRecord, Store, StoreError, fail_directory_sync_after,
        fail_next_atomic_write_before_rename, fail_next_cleanup, fail_next_delete_after_partial,
        fail_next_directory_sync,
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

    fn session_directory(root: &Path, id: SessionId) -> PathBuf {
        root.join("sessions").join(id.to_string())
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

    async fn initialized_log_placeholder(
        store: &Store,
        root: &Path,
        id: SessionId,
    ) -> (LocalSessionLog, PathBuf) {
        let log = store.create_session(record(id)).await.unwrap();
        (log, session_directory(root, id))
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
    async fn initialize_state_machine_handles_residuals_and_missing_files() {
        let (store, root) = test_store("init-state").await;

        let empty_id = session_id(10);
        let mut empty_log = store.create_session(record(empty_id)).await.unwrap();
        let empty_directory = session_directory(&root, empty_id);
        tokio::fs::write(empty_directory.join(CONVERSATION_FILE), b"")
            .await
            .unwrap();
        assert!(matches!(
            store.open_log(empty_id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::NotInitialized
        ));
        assert_eq!(
            empty_log.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::NotInitialized
        );
        empty_log.initialize(manifest(empty_id)).await.unwrap();
        empty_log.close().await.unwrap();

        let manifest_only_id = session_id(11);
        let mut manifest_only = store
            .create_session(record(manifest_only_id))
            .await
            .unwrap();
        let manifest_only_directory = session_directory(&root, manifest_only_id);
        tokio::fs::write(
            manifest_only_directory.join(MANIFEST_FILE),
            serde_json::to_vec(&manifest(manifest_only_id)).unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            store.open_log(manifest_only_id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
        ));
        assert_eq!(
            manifest_only.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::Corrupt
        );
        assert_eq!(
            manifest_only
                .initialize(manifest(manifest_only_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Corrupt
        );

        let log_only_id = session_id(12);
        let mut log_only = store.create_session(record(log_only_id)).await.unwrap();
        let log_only_directory = session_directory(&root, log_only_id);
        tokio::fs::write(log_only_directory.join(CONVERSATION_FILE), b"not-empty")
            .await
            .unwrap();
        assert!(matches!(
            store.open_log(log_only_id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
        ));
        assert_eq!(
            log_only.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::Corrupt
        );
        assert_eq!(
            log_only
                .initialize(manifest(log_only_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Corrupt
        );

        let both_missing_id = session_id(13);
        let mut both_missing = store.create_session(record(both_missing_id)).await.unwrap();
        assert!(matches!(
            store.open_log(both_missing_id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::NotInitialized
        ));
        assert_eq!(
            both_missing.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::NotInitialized
        );
        both_missing
            .initialize(manifest(both_missing_id))
            .await
            .unwrap();
        both_missing.close().await.unwrap();

        let missing_log_id = session_id(14);
        let mut initialized = store.create_session(record(missing_log_id)).await.unwrap();
        initialized
            .initialize(manifest(missing_log_id))
            .await
            .unwrap();
        initialized.close().await.unwrap();
        let missing_log_path = session_directory(&root, missing_log_id).join(CONVERSATION_FILE);
        tokio::fs::remove_file(missing_log_path).await.unwrap();
        assert!(matches!(
            store.open_log(missing_log_id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
        ));

        let mismatched_disk_id = session_id(28);
        let mut mismatched_disk = store
            .create_session(record(mismatched_disk_id))
            .await
            .unwrap();
        let mismatched_disk_directory = session_directory(&root, mismatched_disk_id);
        tokio::fs::write(
            mismatched_disk_directory.join(MANIFEST_FILE),
            serde_json::to_vec(&manifest(session_id(29))).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(mismatched_disk_directory.join(CONVERSATION_FILE), b"")
            .await
            .unwrap();
        assert!(matches!(
            store.open_log(mismatched_disk_id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
        ));
        assert_eq!(
            mismatched_disk.load_manifest().await.unwrap_err().kind(),
            SessionLogErrorKind::Corrupt
        );

        let corrupt_initialized_id = session_id(26);
        let mut corrupt_initialized = store
            .create_session(record(corrupt_initialized_id))
            .await
            .unwrap();
        let corrupt_initialized_directory = session_directory(&root, corrupt_initialized_id);
        tokio::fs::write(
            corrupt_initialized_directory.join(MANIFEST_FILE),
            serde_json::to_vec(&manifest(corrupt_initialized_id)).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(
            corrupt_initialized_directory.join(CONVERSATION_FILE),
            b"not-json\n",
        )
        .await
        .unwrap();
        assert_eq!(
            corrupt_initialized
                .initialize(manifest(corrupt_initialized_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Corrupt
        );

        remove_root(&root).await;
    }

    #[tokio::test]
    async fn initialize_rejects_manifest_identity_mismatch_before_writing() {
        let (store, root) = test_store("identity-mismatch").await;
        let id = session_id(15);
        let other_id = session_id(16);
        let mut log = store.create_session(record(id)).await.unwrap();
        let directory = session_directory(&root, id);

        assert_eq!(
            log.initialize(manifest(other_id)).await.unwrap_err().kind(),
            SessionLogErrorKind::Corrupt
        );
        assert!(
            !tokio::fs::try_exists(directory.join(MANIFEST_FILE))
                .await
                .unwrap()
        );
        assert!(
            !tokio::fs::try_exists(directory.join(CONVERSATION_FILE))
                .await
                .unwrap()
        );

        log.initialize(manifest(id)).await.unwrap();
        log.close().await.unwrap();
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn initialize_marker_failures_classify_and_preserve_state() {
        let (store, root) = test_store("init-faults").await;

        let before_id = session_id(17);
        let (mut before, before_directory) =
            initialized_log_placeholder(&store, &root, before_id).await;
        fail_next_directory_sync(&before_directory);
        assert_eq!(
            before
                .initialize(manifest(before_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Unavailable
        );
        assert!(
            !tokio::fs::try_exists(before_directory.join(MANIFEST_FILE))
                .await
                .unwrap()
        );
        assert!(
            tokio::fs::try_exists(before_directory.join(CONVERSATION_FILE))
                .await
                .unwrap()
        );
        before.initialize(manifest(before_id)).await.unwrap();
        before.close().await.unwrap();

        let before_rename_id = session_id(27);
        let (mut before_rename, before_rename_directory) =
            initialized_log_placeholder(&store, &root, before_rename_id).await;
        before_rename.inject_initialize_before_marker_rename();
        assert_eq!(
            before_rename
                .initialize(manifest(before_rename_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::Unavailable
        );
        assert!(
            !tokio::fs::try_exists(before_rename_directory.join(MANIFEST_FILE))
                .await
                .unwrap()
        );
        before_rename
            .initialize(manifest(before_rename_id))
            .await
            .unwrap();
        before_rename.close().await.unwrap();

        let after_id = session_id(18);
        let (mut after, after_directory) =
            initialized_log_placeholder(&store, &root, after_id).await;
        fail_directory_sync_after(&after_directory, 1);
        assert_eq!(
            after
                .initialize(manifest(after_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert!(
            tokio::fs::try_exists(after_directory.join(MANIFEST_FILE))
                .await
                .unwrap()
        );
        assert!(
            tokio::fs::try_exists(after_directory.join(CONVERSATION_FILE))
                .await
                .unwrap()
        );
        assert_eq!(
            after
                .initialize(manifest(after_id))
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            after.close().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );

        let mut reopened = store.open_log(after_id).await.unwrap();
        assert_eq!(reopened.load_manifest().await.unwrap().session_id, after_id);
        reopened.close().await.unwrap();
        remove_root(&root).await;
    }

    #[tokio::test]
    async fn load_unknown_poisoned_object_rejects_every_followup_operation() {
        let (store, root) = test_store("load-fault").await;
        let id = session_id(30);
        let (mut initialized, directory) = initialized_log(&store, id).await;
        initialized.close().await.unwrap();
        let mut loaded = LocalSessionLog::new(directory.clone());
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(directory.join(CONVERSATION_FILE))
            .await
            .unwrap()
            .write_all(b"partial")
            .await
            .unwrap();
        fail_next_directory_sync(&directory);

        let first_error = loaded.load_manifest().await.unwrap_err();
        assert_eq!(first_error.kind(), SessionLogErrorKind::UnknownOutcome);
        assert_eq!(loaded.load_manifest().await.unwrap_err(), first_error);
        assert_eq!(
            loaded.read_page(None, 1).await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            loaded
                .append(ConversationSeq::ZERO, vec![entry(1, turn_id(30))])
                .await
                .unwrap_err()
                .kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            loaded.close().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );

        remove_root(&root).await;
    }

    #[tokio::test]
    async fn delete_failures_are_unknown_and_missing_is_only_not_found() {
        let (store, root) = test_store("delete-fault").await;

        assert!(matches!(
            store.delete_session(session_id(33)).await,
            Err(StoreError::SessionNotFound)
        ));

        let partial_id = session_id(34);
        let (mut partial_log, partial_directory) = initialized_log(&store, partial_id).await;
        partial_log.close().await.unwrap();
        fail_next_delete_after_partial(&partial_directory);
        assert!(matches!(
            store.delete_session(partial_id).await,
            Err(StoreError::UnknownOutcome)
        ));
        assert!(tokio::fs::try_exists(&partial_directory).await.unwrap());
        assert!(matches!(
            store.load_record(partial_id).await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.list_sessions().await,
            Err(StoreError::Corrupt)
        ));

        let synced_id = session_id(35);
        let (mut synced_log, synced_directory) = initialized_log(&store, synced_id).await;
        synced_log.close().await.unwrap();
        fail_next_directory_sync(&store.sessions_directory());
        assert!(matches!(
            store.delete_session(synced_id).await,
            Err(StoreError::UnknownOutcome)
        ));
        assert!(!tokio::fs::try_exists(&synced_directory).await.unwrap());

        remove_root(&root).await;
    }

    #[tokio::test]
    async fn existing_session_without_metadata_is_corrupt_not_missing() {
        let (store, root) = test_store("missing-record").await;
        let id = session_id(36);
        let (mut log, directory) = initialized_log(&store, id).await;
        log.close().await.unwrap();
        tokio::fs::remove_file(directory.join(SESSION_RECORD_FILE))
            .await
            .unwrap();

        assert!(matches!(
            store.load_record(id).await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.list_sessions().await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(store.open_log(id).await, Err(StoreError::Corrupt)));

        remove_root(&root).await;
    }

    #[tokio::test]
    async fn metadata_failures_clean_precommit_and_retain_unknown_commit() {
        let (store, root) = test_store("metadata-faults").await;

        let before_id = session_id(19);
        let before_directory = session_directory(&root, before_id);
        fail_next_atomic_write_before_rename(&before_directory.join(SESSION_RECORD_FILE));
        assert!(matches!(
            store.create_session(record(before_id)).await,
            Err(StoreError::Unavailable)
        ));
        assert!(!tokio::fs::try_exists(&before_directory).await.unwrap());

        let after_id = session_id(20);
        let after_directory = session_directory(&root, after_id);
        fail_next_directory_sync(&after_directory);
        assert!(matches!(
            store.create_session(record(after_id)).await,
            Err(StoreError::UnknownOutcome)
        ));
        assert!(tokio::fs::try_exists(&after_directory).await.unwrap());
        assert!(
            tokio::fs::try_exists(after_directory.join(SESSION_RECORD_FILE))
                .await
                .unwrap()
        );
        assert_eq!(
            store.load_record(after_id).await.unwrap().session_id,
            after_id
        );

        let cleanup_known_id = session_id(31);
        let cleanup_known_directory = session_directory(&root, cleanup_known_id);
        fail_next_atomic_write_before_rename(&cleanup_known_directory.join(SESSION_RECORD_FILE));
        fail_next_cleanup(&cleanup_known_directory);
        assert!(matches!(
            store.create_session(record(cleanup_known_id)).await,
            Err(StoreError::CleanupFailed { .. })
        ));
        assert!(
            tokio::fs::try_exists(&cleanup_known_directory)
                .await
                .unwrap()
        );
        assert!(matches!(
            store.load_record(cleanup_known_id).await,
            Err(StoreError::Corrupt)
        ));

        let cleanup_unknown_id = session_id(32);
        let cleanup_unknown_directory = session_directory(&root, cleanup_unknown_id);
        fail_next_atomic_write_before_rename(&cleanup_unknown_directory.join(SESSION_RECORD_FILE));
        fail_directory_sync_after(&store.sessions_directory(), 1);
        assert!(matches!(
            store.create_session(record(cleanup_unknown_id)).await,
            Err(StoreError::UnknownOutcome)
        ));
        assert!(
            !tokio::fs::try_exists(&cleanup_unknown_directory)
                .await
                .unwrap()
        );

        remove_root(&root).await;
    }

    #[tokio::test]
    async fn close_flush_and_sync_failures_poison_the_log() {
        let (store, root) = test_store("close-faults").await;

        let flush_id = session_id(21);
        let (mut flush_log, _) = initialized_log(&store, flush_id).await;
        flush_log.inject_close_flush_failure();
        assert_eq!(
            flush_log.close().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            flush_log.read_page(None, 1).await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );

        let sync_id = session_id(22);
        let (mut sync_log, _) = initialized_log(&store, sync_id).await;
        sync_log.inject_close_sync_failure();
        assert_eq!(
            sync_log.close().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );
        assert_eq!(
            sync_log.close().await.unwrap_err().kind(),
            SessionLogErrorKind::UnknownOutcome
        );

        remove_root(&root).await;
    }

    #[tokio::test]
    async fn tail_truncation_directory_sync_failure_is_unknown() {
        let (store, root) = test_store("tail-fault").await;
        let id = session_id(23);
        let (mut log, directory) = initialized_log(&store, id).await;
        log.append(ConversationSeq::ZERO, vec![entry(1, turn_id(23))])
            .await
            .unwrap();
        log.close().await.unwrap();
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(directory.join(CONVERSATION_FILE))
            .await
            .unwrap()
            .write_all(b"partial")
            .await
            .unwrap();
        fail_next_directory_sync(&directory);
        assert!(matches!(
            store.open_log(id).await,
            Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::UnknownOutcome
        ));

        remove_root(&root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_store_paths_are_rejected_without_following() {
        let (store, root) = test_store("symlinks").await;
        let outside = root.join("outside");
        tokio::fs::create_dir(&outside).await.unwrap();

        let linked_id = session_id(24);
        let linked_directory = session_directory(&root, linked_id);
        symlink(&outside, &linked_directory).unwrap();
        assert!(matches!(
            store.list_sessions().await,
            Err(StoreError::Corrupt)
        ));
        assert!(matches!(
            store.load_record(linked_id).await,
            Err(StoreError::Corrupt)
        ));
        tokio::fs::remove_file(&linked_directory).await.unwrap();

        let id = session_id(25);
        let (mut log, directory) = initialized_log(&store, id).await;
        log.close().await.unwrap();

        let record_target = outside.join("record.json");
        tokio::fs::write(
            &record_target,
            tokio::fs::read(directory.join(SESSION_RECORD_FILE))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::remove_file(directory.join(SESSION_RECORD_FILE))
            .await
            .unwrap();
        symlink(&record_target, directory.join(SESSION_RECORD_FILE)).unwrap();
        assert!(matches!(
            store.load_record(id).await,
            Err(StoreError::Corrupt)
        ));
        tokio::fs::remove_file(directory.join(SESSION_RECORD_FILE))
            .await
            .unwrap();
        tokio::fs::write(
            directory.join(SESSION_RECORD_FILE),
            serde_json::to_vec(&record(id)).unwrap(),
        )
        .await
        .unwrap();

        let manifest_target = outside.join("manifest.json");
        tokio::fs::write(&manifest_target, serde_json::to_vec(&manifest(id)).unwrap())
            .await
            .unwrap();
        tokio::fs::remove_file(directory.join(MANIFEST_FILE))
            .await
            .unwrap();
        symlink(&manifest_target, directory.join(MANIFEST_FILE)).unwrap();
        assert!(matches!(store.open_log(id).await, Err(StoreError::Corrupt)));
        tokio::fs::remove_file(directory.join(MANIFEST_FILE))
            .await
            .unwrap();
        tokio::fs::write(
            directory.join(MANIFEST_FILE),
            serde_json::to_vec(&manifest(id)).unwrap(),
        )
        .await
        .unwrap();

        let log_target = outside.join("conversation.log");
        tokio::fs::write(&log_target, b"").await.unwrap();
        tokio::fs::remove_file(directory.join(CONVERSATION_FILE))
            .await
            .unwrap();
        symlink(&log_target, directory.join(CONVERSATION_FILE)).unwrap();
        assert!(matches!(store.open_log(id).await, Err(StoreError::Corrupt)));
        tokio::fs::remove_file(directory.join(CONVERSATION_FILE))
            .await
            .unwrap();
        tokio::fs::write(directory.join(CONVERSATION_FILE), b"")
            .await
            .unwrap();

        let temp_target = outside.join("temp");
        tokio::fs::write(&temp_target, b"outside").await.unwrap();
        symlink(&temp_target, directory.join("session.json.tmp")).unwrap();
        assert!(matches!(
            store.load_record(id).await,
            Err(StoreError::Corrupt)
        ));
        assert_eq!(tokio::fs::read(&temp_target).await.unwrap(), b"outside");
        tokio::fs::remove_file(directory.join("session.json.tmp"))
            .await
            .unwrap();
        let stale_temp = directory.join("session.json.tmp");
        tokio::fs::write(&stale_temp, b"stale").await.unwrap();
        store.touch(id).await.unwrap();
        assert_eq!(tokio::fs::read(stale_temp).await.unwrap(), b"stale");

        let root_two = root.join("root-two");
        let store_two = Store::open(root_two.clone()).await.unwrap();
        drop(store_two);
        let outside_sessions = outside.join("sessions");
        tokio::fs::create_dir(&outside_sessions).await.unwrap();
        tokio::fs::remove_dir(root_two.join("sessions"))
            .await
            .unwrap();
        symlink(&outside_sessions, root_two.join("sessions")).unwrap();
        assert!(matches!(
            Store::open(root_two).await,
            Err(StoreError::InvalidRoot)
        ));

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
