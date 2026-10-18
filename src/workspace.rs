use std::ffi::OsString;
use std::fs::{self as std_fs, File as StdFile, OpenOptions as StdOpenOptions};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;

pub(crate) mod listing;
pub(crate) mod query;
pub(crate) mod scan;
pub(crate) mod search;
pub(crate) mod status;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
pub(crate) const WORKSPACE_CANCEL_GRACE: std::time::Duration =
    std::time::Duration::from_millis(250);

#[cfg(test)]
pub(crate) static BEFORE_RENAME_FAILURES: crate::test_support::Registry<PathBuf> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static DIRECTORY_SYNC_FAILURES: crate::test_support::Registry<PathBuf> =
    crate::test_support::Registry::new();
#[cfg(test)]
static TEMP_ID_OVERRIDES: crate::test_support::Registry<PathBuf, u64> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static BEFORE_RENAME_GATES: crate::test_support::Registry<
    PathBuf,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();
#[cfg(all(test, unix))]
pub(crate) static SWAP_OPEN_TO_FIFO: crate::test_support::Registry<PathBuf> =
    crate::test_support::Registry::new();

#[derive(Clone)]
pub struct Workspace {
    root: Arc<PathBuf>,
    scan_limits: scan::ScanLimits,
    scan_deadline: std::time::Duration,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkspaceError {
    #[error("workspace path is invalid")]
    InvalidPath,
    #[error("workspace path escapes the root")]
    Escape,
    #[error("workspace path was not found")]
    NotFound,
    #[error("workspace path is not a file")]
    NotFile,
    #[error("workspace path is not a directory")]
    NotDirectory,
    #[error("workspace is unavailable")]
    Unavailable,
    #[error("workspace mutation outcome is unknown")]
    UnknownOutcome,
    #[error("workspace file is too large")]
    TooLarge,
    #[error("workspace file is not valid UTF-8 text")]
    Binary,
}

pub(crate) struct ReadPrefix {
    pub(crate) bytes: Vec<u8>,
    pub(crate) visible_len: usize,
    pub(crate) has_more: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FileStamp {
    len: u64,
    modified_nanos: Option<u128>,
}

impl FileStamp {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified_nanos: metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos()),
        }
    }

    pub(crate) fn len(self) -> u64 {
        self.len
    }

    pub(crate) fn modified_unix_ms(self) -> Option<u64> {
        self.modified_nanos
            .and_then(|nanos| u64::try_from(nanos / 1_000_000).ok())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileSnapshot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) stamp: FileStamp,
    pub(crate) stable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileCapture {
    Missing,
    Complete(FileSnapshot),
    TooLarge(FileStamp),
    Error(WorkspaceError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpectedFileState {
    Missing,
    Present(FileStamp),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileState {
    Missing,
    Present(FileStamp),
    Unknown,
}

#[derive(Debug)]
pub(crate) struct AtomicWriteObservation {
    pub(crate) outcome: Result<(), WorkspaceError>,
    pub(crate) before: FileState,
    pub(crate) after: FileState,
    pub(crate) renamed: bool,
    pub(crate) expected_matches: Option<bool>,
    pub(crate) expected_unknown: bool,
}

impl Workspace {
    pub async fn open(root: PathBuf) -> Result<Self, WorkspaceError> {
        let metadata = fs::metadata(&root).await.map_err(map_io_error)?;
        if !metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory);
        }
        let root = fs::canonicalize(root).await.map_err(map_io_error)?;
        let metadata = fs::metadata(&root).await.map_err(map_io_error)?;
        if !metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory);
        }
        Ok(Self {
            root: Arc::new(root),
            scan_limits: scan::ScanLimits::standard(),
            scan_deadline: scan::WORKSPACE_SCAN_DEADLINE,
        })
    }

    pub(crate) fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub(crate) async fn resolve_existing(&self, path: &str) -> Result<PathBuf, WorkspaceError> {
        let relative = normalize_relative(path, false)?;
        let resolved = self.canonicalize_inside(&self.root.join(relative)).await?;
        let metadata = fs::metadata(&resolved).await.map_err(map_io_error)?;
        ensure_regular(&metadata)?;
        Ok(resolved)
    }

    /// Opens a workspace-relative regular file for reading. On Unix the open is
    /// non-blocking and does not follow the final symlink, so a special file or
    /// symlink swapped in after the path checks cannot block the caller; the
    /// post-open metadata check still rejects non-regular files. The workspace
    /// boundary is a sandbox for cooperative local clients, not a defense
    /// against an adversarial process running as the same user.
    pub(crate) async fn open_regular_file(&self, path: &str) -> Result<File, WorkspaceError> {
        let resolved = self.resolve_existing(path).await?;
        let metadata = fs::symlink_metadata(&resolved)
            .await
            .map_err(map_io_error)?;
        if metadata.file_type().is_symlink() {
            return Err(WorkspaceError::Escape);
        }
        ensure_regular(&metadata)?;
        #[cfg(all(test, unix))]
        tests::swap_open_target_with_fifo(&resolved);
        let file = open_read_only(&resolved).await?;
        ensure_regular(&file.metadata().await.map_err(map_io_error)?)?;
        Ok(file)
    }

    /// Canonicalizes a workspace-relative path and rejects anything outside
    /// the canonical root. Used by blocking scan workers before they open or
    /// walk a path.
    pub(crate) fn resolve_inside_sync(
        &self,
        path: &str,
        allow_empty: bool,
    ) -> Result<PathBuf, WorkspaceError> {
        let relative = normalize_relative(path, allow_empty)?;
        let resolved = std_fs::canonicalize(self.root.join(relative)).map_err(map_io_error)?;
        ensure_inside(&self.root, &resolved)?;
        Ok(resolved)
    }

    /// The walker already classified the entry. Keep canonical containment,
    /// no-follow/non-blocking open, and validate the actual opened descriptor.
    pub(crate) fn open_regular_file_sync(
        &self,
        path: &str,
    ) -> Result<(StdFile, std_fs::Metadata), WorkspaceError> {
        let resolved = self.resolve_inside_sync(path, false)?;
        #[cfg(all(test, unix))]
        tests::swap_open_target_with_fifo(&resolved);
        let file = open_read_only_sync(&resolved)?;
        let metadata = file.metadata().map_err(map_io_error)?;
        ensure_regular(&metadata)?;
        Ok((file, metadata))
    }

    pub(crate) async fn resolve_directory(&self, path: &str) -> Result<PathBuf, WorkspaceError> {
        let relative = normalize_relative(path, true)?;
        let resolved = self.canonicalize_inside(&self.root.join(relative)).await?;
        let metadata = fs::metadata(&resolved).await.map_err(map_io_error)?;
        if !metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory);
        }
        Ok(resolved)
    }

    pub(crate) async fn resolve_for_write(&self, path: &str) -> Result<PathBuf, WorkspaceError> {
        let relative = normalize_relative(path, false)?;
        let file_name = relative
            .file_name()
            .map(OsString::from)
            .ok_or(WorkspaceError::InvalidPath)?;
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        let parent = self.resolve_write_parent(parent).await?;
        let target = parent.join(file_name);
        validate_write_target(&target).await?;
        Ok(target)
    }

    pub(crate) fn validate_write_path(&self, path: &str) -> Result<(), WorkspaceError> {
        validate_relative_path(path)
    }

    pub(crate) async fn read_prefix(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<ReadPrefix, WorkspaceError> {
        let file = self.open_regular_file(path).await?;
        let read_limit = max_bytes.saturating_add(4);
        let maximum = u64::try_from(read_limit).unwrap_or(u64::MAX);
        let mut bytes = Vec::with_capacity(read_limit.min(8 * 1024));
        let mut limited = file.take(maximum);
        limited
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| WorkspaceError::Unavailable)?;
        Ok(ReadPrefix {
            visible_len: bytes.len().min(max_bytes),
            has_more: bytes.len() > max_bytes,
            bytes,
        })
    }

    pub(crate) async fn read_text_snapshot(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<(String, FileStamp, bool), WorkspaceError> {
        let snapshot = match self.capture_file(path, max_bytes).await {
            FileCapture::Complete(snapshot) => snapshot,
            FileCapture::Missing => return Err(WorkspaceError::NotFound),
            FileCapture::TooLarge(_) => return Err(WorkspaceError::TooLarge),
            FileCapture::Error(error) => return Err(error),
        };
        if snapshot.bytes.contains(&0) {
            return Err(WorkspaceError::Binary);
        }
        let text = String::from_utf8(snapshot.bytes).map_err(|_| WorkspaceError::Binary)?;
        Ok((text, snapshot.stamp, snapshot.stable))
    }

    /// Best-effort bounded capture for a replacing write. Capture failures are
    /// represented as evidence states so callers can continue the original
    /// write unchanged.
    pub(crate) async fn capture_file(&self, path: &str, max_bytes: usize) -> FileCapture {
        let mut file = match self.open_regular_file(path).await {
            Ok(file) => file,
            Err(WorkspaceError::NotFound) => return FileCapture::Missing,
            Err(error) => return FileCapture::Error(error),
        };
        let Ok(start_metadata) = file.metadata().await else {
            return FileCapture::Error(WorkspaceError::Unavailable);
        };
        let start_stamp = FileStamp::from_metadata(&start_metadata);
        if start_stamp.len() > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
            return FileCapture::TooLarge(start_stamp);
        }
        let read_limit = max_bytes.saturating_add(1);
        let maximum = u64::try_from(read_limit).unwrap_or(u64::MAX);
        let mut bytes = Vec::with_capacity(read_limit.min(8 * 1024));
        let mut limited = (&mut file).take(maximum);
        if limited.read_to_end(&mut bytes).await.is_err() {
            return FileCapture::Error(WorkspaceError::Unavailable);
        }
        drop(limited);
        let Ok(end_metadata) = file.metadata().await else {
            return FileCapture::Error(WorkspaceError::Unavailable);
        };
        let end_stamp = FileStamp::from_metadata(&end_metadata);
        if bytes.len() > max_bytes {
            return FileCapture::TooLarge(start_stamp);
        }
        FileCapture::Complete(FileSnapshot {
            bytes,
            stamp: start_stamp,
            stable: start_stamp == end_stamp,
        })
    }

    /// Once started, the blocking commit and its observer finish together even
    /// if the caller stops awaiting them. Path preflight remains cancellable.
    pub(crate) async fn write_atomic_observed(
        &self,
        path: String,
        bytes: Vec<u8>,
        expected: Option<ExpectedFileState>,
        observe: impl FnOnce(&str, &[u8], &AtomicWriteObservation) + Send + 'static,
    ) -> AtomicWriteObservation {
        let resolved = self.resolve_for_write(&path).await;
        let root = Arc::clone(&self.root);
        tokio::task::spawn_blocking(move || {
            let observation = match resolved {
                Ok(target) => commit_write(&root, &target, &bytes, expected),
                Err(error) => failed_observation(error),
            };
            observe(&path, &bytes, &observation);
            observation
        })
        .await
        .unwrap_or_else(|_| failed_observation(WorkspaceError::UnknownOutcome))
    }

    async fn canonicalize_inside(&self, path: &Path) -> Result<PathBuf, WorkspaceError> {
        let resolved = fs::canonicalize(path).await.map_err(map_io_error)?;
        ensure_inside(&self.root, &resolved)?;
        Ok(resolved)
    }

    async fn resolve_write_parent(&self, relative: &Path) -> Result<PathBuf, WorkspaceError> {
        let mut cursor = self.root.join(relative);
        let mut missing = Vec::new();
        loop {
            match fs::symlink_metadata(&cursor).await {
                Ok(_) => {
                    let mut resolved = self.canonicalize_inside(&cursor).await?;
                    if !fs::metadata(&resolved)
                        .await
                        .map_err(map_io_error)?
                        .is_dir()
                    {
                        return Err(WorkspaceError::NotDirectory);
                    }
                    for component in missing.iter().rev() {
                        resolved.push(component);
                    }
                    return Ok(resolved);
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if cursor == *self.root {
                        return Err(WorkspaceError::NotFound);
                    }
                    let component = cursor
                        .file_name()
                        .map(OsString::from)
                        .ok_or(WorkspaceError::InvalidPath)?;
                    missing.push(component);
                    if !cursor.pop() || !cursor.starts_with(self.root.as_path()) {
                        return Err(WorkspaceError::Escape);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
                    return Err(WorkspaceError::NotDirectory);
                }
                Err(_) => return Err(WorkspaceError::Unavailable),
            }
        }
    }
}

pub(crate) fn normalize_relative(path: &str, allow_empty: bool) -> Result<PathBuf, WorkspaceError> {
    if path.contains('\0') {
        return Err(WorkspaceError::InvalidPath);
    }
    let path = Path::new(path);
    if path.is_absolute() {
        return Err(WorkspaceError::InvalidPath);
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspaceError::InvalidPath);
            }
        }
    }
    if normalized.as_os_str().is_empty() && !allow_empty {
        return Err(WorkspaceError::InvalidPath);
    }
    Ok(normalized)
}

async fn validate_write_target(target: &Path) -> Result<(), WorkspaceError> {
    classify_write_target(fs::symlink_metadata(target).await).map(|_| ())
}

fn commit_write(
    root: &Path,
    target: &Path,
    bytes: &[u8],
    expected: Option<ExpectedFileState>,
) -> AtomicWriteObservation {
    let parent = target
        .parent()
        .expect("resolved write targets have a parent");
    let file_name = target
        .file_name()
        .expect("resolved write targets have a file name");
    let parent = match ensure_parent_directory(root, parent) {
        Ok(parent) => parent,
        Err(error) => return failed_observation(error),
    };
    let target = parent.join(file_name);
    atomic_write(&target, bytes, expected)
}

fn ensure_parent_directory(root: &Path, parent: &Path) -> Result<PathBuf, WorkspaceError> {
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| WorkspaceError::Escape)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(WorkspaceError::InvalidPath);
        };
        let candidate = current.join(name);
        loop {
            match std_fs::symlink_metadata(&candidate) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    let resolved = canonicalize_inside_sync(root, &candidate)?;
                    if !std_fs::metadata(&resolved).map_err(map_io_error)?.is_dir() {
                        return Err(WorkspaceError::NotDirectory);
                    }
                    current = resolved;
                    break;
                }
                Ok(metadata) if metadata.is_dir() => {
                    current = candidate;
                    break;
                }
                Ok(_) => return Err(WorkspaceError::NotDirectory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match std_fs::create_dir(&candidate) {
                        Ok(()) => {
                            sync_directory(&current).map_err(|_| WorkspaceError::Unavailable)?
                        }
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(_) => return Err(WorkspaceError::Unavailable),
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
                    return Err(WorkspaceError::NotDirectory);
                }
                Err(_) => return Err(WorkspaceError::Unavailable),
            }
        }
    }
    Ok(current)
}

fn canonicalize_inside_sync(root: &Path, path: &Path) -> Result<PathBuf, WorkspaceError> {
    let resolved = std_fs::canonicalize(path).map_err(map_io_error)?;
    ensure_inside(root, &resolved)?;
    Ok(resolved)
}

fn classify_write_target(
    metadata: io::Result<std_fs::Metadata>,
) -> Result<Option<std_fs::Metadata>, WorkspaceError> {
    match metadata {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(WorkspaceError::InvalidPath),
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Err(WorkspaceError::NotFile),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
            Err(WorkspaceError::NotDirectory)
        }
        Err(_) => Err(WorkspaceError::Unavailable),
    }
}

fn expected_matches_state(expected: Option<ExpectedFileState>, state: &FileState) -> Option<bool> {
    match expected {
        None => None,
        Some(ExpectedFileState::Missing) => Some(matches!(state, FileState::Missing)),
        Some(ExpectedFileState::Present(expected)) => match state {
            FileState::Present(actual) => Some(expected == *actual),
            FileState::Missing => Some(false),
            FileState::Unknown => None,
        },
    }
}

fn atomic_write(
    target: &Path,
    bytes: &[u8],
    expected: Option<ExpectedFileState>,
) -> AtomicWriteObservation {
    let metadata = match classify_write_target(std_fs::symlink_metadata(target)) {
        Ok(metadata) => metadata,
        Err(error) => return failed_observation(error),
    };
    let before = metadata.as_ref().map_or(FileState::Missing, |metadata| {
        FileState::Present(FileStamp::from_metadata(metadata))
    });
    let mut observation = AtomicWriteObservation {
        before,
        expected_matches: expected_matches_state(expected, &before),
        ..failed_observation(WorkspaceError::Unavailable)
    };
    let (temp_path, mut file) = match create_unique_temp(target) {
        Ok(value) => value,
        Err(error) => {
            observation.outcome = Err(error);
            return observation;
        }
    };
    observation.outcome = (|| {
        // Copy permissions before writing so private files never have a more
        // permissive temporary copy. Ownership and extended attributes are not copied.
        if let Some(metadata) = &metadata {
            file.set_permissions(metadata.permissions())
                .map_err(|_| WorkspaceError::Unavailable)?;
        }
        file.write_all(bytes)
            .map_err(|_| WorkspaceError::Unavailable)?;
        file.flush().map_err(|_| WorkspaceError::Unavailable)?;
        // Writing may clear set-ID bits, so restore the complete mode before syncing.
        if let Some(metadata) = &metadata {
            file.set_permissions(metadata.permissions())
                .map_err(|_| WorkspaceError::Unavailable)?;
        }
        file.sync_all().map_err(|_| WorkspaceError::Unavailable)?;
        drop(file);
        #[cfg(test)]
        if let Some(gate) = BEFORE_RENAME_GATES.take(&target.to_path_buf()) {
            gate.pass_blocking();
        }
        let matches = expected_matches_state(expected, &file_state_sync(target));
        observation.expected_unknown = expected.is_some() && matches.is_none();
        if observation.expected_unknown || matches.is_some() {
            observation.expected_matches = matches;
        }
        #[cfg(test)]
        if BEFORE_RENAME_FAILURES.hit(&target.to_path_buf()) {
            return Err(WorkspaceError::Unavailable);
        }
        classify_write_target(std_fs::symlink_metadata(target))?;
        std_fs::rename(&temp_path, target).map_err(|_| WorkspaceError::Unavailable)?;
        observation.renamed = true;
        observation.after = file_state_sync(target);
        sync_directory(
            target
                .parent()
                .expect("write targets have a resolved parent"),
        )
        .map_err(|_| WorkspaceError::UnknownOutcome)
    })();
    if !observation.renamed {
        let _ = std_fs::remove_file(temp_path);
    }
    observation
}

fn failed_observation(error: WorkspaceError) -> AtomicWriteObservation {
    AtomicWriteObservation {
        outcome: Err(error),
        before: FileState::Unknown,
        after: FileState::Unknown,
        renamed: false,
        expected_matches: None,
        expected_unknown: false,
    }
}

fn file_state_sync(path: &Path) -> FileState {
    match std_fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            FileState::Unknown
        }
        Ok(metadata) => FileState::Present(FileStamp::from_metadata(&metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => FileState::Missing,
        Err(_) => FileState::Unknown,
    }
}

fn create_unique_temp(target: &Path) -> Result<(PathBuf, StdFile), WorkspaceError> {
    for _ in 0..128 {
        let temp_path = unique_temp_path(target);
        if temp_path_aliases_target(&temp_path, target) {
            continue;
        }
        match std_fs::symlink_metadata(&temp_path) {
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(WorkspaceError::Unavailable),
        }
        match StdOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(WorkspaceError::Unavailable),
        }
    }
    Err(WorkspaceError::Unavailable)
}

fn temp_path_aliases_target(candidate: &Path, target: &Path) -> bool {
    let (Some(candidate), Some(target)) = (candidate.file_name(), target.file_name()) else {
        return false;
    };
    candidate == target
        || candidate
            .to_str()
            .zip(target.to_str())
            .is_some_and(|(candidate, target)| candidate.eq_ignore_ascii_case(target))
}

fn unique_temp_path(target: &Path) -> PathBuf {
    let id = next_temp_id(target);
    target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(temp_basename(id))
}

fn temp_basename(id: u64) -> String {
    format!(".minicore-write-{}-{id}.tmp", std::process::id())
}

fn next_temp_id(_target: &Path) -> u64 {
    #[cfg(test)]
    if let Some(id) = TEMP_ID_OVERRIDES.take(&_target.to_path_buf()) {
        return id;
    }
    NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if DIRECTORY_SYNC_FAILURES.hit(&path.to_path_buf()) {
        return Err(io::Error::other(
            "injected workspace directory sync failure",
        ));
    }
    #[cfg(unix)]
    {
        StdFile::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        // Rust has no portable directory-fsync contract on non-Unix platforms.
        let _ = path;
        Ok(())
    }
}

fn map_io_error(error: io::Error) -> WorkspaceError {
    match error.kind() {
        io::ErrorKind::NotFound => WorkspaceError::NotFound,
        io::ErrorKind::NotADirectory => WorkspaceError::NotDirectory,
        _ => WorkspaceError::Unavailable,
    }
}

fn map_read_error(error: io::Error, wrong_type: WorkspaceError) -> WorkspaceError {
    match error.kind() {
        io::ErrorKind::NotFound => WorkspaceError::NotFound,
        io::ErrorKind::IsADirectory | io::ErrorKind::NotADirectory => wrong_type,
        _ => WorkspaceError::Unavailable,
    }
}

/// Rejects a path that is not a workspace-relative lexical path (absolute,
/// root/prefix, `..`, or NUL). This is a cheap check that needs no filesystem
/// access, so callers can reject a request before reserving any resources.
pub(crate) fn validate_relative_path(path: &str) -> Result<(), WorkspaceError> {
    normalize_relative(path, false).map(|_| ())
}

/// Opens a file read-only. On Unix the open uses `O_NONBLOCK` so a special
/// file cannot block the caller, and `O_NOFOLLOW` so a symlink swapped in after
/// the path checks fails the open instead of escaping the workspace.
async fn open_read_only(path: &Path) -> Result<File, WorkspaceError> {
    tokio::fs::OpenOptions::from(read_options())
        .open(path)
        .await
        .map_err(map_open_error)
}

/// Synchronous twin of `open_read_only` for blocking scan workers.
pub(crate) fn open_read_only_sync(path: &Path) -> Result<StdFile, WorkspaceError> {
    read_options().open(path).map_err(map_open_error)
}

fn read_options() -> StdOpenOptions {
    let mut options = StdOpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    options
}

fn ensure_inside(root: &Path, path: &Path) -> Result<(), WorkspaceError> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(WorkspaceError::Escape)
    }
}

fn ensure_regular(metadata: &std_fs::Metadata) -> Result<(), WorkspaceError> {
    if metadata.is_file() {
        Ok(())
    } else {
        Err(WorkspaceError::NotFile)
    }
}

fn map_open_error(error: io::Error) -> WorkspaceError {
    #[cfg(unix)]
    {
        // O_NOFOLLOW reports ELOOP when the final component became a symlink.
        if error.raw_os_error() == Some(libc::ELOOP) {
            return WorkspaceError::Escape;
        }
    }
    map_read_error(error, WorkspaceError::NotFile)
}

#[cfg(test)]
pub(crate) mod tests;
