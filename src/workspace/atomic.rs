//! Atomic replacement, parent creation and commit observations.

use super::{
    FileStamp, Workspace, WorkspaceError, ensure_inside, map_io_error, normalize_relative,
};
use std::ffi::OsString;
use std::fs::{self as std_fs, File as StdFile, OpenOptions as StdOpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
pub(crate) static BEFORE_RENAME_FAILURES: crate::test_support::Registry<PathBuf> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static DIRECTORY_SYNC_FAILURES: crate::test_support::Registry<PathBuf> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(super) static TEMP_ID_OVERRIDES: crate::test_support::Registry<PathBuf, u64> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static BEFORE_RENAME_GATES: crate::test_support::Registry<
    PathBuf,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();
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

    /// Resolves `path` (cancellable) and returns the blocking commit. Once
    /// started, the commit and its observer must run to completion together;
    /// the caller owns that guarantee (see `ToolOwners::run_blocking`).
    pub(crate) async fn prepare_atomic_write(
        &self,
        path: String,
        bytes: Vec<u8>,
        expected: Option<ExpectedFileState>,
        observe: impl FnOnce(&str, &[u8], &AtomicWriteObservation) + Send + 'static,
    ) -> impl FnOnce() -> AtomicWriteObservation + Send + 'static {
        let resolved = self.resolve_for_write(&path).await;
        let root = Arc::clone(&self.root);
        move || {
            let observation = match resolved {
                Ok(target) => commit_write(&root, &target, &bytes, expected),
                Err(error) => failed_observation(error),
            };
            observe(&path, &bytes, &observation);
            observation
        }
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

pub(super) fn temp_basename(id: u64) -> String {
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
