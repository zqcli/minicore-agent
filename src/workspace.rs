use std::ffi::OsString;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use thiserror::Error;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
static BEFORE_RENAME_FAILURES: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
#[cfg(test)]
static DIRECTORY_SYNC_FAILURES: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
#[cfg(test)]
static TEMP_ID_OVERRIDES: OnceLock<Mutex<Vec<(PathBuf, u64)>>> = OnceLock::new();
#[cfg(test)]
static BEFORE_RENAME_GATES: OnceLock<Mutex<Vec<Arc<BeforeRenameGate>>>> = OnceLock::new();

#[cfg(test)]
struct BeforeRenameGate {
    target: PathBuf,
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl BeforeRenameGate {
    fn new(target: PathBuf) -> Self {
        Self {
            target,
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[derive(Clone)]
pub struct Workspace {
    root: Arc<PathBuf>,
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
        })
    }

    pub(crate) async fn resolve_existing(&self, path: &str) -> Result<PathBuf, WorkspaceError> {
        let relative = normalize_relative(path, false)?;
        let resolved = self.canonicalize_inside(&self.root.join(relative)).await?;
        let metadata = fs::metadata(&resolved).await.map_err(map_io_error)?;
        if !metadata.is_file() {
            return Err(WorkspaceError::NotFile);
        }
        Ok(resolved)
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

    pub(crate) async fn read_bytes(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceError> {
        let (bytes, truncated) = self.read_prefix(path, max_bytes).await?;
        if truncated {
            return Err(WorkspaceError::TooLarge);
        }
        Ok(bytes)
    }

    pub(crate) async fn read_prefix(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, bool), WorkspaceError> {
        let resolved = self.resolve_existing(path).await?;
        let metadata = fs::symlink_metadata(&resolved)
            .await
            .map_err(map_io_error)?;
        if metadata.file_type().is_symlink() {
            return Err(WorkspaceError::Escape);
        }
        if !metadata.is_file() {
            return Err(WorkspaceError::NotFile);
        }
        let maximum = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let file = File::open(&resolved)
            .await
            .map_err(|error| map_read_error(error, WorkspaceError::NotFile))?;
        if !file.metadata().await.map_err(map_io_error)?.is_file() {
            return Err(WorkspaceError::NotFile);
        }
        let mut bytes = Vec::with_capacity(max_bytes.min(8 * 1024));
        let mut limited = file.take(maximum.saturating_add(1));
        limited
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| WorkspaceError::Unavailable)?;
        let read_truncated = bytes.len() > max_bytes;
        bytes.truncate(max_bytes);
        Ok((bytes, read_truncated))
    }

    pub(crate) async fn read_text(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<String, WorkspaceError> {
        let bytes = self.read_bytes(path, max_bytes).await?;
        if bytes.contains(&0) {
            return Err(WorkspaceError::Binary);
        }
        String::from_utf8(bytes).map_err(|_| WorkspaceError::Binary)
    }

    pub(crate) async fn write_atomic(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), WorkspaceError> {
        let resolved = self.resolve_for_write(path).await?;
        let file_name = resolved
            .file_name()
            .map(OsString::from)
            .ok_or(WorkspaceError::InvalidPath)?;
        let parent = resolved.parent().ok_or(WorkspaceError::InvalidPath)?;
        let parent = self.ensure_parent_directory(parent).await?;
        let target = parent.join(file_name);
        validate_write_target(&target).await?;
        atomic_write(&target, bytes).await
    }

    async fn canonicalize_inside(&self, path: &Path) -> Result<PathBuf, WorkspaceError> {
        let resolved = fs::canonicalize(path).await.map_err(map_io_error)?;
        if !resolved.starts_with(self.root.as_path()) {
            return Err(WorkspaceError::Escape);
        }
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

    async fn ensure_parent_directory(&self, parent: &Path) -> Result<PathBuf, WorkspaceError> {
        let relative = parent
            .strip_prefix(self.root.as_path())
            .map_err(|_| WorkspaceError::Escape)?;
        let mut current = self.root.as_ref().clone();
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err(WorkspaceError::InvalidPath);
            };
            let candidate = current.join(name);
            loop {
                match fs::symlink_metadata(&candidate).await {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        let resolved = self.canonicalize_inside(&candidate).await?;
                        if !fs::metadata(&resolved)
                            .await
                            .map_err(map_io_error)?
                            .is_dir()
                        {
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
                        match fs::create_dir(&candidate).await {
                            Ok(()) => {
                                sync_directory(&current)
                                    .await
                                    .map_err(|_| WorkspaceError::Unavailable)?;
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
}

fn normalize_relative(path: &str, allow_empty: bool) -> Result<PathBuf, WorkspaceError> {
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
    match fs::symlink_metadata(target).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(WorkspaceError::InvalidPath),
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(WorkspaceError::NotFile),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
            Err(WorkspaceError::NotDirectory)
        }
        Err(_) => Err(WorkspaceError::Unavailable),
    }
}

async fn atomic_write(target: &Path, bytes: &[u8]) -> Result<(), WorkspaceError> {
    let parent = target.parent().ok_or(WorkspaceError::InvalidPath)?;
    let (temp_path, mut file) = create_unique_temp(target).await?;
    if file.write_all(bytes).await.is_err()
        || file.flush().await.is_err()
        || file.sync_all().await.is_err()
    {
        drop(file);
        let _ = fs::remove_file(&temp_path).await;
        return Err(WorkspaceError::Unavailable);
    }
    drop(file);
    #[cfg(test)]
    if let Some(gate) = take_before_rename_gate(target) {
        gate.started.add_permits(1);
        gate.release.acquire().await.unwrap().forget();
    }
    #[cfg(test)]
    if should_fail_before_rename(target) {
        let _ = fs::remove_file(&temp_path).await;
        return Err(WorkspaceError::Unavailable);
    }
    if let Err(error) = validate_write_target(target).await {
        let _ = fs::remove_file(&temp_path).await;
        return Err(error);
    }
    if fs::rename(&temp_path, target).await.is_err() {
        let _ = fs::remove_file(&temp_path).await;
        return Err(WorkspaceError::Unavailable);
    }
    sync_directory(parent)
        .await
        .map_err(|_| WorkspaceError::UnknownOutcome)
}

async fn create_unique_temp(target: &Path) -> Result<(PathBuf, File), WorkspaceError> {
    for _ in 0..128 {
        let temp_path = unique_temp_path(target);
        if temp_path_aliases_target(&temp_path, target) {
            continue;
        }
        match fs::symlink_metadata(&temp_path).await {
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(WorkspaceError::Unavailable),
        }
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
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
    {
        let mut overrides = TEMP_ID_OVERRIDES
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap();
        if let Some(position) = overrides
            .iter()
            .position(|(candidate, _)| candidate == _target)
        {
            return overrides.remove(position).1;
        }
    }
    NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
}

async fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if should_fail_directory_sync(path) {
        return Err(io::Error::other(
            "injected workspace directory sync failure",
        ));
    }
    #[cfg(unix)]
    {
        let directory = File::open(path).await?;
        directory.sync_all().await
    }
    #[cfg(not(unix))]
    {
        // Tokio has no portable directory-fsync contract on non-Unix platforms.
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

#[cfg(test)]
pub(crate) fn fail_next_before_rename(target: PathBuf) {
    BEFORE_RENAME_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(target);
}

#[cfg(test)]
fn should_fail_before_rename(target: &Path) -> bool {
    let mut failures = BEFORE_RENAME_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| candidate == target)
        .map(|position| failures.remove(position))
        .is_some()
}

#[cfg(test)]
pub(crate) fn fail_next_directory_sync(path: PathBuf) {
    DIRECTORY_SYNC_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(path);
}

#[cfg(test)]
fn should_fail_directory_sync(path: &Path) -> bool {
    let mut failures = DIRECTORY_SYNC_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    failures
        .iter()
        .position(|candidate| candidate == path)
        .map(|position| failures.remove(position))
        .is_some()
}

#[cfg(test)]
fn override_temp_ids(target: PathBuf, ids: impl IntoIterator<Item = u64>) {
    TEMP_ID_OVERRIDES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .extend(ids.into_iter().map(|id| (target.clone(), id)));
}

#[cfg(test)]
fn block_before_rename(gate: Arc<BeforeRenameGate>) {
    BEFORE_RENAME_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(gate);
}

#[cfg(test)]
fn take_before_rename_gate(target: &Path) -> Option<Arc<BeforeRenameGate>> {
    let mut gates = BEFORE_RENAME_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|gate| gate.target == target)
        .map(|position| gates.remove(position))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    async fn fixture(label: &str) -> (PathBuf, Workspace) {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-workspace-{label}-{}-{id}",
            std::process::id()
        ));
        let root = base.join("root");
        fs::create_dir_all(&root).await.unwrap();
        let workspace = Workspace::open(root.join(".")).await.unwrap();
        (base, workspace)
    }

    async fn cleanup(base: &Path) {
        let _ = fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn open_canonicalizes_root_and_resolves_relative_files_and_directories() {
        let (base, workspace) = fixture("open").await;
        let root = base.join("root");
        fs::create_dir(root.join("nested")).await.unwrap();
        fs::write(root.join("nested/file.txt"), b"hello")
            .await
            .unwrap();

        assert_eq!(*workspace.root, fs::canonicalize(&root).await.unwrap());
        assert_eq!(
            workspace
                .resolve_existing("./nested/./file.txt")
                .await
                .unwrap(),
            fs::canonicalize(root.join("nested/file.txt"))
                .await
                .unwrap()
        );
        assert_eq!(
            workspace.read_text("nested/file.txt", 32).await.unwrap(),
            "hello"
        );
        assert_eq!(
            workspace.resolve_directory("").await.unwrap(),
            fs::canonicalize(&root).await.unwrap()
        );
        assert_eq!(
            workspace.resolve_directory(".").await.unwrap(),
            fs::canonicalize(&root).await.unwrap()
        );
        assert_eq!(
            workspace.resolve_directory("nested").await.unwrap(),
            fs::canonicalize(root.join("nested")).await.unwrap()
        );

        assert_eq!(
            Workspace::open(base.join("missing")).await.err(),
            Some(WorkspaceError::NotFound)
        );
        fs::write(base.join("not-directory"), b"file")
            .await
            .unwrap();
        assert_eq!(
            Workspace::open(base.join("not-directory")).await.err(),
            Some(WorkspaceError::NotDirectory)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn lexical_validation_rejects_absolute_parent_nul_and_empty_file_paths() {
        let (base, workspace) = fixture("lexical").await;
        let absolute = base.join("root/file.txt");
        let absolute = absolute.to_str().unwrap();

        for path in [absolute, "../file.txt", "a/../file.txt", "bad\0path"] {
            assert_eq!(
                workspace.resolve_existing(path).await,
                Err(WorkspaceError::InvalidPath)
            );
            assert_eq!(
                workspace.resolve_for_write(path).await,
                Err(WorkspaceError::InvalidPath)
            );
            assert_eq!(
                workspace.resolve_directory(path).await,
                Err(WorkspaceError::InvalidPath)
            );
        }
        for path in ["", ".", "././"] {
            assert_eq!(
                workspace.resolve_existing(path).await,
                Err(WorkspaceError::InvalidPath)
            );
            assert_eq!(
                workspace.resolve_for_write(path).await,
                Err(WorkspaceError::InvalidPath)
            );
        }
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn resolvers_enforce_file_and_directory_types() {
        let (base, workspace) = fixture("types").await;
        let root = base.join("root");
        fs::create_dir(root.join("directory")).await.unwrap();
        fs::write(root.join("file.txt"), b"file").await.unwrap();

        assert_eq!(
            workspace.resolve_existing("directory").await,
            Err(WorkspaceError::NotFile)
        );
        assert_eq!(
            workspace.resolve_directory("file.txt").await,
            Err(WorkspaceError::NotDirectory)
        );
        assert_eq!(
            workspace.resolve_for_write("directory").await,
            Err(WorkspaceError::NotFile)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn write_resolution_and_atomic_write_create_nested_parents() {
        let (base, workspace) = fixture("write-parent").await;
        let root = fs::canonicalize(base.join("root")).await.unwrap();

        assert_eq!(
            workspace.resolve_for_write("new.txt").await.unwrap(),
            root.join("new.txt")
        );
        assert_eq!(
            workspace
                .resolve_for_write("nested/deep/new.txt")
                .await
                .unwrap(),
            root.join("nested/deep/new.txt")
        );
        workspace
            .write_atomic("nested/deep/new.txt", b"created")
            .await
            .unwrap();
        assert_eq!(
            workspace
                .read_bytes("nested/deep/new.txt", 32)
                .await
                .unwrap(),
            b"created"
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn bounded_reads_reject_large_and_binary_text() {
        let (base, workspace) = fixture("read-bounds").await;
        let root = base.join("root");
        fs::write(root.join("large.txt"), b"12345").await.unwrap();
        fs::write(root.join("binary"), [0xff, 0xfe]).await.unwrap();

        assert_eq!(
            workspace.read_bytes("large.txt", 4).await,
            Err(WorkspaceError::TooLarge)
        );
        assert_eq!(
            workspace.read_text("binary", 8).await,
            Err(WorkspaceError::Binary)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn text_reads_reject_nul_but_allow_unicode_newlines_and_tabs() {
        let (base, workspace) = fixture("text-boundary").await;
        let root = base.join("root");
        fs::write(root.join("nul.txt"), b"valid\0utf8")
            .await
            .unwrap();
        let unicode = "你好\nline\tvalue";
        fs::write(root.join("unicode.txt"), unicode.as_bytes())
            .await
            .unwrap();

        assert_eq!(
            workspace.read_text("nul.txt", 32).await,
            Err(WorkspaceError::Binary)
        );
        assert_eq!(
            workspace.read_text("unicode.txt", 64).await.unwrap(),
            unicode
        );
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_inside_root_are_readable_and_root_is_canonical() {
        use std::os::unix::fs::symlink;

        let (base, workspace) = fixture("inside-symlink").await;
        let root = base.join("root");
        fs::write(root.join("real.txt"), b"inside").await.unwrap();
        fs::create_dir(root.join("real-dir")).await.unwrap();
        symlink("real.txt", root.join("file-link")).unwrap();
        symlink("real-dir", root.join("dir-link")).unwrap();
        let root_link = base.join("root-link");
        symlink(&root, &root_link).unwrap();
        let linked_workspace = Workspace::open(root_link).await.unwrap();

        assert_eq!(
            workspace.read_text("file-link", 32).await.unwrap(),
            "inside"
        );
        assert_eq!(
            workspace.resolve_directory("dir-link").await.unwrap(),
            fs::canonicalize(root.join("real-dir")).await.unwrap()
        );
        workspace
            .write_atomic("dir-link/new.txt", b"through-link")
            .await
            .unwrap();
        assert_eq!(
            fs::read(root.join("real-dir/new.txt")).await.unwrap(),
            b"through-link"
        );
        assert_eq!(
            *linked_workspace.root,
            fs::canonicalize(&root).await.unwrap()
        );
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_escape_is_rejected_for_existing_directory_and_write_parent() {
        use std::os::unix::fs::symlink;

        let (base, workspace) = fixture("escape").await;
        let root = base.join("root");
        let outside = base.join("outside");
        fs::create_dir(&outside).await.unwrap();
        fs::write(outside.join("outside.txt"), b"outside")
            .await
            .unwrap();
        symlink(outside.join("outside.txt"), root.join("file-link")).unwrap();
        symlink(&outside, root.join("dir-link")).unwrap();

        assert_eq!(
            workspace.resolve_existing("file-link").await,
            Err(WorkspaceError::Escape)
        );
        assert_eq!(
            workspace.resolve_directory("dir-link").await,
            Err(WorkspaceError::Escape)
        );
        assert_eq!(
            workspace.resolve_for_write("dir-link/new.txt").await,
            Err(WorkspaceError::Escape)
        );
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn atomic_write_rejects_target_symlink() {
        use std::os::unix::fs::symlink;

        let (base, workspace) = fixture("target-symlink").await;
        let root = base.join("root");
        fs::write(root.join("real.txt"), b"old").await.unwrap();
        symlink("real.txt", root.join("target.txt")).unwrap();

        assert_eq!(
            workspace.resolve_for_write("target.txt").await,
            Err(WorkspaceError::InvalidPath)
        );
        assert_eq!(
            workspace.write_atomic("target.txt", b"new").await,
            Err(WorkspaceError::InvalidPath)
        );
        assert_eq!(fs::read(root.join("real.txt")).await.unwrap(), b"old");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn atomic_overwrite_has_no_partial_result_before_rename() {
        let (base, workspace) = fixture("atomic-failure").await;
        let root = base.join("root");
        fs::write(root.join("value.txt"), b"old").await.unwrap();
        let target = workspace.resolve_for_write("value.txt").await.unwrap();
        fail_next_before_rename(target);

        assert_eq!(
            workspace.write_atomic("value.txt", b"new").await,
            Err(WorkspaceError::Unavailable)
        );
        assert_eq!(fs::read(root.join("value.txt")).await.unwrap(), b"old");
        let mut entries = fs::read_dir(&root).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            assert!(
                !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".minicore-write-")
            );
        }

        workspace.write_atomic("value.txt", b"new").await.unwrap();
        assert_eq!(fs::read(root.join("value.txt")).await.unwrap(), b"new");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn directory_sync_failure_after_rename_is_unknown_with_complete_target() {
        let (base, workspace) = fixture("atomic-unknown").await;
        let root = base.join("root");
        fs::write(root.join("value.txt"), b"old").await.unwrap();
        fail_next_directory_sync(workspace.root.as_ref().clone());

        assert_eq!(
            workspace.write_atomic("value.txt", b"complete-new").await,
            Err(WorkspaceError::UnknownOutcome)
        );
        assert_eq!(
            fs::read(root.join("value.txt")).await.unwrap(),
            b"complete-new"
        );
        let mut entries = fs::read_dir(&root).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            assert!(!entry.file_name().to_string_lossy().contains(".tmp"));
        }
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn atomic_write_supports_long_valid_target_basename() {
        let (base, workspace) = fixture("long-basename").await;
        let name = "a".repeat(245);

        workspace.write_atomic(&name, b"long-name").await.unwrap();
        assert_eq!(workspace.read_bytes(&name, 32).await.unwrap(), b"long-name");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn temporary_path_skips_exact_target_alias_across_counter_wrap() {
        let (base, workspace) = fixture("temp-target-alias").await;
        let root = base.join("root");
        let target_name = temp_basename(u64::MAX);
        let target = workspace.resolve_for_write(&target_name).await.unwrap();
        override_temp_ids(target.clone(), [u64::MAX, 0]);
        let gate = Arc::new(BeforeRenameGate::new(target.clone()));
        block_before_rename(Arc::clone(&gate));

        let writer = workspace.clone();
        let path = target_name.clone();
        let task = tokio::spawn(async move { writer.write_atomic(&path, b"target").await });
        gate.started.acquire().await.unwrap().forget();
        let target_existed_before_rename = fs::symlink_metadata(&target).await.is_ok();
        let actual_temp = root.join(temp_basename(0));
        let next_candidate_exists = fs::symlink_metadata(&actual_temp).await.is_ok();
        gate.release.add_permits(1);
        task.await.unwrap().unwrap();

        assert!(!target_existed_before_rename);
        assert!(next_candidate_exists);
        assert_eq!(fs::read(&target).await.unwrap(), b"target");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn temporary_path_skips_ascii_case_insensitive_target_alias() {
        let (base, workspace) = fixture("temp-target-case-alias").await;
        let root = base.join("root");
        let target_name = temp_basename(41).to_ascii_uppercase();
        let target = workspace.resolve_for_write(&target_name).await.unwrap();
        override_temp_ids(target.clone(), [41, 42]);
        let gate = Arc::new(BeforeRenameGate::new(target.clone()));
        block_before_rename(Arc::clone(&gate));

        let writer = workspace.clone();
        let path = target_name.clone();
        let task = tokio::spawn(async move { writer.write_atomic(&path, b"case").await });
        gate.started.acquire().await.unwrap().forget();
        let aliased_candidate = root.join(temp_basename(41));
        let alias_candidate_exists = fs::symlink_metadata(&aliased_candidate).await.is_ok();
        let actual_temp = root.join(temp_basename(42));
        let next_candidate_exists = fs::symlink_metadata(&actual_temp).await.is_ok();
        gate.release.add_permits(1);
        task.await.unwrap().unwrap();

        assert!(!alias_candidate_exists);
        assert!(next_candidate_exists);
        assert_eq!(fs::read(&target).await.unwrap(), b"case");
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn temporary_path_skips_real_symlink_after_target_alias() {
        use std::os::unix::fs::symlink;

        let (base, workspace) = fixture("temp-target-symlink").await;
        let root = base.join("root");
        let target_name = temp_basename(51);
        let target = workspace.resolve_for_write(&target_name).await.unwrap();
        let outside = base.join("outside.txt");
        fs::write(&outside, b"outside").await.unwrap();
        let symlink_candidate = root.join(temp_basename(52));
        symlink(&outside, &symlink_candidate).unwrap();
        override_temp_ids(target.clone(), [51, 52, 53]);
        let gate = Arc::new(BeforeRenameGate::new(target.clone()));
        block_before_rename(Arc::clone(&gate));

        let writer = workspace.clone();
        let path = target_name.clone();
        let task = tokio::spawn(async move { writer.write_atomic(&path, b"inside").await });
        gate.started.acquire().await.unwrap().forget();
        let target_existed_before_rename = fs::symlink_metadata(&target).await.is_ok();
        let actual_temp = root.join(temp_basename(53));
        let third_candidate_exists = fs::symlink_metadata(&actual_temp).await.is_ok();
        gate.release.add_permits(1);
        task.await.unwrap().unwrap();

        assert!(!target_existed_before_rename);
        assert!(third_candidate_exists);
        assert!(
            fs::symlink_metadata(&symlink_candidate)
                .await
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&outside).await.unwrap(), b"outside");
        assert_eq!(fs::read(&target).await.unwrap(), b"inside");
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unique_temp_name_does_not_follow_fixed_symlink() {
        use std::os::unix::fs::symlink;

        let (base, workspace) = fixture("temp-symlink").await;
        let root = base.join("root");
        let outside = base.join("outside.txt");
        fs::write(&outside, b"outside").await.unwrap();
        let fixed_temp = root.join(".value.txt.tmp");
        symlink(&outside, &fixed_temp).unwrap();

        workspace
            .write_atomic("value.txt", b"inside")
            .await
            .unwrap();
        assert_eq!(fs::read(root.join("value.txt")).await.unwrap(), b"inside");
        assert_eq!(fs::read(&outside).await.unwrap(), b"outside");
        assert!(
            fs::symlink_metadata(&fixed_temp)
                .await
                .unwrap()
                .file_type()
                .is_symlink()
        );
        cleanup(&base).await;
    }
}
