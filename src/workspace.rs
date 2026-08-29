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
        if metadata.len() > maximum {
            return Err(WorkspaceError::TooLarge);
        }
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
        if bytes.len() > max_bytes {
            return Err(WorkspaceError::TooLarge);
        }
        Ok(bytes)
    }

    pub(crate) async fn read_text(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<String, WorkspaceError> {
        String::from_utf8(self.read_bytes(path, max_bytes).await?)
            .map_err(|_| WorkspaceError::Binary)
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
        .map_err(|_| WorkspaceError::Unavailable)
}

async fn create_unique_temp(target: &Path) -> Result<(PathBuf, File), WorkspaceError> {
    for _ in 0..128 {
        let temp_path = unique_temp_path(target);
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

fn unique_temp_path(target: &Path) -> PathBuf {
    let name = target
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("workspace");
    let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".{name}.tmp-{}-{id}", std::process::id()))
}

async fn sync_directory(path: &Path) -> io::Result<()> {
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
fn fail_next_before_rename(target: PathBuf) {
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
                    .contains(".value.txt.tmp-")
            );
        }

        workspace.write_atomic("value.txt", b"new").await.unwrap();
        assert_eq!(fs::read(root.join("value.txt")).await.unwrap(), b"new");
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
