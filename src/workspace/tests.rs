use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(unix)]
#[tokio::test]
async fn atomic_replacement_preserves_private_and_executable_modes() {
    use std::os::unix::fs::PermissionsExt;
    let (base, workspace) = fixture("permissions").await;
    let path = workspace.root().join("file");
    for mode in [0o600, 0o755, 0o4755] {
        fs::write(&path, b"before").await.unwrap();
        fs::set_permissions(&path, std_fs::Permissions::from_mode(mode))
            .await
            .unwrap();
        workspace.write_atomic("file", b"after").await.unwrap();
        assert_eq!(
            fs::metadata(&path).await.unwrap().permissions().mode() & 0o7777,
            mode
        );
        assert_eq!(fs::read(&path).await.unwrap(), b"after");
    }
    cleanup(&base).await;
}

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
        workspace
            .read_text_snapshot("nested/file.txt", 32)
            .await
            .unwrap()
            .0,
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
            .read_text_snapshot("nested/deep/new.txt", 32)
            .await
            .unwrap()
            .0
            .as_bytes(),
        b"created"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn bounded_reads_reject_large_and_binary_text() {
    let (base, workspace) = fixture("read-bounds").await;
    let root = base.join("root");
    fs::write(root.join("large.txt"), b"12345").await.unwrap();
    fs::write(root.join("lookahead.txt"), b"123456789")
        .await
        .unwrap();
    fs::write(root.join("binary"), [0xff, 0xfe]).await.unwrap();

    assert_eq!(
        workspace.read_text_snapshot("large.txt", 4).await,
        Err(WorkspaceError::TooLarge)
    );
    let prefix = workspace.read_prefix("lookahead.txt", 4).await.unwrap();
    assert_eq!(prefix.visible_len, 4);
    assert!(prefix.has_more);
    assert_eq!(prefix.bytes, b"12345678");
    assert_eq!(
        workspace.read_text_snapshot("binary", 8).await,
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
        workspace.read_text_snapshot("nul.txt", 32).await,
        Err(WorkspaceError::Binary)
    );
    assert_eq!(
        workspace
            .read_text_snapshot("unicode.txt", 64)
            .await
            .unwrap()
            .0,
        unicode
    );
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_inside_root_and_workspace_root_are_supported() {
    use std::os::unix::fs::symlink;

    let (base, workspace) = fixture("inside-symlink").await;
    let root = base.join("root");
    fs::write(root.join("real.txt"), b"inside").await.unwrap();
    fs::create_dir(root.join("real-dir")).await.unwrap();
    symlink("real.txt", root.join("file-link")).unwrap();
    symlink("real-dir", root.join("dir-link")).unwrap();
    let root_link = base.join("root-link");
    symlink(&root, &root_link).unwrap();

    assert_eq!(
        workspace
            .read_text_snapshot("file-link", 32)
            .await
            .unwrap()
            .0,
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
    let canonical_root = fs::canonicalize(&root).await.unwrap();
    assert_eq!(
        Workspace::open(root_link.clone()).await.unwrap().root(),
        canonical_root
    );
    assert_eq!(
        Workspace::open(root_link.join(".")).await.unwrap().root(),
        canonical_root
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
    BEFORE_RENAME_FAILURES.arm(target, ());

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
    DIRECTORY_SYNC_FAILURES.arm(workspace.root.as_ref().clone(), ());

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
    assert_eq!(
        workspace.read_text_snapshot(&name, 32).await.unwrap().0,
        "long-name"
    );
    cleanup(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporary_path_skips_exact_target_alias_across_counter_wrap() {
    let (base, workspace) = fixture("temp-target-alias").await;
    let root = base.join("root");
    let target_name = temp_basename(u64::MAX);
    let target = workspace.resolve_for_write(&target_name).await.unwrap();
    for id in [u64::MAX, 0] {
        TEMP_ID_OVERRIDES.arm(target.clone(), id);
    }
    let gate = Arc::new(crate::test_support::Gate::new());
    BEFORE_RENAME_GATES.arm(target.clone(), Arc::clone(&gate));

    let writer = workspace.clone();
    let path = target_name.clone();
    let task = tokio::spawn(async move { writer.write_atomic(&path, b"target").await });
    gate.wait_started().await;
    let target_existed_before_rename = fs::symlink_metadata(&target).await.is_ok();
    let actual_temp = root.join(temp_basename(0));
    let next_candidate_exists = fs::symlink_metadata(&actual_temp).await.is_ok();
    gate.release();
    task.await.unwrap().unwrap();

    assert!(!target_existed_before_rename);
    assert!(next_candidate_exists);
    assert_eq!(fs::read(&target).await.unwrap(), b"target");
    cleanup(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporary_path_skips_ascii_case_insensitive_target_alias() {
    let (base, workspace) = fixture("temp-target-case-alias").await;
    let root = base.join("root");
    let target_name = temp_basename(41).to_ascii_uppercase();
    let target = workspace.resolve_for_write(&target_name).await.unwrap();
    for id in [41, 42] {
        TEMP_ID_OVERRIDES.arm(target.clone(), id);
    }
    let gate = Arc::new(crate::test_support::Gate::new());
    BEFORE_RENAME_GATES.arm(target.clone(), Arc::clone(&gate));

    let writer = workspace.clone();
    let path = target_name.clone();
    let task = tokio::spawn(async move { writer.write_atomic(&path, b"case").await });
    gate.wait_started().await;
    let aliased_candidate = root.join(temp_basename(41));
    let alias_candidate_exists = fs::symlink_metadata(&aliased_candidate).await.is_ok();
    let actual_temp = root.join(temp_basename(42));
    let next_candidate_exists = fs::symlink_metadata(&actual_temp).await.is_ok();
    gate.release();
    task.await.unwrap().unwrap();

    assert!(!alias_candidate_exists);
    assert!(next_candidate_exists);
    assert_eq!(fs::read(&target).await.unwrap(), b"case");
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
    for id in [51, 52, 53] {
        TEMP_ID_OVERRIDES.arm(target.clone(), id);
    }
    let gate = Arc::new(crate::test_support::Gate::new());
    BEFORE_RENAME_GATES.arm(target.clone(), Arc::clone(&gate));

    let writer = workspace.clone();
    let path = target_name.clone();
    let task = tokio::spawn(async move { writer.write_atomic(&path, b"inside").await });
    gate.wait_started().await;
    let target_existed_before_rename = fs::symlink_metadata(&target).await.is_ok();
    let actual_temp = root.join(temp_basename(53));
    let third_candidate_exists = fs::symlink_metadata(&actual_temp).await.is_ok();
    gate.release();
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

impl Workspace {
    pub(crate) async fn write_atomic(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), WorkspaceError> {
        self.write_atomic_observed(path.to_owned(), bytes.to_vec(), None, |_, _, _| {})
            .await
            .outcome
    }
}

#[cfg(unix)]
pub(super) fn swap_open_target_with_fifo(target: &Path) {
    if SWAP_OPEN_TO_FIFO.hit(&target.to_path_buf()) {
        std_fs::remove_file(target).unwrap();
        assert!(
            std::process::Command::new("mkfifo")
                .arg(target)
                .status()
                .unwrap()
                .success()
        );
    }
}
