use super::*;
use crate::workspace::scan::MIN_RESULT_BYTES;
use std::process::{Command as StdCommand, Output};
use std::sync::Arc;

struct Fixture {
    _dir: crate::test_support::TempDir,
    base: PathBuf,
    root: PathBuf,
    workspace: Arc<Workspace>,
}

/// A workspace directory that is not inside any repository.
async fn plain_fixture(label: &str) -> Fixture {
    let dir = crate::test_support::TempDir::new(label);
    let base = dir.path().to_path_buf();
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());
    Fixture {
        _dir: dir,
        base,
        root,
        workspace,
    }
}

/// A workspace that is the root of its own freshly initialized repository.
async fn repo_fixture(label: &str) -> Fixture {
    let fixture = plain_fixture(label).await;
    git(&fixture.root, &["init", "--quiet"]);
    fixture
}

/// Runs one fixture-local git command. Identity is passed per command, so
/// no global configuration is ever read or written.
fn git_output(root: &Path, args: &[&str]) -> Output {
    StdCommand::new("git")
        .arg("-C")
        .arg(root)
        .arg("-c")
        .arg("user.name=minicore-test")
        .arg("-c")
        .arg("user.email=minicore-test@example.invalid")
        .args(args)
        .output()
        .expect("git must be available for workspace status tests")
}

fn git(root: &Path, args: &[&str]) -> Output {
    let output = git_output(root, args);
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn write_file(root: &Path, path: &str, bytes: &[u8]) {
    let full = root.join(path);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(full, bytes).unwrap();
}

async fn observe(workspace: &Workspace) -> WorkspaceStatusResult {
    request(workspace, None).await.unwrap()
}

async fn request(
    workspace: &Workspace,
    max_bytes: Option<usize>,
) -> Result<WorkspaceStatusResult, AgentError> {
    let request = WorkspaceStatusRequest {
        session_id: SessionId::new().unwrap(),
        max_bytes,
    };
    status(
        workspace,
        &request,
        &CancellationToken::new(),
        &CancellationToken::new(),
        &CancellationToken::new(),
    )
    .await
}

#[cfg(unix)]
fn executable(path: &Path, contents: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, contents).unwrap();
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

#[tokio::test]
async fn a_clean_repository_is_observed_completely() {
    let fixture = repo_fixture("clean").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(
        result
            .head_oid
            .as_deref()
            .is_some_and(|oid| oid.len() == 40)
    );
    assert!(result.branch.is_some());
    assert!(!result.detached);
    assert_eq!(
        (
            result.staged,
            result.unstaged,
            result.untracked,
            result.conflicted
        ),
        (0, 0, 0, 0)
    );
    assert!(result.entries.is_empty());
    assert!(result.complete);
    assert_eq!(result.skipped_paths, 0);
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);
    assert_eq!(result.consistency, WorkspaceScanConsistency::Live);
    assert!(result.observed_at_unix_ms > 0);
}

#[tokio::test]
async fn staged_unstaged_and_untracked_entries_are_counted() {
    let fixture = repo_fixture("changes").await;
    write_file(&fixture.root, "staged.txt", b"one\n");
    write_file(&fixture.root, "unstaged.txt", b"two\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    write_file(&fixture.root, "staged.txt", b"one changed\n");
    git(&fixture.root, &["add", "staged.txt"]);
    write_file(&fixture.root, "unstaged.txt", b"two changed\n");
    write_file(&fixture.root, "untracked.txt", b"three\n");

    let result = observe(&fixture.workspace).await;
    assert_eq!(result.staged, 1);
    assert_eq!(result.unstaged, 1);
    assert_eq!(result.untracked, 1);
    assert_eq!(result.conflicted, 0);
    assert!(result.complete);
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);

    let staged = result
        .entries
        .iter()
        .find(|entry| entry.path == "staged.txt")
        .expect("staged entry");
    assert_eq!(staged.kind, WorkspaceStatusEntryKind::Ordinary);
    assert_eq!(staged.index_status.as_deref(), Some("M"));
    assert_eq!(staged.worktree_status.as_deref(), Some("."));
    assert_eq!(staged.original_path, None);

    let unstaged = result
        .entries
        .iter()
        .find(|entry| entry.path == "unstaged.txt")
        .expect("unstaged entry");
    assert_eq!(unstaged.index_status.as_deref(), Some("."));
    assert_eq!(unstaged.worktree_status.as_deref(), Some("M"));

    let untracked = result
        .entries
        .iter()
        .find(|entry| entry.path == "untracked.txt")
        .expect("untracked entry");
    assert_eq!(untracked.kind, WorkspaceStatusEntryKind::Untracked);
    assert_eq!(untracked.index_status, None);
    assert_eq!(untracked.worktree_status, None);
}

#[tokio::test]
async fn an_unborn_head_reports_a_branch_without_an_oid() {
    let fixture = repo_fixture("unborn").await;

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert_eq!(result.head_oid, None);
    assert!(result.branch.is_some());
    assert!(!result.detached);
    assert!(result.complete);
}

#[tokio::test]
async fn a_detached_head_reports_detached_without_a_branch() {
    let fixture = repo_fixture("detached").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    git(&fixture.root, &["checkout", "--quiet", "--detach"]);

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(result.detached);
    assert_eq!(result.branch, None);
    assert!(result.head_oid.is_some());
    assert!(result.complete);
}

#[tokio::test]
async fn a_conflict_is_reported_as_unmerged() {
    let fixture = repo_fixture("conflict").await;
    write_file(&fixture.root, "shared.txt", b"base\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "base"]);
    git(&fixture.root, &["checkout", "--quiet", "-b", "other"]);
    write_file(&fixture.root, "shared.txt", b"other\n");
    git(&fixture.root, &["commit", "--quiet", "-am", "other"]);
    git(&fixture.root, &["checkout", "--quiet", "-"]);
    write_file(&fixture.root, "shared.txt", b"current\n");
    git(&fixture.root, &["commit", "--quiet", "-am", "current"]);
    let merge = git_output(&fixture.root, &["merge", "other"]);
    assert!(
        !merge.status.success(),
        "the merge was expected to conflict"
    );

    let result = observe(&fixture.workspace).await;
    assert_eq!(result.conflicted, 1);
    assert!(result.complete);
    let entry = result
        .entries
        .iter()
        .find(|entry| entry.kind == WorkspaceStatusEntryKind::Unmerged)
        .expect("unmerged entry");
    assert_eq!(entry.path, "shared.txt");
}

#[tokio::test]
async fn a_rename_keeps_both_paths() {
    let fixture = repo_fixture("rename").await;
    write_file(&fixture.root, "before.txt", b"content\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    std::fs::rename(
        fixture.root.join("before.txt"),
        fixture.root.join("after.txt"),
    )
    .unwrap();
    git(&fixture.root, &["add", "-A"]);

    let result = observe(&fixture.workspace).await;
    assert!(result.complete);
    let entry = result
        .entries
        .iter()
        .find(|entry| entry.kind == WorkspaceStatusEntryKind::Renamed)
        .expect("a rename record");
    assert_eq!(entry.path, "after.txt");
    assert_eq!(entry.original_path.as_deref(), Some("before.txt"));
    assert_eq!(entry.index_status.as_deref(), Some("R"));
}

#[tokio::test]
async fn a_non_repository_is_reported_conservatively() {
    let fixture = plain_fixture("nonrepo").await;

    // Git refuses to resolve a work tree, and a refusal is not proof that
    // no repository exists: the answer stays incomplete instead of
    // claiming a definite missing repository.
    let result = observe(&fixture.workspace).await;
    assert!(!result.repo_available);
    assert_eq!(result.head_oid, None);
    assert_eq!(result.branch, None);
    assert!(!result.detached);
    assert!(result.entries.is_empty());
    assert!(!result.complete);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
}

#[tokio::test]
async fn a_bare_repository_is_a_complete_answer_without_a_work_tree() {
    let fixture = plain_fixture("bare").await;
    git(&fixture.root, &["init", "--quiet", "--bare"]);

    // `--is-bare-repository` answers with `true`: a definite state without
    // a work tree to observe.
    let result = observe(&fixture.workspace).await;
    assert!(!result.repo_available);
    assert!(result.entries.is_empty());
    assert!(result.complete);
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);
}

#[tokio::test]
async fn a_missing_git_executable_is_reported_as_unavailable() {
    let fixture = plain_fixture("missing-git").await;
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        fixture.base.join("missing-git").into_os_string(),
    );

    let result = observe(&fixture.workspace).await;
    assert!(!result.repo_available);
    assert!(!result.complete);
    assert_eq!(
        result.warnings,
        vec![WorkspaceStatusWarning::GitUnavailable]
    );
}

#[tokio::test]
async fn a_nested_workspace_reports_only_paths_inside_it() {
    let base = std::env::temp_dir().join(format!(
        "minicore-workspace-status-nested-{}",
        SessionId::new().unwrap()
    ));
    let repository = base.join("repo");
    let root = repository.join("sub");
    std::fs::create_dir_all(&root).unwrap();
    git(&repository, &["init", "--quiet"]);
    write_file(&repository, "outside.txt", b"outside\n");
    write_file(&repository, "outside-only.txt", b"outside\n");
    write_file(&root, "tracked.txt", b"tracked\n");
    write_file(&root, "removed.txt", b"removed\n");
    git(&repository, &["add", "."]);
    git(&repository, &["commit", "--quiet", "-m", "initial"]);
    // A change outside the workspace that stays outside, a move that
    // crosses into it, a staged deletion whose file is already gone, and an
    // untracked file: only the paths inside may appear.
    write_file(&repository, "outside-only.txt", b"changed outside\n");
    git(&repository, &["mv", "outside.txt", "sub/moved.txt"]);
    std::fs::remove_file(root.join("removed.txt")).unwrap();
    git(&repository, &["add", "-A", "--", "sub/removed.txt"]);
    write_file(&root, "inside.txt", b"inside\n");
    let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());

    let result = observe(&workspace).await;
    assert!(result.repo_available);
    assert!(result.complete);
    assert!(
        result
            .warnings
            .contains(&WorkspaceStatusWarning::NestedRepository)
    );
    let mut paths: Vec<&str> = result
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    // The contract does not order tracked and untracked entries together,
    // so the set is compared, not the emission order.
    paths.sort_unstable();
    assert_eq!(paths, vec!["inside.txt", "moved.txt", "removed.txt"]);
    let moved = result
        .entries
        .iter()
        .find(|entry| entry.path == "moved.txt")
        .expect("the moved entry");
    assert_eq!(moved.original_path, None);
    assert_eq!(
        (
            result.staged,
            result.unstaged,
            result.untracked,
            result.conflicted
        ),
        (2, 0, 1, 0)
    );
    let serialized = serde_json::to_string(&result).unwrap();
    assert!(
        !serialized.contains("outside"),
        "a path outside the workspace leaked into the result"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// A staged gitlink change is a commit recorded in the superproject, so it
/// is reported and counted as staged. The submodule's own work tree is never
/// scanned and never recursed into, and no monitor or hook configured inside
/// it runs.
#[cfg(unix)]
#[tokio::test]
async fn a_staged_gitlink_is_counted_without_entering_the_submodule() {
    let fixture = repo_fixture("gitlink").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    let first = git_output(&fixture.root, &["rev-parse", "HEAD"]);
    let first = String::from_utf8(first.stdout).unwrap().trim().to_owned();
    // A gitlink in the index and in a commit, without a network access and
    // without any submodule fetch.
    git(
        &fixture.root,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000",
            first.as_str(),
            "sub",
        ],
    );
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "record gitlink"],
    );
    write_file(&fixture.root, "second.txt", b"second\n");
    // Only the new file is staged: adding everything would record the
    // absence of the gitlink's (still missing) work tree directory and drop
    // the gitlink from HEAD, which this fixture must keep.
    git(&fixture.root, &["add", "--", "second.txt"]);
    git(&fixture.root, &["commit", "--quiet", "-m", "second"]);
    // The recorded gitlink is still in HEAD, so the change below is a
    // recorded modification rather than an addition.
    let recorded = git_output(&fixture.root, &["ls-tree", "HEAD", "--", "sub"]);
    assert!(
        String::from_utf8(recorded.stdout)
            .unwrap()
            .starts_with("160000 commit"),
        "the fixture lost its recorded gitlink"
    );
    let second = git_output(&fixture.root, &["rev-parse", "HEAD"]);
    let second = String::from_utf8(second.stdout).unwrap().trim().to_owned();
    git(
        &fixture.root,
        &[
            "update-index",
            "--cacheinfo",
            "160000",
            second.as_str(),
            "sub",
        ],
    );

    // A real repository in the submodule's place, with an untracked file
    // and traps that would leave a marker if anything inside it ran.
    let submodule = fixture.root.join("sub");
    std::fs::create_dir_all(&submodule).unwrap();
    git(&submodule, &["init", "--quiet"]);
    write_file(&submodule, "untracked.txt", b"x");
    let fsmonitor_marker = fixture.base.join("submodule-fsmonitor-ran");
    let fsmonitor = fixture.base.join("submodule-fsmonitor");
    executable(
        &fsmonitor,
        &format!("#!/bin/sh\n: > '{}'\n", fsmonitor_marker.display()),
    );
    let hook_marker = fixture.base.join("submodule-hook-ran");
    let hooks = fixture.base.join("submodule-hooks");
    std::fs::create_dir_all(&hooks).unwrap();
    executable(
        &hooks.join("post-index-change"),
        &format!("#!/bin/sh\n: > '{}'\n", hook_marker.display()),
    );
    git(
        &submodule,
        &["config", "core.fsmonitor", fsmonitor.to_str().unwrap()],
    );
    git(
        &submodule,
        &["config", "core.hooksPath", hooks.to_str().unwrap()],
    );

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(result.complete, "{:?}", result.warnings);
    let entry = result
        .entries
        .iter()
        .find(|entry| entry.path == "sub")
        .expect("the gitlink entry");
    assert_eq!(entry.kind, WorkspaceStatusEntryKind::Ordinary);
    assert_eq!(entry.index_status.as_deref(), Some("M"));
    assert_eq!(result.staged, 1);
    // Nothing inside the submodule is reported: the untracked file there
    // does not appear and its work tree is not walked.
    assert_eq!(result.untracked, 0, "{:?}", result);
    assert!(
        !result
            .entries
            .iter()
            .any(|entry| entry.path.starts_with("sub/")),
        "a submodule path was reported"
    );
    assert!(
        !fsmonitor_marker.exists(),
        "a submodule file system monitor ran"
    );
    assert!(!hook_marker.exists(), "a submodule hook ran");
}

/// A rename whose previous name lies outside the workspace keeps the entry
/// and reports the current name only: the previous name is never returned.
#[tokio::test]
async fn a_rename_from_outside_the_workspace_never_reports_its_previous_name() {
    let base = std::env::temp_dir().join(format!(
        "minicore-workspace-status-cross-rename-{}",
        SessionId::new().unwrap()
    ));
    let repository = base.join("repo");
    let root = repository.join("sub");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = Workspace::open(root.clone()).await.unwrap();
    // The workspace root is canonical, so the stand-in work tree root must
    // be too for the prefix check this test exercises.
    let repository = std::fs::canonicalize(&repository).unwrap();
    let mut found = Observation::default();

    let mapped = super::map_entry(
        b"sub/inside.txt",
        Some(b"outside.txt"),
        &repository,
        workspace.root(),
        &mut found,
    );
    assert_eq!(mapped, Some(("inside.txt".to_owned(), None)));
    assert_eq!(found.skipped_paths, 0);
    // An outside path is filtered before it can even be counted.
    let mut found = Observation::default();
    let mapped = super::map_entry(
        b"outside.txt",
        None,
        &repository,
        workspace.root(),
        &mut found,
    );
    assert_eq!(mapped, None);
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn odd_paths_survive_the_nul_parser() {
    let fixture = repo_fixture("odd-paths").await;
    write_file(&fixture.root, "with space.txt", b"x");
    write_file(&fixture.root, "-leading-dash.txt", b"x");
    #[cfg(unix)]
    write_file(&fixture.root, "line\nbreak.txt", b"x");

    let result = observe(&fixture.workspace).await;
    assert!(result.complete);
    let mut paths: Vec<&str> = result
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    paths.sort_unstable();
    let mut expected = vec!["-leading-dash.txt", "with space.txt"];
    #[cfg(unix)]
    expected.push("line\nbreak.txt");
    expected.sort_unstable();
    assert_eq!(paths, expected);
}

#[cfg(all(unix, not(target_os = "macos")))]
#[tokio::test]
async fn a_non_utf8_path_is_skipped_and_marked_incomplete() {
    use std::os::unix::ffi::OsStrExt;
    let fixture = repo_fixture("non-utf8").await;
    let name = std::ffi::OsStr::from_bytes(b"bad-\xff\xfe.txt");
    std::fs::write(fixture.root.join(name), b"x").unwrap();

    let result = observe(&fixture.workspace).await;
    assert_eq!(result.skipped_paths, 1);
    assert!(
        result
            .warnings
            .contains(&WorkspaceStatusWarning::SkippedPaths)
    );
    assert!(!result.complete);
    assert!(result.entries.is_empty());
    assert_eq!(result.untracked, 0);
}

#[tokio::test]
async fn the_stdout_ceiling_stops_the_query() {
    let fixture = repo_fixture("stdout-cap").await;
    for index in 0..40 {
        write_file(
            &fixture.root,
            &format!("a-rather-long-untracked-name-{index:02}.txt"),
            b"x",
        );
    }
    STATUS_CAPS.arm(
        fixture.workspace.root().to_path_buf(),
        StatusCaps {
            stdout_bytes: 512,
            records: MAX_STATUS_RECORDS,
        },
    );

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(!result.complete);
    assert_eq!(
        result.warnings,
        vec![WorkspaceStatusWarning::OutputTruncated]
    );
}

#[tokio::test]
async fn the_record_ceiling_stops_the_query() {
    let fixture = repo_fixture("record-cap").await;
    for index in 0..3 {
        write_file(&fixture.root, &format!("file-{index}.txt"), b"x");
    }
    STATUS_CAPS.arm(
        fixture.workspace.root().to_path_buf(),
        StatusCaps {
            stdout_bytes: MAX_STATUS_STDOUT_BYTES,
            records: 1,
        },
    );

    let result = observe(&fixture.workspace).await;
    assert_eq!(result.untracked, 1);
    assert!(!result.complete);
    assert_eq!(
        result.warnings,
        vec![WorkspaceStatusWarning::OutputTruncated]
    );
}

#[tokio::test]
async fn the_encoded_budget_truncates_entries_without_losing_the_summary() {
    let fixture = repo_fixture("encoded-budget").await;
    for index in 0..40 {
        write_file(
            &fixture.root,
            &format!("a-rather-long-untracked-name-{index:02}.txt"),
            b"x",
        );
    }

    let result = request(&fixture.workspace, Some(MIN_RESULT_BYTES))
        .await
        .unwrap();
    assert_eq!(result.untracked, 40);
    assert!(result.entries.len() < 40);
    assert!(!result.complete);
    assert_eq!(
        result.warnings,
        vec![WorkspaceStatusWarning::OutputTruncated]
    );
    assert!(encoded_len(&result) <= MIN_RESULT_BYTES);
}

#[tokio::test]
async fn an_expired_deadline_reports_an_unavailable_observation() {
    let fixture = repo_fixture("deadline").await;
    STATUS_DEADLINES.arm(fixture.workspace.root().to_path_buf(), Duration::ZERO);

    let result = observe(&fixture.workspace).await;
    assert!(!result.repo_available);
    assert!(!result.complete);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::Deadline]);
    assert!(result.entries.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn a_cancelled_query_stops_and_reaps_the_owned_child() {
    let fixture = plain_fixture("cancel").await;
    let started = fixture.base.join("started");
    let script = fixture.base.join("slow-git");
    // The child reports that it started and then holds its pipes open long
    // enough that only a stopped process can return the query promptly.
    executable(
        &script,
        &format!("#!/bin/sh\n: > '{}'\nsleep 3\n", started.display()),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );

    let session = CancellationToken::new();
    let request = WorkspaceStatusRequest {
        session_id: SessionId::new().unwrap(),
        max_bytes: None,
    };
    let pending = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        let session = session.clone();
        async move {
            status(
                &workspace,
                &request,
                &session,
                &CancellationToken::new(),
                &CancellationToken::new(),
            )
            .await
        }
    });
    for _ in 0..200 {
        if started.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(started.exists(), "the query never started its child");
    session.cancel();
    let cancelled_at = Instant::now();
    let error = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("a cancelled query did not return")
        .expect("the query task panicked")
        .unwrap_err();
    assert!(matches!(error, AgentError::QueryLimit));
    // Returning before the script's sleep ends is only possible because the
    // owned child was stopped and reaped instead of being left behind.
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(2),
        "a cancelled query did not stop its git child"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_stderr_flood_stops_the_owned_child() {
    let fixture = plain_fixture("stderr").await;
    let survived = fixture.base.join("flood-finished");
    let script = fixture.base.join("loud-git");
    // A little stdout for the work tree probe, then far more standard error
    // than the count bound allows: a query must stop the child instead of
    // discarding without bound until the deadline. The trailing marker
    // proves the child was stopped while it still had output to write.
    executable(
        &script,
        &format!(
            "#!/bin/sh\ncase \"$*\" in\n  *rev-parse*) printf '%s\\n' '{}' ;;\nesac\ndd if=/dev/zero bs=1024 count=4096 1>&2 2>/dev/null\nsleep 30\n: > '{}'\n",
            fixture.root.display(),
            survived.display()
        ),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );

    let queried_at = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), observe(&fixture.workspace))
        .await
        .expect("a loud child stalled the query");
    assert!(!result.warnings.contains(&WorkspaceStatusWarning::Deadline));
    assert!(
        result
            .warnings
            .contains(&WorkspaceStatusWarning::OutputTruncated)
    );
    assert!(!result.complete);
    assert!(
        !survived.exists(),
        "a flooding child was allowed to run to completion"
    );
    assert!(queried_at.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn inherited_git_environment_variables_never_reach_git() {
    let fixture = repo_fixture("env").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    write_file(&fixture.root, "untracked.txt", b"x");
    let trace = fixture.base.join("git-trace.log");
    let mut env: Vec<(OsString, OsString)> = Vec::new();
    if let Some(path) = std::env::var_os("PATH") {
        env.push(("PATH".into(), path));
    }
    env.push((
        "GIT_DIR".into(),
        fixture.base.join("elsewhere").into_os_string(),
    ));
    env.push((
        "GIT_WORK_TREE".into(),
        fixture.base.clone().into_os_string(),
    ));
    env.push((
        "GIT_INDEX_FILE".into(),
        fixture.base.join("index").into_os_string(),
    ));
    env.push(("GIT_CONFIG_COUNT".into(), "1".into()));
    env.push(("GIT_CONFIG_KEY_0".into(), "core.fsmonitor".into()));
    env.push((
        "GIT_CONFIG_VALUE_0".into(),
        fixture.base.join("fsmonitor").into_os_string(),
    ));
    env.push(("GIT_TRACE".into(), trace.clone().into_os_string()));
    STATUS_ENVS.arm(fixture.workspace.root().to_path_buf(), env);

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert_eq!(result.untracked, 1);
    assert!(result.complete);
    assert!(!trace.exists(), "a GIT_* variable reached the git child");
}

#[cfg(unix)]
#[tokio::test]
async fn a_configured_fsmonitor_never_runs() {
    let fixture = repo_fixture("fsmonitor").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    write_file(&fixture.root, "untracked.txt", b"x");
    let marker = fixture.base.join("fsmonitor-ran");
    let script = fixture.base.join("fake-fsmonitor");
    executable(&script, &format!("#!/bin/sh\n: > '{}'\n", marker.display()));
    // A repository-local setting only: no global configuration is touched.
    git(
        &fixture.root,
        &["config", "core.fsmonitor", script.to_str().unwrap()],
    );
    let index = fixture.root.join(".git/index");
    let before = std::fs::read(&index).unwrap();
    let before_modified = std::fs::metadata(&index).unwrap().modified().unwrap();

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(result.complete);
    assert!(
        !marker.exists(),
        "a configured fsmonitor ran during a status query"
    );
    assert_eq!(std::fs::read(&index).unwrap(), before);
    assert_eq!(
        std::fs::metadata(&index).unwrap().modified().unwrap(),
        before_modified
    );
}

#[tokio::test]
async fn status_leaves_the_workspace_files_unchanged() {
    let fixture = repo_fixture("read-only").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    write_file(&fixture.root, "untracked.txt", b"x");
    let file = fixture.root.join("tracked.txt");
    let before = std::fs::read(&file).unwrap();
    let before_modified = std::fs::metadata(&file).unwrap().modified().unwrap();

    let _ = observe(&fixture.workspace).await;

    assert_eq!(std::fs::read(&file).unwrap(), before);
    assert_eq!(
        std::fs::metadata(&file).unwrap().modified().unwrap(),
        before_modified
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_child_that_closes_its_pipes_is_still_reaped_by_the_deadline() {
    let fixture = plain_fixture("pipe-close").await;
    let started = fixture.base.join("started");
    let script = fixture.base.join("closing-git");
    // The child closes both of its pipes and keeps running. Reaching the
    // deadline is only possible because the wait for the child is under the
    // same budget as the reads.
    executable(
        &script,
        &format!(
            "#!/bin/sh\nexec >&- 2>&-\n: > '{}'\nsleep 30\n",
            started.display()
        ),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );
    STATUS_DEADLINES.arm(
        fixture.workspace.root().to_path_buf(),
        Duration::from_millis(300),
    );

    let result = tokio::time::timeout(Duration::from_secs(5), observe(&fixture.workspace))
        .await
        .expect("a child that closed its pipes stalled the query");
    assert!(started.exists(), "the query never started its child");
    assert!(!result.repo_available);
    assert!(!result.complete);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::Deadline]);
}

#[cfg(unix)]
#[tokio::test]
async fn a_chunk_that_crosses_the_stdout_ceiling_keeps_its_prefix() {
    let fixture = plain_fixture("crossing").await;
    // One write far past the ceiling: the parseable prefix must survive
    // instead of the whole chunk being discarded.
    let payload: Vec<u8> = (0..200)
        .flat_map(|index| format!("? file-{index:03}.txt\0").into_bytes())
        .collect();
    let payload_path = fixture.base.join("payload");
    std::fs::write(&payload_path, &payload).unwrap();
    let script = fixture.base.join("chatty-git");
    executable(
        &script,
        &format!(
            "#!/bin/sh\ncase \"$*\" in\n  *rev-parse*) printf '%s\\n' '{}' ;;\n  *status*) cat '{}' ;;\nesac\n",
            fixture.root.display(),
            payload_path.display()
        ),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );
    STATUS_CAPS.arm(
        fixture.workspace.root().to_path_buf(),
        StatusCaps {
            stdout_bytes: 1024,
            records: MAX_STATUS_RECORDS,
        },
    );

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(!result.complete);
    assert_eq!(
        result.warnings,
        vec![WorkspaceStatusWarning::OutputTruncated]
    );
    assert!(
        result.untracked > 0,
        "the prefix of the chunk that crossed the ceiling was discarded"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_signal_death_is_a_failed_observation() {
    let fixture = plain_fixture("signalled").await;
    let script = fixture.base.join("killed-git");
    // The work tree probe answers, but the status command dies from a
    // signal: an empty answer is never reported as a clean workspace. Only
    // the standalone `status` argument dies; the fixed `-c` values that
    // contain the word status must not be mistaken for the subcommand.
    executable(
        &script,
        &format!(
            "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = status ]; then\n    kill -9 $$\n  fi\ndone\nprintf '%s\\n' '{}'\n",
            fixture.root.display()
        ),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert!(!result.complete);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
}

#[cfg(unix)]
#[tokio::test]
async fn an_unusable_repository_root_is_a_failed_observation() {
    let fixture = plain_fixture("bad-root").await;
    let script = fixture.base.join("fake-git");
    // The probe answers with a path that is not a directory, so no porcelain
    // path could be attributed to a work tree root.
    executable(
        &script,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' '{}'\n",
            fixture.base.join("nowhere").display()
        ),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );

    let result = observe(&fixture.workspace).await;
    assert!(!result.repo_available);
    assert!(!result.complete);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
}

#[cfg(unix)]
#[tokio::test]
async fn a_cancelled_budget_never_starts_a_process() {
    let fixture = plain_fixture("pre-cancel").await;
    let started = fixture.base.join("started");
    let script = fixture.base.join("marking-git");
    executable(
        &script,
        &format!("#!/bin/sh\n: > '{}'\n", started.display()),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );
    let session = CancellationToken::new();
    session.cancel();
    let request = WorkspaceStatusRequest {
        session_id: SessionId::new().unwrap(),
        max_bytes: None,
    };

    // Cancellation is checked before a process is started, so a cancelled
    // query cannot leave a child behind.
    assert!(matches!(
        status(
            &fixture.workspace,
            &request,
            &session,
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await,
        Err(AgentError::QueryLimit)
    ));
    assert!(!started.exists(), "a cancelled query started a git process");
}

#[cfg(unix)]
#[tokio::test]
async fn an_expired_budget_never_starts_a_process() {
    let fixture = plain_fixture("pre-expired").await;
    let started = fixture.base.join("started");
    let script = fixture.base.join("marking-git");
    executable(
        &script,
        &format!("#!/bin/sh\n: > '{}'\n", started.display()),
    );
    STATUS_PROGRAMS.arm(
        fixture.workspace.root().to_path_buf(),
        script.into_os_string(),
    );
    STATUS_DEADLINES.arm(fixture.workspace.root().to_path_buf(), Duration::ZERO);

    let result = observe(&fixture.workspace).await;
    assert!(!started.exists(), "an expired query started a git process");
    assert!(!result.repo_available);
    assert!(!result.complete);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::Deadline]);
}

#[tokio::test]
async fn a_stream_git_could_not_have_produced_is_not_a_complete_answer() {
    let fixture = repo_fixture("stream-safety").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    let mut stream = String::new();
    stream.push_str("# branch.oid 1111111111111111111111111111111111111111\0");
    stream.push_str("# branch.head main\0");
    // Codes outside git's documented set.
    stream.push_str(
            "1 ZZ N... 100644 100644 100644 1111111111111111111111111111111111111111 1111111111111111111111111111111111111111 tracked.txt\0",
        );
    // An object id that is not a hash.
    stream.push_str("# branch.oid zzzz\0");
    // A branch name beyond the bounded header size.
    stream.push_str(&format!(
        "# branch.head {}\0",
        "a".repeat(MAX_STATUS_BRANCH_BYTES + 1)
    ));
    // A rename record whose second path chunk never arrived.
    stream.push_str(
            "2 R. N... 100644 100644 100644 1111111111111111111111111111111111111111 1111111111111111111111111111111111111111 R100 tracked.txt\0",
        );

    let found = super::observe(
        stream.as_bytes(),
        &fixture.root,
        fixture.workspace.root(),
        StatusCaps::standard(),
    );
    assert!(found.entries.is_empty(), "{:?}", found.entries);
    assert_eq!(found.malformed, 4);
    assert_eq!(found.head_oid, None);
    assert!(found.branch_headers_complete());
}

#[cfg(unix)]
#[tokio::test]
async fn global_and_system_configuration_is_never_read() {
    let fixture = repo_fixture("global-config").await;
    write_file(&fixture.root, "tracked.txt", b"tracked\n");
    git(&fixture.root, &["add", "."]);
    git(&fixture.root, &["commit", "--quiet", "-m", "initial"]);
    write_file(&fixture.root, "untracked.txt", b"x");
    // A home directory whose configuration would both include another file
    // and start an external monitor, plus explicit system and command-line
    // overrides: none of it may reach the child or run.
    let home = fixture.base.join("home");
    std::fs::create_dir_all(home.join(".config/git")).unwrap();
    let marker = fixture.base.join("global-fsmonitor-ran");
    let monitor = fixture.base.join("global-fsmonitor");
    executable(
        &monitor,
        &format!("#!/bin/sh\n: > '{}'\n", marker.display()),
    );
    let included = fixture.base.join("included.config");
    std::fs::write(
        &included,
        format!("[core]\n\tfsmonitor = {}\n", monitor.display()),
    )
    .unwrap();
    std::fs::write(
        home.join(".gitconfig"),
        format!("[include]\n\tpath = {}\n", included.display()),
    )
    .unwrap();
    std::fs::write(
        home.join(".config/git/config"),
        format!("[core]\n\tfsmonitor = {}\n", monitor.display()),
    )
    .unwrap();
    let system = fixture.base.join("system.config");
    std::fs::write(
        &system,
        format!("[core]\n\tfsmonitor = {}\n", monitor.display()),
    )
    .unwrap();
    let mut env: Vec<(OsString, OsString)> = Vec::new();
    if let Some(path) = std::env::var_os("PATH") {
        env.push(("PATH".into(), path));
    }
    env.push(("HOME".into(), home.into_os_string()));
    env.push((
        "XDG_CONFIG_HOME".into(),
        fixture.base.join("xdg").into_os_string(),
    ));
    env.push((
        "GIT_CONFIG_GLOBAL".into(),
        fixture.base.join("override.config").into_os_string(),
    ));
    env.push(("GIT_CONFIG_SYSTEM".into(), system.into_os_string()));
    env.push(("GIT_CONFIG_COUNT".into(), "1".into()));
    env.push(("GIT_CONFIG_KEY_0".into(), "core.fsmonitor".into()));
    env.push((
        "GIT_CONFIG_VALUE_0".into(),
        monitor.clone().into_os_string(),
    ));
    STATUS_ENVS.arm(fixture.workspace.root().to_path_buf(), env);

    let result = observe(&fixture.workspace).await;
    assert!(result.repo_available);
    assert_eq!(result.untracked, 1);
    assert!(result.complete);
    assert!(
        !marker.exists(),
        "a global configuration started an external monitor"
    );
}

/// A branch name long enough that reserving its room would exceed a minimal
/// encoded budget: the answer is a status failure that fits, not an invalid
/// budget, and the caller never sees an argument error for a valid budget.
#[test]
fn a_long_branch_is_omitted_instead_of_failing_the_budget() {
    let observation = Observation {
        branch: Some("b".repeat(MAX_STATUS_BRANCH_BYTES)),
        saw_oid_header: true,
        saw_head_header: true,
        ..Observation::default()
    };

    let result = assemble(true, observation, Vec::new(), true, MIN_RESULT_BYTES, 0)
        .expect("a valid budget is never an argument error");
    assert!(result.repo_available);
    assert!(!result.complete);
    assert_eq!(result.branch, None);
    assert_eq!(result.warnings, vec![WorkspaceStatusWarning::StatusFailed]);
    assert!(encoded_len(&result) <= MIN_RESULT_BYTES);
}

#[tokio::test]
async fn every_cut_point_stays_within_the_encoded_budget() {
    let fixture = repo_fixture("budget-boundary").await;
    for index in 0..30 {
        write_file(&fixture.root, &format!("f-{index:02}.txt"), b"x");
    }
    let full = observe(&fixture.workspace).await;
    assert_eq!(full.untracked, 30);
    assert!(encoded_len(&full) > MIN_RESULT_BYTES);

    for budget in MIN_RESULT_BYTES..MIN_RESULT_BYTES + 64 {
        let result = request(&fixture.workspace, Some(budget)).await.unwrap();
        assert!(
            encoded_len(&result) <= budget,
            "the result exceeded its {budget} byte budget"
        );
        assert_eq!(result.untracked, 30);
        if result.entries.len() < 30 {
            assert!(!result.complete);
            assert!(
                result
                    .warnings
                    .contains(&WorkspaceStatusWarning::OutputTruncated)
            );
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn json_escapes_are_counted_against_the_budget() {
    let fixture = repo_fixture("budget-escapes").await;
    for index in 0..8 {
        write_file(
            &fixture.root,
            &format!("a \"quoted\" \\ backslash-{index:02}.txt"),
            b"x",
        );
    }

    for budget in MIN_RESULT_BYTES..MIN_RESULT_BYTES + 64 {
        let result = request(&fixture.workspace, Some(budget)).await.unwrap();
        assert!(
            encoded_len(&result) <= budget,
            "an escaped entry exceeded its {budget} byte budget"
        );
        assert_eq!(result.untracked, 8);
    }
}

/// Observes the Workspace's Git status through fixed, shell-free git arguments.
pub(crate) async fn status(
    workspace: &Workspace,
    request: &WorkspaceStatusRequest,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    child_cancellation: &CancellationToken,
) -> Result<WorkspaceStatusResult, AgentError> {
    let deadline = status_deadline_at(workspace.root());
    status_with_deadline(
        workspace,
        request,
        session_cancellation,
        shutdown_cancellation,
        child_cancellation,
        deadline,
    )
    .await
}
