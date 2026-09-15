use super::*;
use crate::changes::{ChangeRevision, workspace_records};
use crate::diff::{ChangesDiffRequest, DiffComparison, workspace_diff};

struct Fixture {
    base: PathBuf,
    workspace: Workspace,
    session: SessionId,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}
impl Fixture {
    async fn new() -> Self {
        let session = SessionId::new().unwrap();
        let base = std::env::temp_dir().join(format!("minicore-review-{session}"));
        std::fs::create_dir_all(base.join("repo")).unwrap();
        let workspace = Workspace::open(base.join("repo")).await.unwrap();
        let fixture = Self {
            base,
            workspace,
            session,
        };
        fixture.git(&["init", "-q"]);
        fixture.git(&["config", "user.name", "Test"]);
        fixture.git(&["config", "user.email", "test@example.invalid"]);
        fixture
    }
    fn git(&self, args: &[&str]) {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(self.base.join("repo"))
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git fixture failed: {:?}",
            output.status
        );
    }
    async fn request(&self, path: &str) -> ChangesDiffRequest {
        let token = CancellationToken::new();
        let status = status(
            &self.workspace,
            &WorkspaceStatusRequest {
                session_id: self.session,
                max_bytes: Some(262144),
            },
            &token,
            &token,
            &token,
        )
        .await
        .unwrap();
        let (records, _, _) = workspace_records(self.session, &status);
        let record = records
            .into_iter()
            .find(|record| record.path == path)
            .unwrap();
        ChangesDiffRequest {
            session_id: self.session,
            change_ref: record.change_ref,
            comparison: None,
            context_lines: Some(3),
            cursor: None,
            max_bytes: Some(2048),
        }
    }
    async fn sources(&self, request: &ChangesDiffRequest) -> crate::diff::DiffSources {
        let token = CancellationToken::new();
        diff_sources(
            &self.workspace,
            request,
            &token,
            &token,
            &token,
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn raw_git_diffs_cover_staged_unstaged_and_untracked() {
    let fixture = Fixture::new().await;
    let path = fixture.workspace.root().join("value.txt");
    std::fs::write(&path, b"head\r\n").unwrap();
    fixture.git(&["add", "value.txt"]);
    fixture.git(&["commit", "-qm", "base"]);
    std::fs::write(&path, b"index\r\n").unwrap();
    fixture.git(&["add", "value.txt"]);
    let sources = fixture.sources(&fixture.request("value.txt").await).await;
    assert_eq!(sources.base.comparison, DiffComparison::HeadToIndex);
    let (before, after) = sources.bytes.unwrap();
    assert_eq!(&*before, b"head\r\n");
    assert_eq!(&*after, b"index\r\n");
    std::fs::write(&path, b"worktree\r").unwrap();
    let sources = fixture.sources(&fixture.request("value.txt").await).await;
    assert_eq!(sources.base.comparison, DiffComparison::IndexToWorktree);
    let (before, after) = sources.bytes.unwrap();
    assert_eq!(&*before, b"index\r\n");
    assert_eq!(&*after, b"worktree\r");
    let mut explicit = fixture.request("value.txt").await;
    explicit.comparison = Some(DiffComparison::HeadToIndex);
    let sources = fixture.sources(&explicit).await;
    assert_eq!(&*sources.bytes.unwrap().1, b"index\r\n");
    explicit.comparison = Some(DiffComparison::HeadToWorktree);
    let sources = fixture.sources(&explicit).await;
    let (before, after) = sources.bytes.unwrap();
    assert_eq!(&*before, b"head\r\n");
    assert_eq!(&*after, b"worktree\r");
    std::fs::write(fixture.workspace.root().join("new.txt"), b"new").unwrap();
    let sources = fixture.sources(&fixture.request("new.txt").await).await;
    assert_eq!(sources.base.base_version, ChangeRevision::Missing);
    assert_eq!(&*sources.bytes.unwrap().1, b"new");
}

#[tokio::test]
async fn workspace_pagination_refreshes_versions_and_rejects_changed_content() {
    let fixture = Fixture::new().await;
    let path = fixture.workspace.root().join("long.txt");
    std::fs::write(&path, b"old\n").unwrap();
    fixture.git(&["add", "long.txt"]);
    fixture.git(&["commit", "-qm", "base"]);
    std::fs::write(&path, "é\"\\\r\n".repeat(1500)).unwrap();
    let request = fixture.request("long.txt").await;
    let store = crate::store::Store::open(fixture.base.join("store"))
        .await
        .unwrap();
    let sources = fixture.sources(&request).await;
    let first = workspace_diff(
        store.clone(),
        request.clone(),
        sources,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(first.versions_refreshed);
    assert!(!first.stale);
    assert!(serde_json::to_vec(&first).unwrap().len() <= 2048);
    let mut next = request.clone();
    next.cursor = Some(first.next_cursor.unwrap());
    // Same Git XY code and same line lengths, but different content.
    std::fs::write(&path, "界\"\\\r\n".repeat(1500)).unwrap();
    let sources = fixture.sources(&next).await;
    let stale = workspace_diff(
        store.clone(),
        next,
        sources,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(stale.stale);
    assert!(stale.hunks.is_empty());
    assert_ne!(stale.target_version, first.target_version);
    store.shutdown_diff_workers().await;
}

#[tokio::test]
async fn nested_literal_paths_do_not_read_parent_files_or_run_filters() {
    let mut fixture = Fixture::new().await;
    let repo = fixture.base.join("repo");
    std::fs::create_dir(repo.join("sub")).unwrap();
    std::fs::write(repo.join("outside"), b"secret").unwrap();
    std::fs::write(repo.join("sub/[literal].txt"), b"old").unwrap();
    fixture.git(&["add", "."]);
    fixture.git(&["commit", "-qm", "base"]);
    std::fs::write(repo.join("sub/[literal].txt"), b"new").unwrap();
    fixture.workspace = Workspace::open(repo.join("sub")).await.unwrap();
    let request = fixture.request("[literal].txt").await;
    // Configure after listing: even a clean conversion must not execute during diff.
    fixture.git(&["config", "filter.test.clean", "touch FILTER_RAN; cat"]);
    std::fs::write(repo.join(".gitattributes"), b"* filter=test\n").unwrap();
    let sources = fixture.sources(&request).await;
    let (before, after) = sources.bytes.unwrap();
    assert_eq!(&*before, b"old");
    assert_eq!(&*after, b"new");
    assert!(!repo.join("FILTER_RAN").exists());
    assert!(!repo.join("sub/FILTER_RAN").exists());
    let (head, mut entry) =
        crate::changes::parse_workspace_ref(&request.change_ref, fixture.session).unwrap();
    entry.path = "../outside".to_owned();
    use base64::Engine;
    let bytes = serde_json::to_vec(&(fixture.session, head, entry)).unwrap();
    let invalid = format!(
        "workspace:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    );
    assert!(crate::changes::parse_workspace_ref(&invalid, fixture.session).is_err());
}

#[tokio::test]
async fn unborn_staged_addition_and_deleted_worktree_have_missing_sides() {
    let fixture = Fixture::new().await;
    let path = fixture.workspace.root().join("value.txt");
    std::fs::write(&path, b"new").unwrap();
    fixture.git(&["add", "value.txt"]);
    let sources = fixture.sources(&fixture.request("value.txt").await).await;
    assert_eq!(sources.base.base_version, ChangeRevision::Missing);
    assert_eq!(&*sources.bytes.unwrap().1, b"new");
    std::fs::remove_file(path).unwrap();
    let sources = fixture.sources(&fixture.request("value.txt").await).await;
    assert_eq!(sources.base.target_version, ChangeRevision::Missing);
    assert_eq!(&*sources.bytes.unwrap().0, b"new");
}

#[tokio::test]
async fn merge_conflicts_are_explicitly_unavailable() {
    let fixture = Fixture::new().await;
    let path = fixture.workspace.root().join("value.txt");
    std::fs::write(&path, b"base\n").unwrap();
    fixture.git(&["add", "."]);
    fixture.git(&["commit", "-qm", "base"]);
    fixture.git(&["checkout", "-qb", "other"]);
    std::fs::write(&path, b"other\n").unwrap();
    fixture.git(&["commit", "-qam", "other"]);
    fixture.git(&["checkout", "-q", "-"]);
    std::fs::write(&path, b"main\n").unwrap();
    fixture.git(&["commit", "-qam", "main"]);
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(fixture.workspace.root())
        .args(["merge", "other"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let sources = fixture.sources(&fixture.request("value.txt").await).await;
    assert_eq!(sources.base.kind, crate::ChangeKind::Conflict);
    assert!(sources.bytes.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_diff_cancellation_reaps_the_git_child() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.root().join("new.txt"), b"new").unwrap();
    let request = fixture.request("new.txt").await;
    let program = fixture.base.join("slow-git");
    let started = fixture.base.join("started");
    std::fs::write(
        &program,
        format!(
            "#!/bin/sh\nprintf '%s' $$ > '{}'\nexec sleep 30\n",
            started.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    set_status_program(fixture.workspace.root().to_path_buf(), program);
    let cancellation = CancellationToken::new();
    let worker_cancel = cancellation.clone();
    let workspace = Workspace::open(fixture.workspace.root().to_path_buf())
        .await
        .unwrap();
    let handle = tokio::spawn(async move {
        diff_sources(
            &workspace,
            &request,
            &CancellationToken::new(),
            &CancellationToken::new(),
            &worker_cancel,
            Instant::now() + Duration::from_secs(10),
        )
        .await
    });
    for _ in 0..200 {
        if started.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(started.exists());
    let pid: i32 = std::fs::read_to_string(&started).unwrap().parse().unwrap();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(AgentError::QueryLimit)));
    assert_eq!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
        Err(nix::errno::Errno::ESRCH)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_content_is_not_returned_as_a_regular_file_diff() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.root().join("secret"), b"private").unwrap();
    std::os::unix::fs::symlink("secret", fixture.workspace.root().join("link")).unwrap();
    let sources = fixture.sources(&fixture.request("link").await).await;
    assert!(sources.bytes.is_none());
}
