use super::*;
use crate::workspace::scan::tests::ScanHold;
use crate::workspace::scan::{MAX_RESULT_BYTES, MIN_RESULT_BYTES};
use crate::workspace::scan::{SCAN_HOLDS, ScanLimits};

#[tokio::test]
async fn ignored_only_pages_advance_without_replaying_the_same_entries() {
    let mut fixture = fixture("ignored-pages").await;
    write(&fixture, ".gitignore", b"*.skip\n").await;
    for name in ["a.skip", "b.skip", "c.skip", "d.skip", "keep"] {
        write(&fixture, name, b"").await;
    }
    Arc::make_mut(&mut fixture.workspace).scan_limits.entries = 2;
    let mut req = request(&fixture);
    req.query = Some("keep".into());
    let (found, reason, complete) = collect_all(&fixture, req).await;
    assert_eq!(found, ["keep"]);
    assert_eq!(reason, WorkspaceScanStop::End);
    assert!(complete);
}

#[cfg(unix)]
#[tokio::test]
async fn an_unrepresentable_cursor_does_not_exceed_the_result_budget() {
    let mut fixture = fixture("large-cursor").await;
    let name = "\u{1b}".repeat(100);
    write(&fixture, &format!("{name}/{name}/file"), b"").await;
    Arc::make_mut(&mut fixture.workspace).scan_limits.entries = 2;
    let mut req = request(&fixture);
    req.query = Some("absent".into());
    req.recursive = Some(true);
    req.max_bytes = Some(MIN_RESULT_BYTES);
    let result = list(&fixture, req).await;
    assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
    assert!(result.next_cursor.is_none());
    assert!(!result.scan_complete);
    assert_eq!(result.stopped_by, WorkspaceScanStop::Entries);
}

#[tokio::test]
async fn atomic_write_artifacts_are_hidden_without_deleting_reserved_names() {
    let fixture = fixture("atomic-artifacts").await;
    write(&fixture, "value", b"before").await;
    write(&fixture, ".minicore-write-user.tmp", b"needle").await;
    let gate = Arc::new(crate::test_support::Gate::new());
    crate::workspace::BEFORE_RENAME_GATES
        .arm(fixture.workspace.root().join("value"), Arc::clone(&gate));
    let writer = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        async move { workspace.write_atomic("value", b"needle").await }
    });
    gate.wait_started().await;
    let listing = files(
        Arc::clone(&fixture.workspace),
        request(&fixture),
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await;
    let matches = crate::workspace::search::search(
        Arc::clone(&fixture.workspace),
        crate::workspace::search::WorkspaceSearchRequest {
            session_id: fixture.session,
            query: "needle".into(),
            paths: None,
            case_sensitive: None,
            cursor: None,
            max_matches: None,
            max_bytes: None,
        },
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await;
    let temps = std::fs::read_dir(fixture.workspace.root()).map(|entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".minicore-write-")
            })
            .count()
    });
    gate.release();
    writer.await.unwrap().unwrap();
    assert_eq!(
        temps.unwrap(),
        2,
        "the test must observe a live commit temp file"
    );
    let listing = listing.unwrap();
    assert_eq!(paths(&listing), ["value"]);
    assert!(listing.scan_complete);
    let matches = matches.unwrap();
    assert!(matches.matches.is_empty());
    assert!(matches.scan_complete);
    assert_eq!(
        tokio::fs::read(fixture.workspace.root().join(".minicore-write-user.tmp"))
            .await
            .unwrap(),
        b"needle"
    );
}

#[tokio::test]
async fn worktree_metadata_is_absent_and_pages_are_stable_under_insertions() {
    let fixture = fixture("path-cursor").await;
    write(&fixture, ".git", b"gitdir: /outside/metadata").await;
    write(&fixture, "b/nested", b"").await;
    write(&fixture, "d", b"").await;
    let mut req = request(&fixture);
    req.recursive = Some(true);
    req.limit = Some(1);
    let first = list(&fixture, req.clone()).await;
    assert_eq!(paths(&first), ["b"]);
    assert_eq!(first.skipped_count, 0);
    req.cursor = first.next_cursor;
    write(&fixture, "a", b"").await;
    write(&fixture, "c", b"").await;
    let (rest, reason, complete) = collect_all(&fixture, req).await;
    assert_eq!(rest, ["b/nested", "c", "d"]);
    assert_eq!(reason, WorkspaceScanStop::End);
    assert!(complete);
}

#[cfg(unix)]
#[tokio::test]
async fn intermediate_alias_uses_resolved_ignore_path() {
    let fixture = fixture("resolved-ignore").await;
    write(&fixture, "real/sub/file", b"").await;
    std::os::unix::fs::symlink("real", fixture.workspace.root().join("alias")).unwrap();
    let mut req = request(&fixture);
    req.directory = Some("alias/sub".into());
    assert_eq!(paths(&list(&fixture, req.clone()).await), ["real/sub/file"]);
    write(&fixture, ".gitignore", b"real/\n").await;
    assert!(list(&fixture, req).await.entries.is_empty());
}

struct Fixture {
    _dir: crate::test_support::TempDir,
    session: SessionId,
    workspace: Arc<Workspace>,
}

async fn fixture(label: &str) -> Fixture {
    let dir = crate::test_support::TempDir::new(label);
    let root = dir.path().join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root).await.unwrap());
    Fixture {
        _dir: dir,
        session: SessionId::new().unwrap(),
        workspace,
    }
}

async fn write(fixture: &Fixture, path: &str, bytes: impl AsRef<[u8]>) {
    let full = fixture.workspace.root().join(path);
    if let Some(parent) = full.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(full, bytes).await.unwrap();
}

fn request(fixture: &Fixture) -> WorkspaceFilesRequest {
    WorkspaceFilesRequest {
        session_id: fixture.session,
        directory: None,
        recursive: None,
        query: None,
        cursor: None,
        limit: None,
        max_bytes: None,
    }
}

async fn list(fixture: &Fixture, request: WorkspaceFilesRequest) -> WorkspaceFilesResult {
    files(
        Arc::clone(&fixture.workspace),
        request,
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .unwrap()
}

fn paths(result: &WorkspaceFilesResult) -> Vec<String> {
    let mut paths: Vec<String> = result
        .entries
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    paths.sort();
    paths
}

async fn collect_all(
    fixture: &Fixture,
    mut request: WorkspaceFilesRequest,
) -> (Vec<String>, WorkspaceScanStop, bool) {
    let budget = request.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
    let mut all = Vec::new();
    for _ in 0..64 {
        let result = list(fixture, request.clone()).await;
        let encoded = serde_json::to_vec(&result).unwrap().len();
        assert!(encoded <= budget, "encoded page {encoded} exceeds {budget}");
        all.extend(result.entries.iter().map(|entry| entry.path.clone()));
        match result.next_cursor {
            Some(cursor) => {
                assert!(!result.scan_complete);
                request.cursor = Some(cursor);
            }
            None => return (all, result.stopped_by, result.scan_complete),
        }
    }
    panic!("listing did not finish");
}

#[tokio::test]
async fn lists_one_level_by_default_and_recurses_on_request() {
    let fixture = fixture("levels").await;
    write(&fixture, "a.txt", b"a").await;
    write(&fixture, "sub/b.txt", b"bb").await;
    write(&fixture, "sub/deep/c.txt", b"ccc").await;
    write(&fixture, "other/d.txt", b"d").await;

    let result = list(&fixture, request(&fixture)).await;
    assert_eq!(result.directory, "");
    assert_eq!(paths(&result), vec!["a.txt", "other", "sub"]);
    assert!(result.scan_complete);
    assert_eq!(result.stopped_by, WorkspaceScanStop::End);
    assert!(!result.truncated);
    assert_eq!(result.skipped_count, 0);
    assert_eq!(result.consistency, WorkspaceScanConsistency::Live);
    assert!(result.observed_at_unix_ms > 0);
    let file = result
        .entries
        .iter()
        .find(|entry| entry.path == "a.txt")
        .unwrap();
    assert_eq!(file.kind, WorkspaceFileKind::File);
    assert_eq!(file.size, Some(1));
    let directory = result
        .entries
        .iter()
        .find(|entry| entry.path == "sub")
        .unwrap();
    assert_eq!(directory.kind, WorkspaceFileKind::Directory);
    assert_eq!(directory.size, None);

    let mut recursive = request(&fixture);
    recursive.recursive = Some(true);
    let result = list(&fixture, recursive.clone()).await;
    assert_eq!(
        paths(&result),
        vec![
            "a.txt",
            "other",
            "other/d.txt",
            "sub",
            "sub/b.txt",
            "sub/deep",
            "sub/deep/c.txt"
        ]
    );
    assert!(result.scan_complete);

    let mut nested = request(&fixture);
    nested.directory = Some("sub".to_owned());
    nested.recursive = Some(true);
    let result = list(&fixture, nested).await;
    assert_eq!(
        paths(&result),
        vec!["sub/b.txt", "sub/deep", "sub/deep/c.txt"]
    );
    assert_eq!(result.directory, "sub");
}

#[tokio::test]
async fn ignore_rules_and_git_metadata_are_respected() {
    let fixture = fixture("ignore").await;
    write(&fixture, ".gitignore", b"ignored.txt\ntarget/\n").await;
    write(&fixture, ".ignore", b"also.txt\n").await;
    write(&fixture, ".git/HEAD", b"ref: refs/heads/main\n").await;
    write(&fixture, ".git/objects/junk", b"junk").await;
    write(&fixture, "kept.txt", b"kept").await;
    write(&fixture, "ignored.txt", b"ignored").await;
    write(&fixture, "also.txt", b"ignored").await;
    write(&fixture, "target/build.log", b"log").await;
    write(&fixture, "sub/nested.txt", b"nested").await;
    write(&fixture, "sub/.gitignore", b"nested.txt\n").await;

    let mut recursive = request(&fixture);
    recursive.recursive = Some(true);
    let result = list(&fixture, recursive).await;
    assert_eq!(
        paths(&result),
        vec![".gitignore", ".ignore", "kept.txt", "sub", "sub/.gitignore"]
    );
    assert!(result.scan_complete);
}

#[tokio::test]
async fn gitignore_applies_without_a_git_repository() {
    let fixture = fixture("non-repo").await;
    write(&fixture, ".gitignore", b"skipped.txt\n").await;
    write(&fixture, "kept.txt", b"kept").await;
    write(&fixture, "skipped.txt", b"skipped").await;

    let result = list(&fixture, request(&fixture)).await;
    assert_eq!(paths(&result), vec![".gitignore", "kept.txt"]);
    assert!(result.scan_complete);
}

#[tokio::test]
async fn query_filters_entry_paths_and_never_file_contents() {
    let fixture = fixture("query").await;
    write(&fixture, "needle.txt", b"nothing here").await;
    write(&fixture, "other.txt", b"needle in content").await;
    write(&fixture, "needle/sub/x.txt", b"x").await;
    write(&fixture, "plain/y.txt", b"y").await;

    let mut filtered = request(&fixture);
    filtered.query = Some("needle".to_owned());
    filtered.recursive = Some(true);
    let result = list(&fixture, filtered).await;
    assert_eq!(
        paths(&result),
        vec!["needle", "needle.txt", "needle/sub", "needle/sub/x.txt"]
    );
    assert!(result.skipped_count > 0);
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_are_listed_but_never_followed_or_escaped() {
    use std::os::unix::fs::symlink;

    let outside = std::env::temp_dir().join(format!(
        "minicore-workspace-files-outside-{}",
        SessionId::new().unwrap()
    ));
    tokio::fs::create_dir_all(&outside).await.unwrap();
    tokio::fs::write(outside.join("secret.txt"), b"outside")
        .await
        .unwrap();
    let fixture = fixture("symlink").await;
    write(&fixture, "inside/real.txt", b"real").await;
    let root = fixture.workspace.root().to_path_buf();
    symlink(outside.join("secret.txt"), root.join("escape.txt")).unwrap();
    symlink(outside.clone(), root.join("escape-dir")).unwrap();
    symlink(root.join("inside/real.txt"), root.join("link.txt")).unwrap();
    symlink(root.join("inside"), root.join("inside-link")).unwrap();

    let mut recursive = request(&fixture);
    recursive.recursive = Some(true);
    let result = list(&fixture, recursive).await;
    assert_eq!(
        paths(&result),
        vec![
            "escape-dir",
            "escape.txt",
            "inside",
            "inside-link",
            "inside/real.txt",
            "link.txt"
        ]
    );
    let linked = result
        .entries
        .iter()
        .find(|entry| entry.path == "escape-dir")
        .unwrap();
    assert_eq!(linked.kind, WorkspaceFileKind::Symlink);
    assert_eq!(linked.size, None);

    // A requested directory that resolves outside the workspace, and a
    // requested root that is itself a symlinked directory, are rejected
    // instead of being walked.
    for directory in ["escape-dir", "inside-link"] {
        let mut escaped = request(&fixture);
        escaped.directory = Some(directory.to_owned());
        let error = files(
            Arc::clone(&fixture.workspace),
            escaped,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::Workspace), "{directory}");
    }
    let _ = tokio::fs::remove_dir_all(&outside).await;
}

#[tokio::test]
async fn an_expired_budget_returns_no_entries_and_no_cursor() {
    let mut fixture = fixture("deadline-empty").await;
    write(&fixture, "one.txt", b"x").await;
    Arc::make_mut(&mut fixture.workspace).scan_deadline = std::time::Duration::ZERO;

    let result = list(&fixture, request(&fixture)).await;
    assert!(result.entries.is_empty());
    assert_eq!(result.stopped_by, WorkspaceScanStop::Deadline);
    assert!(result.truncated);
    assert!(!result.scan_complete);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn pagination_is_lossless_and_every_page_fits_its_budget() {
    let fixture = fixture("paging").await;
    let mut expected = Vec::new();
    for index in 0..40 {
        let path = format!("dir-{:02}/file-{index:02}.txt", index % 5);
        write(&fixture, &path, b"x").await;
        expected.push(path);
    }
    write(&fixture, "dir-00/.gitignore", b"nothing\n").await;
    expected.push("dir-00/.gitignore".to_owned());
    for index in 0..5 {
        expected.push(format!("dir-{index:02}"));
    }
    expected.sort();

    let mut recursive = request(&fixture);
    recursive.recursive = Some(true);
    recursive.limit = Some(7);
    recursive.max_bytes = Some(2048);
    let (collected, stopped_by, complete) = collect_all(&fixture, recursive).await;
    let mut collected = collected;
    collected.sort();
    assert_eq!(stopped_by, WorkspaceScanStop::End);
    assert!(complete);
    assert_eq!(collected, expected);
}

#[tokio::test]
async fn one_entry_pages_replay_directories_and_ignored_entries() {
    let fixture = fixture("dfs-paging").await;
    write(&fixture, ".gitignore", b"skip.txt\n").await;
    write(&fixture, "a/one.txt", b"1").await;
    write(&fixture, "a/skip.txt", b"2").await;
    write(&fixture, "a/b/two.txt", b"3").await;
    write(&fixture, "a/b/three.txt", b"4").await;
    write(&fixture, "c/four.txt", b"5").await;
    write(&fixture, "top.txt", b"6").await;

    let mut request = request(&fixture);
    request.recursive = Some(true);
    request.limit = Some(1);
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let mut cursor = None;
    let mut all: Vec<String> = Vec::new();
    for _ in 0..64 {
        let mut page = request.clone();
        page.cursor = cursor.clone();
        let result = list(&fixture, page).await;
        assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
        all.extend(result.entries.iter().map(|entry| entry.path.clone()));
        match result.next_cursor {
            Some(next) => {
                assert!(!result.scan_complete);
                cursor = Some(next);
            }
            None => {
                assert!(result.scan_complete);
                assert_eq!(result.stopped_by, WorkspaceScanStop::End);
                break;
            }
        }
    }
    // No entry is returned twice, and a directory is always returned
    // before the entries inside it, which is only true when a resumed page
    // descends into the directories it positioned past.
    let mut unique = all.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), all.len(), "an entry was returned twice");
    for (index, path) in all.iter().enumerate() {
        for earlier in &all[..index] {
            assert!(
                !earlier.starts_with(&format!("{path}/")),
                "{earlier} was returned before its directory {path}"
            );
        }
    }
    let mut collected = all;
    collected.sort();
    assert_eq!(
        collected,
        vec![
            ".gitignore",
            "a",
            "a/b",
            "a/b/three.txt",
            "a/b/two.txt",
            "a/one.txt",
            "c",
            "c/four.txt",
            "top.txt"
        ]
    );
}

#[tokio::test]
async fn entry_and_depth_ceilings_are_reported_not_hidden() {
    let mut ceilings = fixture("ceilings").await;
    for index in 0..8 {
        write(&ceilings, &format!("file-{index}.txt"), b"x").await;
    }
    Arc::make_mut(&mut ceilings.workspace).scan_limits = ScanLimits {
        entries: 3,
        bytes: 1 << 20,
        depth: 64,
        rule_files: 8,
        rule_bytes: 1 << 20,
    };
    let mut recursive = request(&ceilings);
    recursive.recursive = Some(true);
    let first = list(&ceilings, recursive.clone()).await;
    assert_eq!(first.stopped_by, WorkspaceScanStop::Entries);
    assert_eq!(first.entries.len(), 3);
    assert!(first.truncated);
    assert!(!first.scan_complete);
    assert!(first.next_cursor.is_some());
    let (collected, _, complete) = collect_all(&ceilings, recursive).await;
    assert_eq!(collected.len(), 8);
    assert!(complete);

    let mut deep = fixture("depth").await;
    write(&deep, "a/b/c/d/e.txt", b"deep").await;
    Arc::make_mut(&mut deep.workspace).scan_limits = ScanLimits {
        entries: 1000,
        bytes: 1 << 20,
        depth: 2,
        rule_files: 8,
        rule_bytes: 1 << 20,
    };
    let mut recursive = request(&deep);
    recursive.recursive = Some(true);
    let result = list(&deep, recursive).await;
    // The depth ceiling descends two levels below the requested root.
    assert_eq!(paths(&result), vec!["a", "a/b"]);
    assert_eq!(result.stopped_by, WorkspaceScanStop::Depth);
    assert!(result.truncated);
    assert!(!result.scan_complete);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn result_budget_and_validation_are_enforced() {
    let fixture = fixture("budget").await;
    for index in 0..20 {
        write(
            &fixture,
            &format!("a-rather-long-name-for-budget-{index:02}.txt"),
            b"x",
        )
        .await;
    }
    let mut tight = request(&fixture);
    tight.limit = Some(20);
    tight.max_bytes = Some(MIN_RESULT_BYTES);
    let result = list(&fixture, tight.clone()).await;
    assert!(result.truncated);
    assert_eq!(result.stopped_by, WorkspaceScanStop::Page);
    assert!(!result.entries.is_empty());
    assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
    let (collected, stopped_by, complete) = collect_all(&fixture, tight).await;
    let mut collected = collected;
    collected.sort();
    let mut expected: Vec<String> = (0..20)
        .map(|index| format!("a-rather-long-name-for-budget-{index:02}.txt"))
        .collect();
    expected.sort();
    assert_eq!(stopped_by, WorkspaceScanStop::End);
    assert!(complete);
    assert_eq!(collected, expected);

    for directory in ["", ".", "..", "../escape", "/etc", "a/../../b"] {
        let mut request = request(&fixture);
        request.directory = Some(directory.to_owned());
        if directory.is_empty() {
            assert!(request.validate().is_ok(), "{directory:?}");
        } else {
            assert!(matches!(
                request.validate(),
                Err(AgentError::InvalidArguments)
            ));
        }
    }
    let mut long = request(&fixture);
    long.directory = Some("a".repeat(MAX_PATH_BYTES + 1));
    assert!(matches!(long.validate(), Err(AgentError::InvalidArguments)));
    let mut limit = request(&fixture);
    limit.limit = Some(0);
    assert!(matches!(
        limit.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut limit = request(&fixture);
    limit.limit = Some(MAX_LIMIT + 1);
    assert!(matches!(
        limit.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut budget = request(&fixture);
    budget.max_bytes = Some(MIN_RESULT_BYTES - 1);
    assert!(matches!(
        budget.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut budget = request(&fixture);
    budget.max_bytes = Some(MAX_RESULT_BYTES + 1);
    assert!(matches!(
        budget.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut query = request(&fixture);
    query.query = Some("two\nlines".to_owned());
    assert!(matches!(
        query.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut cursor = request(&fixture);
    cursor.cursor = Some(WorkspaceListCursor {
        path: String::new(),
        scope: "not-a-scope".to_owned(),
    });
    assert!(matches!(
        cursor.validate(),
        Err(AgentError::InvalidArguments)
    ));
}

#[tokio::test]
async fn an_entry_that_cannot_fit_an_empty_page_is_skipped_and_incomplete() {
    let fixture = fixture("unrepresentable").await;
    // Every entry of this directory has a path longer than an empty page
    // can hold, so each one is skipped and counted.
    let deep = (0..8)
        .map(|index| format!("level-{index:02}-{}", "x".repeat(58)))
        .collect::<Vec<_>>()
        .join("/");
    write(&fixture, &format!("{deep}/one.txt"), b"x").await;
    write(&fixture, &format!("{deep}/two.txt"), b"x").await;
    let mut tight = request(&fixture);
    tight.directory = Some(deep);
    tight.recursive = Some(true);
    tight.max_bytes = Some(MIN_RESULT_BYTES);
    let result = list(&fixture, tight).await;
    assert!(result.entries.is_empty());
    assert_eq!(result.skipped_count, 2);
    assert!(result.truncated);
    assert!(!result.scan_complete);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn a_cursor_from_another_request_is_rejected() {
    let fixture = fixture("cursor-mismatch").await;
    write(&fixture, "a.txt", b"a").await;
    write(&fixture, "b.txt", b"b").await;
    let mut request = request(&fixture);
    request.limit = Some(1);
    let first = list(&fixture, request.clone()).await;
    let cursor = first.next_cursor.expect("a cut page continues");

    let mut mismatched = request.clone();
    mismatched.cursor = Some(cursor.clone());
    mismatched.query = Some("b".to_owned());
    let error = files(
        Arc::clone(&fixture.workspace),
        mismatched,
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::InvalidArguments));

    // The cursor is still valid for the request that produced it. The
    // ordinal rule is only exact while the tree is unchanged, so this page
    // is a fresh live observation that may reflect the deletion.
    tokio::fs::remove_file(fixture.workspace.root().join("a.txt"))
        .await
        .unwrap();
    let mut resumed = request;
    resumed.cursor = Some(cursor);
    let second = list(&fixture, resumed).await;
    assert_eq!(second.consistency, WorkspaceScanConsistency::Live);
    assert!(paths(&second).iter().all(|path| path != "a.txt"));
}

#[tokio::test]
async fn a_held_scan_finishes_after_release() {
    let fixture = fixture("hold").await;
    write(&fixture, "a.txt", b"a").await;
    let hold = Arc::new(ScanHold::new());
    SCAN_HOLDS.arm(fixture.workspace.root().to_path_buf(), Arc::clone(&hold));
    let pending = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        let request = request(&fixture);
        async move {
            files(
                workspace,
                request,
                CancellationToken::new(),
                CancellationToken::new(),
            )
            .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), hold.wait_started())
        .await
        .expect("the held scan did not start");
    hold.release();
    let result = pending.await.unwrap().unwrap();
    assert_eq!(paths(&result), vec!["a.txt"]);
    assert!(result.scan_complete);
}

#[tokio::test]
async fn a_cancelled_session_stops_the_scan() {
    let fixture = fixture("cancel").await;
    write(&fixture, "a.txt", b"a").await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = files(
        Arc::clone(&fixture.workspace),
        request(&fixture),
        cancelled,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::QueryLimit));
}
