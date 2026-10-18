use super::*;
use crate::ids::SessionId;

async fn fixture(label: &str) -> (Arc<Workspace>, PathBuf) {
    let base = std::env::temp_dir().join(format!(
        "minicore-workspace-scan-{label}-{}",
        SessionId::new().unwrap()
    ));
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root).await.unwrap());
    (workspace, base)
}

fn write(workspace: &Workspace, path: &str, bytes: &[u8]) {
    let full = workspace.root().join(path);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(full, bytes).unwrap();
}

fn scan_budget(workspace: &Workspace) -> ScanBudget {
    ScanBudget {
        limits: workspace.scan_limits,
        entries: 0,
        bytes: 0,
        rule_files: 0,
        rule_bytes: 0,
        rules_exhausted: false,
        stop: CancellationToken::new(),
        session_cancellation: CancellationToken::new(),
        shutdown_cancellation: CancellationToken::new(),
        deadline: Instant::now() + WORKSPACE_SCAN_DEADLINE,
        expired: false,
    }
}

fn walk(workspace: &Workspace, start_entry: Option<&str>) -> (WalkOutcome, Vec<String>) {
    let mut budget = scan_budget(workspace);
    let mut seen = Vec::new();
    let outcome = walk_scope(
        workspace,
        "",
        WalkOptions {
            recursive: true,
            want_size: false,
            directory_only: false,
            resume_file: false,
        },
        start_entry,
        &mut budget,
        |entry, _| {
            seen.push(entry.relative.clone());
            Ok(Visit::Next)
        },
    )
    .unwrap();
    let entries = budget.entries;
    assert!(entries >= seen.len() as u64);
    (outcome, seen)
}

#[tokio::test]
async fn a_deadline_keeps_the_partial_walk_and_drops_the_continuation() {
    let (workspace, base) = fixture("deadline").await;
    write(&workspace, "a.txt", b"a");
    write(&workspace, "b.txt", b"b");

    let mut budget = scan_budget(&workspace);
    let mut seen = Vec::new();
    let outcome = walk_scope(
        &workspace,
        "",
        WalkOptions {
            recursive: true,
            want_size: false,
            directory_only: false,
            resume_file: false,
        },
        None,
        &mut budget,
        |entry, budget| {
            seen.push(entry.relative.clone());
            // The deadline passes right after the first visited entry.
            budget.expired = true;
            Ok(Visit::Next)
        },
    )
    .unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(outcome.reason, WorkspaceScanStop::Deadline);
    assert!(outcome.next.is_none());
    assert!(outcome.resume.is_none());
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn a_cancelled_session_fails_the_scan_and_exits_the_worker() {
    let (workspace, _base) = fixture("deadline-cancel").await;
    let hold = Arc::new(ScanHold::new());
    SCAN_HOLDS.arm(workspace.root().to_path_buf(), Arc::clone(&hold));
    let session = CancellationToken::new();
    let pending = tokio::spawn({
        let workspace = Arc::clone(&workspace);
        let session = session.clone();
        async move { run_scan(workspace, session, CancellationToken::new(), |_, _| Ok(())).await }
    });
    tokio::time::timeout(Duration::from_secs(5), hold.wait_started())
        .await
        .expect("gated worker did not start");

    // Cancellation is not a partial result: the query fails and the worker
    // is joined instead of being left behind.
    session.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("a cancelled scan did not return")
        .expect("the scan task panicked");
    assert!(matches!(result, Err(AgentError::QueryLimit)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !hold.exited() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        hold.exited(),
        "a cancelled query left its blocking worker behind"
    );
}

#[tokio::test]
async fn cancellation_does_not_wait_forever_for_a_stalled_filesystem_worker() {
    let (workspace, base) = fixture("stalled-worker").await;
    let gate = Arc::new(crate::test_support::Gate::new());
    let cancellation = CancellationToken::new();
    let (exited, exit) = tokio::sync::oneshot::channel();
    let pending = tokio::spawn({
        let gate = Arc::clone(&gate);
        let cancellation = cancellation.clone();
        async move {
            run_scan(
                workspace,
                cancellation,
                CancellationToken::new(),
                move |_, _| {
                    gate.pass_blocking();
                    let _ = exited.send(());
                    Ok(())
                },
            )
            .await
        }
    });
    gate.wait_started().await;
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), pending).await;
    gate.release();
    exit.await.unwrap();
    assert!(matches!(
        result.unwrap().unwrap(),
        Err(AgentError::QueryLimit)
    ));
    tokio::fs::remove_dir_all(base).await.unwrap();
}

#[tokio::test]
async fn dropping_the_awaiter_cancels_the_worker() {
    let (workspace, _base) = fixture("drop-guard").await;
    let hold = Arc::new(ScanHold::new());
    SCAN_HOLDS.arm(workspace.root().to_path_buf(), Arc::clone(&hold));
    let pending = tokio::spawn({
        let workspace = Arc::clone(&workspace);
        async move {
            run_scan(
                workspace,
                CancellationToken::new(),
                CancellationToken::new(),
                |_, _| Ok(()),
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), hold.wait_started())
        .await
        .expect("gated worker did not start");

    // Dropping the awaiting future must cancel the worker through the drop
    // guard instead of detaching it. The worker is cancelled before it can
    // run the scan closure, so the signal comes from the worker scope.
    pending.abort();
    let _ = pending.await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !hold.exited() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        hold.exited(),
        "a dropped query left its blocking worker behind"
    );
}

#[tokio::test]
async fn nested_rules_inherit_and_git_metadata_is_never_walked() {
    let (workspace, base) = fixture("rules").await;
    write(&workspace, ".gitignore", b"root-only.txt\ntarget/\n");
    write(&workspace, ".git/HEAD", b"ref: refs/heads/main\n");
    write(&workspace, "root-only.txt", b"x");
    write(&workspace, "kept.txt", b"x");
    write(&workspace, "target/junk.txt", b"x");
    write(&workspace, "sub/.gitignore", b"nested.txt\n!keep.log\n");
    write(&workspace, "sub/nested.txt", b"x");
    write(&workspace, "sub/keep.txt", b"x");
    write(&workspace, "sub/keep.log", b"x");

    let (outcome, mut seen) = walk(&workspace, None);
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ".gitignore",
            "kept.txt",
            "sub",
            "sub/.gitignore",
            "sub/keep.log",
            "sub/keep.txt"
        ]
    );
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn rule_categories_precede_depth_and_rules_never_leak() {
    let (workspace, base) = fixture("rule-categories").await;
    // Category precedence: an `.ignore` at the root outranks a nested
    // `.gitignore`, so the nested whitelist cannot resurrect the path.
    write(&workspace, ".ignore", b"keep-out\n");
    write(&workspace, "sub/.gitignore", b"!keep-out\nsibling-only\n");
    write(&workspace, "sub/keep-out", b"x");
    write(&workspace, "keep-out", b"x");
    write(&workspace, "sibling-only", b"x");
    // A parent pattern that excludes a directory cannot be undone from
    // inside it, and a rule file inside that directory is never read.
    write(&workspace, ".gitignore", b"blocked/\n");
    write(&workspace, "blocked/.gitignore", b"!*\n");
    write(&workspace, "blocked/inside.txt", b"x");
    write(&workspace, "sub/kept.txt", b"x");

    let (outcome, mut seen) = walk(&workspace, None);
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ".gitignore",
            ".ignore",
            "sibling-only",
            "sub",
            "sub/.gitignore",
            "sub/kept.txt"
        ]
    );

    // A requested subdirectory sees exactly the entries the whole walk
    // reports below it.
    let mut budget = scan_budget(&workspace);
    let mut nested = Vec::new();
    let outcome = walk_scope(
        &workspace,
        "sub",
        WalkOptions {
            recursive: true,
            want_size: false,
            directory_only: false,
            resume_file: false,
        },
        None,
        &mut budget,
        |entry, _| {
            nested.push(entry.relative.clone());
            Ok(Visit::Next)
        },
    )
    .unwrap();
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    nested.sort();
    let whole: Vec<String> = seen
        .iter()
        .filter(|path| path.starts_with("sub/"))
        .cloned()
        .collect();
    assert_eq!(nested, whole);
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn a_subdirectory_root_inherits_ancestor_rules() {
    let (workspace, base) = fixture("subdir").await;
    write(&workspace, ".gitignore", b"root-only.txt\n");
    write(&workspace, "sub/root-only.txt", b"x");
    write(&workspace, "sub/kept.txt", b"x");

    let mut budget = scan_budget(&workspace);
    let mut seen = Vec::new();
    let outcome = walk_scope(
        &workspace,
        "sub",
        WalkOptions {
            recursive: true,
            want_size: false,
            directory_only: false,
            resume_file: false,
        },
        None,
        &mut budget,
        |entry, _| {
            seen.push(entry.relative.clone());
            Ok(Visit::Next)
        },
    )
    .unwrap();
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    assert_eq!(seen, vec!["sub/kept.txt"]);
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn an_explicit_root_is_never_filtered_by_its_own_rules() {
    let (workspace, base) = fixture("root-rules").await;
    // The pattern matches the requested root's ancestor names, so only a
    // strict-descendant rule application leaves the root usable.
    write(&workspace, "sub/below/.gitignore", b"sub\nbelow\n");
    write(&workspace, "sub/below/file.txt", b"x");

    let mut budget = scan_budget(&workspace);
    let mut seen = Vec::new();
    let outcome = walk_scope(
        &workspace,
        "sub/below",
        WalkOptions {
            recursive: true,
            want_size: false,
            directory_only: false,
            resume_file: false,
        },
        None,
        &mut budget,
        |entry, _| {
            seen.push(entry.relative.clone());
            Ok(Visit::Next)
        },
    )
    .unwrap();
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    // The rule file is a normal visible entry: the test is about the rules
    // not filtering the root or its ancestors, not about hiding the file.
    seen.sort();
    assert_eq!(seen, vec!["sub/below/.gitignore", "sub/below/file.txt"]);
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn resuming_does_not_spend_the_entry_budget_on_previous_pages() {
    let (mut workspace, base) = fixture("positioning").await;
    for index in 0..10 {
        write(&workspace, &format!("file-{index}.txt"), b"x");
    }
    Arc::make_mut(&mut workspace).scan_limits = ScanLimits {
        entries: 4,
        bytes: 1 << 20,
        depth: 8,
        rule_files: 8,
        rule_bytes: 1 << 20,
    };
    let (outcome, seen) = walk(&workspace, None);
    assert_eq!(outcome.reason, WorkspaceScanStop::Entries);
    assert_eq!(outcome.next.as_deref(), Some("file-3.txt"));
    assert_eq!(seen.len(), 4);

    let (outcome, seen) = walk(&workspace, Some("file-8.txt"));
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    assert_eq!(outcome.next, None);
    assert_eq!(seen, ["file-9.txt"]);
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn an_exhausted_rule_budget_stops_with_the_rules_reason() {
    let (mut workspace, base) = fixture("rules-budget").await;
    let mut rules = String::new();
    for index in 0..40 {
        rules.push_str(&format!("ignored-{index}.txt\n"));
    }
    write(&workspace, ".gitignore", rules.as_bytes());
    write(&workspace, "kept.txt", b"x");
    Arc::make_mut(&mut workspace).scan_limits = ScanLimits {
        entries: 1_000,
        bytes: 1 << 20,
        depth: 8,
        rule_files: 1,
        rule_bytes: 8,
    };
    let (outcome, seen) = walk(&workspace, None);
    assert_eq!(outcome.reason, WorkspaceScanStop::Rules);
    assert_eq!(outcome.next, None);
    assert_eq!(seen, [".gitignore", "kept.txt"]);
    let _ = tokio::fs::remove_dir_all(&base).await;
}

#[tokio::test]
async fn non_repository_defaults_apply_and_local_rules_win() {
    let (workspace, base) = fixture("defaults").await;
    write(&workspace, "node_modules/package/index.js", b"x");
    write(&workspace, "target/build.log", b"x");
    write(&workspace, "src/main.rs", b"x");
    write(&workspace, ".gitignore", b"!target/\n");

    let (outcome, mut seen) = walk(&workspace, None);
    assert_eq!(outcome.reason, WorkspaceScanStop::End);
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ".gitignore",
            "src",
            "src/main.rs",
            "target",
            "target/build.log"
        ]
    );
    let _ = tokio::fs::remove_dir_all(&base).await;
}

use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct ScanHold {
    gate: crate::test_support::Gate,
    exited: AtomicBool,
}
impl ScanHold {
    pub(crate) fn new() -> Self {
        Self {
            gate: crate::test_support::Gate::new(),
            exited: AtomicBool::new(false),
        }
    }
    pub(crate) async fn wait_started(&self) {
        self.gate.wait_started().await;
    }
    pub(crate) fn release(&self) {
        self.gate.release();
    }
    pub(crate) fn exited(&self) -> bool {
        self.exited.load(Ordering::SeqCst)
    }
}
pub(super) struct ScanExit(Arc<ScanHold>);
impl Drop for ScanExit {
    fn drop(&mut self) {
        self.0.exited.store(true, Ordering::SeqCst);
    }
}
pub(super) fn hold_worker(root: &Path, budget: &ScanBudget) -> Option<ScanExit> {
    let hold = SCAN_HOLDS.take(&root.to_path_buf())?;
    let exit = ScanExit(hold);
    tokio::runtime::Handle::current().block_on(async {
        tokio::select! {
            _ = exit.0.gate.pass() => {},
            _ = budget.stop.cancelled() => {},
            _ = budget.session_cancellation.cancelled() => {},
            _ = budget.shutdown_cancellation.cancelled() => {},
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(budget.deadline)) => {},
        }
    });
    Some(exit)
}
