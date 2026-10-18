use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
};
use minicore_runtime::{LoopId, ToolCallId};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::tool_data::{ToolData, ToolRef};
use crate::tools::IO_GATES;
use crate::workspace::{BEFORE_RENAME_FAILURES, BEFORE_RENAME_GATES, DIRECTORY_SYNC_FAILURES};

async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, WriteTool) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-write-tool-{label}-{}",
        crate::ids::SessionId::new().unwrap()
    ));
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root).await.unwrap());
    let tool = WriteTool::new(Arc::clone(&workspace), None);
    (base, workspace, tool)
}

fn invocation(arguments: Value) -> ToolInvocation {
    ToolInvocation {
        tool_call_id: ToolCallId::new("write-call").unwrap(),
        tool_name: TOOL_NAME.parse().unwrap(),
        arguments,
    }
}

fn context(cancellation: CancellationToken, deadline: Instant) -> ToolContext {
    ToolContext {
        cancellation,
        deadline,
        progress: ToolProgressSink::default(),
    }
}

async fn execute(tool: &WriteTool, arguments: Value) -> Result<String, ToolError> {
    match tool
        .execute(
            invocation(arguments),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await?
    {
        ToolExecutionOutcome::Completed(output) => Ok(output.content().as_str().to_owned()),
        ToolExecutionOutcome::RequestInput(_) => panic!("write must not request input"),
    }
}

fn bound_ref() -> ToolRef {
    ToolRef {
        session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("write-call").unwrap(),
    }
}

async fn cleanup(base: &Path) {
    let _ = tokio::fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn schema_is_strict_and_write_creates_overwrites_and_allows_empty_content() {
    let (base, _, tool) = fixture("basic").await;
    let root = base.join("root");
    let schema = tool.spec().input_schema();
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["path", "content"]));
    assert_eq!(schema["additionalProperties"], false);

    assert_eq!(
        execute(
            &tool,
            json!({"path": "nested/file.txt", "content": "hello"})
        )
        .await
        .unwrap(),
        "wrote 5 bytes to nested/file.txt"
    );
    assert_eq!(
        tokio::fs::read(root.join("nested/file.txt")).await.unwrap(),
        b"hello"
    );
    assert_eq!(
        execute(&tool, json!({"path": "nested/file.txt", "content": "你好"}))
            .await
            .unwrap(),
        "wrote 6 bytes to nested/file.txt"
    );
    assert_eq!(
        execute(&tool, json!({"path": "nested/empty.txt", "content": ""}))
            .await
            .unwrap(),
        "wrote 0 bytes to nested/empty.txt"
    );
    assert_eq!(
        tokio::fs::read(root.join("nested/empty.txt"))
            .await
            .unwrap(),
        b""
    );
    for arguments in [
        json!({"path": "missing-content"}),
        json!({"content": "missing-path"}),
        json!({"path": "file", "content": "x", "extra": true}),
    ] {
        assert_eq!(
            tool.execute(
                invocation(arguments),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
    }
    cleanup(&base).await;
}

#[tokio::test]
async fn bound_write_records_exact_user_dirty_before_and_after() {
    let (base, _, tool) = fixture("bound-record").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), b"user dirty\n")
        .await
        .unwrap();
    let data = Arc::new(ToolData::new());
    let tool_ref = bound_ref();
    data.note_requested(&tool_ref, TOOL_NAME);

    let result = tool
        .execute_bound(
            invocation(json!({"path": "value.txt", "content": "agent\n"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
            Some(tool_ref.clone()),
            Some(Arc::clone(&data)),
        )
        .await
        .unwrap();
    assert!(matches!(result, ToolExecutionOutcome::Completed(_)));

    let records = data.file_change_records(tool_ref.session_id, Some(tool_ref.loop_id));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tool_ref, Some(tool_ref.clone()));
    assert!(records[0].details_available);
    assert_eq!(records[0].commit_state, crate::ChangeCommitState::Applied);
    data.finish_and_snapshot(
        &tool_ref,
        minicore_runtime::tools::ToolResultOutcome::Success,
    )
    .expect("bound write record finishes");
    let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
    assert_eq!(snapshot.file_change_before, Some(b"user dirty\n".to_vec()));
    assert_eq!(snapshot.file_change_after, Some(b"agent\n".to_vec()));

    let created = bound_ref();
    data.note_requested(&created, TOOL_NAME);
    tool.execute_bound(
        invocation(json!({"path": "created.txt", "content": "new\n"})),
        context(
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        ),
        Some(created.clone()),
        Some(Arc::clone(&data)),
    )
    .await
    .unwrap();
    let created_records = data.file_change_records(created.session_id, Some(created.loop_id));
    assert_eq!(created_records.len(), 1);
    assert_eq!(created_records[0].kind, crate::ChangeKind::Added);
    assert_eq!(created_records[0].before, crate::ChangeRevision::Missing);
    assert!(created_records[0].details_available);
    cleanup(&base).await;
}

#[tokio::test]
async fn oversized_write_capture_is_partial_but_write_still_commits() {
    let (base, _, tool) = fixture("oversized-capture").await;
    let root = base.join("root");
    tokio::fs::write(
        root.join("value.txt"),
        vec![b'x'; MAX_WRITE_BYTES.saturating_add(1)],
    )
    .await
    .unwrap();
    let data = Arc::new(ToolData::new());
    let tool_ref = bound_ref();
    data.note_requested(&tool_ref, TOOL_NAME);
    tool.execute_bound(
        invocation(json!({"path": "value.txt", "content": "new"})),
        context(
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        ),
        Some(tool_ref.clone()),
        Some(Arc::clone(&data)),
    )
    .await
    .unwrap();
    let record = data
        .file_change_records(tool_ref.session_id, Some(tool_ref.loop_id))
        .pop()
        .unwrap();
    assert!(matches!(
        record.before,
        crate::ChangeRevision::Metadata { .. }
    ));
    assert_eq!(record.commit_state, crate::ChangeCommitState::Applied);
    assert_eq!(record.coverage, crate::ChangeCoverage::Partial);
    assert!(!record.details_available);
    assert_eq!(
        tokio::fs::read(root.join("value.txt")).await.unwrap(),
        b"new"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn bound_write_preserves_not_committed_and_post_rename_unknown_states() {
    let (base, workspace, tool) = fixture("bound-outcomes").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), b"old\n")
        .await
        .unwrap();
    let data = Arc::new(ToolData::new());
    let target = workspace.resolve_for_write("value.txt").await.unwrap();

    let first = bound_ref();
    data.note_requested(&first, TOOL_NAME);
    BEFORE_RENAME_FAILURES.arm(target.clone(), ());
    assert_eq!(
        tool.execute_bound(
            invocation(json!({"path": "value.txt", "content": "not committed"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
            Some(first.clone()),
            Some(Arc::clone(&data)),
        )
        .await,
        Err(ToolError::Failed)
    );
    let first_record = data
        .file_change_records(first.session_id, Some(first.loop_id))
        .pop()
        .unwrap();
    assert_eq!(
        first_record.commit_state,
        crate::ChangeCommitState::NotCommitted
    );
    assert_eq!(first_record.after, crate::ChangeRevision::Unknown);

    let second = bound_ref();
    data.note_requested(&second, TOOL_NAME);
    DIRECTORY_SYNC_FAILURES.arm(target.parent().unwrap().to_path_buf(), ());
    assert_eq!(
        tool.execute_bound(
            invocation(json!({"path": "value.txt", "content": "unknown"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
            Some(second.clone()),
            Some(Arc::clone(&data)),
        )
        .await,
        Err(ToolError::Failed)
    );
    let second_record = data
        .file_change_records(second.session_id, Some(second.loop_id))
        .pop()
        .unwrap();
    assert_eq!(
        second_record.commit_state,
        crate::ChangeCommitState::Unknown
    );
    assert_eq!(
        second_record.after,
        crate::changes::content_revision(b"unknown")
    );
    assert_eq!(tokio::fs::read(target).await.unwrap(), b"unknown");
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn success_output_escapes_control_characters_in_relative_path() {
    let (base, _, tool) = fixture("escaped-output").await;
    let path = "controls-你-\r-\n-\t-\u{1b}.txt";
    let output = execute(&tool, json!({"path": path, "content": "written"}))
        .await
        .unwrap();

    assert_eq!(
        output,
        "wrote 7 bytes to controls-你-\\r-\\n-\\t-\\u{1b}.txt"
    );
    assert_eq!(output.lines().count(), 1);
    assert!(!output.chars().any(char::is_control));
    assert_eq!(escape_control_characters("\0"), "\\u{0}");
    assert_eq!(
        tokio::fs::read(base.join("root").join(path)).await.unwrap(),
        b"written"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn output_is_prevalidated_before_workspace_mutation() {
    use minicore_runtime::value::MAX_TEXT_BYTES;

    let (base, _, tool) = fixture("output-prevalidation").await;
    let root = base.join("root");
    let path = "\u{1b}".repeat(MAX_TEXT_BYTES);
    assert_eq!(
        tool.execute(
            invocation(json!({"path": path, "content": "must-not-write"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Internal)
    );
    assert!(
        tokio::fs::read_dir(&root)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none()
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn oversize_and_path_escape_are_invalid_invocations() {
    let (base, _, tool) = fixture("invalid").await;
    assert_eq!(
        tool.execute(
            invocation(json!({
                "path": "large.txt",
                "content": "x".repeat(MAX_WRITE_BYTES + 1)
            })),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    for path in ["", "../escape", "/absolute", "bad\0path"] {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": path, "content": "x"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
    }
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_escape_and_final_target_symlink_are_invalid() {
    use std::os::unix::fs::symlink;

    let (base, _, tool) = fixture("symlink").await;
    let root = base.join("root");
    let outside = base.join("outside");
    tokio::fs::create_dir(&outside).await.unwrap();
    tokio::fs::write(outside.join("value"), b"outside")
        .await
        .unwrap();
    symlink(&outside, root.join("escape")).unwrap();
    symlink(outside.join("value"), root.join("target")).unwrap();

    for path in ["escape/new", "target"] {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": path, "content": "new"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
    }
    assert_eq!(
        tokio::fs::read(outside.join("value")).await.unwrap(),
        b"outside"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn workspace_atomic_failures_and_unknown_outcomes_are_failed_tools() {
    let (base, workspace, tool) = fixture("atomic-errors").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value"), b"old").await.unwrap();
    let target = workspace.resolve_for_write("value").await.unwrap();
    BEFORE_RENAME_FAILURES.arm(target.clone(), ());
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value", "content": "before-rename"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"old");

    DIRECTORY_SYNC_FAILURES.arm(target.parent().unwrap().to_path_buf(), ());
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value", "content": "complete-new"})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"complete-new");
    cleanup(&base).await;
}

#[tokio::test]
async fn cancellation_and_deadline_interrupt_waiting_write_operations() {
    let (base, _, tool) = fixture("control").await;
    let root = base.join("root");
    let tool = Arc::new(tool);

    let cancellation = CancellationToken::new();
    let gate = Arc::new(crate::test_support::Gate::new());
    IO_GATES.arm(
        (TOOL_NAME, "cancelled-parent/cancelled".to_owned()),
        Arc::clone(&gate),
    );
    let cancelled_tool = Arc::clone(&tool);
    let cancelled_context = context(
        cancellation.clone(),
        Instant::now() + Duration::from_secs(5),
    );
    let cancelled = tokio::spawn(async move {
        cancelled_tool
            .execute(
                invocation(json!({"path": "cancelled-parent/cancelled", "content": "x"})),
                cancelled_context,
            )
            .await
    });
    gate.wait_started().await;
    cancellation.cancel();
    assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));
    assert!(
        tokio::fs::symlink_metadata(root.join("cancelled-parent"))
            .await
            .is_err()
    );

    let gate = Arc::new(crate::test_support::Gate::new());
    IO_GATES.arm(
        (TOOL_NAME, "deadline-parent/timed-out".to_owned()),
        Arc::clone(&gate),
    );
    let deadline_tool = Arc::clone(&tool);
    let deadline = tokio::spawn(async move {
        deadline_tool
            .execute(
                invocation(json!({"path": "deadline-parent/timed-out", "content": "x"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_millis(25),
                ),
            )
            .await
    });
    gate.wait_started().await;
    assert_eq!(deadline.await.unwrap(), Err(ToolError::TimedOut));
    assert!(
        tokio::fs::symlink_metadata(root.join("deadline-parent"))
            .await
            .is_err()
    );
    assert!(
        tokio::fs::read_dir(&root)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none()
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn the_loop_barrier_joins_a_commit_whose_caller_was_cancelled() {
    let (base, workspace, _) = fixture("owned-commit").await;
    let owners = crate::tools::command::ToolOwners::new();
    let tool = WriteTool::new(Arc::clone(&workspace), Some(Arc::clone(&owners)));
    let target = workspace.resolve_for_write("value").await.unwrap();
    let gate = Arc::new(crate::test_support::Gate::new());
    BEFORE_RENAME_GATES.arm(target.clone(), Arc::clone(&gate));
    let data = Arc::new(ToolData::new());
    let tool_ref = bound_ref();
    data.note_requested(&tool_ref, TOOL_NAME);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn({
        let (data, tool_ref, cancellation) =
            (Arc::clone(&data), tool_ref.clone(), cancellation.clone());
        async move {
            tool.execute_bound(
                invocation(json!({"path": "value", "content": "new"})),
                context(cancellation, Instant::now() + Duration::from_secs(10)),
                Some(tool_ref),
                Some(data),
            )
            .await
        }
    });
    gate.wait_started().await;
    cancellation.cancel();
    assert_eq!(task.await.unwrap(), Err(ToolError::Cancelled));
    // Persistence follows this barrier; it must not pass while the commit runs.
    let join = owners.join_loop(tool_ref.loop_id);
    tokio::pin!(join);
    tokio::select! {
        biased;
        () = &mut join => panic!("the loop barrier passed a running commit"),
        () = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    gate.release();
    join.await;
    let records = data.file_change_records(tool_ref.session_id, Some(tool_ref.loop_id));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].commit_state, crate::ChangeCommitState::Applied);
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"new");
    cleanup(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn interrupted_writes_finish_and_record_without_blocking_the_async_worker() {
    for stop in ["cancel", "deadline", "drop"] {
        for sync_fails in [false, true] {
            let (base, workspace, tool) = fixture("detached-commit").await;
            let root = base.join("root");
            tokio::fs::write(root.join("value"), b"old").await.unwrap();
            let target = workspace.resolve_for_write("value").await.unwrap();
            if sync_fails {
                DIRECTORY_SYNC_FAILURES.arm(target.parent().unwrap().to_path_buf(), ());
            }
            let gate = Arc::new(crate::test_support::Gate::new());
            BEFORE_RENAME_GATES.arm(target.clone(), Arc::clone(&gate));
            let data = Arc::new(ToolData::new());
            let tool_ref = bound_ref();
            data.note_requested(&tool_ref, TOOL_NAME);
            let cancellation = CancellationToken::new();
            let deadline = Instant::now()
                + if stop == "deadline" {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(10)
                };
            let mut task = tokio::spawn({
                let data = Arc::clone(&data);
                let tool_ref = tool_ref.clone();
                let cancellation = cancellation.clone();
                async move {
                    tool.execute_bound(
                        invocation(json!({"path": "value", "content": "complete-new"})),
                        context(cancellation, deadline),
                        Some(tool_ref),
                        Some(data),
                    )
                    .await
                }
            });
            gate.wait_started().await;
            // With only one async worker, this cannot run if fsync/rename is
            // blocking that worker. Release the gate on failure to allow cleanup.
            let responsive =
                tokio::time::timeout(Duration::from_secs(1), tokio::spawn(async {})).await;
            if responsive.is_err() {
                gate.release();
            }
            responsive
                .expect("atomic commit blocked the async worker")
                .unwrap();
            match stop {
                "cancel" => cancellation.cancel(),
                "drop" => task.abort(),
                "deadline" => {}
                _ => unreachable!(),
            }
            let result = tokio::time::timeout(Duration::from_secs(5), &mut task).await;
            gate.release();
            let result = result.expect("caller remained stuck in the commit");
            match stop {
                "cancel" => assert_eq!(result.unwrap(), Err(ToolError::Cancelled)),
                "deadline" => assert_eq!(result.unwrap(), Err(ToolError::TimedOut)),
                "drop" => assert!(result.unwrap_err().is_cancelled()),
                _ => unreachable!(),
            }
            let records = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let records =
                        data.file_change_records(tool_ref.session_id, Some(tool_ref.loop_id));
                    if !records.is_empty() {
                        break records;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("commit observation was lost after dropping its awaiter");
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].commit_state,
                if sync_fails {
                    crate::ChangeCommitState::Unknown
                } else {
                    crate::ChangeCommitState::Applied
                }
            );
            assert_eq!(records[0].before, crate::changes::content_revision(b"old"));
            assert_eq!(
                records[0].after,
                crate::changes::content_revision(b"complete-new")
            );
            assert_eq!(tokio::fs::read(&target).await.unwrap(), b"complete-new");
            let mut entries = tokio::fs::read_dir(&root).await.unwrap();
            while let Some(entry) = entries.next_entry().await.unwrap() {
                assert!(
                    !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".minicore-write-")
                );
            }
            cleanup(&base).await;
        }
    }
}
