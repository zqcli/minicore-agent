use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::Workspace;

use super::{
    MAX_WRITE_BYTES, escape_control_characters, map_workspace_error, precheck_control,
    run_controlled, wait_for_test_io,
};

const TOOL_NAME: &str = "write";

pub(super) struct WriteTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteInput {
    path: String,
    content: String,
}

impl WriteTool {
    pub(super) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("write is a valid tool name"),
            "Atomically write UTF-8 content to a workspace-relative file, creating parent directories.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative destination file path."
                    },
                    "content": {
                        "type": "string",
                        "description": "UTF-8 file content, limited to 512 KiB."
                    }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
        )
        .expect("static write tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for WriteTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'a> {
        Box::pin(async move {
            precheck_control(&context)?;
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input: WriteInput = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            if input.content.len() > MAX_WRITE_BYTES {
                return Err(ToolError::InvalidInvocation);
            }
            self.workspace
                .validate_write_path(&input.path)
                .map_err(map_workspace_error)?;
            let bytes = input.content.len();
            let path_display = escape_control_characters(&input.path);
            let output = ToolOutput::new(format!("wrote {bytes} bytes to {path_display}"))
                .map_err(|_| ToolError::Internal)?;
            run_controlled(&context, async {
                wait_for_test_io(TOOL_NAME, &input.path).await;
                self.workspace
                    .write_atomic(&input.path, input.content.as_bytes())
                    .await
                    .map_err(map_workspace_error)
            })
            .await?;
            Ok(ToolExecutionOutcome::Completed(output))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::ids::{SessionInstanceId, ToolCallId, TurnId};
    use minicore_runtime::tools::{
        Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::tools::{ToolIoGate, block_next_io};
    use crate::workspace::{
        BeforeRenameGate, block_before_rename, fail_next_before_rename, fail_next_directory_sync,
    };

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, WriteTool) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-write-tool-{label}-{}",
            minicore_runtime::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let tool = WriteTool::new(Arc::clone(&workspace));
        (base, workspace, tool)
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            instance_id: "ins_00000000000000000000000000000001"
                .parse::<SessionInstanceId>()
                .unwrap(),
            turn_id: "trn_00000000000000000000000000000001"
                .parse::<TurnId>()
                .unwrap(),
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
        fail_next_before_rename(target.clone());
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

        fail_next_directory_sync(target.parent().unwrap().to_path_buf());
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
        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "cancelled-parent/cancelled"));
        block_next_io(Arc::clone(&gate));
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
        gate.started.acquire().await.unwrap().forget();
        cancellation.cancel();
        assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));
        assert!(
            tokio::fs::symlink_metadata(root.join("cancelled-parent"))
                .await
                .is_err()
        );

        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "deadline-parent/timed-out"));
        block_next_io(Arc::clone(&gate));
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
        gate.started.acquire().await.unwrap().forget();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_during_commit_returns_the_real_commit_result() {
        let (base, workspace, tool) = fixture("commit-cancel").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value"), b"old").await.unwrap();
        let target = workspace.resolve_for_write("value").await.unwrap();
        let gate = Arc::new(BeforeRenameGate::new(target.clone()));
        block_before_rename(Arc::clone(&gate));
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            tool.execute(
                invocation(json!({"path": "value", "content": "complete-new"})),
                context(task_cancellation, Instant::now() + Duration::from_secs(5)),
            )
            .await
        });

        gate.wait_started().await;
        cancellation.cancel();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let finished_before_release = task.is_finished();
        gate.release();
        let result = task.await.unwrap();
        let target_contents = tokio::fs::read(&target).await.unwrap();
        let mut entries = tokio::fs::read_dir(&root).await.unwrap();
        let mut temp_found = false;
        while let Some(entry) = entries.next_entry().await.unwrap() {
            temp_found |= entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(".minicore-write-"));
        }
        cleanup(&base).await;

        assert!(!finished_before_release);
        let ToolExecutionOutcome::Completed(output) = result.unwrap() else {
            panic!("write must not request input");
        };
        assert_eq!(output.content().as_str(), "wrote 12 bytes to value");
        assert_eq!(target_contents, b"complete-new");
        assert!(!temp_found);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_during_commit_returns_post_rename_unknown_outcome() {
        let (base, workspace, tool) = fixture("commit-deadline-unknown").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value"), b"old").await.unwrap();
        let target = workspace.resolve_for_write("value").await.unwrap();
        fail_next_directory_sync(target.parent().unwrap().to_path_buf());
        let gate = Arc::new(BeforeRenameGate::new(target.clone()));
        block_before_rename(Arc::clone(&gate));
        let task = tokio::spawn(async move {
            tool.execute(
                invocation(json!({"path": "value", "content": "complete-new"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_millis(25),
                ),
            )
            .await
        });

        gate.wait_started().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let finished_before_release = task.is_finished();
        gate.release();
        let result = task.await.unwrap();
        let target_contents = tokio::fs::read(&target).await.unwrap();
        let mut entries = tokio::fs::read_dir(&root).await.unwrap();
        let mut temp_found = false;
        while let Some(entry) = entries.next_entry().await.unwrap() {
            temp_found |= entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(".minicore-write-"));
        }
        cleanup(&base).await;

        assert!(!finished_before_release);
        assert_eq!(result, Err(ToolError::Failed));
        assert_eq!(target_contents, b"complete-new");
        assert!(!temp_found);
    }
}
