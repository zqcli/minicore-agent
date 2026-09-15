use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::tool_data::{ToolData, ToolRef};
use crate::{Workspace, WorkspaceError};

use super::{
    FileBefore, MAX_PATCH_BYTES, emit_phase, escape_control_characters, expected_file_state,
    map_workspace_error, precheck_control, record_file_change, run_controlled, wait_for_test_io,
};

const TOOL_NAME: &str = "edit";

pub(crate) struct EditTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditInput {
    path: String,
    old_text: String,
    new_text: String,
    #[serde(default)]
    replace_all: bool,
}

impl EditTool {
    pub(crate) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("edit is a valid tool name"),
            "Replace exact literal UTF-8 text in one existing workspace file.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative existing file path."
                    },
                    "old_text": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Exact literal text to replace."
                    },
                    "new_text": {
                        "type": "string",
                        "description": "Literal replacement text."
                    },
                    "replace_all": {
                        "type": "boolean",
                        "default": false,
                        "description": "Replace every non-overlapping exact match."
                    }
                },
                "required": ["path", "old_text", "new_text"],
                "additionalProperties": false
            }),
        )
        .expect("static edit tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for EditTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        self.execute_bound(invocation, context, None, None)
    }
}

impl EditTool {
    pub(crate) fn execute_bound(
        &self,
        invocation: ToolInvocation,
        context: ToolContext,
        tool_ref: Option<ToolRef>,
        tool_data: Option<Arc<ToolData>>,
    ) -> ToolFuture<'_> {
        Box::pin(async move {
            precheck_control(&context)?;
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input: EditInput = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            if input.old_text.is_empty()
                || input.old_text.len() > MAX_PATCH_BYTES
                || input.new_text.len() > MAX_PATCH_BYTES
                || input.old_text.contains('\0')
                || input.new_text.contains('\0')
            {
                return Err(ToolError::InvalidInvocation);
            }
            self.workspace
                .validate_write_path(&input.path)
                .map_err(map_workspace_error)?;
            let path_display = escape_control_characters(&input.path);

            run_controlled(&context, async {
                wait_for_test_io(TOOL_NAME, &input.path).await;
                emit_phase(&context, "reading");
                let (source, before) = if tool_ref.is_some() && tool_data.is_some() {
                    let (source, stamp, stable) = self
                        .workspace
                        .read_text_snapshot(&input.path, MAX_PATCH_BYTES)
                        .await
                        .map_err(map_source_error)?;
                    let before = FileBefore::Content {
                        bytes: source.as_bytes().to_vec(),
                        stamp,
                        stable,
                    };
                    (source, before)
                } else {
                    (
                        self.workspace
                            .read_text(&input.path, MAX_PATCH_BYTES)
                            .await
                            .map_err(map_source_error)?,
                        FileBefore::Unknown,
                    )
                };
                emit_phase(&context, "matching");
                let (result, replacements) = edit_text(&source, &input)?;
                let output = ToolOutput::new(format!(
                    "replaced {replacements} occurrence(s) in {path_display}"
                ))
                .map_err(|_| ToolError::Internal)?;
                emit_phase(&context, "committing");
                let expected = expected_file_state(&before);
                let observation = self
                    .workspace
                    .write_atomic_observed(&input.path, result.as_bytes(), expected)
                    .await;
                record_file_change(
                    tool_data.as_deref(),
                    tool_ref.as_ref(),
                    &input.path,
                    before,
                    result.as_bytes(),
                    &observation,
                );
                observation.outcome.map_err(map_workspace_error)?;
                Ok(ToolExecutionOutcome::Completed(output))
            })
            .await
        })
    }
}

fn edit_text(source: &str, input: &EditInput) -> Result<(String, usize), ToolError> {
    let replacements = source.match_indices(&input.old_text).count();
    if replacements == 0 || (!input.replace_all && replacements > 1) {
        return Err(ToolError::Failed);
    }
    let replacements = if input.replace_all { replacements } else { 1 };
    let removed = input
        .old_text
        .len()
        .checked_mul(replacements)
        .ok_or(ToolError::InvalidInvocation)?;
    let inserted = input
        .new_text
        .len()
        .checked_mul(replacements)
        .ok_or(ToolError::InvalidInvocation)?;
    let result_len = source
        .len()
        .checked_sub(removed)
        .and_then(|len| len.checked_add(inserted))
        .ok_or(ToolError::InvalidInvocation)?;
    if result_len > MAX_PATCH_BYTES {
        return Err(ToolError::InvalidInvocation);
    }
    let result = if input.replace_all {
        source.replace(&input.old_text, &input.new_text)
    } else {
        source.replacen(&input.old_text, &input.new_text, 1)
    };
    if result.len() != result_len {
        return Err(ToolError::Internal);
    }
    Ok((result, replacements))
}

fn map_source_error(error: WorkspaceError) -> ToolError {
    match error {
        WorkspaceError::InvalidPath | WorkspaceError::Escape => ToolError::InvalidInvocation,
        WorkspaceError::NotFound
        | WorkspaceError::NotFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::Unavailable
        | WorkspaceError::UnknownOutcome
        | WorkspaceError::TooLarge
        | WorkspaceError::Binary => ToolError::Failed,
    }
}

#[cfg(test)]
mod tests {
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
    use crate::tools::{ToolIoGate, block_next_io};
    use crate::workspace::{
        BeforeRenameGate, block_before_rename, fail_next_before_rename, fail_next_directory_sync,
    };

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, EditTool) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-edit-tool-{label}-{}",
            crate::ids::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let tool = EditTool::new(Arc::clone(&workspace));
        (base, workspace, tool)
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            tool_call_id: ToolCallId::new("edit-call").unwrap(),
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

    async fn execute(tool: &EditTool, arguments: Value) -> Result<String, ToolError> {
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
            ToolExecutionOutcome::RequestInput(_) => panic!("edit must not request input"),
        }
    }

    fn bound_ref() -> ToolRef {
        ToolRef {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new("edit-call").unwrap(),
        }
    }

    async fn cleanup(base: &Path) {
        let _ = tokio::fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn schema_is_strict_and_single_literal_match_is_replaced() {
        let (base, _, tool) = fixture("single").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value.txt"), "before β after")
            .await
            .unwrap();
        let schema = tool.spec().input_schema();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], json!(["path", "old_text", "new_text"]));
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["old_text"]["minLength"], 1);
        for arguments in [
            json!({"path": "value.txt", "new_text": "x"}),
            json!({"path": "value.txt", "old_text": "β", "new_text": "x", "extra": true}),
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

        assert_eq!(
            execute(
                &tool,
                json!({"path": "value.txt", "old_text": "β", "new_text": "世界"})
            )
            .await
            .unwrap(),
            "replaced 1 occurrence(s) in value.txt"
        );
        assert_eq!(
            tokio::fs::read_to_string(root.join("value.txt"))
                .await
                .unwrap(),
            "before 世界 after"
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn bound_edit_records_the_same_source_and_computed_result() {
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
                invocation(json!({
                    "path": "value.txt",
                    "old_text": "dirty",
                    "new_text": "clean"
                })),
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
        assert_eq!(records[0].commit_state, crate::ChangeCommitState::Applied);
        data.finish_and_snapshot(
            &tool_ref,
            minicore_runtime::tools::ToolResultOutcome::Success,
        )
        .expect("bound edit record finishes");
        let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
        assert_eq!(snapshot.file_change_before, Some(b"user dirty\n".to_vec()));
        assert_eq!(snapshot.file_change_after, Some(b"user clean\n".to_vec()));
        cleanup(&base).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bound_edit_records_a_conflict_when_the_target_changes_before_commit() {
        let (base, workspace, tool) = fixture("bound-conflict").await;
        let root = base.join("root");
        tokio::fs::write(root.join("value.txt"), b"user dirty\n")
            .await
            .unwrap();
        let target = workspace.resolve_for_write("value.txt").await.unwrap();
        let gate = Arc::new(BeforeRenameGate::new(target.clone()));
        block_before_rename(Arc::clone(&gate));
        let data = Arc::new(ToolData::new());
        let tool_ref = bound_ref();
        data.note_requested(&tool_ref, TOOL_NAME);
        let task_ref = tool_ref.clone();
        let task_data = Arc::clone(&data);
        let task = tokio::spawn(async move {
            tool.execute_bound(
                invocation(json!({
                    "path": "value.txt",
                    "old_text": "dirty",
                    "new_text": "clean"
                })),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5),
                ),
                Some(task_ref),
                Some(task_data),
            )
            .await
        });
        gate.wait_started().await;
        tokio::fs::write(&target, b"external change\n")
            .await
            .unwrap();
        gate.release();
        assert!(matches!(
            task.await.unwrap(),
            Ok(ToolExecutionOutcome::Completed(_))
        ));
        let records = data.file_change_records(tool_ref.session_id, Some(tool_ref.loop_id));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].commit_state, crate::ChangeCommitState::Conflict);
        assert_eq!(records[0].coverage, crate::ChangeCoverage::Partial);
        assert_eq!(tokio::fs::read(target).await.unwrap(), b"user clean\n");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn no_match_and_ambiguous_fail_without_writing_then_replace_all_is_non_overlapping() {
        let (base, _, tool) = fixture("matches").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        tokio::fs::write(&target, "old aaa old").await.unwrap();

        for arguments in [
            json!({"path": "value.txt", "old_text": "missing", "new_text": "x"}),
            json!({"path": "value.txt", "old_text": "old", "new_text": "new"}),
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
                Err(ToolError::Failed)
            );
            assert_eq!(
                tokio::fs::read_to_string(&target).await.unwrap(),
                "old aaa old"
            );
        }

        assert_eq!(
            execute(
                &tool,
                json!({
                    "path": "value.txt",
                    "old_text": "old",
                    "new_text": "new",
                    "replace_all": true
                })
            )
            .await
            .unwrap(),
            "replaced 2 occurrence(s) in value.txt"
        );
        assert_eq!(
            tokio::fs::read_to_string(&target).await.unwrap(),
            "new aaa new"
        );

        tokio::fs::write(&target, "aaa").await.unwrap();
        assert_eq!(
            execute(
                &tool,
                json!({
                    "path": "value.txt",
                    "old_text": "aa",
                    "new_text": "b",
                    "replace_all": true
                })
            )
            .await
            .unwrap(),
            "replaced 1 occurrence(s) in value.txt"
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "ba");

        assert_eq!(
            execute(
                &tool,
                json!({"path": "value.txt", "old_text": "ba", "new_text": "ba"})
            )
            .await
            .unwrap(),
            "replaced 1 occurrence(s) in value.txt"
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "ba");
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn input_source_and_result_limits_are_enforced_before_allocation_or_write() {
        let (base, _, tool) = fixture("limits").await;
        let root = base.join("root");
        let target = root.join("value.txt");
        tokio::fs::write(&target, "value").await.unwrap();

        for arguments in [
            json!({"path": "value.txt", "old_text": "", "new_text": "x"}),
            json!({"path": "value.txt", "old_text": "x".repeat(MAX_PATCH_BYTES + 1), "new_text": "y"}),
            json!({"path": "value.txt", "old_text": "x", "new_text": "y".repeat(MAX_PATCH_BYTES + 1)}),
            json!({"path": "value.txt", "old_text": "x\0", "new_text": "y"}),
            json!({"path": "value.txt", "old_text": "x", "new_text": "y\0"}),
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
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "value");

        tokio::fs::write(root.join("too-large"), "a".repeat(MAX_PATCH_BYTES + 1))
            .await
            .unwrap();
        tokio::fs::write(root.join("binary"), [0xff, 0xfe])
            .await
            .unwrap();
        tokio::fs::write(root.join("nul"), b"text\0value")
            .await
            .unwrap();
        tokio::fs::create_dir(root.join("directory")).await.unwrap();
        for path in ["too-large", "binary", "nul", "directory", "missing"] {
            assert_eq!(
                tool.execute(
                    invocation(json!({"path": path, "old_text": "a", "new_text": "b"})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5)
                    )
                )
                .await,
                Err(ToolError::Failed)
            );
        }

        let at_limit = format!("x{}", "a".repeat(MAX_PATCH_BYTES - 1));
        tokio::fs::write(&target, &at_limit).await.unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "old_text": "x", "new_text": "xx"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), at_limit);
        assert_eq!(
            execute(
                &tool,
                json!({"path": "value.txt", "old_text": "x", "new_text": "y"})
            )
            .await
            .unwrap(),
            "replaced 1 occurrence(s) in value.txt"
        );
        assert_eq!(
            tokio::fs::metadata(&target).await.unwrap().len(),
            MAX_PATCH_BYTES as u64
        );
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn paths_are_contained_final_symlinks_are_rejected_and_output_is_single_line() {
        use std::os::unix::fs::symlink;

        let (base, _, tool) = fixture("paths").await;
        let root = base.join("root");
        let control_path = "control-你-\r-\n-\t-\u{1b}.txt";
        tokio::fs::write(root.join(control_path), "old")
            .await
            .unwrap();
        let output = execute(
            &tool,
            json!({"path": control_path, "old_text": "old", "new_text": "new"}),
        )
        .await
        .unwrap();
        assert_eq!(
            output,
            "replaced 1 occurrence(s) in control-你-\\r-\\n-\\t-\\u{1b}.txt"
        );
        assert_eq!(output.lines().count(), 1);
        assert!(!output.chars().any(char::is_control));

        tokio::fs::write(root.join("real"), "old").await.unwrap();
        symlink("real", root.join("link")).unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "link", "old_text": "old", "new_text": "new"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(
            tokio::fs::read_to_string(root.join("real")).await.unwrap(),
            "old"
        );
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "../escape", "old_text": "x", "new_text": "y"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn cancellation_and_deadline_before_commit_do_not_modify_the_source() {
        let (base, _, tool) = fixture("control").await;
        let root = base.join("root");
        tokio::fs::write(root.join("cancelled"), "old")
            .await
            .unwrap();
        tokio::fs::write(root.join("timed-out"), "old")
            .await
            .unwrap();
        let tool = Arc::new(tool);

        let cancellation = CancellationToken::new();
        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "cancelled"));
        block_next_io(Arc::clone(&gate));
        let cancelled_tool = Arc::clone(&tool);
        let cancelled_context = context(
            cancellation.clone(),
            Instant::now() + Duration::from_secs(5),
        );
        let cancelled = tokio::spawn(async move {
            cancelled_tool
                .execute(
                    invocation(json!({"path": "cancelled", "old_text": "old", "new_text": "new"})),
                    cancelled_context,
                )
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        cancellation.cancel();
        assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));
        assert_eq!(
            tokio::fs::read_to_string(root.join("cancelled"))
                .await
                .unwrap(),
            "old"
        );

        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "timed-out"));
        block_next_io(Arc::clone(&gate));
        let deadline_tool = Arc::clone(&tool);
        let deadline = tokio::spawn(async move {
            deadline_tool
                .execute(
                    invocation(json!({"path": "timed-out", "old_text": "old", "new_text": "new"})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_millis(25),
                    ),
                )
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        assert_eq!(deadline.await.unwrap(), Err(ToolError::TimedOut));
        assert_eq!(
            tokio::fs::read_to_string(root.join("timed-out"))
                .await
                .unwrap(),
            "old"
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn atomic_failures_and_unknown_outcomes_never_report_success() {
        let (base, workspace, tool) = fixture("atomic-errors").await;
        let root = base.join("root");
        let target = root.join("value");
        tokio::fs::write(&target, "old").await.unwrap();
        let resolved = workspace.resolve_for_write("value").await.unwrap();

        fail_next_before_rename(resolved.clone());
        assert_eq!(
            tool.execute(
                invocation(
                    json!({"path": "value", "old_text": "old", "new_text": "before-rename"})
                ),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "old");

        fail_next_directory_sync(resolved.parent().unwrap().to_path_buf());
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value", "old_text": "old", "new_text": "complete-new"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(
            tokio::fs::read_to_string(&target).await.unwrap(),
            "complete-new"
        );
        cleanup(&base).await;
    }
}
