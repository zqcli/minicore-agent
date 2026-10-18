use std::sync::Arc;
use std::time::{Duration, Instant};

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::Workspace;
use crate::tool_data::{ToolData, ToolRef};

use super::{
    FileBefore, MAX_WRITE_BYTES, emit_phase, escape_control_characters, expected_file_state,
    file_before_from_capture, map_workspace_error, precheck_control, record_file_change,
    run_controlled, wait_for_test_io,
};

const TOOL_NAME: &str = "write";
const WRITE_CAPTURE_MAX: Duration = Duration::from_millis(250);

pub(crate) struct WriteTool {
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
    pub(crate) fn new(workspace: Arc<Workspace>) -> Self {
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

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        self.execute_bound(invocation, context, None, None)
    }
}

impl WriteTool {
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
                emit_phase(&context, "writing");
                let before = if tool_ref.is_some() && tool_data.is_some() {
                    capture_before_best_effort(&self.workspace, &input.path, context.deadline).await
                } else {
                    FileBefore::Unknown
                };
                let expected = expected_file_state(&before);
                let observation = self
                    .workspace
                    .write_atomic_observed(&input.path, input.content.as_bytes(), expected)
                    .await;
                record_file_change(
                    tool_data.as_deref(),
                    tool_ref.as_ref(),
                    &input.path,
                    before,
                    input.content.as_bytes(),
                    &observation,
                );
                observation.outcome.map_err(map_workspace_error)
            })
            .await?;
            Ok(ToolExecutionOutcome::Completed(output))
        })
    }
}

async fn capture_before_best_effort(
    workspace: &Workspace,
    path: &str,
    deadline: Instant,
) -> FileBefore {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let quarter_nanos = remaining.as_nanos() / 4;
    let budget_nanos = quarter_nanos.min(WRITE_CAPTURE_MAX.as_nanos());
    if budget_nanos == 0 {
        return FileBefore::Unknown;
    }
    let budget = Duration::from_nanos(budget_nanos as u64);
    match tokio::time::timeout(budget, workspace.capture_file(path, MAX_WRITE_BYTES)).await {
        Ok(capture) => file_before_from_capture(capture),
        Err(_) => FileBefore::Unknown,
    }
}

#[cfg(test)]
mod tests;
