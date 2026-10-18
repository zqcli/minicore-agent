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

use super::file_change::{FileBefore, commit_text, file_before_from_capture};
use super::{
    MAX_WRITE_BYTES, emit_phase, escape_control_characters, map_workspace_error, run_controlled,
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
            let input: WriteInput = super::decode_input(&self.spec, &invocation, &context)?;
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
                #[cfg(test)]
                if let Some(gate) = super::IO_GATES.take(&(TOOL_NAME, input.path.clone())) {
                    gate.pass().await;
                }
                emit_phase(&context, "writing");
                let before = if tool_ref.is_some() && tool_data.is_some() {
                    capture_before_best_effort(&self.workspace, &input.path, context.deadline).await
                } else {
                    FileBefore::Unknown
                };
                commit_text(
                    &self.workspace,
                    tool_ref.as_ref(),
                    tool_data.as_deref(),
                    &input.path,
                    before,
                    input.content.as_bytes(),
                )
                .await
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
