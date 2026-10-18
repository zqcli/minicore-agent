mod apply;
mod codex;
mod unified;

use apply::apply_single_file_patch;

use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::Workspace;
use crate::tool_data::{ToolData, ToolRef};

use super::file_change::{commit_text, read_source_before};
use super::{
    MAX_PATCH_BYTES, emit_phase, escape_control_characters, map_workspace_error, run_controlled,
};

const TOOL_NAME: &str = "apply_patch";
pub(crate) struct ApplyPatchTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyPatchInput {
    path: String,
    patch: String,
}

impl ApplyPatchTool {
    pub(crate) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("apply_patch is a valid tool name"),
            "Apply one complete patch to one existing workspace file. Standard unified diffs are single-file only; Codex envelopes must contain exactly one *** Update File operation. Add File, Delete File, Move to, and multifile envelopes are rejected.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative path of the one existing file; a Codex Update File path must match it."
                    },
                    "patch": {
                        "type": "string",
                        "minLength": 1,
                        "description": concat!(
                            "Single-file standard unified diff or one-file Codex *** Begin Patch envelope. ",
                            "Example: *** Begin Patch\n",
                            "*** Update File: value.txt\n@@\n-old\n+new\n*** End Patch. ",
                            "Add/Delete/Move/multifile forms are rejected; limited to 512 KiB."
                        )
                    }
                },
                "required": ["path", "patch"],
                "additionalProperties": false
            }),
        )
        .expect("static apply_patch tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for ApplyPatchTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        self.execute_bound(invocation, context, None, None)
    }
}

impl ApplyPatchTool {
    pub(crate) fn execute_bound(
        &self,
        invocation: ToolInvocation,
        context: ToolContext,
        tool_ref: Option<ToolRef>,
        tool_data: Option<Arc<ToolData>>,
    ) -> ToolFuture<'_> {
        Box::pin(async move {
            let input: ApplyPatchInput = super::decode_input(&self.spec, &invocation, &context)?;
            if input.patch.is_empty()
                || input.patch.len() > MAX_PATCH_BYTES
                || input.patch.contains('\0')
            {
                return Err(ToolError::InvalidInvocation);
            }
            self.workspace
                .validate_write_path(&input.path)
                .map_err(map_workspace_error)?;
            let path_display = escape_control_characters(&input.path);

            run_controlled(&context, async {
                #[cfg(test)]
                if let Some(gate) = super::IO_GATES.take(&(TOOL_NAME, input.path.clone())) {
                    gate.pass().await;
                }
                emit_phase(&context, "reading");
                let (source, before) = match read_source_before(
                    &self.workspace,
                    &input.path,
                    MAX_PATCH_BYTES,
                    tool_ref.is_some() && tool_data.is_some(),
                )
                .await
                {
                    Ok(source) => source,
                    Err(error) => return super::file_change::source_failure(error),
                };
                emit_phase(&context, "matching");
                let result = apply_single_file_patch(&input.path, &source, &input.patch)?;
                if result.len() > MAX_PATCH_BYTES {
                    return Err(ToolError::InvalidInvocation);
                }
                let output = ToolOutput::new(format!(
                    "patched {} bytes to {} bytes at {path_display}",
                    source.len(),
                    result.len()
                ))
                .map_err(|_| ToolError::Internal)?;
                emit_phase(&context, "committing");
                commit_text(
                    &self.workspace,
                    tool_ref,
                    tool_data,
                    input.path,
                    before,
                    result.into_bytes(),
                )
                .await?;
                Ok(ToolExecutionOutcome::Completed(output))
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests;
