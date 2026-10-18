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
mod tests;
