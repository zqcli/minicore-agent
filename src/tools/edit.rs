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
            let input: EditInput = super::decode_input(&self.spec, &invocation, &context)?;
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
                #[cfg(test)]
                if let Some(gate) = super::IO_GATES.take(&(TOOL_NAME, input.path.clone())) { gate.pass().await; }
                emit_phase(&context, "reading");
                let (source, before) = match read_source_before(
                    &self.workspace, &input.path, MAX_PATCH_BYTES,
                    tool_ref.is_some() && tool_data.is_some(),
                ).await {
                    Ok(source) => source,
                    Err(error) => return super::file_change::source_failure(error),
                };
                emit_phase(&context, "matching");
                let (result, replacements) = match edit_text(&source, &input) {
                    Ok(result) => result,
                    Err(ToolError::Failed) => {
                        let output = ToolOutput::new("Edit requires exactly one match (or replace_all for multiple matches). Read the file and check old_text.")
                            .map_err(|_| ToolError::Internal)?;
                        return Ok(ToolExecutionOutcome::Completed(output));
                    }
                    Err(error) => return Err(error),
                };
                let output = ToolOutput::new(format!(
                    "replaced {replacements} occurrence(s) in {path_display}"
                ))
                .map_err(|_| ToolError::Internal)?;
                emit_phase(&context, "committing");
                commit_text(&self.workspace, tool_ref, tool_data,
                    input.path, before, result.into_bytes()).await?;
                Ok(ToolExecutionOutcome::Completed(output))
            })
            .await
        })
    }
}

fn edit_text(source: &str, input: &EditInput) -> Result<(String, usize), ToolError> {
    let translated;
    let input = if source.contains("\r\n")
        && input.old_text.contains('\n')
        && !input.old_text.contains("\r\n")
    {
        translated = EditInput {
            path: input.path.clone(),
            old_text: input.old_text.replace('\n', "\r\n"),
            new_text: input.new_text.replace("\r\n", "\n").replace('\n', "\r\n"),
            replace_all: input.replace_all,
        };
        &translated
    } else {
        input
    };
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
    Ok((result, replacements))
}

#[cfg(test)]
mod tests;
