use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use minicore_runtime::value::MAX_TEXT_BYTES;
use serde::Deserialize;
use serde_json::json;

use crate::text::floor_char_boundary;
use crate::workspace::ReadPrefix;
use crate::{Workspace, WorkspaceError};

use super::{
    DEFAULT_READ_LIMIT, DEFAULT_READ_OFFSET, MAX_DIRECTORY_ENTRIES, MAX_READ_BYTES, MAX_READ_LINES,
    emit_phase, map_workspace_error, precheck_control, run_controlled, wait_for_test_io,
};

const TOOL_NAME: &str = "read";
const TRUNCATED: &str = "[truncated]";
const OUTPUT_CONTENT_BUDGET: usize = MAX_TEXT_BYTES - TRUNCATED.len() - 1;

pub(super) struct ReadTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    path: String,
    #[serde(default = "default_offset")]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

impl ReadTool {
    pub(super) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("read is a valid tool name"),
            "Read a UTF-8 text file with numbered lines or list one workspace directory level.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative file or directory path."
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 1,
                        "default": DEFAULT_READ_OFFSET,
                        "description": "One-based first line for file reads."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_READ_LINES,
                        "default": DEFAULT_READ_LIMIT,
                        "description": "Maximum file lines to return."
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        )
        .expect("static read tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for ReadTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        Box::pin(async move {
            precheck_control(&context)?;
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input: ReadInput = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            if input.offset == 0 || !(1..=MAX_READ_LINES).contains(&input.limit) {
                return Err(ToolError::InvalidInvocation);
            }
            let output = run_controlled(&context, async {
                wait_for_test_io(TOOL_NAME, &input.path).await;
                emit_phase(&context, "reading");
                read_path(&self.workspace, &input).await
            })
            .await?;
            let output = ToolOutput::new(output).map_err(|_| ToolError::Internal)?;
            Ok(ToolExecutionOutcome::Completed(output))
        })
    }
}

async fn read_path(workspace: &Workspace, input: &ReadInput) -> Result<String, ToolError> {
    match workspace.resolve_directory(&input.path).await {
        Ok(directory) => list_directory(directory).await,
        Err(WorkspaceError::NotDirectory) => read_file(workspace, input).await,
        Err(error) => Err(map_workspace_error(error)),
    }
}

async fn list_directory(directory: PathBuf) -> Result<String, ToolError> {
    let mut reader = tokio::fs::read_dir(directory)
        .await
        .map_err(|_| ToolError::Failed)?;
    let mut entries = BinaryHeap::new();
    let mut count = 0usize;
    while let Some(entry) = reader.next_entry().await.map_err(|_| ToolError::Failed)? {
        count = count.saturating_add(1);
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| ToolError::Failed)?;
        let mut name = safe_entry_name(&name);
        let file_type = entry.file_type().await.map_err(|_| ToolError::Failed)?;
        if file_type.is_dir() {
            name.push('/');
        }
        if entries.len() < MAX_DIRECTORY_ENTRIES {
            entries.push(name);
        } else if entries.peek().is_some_and(|largest| name < *largest) {
            let _ = entries.pop();
            entries.push(name);
        }
    }
    let entries = entries.into_sorted_vec();
    if entries.is_empty() {
        return Ok("[empty directory]".to_owned());
    }
    let mut output = String::new();
    let mut truncated = count > MAX_DIRECTORY_ENTRIES;
    for entry in entries {
        if !push_bounded_line(&mut output, &entry) {
            truncated = true;
            break;
        }
    }
    if truncated {
        append_truncated(&mut output);
    }
    Ok(output)
}

fn safe_entry_name(name: &str) -> String {
    let mut output = String::with_capacity(name.len());
    for character in name.chars() {
        if character.is_control() {
            output.extend(character.escape_default());
        } else {
            output.push(character);
        }
    }
    output
}

async fn read_file(workspace: &Workspace, input: &ReadInput) -> Result<String, ToolError> {
    let prefix = workspace
        .read_prefix(&input.path, MAX_READ_BYTES)
        .await
        .map_err(map_workspace_error)?;
    let byte_truncated = prefix.has_more;
    let text = decode_text_prefix(prefix)?;
    if text
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(ToolError::Failed);
    }
    if text.is_empty() {
        let mut output = "[empty file]".to_owned();
        if byte_truncated {
            append_truncated(&mut output);
        }
        return Ok(output);
    }

    let mut lines = text.lines().enumerate().skip(input.offset - 1);
    let mut output = String::new();
    let mut selected = 0usize;
    let mut output_truncated = false;
    while selected < input.limit {
        let Some((index, line)) = lines.next() else {
            break;
        };
        selected += 1;
        let rendered = format!("{}: {line}", index + 1);
        if !push_bounded_line(&mut output, &rendered) {
            output_truncated = true;
            break;
        }
    }
    let line_truncated = lines.next().is_some();
    let truncated = byte_truncated || line_truncated || output_truncated;
    if output.is_empty() {
        output = format!("[no lines at or after offset {}]", input.offset);
    }
    if truncated {
        append_truncated(&mut output);
    }
    Ok(output)
}

fn decode_text_prefix(prefix: ReadPrefix) -> Result<String, ToolError> {
    let visible = &prefix.bytes[..prefix.visible_len];
    let visible_end = match std::str::from_utf8(visible) {
        Ok(_) => prefix.visible_len,
        Err(error) if error.error_len().is_none() => {
            let start = error.valid_up_to();
            let width = prefix
                .bytes
                .get(start)
                .copied()
                .and_then(utf8_sequence_width)
                .ok_or(ToolError::Failed)?;
            let end = start.checked_add(width).ok_or(ToolError::Failed)?;
            if end > prefix.bytes.len()
                || end <= prefix.visible_len
                || std::str::from_utf8(&prefix.bytes[start..end]).is_err()
            {
                return Err(ToolError::Failed);
            }
            start
        }
        Err(_) => return Err(ToolError::Failed),
    };

    for index in 0..visible_end {
        if prefix.bytes[index] == b'\r' && prefix.bytes.get(index + 1) != Some(&b'\n') {
            return Err(ToolError::Failed);
        }
    }
    let mut text = std::str::from_utf8(&prefix.bytes[..visible_end])
        .map_err(|_| ToolError::Failed)?
        .replace("\r\n", "\n");
    if text.ends_with('\r') {
        text.pop();
    }
    Ok(text)
}

fn utf8_sequence_width(first: u8) -> Option<usize> {
    match first {
        0xc2..=0xdf => Some(2),
        0xe0..=0xef => Some(3),
        0xf0..=0xf4 => Some(4),
        _ => None,
    }
}

fn push_bounded_line(output: &mut String, line: &str) -> bool {
    let separator = usize::from(!output.is_empty());
    let available = OUTPUT_CONTENT_BUDGET.saturating_sub(output.len() + separator);
    if !output.is_empty() && available > 0 {
        output.push('\n');
    }
    if line.len() <= available {
        output.push_str(line);
        return true;
    }
    let end = floor_char_boundary(line, available);
    output.push_str(&line[..end]);
    false
}

fn append_truncated(output: &mut String) {
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(TRUNCATED);
}

const fn default_offset() -> usize {
    DEFAULT_READ_OFFSET
}

const fn default_limit() -> usize {
    DEFAULT_READ_LIMIT
}

#[cfg(test)]
mod tests;
