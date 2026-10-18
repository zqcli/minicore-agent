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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::text::floor_char_boundary;
use crate::workspace::ReadPrefix;
use crate::{Workspace, WorkspaceError};

use super::{
    DEFAULT_READ_LIMIT, DEFAULT_READ_OFFSET, MAX_DIRECTORY_ENTRIES, MAX_READ_BYTES, MAX_READ_LINES,
    emit_phase, map_workspace_error, run_controlled,
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
            let input: ReadInput = super::decode_input(&self.spec, &invocation, &context)?;
            if input.offset == 0 || !(1..=MAX_READ_LINES).contains(&input.limit) {
                return Err(ToolError::InvalidInvocation);
            }
            let output = run_controlled(&context, async {
                #[cfg(test)]
                if let Some(gate) = super::IO_GATES.take(&(TOOL_NAME, input.path.clone())) {
                    gate.pass().await;
                }
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
        #[cfg(test)]
        if let Some(gate) = ENTRY_GATES.take(&entry.path()) {
            gate.pass().await;
        }
        let metadata = match tokio::fs::symlink_metadata(entry.path()).await {
            Ok(metadata) => metadata,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                continue;
            }
            Err(_) => return Err(ToolError::Failed),
        };
        count += 1;
        let name = entry.file_name();
        let mut name = match name.to_str() {
            Some(name) => super::escape_control_characters(name),
            None => format!(
                "{} [non-UTF-8 name]",
                name.as_encoded_bytes().escape_ascii()
            ),
        };
        let file_type = metadata.file_type();
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

async fn read_file(workspace: &Workspace, input: &ReadInput) -> Result<String, ToolError> {
    let file = workspace
        .open_regular_file(&input.path)
        .await
        .map_err(map_workspace_error)?;
    let mut reader = BufReader::new(file);
    let mut skipped = 0;
    while skipped < input.offset - 1 {
        let chunk = reader.fill_buf().await.map_err(|_| ToolError::Failed)?;
        if chunk.is_empty() {
            return Ok(format!("[no lines at or after offset {}]", input.offset));
        }
        let mut consumed = chunk.len();
        for (index, byte) in chunk.iter().enumerate() {
            if *byte == b'\n' {
                skipped += 1;
                if skipped == input.offset - 1 {
                    consumed = index + 1;
                    break;
                }
            }
        }
        reader.consume(consumed);
    }
    let mut bytes = Vec::new();
    reader
        .take((MAX_READ_BYTES + 4) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| ToolError::Failed)?;
    let prefix = ReadPrefix {
        visible_len: bytes.len().min(MAX_READ_BYTES),
        has_more: bytes.len() > MAX_READ_BYTES,
        bytes,
    };
    let byte_truncated = prefix.has_more;
    let text = match decode_text_prefix(prefix) {
        Ok(text) if !text.contains('\0') => text,
        _ => return Ok("Cannot read binary or non-UTF-8 content as text.".to_owned()),
    };
    if text.is_empty() {
        let mut output = "[empty file]".to_owned();
        if byte_truncated {
            append_truncated(&mut output);
        }
        return Ok(output);
    }

    let mut lines = text.split_terminator('\n').enumerate();
    let mut output = String::new();
    let mut selected = 0usize;
    let mut output_truncated = false;
    while selected < input.limit {
        let Some((index, line)) = lines.next() else {
            break;
        };
        selected += 1;
        let mut rendered = format!("{}: ", index + input.offset);
        for character in line.chars() {
            if character.is_control() && character != '\t' {
                rendered.extend(character.escape_default());
            } else {
                rendered.push(character);
            }
        }
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

    let mut text = std::str::from_utf8(&prefix.bytes[..visible_end])
        .map_err(|_| ToolError::Failed)?
        .replace("\r\n", "\n");
    if text.ends_with('\r') && prefix.bytes.get(visible_end) == Some(&b'\n') {
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
static ENTRY_GATES: crate::test_support::Registry<PathBuf, Arc<crate::test_support::Gate>> =
    crate::test_support::Registry::new();

#[cfg(test)]
mod tests;
