use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use crate::{Workspace, WorkspaceError};

use super::truncate::format_size;
use super::{
    DEFAULT_READ_LIMIT, DEFAULT_READ_OFFSET, MAX_DIRECTORY_ENTRIES, MAX_OUTPUT_BYTES,
    MAX_READ_LINES, emit_phase, map_workspace_error, run_controlled,
};

const TOOL_NAME: &str = "read";
const BINARY: &str = "Cannot read binary or non-UTF-8 content as text.";

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
            "Read a UTF-8 text file with numbered lines or list one workspace directory level. \
             Output is cut to 2000 lines or 50KB, whichever is hit first; a notice then gives \
             the offset to continue from.",
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
                        "description": "Maximum file lines to return (the 50KB output limit may cut sooner)."
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
    let mut shown = 0;
    for entry in &entries {
        let separator = usize::from(shown > 0);
        if output.len() + separator + entry.len() > MAX_OUTPUT_BYTES {
            break;
        }
        if shown > 0 {
            output.push('\n');
        }
        output.push_str(entry);
        shown += 1;
    }
    if shown < count {
        let limit = if shown < entries.len() {
            format!(" ({} limit)", format_size(MAX_OUTPUT_BYTES))
        } else {
            String::new()
        };
        output.push_str(&format!("\n\n[Showing {shown} of {count} entries{limit}.]"));
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
    // Rendered lines are never shorter than their source, so no more than the
    // output budget can be shown; one extra byte tells whether the file goes on.
    let mut bytes = Vec::new();
    (&mut reader)
        .take(MAX_OUTPUT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| ToolError::Failed)?;
    let more = bytes.len() > MAX_OUTPUT_BYTES;
    // Only whole lines are shown, so a read cut mid-line drops the partial one.
    let rest = if more {
        bytes.split_off(
            bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |at| at + 1),
        )
    } else {
        Vec::new()
    };
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) if !text.contains('\0') => text.replace("\r\n", "\n"),
        _ => return Ok(BINARY.to_owned()),
    };
    if text.is_empty() && !more {
        return Ok("[empty file]".to_owned());
    }

    let mut lines = text.split_terminator('\n').peekable();
    let mut output = String::new();
    let mut shown = 0;
    while shown < input.limit
        && let Some(line) = lines.peek()
    {
        let mut rendered = format!("{}: ", input.offset + shown);
        for character in line.chars() {
            if character.is_control() && character != '\t' {
                rendered.extend(character.escape_default());
            } else {
                rendered.push(character);
            }
        }
        let separator = usize::from(shown > 0);
        if output.len() + separator + rendered.len() > MAX_OUTPUT_BYTES {
            break;
        }
        if shown > 0 {
            output.push('\n');
        }
        output.push_str(&rendered);
        lines.next();
        shown += 1;
    }
    if shown == 0 {
        let path = input.path.replace('\'', "'\\''");
        return Ok(format!(
            "[Line {} exceeds the {} limit. Use bash: sed -n '{}p' '{path}' | head -c {MAX_OUTPUT_BYTES}]",
            input.offset,
            format_size(MAX_OUTPUT_BYTES),
            input.offset,
        ));
    }
    let after = lines.count()
        + if more {
            count_lines(&rest, &mut reader).await?
        } else {
            0
        };
    if after > 0 {
        let end = input.offset + shown - 1;
        let total = end + after;
        let limit = if shown < input.limit {
            format!(" ({} limit)", format_size(MAX_OUTPUT_BYTES))
        } else {
            String::new()
        };
        output.push_str(&format!(
            "\n\n[Showing lines {}-{end} of {total}{limit}. Use offset={} to continue.]",
            input.offset,
            end + 1
        ));
    }
    Ok(output)
}

/// Counts the lines of `head` followed by the rest of the file.
async fn count_lines(
    head: &[u8],
    reader: &mut BufReader<impl AsyncRead + Unpin>,
) -> Result<usize, ToolError> {
    let mut lines = head.iter().filter(|byte| **byte == b'\n').count();
    let mut last = head.last().copied();
    loop {
        let chunk = reader.fill_buf().await.map_err(|_| ToolError::Failed)?;
        let Some(&end) = chunk.last() else {
            break;
        };
        lines += chunk.iter().filter(|byte| **byte == b'\n').count();
        last = Some(end);
        let consumed = chunk.len();
        reader.consume(consumed);
    }
    Ok(lines + usize::from(last.is_some_and(|byte| byte != b'\n')))
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
