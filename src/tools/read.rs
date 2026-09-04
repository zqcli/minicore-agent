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

use crate::workspace::ReadPrefix;
use crate::{Workspace, WorkspaceError};

use super::{
    DEFAULT_READ_LIMIT, DEFAULT_READ_OFFSET, MAX_DIRECTORY_ENTRIES, MAX_READ_BYTES, MAX_READ_LINES,
    map_workspace_error, precheck_control, run_controlled, wait_for_test_io,
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

fn floor_char_boundary(value: &str, maximum: usize) -> usize {
    let mut end = maximum.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}

const fn default_offset() -> usize {
    DEFAULT_READ_OFFSET
}

const fn default_limit() -> usize {
    DEFAULT_READ_LIMIT
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::ToolCallId;
    use minicore_runtime::tools::{
        Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::tools::{ToolIoGate, block_next_io};

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, ReadTool) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-read-tool-{label}-{}",
            crate::ids::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let tool = ReadTool::new(Arc::clone(&workspace));
        (base, workspace, tool)
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            tool_call_id: ToolCallId::new("read-call").unwrap(),
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

    async fn execute(tool: &ReadTool, arguments: Value) -> Result<String, ToolError> {
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
            ToolExecutionOutcome::RequestInput(_) => panic!("read must not request input"),
        }
    }

    async fn cleanup(base: &Path) {
        let _ = tokio::fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn schema_is_strict_and_file_lines_support_defaults_offset_and_limit() {
        let (base, _, tool) = fixture("lines").await;
        let root = base.join("root");
        tokio::fs::write(root.join("lines.txt"), b"alpha\nbeta\ngamma\n")
            .await
            .unwrap();
        tokio::fs::write(root.join("empty.txt"), b"").await.unwrap();
        let schema = tool.spec().input_schema();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], json!(["path"]));
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["offset"]["minimum"], 1);
        assert_eq!(schema["properties"]["limit"]["maximum"], MAX_READ_LINES);

        assert_eq!(
            execute(&tool, json!({"path": "lines.txt"})).await.unwrap(),
            "1: alpha\n2: beta\n3: gamma"
        );
        assert_eq!(
            execute(&tool, json!({"path": "lines.txt", "offset": 2, "limit": 1}))
                .await
                .unwrap(),
            "2: beta\n[truncated]"
        );
        assert_eq!(
            execute(&tool, json!({"path": "empty.txt"})).await.unwrap(),
            "[empty file]"
        );
        assert_eq!(
            execute(&tool, json!({"path": "lines.txt", "offset": 99}))
                .await
                .unwrap(),
            "[no lines at or after offset 99]"
        );
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "missing.txt"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        for arguments in [
            json!({"path": "lines.txt", "offset": 0}),
            json!({"path": "lines.txt", "limit": 0}),
            json!({"path": "lines.txt", "limit": MAX_READ_LINES + 1}),
            json!({"path": "lines.txt", "extra": true}),
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

    #[tokio::test]
    async fn file_reads_enforce_line_and_byte_truncation_with_utf8_boundaries() {
        let (base, _, tool) = fixture("truncation").await;
        let root = base.join("root");
        let lines = (1..=5)
            .map(|line| format!("line-{line}"))
            .collect::<Vec<_>>()
            .join("\n");
        tokio::fs::write(root.join("many.txt"), lines)
            .await
            .unwrap();
        assert_eq!(
            execute(&tool, json!({"path": "many.txt", "limit": 2}))
                .await
                .unwrap(),
            "1: line-1\n2: line-2\n[truncated]"
        );

        let mut long = vec![b'a'; MAX_READ_BYTES - 1];
        long.extend_from_slice("€".as_bytes());
        tokio::fs::write(root.join("boundary.txt"), long)
            .await
            .unwrap();
        let output = execute(&tool, json!({"path": "boundary.txt"}))
            .await
            .unwrap();
        assert!(output.ends_with(TRUNCATED));
        assert!(output.len() <= MAX_TEXT_BYTES);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn byte_cap_lookahead_validates_utf8_crlf_and_true_eof() {
        let (base, _, tool) = fixture("lookahead").await;
        let root = base.join("root");

        for (name, character) in [("two", "¢"), ("three", "€"), ("four", "😀")] {
            let encoded = character.as_bytes();
            let mut content = vec![b'a'; MAX_READ_BYTES - (encoded.len() - 1)];
            content.extend_from_slice(encoded);
            tokio::fs::write(root.join(name), content).await.unwrap();
            let output = execute(&tool, json!({"path": name})).await.unwrap();
            assert!(output.ends_with(TRUNCATED));
            assert!(!output.contains('�'));
        }

        let mut invalid = vec![b'a'; MAX_READ_BYTES - 1];
        invalid.extend_from_slice(&[0xe2, b'X']);
        tokio::fs::write(root.join("invalid-continuation"), invalid)
            .await
            .unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "invalid-continuation"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );

        let mut crlf = vec![b'a'; MAX_READ_BYTES - 1];
        crlf.extend_from_slice(b"\r\nmore");
        tokio::fs::write(root.join("crlf-split"), crlf)
            .await
            .unwrap();
        let output = execute(&tool, json!({"path": "crlf-split"})).await.unwrap();
        assert!(output.ends_with(TRUNCATED));
        assert!(!output.contains('\r'));

        tokio::fs::write(root.join("eof-incomplete"), b"text\xe2\x82")
            .await
            .unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "eof-incomplete"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );

        tokio::fs::write(root.join("bare-cr"), b"before\rafter")
            .await
            .unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "bare-cr"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        cleanup(&base).await;
    }

    #[test]
    fn decoder_excludes_lookahead_and_strips_split_crlf() {
        for (bytes, visible_len, expected) in [
            (b"aaa\xc2\xa2".as_slice(), 4, "aaa"),
            (b"aa\xe2\x82\xac".as_slice(), 4, "aa"),
            (b"a\xf0\x9f\x98\x80".as_slice(), 4, "a"),
            (b"abc\r\n".as_slice(), 4, "abc"),
        ] {
            assert_eq!(
                decode_text_prefix(ReadPrefix {
                    bytes: bytes.to_vec(),
                    visible_len,
                    has_more: true,
                })
                .unwrap(),
                expected
            );
        }
        for bytes in [b"abc\xe2X".as_slice(), b"abc\xe2".as_slice()] {
            assert_eq!(
                decode_text_prefix(ReadPrefix {
                    bytes: bytes.to_vec(),
                    visible_len: 4,
                    has_more: bytes.len() > 4,
                }),
                Err(ToolError::Failed)
            );
        }
    }

    #[tokio::test]
    async fn binary_nul_and_invalid_utf8_fail_without_output_bytes() {
        let (base, _, tool) = fixture("binary").await;
        let root = base.join("root");
        tokio::fs::write(root.join("nul"), b"text\0more")
            .await
            .unwrap();
        tokio::fs::write(root.join("invalid"), [0xff, 0xfe])
            .await
            .unwrap();
        tokio::fs::write(root.join("escape-control"), b"text\x1bmore")
            .await
            .unwrap();

        assert_eq!(
            tool.execute(
                invocation(json!({"path": "nul"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "escape-control"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "invalid"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn directory_listing_is_sorted_marks_directories_and_truncates_at_1000() {
        let (base, _, tool) = fixture("directory").await;
        let root = base.join("root");
        tokio::fs::create_dir(root.join("empty-directory"))
            .await
            .unwrap();
        assert_eq!(
            execute(&tool, json!({"path": "empty-directory"}))
                .await
                .unwrap(),
            "[empty directory]"
        );
        let small = root.join("small");
        tokio::fs::create_dir(&small).await.unwrap();
        tokio::fs::write(small.join("b.txt"), b"").await.unwrap();
        tokio::fs::write(small.join("a.txt"), b"").await.unwrap();
        tokio::fs::create_dir(small.join("dir")).await.unwrap();
        assert_eq!(
            execute(&tool, json!({"path": "small"})).await.unwrap(),
            "a.txt\nb.txt\ndir/"
        );

        let large = root.join("large");
        tokio::fs::create_dir(&large).await.unwrap();
        for index in 0..=MAX_DIRECTORY_ENTRIES {
            tokio::fs::write(large.join(format!("entry-{index:04}")), b"")
                .await
                .unwrap();
        }
        let output = execute(&tool, json!({"path": "large"})).await.unwrap();
        assert!(output.ends_with(TRUNCATED));
        assert_eq!(output.lines().count(), MAX_DIRECTORY_ENTRIES + 1);
        assert!(output.starts_with("entry-0000\nentry-0001"));
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn path_escape_is_invalid_and_directory_children_are_not_followed() {
        use std::os::unix::fs::symlink;

        let (base, _, tool) = fixture("escape").await;
        let root = base.join("root");
        let outside = base.join("outside");
        tokio::fs::create_dir(&outside).await.unwrap();
        tokio::fs::write(outside.join("secret"), b"secret")
            .await
            .unwrap();
        symlink(&outside, root.join("escape")).unwrap();
        tokio::fs::create_dir(root.join("listing")).await.unwrap();
        symlink(&outside, root.join("listing/link")).unwrap();

        assert_eq!(
            tool.execute(
                invocation(json!({"path": "../outside/secret"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "escape/secret"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(
            execute(&tool, json!({"path": "listing"})).await.unwrap(),
            "link"
        );
        cleanup(&base).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn directory_listing_rejects_non_utf8_names_without_lossy_merging() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let (base, _, tool) = fixture("directory-non-utf8").await;
        let root = base.join("root/non-utf8");
        tokio::fs::create_dir(&root).await.unwrap();
        for name in [
            OsString::from_vec(vec![b'n', 0xff]),
            OsString::from_vec(vec![b'n', 0xfe]),
            OsString::from("n�"),
        ] {
            tokio::fs::write(root.join(name), b"").await.unwrap();
        }
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "non-utf8"})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn cancellation_and_deadline_interrupt_waiting_read_operations() {
        let (base, _, tool) = fixture("control").await;
        let root = base.join("root");
        tokio::fs::write(root.join("file"), b"content")
            .await
            .unwrap();
        let tool = Arc::new(tool);

        let cancellation = CancellationToken::new();
        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "file"));
        block_next_io(Arc::clone(&gate));
        let cancelled_tool = Arc::clone(&tool);
        let cancelled_context = context(
            cancellation.clone(),
            Instant::now() + Duration::from_secs(5),
        );
        let cancelled = tokio::spawn(async move {
            cancelled_tool
                .execute(invocation(json!({"path": "file"})), cancelled_context)
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        cancellation.cancel();
        assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));

        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "file"));
        block_next_io(Arc::clone(&gate));
        let deadline_tool = Arc::clone(&tool);
        let deadline = tokio::spawn(async move {
            deadline_tool
                .execute(
                    invocation(json!({"path": "file"})),
                    context(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_millis(25),
                    ),
                )
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        assert_eq!(deadline.await.unwrap(), Err(ToolError::TimedOut));

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "file", "offset": 0})),
                context(cancellation, Instant::now() - Duration::from_secs(1)),
            )
            .await,
            Err(ToolError::Cancelled)
        );
        cleanup(&base).await;
    }
}
