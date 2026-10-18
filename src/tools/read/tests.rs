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
use crate::tools::IO_GATES;

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
        execute(&tool, json!({"path": "invalid-continuation"}))
            .await
            .unwrap(),
        "Cannot read binary or non-UTF-8 content as text."
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
        execute(&tool, json!({"path": "eof-incomplete"}))
            .await
            .unwrap(),
        "Cannot read binary or non-UTF-8 content as text."
    );

    tokio::fs::write(root.join("bare-cr"), b"before\rafter")
        .await
        .unwrap();
    assert_eq!(
        execute(&tool, json!({"path": "bare-cr"})).await.unwrap(),
        "1: before\\rafter"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn read_streams_to_lines_beyond_the_window_and_escapes_controls() {
    let (base, _, tool) = fixture("late-lines").await;
    let text = format!(
        "{}late\x0c\x1b\rend\n",
        "padding\n".repeat(MAX_READ_BYTES / 8 + 1)
    );
    tokio::fs::write(base.join("root/large"), text)
        .await
        .unwrap();
    let offset = MAX_READ_BYTES / 8 + 2;
    let output = execute(&tool, json!({"path": "large", "offset": offset}))
        .await
        .unwrap();
    assert_eq!(output, format!("{offset}: late\\u{{c}}\\u{{1b}}\\rend"));
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
async fn binary_content_is_explained_and_controls_are_escaped() {
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
        execute(&tool, json!({"path": "nul"})).await.unwrap(),
        "Cannot read binary or non-UTF-8 content as text."
    );
    assert_eq!(
        execute(&tool, json!({"path": "escape-control"}))
            .await
            .unwrap(),
        "1: text\\u{1b}more"
    );
    assert_eq!(
        execute(&tool, json!({"path": "invalid"})).await.unwrap(),
        "Cannot read binary or non-UTF-8 content as text."
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
    let gate = Arc::new(crate::test_support::Gate::new());
    IO_GATES.arm((TOOL_NAME, "file".to_owned()), Arc::clone(&gate));
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
    gate.wait_started().await;
    cancellation.cancel();
    assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));

    let gate = Arc::new(crate::test_support::Gate::new());
    IO_GATES.arm((TOOL_NAME, "file".to_owned()), Arc::clone(&gate));
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
    gate.wait_started().await;
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
