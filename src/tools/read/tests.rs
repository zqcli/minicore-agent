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
use crate::tools::{IO_GATES, MAX_READ_BYTES};

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
        "2: beta\n\n[Showing lines 2-2 of 3. Use offset=3 to continue.]"
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
async fn a_limit_below_the_file_length_names_the_next_offset() {
    let (base, _, tool) = fixture("limit").await;
    let lines = (1..=5)
        .map(|line| format!("line-{line}"))
        .collect::<Vec<_>>()
        .join("\n");
    tokio::fs::write(base.join("root/many.txt"), lines)
        .await
        .unwrap();
    assert_eq!(
        execute(&tool, json!({"path": "many.txt", "limit": 2}))
            .await
            .unwrap(),
        "1: line-1\n2: line-2\n\n[Showing lines 1-2 of 5. Use offset=3 to continue.]"
    );
    assert_eq!(
        execute(&tool, json!({"path": "many.txt", "offset": 4, "limit": 2}))
            .await
            .unwrap(),
        "4: line-4\n5: line-5"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn a_file_longer_than_2000_lines_is_cut_with_the_next_offset() {
    let (base, _, tool) = fixture("line-cap").await;
    let root = base.join("root");
    tokio::fs::write(root.join("long.txt"), "x\n".repeat(2500))
        .await
        .unwrap();
    let output = execute(&tool, json!({"path": "long.txt"})).await.unwrap();
    assert!(output.contains("\n2000: x\n\n"));
    assert!(!output.contains("2001: x"));
    assert!(output.ends_with("\n\n[Showing lines 1-2000 of 2500. Use offset=2001 to continue.]"));
    assert_eq!(
        execute(&tool, json!({"path": "long.txt", "offset": 2001}))
            .await
            .unwrap()
            .lines()
            .count(),
        500
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn a_file_larger_than_50_kib_is_cut_at_a_line_with_the_next_offset() {
    let (base, _, tool) = fixture("byte-cap").await;
    let root = base.join("root");
    // 1000 lines of 100 bytes, the last one without a newline.
    let text = (1..=1000)
        .map(|line| format!("{line:0>99}"))
        .collect::<Vec<_>>()
        .join("\n");
    tokio::fs::write(root.join("wide.txt"), text).await.unwrap();
    let output = execute(&tool, json!({"path": "wide.txt"})).await.unwrap();
    let (body, notice) = output.split_once("\n\n[").unwrap();
    assert!(body.len() <= MAX_OUTPUT_BYTES);
    let shown = body.lines().count();
    assert!(shown < 2000 && shown > 400);
    assert_eq!(
        notice,
        format!(
            "Showing lines 1-{shown} of 1000 (50KB limit). Use offset={} to continue.]",
            shown + 1
        )
    );
    let next = execute(&tool, json!({"path": "wide.txt", "offset": shown + 1}))
        .await
        .unwrap();
    assert!(next.starts_with(&format!("{}: ", shown + 1)));
    cleanup(&base).await;
}

#[tokio::test]
async fn a_character_split_at_the_byte_cut_is_never_shown_half() {
    let (base, _, tool) = fixture("utf8-cut").await;
    let root = base.join("root");
    let line = "€".repeat(10_000);
    tokio::fs::write(root.join("euro.txt"), format!("{line}\r\n{line}\r\n"))
        .await
        .unwrap();
    let output = execute(&tool, json!({"path": "euro.txt"})).await.unwrap();
    assert_eq!(
        output,
        format!("1: {line}\n\n[Showing lines 1-1 of 2 (50KB limit). Use offset=2 to continue.]")
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn a_first_line_over_the_limit_points_to_bash() {
    let (base, _, tool) = fixture("long-line").await;
    let root = base.join("root");
    tokio::fs::write(
        root.join("it's.txt"),
        format!("short\n{}\nafter\n", "a".repeat(60_000)),
    )
    .await
    .unwrap();
    let hint =
        "[Line 2 exceeds the 50KB limit. Use bash: sed -n '2p' 'it'\\''s.txt' | head -c 51200]";
    assert_eq!(
        execute(&tool, json!({"path": "it's.txt", "offset": 2}))
            .await
            .unwrap(),
        hint
    );
    tokio::fs::write(root.join("one.txt"), "b".repeat(MAX_OUTPUT_BYTES + 5))
        .await
        .unwrap();
    assert_eq!(
        execute(&tool, json!({"path": "one.txt"})).await.unwrap(),
        "[Line 1 exceeds the 50KB limit. Use bash: sed -n '1p' 'one.txt' | head -c 51200]"
    );
    assert_eq!(
        execute(&tool, json!({"path": "it's.txt", "offset": 3}))
            .await
            .unwrap(),
        "3: after"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn a_read_that_cuts_nothing_has_no_notice() {
    let (base, _, tool) = fixture("no-notice").await;
    let root = base.join("root");
    tokio::fs::write(root.join("exact.txt"), "x\n".repeat(2000))
        .await
        .unwrap();
    let output = execute(&tool, json!({"path": "exact.txt"})).await.unwrap();
    assert!(output.ends_with("\n2000: x"));
    // Just under the byte limit, with a CRLF that is normalized.
    let text = format!("{}\r\n", "y".repeat(MAX_OUTPUT_BYTES - 4));
    tokio::fs::write(root.join("fits.txt"), text).await.unwrap();
    let output = execute(&tool, json!({"path": "fits.txt"})).await.unwrap();
    assert_eq!(output, format!("1: {}", "y".repeat(MAX_OUTPUT_BYTES - 4)));
    cleanup(&base).await;
}

#[tokio::test]
async fn binary_and_incomplete_utf8_are_explained() {
    let (base, _, tool) = fixture("invalid").await;
    let root = base.join("root");
    tokio::fs::write(root.join("eof-incomplete"), b"text\xe2\x82")
        .await
        .unwrap();
    assert_eq!(
        execute(&tool, json!({"path": "eof-incomplete"}))
            .await
            .unwrap(),
        BINARY
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

#[tokio::test]
async fn binary_content_is_explained_and_controls_are_escaped() {
    let (base, _, tool) = fixture("binary").await;
    let root = base.join("root");
    tokio::fs::write(root.join("nul-byte"), b"text\0more")
        .await
        .unwrap();
    tokio::fs::write(root.join("invalid"), [0xff, 0xfe])
        .await
        .unwrap();
    tokio::fs::write(root.join("escape-control"), b"text\x1bmore")
        .await
        .unwrap();

    assert_eq!(
        execute(&tool, json!({"path": "nul-byte"})).await.unwrap(),
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
    assert!(output.ends_with("\n\n[Showing 1000 of 1001 entries.]"));
    assert_eq!(output.lines().count(), MAX_DIRECTORY_ENTRIES + 2);
    assert!(output.starts_with("entry-0000\nentry-0001"));

    let wide = root.join("wide");
    tokio::fs::create_dir(&wide).await.unwrap();
    for index in 0..600 {
        tokio::fs::write(wide.join(format!("{index:0>100}")), b"")
            .await
            .unwrap();
    }
    let output = execute(&tool, json!({"path": "wide"})).await.unwrap();
    let (body, notice) = output.split_once("\n\n[").unwrap();
    assert!(body.len() <= MAX_OUTPUT_BYTES);
    assert_eq!(
        notice,
        format!(
            "Showing {} of 600 entries (50KB limit).]",
            body.lines().count()
        )
    );
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
async fn directory_listing_escapes_and_marks_non_utf8_names_without_merging() {
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
        execute(&tool, json!({"path": "non-utf8"})).await.unwrap(),
        "n\\xfe [non-UTF-8 name]\nn\\xff [non-UTF-8 name]\nn�"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn directory_listing_skips_an_entry_removed_before_its_metadata_read() {
    let (base, workspace, tool) = fixture("vanished-entry").await;
    let gone = workspace.root().join("gone");
    tokio::fs::write(&gone, b"").await.unwrap();
    tokio::fs::write(workspace.root().join("kept"), b"")
        .await
        .unwrap();
    let gate = Arc::new(crate::test_support::Gate::new());
    ENTRY_GATES.arm(gone.clone(), Arc::clone(&gate));
    let task = tokio::spawn(async move { execute(&tool, json!({"path": "."})).await });
    gate.wait_started().await;
    tokio::fs::remove_file(gone).await.unwrap();
    gate.release();
    assert_eq!(task.await.unwrap().unwrap(), "kept");
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
