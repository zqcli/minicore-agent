use super::*;
use crate::test_support::{Gate, TempDir};
use crate::workspace::page::{MAX_RESULT_BYTES, MIN_RESULT_BYTES};

struct Fixture {
    root: std::path::PathBuf,
    workspace: Arc<Workspace>,
    _dir: TempDir,
}

async fn fixture(label: &str) -> Fixture {
    let dir = TempDir::new(&format!("workspace-query-{label}"));
    let root = dir.path().to_path_buf();
    let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());
    Fixture {
        root,
        workspace,
        _dir: dir,
    }
}

async fn write(fixture: &Fixture, name: &str, bytes: impl AsRef<[u8]>) {
    tokio::fs::write(fixture.root.join(name), bytes)
        .await
        .unwrap();
}

async fn query(
    fixture: &Fixture,
    request: WorkspaceReadRequest,
) -> Result<WorkspaceReadResult, AgentError> {
    read(
        Arc::clone(&fixture.workspace),
        request,
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
}

fn read_request(path: &str) -> WorkspaceReadRequest {
    WorkspaceReadRequest {
        session_id: SessionId::new().unwrap(),
        path: path.to_owned(),
        start_line: None,
        line_byte_offset: None,
        max_lines: None,
        max_bytes: None,
        if_revision: None,
    }
}

fn digest(bytes: &[u8]) -> String {
    revision_of(bytes)
}

/// Pages until the file ends and returns the reassembled content and the
/// revision every page reported. Every page must fit its encoded budget.
async fn collect_pages(fixture: &Fixture, mut request: WorkspaceReadRequest) -> (String, String) {
    let budget = request.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
    let mut content = Vec::new();
    let mut revision: Option<String> = None;
    for _ in 0..4096 {
        let result = query(fixture, request.clone()).await.unwrap();
        assert_eq!(result.status, WorkspaceReadStatus::Ok, "page status");
        let encoded = serde_json::to_vec(&result).unwrap().len();
        assert!(encoded <= budget, "encoded page {encoded} exceeds {budget}");
        assert!(result.content.is_empty() || result.returned_lines >= 1);
        match &revision {
            Some(expected) => assert_eq!(result.revision.as_deref(), Some(expected.as_str())),
            None => revision = result.revision.clone(),
        }
        content.extend_from_slice(result.content.as_bytes());
        match result.next_range {
            Some(next) => {
                let line = request.start_line.unwrap_or(1);
                let offset = request.line_byte_offset.unwrap_or(0);
                assert!(
                    next.start_line > line
                        || (next.start_line == line && next.line_byte_offset > offset),
                    "pagination must advance"
                );
                request.start_line = Some(next.start_line);
                request.line_byte_offset = Some(next.line_byte_offset);
            }
            None => {
                return (
                    String::from_utf8(content).unwrap(),
                    revision.expect("a complete read carries a revision"),
                );
            }
        }
    }
    panic!("pagination did not finish");
}

#[test]
fn escaped_length_matches_the_json_encoding() {
    let mut sample = String::new();
    for value in 0u32..0x300 {
        if let Some(character) = char::from_u32(value) {
            sample.push(character);
        }
    }
    sample.push('💥');
    sample.push('\u{2028}');
    for end in sample
        .char_indices()
        .map(|(index, character)| index + character.len_utf8())
        .chain(std::iter::once(sample.len()))
    {
        let encoded = serde_json::to_string(&sample[..end]).unwrap();
        assert_eq!(
            json_escaped_len(&sample[..end]) + 2,
            encoded.len(),
            "escaped length of {end} bytes"
        );
    }
}

#[tokio::test]
async fn reads_a_whole_file_with_a_byte_exact_revision() {
    let fixture = fixture("whole").await;
    let bytes = b"alpha\nbeta\ngamma\n";
    write(&fixture, "note.txt", bytes).await;

    let mut request = read_request("note.txt");
    request.max_bytes = Some(4096);
    let result = query(&fixture, request).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Ok);
    assert_eq!(result.content, "alpha\nbeta\ngamma\n");
    assert_eq!(result.start_line, 1);
    assert_eq!(result.returned_lines, 3);
    assert!(!result.truncated);
    assert!(!result.line_truncated);
    assert!(result.next_range.is_none());
    assert_eq!(result.encoding, WorkspaceReadEncoding::Utf8);
    assert_eq!(result.revision.as_deref(), Some(digest(bytes).as_str()));
    assert_eq!(
        result.revision.as_deref(),
        Some(digest(result.content.as_bytes()).as_str())
    );
    assert_eq!(result.file_bytes, bytes.len() as u64);
    assert!(result.file_modified_unix_ms.is_some());

    let mut ranged = read_request("note.txt");
    ranged.start_line = Some(2);
    ranged.max_lines = Some(1);
    let result = query(&fixture, ranged).await.unwrap();
    assert_eq!(result.content, "beta\n");
    assert_eq!(result.start_line, 2);
    assert_eq!(result.returned_lines, 1);
    assert!(result.truncated);
    assert_eq!(
        result.next_range,
        Some(WorkspaceReadRange {
            start_line: 3,
            line_byte_offset: 0,
        })
    );
}

#[tokio::test]
async fn pagination_is_lossless_for_long_unicode_and_control_lines() {
    let fixture = fixture("lossless").await;
    let mut file = Vec::new();
    file.extend_from_slice(b"short\n");
    file.extend_from_slice(
        "h\u{e9}llo w\u{f6}rld \u{2014} unicode "
            .repeat(80)
            .as_bytes(),
    );
    file.extend_from_slice(b"\n");
    file.extend_from_slice("control\u{1}\u{2}\t\u{7f} chars\n".as_bytes());
    file.resize(file.len() + 4096, b'x');
    file.extend_from_slice(b"\r\n");
    file.extend_from_slice(b"tail without newline");
    write(&fixture, "mixed.txt", &file).await;

    let mut whole = read_request("mixed.txt");
    whole.max_bytes = Some(MAX_RESULT_BYTES);
    let result = query(&fixture, whole).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Ok);
    assert_eq!(result.content.as_bytes(), file.as_slice());
    assert!(result.next_range.is_none());
    assert_eq!(result.revision.as_deref(), Some(digest(&file).as_str()));

    let mut request = read_request("mixed.txt");
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let (reassembled, revision) = collect_pages(&fixture, request).await;
    assert_eq!(reassembled.as_bytes(), file.as_slice());
    assert_eq!(revision, digest(&file));
}

#[tokio::test]
async fn line_byte_offset_continues_inside_a_long_line() {
    let fixture = fixture("long-line").await;
    let long = "abc\u{e9}\u{4e2d}\u{1f600}".repeat(2000);
    let file = format!("{long}\nsecond\n");
    write(&fixture, "long.txt", file.as_bytes()).await;

    let mut request = read_request("long.txt");
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let (reassembled, revision) = collect_pages(&fixture, request.clone()).await;
    assert_eq!(reassembled, file);
    assert_eq!(revision, digest(file.as_bytes()));

    let first = query(&fixture, request.clone()).await.unwrap();
    assert_eq!(first.start_line, 1);
    assert!(first.line_truncated);
    let next = first.next_range.expect("a long line continues");
    assert_eq!(next.start_line, 1);
    assert!(next.line_byte_offset > 0);
    request.start_line = Some(next.start_line);
    request.line_byte_offset = Some(next.line_byte_offset);
    let second = query(&fixture, request).await.unwrap();
    assert_eq!(second.start_line, 1);
    assert!(second.line_truncated);
    let mut combined = first.content.clone();
    combined.push_str(&second.content);
    assert!(file.starts_with(&combined));
    assert!(!second.content.is_empty());
}

#[tokio::test]
async fn empty_trailing_newline_and_beyond_eof_ranges_are_explicit() {
    let fixture = fixture("edges").await;

    write(&fixture, "empty.txt", b"").await;
    let result = query(&fixture, read_request("empty.txt")).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Ok);
    assert_eq!(result.content, "");
    assert_eq!(result.returned_lines, 0);
    assert!(!result.truncated);
    assert!(result.next_range.is_none());
    assert_eq!(result.revision.as_deref(), Some(digest(b"").as_str()));

    write(&fixture, "one.txt", b"a\n").await;
    let result = query(&fixture, read_request("one.txt")).await.unwrap();
    assert_eq!(result.content, "a\n");
    assert_eq!(result.returned_lines, 1);
    assert!(result.next_range.is_none());

    write(&fixture, "blank.txt", b"a\n\n").await;
    let result = query(&fixture, read_request("blank.txt")).await.unwrap();
    assert_eq!(result.content, "a\n\n");
    assert_eq!(result.returned_lines, 2);
    assert!(result.next_range.is_none());

    let mut beyond = read_request("one.txt");
    beyond.start_line = Some(9);
    let result = query(&fixture, beyond).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Ok);
    assert_eq!(result.content, "");
    assert_eq!(result.returned_lines, 0);
    assert!(result.next_range.is_none());
}

#[tokio::test]
async fn binary_files_are_reported_without_content() {
    let fixture = fixture("binary").await;

    write(&fixture, "nul.txt", b"text\0more").await;
    let result = query(&fixture, read_request("nul.txt")).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Binary);
    assert_eq!(result.content, "");
    assert_eq!(result.returned_lines, 0);
    assert_eq!(result.encoding, WorkspaceReadEncoding::Unknown);
    assert!(result.next_range.is_none());
    assert!(result.revision.is_some());

    write(
        &fixture,
        "invalid.txt",
        [b'f', b'o', b'o', b'\n', 0xff, b'\n'],
    )
    .await;
    let result = query(&fixture, read_request("invalid.txt")).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Binary);
    assert_eq!(result.content, "");
}

#[tokio::test]
async fn files_at_or_over_the_whole_file_bound_are_explicit() {
    let fixture = fixture("bound").await;

    let exact = b"a".repeat(MAX_READ_BYTES);
    write(&fixture, "exact.txt", &exact).await;
    let mut request = read_request("exact.txt");
    request.max_bytes = Some(MAX_RESULT_BYTES);
    let result = query(&fixture, request).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Ok);
    assert_eq!(result.file_bytes, MAX_READ_BYTES as u64);
    assert_eq!(result.revision.as_deref(), Some(digest(&exact).as_str()));
    assert!(result.next_range.is_some());

    let mut over = b"a".repeat(MAX_READ_BYTES);
    over.push(b'b');
    write(&fixture, "over.txt", &over).await;
    let mut request = read_request("over.txt");
    request.max_bytes = Some(MAX_RESULT_BYTES);
    let result = query(&fixture, request).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::TooLarge);
    assert_eq!(result.content, "");
    assert!(result.revision.is_none());
    assert!(result.next_range.is_none());
    assert_eq!(result.file_bytes, over.len() as u64);
}

#[tokio::test]
async fn revision_mismatch_reports_changed_without_content() {
    let fixture = fixture("revision").await;
    write(&fixture, "small.txt", b"small\n").await;
    let first = query(&fixture, read_request("small.txt")).await.unwrap();
    let revision = first.revision.clone().unwrap();
    assert_eq!(revision, digest(b"small\n"));

    let mut matching = read_request("small.txt");
    matching.if_revision = Some(revision.to_uppercase());
    let result = query(&fixture, matching).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Ok);
    assert_eq!(result.content, "small\n");

    let mut stale = read_request("small.txt");
    stale.if_revision = Some("0".repeat(64));
    let result = query(&fixture, stale).await.unwrap();
    assert_eq!(result.status, WorkspaceReadStatus::Changed);
    assert_eq!(result.content, "");
    assert!(!result.truncated);
    assert!(result.next_range.is_none());
    assert_eq!(result.revision.as_deref(), Some(revision.as_str()));
}

#[tokio::test]
async fn validation_rejects_invalid_paths_ranges_and_offsets() {
    let fixture = fixture("validation").await;
    write(&fixture, "file.txt", b"abc\n").await;

    for path in ["", ".", "..", "../escape", "/etc/hosts", "a/../../b"] {
        let request = read_request(path);
        assert!(
            matches!(request.validate(), Err(AgentError::InvalidArguments)),
            "{path:?} must be rejected without IO"
        );
    }
    let request = read_request("nul\0name");
    assert!(matches!(
        request.validate(),
        Err(AgentError::InvalidArguments)
    ));

    let mut request = read_request("file.txt");
    request.start_line = Some(0);
    assert!(request.validate().is_err());
    let mut request = read_request("file.txt");
    request.line_byte_offset = Some(4);
    assert!(request.validate().is_err());
    let mut request = read_request("file.txt");
    request.max_lines = Some(0);
    assert!(request.validate().is_err());
    let mut request = read_request("file.txt");
    request.max_lines = Some(MAX_MAX_LINES + 1);
    assert!(request.validate().is_err());
    let mut request = read_request("file.txt");
    request.max_bytes = Some(MIN_RESULT_BYTES - 1);
    assert!(request.validate().is_err());
    let mut request = read_request("file.txt");
    request.max_bytes = Some(MAX_RESULT_BYTES + 1);
    assert!(request.validate().is_err());
    let mut request = read_request("file.txt");
    request.if_revision = Some("not-a-revision".to_owned());
    assert!(request.validate().is_err());

    // The path byte cap and the offset bound are lexical: both fail before
    // any file is opened or any query slot is reserved, and the bounds
    // themselves are still accepted.
    let request = read_request(&"a".repeat(MAX_PATH_BYTES));
    assert!(request.validate().is_ok());
    let request = read_request(&"a".repeat(MAX_PATH_BYTES + 1));
    assert!(matches!(
        request.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut request = read_request("file.txt");
    request.start_line = Some(1);
    request.line_byte_offset = Some(MAX_READ_BYTES as u32);
    assert!(request.validate().is_ok());
    request.line_byte_offset = Some(MAX_READ_BYTES as u32 + 1);
    assert!(matches!(
        request.validate(),
        Err(AgentError::InvalidArguments)
    ));

    // A budget that cannot even hold the metadata envelope is an argument
    // error, not a truncation.
    let mut request = read_request(&"a".repeat(900));
    request.max_bytes = Some(MIN_RESULT_BYTES);
    assert!(matches!(
        query(&fixture, request).await,
        Err(AgentError::InvalidArguments)
    ));

    // Offsets must fall inside the line and on a character boundary.
    let mut request = read_request("file.txt");
    request.start_line = Some(1);
    request.line_byte_offset = Some(4);
    assert!(matches!(
        query(&fixture, request).await,
        Err(AgentError::InvalidArguments)
    ));
    let mut request = read_request("file.txt");
    request.start_line = Some(1);
    request.line_byte_offset = Some(5);
    assert!(matches!(
        query(&fixture, request).await,
        Err(AgentError::InvalidArguments)
    ));
    write(&fixture, "wide.txt", "\u{e9}\n").await;
    let mut request = read_request("wide.txt");
    request.start_line = Some(1);
    request.line_byte_offset = Some(1);
    assert!(matches!(
        query(&fixture, request).await,
        Err(AgentError::InvalidArguments)
    ));
    let mut request = read_request("file.txt");
    request.start_line = Some(9);
    request.line_byte_offset = Some(1);
    assert!(matches!(
        query(&fixture, request).await,
        Err(AgentError::InvalidArguments)
    ));

    assert!(matches!(
        query(&fixture, read_request("missing.txt")).await,
        Err(AgentError::Workspace)
    ));
    tokio::fs::create_dir(fixture.root.join("dir"))
        .await
        .unwrap();
    assert!(matches!(
        query(&fixture, read_request("dir")).await,
        Err(AgentError::Workspace)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn special_files_and_a_post_check_swap_do_not_block() {
    let fixture = fixture("special").await;
    write(&fixture, "static.txt", b"data\n").await;
    let fifo = fixture.root.join("pipe");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(status.success());
    let static_result = tokio::time::timeout(
        Duration::from_secs(2),
        query(&fixture, read_request("pipe")),
    )
    .await;
    assert!(
        matches!(static_result, Ok(Err(AgentError::Workspace))),
        "a static FIFO is rejected by metadata"
    );

    write(&fixture, "swap.txt", b"data\n").await;
    let target = std::fs::canonicalize(fixture.root.join("swap.txt")).unwrap();
    crate::workspace::SWAP_OPEN_TO_FIFO.arm(target, ());
    let swapped = tokio::time::timeout(
        Duration::from_secs(2),
        query(&fixture, read_request("swap.txt")),
    )
    .await;
    assert!(
        matches!(swapped, Ok(Err(AgentError::Workspace))),
        "a FIFO swapped in after the metadata check must not block the open"
    );
}

#[tokio::test]
async fn deadline_and_cancellation_stop_a_pending_query() {
    let fixture = fixture("cancel").await;
    write(&fixture, "wait.txt", b"data\n").await;

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let result = read(
        Arc::clone(&fixture.workspace),
        read_request("wait.txt"),
        cancelled,
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(result, Err(AgentError::QueryLimit)));

    // Cancelling the owning Session stops a started query.
    let gate = Arc::new(Gate::new());
    READ_GATES.arm("wait.txt".to_owned(), Arc::clone(&gate));
    let session = CancellationToken::new();
    let pending = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        let session = session.clone();
        async move {
            read(
                workspace,
                read_request("wait.txt"),
                session,
                CancellationToken::new(),
            )
            .await
        }
    });
    gate.wait_started().await;
    session.cancel();
    assert!(matches!(
        pending.await.unwrap(),
        Err(AgentError::QueryLimit)
    ));

    // RPC shutdown cancels the query the same way.
    let gate = Arc::new(Gate::new());
    READ_GATES.arm("wait.txt".to_owned(), Arc::clone(&gate));
    let shutdown = CancellationToken::new();
    let pending = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        let shutdown = shutdown.clone();
        async move {
            read(
                workspace,
                read_request("wait.txt"),
                CancellationToken::new(),
                shutdown,
            )
            .await
        }
    });
    gate.wait_started().await;
    shutdown.cancel();
    assert!(matches!(
        pending.await.unwrap(),
        Err(AgentError::QueryLimit)
    ));

    // The deadline wraps the pending step itself.
    let gate = Arc::new(Gate::new());
    READ_GATES.arm("wait.txt".to_owned(), Arc::clone(&gate));
    let pending = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        async move {
            read_until(
                workspace,
                read_request("wait.txt"),
                CancellationToken::new(),
                CancellationToken::new(),
                Instant::now() + Duration::from_millis(20),
            )
            .await
        }
    });
    gate.wait_started().await;
    assert!(matches!(
        pending.await.unwrap(),
        Err(AgentError::QueryLimit)
    ));

    let gate = Arc::new(Gate::new());
    READ_GATES.arm("wait.txt".to_owned(), Arc::clone(&gate));
    let pending = tokio::spawn({
        let workspace = Arc::clone(&fixture.workspace);
        async move {
            read(
                workspace,
                read_request("wait.txt"),
                CancellationToken::new(),
                CancellationToken::new(),
            )
            .await
        }
    });
    gate.wait_started().await;
    gate.release();
    let result = pending.await.unwrap().unwrap();
    assert_eq!(result.content, "data\n");
}

#[tokio::test]
async fn with_query_budget_wraps_a_pending_step() {
    let deadline = Instant::now() + Duration::from_secs(30);

    let session = CancellationToken::new();
    let shutdown = CancellationToken::new();
    session.cancel();
    let pending = with_query_budget(std::future::pending::<()>(), &session, &shutdown, deadline);
    assert!(matches!(pending.await, Err(AgentError::QueryLimit)));

    let session = CancellationToken::new();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let pending = with_query_budget(std::future::pending::<()>(), &session, &shutdown, deadline);
    assert!(matches!(pending.await, Err(AgentError::QueryLimit)));

    let session = CancellationToken::new();
    let shutdown = CancellationToken::new();
    let pending = with_query_budget(
        std::future::pending::<()>(),
        &session,
        &shutdown,
        Instant::now() + Duration::from_millis(10),
    );
    assert!(matches!(pending.await, Err(AgentError::QueryLimit)));

    let session = CancellationToken::new();
    let shutdown = CancellationToken::new();
    let ready = with_query_budget(std::future::ready(7u8), &session, &shutdown, deadline);
    assert_eq!(ready.await.unwrap(), 7);
}

#[tokio::test]
async fn query_leaves_the_file_bytes_unchanged() {
    let fixture = fixture("read-only").await;
    write(&fixture, "keep.txt", b"keep\n").await;
    let before = tokio::fs::read(fixture.root.join("keep.txt"))
        .await
        .unwrap();
    query(&fixture, read_request("keep.txt")).await.unwrap();
    let after = tokio::fs::read(fixture.root.join("keep.txt"))
        .await
        .unwrap();
    assert_eq!(before, after);
}
