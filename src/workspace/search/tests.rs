use super::*;
use crate::workspace::scan::{
    ScanLimits, expire_scan_after_result, set_scan_deadline, set_scan_limits,
};

/// One collected match: path, line number, and absolute line byte ranges.
type MatchedLine = (String, u32, Vec<(u64, u64)>);

struct Fixture {
    session: SessionId,
    workspace: Arc<Workspace>,
}

async fn fixture(label: &str) -> Fixture {
    let base = std::env::temp_dir().join(format!(
        "minicore-workspace-search-{label}-{}",
        SessionId::new().unwrap()
    ));
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root).await.unwrap());
    Fixture {
        session: SessionId::new().unwrap(),
        workspace,
    }
}

fn write(fixture: &Fixture, path: &str, bytes: impl AsRef<[u8]>) {
    let full = fixture.workspace.root().join(path);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(full, bytes).unwrap();
}

fn request(fixture: &Fixture, query: &str) -> WorkspaceSearchRequest {
    WorkspaceSearchRequest {
        session_id: fixture.session,
        query: query.to_owned(),
        paths: None,
        case_sensitive: None,
        cursor: None,
        max_matches: None,
        max_bytes: None,
    }
}

async fn run(fixture: &Fixture, request: WorkspaceSearchRequest) -> WorkspaceSearchResult {
    search(
        Arc::clone(&fixture.workspace),
        request,
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .unwrap()
}

/// Absolute offsets of one record's ranges inside its original line.
fn absolute_ranges(record: &WorkspaceSearchMatch) -> Vec<(u64, u64)> {
    record
        .match_byte_ranges
        .iter()
        .map(|range| {
            (
                u64::from(record.line_text_byte_offset) + u64::from(range.start),
                u64::from(record.line_text_byte_offset) + u64::from(range.end),
            )
        })
        .collect()
}

#[tokio::test]
async fn literal_queries_ignore_regex_metacharacters() {
    let fixture = fixture("literal").await;
    write(&fixture, "a.txt", b"a.b\naxb\na+b\n");

    let result = run(&fixture, request(&fixture, "a.b")).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].line_number, 1);
    assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
    assert!(!result.matches[0].line_truncated);
    assert_eq!(result.consistency, WorkspaceScanConsistency::Live);
    assert!(result.scan_complete);
    assert_eq!(result.stopped_by, WorkspaceScanStop::End);

    let result = run(&fixture, request(&fixture, "a+b")).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].line_number, 3);
    assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
}

#[tokio::test]
async fn case_insensitive_matches_keep_original_byte_ranges() {
    let fixture = fixture("case").await;
    write(&fixture, "cafe.txt", "CAFÉ\ncafé\nplain\n".as_bytes());

    let result = run(&fixture, request(&fixture, "café")).await;
    assert_eq!(result.matches.len(), 2);
    for record in &result.matches {
        assert_eq!(record.match_byte_ranges.len(), 1);
        let range = record.match_byte_ranges[0];
        let slice = &record.line_text[range.start as usize..range.end as usize];
        assert_eq!(slice.to_lowercase(), "café");
    }
    assert_eq!(result.matches[0].line_number, 1);
    assert_eq!(result.matches[1].line_number, 2);

    let mut sensitive = request(&fixture, "café");
    sensitive.case_sensitive = Some(true);
    let result = run(&fixture, sensitive).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].line_number, 2);
}

#[tokio::test]
async fn crlf_terminators_are_not_part_of_the_reported_line() {
    let fixture = fixture("crlf").await;
    write(&fixture, "crlf.txt", b"one\r\ntwo\r\nthree");

    let result = run(&fixture, request(&fixture, "two")).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].line_number, 2);
    assert_eq!(result.matches[0].line_text, "two");
    assert_eq!(result.matches[0].line_text_byte_offset, 0);
    assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
    assert!(!result.matches[0].line_truncated);

    let result = run(&fixture, request(&fixture, "three")).await;
    assert_eq!(result.matches[0].line_number, 3);
    assert_eq!(result.matches[0].line_text, "three");

    let newline = request(&fixture, "two\n");
    assert!(matches!(
        newline.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let carriage = request(&fixture, "two\r");
    assert!(matches!(
        carriage.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let empty = request(&fixture, "");
    assert!(matches!(
        empty.validate(),
        Err(AgentError::InvalidArguments)
    ));
}

#[tokio::test]
async fn binary_invalid_utf8_and_oversized_files_are_skipped() {
    let fixture = fixture("skip").await;
    write(&fixture, "nul.txt", b"hit\0more");
    write(&fixture, "invalid.txt", [b'h', b'i', b't', b'\n', 0xff]);
    write(&fixture, "huge.txt", b"hit".repeat(MAX_READ_BYTES / 3 + 8));
    write(&fixture, "ok.txt", b"hit\n");

    let result = run(&fixture, request(&fixture, "hit")).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "ok.txt");
    assert_eq!(result.skipped_files, 3);
    assert!(result.scan_complete);
}

#[tokio::test]
async fn a_match_after_a_long_prefix_is_returned_as_a_bounded_slice() {
    let fixture = fixture("long-line").await;
    let mut line = "x".repeat(300 * 1024);
    line.push_str("needle\n");
    write(&fixture, "long.txt", line.as_bytes());

    let result = run(&fixture, request(&fixture, "needle")).await;
    assert_eq!(result.matches.len(), 1);
    let record = &result.matches[0];
    assert!(record.line_truncated);
    assert!(record.line_text_byte_offset > 0);
    assert!(record.line_text.ends_with("needle"));
    assert_eq!(absolute_ranges(record), vec![(300 * 1024, 300 * 1024 + 6)]);
    assert_eq!(result.stopped_by, WorkspaceScanStop::End);
    assert!(result.scan_complete);
}

#[tokio::test]
async fn a_line_prefix_that_fits_keeps_its_matches_within_the_budget() {
    let fixture = fixture("long-prefix").await;
    let mut line = String::from("needle ");
    line.push_str(&"x".repeat(300 * 1024));
    line.push('\n');
    write(&fixture, "long.txt", line.as_bytes());

    let mut request = request(&fixture, "needle");
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let result = run(&fixture, request).await;
    assert_eq!(result.matches.len(), 1);
    let record = &result.matches[0];
    assert!(record.line_truncated);
    assert!(!record.line_text.is_empty());
    assert!(record.line_text.len() < line.len());
    assert_eq!(absolute_ranges(record), vec![(0, 6)]);
    assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
}

#[tokio::test]
async fn a_record_that_cannot_fit_even_an_empty_page_is_reported_incomplete() {
    let fixture = fixture("unrepresentable").await;
    // The path alone is longer than an empty page can hold, so even a
    // bounded slice of the matching line cannot be represented.
    let deep = (0..9)
        .map(|index| format!("level-{index:02}-{}", "x".repeat(70)))
        .collect::<Vec<_>>()
        .join("/");
    write(&fixture, &format!("{deep}/quotes.txt"), b"needle\n");

    let mut request = request(&fixture, "needle");
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let result = run(&fixture, request).await;
    assert!(result.matches.is_empty());
    assert!(result.skipped_files >= 1);
    assert!(result.truncated);
    assert!(!result.scan_complete);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn escaped_matches_stay_inside_the_result_budget() {
    let fixture = fixture("escaping").await;
    let mut line = "\"".repeat(4096);
    line.push_str("needle\n");
    write(&fixture, "quotes.txt", line.as_bytes());

    let mut request = request(&fixture, "needle");
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let result = run(&fixture, request).await;
    assert_eq!(result.matches.len(), 1);
    let record = &result.matches[0];
    assert_eq!(absolute_ranges(record), vec![(4096, 4102)]);
    assert_eq!(record.line_text_byte_offset, 4096 - 64);
    assert_eq!(record.line_text.len(), 64 + 6);
    assert!(record.line_truncated);
    assert!(serde_json::to_vec(&result).unwrap().len() <= MIN_RESULT_BYTES);
}

#[tokio::test]
async fn max_matches_splits_a_line_and_the_cursor_resumes_inside_it() {
    let fixture = fixture("split").await;
    write(&fixture, "many.txt", b"hit hit hit hit hit\n");

    let mut request = request(&fixture, "hit");
    request.max_matches = Some(2);
    let mut all = Vec::new();
    let mut pages = 0;
    loop {
        let result = run(&fixture, request.clone()).await;
        pages += 1;
        assert!(pages < 8, "split pagination did not terminate");
        assert!(
            result
                .matches
                .iter()
                .all(|record| !record.line_text.is_empty())
        );
        for record in &result.matches {
            all.extend(absolute_ranges(record));
        }
        match result.next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => break,
        }
    }
    assert_eq!(all, vec![(0, 3), (4, 7), (8, 11), (12, 15), (16, 19)]);
    // Two matches per page: the first two pages are cut by `max_matches`,
    // and the third finds the last match and reports the end of the scope.
    assert_eq!(pages, 3);
}

#[tokio::test]
async fn a_byte_budget_pages_matches_without_loss_or_duplication() {
    let fixture = fixture("byte-paging").await;
    let mut first_line = String::new();
    for _ in 0..12 {
        first_line.push_str("needle ");
    }
    first_line.push('\n');
    write(&fixture, "a.txt", first_line.as_bytes());
    write(&fixture, "b.txt", b"needle here\n");
    write(&fixture, "c.txt", b"needle\n");

    let mut request = request(&fixture, "needle");
    request.max_bytes = Some(MIN_RESULT_BYTES);
    request.max_matches = Some(20);
    let mut collected: Vec<MatchedLine> = Vec::new();
    let mut pages = 0;
    loop {
        let result = run(&fixture, request.clone()).await;
        pages += 1;
        assert!(pages < 32, "byte paging did not terminate");
        let encoded = serde_json::to_vec(&result).unwrap().len();
        assert!(encoded <= MIN_RESULT_BYTES, "page {encoded} exceeds budget");
        for record in &result.matches {
            assert!(
                !record.match_byte_ranges.is_empty(),
                "a record always carries at least one range"
            );
            for (start, end) in absolute_ranges(record) {
                let from = start - u64::from(record.line_text_byte_offset);
                let to = end - u64::from(record.line_text_byte_offset);
                let slice = &record.line_text[from as usize..to as usize];
                assert_eq!(slice.to_lowercase(), "needle");
            }
            if let Some(existing) = collected
                .iter_mut()
                .find(|(path, line, _)| *path == record.path && *line == record.line_number)
            {
                existing.2.extend(absolute_ranges(record));
            } else {
                collected.push((
                    record.path.clone(),
                    record.line_number,
                    absolute_ranges(record),
                ));
            }
        }
        match result.next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => break,
        }
    }
    assert!(pages > 1, "the small budget must page");
    collected.sort();
    let mut expected = vec![
        (
            "a.txt".to_owned(),
            1,
            (0..12).map(|index| (index * 7, index * 7 + 6)).collect(),
        ),
        ("b.txt".to_owned(), 1, vec![(0, 6)]),
        ("c.txt".to_owned(), 1, vec![(0, 6)]),
    ];
    expected.sort();
    assert_eq!(collected, expected);
}

#[tokio::test]
async fn a_deadline_after_a_match_keeps_it_without_a_cursor() {
    let fixture = fixture("deadline-partial").await;
    write(&fixture, "one.txt", b"hit\n");
    write(&fixture, "two.txt", b"hit again\n");
    // The timeout lands as soon as the first match has been committed: the
    // second file is never searched.
    expire_scan_after_result(fixture.workspace.root().to_path_buf());

    let result = run(&fixture, request(&fixture, "hit")).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(absolute_ranges(&result.matches[0]), vec![(0, 3)]);
    assert_eq!(result.stopped_by, WorkspaceScanStop::Deadline);
    assert!(result.truncated);
    assert!(!result.scan_complete);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn an_expired_budget_returns_an_empty_partial_instead_of_an_end() {
    let fixture = fixture("deadline-empty").await;
    write(&fixture, "one.txt", b"hit\n");
    set_scan_deadline(
        fixture.workspace.root().to_path_buf(),
        std::time::Duration::ZERO,
    );

    let result = run(&fixture, request(&fixture, "hit")).await;
    assert!(result.matches.is_empty());
    assert_eq!(result.stopped_by, WorkspaceScanStop::Deadline);
    assert!(result.truncated);
    assert!(!result.scan_complete);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn paging_across_roots_uses_each_root_ordinal() {
    let fixture = fixture("root-paging").await;
    write(&fixture, "one/.gitignore", b"skip.txt\n");
    write(&fixture, "one/nested/deep.txt", b"hit deep\n");
    write(&fixture, "one/skip.txt", b"hit skip\n");
    write(&fixture, "one/top.txt", b"hit top\n");
    write(&fixture, "two/deep/deep.txt", b"hit two\n");
    write(&fixture, "two/other.txt", b"hit other\n");

    let mut request = request(&fixture, "hit");
    request.paths = Some(vec!["one".to_owned(), "two".to_owned()]);
    request.max_matches = Some(1);
    request.max_bytes = Some(MIN_RESULT_BYTES);
    let mut collected: Vec<MatchedLine> = Vec::new();
    for _ in 0..32 {
        let result = run(&fixture, request.clone()).await;
        let encoded = serde_json::to_vec(&result).unwrap().len();
        assert!(encoded <= MIN_RESULT_BYTES, "page {encoded} exceeds budget");
        for record in &result.matches {
            assert_eq!(absolute_ranges(record).len(), 1);
            collected.push((
                record.path.clone(),
                record.line_number,
                absolute_ranges(record),
            ));
        }
        match result.next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => {
                assert!(result.scan_complete);
                break;
            }
        }
    }
    collected.sort();
    let mut expected: Vec<MatchedLine> = vec![
        ("one/nested/deep.txt".to_owned(), 1, vec![(0, 3)]),
        ("one/top.txt".to_owned(), 1, vec![(0, 3)]),
        ("two/deep/deep.txt".to_owned(), 1, vec![(0, 3)]),
        ("two/other.txt".to_owned(), 1, vec![(0, 3)]),
    ];
    expected.sort();
    assert_eq!(collected, expected);
}

#[tokio::test]
async fn paths_restrict_the_search_and_must_resolve() {
    let fixture = fixture("paths").await;
    write(&fixture, "one/hit.txt", b"hit\n");
    write(&fixture, "two/hit.txt", b"hit\n");

    let mut restricted = request(&fixture, "hit");
    restricted.paths = Some(vec!["two".to_owned()]);
    let result = run(&fixture, restricted).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "two/hit.txt");

    let mut single = request(&fixture, "hit");
    single.paths = Some(vec!["one/hit.txt".to_owned()]);
    let result = run(&fixture, single).await;
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "one/hit.txt");

    let mut missing = request(&fixture, "hit");
    missing.paths = Some(vec!["missing".to_owned()]);
    let error = search(
        Arc::clone(&fixture.workspace),
        missing,
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::Workspace));

    let mut escaping = request(&fixture, "hit");
    escaping.paths = Some(vec!["../escape".to_owned()]);
    assert!(matches!(
        escaping.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut many = request(&fixture, "hit");
    many.paths = Some(
        (0..(MAX_SEARCH_PATHS + 1))
            .map(|index| format!("p{index}"))
            .collect(),
    );
    assert!(matches!(many.validate(), Err(AgentError::InvalidArguments)));
    let mut budget = request(&fixture, "hit");
    budget.max_bytes = Some(MIN_RESULT_BYTES - 1);
    assert!(matches!(
        budget.validate(),
        Err(AgentError::InvalidArguments)
    ));
    let mut matches = request(&fixture, "hit");
    matches.max_matches = Some(MAX_MAX_MATCHES + 1);
    assert!(matches!(
        matches.validate(),
        Err(AgentError::InvalidArguments)
    ));
}

#[tokio::test]
async fn duplicate_roots_are_merged_and_overlapping_roots_are_rejected() {
    let fixture = fixture("roots").await;
    write(&fixture, "sub/hit.txt", b"hit\n");

    let mut duplicate = request(&fixture, "hit");
    duplicate.paths = Some(vec!["sub".to_owned(), "sub/".to_owned()]);
    assert!(duplicate.validate().is_ok());
    let result = run(&fixture, duplicate).await;
    assert_eq!(result.matches.len(), 1);

    for paths in [
        vec!["sub".to_owned(), "sub/hit.txt".to_owned()],
        vec![String::new(), "sub".to_owned()],
    ] {
        let mut overlapping = request(&fixture, "hit");
        overlapping.paths = Some(paths);
        assert!(matches!(
            overlapping.validate(),
            Err(AgentError::InvalidArguments)
        ));
    }
}

#[tokio::test]
async fn explicit_roots_cannot_bypass_the_rules() {
    let fixture = fixture("explicit-rules").await;
    write(&fixture, ".gitignore", b"secret.txt\n");
    write(&fixture, "secret.txt", b"hit secret\n");
    write(&fixture, "kept.txt", b"hit kept\n");

    let ignored = run(&fixture, request(&fixture, "hit")).await;
    assert_eq!(ignored.matches.len(), 1);
    assert_eq!(ignored.matches[0].path, "kept.txt");

    let mut explicit = request(&fixture, "hit");
    explicit.paths = Some(vec!["secret.txt".to_owned()]);
    let result = run(&fixture, explicit).await;
    assert!(result.matches.is_empty());
    assert_eq!(result.skipped_files, 1);
    assert!(result.scan_complete);
}

#[tokio::test]
async fn scan_ceiling_reports_zero_matches_and_continues() {
    let fixture = fixture("ceiling").await;
    write(&fixture, "a.txt", b"hit one\n");
    write(&fixture, "b.txt", b"hit two\n");
    set_scan_limits(
        fixture.workspace.root().to_path_buf(),
        ScanLimits {
            entries: 100,
            bytes: 6,
            depth: 64,
            rule_files: 8,
            rule_bytes: 1 << 20,
        },
    );
    let mut request = request(&fixture, "hit");
    let first = run(&fixture, request.clone()).await;
    assert!(first.matches.is_empty());
    assert!(!first.scan_complete);
    assert!(first.truncated);
    assert_eq!(first.stopped_by, WorkspaceScanStop::Bytes);
    let cursor = first.next_cursor.clone().expect("the scan continues");

    set_scan_limits(
        fixture.workspace.root().to_path_buf(),
        ScanLimits {
            entries: 100,
            bytes: 1 << 20,
            depth: 64,
            rule_files: 8,
            rule_bytes: 1 << 20,
        },
    );
    request.cursor = Some(cursor);
    let second = run(&fixture, request).await;
    assert_eq!(second.matches.len(), 2);
    assert!(second.scan_complete);
}

#[tokio::test]
async fn a_cursor_from_another_query_is_rejected() {
    let fixture = fixture("cursor").await;
    write(&fixture, "a.txt", b"hit\n");
    write(&fixture, "b.txt", b"hit\n");

    let mut request = request(&fixture, "hit");
    request.max_matches = Some(1);
    let first = run(&fixture, request.clone()).await;
    let cursor = first.next_cursor.clone().expect("a cut page continues");

    for mismatched in [
        {
            let mut mismatched = request.clone();
            mismatched.query = "other".to_owned();
            mismatched
        },
        {
            let mut mismatched = request.clone();
            mismatched.case_sensitive = Some(true);
            mismatched
        },
        {
            let mut mismatched = request.clone();
            mismatched.paths = Some(vec!["a.txt".to_owned()]);
            mismatched
        },
        {
            let mut mismatched = request.clone();
            mismatched.session_id = SessionId::new().unwrap();
            mismatched
        },
    ] {
        let mut mismatched = mismatched;
        mismatched.cursor = Some(cursor.clone());
        assert!(matches!(
            mismatched.validate(),
            Err(AgentError::InvalidArguments)
        ));
        let error = search(
            Arc::clone(&fixture.workspace),
            mismatched,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::InvalidArguments));
    }

    let mut wrong_index = request.clone();
    wrong_index.cursor = Some(WorkspaceSearchCursor {
        path_index: 3,
        ..cursor
    });
    assert!(matches!(
        wrong_index.validate(),
        Err(AgentError::InvalidArguments)
    ));
}

#[tokio::test]
async fn a_cancelled_session_stops_the_scan() {
    let fixture = fixture("cancel").await;
    write(&fixture, "a.txt", b"hit\n");
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = search(
        Arc::clone(&fixture.workspace),
        request(&fixture, "hit"),
        cancelled,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::QueryLimit));
}

#[cfg(unix)]
#[tokio::test]
async fn special_files_and_escaping_symlinks_are_skipped_without_blocking() {
    use std::os::unix::fs::symlink;
    use std::time::Duration;

    let fixture = fixture("special").await;
    write(&fixture, "ok.txt", b"hit\n");
    let outside = std::env::temp_dir().join(format!(
        "minicore-workspace-search-outside-{}",
        SessionId::new().unwrap()
    ));
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), b"hit secret\n").unwrap();
    let root = fixture.workspace.root().to_path_buf();
    symlink(outside.join("secret.txt"), root.join("escape.txt")).unwrap();
    symlink(root.join("ok.txt"), root.join("link.txt")).unwrap();
    let fifo = root.join("pipe");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(status.success());

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run(&fixture, request(&fixture, "hit")),
    )
    .await
    .expect("a special file must not block the search");
    let mut paths: Vec<String> = result
        .matches
        .iter()
        .map(|record| record.path.clone())
        .collect();
    paths.sort();
    assert_eq!(paths, vec!["link.txt", "ok.txt"]);
    assert_eq!(result.skipped_files, 2);
    let _ = std::fs::remove_dir_all(&outside);
}
