use minicore_runtime::LoopId;
use minicore_runtime::ToolCallId;

use super::*;
use crate::changes::{ChangeCommitState, ChangeCoverage, ChangeKind, FileChange, content_revision};
use crate::tool_data::ToolData;

pub(crate) fn comparison_key(before: &[u8], after: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((before.len() as u64).to_le_bytes());
    hasher.update(before);
    hasher.update((after.len() as u64).to_le_bytes());
    hasher.update(after);
    crate::store::digest_hex(hasher)
}

pub(super) fn block_on_diff_gate(
    before: &[u8],
    after: &[u8],
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), AgentError> {
    if let Some(gate) = DIFF_GATES.take(&comparison_key(before, after)) {
        tokio::runtime::Handle::current().block_on(async {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {},
                _ = gate.pass() => {},
            }
        });
    }
    check_budget(cancellation, deadline)
}

fn session() -> SessionId {
    SessionId::new().unwrap()
}

fn tool_ref(session_id: SessionId) -> ToolRef {
    ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-call").unwrap(),
    }
}

fn change(path: &str, before: &[u8], after: &[u8]) -> FileChange {
    FileChange {
        path: path.to_owned(),
        kind: ChangeKind::Modified,
        before: content_revision(before),
        after: content_revision(after),
        commit_state: ChangeCommitState::Applied,
        coverage: ChangeCoverage::Complete,
        before_captured: true,
        after_captured: true,
        before_bytes: Some(before.to_vec()),
        after_bytes: Some(after.to_vec()),
        before_corrupt: false,
        after_corrupt: false,
    }
}

fn plan(before: &[u8], after: &[u8], context: usize) -> DiffPlan {
    match plan_diff(
        before,
        after,
        context,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    )
    .unwrap()
    {
        DiffOutcome::Plan(plan) => plan,
        DiffOutcome::Binary => panic!("expected a text plan"),
    }
}

/// Reconstructs the exact bytes each plan line refers to, so a test can
/// prove the plan points at the real before/after line offsets.
fn reconstructed(plan: &DiffPlan, before: &[u8], after: &[u8]) -> Vec<(DiffLineKind, Vec<u8>)> {
    let mut out = Vec::new();
    for hunk in &plan.hunks {
        for line in &hunk.lines {
            let source = match line.source {
                LineSource::Before => before,
                LineSource::After => after,
            };
            let end = line.byte_start + line.byte_len;
            out.push((line.kind, source[line.byte_start..end].to_vec()));
        }
    }
    out
}

#[test]
fn line_splitting_matches_similar_mixed_endings_and_counts_before_allocating() {
    // `similar` tokenizes `\r\n`, a bare `\n`, and a bare `\r` as line ends;
    // the table must agree so indices and raw bytes stay aligned.
    let (crlf, crlf_count) = split_lines("a\r\nb\r\n", 100);
    assert_eq!(crlf, vec![(0, 3), (3, 3)]);
    assert_eq!(crlf_count, 2);
    let (mixed, mixed_count) = split_lines("a\rb\nc\r\nd", 100);
    assert_eq!(mixed, vec![(0, 2), (2, 2), (4, 3), (7, 1)]);
    assert_eq!(mixed_count, 4);
    let (blank, blank_count) = split_lines("a\n\nb\n", 100);
    assert_eq!(blank, vec![(0, 2), (2, 1), (3, 2)]);
    assert_eq!(blank_count, 3);
    let (no_final, no_final_count) = split_lines("a\nb", 100);
    assert_eq!(no_final, vec![(0, 2), (2, 1)]);
    assert_eq!(no_final_count, 2);
    let (empty, empty_count) = split_lines("", 100);
    assert!(empty.is_empty());
    assert_eq!(empty_count, 0);
    // The scan stops early and reports the ceiling breach instead of
    // materializing every line.
    let (bounded, bounded_count) = split_lines("x\ny\nz\n", 2);
    assert_eq!(bounded.len(), 3);
    assert_eq!(bounded_count, 3);
}

#[test]
fn bare_cr_and_mixed_endings_plan_the_real_bytes() {
    let before = b"a\rb\nc\r\nd";
    let after = b"a\rB\nc\r\nD";
    let plan = plan(before, after, 1);
    let lines = reconstructed(&plan, before, after);
    assert!(
        lines
            .iter()
            .any(|(kind, bytes)| { *kind == DiffLineKind::Removed && bytes == b"b\n" })
    );
    assert!(
        lines
            .iter()
            .any(|(kind, bytes)| { *kind == DiffLineKind::Added && bytes == b"B\n" })
    );
    assert!(
        lines
            .iter()
            .any(|(kind, bytes)| { *kind == DiffLineKind::Removed && bytes == b"d" })
    );
    assert!(
        lines
            .iter()
            .any(|(kind, bytes)| { *kind == DiffLineKind::Added && bytes == b"D" })
    );
}

#[test]
fn textual_plan_marks_added_and_removed_lines() {
    let plan = plan(b"one\ntwo\nthree\n", b"one\nTWO\nthree\n", 1);
    assert!(!plan.truncated);
    assert_eq!(plan.hunks.len(), 1);
    let kinds: Vec<_> = plan.hunks[0].lines.iter().map(|line| line.kind).collect();
    assert!(kinds.contains(&DiffLineKind::Removed));
    assert!(kinds.contains(&DiffLineKind::Added));
    assert_eq!(plan.fingerprint.len(), 64);
}

#[test]
fn equal_ends_are_trimmed_before_applying_the_myers_bound() {
    let prefix = "prefix\r\n".repeat(MAX_MYERS_LINES);
    let suffix = "suffix\r".repeat(MAX_MYERS_LINES);
    let before = format!("{prefix}old\nkeep\nold tail\n{suffix}");
    let after = format!("{prefix}new\nkeep\nnew tail\n{suffix}");
    let plan = plan(before.as_bytes(), after.as_bytes(), 0);
    assert!(!plan.truncated);
    assert_eq!(plan.hunks.len(), 2);
    assert_eq!(plan.hunks[0].old_start, MAX_MYERS_LINES);
    assert_eq!(plan.hunks[1].new_start, MAX_MYERS_LINES + 2);
    assert_eq!(
        reconstructed(&plan, before.as_bytes(), after.as_bytes()),
        vec![
            (DiffLineKind::Removed, b"old\n".to_vec()),
            (DiffLineKind::Added, b"new\n".to_vec()),
            (DiffLineKind::Removed, b"old tail\n".to_vec()),
            (DiffLineKind::Added, b"new tail\n".to_vec()),
        ]
    );
}

#[test]
fn middle_line_bound_selects_myers_or_one_lossless_replacement() {
    for extra in [0, 1] {
        let middle = "same\n".repeat(MAX_MYERS_LINES / 2 - 2);
        let before = format!("prefix\nold\n{middle}old tail\nsuffix\n");
        let after = format!(
            "prefix\nnew\n{middle}{}new tail\nsuffix\n",
            "extra\n".repeat(extra)
        );
        let plan = plan(before.as_bytes(), after.as_bytes(), 0);
        assert!(!plan.truncated);
        assert_eq!(plan.hunks.len(), if extra == 0 { 2 } else { 1 });
        if extra == 1 {
            let lines = reconstructed(&plan, before.as_bytes(), after.as_bytes());
            for (kind, source) in [
                (DiffLineKind::Removed, &before),
                (DiffLineKind::Added, &after),
            ] {
                let rebuilt: Vec<_> = lines
                    .iter()
                    .filter(|(k, _)| *k == kind)
                    .flat_map(|(_, bytes)| bytes.iter().copied())
                    .collect();
                assert_eq!(
                    rebuilt,
                    source
                        .strip_prefix("prefix\n")
                        .unwrap()
                        .strip_suffix("suffix\n")
                        .unwrap()
                        .as_bytes()
                );
            }
        }
    }
}

#[test]
fn replacement_pages_replan_with_stable_cursors_and_exact_bytes() {
    let before = "old é\r\n".repeat(MAX_MYERS_LINES);
    let after = "new 🙂\r".repeat(2);
    let base = page_base(Some(tool_ref(session())));
    let mut request = ChangesDiffRequest {
        session_id: base.session_id_for_test(),
        change_ref: base.change_ref.clone(),
        context_lines: Some(0),
        comparison: None,
        cursor: None,
        max_bytes: Some(MIN_DIFF_MAX_BYTES),
    };
    let mut removed = String::new();
    let mut added = String::new();
    let mut pages = 0;
    loop {
        let plan = plan(before.as_bytes(), after.as_bytes(), 0);
        let start = request.cursor.as_ref().map_or((0, 0, 0), |cursor| {
            assert_eq!(cursor.ops_fingerprint, plan.fingerprint);
            (
                cursor.hunk_index,
                cursor.line_index,
                cursor.line_byte_offset,
            )
        });
        let (hunks, cursor, complete) = build_page(
            &request,
            &base,
            &plan,
            before.as_bytes(),
            after.as_bytes(),
            0,
            start,
            MIN_DIFF_MAX_BYTES,
        )
        .unwrap();
        for line in hunks.iter().flat_map(|hunk| &hunk.lines) {
            match line.kind {
                DiffLineKind::Removed => removed.push_str(&line.text),
                DiffLineKind::Added => added.push_str(&line.text),
                DiffLineKind::Context => panic!("no context requested"),
            }
        }
        let result = DiffResult {
            hunks,
            next_cursor: cursor.clone(),
            complete,
            ..base.clone()
        };
        assert!(encoded_len(&result) <= MIN_DIFF_MAX_BYTES);
        pages += 1;
        assert!(pages < 2_000);
        if complete {
            break;
        }
        request.cursor = Some(cursor.expect("incomplete page has a cursor"));
    }
    assert!(pages > 1);
    assert_eq!(removed, before);
    assert_eq!(added, after);
}

#[test]
fn myers_bound_handles_disjoint_full_size_snapshots() {
    let line_count = MAX_MYERS_LINES / 2;
    let shared = "x".repeat(crate::changes::MAX_CHANGE_SNAPSHOT_BYTES / line_count - 2);
    let before = format!("{shared}a\n").repeat(line_count);
    let after = format!("{shared}b\n").repeat(line_count);
    let plan = plan(before.as_bytes(), after.as_bytes(), 0);
    assert!(!plan.truncated);
    assert_eq!(plan.hunks.len(), 1);
    assert_eq!(plan.hunks[0].old_count, line_count);
    assert_eq!(plan.hunks[0].new_count, line_count);
    assert_eq!(plan.hunks[0].lines.len(), MAX_MYERS_LINES);
}

#[test]
fn alignment_path_is_part_of_the_plan_fingerprint() {
    let plan = plan(b"old\n", b"new\n", 0);
    assert_ne!(
        plan_fingerprint(0, false, &plan.hunks),
        plan_fingerprint(0, true, &plan.hunks)
    );
}

#[test]
fn a_side_over_the_line_ceiling_is_truncated_not_compared() {
    // 100_001 short lines on one side trips the ceiling before the diff is
    // started; the plan reports truncation with no hunks.
    let mut before = Vec::new();
    for index in 0..100_001 {
        before.extend_from_slice(format!("{index}\n").as_bytes());
    }
    let plan = plan(&before, b"after\n", 0);
    assert!(plan.truncated);
    assert!(plan.hunks.is_empty());
}

#[test]
fn binary_and_invalid_utf8_are_reported_without_replacement_characters() {
    let before = [0x00u8, 0x01, 0x02];
    let after = [0x03u8, 0xff];
    assert!(matches!(
        plan_diff(
            &before,
            &after,
            3,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap(),
        DiffOutcome::Binary
    ));
    let invalid = [0xffu8, 0xfe];
    assert!(matches!(
        plan_diff(
            b"text",
            &invalid,
            3,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap(),
        DiffOutcome::Binary
    ));
}

#[test]
fn an_expired_deadline_is_a_query_limit_not_an_empty_diff() {
    assert!(matches!(
        plan_diff(
            b"a\n",
            b"b\n",
            3,
            &CancellationToken::new(),
            Instant::now() - Duration::from_millis(1),
        ),
        Err(AgentError::QueryLimit)
    ));
}

#[test]
fn a_cancelled_worker_stops_before_any_comparison() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        plan_diff(
            b"a\n",
            b"b\n",
            3,
            &cancellation,
            Instant::now() + Duration::from_secs(5),
        ),
        Err(AgentError::QueryLimit)
    ));
}

fn page_base(tool_ref: Option<ToolRef>) -> DiffResult {
    DiffResult {
        versions_refreshed: false,
        change_ref: format!("tool:{}", "a".repeat(64)),
        path: "value.txt".to_owned(),
        kind: ChangeKind::Modified,
        origin: ChangeOrigin::Tool,
        tool_ref,
        comparison: DiffComparison::ToolBeforeAfter,
        base_version: content_revision(b"x"),
        target_version: content_revision(b"y"),
        commit_state: ChangeCommitState::Applied,
        coverage: ChangeCoverage::Complete,
        binary: false,
        stale: false,
        availability: DiffAvailability::Available,
        hunks: Vec::new(),
        complete: false,
        truncated: false,
        next_cursor: None,
    }
}

/// Pages one plan with `max_bytes`, returning the total line bytes and the
/// final completeness flag. Every page is asserted to stay within the
/// budget, every fragment contiguous inside its logical line, and every
/// completed line byte-identical to its real before/after source line.
fn page_all(
    base: &DiffResult,
    plan: &DiffPlan,
    before: &[u8],
    after: &[u8],
    context: usize,
    max_bytes: usize,
) -> (Vec<u8>, bool, usize) {
    let (before_lines, _) = split_lines(
        std::str::from_utf8(before).expect("test source is UTF-8"),
        MAX_DIFF_LINES,
    );
    let (after_lines, _) = split_lines(
        std::str::from_utf8(after).expect("test source is UTF-8"),
        MAX_DIFF_LINES,
    );
    let request = ChangesDiffRequest {
        session_id: base.session_id_for_test(),
        change_ref: base.change_ref.clone(),
        context_lines: Some(context),
        comparison: None,
        cursor: None,
        max_bytes: Some(max_bytes),
    };
    let mut rebuilt = Vec::new();
    let mut line_buf = Vec::new();
    let mut line_offset = 0usize;
    let mut start = (0usize, 0usize, 0usize);
    let mut cursor = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 100_000, "paging did not terminate");
        let request = ChangesDiffRequest {
            cursor: cursor.clone(),
            ..request.clone()
        };
        let (hunks, next, done) = build_page(
            &request, base, plan, before, after, context, start, max_bytes,
        )
        .unwrap();
        // The measured page must respect the budget on every step.
        let mut check = base.clone();
        check.hunks = hunks.clone();
        check.next_cursor = next.clone();
        assert!(crate::budget::encoded_len(&check) <= max_bytes);
        for hunk in &hunks {
            for line in &hunk.lines {
                assert_eq!(line.line_byte_offset, line_offset);
                line_buf.extend_from_slice(line.text.as_bytes());
                line_offset += line.text.len();
                rebuilt.extend_from_slice(line.text.as_bytes());
                if line.line_complete {
                    let expected = match (line.old_index, line.new_index) {
                        (Some(index), None) => slice(before, &before_lines, index),
                        (_, Some(index)) => slice(after, &after_lines, index),
                        (None, None) => panic!("a line has no index"),
                    };
                    assert_eq!(line_buf, expected);
                    assert_eq!(line_buf.len(), line.line_byte_len);
                    line_buf.clear();
                    line_offset = 0;
                }
            }
        }
        if done {
            break;
        }
        let next = next.expect("an incomplete page has a cursor");
        start = (next.hunk_index, next.line_index, next.line_byte_offset);
        cursor = Some(next);
    }
    (rebuilt, true, pages)
}

fn slice(source: &[u8], lines: &[(usize, usize)], index: usize) -> Vec<u8> {
    let (start, len) = lines[index];
    source[start..start + len].to_vec()
}

impl DiffResult {
    fn session_id_for_test(&self) -> SessionId {
        match &self.tool_ref {
            Some(tool_ref) => tool_ref.session_id,
            None => session(),
        }
    }
}

#[test]
fn build_page_fragments_a_long_unicode_line_losslessly_at_the_minimum_budget() {
    let long = format!("{}{}\n", "a".repeat(3_000), "é🙂世界".repeat(400));
    let base = page_base(None);
    let plan = plan(b"", long.as_bytes(), 1);
    assert_eq!(plan.hunks.len(), 1);
    let before = Vec::new();
    let after = long.as_bytes();
    let (rebuilt, complete, pages) = page_all(&base, &plan, &before, after, 1, MIN_DIFF_MAX_BYTES);
    assert!(complete);
    assert!(pages > 1, "a long line must span pages");
    assert_eq!(rebuilt, after);
}

#[test]
fn build_page_preserves_crlf_and_a_missing_final_newline_across_pages() {
    let before = b"one\r\ntwo\r\nthree";
    let after = b"one\r\nTWO\r\nthree";
    let base = page_base(None);
    let plan = plan(before, after, 1);
    let (rebuilt, complete, _) = page_all(&base, &plan, before, after, 1, MIN_DIFF_MAX_BYTES);
    assert!(complete);
    // Reconstructed bytes are exactly the real source line text, including
    // the CRLF terminators and the missing final newline on `three`.
    assert!(rebuilt.windows(2).any(|w| w == b"\r\n"));
    assert!(rebuilt.ends_with(b"three"));
}

#[test]
fn build_page_preserves_a_bare_cr_line_ending_across_pages() {
    let before = b"aaa\rbbb\r";
    let after = b"aaa\rBBB\r";
    let base = page_base(None);
    let plan = plan(before, after, 0);
    let (rebuilt, complete, _) = page_all(&base, &plan, before, after, 0, 4096);
    assert!(complete);
    // Context 0 keeps only the changed lines, and each bare-CR line is
    // reconstructed with its terminator attached.
    assert_eq!(rebuilt, b"bbb\rBBB\r");
}

#[test]
fn build_page_rejects_a_forged_start_tuple() {
    let before = b"a\nb\nc\n";
    let after = b"a\nB\nc\n";
    let base = page_base(None);
    let plan = plan(before, after, 0);
    let request = ChangesDiffRequest {
        session_id: base.session_id_for_test(),
        change_ref: base.change_ref.clone(),
        context_lines: Some(0),
        comparison: None,
        cursor: None,
        max_bytes: Some(MIN_DIFF_MAX_BYTES),
    };
    let lines = plan.hunks[0].lines.len();
    // Past the last hunk with a non-canonical tail is not a valid end.
    assert!(matches!(
        build_page(
            &request,
            &base,
            &plan,
            before,
            after,
            0,
            (plan.hunks.len(), 1, 0),
            MIN_DIFF_MAX_BYTES,
        ),
        Err(AgentError::InvalidArguments)
    ));
    // A line index past the hunk's lines is rejected, not skipped.
    assert!(matches!(
        build_page(
            &request,
            &base,
            &plan,
            before,
            after,
            0,
            (0, lines + 5, 0),
            MIN_DIFF_MAX_BYTES,
        ),
        Err(AgentError::InvalidArguments)
    ));
    // A byte offset past the line is rejected.
    let byte_len = plan.hunks[0].lines[0].byte_len;
    assert!(matches!(
        build_page(
            &request,
            &base,
            &plan,
            before,
            after,
            0,
            (0, 0, byte_len + 1),
            MIN_DIFF_MAX_BYTES,
        ),
        Err(AgentError::InvalidArguments)
    ));
    // The canonical end tuple is accepted and yields an empty complete page.
    let (hunks, cursor, complete) = build_page(
        &request,
        &base,
        &plan,
        before,
        after,
        0,
        (plan.hunks.len(), 0, 0),
        MIN_DIFF_MAX_BYTES,
    )
    .unwrap();
    assert!(hunks.is_empty());
    assert!(cursor.is_none());
    assert!(complete);
}

#[test]
fn build_page_rejects_a_non_utf8_boundary_offset() {
    // "é" is two bytes, so offset 1 lands inside the first character.
    let before = b"\xc3\xa9\n";
    let after = b"x\n";
    let base = page_base(None);
    let plan = plan(before, after, 0);
    let request = ChangesDiffRequest {
        session_id: base.session_id_for_test(),
        change_ref: base.change_ref.clone(),
        context_lines: Some(0),
        comparison: None,
        cursor: None,
        max_bytes: Some(MIN_DIFF_MAX_BYTES),
    };
    let removed = plan.hunks[0]
        .lines
        .iter()
        .position(|line| line.source == LineSource::Before)
        .expect("a removed line exists");
    assert!(matches!(
        build_page(
            &request,
            &base,
            &plan,
            before,
            after,
            0,
            (0, removed, 1),
            MIN_DIFF_MAX_BYTES,
        ),
        Err(AgentError::InvalidArguments)
    ));
}

#[test]
fn cursor_bound_page_refuses_a_tool_ref_that_cannot_fit() {
    let tool_call_id = ToolCallId::new("long-call").unwrap();
    let session_id = session();
    let long_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id,
    };
    let mut base = page_base(Some(long_ref.clone()));
    base.path = "x".repeat(4_000);
    let before = b"a\n";
    let after = b"a\nb\n";
    let plan = plan(before, after, 0);
    let request = ChangesDiffRequest {
        session_id,
        change_ref: base.change_ref.clone(),
        comparison: None,
        context_lines: Some(0),
        cursor: None,
        max_bytes: Some(MIN_DIFF_MAX_BYTES),
    };
    // A path this long cannot fit the minimum budget, so the final
    // encoded-size check refuses the page instead of overrunning it.
    assert!(matches!(
        build_page(
            &request,
            &base,
            &plan,
            before,
            after,
            0,
            (0, 0, 0),
            MIN_DIFF_MAX_BYTES,
        ),
        Err(AgentError::InvalidArguments)
    ));
    // A short ToolRef fits and pages normally.
    let short_base = page_base(Some(ToolRef {
        session_id,
        loop_id: long_ref.loop_id,
        request_index: 0,
        tool_call_id: ToolCallId::new("short").unwrap(),
    }));
    let (rebuilt, complete, _) = page_all(&short_base, &plan, before, after, 0, MIN_DIFF_MAX_BYTES);
    assert!(complete);
    assert!(rebuilt.ends_with(b"\n"));
}

#[test]
fn tool_data_candidates_and_lookup_are_identity_scoped() {
    let session_id = session();
    let tool_ref = tool_ref(session_id);
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(&tool_ref, change("value.txt", b"user\n", b"agent\n"));
    assert!(data.file_change(&tool_ref).is_some());
    let candidates = data.file_change_records(session_id, None);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].tool_ref.as_ref(), Some(&tool_ref));
    assert_eq!(
        candidates[0].change_ref,
        crate::changes::tool_change_ref(&tool_ref, &change("value.txt", b"user\n", b"agent\n"))
    );
    let other = session();
    assert!(data.file_change_records(other, None).is_empty());
}
