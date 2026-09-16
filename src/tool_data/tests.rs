#[test]
fn historical_memory_only_record_needs_stored_details() {
    let mut record = super::ToolRecord::new("bash".to_owned());
    record.state = super::ToolExecutionState::Succeeded;
    record.outcome = Some(super::ToolResultOutcome::Success);
    record.result_seen = true;
    assert!(record.needs_stored());
}

#[test]
fn complete_empty_streams_do_not_need_stored_details() {
    let mut record = super::ToolRecord::new("bash".to_owned());
    record.state = super::ToolExecutionState::Succeeded;
    record.outcome = Some(super::ToolResultOutcome::Success);
    record.input_seen = true;
    record.result_seen = true;
    record.recording = super::ToolRecordingState::Saved;
    record.stdout.complete = true;
    record.stderr.complete = true;
    record.command = Some(super::CommandResult {
        status: super::CommandStatus::Exited,
        exit_code: Some(0),
        signal: None,
        termination_confirmed: true,
        stdout_base_offset: 0,
        stdout_observed_end: 0,
        stderr_base_offset: 0,
        stderr_observed_end: 0,
        output_complete: true,
        output_truncated: false,
    });
    assert!(!record.needs_stored());
}

use serde_json::json;

use crate::changes::FileChange;

use super::*;

fn session(value: u8) -> SessionId {
    let mut bytes = [0_u8; 16];
    bytes[15] = value;
    format!(
        "ses_{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .unwrap()
}

fn loop_id(value: u8) -> LoopId {
    let mut bytes = [0_u8; 16];
    bytes[15] = value;
    format!(
        "lup_{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .unwrap()
}

fn make_tool_ref(session_id: SessionId, loop_key: LoopId, request: u32, call: &str) -> ToolRef {
    ToolRef {
        session_id,
        loop_id: loop_key,
        request_index: request,
        tool_call_id: ToolCallId::new(call).unwrap(),
    }
}

fn invocation(call: &str, name: &str, arguments: Value) -> ToolInvocation {
    ToolInvocation {
        tool_call_id: ToolCallId::new(call).unwrap(),
        tool_name: name.parse().unwrap(),
        arguments,
    }
}

fn read_request(tool_ref: ToolRef, max_bytes: Option<usize>) -> ToolReadRequest {
    ToolReadRequest {
        tool_ref,
        max_bytes,
    }
}

fn output_request(
    tool_ref: ToolRef,
    stream: ToolDataStream,
    offset: u64,
    max_bytes: Option<usize>,
) -> ToolOutputRequest {
    ToolOutputRequest {
        tool_ref,
        stream,
        offset,
        max_bytes,
    }
}

fn invocation_of(data: &ToolData, tool_ref: &ToolRef, call: &str, name: &str, arguments: Value) {
    let _ = data.note_invocation(tool_ref, &invocation(call, name, arguments));
}

#[test]
fn session_window_bytes_are_counted_into_the_real_budget() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "window-budget");
    data.note_requested(&tool_ref, "bash");
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &vec![b'x'; 512 * 1024]);
    assert_accounting(&data);
    // Many windows cannot exceed the Session budget just because each one
    // is individually bounded.
    for index in 0..16u32 {
        let filler = make_tool_ref(session(1), loop_id(1), index + 1, "filler");
        data.note_requested(&filler, "bash");
        data.note_stream_chunk(&filler, ToolDataStream::Stdout, &vec![b'y'; 512 * 1024]);
        assert_accounting(&data);
    }
    let inner = data.lock();
    assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
    // The oldest window was freed rather than silently retained beyond the
    // Session budget.
    assert!(inner.records[&tool_ref].stdout.expired);
}

#[test]
fn multi_round_push_and_multi_stream_capacity_accounting_stays_within_budget() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "capacity-test");
    data.note_requested(&tool_ref, "bash");

    // Push 200 rounds of 8 KiB chunks (~1.6 MiB total) to test repeated draining
    // does not inflate backing capacity beyond 1 MiB.
    let chunk = vec![b'c'; 8 * 1024];
    for _ in 0..200 {
        data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &chunk);
        assert_accounting(&data);
    }
    {
        let inner = data.lock();
        let window = inner.records[&tool_ref].stdout.bytes.as_slice();
        assert_eq!(window.len(), MAX_TOOL_STREAM_BYTES);
        let capacity = inner.records[&tool_ref].stdout.bytes.capacity();
        assert!(
            capacity <= MAX_TOOL_STREAM_BYTES,
            "single stream capacity {capacity} exceeded bound {MAX_TOOL_STREAM_BYTES}"
        );
    }

    // Test oversized chunk: 2.5 MiB in one push must not allocate > 1 MiB
    let oversized = vec![b'z'; 2_500_000];
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stderr, &oversized);
    assert_accounting(&data);
    {
        let inner = data.lock();
        let cap = inner.records[&tool_ref].stderr.bytes.capacity();
        assert!(
            cap <= MAX_TOOL_STREAM_BYTES,
            "oversized stream capacity {cap} exceeded bound {MAX_TOOL_STREAM_BYTES}"
        );
        assert_eq!(
            inner.records[&tool_ref].stderr.bytes.len(),
            MAX_TOOL_STREAM_BYTES
        );
        assert_eq!(
            inner.records[&tool_ref].stderr.start_offset,
            (2_500_000 - MAX_TOOL_STREAM_BYTES) as u64
        );
    }

    // Multiple streams across multiple records: verify total capacity accounting <= 8 MiB
    for index in 1..=12 {
        let other = make_tool_ref(session(1), loop_id(1), index, &format!("other-{index}"));
        data.note_requested(&other, "bash");
        // Push multi-round chunks into both stdout and stderr
        for _ in 0..16 {
            data.note_stream_chunk(&other, ToolDataStream::Stdout, &vec![b'o'; 64 * 1024]);
            data.note_stream_chunk(&other, ToolDataStream::Stderr, &vec![b'e'; 64 * 1024]);
        }
        assert_accounting(&data);
    }
    let inner = data.lock();
    assert!(
        inner.total_bytes <= MAX_TOOL_TOTAL_BYTES,
        "total capacity accounting {} exceeded {}",
        inner.total_bytes,
        MAX_TOOL_TOTAL_BYTES
    );
}

#[test]
fn paging_from_stale_offset_zero_recovers_retained_tail_for_running_and_terminal() {
    use base64::Engine;

    let data = ToolData::new();

    // 1. Running stream test with binary data
    let running_ref = make_tool_ref(session(10), loop_id(10), 0, "running-tail");
    data.note_requested(&running_ref, "bash");
    // Push 1.4 MiB of binary data
    let binary_payload: Vec<u8> = (0..1_400_000u32).map(|i| (i % 251) as u8).collect();
    for chunk in binary_payload.chunks(128 * 1024) {
        data.note_stream_chunk(&running_ref, ToolDataStream::Stdout, chunk);
    }
    let (base_offset, observed_end) = data
        .stream_range(&running_ref, ToolDataStream::Stdout)
        .unwrap();
    assert_eq!(observed_end, 1_400_000);
    assert_eq!(base_offset, 1_400_000 - MAX_TOOL_STREAM_BYTES as u64);

    // Client queries from stale offset 0
    let page_budget = 4096;
    let notice = data
        .output(
            &output_request(running_ref.clone(), ToolDataStream::Stdout, 0, None),
            page_budget,
        )
        .unwrap();
    assert!(notice.data.is_empty());
    assert_eq!(notice.base_offset, base_offset);
    assert_eq!(notice.next_offset, base_offset);
    assert!(!notice.eof, "running stream must not report eof");
    assert!(notice.truncated);
    assert_eq!(notice.availability, ToolDataAvailability::Partial);

    // Resume from notice.next_offset and reconstruct retained tail
    let mut offset = notice.next_offset;
    let mut reconstructed_binary = Vec::new();
    while offset < observed_end {
        let page = data
            .output(
                &output_request(running_ref.clone(), ToolDataStream::Stdout, offset, None),
                page_budget,
            )
            .unwrap();
        let page_json_len = serde_json::to_vec(&page).unwrap().len();
        assert!(
            page_json_len <= page_budget,
            "page encoded size {page_json_len} exceeded budget {page_budget}"
        );
        assert!(!page.eof, "running stream must not report eof before end");
        let chunk_bytes = base64::engine::general_purpose::STANDARD
            .decode(&page.data)
            .unwrap();
        reconstructed_binary.extend_from_slice(&chunk_bytes);
        assert!(page.next_offset > offset);
        offset = page.next_offset;
    }
    assert_eq!(offset, observed_end);
    let expected_tail = &binary_payload[base_offset as usize..];
    assert_eq!(reconstructed_binary.as_slice(), expected_tail);

    // 2. Terminal stream test with UTF-8 data
    let terminal_ref = make_tool_ref(session(11), loop_id(11), 0, "terminal-tail");
    data.note_requested(&terminal_ref, "bash");
    let line = "Line content for UTF-8 test with unicode: 你好，世界！\n";
    let mut utf8_payload = String::new();
    while utf8_payload.len() < 1_300_000 {
        utf8_payload.push_str(line);
    }
    let utf8_bytes = utf8_payload.as_bytes();
    for chunk in utf8_bytes.chunks(128 * 1024) {
        data.note_stream_chunk(&terminal_ref, ToolDataStream::Stdout, chunk);
    }
    data.note_stream_end(&terminal_ref, ToolDataStream::Stdout);
    let (t_base, t_end) = data
        .stream_range(&terminal_ref, ToolDataStream::Stdout)
        .unwrap();
    assert_eq!(t_end, utf8_bytes.len() as u64);
    assert_eq!(t_base, (utf8_bytes.len() - MAX_TOOL_STREAM_BYTES) as u64);

    // Query from stale offset 0
    let notice = data
        .output(
            &output_request(terminal_ref.clone(), ToolDataStream::Stdout, 0, None),
            page_budget,
        )
        .unwrap();
    assert!(notice.data.is_empty());
    assert_eq!(notice.base_offset, t_base);
    assert_eq!(notice.next_offset, t_base);
    assert!(
        !notice.eof,
        "stale offset 0 notice must not report eof when tail is retained"
    );
    assert!(notice.truncated);

    // Page until eof == true
    let mut offset = notice.next_offset;
    let mut reconstructed_utf8 = Vec::new();
    let mut saw_eof = false;
    while !saw_eof {
        let page = data
            .output(
                &output_request(terminal_ref.clone(), ToolDataStream::Stdout, offset, None),
                page_budget,
            )
            .unwrap();
        let page_json_len = serde_json::to_vec(&page).unwrap().len();
        assert!(
            page_json_len <= page_budget,
            "page encoded size {page_json_len} exceeded budget {page_budget}"
        );
        let chunk_bytes = base64::engine::general_purpose::STANDARD
            .decode(&page.data)
            .unwrap();
        reconstructed_utf8.extend_from_slice(&chunk_bytes);
        saw_eof = page.eof;
        assert!(page.next_offset > offset || page.eof);
        offset = page.next_offset;
    }
    assert_eq!(offset, t_end);
    let expected_utf8_tail = &utf8_bytes[t_base as usize..];
    assert_eq!(reconstructed_utf8.as_slice(), expected_utf8_tail);
}

#[test]
fn an_abandoned_stream_reports_truncated_and_eof_not_an_endless_page() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(2), loop_id(2), 0, "cut");
    data.note_requested(&tool_ref, "bash");
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, b"partial");
    // The owner stopped observing before a real end of output.
    data.note_stream_cut(&tool_ref, ToolDataStream::Stdout);
    let request = ToolOutputRequest {
        tool_ref,
        stream: ToolDataStream::Stdout,
        offset: 0,
        max_bytes: None,
    };
    let page = data.output(&request, 64 * 1024).unwrap();
    assert!(page.eof, "a cut stream is final");
    assert!(page.truncated, "a cut stream never claims a clean end");
    assert_eq!(page.availability, ToolDataAvailability::Partial);
}

#[test]
fn offset_pages_never_return_overlapping_or_duplicate_bytes() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(5), loop_id(5), 0, "paging");
    data.note_requested(&tool_ref, "bash");
    // Large enough that a 4 KiB budget really forces several pages while
    // staying well above the page frame's own JSON size.
    let payload: Vec<u8> = (0..8192u32).map(|byte| byte as u8).collect();
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &payload);
    data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
    let mut offset = 0u64;
    let mut assembled = Vec::new();
    let mut pages = 0usize;
    while offset < payload.len() as u64 {
        pages += 1;
        assert!(pages < 64, "paging did not make progress");
        let page = data
            .output(
                &ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset,
                    max_bytes: None,
                },
                4096,
            )
            .unwrap();
        assert!(page.next_offset > offset, "a page must make progress");
        assembled.extend(
            base64::engine::general_purpose::STANDARD
                .decode(&page.data)
                .unwrap(),
        );
        offset = page.next_offset;
    }
    assert!(pages > 1, "the budget did not force pagination");
    assert_eq!(assembled, payload);
}

#[test]
fn a_running_command_reports_live_ranges_and_retention_truncation() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(7), loop_id(7), 0, "live-ranges");
    data.note_requested(&tool_ref, "bash");
    // The owner publishes its opening record before any byte exists.
    data.note_command(
        &tool_ref,
        CommandResult {
            status: CommandStatus::Running,
            exit_code: None,
            signal: None,
            termination_confirmed: false,
            stdout_base_offset: 0,
            stdout_observed_end: 0,
            stderr_base_offset: 0,
            stderr_observed_end: 0,
            output_complete: false,
            output_truncated: false,
        },
    );
    // Bytes arrive afterwards; the stored record is not rewritten.
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, b"hello");
    let snapshot = data.snapshot(&tool_ref).unwrap();
    let command = snapshot.command.expect("a running command record");
    assert_eq!(command.status, CommandStatus::Running);
    assert_eq!(
        (command.stdout_base_offset, command.stdout_observed_end),
        (0, 5),
        "tool.read must see the live window range while streaming"
    );
    // The stream query reports the same range from the same source.
    assert_eq!(
        data.stream_range(&tool_ref, ToolDataStream::Stdout),
        Some((0, 5))
    );
    assert!(!command.output_truncated);
}

#[test]
fn a_full_eof_with_a_dropped_tail_window_reports_truncated() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(8), loop_id(8), 0, "tail-loss");
    data.note_requested(&tool_ref, "bash");
    // More than one 1 MiB window, so the oldest tail bytes were dropped.
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &vec![b'x'; 1_200_000]);
    data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
    let command = CommandResult {
        status: CommandStatus::Exited,
        exit_code: Some(0),
        signal: None,
        termination_confirmed: true,
        stdout_base_offset: 0,
        stdout_observed_end: 0,
        stderr_base_offset: 0,
        stderr_observed_end: 0,
        output_complete: true,
        output_truncated: false,
    };
    let snapshot = data.note_command(&tool_ref, command).unwrap();
    let command = snapshot.command.unwrap();
    assert!(command.output_complete, "the stream really ended");
    assert!(
        command.output_truncated,
        "a dropped tail window must still be reported as truncation"
    );
    // The reported range is the retained window, and the observed end is
    // the real stream end, not the retained length.
    assert_eq!(command.stdout_observed_end, 1_200_000);
    assert!(command.stdout_base_offset > 0);
    assert_eq!(
        (command.stdout_base_offset, command.stdout_observed_end),
        data.stream_range(&tool_ref, ToolDataStream::Stdout)
            .unwrap()
    );
}

#[test]
fn a_terminal_runtime_outcome_alone_never_closes_a_live_process_stream() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(6), loop_id(6), 0, "owner-finality");
    data.note_requested(&tool_ref, "bash");
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, b"still-running");
    // The owner published only a running command record.
    data.note_command(
        &tool_ref,
        CommandResult {
            status: CommandStatus::Running,
            exit_code: None,
            signal: None,
            termination_confirmed: false,
            stdout_base_offset: 0,
            stdout_observed_end: 13,
            stderr_base_offset: 0,
            stderr_observed_end: 0,
            output_complete: false,
            output_truncated: false,
        },
    );
    // The Runtime outcome is terminal while the owner has not finished.
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Cancelled)
        .unwrap();
    let request = output_request(tool_ref.clone(), ToolDataStream::Stdout, 0, None);
    let page = data.output(&request, 64 * 1024).unwrap();
    assert!(
        !page.eof,
        "a Runtime terminal outcome alone must not close a live stream"
    );
    assert_eq!(page.availability, ToolDataAvailability::Available);

    // Only the owner's terminal record closes it.
    data.note_command(
        &tool_ref,
        CommandResult {
            status: CommandStatus::Cancelled,
            exit_code: None,
            signal: None,
            termination_confirmed: true,
            stdout_base_offset: 0,
            stdout_observed_end: 13,
            stderr_base_offset: 0,
            stderr_observed_end: 0,
            output_complete: true,
            output_truncated: false,
        },
    );
    data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
    let page = data.output(&request, 64 * 1024).unwrap();
    assert!(page.eof);
}

#[test]
fn process_stream_pages_are_base64_with_raw_offsets() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(3), loop_id(3), 1, "call-stream");
    data.note_requested(&tool_ref, "bash");
    let payload = b"caf\xc3\xa9-\xff\x1b[31m";
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &payload[..5]);
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &payload[5..]);
    let request = ToolOutputRequest {
        tool_ref: tool_ref.clone(),
        stream: ToolDataStream::Stdout,
        offset: 0,
        max_bytes: None,
    };
    let page = data.output(&request, 64 * 1024).unwrap();
    assert_eq!(page.encoding, STREAM_ENCODING);
    assert_eq!(page.base_offset, 0);
    assert_eq!(page.next_offset, payload.len() as u64);
    assert!(
        !page.eof,
        "a running stream must never report an end of output"
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&page.data)
            .unwrap(),
        payload.to_vec()
    );
    // A page cut by the budget stays honest about the next raw offset.
    // The JSON frame has a real minimum size, so a budget that cannot hold
    // the frame is rejected instead of silently returning an empty page.
    assert!(data.output(&request, 64).is_err());
    // Only the owner can end the stream, and then eof is real.
    data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
    let ended = data.output(&request, 64 * 1024).unwrap();
    assert!(ended.eof);
    assert_eq!(ended.next_offset, payload.len() as u64);
}

#[test]
fn an_evicted_stream_window_anchors_at_the_observed_end() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(4), loop_id(4), 1, "call-evict");
    data.note_requested(&tool_ref, "bash");
    data.note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &vec![b'x'; 1_200_000]);
    let (base, end) = data
        .stream_range(&tool_ref, ToolDataStream::Stdout)
        .unwrap();
    assert_eq!(end, 1_200_000);
    assert!(base > 0, "the window retained more than its bound");
    let request = ToolOutputRequest {
        tool_ref,
        stream: ToolDataStream::Stdout,
        offset: 0,
        max_bytes: None,
    };
    let page = data.output(&request, 64 * 1024).unwrap();
    assert!(page.truncated);
    assert_eq!(page.base_offset, base);
    assert_eq!(page.availability, ToolDataAvailability::Partial);
    {
        let mut inner = data.lock();
        inner.resize(&request.tool_ref, |record| record.stdout.evict());
    }
    let expired = data.output(&request, 64 * 1024).unwrap();
    assert_eq!(expired.availability, ToolDataAvailability::Expired);
    assert_eq!(expired.next_offset, end);
    assert!(!expired.eof);
    let past_end = ToolOutputRequest {
        offset: end + 1,
        ..request
    };
    assert!(matches!(
        data.output(&past_end, 64 * 1024),
        Err(AgentError::InvalidArguments)
    ));
}

#[test]
fn full_identity_separates_sessions_loops_requests_and_calls() {
    let data = ToolData::new();
    let first = make_tool_ref(session(1), loop_id(1), 0, "call-a");
    let second = make_tool_ref(session(2), loop_id(1), 0, "call-a");
    let third = make_tool_ref(session(1), loop_id(2), 0, "call-a");
    let fourth = make_tool_ref(session(1), loop_id(1), 1, "call-a");
    let fifth = make_tool_ref(session(1), loop_id(1), 0, "call-b");
    for other in [&first, &second, &third, &fourth, &fifth] {
        data.note_requested(other, "read");
    }
    let result = data.read(&read_request(first.clone(), None), 4096).unwrap();
    assert_eq!(result.execution.tool_ref, first);
    for other in [&second, &third, &fourth, &fifth] {
        assert_ne!(result.execution.tool_ref, *other);
        assert!(data.read(&read_request(other.clone(), None), 4096).is_ok());
    }
}

#[test]
fn tool_ref_rejects_unknown_fields() {
    let value = json!({
        "session_id": session(1).to_string(),
        "loop_id": loop_id(1).to_string(),
        "request_index": 0,
        "tool_call_id": "call-a",
        "extra": "no",
    });
    assert!(serde_json::from_value::<ToolRef>(value).is_err());
}

#[test]
fn states_never_report_running_while_waiting_for_policy() {
    let data = ToolData::new();
    let loop_key = loop_id(1);
    let tool_ref = make_tool_ref(session(1), loop_key, 0, "write-call");
    data.note_requested(&tool_ref, "write");
    assert_eq!(
        data.read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap()
            .execution
            .state,
        ToolExecutionState::Requested
    );
    // Requested input is published at the policy boundary.
    invocation_of(
        &data,
        &tool_ref,
        "write-call",
        "write",
        json!({"path": "a.txt", "content": "hi"}),
    );
    data.mark_awaiting_policy(&tool_ref);
    let waiting = data
        .read(&read_request(tool_ref.clone(), None), 4096)
        .unwrap();
    assert_eq!(waiting.execution.state, ToolExecutionState::AwaitingPolicy);
    let waiting_invocation = waiting.invocation.expect("requested input");
    assert!(matches!(
        waiting_invocation.subject,
        ToolSubject::File { ref path } if path == "a.txt"
    ));
    // No execution time before the tool actually runs.
    assert!(waiting.execution.started_at.is_none());

    data.mark_running(&tool_ref);
    let running = data
        .read(&read_request(tool_ref.clone(), None), 4096)
        .unwrap();
    assert_eq!(running.execution.state, ToolExecutionState::Running);
    assert!(running.execution.started_at.is_some());

    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("tracked record finishes");
    let finished = data.read(&read_request(tool_ref, None), 4096).unwrap();
    assert_eq!(finished.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(finished.execution.outcome, Some(ToolResultOutcome::Success));
    assert!(finished.execution.finished_at.is_some());
}

#[test]
fn unknown_phase_text_is_ignored_and_never_stored() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "read-call");
    data.note_requested(&tool_ref, "read");
    data.mark_running(&tool_ref);
    data.note_phase(&tool_ref, "reading");
    assert_eq!(
        data.read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap()
            .execution
            .phase,
        Some(ToolPhase::Reading)
    );
    data.note_phase(&tool_ref, "TOP-SECRET-ARBITRARY-PROGRESS");
    let execution = data
        .read(&read_request(tool_ref, None), 4096)
        .unwrap()
        .execution;
    assert_eq!(execution.phase, Some(ToolPhase::Reading));
    let debug = format!("{execution:?}");
    assert!(!debug.contains("TOP-SECRET"));
}

#[test]
fn raw_output_is_paged_by_offset_without_escaping() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "read-call");
    data.note_requested(&tool_ref, "read");
    invocation_of(
        &data,
        &tool_ref,
        "read-call",
        "read",
        json!({"path": "é.txt"}),
    );
    let content = "1: 你好\n2: café\tend\n".repeat(64);
    data.note_result(&tool_ref, &content);
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("tracked record finishes");

    let mut collected = String::new();
    let mut offset = 0_u64;
    loop {
        let page = data
            .output(
                &output_request(tool_ref.clone(), ToolDataStream::Output, offset, Some(512)),
                512,
            )
            .unwrap();
        assert_eq!(page.base_offset, offset);
        assert_eq!(page.observed_end, content.len() as u64);
        collected.push_str(&page.data);
        offset = page.next_offset;
        if page.eof {
            break;
        }
    }
    assert_eq!(collected, content);
    assert!(collected.contains('\t'));
    assert!(collected.contains("你好"));
}

#[test]
fn output_stream_is_pending_until_the_result_is_observed() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "bash-call");
    data.note_requested(&tool_ref, "bash");
    invocation_of(
        &data,
        &tool_ref,
        "bash-call",
        "bash",
        json!({"command": "sleep 1"}),
    );
    data.mark_running(&tool_ref);

    // A running call has no result bytes yet: not eof, not available.
    let pending = data
        .output(
            &output_request(tool_ref.clone(), ToolDataStream::Output, 0, None),
            4096,
        )
        .unwrap();
    assert_eq!(pending.availability, ToolDataAvailability::Pending);
    assert_eq!(pending.observed_end, 0);
    assert!(!pending.eof);
    assert!(!pending.truncated);
    assert!(pending.data.is_empty());

    // A non-zero offset on an unobserved stream is rejected.
    assert!(matches!(
        data.output(
            &output_request(tool_ref.clone(), ToolDataStream::Output, 1, None),
            4096
        ),
        Err(AgentError::InvalidArguments)
    ));

    data.note_result(&tool_ref, "done");
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("tracked record finishes");
    let complete = data
        .output(
            &output_request(tool_ref, ToolDataStream::Output, 0, None),
            4096,
        )
        .unwrap();
    assert_eq!(complete.availability, ToolDataAvailability::Available);
    assert!(complete.eof);
    assert_eq!(complete.data, "done");
}

#[test]
fn report_reconciliation_records_failed_results_and_aligns_state() {
    use minicore_runtime::history::ToolResultHistory;
    use minicore_runtime::tools::ToolOutput;

    let data = ToolData::new();
    let session_id = session(3);
    let loop_key = loop_id(3);
    let tool_ref = make_tool_ref(session_id, loop_key, 0, "read-call");
    data.note_requested(&tool_ref, "read");
    invocation_of(
        &data,
        &tool_ref,
        "read-call",
        "read",
        json!({"path": "missing.txt"}),
    );
    data.mark_running(&tool_ref);
    // The wrapper never captured a result (for example its future was
    // dropped); only the report knows the terminal outcome.
    let item = HistoryItem::ToolResult(ToolResultHistory {
        loop_id: loop_key,
        request_index: 0,
        call_id: ToolCallId::new("read-call").unwrap(),
        tool_name: "read".parse().unwrap(),
        outcome: ToolResultOutcome::Failed,
        output: ToolOutput::new("tool failed").unwrap(),
    });
    data.reconcile(session_id, std::slice::from_ref(&item));
    let result = data
        .read(&read_request(tool_ref.clone(), None), 4096)
        .unwrap();
    assert_eq!(result.execution.state, ToolExecutionState::Failed);
    assert_eq!(result.execution.outcome, Some(ToolResultOutcome::Failed));
    let page = data
        .output(
            &output_request(tool_ref, ToolDataStream::Output, 0, None),
            4096,
        )
        .unwrap();
    assert_eq!(page.data, "tool failed");
    assert_eq!(page.availability, ToolDataAvailability::Available);
    assert!(page.eof);
}

#[test]
fn reconcile_does_not_resurrect_evicted_result_bytes() {
    use minicore_runtime::history::ToolResultHistory;
    use minicore_runtime::tools::ToolOutput;

    let data = ToolData::new();
    let session_id = session(4);
    let loop_key = loop_id(4);
    let tool_ref = make_tool_ref(session_id, loop_key, 0, "read-call");
    data.note_requested(&tool_ref, "read");
    invocation_of(
        &data,
        &tool_ref,
        "read-call",
        "read",
        json!({"path": "a.txt"}),
    );
    data.note_result(&tool_ref, &"x".repeat(MAX_TOOL_RESULT_BYTES));
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("tracked record finishes");
    // Enough distinct large result records to cross the Session byte budget
    // and evict the oldest result bytes.
    let per_record = MAX_TOOL_RESULT_BYTES;
    let needed = MAX_TOOL_TOTAL_BYTES / per_record + 2;
    for index in 1..=needed {
        let filler = make_tool_ref(
            session_id,
            loop_key,
            index as u32,
            &format!("filler-{index}"),
        );
        data.note_requested(&filler, "read");
        invocation_of(
            &data,
            &filler,
            &format!("filler-{index}"),
            "read",
            json!({"path": "f"}),
        );
        data.note_result(&filler, &"y".repeat(per_record));
        data.finish_and_snapshot(&filler, ToolResultOutcome::Success)
            .expect("tracked record finishes");
    }
    assert_eq!(
        data.read(&read_request(tool_ref.clone(), None), 4096)
            .unwrap()
            .execution
            .output_availability,
        ToolDataAvailability::Expired
    );
    let item = HistoryItem::ToolResult(ToolResultHistory {
        loop_id: loop_key,
        request_index: 0,
        call_id: ToolCallId::new("read-call").unwrap(),
        tool_name: "read".parse().unwrap(),
        outcome: ToolResultOutcome::Success,
        output: ToolOutput::new("authoritative").unwrap(),
    });
    data.reconcile(session_id, std::slice::from_ref(&item));
    let page = data
        .output(
            &output_request(tool_ref, ToolDataStream::Output, 0, None),
            4096,
        )
        .unwrap();
    assert_eq!(page.availability, ToolDataAvailability::Expired);
    assert!(page.data.is_empty());
    assert!(page.truncated);
    assert!(page.eof);
}

#[test]
fn byte_budget_counts_metadata_and_frees_capacity() {
    let data = ToolData::new();
    // Large subjects are bounded metadata and must count toward the budget.
    for index in 0..64u32 {
        let tool_ref = make_tool_ref(session(1), loop_id(1), index, &format!("subject-{index}"));
        data.note_requested(&tool_ref, "bash");
        assert_accounting(&data);
        invocation_of(
            &data,
            &tool_ref,
            &format!("subject-{index}"),
            "bash",
            json!({"command": "c".repeat(4 * 1024)}),
        );
        assert_accounting(&data);
        data.note_result(&tool_ref, &"r".repeat(4 * 1024));
        assert_accounting(&data);
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("tracked record finishes");
        assert_accounting(&data);
    }
    let inner = data.lock();
    assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
    // Metadata is genuinely counted, not just the raw input bytes.
    assert!(inner.total_bytes > 200 * 1024);
    let sum: usize = inner.records.values().map(ToolRecord::retained_bytes).sum();
    assert_eq!(sum, inner.total_bytes);
}

#[test]
fn eviction_releases_backing_capacity_and_keeps_accounting_exact() {
    let data = ToolData::new();
    let session_id = session(1);
    let loop_key = loop_id(1);
    let oldest = make_tool_ref(session_id, loop_key, 0, "oldest");
    data.note_requested(&oldest, "read");
    invocation_of(&data, &oldest, "oldest", "read", json!({"path": "a.txt"}));
    data.note_result(&oldest, &"x".repeat(MAX_TOOL_RESULT_BYTES));
    data.finish_and_snapshot(&oldest, ToolResultOutcome::Success)
        .expect("tracked record finishes");
    let capacity_before = data
        .lock()
        .records
        .get(&oldest)
        .map_or(0, |record| record.result.capacity());
    assert!(capacity_before >= MAX_TOOL_RESULT_BYTES);

    let needed = MAX_TOOL_TOTAL_BYTES / MAX_TOOL_RESULT_BYTES + 2;
    for index in 1..=needed {
        let filler = make_tool_ref(
            session_id,
            loop_key,
            index as u32,
            &format!("filler-{index}"),
        );
        data.note_requested(&filler, "read");
        invocation_of(
            &data,
            &filler,
            &format!("filler-{index}"),
            "read",
            json!({"path": "f"}),
        );
        data.note_result(&filler, &"y".repeat(MAX_TOOL_RESULT_BYTES));
        data.finish_and_snapshot(&filler, ToolResultOutcome::Success)
            .expect("tracked record finishes");
        assert_accounting(&data);
    }

    let inner = data.lock();
    assert_eq!(
        inner.records[&oldest].output_availability(),
        ToolDataAvailability::Expired
    );
    // The evicted backing buffer is actually released, not logically cleared.
    assert_eq!(inner.records[&oldest].result.capacity(), 0);
    let sum: usize = inner.records.values().map(ToolRecord::retained_bytes).sum();
    assert_eq!(sum, inner.total_bytes);
    assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
}

fn assert_accounting(data: &ToolData) {
    let inner = data.lock();
    let sum: usize = inner.records.values().map(ToolRecord::retained_bytes).sum();
    assert_eq!(
        sum, inner.total_bytes,
        "tracked total drifted from retained records"
    );
    assert!(inner.total_bytes <= MAX_TOOL_TOTAL_BYTES);
}

#[test]
fn note_requested_alone_stays_bounded() {
    let data = ToolData::new();
    for index in 0..(MAX_TOOL_RECORDS + 64) {
        let tool_ref = make_tool_ref(
            session(1),
            loop_id(1),
            index as u32,
            &format!("call-{index}"),
        );
        data.note_requested(&tool_ref, "read");
    }
    assert!(data.lock().records.len() <= MAX_TOOL_RECORDS);
}

#[test]
fn response_byte_budget_includes_json_encoding() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "read-call");
    data.note_requested(&tool_ref, "read");
    invocation_of(
        &data,
        &tool_ref,
        "read-call",
        "read",
        json!({"path": "src/\"quoted\"/文件.txt", "limit": 32}),
    );
    data.note_result(&tool_ref, &"quote\" and slash\\ and 世界\n".repeat(32));
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("tracked record finishes");

    let page = data
        .output(
            &output_request(tool_ref.clone(), ToolDataStream::Output, 0, Some(1024)),
            1024,
        )
        .unwrap();
    assert!(encoded_len(&page).unwrap() <= 1024);
    assert!(!page.eof || page.next_offset == page.observed_end || page.truncated);

    let large = make_tool_ref(session(1), loop_id(1), 1, "write-call");
    data.note_requested(&large, "write");
    invocation_of(
        &data,
        &large,
        "write-call",
        "write",
        json!({"path": "a.txt", "content": "z".repeat(16 * 1024)}),
    );
    let read = data.read(&read_request(large, Some(1024)), 1024).unwrap();
    assert!(encoded_len(&read).unwrap() <= 1024);
    assert!(read.invocation.unwrap().input.truncated);

    let error = data
        .output(
            &output_request(tool_ref.clone(), ToolDataStream::Output, 0, Some(1)),
            1,
        )
        .unwrap_err();
    assert!(matches!(error, AgentError::InvalidArguments));
    let error = data.read(&read_request(tool_ref, Some(1)), 1).unwrap_err();
    assert!(matches!(error, AgentError::InvalidArguments));
}

#[test]
fn fit_encoded_string_accepts_a_minimal_multibyte_budget() {
    // `"€"` is 5 encoded bytes; a budget of exactly 5 must keep it.
    assert_eq!(fit_encoded_string("€", 5).0, "€");
    assert!(!fit_encoded_string("€", 5).1);
    // One byte short cannot fit the character and returns an empty prefix.
    assert!(fit_encoded_string("€", 4).0.is_empty());
    assert!(fit_encoded_string("€", 4).1);
    // A larger value keeps the longest fitting prefix on a char boundary.
    assert_eq!(fit_encoded_string("€x", 5).0, "€");
    assert!(fit_encoded_string("€x", 5).1);
    assert_eq!(fit_encoded_string("€x", 6).0, "€x");
    assert!(!fit_encoded_string("€x", 6).1);
    assert_eq!(fit_encoded_string("a你好z", 64).0, "a你好z");
}

#[test]
fn terminal_without_observed_result_is_unavailable_not_pending() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "bash-call");
    data.note_requested(&tool_ref, "bash");
    invocation_of(
        &data,
        &tool_ref,
        "bash-call",
        "bash",
        json!({"command": "echo hi"}),
    );
    data.mark_running(&tool_ref);
    // Terminal with no recorded result: the stream is ended but empty.
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Cancelled)
        .expect("tracked record finishes");
    let execution = data
        .read(&read_request(tool_ref.clone(), None), 4096)
        .unwrap()
        .execution;
    assert_eq!(
        execution.output_availability,
        ToolDataAvailability::Unavailable
    );
    let page = data
        .output(
            &output_request(tool_ref, ToolDataStream::Output, 0, None),
            4096,
        )
        .unwrap();
    assert_eq!(page.availability, ToolDataAvailability::Unavailable);
    assert!(page.eof);
    assert!(page.data.is_empty());
}

#[test]
fn unknown_reference_is_not_found() {
    let data = ToolData::new();
    let missing = make_tool_ref(session(9), loop_id(9), 9, "missing");
    assert!(matches!(
        data.read(&read_request(missing.clone(), None), 4096),
        Err(AgentError::ToolNotFound)
    ));
    assert!(matches!(
        data.output(
            &output_request(missing, ToolDataStream::Output, 0, None),
            4096
        ),
        Err(AgentError::ToolNotFound)
    ));
}

#[test]
fn debug_never_exposes_raw_content() {
    let data = ToolData::new();
    let tool_ref = make_tool_ref(session(1), loop_id(1), 0, "bash-call");
    data.note_requested(&tool_ref, "bash");
    invocation_of(
        &data,
        &tool_ref,
        "bash-call",
        "bash",
        json!({"command": "curl -H 'Authorization: TOP-SECRET' https://private.invalid"}),
    );
    data.note_result(&tool_ref, "TOP-SECRET-OUTPUT");
    let read = data
        .read(&read_request(tool_ref.clone(), None), 8192)
        .unwrap();
    let page = data
        .output(
            &output_request(tool_ref, ToolDataStream::Output, 0, None),
            8192,
        )
        .unwrap();
    for debug in [format!("{read:?}"), format!("{page:?}")] {
        assert!(!debug.contains("TOP-SECRET"));
        assert!(!debug.contains("private.invalid"));
        assert!(!debug.contains("Authorization"));
    }
}

#[test]
fn merge_stored_rejects_incompatible_identity_totals_and_state() {
    let mut mem = ToolRecord::new("bash".to_owned());
    mem.state = ToolExecutionState::Succeeded;
    mem.input_seen = true;
    mem.input_total = 100;
    mem.stdout.seen = true;
    mem.stdout.observed_end = 500;

    // Incompatible tool name
    let stored_diff_name = ToolRecord::new("read".to_owned());
    assert!(!mem.merge_stored(stored_diff_name));

    // Incompatible state
    let mut stored_diff_state = ToolRecord::new("bash".to_owned());
    stored_diff_state.state = ToolExecutionState::Failed;
    assert!(!mem.merge_stored(stored_diff_state));

    // Incompatible input total
    let mut stored_diff_total = ToolRecord::new("bash".to_owned());
    stored_diff_total.state = ToolExecutionState::Succeeded;
    stored_diff_total.input_seen = true;
    stored_diff_total.input_total = 999;
    assert!(!mem.merge_stored(stored_diff_total));

    // Incompatible stdout observed end
    let mut stored_diff_end = ToolRecord::new("bash".to_owned());
    stored_diff_end.state = ToolExecutionState::Succeeded;
    stored_diff_end.input_seen = true;
    stored_diff_end.input_total = 100;
    stored_diff_end.stdout.seen = true;
    stored_diff_end.stdout.observed_end = 999;
    assert!(!mem.merge_stored(stored_diff_end));

    // Memory remains completely unmutated
    assert_eq!(mem.input_total, 100);
    assert_eq!(mem.stdout.observed_end, 500);
}

#[test]
fn merge_stored_preserves_known_empty_eof_and_fills_unknown() {
    // Known empty EOF in memory cannot be replaced by non-empty stream
    let mut mem = ToolRecord::new("bash".to_owned());
    mem.state = ToolExecutionState::Succeeded;
    mem.stdout.complete = true;
    mem.stdout.observed_end = 0;

    let mut stored = ToolRecord::new("bash".to_owned());
    stored.state = ToolExecutionState::Succeeded;
    stored.stdout.observed_end = 120;
    assert!(!mem.merge_stored(stored));
    assert_eq!(mem.stdout.observed_end, 0);
    assert!(mem.stdout.complete);

    // Unknown in memory learns complete EOF even if seen is false
    let mut mem_unknown = ToolRecord::new("bash".to_owned());
    mem_unknown.state = ToolExecutionState::Succeeded;
    let mut stored_empty_eof = ToolRecord::new("bash".to_owned());
    stored_empty_eof.state = ToolExecutionState::Succeeded;
    stored_empty_eof.stdout.complete = true;
    stored_empty_eof.stdout.observed_end = 0;
    assert!(mem_unknown.merge_stored(stored_empty_eof));
    assert!(mem_unknown.stdout.complete);
    assert_eq!(mem_unknown.stdout.observed_end, 0);
}

#[test]
fn merge_stored_reconciled_result_learns_input_and_command_from_disk() {
    let mut mem = ToolRecord::new("bash".to_owned());
    mem.state = ToolExecutionState::Succeeded;
    mem.result = "cmd output\n".to_owned();
    mem.result_seen = true;
    mem.result_total = 11;
    mem.finished_at = Some("2026-09-15T00:00:00Z".to_owned());
    // input and command are missing in memory
    assert!(!mem.input_seen);
    assert!(mem.command.is_none());

    let mut stored = ToolRecord::new("bash".to_owned());
    stored.state = ToolExecutionState::Succeeded;
    stored.phase = Some(ToolPhase::Running);
    stored.started_at = Some("2026-09-14T00:00:00Z".to_owned());
    stored.finished_at = Some("2026-09-14T00:00:01Z".to_owned());
    stored.result = "cmd output\n".to_owned();
    stored.result_seen = true;
    stored.result_total = 11;
    stored.input = "echo hello".to_owned();
    stored.input_seen = true;
    stored.input_total = 10;
    stored.stdout.seen = true;
    stored.stdout.bytes = b"cmd output\n".to_vec();
    stored.stdout.start_offset = 0;
    stored.stdout.observed_end = 11;
    stored.stdout.complete = true;
    stored.command = Some(CommandResult {
        status: CommandStatus::Exited,
        exit_code: Some(0),
        signal: None,
        termination_confirmed: true,
        stdout_base_offset: 0,
        stdout_observed_end: 11,
        stderr_base_offset: 0,
        stderr_observed_end: 0,
        output_complete: true,
        output_truncated: false,
    });

    assert!(mem.merge_stored(stored));
    assert!(mem.input_seen);
    assert_eq!(mem.input, "echo hello");
    assert_eq!(mem.input_total, 10);
    assert_eq!(mem.phase, Some(ToolPhase::Running));
    assert_eq!(mem.started_at.as_deref(), Some("2026-09-14T00:00:00Z"));
    assert_eq!(mem.finished_at.as_deref(), Some("2026-09-14T00:00:01Z"));
    let cmd = mem.command.expect("command restored from disk");
    assert_eq!(cmd.status, CommandStatus::Exited);
    assert_eq!(cmd.stdout_observed_end, 11);
}

#[test]
fn large_file_snapshots_are_evicted_by_real_capacity() {
    let data = ToolData::new();
    let before = vec![b'b'; crate::changes::MAX_CHANGE_SNAPSHOT_BYTES];
    let after = vec![b'a'; crate::changes::MAX_CHANGE_SNAPSHOT_BYTES];
    let session_id = session(1);
    let mut refs = Vec::new();
    for index in 0..8 {
        let tool_ref = make_tool_ref(
            session_id,
            loop_id(index as u8 + 1),
            index,
            &format!("write-{index}"),
        );
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: format!("file-{index}.txt"),
                kind: crate::changes::ChangeKind::Modified,
                before: crate::changes::content_revision(&before),
                after: crate::changes::content_revision(&after),
                commit_state: crate::changes::ChangeCommitState::Applied,
                coverage: crate::changes::ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(before.clone()),
                after_bytes: Some(after.clone()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        refs.push(tool_ref);
    }
    let first = data.get_record(&refs[0]).unwrap();
    let first_change = first.file_change.as_ref().unwrap();
    assert!(first_change.before_bytes.is_none());
    assert!(first_change.after_bytes.is_none());
    assert_eq!(data.file_change_records(session_id, None).len(), 8);
    assert!(data.get_record(&refs[7]).unwrap().file_change.is_some());
}
