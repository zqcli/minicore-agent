use super::*;
use crate::tool_data::{ToolData, ToolExecutionState};
use crate::tools::command::CommandOwners;
use crate::tools::{
    CommandEnvironment, NativeApplyPatchTool, NativeEditTool, NativeWriteTool, OwnedBashTool,
};
use minicore_runtime::tools::{Tool, ToolContext, ToolError, ToolInvocation, ToolProgressSink};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

#[test]
fn tool_error_presentation_is_static_for_every_variant() {
    let cases = [
        (
            minicore_runtime::tools::ToolError::Cancelled,
            "tool execution was cancelled",
        ),
        (
            minicore_runtime::tools::ToolError::Failed,
            "tool execution failed",
        ),
        (
            minicore_runtime::tools::ToolError::TimedOut,
            "tool execution timed out",
        ),
        (
            minicore_runtime::tools::ToolError::Panicked,
            "tool operation panicked",
        ),
        (
            minicore_runtime::tools::ToolError::InvalidInvocation,
            "tool invocation is invalid",
        ),
        (
            minicore_runtime::tools::ToolError::Internal,
            "tool operation failed internally",
        ),
    ];
    for (error, expected) in cases {
        let actual = safe_tool_error_text(error);
        assert_eq!(actual, expected);
        assert!(!actual.contains("missing-secret.txt"));
        assert!(!actual.contains("prompt"));
        assert!(!actual.contains("diagnostic"));
    }
}

#[test]
fn redacted_debug_never_exposes_raw_text() {
    let display = ToolDisplay {
        detail: "$ rm -rf ~/secrets".to_owned(),
        expanded_input: Some("TOP SECRET CONTENT".to_owned()),
        input_line_count: Some(1),
        hidden_line_count: Some(2),
        truncated: false,
    };
    let debug = format!("{display:?}");
    assert!(!debug.contains("rm -rf"));
    assert!(!debug.contains("TOP SECRET"));
    assert!(!debug.contains("secrets"));
    assert!(debug.contains("detail_bytes"));
    assert!(debug.contains("expanded_input_bytes"));
}

#[test]
fn whitelist_details_and_counts() {
    let bash = build_tool_display(
        "bash",
        Some(&serde_json::json!({ "command": "cargo test" })),
        Some("ok\nok"),
    );
    assert_eq!(bash.detail, "$ cargo test");
    assert!(bash.expanded_input.is_none());
    assert_eq!(bash.hidden_line_count, Some(5));

    let write = build_tool_display(
        "write",
        Some(&serde_json::json!({ "path": "a.txt", "content": "l1\nl2\nl3" })),
        Some("created"),
    );
    assert_eq!(write.detail, "a.txt");
    assert_eq!(write.input_line_count, Some(3));
    assert_eq!(write.hidden_line_count, Some(4));

    let read = build_tool_display(
        "read",
        Some(&serde_json::json!({ "path": "src/main.rs", "offset": 10, "limit": 5 })),
        None,
    );
    assert_eq!(read.detail, "src/main.rs:10-14");
    assert_eq!(read.hidden_line_count, Some(5));

    let edit = build_tool_display(
        "edit",
        Some(&serde_json::json!({ "path": "a.rs", "old_text": "x\ny", "new_text": "z" })),
        None,
    );
    assert_eq!(edit.detail, "a.rs");
    assert_eq!(edit.expanded_input.as_deref(), Some("x\ny\nz"));
    assert_eq!(edit.input_line_count, Some(3));
}

#[test]
fn path_details_preserve_paths_without_a_home_directory() {
    for path in ["/tmp/a.txt", "/src/main.rs", "a.rs", "", "C:\\work\\a.rs"] {
        let args = serde_json::json!({ "path": path });
        assert_eq!(path_arg(&args, "").as_deref(), Some(path));
    }
    let args = serde_json::json!({ "file_path": "/tmp/a.txt" });
    assert_eq!(path_arg(&args, "").as_deref(), Some("/tmp/a.txt"));
}

#[test]
fn path_details_only_abbreviate_a_known_home_boundary() {
    for (path, home, expected) in [
        ("/home/test", "/home/test", "~"),
        ("/home/test/src/main.rs", "/home/test", "~/src/main.rs"),
        ("/home/testing/a.rs", "/home/test", "/home/testing/a.rs"),
        ("/tmp/a.txt", "/home/test", "/tmp/a.txt"),
        ("/", "/", "~"),
        ("/src/main.rs", "/", "~/src/main.rs"),
    ] {
        let args = serde_json::json!({ "path": path });
        assert_eq!(path_arg(&args, home).as_deref(), Some(expected));
    }
}

#[test]
fn truncated_input_is_flagged_and_counts_reflect_displayable_rows() {
    let content = "a\n".repeat(300_000); // > 512 KiB
    let write = build_tool_display(
        "write",
        Some(&serde_json::json!({ "path": "x", "content": content })),
        None,
    );
    assert!(write.truncated);
    assert_eq!(write.detail, "x");
    // input_line_count reflects the source arg; hidden rows reflect only
    // what is actually expandable, so the TUI cannot promise rows it
    // cannot show. The trailing newline creates one final empty row.
    assert_eq!(write.input_line_count, Some(300_001));
    let expandable = write
        .expanded_input
        .as_ref()
        .map(|text| count_lines(text))
        .unwrap();
    assert!(expandable < 300_001);
    assert_eq!(write.hidden_line_count, Some(expandable));
}

#[test]
fn generic_tool_detail_is_bounded_single_line_and_redacted() {
    let display = build_tool_display(
        "unlisted",
        Some(&serde_json::json!({
            "command": "DO NOT DISPLAY",
            "path": "/private/secret.txt",
            "args": "x"
        })),
        None,
    );
    assert!(!display.detail.contains('\n'));
    assert!(!display.detail.contains("DO NOT DISPLAY"));
    assert!(!display.detail.contains("secret.txt"));
    assert!(display.expanded_input.is_none());
    assert_eq!(display.hidden_line_count, Some(5));
}

#[test]
fn result_truncation_marks_display_and_bounds_hidden_rows() {
    let result = "x\n".repeat(300_000);
    let display = build_tool_display(
        "bash",
        Some(&serde_json::json!({ "command": "printf x" })),
        Some(&result),
    );
    assert!(display.truncated);
    assert_eq!(
        display.hidden_line_count,
        Some(3 + count_lines(&result[..MAX_RESULT_DISPLAY_BYTES]))
    );
}

#[test]
fn assistant_parts_preserve_visible_order_and_redact_debug() {
    let tool_call = minicore_runtime::model::ToolCall::new(
        ToolCallId::new("call-1").unwrap(),
        "read".parse().unwrap(),
        serde_json::json!({"path": "secret.txt"}),
        0,
    )
    .unwrap();
    let reasoning = minicore_runtime::model::ReasoningContent::new(
        Some("think".to_owned()),
        Some("summary".to_owned()),
        None,
        None,
    )
    .unwrap();
    let parts = assistant_display_parts(&[
        minicore_runtime::model::AssistantPart::Text("before".to_owned()),
        minicore_runtime::model::AssistantPart::Reasoning(reasoning),
        minicore_runtime::model::AssistantPart::ToolCall(tool_call),
        minicore_runtime::model::AssistantPart::Text("after".to_owned()),
    ]);
    assert!(matches!(&parts[0], AssistantDisplayPart::Text { text } if text == "before"));
    assert!(
        matches!(&parts[1], AssistantDisplayPart::Reasoning { text } if text == "thinksummary")
    );
    assert!(matches!(&parts[2], AssistantDisplayPart::ToolCall { name, .. } if name == "read"));
    assert!(matches!(&parts[3], AssistantDisplayPart::Text { text } if text == "after"));
    let debug = format!("{:?}", parts[1]);
    assert!(!debug.contains("think"));
}

#[test]
fn editor_old_and_new_and_bash_hint_lines() {
    let edit = build_tool_display(
        "edit",
        Some(&serde_json::json!({ "old_text": "a\nb\nc", "new_text": "d" })),
        Some("updated"),
    );
    assert_eq!(edit.input_line_count, Some(4));
    // input(4) + result(1) = 5 hidden rows.
    assert_eq!(edit.hidden_line_count, Some(5));
}

#[test]
fn hidden_counts_follow_the_fixed_tool_execution_estimator() {
    for (result_rows, expected) in [(19, 20), (20, 21), (21, 22)] {
        let result = (0..result_rows)
            .map(|line| format!("result {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let display =
            build_tool_display("custom_tool", Some(&serde_json::json!({})), Some(&result));
        assert_eq!(display.hidden_line_count, Some(expected));
    }

    let read = build_tool_display(
        "read",
        Some(&serde_json::json!({
            "path": "src/main.rs",
            "offset": 10,
            "limit": 5
        })),
        Some("line 10\nline 11"),
    );
    assert_eq!(read.hidden_line_count, Some(7));

    let write = build_tool_display(
        "write",
        Some(&serde_json::json!({"path": "src/main.rs"})),
        Some("created"),
    );
    assert_eq!(write.hidden_line_count, Some(4));
}

#[test]
fn steer_receipt_commits_only_at_real_start_and_emits_once_per_count() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    let presentation = Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
    let loop_r0 = LoopId::new().unwrap();
    let key_a = RequestKey {
        loop_id: loop_r0,
        request_index: 0,
    };
    let key_b = RequestKey {
        loop_id: loop_r0,
        request_index: 1,
    };
    let drain = |receiver: &mut tokio::sync::mpsc::Receiver<AgentEvent>| -> Vec<AgentEvent> {
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        events
    };

    // Preparing alone must never release the queue: no event before start.
    presentation.commit_prepared_request(key_a, 0);
    assert_eq!(presentation.snapshot().steer_progress, None);

    // Real Model::start commits the prepared count and emits the receipt.
    presentation.note_request_start(key_a);
    let events = drain(&mut receiver);
    assert!(
        matches!(
            events
                .iter()
                .find(|event| matches!(event, AgentEvent::SteerProgress { .. })),
            Some(AgentEvent::SteerProgress {
                request_index: 0,
                applied_count: 0,
                ..
            })
        ),
        "first request applies count 0: {events:?}"
    );
    assert_eq!(
        presentation
            .snapshot()
            .steer_progress
            .as_ref()
            .map(|view| view.applied_count),
        Some(0)
    );

    // Two accepted steers before the next boundary: history then shows 2.
    assert_eq!(presentation.note_steer_accepted(Some("t0".into())), Some(1));
    assert_eq!(presentation.note_steer_accepted(Some("t1".into())), Some(2));
    presentation.commit_prepared_request(key_b, 2);
    presentation.note_request_start(key_b);
    let events = drain(&mut receiver);
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::SteerProgress {
                request_index: 1,
                applied_count: 2,
                ..
            }
        )),
        "second request applies count 2: {events:?}"
    );
    assert_eq!(
        presentation
            .snapshot()
            .steer_progress
            .as_ref()
            .map(|view| view.applied_count),
        Some(2)
    );

    // A later request observing the same count must not re-emit or rewrite.
    let key_c = RequestKey {
        loop_id: loop_r0,
        request_index: 2,
    };
    presentation.commit_prepared_request(key_c, 2);
    presentation.note_request_start(key_c);
    let events = drain(&mut receiver);
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, AgentEvent::SteerProgress { .. })),
        "same count must not re-emit: {events:?}"
    );

    // Loop reset clears accepted count and progress.
    presentation.reset_before_loop_start();
    presentation.bind_started_loop(LoopId::new().unwrap(), None);
    assert_eq!(presentation.note_steer_accepted(Some("t2".into())), Some(1));
    assert_eq!(presentation.snapshot().steer_progress, None);
}

#[test]
fn steer_times_still_feed_the_same_live_times_queue() {
    let (sender, _receiver) = tokio::sync::mpsc::channel(8);
    let presentation = Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
    presentation.note_steer_accepted(Some("t0".into()));
    presentation.note_steer_accepted(None);
    assert_eq!(
        presentation.peek_user_times(3),
        vec![Some("t0".to_owned()), None]
    );
}

/// A loop whose tool future was cancelled leaves no live-table entry, and a
/// later loop that reuses the same call id must still get its own identity:
/// the `ToolRef` is captured per execution, never looked up or reused.
#[test]
fn a_reused_call_id_in_a_new_loop_gets_its_own_identity() {
    let (sender, _receiver) = tokio::sync::mpsc::channel(8);
    let presentation = Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
    let observer = ToolObserver::new(
        presentation.session_id(),
        Arc::new(ToolData::new()),
        AgentEventSink::new(tokio::sync::mpsc::channel(8).0),
    );
    let call_id = ToolCallId::new("call-a").unwrap();
    let invocation = ToolInvocation {
        tool_call_id: call_id.clone(),
        tool_name: "bash".parse().unwrap(),
        arguments: serde_json::json!({"command": "true"}),
    };
    let display = ToolDisplay {
        detail: "$ true".to_owned(),
        expanded_input: None,
        input_line_count: None,
        hidden_line_count: None,
        truncated: false,
    };

    // First loop: the call is published, then cancelled, so its future
    // never runs `finish_tool` and the live table keeps the entry.
    let first_key = RequestKey {
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
    };
    presentation.note_request_start(first_key);
    presentation.begin_tool(Some(first_key), &invocation, display.clone());
    let first_ref = ToolRef {
        session_id: presentation.session_id(),
        loop_id: first_key.loop_id,
        request_index: 0,
        tool_call_id: call_id.clone(),
    };
    observer.tool_data().note_requested(&first_ref, "bash");

    // Second loop on the same Session reuses the call id.
    let second_key = RequestKey {
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
    };
    presentation.reset_before_loop_start();
    presentation.bind_started_loop(second_key.loop_id, None);
    presentation.note_request_start(second_key);
    presentation.begin_tool(Some(second_key), &invocation, display);
    let second_ref = ToolRef {
        session_id: presentation.session_id(),
        loop_id: second_key.loop_id,
        request_index: 0,
        tool_call_id: call_id.clone(),
    };

    // The captured identities are distinct and never resolved by position.
    assert_ne!(first_ref.loop_id, second_ref.loop_id);
    assert_eq!(second_ref.loop_id, second_key.loop_id);
    // The new loop's binding only owns the new loop's registry.
    let binding = observer.command_binding(CommandOwners::new());
    assert_eq!(binding.owners().active(), 0);
}

/// A completed call removes its live entry, so the Session's close join is
/// not relying on stale bookkeeping to reach an owned command.
#[test]
fn finishing_a_call_removes_its_live_entry() {
    let (sender, _receiver) = tokio::sync::mpsc::channel(8);
    let presentation = Presentation::new(SessionId::new().unwrap(), AgentEventSink::new(sender));
    let key = RequestKey {
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
    };
    let call_id = ToolCallId::new("call-finish").unwrap();
    let invocation = ToolInvocation {
        tool_call_id: call_id.clone(),
        tool_name: "bash".parse().unwrap(),
        arguments: serde_json::json!({"command": "true"}),
    };
    presentation.note_request_start(key);
    presentation.begin_tool(
        Some(key),
        &invocation,
        ToolDisplay {
            detail: "$ true".to_owned(),
            expanded_input: None,
            input_line_count: None,
            hidden_line_count: None,
            truncated: false,
        },
    );
    presentation.finish_tool(Some(key), &call_id, None);
    assert!(presentation.lock().live_tools.is_empty());
}

#[tokio::test]
async fn presentation_tool_variants_share_binding_and_finish() {
    let session_id = SessionId::new().unwrap();
    let base = std::env::temp_dir().join(format!("minicore-agent-presentation-tool-{session_id}"));
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(crate::Workspace::open(root).await.unwrap());

    let (presentation_sender, mut presentation_events) = tokio::sync::mpsc::channel(16);
    let presentation = Presentation::new(
        session_id,
        crate::event::AgentEventSink::new(presentation_sender),
    );
    let (observer_sender, mut observer_events) = tokio::sync::mpsc::channel(16);
    let observer = ToolObserver::new(
        session_id,
        Arc::new(ToolData::new()),
        crate::event::AgentEventSink::new(observer_sender),
    );
    let owners = CommandOwners::new();
    let plain = crate::tools::build_tools(
        &["read".to_owned()],
        Arc::clone(&workspace),
        CommandEnvironment::new(std::iter::empty::<std::ffi::OsString>()),
    )
    .unwrap()
    .get(&"read".parse().unwrap())
    .unwrap();
    let bash = Arc::new(OwnedBashTool::with_binding(
        Arc::clone(&workspace),
        CommandEnvironment::new(std::iter::empty::<std::ffi::OsString>()),
        Some(observer.command_binding(Arc::clone(&owners))),
    ));
    let write = Arc::new(NativeWriteTool::new(Arc::clone(&workspace)));
    let edit = Arc::new(NativeEditTool::new(Arc::clone(&workspace)));
    let apply_patch = Arc::new(NativeApplyPatchTool::new(Arc::clone(&workspace)));
    let tools = [
        (
            "read",
            PresentationTool::new_with_observer(
                plain,
                Arc::clone(&presentation),
                Arc::clone(&observer),
            ),
        ),
        (
            "bash",
            PresentationTool::new_bash_with_observer(
                bash,
                Arc::clone(&presentation),
                Arc::clone(&observer),
            ),
        ),
        (
            "write",
            PresentationTool::new_write_with_observer(
                write,
                Arc::clone(&presentation),
                Arc::clone(&observer),
            ),
        ),
        (
            "edit",
            PresentationTool::new_edit_with_observer(
                edit,
                Arc::clone(&presentation),
                Arc::clone(&observer),
            ),
        ),
        (
            "apply_patch",
            PresentationTool::new_apply_patch_with_observer(
                apply_patch,
                Arc::clone(&presentation),
                Arc::clone(&observer),
            ),
        ),
    ];
    assert!(matches!(&tools[0].1.inner, ToolImpl::Plain(_)));
    assert!(matches!(&tools[1].1.inner, ToolImpl::Bash(_)));
    assert!(matches!(&tools[2].1.inner, ToolImpl::Write(_)));
    assert!(matches!(&tools[3].1.inner, ToolImpl::Edit(_)));
    assert!(matches!(&tools[4].1.inner, ToolImpl::ApplyPatch(_)));

    for (index, (name, tool)) in tools.into_iter().enumerate() {
        assert_eq!(tool.spec().name().as_str(), name);
        assert!(Arc::ptr_eq(&tool.presentation, &presentation));
        assert!(Arc::ptr_eq(&tool.observer, &observer));
        let key = RequestKey {
            loop_id: LoopId::new().unwrap(),
            request_index: index as u32,
        };
        observer.note_request_start(key);
        let tool_call_id = ToolCallId::new(format!("presentation-{name}")).unwrap();
        let old_ref = ToolRef {
            session_id,
            loop_id: key.loop_id,
            request_index: key.request_index,
            tool_call_id: tool_call_id.clone(),
        };
        let invocation = ToolInvocation {
            tool_call_id,
            tool_name: name.parse().unwrap(),
            arguments: serde_json::json!({}),
        };
        let future = tool.execute(
            invocation,
            ToolContext {
                cancellation: CancellationToken::new(),
                deadline: Instant::now() + Duration::from_secs(5),
                progress: ToolProgressSink::default(),
            },
        );

        let execution = observer
            .tool_data()
            .snapshot(&old_ref)
            .expect("execute must publish the old ToolRef before polling");
        assert_eq!(execution.tool_ref, old_ref);
        assert_eq!(execution.state, ToolExecutionState::Running);
        match observer_events.try_recv().expect("invocation event") {
            AgentEvent::ToolInvocation { data, meta, .. } => {
                assert_eq!(data.tool_ref, old_ref);
                assert_eq!(meta.session_id, session_id);
                assert_eq!(meta.loop_id, Some(key.loop_id));
            }
            event => panic!("unexpected observer event: {event:?}"),
        }

        let new_key = RequestKey {
            loop_id: LoopId::new().unwrap(),
            request_index: 100 + index as u32,
        };
        observer.note_request_start(new_key);
        assert_eq!(future.await, Err(ToolError::InvalidInvocation));

        match presentation_events.try_recv().expect("finish event") {
            AgentEvent::ToolPresentation {
                turn,
                request_index,
                tool_call_id,
                tool_name,
                meta,
                ..
            } => {
                assert_eq!(tool_name, name);
                assert_eq!(turn.loop_id, old_ref.loop_id);
                assert_eq!(request_index, old_ref.request_index);
                assert_eq!(tool_call_id, old_ref.tool_call_id);
                assert_eq!(meta.session_id, session_id);
                assert_eq!(meta.loop_id, Some(old_ref.loop_id));
                assert_ne!(turn.loop_id, new_key.loop_id);
            }
            event => panic!("unexpected presentation event: {event:?}"),
        }
        assert!(presentation.lock().live_tools.is_empty());
    }

    assert!(observer_events.try_recv().is_err());
    assert!(presentation_events.try_recv().is_err());
    let _ = tokio::fs::remove_dir_all(base).await;
}
