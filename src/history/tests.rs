use std::collections::HashMap;

use super::*;
use minicore_runtime::history::{AssistantHistory, ToolResultHistory};
use minicore_runtime::model::{AssistantPart, ModelFinishReason, ModelRef, Usage};
use minicore_runtime::tools::{ToolName, ToolOutput, ToolResultOutcome};

fn tool_pair(
    loop_id: LoopId,
    request_index: u32,
    call_id: &str,
    output: &str,
) -> (HistoryItem, HistoryItem) {
    let call_id = call_id.parse::<ToolCallId>().unwrap();
    let call = minicore_runtime::model::ToolCall::new(
        call_id.clone(),
        "bash".parse::<ToolName>().unwrap(),
        serde_json::json!({"command": call_id.as_str()}),
        0,
    )
    .unwrap();
    (
        HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
            loop_id,
            request_index,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::ToolCall(call)],
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default(),
        }),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index,
            call_id,
            tool_name: "bash".parse::<ToolName>().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new(output).unwrap(),
        }),
    )
}

fn tool_batch(loop_id: LoopId, request_index: u32) -> (HistoryItem, Vec<HistoryItem>) {
    let call_ids = ["batch-one", "batch-two"];
    let calls = call_ids
        .iter()
        .enumerate()
        .map(|(call_index, call_id)| {
            let call_id = call_id.parse::<ToolCallId>().unwrap();
            minicore_runtime::model::ToolCall::new(
                call_id.clone(),
                "bash".parse::<ToolName>().unwrap(),
                serde_json::json!({"command": call_id.as_str()}),
                call_index as u32,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let results = [
        ("batch-one", "first batch result"),
        ("batch-two", "second batch result"),
    ]
    .into_iter()
    .map(|(call_id, output)| {
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index,
            call_id: call_id.parse().unwrap(),
            tool_name: "bash".parse::<ToolName>().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new(output).unwrap(),
        })
    })
    .collect();
    (
        HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
            loop_id,
            request_index,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: calls.into_iter().map(AssistantPart::ToolCall).collect(),
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default(),
        }),
        results,
    )
}

#[test]
fn page_history_does_not_process_unrelated_tool_results() {
    let mut history = Vec::new();
    for index in 0..128 {
        let (assistant, result) = tool_pair(
            LoopId::new().unwrap(),
            0,
            &format!("old-call-{index}"),
            "old result",
        );
        history.extend([assistant, result]);
    }
    let current_assistant_index = history.len();
    let (assistant, result) =
        tool_pair(LoopId::new().unwrap(), 0, "current-call", "current result");
    history.extend([assistant, result]);

    reset_tool_result_scan_count();
    let page = page_history(&history, current_assistant_index, 1, &HashMap::new());

    assert_eq!(tool_result_scan_count(), 1);
    let HistoryItemView::Assistant(assistant) = &page.items[0].item else {
        panic!("expected the current Assistant item");
    };
    assert_eq!(
        assistant.tool_calls[0].display.as_ref().unwrap().detail,
        "$ current-call"
    );
}

#[test]
fn page_history_skips_result_collection_without_page_tool_calls() {
    let loop_id = LoopId::new().unwrap();
    let mut history = vec![HistoryItem::User(UserHistory {
        loop_id,
        kind: UserMessageKind::Prompt,
        input: minicore_runtime::execution::UserInput::text("plain").unwrap(),
    })];
    for index in 0..128 {
        let (assistant, result) = tool_pair(
            LoopId::new().unwrap(),
            0,
            &format!("old-call-{index}"),
            "old result",
        );
        history.extend([assistant, result]);
    }

    reset_tool_result_scan_count();
    let page = page_history(&history, 0, 1, &HashMap::new());

    assert_eq!(tool_result_scan_count(), 0);
    assert!(matches!(page.items[0].item, HistoryItemView::User(_)));
}

#[test]
fn page_history_keeps_missing_and_cross_identity_results_distinct() {
    let loop_id = LoopId::new().unwrap();
    let (assistant_one, result_one) = tool_pair(loop_id, 0, "same-call", "first result");
    let (assistant_two, result_two) = tool_pair(loop_id, 1, "same-call", "second result\nextra");
    let missing_loop = LoopId::new().unwrap();
    let (missing_assistant, _) = tool_pair(missing_loop, 0, "missing-call", "unused");

    let history = vec![
        assistant_one,
        result_one,
        assistant_two,
        result_two,
        missing_assistant,
    ];
    let page = page_history(&history, 0, 5, &HashMap::new());

    let HistoryItemView::Assistant(first) = &page.items[0].item else {
        panic!("expected first Assistant item");
    };
    let HistoryItemView::Assistant(second) = &page.items[2].item else {
        panic!("expected second Assistant item");
    };
    let HistoryItemView::Assistant(missing) = &page.items[4].item else {
        panic!("expected missing-result Assistant item");
    };
    assert_eq!(
        first.tool_calls[0]
            .display
            .as_ref()
            .unwrap()
            .hidden_line_count,
        Some(2)
    );
    assert_eq!(
        second.tool_calls[0]
            .display
            .as_ref()
            .unwrap()
            .hidden_line_count,
        Some(3)
    );
    assert_eq!(
        missing.tool_calls[0]
            .display
            .as_ref()
            .unwrap()
            .hidden_line_count,
        Some(1)
    );
}

#[test]
fn page_history_finds_a_tool_batch_beyond_the_page_boundary() {
    let loop_id = LoopId::new().unwrap();
    let (assistant, results) = tool_batch(loop_id, 0);
    let mut history = vec![assistant];
    history.extend(results);

    reset_tool_result_scan_count();
    let page = page_history(&history, 0, 1, &HashMap::new());

    assert_eq!(tool_result_scan_count(), 2);
    let HistoryItemView::Assistant(assistant) = &page.items[0].item else {
        panic!("expected Assistant item");
    };
    assert_eq!(assistant.tool_calls.len(), 2);
    assert_eq!(assistant.tool_calls[0].call_index, 0);
    assert_eq!(assistant.tool_calls[1].call_index, 1);
    for call in &assistant.tool_calls {
        // Count the visible command and result, not pretty-printed argument JSON.
        let display = call.display.as_ref().unwrap();
        assert_eq!(display.hidden_line_count, Some(2));
        assert_eq!(
            display.expanded_input.as_deref(),
            display.detail.strip_prefix("$ ")
        );
    }
    assert_eq!(
        assistant.tool_calls[0].display.as_ref().unwrap().detail,
        "$ batch-one"
    );
    assert_eq!(
        assistant.tool_calls[1].display.as_ref().unwrap().detail,
        "$ batch-two"
    );
}

#[test]
fn history_tool_display_results_use_the_full_tool_identity() {
    let call_id: ToolCallId = "same-call".parse().unwrap();
    let call_one = minicore_runtime::model::ToolCall::new(
        call_id.clone(),
        "bash".parse::<ToolName>().unwrap(),
        serde_json::json!({"command": "first"}),
        0,
    )
    .unwrap();
    let call_two = minicore_runtime::model::ToolCall::new(
        call_id.clone(),
        "bash".parse::<ToolName>().unwrap(),
        serde_json::json!({"command": "second"}),
        0,
    )
    .unwrap();
    let loop_one: LoopId = "lup_00000000000000000000000000000001".parse().unwrap();
    let loop_two: LoopId = "lup_00000000000000000000000000000002".parse().unwrap();
    let history = vec![
        HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
            loop_id: loop_one,
            request_index: 0,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::ToolCall(call_one)],
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default(),
        }),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id: loop_one,
            request_index: 0,
            call_id: call_id.clone(),
            tool_name: "bash".parse::<ToolName>().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("first result").unwrap(),
        }),
        HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
            loop_id: loop_two,
            request_index: 0,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::ToolCall(call_two)],
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::default(),
        }),
        HistoryItem::ToolResult(ToolResultHistory {
            loop_id: loop_two,
            request_index: 0,
            call_id,
            tool_name: "bash".parse::<ToolName>().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("second result").unwrap(),
        }),
    ];
    let results = collect_tool_results(&history, 0, history.len());
    let one = match &history[0] {
        HistoryItem::Assistant(assistant) => assistant_history_view(assistant, &results),
        _ => unreachable!(),
    };
    let two = match &history[2] {
        HistoryItem::Assistant(assistant) => assistant_history_view(assistant, &results),
        _ => unreachable!(),
    };
    assert_eq!(
        one.tool_calls[0].display.as_ref().unwrap().detail,
        "$ first"
    );
    assert_eq!(
        two.tool_calls[0].display.as_ref().unwrap().detail,
        "$ second"
    );
    assert_eq!(
        one.tool_calls[0]
            .display
            .as_ref()
            .unwrap()
            .hidden_line_count,
        Some(2)
    );
    assert_eq!(
        two.tool_calls[0]
            .display
            .as_ref()
            .unwrap()
            .hidden_line_count,
        Some(2)
    );
}

#[test]
fn history_view_debug_redacts_user_and_tool_content() {
    let user = UserHistoryView {
        loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
        kind: UserMessageKind::Prompt,
        text: "do not log this prompt".to_owned(),
        timestamp: Some("2026-09-05T14:05:06Z".to_owned()),
    };
    let tool = ToolResultHistoryView {
        loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
        request_index: 0,
        tool_call_id: "call".parse().unwrap(),
        tool_name: "bash".to_owned(),
        outcome: ToolResultOutcome::Success,
        content: "do not log this result".to_owned(),
        content_truncated: false,
    };
    let debug = format!("{user:?} {tool:?}");
    assert!(!debug.contains("do not log this prompt"));
    assert!(!debug.contains("do not log this result"));
    assert!(debug.contains("text_bytes"));
    assert!(debug.contains("content_len"));
}
