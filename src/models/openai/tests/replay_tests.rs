use super::*;
use minicore_runtime::history::{AssistantHistory, HistoryItem};
use minicore_runtime::model::ProviderReplay;

const URL: &str = "http://localhost/v1/responses";
const MODEL: &str = "provider-model";
const SECRET: &str = "SYNTHETIC-OPAQUE-REPLAY-ONLY";

fn reasoning() -> Value {
    json!({"type":"reasoning", "id":"rs_1", "summary":[], "encrypted_content":SECRET})
}
fn message(text: &str) -> Value {
    json!({"type":"message", "id":"msg_1", "role":"assistant", "content":[{"type":"output_text", "text":text}]})
}
fn tool(id: &str) -> Value {
    json!({"type":"function_call", "id":format!("fc_{id}"), "call_id":id, "name":"read", "arguments":"{ \"path\": \"a.txt\" }"})
}
fn parts() -> Vec<AssistantPart> {
    vec![AssistantPart::ToolCall(
        ToolCall::new(
            ToolCallId::new("call_1").unwrap(),
            "read".parse().unwrap(),
            json!({"path":"a.txt"}),
            0,
        )
        .unwrap(),
    )]
}
fn request(parts: Vec<AssistantPart>, replay: ProviderReplay) -> ModelRequest {
    let results = parts
        .iter()
        .filter_map(AssistantPart::as_tool_call)
        .map(|call| {
            ModelMessage::tool_with_outcome(
                call.tool_call_id().clone(),
                ToolOutput::new("result").unwrap(),
                ToolResultOutcome::Success,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut messages = vec![
        ModelMessage::user("continue").unwrap(),
        ModelMessage::assistant_with_provider_replay(parts, Some(replay)).unwrap(),
    ];
    messages.extend(results);
    ModelRequest::new(
        messages,
        vec![],
        ModelLimits::default(),
        ReasoningPreference::High,
    )
    .unwrap()
}
fn state() -> StreamState {
    StreamState::new_with_replay(
        Box::pin(futures_util::stream::empty()),
        CancellationToken::new(),
        TokioInstant::now() + Duration::from_secs(5),
        Some((URL.to_owned(), MODEL.to_owned())),
        None,
    )
}
fn frame(state: &mut StreamState, value: Value) -> Result<(), ()> {
    handle_frame(state, &serde_json::to_vec(&value).unwrap())
}
fn done(state: &mut StreamState, index: u32, item: Value) -> Result<(), ()> {
    frame(
        state,
        json!({"type":"response.output_item.done", "output_index":index, "item":item}),
    )
}
fn finish(state: &mut StreamState, output: Vec<Value>) -> Result<(), ()> {
    frame(
        state,
        json!({"type":"response.completed", "response":{"status":"completed", "output":output}}),
    )
}
fn extract(state: &StreamState) -> ProviderReplay {
    let events = state
        .pending
        .iter()
        .filter_map(|event| match event {
            Ok(ModelEvent::ProviderReplay { replay }) => Some(replay.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 1);
    events[0].clone()
}

#[test]
fn encrypted_only_response_retains_real_metadata_without_display_text() {
    let mut state = state();
    finish(&mut state, vec![reasoning()]).unwrap();
    let replay = extract(&state);
    assert!(!state.pending.iter().any(|event| matches!(
        event,
        Ok(ModelEvent::TextDelta { .. } | ModelEvent::ReasoningDelta { .. })
    )));
    let body: Value = serde_json::from_slice(
        &model("http://localhost/v1")
            .build_request(&request(vec![], replay))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["input"][1], reasoning());
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert!(!format!("{:?}", state.pending).contains(SECRET));
}

#[test]
fn final_text_capture_and_streamed_text_are_not_duplicated() {
    for streamed in [false, true] {
        let mut state = state();
        if streamed {
            frame(
                &mut state,
                json!({"type":"response.output_text.delta", "delta":"answer"}),
            )
            .unwrap();
        }
        finish(&mut state, vec![reasoning(), message("answer")]).unwrap();
        let text = state
            .pending
            .iter()
            .filter_map(|event| match event {
                Ok(ModelEvent::TextDelta { delta }) => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(text, "answer");
        let replay = extract(&state);
        replay::validate_history(&replay, &[AssistantPart::Text("answer".into())]).unwrap();
    }
}

#[test]
fn late_ciphertext_enrichment_is_strictly_additive() {
    for null in [false, true] {
        let mut initial = reasoning();
        if null {
            initial["encrypted_content"] = Value::Null;
        } else {
            initial.as_object_mut().unwrap().remove("encrypted_content");
        }
        let mut state = state();
        done(&mut state, 0, initial).unwrap();
        finish(&mut state, vec![reasoning()]).unwrap();
        assert!(
            serde_json::to_string(&extract(&state))
                .unwrap()
                .contains(SECRET)
        );
    }
    for (field, value) in [
        ("id", json!("rs_other")),
        ("summary", json!([{"type":"summary_text","text":"changed"}])),
        ("encrypted_content", json!("replacement")),
    ] {
        let mut state = state();
        done(&mut state, 0, reasoning()).unwrap();
        let mut changed = reasoning();
        changed[field] = value;
        assert!(finish(&mut state, vec![changed]).is_err());
        assert!(!state.pending.iter().any(|event| matches!(
            event,
            Ok(ModelEvent::ProviderReplay { .. } | ModelEvent::Finish { .. })
        )));
    }
}

#[test]
fn durable_tool_projection_preserves_raw_order_arguments_and_one_result() {
    let output = vec![reasoning(), tool("call_1")];
    let replay = capture(URL, MODEL, output.clone()).unwrap();
    let mut messages = vec![
        ModelMessage::user("inspect").unwrap(),
        ModelMessage::assistant_with_provider_replay(parts(), Some(replay)).unwrap(),
    ];
    messages.push(
        ModelMessage::tool_with_outcome(
            ToolCallId::new("call_1").unwrap(),
            ToolOutput::new("one result").unwrap(),
            ToolResultOutcome::Success,
        )
        .unwrap(),
    );
    let req = ModelRequest::new(
        messages,
        vec![],
        ModelLimits::default(),
        ReasoningPreference::High,
    )
    .unwrap();
    let model = model("http://localhost/v1");
    let bytes = model.build_request(&req).unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(&body["input"].as_array().unwrap()[1..3], output);
    assert_eq!(
        body["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "function_call_output")
            .count(),
        1
    );
    for (loop_id, index) in [
        (None, None),
        (Some(LoopId::new().unwrap()), Some(0)),
        (Some(LoopId::new().unwrap()), Some(9)),
    ] {
        assert_eq!(
            model.estimate_request_bytes(&req, loop_id, index).unwrap(),
            bytes.len()
        );
    }
}

#[test]
fn incompatible_identity_and_unknown_version_fall_back_to_canonical() {
    for (url, name) in [
        ("http://localhost/other/responses", MODEL),
        (URL, "other-model"),
    ] {
        let replay = capture(url, name, vec![reasoning(), message("answer")]).unwrap();
        let req = request(vec![AssistantPart::Text("answer".into())], replay);
        let bytes = model("http://localhost/v1").build_request(&req).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains(SECRET));
        assert!(String::from_utf8_lossy(&bytes).contains("answer"));
    }
    let future = ProviderReplay::new("openai-responses-v999", json!({"future":SECRET})).unwrap();
    let req = request(vec![AssistantPart::Text("answer".into())], future.clone());
    assert!(
        !String::from_utf8_lossy(&model("http://localhost/v1").build_request(&req).unwrap())
            .contains(SECRET)
    );
    assert!(
        model("http://localhost/v1")
            .build_request(&request(vec![], future))
            .is_err()
    );
}

#[test]
fn stale_canonical_tool_or_text_is_rejected_even_with_incompatible_identity() {
    let replay = capture(URL, MODEL, vec![tool("call_1")]).unwrap();
    for replacement in ["name", "arguments", "call_id"] {
        let mut value = serde_json::to_value(&replay).unwrap();
        value["payload"]["output"][0][replacement] = match replacement {
            "name" => json!("bash"),
            "arguments" => json!("{\"path\":\"other\"}"),
            _ => json!("other"),
        };
        let changed: ProviderReplay = serde_json::from_value(value).unwrap();
        assert!(
            model("http://localhost/other")
                .build_request(&request(parts(), changed))
                .is_err()
        );
    }
    let replay = capture(URL, MODEL, vec![message("other")]).unwrap();
    assert!(
        model("http://localhost/v1")
            .build_request(&request(vec![AssistantPart::Text("answer".into())], replay))
            .is_err()
    );
}

#[test]
fn roles_input_kinds_duplicate_ids_and_empty_blobs_cannot_be_replayed() {
    let mut invalid = vec![
        json!({"type":"function_call_output","call_id":"c","output":"bad"}),
        json!({"type":"input_text","text":"bad"}),
        json!({"type":"reasoning","id":"r","summary":[]}),
    ];
    for role in ["user", "developer", "system", "tool"] {
        let mut item = message("bad");
        item["role"] = json!(role);
        invalid.push(item);
    }
    for item in invalid {
        assert!(capture(URL, MODEL, vec![item]).is_err());
    }
    assert!(capture(URL, MODEL, vec![reasoning(), reasoning()]).is_err());
    assert!(capture(URL, MODEL, vec![tool("call_1"), tool("call_1")]).is_err());
}

#[test]
fn known_malformed_payload_is_rejected_during_durable_admission() {
    for value in [
        json!({}),
        json!({"endpoint":URL,"model":MODEL,"output":[{"type":"message","role":"user"}]}),
        json!({"endpoint":"http://secret:password@localhost/v1/responses","model":MODEL,"output":[reasoning()]}),
    ] {
        let replay = ProviderReplay::new("openai-responses-v1", value).unwrap();
        assert!(replay::validate_history(&replay, &[]).is_err());
    }
}

#[test]
fn terminal_tools_must_be_observed_in_canonical_order_before_finish() {
    let mut unobserved = state();
    assert!(finish(&mut unobserved, vec![tool("call_1")]).is_err());
    let mut reordered = state();
    done(&mut reordered, 1, tool("call_2")).unwrap();
    done(&mut reordered, 0, tool("call_1")).unwrap();
    assert!(finish(&mut reordered, vec![tool("call_1"), tool("call_2")]).is_err());
    let mut incomplete = state();
    frame(
        &mut incomplete,
        json!({"type":"response.reasoning_summary_text.delta","delta":"thinking"}),
    )
    .unwrap();
    done(&mut incomplete, 0, tool("call_1")).unwrap();
    assert!(finish(&mut incomplete, vec![tool("call_1")]).is_err());
}

#[test]
fn replay_capture_bounds_fail_without_truncation_or_partial_attachment() {
    let mut oversized = reasoning();
    oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES));
    let mut state = state();
    assert!(finish(&mut state, vec![oversized]).is_err());
    assert!(!state.pending.iter().any(|event| matches!(
        event,
        Ok(ModelEvent::ProviderReplay { .. } | ModelEvent::Finish { .. })
    )));
    let too_many = (0..257)
        .map(|index| {
            let mut item = reasoning();
            item["id"] = json!(format!("r_{index}"));
            item
        })
        .collect();
    assert!(capture(URL, MODEL, too_many).is_err());
    let mut deep = json!(null);
    for _ in 0..40 {
        deep = json!([deep]);
    }
    let mut item = reasoning();
    item["extension"] = deep;
    assert!(capture(URL, MODEL, vec![item]).is_err());
}

#[test]
fn oversized_replay_preserves_canonical_content_from_terminal_or_done_items() {
    for terminal in [false, true] {
        let mut oversized = reasoning();
        oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES));
        let output = vec![oversized, message("answer"), tool("call_1")];
        let mut state = state();
        for (index, item) in output.iter().enumerate() {
            done(&mut state, index as u32, item.clone()).unwrap();
        }
        assert!(
            state.output_items.is_none(),
            "oversized raw items must not accumulate"
        );
        if terminal {
            finish(&mut state, output).unwrap();
        } else {
            frame(
                &mut state,
                json!({"type":"response.completed","response":{}}),
            )
            .unwrap();
        }
        assert_eq!(state.text, "answer");
        assert!(
            !state
                .pending
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::ProviderReplay { .. })))
        );
        assert!(
            state
                .pending
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::ToolCallEnd { .. })))
        );
        assert!(matches!(
            state.pending.back(),
            Some(Ok(ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls
            }))
        ));
    }
    // No output-item events or text deltas: recover the terminal's canonical text.
    let mut state = state();
    let mut oversized = reasoning();
    oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES));
    finish(&mut state, vec![oversized, message("terminal answer")]).unwrap();
    assert_eq!(state.text, "terminal answer");
    assert!(
        !state
            .pending
            .iter()
            .any(|event| matches!(event, Ok(ModelEvent::ProviderReplay { .. })))
    );
}

#[test]
fn replay_envelope_overflow_and_structural_limits_keep_nonempty_canonical_output() {
    let mut oversized = reasoning();
    oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES - 250));
    let output = vec![oversized, message("answer")];
    assert!(
        output.iter().map(crate::budget::encoded_len).sum::<usize>() < ProviderReplay::MAX_BYTES
    );
    assert!(
        replay::capture(URL, MODEL, output.clone())
            .unwrap()
            .is_none()
    );
    let mut response = state();
    finish(&mut response, output).unwrap();
    assert_eq!(response.text, "answer");
    assert!(
        !response
            .pending
            .iter()
            .any(|event| matches!(event, Ok(ModelEvent::ProviderReplay { .. })))
    );

    for nodes in [false, true] {
        let mut item = reasoning();
        let mut extension = json!(null);
        if nodes {
            extension = json!(vec![Value::Null; ProviderReplay::MAX_NODES]);
        } else {
            for _ in 0..ProviderReplay::MAX_DEPTH {
                extension = json!([extension]);
            }
        }
        item["extension"] = extension;
        let mut state = state();
        finish(&mut state, vec![item, message("answer")]).unwrap();
        assert_eq!(state.text, "answer");
        assert!(
            !state
                .pending
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::ProviderReplay { .. })))
        );
    }
}

#[test]
fn oversized_replay_does_not_bypass_terminal_text_tool_or_shape_validation() {
    for corrupt in ["text", "tool", "shape"] {
        let mut state = state();
        let mut oversized = reasoning();
        oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES));
        frame(
            &mut state,
            json!({"type":"response.output_text.delta", "delta":"answer"}),
        )
        .unwrap();
        done(&mut state, 0, oversized.clone()).unwrap();
        done(&mut state, 2, tool("call_1")).unwrap();
        let mut output = vec![oversized, message("answer"), tool("call_1")];
        match corrupt {
            "text" => output[1] = message("different"),
            "tool" => output[2]["arguments"] = json!("{\"path\":\"other\"}"),
            _ => output[0]["encrypted_content"] = json!(17),
        }
        assert!(finish(&mut state, output).is_err());
        assert!(
            !state
                .pending
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::Finish { .. })))
        );
    }
}

#[test]
fn overflow_preserves_done_only_reconciliation_and_duplicate_id_checks() {
    for terminal in [false, true] {
        let mut response = state();
        let mut oversized = reasoning();
        oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES));
        done(&mut response, 0, oversized.clone()).unwrap();
        done(&mut response, 1, message("original")).unwrap();
        assert!(response.output_items.is_none());
        if terminal {
            assert!(finish(&mut response, vec![oversized, message("rewritten")]).is_err());
        } else {
            assert!(done(&mut response, 2, message("duplicate id")).is_err());
        }
        assert!(
            !response
                .pending
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::Finish { .. })))
        );
    }
    let mut oversized = reasoning();
    oversized["encrypted_content"] = json!("x".repeat(ProviderReplay::MAX_BYTES));
    let mut response = state();
    done(&mut response, 0, oversized.clone()).unwrap();
    assert!(
        finish(
            &mut response,
            vec![oversized, message("first"), message("duplicate")]
        )
        .is_err()
    );
}

#[test]
fn independently_captured_anonymous_responses_have_no_input_ids() {
    let mut messages = vec![ModelMessage::user("first question").unwrap()];
    for answer in ["first answer", "second answer"] {
        let mut item = message(answer);
        item.as_object_mut().unwrap().remove("id");
        let replay = capture(URL, MODEL, vec![item]).unwrap();
        let replay = serde_json::from_slice(&serde_json::to_vec(&replay).unwrap()).unwrap();
        messages.push(
            ModelMessage::assistant_with_provider_replay(
                vec![AssistantPart::Text(answer.into())],
                Some(replay),
            )
            .unwrap(),
        );
        messages.push(ModelMessage::user("next question").unwrap());
    }
    let request = ModelRequest::new(
        messages,
        vec![],
        ModelLimits::default(),
        ReasoningPreference::Auto,
    )
    .unwrap();
    let body: Value = serde_json::from_slice(
        &model("http://localhost/v1")
            .build_request(&request)
            .unwrap(),
    )
    .unwrap();
    let assistants = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["role"] == "assistant")
        .collect::<Vec<_>>();
    assert_eq!(assistants.len(), 2);
    for (item, expected) in assistants.iter().zip(["first answer", "second answer"]) {
        assert!(item.get("id").is_none());
        assert_eq!(item["content"][0]["text"], expected);
    }
}

#[test]
fn missing_item_ids_preserve_canonical_text_and_other_items_replay() {
    for missing_message in [false, true] {
        let mut anonymous = reasoning();
        anonymous.as_object_mut().unwrap().remove("id");
        anonymous["encrypted_content"] = json!("ANONYMOUS-SECRET");
        let mut text = message("answer");
        if missing_message {
            text.as_object_mut().unwrap().remove("id");
            text["extension"] = json!("ANONYMOUS-EXTENSION");
        }
        let mut state = state();
        finish(&mut state, vec![anonymous, text, reasoning()]).unwrap();
        assert_eq!(state.text, "answer");
        let replay = extract(&state);
        replay::validate_history(&replay, &[AssistantPart::Text("answer".into())]).unwrap();
        let body = model("http://localhost/v1")
            .build_request(&request(vec![AssistantPart::Text("answer".into())], replay))
            .unwrap();
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains(SECRET));
        assert!(body.contains("answer"));
        assert!(!body.contains("ANONYMOUS-SECRET"));
        assert!(!body.contains("ANONYMOUS-EXTENSION"));
    }
    for id in [json!(null), json!(""), json!(12)] {
        let mut item = message("answer");
        item["id"] = id;
        assert!(finish(&mut state(), vec![item]).is_err());
    }
    let mut item = reasoning();
    item.as_object_mut().unwrap().remove("id");
    assert!(
        finish(&mut state(), vec![item]).is_err(),
        "anonymous replay-only output has no usable content"
    );
}

#[test]
fn anonymous_item_fallback_does_not_create_duplicate_ids_or_empty_replay() {
    let mut anonymous = message("answer");
    anonymous.as_object_mut().unwrap().remove("id");
    let mut identified = reasoning();
    identified["id"] = json!("msg_minicore_0");
    let mut response = state();
    finish(&mut response, vec![anonymous, identified]).unwrap();
    replay::validate_history(&extract(&response), &[AssistantPart::Text("answer".into())]).unwrap();

    let mut anonymous = reasoning();
    anonymous.as_object_mut().unwrap().remove("id");
    let mut empty = reasoning();
    empty["encrypted_content"] = json!("");
    let mut response = state();
    frame(
        &mut response,
        json!({"type":"response.reasoning_summary_text.delta", "delta":"thinking"}),
    )
    .unwrap();
    finish(&mut response, vec![anonymous, empty]).unwrap();
    assert!(
        !response
            .pending
            .iter()
            .any(|event| matches!(event, Ok(ModelEvent::ProviderReplay { .. })))
    );
    assert!(matches!(
        response.pending.back(),
        Some(Ok(ModelEvent::Finish { .. }))
    ));
}

#[tokio::test]
async fn eof_and_cancellation_never_produce_durable_attachment() {
    for cancelled in [false, true] {
        let mut state = state();
        done(&mut state, 0, reasoning()).unwrap();
        if cancelled {
            state.cancellation.cancel();
        }
        let (event, state) = next_stream_event(state).await.unwrap();
        assert!(event.is_err());
        assert!(!state.pending.iter().any(|event| matches!(
            event,
            Ok(ModelEvent::ProviderReplay { .. } | ModelEvent::Finish { .. })
        )));
    }
}

#[test]
fn durable_normalization_preserves_replay_public_normalization_redacts_it() {
    let replay = capture(URL, MODEL, vec![reasoning()]).unwrap();
    let item = HistoryItem::Assistant(AssistantHistory {
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        model: "main".parse().unwrap(),
        reasoning: ReasoningPreference::High,
        content: vec![],
        finish_reason: ModelFinishReason::Stop,
        usage: Usage::default(),
        provider_replay: Some(replay),
    });
    let serialized = serde_json::to_vec(&item).unwrap();
    let restored: HistoryItem = serde_json::from_slice(&serialized).unwrap();
    let durable = crate::history::normalize_history(&[restored]).unwrap();
    assert_eq!(durable.len(), 1);
    assert_eq!(durable[0], item);
    assert!(
        crate::history::sanitize_history(&durable)
            .unwrap()
            .is_empty()
    );
    assert!(!format!("{durable:?}").contains(SECRET));
    let mut legacy = serde_json::to_value(&item).unwrap();
    legacy["data"]["provider_replay"] = Value::Null;
    // Runtime's optional attachment explicitly accepts null; legacy canonical
    // empty history is normalized away, not promoted into synthetic text.
    let restored = serde_json::from_value::<HistoryItem>(legacy).unwrap();
    assert!(
        crate::history::normalize_history(&[restored])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn absent_cache_write_keeps_partition_unknown_and_validates_known_totals() {
    for value in [None, Some(Value::Null), Some(json!(0)), Some(json!(3))] {
        let mut raw = usage();
        match value {
            Some(value) => raw["input_tokens_details"]["cache_write_tokens"] = value,
            None => {
                raw["input_tokens_details"]
                    .as_object_mut()
                    .unwrap()
                    .remove("cache_write_tokens");
            }
        }
        let usage = provider_usage(serde_json::from_value(raw.clone()).unwrap()).unwrap();
        let known = raw["input_tokens_details"]["cache_write_tokens"].as_u64();
        assert_eq!(usage.cache_write_tokens(), known);
        assert_eq!(usage.input_tokens(), known.map(|write| 20 - 5 - write));
        assert_eq!(usage.cache_read_tokens(), Some(5));
        assert_eq!(usage.provider_total_tokens(), Some(31));
    }
    for raw in [
        json!({"input_tokens":1,"output_tokens":1,"total_tokens":2,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":0}}),
        json!({"input_tokens":1,"output_tokens":1,"total_tokens":3,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}),
    ] {
        assert!(provider_usage(serde_json::from_value(raw).unwrap()).is_err());
    }
}

#[test]
fn generic_utility_budget_includes_replay_conservatively_without_fake_identity_skip() {
    let replay = capture(
        "http://other.example/v2/responses",
        "different-model",
        vec![reasoning(), message("answer")],
    )
    .unwrap();
    let req = request(vec![AssistantPart::Text("answer".into())], replay);
    let mut generic = Vec::new();
    serialize_request_for_budget(&req, &mut generic).unwrap();
    assert!(String::from_utf8_lossy(&generic).contains(SECRET));
    let actual = model("http://localhost/v1");
    let bytes = actual.build_request(&req).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains(SECRET));
    assert_eq!(
        actual.estimate_request_bytes(&req, None, None).unwrap(),
        bytes.len()
    );
    assert!(generic.len() > bytes.len());
}

#[test]
fn terminal_only_refusal_preserves_refusal_finish_semantics() {
    let mut state = state();
    finish(&mut state,vec![json!({"type":"message","id":"msg_refusal","role":"assistant","content":[{"type":"refusal","refusal":"cannot comply"}]})]).unwrap();
    assert!(state.pending.iter().any(|event| matches!(
        event,
        Ok(ModelEvent::Finish {
            reason: ModelFinishReason::Refused
        })
    )));
    assert_eq!(state.text, "cannot comply");
    extract(&state);
}

#[test]
fn normalized_projection_invalidates_only_changed_legacy_reasoning_usage() {
    let raw_model = std::sync::Arc::new(model("http://localhost/v1"));
    let budget =
        std::sync::Arc::clone(&raw_model) as std::sync::Arc<dyn crate::models::ProviderBudget>;
    let state = crate::compaction::CompactionState::new();
    let loop_id = LoopId::new().unwrap();
    let original_request = basic_request(ReasoningPreference::Auto);
    let replay = capture(URL, MODEL, vec![message("answer")]).unwrap();
    let mut raw = vec![HistoryItem::Assistant(AssistantHistory {
        loop_id,
        request_index: 0,
        model: "main".parse().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: vec![AssistantPart::Text("answer".into())],
        provider_replay: Some(replay),
        finish_reason: ModelFinishReason::Stop,
        usage: Usage::default().with_provider_total_tokens(Some(9_000)),
    })];
    state.note_issued_projection(
        loop_id,
        0,
        raw_model.descriptor(),
        &original_request,
        &*budget,
    );
    let normalized = crate::history::normalize_history(&raw).unwrap();
    assert_eq!(raw.as_slice(), normalized.as_ref());
    state.note_normalized_projection(&raw, &normalized);
    assert_eq!(
        state.threshold_estimate(
            raw_model.descriptor(),
            &original_request,
            &[&normalized[0]],
            123,
            &*budget
        ),
        9_000,
        "unchanged validated replay remains usage-backed"
    );
    if let HistoryItem::Assistant(value) = &mut raw[0] {
        value.provider_replay = None;
        value.content.push(AssistantPart::Reasoning(
            minicore_runtime::model::ReasoningContent::new(
                Some("visible reasoning".into()),
                None,
                Some("legacy-encrypted".repeat(1000)),
                Some("legacy-signature".into()),
            )
            .unwrap(),
        ));
    }
    let normalized = crate::history::normalize_history(&raw).unwrap();
    assert_ne!(raw.as_slice(), normalized.as_ref());
    state.note_normalized_projection(&raw, &normalized);
    assert_eq!(
        state.threshold_estimate(
            raw_model.descriptor(),
            &original_request,
            &[&normalized[0]],
            123,
            &*budget
        ),
        123,
        "changed legacy opaque projection falls back to its current small estimate"
    );
}

fn capture(endpoint: &str, model: &str, output: Vec<Value>) -> Result<ProviderReplay, ()> {
    replay::capture(endpoint, model, output)?.ok_or(())
}
