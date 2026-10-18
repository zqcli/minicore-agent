use super::*;

#[tokio::test]
async fn reopened_session_projects_external_summary_as_bounded_user_data() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-snapshot-{}", next_id()), false).await;

    let model = FakeModel::new("main", []);
    let next_model = FakeModel::new("other", [ModelScript::Text("next answer")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model)),
            ("other".to_owned(), Arc::clone(&next_model)),
        ]),
        read_profile(),
    )
    .await;
    let reopened = agent.open_session(session_id).await.unwrap();
    assert_eq!(reopened.session_id, session_id);
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.coverage.covered_loop_count, 1);
    assert_eq!(context.coverage.covered_item_count, 2);
    assert_eq!(context.coverage.retained_item_count, 2);
    assert_eq!(context.budget.estimated_history_items, 2);
    assert_eq!(context.budget.within_runtime_limits, Some(true));
    // Loading and projecting a derived snapshot must not rewrite core history.
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let candidate = agent.config().clone();
    let candidate_models = Models::from_values(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model) as Arc<dyn Model>),
        (
            "other".to_owned(),
            Arc::clone(&next_model) as Arc<dyn Model>,
        ),
    ]));
    agent
        .reload_settings_with_models(candidate, candidate_models)
        .unwrap();
    agent
        .update_session(UpdateSession {
            session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

    let turn = send_text(&mut agent, session_id, "current user").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let requests = next_model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let messages = requests[0].messages();

    let systems = messages
        .iter()
        .filter_map(|message| match message {
            ModelMessage::System(text) => Some(text.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(systems.len(), 1);
    assert!(systems[0].contains("test system prompt"));
    assert!(systems[0].contains("SNAPSHOT_AGENTS"));
    assert!(!systems[0].contains(SUMMARY_CONTENT));
    assert!(!systems[0].contains("current user"));
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::System(text) if text.contains(SUMMARY_CONTENT))
    }));

    let summary_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT)),
        )
        .expect("valid summary snapshot must become a non-system data message");
    let summary_text = match &messages[summary_index] {
        ModelMessage::User(text) => text,
        _ => unreachable!("summary index is selected from User messages"),
    };
    assert_eq!(summary_text.as_ref(), EXPECTED_SUMMARY_ENVELOPE);
    assert_ne!(summary_text.as_ref(), SUMMARY_CONTENT);

    let suffix_user_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "suffix user"),
        )
        .expect("uncovered suffix user must remain in the request");
    let suffix_assistant_index = messages
        .iter()
        .position(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text.as_ref() == "suffix assistant")))
        })
        .expect("uncovered suffix assistant must remain in the request");
    let current_user_index = messages
        .iter()
        .position(|message| matches!(message, ModelMessage::User(text) if text.as_ref() == "current user"))
        .expect("current user must remain in the request");
    assert!(summary_index < suffix_user_index);
    assert!(suffix_user_index < suffix_assistant_index);
    assert!(suffix_assistant_index < current_user_index);
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::User(text) if text.as_ref() == "covered user")
    }));
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text.as_ref() == "covered assistant")))
    }));

    // The request reached a completed Agent loop, so the final projected
    // ModelRequest passed Runtime's exchange validator.
    assert!(
        std::fs::read(&history_path)
            .unwrap()
            .starts_with(&history_before)
    );
}

#[tokio::test]
async fn projected_summary_binding_survives_a_model_update_in_the_same_loop() {
    let (data_dir, _data_guard, session_id, _history_path, _history_before) =
        synthetic_summary_session(&format!("summary-same-loop-update-{}", next_id()), true).await;
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "main",
        [ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({"path": "suffix.txt"}),
        )],
    );
    let model_b = FakeModel::new("other", [ModelScript::Text("from model b")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();

    let turn = send_text(&mut agent, session_id, "same loop current").await;
    gate.entered.notified().await;
    let updated = agent
        .update_session(UpdateSession {
            session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_some());
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let requests = model_b.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let messages = requests[0].messages();
    assert_eq!(
        messages
            .iter()
            .filter(|message| {
                matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT))
            })
            .count(),
        1
    );
    assert!(messages.iter().any(
        |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "suffix user")
    ));
    assert!(messages.iter().any(|message| {
        matches!(message, ModelMessage::Tool { tool_call_id, .. } if tool_call_id.as_str() == "suffix-read-call")
    }));
    assert!(
        messages.iter().any(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "same loop current")
        )
    );
    assert!(!messages.iter().any(
        |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "covered user")
    ));
}

#[tokio::test]
async fn snapshot_with_same_count_different_loaded_history_is_ignored() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(
            &format!("summary-loaded-history-mismatch-{}", next_id()),
            false,
        )
        .await;
    let summary_path = history_path.parent().unwrap().join("summary.json");
    let summary_before = std::fs::read(&summary_path).unwrap();
    let store = Store::open(data_dir).await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    let mut different_history = loaded.history.to_vec();
    let mut changed = false;
    for item in &mut different_history {
        let HistoryItem::User(user) = item else {
            continue;
        };
        let loop_id = user.loop_id;
        let kind = user.kind;
        user.input =
            minicore_runtime::execution::UserInput::text("different loaded history user").unwrap();
        assert_eq!(user.loop_id, loop_id);
        assert_eq!(user.kind, kind);
        changed = true;
        break;
    }
    assert!(changed);
    assert_eq!(different_history.len(), loaded.history.len());

    let state = crate::compaction::load_state(&store, session_id, &different_history).await;
    assert!(state.project(&different_history).is_none());
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_before);
}

#[tokio::test]
async fn invalid_external_summary_falls_back_to_full_history() {
    for variant in [
        "hash",
        "hash-shape",
        "truncated",
        "boundary",
        "count",
        "last-loop",
        "version",
        "unknown",
        "wrong-session",
        "blank",
        "body-oversized",
        "oversized",
        "directory",
    ] {
        let (data_dir, _data_guard, session_id, history_path, history_before) =
            synthetic_summary_session(&format!("summary-fallback-{variant}-{}", next_id()), false)
                .await;
        let summary_path = history_path.parent().unwrap().join("summary.json");
        match variant {
            "hash" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                let old = format!("\"sha256\": \"{SUMMARY_PREFIX_SHA256}\"");
                let new = format!("\"sha256\": \"b{}\"", &SUMMARY_PREFIX_SHA256[1..]);
                assert!(raw.contains(&old));
                std::fs::write(&summary_path, raw.replacen(&old, &new, 1)).unwrap();
            }
            "hash-shape" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &format!("\"sha256\": \"{SUMMARY_PREFIX_SHA256}\""),
                        "\"sha256\": \"bad\"",
                        1,
                    ),
                )
                .unwrap();
            }
            "truncated" => {
                std::fs::write(&summary_path, b"{\"format_version\":1").unwrap();
            }
            "boundary" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &format!("\"prefix_bytes\": {SUMMARY_PREFIX_BYTES}"),
                        "\"prefix_bytes\": 1010",
                        1,
                    ),
                )
                .unwrap();
            }
            "count" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen("\"covered_item_count\": 2", "\"covered_item_count\": 3", 1),
                )
                .unwrap();
            }
            "last-loop" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &format!("\"last_loop_id\": \"{SUMMARY_COVERED_LOOP_ID}\""),
                        "\"last_loop_id\": \"lup_44444444444444444444444444444444\"",
                        1,
                    ),
                )
                .unwrap();
            }
            "version" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen("\"format_version\": 1", "\"format_version\": 2", 1),
                )
                .unwrap();
            }
            "unknown" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                let raw = raw.replace(SUMMARY_CONTENT, "DERIVED_SECRET");
                let insert_at = raw.rfind('}').unwrap();
                let mut raw = raw;
                raw.insert_str(insert_at, ",\n  \"unknown\": true");
                std::fs::write(&summary_path, raw).unwrap();
            }
            "wrong-session" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &session_id.to_string(),
                        "ses_44444444444444444444444444444444",
                        1,
                    ),
                )
                .unwrap();
            }
            "blank" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(&summary_path, raw.replacen(SUMMARY_CONTENT, r#" \n\t "#, 1))
                    .unwrap();
            }
            "body-oversized" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                let oversized = "x".repeat(64 * 1024 + 1);
                std::fs::write(&summary_path, raw.replacen(SUMMARY_CONTENT, &oversized, 1))
                    .unwrap();
            }
            "oversized" => {
                std::fs::write(&summary_path, vec![b'x'; 256 * 1024 + 1]).unwrap();
            }
            "directory" => {
                std::fs::remove_file(&summary_path).unwrap();
                std::fs::create_dir(&summary_path).unwrap();
            }
            _ => unreachable!("all summary fallback variants are listed above"),
        }

        let model = FakeModel::new("main", [ModelScript::Text("fallback answer")]);
        let mut agent = open_agent(
            &data_dir,
            BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
            read_profile(),
        )
        .await;
        agent.open_session(session_id).await.unwrap();
        assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
        let turn = send_text(&mut agent, session_id, "fallback current user").await;
        wait_text(&agent, turn).await;

        let requests = model.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "fallback variant: {variant}");
        let messages = requests[0].messages();
        assert!(
            messages.iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.as_ref() == "covered user")
            }),
            "covered user missing for {variant}"
        );
        assert!(messages.iter().any(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text.as_ref() == "covered assistant")))
        }), "covered assistant missing for {variant}");
        assert!(
            messages.iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.as_ref() == "suffix user")
            }),
            "suffix user missing for {variant}"
        );
        assert!(
            messages.iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.as_ref() == "fallback current user")
            }),
            "current user missing for {variant}"
        );
        assert!(!messages.iter().any(|message| {
            matches!(message, ModelMessage::User(text) | ModelMessage::System(text) if text.contains(SUMMARY_CONTENT) || text.contains("DERIVED_SECRET"))
        }), "invalid derived body leaked for {variant}");
        assert!(
            std::fs::read(&history_path)
                .unwrap()
                .starts_with(&history_before)
        );
    }
}

#[tokio::test]
async fn invalid_summary_does_not_mask_core_history_corruption() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-core-corrupt-{}", next_id()), false).await;
    let summary_path = history_path.parent().unwrap().join("summary.json");
    std::fs::write(&summary_path, vec![b'x'; 256 * 1024 + 1]).unwrap();

    let mut corrupt_history = history_before;
    corrupt_history.extend_from_slice(b"{\"broken\":true}\n");
    std::fs::write(&history_path, corrupt_history).unwrap();

    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    assert!(matches!(
        agent.open_session(session_id).await,
        Err(AgentError::Store)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn summary_symlink_is_ignored_but_core_symlink_is_not() {
    use std::os::unix::fs::symlink;

    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-symlink-{}", next_id()), false).await;
    let summary_path = history_path.parent().unwrap().join("summary.json");
    let summary_target = data_dir.join("summary-target.json");
    std::fs::copy(&summary_path, &summary_target).unwrap();
    std::fs::remove_file(&summary_path).unwrap();
    symlink(&summary_target, &summary_path).unwrap();

    let model = FakeModel::new("main", [ModelScript::Text("symlink fallback")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let turn = send_text(&mut agent, session_id, "symlink current user").await;
    wait_text(&agent, turn).await;
    let requests = model.requests();
    {
        let requests = requests.lock().unwrap();
        assert!(requests[0].messages().iter().any(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "covered user")
        ));
        assert!(!requests[0].messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT))
        }));
    }

    agent.close_session(session_id).await.unwrap();
    let record_path = history_path.parent().unwrap().join("session.json");
    let record_target = data_dir.join("record-target.json");
    std::fs::copy(&record_path, &record_target).unwrap();
    std::fs::remove_file(&record_path).unwrap();
    symlink(&record_target, &record_path).unwrap();
    assert!(matches!(
        agent.open_session(session_id).await,
        Err(AgentError::Store)
    ));
}

#[tokio::test]
async fn projected_summary_keeps_suffix_tool_exchange_and_current_user() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-tool-suffix-{}", next_id()), true).await;
    let model = FakeModel::new("main", [ModelScript::Text("tool suffix answer")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let turn = send_text(&mut agent, session_id, "tool current user").await;
    wait_text(&agent, turn).await;

    let requests = model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.tools().len(), 1);
    let messages = request.messages();
    let summary_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT)),
        )
        .expect("tool suffix request must contain the summary data message");
    let suffix_user_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "suffix user"),
        )
        .expect("tool suffix user must remain");
    let call_index = messages
        .iter()
        .position(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::ToolCall(call) if call.tool_call_id().as_str() == "suffix-read-call")))
        })
        .expect("tool call assistant must remain");
    let result_index = messages
        .iter()
        .position(|message| {
            matches!(message, ModelMessage::Tool { tool_call_id, .. } if tool_call_id.as_str() == "suffix-read-call")
        })
        .expect("tool result must remain");
    let current_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.as_ref() == "tool current user"),
        )
        .expect("current user must remain");
    assert!(summary_index < suffix_user_index);
    assert!(suffix_user_index < call_index);
    assert!(call_index < result_index);
    assert!(result_index < current_index);
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::System(text) if text.contains(SUMMARY_CONTENT))
    }));
}

pub(super) fn flatten_texts(items: &[HistoryItem]) -> Vec<String> {
    let mut texts = Vec::new();
    for item in items {
        match item {
            HistoryItem::User(user) => texts.push(user.input.as_text().to_owned()),
            HistoryItem::Assistant(assistant) => {
                for part in &assistant.content {
                    if let minicore_runtime::model::AssistantPart::Text(text) = part {
                        texts.push(text.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    texts
}
