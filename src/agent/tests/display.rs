use super::*;

#[tokio::test]
async fn replay_only_history_reads_redact_loaded_unloaded_and_append_failure_does_not_merge() {
    const OPAQUE: &str = "SYNTHETIC-READ-REPLAY-SECRET";
    for fail in [false, true] {
        let (data_dir, _guard) = fixture_dir(&format!("replay-read-{fail}-{}", next_id()));
        let (workspace, _workspace_guard) = workspace_file("replay-read-ws", "a.txt", b"hello");
        let replay=minicore_runtime::model::ProviderReplay::new("openai-responses-v1", json!({
            "endpoint":"http://localhost/v1/responses", "model":"provider-model",
            "output":[{"type":"reasoning","id":"rs_only","summary":[],"encrypted_content":OPAQUE}]
        })).unwrap();
        let model = FakeModel::new("main", [ModelScript::ReplayOnly(replay)]);
        let mut agent = open_agent(
            &data_dir,
            BTreeMap::from([("main".to_owned(), model)]),
            read_profile(),
        )
        .await;
        let info = create_session(&mut agent, &workspace).await;
        if fail {
            crate::store::APPEND_FAILURES.arm(info.session_id, true);
        }
        let turn = send_text(&mut agent, info.session_id, "opaque-only response").await;
        let result = wait_text(&agent, turn).await;
        assert_eq!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Completed
        );
        assert!(result.report.appended.iter().any(|item| matches!(item,HistoryItem::Assistant(a) if a.content.is_empty() && a.provider_replay.is_some())));
        let expected = if fail { 0 } else { 2 };
        assert_eq!(
            agent
                .history(GetHistory {
                    session_id: info.session_id,
                    offset: 0,
                    limit: 100
                })
                .unwrap()
                .total,
            expected
        );
        if fail {
            assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
            assert!(
                read_store_history(&data_dir, info.session_id)
                    .await
                    .is_empty()
            );
        } else {
            assert!(
                serde_json::to_string(&read_store_history(&data_dir, info.session_id).await)
                    .unwrap()
                    .contains(OPAQUE)
            );
            for loaded in [true, false] {
                if !loaded {
                    agent.close_session(info.session_id).await.unwrap();
                }
                let mut cursor = None;
                let mut captured_end = None;
                let mut history_revision = None;
                let mut indexes = std::collections::BTreeSet::new();
                loop {
                    let page = agent
                        .read_session(ReadSession {
                            view: crate::read::ReadView::Canonical,
                            projection_revision: None,
                            session_id: info.session_id,
                            cursor,
                            limit: 1,
                            max_bytes: Some(4096),
                            captured_end,
                            history_revision,
                        })
                        .await
                        .unwrap();
                    assert!(!serde_json::to_string(&page).unwrap().contains(OPAQUE));
                    for item in &page.items {
                        indexes.insert(item.index);
                    }
                    cursor = page.next_cursor;
                    captured_end = Some(page.captured_end);
                    history_revision = Some(page.history_revision);
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(indexes, std::collections::BTreeSet::from([0, 1]));
            }
        }
        agent.shutdown().await.unwrap();
    }
}

pub(super) fn display_read(session_id: SessionId) -> ReadSession {
    ReadSession {
        session_id,
        cursor: None,
        limit: 100,
        max_bytes: None,
        captured_end: None,
        history_revision: None,
        view: crate::read::ReadView::Display,
        projection_revision: None,
    }
}

pub(super) fn continue_display_read(request: &mut ReadSession, page: &crate::ReadSessionResult) {
    request.cursor = page.next_cursor;
    request.captured_end = Some(page.captured_end);
    request.history_revision = Some(page.history_revision.clone());
    request.projection_revision = Some(page.projection.as_ref().unwrap().revision.clone());
}

#[tokio::test]
async fn display_history_projects_summary_tool_metadata_and_original_indexes() {
    let (data_dir, _guard, session_id, history_path, before) =
        synthetic_summary_session(&format!("display-history-{}", next_id()), true).await;
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let cold = agent.read_session(display_read(session_id)).await.unwrap();
    assert!(!cold.session.loaded);
    assert!(agent.loaded_session(session_id).is_none());
    agent.open_session(session_id).await.unwrap();
    let page = agent.read_session(display_read(session_id)).await.unwrap();
    assert_eq!(cold.items, page.items);
    assert_eq!(cold.projection, page.projection);
    assert_eq!(cold.history_revision, page.history_revision);
    let projection = page.projection.as_ref().unwrap();
    assert_eq!(
        (
            projection.first_item,
            projection.covered_item_count,
            page.total
        ),
        (1, 2, 5)
    );
    assert_eq!(projection.covered_usage.loop_count, 1);
    assert_eq!(
        projection.covered_usage.last_loop_id.unwrap().to_string(),
        SUMMARY_COVERED_LOOP_ID
    );
    assert_eq!(projection.covered_usage.usage, Usage::new(1, 2, 0));
    assert_eq!(
        page.items.iter().map(|item| item.index).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    let values = page
        .items
        .iter()
        .map(|item| serde_json::from_str::<serde_json::Value>(&item.data).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values[0]["derived_summary"], true);
    assert_eq!(values[0]["item"]["data"]["content"], SUMMARY_CONTENT);
    assert_eq!(
        values[2]["tool_summaries"][0]["display"]["detail"],
        "suffix.txt"
    );
    assert_eq!(values[2]["tool_summaries"][0]["output_line_count"], 1);
    assert_eq!(values[2]["tool_summaries"][0]["count_state"], "exact");
    assert_eq!(
        values[2]["tool_summaries"][0]["tool_ref"]["session_id"],
        SUMMARY_SESSION_ID
    );
    assert!(
        values[2]["item"]["data"]["content"][0]["data"]
            .get("arguments")
            .is_none()
    );
    assert!(values[3]["item"]["data"].get("output").is_none());
    let all = page
        .items
        .iter()
        .map(|item| item.data.as_str())
        .collect::<String>();
    assert!(!all.contains("covered user"));
    assert!(!all.contains("suffix tool result"));
    assert!(!all.contains("expanded_input"));
    let mut raw = display_read(session_id);
    raw.view = crate::read::ReadView::Canonical;
    let raw = agent.read_session(raw).await.unwrap();
    assert!(raw.projection.is_none());
    assert_eq!(raw.items[0].index, 0);
    assert!(
        raw.items
            .iter()
            .any(|item| item.data.contains("covered user"))
    );
    assert!(
        raw.items
            .iter()
            .any(|item| item.data.contains("suffix tool result"))
    );
    assert_eq!(std::fs::read(history_path).unwrap(), before);
}

#[tokio::test]
async fn display_history_chunks_unicode_summary_and_rejects_summary_only_replacement() {
    let (data_dir, _guard, session_id, _, _) =
        synthetic_summary_session(&format!("display-chunks-{}", next_id()), true).await;
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    let state = agent.loaded_session(session_id).unwrap().compaction_state();
    let body = "中文🙂\\\"\n".repeat(1_000);
    state.publish(BoundedText::new(&body).unwrap(), 1, 2);
    let mut request = display_read(session_id);
    request.limit = 1;
    request.max_bytes = Some(2048);
    let first = agent.read_session(request.clone()).await.unwrap();
    assert_eq!(first.items.len(), 1);
    assert!(!first.items[0].complete);
    let original_revision = first.history_revision.clone();
    let mut assembled = first.items[0].data.clone();
    continue_display_read(&mut request, &first);
    while request.cursor.is_some_and(|cursor| cursor.item == 1) {
        let page = agent.read_session(request.clone()).await.unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= 2048);
        assert_eq!(page.items[0].index, 1);
        assembled.push_str(&page.items[0].data);
        continue_display_read(&mut request, &page);
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&assembled).unwrap()["item"]["data"]["content"],
        body
    );
    let page = agent.read_session(request.clone()).await.unwrap();
    assert_eq!(page.items[0].index, 2);
    state.publish(
        BoundedText::new("new summary, unchanged history").unwrap(),
        1,
        2,
    );
    assert!(matches!(
        agent.read_session(request).await,
        Err(AgentError::InvalidState)
    ));
    let fresh = agent.read_session(display_read(session_id)).await.unwrap();
    assert_eq!(fresh.history_revision, original_revision);
    assert_ne!(
        fresh.projection.unwrap().revision,
        first.projection.unwrap().revision
    );
}

#[tokio::test]
async fn display_history_invalid_summary_falls_back_and_never_crosses_valid_boundary() {
    let (data_dir, _guard, session_id, history_path, _) =
        synthetic_summary_session(&format!("display-fallback-{}", next_id()), false).await;
    std::fs::write(
        history_path.parent().unwrap().join("summary.json"),
        b"invalid summary",
    )
    .unwrap();
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let cold = agent.read_session(display_read(session_id)).await.unwrap();
    assert_eq!(cold.projection.as_ref().unwrap().covered_item_count, 0);
    assert_eq!(cold.items[0].index, 0);
    assert!(!cold.session.loaded);
    agent.open_session(session_id).await.unwrap();
    let page = agent.read_session(display_read(session_id)).await.unwrap();
    assert_eq!(page.projection.as_ref().unwrap().covered_item_count, 0);
    assert_eq!(page.items[0].index, 0);
    let state = agent.loaded_session(session_id).unwrap().compaction_state();
    state.publish(BoundedText::new("all loops covered").unwrap(), 2, 4);
    let page = agent.read_session(display_read(session_id)).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].index, 3);
    assert!(page.next_cursor.is_none());
    assert_eq!(
        page.projection.as_ref().unwrap().covered_usage.usage,
        Usage::new(2, 4, 0)
    );
    let mut before_boundary = display_read(session_id);
    continue_display_read(&mut before_boundary, &page);
    before_boundary.cursor = Some(crate::ReadCursor::start());
    assert!(matches!(
        agent.read_session(before_boundary).await,
        Err(AgentError::InvalidArguments)
    ));
}

#[tokio::test]
async fn cold_display_invalid_anchor_restores_cursor_and_limit_without_raw_tools() {
    for (field, value) in [
        ("sha256", json!("0".repeat(64))),
        ("prefix_bytes", json!(SUMMARY_PREFIX_BYTES - 1)),
        ("covered_item_count", json!(3)),
        ("covered_loop_count", json!(2)),
        ("last_loop_id", json!(SUMMARY_SUFFIX_LOOP_ID)),
    ] {
        let (data_dir, _guard, session_id, history_path, before) =
            synthetic_summary_session(&format!("cold-display-invalid-{field}-{}", next_id()), true)
                .await;
        let summary_path = history_path.parent().unwrap().join("summary.json");
        let mut summary: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&summary_path).unwrap()).unwrap();
        summary["source"][field] = value;
        let summary_bytes = serde_json::to_vec(&summary).unwrap();
        std::fs::write(&summary_path, &summary_bytes).unwrap();
        let model = FakeModel::new("main", []);
        let agent = open_agent(
            &data_dir,
            BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
            read_profile(),
        )
        .await;
        let mut request = display_read(session_id);
        request.limit = 1;
        let first = agent.read_session(request.clone()).await.unwrap();
        assert_eq!(
            first.projection.as_ref().unwrap().covered_item_count,
            0,
            "{field}"
        );
        assert_eq!(
            first.items.len(),
            1,
            "fallback must restore reserved summary slot"
        );
        assert_eq!(
            first.items[0].index, 0,
            "fallback must restore original cursor"
        );
        assert!(first.items[0].data.contains("covered user"));
        let mut all = first.items[0].data.clone();
        continue_display_read(&mut request, &first);
        while request.cursor.is_some() {
            let page = agent.read_session(request.clone()).await.unwrap();
            all.extend(page.items.iter().map(|item| item.data.as_str()));
            continue_display_read(&mut request, &page);
        }
        assert!(all.contains("tool_summaries"));
        assert!(!all.contains("suffix tool result"));
        assert!(!all.contains("arguments"));
        assert!(!all.contains("expanded_input"));
        assert!(!first.session.loaded);
        assert!(agent.loaded_session(session_id).is_none());
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read(&history_path).unwrap(), before);
        assert_eq!(std::fs::read(&summary_path).unwrap(), summary_bytes);
    }
}

#[tokio::test]
async fn cold_display_unicode_pins_missing_workspace_and_partial_tail_are_read_only() {
    let (data_dir, _guard, session_id, history_path, mut before) =
        synthetic_summary_session(&format!("cold-display-pins-{}", next_id()), true).await;
    let directory = history_path.parent().unwrap();
    let record_path = directory.join("session.json");
    let mut record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
    record["workspace"] = json!(data_dir.join("missing-workspace"));
    record["model"] = json!("unavailable-model");
    let record_bytes = serde_json::to_vec(&record).unwrap();
    std::fs::write(&record_path, &record_bytes).unwrap();
    let summary_path = directory.join("summary.json");
    let mut summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&summary_path).unwrap()).unwrap();
    let body = "中文🙂\\\"\n".repeat(1_000);
    summary["summary"] = json!(body);
    let summary_bytes = serde_json::to_vec(&summary).unwrap();
    std::fs::write(&summary_path, &summary_bytes).unwrap();
    before.extend_from_slice(b"{partial tail");
    std::fs::write(&history_path, &before).unwrap();
    let model = FakeModel::new("main", []);
    let agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut request = display_read(session_id);
    request.limit = 1;
    request.max_bytes = Some(2048);
    let first = agent.read_session(request.clone()).await.unwrap();
    assert!(first.trailing_incomplete);
    assert_eq!(first.session.model, "unavailable-model");
    assert!(!first.items[0].complete);
    let mut assembled = first.items[0].data.clone();
    continue_display_read(&mut request, &first);
    while request.cursor.is_some_and(|cursor| cursor.item == 1) {
        let page = agent.read_session(request.clone()).await.unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= 2048);
        assembled.push_str(&page.items[0].data);
        continue_display_read(&mut request, &page);
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&assembled).unwrap()["item"]["data"]["content"],
        body
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), before);
    assert_eq!(std::fs::read(&record_path).unwrap(), record_bytes);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_bytes);
    summary["summary"] = json!("replacement summary, identical source");
    std::fs::write(&summary_path, serde_json::to_vec(&summary).unwrap()).unwrap();
    assert!(matches!(
        agent.read_session(request.clone()).await,
        Err(AgentError::InvalidState)
    ));
    std::fs::write(&summary_path, &summary_bytes).unwrap();
    summary["source"]["sha256"] = json!("0".repeat(64));
    std::fs::write(&summary_path, serde_json::to_vec(&summary).unwrap()).unwrap();
    assert!(matches!(
        agent.read_session(request.clone()).await,
        Err(AgentError::InvalidState)
    ));
    std::fs::write(&summary_path, &summary_bytes).unwrap();
    let changed = String::from_utf8(before.clone())
        .unwrap()
        .replace("covered user", "changed user");
    std::fs::write(&history_path, changed).unwrap();
    assert!(matches!(
        agent.read_session(request).await,
        Err(AgentError::InvalidState)
    ));
    assert!(agent.loaded_session(session_id).is_none());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}
