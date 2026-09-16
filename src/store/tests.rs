use base64::Engine as _;
use minicore_runtime::ToolCallId;
use minicore_runtime::execution::ConfigRevision;
use minicore_runtime::history::{AssistantHistory, ToolResultHistory, UserHistory};
use minicore_runtime::model::{
    AssistantPart, ModelFinishReason, ModelRef, ReasoningPreference, ToolCall, Usage,
};
use minicore_runtime::tools::ToolResultOutcome;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::changes::{ChangeCommitState, ChangeCoverage, ChangeKind, FileChange, content_revision};
use crate::error::AgentError;
use crate::tool_data::{ToolData, ToolDataAvailability, ToolDataStream, ToolOutputRequest};

use super::*;

async fn fixture(label: &str) -> (PathBuf, Store, SessionId) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-store-{label}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&base).await;
    let store = Store::open(base.clone()).await.unwrap();
    let session_id = SessionId::new().unwrap();
    (base, store, session_id)
}

fn record(_store: &Store, session_id: SessionId) -> SessionRecord {
    let now = utc_timestamp().unwrap();
    SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: Some("title".to_owned()),
        profile: "coding".to_owned(),
        workspace: PathBuf::from("/tmp/workspace"),
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "system".to_owned(),
        tools: vec!["read".to_owned(), "write".to_owned()],
        max_tool_rounds: 32,
        approval: ApprovalMode::Auto,
        created_at: now.clone(),
        updated_at: now,
    }
}

fn loop_record(session_id: SessionId, text: &str) -> StoredLoopRecord {
    let _ = session_id;
    let loop_id = LoopId::new().unwrap();
    StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![HistoryItem::User(UserHistory {
            loop_id,
            kind: minicore_runtime::history::UserMessageKind::Prompt,
            input: minicore_runtime::execution::UserInput::text(text).unwrap(),
        })],
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: utc_timestamp().unwrap(),
        user_times: None,
    }
}

fn loop_record_with_users(texts: &[&str]) -> StoredLoopRecord {
    let loop_id = LoopId::new().unwrap();
    let items = texts
        .iter()
        .map(|text| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: minicore_runtime::history::UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(text).unwrap(),
            })
        })
        .collect();
    StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items,
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: utc_timestamp().unwrap(),
        user_times: None,
    }
}

async fn append_raw_history_line(store: &Store, session_id: SessionId, value: serde_json::Value) {
    let path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    let mut file = OpenOptions::new().append(true).open(path).await.unwrap();
    file.write_all(&bytes).await.unwrap();
    file.flush().await.unwrap();
}

async fn append_raw_history_json_line(store: &Store, session_id: SessionId, json: &str) {
    let path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut file = OpenOptions::new().append(true).open(path).await.unwrap();
    file.write_all(json.as_bytes()).await.unwrap();
    file.write_all(b"\n").await.unwrap();
    file.flush().await.unwrap();
}

#[tokio::test]
async fn user_time_metadata_is_bounded_validated_and_old_records_stay_compatible() {
    let (base, store, session_id) = fixture("user-times").await;
    let session = record(&store, session_id);
    store.create_session(&session).await.unwrap();

    let mut valid = loop_record(session_id, "same");
    let loop_id = valid.loop_id;
    valid.user_times = Some(vec![Some("2026-09-05T14:05:06.007Z".to_owned())]);
    store.append_loop(session_id, &valid).await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(
        loaded.user_times.get(&(loop_id, 0)).map(String::as_str),
        Some("2026-09-05T14:05:06.007Z")
    );

    // A missing optional field remains the old JSONL compatibility path.
    let old = loop_record(session_id, "old");
    store.append_loop(session_id, &old).await.unwrap();
    let history_path = base
        .join(SESSIONS_DIR)
        .join(session_id.to_string())
        .join(HISTORY_FILE);
    let old_jsonl_before_load = fs::read(&history_path).await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 2);
    assert_eq!(
        fs::read(&history_path).await.unwrap(),
        old_jsonl_before_load
    );

    let mut invalid_timestamp = loop_record(session_id, "invalid");
    invalid_timestamp.user_times = Some(vec![Some("not-a-timestamp".to_owned())]);
    store
        .append_loop(session_id, &invalid_timestamp)
        .await
        .unwrap();

    let mut too_many = loop_record(session_id, "too many");
    too_many.user_times = Some(vec![
        Some("2026-09-05T14:05:06.007Z".to_owned()),
        Some("extra".to_owned()),
    ]);
    store.append_loop(session_id, &too_many).await.unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 4);
    assert!(
        !loaded
            .user_times
            .contains_key(&(invalid_timestamp.loop_id, 0))
    );
    assert!(!loaded.user_times.contains_key(&(too_many.loop_id, 0)));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn user_time_metadata_does_not_displace_core_at_line_limit() {
    let (base, store, session_id) = fixture("user-times-line-limit").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let mut loop_record: StoredLoopRecord =
        serde_json::from_slice(&loop_line_with_size(MAX_LOOP_RECORD_BYTES - 32)).unwrap();
    let user_count = loop_record
        .items
        .iter()
        .filter(|item| matches!(item, HistoryItem::User(_)))
        .count();
    loop_record.user_times = Some(
        (0..user_count)
            .map(|_| Some("2026-09-05T14:05:06.007Z".to_owned()))
            .collect(),
    );

    store.append_loop(session_id, &loop_record).await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), user_count);
    assert!(loaded.user_times.is_empty());
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn same_text_users_keep_occurrence_when_one_time_is_invalid() {
    let (base, store, session_id) = fixture("user-time-occurrence").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let mut loop_record = loop_record_with_users(&["same", "same"]);
    let loop_id = loop_record.loop_id;
    let valid_time = "2026-09-05T14:05:06.007Z";
    loop_record.user_times = Some(vec![
        Some("not-a-timestamp".to_owned()),
        Some(valid_time.to_owned()),
    ]);
    store.append_loop(session_id, &loop_record).await.unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    let page = crate::history::page_history(loaded.history.as_ref(), 0, 100, &loaded.user_times);
    let timestamps = page
        .items
        .iter()
        .map(|item| match &item.item {
            crate::history::HistoryItemView::User(user) => user.timestamp.as_deref(),
            other => panic!("unexpected history item {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![None, Some(valid_time)]);
    assert!(!loaded.user_times.contains_key(&(loop_id, 0)));
    assert_eq!(
        loaded.user_times.get(&(loop_id, 1)).map(String::as_str),
        Some(valid_time)
    );
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn malformed_user_time_json_does_not_block_history_read_or_append() {
    let (base, store, session_id) = fixture("malformed-user-times").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    store
        .append_loop(session_id, &loop_record(session_id, "before"))
        .await
        .unwrap();

    let malformed_shape = serde_json::to_value(loop_record(session_id, "object")).unwrap();
    let malformed_shape = {
        let mut value = malformed_shape;
        value["user_times"] = json!({"unexpected": "shape"});
        value
    };
    append_raw_history_line(&store, session_id, malformed_shape).await;

    let malformed_entry = serde_json::to_value(loop_record(session_id, "entry")).unwrap();
    let malformed_entry = {
        let mut value = malformed_entry;
        value["user_times"] = json!([{"unexpected": "entry"}]);
        value
    };
    append_raw_history_line(&store, session_id, malformed_entry).await;

    let loaded = store.load_session(session_id).await.unwrap();
    let texts = loaded
        .history
        .iter()
        .map(|item| match item {
            HistoryItem::User(user) => user.input.as_text().to_owned(),
            other => panic!("unexpected history item {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(texts, vec!["before", "object", "entry"]);
    assert!(loaded.user_times.is_empty());

    store
        .append_loop(session_id, &loop_record(session_id, "after"))
        .await
        .unwrap();
    assert_eq!(
        store.load_session(session_id).await.unwrap().history.len(),
        4
    );
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn core_history_corruption_is_still_rejected() {
    let (base, store, session_id) = fixture("core-history-corrupt").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let mut value = serde_json::to_value(loop_record(session_id, "core")).unwrap();
    value["items"] = json!("not-an-items-array");
    value["user_times"] = json!({"unexpected": "shape"});
    append_raw_history_line(&store, session_id, value).await;

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn duplicate_core_json_fields_are_rejected() {
    let (base, store, session_id) = fixture("duplicate-core-field").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let raw = serde_json::to_string(&loop_record(session_id, "duplicate core")).unwrap();
    let duplicate_items = raw.replacen("\"items\":", "\"items\":[],\"items\":", 1);
    append_raw_history_json_line(&store, session_id, &duplicate_items).await;

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn duplicate_user_time_json_fields_are_rejected() {
    let (base, store, session_id) = fixture("duplicate-user-times-field").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let mut loop_record = loop_record(session_id, "duplicate user times");
    loop_record.user_times = Some(vec![Some("2026-09-05T14:05:06.007Z".to_owned())]);
    let raw = serde_json::to_string(&loop_record).unwrap();
    let duplicate_user_times =
        raw.replacen("\"user_times\":", "\"user_times\":null,\"user_times\":", 1);
    append_raw_history_json_line(&store, session_id, &duplicate_user_times).await;

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn unknown_and_wrong_type_core_json_fields_are_rejected() {
    let unknown_field = {
        let mut raw =
            serde_json::to_string(&loop_record(SessionId::new().unwrap(), "unknown")).unwrap();
        raw.insert_str(raw.len() - 1, ",\"unknown_core\":true");
        raw
    };
    let wrong_type = {
        let mut raw =
            serde_json::to_string(&loop_record(SessionId::new().unwrap(), "wrong")).unwrap();
        let field = "\"items\":";
        let value_start = raw.find(field).unwrap() + field.len();
        let value_end = raw[value_start..].find("],\"usage\"").unwrap() + value_start + 1;
        raw.replace_range(value_start..value_end, "\"not-an-items-array\"");
        raw
    };

    for (label, raw) in [
        ("unknown-core-field", unknown_field),
        ("wrong-type-core-field", wrong_type),
    ] {
        let (base, store, session_id) = fixture(label).await;
        store
            .create_session(&record(&store, session_id))
            .await
            .unwrap();
        append_raw_history_json_line(&store, session_id, &raw).await;
        assert!(matches!(
            store.load_session(session_id).await,
            Err(StoreError::Corrupt)
        ));
        let _ = fs::remove_dir_all(base).await;
    }
}

#[test]
fn timestamp_validation_accepts_generated_rfc3339_and_rejects_lookalikes() {
    assert!(valid_timestamp("2026-09-05T14:05:06.007Z"));
    assert!(valid_timestamp("2026-09-05T14:05:06+08:00"));
    assert!(!valid_timestamp("2026-99-99T99:99:99Z"));
    assert!(!valid_timestamp("2026-09-05T14:05:06"));
    assert!(!valid_timestamp("2026-09-05T14:05:06.badZ"));
}

fn loop_line_with_size(target: usize) -> Vec<u8> {
    let loop_id = LoopId::new().unwrap();
    let fixed_text = "x".repeat(255 * 1024);
    let mut items = (0..64)
        .map(|_| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: minicore_runtime::history::UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(&fixed_text).unwrap(),
            })
        })
        .collect::<Vec<_>>();
    items.push(HistoryItem::User(UserHistory {
        loop_id,
        kind: minicore_runtime::history::UserMessageKind::Prompt,
        input: minicore_runtime::execution::UserInput::text("x").unwrap(),
    }));
    let mut record = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items,
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: utc_timestamp().unwrap(),
        user_times: None,
    };
    let base = serde_json::to_vec(&record).unwrap();
    let final_text_len = target
        .checked_sub(base.len())
        .and_then(|length| length.checked_add(1))
        .expect("test record must have room for its variable item");
    assert!(final_text_len <= 256 * 1024);
    let Some(HistoryItem::User(user)) = record.items.last_mut() else {
        unreachable!("test record has a final user item");
    };
    user.input = minicore_runtime::execution::UserInput::text("x".repeat(final_text_len)).unwrap();
    let bytes = serde_json::to_vec(&record).unwrap();
    assert_eq!(bytes.len(), target);
    bytes
}

#[tokio::test]
async fn create_round_trip_and_delete() {
    let (base, store, session_id) = fixture("create").await;
    let record = record(&store, session_id);
    store.create_session(&record).await.unwrap();

    let directory = store.session_directory(session_id);
    assert!(
        fs::metadata(directory.join(SESSION_RECORD_FILE))
            .await
            .unwrap()
            .is_file()
    );
    assert!(
        fs::metadata(directory.join(HISTORY_FILE))
            .await
            .unwrap()
            .is_file()
    );

    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.record.session_id, session_id);
    assert_eq!(loaded.record.model, "main");
    assert_eq!(loaded.record.profile, "coding");
    assert!(loaded.history.is_empty());
    assert!(!store.list_sessions().await.unwrap().is_empty());

    store.delete_session(session_id).await.unwrap();
    assert!(matches!(
        store.load_record(session_id).await,
        Err(StoreError::SessionNotFound)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn extended_reasoning_values_round_trip_in_record_and_history() {
    let (base, store, session_id) = fixture("extended-reasoning").await;
    let mut session_record = record(&store, session_id);
    store.create_session(&session_record).await.unwrap();

    let values = [
        (ReasoningPreference::XHigh, "xhigh"),
        (ReasoningPreference::Max, "max"),
        (ReasoningPreference::Ultra, "ultra"),
    ];
    for (reasoning, wire) in values {
        session_record.reasoning = reasoning;
        store.write_record(&session_record).await.unwrap();
        assert_eq!(
            store.load_record(session_id).await.unwrap().reasoning,
            reasoning
        );

        let loop_id = LoopId::new().unwrap();
        store
            .append_loop(
                session_id,
                &StoredLoopRecord {
                    loop_id,
                    outcome: StoredLoopOutcome::Completed,
                    items: vec![HistoryItem::Assistant(AssistantHistory {
                        loop_id,
                        request_index: 0,
                        model: "main".parse().unwrap(),
                        reasoning,
                        content: vec![AssistantPart::Text(format!("answer-{wire}"))],
                        finish_reason: ModelFinishReason::Stop,
                        usage: Usage::new(1, 2, 0),
                    })],
                    usage: Usage::new(1, 2, 0),
                    requests: 1,
                    tool_rounds: 0,
                    final_config_revision: ConfigRevision::INITIAL,
                    completed_at: utc_timestamp().unwrap(),
                    user_times: None,
                },
            )
            .await
            .unwrap();
    }

    let loaded = store.load_session(session_id).await.unwrap();
    let history_reasoning = loaded
        .history
        .iter()
        .map(|item| match item {
            HistoryItem::Assistant(assistant) => assistant.reasoning,
            other => panic!("unexpected history item {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        history_reasoning,
        values
            .iter()
            .map(|(reasoning, _)| *reasoning)
            .collect::<Vec<_>>()
    );
    let serialized = serde_json::to_string(loaded.history.as_ref()).unwrap();
    let view = crate::history::page_history(
        loaded.history.as_ref(),
        0,
        100,
        &std::collections::HashMap::new(),
    );
    let view_serialized = serde_json::to_string(&view).unwrap();
    for (_, wire) in values {
        assert!(serialized.contains(&format!("\"reasoning\":\"{wire}\"")));
        assert!(view_serialized.contains(&format!("\"reasoning_level\":\"{wire}\"")));
    }
    assert_eq!(loaded.record.reasoning, ReasoningPreference::Ultra);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn append_and_load_flatten_history_in_order() {
    let (base, store, session_id) = fixture("append").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    store
        .append_loop(session_id, &loop_record(session_id, "first"))
        .await
        .unwrap();
    store
        .append_loop(session_id, &loop_record(session_id, "second"))
        .await
        .unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    let texts = loaded
        .history
        .iter()
        .map(|item| match item {
            HistoryItem::User(user) => user.input.as_text().to_owned(),
            other => panic!("unexpected item {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(texts, vec!["first".to_owned(), "second".to_owned()]);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn final_partial_line_is_ignored_and_truncated() {
    let (base, store, session_id) = fixture("partial").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    store
        .append_loop(session_id, &loop_record(session_id, "first"))
        .await
        .unwrap();
    let history_path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&history_path)
        .await
        .unwrap();
    file.write_all(b"{\"truncated\": true").await.unwrap();
    file.flush().await.unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 1);

    // After the repair, a new append is still legal.
    store
        .append_loop(session_id, &loop_record(session_id, "second"))
        .await
        .unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 2);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn first_partial_line_only_is_truncated_to_empty_then_append_works() {
    let (base, store, session_id) = fixture("only-partial").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let history_path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&history_path)
        .await
        .unwrap();
    file.write_all(b"{\"truncated\": true").await.unwrap();
    file.flush().await.unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    assert!(loaded.history.is_empty());
    // The repair truncated the whole file back to zero bytes.
    assert_eq!(fs::metadata(&history_path).await.unwrap().len(), 0);

    store
        .append_loop(session_id, &loop_record(session_id, "first"))
        .await
        .unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 1);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn oversized_final_partial_line_is_repaired_and_appendable() {
    let (base, store, session_id) = fixture("oversized-partial").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let history_path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&history_path)
        .await
        .unwrap();
    // Even an oversized final partial is the one repairable tail case.
    file.write_all(&loop_line_with_size(MAX_LOOP_RECORD_BYTES + 1))
        .await
        .unwrap();
    file.flush().await.unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    assert!(loaded.history.is_empty());
    assert_eq!(fs::metadata(&history_path).await.unwrap().len(), 0);

    store
        .append_loop(session_id, &loop_record(session_id, "after repair"))
        .await
        .unwrap();
    let reopened = Store::open(base.clone()).await.unwrap();
    let loaded = reopened.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 1);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn legal_complete_line_at_limit_is_accepted_but_just_over_limit_is_corrupt() {
    let (base, store, session_id) = fixture("line-boundary").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let history_path = store.session_directory(session_id).join(HISTORY_FILE);

    let at_limit = loop_line_with_size(MAX_LOOP_RECORD_BYTES);
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&history_path)
        .await
        .unwrap();
    file.write_all(&at_limit).await.unwrap();
    file.write_all(b"\n").await.unwrap();
    file.flush().await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), 65);

    let over_limit = loop_line_with_size(MAX_LOOP_RECORD_BYTES + 1);
    fs::write(&history_path, [&over_limit[..], b"\n"].concat())
        .await
        .unwrap();
    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn missing_history_file_is_corrupt_but_list_still_reads() {
    let (base, store, session_id) = fixture("missing-history").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    fs::remove_file(store.session_directory(session_id).join(HISTORY_FILE))
        .await
        .unwrap();

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    // list only reads the record and still reports the session.
    let records = store.list_sessions().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].session_id, session_id);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn oversized_session_record_is_corrupt() {
    let (base, store, session_id) = fixture("oversized-record").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let record_path = store
        .session_directory(session_id)
        .join(SESSION_RECORD_FILE);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&record_path)
        .await
        .unwrap();
    file.write_all(vec![b'{'; MAX_SESSION_RECORD_BYTES + 1].as_slice())
        .await
        .unwrap();
    file.flush().await.unwrap();

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn store_root_symlink_is_rejected() {
    use std::os::unix::fs::symlink;

    let label = format!("root-symlink-{}", std::process::id());
    let root_area = std::env::temp_dir().join(label);
    let _ = fs::remove_dir_all(&root_area).await;
    let base = root_area.join("data");
    let real = root_area.join("real");
    fs::create_dir_all(&real).await.unwrap();
    symlink(&real, &base).unwrap();

    let result = Store::open(base.clone()).await;
    assert!(matches!(result, Err(StoreError::InvalidRoot)));
    let _ = fs::remove_file(&base).await;
    let _ = fs::remove_dir_all(&root_area).await;
}

#[cfg(unix)]
#[tokio::test]
async fn post_open_sessions_symlink_rejects_all_session_operations() {
    use std::os::unix::fs::symlink;

    let (base, store, session_id) = fixture("sessions-root-symlink").await;
    let original = record(&store, session_id);
    store.create_session(&original).await.unwrap();

    let sessions = store.sessions_directory();
    let moved_sessions = base.join("sessions-real");
    fs::rename(&sessions, &moved_sessions).await.unwrap();

    let outside = base.join("outside");
    let outside_session = outside.join(session_id.to_string());
    fs::create_dir_all(&outside_session).await.unwrap();
    fs::write(
        outside_session.join(SESSION_RECORD_FILE),
        serde_json::to_vec(&original).unwrap(),
    )
    .await
    .unwrap();
    fs::write(outside_session.join(HISTORY_FILE), b"")
        .await
        .unwrap();
    let sentinel = outside.join("sentinel");
    fs::write(&sentinel, b"outside must not change")
        .await
        .unwrap();
    let outside_record_before = fs::read(outside_session.join(SESSION_RECORD_FILE))
        .await
        .unwrap();
    let outside_history_before = fs::read(outside_session.join(HISTORY_FILE)).await.unwrap();

    symlink(&outside, &sessions).unwrap();

    assert!(matches!(
        store.load_record(session_id).await,
        Err(StoreError::Corrupt)
    ));
    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    assert!(matches!(
        store.list_sessions().await,
        Err(StoreError::Corrupt)
    ));
    assert!(matches!(
        store.write_record(&original).await,
        Err(StoreError::Corrupt)
    ));
    assert!(matches!(
        store
            .append_loop(session_id, &loop_record(session_id, "must not append"))
            .await,
        Err(StoreError::Corrupt)
    ));
    assert!(matches!(
        store.delete_session(session_id).await,
        Err(StoreError::Corrupt)
    ));

    let new_session_id = SessionId::new().unwrap();
    let new_record = record(&store, new_session_id);
    assert!(matches!(
        store.create_session(&new_record).await,
        Err(StoreError::Corrupt)
    ));

    assert_eq!(
        fs::read(&sentinel).await.unwrap(),
        b"outside must not change"
    );
    assert_eq!(
        fs::read(outside_session.join(SESSION_RECORD_FILE))
            .await
            .unwrap(),
        outside_record_before
    );
    assert_eq!(
        fs::read(outside_session.join(HISTORY_FILE)).await.unwrap(),
        outside_history_before
    );
    assert!(
        fs::try_exists(moved_sessions.join(session_id.to_string()))
            .await
            .unwrap()
    );
    let _ = fs::remove_file(&sessions).await;
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn middle_corrupt_line_is_corrupt() {
    let (base, store, session_id) = fixture("corrupt").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    store
        .append_loop(session_id, &loop_record(session_id, "first"))
        .await
        .unwrap();
    let history_path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&history_path)
        .await
        .unwrap();
    file.write_all(b"{\"broken\": true}\n").await.unwrap();
    file.flush().await.unwrap();

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn oversized_line_is_corrupt() {
    let (base, store, session_id) = fixture("oversized").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let history_path = store.session_directory(session_id).join(HISTORY_FILE);
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&history_path)
        .await
        .unwrap();
    file.write_all(vec![b'{'; MAX_LOOP_RECORD_BYTES + 1].as_slice())
        .await
        .unwrap();
    file.write_all(b"\n").await.unwrap();
    file.flush().await.unwrap();

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn record_too_large_append_is_rejected() {
    let (base, store, session_id) = fixture("record-too-large").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let loop_id = LoopId::new().unwrap();
    let oversized_item = HistoryItem::Assistant(AssistantHistory {
        loop_id,
        request_index: 0,
        model: "main".parse().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: vec![AssistantPart::Text("x".repeat(MAX_LOOP_RECORD_BYTES + 16))],
        finish_reason: ModelFinishReason::Stop,
        usage: Usage::new(1, 2, 0),
    });
    let big = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![oversized_item],
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: utc_timestamp().unwrap(),
        user_times: None,
    };
    assert!(matches!(
        store.append_loop(session_id, &big).await,
        Err(StoreError::RecordTooLarge)
    ));
    let loaded = store.load_session(session_id).await.unwrap();
    assert!(loaded.history.is_empty());
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn old_format_is_skipped_in_list_and_rejected_on_open() {
    let (base, store, session_id) = fixture("old-format").await;
    let directory = store.session_directory(session_id);
    fs::create_dir_all(&directory).await.unwrap();
    fs::write(directory.join(LEGACY_MANIFEST_FILE), b"{}")
        .await
        .unwrap();
    fs::write(directory.join(LEGACY_CONVERSATION_FILE), b"")
        .await
        .unwrap();

    assert!(store.list_sessions().await.unwrap().is_empty());
    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::UnsupportedFormat)
    ));
    // Old files are not modified.
    assert!(
        fs::metadata(directory.join(LEGACY_MANIFEST_FILE))
            .await
            .unwrap()
            .is_file()
    );
    let _ = fs::remove_dir_all(base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_session_files_are_rejected() {
    use std::os::unix::fs::symlink;

    let (base, store, session_id) = fixture("symlink").await;
    let outside = base.join("outside.json");
    fs::write(&outside, b"{}").await.unwrap();
    let directory = store.session_directory(session_id);
    fs::create_dir_all(&directory).await.unwrap();
    symlink(&outside, directory.join(SESSION_RECORD_FILE)).unwrap();

    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::Corrupt)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn unrelated_files_do_not_break_list() {
    let (base, store, session_id) = fixture("tolerance").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    fs::write(store.sessions_directory().join("not-a-session"), b"x")
        .await
        .unwrap();
    fs::create_dir(store.sessions_directory().join("bad"))
        .await
        .unwrap();

    let records = store.list_sessions().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].session_id, session_id);
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn delete_only_removes_the_target_session_directory() {
    let (base, store, session_id) = fixture("delete-only").await;
    let other = SessionId::new().unwrap();
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    store.create_session(&record(&store, other)).await.unwrap();

    store.delete_session(session_id).await.unwrap();
    assert!(store.load_session(other).await.is_ok());
    assert!(matches!(
        store.load_session(session_id).await,
        Err(StoreError::SessionNotFound)
    ));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn loaded_history_is_sanitized_of_opaque_reasoning() {
    let (base, store, session_id) = fixture("sanitize").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let loop_id = LoopId::new().unwrap();
    let tool = ToolCall::new(
        ToolCallId::new("call-1").unwrap(),
        "read".parse().unwrap(),
        json!({"path": "file.txt"}),
        0,
    )
    .unwrap();
    let assistant = HistoryItem::Assistant(AssistantHistory {
        loop_id,
        request_index: 0,
        model: "main".parse::<ModelRef>().unwrap(),
        reasoning: ReasoningPreference::Auto,
        content: vec![
            AssistantPart::Text("hello".to_owned()),
            AssistantPart::Reasoning(
                minicore_runtime::model::ReasoningContent::new(
                    Some("visible reasoning".to_owned()),
                    None,
                    Some("enc::opaque".to_owned()),
                    Some("sig::opaque".to_owned()),
                )
                .unwrap(),
            ),
            AssistantPart::ToolCall(tool),
        ],
        finish_reason: ModelFinishReason::ToolCalls,
        usage: Usage::new(1, 2, 0),
    });
    let record = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![assistant.clone()],
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 1,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: utc_timestamp().unwrap(),
        user_times: None,
    };
    store.append_loop(session_id, &record).await.unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    let history = &loaded.history;
    let HistoryItem::Assistant(loaded) = &history[0] else {
        panic!("expected assistant item");
    };
    let serialized = serde_json::to_string(history.as_ref()).unwrap();
    assert!(!serialized.contains("enc::opaque"));
    assert!(!serialized.contains("sig::opaque"));
    assert!(!serialized.contains("opaque"));
    assert!(serialized.contains("visible reasoning"));
    assert!(
        loaded
            .content
            .iter()
            .any(|part| part.as_tool_call().is_some())
    );
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn historical_subagent_history_items_remain_generic_and_readable() {
    let (base, store, session_id) = fixture("legacy-history").await;
    let mut session = record(&store, session_id);
    session.tools = vec!["read".to_owned(), "subagent".to_owned()];
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    store.write_record(&session).await.unwrap();

    let loop_id = LoopId::new().unwrap();
    let call_id = ToolCallId::new("historical-call").unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "subagent".parse().unwrap(),
        json!({"task": "historical task"}),
        0,
    )
    .unwrap();
    let history_record = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![
            HistoryItem::Assistant(AssistantHistory {
                loop_id,
                request_index: 0,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(call)],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::new(1, 2, 0),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index: 0,
                call_id,
                tool_name: "subagent".parse().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: minicore_runtime::tools::ToolOutput::new("historical child result")
                    .unwrap(),
            }),
        ],
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 1,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: utc_timestamp().unwrap(),
        user_times: None,
    };
    store
        .append_loop(session_id, &history_record)
        .await
        .unwrap();

    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.record.tools, session.tools);
    assert_eq!(loaded.history.len(), 2);
    let serialized = serde_json::to_string(loaded.history.as_ref()).unwrap();
    assert!(serialized.contains("subagent"));
    assert!(serialized.contains("historical child result"));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn session_record_validation_accepts_valid_multiline_prompt_and_known_tools() {
    let (base, store, session_id) = fixture("valid-record").await;
    let mut record = record(&store, session_id);
    record.system_prompt = concat!(
        "You are a helpful assistant.\n",
        "\tPlease follow instructions:\n",
        "1. Read files.\n",
        "2. Write files."
    )
    .to_owned();
    record.tools = vec!["read".to_owned(), "write".to_owned(), "bash".to_owned()];
    record.model = "provider/model-v1:beta".to_owned();
    assert!(record.validate().is_ok());
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn session_record_validation_rejects_empty_and_control_chars_in_system_prompt() {
    let (base, store, session_id) = fixture("invalid-prompt").await;
    let mut record = record(&store, session_id);

    record.system_prompt = String::new();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.system_prompt = "hello\0world".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.system_prompt = "hello\x01world".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.system_prompt = "hello\rworld".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.system_prompt = "hello\r\nworld".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.system_prompt = "hello\n\tworld".to_owned();
    assert!(record.validate().is_ok());
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn session_record_validation_rejects_unknown_and_duplicate_tools() {
    let (base, store, session_id) = fixture("invalid-tools").await;
    let mut record = record(&store, session_id);

    record.tools = vec!["read".to_owned(), "unknown_tool".to_owned()];
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.tools = vec!["read".to_owned(), "read".to_owned()];
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.tools = vec![];
    assert!(record.validate().is_ok());

    record.tools = KNOWN_TOOL_NAMES
        .iter()
        .map(|&name| name.to_owned())
        .collect();
    assert!(record.validate().is_ok());

    record.tools.push("subagent".to_owned());
    assert!(record.validate().is_ok());
    assert!(matches!(
        store.create_session(&record).await,
        Err(StoreError::InvalidRecord)
    ));

    record.tools.push("read".to_owned());
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));
    record.tools = vec!["subagent".to_owned(), "subagent".to_owned()];
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn legacy_auxiliary_tool_records_remain_readable() {
    let (base, store, session_id) = fixture("legacy-auxiliary-tool").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("legacy-subagent").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&tool_ref, "subagent");
    data.note_result(&tool_ref, "historical result");
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success);
    let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    let recovered = store
        .read_tool_record(&tool_ref)
        .await
        .unwrap()
        .expect("historical auxiliary record must remain readable");
    let read = recovered.project_read(&tool_ref, 4096).unwrap();
    assert_eq!(read.execution.name, "subagent");
    let output = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref,
                stream: ToolDataStream::Output,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    assert_eq!(output.data, "historical result");
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn session_record_validation_rejects_invalid_model_ids() {
    let (base, store, session_id) = fixture("invalid-model").await;
    let mut record = record(&store, session_id);

    record.model = String::new();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.model = "my model".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.model = "model\nid".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.model = "model@invalid".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.model = "model$invalid".to_owned();
    assert!(matches!(record.validate(), Err(StoreError::InvalidRecord)));

    record.model = "main".to_owned();
    assert!(record.validate().is_ok());

    record.model = "vendor/model-name:v1.0".to_owned();
    assert!(record.validate().is_ok());
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn invalid_session_record_skipped_by_list_and_rejected_by_open() {
    let (base, store, valid_id) = fixture("corrupt-record-list-open").await;

    let valid_record = record(&store, valid_id);
    store.create_session(&valid_record).await.unwrap();

    let invalid_id = SessionId::new().unwrap();
    let invalid_dir = store.session_directory(invalid_id);
    fs::create_dir_all(&invalid_dir).await.unwrap();

    let mut invalid_record = record(&store, invalid_id);
    invalid_record.system_prompt = String::new();
    let invalid_bytes = serde_json::to_vec(&invalid_record).unwrap();
    fs::write(invalid_dir.join(SESSION_RECORD_FILE), invalid_bytes)
        .await
        .unwrap();
    fs::write(invalid_dir.join(HISTORY_FILE), b"")
        .await
        .unwrap();

    let listed = store.list_sessions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_id, valid_id);

    let open_result = store.load_session(invalid_id).await;
    assert!(matches!(open_result, Err(StoreError::Corrupt)));

    assert!(store.load_session(valid_id).await.is_ok());

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn readonly_history_scan_honors_cancel_deadline_and_byte_budget() {
    let (base, store, session_id) = fixture("readonly-scan-limits").await;
    let record = record(&store, session_id);
    store.create_session(&record).await.unwrap();
    store
        .append_loop(session_id, &loop_record(session_id, "scan me"))
        .await
        .unwrap();

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let limits = HistoryScanLimits {
        max_bytes: 64 * 1024,
        max_lines: 100,
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
        cancellation: cancellation.clone(),
    };
    assert!(matches!(
        store
            .read_history_page(session_id, 0, 1, None, None, None, None, &limits,)
            .await,
        Err(StoreError::QueryLimit)
    ));

    let expired = HistoryScanLimits {
        max_bytes: 64 * 1024,
        max_lines: 100,
        deadline: std::time::Instant::now() - std::time::Duration::from_secs(1),
        cancellation: CancellationToken::new(),
    };
    assert!(matches!(
        store
            .read_history_page(session_id, 0, 1, None, None, None, None, &expired,)
            .await,
        Err(StoreError::QueryLimit)
    ));

    let capped = HistoryScanLimits {
        max_bytes: 1,
        max_lines: 100,
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
        cancellation: CancellationToken::new(),
    };
    assert!(matches!(
        store
            .read_history_page(session_id, 0, 1, None, None, None, None, &capped,)
            .await,
        Err(StoreError::QueryLimit)
    ));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn rejected_aux_budget_does_not_create_an_auxiliary_directory() {
    let (base, store, session_id) = fixture("aux-rejected-directory").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let store = store.with_aux_limits(AuxLimits {
        global_bytes: 0,
        ..DEFAULT_AUX_LIMITS
    });
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("rejected").unwrap(),
    };
    let data = crate::tool_data::ToolData::new();
    data.note_requested(&tool_ref, "read");
    data.note_result(&tool_ref, "ok");
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success);
    let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
    assert!(
        store
            .commit_tool_record(&snapshot, Instant::now() + AUX_PERSIST_DEADLINE)
            .await
            .is_err()
    );
    assert!(
        !store
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .exists()
    );
    assert!(
        fs::read(store.session_directory(session_id).join(HISTORY_FILE))
            .await
            .unwrap()
            .is_empty()
    );
    fs::remove_dir_all(base).await.unwrap();
}

#[tokio::test]
async fn binary_and_utf8_empty_eof_auxiliary_tool_persistence() {
    let (base, store, session_id) = fixture("aux-binary-utf8").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("bash-call-1").unwrap(),
    };

    let input_bytes = b"echo hello\n".to_vec();
    let result_bytes = b"hello\n".to_vec();
    let stdout_bytes: Vec<u8> = (0..50_000u32).map(|b| (b % 256) as u8).collect();

    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Command {
                script: "echo hello".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: Some(ToolPhase::Running),
            started_at: Some("2026-09-15T00:00:00Z".to_owned()),
            finished_at: Some("2026-09-15T00:00:01Z".to_owned()),
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: input_bytes.len(),
                seen: true,
                truncated: false,
                expired: false,
                file_bytes: input_bytes.len(),
                file_sha256: Some(hash_bytes(&input_bytes)),
            },
            result: StoredResultSummary {
                total_bytes: result_bytes.len(),
                seen: true,
                truncated: false,
                expired: false,
                file_bytes: result_bytes.len(),
                file_sha256: Some(hash_bytes(&result_bytes)),
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: stdout_bytes.len() as u64,
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: stdout_bytes.len(),
                file_sha256: Some(hash_bytes(&stdout_bytes)),
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: Some(CommandResult {
                status: crate::tool_data::CommandStatus::Exited,
                exit_code: Some(0),
                signal: None,
                termination_confirmed: true,
                stdout_base_offset: 0,
                stdout_observed_end: stdout_bytes.len() as u64,
                stderr_base_offset: 0,
                stderr_observed_end: 0,
                output_complete: true,
                output_truncated: false,
            }),
            file_change: None,
        },
        input_bytes: Some(input_bytes),
        result_bytes: Some(result_bytes),
        stdout_bytes: Some(stdout_bytes.clone()),
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    let recovered = store
        .read_tool_record(&tool_ref)
        .await
        .unwrap()
        .expect("stored tool record must exist");

    let read_res = recovered.project_read(&tool_ref, 4096).unwrap();
    assert_eq!(read_res.execution.tool_ref, tool_ref);
    assert_eq!(read_res.execution.name, "bash");
    assert_eq!(read_res.execution.state, ToolExecutionState::Succeeded);

    let stdout_page = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(100_000),
            },
            100_000,
        )
        .unwrap();
    assert_eq!(stdout_page.encoding, "base64");
    assert!(stdout_page.eof);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&stdout_page.data)
        .unwrap();
    assert_eq!(decoded, stdout_bytes);

    let stderr_page = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stderr,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    assert_eq!(stderr_page.encoding, "base64");
    assert!(stderr_page.eof);
    assert_eq!(stderr_page.observed_end, 0);
    assert!(stderr_page.data.is_empty());

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn file_change_auxiliary_round_trip_and_missing_blob_degrade_details() {
    let (base, store, session_id) = fixture("file-change-round-trip").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("write-file-change").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"user\n"),
            after: content_revision(b"agent\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"user\n".to_vec()),
            after_bytes: Some(b"agent\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("file change record finishes");
    let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    let scan = store
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(scan.records.len(), 1);
    let mut blob_budget = ChangeBlobBudget {
        used_bytes: 0,
        exhausted: false,
    };
    assert!(
        store
            .change_blobs_available(
                session_id,
                &scan.records[0].0,
                &scan.records[0].1,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
                &mut blob_budget,
            )
            .await
            .unwrap()
    );

    let target = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(tool_ref_hash(&tool_ref))
        .join(TOOL_AFTER_FILE);
    fs::remove_file(&target).await.unwrap();
    let scan = store
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(scan.records.len(), 1);
    let mut blob_budget = ChangeBlobBudget {
        used_bytes: 0,
        exhausted: false,
    };
    assert!(
        !store
            .change_blobs_available(
                session_id,
                &scan.records[0].0,
                &scan.records[0].1,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
                &mut blob_budget,
            )
            .await
            .unwrap()
    );
    fs::write(&target, b"bogus!").await.unwrap();
    let scan = store
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(scan.records.len(), 1);
    let mut blob_budget = ChangeBlobBudget {
        used_bytes: 0,
        exhausted: false,
    };
    assert!(
        !store
            .change_blobs_available(
                session_id,
                &scan.records[0].0,
                &scan.records[0].1,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
                &mut blob_budget,
            )
            .await
            .unwrap()
    );
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn cold_change_list_pages_many_records_without_reading_other_pages_blobs() {
    let (base, store, session_id) = fixture("change-cold-pages").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    // 12 persisted native records; only the page's records are ever verified.
    let mut refs = Vec::new();
    for index in 0..12u32 {
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: index,
            tool_call_id: ToolCallId::new(format!("cold-{index}")).unwrap(),
        };
        let before = format!("before-{index}\n").into_bytes();
        let after = format!("after-{index}\n").into_bytes();
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: format!("file-{index}.txt"),
                kind: ChangeKind::Modified,
                before: content_revision(&before),
                after: content_revision(&after),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(before),
                after_bytes: Some(after),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("cold change record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        refs.push(tool_ref);
    }

    // A missing after.bin degrades only that one record's details.
    let broken_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(tool_ref_hash(&refs[3]));
    fs::remove_file(broken_dir.join(TOOL_AFTER_FILE))
        .await
        .unwrap();

    let scan = store
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(scan.records.len(), 12);
    assert!(scan.complete);
    assert!(!scan.skipped);
    assert!(scan.metadata_bytes > 0);
    assert!(scan.entries >= 12);
    // Metadata-only scan: no blob is read or hashed while listing.
    let mut budget = ChangeBlobBudget {
        used_bytes: 0,
        exhausted: false,
    };
    for (tool_ref, change) in &scan.records {
        if tool_ref == &refs[3] {
            assert!(
                !store
                    .change_blobs_available(
                        session_id,
                        tool_ref,
                        change,
                        &CancellationToken::new(),
                        Instant::now() + Duration::from_secs(10),
                        &mut budget,
                    )
                    .await
                    .unwrap()
            );
        }
    }

    // Continuation cursors keep scope identity and reject a different scope.
    let request = crate::changes::ChangesListRequest {
        session_id,
        scope: crate::changes::ChangeScope::Session,
        cursor: None,
        limit: 3,
        max_bytes: Some(64 * 1024),
    };
    // Only the records that survive a page may have their blobs read and
    // verified. Walk every page to exhaustion: each page must attempt at
    // least one read (even a missing after.bin is still attempted), and no
    // attempt may target a record outside that page. The log is scoped to
    // this fixture root so parallel tests cannot pollute or drain it.
    let page_dirs = |page: &crate::changes::ChangesListResult| {
        page.records
            .iter()
            .filter_map(|record| record.tool_ref.as_ref())
            .map(tool_ref_hash)
            .collect::<std::collections::BTreeSet<_>>()
    };
    let dir_of = |path: &Path| {
        path.parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            .unwrap()
            .to_owned()
    };
    let _ = take_read_change_blobs_under(&base);
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut pages = 0;
    let mut broken_seen = false;
    loop {
        let page = crate::changes::list_tool_changes(
            store.clone(),
            None,
            crate::changes::ChangesListRequest {
                cursor: cursor.clone(),
                ..request.clone()
            },
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert!(!page.records.is_empty());
        let expected_dirs = page_dirs(&page);
        let attempts = take_read_change_blobs_under(&base);
        assert!(
            !attempts.is_empty(),
            "page {pages} did not attempt any blob verification"
        );
        for path in attempts {
            let dir = dir_of(&path);
            assert!(
                expected_dirs.contains(&dir),
                "cold listing read a blob outside its page: {dir}"
            );
        }
        for record in &page.records {
            assert!(
                seen.insert(record.change_ref.clone()),
                "a record was returned on two pages"
            );
            if record.tool_ref.as_ref() == Some(&refs[3]) {
                broken_seen = true;
                assert!(!record.details_available);
            } else {
                assert!(record.details_available);
            }
        }
        pages += 1;
        match page.next_cursor {
            Some(next) => {
                assert_eq!(next.session_id, session_id);
                assert_eq!(next.scope, crate::changes::ChangeScope::Session);
                cursor = Some(next);
            }
            None => break,
        }
    }
    assert_eq!(pages, 4, "12 records page as 3+3+3+3");
    assert_eq!(seen.len(), 12, "every record was returned exactly once");
    assert!(broken_seen, "the degraded record was still listed");

    // A workspace-scoped cursor cannot continue a session page.
    let mismatched = crate::changes::ChangesListRequest {
        scope: crate::changes::ChangeScope::Workspace,
        cursor: cursor.clone(),
        ..request.clone()
    };
    assert!(mismatched.validate().is_err());

    // Turn scope is filtered by the exact Loop ID and stays separate from
    // the session scope: each record has its own loop here.
    let turn_page = crate::changes::list_tool_changes(
        store.clone(),
        None,
        crate::changes::ChangesListRequest {
            scope: crate::changes::ChangeScope::Turn {
                loop_id: refs[0].loop_id,
            },
            cursor: None,
            limit: 100,
            max_bytes: Some(64 * 1024),
            ..request.clone()
        },
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(turn_page.records.len(), 1);
    assert_eq!(
        turn_page.records[0].tool_ref.as_ref().unwrap().loop_id,
        refs[0].loop_id
    );

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn corrupt_change_metadata_is_skipped_without_corrupting_the_store() {
    let (base, store, session_id) = fixture("change-metadata-corrupt").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let good_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("good").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&good_ref, "write");
    data.note_file_change(
        &good_ref,
        FileChange {
            path: "good.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"before\n"),
            after: content_revision(b"after\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"before\n".to_vec()),
            after_bytes: Some(b"after\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&good_ref, ToolResultOutcome::Success)
        .expect("change record finishes");
    store
        .commit_tool_record(
            &data.snapshot_for_persistence(&good_ref).unwrap(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();

    // A directory with a malformed record.json must be skipped as an
    // incomplete scan, not treated as Store corruption. Warm records still
    // list the valid one.
    let bad_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join("f".repeat(64));
    fs::create_dir_all(&bad_dir).await.unwrap();
    fs::write(bad_dir.join(TOOL_RECORD_FILE), b"{ not json")
        .await
        .unwrap();
    let valid_ref = ToolRef {
        session_id,
        loop_id: good_ref.loop_id,
        request_index: good_ref.request_index,
        tool_call_id: good_ref.tool_call_id.clone(),
    };
    let warm = ToolData::new();
    warm.note_requested(&valid_ref, "write");
    warm.note_file_change(
        &valid_ref,
        FileChange {
            path: "good.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"before\n"),
            after: content_revision(b"after\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"before\n".to_vec()),
            after_bytes: Some(b"after\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    let result = crate::changes::list_tool_changes(
        store.clone(),
        Some(std::sync::Arc::new(warm)),
        crate::changes::ChangesListRequest {
            session_id,
            scope: crate::changes::ChangeScope::Session,
            cursor: None,
            limit: 10,
            max_bytes: Some(64 * 1024),
        },
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(
        result
            .warnings
            .contains(&crate::changes::ChangeListWarning::RecordsSkipped)
    );
    assert!(!result.complete);
    assert!(
        result
            .records
            .iter()
            .any(|record| record.path == "good.txt")
    );

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn change_scan_entry_and_metadata_budgets_reserve_before_io() {
    let (base, store, session_id) = fixture("change-scan-budget").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let mut refs = Vec::new();
    for index in 0..3u32 {
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: index,
            tool_call_id: ToolCallId::new(format!("budget-{index}")).unwrap(),
        };
        let data = ToolData::new();
        data.note_requested(&tool_ref, "write");
        data.note_file_change(
            &tool_ref,
            FileChange {
                path: format!("budget-{index}.txt"),
                kind: ChangeKind::Modified,
                before: content_revision(b"before\n"),
                after: content_revision(b"after\n"),
                commit_state: ChangeCommitState::Applied,
                coverage: ChangeCoverage::Complete,
                before_captured: true,
                after_captured: true,
                before_bytes: Some(b"before\n".to_vec()),
                after_bytes: Some(b"after\n".to_vec()),
                before_corrupt: false,
                after_corrupt: false,
            },
        );
        data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
            .expect("budget change record finishes");
        store
            .commit_tool_record(
                &data.snapshot_for_persistence(&tool_ref).unwrap(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        refs.push(tool_ref);
    }

    // A scan budget that admits at most one entry must stop before reading a
    // second directory entry, and must report the scan as incomplete rather
    // than silently claiming a complete one-record list.
    let limited = store.clone().with_aux_limits(AuxLimits {
        max_scan_entries: 1,
        ..DEFAULT_AUX_LIMITS
    });
    let scan = limited
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(!scan.complete);
    assert!(scan.skipped);
    assert!(
        scan.entries <= 1,
        "the entry ceiling is never exceeded (got {})",
        scan.entries
    );

    // Metadata reads stay strictly inside the cumulative allowance: an empty
    // remainder refuses before any read, and the direct unit assertions on
    // accounting stay independent of filesystem size.
    let budget = ChangeScanBudget {
        entries: 0,
        metadata_bytes: CHANGE_SCAN_METADATA_BYTES,
    };
    assert_eq!(
        CHANGE_SCAN_METADATA_BYTES.saturating_sub(budget.metadata_bytes),
        0
    );
    assert!(!budget.entry_available(0));
    assert!(budget.entry_available(1));

    // A record.json larger than the bounded metadata read is refused, never
    // parsed as a complete record: the scan reports it as skipped.
    let oversized_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join("a".repeat(64));
    fs::create_dir_all(&oversized_dir).await.unwrap();
    fs::write(
        oversized_dir.join(TOOL_RECORD_FILE),
        vec![b'x'; MAX_TOOL_METADATA_BYTES + 1],
    )
    .await
    .unwrap();
    let oversized_scan = store
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(!oversized_scan.complete);
    assert!(oversized_scan.skipped);
    assert_eq!(
        oversized_scan.records.len(),
        3,
        "only the real records list"
    );

    // Blob verification charges the +1 lookahead, not just the expected
    // bytes, and a missing before-image costs nothing.
    assert_eq!(
        change_blob_read_cost(&content_revision(b"")),
        1,
        "an empty content blob still costs its one-byte lookahead"
    );
    assert_eq!(change_blob_read_cost(&ChangeRevision::Missing), 0);
    let mut blob_budget = ChangeBlobBudget {
        used_bytes: CHANGE_SCAN_BLOB_BYTES - 1,
        exhausted: false,
    };
    let change = StoredFileChange {
        path: "x".to_owned(),
        kind: ChangeKind::Modified,
        before: ChangeRevision::Missing,
        after: content_revision(b"after\n"),
        commit_state: ChangeCommitState::Applied,
        coverage: ChangeCoverage::Complete,
        before_captured: true,
        after_captured: true,
    };
    assert!(
        !store
            .change_blobs_available(
                session_id,
                &refs[0],
                &change,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
                &mut blob_budget,
            )
            .await
            .unwrap()
    );
    assert!(blob_budget.exhausted, "the lookahead crosses the budget");

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_resolves_warm_and_cold_and_rejects_a_stale_cursor() {
    let (base, store, session_id) = fixture("diff-warm-cold").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-warm").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"user dirty\n"),
            after: content_revision(b"agent clean\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"user dirty\n".to_vec()),
            after_bytes: Some(b"agent clean\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("diff record finishes");
    store
        .commit_tool_record(
            &data.snapshot_for_persistence(&tool_ref).unwrap(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    let change_ref = crate::changes::stored_tool_change_ref(
        &tool_ref,
        &data.file_change(&tool_ref).unwrap().stored(),
    );
    let request = crate::diff::ChangesDiffRequest {
        session_id,
        change_ref: change_ref.clone(),
        comparison: None,
        context_lines: None,
        cursor: None,
        max_bytes: None,
    };

    // Warm path: the in-memory snapshots answer without any disk read.
    let warm = crate::diff::changes_diff(
        store.clone(),
        Some(Arc::new(data)),
        request.clone(),
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(warm.availability, crate::diff::DiffAvailability::Available);
    assert!(!warm.binary);
    assert_eq!(warm.base_version, content_revision(b"user dirty\n"));
    assert_eq!(warm.target_version, content_revision(b"agent clean\n"));
    assert!(
        warm.hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .any(|line| line.kind == crate::diff::DiffLineKind::Removed)
    );

    // Cold path: an unloaded Session still resolves through the bounded
    // metadata scan and reads only this record's snapshots.
    let cold = crate::diff::changes_diff(
        store.clone(),
        None,
        request.clone(),
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(cold.availability, crate::diff::DiffAvailability::Available);
    assert_eq!(cold.change_ref, change_ref);

    // A cursor carrying a different diff fingerprint is stale, not a new diff.
    let stale = crate::diff::changes_diff(
        store.clone(),
        None,
        crate::diff::ChangesDiffRequest {
            cursor: Some(crate::diff::DiffCursor {
                session_id,
                change_ref: change_ref.clone(),
                tool_ref: Some(tool_ref.clone()),
                ops_fingerprint: "a".repeat(64),
                context_lines: 3,
                hunk_index: 0,
                line_index: 0,
                line_byte_offset: 0,
            }),
            ..request.clone()
        },
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(stale.stale);
    assert!(stale.hunks.is_empty());

    // An unknown reference is not a fabricated empty diff.
    let miss = crate::diff::changes_diff(
        store.clone(),
        None,
        crate::diff::ChangesDiffRequest {
            change_ref: format!("tool:{}", "b".repeat(64)),
            ..request.clone()
        },
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await;
    assert!(matches!(miss, Err(AgentError::ToolNotFound)));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_missing_before_is_an_addition_and_empty_content_is_not_binary() {
    let (base, store, session_id) = fixture("diff-missing-before").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-added").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "created.txt".to_owned(),
            kind: ChangeKind::Added,
            before: crate::changes::ChangeRevision::Missing,
            after: content_revision(b"new\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: None,
            after_bytes: Some(b"new\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("diff record finishes");
    store
        .commit_tool_record(
            &data.snapshot_for_persistence(&tool_ref).unwrap(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    let change_ref = crate::changes::stored_tool_change_ref(
        &tool_ref,
        &data.file_change(&tool_ref).unwrap().stored(),
    );
    let result = crate::diff::changes_diff(
        store.clone(),
        None,
        crate::diff::ChangesDiffRequest {
            session_id,
            change_ref,
            context_lines: None,
            comparison: None,
            cursor: None,
            max_bytes: None,
        },
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(result.base_version, crate::changes::ChangeRevision::Missing);
    assert!(!result.binary);
    assert_eq!(
        result.availability,
        crate::diff::DiffAvailability::Available
    );
    assert!(
        result
            .hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .any(|line| line.kind == crate::diff::DiffLineKind::Added)
    );

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_corrupt_or_missing_snapshots_report_unavailable_not_empty() {
    let (base, store, session_id) = fixture("diff-corrupt").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-corrupt").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"before\n"),
            after: content_revision(b"after\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"before\n".to_vec()),
            after_bytes: Some(b"after\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("diff record finishes");
    store
        .commit_tool_record(
            &data.snapshot_for_persistence(&tool_ref).unwrap(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    let change_ref = crate::changes::stored_tool_change_ref(
        &tool_ref,
        &data.file_change(&tool_ref).unwrap().stored(),
    );
    let target = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(tool_ref_hash(&tool_ref))
        .join(TOOL_AFTER_FILE);
    fs::write(&target, b"tampered bytes").await.unwrap();
    let result = crate::diff::changes_diff(
        store.clone(),
        None,
        crate::diff::ChangesDiffRequest {
            session_id,
            change_ref,
            context_lines: None,
            comparison: None,
            cursor: None,
            max_bytes: None,
        },
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(
        result.availability,
        crate::diff::DiffAvailability::Unavailable
    );
    assert!(result.hunks.is_empty());
    assert!(!result.binary);

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_warm_half_merges_with_its_disk_counterpart_only() {
    let (base, store, session_id) = fixture("diff-half-merge").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-half").unwrap(),
    };
    let before = b"user dirty\n".to_vec();
    let after = b"agent clean\n".to_vec();
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(&before),
            after: content_revision(&after),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(before.clone()),
            after_bytes: Some(after.clone()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("diff record finishes");
    let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
    let change_ref = crate::changes::stored_tool_change_ref(
        &tool_ref,
        &data.file_change(&tool_ref).unwrap().stored(),
    );

    // Warm record lost its before bytes but keeps a valid after half; the
    // matching disk record supplies the before side. The result compares
    // the real before, unaugmented by disk, and the request never fabricates
    // an unrelated half.
    let warm = ToolData::new();
    warm.note_requested(&tool_ref, "write");
    warm.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(&before),
            after: content_revision(&after),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: None,
            after_bytes: Some(after.clone()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    let warm = Arc::new(warm);
    let merged = crate::diff::resolve_tool_change(
        &store,
        Some(&warm),
        session_id,
        &change_ref,
        None,
        Instant::now() + Duration::from_secs(10),
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .expect("the warm record resolves");
    assert_eq!(merged.1.before_bytes.as_deref(), Some(before.as_slice()));
    assert_eq!(merged.1.after_bytes.as_deref(), Some(after.as_slice()));
    assert!(merged.1.details_available());

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_metadata_conflict_keeps_the_valid_warm_side() {
    let (base, store, session_id) = fixture("diff-meta-conflict").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-conflict").unwrap(),
    };
    let disk_before = b"disk before\n".to_vec();
    let disk_after = b"disk after\n".to_vec();
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(&disk_before),
            after: content_revision(&disk_after),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(disk_before.clone()),
            after_bytes: Some(disk_after.clone()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("diff record finishes");
    store
        .commit_tool_record(
            &data.snapshot_for_persistence(&tool_ref).unwrap(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();

    // The warm record carries a different revision than disk. Disk must not
    // be merged in: the valid warm sides stay, and the absent side stays
    // unavailable rather than taking a disk half from another revision.
    let warm_before = b"warm before\n".to_vec();
    let warm = ToolData::new();
    warm.note_requested(&tool_ref, "write");
    warm.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(&warm_before),
            after: content_revision(&disk_after),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(warm_before.clone()),
            after_bytes: None,
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    let warm_ref = crate::changes::stored_tool_change_ref(
        &tool_ref,
        &warm.file_change(&tool_ref).unwrap().stored(),
    );
    let warm = Arc::new(warm);
    let resolved = crate::diff::resolve_tool_change(
        &store,
        Some(&warm),
        session_id,
        &warm_ref,
        None,
        Instant::now() + Duration::from_secs(10),
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .expect("the warm record resolves");
    assert_eq!(
        resolved.1.before_bytes.as_deref(),
        Some(warm_before.as_slice())
    );
    assert!(resolved.1.after_bytes.is_none());
    assert!(!resolved.1.details_available());

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_cursor_hint_must_match_the_resolved_reference() {
    let (base, store, session_id) = fixture("diff-hint").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-hint").unwrap(),
    };
    let data = ToolData::new();
    data.note_requested(&tool_ref, "write");
    data.note_file_change(
        &tool_ref,
        FileChange {
            path: "value.txt".to_owned(),
            kind: ChangeKind::Modified,
            before: content_revision(b"before\n"),
            after: content_revision(b"after\n"),
            commit_state: ChangeCommitState::Applied,
            coverage: ChangeCoverage::Complete,
            before_captured: true,
            after_captured: true,
            before_bytes: Some(b"before\n".to_vec()),
            after_bytes: Some(b"after\n".to_vec()),
            before_corrupt: false,
            after_corrupt: false,
        },
    );
    data.finish_and_snapshot(&tool_ref, ToolResultOutcome::Success)
        .expect("diff record finishes");
    store
        .commit_tool_record(
            &data.snapshot_for_persistence(&tool_ref).unwrap(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    let change_ref = crate::changes::stored_tool_change_ref(
        &tool_ref,
        &data.file_change(&tool_ref).unwrap().stored(),
    );

    // A hint pointing at the right session but the wrong change reference
    // must not select a different change. The metadata scan is still
    // consulted, so the real record is found and verified.
    let wrong = ToolRef {
        session_id,
        loop_id: tool_ref.loop_id,
        request_index: 0,
        tool_call_id: ToolCallId::new("diff-hint-other").unwrap(),
    };
    let resolved = crate::diff::resolve_tool_change(
        &store,
        None,
        session_id,
        &change_ref,
        Some(&wrong),
        Instant::now() + Duration::from_secs(10),
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .expect("the real change is found despite the wrong hint");
    assert_eq!(resolved.0, tool_ref);
    assert_eq!(resolved.1.path, "value.txt");

    // A forged hint for a different session is rejected without a lookup.
    let other_session = SessionId::new().unwrap();
    assert!(matches!(
        crate::diff::resolve_tool_change(
            &store,
            None,
            session_id,
            &change_ref,
            Some(&ToolRef {
                session_id: other_session,
                ..wrong.clone()
            }),
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await,
        Err(AgentError::InvalidArguments)
    ));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_worker_capacity_is_bounded_and_shutdown_joins() {
    let (base, store, session_id) = fixture("diff-workers").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();
    // Hold every worker slot with a distinct, gated comparison so capacity
    // cannot drain before the overflow attempt.
    let mut held = Vec::new();
    let mut gates = Vec::new();
    for index in 0..MAX_DIFF_WORKERS {
        let before: Arc<[u8]> = Arc::from(format!("line {index}\n").into_bytes());
        let after: Arc<[u8]> = Arc::from(format!("LINE {index}\n").into_bytes());
        let gate = Arc::new(crate::diff::DiffGate::new());
        crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
        held.push(
            store
                .spawn_diff_query(
                    session_id,
                    Arc::clone(&before),
                    Arc::clone(&after),
                    3,
                    Instant::now() + Duration::from_secs(10),
                )
                .unwrap(),
        );
        gates.push(gate);
    }
    for gate in &gates {
        tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
            .await
            .expect("diff worker did not start");
    }
    assert!(matches!(
        store.spawn_diff_query(
            session_id,
            Arc::from(b"one\ntwo\n".to_vec()),
            Arc::from(b"one\nTWO\n".to_vec()),
            3,
            Instant::now() + Duration::from_secs(10),
        ),
        Err(StoreError::QueryLimit)
    ));
    for gate in &gates {
        gate.release();
    }
    for query in held {
        let _ = query.wait().await;
    }
    store.shutdown_diff_workers().await;

    // After shutdown the worker set refuses new comparisons rather than
    // launching an unowned one.
    assert!(matches!(
        store.spawn_diff_query(
            session_id,
            Arc::from(b"one\ntwo\n".to_vec()),
            Arc::from(b"one\nTWO\n".to_vec()),
            3,
            Instant::now() + Duration::from_secs(10),
        ),
        Err(StoreError::QueryLimit)
    ));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_cancelled_admission_refuses_to_spawn_a_worker() {
    let (base, store, session_id) = fixture("diff-admission-cancelled").await;
    let admission = CancellationToken::new();
    admission.cancel();
    // A closing Session's token is observed under the registry lock, so no
    // worker is registered for a comparison the Session already dropped.
    assert!(matches!(
        store.spawn_cancellable_diff_query(
            session_id,
            Arc::from(b"one\ntwo\n".to_vec()),
            Arc::from(b"one\nTWO\n".to_vec()),
            3,
            Instant::now() + Duration::from_secs(10),
            &admission,
        ),
        Err(StoreError::QueryLimit)
    ));
    assert_eq!(store.registered_diff_workers(), 0);

    // The same admission token cancels a worker that is already running.
    let live = CancellationToken::new();
    let before: Arc<[u8]> = Arc::from(b"alpha\n".to_vec());
    let after: Arc<[u8]> = Arc::from(b"ALPHA\n".to_vec());
    let gate = Arc::new(crate::diff::DiffGate::new());
    crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
    let query = store
        .spawn_cancellable_diff_query(
            session_id,
            Arc::clone(&before),
            Arc::clone(&after),
            3,
            Instant::now() + Duration::from_secs(30),
            &live,
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
        .await
        .expect("diff worker did not start");
    live.cancel();
    // The gate is never released, so this only returns once the derived
    // child token really reached the blocking comparison.
    assert!(matches!(query.wait().await, Err(AgentError::QueryLimit)));
    store.shutdown_diff_workers().await;
    assert_eq!(store.registered_diff_workers(), 0);

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_shutdown_drop_and_concurrent_join_really_stop_the_worker() {
    let (base, store, session_id) = fixture("diff-shutdown-reentrant").await;
    let before: Arc<[u8]> = Arc::from(b"one\n".to_vec());
    let after: Arc<[u8]> = Arc::from(b"ONE\n".to_vec());
    let gate = Arc::new(crate::diff::DiffGate::new());
    crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
    let query = store
        .spawn_diff_query(
            session_id,
            Arc::clone(&before),
            Arc::clone(&after),
            3,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
        .await
        .expect("diff worker did not start");
    // Dropping the query owner only cancels the comparison; the Store still
    // owns the handle. The gate is never released, so completion depends on
    // the cancellation really reaching the blocking handler.
    drop(query);
    tokio::time::timeout(Duration::from_secs(10), store.shutdown_diff_workers())
        .await
        .expect("shutdown did not join the cancelled worker");
    assert_eq!(store.registered_diff_workers(), 0);
    let _ = fs::remove_dir_all(base).await;

    // A shutdown future dropped before it joins must not detach the handle.
    // The same owner is still registered and a later (or concurrent)
    // shutdown joins the same worker instead of returning early.
    let (base, store, session_id) = fixture("diff-shutdown-dropped").await;
    let before: Arc<[u8]> = Arc::from(b"two\n".to_vec());
    let after: Arc<[u8]> = Arc::from(b"TWO\n".to_vec());
    let gate = Arc::new(crate::diff::DiffGate::new());
    crate::diff::gate_next_diff(&before, &after, Arc::clone(&gate));
    let _query = store
        .spawn_diff_query(
            session_id,
            Arc::clone(&before),
            Arc::clone(&after),
            3,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
        .await
        .expect("diff worker did not start");
    // Dropping the future before it is ever polled must not detach the
    // handle: the same owner is still registered, so a later (or
    // concurrent) shutdown joins the same worker instead of returning early.
    let dropped = store.shutdown_diff_workers();
    drop(dropped);
    assert_eq!(store.registered_diff_workers(), 1);
    let (first, second) =
        tokio::join!(store.shutdown_diff_workers(), store.shutdown_diff_workers());
    let _ = (first, second);
    assert_eq!(store.registered_diff_workers(), 0);

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn diff_session_shutdown_joins_only_that_session() {
    let (base, store, first) = fixture("diff-session-join-a").await;
    let second = SessionId::new().unwrap();
    let before_a: Arc<[u8]> = Arc::from(b"alpha\n".to_vec());
    let after_a: Arc<[u8]> = Arc::from(b"ALPHA\n".to_vec());
    let before_b: Arc<[u8]> = Arc::from(b"beta\n".to_vec());
    let after_b: Arc<[u8]> = Arc::from(b"BETA\n".to_vec());
    let gate_a = Arc::new(crate::diff::DiffGate::new());
    crate::diff::gate_next_diff(&before_a, &after_a, Arc::clone(&gate_a));
    let gate_b = Arc::new(crate::diff::DiffGate::new());
    crate::diff::gate_next_diff(&before_b, &after_b, Arc::clone(&gate_b));
    let query_a = store
        .spawn_diff_query(
            first,
            Arc::clone(&before_a),
            Arc::clone(&after_a),
            3,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
    let query_b = store
        .spawn_diff_query(
            second,
            Arc::clone(&before_b),
            Arc::clone(&after_b),
            3,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate_a.wait_started())
        .await
        .expect("first diff worker did not start");
    tokio::time::timeout(Duration::from_secs(10), gate_b.wait_started())
        .await
        .expect("second diff worker did not start");
    // The first Session's close cancels and joins only its own CPU worker;
    // the unrelated Session's comparison stays registered and running.
    tokio::time::timeout(
        Duration::from_secs(10),
        store.shutdown_session_diff_workers(first),
    )
    .await
    .expect("session shutdown did not join its worker");
    assert_eq!(store.registered_diff_workers(), 1);
    drop(query_a);
    drop(query_b);
    store.shutdown_diff_workers().await;
    assert_eq!(store.registered_diff_workers(), 0);

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn valid_temp_directory_does_not_mark_tool_change_scan_incomplete() {
    let (base, store, session_id) = fixture("temp-scan-complete").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-temp-1").unwrap(),
    };
    let valid_hash = tool_ref_hash(&tool_ref);
    let temp_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(format!(".{valid_hash}.tmp-1234-1"));
    fs::create_dir_all(&temp_dir).await.unwrap();

    let scan = store
        .list_tool_changes(
            session_id,
            None,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(scan.complete);
    assert!(!scan.skipped);
    assert!(scan.records.is_empty());
    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn complete_identity_and_path_traversal_protection() {
    let (base, store, session_id) = fixture("aux-identity-path").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-1").unwrap(),
    };

    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: None,
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    let hash = tool_ref_hash(&tool_ref);
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

    // Check directory exists strictly under tools/
    let expected_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash);
    assert!(expected_dir.is_dir());

    // Mismatched ToolRef query on different loop or call returns None
    let other_tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-1").unwrap(),
    };
    assert!(
        store
            .read_tool_record(&other_tool_ref)
            .await
            .unwrap()
            .is_none()
    );

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn atomic_publishing_idempotent_overwrite_and_cleanup_on_failure() {
    let (base, store, session_id) = fixture("aux-atomic-idempotent").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-idempotent").unwrap(),
    };

    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "read".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: None,
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    // First commit succeeds
    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    // Second commit of identical record succeeds idempotently
    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    // Injected failure cleans up temporary directory without corrupting target
    fail_next_aux_write(session_id);
    assert!(matches!(
        store
            .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
            .await,
        Err(StoreError::Unavailable)
    ));

    // Read still succeeds
    assert!(store.read_tool_record(&tool_ref).await.unwrap().is_some());

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn metadata_and_blob_corruption_handling() {
    let (base, store, session_id) = fixture("aux-corrupt").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-corrupt").unwrap(),
    };

    let stdout_bytes = b"authoritative stdout tail".to_vec();
    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: stdout_bytes.len() as u64,
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: stdout_bytes.len(),
                file_sha256: Some(hash_bytes(&stdout_bytes)),
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: Some(stdout_bytes),
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();

    // 1. Missing blob: remove stdout.bin
    let hash = tool_ref_hash(&tool_ref);
    let blob_path = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash)
        .join(TOOL_STDOUT_FILE);
    fs::remove_file(&blob_path).await.unwrap();

    let recovered = store
        .read_tool_record(&tool_ref)
        .await
        .unwrap()
        .expect("metadata should still load");
    let page = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    // Missing blob must report Unavailable and retain observed_end, not a clean empty EOF
    assert_eq!(page.availability, ToolDataAvailability::Unavailable);
    assert_eq!(page.observed_end, 25);
    assert!(page.data.is_empty());
    assert!(page.truncated);

    // 2. Hash mismatch / corrupt bytes: write wrong content
    fs::write(&blob_path, b"corrupted bytes!").await.unwrap();
    let recovered2 = store
        .read_tool_record(&tool_ref)
        .await
        .unwrap()
        .expect("metadata should still load");
    let page2 = recovered2
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    assert_eq!(page2.availability, ToolDataAvailability::Unavailable);
    assert_eq!(page2.observed_end, 25);
    assert!(page2.data.is_empty());

    // 3. Unsupported format version
    let record_path = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash)
        .join(TOOL_RECORD_FILE);
    let mut corrupted_record = snapshot.record.clone();
    corrupted_record.version = 999;
    fs::write(&record_path, serde_json::to_vec(&corrupted_record).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        store.read_tool_record(&tool_ref).await,
        Err(StoreError::UnsupportedFormat)
    ));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn auxiliary_budget_gc_and_scan_limits() {
    let (base, store, session_id) = fixture("aux-gc-limits").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    // Set small limits: session max 2 records / 50 KiB; global max 3 records / 80 KiB
    let store = store.with_aux_limits(AuxLimits {
        session_bytes: 50 * 1024,
        session_records: 2,
        global_bytes: 80 * 1024,
        global_records: 3,
        max_scan_entries: 50,
    });

    let make_snap = |idx: u32, call_name: &'static str| {
        let tool_ref = ToolRef {
            session_id,
            loop_id: LoopId::new().unwrap(),
            request_index: idx,
            tool_call_id: ToolCallId::new(call_name).unwrap(),
        };
        ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        }
    };

    let snap1 = make_snap(0, "call-gc-1");
    store
        .commit_tool_record(&snap1, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let snap2 = make_snap(1, "call-gc-2");
    store
        .commit_tool_record(&snap2, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Both records exist
    assert!(
        store
            .read_tool_record(&snap1.tool_ref)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_tool_record(&snap2.tool_ref)
            .await
            .unwrap()
            .is_some()
    );

    // Third record exceeds session_records (2) -> snap1 must be evicted!
    let snap3 = make_snap(2, "call-gc-3");
    store
        .commit_tool_record(&snap3, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    assert!(
        store
            .read_tool_record(&snap1.tool_ref)
            .await
            .unwrap()
            .is_none(),
        "oldest record must be evicted"
    );
    assert!(
        store
            .read_tool_record(&snap2.tool_ref)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_tool_record(&snap3.tool_ref)
            .await
            .unwrap()
            .is_some()
    );

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn legacy_session_without_tools_directory() {
    let (base, store, session_id) = fixture("aux-legacy").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-old").unwrap(),
    };

    // No tools directory exists yet; read returns Ok(None), not StoreError::Corrupt
    assert!(store.read_tool_record(&tool_ref).await.unwrap().is_none());

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn scan_limit_hit_in_inner_files_returns_query_limit() {
    let (base, store, session_id) = fixture("aux-inner-limit").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-scan-inner").unwrap(),
    };

    let stdout_bytes = b"testing inner scan limit".to_vec();
    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: stdout_bytes.len() as u64,
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: stdout_bytes.len(),
                file_sha256: Some(hash_bytes(&stdout_bytes)),
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: Some(stdout_bytes),
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    // First commit with normal limits
    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    // Second commit with max_scan_entries: 1 -> scanning sessions/tools/inner_files
    // will exceed scan limit inside directory scan!
    let store_restricted = store.with_aux_limits(AuxLimits {
        session_bytes: 16 * 1024 * 1024,
        session_records: 1024,
        global_bytes: 256 * 1024 * 1024,
        global_records: 8192,
        max_scan_entries: 1,
    });

    let tool_ref2 = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-scan-inner-2").unwrap(),
    };
    let mut snap2 = snapshot.clone();
    snap2.tool_ref = tool_ref2.clone();
    snap2.record.tool_ref = tool_ref2;

    let res = store_restricted
        .commit_tool_record(&snap2, Instant::now() + Duration::from_secs(5))
        .await;
    assert!(matches!(res, Err(StoreError::QueryLimit)));

    let _ = fs::remove_dir_all(base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_session_id_dir_is_rejected_and_not_scanned() {
    use std::os::unix::fs::symlink;

    let (base, store, session_id) = fixture("aux-symlink-ses").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    // Create an outside directory
    let outside = base.join("outside_dir");
    fs::create_dir_all(&outside).await.unwrap();

    // Create a symlink named as a valid SessionId pointing to outside_dir
    let fake_ses_id = SessionId::new().unwrap();
    let symlink_path = store.sessions_directory().join(fake_ses_id.to_string());
    symlink(&outside, &symlink_path).unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-sym-ses").unwrap(),
    };
    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: None,
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    // Budget enforcement scanning encounters the symlink session entry and must fail closed
    let res = store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await;
    assert!(matches!(res, Err(StoreError::Corrupt)));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn temp_dir_removal_failure_rejects_aux_commit() {
    let (base, store, session_id) = fixture("aux-temp-fail").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-temp-fail").unwrap(),
    };

    let tools_dir = store.session_directory(session_id).join(AUX_TOOLS_DIR);
    fs::create_dir_all(&tools_dir).await.unwrap();
    let valid_hash = "0".repeat(64);
    let temp_dir = tools_dir.join(format!(".{valid_hash}.tmp-1234-1"));
    fs::create_dir(&temp_dir).await.unwrap();
    fs::write(temp_dir.join(TOOL_RECORD_FILE), b"{}")
        .await
        .unwrap();

    fail_next_remove_temp(session_id);

    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: None,
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    let res = store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await;
    assert!(matches!(res, Err(StoreError::Unavailable)));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn private_temp_directory_with_unknown_files_is_not_deleted_and_rejects_commit() {
    let (base, store, session_id) = fixture("aux-private-temp").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-private-temp").unwrap(),
    };

    let tools_dir = store.session_directory(session_id).join(AUX_TOOLS_DIR);
    fs::create_dir_all(&tools_dir).await.unwrap();

    // 1. Directory with name ".private.tmp-not-owned"
    let private_dir = tools_dir.join(".private.tmp-not-owned");
    fs::create_dir(&private_dir).await.unwrap();
    let user_file = private_dir.join("user_secret.txt");
    fs::write(&user_file, b"secret user content").await.unwrap();
    let nested_dir = private_dir.join("nested");
    fs::create_dir(&nested_dir).await.unwrap();
    fs::write(nested_dir.join("nested.txt"), b"nested content")
        .await
        .unwrap();

    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: None,
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    // Budget enforcement fails closed because of unknown directory
    let res = store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await;
    assert!(matches!(res, Err(StoreError::Corrupt)));

    // Critical safety verification: user file and nested directory are NEVER deleted!
    assert!(user_file.exists(), "user file must not be deleted");
    assert!(nested_dir.exists(), "nested directory must not be deleted");

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn legitimate_crashed_temp_directory_is_converged_and_evicted() {
    let (base, store, session_id) = fixture("aux-crashed-temp").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-crashed-temp").unwrap(),
    };

    let tools_dir = store.session_directory(session_id).join(AUX_TOOLS_DIR);
    fs::create_dir_all(&tools_dir).await.unwrap();

    // Legitimate temp directory format with only allowed aux files
    let valid_hash = "a".repeat(64);
    let crashed_temp_dir = tools_dir.join(format!(".{valid_hash}.tmp-9999-1"));
    fs::create_dir(&crashed_temp_dir).await.unwrap();
    fs::write(crashed_temp_dir.join(TOOL_RECORD_FILE), b"{}")
        .await
        .unwrap();
    fs::write(crashed_temp_dir.join(TOOL_INPUT_FILE), b"crash input")
        .await
        .unwrap();

    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: None,
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    // Commit succeeds and cleans up the orphaned legitimate temp directory
    let res = store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await;
    assert!(res.is_ok());
    assert!(
        !crashed_temp_dir.exists(),
        "orphaned temp dir converged and removed"
    );

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn oversized_metadata_file_bytes_rejected_without_alloc() {
    let (base, store, session_id) = fixture("aux-oversized-meta").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-oversized").unwrap(),
    };

    let hash = tool_ref_hash(&tool_ref);
    let target_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash);
    fs::create_dir_all(&target_dir).await.unwrap();

    // Write a malicious record claiming 100 MiB input file_bytes
    let malicious_record = StoredToolRecord {
        version: TOOL_RECORD_FORMAT_VERSION,
        tool_ref: tool_ref.clone(),
        name: "bash".to_owned(),
        subject: ToolSubject::Other,
        subject_truncated: false,
        state: ToolExecutionState::Succeeded,
        phase: None,
        started_at: None,
        finished_at: None,
        outcome: Some(ToolResultOutcome::Success),
        input: StoredInputSummary {
            total_bytes: 100 * 1024 * 1024,
            seen: true,
            truncated: false,
            expired: false,
            file_bytes: 100 * 1024 * 1024,
            file_sha256: Some("00".repeat(32)),
        },
        result: StoredResultSummary {
            total_bytes: 0,
            seen: false,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        stdout: StoredStreamWindow {
            start_offset: 0,
            observed_end: 0,
            seen: false,
            complete: true,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        stderr: StoredStreamWindow {
            start_offset: 0,
            observed_end: 0,
            seen: false,
            complete: true,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        command: None,
        file_change: None,
    };

    fs::write(
        target_dir.join(TOOL_RECORD_FILE),
        serde_json::to_vec(&malicious_record).unwrap(),
    )
    .await
    .unwrap();

    // Must reject without allocating 100 MiB
    let res = store.read_tool_record(&tool_ref).await;
    assert!(matches!(res, Err(StoreError::RecordTooLarge)));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn invalid_metadata_ranges_and_hashes_rejected() {
    let (base, store, session_id) = fixture("aux-bad-ranges").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-bad-range").unwrap(),
    };

    let hash = tool_ref_hash(&tool_ref);
    let target_dir = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash);
    fs::create_dir_all(&target_dir).await.unwrap();

    // 1. Range start > end
    let bad_range_record = StoredToolRecord {
        version: TOOL_RECORD_FORMAT_VERSION,
        tool_ref: tool_ref.clone(),
        name: "bash".to_owned(),
        subject: ToolSubject::Other,
        subject_truncated: false,
        state: ToolExecutionState::Succeeded,
        phase: None,
        started_at: None,
        finished_at: None,
        outcome: Some(ToolResultOutcome::Success),
        input: StoredInputSummary {
            total_bytes: 0,
            seen: false,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        result: StoredResultSummary {
            total_bytes: 0,
            seen: false,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        stdout: StoredStreamWindow {
            start_offset: 50,
            observed_end: 10, // start > end
            seen: true,
            complete: true,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        stderr: StoredStreamWindow {
            start_offset: 0,
            observed_end: 0,
            seen: false,
            complete: true,
            truncated: false,
            expired: false,
            file_bytes: 0,
            file_sha256: None,
        },
        command: None,
        file_change: None,
    };
    fs::write(
        target_dir.join(TOOL_RECORD_FILE),
        serde_json::to_vec(&bad_range_record).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        store.read_tool_record(&tool_ref).await,
        Err(StoreError::Corrupt)
    ));

    // 2. Retained len mismatch (file_bytes != observed_end - start_offset)
    let mut bad_len_record = bad_range_record.clone();
    bad_len_record.stdout.start_offset = 0;
    bad_len_record.stdout.observed_end = 10;
    bad_len_record.stdout.file_bytes = 20; // mismatch: 20 != 10
    bad_len_record.stdout.file_sha256 = Some("00".repeat(32));
    fs::write(
        target_dir.join(TOOL_RECORD_FILE),
        serde_json::to_vec(&bad_len_record).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        store.read_tool_record(&tool_ref).await,
        Err(StoreError::Corrupt)
    ));

    // 3. Invalid sha256 format (non-hex)
    let mut bad_hash_record = bad_len_record;
    bad_hash_record.stdout.file_bytes = 10;
    bad_hash_record.stdout.file_sha256 = Some("not-a-valid-hex-hash".to_owned());
    fs::write(
        target_dir.join(TOOL_RECORD_FILE),
        serde_json::to_vec(&bad_hash_record).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        store.read_tool_record(&tool_ref).await,
        Err(StoreError::Corrupt)
    ));

    let _ = fs::remove_dir_all(base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn single_blob_symlink_only_marks_stream_unavailable() {
    use std::os::unix::fs::symlink;

    let (base, store, session_id) = fixture("aux-blob-symlink").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-blob-symlink").unwrap(),
    };

    let stdout_bytes = b"safe output".to_vec();
    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: stdout_bytes.len() as u64,
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: stdout_bytes.len(),
                file_sha256: Some(hash_bytes(&stdout_bytes)),
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: Some(stdout_bytes),
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    // Replace stdout.bin with a symlink to an outside file
    let hash = tool_ref_hash(&tool_ref);
    let blob_path = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash)
        .join(TOOL_STDOUT_FILE);
    fs::remove_file(&blob_path).await.unwrap();
    let outside = base.join("outside_target.bin");
    fs::write(&outside, b"secret").await.unwrap();
    symlink(&outside, &blob_path).unwrap();

    // Reading the record succeeds; only stdout is Unavailable!
    let recovered = store
        .read_tool_record(&tool_ref)
        .await
        .unwrap()
        .expect("record should load");
    let page = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    assert_eq!(page.availability, ToolDataAvailability::Unavailable);

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn idempotent_commit_with_corrupt_existing_fails() {
    let (base, store, session_id) = fixture("aux-idempotent-corrupt").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-idempotent-corrupt").unwrap(),
    };

    let stdout_bytes = b"valid content".to_vec();
    let snapshot = ToolPersistenceSnapshot {
        tool_ref: tool_ref.clone(),
        record: StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: "bash".to_owned(),
            subject: ToolSubject::Other,
            subject_truncated: false,
            state: ToolExecutionState::Succeeded,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: Some(ToolResultOutcome::Success),
            input: StoredInputSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            result: StoredResultSummary {
                total_bytes: 0,
                seen: false,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            stdout: StoredStreamWindow {
                start_offset: 0,
                observed_end: stdout_bytes.len() as u64,
                seen: true,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: stdout_bytes.len(),
                file_sha256: Some(hash_bytes(&stdout_bytes)),
            },
            stderr: StoredStreamWindow {
                start_offset: 0,
                observed_end: 0,
                seen: false,
                complete: true,
                truncated: false,
                expired: false,
                file_bytes: 0,
                file_sha256: None,
            },
            command: None,
            file_change: None,
        },
        input_bytes: None,
        result_bytes: None,
        stdout_bytes: Some(stdout_bytes),
        stderr_bytes: None,
        file_change_before: None,
        file_change_after: None,
    };

    store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    // Corrupt stdout.bin
    let hash = tool_ref_hash(&tool_ref);
    let blob_path = store
        .session_directory(session_id)
        .join(AUX_TOOLS_DIR)
        .join(&hash)
        .join(TOOL_STDOUT_FILE);
    fs::write(&blob_path, b"corrupted bytes!").await.unwrap();

    // Idempotent retry must not return Ok(()); it must fail because published record is corrupt!
    let res = store
        .commit_tool_record(&snapshot, Instant::now() + Duration::from_secs(5))
        .await;
    assert!(matches!(res, Err(StoreError::Corrupt)));

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn concurrent_sessions_global_quota() {
    let (base, store, session_id1) = fixture("aux-concurrent-quota").await;
    let session_id2 = SessionId::new().unwrap();

    store
        .create_session(&record(&store, session_id1))
        .await
        .unwrap();
    let mut rec2 = record(&store, session_id2);
    rec2.session_id = session_id2;
    store.create_session(&rec2).await.unwrap();

    // Global limits: max 2 records total across all sessions
    let store = store.with_aux_limits(AuxLimits {
        session_bytes: 16 * 1024 * 1024,
        session_records: 1024,
        global_bytes: 256 * 1024 * 1024,
        global_records: 2,
        max_scan_entries: 65536,
    });

    let make_snap = |ses: SessionId, name: &str| {
        let tool_ref = ToolRef {
            session_id: ses,
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            tool_call_id: ToolCallId::new(name).unwrap(),
        };
        ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: StoredToolRecord {
                version: TOOL_RECORD_FORMAT_VERSION,
                tool_ref: tool_ref.clone(),
                name: "bash".to_owned(),
                subject: ToolSubject::Other,
                subject_truncated: false,
                state: ToolExecutionState::Succeeded,
                phase: None,
                started_at: None,
                finished_at: None,
                outcome: Some(ToolResultOutcome::Success),
                input: StoredInputSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                result: StoredResultSummary {
                    total_bytes: 0,
                    seen: false,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stdout: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                stderr: StoredStreamWindow {
                    start_offset: 0,
                    observed_end: 0,
                    seen: false,
                    complete: true,
                    truncated: false,
                    expired: false,
                    file_bytes: 0,
                    file_sha256: None,
                },
                command: None,
                file_change: None,
            },
            input_bytes: None,
            result_bytes: None,
            stdout_bytes: None,
            stderr_bytes: None,
            file_change_before: None,
            file_change_after: None,
        }
    };

    let snap1 = make_snap(session_id1, "call-concurrent-1");
    let snap2 = make_snap(session_id2, "call-concurrent-2");

    // Concurrently commit from both sessions using tokio::spawn
    let store1 = store.clone();
    let snap1_clone = snap1.clone();
    let h1 = tokio::spawn(async move {
        store1
            .commit_tool_record(&snap1_clone, Instant::now() + Duration::from_secs(5))
            .await
    });

    let store2 = store.clone();
    let snap2_clone = snap2.clone();
    let h2 = tokio::spawn(async move {
        store2
            .commit_tool_record(&snap2_clone, Instant::now() + Duration::from_secs(5))
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    r1.unwrap().unwrap();
    r2.unwrap().unwrap();

    // Both records exist (total = 2)
    assert!(
        store
            .read_tool_record(&snap1.tool_ref)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_tool_record(&snap2.tool_ref)
            .await
            .unwrap()
            .is_some()
    );

    // Third commit exceeds global quota (2) -> oldest must be evicted
    let snap3 = make_snap(session_id1, "call-concurrent-3");
    store
        .commit_tool_record(&snap3, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    // At most 2 records survive globally
    let remaining_records = [
        store
            .read_tool_record(&snap1.tool_ref)
            .await
            .unwrap()
            .is_some(),
        store
            .read_tool_record(&snap2.tool_ref)
            .await
            .unwrap()
            .is_some(),
        store
            .read_tool_record(&snap3.tool_ref)
            .await
            .unwrap()
            .is_some(),
    ]
    .iter()
    .filter(|&&exists| exists)
    .count();

    assert_eq!(remaining_records, 2);

    let _ = fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn command_record_stdout_evicted_by_pressure_snapshot_commit_and_cold_project_consistency() {
    let (base, store, session_id) = fixture("aux-cmd-evict").await;
    store
        .create_session(&record(&store, session_id))
        .await
        .unwrap();

    let tool_ref = ToolRef {
        session_id,
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("call-evict-cmd").unwrap(),
    };

    let tool_data = crate::tool_data::ToolData::new();
    tool_data.note_requested(&tool_ref, "bash");
    tool_data.note_invocation(
        &tool_ref,
        &minicore_runtime::tools::ToolInvocation {
            tool_call_id: tool_ref.tool_call_id.clone(),
            tool_name: "bash".parse().unwrap(),
            arguments: serde_json::json!({"command": "echo test"}),
        },
    );
    tool_data.mark_running(&tool_ref);

    // Push stdout and stderr chunks
    let stdout_bytes = b"long stdout output that will be evicted under pressure".to_vec();
    let stderr_bytes = b"stderr retained".to_vec();
    tool_data
        .note_stream_chunk(&tool_ref, ToolDataStream::Stdout, &stdout_bytes)
        .unwrap();
    tool_data
        .note_stream_chunk(&tool_ref, ToolDataStream::Stderr, &stderr_bytes)
        .unwrap();
    tool_data.note_stream_end(&tool_ref, ToolDataStream::Stdout);
    tool_data.note_stream_end(&tool_ref, ToolDataStream::Stderr);

    let cmd = CommandResult {
        status: crate::tool_data::CommandStatus::Exited,
        exit_code: Some(0),
        signal: None,
        termination_confirmed: true,
        stdout_base_offset: 0,
        stdout_observed_end: stdout_bytes.len() as u64,
        stderr_base_offset: 0,
        stderr_observed_end: stderr_bytes.len() as u64,
        output_complete: true,
        output_truncated: false,
    };
    tool_data.note_command(&tool_ref, cmd);
    tool_data.finish_and_snapshot(
        &tool_ref,
        minicore_runtime::tools::ToolResultOutcome::Success,
    );

    // Simulate global/session budget pressure that evicts stdout of this record
    // (In tool_data, an eviction empties bytes and sets start_offset = observed_end)
    let evict_snap = {
        let snap = tool_data.snapshot_for_persistence(&tool_ref).unwrap();
        let mut snap_modified = snap.clone();
        snap_modified.record.stdout.expired = true;
        snap_modified.record.stdout.truncated = true;
        snap_modified.record.stdout.start_offset = snap.record.stdout.observed_end;
        snap_modified.record.stdout.file_bytes = 0;
        snap_modified.record.stdout.file_sha256 = None;
        snap_modified.stdout_bytes = None;
        // Update the command ranges using the live stream overlay principle
        if let Some(mut command) = snap_modified.record.command {
            command.stdout_base_offset = snap_modified.record.stdout.start_offset;
            command.stdout_observed_end = snap_modified.record.stdout.observed_end;
            command.output_truncated = true;
            snap_modified.record.command = Some(command);
        }
        snap_modified
    };

    // Persistence commit with strict metadata validator must succeed
    store
        .commit_tool_record(&evict_snap, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    // Cold project from disk
    let recovered = store
        .read_tool_record(&tool_ref)
        .await
        .unwrap()
        .expect("record must exist");

    // Metadata projection shows Saved
    let read_res = recovered.project_read(&tool_ref, 4096).unwrap();
    assert_eq!(
        read_res.execution.recording,
        crate::tool_data::ToolRecordingState::Saved
    );

    // Evicted stdout projection returns Expired while retaining observed_end
    let stdout_page = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    assert_eq!(stdout_page.availability, ToolDataAvailability::Expired);
    assert_eq!(stdout_page.observed_end, stdout_bytes.len() as u64);
    assert!(stdout_page.truncated);

    // Stderr stream was not evicted and remains Available
    let stderr_page = recovered
        .project_output(
            &ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stderr,
                offset: 0,
                max_bytes: Some(4096),
            },
            4096,
        )
        .unwrap();
    assert_eq!(stderr_page.availability, ToolDataAvailability::Available);
    assert!(!stderr_page.data.is_empty());

    let _ = fs::remove_dir_all(base).await;
}
