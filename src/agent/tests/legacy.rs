use super::*;

/// S1 public read path: a legacy six-tool record, its historical `subagent`
/// exchange, and its retained auxiliary record must all be readable through
/// the public Agent queries, while `open` must refuse without repairing the
/// deliberately partial history tail.
#[tokio::test]
async fn s1_legacy_history_and_aux_are_readable_through_public_queries() {
    use crate::read::TurnResultAvailability;

    let fixture = AgentFixture::new("s1-public-read").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    let session_id = SessionId::new().unwrap();
    let store = Store::open(data_dir.clone()).await.unwrap();
    let now = "2026-01-02T03:04:05.000Z".to_owned();
    let mut record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: Some("legacy six".to_owned()),
        profile: "test".to_owned(),
        workspace: workspace.clone(),
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    // The Store refuses to *create* the removed tool, so a legacy record is
    // produced the way the old version would have persisted it: create a valid
    // five-tool record, then append the historical sixth name.
    store.create_session(&record).await.unwrap();
    record.tools.push("subagent".to_owned());
    store.write_record(&record).await.unwrap();

    let loop_id = LoopId::new().unwrap();
    let call_id = ToolCallId::new("legacy-subagent-call").unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "subagent".parse().unwrap(),
        json!({"task": "historical child"}),
        0,
    )
    .unwrap();
    let legacy_loop = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("delegate this").unwrap(),
            }),
            HistoryItem::Assistant(AssistantHistory {
                provider_replay: None,
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
                call_id: call_id.clone(),
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
        completed_at: now.clone(),
        user_times: None,
        interrupted: None,
    };
    store.append_loop(session_id, &legacy_loop).await.unwrap();

    // A retained auxiliary record for the historical subagent call.
    let legacy_ref = crate::tool_data::ToolRef {
        session_id,
        loop_id,
        request_index: 0,
        tool_call_id: call_id.clone(),
    };
    let tool_data = ToolData::new();
    tool_data.note_requested(&legacy_ref, "subagent");
    tool_data.note_result(&legacy_ref, "historical auxiliary result");
    tool_data.finish_and_snapshot(&legacy_ref, ToolResultOutcome::Success);
    let snapshot = tool_data.snapshot_for_persistence(&legacy_ref).unwrap();
    store
        .commit_tool_record(
            &snapshot,
            std::time::Instant::now() + std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

    // A partial trailing history line that must never be repaired by a read or
    // by the open precheck.
    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .unwrap();
        file.write_all(b"{\"partial_legacy_tail\": true").unwrap();
    }
    let history_before = std::fs::read(&history_path).unwrap();

    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let mut agent = fixture.open().await;

    // list/read succeed without loading the removed executor.
    let listed = agent.list_sessions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_id, session_id);
    let read = agent
        .read_session(ReadSession {
            view: crate::read::ReadView::Canonical,
            projection_revision: None,
            session_id,
            cursor: None,
            limit: 100,
            max_bytes: None,
            captured_end: None,
            history_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(read.session.session_id, session_id);
    assert_eq!(read.total, 3, "the three stored legacy items are visible");
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

    // turn.result reads the stored legacy exchange, including the subagent name.
    let turn_page = agent
        .turn_result(crate::TurnResultRequest {
            turn: TurnRef {
                session_id,
                loop_id,
            },
            cursor: None,
            limit: 100,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(turn_page.availability, TurnResultAvailability::Stored);
    assert!(
        turn_page
            .items
            .iter()
            .any(|chunk| chunk.data.contains("subagent")
                || chunk.data.contains("historical child result"))
    );

    // tool.read/tool.output serve the legal auxiliary record through the
    // generic path.
    let read_tool = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            display: false,
            tool_ref: legacy_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(read_tool.execution.name, "subagent");
    let output = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: legacy_ref.clone(),
            stream: crate::tool_data::ToolDataStream::Output,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(output.data, "historical auxiliary result");

    // The open precheck refuses before execution and does not repair the tail.
    assert!(matches!(
        agent.open_session(session_id).await,
        Err(AgentError::InvalidSessionSettings)
    ));
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert!(agent.loaded_session(session_id).is_none());
    assert!(model.requests().lock().unwrap().is_empty());
}

/// S1 five-tool subset: a record whose saved tool list is the current five but
/// whose history contains an old `subagent` exchange must still open and run.
#[tokio::test]
async fn s1_five_tool_subset_record_with_old_history_still_executes() {
    let fixture = AgentFixture::new("s1-five-subset").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let workspace = fixture.workspace.clone();
    let session_id = SessionId::new().unwrap();
    let store = Store::open(data_dir.clone()).await.unwrap();
    let now = "2026-01-02T03:04:05.000Z".to_owned();
    let record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: Some("five subset".to_owned()),
        profile: "test".to_owned(),
        workspace: workspace.clone(),
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    store.create_session(&record).await.unwrap();

    let loop_id = LoopId::new().unwrap();
    let call_id = ToolCallId::new("old-subagent-call").unwrap();
    let legacy_loop = StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("old delegate").unwrap(),
            }),
            HistoryItem::Assistant(AssistantHistory {
                provider_replay: None,
                loop_id,
                request_index: 0,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(
                    ToolCall::new(
                        call_id.clone(),
                        "subagent".parse().unwrap(),
                        json!({"task": "old"}),
                        0,
                    )
                    .unwrap(),
                )],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::new(1, 2, 0),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index: 0,
                call_id,
                tool_name: "subagent".parse().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: minicore_runtime::tools::ToolOutput::new("old child result").unwrap(),
            }),
        ],
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 1,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: now,
        user_times: None,
        interrupted: None,
    };
    store.append_loop(session_id, &legacy_loop).await.unwrap();

    let model = ScriptedModel::new("main", [Step::Text("after old history")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let mut agent = fixture.open().await;
    let opened = agent.open_session(session_id).await.unwrap();
    assert!(opened.loaded);
    let turn = send_text(&mut agent, session_id, "continue").await;
    wait_text(&agent, turn).await;
    assert_eq!(model.requests().lock().unwrap().len(), 1);
    agent.close_session(session_id).await.unwrap();
}

/// S1 hallucinated tool: a model request for the removed `subagent` tool in a
/// five-tool Session must become the Runtime's unknown-tool failure, never a
/// dispatched child loop or a new process.
#[tokio::test]
async fn s1_hallucinated_subagent_is_an_unavailable_tool_not_a_dispatch() {
    let fixture = AgentFixture::new("s1-hallucinated").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("subagent", json!({"task": "go delegate"})),
            Step::Text("after hallucination"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "try to delegate").await;
    let result = wait_text(&agent, turn).await;
    // Runtime's existing unknown-tool handling rejects the response at
    // assembly time: a tool call absent from the request schema is a malformed
    // model response, so the loop fails without ever dispatching a tool.
    assert!(
        matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Failed(_)
        ),
        "unexpected outcome: {:?}",
        result.report.outcome
    );
    // Exactly one main-loop request ran; the second scripted request was never
    // reached, and no child model call happened.
    assert_eq!(model.requests().lock().unwrap().len(), 1);

    let stored = read_store_history(&data_dir, info.session_id).await;
    assert!(
        !stored.iter().any(|item| matches!(
            item,
            HistoryItem::ToolResult(result) if result.tool_name.as_str() == "subagent"
        )),
        "a hallucinated tool must not produce a tool result"
    );
    // No ToolRef was recorded: the call never reached a real Tool::execute.
    let refs = history_tool_refs(&agent, info.session_id);
    assert!(refs.is_empty());
}
