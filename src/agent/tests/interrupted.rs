use super::*;
use crate::read::{InterruptedRequest, PartialStopReason, ReadView, TurnResultRequest};

fn canonical_read(session_id: SessionId) -> ReadSession {
    ReadSession {
        view: ReadView::Canonical,
        ..display_read(session_id)
    }
}

fn delta(text: &str) -> Result<ModelEvent, ModelError> {
    Ok(ModelEvent::text_delta(text).unwrap())
}

/// Streams `events`, then hangs until the gate is released.
fn hanging(events: Vec<Result<ModelEvent, ModelError>>, gate: &Arc<Gate>) -> Step {
    Step::PreviewEvents(events, Some(Arc::clone(gate)))
}

async fn cancel_hanging_turn(
    agent: &mut Agent,
    session_id: SessionId,
    gate: &Gate,
) -> (TurnRef, Arc<crate::sessions::TurnResult>) {
    let turn = send_text(agent, session_id, "cancel me").await;
    gate.wait_started().await;
    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert_persisted(&result);
    (turn, result)
}

#[tokio::test]
async fn a_cancelled_stream_is_readable_after_reopen_but_never_reaches_a_model() {
    let gate = Arc::new(Gate::new());
    let call_id = ToolCallId::new("call-partial").unwrap();
    let model = ScriptedModel::new(
        "main",
        [
            hanging(
                vec![
                    Ok(ModelEvent::reasoning_delta("weighing it").unwrap()),
                    delta("Let me che"),
                    delta("ck"),
                    Ok(ModelEvent::ToolCallStart {
                        tool_call_id: call_id.clone(),
                        tool_name: "read".parse().unwrap(),
                    }),
                    Ok(
                        ModelEvent::tool_call_arguments_delta(call_id, "{\"path\":\"PARTIAL-ARGS")
                            .unwrap(),
                    ),
                ],
                &gate,
            ),
            Step::Text("fresh answer"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let requests = model.requests();
    let fixture = AgentFixture::new("interrupted-partial")
        .file("a.txt", b"hello")
        .models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let session_id = info.session_id;

    let (turn, result) = cancel_hanging_turn(&mut agent, session_id, &gate).await;
    let expected = InterruptedRequest {
        stop_reason: PartialStopReason::Aborted,
        request_index: 0,
        model: "main".to_owned(),
        reasoning: "weighing it".to_owned(),
        text: "Let me check".to_owned(),
    };
    assert_eq!(result.interrupted.as_ref(), Some(&expected));
    let turn_request = TurnResultRequest {
        turn,
        cursor: None,
        limit: 100,
        max_bytes: None,
    };
    let live = agent.turn_result(turn_request.clone()).await.unwrap();
    assert_eq!(live.availability, crate::TurnResultAvailability::Live);
    assert_eq!(live.interrupted.as_ref(), Some(&expected));

    // The next turn runs the same Session; its request must not see the partial.
    let next = send_text(&mut agent, session_id, "next").await;
    let next_result = wait_text(&agent, next).await;
    assert_completed(&next_result);
    let sent = serde_json::to_string(requests.lock().unwrap()[1].messages()).unwrap();
    assert!(sent.contains("cancel me") && sent.contains("next"));
    for partial in ["Let me che", "weighing it", "PARTIAL-ARGS"] {
        assert!(!sent.contains(partial), "{partial} reached the model");
    }

    let stored = agent.turn_result(turn_request).await.unwrap();
    assert_eq!(stored.availability, crate::TurnResultAvailability::Stored);
    assert_eq!(stored.interrupted.as_ref(), Some(&expected));
    let before = agent
        .read_session(canonical_read(session_id))
        .await
        .unwrap();
    assert_eq!(before.total, 3, "the partial is not a history item");
    assert_eq!(before.records[0].interrupted.as_ref(), Some(&expected));
    assert_eq!(before.records[1].interrupted, None);
    let history_before = history_page(&agent, session_id, 100).total;

    agent.close_session(session_id).await.unwrap();
    let cold = agent
        .read_session(canonical_read(session_id))
        .await
        .unwrap();
    drop(agent);
    let mut agent = fixture.open().await;
    agent.open_session(session_id).await.unwrap();
    let reopened = agent
        .read_session(canonical_read(session_id))
        .await
        .unwrap();
    for read in [&cold, &reopened] {
        assert_eq!(read.items, before.items);
        assert_eq!(read.records, before.records);
        assert_eq!(read.history_revision, before.history_revision);
    }
    assert_eq!(history_page(&agent, session_id, 100).total, history_before);
    let history = serde_json::to_string(&read_store_history(&fixture.data_dir, session_id).await);
    assert!(!history.unwrap().contains("Let me che"));
}

#[tokio::test]
async fn a_stream_without_visible_output_leaves_no_partial() {
    let gate = Arc::new(Gate::new());
    let call_id = ToolCallId::new("call-silent").unwrap();
    let fixture = AgentFixture::new("interrupted-silent")
        .file("a.txt", b"hello")
        .models(BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new(
                "main",
                [hanging(
                    vec![Ok(ModelEvent::ToolCallStart {
                        tool_call_id: call_id,
                        tool_name: "read".parse().unwrap(),
                    })],
                    &gate,
                )],
            )
            .with_pacing(std::time::Duration::from_millis(2)),
        )]));
    let (mut agent, info) = fixture.start().await;

    let (_, result) = cancel_hanging_turn(&mut agent, info.session_id, &gate).await;
    assert_eq!(result.interrupted, None);
    let read = agent
        .read_session(canonical_read(info.session_id))
        .await
        .unwrap();
    assert_eq!(read.records[0].interrupted, None);
}

#[tokio::test]
async fn the_kept_partial_is_the_head_of_the_stream_within_its_bound() {
    let gate = Arc::new(Gate::new());
    let chunks = ["a", "b", "c", "d", "e"].map(|letter| letter.repeat(4000));
    let all = chunks.concat();
    let fixture = AgentFixture::new("interrupted-bound")
        .file("a.txt", b"hello")
        .models(BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new(
                "main",
                [hanging(
                    chunks.iter().map(|chunk| delta(chunk)).collect(),
                    &gate,
                )],
            )
            .with_pacing(std::time::Duration::from_millis(2)),
        )]));
    let (mut agent, info) = fixture.start().await;

    let (_, result) = cancel_hanging_turn(&mut agent, info.session_id, &gate).await;
    let interrupted = result.interrupted.clone().unwrap();
    assert_eq!(interrupted.text, all[..16 * 1024]);
    let read = agent
        .read_session(canonical_read(info.session_id))
        .await
        .unwrap();
    assert_eq!(read.records[0].interrupted.as_ref(), Some(&interrupted));
}

#[tokio::test]
async fn cancelling_during_tool_execution_records_no_partial() {
    let fixture = AgentFixture::new("interrupted-tool").file("a.txt", b"hello");
    let workspace = fixture.workspace.clone();
    // The completed request streamed text too: it is already a history item.
    let model = ScriptedModel::new(
        "main",
        [Step::BurstTextThenTool(
            "streamed before the tool ",
            2,
            "bash",
            json!({"command": "sleep 30"}),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), model)]))
        .profile(bash_profile());
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run slowly").await;
    events
        .expect(
            std::time::Duration::from_secs(2),
            "tool start event must arrive",
            |event| {
                matches!(event, AgentEvent::ToolStarted { turn: started, .. } if started == turn)
                    .then_some(())
            },
        )
        .await;
    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert_eq!(result.interrupted, None);
    let read = agent
        .read_session(canonical_read(info.session_id))
        .await
        .unwrap();
    assert_eq!(read.records[0].interrupted, None);
    let history = std::fs::read_to_string(
        fixture
            .data_dir
            .join("sessions")
            .join(info.session_id.to_string())
            .join("history.jsonl"),
    )
    .unwrap();
    let record: serde_json::Value = serde_json::from_str(history.lines().next().unwrap()).unwrap();
    assert!(record.get("interrupted").is_none());
}
