use super::*;
use crate::agent::StartedTurn;
use crate::sessions::await_compaction_completion;
use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{ModelErrorKind, ModelRequest};
use std::time::Duration;

const STEER: InputMode = InputMode::Steer;
const FOLLOW_UP: InputMode = InputMode::FollowUp;

fn user_texts(request: &ModelRequest) -> Vec<String> {
    request
        .messages()
        .iter()
        .filter_map(|message| match message {
            ModelMessage::User(text) => {
                let text: &str = text.as_ref();
                Some(text.to_owned())
            }
            _ => None,
        })
        .collect()
}

fn user_items(items: &[HistoryItem]) -> Vec<(String, UserMessageKind)> {
    items
        .iter()
        .filter_map(|item| match item {
            HistoryItem::User(user) => Some((user.input.as_text().to_owned(), user.kind)),
            _ => None,
        })
        .collect()
}

fn queue_texts(agent: &Agent, session_id: SessionId) -> Vec<String> {
    agent
        .list_inputs(session_id)
        .unwrap()
        .items
        .into_iter()
        .map(|item| item.text)
        .collect()
}

/// Waits until the gated model call has started, bounded so a lost wake-up
/// fails instead of hanging.
async fn started(gate: &Gate) {
    tokio::time::timeout(Duration::from_secs(10), gate.wait_started())
        .await
        .expect("the gated model call must start");
}

/// Waits for `turn`, bounded so a lost wake-up fails instead of hanging.
async fn wait_bounded(agent: &Agent, turn: TurnRef) -> Arc<crate::sessions::TurnResult> {
    tokio::time::timeout(Duration::from_secs(10), agent.wait_turn(turn))
        .await
        .expect("the turn must finish")
        .unwrap()
}

fn next_turn(result: &crate::sessions::TurnResult) -> StartedTurn {
    result.next.clone().expect("the queue started a successor")
}

fn failing_step() -> Step {
    Step::Fail(ModelError::started(
        ModelErrorKind::Internal,
        DiagnosticSummary::new(
            DiagnosticCode::Internal,
            DiagnosticCategory::Internal,
            BoundedText::new("fake model failure").unwrap(),
            false,
        )
        .unwrap(),
    ))
}

#[tokio::test]
async fn a_running_turn_takes_every_steer_and_follow_ups_then_run_one_turn_each() {
    let fixture = AgentFixture::new("inputs-order").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::new(
        "main",
        [
            Step::gated_tool(gate.clone(), "read", json!({"path": "a.txt"})),
            Step::Text("answer"),
            Step::Text("answer f1"),
            Step::Text("answer f2"),
        ],
    )
    .with_pacing(Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let data_dir = fixture.data_dir.clone();
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &fixture.workspace).await;
    let id = info.session_id;

    let turn = send_text(&mut agent, id, "main prompt").await;
    started(&gate).await;
    // F1 N1 F2 S1 N2
    let f1 = push(&agent, id, FOLLOW_UP, "f1").await;
    let n1 = push(&agent, id, STEER, "n1").await;
    let f2 = push(&agent, id, FOLLOW_UP, "f2").await;
    let s1 = push(&agent, id, STEER, "s1").await;
    let n2 = push(&agent, id, STEER, "n2").await;
    for pushed in [&f1, &n1, &f2, &s1, &n2] {
        assert!(pushed.started.is_none(), "a running turn starts nothing");
    }
    assert_eq!(queue_texts(&agent, id), ["n1", "s1", "n2", "f1", "f2"]);
    gate.release();

    let first = wait_bounded(&agent, turn).await;
    assert_completed(&first);
    let second = next_turn(&first);
    assert_eq!(second.input.id, f1.id);
    let second_result = wait_bounded(&agent, second.turn).await;
    let third = next_turn(&second_result);
    assert_eq!(third.input.id, f2.id);
    let third_result = wait_bounded(&agent, third.turn).await;
    assert_eq!(third_result.next, None);
    settle_turn(&agent, id).await;

    {
        let requests = model.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(user_texts(&requests[0]), ["main prompt"]);
        assert_eq!(user_texts(&requests[1]), ["main prompt", "n1", "s1", "n2"]);
        assert_eq!(user_texts(&requests[2]).last().unwrap(), "f1");
        assert_eq!(user_texts(&requests[3]).last().unwrap(), "f2");
    }

    assert_eq!(
        user_items(&read_store_history(&data_dir, id).await),
        [
            ("main prompt".to_owned(), UserMessageKind::Prompt),
            ("n1".to_owned(), UserMessageKind::Steering),
            ("s1".to_owned(), UserMessageKind::Steering),
            ("n2".to_owned(), UserMessageKind::Steering),
            ("f1".to_owned(), UserMessageKind::Prompt),
            ("f2".to_owned(), UserMessageKind::Prompt),
        ]
    );
    assert!(queue_texts(&agent, id).is_empty());

    let delivered = events
        .expect(
            Duration::from_secs(5),
            "input_delivered must arrive",
            |event| match event {
                AgentEvent::InputDelivered { turn, items, .. } => Some((turn, items)),
                _ => None,
            },
        )
        .await;
    assert_eq!(delivered.0, turn);
    let ids = delivered.1.iter().map(|item| item.id.clone());
    assert_eq!(ids.collect::<Vec<_>>(), [n1.id, s1.id, n2.id]);
}

#[tokio::test]
async fn the_successor_is_running_before_the_finished_turns_waiter_resolves() {
    let gate_one = Arc::new(Gate::new());
    let gate_two = Arc::new(Gate::new());
    let fixture = AgentFixture::new("inputs-installed").script([
        Step::gated_text(gate_one.clone(), "one"),
        Step::gated_text(gate_two.clone(), "two"),
    ]);
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "first").await;
    started(&gate_one).await;
    push(&agent, id, FOLLOW_UP, "second").await;
    gate_one.release();

    let result = wait_bounded(&agent, turn).await;
    let next = next_turn(&result);
    // The successor's gated model call keeps it alive, so it is the active
    // loop at the moment the first waiter resolves.
    let state = agent.session_state(id).unwrap();
    assert_eq!(
        state.active_loop.map(|state| state.loop_id),
        Some(next.turn.loop_id)
    );
    // The replaced turn stays waitable for a late `turn.wait`.
    assert_eq!(wait_bounded(&agent, turn).await.next, result.next);

    started(&gate_two).await;
    gate_two.release();
    assert_eq!(wait_bounded(&agent, next.turn).await.next, None);
    settle_turn(&agent, id).await;
}

#[tokio::test]
async fn the_oldest_steer_starts_first_and_the_remaining_steers_ride_along() {
    let gate = Arc::new(Gate::new());
    let fixture = AgentFixture::new("inputs-head").script([
        Step::gated_text(gate.clone(), "one"),
        Step::Text("two"),
        Step::Text("three"),
    ]);
    let data_dir = fixture.data_dir.clone();
    let model = fixture.model("main");
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "first").await;
    started(&gate).await;
    // A final answer takes no inputs, so all three wait for the successor.
    push(&agent, id, FOLLOW_UP, "f1").await;
    push(&agent, id, STEER, "s1").await;
    push(&agent, id, STEER, "s2").await;
    gate.release();

    let first = wait_bounded(&agent, turn).await;
    let second = next_turn(&first);
    assert_eq!(
        second.input.text, "s1",
        "a steer overtakes the older follow-up"
    );
    let second_result = wait_bounded(&agent, second.turn).await;
    let third = next_turn(&second_result);
    assert_eq!(third.input.text, "f1");
    wait_bounded(&agent, third.turn).await;
    settle_turn(&agent, id).await;

    {
        let requests = model.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(user_texts(&requests[1]), ["first", "s1", "s2"]);
    }
    assert_eq!(
        user_items(&read_store_history(&data_dir, id).await),
        [
            ("first".to_owned(), UserMessageKind::Prompt),
            ("s1".to_owned(), UserMessageKind::Prompt),
            ("s2".to_owned(), UserMessageKind::Steering),
            ("f1".to_owned(), UserMessageKind::Prompt),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_pushes_to_an_idle_session_start_exactly_one_turn() {
    let gate = Arc::new(Gate::new());
    let fixture =
        AgentFixture::new("inputs-concurrent").script([Step::gated_text(gate.clone(), "one")]);
    let (agent, info) = fixture.start().await;
    let id = info.session_id;
    let session = agent.loaded_session(id).unwrap();

    let pushes = (0..8)
        .map(|index| {
            let session = session.clone();
            tokio::spawn(async move {
                session
                    .accept_input(STEER, format!("push {index}"))
                    .unwrap()
                    .start()
                    .await
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut pushed = Vec::new();
    for push in pushes {
        pushed.push(push.await.unwrap());
    }
    let starts = pushed
        .iter()
        .filter(|pushed| pushed.started.is_some())
        .count();
    assert_eq!(starts, 1, "only one push may start the turn");

    started(&gate).await;
    gate.release();
    let mut turn = pushed
        .iter()
        .find_map(|pushed| pushed.started.clone())
        .unwrap()
        .turn;
    // Every other entry follows as a delivered steer or its own turn.
    loop {
        let result = wait_bounded(&agent, turn).await;
        match result.next.clone() {
            Some(next) => turn = next.turn,
            None => break,
        }
    }
    settle_turn(&agent, id).await;
    let history = history_page(&agent, id, 100);
    let users = history
        .items
        .iter()
        .filter(|item| matches!(item.item, crate::history::HistoryItemView::User(_)))
        .count();
    assert_eq!(users, 8, "every push reached the model exactly once");
    assert!(queue_texts(&agent, id).is_empty());
    agent.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_while_the_first_turn_is_starting_is_queued_and_never_lost() {
    let fixture =
        AgentFixture::new("inputs-starting").script([Step::Text("one"), Step::Text("two")]);
    let data_dir = fixture.data_dir.clone();
    let (agent, info) = fixture.start().await;
    let id = info.session_id;
    let gate = Arc::new(Gate::new());
    // A failed assertion must not leave the starting thread parked.
    struct Release(Arc<Gate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let _release = Release(Arc::clone(&gate));
    PAUSE_AFTER_RUNTIME_START.arm(id, Arc::clone(&gate));
    let session = agent.loaded_session(id).unwrap();
    let starter = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .accept_input(STEER, "starter".to_owned())
                .unwrap()
                .start()
                .await
                .unwrap()
        })
    };
    started(&gate).await;
    // The loop runs in the Runtime but is not installed yet, so the push may
    // queue or be taken by that loop already, never start a second one.
    let queued = session
        .accept_input(STEER, "during start".to_owned())
        .unwrap();
    assert!(!queued.needs_start());
    queued.finish(None);
    gate.release();

    let started = starter.await.unwrap().started.unwrap();
    let mut turn = started.turn;
    while let Some(next) = wait_bounded(&agent, turn).await.next.clone() {
        turn = next.turn;
    }
    settle_turn(&agent, id).await;
    let texts = flatten_texts(&read_store_history(&data_dir, id).await);
    assert_eq!(
        texts.iter().filter(|text| *text == "during start").count(),
        1
    );
    assert!(queue_texts(&agent, id).is_empty());
    agent.shutdown().await.unwrap();
}

/// The fixture owns the temporary store, so it must outlive the test.
async fn paused_after_cancel(label: &str) -> (AgentFixture, Agent, SessionId, Arc<ScriptedModel>) {
    let gate = Arc::new(Gate::new());
    let fixture = AgentFixture::new(label).script([
        Step::gated_text(gate.clone(), "one"),
        Step::Text("two"),
        Step::Text("three"),
    ]);
    let model = fixture.model("main");
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "first").await;
    started(&gate).await;
    push(&agent, id, FOLLOW_UP, "f1").await;
    push(&agent, id, FOLLOW_UP, "f2").await;
    assert!(agent.cancel(turn).unwrap());
    let result = wait_bounded(&agent, turn).await;
    assert_eq!(
        crate::event::LoopOutcomeView::from_report(&result.report),
        crate::event::LoopOutcomeView::Cancelled {
            reason: crate::event::CancelReasonView::User
        }
    );
    assert_eq!(result.next, None, "a cancelled turn starts nothing");
    gate.release();
    (fixture, agent, id, model)
}

#[tokio::test]
async fn a_cancelled_turn_pauses_the_queue_and_resume_starts_the_head() {
    let (_fixture, agent, id, model) = paused_after_cancel("inputs-cancel-resume").await;
    settle_turn(&agent, id).await;
    let queue = agent.list_inputs(id).unwrap();
    assert!(queue.paused);
    assert_eq!(queue_texts(&agent, id), ["f1", "f2"]);
    assert_eq!(model.calls(), 1, "nothing starts while paused");
    assert_eq!(
        agent.session_state(id).unwrap().status,
        crate::sessions::SessionStatus::Idle
    );

    let resumed = agent.resume_inputs(id).await.unwrap();
    let started = resumed.started.expect("resume starts the head");
    assert_eq!(started.input.text, "f1");
    assert!(!resumed.queue.paused);
    let result = wait_bounded(&agent, started.turn).await;
    assert_eq!(next_turn(&result).input.text, "f2");
}

#[tokio::test]
async fn a_push_while_paused_clears_the_pause_and_starts_the_older_entry() {
    let (_fixture, agent, id, _model) = paused_after_cancel("inputs-cancel-push").await;
    settle_turn(&agent, id).await;
    assert!(agent.list_inputs(id).unwrap().paused);

    let pushed = push(&agent, id, FOLLOW_UP, "f3").await;
    let started = pushed.started.expect("a push while paused starts the head");
    assert_eq!(
        started.input.text, "f1",
        "queue order puts older entries first"
    );
    assert!(!pushed.queue.paused);
    assert_eq!(
        pushed
            .queue
            .items
            .iter()
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>(),
        ["f2", "f3"]
    );
}

#[tokio::test]
async fn a_failed_turn_pauses_the_queue() {
    let gate = Arc::new(Gate::new());
    let fixture = AgentFixture::new("inputs-failed").script([
        Step::Gated(gate.clone(), Box::new(failing_step())),
        Step::Text("two"),
    ]);
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "first").await;
    started(&gate).await;
    push(&agent, id, FOLLOW_UP, "f1").await;
    gate.release();
    let result = wait_bounded(&agent, turn).await;
    assert!(matches!(
        crate::event::LoopOutcomeView::from_report(&result.report),
        crate::event::LoopOutcomeView::Failed { .. }
    ));
    assert_eq!(result.next, None);
    assert!(agent.list_inputs(id).unwrap().paused);
    assert_eq!(queue_texts(&agent, id), ["f1"]);
}

#[tokio::test]
async fn failed_persistence_pauses_the_queue_and_resume_runs_it() {
    let gate = Arc::new(Gate::new());
    let fixture = AgentFixture::new("inputs-persistence")
        .script([Step::gated_text(gate.clone(), "one"), Step::Text("two")]);
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    crate::store::APPEND_FAILURES.arm(id, false);
    let turn = send_text(&mut agent, id, "first").await;
    started(&gate).await;
    push(&agent, id, FOLLOW_UP, "f1").await;
    gate.release();
    let result = wait_bounded(&agent, turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(result.next, None);
    assert!(agent.list_inputs(id).unwrap().paused);
    assert!(agent.session_state(id).unwrap().block_reason.is_none());

    let started = agent.resume_inputs(id).await.unwrap().started.unwrap();
    let resumed = wait_bounded(&agent, started.turn).await;
    assert_persisted(&resumed);
    assert!(queue_texts(&agent, id).is_empty());
}

#[tokio::test]
async fn recall_returns_the_texts_in_run_order_and_close_is_refused_while_queued() {
    let gate = Arc::new(Gate::new());
    let fixture =
        AgentFixture::new("inputs-recall").script([Step::gated_text(gate.clone(), "one")]);
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "first").await;
    started(&gate).await;
    push(&agent, id, FOLLOW_UP, "f1").await;
    push(&agent, id, STEER, "s1").await;
    push(&agent, id, FOLLOW_UP, "f2").await;

    assert!(matches!(
        agent.close_session(id).await,
        Err(AgentError::SessionBusy)
    ));
    assert!(matches!(
        agent.delete_session(id).await,
        Err(AgentError::SessionAlreadyLoaded)
    ));
    assert!(
        agent.session_state(id).unwrap().active_loop.is_some(),
        "a refused close leaves the turn running"
    );

    let recalled = agent.recall_inputs(id).unwrap();
    assert_eq!(recalled.texts, ["s1", "f1", "f2"]);
    assert!(recalled.queue.items.is_empty());
    gate.release();
    let result = wait_bounded(&agent, turn).await;
    assert_eq!(result.next, None);
    agent.close_session(id).await.unwrap();
}

#[tokio::test]
async fn a_steer_pushed_during_an_approval_wait_reaches_the_request_after_the_answer() {
    let fixture = AgentFixture::new("inputs-approval").profile(Profile {
        tools: vec!["write".to_owned()],
        approval: ApprovalMode::Ask,
        ..read_profile()
    });
    let model = ScriptedModel::new(
        "main",
        [
            Step::ToolCall("write", json!({"path": "out.txt", "content": "written"})),
            Step::Text("done"),
        ],
    )
    .with_pacing(Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &fixture.workspace).await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "write it").await;
    let interaction = events
        .expect(
            Duration::from_secs(5),
            "approval request must arrive",
            |event| match event {
                AgentEvent::InteractionRequested { interaction, .. } => Some(interaction),
                _ => None,
            },
        )
        .await;

    let pushed = push(&agent, id, STEER, "also this").await;
    assert!(pushed.started.is_none());
    assert_eq!(
        queue_texts(&agent, id),
        ["also this"],
        "no boundary while waiting"
    );

    agent
        .answer(crate::agent::AnswerInteraction {
            turn,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::interaction::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::AllowOnce,
            ),
        })
        .await
        .unwrap();
    let result = wait_bounded(&agent, turn).await;
    assert_eq!(result.next, None);
    let requests = model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(user_texts(&requests[1]), ["write it", "also this"]);
}

#[tokio::test]
async fn a_push_during_a_manual_compaction_starts_when_it_ends() {
    let fixture = AgentFixture::new("inputs-compaction").file("a.txt", b"hello");
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::with_window(
        "main",
        40_000,
        [
            Step::Text("settled"),
            Step::gated_text(gate.clone(), "summary"),
            Step::Text("queued answer"),
        ],
    )
    .with_pacing(Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]));
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, &"settled history ".repeat(128)).await;
    wait_bounded(&agent, turn).await;
    agent.close_session(id).await.unwrap();
    let tail = synthetic_loop_record(
        LoopId::new().unwrap(),
        &"recent ".repeat(12_000),
        "RECENT_TAIL_ANSWER",
        false,
    );
    agent.store.append_loop(id, &tail).await.unwrap();
    agent.open_session(id).await.unwrap();

    let receiver = agent
        .compact_session(CompactSession {
            session_id: id,
            operation_id: "inputs-compaction".to_owned(),
        })
        .await
        .unwrap();
    started(&gate).await;
    let pushed = push(&agent, id, FOLLOW_UP, "after compaction").await;
    assert!(
        pushed.started.is_none(),
        "nothing starts during a compaction"
    );
    assert_eq!(queue_texts(&agent, id), ["after compaction"]);
    gate.release();

    let completion = tokio::time::timeout(
        Duration::from_secs(10),
        await_compaction_completion(receiver),
    )
    .await
    .expect("the compaction must finish")
    .unwrap();
    assert_eq!(
        completion.result.status,
        crate::compaction::CompactionStatus::Compacted
    );
    let next = completion
        .next
        .expect("the head starts when the compaction ends");
    assert_eq!(next.input.id, pushed.id);
    // The turn was installed before the result was published, so it is known.
    let result = wait_bounded(&agent, next.turn).await;
    assert_persisted(&result);
    assert!(queue_texts(&agent, id).is_empty());
}

#[tokio::test]
async fn delivered_and_initial_inputs_keep_their_acceptance_time_as_the_user_timestamp() {
    let fixture = AgentFixture::new("inputs-times").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let gate = Arc::new(Gate::new());
    let fixture = fixture.script([
        Step::gated_tool(gate.clone(), "read", json!({"path": "a.txt"})),
        Step::Text("final"),
        Step::Text("follow-up answer"),
    ]);
    let (mut agent, info) = fixture.start().await;
    let id = info.session_id;
    let turn = send_text(&mut agent, id, "main prompt").await;
    started(&gate).await;
    let one = push(&agent, id, STEER, "steer one").await;
    let two = push(&agent, id, STEER, "steer two").await;
    let later = push(&agent, id, FOLLOW_UP, "later").await;
    gate.release();

    let result = wait_bounded(&agent, turn).await;
    assert_persisted(&result);
    let next = next_turn(&result);
    assert_eq!(next.input.accepted_at, later.accepted_at);
    wait_bounded(&agent, next.turn).await;
    settle_turn(&agent, id).await;

    let page = history_page(&agent, id, 100);
    let timestamps = page
        .items
        .iter()
        .filter_map(|item| match &item.item {
            crate::history::HistoryItemView::User(user) => user.timestamp.clone(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(timestamps.len(), 4);
    assert!(timestamps.iter().all(|timestamp| timestamp.ends_with('Z')));
    assert_eq!(timestamps[1], one.accepted_at.clone().unwrap());
    assert_eq!(timestamps[2], two.accepted_at.clone().unwrap());
    assert_eq!(timestamps[3], later.accepted_at.clone().unwrap());

    // Paging keeps the loop-local occurrence instead of restarting at zero;
    // item 3 is the first delivered steer, after the tool exchange.
    let second_page = agent
        .history(GetHistory {
            session_id: id,
            offset: 3,
            limit: 1,
        })
        .unwrap();
    let crate::history::HistoryItemView::User(second_user) = &second_page.items[0].item else {
        panic!("expected the second page to contain a User item");
    };
    assert_eq!(
        second_user.timestamp.as_deref(),
        Some(timestamps[1].as_str())
    );

    let loaded = Store::open(data_dir)
        .await
        .unwrap()
        .load_session(id)
        .await
        .unwrap();
    assert_eq!(loaded.user_times.len(), 4);
    assert_eq!(
        loaded
            .user_times
            .keys()
            .filter(|(loop_id, _)| *loop_id == turn.loop_id)
            .count(),
        3
    );
}

#[tokio::test]
async fn a_stale_turn_ref_is_rejected() {
    let fixture = AgentFixture::new("inputs-stale")
        .file("a.txt", b"hello")
        .script([Step::Text("one"), Step::Text("two")]);
    let (mut agent, info) = fixture.start().await;
    let first = send_text(&mut agent, info.session_id, "one").await;
    wait_text(&agent, first).await;
    let second = send_text(&mut agent, info.session_id, "two").await;
    wait_text(&agent, second).await;

    assert!(matches!(
        agent.wait_turn(first).await,
        Err(AgentError::TurnNotFound)
    ));
    assert!(matches!(agent.cancel(first), Err(AgentError::TurnNotFound)));
}
