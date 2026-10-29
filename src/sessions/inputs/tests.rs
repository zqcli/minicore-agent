use super::*;
use crate::test_support::TempDir;

/// A loaded Session whose queue pretends a turn runs, so pushes only queue.
async fn busy_session() -> (
    TempDir,
    crate::Agent,
    Session,
    crate::event::AgentEventStream,
) {
    let directory = TempDir::new("inputs-queue");
    let config = serde_json::from_value(serde_json::json!({
        "data_dir": directory.path().join("data"), "default_profile": "test",
        "profiles": {"test": {"model":"main", "reasoning":"auto", "system_prompt":"test", "tools":[], "approval":"auto"}},
        "models": {"main": {"provider":"open_ai_responses", "model":"test", "base_url":"https://example.invalid/v1", "api_key_env":"PATH",
            "physical_context_window":32000, "output_budget_tokens":2000, "safety_margin_tokens":1000, "supported_reasoning":["auto"], "supports_tools":true}},
        "compaction":{"enabled":false}
    })).unwrap();
    let mut agent = crate::Agent::open(config).await.unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(crate::CreateSession {
            workspace: directory.path().into(),
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let session = agent.loaded_session(info.session_id).unwrap();
    let mut queue = session.lock_queue();
    queue.running = true;
    queue.generation = 1;
    queue.binding = Some(1);
    drop(queue);
    (directory, agent, session, events)
}

fn push(session: &Session, mode: InputMode, text: &str) -> InputPushed {
    let pending = session.accept_input(mode, text.to_owned()).unwrap();
    assert!(!pending.needs_start());
    pending.finish(None)
}

fn texts(queue: &Queue) -> Vec<&str> {
    queue.items.iter().map(|item| item.text.as_str()).collect()
}

fn inputs(session: &Session, generation: u64) -> SessionInputs {
    SessionInputs::new(session.clone(), session.presentation(), generation)
}

fn taken(inputs: &[UserInput]) -> Vec<&str> {
    inputs.iter().map(UserInput::as_text).collect()
}

#[tokio::test]
async fn the_queue_lists_steers_first_and_keeps_acceptance_order_within_a_mode() {
    let (_directory, agent, session, _events) = busy_session().await;
    push(&session, InputMode::FollowUp, "f1");
    push(&session, InputMode::Steer, "n1");
    push(&session, InputMode::FollowUp, "f2");
    push(&session, InputMode::Steer, "s1");
    let pushed = push(&session, InputMode::Steer, "n2");
    assert_eq!(texts(&pushed.queue), ["n1", "s1", "n2", "f1", "f2"]);
    assert_eq!(texts(&session.queue()), ["n1", "s1", "n2", "f1", "f2"]);
    let ids = session
        .queue()
        .items
        .iter()
        .map(|item| item.id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), 5, "ids are unique");
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_queue_holds_at_most_the_bound_and_refuses_the_next_push() {
    let (_directory, agent, session, _events) = busy_session().await;
    for index in 0..MAX_QUEUED_INPUTS {
        push(&session, InputMode::FollowUp, &format!("f{index}"));
    }
    assert!(matches!(
        session.accept_input(InputMode::Steer, "one too many".to_owned()),
        Err(AgentError::InputQueueFull)
    ));
    assert_eq!(session.queue().items.len(), MAX_QUEUED_INPUTS);
    let first = session.queue().items[0].id.clone();
    session.remove_input(&first).unwrap();
    push(&session, InputMode::Steer, "fits again");
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_and_oversized_text_is_invalid_input() {
    let (_directory, agent, session, _events) = busy_session().await;
    for text in [String::new(), "x".repeat(BoundedText::MAX_BYTES + 1)] {
        assert!(matches!(
            session.accept_input(InputMode::Steer, text),
            Err(AgentError::InvalidInput)
        ));
    }
    assert!(session.queue().items.is_empty());
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn take_hands_over_every_steer_in_order_and_leaves_the_follow_ups() {
    let (_directory, agent, session, _events) = busy_session().await;
    let f1 = push(&session, InputMode::FollowUp, "f1");
    let n1 = push(&session, InputMode::Steer, "n1");
    let n2 = push(&session, InputMode::Steer, "n2");
    let loop_inputs = inputs(&session, 1);

    let first = loop_inputs.take();
    assert_eq!(taken(&first), ["n1", "n2"]);
    assert_eq!(texts(&session.queue()), ["f1"]);
    assert!(loop_inputs.take().is_empty(), "handing over is final");
    // Each handed-over input's acceptance time becomes its user timestamp.
    assert_eq!(
        session.presentation().peek_user_times(3),
        vec![n1.accepted_at.clone(), n2.accepted_at.clone()]
    );
    assert_eq!(session.queue().items[0].id, f1.id);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_stale_loops_take_returns_nothing() {
    let (_directory, agent, session, _events) = busy_session().await;
    push(&session, InputMode::Steer, "n1");
    let stale = inputs(&session, 0);
    assert!(stale.take().is_empty(), "another generation owns the queue");
    session.lock_queue().end_period();
    let current = inputs(&session, 1);
    assert!(
        current.take().is_empty(),
        "no loop is bound after the period"
    );
    assert_eq!(texts(&session.queue()), ["n1"]);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn remove_and_take_race_resolves_at_the_queue_lock_either_way() {
    let (_directory, agent, session, _events) = busy_session().await;
    let loop_inputs = inputs(&session, 1);

    // Remove first: the loop never sees the entry.
    let removed = push(&session, InputMode::Steer, "removed first");
    let kept = push(&session, InputMode::Steer, "kept");
    let result = session.remove_input(&removed.id).unwrap();
    assert_eq!(result.text, "removed first");
    assert_eq!(taken(&loop_inputs.take()), ["kept"]);
    assert!(matches!(
        session.remove_input(&kept.id),
        Err(AgentError::InputNotQueued)
    ));

    // Take first: the entry is delivered and can no longer be removed.
    let delivered = push(&session, InputMode::Steer, "taken first");
    assert_eq!(taken(&loop_inputs.take()), ["taken first"]);
    assert!(matches!(
        session.remove_input(&delivered.id),
        Err(AgentError::InputNotQueued)
    ));
    assert!(session.queue().items.is_empty());
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_empty_queue_is_never_paused() {
    let (_directory, agent, session, _events) = busy_session().await;
    let only = push(&session, InputMode::FollowUp, "only");
    session.lock_queue().paused = true;
    assert!(session.queue().paused);
    assert!(!session.remove_input(&only.id).unwrap().queue.paused);

    push(&session, InputMode::FollowUp, "again");
    session.lock_queue().paused = true;
    let recalled = session.recall_inputs();
    assert_eq!(recalled.texts, ["again"]);
    assert!(!recalled.queue.paused);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn inputs_taken_before_the_loop_is_bound_are_announced_in_order_after_bind() {
    let (_directory, agent, session, mut events) = busy_session().await;
    let first = push(&session, InputMode::Steer, "early");
    let loop_inputs = inputs(&session, 1);
    loop_inputs.take();
    let middle = push(&session, InputMode::Steer, "early again");
    loop_inputs.take();
    let turn = TurnRef {
        session_id: session.session_id(),
        loop_id: LoopId::new().unwrap(),
    };
    loop_inputs.bind(turn);
    let second = push(&session, InputMode::Steer, "late");
    loop_inputs.take();

    let mut delivered = Vec::new();
    while let Some(event) = events.try_recv() {
        if let AgentEvent::InputDelivered {
            turn: seen, items, ..
        } = event
        {
            assert_eq!(seen, turn);
            delivered.extend(items.into_iter().map(|item| item.id));
        }
    }
    assert_eq!(delivered, [first.id, middle.id, second.id]);
    agent.shutdown().await.unwrap();
}
