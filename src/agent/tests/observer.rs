use super::*;

#[tokio::test]
async fn a_late_watch_publishes_its_initial_interaction_and_closes_with_retained_runtime_handles() {
    let fixture = AgentFixture::new("late-watch")
        .profile(Profile {
            tools: vec!["write".into()],
            approval: ApprovalMode::Ask,
            ..read_profile()
        })
        .script([
            Step::ToolCall("write", json!({"path":"created.txt","content":"body"})),
            Step::Text("done"),
        ]);
    let (mut agent, info) = fixture.start().await;
    let mut events = agent.take_events().unwrap();
    let initial = Arc::new(Gate::new());
    crate::sessions::PAUSE_OBSERVER_INITIAL.arm(info.session_id, initial.clone());
    let turn = send_text(&mut agent, info.session_id, "write").await;
    tokio::time::timeout(std::time::Duration::from_secs(5), initial.wait_started())
        .await
        .unwrap();
    let session = agent.loaded_session(info.session_id).unwrap();
    let handle = session.runtime_handle_for_test();
    let mut state = handle.watch_state();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.wait_for(|state| state.pending_interaction.is_some()),
    )
    .await
    .unwrap()
    .unwrap();
    session.mark_dirty();
    // The install mark must be consumed before the gated observer can mark.
    let first = events
        .expect(
            std::time::Duration::from_secs(5),
            "waiting snapshot",
            |event| match event {
                AgentEvent::SessionSnapshot { snapshot, .. }
                    if snapshot.pending_interaction.is_some() =>
                {
                    Some(snapshot)
                }
                _ => None,
            },
        )
        .await;
    assert_eq!(first.load, session.load());
    session.await_control_idle_for_test().await;
    let revision = session.sample().unwrap().1.revision;
    initial.release();
    let observed = events
        .expect(
            std::time::Duration::from_secs(5),
            "late observer initial value",
            |event| match event {
                AgentEvent::SessionSnapshot { snapshot, .. } if snapshot.revision > revision => {
                    Some(snapshot)
                }
                _ => None,
            },
        )
        .await;
    let pending = observed
        .pending_interaction
        .expect("actionable interaction");
    assert_eq!(pending.loop_id, turn.loop_id);
    assert_eq!(pending.request_index, 0);
    assert_eq!(pending.interaction.tool_name, "write");
    agent
        .answer(crate::AnswerInteraction {
            turn,
            interaction_id: pending.interaction.interaction_id,
            answer: minicore_runtime::interaction::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::AllowOnce,
            ),
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), agent.wait_turn(turn))
        .await
        .unwrap()
        .unwrap();
    let final_state = events
        .expect(
            std::time::Duration::from_secs(5),
            "completed control",
            |event| match event {
                AgentEvent::SessionSnapshot { snapshot, .. }
                    if snapshot.completions.through_seq == 1 =>
                {
                    Some(snapshot)
                }
                _ => None,
            },
        )
        .await;
    assert_eq!(final_state.execution, crate::SessionExecution::None);
    assert!(final_state.pending_interaction.is_none());
    session.join_observer_for_test().await;
    drop(session);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.close_session(info.session_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(handle.is_finished());
    drop(state);
    drop(handle);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn close_publishes_the_final_suffix_and_a_late_sample_cannot_reset_it_after_unregister() {
    let fixture = AgentFixture::new("closed-snapshot").script([Step::Text("done")]);
    let (mut agent, info) = fixture.start().await;
    let mut events = agent.take_events().unwrap();
    let turn = send_text(&mut agent, info.session_id, "first").await;
    tokio::time::timeout(std::time::Duration::from_secs(5), agent.wait_turn(turn))
        .await
        .unwrap()
        .unwrap();
    let old = agent.loaded_session(info.session_id).unwrap();
    old.join_turn_task().await;
    old.await_control_idle_for_test().await;
    let before = old.sample().unwrap().1;
    let before_revision = before.revision;
    assert_eq!(before.completions.through_seq, 1);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.close_session(info.session_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(old.sample(), Err(AgentError::StaleLoad)));
    let mut latest = before;
    loop {
        let event = events
            .next_event(
                std::time::Duration::from_secs(5),
                "closed load final control",
            )
            .await;
        match event {
            AgentEvent::SessionSnapshot { snapshot, .. }
                if snapshot.load == old.load() && snapshot.revision > latest.revision =>
            {
                latest = snapshot
            }
            AgentEvent::SessionClosed { meta, .. } if meta.load == old.load() => break,
            _ => {}
        }
    }
    assert_eq!(latest.completions.through_seq, 1);
    assert!(latest.revision > before_revision);
    assert_eq!(latest.execution, crate::SessionExecution::None);
    assert!(latest.pending_interaction.is_none());
    agent.open_session(info.session_id).await.unwrap();
    let reopened = agent.loaded_session(info.session_id).unwrap();
    assert_ne!(reopened.load(), old.load());
    assert_eq!(reopened.sample().unwrap().1.completions.through_seq, 0);
    assert!(matches!(old.sample(), Err(AgentError::StaleLoad)));
    drop(old);
    drop(reopened);
    agent.shutdown().await.unwrap();
}
