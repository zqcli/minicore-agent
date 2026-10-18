use super::*;

#[tokio::test]
async fn steer_is_applied_and_persisted_in_order() {
    let fixture = AgentFixture::new("steer").file("a.txt", b"hello");
    let data_dir = fixture.data_dir.clone();
    let gate = Arc::new(Gate::new());

    let fixture = fixture.script([Step::gated_text(gate.clone(), "final")]);
    let (mut agent, info) = fixture.start().await;
    let turn = send_text(&mut agent, info.session_id, "main prompt").await;

    agent
        .steer(crate::agent::SteerMessage {
            turn,
            text: "steer one".to_owned(),
        })
        .unwrap();
    agent
        .steer(crate::agent::SteerMessage {
            turn,
            text: "steer two".to_owned(),
        })
        .unwrap();
    gate.wait_started().await;
    gate.release();

    let result = wait_text(&agent, turn).await;
    assert_persisted(&result);

    let stored = read_store_history(&data_dir, info.session_id).await;
    let kinds = stored
        .iter()
        .filter_map(|item| match item {
            HistoryItem::User(user) => Some((user.input.as_text().to_owned(), user.kind)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            ("main prompt".to_owned(), UserMessageKind::Prompt),
            ("steer one".to_owned(), UserMessageKind::Steering),
            ("steer two".to_owned(), UserMessageKind::Steering),
        ]
    );

    let page = history_page(&agent, info.session_id, 100);
    let timestamps = page
        .items
        .iter()
        .filter_map(|item| match &item.item {
            crate::history::HistoryItemView::User(user) => user.timestamp.clone(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(timestamps.len(), 3);
    assert!(timestamps.iter().all(|timestamp| timestamp.ends_with('Z')));

    // Paging must retain the loop-local occurrence instead of restarting at
    // zero on every page.
    let second_page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 1,
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

    let loaded = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_session(info.session_id)
        .await
        .unwrap();
    assert_eq!(loaded.user_times.len(), 3);
    assert!(
        loaded
            .user_times
            .keys()
            .all(|(loop_id, _)| *loop_id == turn.loop_id)
    );
}

#[tokio::test]
async fn stale_turn_ref_is_rejected() {
    let fixture = AgentFixture::new("stale")
        .file("a.txt", b"hello")
        .script([Step::Text("one"), Step::Text("two")]);
    let (mut agent, info) = fixture.start().await;
    let first = send_text(&mut agent, info.session_id, "one").await;
    wait_text(&agent, first).await;
    let second = send_text(&mut agent, info.session_id, "two").await;

    let stale = TurnRef {
        session_id: info.session_id,
        loop_id: first.loop_id,
    };
    assert!(matches!(
        agent.steer(crate::agent::SteerMessage {
            turn: stale,
            text: "x".to_owned(),
        }),
        Err(AgentError::TurnNotFound)
    ));
    assert!(matches!(agent.cancel(stale), Err(AgentError::TurnNotFound)));
    let _ = second;
}
