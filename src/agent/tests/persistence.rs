use super::*;

#[tokio::test]
async fn panicked_agent_worker_publishes_internal_and_blocks_session() {
    let (data_dir, _guard) = fixture_dir(&format!("worker-panic-{}", next_id()));
    let (workspace, _guard) = workspace_file("worker-panic-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("unused")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    PANIC_WORKERS.arm(info.session_id, ());

    let turn = send_text(&mut agent, info.session_id, "panic").await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("worker panic must complete the waiter");
    assert!(matches!(result, Err(AgentError::Internal)));

    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Blocked);
    assert_eq!(
        state.block_reason,
        Some(crate::sessions::SessionBlockReason::Internal)
    );
    assert!(state.active_loop.is_none());

    let error = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "after panic".to_owned(),
        })
        .await;
    assert!(matches!(error, Err(AgentError::SessionBlocked)));
    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Blocked);
    let close = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        agent.close_session(info.session_id),
    )
    .await
    .expect("close must reclaim a panicked worker");
    assert!(matches!(close, Err(AgentError::Internal)));
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn aborted_agent_worker_wait_close_and_shutdown_reclaim_without_busy_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("worker-abort-{}", next_id()));
    let (workspace, _guard) = workspace_file("worker-abort-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("unused")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "abort").await;
    // Abort immediately, including the case where the worker has not polled
    // yet. CompletionGuard is captured before spawn and still publishes the
    // internal failure when the future is dropped.
    agent.abort_active_task_for_test(info.session_id).unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("worker abort must complete the waiter");
    assert!(matches!(result, Err(AgentError::Internal)));

    let close = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        agent.close_session(info.session_id),
    )
    .await
    .expect("close must reclaim an aborted worker");
    assert!(matches!(close, Err(AgentError::Internal)));
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn unobserved_event_stream_still_persists() {
    let (data_dir, _guard) = fixture_dir(&format!("noevents-{}", next_id()));
    let (workspace, _guard) = workspace_file("noevents-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    // Do not take_events at all.
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 2);
}

#[tokio::test]
async fn next_send_cleans_up_finished_turn_without_explicit_wait() {
    let (data_dir, _guard) = fixture_dir(&format!("cleanup-{}", next_id()));
    let (workspace, _guard) = workspace_file("cleanup-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("one"), ModelScript::Text("two")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "one").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let state = agent.session_state(info.session_id).unwrap();
        if state.status == crate::sessions::SessionStatus::Idle {
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "first turn did not reach Idle; final status: {:?}",
                state.status
            );
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(10))).await;
    }
    // Send again after Agent-level completion, without waiting for the first turn.
    let second = send_text(&mut agent, info.session_id, "two").await;
    assert_ne!(turn.loop_id, second.loop_id);
    let result = wait_text(&agent, second).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
}

#[tokio::test]
async fn rolled_back_append_keeps_history_unchanged_and_allows_the_next_turn() {
    let (data_dir, _guard) = fixture_dir(&format!("append-rollback-{}", next_id()));
    let (workspace, _guard) = workspace_file("append-rollback-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [ModelScript::Text("not saved"), ModelScript::Text("saved")],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    crate::store::APPEND_FAILURES.arm(info.session_id, false);
    let first = send_text(&mut agent, info.session_id, "rolled back").await;
    let result = wait_text(&agent, first).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(
        agent
            .history(GetHistory {
                session_id: info.session_id,
                offset: 0,
                limit: 100
            })
            .unwrap()
            .total,
        0
    );
    assert!(
        agent
            .session_state(info.session_id)
            .unwrap()
            .block_reason
            .is_none()
    );
    let second = send_text(&mut agent, info.session_id, "keep this").await;
    let result = wait_text(&agent, second).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let expected = result.report.appended.len();
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
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
    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn append_failure_blocks_session_and_returns_failed_persistence() {
    let (data_dir, _guard) = fixture_dir(&format!("blocked-{}", next_id()));
    let (workspace, _guard) = workspace_file("blocked-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    crate::store::APPEND_FAILURES.arm(info.session_id, true);

    let turn = send_text(&mut agent, info.session_id, "will fail").await;
    let result = wait_text(&agent, turn).await;
    // The report is still fully returned.
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    // History was not merged into memory.
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 0);

    // Session is blocked: further sends and updates are refused.
    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Blocked);
    assert_eq!(
        state.block_reason,
        Some(crate::sessions::SessionBlockReason::Persistence)
    );
    let busy = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "after".to_owned(),
        })
        .await;
    assert!(matches!(busy, Err(AgentError::SessionBlocked)));
}

#[tokio::test]
async fn blocked_send_does_not_discard_previous_turn_result() {
    let (data_dir, _guard) = fixture_dir(&format!("blocked-discard-{}", next_id()));
    let (workspace, _guard) = workspace_file("blocked-discard-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("persist me")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    crate::store::APPEND_FAILURES.arm(info.session_id, true);

    let turn = send_text(&mut agent, info.session_id, "first turn").await;

    // Wait deterministically for the session to transition to Blocked.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let state = agent.session_state(info.session_id).unwrap();
        if state.status == crate::sessions::SessionStatus::Blocked {
            assert_eq!(
                state.block_reason,
                Some(crate::sessions::SessionBlockReason::Persistence)
            );
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "session did not reach Blocked; final status: {:?}",
                state.status
            );
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(10))).await;
    }

    // A second send fails with SessionBlocked.
    let second_send = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "second turn".to_owned(),
        })
        .await;
    assert!(matches!(second_send, Err(AgentError::SessionBlocked)));

    // First turn.wait still resolves with the authoritative runtime outcome and persistence failure.
    let first_result = agent
        .wait_turn(turn)
        .await
        .expect("previous turn wait must succeed even after blocked send");
    assert_eq!(
        first_result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        first_result.persistence,
        crate::sessions::TurnPersistence::Failed
    );

    // Repeated wait returns the same result.
    let second_wait = agent
        .wait_turn(turn)
        .await
        .expect("repeated wait on blocked turn must succeed");
    assert_eq!(
        second_wait.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        second_wait.persistence,
        crate::sessions::TurnPersistence::Failed
    );
    assert_eq!(second_wait.turn, turn);

    // Close reclaims the active task cleanly.
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn close_and_reopen_recovers_from_blocked_session() {
    let (data_dir, _guard) = fixture_dir(&format!("recover-{}", next_id()));
    let (workspace, _guard) = workspace_file("recover-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    crate::store::APPEND_FAILURES.arm(info.session_id, true);
    let turn = send_text(&mut agent, info.session_id, "fail").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);

    agent.close_session(info.session_id).await.unwrap();
    let opened = agent.open_session(info.session_id).await.unwrap();
    let state = agent.session_state(opened.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
    assert!(state.block_reason.is_none());

    let turn = send_text(&mut agent, info.session_id, "after recovery").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
}

#[tokio::test]
async fn metadata_write_failure_does_not_fail_the_turn() {
    let (data_dir, _guard) = fixture_dir(&format!("metadata-{}", next_id()));
    let (workspace, _guard) = workspace_file("metadata-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    crate::store::RECORD_WRITE_FAILURES.arm(info.session_id, ());

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    // History merged even though session.json updated_at write failed.
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 2);
}
