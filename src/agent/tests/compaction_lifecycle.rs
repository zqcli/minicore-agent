use super::*;

#[tokio::test]
async fn manual_compaction_rejects_reserved_auto_namespace_but_cancel_accepts_it() {
    let (data_dir, _guard) = fixture_dir("manual-auto-namespace");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let mut agent = AgentFixture::at(&data_dir)
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .auto()
        .open()
        .await;
    let info = create_session(&mut agent, &workspace).await;
    let request = CompactSession {
        session_id: info.session_id,
        operation_id: "auto-forged".to_owned(),
    };
    assert!(matches!(
        agent.compact_session(request.clone()).await,
        Err(AgentError::InvalidInput)
    ));
    assert!(!agent.cancel_compaction(request).unwrap());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_close_future_keeps_normal_worker_owned_until_second_join() {
    let fixture = AgentFixture::new("close-cancel-owned").file("a.txt", b"hello");
    let model = ScriptedModel::new("main", [Step::Text("done")])
        .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let gate = Arc::new(Gate::new());
    PAUSE_BEFORE_JOIN.arm(info.session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, info.session_id, "close later").await;
    gate.wait_started().await;
    let first = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        agent.close_session(info.session_id),
    )
    .await;
    assert!(
        first.is_err(),
        "cancelled close must still be waiting for the worker"
    );
    assert!(
        agent
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|session| session.session_id == info.session_id && session.loaded)
    );
    gate.release();
    let _ = agent.wait_turn(turn).await;
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_close_future_keeps_manual_worker_owned_until_second_join() {
    let fixture = AgentFixture::new("compact-close-cancel").file("a.txt", b"hello");
    let model = ScriptedModel::with_window(
        "main",
        40_000,
        [Step::Text("settled"), Step::Text("summary")],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    let settled_text = "settled history ".repeat(128);
    let turn = send_text(&mut agent, info.session_id, &settled_text).await;
    wait_text(&agent, turn).await;
    agent.close_session(info.session_id).await.unwrap();
    let tail = synthetic_loop_record(
        LoopId::new().unwrap(),
        &"recent ".repeat(12_000),
        "RECENT_TAIL_ANSWER",
        false,
    );
    agent
        .store
        .append_loop(info.session_id, &tail)
        .await
        .unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let gate = Arc::new(Gate::new());
    PAUSE_AFTER_COMPACTION_RESULT.arm(info.session_id, Arc::clone(&gate));
    let result = agent
        .compact(CompactSession {
            session_id: info.session_id,
            operation_id: "close-cancel-compaction".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        result.status,
        crate::compaction::CompactionStatus::Compacted,
        "unexpected manual compaction result: {result:?}"
    );
    gate.wait_started().await;
    let first = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        agent.close_session(info.session_id),
    )
    .await;
    assert!(
        first.is_err(),
        "cancelled close must still be waiting for compaction"
    );
    assert!(
        agent
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|session| session.session_id == info.session_id && session.loaded)
    );
    gate.release();
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn compaction_operation_ids_are_bounded_and_reset_on_reopen() {
    let fixture = AgentFixture::new("compact-id-cap").file("a.txt", b"hello");
    let model = ScriptedModel::new("main", []).with_pacing(std::time::Duration::from_millis(2));
    let fixture = fixture.models(BTreeMap::from([("main".to_owned(), model)]));
    let (mut agent, info) = fixture.start().await;
    for index in 0..4_096 {
        let result = agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: format!("bounded-{index}"),
            })
            .await
            .unwrap();
        assert_eq!(result.status, crate::compaction::CompactionStatus::Noop);
    }
    assert!(matches!(
        agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: "bounded-over-cap".to_owned()
            })
            .await,
        Err(AgentError::InvalidInput)
    ));
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(
        agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: "bounded-0".to_owned()
            })
            .await
            .unwrap()
            .status,
        crate::compaction::CompactionStatus::Noop
    );
    agent.shutdown().await.unwrap();
}
