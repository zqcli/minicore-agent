use super::*;
use crate::test_support::TempDir;

#[tokio::test]
async fn shutdown_broadcasts_to_all_sessions_before_waiting_for_any_worker() {
    let data = TempDir::new("core-shutdown-data");
    let workspace = TempDir::new("core-shutdown-workspace");
    let model = FakeModel::new("main", [ModelScript::Text("one"), ModelScript::Text("two")]);
    let mut agent = open_agent(
        data.path(),
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let store = agent.store_handle();
    let mut ids = Vec::new();
    let mut cancellations = Vec::new();
    let mut gates = Vec::new();
    for _ in 0..2 {
        let info = create_session(&mut agent, workspace.path()).await;
        ids.push(info.session_id);
        let gate = Arc::new(Gate::new());
        PAUSE_BEFORE_JOIN.arm(info.session_id, gate.clone());
        send_text(&mut agent, info.session_id, "hold completion").await;
        gate.wait_started().await;
        cancellations.push(
            agent
                .loaded_session(info.session_id)
                .unwrap()
                .query_cancellation(),
        );
        gates.push(gate);
    }
    let mut shutdown = tokio::spawn(agent.shutdown());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        for token in &cancellations {
            token.cancelled().await;
        }
    })
    .await
    .expect("all sessions must be cancelled before any worker is released");
    gates[1].release();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut shutdown)
            .await
            .is_err()
    );
    assert!(ids.iter().all(|id| store.has_history_index(*id)));
    gates[0].release();
    assert!(shutdown.await.unwrap().is_ok());
    assert!(ids.iter().all(|id| !store.has_history_index(*id)));
}
