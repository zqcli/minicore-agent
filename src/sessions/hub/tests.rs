use super::*;
use crate::sessions::Session;
use crate::test_support::TempDir;
use std::time::Duration;

async fn fixture(label: &str) -> (TempDir, crate::Agent, Session, crate::AgentEventStream) {
    let directory = TempDir::new(label);
    let config = serde_json::from_value(serde_json::json!({
        "data_dir": directory.path().join("data"), "default_profile": "test",
        "profiles": {"test": {"model":"main", "reasoning":"auto", "system_prompt":"test", "tools":[], "approval":"auto"}},
        "models": {"main": {"provider":"open_ai_responses", "model":"test", "base_url":"https://example.invalid/v1", "api_key_env":"MINICORE_AGENT_TEST_KEY",
            "physical_context_window":32000, "output_budget_tokens":2000, "safety_margin_tokens":1000, "supported_reasoning":["auto"], "supports_tools":true}},
        "compaction":{"enabled":false}
    })).unwrap();
    let model: Arc<dyn minicore_runtime::model::Model> =
        crate::test_support::ScriptedModel::new("main", []);
    let models = crate::models::Models::from_values(std::collections::BTreeMap::from([(
        "main".to_owned(),
        model,
    )]));
    let mut agent = crate::Agent::open_with_models(config, models)
        .await
        .unwrap();
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
    let stream = agent.take_events().unwrap();
    (directory, agent, session, stream)
}

async fn next_snapshot(stream: &mut crate::AgentEventStream) -> crate::SessionSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(AgentEvent::SessionSnapshot { snapshot, .. }) = stream.recv().await {
                return snapshot;
            }
        }
    })
    .await
    .expect("snapshot publication")
}

#[tokio::test]
async fn the_last_mark_is_published_without_another_event() {
    let (_directory, agent, session, mut stream) = fixture("hub-dirty").await;
    let first = next_snapshot(&mut stream).await;
    assert_eq!(first.execution, crate::SessionExecution::None);
    session.mark_dirty();
    let second = next_snapshot(&mut stream).await;
    assert!(second.revision > first.revision);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshots_advance_the_revision_and_never_reuse_a_published_one() {
    let (_directory, agent, session, mut stream) = fixture("hub-revision").await;
    let mut seen = Vec::new();
    for _ in 0..3 {
        let snapshot = next_snapshot(&mut stream).await;
        assert_eq!(snapshot.load, session.load());
        seen.push(snapshot.revision);
        session.mark_dirty();
    }
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "{seen:?}");
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_snapshot_budget_is_charged_and_released_exactly() {
    let budget = Arc::new(SnapshotBudget {
        limit: 1024,
        used: AtomicUsize::new(0),
        notify: Notify::new(),
    });
    let charge = budget
        .reserve(1024, &CancellationToken::new())
        .await
        .expect("budget");
    assert_eq!(budget.used.load(Ordering::SeqCst), 1024);
    drop(charge);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn sampling_waits_for_capacity_before_cloning_or_assigning_a_revision() {
    let (_directory, agent, session, _stream) = fixture("hub-reserve-first").await;
    let hub = Arc::clone(&session.shared.hub);
    hub.shutdown_now();
    let (sink, mut stream) = AgentEventSink::channel(1);
    let hub = SnapshotHub::new(sink);
    hub.register(session.load(), &session);
    assert_eq!(hub.take_batch().len(), 1);
    let held = hub
        .budget
        .reserve(SNAPSHOT_BUDGET_BYTES, &CancellationToken::new())
        .await
        .unwrap();
    let before = session.lock_inner().revision;
    let publication = hub.publish(session.load(), Arc::downgrade(&session.shared));
    tokio::pin!(publication);
    assert!(futures_util::poll!(&mut publication).is_pending());
    assert_eq!(session.lock_inner().revision, before);
    hub.mark(session.load());
    drop(held);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), publication)
            .await
            .unwrap()
    );
    assert_eq!(session.lock_inner().revision, before + 1);
    let first = next_snapshot(&mut stream).await;
    let next = hub.take_batch();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].0, session.load());
    assert!(
        hub.publish(session.load(), Arc::downgrade(&session.shared))
            .await
    );
    let second = next_snapshot(&mut stream).await;
    assert!(second.revision > first.revision);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_small_budget_waiter_is_woken_when_the_last_charge_is_released() {
    let budget = Arc::new(SnapshotBudget {
        limit: 16,
        used: AtomicUsize::new(0),
        notify: Notify::new(),
    });
    let stopped = CancellationToken::new();
    let charge = budget.reserve(16, &stopped).await.unwrap();
    let next = budget.reserve(8, &stopped);
    tokio::pin!(next);
    assert!(futures_util::poll!(&mut next).is_pending());
    drop(charge);
    let next = tokio::time::timeout(Duration::from_secs(5), next)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(budget.used.load(Ordering::SeqCst), 8);
    drop(next);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn global_eviction_publishes_the_affected_load_without_another_local_change() {
    use crate::sessions::results::{PreparedSource, ResultLimits};
    use crate::sessions::{
        CompletionEntry, CompletionKind, CompletionOutcome, ResultAvailability, TurnPersistence,
        UnavailableReason,
    };
    let (directory, mut agent, a, mut stream) = fixture("hub-global-eviction").await;
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
    let b = agent.loaded_session(info.session_id).unwrap();
    let catalog = &a.shared.results;
    catalog.set_limits_for_test(ResultLimits {
        entries: 1,
        sources: 1,
        ..ResultLimits::default()
    });
    let loop_id = minicore_runtime::LoopId::new().unwrap();
    let entry = CompletionEntry {
        seq: 0,
        loop_id,
        input_id: None,
        completion_kind: CompletionKind::Reported,
        outcome_kind: Some(CompletionOutcome::Completed),
        persistence: Some(TurnPersistence::Failed),
        persistence_uncertain: false,
        availability: ResultAvailability::Live,
        unavailable_reason: None,
        next_loop_id: None,
        after_saved_loop_id: None,
    };
    let source = PreparedSource::new(vec!["body".into()], None).unwrap();
    let charge = catalog.reserve(a.load(), 4096).unwrap();
    let source = catalog.finish_reservation(source, charge).unwrap();
    drop(catalog.insert(a.load(), entry.clone(), Some(source)));
    loop {
        let snapshot = next_snapshot(&mut stream).await;
        if snapshot.load == a.load() && snapshot.completions.through_seq == 1 {
            assert_eq!(
                snapshot.completions.entries[0].availability,
                ResultAvailability::Live
            );
            break;
        }
    }
    let reservation = catalog.reserve(b.load(), 4096).unwrap();
    loop {
        let snapshot = next_snapshot(&mut stream).await;
        if snapshot.load == a.load()
            && snapshot.completions.entries[0].availability == ResultAvailability::Unavailable
        {
            assert_eq!(
                snapshot.completions.entries[0].unavailable_reason,
                Some(UnavailableReason::Expired)
            );
            break;
        }
    }
    drop(reservation);
    let mut entry = entry;
    entry.availability = ResultAvailability::Stored;
    entry.persistence = Some(TurnPersistence::Persisted);
    drop(catalog.insert(b.load(), entry, None));
    loop {
        let snapshot = next_snapshot(&mut stream).await;
        if snapshot.load == a.load() && snapshot.completions.entries.is_empty() {
            assert_eq!(snapshot.completions.through_seq, 1);
            assert_eq!(snapshot.completions.retained_from_seq, 2);
            assert!(snapshot.completions.truncated);
            break;
        }
    }
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn batches_rotate_after_sixteen_dirty_loads_and_keep_new_marks() {
    let (directory, mut agent, session, _stream) = fixture("hub-fair-batches").await;
    session.shared.hub.shutdown_now();
    let (sink, _stream) = AgentEventSink::channel(1);
    let hub = SnapshotHub::new(sink);
    let mut sessions = vec![session];
    for _ in 1..18 {
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
        sessions.push(agent.loaded_session(info.session_id).unwrap());
    }
    for session in &sessions {
        hub.register(session.load(), session);
    }
    let first = hub.take_batch();
    assert_eq!(
        first.iter().map(|(load, _)| *load).collect::<Vec<_>>(),
        sessions[..16].iter().map(Session::load).collect::<Vec<_>>()
    );
    hub.mark(sessions[0].load());
    let next = hub.take_batch();
    assert_eq!(
        next.iter().map(|(load, _)| *load).collect::<Vec<_>>(),
        [sessions[16].load(), sessions[17].load(), sessions[0].load()]
    );
    assert!(hub.take_batch().is_empty());
    agent.shutdown().await.unwrap();
}
