use super::*;

#[tokio::test]
async fn read_snapshots_share_timestamps_until_a_writer_changes_them() {
    let directory = crate::test_support::TempDir::new("snapshot-timestamps");
    let config = serde_json::from_value(serde_json::json!({
        "data_dir": directory.path().join("data"), "default_profile": "test",
        "profiles": {"test": {"model":"main", "reasoning":"auto", "system_prompt":"test", "tools":[], "approval":"auto"}},
        "models": {"main": {"provider":"open_ai_responses", "model":"test", "base_url":"https://example.invalid/v1", "api_key_env":"PATH",
            "physical_context_window":32000, "output_budget_tokens":2000, "safety_margin_tokens":1000, "supported_reasoning":["auto"], "supports_tools":true}},
        "compaction":{"enabled":false}
    })).unwrap();
    let mut agent = crate::Agent::open(config).await.unwrap();
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
    let first = session.read_snapshot();
    let second = session.read_snapshot();
    assert!(Arc::ptr_eq(&first.user_times, &second.user_times));
    let key = (LoopId::new().unwrap(), 0);
    Arc::make_mut(&mut session.lock_inner().user_times).insert(key, "2026-01-01T00:00:00Z".into());
    let next = session.read_snapshot();
    assert!(first.user_times.is_empty());
    assert!(next.user_times.contains_key(&key));
    assert!(Arc::ptr_eq(
        &next.user_times,
        &session.read_snapshot().user_times
    ));
    agent.shutdown().await.unwrap();
}
use crate::compaction::EphemeralGroupKey;
use crate::event::{AgentEvent, AgentEventSink, EventMeta, LoopOutcomeView};
use crate::ids::SessionId;

#[tokio::test]
async fn ignored_finished_drops_accumulate_into_subsequent_agent_event() {
    let (sink, mut receiver) = AgentEventSink::channel(8);
    let turn = TurnRef {
        session_id: SessionId::new().unwrap(),
        loop_id: LoopId::new().unwrap(),
    };
    // The runner drains the loop event stream to close after `join`, so
    // the ignored runtime `Finished` envelope still records its cumulative
    // drop count (here 7) on the Agent sink.
    sink.record_core_drops(7);
    sink.try_send(AgentEvent::TurnFinished {
        turn,
        outcome: LoopOutcomeView::Completed,
        persistence: TurnPersistence::Persisted,
        next: None,
        queue: crate::sessions::Queue::default(),
        meta: EventMeta::for_turn(turn),
    });
    let received = receiver
        .recv()
        .await
        .expect("agent event must be delivered");
    let AgentEvent::TurnFinished { meta, .. } = received else {
        panic!("expected a turn finished event");
    };
    assert_eq!(meta.dropped_before, 7);
}

#[test]
fn an_old_loop_cleanup_cannot_clear_a_new_loop_summary() {
    let state = CompactionState::new();
    let old_loop = LoopId::new().unwrap();
    let new_loop = LoopId::new().unwrap();
    let cleanup = EphemeralSummaryCleanup {
        state: Arc::clone(&state),
        loop_id: old_loop,
    };
    assert!(state.cache_ephemeral(
        old_loop,
        EphemeralGroupKey {
            start: 0,
            end: 1,
            source_hash: [1; 32],
        },
        BoundedText::new("old").unwrap(),
    ));
    state.clear_ephemeral(old_loop);
    assert!(state.cache_ephemeral(
        new_loop,
        EphemeralGroupKey {
            start: 0,
            end: 1,
            source_hash: [2; 32],
        },
        BoundedText::new("new").unwrap(),
    ));
    drop(cleanup);
    assert!(state.ephemeral(new_loop).is_some());
}
impl Session {
    pub(crate) fn options_for_test(&self) -> LoopOptions {
        let inner = self.lock_inner();
        inner.options.clone()
    }
    #[cfg(all(test, unix))]
    pub(crate) fn active_status_workers(&self) -> usize {
        self.shared
            .status_workers
            .lock()
            .unwrap()
            .workers
            .iter()
            .filter(|handle| !handle.is_finished())
            .count()
    }
    pub(crate) fn abort_active_task(&self) -> Result<(), AgentError> {
        let inner = self.lock_inner();
        let task = inner
            .active
            .as_ref()
            .map(|active| &active.task)
            .ok_or(AgentError::TurnNotFound)?;
        task.abort();
        Ok(())
    }
    /// `turn.wait` returns before settlement and tool-record persistence;
    /// join the turn's owned task the way the next submit or close does.
    pub(crate) async fn join_turn_task(&self) {
        let task = self
            .lock_inner()
            .active
            .as_ref()
            .map(|active| active.task.clone());
        if let Some(task) = task {
            task.join().await.unwrap();
        }
    }
    pub(crate) fn runtime_loop_finished(&self) -> bool {
        let inner = self.lock_inner();
        inner
            .active
            .as_ref()
            .is_some_and(|active| active.handle.is_finished())
    }
}
impl SessionTask {
    fn abort(&self) {
        if let Ok(slot) = self.join.try_lock()
            && let Some(handle) = slot.as_ref()
        {
            handle.abort();
        }
    }
}

pub(super) async fn pause_worker(registry: &Registry<SessionId, Arc<Gate>>, id: SessionId) {
    if let Some(gate) = registry.take(&id) {
        gate.pass().await;
    }
}

pub(super) fn panic_worker(id: SessionId) {
    assert!(!PANIC_WORKERS.hit(&id), "injected worker panic");
}

pub(super) fn pause_runtime_start<'a>(
    session: &'a Session,
    inner: std::sync::MutexGuard<'a, SessionInner>,
) -> std::sync::MutexGuard<'a, SessionInner> {
    if let Some(gate) = PAUSE_AFTER_RUNTIME_START.take(&inner.record.session_id) {
        drop(inner);
        tokio::task::block_in_place(|| gate.pass_blocking());
        session.lock_inner()
    } else {
        inner
    }
}
