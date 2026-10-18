use super::*;
use crate::compaction::EphemeralGroupKey;
use crate::event::{AgentEvent, AgentEventSink, EventMeta, LoopOutcomeView};
use crate::ids::SessionId;

#[tokio::test]
async fn ignored_finished_drops_accumulate_into_subsequent_agent_event() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    let sink = AgentEventSink::new(sender);
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
        if let Ok(slot) = self.join.try_lock() {
            if let Some(handle) = slot.as_ref() {
                handle.abort();
            }
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
