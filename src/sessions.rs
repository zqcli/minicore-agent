use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::event::{
    AgentEvent, AgentEventSink, CompletionCancellation, EventMeta, TurnCoordinator,
};

use minicore_runtime::error::SessionShutdownError;
use minicore_runtime::ids::{SessionId, TurnId};
use minicore_runtime::session::{SessionHandle, SessionRuntime, TurnHandle};
use tokio::sync::Notify;

use crate::agent::TurnRef;
use crate::error::AgentError;
use crate::store::SessionRecord;

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, LoadedSession>,
}

pub(crate) struct LoadedSession {
    pub(crate) record: SessionRecord,
    pub(crate) runtime: SessionRuntime,
    pub(crate) handle: SessionHandle,
    pub(crate) active_turn: Option<TurnHandle>,
    pub(crate) event_task: tokio::task::JoinHandle<()>,
    pub(crate) state_task: tokio::task::JoinHandle<()>,
    pub(crate) event_sink: AgentEventSink,
    pub(crate) completion_slot: CompletionSlot,
    pub(crate) completion: Option<CompletionTask>,
    pub(crate) metadata_cancel: CompletionCancellation,
}

#[derive(Clone)]
pub(crate) struct CompletionSlot {
    state: Arc<Mutex<CompletionSlotState>>,
    drain: Arc<Notify>,
    event_sink: AgentEventSink,
}

struct CompletionSlotState {
    current: Option<Arc<TurnCoordinator>>,
    pending_terminal: Option<(TurnId, EventMeta)>,
    pending_state_terminal: Option<TurnId>,
    core_closed: bool,
}

pub(crate) struct CompletionTask {
    pub(crate) coordinator: Arc<TurnCoordinator>,
    pub(crate) cancellation: CompletionCancellation,
    pub(crate) event_sink: AgentEventSink,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl CompletionTask {
    pub(crate) async fn cancel_and_join(self) -> Result<(), AgentError> {
        let Self {
            coordinator,
            cancellation,
            event_sink,
            task,
        } = self;
        cancellation.cancel();
        if let Some(meta) = coordinator.abandon() {
            event_sink.record_core_drops(meta.dropped_before);
        }
        task.await.map_err(|_| AgentError::Internal).map(|_| ())
    }

    pub(crate) async fn finish_after_barrier(self) -> Result<(), AgentError> {
        let Self {
            coordinator,
            cancellation,
            event_sink,
            task,
        } = self;
        tokio::task::yield_now().await;
        if !task.is_finished() {
            cancellation.cancel();
            if let Some(meta) = coordinator.abandon() {
                event_sink.record_core_drops(meta.dropped_before);
            }
        }
        task.await.map_err(|_| AgentError::Internal).map(|_| ())
    }
}

impl CompletionSlot {
    pub(crate) fn new(event_sink: AgentEventSink) -> Self {
        Self {
            state: Arc::new(Mutex::new(CompletionSlotState {
                current: None,
                pending_terminal: None,
                pending_state_terminal: None,
                core_closed: false,
            })),
            drain: Arc::new(Notify::new()),
            event_sink,
        }
    }

    pub(crate) fn register(&self, turn: TurnRef) -> Arc<TurnCoordinator> {
        let coordinator = Arc::new(TurnCoordinator::new(turn));
        let (terminal, state_terminal, core_closed, late_terminal) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.current = Some(Arc::clone(&coordinator));
            let (terminal, late_terminal) = match state.pending_terminal.take() {
                Some((turn_id, meta)) if turn_id == turn.turn_id => (Some(meta), None),
                Some((_, meta)) => (None, Some(meta.dropped_before)),
                None => (None, None),
            };
            let state_terminal = state
                .pending_state_terminal
                .take()
                .filter(|turn_id| *turn_id == turn.turn_id);
            let core_closed = state.core_closed;
            (terminal, state_terminal, core_closed, late_terminal)
        };
        if let Some(dropped_before) = late_terminal {
            self.event_sink.record_core_drops(dropped_before);
        }
        if let Some(meta) = terminal {
            coordinator.observe_terminal(meta);
        }
        if state_terminal.is_some() {
            coordinator.observe_state_terminal();
            self.request_drain();
        }
        if core_closed {
            coordinator.observe_core_closed();
        }
        coordinator
    }

    pub(crate) fn observe_terminal(&self, turn_id: TurnId, meta: EventMeta) {
        let (coordinator, late_terminal, previous_dropped) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.current.as_ref() {
                Some(coordinator) if coordinator.turn_id() == turn_id => {
                    (Some(Arc::clone(coordinator)), false, None)
                }
                Some(_) => (None, true, None),
                None => {
                    let previous = state.pending_terminal.replace((turn_id, meta));
                    (None, false, previous.map(|(_, value)| value.dropped_before))
                }
            }
        };
        if let Some(dropped_before) = previous_dropped {
            self.event_sink.record_core_drops(dropped_before);
        }
        if late_terminal
            || coordinator.is_some_and(|coordinator| !coordinator.observe_terminal(meta))
        {
            self.event_sink.record_core_drops(meta.dropped_before);
        }
    }

    pub(crate) fn observe_state_terminal(&self, turn_id: TurnId) {
        let coordinator = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.current.as_ref() {
                Some(coordinator) if coordinator.turn_id() == turn_id => {
                    Some(Arc::clone(coordinator))
                }
                Some(_) => None,
                None => {
                    state.pending_state_terminal = Some(turn_id);
                    None
                }
            }
        };
        if let Some(coordinator) = coordinator {
            coordinator.observe_state_terminal();
        }
        self.request_drain();
    }

    pub(crate) fn observe_quiescence(&self) {
        let coordinator = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .current
            .clone();
        if let Some(coordinator) = coordinator {
            coordinator.observe_core_quiescence();
        }
    }

    pub(crate) fn observe_core_closed(&self) {
        let coordinator = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.core_closed = true;
            state.current.clone()
        };
        if let Some(coordinator) = coordinator {
            coordinator.observe_core_closed();
        }
    }

    pub(crate) fn clear(&self, coordinator: &Arc<TurnCoordinator>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, coordinator))
        {
            state.current = None;
        }
    }

    pub(crate) fn request_drain(&self) {
        self.drain.notify_one();
    }

    pub(crate) async fn drain_requested(&self) {
        self.drain.notified().await;
    }
}

impl Sessions {
    pub(crate) fn new() -> Self {
        Self {
            loaded: HashMap::new(),
        }
    }

    pub(crate) fn contains(&self, session_id: SessionId) -> bool {
        self.loaded.contains_key(&session_id)
    }

    pub(crate) fn get(&self, session_id: SessionId) -> Option<&LoadedSession> {
        self.loaded.get(&session_id)
    }

    pub(crate) fn get_mut(&mut self, session_id: SessionId) -> Option<&mut LoadedSession> {
        self.loaded.get_mut(&session_id)
    }

    pub(crate) fn insert(
        &mut self,
        session_id: SessionId,
        loaded: LoadedSession,
    ) -> Option<LoadedSession> {
        self.loaded.insert(session_id, loaded)
    }

    pub(crate) fn remove(&mut self, session_id: SessionId) -> Option<LoadedSession> {
        self.loaded.remove(&session_id)
    }

    pub(crate) async fn shutdown_all(&mut self) -> Result<(), AgentError> {
        let mut ids = self.loaded.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        let mut first_error = None;
        for session_id in ids {
            let Some(loaded) = self.loaded.remove(&session_id) else {
                continue;
            };
            let event_sink = loaded.event_sink.clone();
            let meta = EventMeta {
                session_id,
                instance_id: loaded.handle.instance_id(),
                dropped_before: 0,
            };
            let result = loaded.shutdown().await;
            if result.is_ok() {
                let _ = event_sink.try_send(AgentEvent::SessionClosed { session_id, meta });
            } else if first_error.is_none() {
                first_error = result.err();
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl LoadedSession {
    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        let Self {
            runtime,
            event_task,
            state_task,
            completion_slot,
            completion,
            metadata_cancel,
            ..
        } = self;
        metadata_cancel.cancel();
        let runtime_error = runtime.shutdown().await.err();
        let event_error = event_task.await.err();
        let state_error = state_task.await.err();
        let completion_error = match completion {
            Some(completion) => {
                let coordinator = Arc::clone(&completion.coordinator);
                let result = completion.finish_after_barrier().await.err();
                completion_slot.clear(&coordinator);
                result
            }
            None => None,
        };
        if let Some(error) = runtime_error {
            return Err(map_session_shutdown_error(error));
        }
        if event_error.is_some() || state_error.is_some() {
            return Err(AgentError::Internal);
        }
        if completion_error.is_some() {
            return Err(AgentError::Internal);
        }
        Ok(())
    }

    pub(crate) fn active_turn_id(&self) -> Option<TurnId> {
        self.active_turn.as_ref().map(TurnHandle::turn_id)
    }
}

impl Drop for Sessions {
    fn drop(&mut self) {
        for loaded in self.loaded.values() {
            if let Some(completion) = &loaded.completion {
                completion.cancellation.cancel();
            }
            loaded.metadata_cancel.cancel();
        }
    }
}

pub(crate) fn map_session_shutdown_error(error: SessionShutdownError) -> AgentError {
    match error {
        SessionShutdownError::Timeout(diagnostic) => AgentError::Core(
            crate::error::CoreErrorView::new("session shutdown timeout", diagnostic.retryable),
        ),
        SessionShutdownError::Durability(diagnostic) => {
            AgentError::Core(crate::error::CoreErrorView::new(
                "session shutdown durability failed",
                diagnostic.retryable,
            ))
        }
        SessionShutdownError::LogClose(diagnostic) => AgentError::Core(
            crate::error::CoreErrorView::new("session log close failed", diagnostic.retryable),
        ),
        SessionShutdownError::ActorTerminated(diagnostic) => AgentError::Core(
            crate::error::CoreErrorView::new("session actor terminated", diagnostic.retryable),
        ),
        _ => AgentError::Internal,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use minicore_runtime::conversation::TurnTerminal;
    use minicore_runtime::ids::{SessionId, SessionInstanceId, TurnId};
    use minicore_runtime::model::Usage;

    use crate::event::{CompletionCancellation, CoordinatorReady};

    use super::*;

    fn turn() -> TurnRef {
        TurnRef {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            instance_id: "ins_00000000000000000000000000000001".parse().unwrap(),
            turn_id: "trn_00000000000000000000000000000001".parse().unwrap(),
        }
    }

    fn outcome() -> minicore_runtime::TurnOutcome {
        minicore_runtime::TurnOutcome {
            turn_id: turn().turn_id,
            terminal: TurnTerminal::Completed,
            usage: Usage::default(),
        }
    }

    fn meta(dropped_before: u64) -> EventMeta {
        EventMeta {
            session_id: turn().session_id,
            instance_id: turn().instance_id,
            dropped_before,
        }
    }

    #[tokio::test]
    async fn turn_coordinator_merges_outcome_and_terminal_in_either_order_once() {
        for terminal_first in [false, true] {
            let coordinator = Arc::new(TurnCoordinator::new(turn()));
            let cancellation = CompletionCancellation::new();
            let waiter = tokio::spawn({
                let coordinator = Arc::clone(&coordinator);
                let cancellation = cancellation.clone();
                async move { coordinator.wait_ready(&cancellation).await }
            });
            if terminal_first {
                assert!(coordinator.observe_terminal(meta(7)));
                coordinator.set_outcome(Ok(outcome()));
            } else {
                coordinator.set_outcome(Ok(outcome()));
                tokio::task::yield_now().await;
                assert!(coordinator.observe_terminal(meta(7)));
            }
            let ready = tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(
                ready,
                CoordinatorReady::Finish {
                    outcome: value,
                    terminal_meta: EventMeta {
                        dropped_before: 7,
                        ..
                    }
                } if value == outcome()
            ));
            assert!(!coordinator.observe_terminal(meta(8)));
        }
    }

    #[tokio::test]
    async fn turn_coordinator_uses_state_and_core_quiescence_fallback() {
        let coordinator = Arc::new(TurnCoordinator::new(turn()));
        let cancellation = CompletionCancellation::new();
        coordinator.set_outcome(Ok(outcome()));
        let waiter = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            let cancellation = cancellation.clone();
            async move { coordinator.wait_ready(&cancellation).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        coordinator.observe_state_terminal();
        coordinator.observe_core_quiescence();
        let ready = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            ready,
            CoordinatorReady::Finish {
                terminal_meta: EventMeta {
                    dropped_before: 0,
                    ..
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn turn_coordinator_falls_back_when_core_stream_closes() {
        let coordinator = Arc::new(TurnCoordinator::new(turn()));
        let cancellation = CompletionCancellation::new();
        coordinator.set_outcome(Ok(outcome()));
        coordinator.observe_core_closed();
        let ready = tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.wait_ready(&cancellation),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(ready, CoordinatorReady::Finish { .. }));
    }

    #[tokio::test]
    async fn completion_slot_accepts_terminal_before_coordinator_registration() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let sink = AgentEventSink::new(sender);
        let slot = CompletionSlot::new(sink);
        slot.observe_terminal(turn().turn_id, meta(13));
        let coordinator = slot.register(turn());
        coordinator.set_outcome(Ok(outcome()));
        let cancellation = CompletionCancellation::new();
        let ready = coordinator.wait_ready(&cancellation).await.unwrap();
        assert!(matches!(
            ready,
            CoordinatorReady::Finish {
                terminal_meta: EventMeta {
                    dropped_before: 13,
                    ..
                },
                ..
            }
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn completion_slot_does_not_reuse_terminal_for_a_different_turn() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let sink = AgentEventSink::new(sender);
        let slot = CompletionSlot::new(sink);
        slot.observe_terminal(turn().turn_id, meta(3));
        let next_turn = TurnRef {
            turn_id: "trn_00000000000000000000000000000002".parse().unwrap(),
            ..turn()
        };
        let coordinator = slot.register(next_turn);
        coordinator.set_outcome(Ok(minicore_runtime::TurnOutcome {
            turn_id: next_turn.turn_id,
            terminal: TurnTerminal::Completed,
            usage: Usage::default(),
        }));
        slot.observe_state_terminal(next_turn.turn_id);
        slot.observe_quiescence();
        let cancellation = CompletionCancellation::new();
        let ready = coordinator.wait_ready(&cancellation).await.unwrap();
        assert!(matches!(
            ready,
            CoordinatorReady::Finish {
                terminal_meta: EventMeta {
                    dropped_before: 0,
                    ..
                },
                ..
            }
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn late_core_terminal_drop_is_reported_by_the_next_agent_event() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let sink = AgentEventSink::new(sender);
        let slot = CompletionSlot::new(sink.clone());
        let coordinator = slot.register(turn());
        coordinator.set_outcome(Ok(outcome()));
        slot.observe_state_terminal(turn().turn_id);
        slot.observe_quiescence();
        let cancellation = CompletionCancellation::new();
        let ready = coordinator.wait_ready(&cancellation).await.unwrap();
        assert!(matches!(ready, CoordinatorReady::Finish { .. }));

        slot.observe_terminal(turn().turn_id, meta(9));
        assert_eq!(
            sink.try_send(AgentEvent::SessionClosed {
                session_id: turn().session_id,
                meta: meta(0),
            }),
            crate::event::AgentSendResult::Sent
        );
        assert!(matches!(
            receiver.recv().await.unwrap(),
            AgentEvent::SessionClosed {
                meta: EventMeta {
                    dropped_before: 9,
                    ..
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn terminal_metadata_keeps_the_core_identity_and_drop_count() {
        let expected_session: SessionId = turn().session_id;
        let expected_instance: SessionInstanceId = turn().instance_id;
        let expected_turn: TurnId = turn().turn_id;
        let coordinator = TurnCoordinator::new(turn());
        assert!(coordinator.observe_terminal(meta(11)));
        coordinator.set_outcome(Ok(outcome()));
        let cancellation = CompletionCancellation::new();
        let ready = coordinator.wait_ready(&cancellation).await;
        assert!(matches!(
            ready,
            Some(CoordinatorReady::Finish {
                outcome: value,
                terminal_meta: EventMeta {
                    session_id,
                    instance_id,
                    dropped_before: 11,
                }
            }) if value.turn_id == expected_turn
                && session_id == expected_session
                && instance_id == expected_instance
        ));
    }
}
