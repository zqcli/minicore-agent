use std::collections::HashMap;

use crate::event::{AgentEvent, AgentEventSink, CompletionCancellation, EventMeta};

use minicore_runtime::error::SessionShutdownError;
use minicore_runtime::ids::{SessionId, TurnId};
use minicore_runtime::session::{SessionHandle, SessionRuntime, TurnHandle};

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
    pub(crate) completion_cancel: CompletionCancellation,
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
            completion_cancel,
            ..
        } = self;
        let runtime_error = runtime.shutdown().await.err();
        completion_cancel.cancel();
        let event_error = event_task.await.err();
        let state_error = state_task.await.err();
        if let Some(error) = runtime_error {
            return Err(map_session_shutdown_error(error));
        }
        if event_error.is_some() || state_error.is_some() {
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
            loaded.completion_cancel.cancel();
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
