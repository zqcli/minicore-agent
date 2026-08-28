use std::collections::HashMap;

use minicore_runtime::ids::{SessionId, TurnId};
use minicore_runtime::session::{SessionHandle, SessionRuntime, TurnHandle};
use tokio::sync::mpsc;

use crate::event::AgentEvent;
use tokio::task::JoinHandle;

use crate::error::AgentError;

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, LoadedSession>,
}

pub(crate) struct LoadedSession {
    pub(crate) runtime: SessionRuntime,
    pub(crate) handle: SessionHandle,
    pub(crate) active_turn: Option<TurnHandle>,
    pub(crate) event_task: JoinHandle<()>,
    pub(crate) state_task: JoinHandle<()>,
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

    pub(crate) async fn shutdown_all(
        &mut self,
        events_tx: &mpsc::Sender<AgentEvent>,
    ) -> Result<(), AgentError> {
        let mut ids = self.loaded.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        let mut first_error = None;
        for session_id in ids {
            let Some(loaded) = self.loaded.remove(&session_id) else {
                continue;
            };
            let result = loaded.shutdown().await;
            if result.is_ok() {
                let _ = events_tx.try_send(AgentEvent::SessionClosed { session_id });
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
            ..
        } = self;
        let runtime_error = runtime.shutdown().await.err();
        let event_error = event_task.await.err();
        let state_error = state_task.await.err();
        if runtime_error.is_some() {
            return Err(AgentError::Core(crate::error::CoreErrorView::new(
                "session shutdown failed",
                false,
            )));
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
