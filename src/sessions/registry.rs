use super::*;

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, Session>,
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

    pub(crate) fn get(&self, session_id: SessionId) -> Option<&Session> {
        self.loaded.get(&session_id)
    }

    pub(crate) fn insert(&mut self, session_id: SessionId, session: Session) -> Option<Session> {
        self.loaded.insert(session_id, session)
    }

    pub(crate) fn remove(&mut self, session_id: SessionId) -> Option<Session> {
        let session = self.loaded.remove(&session_id)?;
        session.shared.store.unload_history(session_id);
        // Workspace queries are owned by the loaded Session; closing it stops
        // them instead of reading a Workspace whose Session is gone.
        session.cancel_queries();
        Some(session)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Session> {
        self.loaded.values()
    }

    pub(crate) async fn shutdown_all(&mut self) -> Result<(), AgentError> {
        let mut ids = self.loaded.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        for session in self.loaded.values() {
            session.begin_shutdown();
        }
        let results = futures_util::future::join_all(
            ids.iter().map(|id| self.loaded[id].clone().join_shutdown()),
        )
        .await;
        for id in ids {
            self.remove(id);
        }
        results.into_iter().collect()
    }
}

impl Drop for Sessions {
    fn drop(&mut self) {
        // Ordinary Agent drop is intentionally non-blocking. Manual and
        // compaction workers receive cancellation through their Session-owned
        // token; the async shutdown path above is the complete
        // join barrier.
        for session in self.loaded.values() {
            session.cancel_active_on_drop();
            session.cancel_compaction_on_drop();
            session.cancel_queries();
        }
    }
}
