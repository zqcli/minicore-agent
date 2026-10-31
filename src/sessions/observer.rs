use super::*;

pub(super) struct StateObserver {
    stop: CancellationToken,
    pub(super) task: Arc<SessionTask>,
}

impl StateObserver {
    pub(super) fn spawn(
        session: &Session,
        loop_id: LoopId,
        mut state: watch::Receiver<minicore_runtime::LoopState>,
        mut completion: watch::Receiver<Option<TurnCompletion>>,
    ) -> Self {
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let closed = session.shared.close.clone();
        #[cfg(test)]
        let session_id = session.session_id();
        let session = Arc::downgrade(&session.shared);
        let task = Arc::new(SessionTask::empty());
        task.set(tokio::spawn(async move {
            #[cfg(test)]
            if let Some(gate) = PAUSE_OBSERVER_INITIAL.take(&session_id) {
                gate.pass().await;
            }
            state.borrow_and_update();
            if !Self::observed(&session, loop_id) {
                return;
            }
            loop {
                tokio::select! {
                    changed = state.changed() => {
                        state.borrow_and_update();
                        if !Self::observed(&session, loop_id) || changed.is_err() {
                            return;
                        }
                    }
                    _ = completion.changed() => {
                        Self::observed(&session, loop_id);
                        return;
                    }
                    _ = stopped.cancelled() => {
                        Self::observed(&session, loop_id);
                        return;
                    }
                    _ = closed.cancelled() => {
                        Self::observed(&session, loop_id);
                        return;
                    }
                }
            }
        }));
        Self { stop, task }
    }

    fn observed(shared: &std::sync::Weak<SessionShared>, loop_id: LoopId) -> bool {
        let Some(shared) = shared.upgrade() else {
            return false;
        };
        let session = Session::from_shared(shared);
        let installed = {
            let inner = session.lock_inner();
            inner
                .active
                .as_ref()
                .is_some_and(|active| active.turn.loop_id == loop_id)
        };
        if installed {
            session.mark_dirty();
        }
        installed
    }
}

impl Drop for StateObserver {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
