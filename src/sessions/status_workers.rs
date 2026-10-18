use super::*;

/// Owned `workspace.status` workers run at most this many at a time.
pub(crate) const MAX_STATUS_QUERY_WORKERS: usize = 4;

/// The Session owns `workspace.status` worker tasks. Each worker keeps its git
/// child until the child is stopped and reaped, so closing the Session joins
/// this set instead of relying on the child's drop behaviour.
pub(super) struct StatusWorkers {
    pub(super) workers: Vec<JoinHandle<()>>,
}

impl StatusWorkers {
    pub(super) fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    /// Drops the handles of workers that already finished and reaped.
    fn reap_finished(&mut self) {
        self.workers.retain(|handle| !handle.is_finished());
    }
}

/// One owned `workspace.status` query. Awaiting it observes the worker's
/// result only; dropping it stops that query's child through the child token,
/// never through the Session token, so other queries keep running.
pub(crate) struct StatusQuery<T = crate::WorkspaceStatusResult> {
    receiver: tokio::sync::oneshot::Receiver<Result<T, AgentError>>,
    child_cancel: CancellationToken,
    shutdown: CancellationToken,
}

impl<T> StatusQuery<T> {
    pub(crate) async fn wait(mut self) -> Result<T, AgentError> {
        let received = tokio::select! {
            result = &mut self.receiver => Some(result),
            _ = self.child_cancel.cancelled() => None,
            _ = self.shutdown.cancelled() => None,
        };
        let received = match received {
            Some(result) => result,
            None => match tokio::time::timeout(
                crate::workspace::WORKSPACE_CANCEL_GRACE,
                &mut self.receiver,
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        "cancelled workspace status waiter detached from stalled worker"
                    );
                    return Err(AgentError::QueryLimit);
                }
            },
        };
        match received {
            Ok(result) => result,
            // The worker ended without publishing a result, so nothing was
            // observed; the failure stays inside the query boundary.
            Err(_) => Err(AgentError::Internal),
        }
    }
}

impl<T> Drop for StatusQuery<T> {
    fn drop(&mut self) {
        self.child_cancel.cancel();
    }
}

impl Session {
    /// Completes an explicit `workspace.status` observation. Branch is a
    /// projection of the complete status result, never a separate Git query;
    /// an unavailable or incomplete observation clears the cache. The Session
    /// lock makes close and projection linearizable, while the caller token
    /// prevents a cancelled request from publishing a late result.
    pub(crate) fn complete_status_query(
        &self,
        result: &crate::WorkspaceStatusResult,
        cancellation: &CancellationToken,
    ) {
        let inner = self.lock_inner();
        if inner.closing || self.shared.close.is_cancelled() || cancellation.is_cancelled() {
            return;
        }
        let branch = result
            .complete
            .then(|| {
                result
                    .repo_available
                    .then(|| result.branch.clone())
                    .flatten()
            })
            .flatten();
        inner.presentation.set_branch(branch);
    }

    /// Starts one owned `workspace.status` query. Registration and the capacity
    /// check happen under the worker lock, so a Session that starts closing
    /// either observes this worker or refuses it here. The worker captures only
    /// the workspace, the request, the observed tokens, and the result sender;
    /// it never holds the Session.
    pub(crate) fn spawn_status_query(
        &self,
        workspace: Arc<Workspace>,
        request: crate::WorkspaceStatusRequest,
        shutdown_cancellation: CancellationToken,
    ) -> Result<StatusQuery, AgentError> {
        let deadline = crate::workspace::status::status_deadline_at(workspace.root());
        self.spawn_status_query_with_deadline(workspace, request, shutdown_cancellation, deadline)
    }

    pub(crate) fn spawn_status_query_with_deadline(
        &self,
        workspace: Arc<Workspace>,
        request: crate::WorkspaceStatusRequest,
        shutdown_cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<StatusQuery, AgentError> {
        self.register_worker(
            shutdown_cancellation.clone(),
            move |session_cancel, child_cancel| async move {
                crate::workspace::status::status_with_deadline(
                    &workspace,
                    &request,
                    &session_cancel,
                    &shutdown_cancellation,
                    &child_cancel,
                    deadline,
                )
                .await
            },
        )
    }

    pub(crate) fn spawn_workspace_diff(
        &self,
        request: crate::ChangesDiffRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<StatusQuery<crate::diff::DiffSources>, AgentError> {
        let workspace = self.workspace();
        self.register_worker(
            cancellation.clone(),
            move |session_cancel, child_cancel| async move {
                crate::workspace::status::diff_sources(
                    &workspace,
                    &request,
                    &session_cancel,
                    &cancellation,
                    &child_cancel,
                    deadline,
                )
                .await
            },
        )
    }

    fn register_worker<T, F>(
        &self,
        shutdown: CancellationToken,
        work: impl FnOnce(CancellationToken, CancellationToken) -> F,
    ) -> Result<StatusQuery<T>, AgentError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, AgentError>> + Send + 'static,
    {
        let mut workers = self.shared.status_workers.lock().unwrap();
        workers.reap_finished();
        if self.shared.close.is_cancelled() {
            return Err(AgentError::SessionNotLoaded);
        }
        if workers.workers.len() >= MAX_STATUS_QUERY_WORKERS {
            return Err(AgentError::QueryLimit);
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let child_cancel = self.shared.close.child_token();
        let future = work(self.shared.close.clone(), child_cancel.clone());
        workers.workers.push(tokio::spawn(async move {
            let _ = sender.send(future.await);
        }));
        Ok(StatusQuery {
            receiver,
            child_cancel,
            shutdown,
        })
    }

    /// Joins registered workers after Session cancellation has stopped admission.
    pub(super) async fn join_status_workers(&self) {
        let workers = {
            let mut workers = self.shared.status_workers.lock().unwrap();
            std::mem::take(&mut workers.workers)
        };
        let deadline = tokio::time::Instant::now() + crate::workspace::WORKSPACE_CANCEL_GRACE;
        for mut handle in workers {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                tracing::warn!("detaching cancelled workspace status worker after reap grace");
            }
        }
    }
}
