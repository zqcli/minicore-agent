use super::*;
use crate::queries::QueryFuture;
use crate::read::{TurnResultPage, TurnResultRequest};
use tokio_util::sync::CancellationToken;

const MAX_RESULT_QUERIES: usize = 4;

impl ResultCatalog {
    pub(crate) fn query(
        &self,
        store: Store,
        loaded: Option<Session>,
        request: TurnResultRequest,
        caller: CancellationToken,
    ) -> Result<QueryFuture<TurnResultPage>, AgentError> {
        let close = loaded
            .as_ref()
            .map_or_else(CancellationToken::new, Session::query_cancellation);
        let tokens = {
            let state = self.state.lock().unwrap();
            if caller.is_cancelled() || self.queries.is_closed() {
                return Err(AgentError::QueryLimit);
            }
            if close.is_cancelled() {
                return Err(AgentError::SessionNotLoaded);
            }
            if self.queries.len() >= MAX_RESULT_QUERIES {
                return Err(AgentError::QueryLimit);
            }
            let local = loaded
                .as_ref()
                .map(|session| state.loads[&session.load()].queries.token());
            (self.queries.token(), local)
        };
        let cancel = caller.child_token();
        let guard = cancel.clone().drop_guard();
        let shutdown = self.query_cancel.clone();
        let deadline = tokio::time::Instant::now() + crate::read::READ_DEADLINE;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            // Tokens hold both the concurrency slot and shutdown ownership until all work exits.
            let _tokens = tokens;
            let future = crate::read::turn_result(store, loaded, request, cancel.clone());
            tokio::pin!(future);
            let result = tokio::select! {
                result = &mut future => result,
                _ = cancel.cancelled() => future.await,
                _ = close.cancelled() => { cancel.cancel(); future.await },
                _ = shutdown.cancelled() => { cancel.cancel(); future.await },
                _ = tokio::time::sleep_until(deadline) => { cancel.cancel(); future.await },
            };
            let _ = sender.send(result);
        });
        Ok(Box::pin(async move {
            let _guard = guard;
            receiver.await.map_err(|_| AgentError::Internal)?
        }))
    }

    pub(crate) fn cancel_queries(&self) {
        let _state = self.state.lock().unwrap();
        self.queries.close();
        self.query_cancel.cancel();
    }

    pub(crate) async fn join_queries(&self, load: Option<LoadId>) {
        let tracker = {
            let state = self.state.lock().unwrap();
            let tracker = match load {
                Some(load) => &state.loads[&load].queries,
                None => &self.queries,
            };
            tracker.close();
            tracker.clone()
        };
        tracker.wait().await;
    }
}
