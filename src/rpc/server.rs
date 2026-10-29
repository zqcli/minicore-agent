use std::collections::HashSet;
use std::future::Future;
use std::io as std_io;

use serde::Serialize;
use serde_json::Value;
use tokio::io::{
    self as tokio_io, AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    BufWriter,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::agent::{
    Agent, AnswerInteraction, CompactSession, CreateSession, PushInput, RemoveInput, RenameSession,
    UpdateSession,
};
use crate::changes::{ChangesListRequest, change_deadline};
use crate::diff::{ChangesDiffRequest, diff_deadline};
use crate::error::AgentError;
use crate::event::{AgentEventStream, SessionStateView, TurnResultView};
use crate::read::{ReadSession, TurnResultRequest};
use crate::sessions::{TurnRef, await_compaction_completion, await_turn_completion};
use crate::tool_data::{ToolOutputRequest, ToolReadRequest};
use crate::workspace::listing::WorkspaceFilesRequest;
use crate::workspace::query::WorkspaceReadRequest;
use crate::workspace::search::WorkspaceSearchRequest;
use crate::workspace::status::WorkspaceStatusRequest;

use super::protocol::{
    AgentEventNotification, CONTEXT_UNCOMPRESSIBLE, CancelledResult, CompactResult, EmptyParams,
    INPUT_NOT_QUEUED, INPUT_QUEUE_FULL, INTERACTION_NOT_FOUND, INTERNAL_ERROR, INVALID_PARAMS,
    INVALID_REQUEST, INVALID_SESSION_SETTINGS, INVALID_STATE, InteractionAnswerParams,
    METHOD_NOT_FOUND, MODEL_NOT_FOUND, ModelsResult, OkResult, PARSE_ERROR, PROFILE_NOT_FOUND,
    ProfilesResult, QUERY_LIMIT, RELOAD_REQUIRES_RESTART, RELOAD_UNAVAILABLE, RESOURCE_EXHAUSTED,
    RUNTIME_ERROR, RpcId, RpcOutbound, RpcRequest, RpcResponse, SESSION_BLOCKED, SESSION_BUSY,
    SESSION_NOT_FOUND, SESSION_NOT_LOADED, STORE_ERROR, SessionHistoryParams, SessionParams,
    SessionResult, SessionUpdateResult, SessionsResult, TOOL_NOT_FOUND, TURN_NOT_FOUND,
    ToolOutputParams, ToolReadParams, TurnParams, TurnResultParams, TurnWaitResult,
    WORKSPACE_ERROR, decode_params, parse_request, request_id,
};
mod dispatch;
mod errors;
mod io;
#[cfg(test)]
use dispatch::METHODS;
#[cfg(test)]
use dispatch::canonical_method;
use errors::{
    agent_error, agent_result, invalid_params, method_not_found, params_or_error, query_error,
    resource_exhausted, success,
};
#[cfg(test)]
use io::MAX_RPC_LINE_BYTES;
use io::{Frame, parse_frame, pump_events, read_frame, run_writer, shutdown_signal};
const OUTBOUND_CAPACITY: usize = 128;
/// Upper bound on concurrently registered deferred waiters and read queries.
/// These tasks are client-driven, so an unbounded count is a resource leak.
const MAX_DEFERRED_WAITERS: usize = 32;
/// Read scans are heavier than wait notifications, so keep a smaller query
/// subset while retaining the shared deferred admission ceiling above.
const MAX_DEFERRED_QUERIES: usize = 4;

pub async fn run_stdio(agent: Agent) -> Result<(), AgentError> {
    run_with_io(
        agent,
        BufReader::new(tokio_io::stdin()),
        BufWriter::new(tokio_io::stdout()),
        shutdown_signal(),
    )
    .await
}

pub(crate) async fn run_with_io<R, W, S>(
    mut agent: Agent,
    mut reader: R,
    writer: W,
    shutdown_signal: S,
) -> Result<(), AgentError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Send + Unpin + 'static,
    S: Future<Output = std_io::Result<()>> + Send,
{
    let events = agent.take_events()?;
    let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CAPACITY);
    let (writer_status_tx, writer_status_rx) = oneshot::channel();
    let writer_task = tokio::spawn(run_writer(writer, outbound_rx, writer_status_tx));
    let event_task = tokio::spawn(pump_events(events, outbound_tx.clone()));
    let server = RpcServer {
        agent: Some(agent),
        outbound_tx,
        waiters: JoinSet::new(),
        queries: JoinSet::new(),
        operations: JoinSet::new(),
        reservations: HashSet::new(),
        query_cancellation: CancellationToken::new(),
    };
    server
        .run(
            &mut reader,
            shutdown_signal,
            event_task,
            writer_task,
            writer_status_rx,
        )
        .await
}

struct RpcServer {
    agent: Option<Agent>,
    outbound_tx: mpsc::Sender<RpcOutbound>,
    waiters: JoinSet<()>,
    queries: JoinSet<()>,
    operations: JoinSet<DeferredResult>,
    reservations: HashSet<Reservation>,
    query_cancellation: CancellationToken,
}

type ApplyOperation = Box<dyn FnOnce(&mut Agent) -> OperationReply + Send>;

enum OperationReply {
    Response(RpcResponse),
    Waiter(futures_util::future::BoxFuture<'static, RpcResponse>),
}

impl From<RpcResponse> for OperationReply {
    fn from(response: RpcResponse) -> Self {
        Self::Response(response)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
enum Reservation {
    Session(crate::ids::SessionId),
    Reload,
}

#[cfg(test)]
static OPERATION_GATES: crate::test_support::Registry<
    std::path::PathBuf,
    std::sync::Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

struct DeferredResult {
    reservation: Option<Reservation>,
    apply: ApplyOperation,
}

enum Dispatch {
    Response(RpcResponse),
    Deferred,
    Shutdown(RpcId),
}

enum StopReason {
    Eof,
    Signal,
    Shutdown(RpcId),
    Error(AgentError),
    WriterStopped,
}

impl RpcServer {
    async fn run<R, S>(
        mut self,
        reader: &mut R,
        shutdown_signal: S,
        event_task: JoinHandle<()>,
        writer_task: JoinHandle<()>,
        writer_status_rx: oneshot::Receiver<Result<(), AgentError>>,
    ) -> Result<(), AgentError>
    where
        R: AsyncBufRead + Unpin,
        S: Future<Output = std_io::Result<()>> + Send,
    {
        tokio::pin!(shutdown_signal);
        let mut writer_status_rx = writer_status_rx;
        let mut observed_writer_result = None;
        // Accumulated bytes of the frame currently being read. It lives outside
        // the `select!` so cancelling `read_frame` (a resolved deferred waiter,
        // signal, or writer status) cannot discard a partially consumed frame.
        let mut frame_buffer = Vec::new();

        let reason = loop {
            tokio::select! {
                biased;
                writer_result = &mut writer_status_rx => {
                    tracing::warn!(kind = "writer_stopped", "rpc writer failure");
                    observed_writer_result = Some(writer_result.unwrap_or(Err(AgentError::Internal)));
                    break StopReason::WriterStopped;
                }
                signal = &mut shutdown_signal => {
                    break match signal {
                        Ok(()) => StopReason::Signal,
                        Err(error) => {
                            tracing::warn!(kind = "signal_failure", "rpc shutdown signal failed");
                            StopReason::Error(AgentError::Io(error))
                        }
                    };
                }
                operation = self.operations.join_next(), if !self.operations.is_empty() => {
                    match operation.expect("nonempty operations") {
                        Ok(result) => {
                            if self.complete_operation(result).await.is_err() {
                                break StopReason::WriterStopped;
                            }
                        }
                        Err(_) => break StopReason::Error(AgentError::Internal),
                    }
                }
                waiter = self.waiters.join_next(), if !self.waiters.is_empty() => {
                    if waiter.is_some_and(|result| result.is_err()) {
                        tracing::warn!(kind = "waiter_join", "rpc waiter failed");
                        break StopReason::Error(AgentError::Internal);
                    }
                }
                query = self.queries.join_next(), if !self.queries.is_empty() => {
                    if query.is_some_and(|result| result.is_err()) {
                        tracing::warn!(kind = "query_join", "rpc query failed");
                        break StopReason::Error(AgentError::Internal);
                    }
                }
                frame_result = read_frame(reader, &mut frame_buffer) => {
                    match frame_result {
                        Err(error) => {
                            tracing::warn!(kind = "read_failure", "rpc read failed");
                            break StopReason::Error(AgentError::Io(error));
                        }
                        Ok(Frame::Eof) => {
                            tracing::debug!("rpc stdin eof");
                            break StopReason::Eof;
                        }
                        Ok(Frame::Oversized) => {
                            tracing::warn!(kind = "oversized_frame", "rpc request rejected");
                            if self.send(RpcResponse::error(
                                None,
                                PARSE_ERROR,
                                "parse error",
                                "parse_error",
                                false,
                            )).await.is_err() {
                                break StopReason::WriterStopped;
                            }
                            break StopReason::Eof;
                        }
                        Ok(Frame::Data(frame)) => match parse_frame(&frame) {
                            Err(response) => {
                                if self.send(response).await.is_err() {
                                    break StopReason::WriterStopped;
                                }
                            }
                            Ok(request) => match self.dispatch(request) {
                                Dispatch::Response(response) => {
                                    if self.send(response).await.is_err() {
                                        break StopReason::WriterStopped;
                                    }
                                }
                                Dispatch::Deferred => {}
                                Dispatch::Shutdown(id) => break StopReason::Shutdown(id),
                            },
                        },
                    }
                }
            }
        };

        self.finish(
            reason,
            event_task,
            writer_task,
            writer_status_rx,
            observed_writer_result,
        )
        .await
    }

    /// Registers one already-prepared query on the existing queries JoinSet and
    /// encodes its success or `query_error` on the outbound channel.
    ///
    /// The caller keeps admission: capacity was checked and any Session-owned
    /// worker was registered synchronously before this call. The helper adds no
    /// timeout, validation, cancellation, or retry; the caller's future retains
    /// its existing business error mapping.
    fn defer_query<T, F>(&mut self, id: RpcId, future: F) -> Dispatch
    where
        T: Serialize + Send + 'static,
        F: Future<Output = Result<T, AgentError>> + Send + 'static,
    {
        let outbound = self.outbound_tx.clone();
        self.queries.spawn(async move {
            let response = match future.await {
                Ok(result) => success(&id, result),
                Err(error) => query_error(id, &error),
            };
            let _ = outbound.send(RpcOutbound::Response(response)).await;
        });
        Dispatch::Deferred
    }

    fn defer_waiter<T: Serialize + Send + 'static>(
        &mut self,
        id: RpcId,
        future: impl Future<Output = Result<T, AgentError>> + Send + 'static,
    ) -> Dispatch {
        let outbound = self.outbound_tx.clone();
        self.waiters.spawn(async move {
            let response = agent_result(&id, future.await);
            let _ = outbound.send(RpcOutbound::Response(response)).await;
        });
        Dispatch::Deferred
    }

    fn defer_response<T: Serialize + Send + 'static>(
        &mut self,
        id: RpcId,
        reservation: Option<Reservation>,
        future: impl Future<Output = Result<T, AgentError>> + Send + 'static,
    ) -> Dispatch {
        self.defer_operation(reservation, async move {
            let result = future.await;
            Box::new(move |_: &mut Agent| agent_result(&id, result).into()) as ApplyOperation
        })
    }

    fn deferred_count(&self) -> usize {
        self.waiters.len() + self.queries.len() + self.operations.len()
    }

    fn admit_operation(
        &self,
        id: &RpcId,
        reservation: Option<Reservation>,
    ) -> Result<(), RpcResponse> {
        if self.reservations.contains(&Reservation::Reload)
            || (reservation == Some(Reservation::Reload) && !self.operations.is_empty())
            || reservation.is_some_and(|id| self.reservations.contains(&id))
        {
            return Err(agent_error(id.clone(), &AgentError::SessionBusy));
        }
        if self.deferred_count() >= MAX_DEFERRED_WAITERS {
            return Err(resource_exhausted(id.clone()));
        }
        Ok(())
    }

    fn defer_operation(
        &mut self,
        reservation: Option<Reservation>,
        future: impl Future<Output = ApplyOperation> + Send + 'static,
    ) -> Dispatch {
        if let Some(id) = reservation {
            self.reservations.insert(id);
        }
        #[cfg(test)]
        let gate_key = self.agent().config().data_dir.clone();
        self.operations.spawn(async move {
            #[cfg(test)]
            tests::pause_operation(gate_key).await;
            DeferredResult {
                reservation,
                apply: future.await,
            }
        });
        Dispatch::Deferred
    }

    async fn complete_operation(&mut self, result: DeferredResult) -> Result<(), ()> {
        if let Some(id) = result.reservation {
            self.reservations.remove(&id);
        }
        match (result.apply)(self.agent_mut()) {
            OperationReply::Response(response) => self.send(response).await,
            OperationReply::Waiter(future) => {
                let outbound = self.outbound_tx.clone();
                self.waiters.spawn(async move {
                    let _ = outbound.send(RpcOutbound::Response(future.await)).await;
                });
                Ok(())
            }
        }
    }

    async fn send(&self, response: RpcResponse) -> Result<(), ()> {
        self.outbound_tx
            .send(RpcOutbound::Response(response))
            .await
            .map_err(|_| {
                tracing::warn!(kind = "outbound_closed", "rpc writer failure");
            })
    }

    async fn finish(
        mut self,
        reason: StopReason,
        event_task: JoinHandle<()>,
        writer_task: JoinHandle<()>,
        writer_status_rx: oneshot::Receiver<Result<(), AgentError>>,
        mut writer_result: Option<Result<(), AgentError>>,
    ) -> Result<(), AgentError> {
        let reason_kind = match &reason {
            StopReason::Eof => "eof",
            StopReason::Signal => "signal",
            StopReason::Shutdown(_) => "request",
            StopReason::Error(_) => "error",
            StopReason::WriterStopped => "writer_stopped",
        };
        tracing::info!(reason = reason_kind, "rpc shutdown begin");
        let (shutdown_id, mut primary_error) = match reason {
            StopReason::Shutdown(id) => (Some(id), None),
            StopReason::Error(error) => (None, Some(error)),
            StopReason::Eof | StopReason::Signal | StopReason::WriterStopped => (None, None),
        };

        self.query_cancellation.cancel();
        self.agent().begin_shutdown();
        while let Some(result) = self.operations.join_next().await {
            match result {
                Ok(result) => {
                    let _ = self.complete_operation(result).await;
                }
                Err(_) if primary_error.is_none() => primary_error = Some(AgentError::Internal),
                Err(_) => {}
            }
        }
        let shutdown_result = self
            .agent
            .take()
            .expect("RPC server owns Agent until shutdown")
            .shutdown()
            .await;

        while let Some(result) = self.waiters.join_next().await {
            if result.is_err() && primary_error.is_none() {
                primary_error = Some(AgentError::Internal);
            }
        }
        while let Some(result) = self.queries.join_next().await {
            if result.is_err() && primary_error.is_none() {
                primary_error = Some(AgentError::Internal);
            }
        }
        if event_task.await.is_err() && primary_error.is_none() {
            primary_error = Some(AgentError::Internal);
        }

        if let Some(id) = shutdown_id {
            let response = match &shutdown_result {
                Ok(()) => success(&id, OkResult::TRUE),
                Err(error) => agent_error(id, error),
            };
            if self.send(response).await.is_err() && primary_error.is_none() {
                primary_error = Some(AgentError::Internal);
            }
        }

        drop(self.outbound_tx);
        if writer_result.is_none() {
            writer_result = Some(writer_status_rx.await.unwrap_or(Err(AgentError::Internal)));
        }
        let writer_join = writer_task.await;

        let result = if let Some(Err(error)) = writer_result {
            Err(error)
        } else if writer_join.is_err() {
            Err(AgentError::Internal)
        } else if let Some(error) = primary_error {
            Err(error)
        } else {
            shutdown_result
        };
        tracing::info!(success = result.is_ok(), "rpc shutdown end");
        result
    }

    fn agent(&self) -> &Agent {
        self.agent.as_ref().expect("Agent is present while serving")
    }

    fn agent_mut(&mut self) -> &mut Agent {
        self.agent.as_mut().expect("Agent is present while serving")
    }

    fn admit_query(
        &self,
        id: &RpcId,
        validation: Result<(), AgentError>,
    ) -> Result<(), RpcResponse> {
        validation.map_err(|error| query_error(id.clone(), &error))?;
        if !self.query_capacity_available() {
            return Err(resource_exhausted(id.clone()));
        }
        Ok(())
    }

    fn query_capacity_available(&self) -> bool {
        self.queries.len() < MAX_DEFERRED_QUERIES && self.deferred_count() < MAX_DEFERRED_WAITERS
    }
}

#[cfg(test)]
mod tests;
