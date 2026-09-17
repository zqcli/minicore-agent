use std::future::Future;
use std::io;

use serde::Serialize;
use serde_json::Value;
use tokio::io::{
    self as tokio_io, AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    BufWriter,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::agent::{Agent, AnswerInteraction, CompactSession, SendMessage};
use crate::changes::{ChangesListRequest, change_deadline};
use crate::diff::{ChangesDiffRequest, diff_deadline};
use crate::error::AgentError;
use crate::event::{AgentEventStream, HistoryPageView, SessionStateView, TurnResultView};
use crate::read::{ReadSession, TurnResultRequest};
use crate::sessions::{
    TurnRef, await_compaction_completion, await_loop_preparation, await_turn_completion,
};
use crate::tool_data::{ToolOutputRequest, ToolReadRequest};
use crate::workspace::listing::WorkspaceFilesRequest;
use crate::workspace::query::{WORKSPACE_READ_DEADLINE, WorkspaceReadRequest};
use crate::workspace::search::WorkspaceSearchRequest;
use crate::workspace::status::WorkspaceStatusRequest;

use super::protocol::{
    AgentEventNotification, CONTEXT_UNCOMPRESSIBLE, CancelledResult, EmptyParams,
    HISTORY_TOO_LARGE, INTERACTION_NOT_FOUND, INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST,
    INVALID_SESSION_SETTINGS, INVALID_STATE, InteractionAnswerParams, METHOD_NOT_FOUND,
    MODEL_NOT_FOUND, ModelsResult, OkResult, PARSE_ERROR, PROFILE_NOT_FOUND, ProfilesResult,
    QUERY_LIMIT, RELOAD_REQUIRES_RESTART, RELOAD_UNAVAILABLE, RESOURCE_EXHAUSTED, RUNTIME_ERROR,
    RpcId, RpcOutbound, RpcRequest, RpcResponse, SESSION_BLOCKED, SESSION_BUSY, SESSION_NOT_FOUND,
    SESSION_NOT_LOADED, STEER_QUEUE_FULL, STORE_ERROR, SessionCompactParams, SessionCreateParams,
    SessionHistoryParams, SessionParams, SessionReadParams, SessionRenameParams, SessionResult,
    SessionUpdateParams, SessionUpdateResult, SessionsResult, SteerResult, TOOL_NOT_FOUND,
    TURN_NOT_FOUND, ToolOutputParams, ToolReadParams, TurnParams, TurnResult, TurnResultParams,
    TurnSendParams, TurnSteerParams, WORKSPACE_ERROR, decode_params, parse_request, request_id,
};
const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;
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
        tokio::signal::ctrl_c(),
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
    S: Future<Output = io::Result<()>> + Send,
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
    query_cancellation: CancellationToken,
}

enum Dispatch {
    Response(RpcResponse),
    Deferred,
    Shutdown(RpcId),
}

enum Frame {
    Eof,
    Data(Vec<u8>),
    Oversized,
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
        S: Future<Output = io::Result<()>> + Send,
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
                            Ok(request) => match self.dispatch(request).await {
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

    async fn dispatch(&mut self, request: RpcRequest) -> Dispatch {
        match self.dispatch_inner(request).await {
            Ok(dispatch) => dispatch,
            Err(response) => Dispatch::Response(response),
        }
    }

    async fn dispatch_inner(&mut self, request: RpcRequest) -> Result<Dispatch, RpcResponse> {
        let RpcRequest { id, method, params } = request;
        tracing::debug!(method = canonical_method(&method), "rpc dispatch");
        Ok(match method.as_str() {
            "agent.ping" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                let result = self.agent().ping();
                Dispatch::Response(success(&id, result))
            }
            "agent.reload" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                let result = self.agent_mut().reload().await;
                Dispatch::Response(agent_result(&id, result))
            }
            "agent.shutdown" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                Dispatch::Shutdown(id)
            }
            "profile.list" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                Dispatch::Response(success(
                    &id,
                    ProfilesResult {
                        profiles: self.agent().list_profiles(),
                    },
                ))
            }
            "model.list" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                Dispatch::Response(success(
                    &id,
                    ModelsResult {
                        models: self.agent().list_models(),
                    },
                ))
            }
            "session.list" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .list_sessions()
                    .await
                    .map(|sessions| SessionsResult { sessions });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.create" => {
                let params: SessionCreateParams = params_or_error(&id, params)?;
                let result = self
                    .agent_mut()
                    .create_session(params.into())
                    .await
                    .map(|session| SessionResult { session });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.open" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self
                    .agent_mut()
                    .open_session(params.session_id)
                    .await
                    .map(|session| SessionResult { session });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.close" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self
                    .agent_mut()
                    .close_session(params.session_id)
                    .await
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.delete" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self
                    .agent_mut()
                    .delete_session(params.session_id)
                    .await
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.state" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .session_state(params.session_id)
                    .map(|state| SessionStateView::from(&state));
                Dispatch::Response(agent_result(&id, result))
            }
            "session.context" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self.agent().session_context(params.session_id);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.compact" => {
                let params: SessionCompactParams = params_or_error(&id, params)?;
                // Reserve waiter capacity before starting the Session-owned
                // operation, so a full waiter set never launches a compaction
                // that cannot be awaited.
                if self.waiters.len() + self.queries.len() >= MAX_DEFERRED_WAITERS {
                    return Err(resource_exhausted(id));
                }
                match self.agent_mut().compact_session(params.into()).await {
                    Ok(receiver) => {
                        let outbound = self.outbound_tx.clone();
                        self.waiters.spawn(async move {
                            let response = match await_compaction_completion(receiver).await {
                                Ok(result) => success(&id, result),
                                Err(error) => agent_error(id, &error),
                            };
                            let _ = outbound.send(RpcOutbound::Response(response)).await;
                        });
                        Dispatch::Deferred
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "session.compact.cancel" => {
                let params: SessionCompactParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .cancel_compaction(params.into())
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.update" => {
                let params: SessionUpdateParams = params_or_error(&id, params)?;
                if !params.has_field() {
                    return Err(invalid_params(Some(id)));
                }
                let result = self
                    .agent_mut()
                    .update_session(params.into())
                    .await
                    .map(SessionUpdateResult::from);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.rename" => {
                let params: SessionRenameParams = params_or_error(&id, params)?;
                let result = self
                    .agent_mut()
                    .rename_session(params.into())
                    .await
                    .map(|session| SessionResult { session });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.history" => {
                let params: SessionHistoryParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .history(params.into())
                    .map(|page| HistoryPageView::from(&page));
                Dispatch::Response(agent_result(&id, result))
            }
            "session.read" => {
                let params: SessionReadParams = params_or_error(&id, params)?;
                let request: ReadSession = params.into();
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response = match tokio::time::timeout(
                        crate::read::READ_DEADLINE,
                        crate::read::read_session(store, loaded, request, cancellation),
                    )
                    .await
                    {
                        Ok(Ok(result)) => success(&id, result),
                        Ok(Err(error)) => query_error(id, &error),
                        Err(_) => agent_error(id, &AgentError::QueryLimit),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "workspace.read" => {
                let request: WorkspaceReadRequest = params_or_error(&id, params)?;
                // Lexical path/range validation happens before a query slot is
                // reserved; the offset itself is checked against the file.
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let session_cancellation = session.query_cancellation();
                let workspace = session.workspace();
                drop(session);
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response = match tokio::time::timeout(
                        WORKSPACE_READ_DEADLINE,
                        crate::workspace::query::read(
                            workspace,
                            request,
                            session_cancellation,
                            cancellation,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(result)) => success(&id, result),
                        Ok(Err(error)) => query_error(id, &error),
                        Err(_) => agent_error(id, &AgentError::QueryLimit),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "workspace.files" => {
                let request: WorkspaceFilesRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let session_cancellation = session.query_cancellation();
                let workspace = session.workspace();
                drop(session);
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    // The scan owns one retained blocking worker and enforces
                    // its own deadline, so an outer timeout here would drop
                    // the worker join instead of cancelling it.
                    let response = match crate::workspace::listing::files(
                        workspace,
                        request,
                        session_cancellation,
                        cancellation,
                    )
                    .await
                    {
                        Ok(result) => success(&id, result),
                        Err(error) => query_error(id, &error),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "workspace.search" => {
                let request: WorkspaceSearchRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let session_cancellation = session.query_cancellation();
                let workspace = session.workspace();
                drop(session);
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    // Same retained-worker contract as `workspace.files`.
                    let response = match crate::workspace::search::search(
                        workspace,
                        request,
                        session_cancellation,
                        cancellation,
                    )
                    .await
                    {
                        Ok(result) => success(&id, result),
                        Err(error) => query_error(id, &error),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "workspace.status" => {
                let request: WorkspaceStatusRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                // Registration happens synchronously inside `prepare`, so the
                // Session owns the worker before this dispatcher can be
                // dropped. The returned future only waits and completes the
                // cache; `session` moves into it.
                let cancellation = self.query_cancellation.clone();
                let query = match crate::queries::prepare_workspace_status(
                    session,
                    request,
                    cancellation,
                ) {
                    Ok(query) => query,
                    Err(error) => return Err(query_error(id, &error)),
                };
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    // The awaiter only observes the owned worker's result; the
                    // worker owns its git child and enforces its own deadline.
                    let response = match query.await {
                        Ok(result) => success(&id, result),
                        Err(error) => query_error(id, &error),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "changes.list" => {
                let request: ChangesListRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let deadline = change_deadline();
                let cancellation = self.query_cancellation.clone();
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                // Workspace scope registers its owned worker synchronously
                // inside `prepare`, before this dispatcher can be dropped. The
                // returned future only waits and post-processes.
                let query = match crate::queries::prepare_changes_list(
                    store,
                    loaded,
                    request,
                    cancellation,
                    deadline,
                ) {
                    Ok(query) => query,
                    Err(error) => return Err(query_error(id, &error)),
                };
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response = match query.await {
                        Ok(result) => success(&id, result),
                        Err(error) => query_error(id, &error),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "changes.diff" => {
                let request: ChangesDiffRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                let deadline = diff_deadline();
                let cancellation = self.query_cancellation.clone();
                // A `workspace:` ref registers its owned source worker here; a
                // tool ref stays a pure cold read that needs no Workspace or
                // model.
                let query = match crate::queries::prepare_changes_diff(
                    store,
                    loaded,
                    request,
                    cancellation,
                    deadline,
                ) {
                    Ok(query) => query,
                    Err(error) => return Err(query_error(id, &error)),
                };
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response = match query.await {
                        Ok(result) => success(&id, result),
                        Err(error) => query_error(id, &error),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "session.presentation" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self.agent().session_presentation(params.session_id);
                Dispatch::Response(agent_result(&id, result))
            }
            "tool.read" => {
                let params: ToolReadParams = params_or_error(&id, params)?;
                let request: ToolReadRequest = params.into();
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let store = self.agent().store_handle();
                let tool_data = self.agent().tool_data(request.tool_ref.session_id).ok();
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response =
                        match crate::read::tool_read(store, tool_data, request, cancellation).await
                        {
                            Ok(result) => success(&id, result),
                            Err(error) => query_error(id, &error),
                        };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "tool.output" => {
                let params: ToolOutputParams = params_or_error(&id, params)?;
                let request: ToolOutputRequest = params.into();
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let store = self.agent().store_handle();
                let tool_data = self.agent().tool_data(request.tool_ref.session_id).ok();
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response =
                        match crate::read::tool_output(store, tool_data, request, cancellation)
                            .await
                        {
                            Ok(result) => success(&id, result),
                            Err(error) => query_error(id, &error),
                        };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "turn.send" => {
                let params: TurnSendParams = params_or_error(&id, params)?;
                let automatic = match self.agent().automatic_compaction_enabled(params.session_id) {
                    Ok(automatic) => automatic,
                    Err(error) => return Err(agent_error(id, &error)),
                };
                if minicore_runtime::execution::UserInput::text(&params.text).is_err() {
                    return Err(invalid_params(Some(id)));
                }
                // Automatic admission may need a deferred response. Reserve
                // waiter capacity before publishing the Session reservation;
                // otherwise a full waiter set could leave a real loop with no
                // response path.
                if automatic && self.waiters.len() + self.queries.len() >= MAX_DEFERRED_WAITERS {
                    return Err(resource_exhausted(id));
                }
                let submission = self
                    .agent_mut()
                    .submit_accepted(SendMessage {
                        session_id: params.session_id,
                        text: params.text,
                    })
                    .await;
                match submission {
                    Ok(crate::sessions::LoopSubmission::Accepted(accepted)) => {
                        Dispatch::Response(success(
                            &id,
                            TurnResult {
                                turn: accepted.turn,
                                accepted_at: accepted.accepted_at,
                            },
                        ))
                    }
                    Ok(crate::sessions::LoopSubmission::Preparing(waiter)) => {
                        let operation_id = waiter.operation_id().to_owned();
                        if self.waiters.len() + self.queries.len() >= MAX_DEFERRED_WAITERS {
                            // Cancel the preparation we just started rather
                            // than leave an unawaitable operation running.
                            let _ = self.agent().cancel_compaction(CompactSession {
                                session_id: params.session_id,
                                operation_id,
                            });
                            return Err(resource_exhausted(id));
                        }
                        let outbound = self.outbound_tx.clone();
                        self.waiters.spawn(async move {
                            let response = match await_loop_preparation(waiter).await {
                                Ok(accepted) => success(
                                    &id,
                                    TurnResult {
                                        turn: accepted.turn,
                                        accepted_at: accepted.accepted_at,
                                    },
                                ),
                                Err(error) => agent_error(id, &error),
                            };
                            let _ = outbound.send(RpcOutbound::Response(response)).await;
                        });
                        Dispatch::Deferred
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "turn.steer" => {
                let params: TurnSteerParams = params_or_error(&id, params)?;
                let result =
                    self.agent()
                        .steer_accepted(params.into())
                        .map(|accepted| SteerResult {
                            ok: true,
                            accepted_at: accepted.accepted_at,
                            steer_index: accepted.steer_index,
                        });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.cancel" => {
                let params: TurnParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .cancel(params.into())
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.wait" => {
                let params: TurnParams = params_or_error(&id, params)?;
                match self.agent().wait_turn_receiver(params.into()) {
                    Ok(receiver) => {
                        if self.waiters.len() + self.queries.len() >= MAX_DEFERRED_WAITERS {
                            return Err(resource_exhausted(id));
                        }
                        let outbound = self.outbound_tx.clone();
                        self.waiters.spawn(async move {
                            let response = match await_turn_completion(receiver).await {
                                Ok(result) => {
                                    success(&id, TurnResultView::from_turn_result(&result))
                                }
                                Err(error) => agent_error(id, &error),
                            };
                            let _ = outbound.send(RpcOutbound::Response(response)).await;
                        });
                        Dispatch::Deferred
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "turn.result" => {
                let params: TurnResultParams = params_or_error(&id, params)?;
                let request: TurnResultRequest = params.into();
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.turn.session_id);
                let cancellation = self.query_cancellation.clone();
                let outbound = self.outbound_tx.clone();
                self.queries.spawn(async move {
                    let response = match tokio::time::timeout(
                        crate::read::READ_DEADLINE,
                        crate::read::turn_result(store, loaded, request, cancellation),
                    )
                    .await
                    {
                        Ok(Ok(result)) => success(&id, result),
                        Ok(Err(error)) => query_error(id, &error),
                        Err(_) => agent_error(id, &AgentError::QueryLimit),
                    };
                    let _ = outbound.send(RpcOutbound::Response(response)).await;
                });
                Dispatch::Deferred
            }
            "interaction.answer" => {
                let params: InteractionAnswerParams = params_or_error(&id, params)?;
                let answer = match params.answer.into_runtime() {
                    Ok(answer) => answer,
                    Err(_) => return Err(invalid_params(Some(id))),
                };
                let turn = TurnRef {
                    session_id: params.session_id,
                    loop_id: params.loop_id,
                };
                let result = self
                    .agent()
                    .answer(AnswerInteraction {
                        turn,
                        interaction_id: params.interaction_id,
                        answer,
                    })
                    .await
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            _ => Dispatch::Response(RpcResponse::error(
                Some(id),
                METHOD_NOT_FOUND,
                "method not found",
                "method_not_found",
                false,
            )),
        })
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

    fn query_capacity_available(&self) -> bool {
        self.queries.len() < MAX_DEFERRED_QUERIES
            && self.waiters.len() + self.queries.len() < MAX_DEFERRED_WAITERS
    }
}

async fn pump_events(mut events: AgentEventStream, outbound: mpsc::Sender<RpcOutbound>) {
    while let Some(event) = events.recv().await {
        if outbound
            .send(RpcOutbound::Event(Box::new(AgentEventNotification::new(
                event,
            ))))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn run_writer<W>(
    writer: W,
    outbound: mpsc::Receiver<RpcOutbound>,
    status: oneshot::Sender<Result<(), AgentError>>,
) where
    W: AsyncWrite + Unpin,
{
    let result = write_outbound(writer, outbound).await;
    if result.is_err() {
        tracing::warn!(kind = "write_failure", "rpc writer failure");
    }
    let _ = status.send(result);
}

async fn write_outbound<W>(
    mut writer: W,
    mut outbound: mpsc::Receiver<RpcOutbound>,
) -> Result<(), AgentError>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = outbound.recv().await {
        let encoded = serde_json::to_vec(&frame).map_err(|_| AgentError::RpcSerialization)?;
        writer.write_all(&encoded).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

fn parse_frame(frame: &[u8]) -> Result<RpcRequest, RpcResponse> {
    let value = serde_json::from_slice::<Value>(frame).map_err(|_| {
        tracing::warn!(kind = "parse_error", "rpc request rejected");
        RpcResponse::error(None, PARSE_ERROR, "parse error", "parse_error", false)
    })?;
    let candidate_id = request_id(&value);
    parse_request(value).map_err(|_| {
        tracing::warn!(kind = "invalid_request", "rpc request rejected");
        RpcResponse::error(
            candidate_id,
            INVALID_REQUEST,
            "invalid request",
            "invalid_request",
            false,
        )
    })
}

fn params_or_error<T>(id: &RpcId, params: Option<Value>) -> Result<T, RpcResponse>
where
    T: serde::de::DeserializeOwned,
{
    decode_params(params).map_err(|_| invalid_params(Some(id.clone())))
}

fn success<T>(id: &RpcId, result: T) -> RpcResponse
where
    T: Serialize,
{
    match serde_json::to_value(result) {
        Ok(result) => RpcResponse::success(id.clone(), result),
        Err(_) => RpcResponse::error(
            Some(id.clone()),
            INTERNAL_ERROR,
            "internal error",
            "internal_error",
            false,
        ),
    }
}

fn agent_result<T>(id: &RpcId, result: Result<T, AgentError>) -> RpcResponse
where
    T: Serialize,
{
    match result {
        Ok(result) => success(id, result),
        Err(error) => agent_error(id.clone(), &error),
    }
}

fn invalid_params(id: Option<RpcId>) -> RpcResponse {
    tracing::debug!(kind = "invalid_params", "rpc request rejected");
    RpcResponse::error(
        id,
        INVALID_PARAMS,
        "invalid params",
        "invalid_params",
        false,
    )
}

fn resource_exhausted(id: RpcId) -> RpcResponse {
    tracing::warn!(
        kind = "resource_exhausted",
        "rpc deferred request limit reached"
    );
    RpcResponse::error(
        Some(id),
        RESOURCE_EXHAUSTED,
        "too many deferred requests",
        "resource_exhausted",
        true,
    )
}

fn query_error(id: RpcId, error: &AgentError) -> RpcResponse {
    if matches!(error, AgentError::InvalidArguments) {
        invalid_params(Some(id))
    } else {
        agent_error(id, error)
    }
}

fn agent_error(id: RpcId, error: &AgentError) -> RpcResponse {
    let (code, message, kind, retryable) = match error {
        AgentError::SessionNotFound => (
            SESSION_NOT_FOUND,
            "session not found",
            "session_not_found",
            false,
        ),
        AgentError::SessionNotLoaded => (
            SESSION_NOT_LOADED,
            "session is not loaded",
            "session_not_loaded",
            false,
        ),
        AgentError::SessionBusy => (SESSION_BUSY, "session is busy", "session_busy", true),
        AgentError::SessionBlocked => (
            SESSION_BLOCKED,
            "session is blocked",
            "session_blocked",
            false,
        ),
        AgentError::SessionAlreadyLoaded
        | AgentError::InvalidInteraction
        | AgentError::InvalidState => (INVALID_STATE, "invalid state", "invalid_state", false),
        AgentError::InteractionNotFound => (
            INTERACTION_NOT_FOUND,
            "interaction not found",
            "interaction_not_found",
            false,
        ),
        AgentError::TurnNotFound => (TURN_NOT_FOUND, "turn not found", "turn_not_found", false),
        AgentError::ToolNotFound => (
            TOOL_NOT_FOUND,
            "tool reference not found",
            "tool_not_found",
            false,
        ),
        AgentError::ContextUncompressible => (
            CONTEXT_UNCOMPRESSIBLE,
            "context cannot be reduced without dropping user constraints",
            "context_uncompressible",
            false,
        ),
        AgentError::ProfileNotFound => (
            PROFILE_NOT_FOUND,
            "profile not found",
            "profile_not_found",
            false,
        ),
        AgentError::ModelNotFound => (MODEL_NOT_FOUND, "model not found", "model_not_found", false),
        AgentError::InvalidSessionSettings => (
            INVALID_SESSION_SETTINGS,
            "invalid session settings",
            "invalid_session_settings",
            false,
        ),
        AgentError::Workspace => (WORKSPACE_ERROR, "workspace error", "workspace_error", false),
        AgentError::Store => (STORE_ERROR, "store error", "store_error", false),
        AgentError::SteerQueueFull => (
            STEER_QUEUE_FULL,
            "steer queue is full",
            "steer_queue_full",
            false,
        ),
        AgentError::HistoryTooLarge => (
            HISTORY_TOO_LARGE,
            "history too large",
            "history_too_large",
            false,
        ),
        AgentError::ReloadRequiresRestart => (
            RELOAD_REQUIRES_RESTART,
            "configuration reload requires restart",
            "reload_requires_restart",
            false,
        ),
        AgentError::ReloadUnavailable => (
            RELOAD_UNAVAILABLE,
            "configuration reload is unavailable",
            "reload_unavailable",
            false,
        ),
        AgentError::Runtime(view) => (
            RUNTIME_ERROR,
            "runtime error",
            "runtime_error",
            view.retryable,
        ),
        AgentError::InvalidInput => {
            return invalid_params(Some(id));
        }
        AgentError::QueryLimit => (QUERY_LIMIT, "query limit reached", "query_limit", true),
        AgentError::Config(_)
        | AgentError::Internal
        | AgentError::EventStreamTaken
        | AgentError::InvalidArguments
        | AgentError::RpcSerialization
        | AgentError::Io(_) => (INTERNAL_ERROR, "internal error", "internal_error", false),
    };
    tracing::debug!(error_kind = kind, retryable = retryable, "rpc domain error");
    RpcResponse::error(Some(id), code, message, kind, retryable)
}

fn canonical_method(method: &str) -> &'static str {
    match method {
        "agent.ping" => "agent.ping",
        "agent.reload" => "agent.reload",
        "agent.shutdown" => "agent.shutdown",
        "profile.list" => "profile.list",
        "model.list" => "model.list",
        "session.list" => "session.list",
        "session.create" => "session.create",
        "session.open" => "session.open",
        "session.close" => "session.close",
        "session.delete" => "session.delete",
        "session.state" => "session.state",
        "session.context" => "session.context",
        "session.compact" => "session.compact",
        "session.compact.cancel" => "session.compact.cancel",
        "session.update" => "session.update",
        "session.rename" => "session.rename",
        "session.history" => "session.history",
        "session.read" => "session.read",
        "session.presentation" => "session.presentation",
        "workspace.read" => "workspace.read",
        "workspace.files" => "workspace.files",
        "workspace.search" => "workspace.search",
        "workspace.status" => "workspace.status",
        "changes.list" => "changes.list",
        "changes.diff" => "changes.diff",
        "tool.read" => "tool.read",
        "tool.output" => "tool.output",
        "turn.send" => "turn.send",
        "turn.steer" => "turn.steer",
        "turn.cancel" => "turn.cancel",
        "turn.wait" => "turn.wait",
        "turn.result" => "turn.result",
        "interaction.answer" => "interaction.answer",
        _ => "unknown",
    }
}

async fn read_frame<R>(reader: &mut R, frame: &mut Vec<u8>) -> io::Result<Frame>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(Frame::Eof)
            } else {
                Ok(Frame::Data(std::mem::take(frame)))
            };
        }

        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |position| position + 1);
        // The limit covers the whole frame, including bytes accumulated before
        // an earlier cancellation.
        if frame
            .len()
            .checked_add(consumed)
            .is_none_or(|length| length > MAX_RPC_LINE_BYTES)
        {
            frame.clear();
            return Ok(Frame::Oversized);
        }
        frame.extend_from_slice(&buffer[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Frame::Data(std::mem::take(frame)));
        }
    }
}

#[cfg(test)]
mod tests;
