use std::collections::HashSet;
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

use crate::agent::{
    Agent, AnswerInteraction, CompactSession, CreateSession, RenameSession, SendMessage,
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
    AgentEventNotification, CONTEXT_UNCOMPRESSIBLE, CancelledResult, EmptyParams,
    HISTORY_TOO_LARGE, INTERACTION_NOT_FOUND, INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST,
    INVALID_SESSION_SETTINGS, INVALID_STATE, InteractionAnswerParams, METHOD_NOT_FOUND,
    MODEL_NOT_FOUND, ModelsResult, OkResult, PARSE_ERROR, PROFILE_NOT_FOUND, ProfilesResult,
    QUERY_LIMIT, RELOAD_REQUIRES_RESTART, RELOAD_UNAVAILABLE, RESOURCE_EXHAUSTED, RUNTIME_ERROR,
    RpcId, RpcOutbound, RpcRequest, RpcResponse, SESSION_BLOCKED, SESSION_BUSY, SESSION_NOT_FOUND,
    SESSION_NOT_LOADED, STEER_QUEUE_FULL, STORE_ERROR, SessionHistoryParams, SessionParams,
    SessionResult, SessionUpdateResult, SessionsResult, SteerResult, TOOL_NOT_FOUND,
    TURN_NOT_FOUND, ToolOutputParams, ToolReadParams, TurnParams, TurnResult, TurnResultParams,
    TurnSteerParams, WORKSPACE_ERROR, decode_params, parse_request, request_id,
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

    fn dispatch(&mut self, request: RpcRequest) -> Dispatch {
        match self.dispatch_inner(request) {
            Ok(dispatch) => dispatch,
            Err(response) => Dispatch::Response(response),
        }
    }

    fn dispatch_inner(&mut self, request: RpcRequest) -> Result<Dispatch, RpcResponse> {
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
                self.admit_operation(&id, Some(Reservation::Reload))?;
                let future = self
                    .agent()
                    .prepare_reload()
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(Some(Reservation::Reload), async move {
                    let result = future.await;
                    Box::new(move |agent: &mut Agent| {
                        agent_result(
                            &id,
                            result.and_then(|(config, models)| {
                                agent.apply_reload_settings(config, models)
                            }),
                        )
                        .into()
                    }) as ApplyOperation
                })
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
                self.admit_operation(&id, None)?;
                let store = self.agent().store_handle();
                self.defer_operation(None, async move {
                    let result = store.list_sessions().await.map_err(AgentError::from);
                    Box::new(move |agent: &mut Agent| {
                        agent_result(
                            &id,
                            result.map(|records| SessionsResult {
                                sessions: agent.session_infos(records),
                            }),
                        )
                        .into()
                    }) as ApplyOperation
                })
            }
            "session.create" => {
                let request: CreateSession = params_or_error(&id, params)?;
                self.admit_operation(&id, None)?;
                let session_id = crate::ids::SessionId::new()
                    .map_err(|_| agent_error(id.clone(), &AgentError::Internal))?;
                let future = self
                    .agent()
                    .prepare_create(request, session_id)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = future.await;
                    Box::new(move |agent: &mut Agent| {
                        agent_result(
                            &id,
                            result.map(|(stored, workspace, setup)| SessionResult {
                                session: agent.install_session(stored, workspace, setup),
                            }),
                        )
                        .into()
                    }) as ApplyOperation
                })
            }
            "session.open" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                if let Some(session) = self.agent().loaded_session(session_id) {
                    Dispatch::Response(success(
                        &id,
                        SessionResult {
                            session: session.info(true),
                        },
                    ))
                } else {
                    let store = self.agent().store_handle();
                    self.defer_operation(Some(Reservation::Session(session_id)), async move {
                        let result = Agent::prepare_open(store, session_id).await;
                        Box::new(move |agent: &mut Agent| {
                            agent_result(
                                &id,
                                result
                                    .and_then(|prepared| agent.install_open(prepared))
                                    .map(|session| SessionResult { session }),
                            )
                            .into()
                        }) as ApplyOperation
                    })
                }
            }
            "session.close" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let session = self
                    .agent()
                    .loaded_session(session_id)
                    .ok_or_else(|| agent_error(id.clone(), &AgentError::SessionNotLoaded))?;
                session.begin_shutdown();
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = session.join_shutdown().await;
                    Box::new(move |agent: &mut Agent| {
                        agent.finish_close(session_id);
                        agent_result(&id, result.map(|()| OkResult::TRUE)).into()
                    }) as ApplyOperation
                })
            }
            "session.delete" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                if self.agent().loaded_session(session_id).is_some() {
                    return Err(agent_error(id, &AgentError::SessionAlreadyLoaded));
                }
                let store = self.agent().store_handle();
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = store
                        .delete_session(session_id)
                        .await
                        .map_err(AgentError::from);
                    Box::new(move |_: &mut Agent| {
                        agent_result(&id, result.map(|()| OkResult::TRUE)).into()
                    }) as ApplyOperation
                })
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
                let request: CompactSession = params_or_error(&id, params)?;
                let reservation = Some(Reservation::Session(request.session_id));
                self.admit_operation(&id, reservation)?;
                let future = self
                    .agent()
                    .prepare_compact(request)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(reservation, async move {
                    let result = future.await;
                    Box::new(move |_: &mut Agent| match result {
                        Ok(receiver) => OperationReply::Waiter(Box::pin(async move {
                            agent_result(&id, await_compaction_completion(receiver).await)
                        })),
                        Err(error) => agent_error(id, &error).into(),
                    }) as ApplyOperation
                })
            }
            "session.compact.cancel" => {
                let params: CompactSession = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .cancel_compaction(params)
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.update" => {
                let request: UpdateSession = params_or_error(&id, params)?;
                if !request.has_field() {
                    return Err(invalid_params(Some(id)));
                }
                self.admit_operation(&id, Some(Reservation::Session(request.session_id)))?;
                let session_id = request.session_id;
                let future = self
                    .agent()
                    .prepare_update(request)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    future.await.map(SessionUpdateResult::from)
                })
            }
            "session.rename" => {
                let request: RenameSession = params_or_error(&id, params)?;
                let session_id = request.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let future = self
                    .agent()
                    .prepare_rename(request)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    future.await.map(|session| SessionResult { session })
                })
            }
            "session.history" => {
                let params: SessionHistoryParams = params_or_error(&id, params)?;
                let result = self.agent().history(params.into());
                Dispatch::Response(agent_result(&id, result))
            }
            "session.read" => {
                let params: ReadSession = params_or_error(&id, params)?;
                let request = params;
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_session_read(store, loaded, request, cancellation),
                )
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
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_workspace_read(session, request, cancellation),
                )
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
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_workspace_files(session, request, cancellation),
                )
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
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_workspace_search(session, request, cancellation),
                )
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
                // The awaiter only observes the owned worker's result; the
                // worker owns its git child and enforces its own deadline.
                self.defer_query(id, query)
            }
            "changes.list" => {
                let request: ChangesListRequest = params_or_error(&id, params)?;
                self.admit_query(&id, request.validate())?;
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
                self.defer_query(id, query)
            }
            "changes.diff" => {
                let request: ChangesDiffRequest = params_or_error(&id, params)?;
                self.admit_query(&id, request.validate())?;
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
                self.defer_query(id, query)
            }
            "session.presentation" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self.agent().session_presentation(params.session_id);
                Dispatch::Response(agent_result(&id, result))
            }
            "tool.read" => {
                let params: ToolReadParams = params_or_error(&id, params)?;
                let request: ToolReadRequest = params.into();
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let tool_data = self.agent().tool_data(request.tool_ref.session_id).ok();
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_tool_read(store, tool_data, request, cancellation),
                )
            }
            "tool.output" => {
                let params: ToolOutputParams = params_or_error(&id, params)?;
                let request: ToolOutputRequest = params.into();
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let tool_data = self.agent().tool_data(request.tool_ref.session_id).ok();
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_tool_output(store, tool_data, request, cancellation),
                )
            }
            "turn.send" => {
                let request: SendMessage = params_or_error(&id, params)?;
                let session_id = request.session_id;
                let input = minicore_runtime::execution::UserInput::text(request.text)
                    .map_err(|_| invalid_params(Some(id.clone())))?;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let future = self
                    .agent()
                    .prepare_submit(session_id, input)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    let crate::sessions::LoopSubmission::Accepted(accepted) = future.await?;
                    Ok(TurnResult {
                        turn: accepted.turn,
                        accepted_at: accepted.accepted_at,
                    })
                })
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
                        if self.deferred_count() >= MAX_DEFERRED_WAITERS {
                            return Err(resource_exhausted(id));
                        }
                        self.defer_waiter(id, async move {
                            await_turn_completion(receiver)
                                .await
                                .map(|result| TurnResultView::from_turn_result(&result))
                        })
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "turn.result" => {
                let params: TurnResultParams = params_or_error(&id, params)?;
                let request: TurnResultRequest = params.into();
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.turn.session_id);
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_turn_result(store, loaded, request, cancellation),
                )
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
                    .answer_interaction(AnswerInteraction {
                        turn,
                        interaction_id: params.interaction_id,
                        answer,
                    })
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
        AgentError::StoreUnavailable => (STORE_ERROR, "store error", "store_error", true),
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
        AgentError::Config(_) => (
            super::protocol::CONFIG_ERROR,
            "configuration error",
            "config_error",
            false,
        ),
        AgentError::Internal
        | AgentError::EventStreamTaken
        | AgentError::InvalidArguments
        | AgentError::RpcSerialization
        | AgentError::Io(_) => (INTERNAL_ERROR, "internal error", "internal_error", false),
    };
    tracing::debug!(error_kind = kind, retryable = retryable, "rpc domain error");
    RpcResponse::error(Some(id), code, message, kind, retryable)
}

const METHODS: &[&str] = &[
    "agent.ping",
    "agent.reload",
    "agent.shutdown",
    "profile.list",
    "model.list",
    "session.list",
    "session.create",
    "session.open",
    "session.close",
    "session.delete",
    "session.state",
    "session.context",
    "session.compact",
    "session.compact.cancel",
    "session.update",
    "session.rename",
    "session.history",
    "session.read",
    "session.presentation",
    "workspace.read",
    "workspace.files",
    "workspace.search",
    "workspace.status",
    "changes.list",
    "changes.diff",
    "tool.read",
    "tool.output",
    "turn.send",
    "turn.steer",
    "turn.cancel",
    "turn.wait",
    "turn.result",
    "interaction.answer",
];

fn canonical_method(method: &str) -> &'static str {
    METHODS
        .iter()
        .copied()
        .find(|candidate| *candidate == method)
        .unwrap_or("unknown")
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
