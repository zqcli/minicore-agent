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

use minicore_runtime::error::TurnWaitError;

use crate::agent::{Agent, AnswerInteraction, GetTranscript, SendMessage};
use crate::error::AgentError;
use crate::event::{AgentEventStream, SessionStateView, TurnOutcomeView};

use super::protocol::{
    AgentEventNotification, CORE_ERROR, CancelledResult, EmptyParams, INTERACTION_NOT_FOUND,
    INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, INVALID_STATE, InteractionAnswerParams,
    METHOD_NOT_FOUND, MODEL_NOT_FOUND, ModelsResult, OkResult, PARSE_ERROR, PROFILE_NOT_FOUND,
    PROVIDER_ERROR, ProfilesResult, RpcId, RpcOutbound, RpcRequest, RpcResponse, SESSION_BUSY,
    SESSION_CLOSED, SESSION_NOT_FOUND, SESSION_NOT_LOADED, STORE_ERROR, SessionCreateParams,
    SessionParams, SessionResult, SessionTranscriptParams, SessionsResult, TURN_NOT_FOUND,
    TranscriptPageView, TurnParams, TurnResult, TurnSendParams, WORKSPACE_ERROR, decode_params,
    parse_request, request_id,
};

const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;
const OUTBOUND_CAPACITY: usize = 128;

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
                frame = read_frame(reader) => {
                    match frame {
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
        let RpcRequest { id, method, params } = request;
        tracing::debug!(method = canonical_method(&method), "rpc dispatch");
        match method.as_str() {
            "agent.ping" => {
                let _: EmptyParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self.agent().ping();
                Dispatch::Response(success(&id, result))
            }
            "agent.shutdown" => {
                let _: EmptyParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                Dispatch::Shutdown(id)
            }
            "profile.list" => {
                let _: EmptyParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                Dispatch::Response(success(
                    &id,
                    ProfilesResult {
                        profiles: self.agent().profile_infos(),
                    },
                ))
            }
            "model.list" => {
                let _: EmptyParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                Dispatch::Response(success(
                    &id,
                    ModelsResult {
                        models: self.agent().model_infos(),
                    },
                ))
            }
            "session.list" => {
                let _: EmptyParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent()
                    .list_sessions()
                    .await
                    .map(|sessions| SessionsResult { sessions });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.create" => {
                let params: SessionCreateParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent_mut()
                    .create_session(params.into())
                    .await
                    .map(|session| SessionResult { session });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.open" => {
                let params: SessionParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent_mut()
                    .open_session(params.session_id)
                    .await
                    .map(|session| SessionResult { session });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.close" => {
                let params: SessionParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent_mut()
                    .close_session(params.session_id)
                    .await
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.delete" => {
                let params: SessionParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent_mut()
                    .delete_session(params.session_id)
                    .await
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.state" => {
                let params: SessionParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent()
                    .session_state(params.session_id)
                    .map(|state| SessionStateView::from(&state));
                Dispatch::Response(agent_result(&id, result))
            }
            "session.transcript" => {
                let params: SessionTranscriptParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                if params.validate().is_err() {
                    return Dispatch::Response(invalid_params(Some(id)));
                }
                let result = self
                    .agent()
                    .transcript(GetTranscript {
                        session_id: params.session_id,
                        after: params.after,
                        limit: params.limit,
                    })
                    .await
                    .map(TranscriptPageView::from);
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.send" => {
                let params: TurnSendParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent_mut()
                    .send(SendMessage {
                        session_id: params.session_id,
                        text: params.text,
                    })
                    .await
                    .map(|turn| TurnResult { turn });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.cancel" => {
                let params: TurnParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let result = self
                    .agent()
                    .cancel(params.into())
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.wait" => {
                let params: TurnParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                match self.agent().turn_handle(params.into()) {
                    Ok(handle) => {
                        let outbound = self.outbound_tx.clone();
                        self.waiters.spawn(async move {
                            let response = match handle.wait().await {
                                Ok(outcome) => success(&id, TurnOutcomeView::from(&outcome)),
                                Err(error) => turn_wait_error(id, &error),
                            };
                            let _ = outbound.send(RpcOutbound::Response(response)).await;
                        });
                        Dispatch::Deferred
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "interaction.answer" => {
                let params: InteractionAnswerParams = match params_or_error(&id, params) {
                    Ok(params) => params,
                    Err(response) => return Dispatch::Response(response),
                };
                let answer = match params.answer.into_runtime() {
                    Ok(answer) => answer,
                    Err(_) => return Dispatch::Response(invalid_params(Some(id))),
                };
                let result = self
                    .agent()
                    .answer(AnswerInteraction {
                        session_id: params.session_id,
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
        AgentError::SessionClosed => (SESSION_CLOSED, "session is closed", "session_closed", false),
        AgentError::SessionAlreadyLoaded
        | AgentError::SessionSpecMismatch
        | AgentError::SessionDegraded
        | AgentError::InvalidInteraction => {
            (INVALID_STATE, "invalid state", "invalid_state", false)
        }
        AgentError::InteractionNotFound => (
            INTERACTION_NOT_FOUND,
            "interaction not found",
            "interaction_not_found",
            false,
        ),
        AgentError::TurnNotFound => (TURN_NOT_FOUND, "turn not found", "turn_not_found", false),
        AgentError::ProfileNotFound => (
            PROFILE_NOT_FOUND,
            "profile not found",
            "profile_not_found",
            false,
        ),
        AgentError::ModelNotFound => (MODEL_NOT_FOUND, "model not found", "model_not_found", false),
        AgentError::Workspace => (WORKSPACE_ERROR, "workspace error", "workspace_error", false),
        AgentError::Store => (STORE_ERROR, "store error", "store_error", false),
        AgentError::ModelNotImplemented => {
            (PROVIDER_ERROR, "provider error", "provider_error", false)
        }
        AgentError::Core(view) => (CORE_ERROR, "core error", "core_error", view.retryable),
        AgentError::InvalidInput => {
            return invalid_params(Some(id));
        }
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

fn turn_wait_error(id: RpcId, error: &TurnWaitError) -> RpcResponse {
    let retryable = match error {
        TurnWaitError::DurabilityUnknown(diagnostic)
        | TurnWaitError::DurabilityUnavailable(diagnostic)
        | TurnWaitError::RuntimeTerminated(diagnostic) => diagnostic.retryable,
        _ => false,
    };
    tracing::debug!(
        error_kind = "core_error",
        retryable = retryable,
        "rpc domain error"
    );
    RpcResponse::error(Some(id), CORE_ERROR, "core error", "core_error", retryable)
}

fn canonical_method(method: &str) -> &'static str {
    match method {
        "agent.ping" => "agent.ping",
        "agent.shutdown" => "agent.shutdown",
        "profile.list" => "profile.list",
        "model.list" => "model.list",
        "session.list" => "session.list",
        "session.create" => "session.create",
        "session.open" => "session.open",
        "session.close" => "session.close",
        "session.delete" => "session.delete",
        "session.state" => "session.state",
        "session.transcript" => "session.transcript",
        "turn.send" => "turn.send",
        "turn.cancel" => "turn.cancel",
        "turn.wait" => "turn.wait",
        "interaction.answer" => "interaction.answer",
        _ => "unknown",
    }
}

async fn read_frame<R>(reader: &mut R) -> io::Result<Frame>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(Frame::Eof)
            } else {
                Ok(Frame::Data(frame))
            };
        }

        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |position| position + 1);
        if frame
            .len()
            .checked_add(consumed)
            .is_none_or(|length| length > MAX_RPC_LINE_BYTES)
        {
            return Ok(Frame::Oversized);
        }
        frame.extend_from_slice(&buffer[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Frame::Data(frame));
        }
    }
}

#[cfg(test)]
mod tests;
