use serde_json::Value;
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::agent::Agent;
use crate::error::AgentError;

use super::protocol::{
    INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND, PARSE_ERROR, RpcResponse, parse_request,
    request_id,
};
const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;

pub async fn run_stdio(agent: Agent) -> Result<(), AgentError> {
    RpcServer::new(agent).run().await
}

struct RpcServer {
    agent: Agent,
}

enum Dispatch {
    Continue(RpcResponse),
    Shutdown(RpcResponse),
}

enum Frame {
    Eof,
    Data(Vec<u8>),
    Oversized,
}

impl RpcServer {
    fn new(agent: Agent) -> Self {
        Self { agent }
    }

    async fn run(self) -> Result<(), AgentError> {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut reader = BufReader::new(stdin);
        let mut writer = BufWriter::new(stdout);

        loop {
            match read_frame(&mut reader).await? {
                Frame::Eof => break,
                Frame::Oversized => {
                    Self::write_response(
                        &mut writer,
                        RpcResponse::error(None, PARSE_ERROR, "parse error", "parse_error"),
                    )
                    .await?;
                    self.agent.shutdown().await?;
                    return Ok(());
                }
                Frame::Data(frame) => match self.dispatch(&frame) {
                    Dispatch::Continue(response) => {
                        Self::write_response(&mut writer, response).await?;
                    }
                    Dispatch::Shutdown(response) => {
                        self.agent.shutdown().await?;
                        Self::write_response(&mut writer, response).await?;
                        return Ok(());
                    }
                },
            }
        }

        self.agent.shutdown().await?;
        writer.flush().await?;
        Ok(())
    }

    async fn write_response(
        writer: &mut BufWriter<io::Stdout>,
        response: RpcResponse,
    ) -> Result<(), AgentError> {
        let encoded = serde_json::to_vec(&response).map_err(|_| AgentError::RpcSerialization)?;
        writer.write_all(&encoded).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }

    fn dispatch(&self, frame: &[u8]) -> Dispatch {
        let value = match serde_json::from_slice::<Value>(frame) {
            Ok(value) => value,
            Err(_) => {
                return Dispatch::Continue(RpcResponse::error(
                    None,
                    PARSE_ERROR,
                    "parse error",
                    "parse_error",
                ));
            }
        };
        let candidate_id = request_id(&value);
        let request = match parse_request(value) {
            Ok(request) => request,
            Err(_) => {
                return Dispatch::Continue(RpcResponse::error(
                    candidate_id,
                    INVALID_REQUEST,
                    "invalid request",
                    "invalid_request",
                ));
            }
        };
        let id = request.id;

        match request.method.as_str() {
            "agent.ping" => {
                if !empty_params(request.params.as_ref()) {
                    return Dispatch::Continue(RpcResponse::error(
                        Some(id),
                        INVALID_PARAMS,
                        "invalid params",
                        "invalid_params",
                    ));
                }
                let result = serde_json::json!({"version": self.agent.ping().version});
                Dispatch::Continue(RpcResponse::success(id, result))
            }
            "agent.shutdown" => {
                if !empty_params(request.params.as_ref()) {
                    return Dispatch::Continue(RpcResponse::error(
                        Some(id),
                        INVALID_PARAMS,
                        "invalid params",
                        "invalid_params",
                    ));
                }
                Dispatch::Shutdown(RpcResponse::success(id, serde_json::json!({"ok": true})))
            }
            _ => Dispatch::Continue(RpcResponse::error(
                Some(id),
                METHOD_NOT_FOUND,
                "method not found",
                "method_not_found",
            )),
        }
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

fn empty_params(params: Option<&serde_json::Value>) -> bool {
    match params {
        None => true,
        Some(serde_json::Value::Object(object)) => object.is_empty(),
        Some(_) => false,
    }
}
