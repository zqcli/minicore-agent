use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::agent::Agent;
use crate::error::AgentError;

use super::protocol::{
    INVALID_PARAMS, INVALID_REQUEST, JSONRPC_VERSION, METHOD_NOT_FOUND, PARSE_ERROR, RpcRequest,
    RpcResponse,
};
const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;

pub async fn run_stdio(agent: Agent) -> Result<(), AgentError> {
    RpcServer::new(agent).run().await
}

struct RpcServer {
    agent: Agent,
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
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break;
            }

            let (response, stop) = if bytes_read > MAX_RPC_LINE_BYTES {
                (
                    RpcResponse::error(None, PARSE_ERROR, "parse error", "parse_error"),
                    true,
                )
            } else {
                self.dispatch(&line)
            };
            self.write_response(&mut writer, response).await?;
            if stop {
                break;
            }
        }

        self.agent.shutdown().await?;
        writer.flush().await?;
        Ok(())
    }

    async fn write_response(
        &self,
        writer: &mut BufWriter<io::Stdout>,
        response: RpcResponse,
    ) -> Result<(), AgentError> {
        let encoded = serde_json::to_vec(&response).map_err(|_| AgentError::RpcSerialization)?;
        writer.write_all(&encoded).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }

    fn dispatch(&self, line: &str) -> (RpcResponse, bool) {
        let request = match serde_json::from_str::<RpcRequest>(line) {
            Ok(request) => request,
            Err(_) => {
                return (
                    RpcResponse::error(None, PARSE_ERROR, "parse error", "parse_error"),
                    false,
                );
            }
        };

        let id = request.id.clone();
        if request.jsonrpc != JSONRPC_VERSION {
            return (
                RpcResponse::error(id, INVALID_REQUEST, "invalid request", "invalid_request"),
                false,
            );
        }
        let Some(id) = request.id else {
            return (
                RpcResponse::error(None, INVALID_REQUEST, "invalid request", "invalid_request"),
                false,
            );
        };

        match request.method.as_str() {
            "agent.ping" => {
                if !empty_params(request.params.as_ref()) {
                    return (
                        RpcResponse::error(
                            Some(id),
                            INVALID_PARAMS,
                            "invalid params",
                            "invalid_params",
                        ),
                        false,
                    );
                }
                let result = serde_json::json!({"version": self.agent.ping().version});
                (RpcResponse::success(id, result), false)
            }
            "agent.shutdown" => {
                if !empty_params(request.params.as_ref()) {
                    return (
                        RpcResponse::error(
                            Some(id),
                            INVALID_PARAMS,
                            "invalid params",
                            "invalid_params",
                        ),
                        false,
                    );
                }
                (
                    RpcResponse::success(id, serde_json::json!({"ok": true})),
                    true,
                )
            }
            _ => (
                RpcResponse::error(
                    Some(id),
                    METHOD_NOT_FOUND,
                    "method not found",
                    "method_not_found",
                ),
                false,
            ),
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
