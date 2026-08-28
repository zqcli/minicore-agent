use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const JSONRPC_VERSION: &str = "2.0";
pub(crate) const PARSE_ERROR: i32 = -32_700;
pub(crate) const INVALID_REQUEST: i32 = -32_600;
pub(crate) const METHOD_NOT_FOUND: i32 = -32_601;
pub(crate) const INVALID_PARAMS: i32 = -32_602;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RpcRequest {
    pub(crate) jsonrpc: String,
    pub(crate) id: Option<RpcId>,
    pub(crate) method: String,
    #[serde(default)]
    pub(crate) params: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum RpcId {
    Number(serde_json::Number),
    String(String),
}

#[derive(Serialize)]
pub(crate) struct RpcResponse {
    pub(crate) jsonrpc: &'static str,
    pub(crate) id: Option<RpcId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<RpcError>,
}

#[derive(Serialize)]
pub(crate) struct RpcError {
    pub(crate) code: i32,
    pub(crate) message: &'static str,
    pub(crate) data: RpcErrorData,
}

#[derive(Serialize)]
pub(crate) struct RpcErrorData {
    pub(crate) kind: &'static str,
}

impl RpcResponse {
    pub(crate) fn success(id: RpcId, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id: Some(id),
            result: Some(result),
            error: None,
        }
    }

    pub(crate) fn error(
        id: Option<RpcId>,
        code: i32,
        message: &'static str,
        kind: &'static str,
    ) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result: None,
            error: Some(RpcError {
                code,
                message,
                data: RpcErrorData { kind },
            }),
        }
    }
}
