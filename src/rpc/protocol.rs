use serde::{Serialize, Serializer};
use serde_json::Value;

pub(crate) const JSONRPC_VERSION: &str = "2.0";
pub(crate) const PARSE_ERROR: i32 = -32_700;
pub(crate) const INVALID_REQUEST: i32 = -32_600;
pub(crate) const METHOD_NOT_FOUND: i32 = -32_601;
pub(crate) const INVALID_PARAMS: i32 = -32_602;

#[derive(Clone, Debug)]
pub(crate) struct RpcRequest {
    pub(crate) id: RpcId,
    pub(crate) method: String,
    pub(crate) params: Option<Value>,
}

#[derive(Clone, Debug)]
pub(crate) enum RpcId {
    Number(serde_json::Number),
    String(String),
}

impl Serialize for RpcId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Number(value) => value.serialize(serializer),
            Self::String(value) => value.serialize(serializer),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidRequest;

pub(crate) fn parse_request(value: Value) -> Result<RpcRequest, InvalidRequest> {
    let object = value.as_object().ok_or(InvalidRequest)?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "jsonrpc" | "id" | "method" | "params"))
    {
        return Err(InvalidRequest);
    }

    if object.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC_VERSION) {
        return Err(InvalidRequest);
    }
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| !method.is_empty())
        .ok_or(InvalidRequest)?
        .to_owned();
    let id = object.get("id").and_then(parse_id).ok_or(InvalidRequest)?;

    Ok(RpcRequest {
        id,
        method,
        params: object.get("params").cloned(),
    })
}

pub(crate) fn request_id(value: &Value) -> Option<RpcId> {
    value
        .as_object()
        .and_then(|object| object.get("id"))
        .and_then(parse_id)
}

fn parse_id(value: &Value) -> Option<RpcId> {
    match value {
        Value::String(value) => Some(RpcId::String(value.clone())),
        Value::Number(value) if value.is_i64() || value.is_u64() => {
            Some(RpcId::Number(value.clone()))
        }
        Value::Array(_) | Value::Bool(_) | Value::Null | Value::Number(_) | Value::Object(_) => {
            None
        }
    }
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
