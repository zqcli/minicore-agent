use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

use minicore_runtime::conversation::ConversationSeq;
use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, TurnId};
use minicore_runtime::session::InteractionAnswer;
use minicore_runtime::tools::{ApprovalDecision, ToolInputAnswer};
use minicore_runtime::value::BoundedText;

use crate::agent::{CreateSession, SessionInfo, TurnRef};
use crate::event::AgentEvent;
use crate::models::ModelInfo;
use crate::profiles::ProfileInfo;

pub(crate) const JSONRPC_VERSION: &str = "2.0";
pub(crate) const PARSE_ERROR: i32 = -32_700;
pub(crate) const INVALID_REQUEST: i32 = -32_600;
pub(crate) const METHOD_NOT_FOUND: i32 = -32_601;
pub(crate) const INVALID_PARAMS: i32 = -32_602;
pub(crate) const INTERNAL_ERROR: i32 = -32_603;
pub(crate) const SESSION_NOT_FOUND: i32 = -32_001;
pub(crate) const SESSION_NOT_LOADED: i32 = -32_002;
pub(crate) const SESSION_BUSY: i32 = -32_003;
pub(crate) const SESSION_CLOSED: i32 = -32_004;
pub(crate) const INVALID_STATE: i32 = -32_005;
pub(crate) const INTERACTION_NOT_FOUND: i32 = -32_006;
pub(crate) const TURN_NOT_FOUND: i32 = -32_007;
pub(crate) const PROFILE_NOT_FOUND: i32 = -32_008;
pub(crate) const MODEL_NOT_FOUND: i32 = -32_009;
pub(crate) const WORKSPACE_ERROR: i32 = -32_010;
pub(crate) const STORE_ERROR: i32 = -32_011;
pub(crate) const PROVIDER_ERROR: i32 = -32_012;
pub(crate) const CORE_ERROR: i32 = -32_013;

#[derive(Clone, Debug)]
pub(crate) struct RpcRequest {
    pub(crate) id: RpcId,
    pub(crate) method: String,
    pub(crate) params: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidParams;

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

pub(crate) fn decode_params<T>(params: Option<Value>) -> Result<T, InvalidParams>
where
    T: DeserializeOwned,
{
    let value = params.unwrap_or_else(|| Value::Object(Default::default()));
    if !value.is_object() {
        return Err(InvalidParams);
    }
    serde_json::from_value(value).map_err(|_| InvalidParams)
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmptyParams {}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionCreateParams {
    pub(crate) workspace: PathBuf,
    #[serde(default)]
    pub(crate) profile: String,
    #[serde(default)]
    pub(crate) title: Option<String>,
}

impl From<SessionCreateParams> for CreateSession {
    fn from(value: SessionCreateParams) -> Self {
        Self {
            workspace: value.workspace,
            profile: value.profile,
            title: value.title,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionParams {
    pub(crate) session_id: SessionId,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionTranscriptParams {
    pub(crate) session_id: SessionId,
    #[serde(default)]
    pub(crate) after: Option<ConversationSeq>,
    #[serde(default = "default_transcript_limit")]
    pub(crate) limit: usize,
}

impl SessionTranscriptParams {
    pub(crate) fn validate(&self) -> Result<(), InvalidParams> {
        if (1..=100).contains(&self.limit) {
            Ok(())
        } else {
            Err(InvalidParams)
        }
    }
}

fn default_transcript_limit() -> usize {
    100
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnSendParams {
    pub(crate) session_id: SessionId,
    pub(crate) text: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnParams {
    pub(crate) session_id: SessionId,
    pub(crate) instance_id: SessionInstanceId,
    pub(crate) turn_id: TurnId,
}

impl From<TurnParams> for TurnRef {
    fn from(value: TurnParams) -> Self {
        Self {
            session_id: value.session_id,
            instance_id: value.instance_id,
            turn_id: value.turn_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InteractionAnswerParams {
    pub(crate) session_id: SessionId,
    pub(crate) interaction_id: InteractionId,
    pub(crate) answer: InteractionAnswerWire,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum InteractionAnswerWire {
    Approval { decision: ApprovalDecisionWire },
    Text { text: String },
    Choice { index: usize },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ApprovalDecisionWire {
    AllowOnce,
    Deny,
}

impl InteractionAnswerWire {
    pub(crate) fn into_runtime(self) -> Result<InteractionAnswer, InvalidParams> {
        match self {
            Self::Approval { decision } => Ok(InteractionAnswer::Approval(match decision {
                ApprovalDecisionWire::AllowOnce => ApprovalDecision::AllowOnce,
                ApprovalDecisionWire::Deny => ApprovalDecision::Deny,
            })),
            Self::Text { text } => {
                if text.is_empty() || text.len() > 8_192 || text.chars().any(char::is_control) {
                    return Err(InvalidParams);
                }
                let text =
                    BoundedText::new_with_max_bytes(text, 8_192).map_err(|_| InvalidParams)?;
                Ok(InteractionAnswer::ToolInput(ToolInputAnswer::Text(text)))
            }
            Self::Choice { index } => Ok(InteractionAnswer::ToolInput(ToolInputAnswer::Choice {
                index,
            })),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct ProfilesResult {
    pub(crate) profiles: Vec<ProfileInfo>,
}

#[derive(Serialize)]
pub(crate) struct ModelsResult {
    pub(crate) models: Vec<ModelInfo>,
}

#[derive(Serialize)]
pub(crate) struct SessionsResult {
    pub(crate) sessions: Vec<SessionInfo>,
}

#[derive(Serialize)]
pub(crate) struct SessionResult {
    pub(crate) session: SessionInfo,
}

#[derive(Serialize)]
pub(crate) struct TurnResult {
    pub(crate) turn: TurnRef,
}

#[derive(Serialize)]
pub(crate) struct CancelledResult {
    pub(crate) cancelled: bool,
}

#[derive(Clone, Copy, Serialize)]
pub(crate) struct OkResult {
    pub(crate) ok: bool,
}

impl OkResult {
    pub(crate) const TRUE: Self = Self { ok: true };
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
    pub(crate) retryable: bool,
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
        retryable: bool,
    ) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result: None,
            error: Some(RpcError {
                code,
                message,
                data: RpcErrorData { kind, retryable },
            }),
        }
    }
}

pub(crate) enum RpcOutbound {
    Response(RpcResponse),
    Event(Box<AgentEventNotification>),
}

impl Serialize for RpcOutbound {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Response(response) => response.serialize(serializer),
            Self::Event(notification) => notification.serialize(serializer),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct AgentEventNotification {
    jsonrpc: &'static str,
    method: &'static str,
    params: AgentEvent,
}

impl AgentEventNotification {
    pub(crate) fn new(event: AgentEvent) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            method: "agent.event",
            params: event,
        }
    }
}
