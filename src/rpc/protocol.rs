use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

use minicore_runtime::interaction::InteractionAnswer;
use minicore_runtime::model::ReasoningPreference;
use minicore_runtime::tools::{ApprovalDecision, ToolInputAnswer};
use minicore_runtime::value::BoundedText;
use minicore_runtime::{InteractionId, LoopId};

use crate::agent::{
    CreateSession, RenameSession, SessionInfo, SteerMessage, TurnRef, UpdateSession,
};
use crate::event::AgentEvent;
use crate::history::GetHistory;
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
pub(crate) const SESSION_BLOCKED: i32 = -32_004;
pub(crate) const INVALID_STATE: i32 = -32_005;
pub(crate) const INTERACTION_NOT_FOUND: i32 = -32_006;
pub(crate) const TURN_NOT_FOUND: i32 = -32_007;
pub(crate) const PROFILE_NOT_FOUND: i32 = -32_008;
pub(crate) const MODEL_NOT_FOUND: i32 = -32_009;
pub(crate) const WORKSPACE_ERROR: i32 = -32_010;
pub(crate) const STORE_ERROR: i32 = -32_011;
pub(crate) const RUNTIME_ERROR: i32 = -32_013;
pub(crate) const INVALID_SESSION_SETTINGS: i32 = -32_014;
pub(crate) const HISTORY_TOO_LARGE: i32 = -32_015;
pub(crate) const STEER_QUEUE_FULL: i32 = -32_016;
pub(crate) const RELOAD_REQUIRES_RESTART: i32 = -32_017;
pub(crate) const RELOAD_UNAVAILABLE: i32 = -32_018;

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
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) reasoning: Option<ReasoningPreference>,
    #[serde(default)]
    pub(crate) title: Option<String>,
}

impl From<SessionCreateParams> for CreateSession {
    fn from(value: SessionCreateParams) -> Self {
        Self {
            workspace: value.workspace,
            profile: value.profile,
            model: value.model,
            reasoning: value.reasoning,
            title: value.title,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionParams {
    pub(crate) session_id: crate::ids::SessionId,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionHistoryParams {
    pub(crate) session_id: crate::ids::SessionId,
    #[serde(default)]
    pub(crate) offset: usize,
    #[serde(default = "default_history_limit")]
    pub(crate) limit: usize,
}

impl From<SessionHistoryParams> for GetHistory {
    fn from(value: SessionHistoryParams) -> Self {
        Self {
            session_id: value.session_id,
            offset: value.offset,
            limit: value.limit,
        }
    }
}

fn default_history_limit() -> usize {
    100
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionUpdateParams {
    pub(crate) session_id: crate::ids::SessionId,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) reasoning: Option<ReasoningPreference>,
}

impl SessionUpdateParams {
    pub(crate) fn has_field(&self) -> bool {
        self.model.is_some() || self.reasoning.is_some()
    }
}

impl From<SessionUpdateParams> for UpdateSession {
    fn from(value: SessionUpdateParams) -> Self {
        Self {
            session_id: value.session_id,
            model: value.model,
            reasoning: value.reasoning,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionRenameParams {
    pub(crate) session_id: crate::ids::SessionId,
    pub(crate) title: String,
}

impl From<SessionRenameParams> for RenameSession {
    fn from(value: SessionRenameParams) -> Self {
        Self {
            session_id: value.session_id,
            title: value.title,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnSendParams {
    pub(crate) session_id: crate::ids::SessionId,
    pub(crate) text: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnParams {
    pub(crate) session_id: crate::ids::SessionId,
    pub(crate) loop_id: LoopId,
}

impl From<TurnParams> for TurnRef {
    fn from(value: TurnParams) -> Self {
        Self {
            session_id: value.session_id,
            loop_id: value.loop_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnSteerParams {
    pub(crate) session_id: crate::ids::SessionId,
    pub(crate) loop_id: LoopId,
    pub(crate) text: String,
}

impl From<TurnSteerParams> for SteerMessage {
    fn from(value: TurnSteerParams) -> Self {
        Self {
            turn: TurnRef {
                session_id: value.session_id,
                loop_id: value.loop_id,
            },
            text: value.text,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InteractionAnswerParams {
    pub(crate) session_id: crate::ids::SessionId,
    pub(crate) loop_id: LoopId,
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
pub(crate) struct SessionUpdateResult {
    pub(crate) session: SessionInfo,
    pub(crate) active_revision: Option<minicore_runtime::execution::ConfigRevision>,
}

impl From<crate::agent::SessionUpdateResult> for SessionUpdateResult {
    fn from(value: crate::agent::SessionUpdateResult) -> Self {
        Self {
            session: value.session,
            active_revision: value.active_revision,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct TurnResult {
    pub(crate) turn: TurnRef,
    /// Agent acceptance time for this Prompt (optional; absent when the
    /// system clock was unavailable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) accepted_at: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct SteerResult {
    pub(crate) ok: bool,
    /// Agent acceptance time for this Steer (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) accepted_at: Option<String>,
    /// 1-based FIFO acceptance index within the loop; absent when the Agent
    /// predates steer receipts. Used by the TUI only as read-only progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) steer_index: Option<u64>,
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
