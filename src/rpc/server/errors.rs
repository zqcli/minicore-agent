use super::*;

pub(super) fn params_or_error<T>(id: &RpcId, params: Option<Value>) -> Result<T, RpcResponse>
where
    T: serde::de::DeserializeOwned,
{
    decode_params(params).map_err(|_| invalid_params(Some(id.clone())))
}

pub(super) fn success<T>(id: &RpcId, result: T) -> RpcResponse
where
    T: Serialize,
{
    match RpcResponse::success(id.clone(), result) {
        Ok(response) => response,
        Err(_) => RpcResponse::error(
            Some(id.clone()),
            INTERNAL_ERROR,
            "internal error",
            "internal_error",
            false,
        ),
    }
}

pub(super) fn agent_result<T>(id: &RpcId, result: Result<T, AgentError>) -> RpcResponse
where
    T: Serialize,
{
    match result {
        Ok(result) => success(id, result),
        Err(error) => agent_error(id.clone(), &error),
    }
}

pub(super) fn method_not_found(id: RpcId) -> RpcResponse {
    RpcResponse::error(
        Some(id),
        METHOD_NOT_FOUND,
        "method not found",
        "method_not_found",
        false,
    )
}

pub(super) fn invalid_params(id: Option<RpcId>) -> RpcResponse {
    tracing::debug!(kind = "invalid_params", "rpc request rejected");
    RpcResponse::error(
        id,
        INVALID_PARAMS,
        "invalid params",
        "invalid_params",
        false,
    )
}

pub(super) fn resource_exhausted(id: RpcId) -> RpcResponse {
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

pub(super) fn query_error(id: RpcId, error: &AgentError) -> RpcResponse {
    if matches!(error, AgentError::InvalidArguments) {
        invalid_params(Some(id))
    } else {
        agent_error(id, error)
    }
}

pub(super) fn agent_error(id: RpcId, error: &AgentError) -> RpcResponse {
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
        AgentError::InputQueueFull => (
            INPUT_QUEUE_FULL,
            "input queue is full",
            "input_queue_full",
            false,
        ),
        AgentError::InputNotQueued => (
            INPUT_NOT_QUEUED,
            "input is not queued",
            "input_not_queued",
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
            crate::rpc::protocol::CONFIG_ERROR,
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
