use std::fmt;
use std::io;

use serde::Serialize;
use thiserror::Error;

use crate::config::ConfigError;

/// Redacted view of a Runtime control/start failure. Only a stable kind string
/// and a retryability flag cross the process boundary; never diagnostic bodies.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RuntimeErrorView {
    pub kind: &'static str,
    pub retryable: bool,
}

impl RuntimeErrorView {
    pub const fn new(kind: &'static str, retryable: bool) -> Self {
        Self { kind, retryable }
    }
}

impl fmt::Display for RuntimeErrorView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind)
    }
}

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("invalid configuration: {0}")]
    Config(#[source] ConfigError),
    #[error("session not found")]
    SessionNotFound,
    #[error("session is not loaded")]
    SessionNotLoaded,
    #[error("session is already loaded")]
    SessionAlreadyLoaded,
    #[error("session is busy")]
    SessionBusy,
    #[error("session is blocked after a persistence failure")]
    SessionBlocked,
    #[error("turn not found")]
    TurnNotFound,
    #[error("interaction not found")]
    InteractionNotFound,
    #[error("interaction answer is invalid")]
    InvalidInteraction,
    #[error("user input is invalid")]
    InvalidInput,
    #[error("session history exceeds the loop limits")]
    HistoryTooLarge,
    #[error("profile not found")]
    ProfileNotFound,
    #[error("model not found")]
    ModelNotFound,
    #[error("session settings are incompatible")]
    InvalidSessionSettings,
    #[error("workspace is unavailable")]
    Workspace,
    #[error("store error")]
    Store,
    #[error("the operation is not valid in the current state")]
    InvalidState,
    #[error("configuration reload requires restart")]
    ReloadRequiresRestart,
    #[error("configuration reload is unavailable")]
    ReloadUnavailable,
    #[error("the steer queue is full")]
    SteerQueueFull,
    #[error("runtime error: {0}")]
    Runtime(RuntimeErrorView),
    #[error("internal error")]
    Internal,
    #[error("agent event stream was already taken")]
    EventStreamTaken,
    #[error("invalid command line arguments")]
    InvalidArguments,
    #[error("RPC response serialization failed")]
    RpcSerialization,
    #[error("history query exceeded its scan budget")]
    QueryLimit,
    #[error("tool reference is unknown or was evicted")]
    ToolNotFound,
    #[error("the request context cannot be reduced without dropping user constraints")]
    ContextUncompressible,
    #[error("I/O failure")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("store root is invalid")]
    InvalidRoot,
    #[error("session record is invalid")]
    InvalidRecord,
    #[error("session storage format is unsupported")]
    UnsupportedFormat,
    #[error("session not found")]
    SessionNotFound,
    #[error("session already exists")]
    SessionAlreadyExists,
    #[error("store data is corrupt")]
    Corrupt,
    #[error("store record exceeds the single-line limit")]
    RecordTooLarge,
    #[error("store is unavailable")]
    Unavailable,
    #[error("history changed while it was being read")]
    HistoryChanged,
    #[error("history query exceeded its scan budget")]
    QueryLimit,
    #[error("invalid history query arguments")]
    InvalidArguments,
}

impl StoreError {
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidRoot => "invalid_root",
            Self::InvalidRecord => "invalid_record",
            Self::UnsupportedFormat => "unsupported_format",
            Self::SessionNotFound => "session_not_found",
            Self::SessionAlreadyExists => "session_already_exists",
            Self::Corrupt => "corrupt",
            Self::RecordTooLarge => "record_too_large",
            Self::Unavailable => "unavailable",
            Self::HistoryChanged => "history_changed",
            Self::QueryLimit => "query_limit",
            Self::InvalidArguments => "invalid_arguments",
        }
    }
}
