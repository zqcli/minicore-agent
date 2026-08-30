use std::fmt;
use std::io;

use serde::Serialize;
use thiserror::Error;

use crate::config::ConfigError;

use minicore_runtime::storage::{SessionLogError, SessionLogErrorKind};

pub(crate) const fn session_log_error_kind(kind: SessionLogErrorKind) -> &'static str {
    match kind {
        SessionLogErrorKind::NotInitialized => "session_log_not_initialized",
        SessionLogErrorKind::AlreadyInitialized => "session_log_already_initialized",
        SessionLogErrorKind::Conflict => "session_log_conflict",
        SessionLogErrorKind::Corrupt => "session_log_corrupt",
        SessionLogErrorKind::Unavailable => "session_log_unavailable",
        SessionLogErrorKind::UnknownOutcome => "session_log_unknown_outcome",
        SessionLogErrorKind::Closed => "session_log_closed",
        SessionLogErrorKind::Internal => "session_log_internal",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CoreErrorView {
    pub kind: &'static str,
    pub retryable: bool,
}

impl CoreErrorView {
    pub const fn new(kind: &'static str, retryable: bool) -> Self {
        Self { kind, retryable }
    }
}

impl fmt::Display for CoreErrorView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind)
    }
}

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("invalid configuration")]
    Config(#[source] ConfigError),
    #[error("session not found")]
    SessionNotFound,
    #[error("session is not loaded")]
    SessionNotLoaded,
    #[error("session is already loaded")]
    SessionAlreadyLoaded,
    #[error("session specification is incompatible with current configuration")]
    SessionSpecMismatch,
    #[error("session is busy")]
    SessionBusy,
    #[error("session is closed")]
    SessionClosed,
    #[error("session durability is degraded")]
    SessionDegraded,
    #[error("turn not found")]
    TurnNotFound,
    #[error("interaction not found")]
    InteractionNotFound,
    #[error("interaction answer is invalid")]
    InvalidInteraction,
    #[error("user input is invalid")]
    InvalidInput,
    #[error("profile not found")]
    ProfileNotFound,
    #[error("model not found")]
    ModelNotFound,
    #[error("session settings are incompatible")]
    InvalidSessionSettings,
    #[error("model-backed compaction is not implemented in this phase")]
    ModelNotImplemented,
    #[error("workspace is unavailable")]
    Workspace,
    #[error("store error")]
    Store,
    #[error("core error: {0}")]
    Core(CoreErrorView),
    #[error("internal error")]
    Internal,
    #[error("agent event stream was already taken")]
    EventStreamTaken,
    #[error("invalid command line arguments")]
    InvalidArguments,
    #[error("RPC response serialization failed")]
    RpcSerialization,
    #[error("I/O failure")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("store root is invalid")]
    InvalidRoot,
    #[error("session metadata is invalid")]
    InvalidRecord,
    #[error("session not found")]
    SessionNotFound,
    #[error("session already exists")]
    SessionAlreadyExists,
    #[error("store data is corrupt")]
    Corrupt,
    #[error("store is unavailable")]
    Unavailable,
    #[error("store mutation outcome is unknown")]
    UnknownOutcome,
    #[error("store cleanup failed after a known store error")]
    CleanupFailed {
        primary: Box<StoreError>,
        cleanup: Box<StoreError>,
    },
    #[error("store operation failed internally")]
    Internal,
    #[error("session log operation failed")]
    Log(#[from] SessionLogError),
}

impl StoreError {
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidRoot => "invalid_root",
            Self::InvalidRecord => "invalid_record",
            Self::SessionNotFound => "session_not_found",
            Self::SessionAlreadyExists => "session_already_exists",
            Self::Corrupt => "corrupt",
            Self::Unavailable => "unavailable",
            Self::UnknownOutcome => "unknown_outcome",
            Self::CleanupFailed { .. } => "cleanup_failed",
            Self::Internal => "internal",
            Self::Log(error) => session_log_error_kind(error.kind()),
        }
    }
}
