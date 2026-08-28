use std::fmt;
use std::io;

use serde::Serialize;
use thiserror::Error;

use crate::config::ConfigError;

use minicore_runtime::storage::SessionLogError;

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
    #[error("model provider is not implemented in this phase")]
    ModelNotImplemented,
    #[error("tools are not implemented in this phase")]
    ToolsNotImplemented,
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
