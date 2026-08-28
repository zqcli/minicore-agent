use std::io;

use thiserror::Error;

use crate::config::ConfigError;

use minicore_runtime::storage::SessionLogError;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("invalid configuration")]
    Config(#[source] ConfigError),
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
    #[error("store operation failed internally")]
    Internal,
    #[error("session log operation failed")]
    Log(#[from] SessionLogError),
}
