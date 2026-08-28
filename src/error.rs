use std::io;

use thiserror::Error;

use crate::config::ConfigError;

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
