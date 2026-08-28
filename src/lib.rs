#![forbid(unsafe_code)]

mod agent;
mod config;
mod error;
mod event;
mod rpc;
#[expect(dead_code, reason = "Store API is consumed by the Sessions phase")]
pub(crate) mod store;

pub use agent::{Agent, PingResponse, agent_version};
pub use config::{AgentConfig, ConfigError};
pub use error::AgentError;
pub use event::{AgentEvent, AgentEventStream};
pub use rpc::run_stdio;
