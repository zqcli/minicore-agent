#![forbid(unsafe_code)]

mod agent;
mod config;
mod context;
mod error;
mod event;
mod models;
mod policy;
mod profiles;
mod rpc;
mod sessions;
#[expect(dead_code, reason = "Store API is consumed by the Sessions phase")]
pub(crate) mod store;
mod tools;
mod workspace;

pub use agent::{
    Agent, AnswerInteraction, CreateSession, GetTranscript, PingResponse, SendMessage, SessionInfo,
    TurnRef, agent_version,
};
pub use config::{
    AgentConfig, ApprovalMode, ConfigError, KernelOverrides, Profile, ProfileCompaction,
};
pub use error::{AgentError, CoreErrorView};
pub use event::{
    AgentEvent, AgentEventStream, EventMeta, OutputChannel, ToolProgressView, ToolResultView,
};
pub use models::{ModelConfig, ModelInfo};
pub use profiles::ProfileInfo;
pub use rpc::run_stdio;
pub use workspace::{Workspace, WorkspaceError};
