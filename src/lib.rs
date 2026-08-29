#![forbid(unsafe_code)]

mod agent;
mod config;
#[allow(
    dead_code,
    reason = "ProjectContext is assembled into Agent capabilities in a later phase"
)]
mod context;
mod error;
mod event;
mod models;
#[allow(
    dead_code,
    reason = "Policy is assembled into Agent capabilities in a later phase"
)]
mod policy;
mod profiles;
mod rpc;
mod sessions;
#[expect(dead_code, reason = "Store API is consumed by the Sessions phase")]
pub(crate) mod store;
#[allow(
    dead_code,
    reason = "read/write Tools are assembled into Agent capabilities in a later phase"
)]
mod tools;
#[allow(
    dead_code,
    reason = "Workspace path and I/O methods are consumed by the later Tools phase"
)]
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
pub use models::ModelConfig;
pub use rpc::run_stdio;
pub use workspace::{Workspace, WorkspaceError};
