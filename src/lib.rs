#![forbid(unsafe_code)]

mod agent;
mod config;
mod error;
mod event;
mod history;
mod ids;
mod models;
mod policy;
mod presentation;
mod profiles;
mod prompt;
mod rpc;
mod sessions;
pub(crate) mod store;
mod tools;
mod workspace;

pub use agent::{
    Agent, AnswerInteraction, CreateSession, GetHistory, HistoryPage, PingResponse, RenameSession,
    SendMessage, SessionInfo, SessionState, SessionStatus, SessionUpdateResult, SteerMessage,
    TurnPersistence, TurnRef, TurnResult, UpdateSession, agent_version,
};
pub use config::{AgentConfig, ApprovalMode, ConfigError, LoopOverrides, Profile};
pub use error::{AgentError, RuntimeErrorView};
pub use event::{
    AgentEvent, AgentEventStream, EventMeta, OutputChannel, ToolProgressView, ToolResultView,
};
pub use ids::{SessionId, SessionIdError};
pub use models::{ModelConfig, ModelInfo};
pub use presentation::{AssistantDisplayPart, PresentationView, ToolDisplay};
pub use profiles::ProfileInfo;
pub use rpc::run_stdio;
pub use sessions::{LoopAccepted, SessionBlockReason, SteerAccepted};
pub use workspace::{Workspace, WorkspaceError};
