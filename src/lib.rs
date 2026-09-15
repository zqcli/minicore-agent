#![forbid(unsafe_code)]

mod agent;
mod changes;
mod compaction;
mod config;
mod diff;
mod error;
mod event;
mod history;
mod ids;
mod models;
mod policy;
mod presentation;
mod profiles;
mod prompt;
mod read;
mod rpc;
mod sessions;
pub(crate) mod store;
mod subagents;
mod tool_data;
mod tools;
mod workspace;

// The loopback HTTP mock is shared by unit tests and by the process test
// crates under `tests/`; load it once here so within this crate it is a
// single module instead of a `#[path]` module in every test subtree.
#[cfg(test)]
#[path = "../tests/support/openai_mock.rs"]
pub(crate) mod openai_mock;

pub use agent::{
    Agent, AnswerInteraction, CompactSession, CreateSession, GetHistory, HistoryPage, PingResponse,
    RPC_CAPABILITIES, RPC_PROTOCOL_VERSION, ReadCursor, ReadItemChunk, ReadSession,
    ReadSessionResult, ReadTurnSummary, ReloadResult, RenameSession, SendMessage, SessionInfo,
    SessionState, SessionStatus, SessionUpdateResult, SteerMessage, TurnPersistence, TurnRef,
    TurnResult, TurnResultAvailability, TurnResultPage, TurnResultRequest, UpdateSession,
    agent_version,
};
pub use changes::{
    ChangeCommitState, ChangeCoverage, ChangeCursor, ChangeKind, ChangeListConsistency,
    ChangeListWarning, ChangeOrigin, ChangeRecord, ChangeRevision, ChangeScope, ChangesListRequest,
    ChangesListResult,
};
pub use compaction::{
    AutomaticCompactionObservation, AutomaticCompactionView, CompactionResult, CompactionStatus,
    CompactionUtilityUsage, RecoveryObservation,
};
pub use config::{
    AgentConfig, ApprovalMode, CompactionConfig, ConfigError, LoopOverrides, Profile,
};
pub use diff::{
    ChangesDiffRequest, DiffAvailability, DiffComparison, DiffCursor, DiffHunk, DiffLine,
    DiffLineKind, DiffResult,
};
pub use error::{AgentError, RuntimeErrorView};
pub use event::{
    AgentEvent, AgentEventStream, EventMeta, OutputChannel, ToolProgressView, ToolResultView,
};
pub use ids::{SessionId, SessionIdError};
pub use models::{ModelConfig, ModelInfo};
pub use presentation::{AssistantDisplayPart, PresentationView, ToolDisplay};
pub use profiles::ProfileInfo;
pub use rpc::run_stdio;
pub use sessions::{
    CompactionPhase, CompactionProgress, ContextBudget, LoopAccepted, SessionBlockReason,
    SessionContext, SteerAccepted, SummaryCoverage,
};
pub use tool_data::{
    CommandResult, CommandStatus, ToolDataAvailability, ToolDataStream, ToolExecutionData,
    ToolExecutionState, ToolInputSummary, ToolInvocationData, ToolOutputPage, ToolOutputRequest,
    ToolPhase, ToolProcessChunk, ToolProcessData, ToolReadRequest, ToolReadResult,
    ToolRecordingState, ToolRef, ToolSubject,
};
pub use workspace::listing::{
    WorkspaceFileEntry, WorkspaceFilesRequest, WorkspaceFilesResult, WorkspaceListCursor,
};
pub use workspace::query::{
    WorkspaceReadEncoding, WorkspaceReadRange, WorkspaceReadRequest, WorkspaceReadResult,
    WorkspaceReadStatus,
};
pub use workspace::scan::{WorkspaceFileKind, WorkspaceScanConsistency, WorkspaceScanStop};
pub use workspace::search::{
    WorkspaceByteRange, WorkspaceSearchCursor, WorkspaceSearchMatch, WorkspaceSearchRequest,
    WorkspaceSearchResult,
};
pub use workspace::status::{
    WorkspaceStatusEntry, WorkspaceStatusEntryKind, WorkspaceStatusRequest, WorkspaceStatusResult,
    WorkspaceStatusWarning,
};
pub use workspace::{Workspace, WorkspaceError};
