use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use minicore_runtime::InteractionId;
use minicore_runtime::LoopOptions;
use minicore_runtime::execution::{ConfigRevision, ExecutionConfig, UserInput};
use minicore_runtime::interaction::InteractionAnswer;
use minicore_runtime::model::ReasoningPreference;
use minicore_runtime::prompt::PromptProvider;
use minicore_runtime::tools::ToolPolicy;

use crate::Workspace;
use crate::changes::{ChangesListRequest, ChangesListResult, change_deadline};
use crate::compaction::{
    AutoContext, CompactionResult, CompactionState, load_state, valid_operation_id,
};
use crate::config::AgentConfig;
use crate::diff::{ChangesDiffRequest, DiffResult, diff_deadline};
use crate::error::AgentError;
use crate::event::{AgentEvent, AgentEventSink, AgentEventStream, EventMeta};
use crate::models::{ModelConfig, ModelConfigError, ModelInfo, Models};
use crate::policy::Policy;
use crate::profiles::{Profile, ProfileInfo, Profiles};
use crate::prompt::ProjectPromptProvider;
use crate::sessions::{Sessions, TurnCompletion, await_compaction_completion};
use crate::store::{SESSION_FORMAT_VERSION, SessionRecord, Store};
use crate::tool_data::ToolData;
use crate::tools::command::CommandOwners;
use crate::tools::observe::ToolObserver;
use crate::tools::{BuildToolsError, CommandEnvironment};

pub use crate::history::{GetHistory, HistoryPage};
pub use crate::read::{
    CoveredUsage, DisplayProjection, ReadCursor, ReadItemChunk, ReadSession, ReadSessionResult,
    ReadTurnSummary, ReadView, TurnResultAvailability, TurnResultPage, TurnResultRequest,
};
pub use crate::sessions::{
    SessionContext, SessionState, SessionStatus, TurnPersistence, TurnRef, TurnResult,
};

type AgentFuture<T> = futures_util::future::BoxFuture<'static, Result<T, AgentError>>;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RPC_PROTOCOL_VERSION: u32 = 1;
pub const RPC_CAPABILITIES: &[&str] = &[
    "session.read",
    "session.read.display",
    "session.context",
    "turn.result",
    "tool.read",
    "tool.read.display",
    "tool.output",
    "session.history",
    "workspace.read",
    "workspace.files",
    "workspace.search",
    "workspace.status",
    "changes.list",
    "changes.diff",
    "deferred.waiter_limit",
    "tool.arguments.preview",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PingResponse {
    pub version: &'static str,
    pub protocol_version: u32,
    pub capabilities: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ReloadResult {
    pub ok: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSession {
    pub workspace: PathBuf,

    #[serde(default)]
    pub profile: String,

    #[serde(default)]
    pub model: Option<String>,

    #[serde(default)]
    pub reasoning: Option<ReasoningPreference>,

    pub title: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessage {
    pub session_id: crate::ids::SessionId,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactSession {
    pub session_id: crate::ids::SessionId,
    pub operation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SteerMessage {
    pub turn: TurnRef,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnswerInteraction {
    pub turn: TurnRef,
    pub interaction_id: InteractionId,
    pub answer: InteractionAnswer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSession {
    pub session_id: crate::ids::SessionId,

    #[serde(default)]
    pub model: Option<String>,

    #[serde(default)]
    pub reasoning: Option<ReasoningPreference>,
}

impl UpdateSession {
    pub(crate) fn has_field(&self) -> bool {
        self.model.is_some() || self.reasoning.is_some()
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenameSession {
    pub session_id: crate::ids::SessionId,
    pub title: String,
}

impl fmt::Debug for RenameSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenameSession")
            .field("session_id", &self.session_id)
            .field("title", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SessionUpdateResult {
    pub session: SessionInfo,
    pub active_revision: Option<ConfigRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInfo {
    pub session_id: crate::ids::SessionId,
    pub title: Option<String>,
    pub profile: String,
    pub workspace: PathBuf,
    pub model: String,
    pub reasoning: ReasoningPreference,
    pub loaded: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl SessionInfo {
    pub(crate) fn from_record(record: &SessionRecord, loaded: bool) -> Self {
        Self {
            session_id: record.session_id,
            title: record.title.clone(),
            profile: record.profile.clone(),
            workspace: record.workspace.clone(),
            model: record.model.clone(),
            reasoning: record.reasoning,
            loaded,
            created_at: record.created_at.clone(),
            updated_at: record.updated_at.clone(),
        }
    }
}

pub(crate) struct PreparedSession {
    stored: crate::store::StoredSession,
    workspace: Arc<Workspace>,
    compaction: Arc<CompactionState>,
}

struct ResolvedSessionSettings {
    profile: String,
    model: String,
    reasoning: ReasoningPreference,
}

struct ExecutionConfigFactory<'a> {
    models: &'a Models,
    command_environment: &'a CommandEnvironment,
    policy: crate::compaction::CompactionPolicy,
}

impl ExecutionConfigFactory<'_> {
    fn setup(
        &self,
        record: &SessionRecord,
        workspace: Arc<Workspace>,
        options: LoopOptions,
        compaction: Arc<CompactionState>,
        events: &AgentEventSink,
    ) -> Result<SessionSetup, AgentError> {
        let presentation = crate::presentation::Presentation::new(
            record.session_id,
            events.clone(),
            Arc::clone(&compaction),
        );
        presentation.set_model_label(record.model.clone());
        let tool_data = Arc::new(ToolData::new());
        let command_owners = CommandOwners::new();
        let observer = ToolObserver::new(record.session_id, Arc::clone(&tool_data), events.clone());
        let parts = SessionParts {
            presentation,
            observer,
            command_owners,
            compaction,
        };
        let (config, auto) = self.build(record, workspace, &parts, &options)?;
        Ok(SessionSetup {
            parts,
            tool_data,
            config,
            auto,
            options,
        })
    }

    fn build(
        &self,
        record: &SessionRecord,
        workspace: Arc<Workspace>,
        parts: &SessionParts,
        options: &LoopOptions,
    ) -> Result<(ExecutionConfig, Option<AutoContext>), AgentError> {
        let model = self
            .models
            .get(&record.model)
            .map_err(map_model_config_error)?;
        let budget = self
            .models
            .get_budget(&record.model)
            .map_err(map_model_config_error)?;
        let utility_model = self
            .models
            .get_utility(&record.model)
            .map_err(map_model_config_error)?;
        let auto = self.policy.enabled.then(|| AutoContext {
            model: utility_model,
            budget: Arc::clone(&budget),
            policy: self.policy,
            max_prompt_messages: options.limits.max_prompt_messages,
            state: Arc::clone(&parts.compaction),
        });
        // The binding is published only when the Session actually installs
        // this config (`Session::new`/`update`/`replace_future_config`).
        // Publishing here would bind the shared state to a config that may
        // still fail later in this build or be rejected before commit.
        //
        // With automatic compaction disabled no recovery ticket can ever
        // exist, so keep the raw model (and its established cancellation
        // behavior) instead of paying the wrapper's per-request content and
        // ticket hashing for nothing.
        let model: Arc<dyn minicore_runtime::model::Model> = if auto.is_some() {
            crate::compaction::CompactingModel::new(model, budget, Arc::clone(&parts.compaction))
        } else {
            model
        };
        let model = crate::presentation::PresentationModel::new_with_observer(
            model,
            self.models
                .physical_context_window(&record.model)
                .expect("validated model"),
            Arc::clone(&parts.presentation),
            Arc::clone(&parts.observer),
        );
        let tools = crate::tools::build_tools_with_presentation(
            &record.tools,
            Arc::clone(&workspace),
            self.command_environment.clone(),
            &parts.presentation,
            &parts.observer,
            &parts.command_owners,
        )
        .map_err(map_build_tools_error)?;
        let policy: Option<Arc<dyn ToolPolicy>> = if record.tools.is_empty() {
            None
        } else {
            Some(crate::presentation::PresentationPolicy::new_with_observer(
                Arc::new(Policy::new(record.approval)),
                Arc::clone(&parts.observer),
            ))
        };
        let prompt: Arc<dyn PromptProvider> = crate::presentation::SteerReceiptPrompt::new(
            Arc::new(
                ProjectPromptProvider::with_auto(
                    workspace,
                    record.system_prompt.clone(),
                    Arc::clone(&parts.compaction),
                    auto.clone(),
                )
                .map_err(|_| AgentError::InvalidSessionSettings)?,
            ),
            Arc::clone(&parts.presentation),
        );
        let config = ExecutionConfig::new(model, record.reasoning, tools, policy, prompt)
            .map_err(|_| AgentError::InvalidSessionSettings)?;
        Ok((config, auto))
    }
}

pub(crate) struct SessionParts {
    pub(crate) presentation: Arc<crate::presentation::Presentation>,
    pub(crate) command_owners: Arc<CommandOwners>,
    pub(crate) observer: Arc<ToolObserver>,
    pub(crate) compaction: Arc<CompactionState>,
}

pub(crate) struct SessionSetup {
    pub(crate) parts: SessionParts,
    pub(crate) tool_data: Arc<ToolData>,
    pub(crate) config: ExecutionConfig,
    pub(crate) auto: Option<AutoContext>,
    pub(crate) options: LoopOptions,
}

/// Top-level coordinator for the local Store and loaded Sessions.
///
/// `Agent::shutdown` is the cleanup barrier for embedded Rust callers: it
/// cancels active loops, waits for Agent-owned loop and worker tasks, and
/// awaits persistence wrap-up. Dropping an `Agent` with live turns does not
/// synchronously wait for Agent-owned loop tasks.
pub struct Agent {
    config: AgentConfig,
    config_path: Option<PathBuf>,
    store: Store,
    profiles: Profiles,
    models: Models,
    command_environment: CommandEnvironment,
    sessions: Sessions,
    event_sink: AgentEventSink,
    events_rx: Option<mpsc::Receiver<AgentEvent>>,
}

impl Agent {
    fn execution_factory(&self) -> ExecutionConfigFactory<'_> {
        ExecutionConfigFactory {
            models: &self.models,
            command_environment: &self.command_environment,
            policy: self.config.compaction.policy(),
        }
    }

    fn session(&self, id: crate::ids::SessionId) -> Result<&crate::sessions::Session, AgentError> {
        self.sessions.get(id).ok_or(AgentError::SessionNotLoaded)
    }

    pub async fn open(config: AgentConfig) -> Result<Self, AgentError> {
        config.validate().map_err(AgentError::Config)?;
        let models = Models::from_config(&config.models)
            .await
            .map_err(map_model_config_error)?;
        Self::open_parts(config, models, None).await
    }

    /// Opens an Agent from a configuration file and retains its absolute
    /// lexical path as the only source used by `reload`.
    pub async fn open_file(path: impl AsRef<std::path::Path>) -> Result<Self, AgentError> {
        let path = AgentConfig::absolute_lexical_path(path.as_ref()).map_err(AgentError::Config)?;
        let config = AgentConfig::load(&path).map_err(AgentError::Config)?;
        let models = Models::from_config(&config.models)
            .await
            .map_err(map_model_config_error)?;
        Self::open_parts(config, models, Some(path)).await
    }

    async fn open_parts(
        config: AgentConfig,
        models: Models,
        config_path: Option<PathBuf>,
    ) -> Result<Self, AgentError> {
        let command_environment = command_environment(&config);
        let profiles = config.profiles();
        let store = Store::open(config.data_dir.clone()).await?;
        let (events_tx, events_rx) = mpsc::channel(config.event_capacity);
        let event_sink = AgentEventSink::new(events_tx);
        let agent = Self {
            config,
            config_path,
            store,
            profiles,
            models,
            command_environment,
            sessions: Sessions::new(),
            event_sink,
            events_rx: Some(events_rx),
        };
        tracing::info!("agent open success");
        Ok(agent)
    }

    pub fn config(&self) -> &AgentConfig {
        &self.config
    }

    /// Reloads the retained startup file into future-turn state. The
    /// fallible candidate build completes before any in-memory swap.
    pub async fn reload(&mut self) -> Result<ReloadResult, AgentError> {
        let (config, models) = self.prepare_reload()?.await?;
        self.apply_reload_settings(config, models)
    }

    pub(crate) fn prepare_reload(&self) -> Result<AgentFuture<(AgentConfig, Models)>, AgentError> {
        let path = self
            .config_path
            .clone()
            .ok_or(AgentError::ReloadUnavailable)?;
        let data_dir = self.config.data_dir.clone();
        let event_capacity = self.config.event_capacity;
        Ok(Box::pin(async move {
            let config = tokio::task::spawn_blocking(move || AgentConfig::load(&path))
                .await
                .map_err(|_| AgentError::Internal)?
                .map_err(AgentError::Config)?;
            if config.data_dir != data_dir || config.event_capacity != event_capacity {
                return Err(AgentError::ReloadRequiresRestart);
            }
            let models = Models::from_config(&config.models)
                .await
                .map_err(map_model_config_error)?;
            Ok((config, models))
        }))
    }

    pub(crate) fn apply_reload_settings(
        &mut self,
        candidate: AgentConfig,
        models: Models,
    ) -> Result<ReloadResult, AgentError> {
        let profiles = candidate.profiles();
        let command_environment = reload_command_environment(&self.command_environment, &candidate);
        let mut session_candidates = Vec::with_capacity(self.sessions.iter().count());
        for session in self.sessions.iter() {
            let record = session.record();
            let workspace = session.workspace();
            let options = candidate
                .loop_options_for_model(&record.model)
                .map_err(AgentError::Config)?;
            let factory = ExecutionConfigFactory {
                models: &models,
                command_environment: &command_environment,
                policy: candidate.compaction.policy(),
            };
            let (config, auto) = factory.build(&record, workspace, &session.parts(), &options)?;
            session_candidates.push((
                session.clone(),
                config,
                auto,
                candidate.compaction.policy(),
                options,
            ));
        }

        self.config = candidate;
        self.profiles = profiles;
        self.models = models;
        self.command_environment = command_environment;
        for (session, config, auto, policy, options) in session_candidates {
            session.replace_future_config(config, auto, policy, options);
        }
        tracing::info!("agent configuration reloaded");
        Ok(ReloadResult { ok: true })
    }

    pub fn take_events(&mut self) -> Result<AgentEventStream, AgentError> {
        self.events_rx
            .take()
            .map(AgentEventStream::new)
            .ok_or(AgentError::EventStreamTaken)
    }

    pub const fn ping(&self) -> PingResponse {
        PingResponse {
            version: VERSION,
            protocol_version: RPC_PROTOCOL_VERSION,
            capabilities: RPC_CAPABILITIES,
        }
    }

    pub(crate) fn loaded_session(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Option<crate::sessions::Session> {
        self.sessions.get(session_id).cloned()
    }

    pub(crate) fn store_handle(&self) -> Store {
        self.store.clone()
    }

    pub async fn read_session(
        &self,
        request: ReadSession,
    ) -> Result<ReadSessionResult, AgentError> {
        request.validate()?;
        crate::queries::prepare_session_read(
            self.store.clone(),
            self.loaded_session(request.session_id),
            request,
            CancellationToken::new(),
        )
        .await
    }

    pub async fn turn_result(
        &self,
        request: TurnResultRequest,
    ) -> Result<TurnResultPage, AgentError> {
        request.validate()?;
        crate::queries::prepare_turn_result(
            self.store.clone(),
            self.loaded_session(request.turn.session_id),
            request,
            CancellationToken::new(),
        )
        .await
    }

    /// Reads one bounded page of a workspace file. Only a loaded Session
    /// locates its Workspace, and closing that Session cancels the read. The
    /// query never appends history or calls a model.
    pub async fn workspace_read(
        &self,
        request: crate::WorkspaceReadRequest,
    ) -> Result<crate::WorkspaceReadResult, AgentError> {
        request.validate()?;
        let session = self.session(request.session_id)?.clone();
        crate::queries::prepare_workspace_read(session, request, CancellationToken::new()).await
    }

    /// Lists one bounded page of Workspace entries. Only a loaded Session
    /// locates its Workspace, and closing that Session cancels the scan. The
    /// scan owns one retained blocking worker and enforces its own deadline,
    /// so this call is never wrapped in an outer timeout that could drop the
    /// worker join.
    pub async fn workspace_files(
        &self,
        request: crate::WorkspaceFilesRequest,
    ) -> Result<crate::WorkspaceFilesResult, AgentError> {
        request.validate()?;
        let session = self.session(request.session_id)?.clone();
        crate::queries::prepare_workspace_files(session, request, CancellationToken::new()).await
    }

    /// Searches one bounded page of literal matches in Workspace files. Only a
    /// loaded Session locates its Workspace, and closing that Session cancels
    /// the scan.
    pub async fn workspace_search(
        &self,
        request: crate::WorkspaceSearchRequest,
    ) -> Result<crate::WorkspaceSearchResult, AgentError> {
        request.validate()?;
        let session = self.session(request.session_id)?.clone();
        crate::queries::prepare_workspace_search(session, request, CancellationToken::new()).await
    }

    /// Observes the Workspace's Git status. Only a loaded Session locates its
    /// Workspace, and closing that Session cancels the query. The query owns
    /// its git child and enforces its own deadline, so this call is never
    /// wrapped in an outer timeout that could drop that ownership.
    pub async fn workspace_status(
        &self,
        request: crate::WorkspaceStatusRequest,
    ) -> Result<crate::WorkspaceStatusResult, AgentError> {
        request.validate()?;
        let session = self.session(request.session_id)?.clone();
        // The Session owns the worker that runs the query, so closing the
        // Session stops and reaps this query's child even if this caller goes
        // away first. Registration happens synchronously inside `prepare`.
        let cancellation = CancellationToken::new();
        crate::queries::prepare_workspace_status(session, request, cancellation)?.await
    }

    pub async fn changes_list(
        &self,
        request: ChangesListRequest,
    ) -> Result<ChangesListResult, AgentError> {
        self.changes_list_with_cancellation(request, CancellationToken::new())
            .await
    }

    pub(crate) async fn changes_list_with_cancellation(
        &self,
        request: ChangesListRequest,
        cancellation: CancellationToken,
    ) -> Result<ChangesListResult, AgentError> {
        request.validate()?;
        let deadline = change_deadline();
        let loaded = self.loaded_session(request.session_id);
        crate::queries::prepare_changes_list(
            self.store.clone(),
            loaded,
            request,
            cancellation,
            deadline,
        )?
        .await
    }

    /// Returns one bounded review diff for a retained native Tool change. A
    /// loaded Session lets an in-memory change answer without disk I/O; an
    /// unloaded Session, a removed Workspace, or a missing model never blocks
    /// the cold path. The CPU comparison is owned by the Store worker set.
    pub async fn changes_diff(
        &self,
        request: ChangesDiffRequest,
    ) -> Result<DiffResult, AgentError> {
        self.changes_diff_with_cancellation(request, CancellationToken::new())
            .await
    }

    pub(crate) async fn changes_diff_with_cancellation(
        &self,
        request: ChangesDiffRequest,
        cancellation: CancellationToken,
    ) -> Result<DiffResult, AgentError> {
        request.validate()?;
        let deadline = diff_deadline();
        let loaded = self.loaded_session(request.session_id);
        crate::queries::prepare_changes_diff(
            self.store.clone(),
            loaded,
            request,
            cancellation,
            deadline,
        )?
        .await
    }

    /// Read-only structured facts for one tool call. Loaded in-memory facts
    /// take precedence; unloaded sessions or evicted records fall back to durable
    /// auxiliary storage.
    pub async fn tool_read(
        &self,
        request: crate::tool_data::ToolReadRequest,
    ) -> Result<crate::tool_data::ToolReadResult, AgentError> {
        let tool_data = self.tool_data(request.tool_ref.session_id).ok();
        crate::queries::prepare_tool_read(
            self.store.clone(),
            tool_data,
            request,
            CancellationToken::new(),
        )
        .await
    }

    /// Bounded raw input or result bytes for one tool call, addressed by
    /// offset. Loaded in-memory facts take precedence; unloaded sessions or
    /// evicted records fall back to durable auxiliary storage.
    pub async fn tool_output(
        &self,
        request: crate::tool_data::ToolOutputRequest,
    ) -> Result<crate::tool_data::ToolOutputPage, AgentError> {
        let tool_data = self.tool_data(request.tool_ref.session_id).ok();
        crate::queries::prepare_tool_output(
            self.store.clone(),
            tool_data,
            request,
            CancellationToken::new(),
        )
        .await
    }

    pub(crate) fn tool_data(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<Arc<crate::tool_data::ToolData>, AgentError> {
        self.sessions
            .get(session_id)
            .map(|session| session.tool_data())
            .ok_or(AgentError::SessionNotLoaded)
    }

    pub fn list_profiles(&self) -> Vec<ProfileInfo> {
        self.profiles.list()
    }

    pub fn list_models(&self) -> Vec<ModelInfo> {
        self.models.list()
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>, AgentError> {
        let records = self.store.list_sessions().await?;
        Ok(self.session_infos(records))
    }

    pub(crate) fn session_infos(&self, records: Vec<SessionRecord>) -> Vec<SessionInfo> {
        let mut sessions = Vec::with_capacity(records.len());
        for record in records {
            let session_id = record.session_id;
            if let Some(session) = self.sessions.get(session_id) {
                sessions.push(session.info(true));
            } else {
                sessions.push(SessionInfo::from_record(&record, false));
            }
        }
        sessions.sort_by_key(|info| info.session_id);
        sessions
    }

    pub async fn create_session(
        &mut self,
        request: CreateSession,
    ) -> Result<SessionInfo, AgentError> {
        let session_id = crate::ids::SessionId::new().map_err(|_| AgentError::Internal)?;
        let (stored, workspace, setup) = self.prepare_create(request, session_id)?.await?;
        Ok(self.install_session(stored, workspace, setup))
    }

    pub(crate) fn prepare_create(
        &self,
        request: CreateSession,
        session_id: crate::ids::SessionId,
    ) -> Result<AgentFuture<(crate::store::StoredSession, Arc<Workspace>, SessionSetup)>, AgentError>
    {
        let settings = self.resolve_session_settings(&request)?;
        let profile = self
            .profiles
            .get(&settings.profile)
            .cloned()
            .ok_or(AgentError::ProfileNotFound)?;
        let options = self
            .config
            .loop_options_for_model(&settings.model)
            .map_err(AgentError::Config)?;
        let models = self.models.clone();
        let command_environment = self.command_environment.clone();
        let policy = self.config.compaction.policy();
        let store = self.store.clone();
        let events = self.event_sink.clone();
        Ok(Box::pin(async move {
            let workspace = Arc::new(
                Workspace::open(request.workspace)
                    .await
                    .map_err(|_| AgentError::Workspace)?,
            );
            let record = build_record(
                session_id,
                &settings,
                &profile,
                request.title,
                workspace.root().to_path_buf(),
            )?;
            let setup = ExecutionConfigFactory {
                models: &models,
                command_environment: &command_environment,
                policy,
            }
            .setup(
                &record,
                Arc::clone(&workspace),
                options,
                CompactionState::new(),
                &events,
            )?;
            store.create_session(&record).await?;
            Ok((
                crate::store::StoredSession {
                    record,
                    history: Vec::new().into(),
                    user_times: Default::default(),
                },
                workspace,
                setup,
            ))
        }))
    }

    pub async fn open_session(
        &mut self,
        session_id: crate::ids::SessionId,
    ) -> Result<SessionInfo, AgentError> {
        if let Some(session) = self.sessions.get(session_id) {
            return Ok(session.info(true));
        }
        let prepared = Self::prepare_open(self.store.clone(), session_id).await?;
        self.install_open(prepared)
    }

    pub(crate) async fn prepare_open(
        store: Store,
        session_id: crate::ids::SessionId,
    ) -> Result<PreparedSession, AgentError> {
        // load_session repairs incomplete history tails. Reject removed tool
        // settings before that write: unsupported sessions remain read-only.
        let record = store.load_record(session_id).await?;
        if contains_removed_tool(&record.tools) {
            return Err(AgentError::InvalidSessionSettings);
        }
        let stored = store.load_session(session_id).await?;
        if contains_removed_tool(&stored.record.tools) {
            return Err(AgentError::InvalidSessionSettings);
        }
        let workspace = Arc::new(
            Workspace::open(stored.record.workspace.clone())
                .await
                .map_err(|_| AgentError::Workspace)?,
        );
        // The record stores the canonical workspace captured at creation.
        // If the directory was moved or its path now redirects elsewhere (for
        // example through a symlink), refuse to run against the wrong root
        // instead of silently continuing with different project context.
        if workspace.root() != stored.record.workspace.as_path() {
            tracing::warn!(
                session_id = %session_id,
                stored_workspace = %stored.record.workspace.display(),
                actual_workspace = %workspace.root().display(),
                "session workspace identity changed on reopen"
            );
            return Err(AgentError::Workspace);
        }
        let compaction = load_state(&store, session_id, &stored.history).await;
        Ok(PreparedSession {
            stored,
            workspace,
            compaction,
        })
    }

    pub(crate) fn install_open(
        &mut self,
        prepared: PreparedSession,
    ) -> Result<SessionInfo, AgentError> {
        let PreparedSession {
            stored,
            workspace,
            compaction,
        } = prepared;
        let session_id = stored.record.session_id;
        let options = self
            .config
            .loop_options_for_model(&stored.record.model)
            .map_err(AgentError::Config)?;
        let setup = self.execution_factory().setup(
            &stored.record,
            Arc::clone(&workspace),
            options,
            compaction,
            &self.event_sink,
        )?;
        let info = self.install_session(stored, workspace, setup);
        tracing::info!(session_id = %session_id, "session opened");
        Ok(info)
    }

    /// Closes and unloads a loaded Session by ID.
    ///
    /// If the Session has active work, its Agent-owned loop or manual-compaction task
    /// is cancelled and joined before unloading. MiniCore Agent v0.3 uses the Runtime
    /// user-cancellation path when closing or shutting down an active Session;
    /// it does not currently preserve a distinct shutdown cancellation reason.
    pub async fn close_session(
        &mut self,
        session_id: crate::ids::SessionId,
    ) -> Result<(), AgentError> {
        let session = self.session(session_id)?.clone();
        let result = session.shutdown().await;
        self.finish_close(session_id);
        result
    }

    pub(crate) fn finish_close(&mut self, session_id: crate::ids::SessionId) {
        // The map retains the Session until its complete shutdown barrier has
        // joined every owned task. This prevents a dropped local owner from
        // detaching a manual-compaction worker during close.
        self.sessions.remove(session_id);
        self.event_sink.try_send(AgentEvent::SessionClosed {
            session_id,
            meta: EventMeta::session(session_id),
        });
        tracing::info!(session_id = %session_id, "session closed");
    }

    pub async fn delete_session(
        &mut self,
        session_id: crate::ids::SessionId,
    ) -> Result<(), AgentError> {
        if self.sessions.contains(session_id) {
            return Err(AgentError::SessionAlreadyLoaded);
        }
        let result = self
            .store
            .delete_session(session_id)
            .await
            .map_err(AgentError::from);
        if result.is_ok() {
            tracing::info!(session_id = %session_id, "session deleted");
        }
        result
    }

    pub fn session_state(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<SessionState, AgentError> {
        self.sessions
            .get(session_id)
            .map(|session| session.state())
            .ok_or(AgentError::SessionNotLoaded)
    }

    pub fn session_context(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<SessionContext, AgentError> {
        self.sessions
            .get(session_id)
            .map(|session| session.context())
            .ok_or(AgentError::SessionNotLoaded)
    }

    pub async fn update_session(
        &mut self,
        request: UpdateSession,
    ) -> Result<SessionUpdateResult, AgentError> {
        self.prepare_update(request)?.await
    }

    pub(crate) fn prepare_update(
        &self,
        request: UpdateSession,
    ) -> Result<AgentFuture<SessionUpdateResult>, AgentError> {
        if !request.has_field() {
            return Err(AgentError::InvalidArguments);
        }
        let session_id = request.session_id;
        let session = self.session(session_id)?.clone();
        let workspace = session.workspace();
        let mut candidate = session.record();
        if let Some(model) = &request.model {
            candidate.model = model.clone();
        }
        if let Some(reasoning) = request.reasoning {
            candidate.reasoning = reasoning;
        }
        let options = self
            .config
            .loop_options_for_model(&candidate.model)
            .map_err(AgentError::Config)?;
        let (config, auto) = ExecutionConfigFactory {
            models: &self.models,
            command_environment: &self.command_environment,
            policy: session.policy(),
        }
        .build(&candidate, workspace, &session.parts(), &options)?;
        Ok(Box::pin(async move {
            let active_revision = session.update(candidate, config, auto, options).await?;
            Ok(SessionUpdateResult {
                session: session.info(true),
                active_revision,
            })
        }))
    }

    /// Returns only after the updated SessionInfo is persisted successfully.
    /// If cancellation or a transport disconnect prevents the response from
    /// reaching the caller, the persistence outcome is unknown; reread the
    /// Session before deciding whether a retry is safe.
    pub async fn rename_session(
        &mut self,
        request: RenameSession,
    ) -> Result<SessionInfo, AgentError> {
        self.prepare_rename(request)?.await
    }

    pub(crate) fn prepare_rename(
        &self,
        request: RenameSession,
    ) -> Result<AgentFuture<SessionInfo>, AgentError> {
        let title =
            crate::store::normalize_title(&request.title).map_err(|_| AgentError::InvalidInput)?;
        let session = self.loaded_session(request.session_id);
        let store = self.store.clone();
        Ok(Box::pin(async move {
            if let Some(session) = session {
                session.rename(title).await?;
                return Ok(session.info(true));
            }
            let mut record = store.load_record(request.session_id).await?;
            record.title = title;
            record.updated_at = crate::store::utc_timestamp().map_err(|_| AgentError::Store)?;
            store.write_record(&record).await?;
            Ok(SessionInfo::from_record(&record, false))
        }))
    }

    /// Starts an ordinary user turn without pre-turn summarization.
    pub async fn send(&mut self, request: SendMessage) -> Result<TurnRef, AgentError> {
        let input = UserInput::text(request.text).map_err(|_| AgentError::InvalidInput)?;
        match self.prepare_submit(request.session_id, input)?.await? {
            crate::sessions::LoopSubmission::Accepted(accepted) => Ok(accepted.turn),
        }
    }

    pub(crate) fn prepare_submit(
        &self,
        session_id: crate::ids::SessionId,
        input: UserInput,
    ) -> Result<AgentFuture<crate::sessions::LoopSubmission>, AgentError> {
        let session = self.session(session_id)?.clone();
        Ok(Box::pin(async move { session.submit(input).await }))
    }

    pub(crate) async fn compact_session(
        &mut self,
        request: CompactSession,
    ) -> Result<watch::Receiver<Option<CompactionResult>>, AgentError> {
        self.prepare_compact(request)?.await
    }

    pub(crate) fn prepare_compact(
        &self,
        request: CompactSession,
    ) -> Result<AgentFuture<watch::Receiver<Option<CompactionResult>>>, AgentError> {
        if !valid_operation_id(&request.operation_id) || request.operation_id.starts_with("auto-") {
            return Err(AgentError::InvalidInput);
        }
        let session = self.session(request.session_id)?.clone();
        let record = session.record();
        let model = self
            .models
            .get_utility(&record.model)
            .map_err(map_model_config_error)?;
        let descriptor = model.descriptor().clone();
        let admission = session.admit_compaction(request.operation_id)?;
        Ok(Box::pin(async move {
            session.start_compaction(admission, model, descriptor).await
        }))
    }

    pub fn cancel_compaction(&self, request: CompactSession) -> Result<bool, AgentError> {
        if !valid_operation_id(&request.operation_id) {
            return Err(AgentError::InvalidInput);
        }
        let session = self.session(request.session_id)?;
        session.cancel_compaction(&request.operation_id)
    }

    /// Runs one manual compaction and resolves after its Session-owned worker
    /// has published the final result. The RPC server uses the crate-private
    /// start method above so the request itself remains deferred.
    pub async fn compact(
        &mut self,
        request: CompactSession,
    ) -> Result<CompactionResult, AgentError> {
        let receiver = self.compact_session(request).await?;
        await_compaction_completion(receiver).await
    }

    /// Steers the active turn and preserves the original public return type.
    /// RPC uses `steer_accepted` when it also needs the optional timestamp.
    pub fn steer(&self, request: SteerMessage) -> Result<(), AgentError> {
        self.steer_accepted(request).map(|_| ())
    }

    pub(crate) fn steer_accepted(
        &self,
        request: SteerMessage,
    ) -> Result<crate::sessions::SteerAccepted, AgentError> {
        let session = self.session(request.turn.session_id)?;
        session.steer(request.turn, request.text)
    }

    /// Read-only footer/detail data for one loaded session (never drives
    /// execution).
    pub fn session_presentation(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<crate::presentation::PresentationView, AgentError> {
        let session = self.session(session_id)?;
        Ok(session.presentation_view())
    }

    pub fn cancel(&self, turn: TurnRef) -> Result<bool, AgentError> {
        let session = self.session(turn.session_id)?;
        session.cancel(turn)
    }

    pub async fn wait_turn(
        &self,
        turn: TurnRef,
    ) -> Result<Arc<crate::sessions::TurnResult>, AgentError> {
        let session = self.session(turn.session_id)?;
        session.wait(turn).await
    }

    pub(crate) fn wait_turn_receiver(
        &self,
        turn: TurnRef,
    ) -> Result<watch::Receiver<Option<TurnCompletion>>, AgentError> {
        let session = self.session(turn.session_id)?;
        session.wait_receiver(turn)
    }

    pub async fn answer(&self, request: AnswerInteraction) -> Result<(), AgentError> {
        self.answer_interaction(request)
    }

    pub(crate) fn answer_interaction(&self, request: AnswerInteraction) -> Result<(), AgentError> {
        let session = self.session(request.turn.session_id)?;
        session.answer(request.turn, request.interaction_id, request.answer)
    }

    pub fn history(&self, request: GetHistory) -> Result<HistoryPage, AgentError> {
        let session = self.session(request.session_id)?;
        session.history(&request)
    }

    /// Orderly shutdown barrier for embedded Rust callers.
    ///
    /// Cancels active loops and manual compaction across all loaded Sessions, waits
    /// for Agent-owned loop/compaction and worker tasks plus persistence
    /// completion, and drops event channels.
    /// Dropping an `Agent` with live turns does not synchronously wait for
    /// Agent-owned loop tasks.
    ///
    /// MiniCore Agent v0.3 uses the Runtime user-cancellation path when closing
    /// or shutting down an active Session; it does not currently preserve a distinct
    /// shutdown cancellation reason.
    pub(crate) fn begin_shutdown(&self) {
        self.store.cancel_diff_workers();
        for session in self.sessions.iter() {
            session.begin_shutdown();
        }
    }

    pub async fn shutdown(mut self) -> Result<(), AgentError> {
        tracing::info!("agent shutdown begin");
        self.store.cancel_diff_workers();
        let result = self.sessions.shutdown_all().await;
        self.store.shutdown_diff_workers().await;
        drop(self.event_sink);
        drop(self.events_rx.take());
        tracing::info!(success = result.is_ok(), "agent shutdown end");
        result
    }

    fn resolve_session_settings(
        &self,
        request: &CreateSession,
    ) -> Result<ResolvedSessionSettings, AgentError> {
        let profile = if request.profile.is_empty() {
            self.config.default_profile.clone()
        } else {
            request.profile.clone()
        };
        let template = self
            .profiles
            .get(&profile)
            .ok_or(AgentError::ProfileNotFound)?;
        let model = request
            .model
            .clone()
            .unwrap_or_else(|| template.model.clone());
        let reasoning = request.reasoning.unwrap_or(template.reasoning);
        let configured = self.models.get(&model).map_err(map_model_config_error)?;
        let descriptor = configured.descriptor();
        if !descriptor.supports_reasoning(reasoning)
            || (!template.tools.is_empty() && !descriptor.supports_tools)
        {
            return Err(AgentError::InvalidSessionSettings);
        }
        Ok(ResolvedSessionSettings {
            profile,
            model,
            reasoning,
        })
    }

    pub(crate) fn install_session(
        &mut self,
        stored: crate::store::StoredSession,
        workspace: Arc<Workspace>,
        setup: SessionSetup,
    ) -> SessionInfo {
        let record = &stored.record;
        let session_id = record.session_id;
        let presentation = Arc::clone(&setup.parts.presentation);
        let compaction = Arc::clone(&setup.parts.compaction);
        let context_history = Arc::clone(&stored.history);
        let model_ref = setup.config.descriptor().model_ref.clone();
        let physical_window = self
            .models
            .physical_context_window(&record.model)
            .expect("validated model");
        let session = crate::sessions::Session::new(
            stored,
            workspace,
            setup,
            self.config.compaction.policy(),
            self.store.clone(),
            self.event_sink.clone(),
        );
        let active_history = compaction.project(&context_history);
        presentation.restore_context(
            active_history
                .as_ref()
                .map_or(context_history.as_ref(), |projection| projection.suffix),
            model_ref.as_str(),
            physical_window,
        );
        let info = session.info(true);
        self.sessions.insert(session_id, session);
        self.event_sink.try_send(AgentEvent::SessionOpened {
            session: info.clone(),
            meta: EventMeta::session(session_id),
        });
        info
    }
}

fn build_record(
    session_id: crate::ids::SessionId,
    settings: &ResolvedSessionSettings,
    profile: &Profile,
    title: Option<String>,
    workspace: PathBuf,
) -> Result<SessionRecord, AgentError> {
    let now = crate::store::utc_timestamp().map_err(|_| AgentError::Store)?;
    Ok(SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title,
        profile: settings.profile.clone(),
        workspace,
        model: settings.model.clone(),
        reasoning: settings.reasoning,
        system_prompt: profile.system_prompt.clone(),
        tools: profile.tools.clone(),
        max_tool_rounds: profile.max_tool_rounds,
        approval: profile.approval,
        created_at: now.clone(),
        updated_at: now,
    })
}

const REMOVED_TOOL_NAME: &str = "subagent";

fn contains_removed_tool(tools: &[String]) -> bool {
    tools.iter().any(|name| name == REMOVED_TOOL_NAME)
}

fn map_build_tools_error(error: BuildToolsError) -> AgentError {
    match error {
        BuildToolsError::InvalidConfiguration => AgentError::InvalidSessionSettings,
        BuildToolsError::Internal => AgentError::Internal,
    }
}

fn map_model_config_error(error: ModelConfigError) -> AgentError {
    match error {
        ModelConfigError::NotFound => AgentError::ModelNotFound,
        ModelConfigError::MissingApiKey => {
            AgentError::Config(crate::config::ConfigError::MissingModelApiKey)
        }
        ModelConfigError::InvalidTlsTrustStore => {
            AgentError::Config(crate::config::ConfigError::InvalidTlsTrustStore)
        }
        ModelConfigError::InvalidConfiguration
        | ModelConfigError::ClientBuild
        | ModelConfigError::InvalidReference => {
            AgentError::Config(crate::config::ConfigError::InvalidModel)
        }
    }
}

pub const fn agent_version() -> &'static str {
    VERSION
}

fn command_environment(config: &AgentConfig) -> CommandEnvironment {
    CommandEnvironment::new(
        config
            .models
            .values()
            .map(ModelConfig::credential_env_name)
            .map(OsString::from),
    )
}

// Keep prior credential names scrubbed from future Bash children too. A
// removed model's key may still be present in the Agent process environment;
// dropping that name during reload would make it visible to a later command.
fn reload_command_environment(
    current: &CommandEnvironment,
    candidate: &AgentConfig,
) -> CommandEnvironment {
    current.extended(
        candidate
            .models
            .values()
            .map(ModelConfig::credential_env_name)
            .map(OsString::from),
    )
}

#[cfg(test)]
mod tests;
