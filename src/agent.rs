use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use minicore_runtime::InteractionId;
use minicore_runtime::execution::{ConfigRevision, ExecutionConfig, UserInput};
use minicore_runtime::interaction::InteractionAnswer;
use minicore_runtime::model::ReasoningPreference;
use minicore_runtime::prompt::PromptProvider;
use minicore_runtime::tools::ToolPolicy;

use crate::Workspace;
use crate::config::AgentConfig;
use crate::error::AgentError;
use crate::event::{AgentEvent, AgentEventSink, AgentEventStream, EventMeta};
use crate::models::{ModelConfig, ModelConfigError, ModelInfo, Models};
use crate::policy::Policy;
use crate::profiles::{Profile, ProfileInfo, Profiles};
use crate::prompt::ProjectPromptProvider;
use crate::sessions::{Sessions, TurnCompletion};
use crate::store::{SESSION_FORMAT_VERSION, SessionRecord, Store};
use crate::tools::{BuildToolsError, CommandEnvironment};

pub use crate::history::{GetHistory, HistoryPage};
pub use crate::sessions::{SessionState, SessionStatus, TurnPersistence, TurnRef, TurnResult};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PingResponse {
    pub version: &'static str,
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

struct ResolvedSessionSettings {
    profile: String,
    model: String,
    reasoning: ReasoningPreference,
}

/// Top-level coordinator for the local Store and loaded Sessions.
///
/// `Agent::shutdown` is the cleanup barrier for embedded Rust callers: it
/// cancels active loops, waits for Agent-owned loop tasks, and awaits
/// persistence wrap-up. Dropping an `Agent` with live turns does not
/// synchronously wait for Agent-owned loop tasks.
pub struct Agent {
    config: AgentConfig,
    store: Store,
    profiles: Profiles,
    models: Models,
    command_environment: CommandEnvironment,
    sessions: Sessions,
    event_sink: AgentEventSink,
    events_rx: Option<mpsc::Receiver<AgentEvent>>,
}

impl Agent {
    pub async fn open(config: AgentConfig) -> Result<Self, AgentError> {
        config.validate().map_err(AgentError::Config)?;
        let models = Models::from_config(&config.models)
            .await
            .map_err(map_model_config_error)?;
        Self::open_parts(config, models).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_models(
        config: AgentConfig,
        models: Models,
    ) -> Result<Self, AgentError> {
        config.validate().map_err(AgentError::Config)?;
        Self::open_parts(config, models).await
    }

    async fn open_parts(config: AgentConfig, models: Models) -> Result<Self, AgentError> {
        let command_environment = CommandEnvironment::new(
            config
                .models
                .values()
                .map(ModelConfig::credential_env_name)
                .map(OsString::from),
        );
        let profiles = config.profiles();
        let store = Store::open(config.data_dir.clone())
            .await
            .map_err(crate::sessions::map_store_error)?;
        let (events_tx, events_rx) = mpsc::channel(config.event_capacity);
        let event_sink = AgentEventSink::new(events_tx);
        let agent = Self {
            config,
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

    pub fn take_events(&mut self) -> Result<AgentEventStream, AgentError> {
        self.events_rx
            .take()
            .map(AgentEventStream::new)
            .ok_or(AgentError::EventStreamTaken)
    }

    pub const fn ping(&self) -> PingResponse {
        PingResponse { version: VERSION }
    }

    pub fn list_profiles(&self) -> Vec<ProfileInfo> {
        self.profiles.list()
    }

    pub fn list_models(&self) -> Vec<ModelInfo> {
        self.models.list()
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>, AgentError> {
        let records = self
            .store
            .list_sessions()
            .await
            .map_err(crate::sessions::map_store_error)?;
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
        Ok(sessions)
    }

    pub async fn create_session(
        &mut self,
        request: CreateSession,
    ) -> Result<SessionInfo, AgentError> {
        let settings = self.resolve_session_settings(&request)?;
        let profile = self
            .profiles
            .get(&settings.profile)
            .cloned()
            .ok_or(AgentError::ProfileNotFound)?;
        let workspace = Arc::new(
            Workspace::open(request.workspace)
                .await
                .map_err(|_| AgentError::Workspace)?,
        );
        let canonical_workspace = workspace.root().to_path_buf();
        let session_id = crate::ids::SessionId::new().map_err(|_| AgentError::Internal)?;
        let record = build_record(
            session_id,
            &settings,
            &profile,
            request.title,
            canonical_workspace,
        )?;
        // Every predictable configuration error must fail before the store write.
        let presentation = self
            .build_presentation(session_id, Arc::clone(&workspace), record.model.clone())
            .await;
        let config =
            self.execution_config(&record, Arc::clone(&workspace), Arc::clone(&presentation))?;
        let options = self
            .config
            .loop_options(record.max_tool_rounds)
            .map_err(AgentError::Config)?;
        self.store
            .create_session(&record)
            .await
            .map_err(crate::sessions::map_store_error)?;
        let session = crate::sessions::Session::new(
            record.clone(),
            workspace,
            Vec::new().into(),
            std::collections::HashMap::new(),
            presentation,
            config,
            options,
            self.store.clone(),
            self.event_sink.clone(),
        );
        let _ = self.sessions.insert(session_id, session);
        let info = self
            .sessions
            .get(session_id)
            .expect("created session is loaded")
            .info(true);
        self.event_sink.try_send(AgentEvent::SessionOpened {
            session: info.clone(),
            meta: EventMeta {
                session_id,
                loop_id: None,
                dropped_before: 0,
            },
        });
        tracing::info!(session_id = %session_id, "session created");
        Ok(info)
    }

    pub async fn open_session(
        &mut self,
        session_id: crate::ids::SessionId,
    ) -> Result<SessionInfo, AgentError> {
        if let Some(session) = self.sessions.get(session_id) {
            tracing::debug!(session_id = %session_id, "session already loaded");
            return Ok(session.info(true));
        }
        let stored = self
            .store
            .load_session(session_id)
            .await
            .map_err(crate::sessions::map_store_error)?;
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
        let presentation = self
            .build_presentation(
                session_id,
                Arc::clone(&workspace),
                stored.record.model.clone(),
            )
            .await;
        let config = self.execution_config(
            &stored.record,
            Arc::clone(&workspace),
            Arc::clone(&presentation),
        )?;
        let options = self
            .config
            .loop_options(stored.record.max_tool_rounds)
            .map_err(AgentError::Config)?;
        let session = crate::sessions::Session::new(
            stored.record.clone(),
            workspace,
            stored.history,
            stored.user_times,
            presentation,
            config,
            options,
            self.store.clone(),
            self.event_sink.clone(),
        );
        let _ = self.sessions.insert(session_id, session);
        let info = self
            .sessions
            .get(session_id)
            .expect("opened session is loaded")
            .info(true);
        self.event_sink.try_send(AgentEvent::SessionOpened {
            session: info.clone(),
            meta: EventMeta {
                session_id,
                loop_id: None,
                dropped_before: 0,
            },
        });
        tracing::info!(session_id = %session_id, "session opened");
        Ok(info)
    }

    /// Closes and unloads a loaded Session by ID.
    ///
    /// If the Session has an active loop, it is cancelled and its Agent-owned task
    /// is joined before unloading. MiniCore Agent v0.3 uses the Runtime
    /// user-cancellation path when closing or shutting down an active Session;
    /// it does not currently preserve a distinct shutdown cancellation reason.
    pub async fn close_session(
        &mut self,
        session_id: crate::ids::SessionId,
    ) -> Result<(), AgentError> {
        let session = self
            .sessions
            .remove(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        let result = session.shutdown().await;
        self.event_sink.try_send(AgentEvent::SessionClosed {
            session_id,
            meta: EventMeta {
                session_id,
                loop_id: None,
                dropped_before: 0,
            },
        });
        if result.is_ok() {
            tracing::info!(session_id = %session_id, "session closed");
        }
        result
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
            .map_err(crate::sessions::map_store_error);
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

    pub async fn update_session(
        &mut self,
        request: UpdateSession,
    ) -> Result<SessionUpdateResult, AgentError> {
        if request.model.is_none() && request.reasoning.is_none() {
            return Err(AgentError::InvalidArguments);
        }
        let session_id = request.session_id;
        let session = self
            .sessions
            .get(session_id)
            .cloned()
            .ok_or(AgentError::SessionNotLoaded)?;
        let workspace = session.workspace();
        let mut candidate = session.record();
        if let Some(model) = &request.model {
            candidate.model = model.clone();
        }
        if let Some(reasoning) = request.reasoning {
            candidate.reasoning = reasoning;
        }
        let config = self.execution_config(&candidate, workspace, session.presentation())?;
        let active_revision = session.update(candidate.clone(), config).await?;
        session
            .presentation()
            .set_model_label(candidate.model.clone());
        Ok(SessionUpdateResult {
            session: session.info(true),
            active_revision,
        })
    }

    /// Sends a prompt and preserves the original public return type. RPC uses
    /// `send_accepted` when it also needs the optional acceptance timestamp.
    pub async fn send(&mut self, request: SendMessage) -> Result<TurnRef, AgentError> {
        Ok(self.send_accepted(request).await?.turn)
    }

    pub(crate) async fn send_accepted(
        &mut self,
        request: SendMessage,
    ) -> Result<crate::sessions::LoopAccepted, AgentError> {
        let session = self
            .sessions
            .get(request.session_id)
            .cloned()
            .ok_or(AgentError::SessionNotLoaded)?;
        let input = UserInput::text(request.text).map_err(|_| AgentError::InvalidInput)?;
        session.start_loop(input).await
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
        let session = self
            .sessions
            .get(request.turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.steer(request.turn, request.text)
    }

    /// Read-only footer/detail data for one loaded session (never drives
    /// execution).
    pub fn session_presentation(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<crate::presentation::PresentationView, AgentError> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        Ok(session.presentation_view())
    }

    pub fn cancel(&self, turn: TurnRef) -> Result<bool, AgentError> {
        let session = self
            .sessions
            .get(turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.cancel(turn)
    }

    #[cfg(test)]
    pub(crate) fn abort_active_task_for_test(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<(), AgentError> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.abort_active_task()
    }

    #[cfg(test)]
    pub(crate) fn runtime_loop_finished_for_test(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<bool, AgentError> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        Ok(session.runtime_loop_finished())
    }

    #[cfg(test)]
    pub(crate) fn forward_runtime_event_for_test(
        &self,
        session_id: crate::ids::SessionId,
        envelope: minicore_runtime::LoopEventEnvelope,
    ) -> Result<(), AgentError> {
        let session = self
            .sessions
            .get(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        crate::sessions::forward_loop_event(session_id, envelope, session);
        Ok(())
    }

    pub async fn wait_turn(
        &self,
        turn: TurnRef,
    ) -> Result<Arc<crate::sessions::TurnResult>, AgentError> {
        let session = self
            .sessions
            .get(turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.wait(turn).await
    }

    pub(crate) fn wait_turn_receiver(
        &self,
        turn: TurnRef,
    ) -> Result<watch::Receiver<Option<TurnCompletion>>, AgentError> {
        let session = self
            .sessions
            .get(turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.wait_receiver(turn)
    }

    pub async fn answer(&self, request: AnswerInteraction) -> Result<(), AgentError> {
        let session = self
            .sessions
            .get(request.turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.answer(request.turn, request.interaction_id, request.answer)
    }

    pub fn history(&self, request: GetHistory) -> Result<HistoryPage, AgentError> {
        let session = self
            .sessions
            .get(request.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        session.history(&request)
    }

    /// Orderly shutdown barrier for embedded Rust callers.
    ///
    /// Cancels active loops across all loaded Sessions, waits for Agent-owned
    /// loop tasks and persistence completion, and drops event channels.
    /// Dropping an `Agent` with live turns does not synchronously wait for
    /// Agent-owned loop tasks.
    ///
    /// MiniCore Agent v0.3 uses the Runtime user-cancellation path when closing
    /// or shutting down an active Session; it does not currently preserve a distinct
    /// shutdown cancellation reason.
    pub async fn shutdown(mut self) -> Result<(), AgentError> {
        tracing::info!("agent shutdown begin");
        let result = self.sessions.shutdown_all().await;
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

    /// Assembles a complete runtime `ExecutionConfig` from the session record:
    /// model, tools, policy, and the project prompt provider. The same
    /// settings are reused across every turn and replaced atomically on
    /// update. Model and tools are wrapped with the per-session presentation
    /// (identity capture + bounded display; never execution semantics).
    fn execution_config(
        &self,
        record: &SessionRecord,
        workspace: Arc<Workspace>,
        presentation: Arc<crate::presentation::Presentation>,
    ) -> Result<ExecutionConfig, AgentError> {
        let model = self
            .models
            .get(&record.model)
            .map_err(map_model_config_error)?;
        let model = crate::presentation::PresentationModel::new(model, Arc::clone(&presentation));
        let tools = crate::tools::build_tools_with_presentation(
            &record.tools,
            Arc::clone(&workspace),
            self.command_environment.clone(),
            &presentation,
        )
        .map_err(map_build_tools_error)?;
        let policy: Option<Arc<dyn ToolPolicy>> = if record.tools.is_empty() {
            None
        } else {
            Some(Arc::new(Policy::new(record.approval)))
        };
        let prompt: Arc<dyn PromptProvider> = crate::presentation::SteerReceiptPrompt::new(
            Arc::new(
                ProjectPromptProvider::new(workspace, record.system_prompt.clone())
                    .map_err(|_| AgentError::InvalidSessionSettings)?,
            ),
            presentation,
        );
        ExecutionConfig::new(model, record.reasoning, tools, policy, prompt)
            .map_err(|_| AgentError::InvalidSessionSettings)
    }

    /// Per-session presentation wired into this session's model/tool wrappers
    /// and the `session.presentation` read. Refreshes the git branch at
    /// create/open; later refreshes happen at tool-batch boundaries in the
    /// per-loop worker.
    async fn build_presentation(
        &self,
        session_id: crate::ids::SessionId,
        workspace: Arc<Workspace>,
        model_label: String,
    ) -> Arc<crate::presentation::Presentation> {
        let presentation =
            crate::presentation::Presentation::new(session_id, self.event_sink.clone());
        presentation.set_model_label(model_label);
        presentation.set_branch(workspace.git_branch().await);
        presentation
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

#[cfg(test)]
mod tests;
