use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use minicore_runtime::config::{SessionSpec, Timestamp, TurnOptions, UserInput};
use minicore_runtime::context::ContextProvider;
use minicore_runtime::conversation::{TranscriptPage, TurnTerminal};
use minicore_runtime::error::{SessionError, SessionOpenErrorKind};
use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, TurnId};
use minicore_runtime::session::{
    InteractionAnswer, SessionRuntime, SessionRuntimeOptions, SessionState, TurnHandle,
};
use minicore_runtime::storage::{SessionLog, SessionLogError};
use minicore_runtime::tools::ToolPolicy;

use crate::Workspace;
use crate::config::AgentConfig;
use crate::context::ProjectContext;
use crate::error::{AgentError, StoreError};
use crate::event::{AgentEvent, AgentEventSink, AgentEventStream, EventMeta};
use crate::models::{ModelConfig, ModelConfigError, Models};
use crate::policy::Policy;
use crate::profiles::{Profile, Profiles};
use crate::sessions::{
    ActiveTurn, CompletionReady, LoadedSession, MetadataWorker, SessionPump, Sessions,
};
use crate::store::{SessionRecord, Store};
use crate::tools::{BuildToolsError, CommandEnvironment, build_tools};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PingResponse {
    pub version: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSession {
    pub workspace: PathBuf,
    pub profile: String,
    pub title: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessage {
    pub session_id: SessionId,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnswerInteraction {
    pub session_id: SessionId,
    pub interaction_id: InteractionId,
    pub answer: InteractionAnswer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetTranscript {
    pub session_id: SessionId,
    pub after: Option<minicore_runtime::ConversationSeq>,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInfo {
    pub session_id: SessionId,
    pub title: Option<String>,
    pub profile: String,
    pub workspace: PathBuf,
    pub loaded: bool,
    pub instance_id: Option<SessionInstanceId>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnRef {
    pub session_id: SessionId,
    pub instance_id: SessionInstanceId,
    pub turn_id: TurnId,
}

impl TurnRef {
    fn from_handle(handle: &TurnHandle) -> Self {
        Self {
            session_id: handle.session_id(),
            instance_id: handle.instance_id(),
            turn_id: handle.turn_id(),
        }
    }
}

pub struct Agent {
    config: AgentConfig,
    store: Store,
    profiles: Profiles,
    models: Models,
    command_environment: CommandEnvironment,
    sessions: Sessions,
    events_tx: mpsc::Sender<AgentEvent>,
    events_rx: Option<mpsc::Receiver<AgentEvent>>,
    task_runtime: Handle,
    kernel: minicore_runtime::KernelConfig,
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
        let task_runtime = Handle::try_current().map_err(|_| AgentError::Internal)?;
        let kernel = config.kernel_config().map_err(AgentError::Config)?;
        let profiles = config.profiles();
        let store = Store::open(config.data_dir.clone())
            .await
            .map_err(|error| map_logged_store_error("agent_open", None, error))?;
        let (events_tx, events_rx) = mpsc::channel(config.event_capacity);
        let agent = Self {
            config,
            store,
            profiles,
            models,
            command_environment,
            sessions: Sessions::new(),
            events_tx,
            events_rx: Some(events_rx),
            task_runtime,
            kernel,
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

    pub(crate) fn profile_infos(&self) -> Vec<crate::profiles::ProfileInfo> {
        self.profiles.list()
    }

    pub(crate) fn model_infos(&self) -> Vec<crate::models::ModelInfo> {
        self.models.list()
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>, AgentError> {
        let records = self
            .store
            .list_sessions()
            .await
            .map_err(|error| map_logged_store_error("list_sessions", None, error))?;
        Ok(records
            .iter()
            .map(|record| self.session_info(record, self.sessions.get(record.session_id)))
            .collect())
    }

    pub async fn create_session(
        &mut self,
        request: CreateSession,
    ) -> Result<SessionInfo, AgentError> {
        let profile_id = if request.profile.is_empty() {
            self.config.default_profile.clone()
        } else {
            request.profile.clone()
        };
        let profile = self
            .profiles
            .get(&profile_id)
            .cloned()
            .ok_or(AgentError::ProfileNotFound)?;
        let workspace = Arc::new(
            Workspace::open(request.workspace)
                .await
                .map_err(|_| AgentError::Workspace)?,
        );
        let canonical_workspace = workspace.root().to_path_buf();
        let (spec, options) = self.session_parts(&profile, workspace)?;
        let session_id = SessionId::new().map_err(|_| AgentError::Internal)?;
        let timestamp = current_timestamp()?;
        let record = SessionRecord {
            session_id,
            title: request.title,
            profile: profile_id,
            workspace: canonical_workspace,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        let log = self
            .store
            .create_session(record.clone())
            .await
            .map_err(|error| map_logged_store_error("create_session", Some(session_id), error))?;
        let runtime = match SessionRuntime::create(session_id, spec, Box::new(log), options).await {
            Ok(runtime) => runtime,
            Err(error) => {
                if let Err(cleanup_error) = self.store.delete_session(session_id).await {
                    log_store_error("create_session_cleanup", Some(session_id), &cleanup_error);
                }
                return Err(map_session_open_error(error));
            }
        };
        let loaded = match self.attach_runtime(runtime, record.clone()).await {
            Ok(loaded) => loaded,
            Err(error) => {
                if let Err(cleanup_error) = self.store.delete_session(session_id).await {
                    log_store_error("create_session_cleanup", Some(session_id), &cleanup_error);
                }
                return Err(error);
            }
        };
        let info = self.session_info(&record, Some(&loaded));
        let instance_id = loaded.handle.instance_id();
        let _ = self.sessions.insert(session_id, loaded);
        tracing::info!(
            session_id = %session_id,
            instance_id = %instance_id,
            "session created"
        );
        Ok(info)
    }

    pub async fn open_session(&mut self, session_id: SessionId) -> Result<SessionInfo, AgentError> {
        if let Some(loaded) = self.sessions.get(session_id) {
            let info = self.session_info(&loaded.record, Some(loaded));
            tracing::debug!(
                session_id = %session_id,
                instance_id = %loaded.handle.instance_id(),
                "session opened"
            );
            return Ok(info);
        }
        let record = self
            .store
            .load_record(session_id)
            .await
            .map_err(|error| map_logged_store_error("load_record", Some(session_id), error))?;
        let workspace = Arc::new(
            Workspace::open(record.workspace.clone())
                .await
                .map_err(|_| AgentError::Workspace)?,
        );
        if workspace.root() != record.workspace.as_path() {
            return Err(AgentError::Workspace);
        }
        let profile = self
            .profiles
            .get(&record.profile)
            .cloned()
            .ok_or(AgentError::ProfileNotFound)?;
        let (spec, options) = self.session_parts(&profile, workspace)?;
        let mut log = self.store.open_log(session_id).await.map_err(|error| {
            if matches!(&error, StoreError::Log(_)) {
                map_store_error(error)
            } else {
                map_logged_store_error("open_log", Some(session_id), error)
            }
        })?;
        let manifest = log.load_manifest().await.map_err(map_log_error)?;
        if manifest.session_id != session_id || manifest.spec != spec {
            log.close().await.map_err(map_log_error)?;
            return Err(AgentError::SessionSpecMismatch);
        }
        let runtime = SessionRuntime::load(session_id, Box::new(log), options)
            .await
            .map_err(map_session_open_error)?;
        let loaded = self.attach_runtime(runtime, record.clone()).await?;
        let info = self.session_info(&record, Some(&loaded));
        let instance_id = loaded.handle.instance_id();
        let _ = self.sessions.insert(session_id, loaded);
        tracing::info!(
            session_id = %session_id,
            instance_id = %instance_id,
            "session opened"
        );
        Ok(info)
    }

    pub async fn close_session(&mut self, session_id: SessionId) -> Result<(), AgentError> {
        let loaded = self
            .sessions
            .remove(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        let meta = EventMeta {
            session_id,
            instance_id: loaded.handle.instance_id(),
            dropped_before: 0,
        };
        let instance_id = meta.instance_id;
        let result = loaded.shutdown(meta).await;
        if result.is_ok() {
            tracing::info!(
                session_id = %session_id,
                instance_id = %instance_id,
                "session closed"
            );
        } else {
            tracing::warn!(
                session_id = %session_id,
                instance_id = %instance_id,
                error_kind = "shutdown",
                "session close failed"
            );
        }
        result
    }

    pub async fn delete_session(&mut self, session_id: SessionId) -> Result<(), AgentError> {
        if self.sessions.contains(session_id) {
            return Err(AgentError::SessionAlreadyLoaded);
        }
        let result = self
            .store
            .delete_session(session_id)
            .await
            .map_err(|error| map_logged_store_error("delete_session", Some(session_id), error));
        if result.is_ok() {
            tracing::info!(session_id = %session_id, "session deleted");
        }
        result
    }

    pub fn session_state(&self, session_id: SessionId) -> Result<SessionState, AgentError> {
        self.sessions
            .get(session_id)
            .map(|loaded| loaded.handle.state())
            .ok_or(AgentError::SessionNotLoaded)
    }

    pub async fn send(&mut self, request: SendMessage) -> Result<TurnRef, AgentError> {
        let session_id = request.session_id;
        self.cleanup_finished_turn(session_id).await?;
        let input = UserInput::text(request.text).map_err(|_| AgentError::InvalidInput)?;
        let handle = {
            let loaded = self
                .sessions
                .get_mut(session_id)
                .ok_or(AgentError::SessionNotLoaded)?;
            loaded.handle.clone()
        };
        let turn = handle
            .submit(input, TurnOptions::default())
            .await
            .map_err(map_session_error)?;
        let turn_ref = TurnRef::from_handle(&turn);
        let (completion_tx, stop) = {
            let loaded = self
                .sessions
                .get(session_id)
                .expect("submitted turn belongs to loaded session");
            (loaded.pump.completion_sender(), loaded.pump.stop_token())
        };
        let completion_turn = turn.clone();
        let completion_task = self.task_runtime.spawn(async move {
            let outcome = tokio::select! {
                biased;
                _ = stop.cancelled() => return,
                outcome = completion_turn.wait() => outcome,
            };
            let Ok(outcome) = outcome else {
                tracing::warn!(
                    session_id = %turn_ref.session_id,
                    instance_id = %turn_ref.instance_id,
                    turn_id = %turn_ref.turn_id,
                    error_kind = "turn_wait",
                    "turn completion wait failed"
                );
                return;
            };
            tracing::info!(
                session_id = %turn_ref.session_id,
                instance_id = %turn_ref.instance_id,
                turn_id = %turn_ref.turn_id,
                outcome = turn_terminal_category(&outcome.terminal),
                "turn completed"
            );
            let ready = CompletionReady { turn_ref, outcome };
            tokio::select! {
                biased;
                _ = stop.cancelled() => {}
                _ = completion_tx.send(ready) => {}
            }
        });
        let loaded = self
            .sessions
            .get_mut(session_id)
            .expect("submitted turn belongs to loaded session");
        loaded.active_turn = Some(ActiveTurn {
            handle: turn,
            completion_task,
        });
        tracing::info!(
            session_id = %turn_ref.session_id,
            instance_id = %turn_ref.instance_id,
            turn_id = %turn_ref.turn_id,
            "turn submitted"
        );
        self.schedule_touch(session_id);
        Ok(turn_ref)
    }

    pub fn cancel(&self, turn: TurnRef) -> Result<bool, AgentError> {
        let loaded = self
            .sessions
            .get(turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        validate_turn_ref(loaded, turn)?;
        let active = loaded
            .active_turn
            .as_ref()
            .ok_or(AgentError::TurnNotFound)?;
        if active.handle.is_finished() {
            tracing::debug!(
                session_id = %turn.session_id,
                instance_id = %turn.instance_id,
                turn_id = %turn.turn_id,
                cancelled = false,
                "turn cancel requested"
            );
            return Ok(false);
        }
        let cancelled = active.handle.cancel();
        tracing::info!(
            session_id = %turn.session_id,
            instance_id = %turn.instance_id,
            turn_id = %turn.turn_id,
            cancelled = cancelled,
            "turn cancel requested"
        );
        Ok(cancelled)
    }

    pub fn turn_handle(&self, turn: TurnRef) -> Result<TurnHandle, AgentError> {
        // The v0.1 contract retains only one active TurnHandle per loaded session. Callers that
        // need to wait asynchronously (including a future RPC turn.wait method) must clone this
        // handle at request time; TurnRef is an identity, not a historical handle registry key.
        let loaded = self
            .sessions
            .get(turn.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        validate_turn_ref(loaded, turn)?;
        loaded
            .active_turn
            .as_ref()
            .map(|active| active.handle.clone())
            .ok_or(AgentError::TurnNotFound)
    }

    pub async fn answer(&self, request: AnswerInteraction) -> Result<(), AgentError> {
        let loaded = self
            .sessions
            .get(request.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        loaded
            .handle
            .answer(request.interaction_id, request.answer)
            .await
            .map_err(map_session_error)
    }

    pub async fn transcript(&self, request: GetTranscript) -> Result<TranscriptPage, AgentError> {
        let loaded = self
            .sessions
            .get(request.session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        loaded
            .handle
            .transcript(request.after, request.limit)
            .await
            .map_err(map_session_error)
    }

    pub async fn shutdown(mut self) -> Result<(), AgentError> {
        tracing::info!("agent shutdown begin");
        let result = self.sessions.shutdown_all().await;
        drop(self.events_tx);
        drop(self.events_rx.take());
        tracing::info!(success = result.is_ok(), "agent shutdown end");
        result
    }

    fn session_parts(
        &self,
        profile: &Profile,
        workspace: Arc<Workspace>,
    ) -> Result<(SessionSpec, SessionRuntimeOptions), AgentError> {
        let model = self
            .models
            .get(&profile.model)
            .map_err(map_model_config_error)?;
        let model_ref = Models::model_ref(&profile.model).map_err(map_model_config_error)?;
        let enabled_tools = profile
            .tools
            .iter()
            .map(|name| name.parse().map_err(|_| AgentError::InvalidInput))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let system_prompt = minicore_runtime::BoundedText::new(&profile.system_prompt)
            .map_err(|_| AgentError::InvalidInput)?;
        let spec = SessionSpec::new(
            model_ref,
            profile.reasoning,
            system_prompt,
            enabled_tools,
            profile.max_tool_rounds,
            minicore_runtime::CompactionConfig::Disabled,
        )
        .map_err(|_| AgentError::InvalidInput)?;
        let tools = build_tools(
            &profile.tools,
            Arc::clone(&workspace),
            self.command_environment.clone(),
        )
        .map_err(map_build_tools_error)?;
        let policy: Option<Arc<dyn ToolPolicy>> = if spec.enabled_tools.is_empty() {
            None
        } else {
            Some(Arc::new(Policy::new(profile.approval)))
        };
        let context: Arc<dyn ContextProvider> = Arc::new(ProjectContext::new(workspace));
        let bindings =
            minicore_runtime::SessionBindings::new(model, tools, policy, Some(context), None);
        let options =
            SessionRuntimeOptions::new(self.kernel.clone(), bindings, self.task_runtime.clone())
                .map_err(|_| {
                    AgentError::Core(crate::error::CoreErrorView::new(
                        "session options are invalid",
                        false,
                    ))
                })?;
        Ok((spec, options))
    }

    async fn attach_runtime(
        &self,
        mut runtime: SessionRuntime,
        record: SessionRecord,
    ) -> Result<LoadedSession, AgentError> {
        let event_stream = match runtime.take_events() {
            Ok(stream) => stream,
            Err(_) => {
                let _ = runtime.shutdown().await;
                return Err(AgentError::Internal);
            }
        };
        let handle = runtime.handle();
        let state = handle.watch_state();
        let event_sink = AgentEventSink::new(self.events_tx.clone());
        let opened = SessionInfo {
            session_id: record.session_id,
            title: record.title.clone(),
            profile: record.profile.clone(),
            workspace: record.workspace.clone(),
            loaded: true,
            instance_id: Some(handle.instance_id()),
            created_at: record.created_at.clone(),
            updated_at: record.updated_at.clone(),
        };
        let pump = SessionPump::new(
            &self.task_runtime,
            event_stream,
            state,
            event_sink.clone(),
            opened,
        );
        let metadata =
            MetadataWorker::new(&self.task_runtime, self.store.clone(), runtime.session_id());
        Ok(LoadedSession {
            record,
            runtime,
            handle,
            active_turn: None,
            pump,
            metadata,
            event_sink,
        })
    }

    fn session_info(&self, record: &SessionRecord, loaded: Option<&LoadedSession>) -> SessionInfo {
        SessionInfo {
            session_id: record.session_id,
            title: record.title.clone(),
            profile: record.profile.clone(),
            workspace: record.workspace.clone(),
            loaded: loaded.is_some(),
            instance_id: loaded.map(|loaded| loaded.handle.instance_id()),
            created_at: record.created_at.clone(),
            updated_at: record.updated_at.clone(),
        }
    }

    async fn cleanup_finished_turn(&mut self, session_id: SessionId) -> Result<(), AgentError> {
        let (completion_result, instance_id, turn_id) = {
            let loaded = self
                .sessions
                .get_mut(session_id)
                .ok_or(AgentError::SessionNotLoaded)?;
            let Some(active) = loaded.active_turn.as_mut() else {
                return Ok(());
            };
            if !active.handle.is_finished() {
                return Ok(());
            }
            let instance_id = active.handle.instance_id();
            let turn_id = active.handle.turn_id();
            let result = (&mut active.completion_task).await;
            (result, instance_id, turn_id)
        };
        self.sessions
            .get_mut(session_id)
            .expect("finished turn belongs to loaded session")
            .active_turn
            .take();
        if completion_result.is_err() {
            tracing::warn!(
                session_id = %session_id,
                instance_id = %instance_id,
                turn_id = %turn_id,
                error_kind = "completion_join",
                "turn completion task failed"
            );
            return Err(AgentError::Internal);
        }
        Ok(())
    }

    fn schedule_touch(&mut self, session_id: SessionId) {
        let Ok(updated_at) = current_timestamp() else {
            return;
        };
        let Some(loaded) = self.sessions.get_mut(session_id) else {
            return;
        };
        loaded.record.updated_at = updated_at.clone();
        // A terminal metadata failure only stops persistence; the in-memory session timestamp and
        // the already accepted turn remain valid. The worker reports false rather than pretending
        // that this update was queued.
        let _ = loaded.metadata.update(updated_at);
    }
}

fn validate_turn_ref(loaded: &LoadedSession, turn: TurnRef) -> Result<(), AgentError> {
    if loaded.handle.instance_id() != turn.instance_id
        || loaded.handle.session_id() != turn.session_id
        || loaded.active_turn_id() != Some(turn.turn_id)
    {
        return Err(AgentError::TurnNotFound);
    }
    Ok(())
}

fn turn_terminal_category(terminal: &TurnTerminal) -> &'static str {
    match terminal {
        TurnTerminal::Completed => "completed",
        TurnTerminal::Failed { .. } => "failed",
        TurnTerminal::CancelledByUser => "cancelled_by_user",
        TurnTerminal::CancelledByShutdown => "cancelled_by_shutdown",
        TurnTerminal::CancelledByRestart => "cancelled_by_restart",
        TurnTerminal::BudgetExceeded => "budget_exceeded",
    }
}

fn current_timestamp() -> Result<String, AgentError> {
    Timestamp::now_utc()
        .map(|timestamp| timestamp.as_str().to_owned())
        .map_err(|_| AgentError::Internal)
}

fn map_build_tools_error(error: BuildToolsError) -> AgentError {
    match error {
        BuildToolsError::InvalidConfiguration => {
            AgentError::Config(crate::config::ConfigError::InvalidProfile)
        }
        BuildToolsError::Internal => AgentError::Internal,
    }
}

fn map_log_error(error: SessionLogError) -> AgentError {
    map_store_error(StoreError::Log(error))
}

fn map_model_config_error(error: ModelConfigError) -> AgentError {
    match error {
        ModelConfigError::NotFound => AgentError::ModelNotFound,
        ModelConfigError::InvalidConfiguration
        | ModelConfigError::MissingApiKey
        | ModelConfigError::ClientBuild
        | ModelConfigError::InvalidReference => {
            AgentError::Config(crate::config::ConfigError::InvalidModel)
        }
    }
}

fn map_store_error(error: StoreError) -> AgentError {
    match error {
        StoreError::SessionNotFound => AgentError::SessionNotFound,
        StoreError::SessionAlreadyExists
        | StoreError::InvalidRoot
        | StoreError::InvalidRecord
        | StoreError::Corrupt
        | StoreError::Unavailable
        | StoreError::UnknownOutcome
        | StoreError::CleanupFailed { .. }
        | StoreError::Internal
        | StoreError::Log(_) => AgentError::Store,
    }
}

fn map_logged_store_error(
    operation: &'static str,
    session_id: Option<SessionId>,
    error: StoreError,
) -> AgentError {
    log_store_error(operation, session_id, &error);
    map_store_error(error)
}

fn log_store_error(operation: &'static str, session_id: Option<SessionId>, error: &StoreError) {
    if let Some(session_id) = session_id {
        tracing::warn!(
            operation = operation,
            session_id = %session_id,
            error_kind = error.kind(),
            "store operation failed"
        );
    } else {
        tracing::warn!(
            operation = operation,
            error_kind = error.kind(),
            "store operation failed"
        );
    }
}

fn map_session_open_error(error: minicore_runtime::error::SessionOpenError) -> AgentError {
    match error.kind() {
        SessionOpenErrorKind::InvalidConfiguration
        | SessionOpenErrorKind::InvalidManifest
        | SessionOpenErrorKind::BindingMismatch
        | SessionOpenErrorKind::SessionIdMismatch => AgentError::Core(
            crate::error::CoreErrorView::new("session open failed", false),
        ),
        SessionOpenErrorKind::Log => {
            let retryable = error
                .log_error()
                .is_some_and(|log_error| log_error.diagnostic().retryable);
            AgentError::Core(crate::error::CoreErrorView::new(
                "session log open failed",
                retryable,
            ))
        }
        SessionOpenErrorKind::RecoveryUncertain => AgentError::Core(
            crate::error::CoreErrorView::new("session recovery is uncertain", false),
        ),
        SessionOpenErrorKind::ActorStartFailed => AgentError::Internal,
        _ => AgentError::Core(crate::error::CoreErrorView::new(
            "session open failed",
            false,
        )),
    }
}

fn map_session_error(error: SessionError) -> AgentError {
    match error {
        SessionError::Closed => AgentError::SessionClosed,
        SessionError::Busy { .. } => AgentError::SessionBusy,
        SessionError::Degraded(_) => {
            AgentError::Core(crate::error::CoreErrorView::new("session degraded", false))
        }
        SessionError::Backpressure => AgentError::Core(crate::error::CoreErrorView::new(
            "session command backpressure",
            true,
        )),
        SessionError::InvalidInput(_) => AgentError::InvalidInput,
        SessionError::InteractionNotFound | SessionError::InteractionAlreadyResolved => {
            AgentError::InteractionNotFound
        }
        SessionError::InteractionKindMismatch => AgentError::InvalidInteraction,
        SessionError::TranscriptUnavailable(diagnostic) => {
            AgentError::Core(crate::error::CoreErrorView::new(
                "session transcript unavailable",
                diagnostic.retryable,
            ))
        }
        _ => AgentError::Internal,
    }
}

pub const fn agent_version() -> &'static str {
    VERSION
}

#[cfg(test)]
mod tests;
