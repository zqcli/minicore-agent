use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use minicore_runtime::config::{SessionSpec, Timestamp, TurnOptions, UserInput};
use minicore_runtime::context::ContextProvider;
use minicore_runtime::conversation::TranscriptPage;
use minicore_runtime::error::{SessionError, SessionOpenErrorKind};
use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, TurnId};
use minicore_runtime::session::{
    InteractionAnswer, SessionRuntime, SessionRuntimeOptions, SessionState, TurnHandle,
};
use minicore_runtime::storage::{SessionLog, SessionLogError};
use minicore_runtime::tools::ToolPolicy;

use crate::Workspace;
use crate::config::{AgentConfig, ProfileCompaction};
use crate::context::ProjectContext;
use crate::error::{AgentError, StoreError};
use crate::event::{AgentEvent, AgentEventSink, AgentEventStream, EventMeta};
use crate::models::{ModelConfigError, Models};
use crate::policy::Policy;
use crate::profiles::{Profile, Profiles};
use crate::sessions::{
    CompletionNotifier, LoadedSession, MetadataWorker, OutboundSequencer, Sessions,
};
use crate::store::{SessionRecord, Store};
use crate::tools::{BuildToolsError, build_tools};

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
        let task_runtime = Handle::try_current().map_err(|_| AgentError::Internal)?;
        let kernel = config.kernel_config().map_err(AgentError::Config)?;
        let profiles = config.profiles();
        for profile in config.profiles.values() {
            if !models.contains(&profile.model) {
                return Err(AgentError::ModelNotFound);
            }
        }
        let store = Store::open(config.data_dir.clone())
            .await
            .map_err(map_store_error)?;
        let (events_tx, events_rx) = mpsc::channel(config.event_capacity);
        Ok(Self {
            config,
            store,
            profiles,
            models,
            sessions: Sessions::new(),
            events_tx,
            events_rx: Some(events_rx),
            task_runtime,
            kernel,
        })
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

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>, AgentError> {
        let records = self.store.list_sessions().await.map_err(map_store_error)?;
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
            .map_err(map_store_error)?;
        let runtime = match SessionRuntime::create(session_id, spec, Box::new(log), options).await {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = self.store.delete_session(session_id).await;
                return Err(map_session_open_error(error));
            }
        };
        let loaded = match self.attach_runtime(runtime, record.clone()).await {
            Ok(loaded) => loaded,
            Err(error) => {
                let _ = self.store.delete_session(session_id).await;
                return Err(error);
            }
        };
        let info = self.session_info(&record, Some(&loaded));
        let _ = self.sessions.insert(session_id, loaded);
        Ok(info)
    }

    pub async fn open_session(&mut self, session_id: SessionId) -> Result<SessionInfo, AgentError> {
        if let Some(loaded) = self.sessions.get(session_id) {
            return Ok(self.session_info(&loaded.record, Some(loaded)));
        }
        let record = self
            .store
            .load_record(session_id)
            .await
            .map_err(map_store_error)?;
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
        let mut log = self
            .store
            .open_log(session_id)
            .await
            .map_err(map_store_error)?;
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
        let _ = self.sessions.insert(session_id, loaded);
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
        loaded.shutdown(meta).await
    }

    pub async fn delete_session(&mut self, session_id: SessionId) -> Result<(), AgentError> {
        if self.sessions.contains(session_id) {
            return Err(AgentError::SessionAlreadyLoaded);
        }
        self.store
            .delete_session(session_id)
            .await
            .map_err(map_store_error)
    }

    pub fn session_state(&self, session_id: SessionId) -> Result<SessionState, AgentError> {
        self.sessions
            .get(session_id)
            .map(|loaded| loaded.handle.state())
            .ok_or(AgentError::SessionNotLoaded)
    }

    pub async fn send(&mut self, request: SendMessage) -> Result<TurnRef, AgentError> {
        let session_id = request.session_id;
        self.cleanup_finished_turn(session_id)?;
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
        let loaded = self
            .sessions
            .get_mut(session_id)
            .expect("submitted turn belongs to loaded session");
        loaded.active_turn = Some(turn.clone());
        loaded.completion.enqueue(turn.clone(), turn_ref);
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
        if active.is_finished() {
            return Ok(false);
        }
        Ok(active.cancel())
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
            .cloned()
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
        let result = self.sessions.shutdown_all().await;
        drop(self.events_tx);
        drop(self.events_rx.take());
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
        let compaction = match &profile.compaction {
            ProfileCompaction::Disabled => minicore_runtime::CompactionConfig::Disabled,
            ProfileCompaction::Model { .. } => return Err(AgentError::ModelNotImplemented),
        };
        let system_prompt = minicore_runtime::BoundedText::new(&profile.system_prompt)
            .map_err(|_| AgentError::InvalidInput)?;
        let spec = SessionSpec::new(
            model_ref,
            profile.reasoning,
            system_prompt,
            enabled_tools,
            profile.max_tool_rounds,
            compaction,
        )
        .map_err(|_| AgentError::InvalidInput)?;
        let tools =
            build_tools(&profile.tools, Arc::clone(&workspace)).map_err(map_build_tools_error)?;
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
        let sequencer = OutboundSequencer::new(
            &self.task_runtime,
            event_stream,
            state,
            handle.clone(),
            event_sink.clone(),
            opened,
        );
        let completion = CompletionNotifier::new(
            &self.task_runtime,
            sequencer.completion_sender(),
            sequencer.stop_signal(),
        );
        let metadata =
            MetadataWorker::new(&self.task_runtime, self.store.clone(), runtime.session_id());
        Ok(LoadedSession {
            record,
            runtime,
            handle,
            active_turn: None,
            sequencer,
            completion,
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

    fn cleanup_finished_turn(&mut self, session_id: SessionId) -> Result<(), AgentError> {
        let loaded = self
            .sessions
            .get_mut(session_id)
            .ok_or(AgentError::SessionNotLoaded)?;
        if loaded
            .active_turn
            .as_ref()
            .is_some_and(TurnHandle::is_finished)
        {
            loaded.active_turn = None;
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
        ModelConfigError::NotImplemented => AgentError::ModelNotImplemented,
        ModelConfigError::InvalidConfiguration | ModelConfigError::InvalidReference => {
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
