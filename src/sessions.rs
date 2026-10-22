#[cfg(test)]
use crate::test_support::{Gate, Registry};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use minicore_runtime::execution::{ConfigRevision, ExecutionConfig, UserInput};
use minicore_runtime::history::{HistoryItem, estimate_history_bytes};
use minicore_runtime::interaction::InteractionAnswer;
use minicore_runtime::model::{Model, ModelDescriptor};
use minicore_runtime::tools::{ToolName, ToolSpec};
use minicore_runtime::value::BoundedText;
use minicore_runtime::{
    AgentLoop, AnswerError, LoopHandle, LoopOptions, LoopReport, LoopRequest, SteerError,
    UpdateError,
};
use minicore_runtime::{InteractionId, LoopId};

use crate::agent::SessionInfo;
use crate::compaction::{
    AutoContext, AutomaticCompactionView, CompactionInput, CompactionOrigin, CompactionPolicy,
    CompactionResult, CompactionState, CompactionStatus, CompactionUtilityUsage, generate_summary,
};
use crate::config::map_loop_start_error;
use crate::error::AgentError;
use crate::event::{
    AgentEvent, AgentEventSink, CancelReasonView, EventMeta, LoopOutcomeView, ModelErrorView,
    OutputChannel, ToolProgressView, ToolResultView,
};
use crate::history::{GetHistory, HistoryPage, normalize_history, page_history};
use crate::ids::SessionId;
use crate::presentation::SteerReceiptPrompt;
use crate::prompt::ProjectPromptProvider;
use crate::store::{Store, StoredLoopOutcome, StoredLoopRecord, SummaryCommit, utc_timestamp};
use crate::tool_data::ToolData;
use crate::tools::command::ToolOwners;
use crate::tools::observe::ToolObserver;
use crate::workspace::{Workspace, WorkspaceError};

mod active_loop;
mod budget;
mod compact;
mod events;
mod registry;
mod status_workers;
mod types;

use active_loop::ActiveLoop;
#[cfg(test)]
use active_loop::{EphemeralSummaryCleanup, SessionTask};
pub(crate) use active_loop::{
    LoopSubmission, TurnCompletion, await_compaction_completion, await_turn_completion,
};
use budget::history_fits_runtime_limits;
pub(crate) use events::forward_loop_event;
pub(crate) use registry::Sessions;
use status_workers::StatusWorkers;
pub use types::{
    CompactionPhase, CompactionProgress, ContextBudget, LoopAccepted, SessionBlockReason,
    SessionContext, SessionState, SessionStatus, SteerAccepted, SummaryCoverage, TurnPersistence,
    TurnRef, TurnResult,
};

#[cfg(test)]
pub(crate) static PANIC_WORKERS: Registry<SessionId> = Registry::new();
#[cfg(test)]
pub(crate) static PAUSE_BEFORE_JOIN: Registry<SessionId, Arc<Gate>> = Registry::new();
#[cfg(test)]
pub(crate) static PAUSE_AFTER_COMPACTION_RESULT: Registry<SessionId, Arc<Gate>> = Registry::new();
#[cfg(test)]
pub(crate) static PAUSE_AFTER_RUNTIME_START: Registry<SessionId, Arc<Gate>> = Registry::new();
#[cfg(test)]
pub(crate) static INSTALLED_LOOP_OPTIONS: Registry<LoopId, LoopOptions> = Registry::new();

/// A loaded product session. Cheap to clone and shared with the per-loop
/// background task; the mutable product state lives behind the two locks.
#[derive(Clone)]
pub(crate) struct Session {
    shared: Arc<SessionShared>,
}

pub(crate) struct ReadSnapshot {
    pub(crate) info: SessionInfo,
    pub(crate) history: Arc<[HistoryItem]>,
    pub(crate) user_times: Arc<HashMap<(LoopId, usize), String>>,
}

struct SessionShared {
    /// Short critical sections; never holds across an await, I/O, or join.
    inner: Mutex<SessionInner>,
    /// Serializes history appends and session.json updates for one session.
    io: tokio::sync::Mutex<()>,
    /// Cancelled when the loaded Session is closed or dropped. In-flight
    /// workspace queries owned by this Session observe it and stop.
    close: CancellationToken,
    /// Owned `workspace.status` workers; the `close` token stops their children
    /// and this set is joined by the shutdown barrier.
    status_workers: Mutex<StatusWorkers>,
    store: Store,
    events: AgentEventSink,
    compaction: Arc<CompactionState>,
    tool_data: Arc<ToolData>,
    tool_owners: Arc<ToolOwners>,
    tool_observer: Arc<ToolObserver>,
}

impl Drop for SessionShared {
    fn drop(&mut self) {
        self.close.cancel();
    }
}

struct SessionInner {
    record: crate::store::SessionRecord,
    workspace: Arc<Workspace>,
    history: Arc<[HistoryItem]>,
    user_times: Arc<HashMap<(LoopId, usize), String>>,
    presentation: Arc<crate::presentation::Presentation>,
    config: ExecutionConfig,
    /// Automatic-compaction binding for the current execution config. It is
    /// replaced together with `config` so the prompt provider always sees the
    /// matching policy/state; `None` when automatic compaction is disabled.
    auto: Option<AutoContext>,
    options: LoopOptions,
    active: Option<ActiveLoop>,
    blocked: Option<SessionBlockReason>,
    closing: bool,
    compaction: Option<Arc<CompactionOperation>>,
    // Cancellable admission while completed workers are still being reaped.
    pending_compaction: Option<Arc<CompactionOperation>>,
    compaction_progress: Option<CompactionProgress>,
    last_compaction_result: Option<CompactionResult>,
    policy: CompactionPolicy,
    // Operation IDs stay reserved for this loaded Session so stale cancel
    // requests cannot target a later operation with the same identity.
    used_compaction_ids: BTreeSet<String>,
}

/// Frozen tool schemas for the current execution config, derived only from the
/// record's enabled tools and the live `ToolSet`.
fn frozen_tool_specs(
    config: &ExecutionConfig,
    record: &crate::store::SessionRecord,
) -> Vec<ToolSpec> {
    let enabled = record
        .tools
        .iter()
        .filter_map(|name| name.parse::<ToolName>().ok())
        .collect();
    config.tools().specs_for(&enabled)
}

async fn join_task(join: &tokio::sync::Mutex<Option<JoinHandle<()>>>) -> Result<(), AgentError> {
    let mut slot = join.lock().await;
    let Some(handle) = slot.as_mut() else {
        return Ok(());
    };
    let result = std::pin::Pin::new(handle).await;
    slot.take();
    result.map_err(|_| AgentError::Internal)
}

pub(crate) struct CompactionAdmission {
    session: Session,
    operation: Arc<CompactionOperation>,
}

impl Drop for CompactionAdmission {
    fn drop(&mut self) {
        self.session.lock_inner().pending_compaction = None;
    }
}

struct CompactionOperation {
    origin: CompactionOrigin,
    operation_id: String,
    cancellation: CancellationToken,
    result: watch::Sender<Option<CompactionResult>>,
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    state: AtomicU8,
}

const COMPACTION_RUNNING: u8 = 0;
const COMPACTION_CANCELLED: u8 = 1;
const COMPACTION_COMMITTING: u8 = 2;
const COMPACTION_COMPLETED: u8 = 3;
const MAX_USED_COMPACTION_IDS: usize = 4_096;

struct CompactionReservation {
    operation: Arc<CompactionOperation>,
    model: Arc<dyn Model>,
    record: crate::store::SessionRecord,
    history: Arc<[HistoryItem]>,
    workspace: Arc<Workspace>,
    tool_schemas: Vec<ToolSpec>,
    previous_summary: Option<minicore_runtime::value::BoundedText>,
    previous_covered_item_count: usize,
    /// Effective model input ceiling for utility and reduced normal requests.
    hard_tokens: u64,
    /// Only post-turn operations use a soft trigger. Manual compaction is
    /// never gated by this estimate.
    automatic_budget: Option<Arc<dyn crate::models::ProviderBudget>>,
    trigger_tokens: Option<u64>,
    history_triggered: bool,
    /// Set when a turn waits on the operation: its history must fit these.
    fit_limits: Option<minicore_runtime::LoopLimits>,
    deadline: Instant,
}

impl Session {
    fn lock_inner(&self) -> std::sync::MutexGuard<'_, SessionInner> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn new(
        stored: crate::store::StoredSession,
        workspace: Arc<Workspace>,
        setup: crate::agent::SessionSetup,
        policy: CompactionPolicy,
        store: Store,
        events: AgentEventSink,
    ) -> Self {
        let crate::store::StoredSession {
            record,
            history,
            user_times,
            history_index: _,
        } = stored;
        let crate::agent::SessionSetup {
            parts,
            tool_data,
            config,
            auto,
            options,
        } = setup;
        let crate::agent::SessionParts {
            presentation,
            observer: tool_observer,
            tool_owners,
            compaction,
        } = parts;
        compaction.note_settings_installed();
        presentation.install_model(config.model());
        let inner = SessionInner {
            record,
            workspace,
            history,
            user_times: Arc::new(user_times),
            presentation,
            config,
            auto,
            options,
            active: None,
            blocked: None,
            closing: false,
            compaction: None,
            pending_compaction: None,
            compaction_progress: None,
            last_compaction_result: None,
            policy,
            used_compaction_ids: BTreeSet::new(),
        };
        Self {
            shared: Arc::new(SessionShared {
                inner: Mutex::new(inner),
                io: tokio::sync::Mutex::new(()),
                close: CancellationToken::new(),
                status_workers: Mutex::new(StatusWorkers::new()),
                store,
                events,
                compaction,
                tool_data,
                tool_owners,
                tool_observer,
            }),
        }
    }

    pub(crate) fn session_id(&self) -> SessionId {
        let inner = self.lock_inner();
        inner.record.session_id
    }

    pub(crate) fn record(&self) -> crate::store::SessionRecord {
        let inner = self.lock_inner();
        inner.record.clone()
    }

    pub(crate) fn workspace(&self) -> Arc<Workspace> {
        let inner = self.lock_inner();
        Arc::clone(&inner.workspace)
    }

    /// Cancellation observed by read queries that are owned by this loaded
    /// Session's Workspace. It fires when the Session is closed or dropped.
    pub(crate) fn query_cancellation(&self) -> CancellationToken {
        self.shared.close.clone()
    }

    /// Stops in-flight workspace queries owned by this loaded Session.
    pub(crate) fn cancel_queries(&self) {
        self.shared.close.cancel();
    }

    pub(crate) fn compaction_state(&self) -> Arc<CompactionState> {
        Arc::clone(&self.shared.compaction)
    }

    pub(crate) fn policy(&self) -> CompactionPolicy {
        self.lock_inner().policy
    }

    pub(crate) fn info(&self, loaded: bool) -> SessionInfo {
        SessionInfo::from_record(&self.lock_inner().record, loaded)
    }

    pub(crate) fn state(&self) -> SessionState {
        let inner = self.lock_inner();
        let completion = inner
            .active
            .as_ref()
            .and_then(|active| active.completion.borrow().clone());
        let active_loop = inner
            .active
            .as_ref()
            .filter(|_| completion.is_none())
            .map(|active| active.handle.state());
        let status = match (&inner.blocked, &inner.active) {
            (Some(_), _) => SessionStatus::Blocked,
            (None, None) => SessionStatus::Idle,
            (None, Some(active)) => {
                // Agent-level completion published: the turn is done even
                // though the loop task is still being reaped.
                if active.completion.borrow().is_some() {
                    SessionStatus::Idle
                } else {
                    match active.handle.state().status {
                        minicore_runtime::LoopStatus::Starting
                        | minicore_runtime::LoopStatus::RunningModel
                        | minicore_runtime::LoopStatus::RunningTools => SessionStatus::Running,
                        minicore_runtime::LoopStatus::WaitingForInput => {
                            SessionStatus::WaitingForInput
                        }
                        minicore_runtime::LoopStatus::Finishing
                        | minicore_runtime::LoopStatus::Finished => SessionStatus::Finishing,
                        _ => SessionStatus::Unknown,
                    }
                }
            }
        };
        SessionState {
            session_id: inner.record.session_id,
            status,
            active_loop,
            block_reason: inner.blocked,
            compaction: inner.compaction_progress.clone(),
        }
    }

    pub(crate) fn read_snapshot(&self) -> ReadSnapshot {
        let inner = self.lock_inner();
        ReadSnapshot {
            info: SessionInfo::from_record(&inner.record, true),
            history: Arc::clone(&inner.history),
            user_times: Arc::clone(&inner.user_times),
        }
    }

    pub(crate) fn history(&self, request: &GetHistory) -> Result<HistoryPage, AgentError> {
        request.validate()?;
        let inner = self.lock_inner();
        Ok(page_history(
            &inner.history,
            request.offset,
            request.limit,
            &inner.user_times,
        ))
    }

    pub(crate) fn presentation(&self) -> Arc<crate::presentation::Presentation> {
        let inner = self.lock_inner();
        Arc::clone(&inner.presentation)
    }

    pub(crate) fn tool_data(&self) -> Arc<crate::tool_data::ToolData> {
        Arc::clone(&self.shared.tool_data)
    }

    pub(crate) fn tool_owners(&self) -> Arc<ToolOwners> {
        Arc::clone(&self.shared.tool_owners)
    }

    pub(crate) fn parts(&self) -> crate::agent::SessionParts {
        crate::agent::SessionParts {
            presentation: self.presentation(),
            observer: Arc::clone(&self.shared.tool_observer),
            tool_owners: self.tool_owners(),
            compaction: self.compaction_state(),
        }
    }

    pub(crate) fn presentation_view(&self) -> crate::presentation::PresentationView {
        self.presentation().snapshot()
    }

    /// Replaces only the future-turn execution snapshot and system prompt. The
    /// record is persisted with the next turn; an active loop keeps its config.
    pub(crate) fn replace_future_config(
        &self,
        system_prompt: String,
        config: ExecutionConfig,
        auto: Option<AutoContext>,
        policy: CompactionPolicy,
        options: LoopOptions,
    ) {
        let mut inner = self.lock_inner();
        inner.record.system_prompt = system_prompt;
        inner.presentation.install_model(config.model());
        inner.config = config;
        inner.auto = auto;
        inner.policy = policy;
        inner.options = options;
        // A reload is a settings change: bumping the config generation in one
        // step invalidates tickets bound to the previous configuration.
        self.shared.compaction.note_settings_installed();
    }

    /// Persists the new record and swaps the long-lived execution config.
    /// While a loop runs, the update is forwarded through `LoopHandle::update`
    /// and takes effect at the next request boundary.
    pub(crate) async fn update(
        &self,
        record: crate::store::SessionRecord,
        config: ExecutionConfig,
        auto: Option<AutoContext>,
        options: LoopOptions,
    ) -> Result<Option<ConfigRevision>, AgentError> {
        let _io = self.shared.io.lock().await;
        let active_summary = {
            let inner = self.lock_inner();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing
                || inner.pending_compaction.is_some()
                || (inner.active.is_none() && inner.compaction.is_some())
            {
                return Err(AgentError::SessionBusy);
            }
            inner
                .active
                .as_ref()
                .and_then(|active| active.execution_summary.clone())
        };
        let config = self.bind_execution_config(
            config,
            active_summary,
            record.system_prompt.clone(),
            auto.clone(),
        )?;
        self.shared.store.write_record(&record).await?;
        let (handle, update_config) = {
            let mut inner = self.lock_inner();
            inner.presentation.set_model_label(record.model.clone());
            inner.presentation.install_model(config.model());
            inner.record = record;
            let update_config = config.clone();
            inner.config = config;
            inner.auto = auto;
            inner.options = options;
            self.shared.compaction.note_settings_installed();
            (
                inner.active.as_ref().map(|active| active.handle.clone()),
                update_config,
            )
        };
        let Some(handle) = handle else {
            return Ok(None);
        };
        match handle.update(update_config) {
            Ok(revision) => Ok(Some(revision)),
            // The loop sealed while the update was in flight; the session
            // settings still persist for the next turn.
            Err(UpdateError::NotActive) => Ok(None),
            Err(_) => Err(AgentError::Internal),
        }
    }

    /// Persists only the title and timestamp while serializing with history
    /// append and metadata touches for this Session.
    pub(crate) async fn rename(&self, title: Option<String>) -> Result<(), AgentError> {
        let _io = self.shared.io.lock().await;
        let updated_at = utc_timestamp().map_err(|_| AgentError::Store)?;
        let record = {
            let inner = self.lock_inner();
            let mut record = inner.record.clone();
            record.title = title;
            record.updated_at = updated_at;
            record
        };
        self.shared.store.write_record(&record).await?;
        let mut inner = self.lock_inner();
        inner.record = record;
        Ok(())
    }

    pub(crate) fn begin_shutdown(&self) {
        let mut inner = self.lock_inner();
        inner.closing = true;
        self.shared.close.cancel();
        if let Some(active) = &inner.active {
            active.handle.shutdown();
        }
        for operation in inner.compaction.iter().chain(&inner.pending_compaction) {
            operation.cancel();
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        self.begin_shutdown();
        self.join_shutdown().await
    }

    /// Joins all owned work after `begin_shutdown` has stopped new admission.
    pub(crate) async fn join_shutdown(self) -> Result<(), AgentError> {
        let session_id = self.session_id();
        let (active_task, compaction) = {
            let inner = self.lock_inner();
            (
                inner.active.as_ref().map(|active| active.task.clone()),
                inner.compaction.clone(),
            )
        };
        let mut first_error = None;
        if let Some(task) = active_task {
            if task.join().await.is_err() {
                first_error = Some(AgentError::Internal);
            }
            let mut inner = self.lock_inner();
            Self::clear_active_task(&mut inner, &task);
        }
        if let Some(compaction) = compaction {
            if compaction.join().await.is_err() && first_error.is_none() {
                first_error = Some(AgentError::Internal);
            }
            let mut inner = self.lock_inner();
            if inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &compaction))
            {
                inner.compaction = None;
                inner.compaction_progress = None;
            }
        }
        self.join_status_workers().await;
        // A closed Session must reap its own CPU comparisons, not merely cancel
        // the query token. The Store owns those workers, so closing joins them
        // by session before the Session reports closure.
        self.shared
            .store
            .shutdown_session_diff_workers(session_id)
            .await;
        // Owned commands are joined after every owner was told to stop, and
        // before the Session reports closure: a command is only finished when
        // its owner really reaped it.
        self.shared.tool_owners.join_all().await;
        first_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests;
