use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use minicore_runtime::execution::{ConfigRevision, ExecutionConfig, UserInput};
use minicore_runtime::history::HistoryItem;
use minicore_runtime::interaction::InteractionAnswer;
use minicore_runtime::{
    AgentLoop, AnswerError, LoopHandle, LoopOptions, LoopReport, SteerError, UpdateError,
};
use minicore_runtime::{InteractionId, LoopId};

use crate::agent::SessionInfo;
use crate::compaction::CompactionState;
use crate::config::map_loop_start_error;
use crate::error::{AgentError, StoreError};
use crate::event::{
    AgentEvent, AgentEventSink, CancelReasonView, EventMeta, LoopOutcomeView, ModelErrorView,
    OutputChannel, ToolProgressView, ToolResultView,
};
use crate::history::{GetHistory, HistoryPage, page_history, sanitize_history};
use crate::ids::SessionId;
use crate::store::{
    Store, StoredCancelReason, StoredLoopOutcome, StoredLoopRecord, StoredModelError, utc_timestamp,
};
use crate::subagents::SubagentService;
use crate::workspace::Workspace;

#[cfg(test)]
static PANIC_WORKERS: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
type WorkerGateEntry = (SessionId, Arc<WorkerGate>);
#[cfg(test)]
static PAUSE_BEFORE_JOIN: OnceLock<Mutex<Vec<WorkerGateEntry>>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct WorkerGate {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl WorkerGate {
    pub(crate) fn new() -> Self {
        Self {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
pub(crate) fn panic_next_worker(session_id: SessionId) {
    PANIC_WORKERS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(session_id);
}

#[cfg(test)]
fn should_panic_worker(session_id: SessionId) -> bool {
    let mut workers = PANIC_WORKERS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    workers
        .iter()
        .position(|candidate| *candidate == session_id)
        .map(|position| {
            workers.remove(position);
            true
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn pause_next_worker_before_join(session_id: SessionId, gate: Arc<WorkerGate>) {
    PAUSE_BEFORE_JOIN
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, gate));
}

#[cfg(test)]
fn take_pause_before_join(session_id: SessionId) -> Option<Arc<WorkerGate>> {
    let mut gates = PAUSE_BEFORE_JOIN
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|(candidate, _)| *candidate == session_id)
        .map(|position| gates.remove(position).1)
}

/// A user turn maps one-to-one to one runtime `AgentLoop`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnRef {
    pub session_id: SessionId,
    pub loop_id: LoopId,
}

/// Product-level result of one turn. A loop that failed at runtime is still a
/// normal `TurnResult`; only agent-internal failures are `AgentError`s.
#[derive(Clone, Debug)]
pub struct TurnResult {
    pub turn: TurnRef,
    pub report: Arc<LoopReport>,
    pub persistence: TurnPersistence,
}

/// One accepted Prompt: the new turn plus the Agent acceptance time (may be
/// `None` when the clock is unavailable; the TUI shows pending until then).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoopAccepted {
    pub turn: TurnRef,
    pub accepted_at: Option<String>,
}

/// One accepted Steer: the runtime accepted it into the loop queue; the field
/// carries the Agent acceptance time, not the applied/persisted time, and the
/// 1-based FIFO acceptance index within the loop (absent on older semantics
/// where the index is unavailable).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SteerAccepted {
    pub accepted_at: Option<String>,
    pub steer_index: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPersistence {
    Persisted,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    WaitingForInput,
    Finishing,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionBlockReason {
    Persistence,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionState {
    pub session_id: SessionId,
    pub status: SessionStatus,
    pub active_loop: Option<minicore_runtime::LoopState>,
    pub block_reason: Option<SessionBlockReason>,
}

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, Session>,
}

impl Sessions {
    pub(crate) fn new() -> Self {
        Self {
            loaded: HashMap::new(),
        }
    }

    pub(crate) fn contains(&self, session_id: SessionId) -> bool {
        self.loaded.contains_key(&session_id)
    }

    pub(crate) fn get(&self, session_id: SessionId) -> Option<&Session> {
        self.loaded.get(&session_id)
    }

    pub(crate) fn insert(&mut self, session_id: SessionId, session: Session) -> Option<Session> {
        self.loaded.insert(session_id, session)
    }

    pub(crate) fn remove(&mut self, session_id: SessionId) -> Option<Session> {
        self.loaded.remove(&session_id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Session> {
        self.loaded.values()
    }

    pub(crate) async fn shutdown_all(&mut self) -> Result<(), AgentError> {
        let mut ids = self.loaded.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        let mut first_error = None;
        for session_id in ids {
            let Some(session) = self.loaded.remove(&session_id) else {
                continue;
            };
            if let Err(error) = session.shutdown().await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

/// A loaded product session. Cheap to clone and shared with the per-loop
/// background task; the mutable product state lives behind the two locks.
#[derive(Clone)]
pub(crate) struct Session {
    shared: Arc<SessionShared>,
}

struct SessionShared {
    /// Short critical sections; never holds across an await, I/O, or join.
    inner: Mutex<SessionInner>,
    /// Serializes history appends and session.json updates for one session.
    io: tokio::sync::Mutex<()>,
    store: Store,
    events: AgentEventSink,
    subagents: Arc<SubagentService>,
    compaction: Arc<CompactionState>,
}

struct SessionInner {
    record: crate::store::SessionRecord,
    workspace: Arc<Workspace>,
    history: Arc<[HistoryItem]>,
    user_times: std::collections::HashMap<(LoopId, usize), String>,
    presentation: Arc<crate::presentation::Presentation>,
    config: ExecutionConfig,
    options: LoopOptions,
    active: Option<ActiveLoop>,
    blocked: Option<SessionBlockReason>,
}

struct ActiveLoop {
    turn: TurnRef,
    handle: LoopHandle,
    completion: watch::Receiver<Option<TurnCompletion>>,
    task: Option<JoinHandle<()>>,
}

struct CompletionGuard {
    session: Session,
    sender: watch::Sender<Option<TurnCompletion>>,
    armed: bool,
}

impl CompletionGuard {
    fn new(session: Session, sender: watch::Sender<Option<TurnCompletion>>) -> Self {
        Self {
            session,
            sender,
            armed: true,
        }
    }

    fn publish_internal(&mut self) {
        if !self.armed {
            return;
        }
        mark_internal(&self.session);
        self.sender.send_replace(Some(TurnCompletion::Internal));
        self.session.emit_state();
        self.armed = false;
    }

    fn publish_finished(&mut self, result: Arc<TurnResult>) {
        if !self.armed {
            return;
        }
        self.sender
            .send_replace(Some(TurnCompletion::Finished(result)));
        self.armed = false;
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        mark_internal(&self.session);
        self.sender.send_replace(Some(TurnCompletion::Internal));
        self.session.emit_state();
    }
}

#[derive(Clone)]
pub(crate) enum TurnCompletion {
    Finished(Arc<TurnResult>),
    Internal,
}

impl Session {
    // These fields are the Session ownership boundary; keeping construction
    // explicit makes the model/tool/presentation wiring auditable.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        record: crate::store::SessionRecord,
        workspace: Arc<Workspace>,
        history: Arc<[HistoryItem]>,
        user_times: std::collections::HashMap<(LoopId, usize), String>,
        presentation: Arc<crate::presentation::Presentation>,
        config: ExecutionConfig,
        options: LoopOptions,
        subagents: Arc<SubagentService>,
        compaction: Arc<CompactionState>,
        store: Store,
        events: AgentEventSink,
    ) -> Self {
        let inner = SessionInner {
            record,
            workspace,
            history,
            user_times,
            presentation,
            config,
            options,
            active: None,
            blocked: None,
        };
        Self {
            shared: Arc::new(SessionShared {
                inner: Mutex::new(inner),
                io: tokio::sync::Mutex::new(()),
                store,
                events,
                subagents,
                compaction,
            }),
        }
    }

    pub(crate) fn session_id(&self) -> SessionId {
        let inner = self.shared.inner.lock().unwrap();
        inner.record.session_id
    }

    pub(crate) fn record(&self) -> crate::store::SessionRecord {
        let inner = self.shared.inner.lock().unwrap();
        inner.record.clone()
    }

    pub(crate) fn workspace(&self) -> Arc<Workspace> {
        let inner = self.shared.inner.lock().unwrap();
        Arc::clone(&inner.workspace)
    }

    pub(crate) fn compaction_state(&self) -> Arc<CompactionState> {
        Arc::clone(&self.shared.compaction)
    }

    pub(crate) fn info(&self, loaded: bool) -> SessionInfo {
        let inner = self.shared.inner.lock().unwrap();
        SessionInfo {
            session_id: inner.record.session_id,
            title: inner.record.title.clone(),
            profile: inner.record.profile.clone(),
            workspace: inner.record.workspace.clone(),
            model: inner.record.model.clone(),
            reasoning: inner.record.reasoning,
            loaded,
            created_at: inner.record.created_at.clone(),
            updated_at: inner.record.updated_at.clone(),
        }
    }

    pub(crate) fn state(&self) -> SessionState {
        let inner = self.shared.inner.lock().unwrap();
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
                    }
                }
            }
        };
        SessionState {
            session_id: inner.record.session_id,
            status,
            active_loop,
            block_reason: inner.blocked,
        }
    }

    /// Starts a new `AgentLoop` for one user message. A session runs at most
    /// one active loop; no queueing or auto-cancellation.
    pub(crate) async fn start_loop(&self, input: UserInput) -> Result<LoopAccepted, AgentError> {
        self.cleanup_finished().await?;
        let (history, config, options) = {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.active.is_some() {
                return Err(AgentError::SessionBusy);
            }
            (
                Arc::clone(&inner.history),
                inner.config.clone(),
                inner.options.clone(),
            )
        };
        let request = minicore_runtime::LoopRequest::new(history, input, config);
        let mut agent_loop = AgentLoop::start(request, options).map_err(map_loop_start_error)?;
        let handle = agent_loop.handle();
        let turn = TurnRef {
            session_id: self.session_id(),
            loop_id: handle.id(),
        };
        let events = agent_loop.take_events().map_err(|_| AgentError::Internal)?;
        let (completion_tx, completion_rx) = watch::channel(None);
        // Mark the new current loop before spawning its worker. Otherwise a
        // very fast model/tool could populate the presentation cache and then
        // lose its identity when `note_loop_started` clears stale state.
        let accepted_at = crate::store::utc_timestamp().ok();
        {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.active.is_some() {
                return Err(AgentError::SessionBusy);
            }
            inner.active = Some(ActiveLoop {
                turn,
                handle: handle.clone(),
                completion: completion_rx,
                task: None,
            });
            inner
                .presentation
                .note_loop_started(turn.loop_id, accepted_at.clone());
            inner.presentation.record_prompt_time(accepted_at.clone());
        }
        let task = tokio::spawn(run_active_loop(
            self.clone(),
            turn,
            agent_loop,
            events,
            CompletionGuard::new(self.clone(), completion_tx),
        ));
        {
            let mut inner = self.shared.inner.lock().unwrap();
            match inner.active.as_mut() {
                Some(active) if active.turn == turn => active.task = Some(task),
                _ => {
                    task.abort();
                    return Err(AgentError::Internal);
                }
            }
        }
        tracing::info!(
            session_id = %turn.session_id,
            loop_id = %turn.loop_id,
            "turn submitted"
        );
        Ok(LoopAccepted { turn, accepted_at })
    }

    pub(crate) async fn cleanup_finished(&self) -> Result<(), AgentError> {
        let (finished, completed) = {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            match inner.active.as_ref() {
                None => return Ok(()),
                Some(active) => (
                    active.task.as_ref().is_some_and(|task| task.is_finished()),
                    active.completion.borrow().is_some(),
                ),
            }
        };
        // Agent-level completion is published and the runtime loop has joined;
        // the worker is only finishing its best-effort event emits. Reaping it
        // now (instead of reporting busy) keeps an immediate next send from
        // observing a SessionBusy window after `turn.wait` resolved.
        if !finished && !completed {
            return Err(AgentError::SessionBusy);
        }
        let task = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            inner.active.as_mut().and_then(|active| active.task.take())
        };
        if let Some(task) = task {
            if task.await.is_err() {
                let mut inner = self.shared.inner.lock().unwrap();
                inner.active = None;
                inner.blocked = Some(SessionBlockReason::Internal);
                drop(inner);
                self.emit_state();
                return Err(AgentError::SessionBlocked);
            }
        }
        let mut inner = self.shared.inner.lock().unwrap();
        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }
        inner.active = None;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn abort_active_task(&self) -> Result<(), AgentError> {
        let inner = self.shared.inner.lock().unwrap();
        let task = inner
            .active
            .as_ref()
            .and_then(|active| active.task.as_ref())
            .ok_or(AgentError::TurnNotFound)?;
        task.abort();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn runtime_loop_finished(&self) -> bool {
        let inner = self.shared.inner.lock().unwrap();
        inner
            .active
            .as_ref()
            .is_some_and(|active| active.handle.is_finished())
    }

    /// Agent-level completion: the runtime loop has joined and the report has
    /// been persisted before this resolves.
    pub(crate) fn wait_receiver(
        &self,
        turn: TurnRef,
    ) -> Result<watch::Receiver<Option<TurnCompletion>>, AgentError> {
        let inner = self.shared.inner.lock().unwrap();
        inner
            .active
            .as_ref()
            .filter(|active| active.turn == turn)
            .map(|active| active.completion.clone())
            .ok_or(AgentError::TurnNotFound)
    }

    pub(crate) async fn wait(&self, turn: TurnRef) -> Result<Arc<TurnResult>, AgentError> {
        let receiver = self.wait_receiver(turn)?;
        await_turn_completion(receiver).await
    }

    pub(crate) fn steer(&self, turn: TurnRef, text: String) -> Result<SteerAccepted, AgentError> {
        let input = UserInput::text(text).map_err(|_| AgentError::InvalidInput)?;
        let inner = self.shared.inner.lock().unwrap();
        let active = inner.active.as_ref().ok_or(AgentError::TurnNotFound)?;
        if active.turn != turn {
            return Err(AgentError::TurnNotFound);
        }
        active.handle.steer(input).map_err(map_steer_error)?;
        let accepted_at = crate::store::utc_timestamp().ok();
        let steer_index = inner.presentation.note_steer_accepted(accepted_at.clone());
        Ok(SteerAccepted {
            accepted_at,
            steer_index,
        })
    }

    pub(crate) fn cancel(&self, turn: TurnRef) -> Result<bool, AgentError> {
        let inner = self.shared.inner.lock().unwrap();
        let active = inner.active.as_ref().ok_or(AgentError::TurnNotFound)?;
        if active.turn != turn {
            return Err(AgentError::TurnNotFound);
        }
        if active.handle.is_finished() {
            return Ok(false);
        }
        Ok(active.handle.cancel())
    }

    pub(crate) fn answer(
        &self,
        turn: TurnRef,
        interaction_id: InteractionId,
        answer: InteractionAnswer,
    ) -> Result<(), AgentError> {
        let inner = self.shared.inner.lock().unwrap();
        let active = inner.active.as_ref().ok_or(AgentError::TurnNotFound)?;
        if active.turn != turn {
            return Err(AgentError::TurnNotFound);
        }
        active
            .handle
            .answer(interaction_id, answer)
            .map_err(map_answer_error)
    }

    pub(crate) fn history(&self, request: &GetHistory) -> Result<HistoryPage, AgentError> {
        request.validate()?;
        let inner = self.shared.inner.lock().unwrap();
        Ok(page_history(
            &inner.history,
            request.offset,
            request.limit,
            &inner.user_times,
        ))
    }

    pub(crate) fn presentation(&self) -> Arc<crate::presentation::Presentation> {
        let inner = self.shared.inner.lock().unwrap();
        Arc::clone(&inner.presentation)
    }

    pub(crate) fn presentation_view(&self) -> crate::presentation::PresentationView {
        self.presentation().snapshot()
    }

    /// Replaces only the future-turn execution snapshot. This never persists
    /// Session metadata and never forwards a config update to an active loop.
    pub(crate) fn replace_future_config(&self, config: ExecutionConfig, options: LoopOptions) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.config = config;
        inner.options = options;
    }

    /// Persists the new record and swaps the long-lived execution config.
    /// While a loop runs, the update is forwarded through `LoopHandle::update`
    /// and takes effect at the next request boundary.
    pub(crate) async fn update(
        &self,
        record: crate::store::SessionRecord,
        config: ExecutionConfig,
    ) -> Result<Option<ConfigRevision>, AgentError> {
        let _io = self.shared.io.lock().await;
        {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
        }
        self.shared
            .store
            .write_record(&record)
            .await
            .map_err(map_store_error)?;
        let (handle, update_config) = {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.record = record;
            let update_config = config.clone();
            inner.config = config;
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
            Err(UpdateError::InvalidConfig) => Err(AgentError::Internal),
            Err(_) => Err(AgentError::Internal),
        }
    }

    /// Persists only the title and timestamp while serializing with history
    /// append and metadata touches for this Session.
    pub(crate) async fn rename(&self, title: Option<String>) -> Result<(), AgentError> {
        let _io = self.shared.io.lock().await;
        let updated_at = utc_timestamp().map_err(|_| AgentError::Store)?;
        let record = {
            let inner = self.shared.inner.lock().unwrap();
            let mut record = inner.record.clone();
            record.title = title;
            record.updated_at = updated_at;
            record
        };
        self.shared
            .store
            .write_record(&record)
            .await
            .map_err(map_store_error)?;
        let mut inner = self.shared.inner.lock().unwrap();
        inner.record = record;
        Ok(())
    }

    /// Cancels the active loop (if any), awaits its Agent-owned task, and
    /// drains any child workers owned by this Session.
    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        let session_id = self.session_id();
        let active = {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.active.take()
        };
        let task_result = if let Some(active) = active {
            active.handle.cancel();
            match active.task {
                Some(task) => task.await.map_err(|_| AgentError::Internal),
                None => Ok(()),
            }
        } else {
            Ok(())
        };
        self.shared.subagents.drain_session(session_id).await;
        task_result
    }

    fn emit_state(&self) {
        let state = self.state();
        let loop_id = state
            .active_loop
            .as_ref()
            .map(|loop_state| loop_state.loop_id);
        self.shared.events.try_send(AgentEvent::SessionState {
            meta: EventMeta {
                session_id: state.session_id,
                loop_id,
                dropped_before: 0,
            },
            state,
        });
    }
}

/// The single Agent-owned task per active loop: forwards runtime events, joins
/// the loop, persists the report, merges sanitized history, and publishes the
/// Agent-level completion.
async fn run_active_loop(
    session: Session,
    turn: TurnRef,
    agent_loop: AgentLoop,
    mut events: minicore_runtime::LoopEventStream,
    mut completion: CompletionGuard,
) {
    #[cfg(test)]
    if should_panic_worker(turn.session_id) {
        panic!("injected Agent worker panic");
    }
    #[cfg(test)]
    if let Some(gate) = take_pause_before_join(turn.session_id) {
        gate.started.notify_one();
        gate.release.notified().await;
    }
    let join = agent_loop.join();
    tokio::pin!(join);
    let mut events_open = true;
    let mut tool_batch_dirty = false;
    let result = loop {
        tokio::select! {
            biased;
            result = &mut join => break result,
            envelope = events.recv(), if events_open => {
                match envelope {
                    Some(envelope) => {
                        forward_loop_event_and_refresh(
                            turn.session_id,
                            envelope,
                            &session,
                            &mut tool_batch_dirty,
                        )
                        .await;
                    }
                    None => events_open = false,
                }
            }
        }
    };
    // Runtime join means the producer is finished. Drain only envelopes that
    // are already queued; waiting for channel closure here would add an
    // unnecessary completion dependency. This also accounts for a queued
    // `Finished` envelope without mapping it to Agent `TurnFinished`.
    while let Ok(envelope) = events.try_recv() {
        forward_loop_event_and_refresh(turn.session_id, envelope, &session, &mut tool_batch_dirty)
            .await;
    }
    if tool_batch_dirty {
        refresh_branch(&session).await;
    }
    session
        .shared
        .subagents
        .drain_session(turn.session_id)
        .await;
    let report = match result {
        Ok(report) => {
            if let minicore_runtime::LoopOutcome::Failed(failure) = &report.outcome {
                tracing::warn!(
                    session_id = %turn.session_id,
                    loop_id = %turn.loop_id,
                    error_kind = failure.model_error().map(|error| format!("{:?}", error.kind())),
                    diagnostic_code = ?failure.diagnostic.code,
                    "loop failed"
                );
            }
            report
        }
        Err(_) => {
            completion.publish_internal();
            return;
        }
    };

    let sanitized = match sanitize_history(report.appended.as_ref()) {
        Ok(items) => items,
        Err(_) => {
            completion.publish_internal();
            return;
        }
    };
    // The runtime loop is complete regardless of whether the later JSONL
    // append succeeds. Keep live footer state honest on both outcomes.
    refresh_branch(&session).await;
    session.presentation().note_loop_finished();
    let user_item_count = sanitized
        .iter()
        .filter(|item| matches!(item, HistoryItem::User(_)))
        .count();
    let user_times = session.presentation().peek_user_times(user_item_count);
    let stored = StoredLoopRecord {
        loop_id: report.loop_id,
        outcome: StoredLoopOutcome::from_report(&report),
        items: sanitized.to_vec(),
        usage: report.usage,
        requests: report.requests,
        tool_rounds: report.tool_rounds,
        final_config_revision: report.final_config_revision,
        completed_at: utc_timestamp().unwrap_or_default(),
        user_times: (!user_times.is_empty()).then_some(user_times),
    };

    let persistence = {
        // The same per-session IO lock serializes this metadata touch with
        // rename and settings updates; each write starts from current record state.
        let _io = session.shared.io.lock().await;
        match session
            .shared
            .store
            .append_loop(turn.session_id, &stored)
            .await
        {
            Ok(()) => {
                {
                    let mut inner = session.shared.inner.lock().unwrap();
                    // Persisted user timestamps align by (loop_id, occurrence).
                    let mut occurrence = 0usize;
                    for item in sanitized.iter() {
                        if let HistoryItem::User(_) = item {
                            if let Some(Some(time)) = stored
                                .user_times
                                .as_ref()
                                .and_then(|times| times.get(occurrence))
                            {
                                inner
                                    .user_times
                                    .insert((report.loop_id, occurrence), time.clone());
                            }
                            occurrence += 1;
                        }
                    }
                    let mut merged = Vec::with_capacity(inner.history.len() + sanitized.len());
                    merged.extend(inner.history.iter().cloned());
                    merged.extend(sanitized.iter().cloned());
                    inner.history = merged.into();
                    inner.blocked = None;
                }
                session.presentation().clear_user_times();
                touch_updated_at_best_effort(&session).await;
                TurnPersistence::Persisted
            }
            Err(error) => {
                tracing::warn!(
                    session_id = %turn.session_id,
                    loop_id = %turn.loop_id,
                    error_kind = error.kind(),
                    "history.jsonl append failed"
                );
                let mut inner = session.shared.inner.lock().unwrap();
                inner.blocked = Some(SessionBlockReason::Persistence);
                drop(inner);
                TurnPersistence::Failed
            }
        }
    };

    let result = Arc::new(TurnResult {
        turn,
        report: Arc::clone(&report),
        persistence,
    });

    // Authoritative completion precedes best-effort events: `turn.wait` may
    // resolve before the `TurnFinished` event is observed.
    completion.publish_finished(Arc::clone(&result));

    session.shared.events.try_send(AgentEvent::TurnFinished {
        turn,
        outcome: LoopOutcomeView::from_report(&report),
        persistence,
        meta: EventMeta {
            session_id: turn.session_id,
            loop_id: Some(turn.loop_id),
            dropped_before: 0,
        },
    });
    session.emit_state();
}

fn mark_internal(session: &Session) {
    let mut inner = session
        .shared
        .inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    inner.blocked = Some(SessionBlockReason::Internal);
}

async fn touch_updated_at_best_effort(session: &Session) {
    let Ok(updated_at) = utc_timestamp() else {
        return;
    };
    let record = {
        let mut inner = session.shared.inner.lock().unwrap();
        let mut record = inner.record.clone();
        record.updated_at = updated_at.clone();
        inner.record = record.clone();
        record
    };
    if session.shared.store.write_record(&record).await.is_err() {
        tracing::warn!(
            session_id = %record.session_id,
            "session.json updated_at write failed"
        );
    }
}

pub(crate) fn forward_loop_event(
    session_id: SessionId,
    envelope: minicore_runtime::LoopEventEnvelope,
    session: &Session,
) {
    session
        .shared
        .events
        .record_core_drops(envelope.dropped_before);
    if let Some(event) = map_loop_event(session_id, envelope.event, session) {
        session.shared.events.try_send(event);
    }
}

async fn forward_loop_event_and_refresh(
    session_id: SessionId,
    envelope: minicore_runtime::LoopEventEnvelope,
    session: &Session,
    tool_batch_dirty: &mut bool,
) {
    let refresh_before_next_request = *tool_batch_dirty
        && matches!(
            &envelope.event,
            minicore_runtime::LoopEvent::RequestStarted { .. }
        );
    let tool_finished = matches!(
        &envelope.event,
        minicore_runtime::LoopEvent::ToolFinished { .. }
    );
    forward_loop_event(session_id, envelope, session);
    if tool_finished {
        *tool_batch_dirty = true;
    }
    if refresh_before_next_request {
        refresh_branch(session).await;
        *tool_batch_dirty = false;
    }
}

async fn refresh_branch(session: &Session) {
    let branch = session.workspace().git_branch().await;
    session.presentation().set_branch(branch);
}

fn extract_loop_id(event: &minicore_runtime::LoopEvent) -> Option<LoopId> {
    match event {
        minicore_runtime::LoopEvent::Started { loop_id }
        | minicore_runtime::LoopEvent::RequestStarted { loop_id, .. }
        | minicore_runtime::LoopEvent::OutputDelta { loop_id, .. }
        | minicore_runtime::LoopEvent::ToolStarted { loop_id, .. }
        | minicore_runtime::LoopEvent::ToolProgress { loop_id, .. }
        | minicore_runtime::LoopEvent::ToolFinished { loop_id, .. }
        | minicore_runtime::LoopEvent::InteractionRequested { loop_id, .. }
        | minicore_runtime::LoopEvent::InteractionResolved { loop_id, .. }
        | minicore_runtime::LoopEvent::Finished { loop_id, .. } => Some(*loop_id),
        minicore_runtime::LoopEvent::StateChanged { state } => Some(state.loop_id),
        _ => None,
    }
}

fn map_loop_event(
    session_id: SessionId,
    event: minicore_runtime::LoopEvent,
    session: &Session,
) -> Option<AgentEvent> {
    let loop_id = extract_loop_id(&event)?;
    let turn = TurnRef {
        session_id,
        loop_id,
    };
    let meta = EventMeta {
        session_id,
        loop_id: Some(loop_id),
        dropped_before: 0,
    };
    match event {
        minicore_runtime::LoopEvent::Started { .. } => Some(AgentEvent::TurnStarted { turn, meta }),
        minicore_runtime::LoopEvent::StateChanged { .. } => Some(AgentEvent::SessionState {
            state: session.state(),
            meta,
        }),
        minicore_runtime::LoopEvent::RequestStarted {
            config_revision,
            model,
            reasoning,
            request_index,
            ..
        } => Some(AgentEvent::RequestStarted {
            turn,
            request_index,
            config_revision,
            model: model.as_str().to_owned(),
            reasoning,
            meta,
        }),
        minicore_runtime::LoopEvent::OutputDelta {
            request_index,
            channel,
            delta,
            ..
        } => Some(AgentEvent::OutputDelta {
            turn,
            request_index,
            channel: OutputChannel::from_runtime(channel),
            delta: delta.as_str().to_owned(),
            meta,
        }),
        minicore_runtime::LoopEvent::ToolStarted {
            request_index,
            call_id,
            tool_name,
            ..
        } => Some(AgentEvent::ToolStarted {
            turn,
            request_index,
            tool_call_id: call_id,
            tool_name: tool_name.to_string(),
            meta,
        }),
        minicore_runtime::LoopEvent::ToolProgress {
            request_index,
            call_id,
            progress,
            ..
        } => Some(AgentEvent::ToolProgress {
            turn,
            request_index,
            tool_call_id: call_id,
            progress: ToolProgressView::from(&progress),
            meta,
        }),
        minicore_runtime::LoopEvent::ToolFinished {
            request_index,
            call_id,
            outcome,
            output_bytes,
            ..
        } => {
            // The wrapper normally finishes before Runtime emits this event;
            // the lookup remains best-effort because the event and presentation
            // channels have independent delivery/drop semantics.
            let presentation_result = session.presentation().tool_result(
                crate::presentation::RequestKey {
                    loop_id,
                    request_index,
                },
                &call_id,
            );
            Some(AgentEvent::ToolFinished {
                turn,
                request_index,
                tool_call_id: call_id,
                result: ToolResultView {
                    outcome,
                    content_bytes: output_bytes,
                    content: presentation_result
                        .as_ref()
                        .map(|(content, _)| content.clone()),
                    content_truncated: presentation_result.is_some_and(|(_, truncated)| truncated),
                },
                meta,
            })
        }
        minicore_runtime::LoopEvent::InteractionRequested { interaction, .. } => {
            Some(AgentEvent::InteractionRequested {
                turn,
                interaction: (&interaction).into(),
                meta,
            })
        }
        minicore_runtime::LoopEvent::InteractionResolved { interaction_id, .. } => {
            Some(AgentEvent::InteractionResolved {
                turn,
                interaction_id,
                meta,
            })
        }
        // Runtime Finished is not an Agent TurnFinished: JSONL persistence and
        // history merge have not happened yet. Its drops were already recorded.
        minicore_runtime::LoopEvent::Finished { .. } => None,
        _ => None,
    }
}

impl LoopOutcomeView {
    pub(crate) fn from_report(report: &LoopReport) -> Self {
        match &report.outcome {
            minicore_runtime::LoopOutcome::Completed => Self::Completed,
            minicore_runtime::LoopOutcome::Cancelled(reason) => Self::Cancelled {
                reason: match reason {
                    minicore_runtime::CancelReason::User => CancelReasonView::User,
                    minicore_runtime::CancelReason::OwnerDropped => CancelReasonView::OwnerDropped,
                    minicore_runtime::CancelReason::Shutdown => CancelReasonView::Shutdown,
                    minicore_runtime::CancelReason::Deadline => CancelReasonView::Deadline,
                },
            },
            minicore_runtime::LoopOutcome::Failed(failure) => Self::Failed {
                kind: loop_failure_kind(&failure.kind).to_owned(),
                model_error: failure.model_error().map(ModelErrorView::from_model),
            },
        }
    }
}

fn loop_failure_kind(kind: &minicore_runtime::LoopFailureKind) -> &'static str {
    match kind {
        minicore_runtime::LoopFailureKind::Prompt => "prompt",
        minicore_runtime::LoopFailureKind::Model => "model",
        minicore_runtime::LoopFailureKind::InvalidModelResponse => "invalid_model_response",
        minicore_runtime::LoopFailureKind::OutputLimit => "output_limit",
        minicore_runtime::LoopFailureKind::Refused => "refused",
        minicore_runtime::LoopFailureKind::ContentFiltered => "content_filtered",
        minicore_runtime::LoopFailureKind::Policy => "policy",
        minicore_runtime::LoopFailureKind::Interaction => "interaction",
        minicore_runtime::LoopFailureKind::MaxToolRounds => "max_tool_rounds",
        minicore_runtime::LoopFailureKind::Internal => "internal",
        _ => "internal",
    }
}

impl StoredLoopOutcome {
    pub(crate) fn from_report(report: &LoopReport) -> Self {
        match &report.outcome {
            minicore_runtime::LoopOutcome::Completed => Self::Completed,
            minicore_runtime::LoopOutcome::Cancelled(reason) => Self::Cancelled {
                reason: match reason {
                    minicore_runtime::CancelReason::User => StoredCancelReason::User,
                    minicore_runtime::CancelReason::OwnerDropped => {
                        StoredCancelReason::OwnerDropped
                    }
                    minicore_runtime::CancelReason::Shutdown => StoredCancelReason::Shutdown,
                    minicore_runtime::CancelReason::Deadline => StoredCancelReason::Deadline,
                },
            },
            minicore_runtime::LoopOutcome::Failed(failure) => Self::Failed {
                kind: loop_failure_kind(&failure.kind).to_owned(),
                model_error: failure.model_error().map(StoredModelError::from_model),
            },
        }
    }
}

/// Awaits an Agent-level turn completion. Used by `Session::wait` and the RPC
/// deferred waiter; never waits on a raw runtime `LoopHandle`.
pub(crate) async fn await_turn_completion(
    mut receiver: watch::Receiver<Option<TurnCompletion>>,
) -> Result<Arc<TurnResult>, AgentError> {
    loop {
        {
            let value = receiver.borrow();
            if let Some(completion) = value.as_ref() {
                return match completion {
                    TurnCompletion::Finished(result) => Ok(Arc::clone(result)),
                    TurnCompletion::Internal => Err(AgentError::Internal),
                };
            }
        }
        if receiver.changed().await.is_err() {
            return Err(AgentError::Internal);
        }
    }
}

pub(crate) fn map_store_error(error: StoreError) -> AgentError {
    match error {
        StoreError::SessionNotFound => AgentError::SessionNotFound,
        StoreError::SessionAlreadyExists
        | StoreError::InvalidRoot
        | StoreError::InvalidRecord
        | StoreError::UnsupportedFormat
        | StoreError::Corrupt
        | StoreError::RecordTooLarge
        | StoreError::Unavailable => AgentError::Store,
    }
}

fn map_steer_error(error: SteerError) -> AgentError {
    match error {
        SteerError::QueueFull => AgentError::SteerQueueFull,
        SteerError::WaitingForInput => AgentError::InvalidState,
        SteerError::NotActive => AgentError::TurnNotFound,
        SteerError::InvalidInput => AgentError::InvalidInput,
        _ => AgentError::Internal,
    }
}

fn map_answer_error(error: AnswerError) -> AgentError {
    match error {
        AnswerError::InteractionNotFound => AgentError::InteractionNotFound,
        AnswerError::WrongInteraction | AnswerError::NotActive => AgentError::InvalidInteraction,
        _ => AgentError::InvalidInteraction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{AgentEvent, AgentEventSink, EventMeta, LoopOutcomeView};
    use crate::ids::SessionId;

    #[tokio::test]
    async fn ignored_finished_drops_accumulate_into_subsequent_agent_event() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let sink = AgentEventSink::new(sender);
        let turn = TurnRef {
            session_id: SessionId::new().unwrap(),
            loop_id: LoopId::new().unwrap(),
        };
        // The runner drains the loop event stream to close after `join`, so
        // the ignored runtime `Finished` envelope still records its cumulative
        // drop count (here 7) on the Agent sink.
        sink.record_core_drops(7);
        sink.try_send(AgentEvent::TurnFinished {
            turn,
            outcome: LoopOutcomeView::Completed,
            persistence: TurnPersistence::Persisted,
            meta: EventMeta {
                session_id: turn.session_id,
                loop_id: Some(turn.loop_id),
                dropped_before: 0,
            },
        });
        let received = receiver
            .recv()
            .await
            .expect("agent event must be delivered");
        let AgentEvent::TurnFinished { meta, .. } = received else {
            panic!("expected a turn finished event");
        };
        assert_eq!(meta.dropped_before, 7);
    }
}
