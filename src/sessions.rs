use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(test)]
use std::sync::{Condvar, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use minicore_runtime::execution::{ConfigRevision, ExecutionConfig, UserInput};
use minicore_runtime::history::HistoryItem;
use minicore_runtime::interaction::InteractionAnswer;
use minicore_runtime::model::{AssistantPart, Model, ModelDescriptor};
use minicore_runtime::tools::{ToolName, ToolSpec};
use minicore_runtime::value::BoundedText;
use minicore_runtime::{
    AgentLoop, AnswerError, LoopHandle, LoopOptions, LoopReport, LoopRequest, SteerError,
    UpdateError,
};
use minicore_runtime::{InteractionId, LoopId};

use crate::agent::SessionInfo;
use crate::compaction::{
    AutoContext, AutomaticCompactionObservation, AutomaticCompactionView, CompactionInput,
    CompactionPolicy, CompactionResult, CompactionState, CompactionStatus, CompactionUtilityUsage,
    generate_summary,
};
use crate::config::map_loop_start_error;
use crate::error::{AgentError, StoreError};
use crate::event::{
    AgentEvent, AgentEventSink, CancelReasonView, EventMeta, LoopOutcomeView, ModelErrorView,
    OutputChannel, ToolProgressView, ToolResultView,
};
use crate::history::{GetHistory, HistoryPage, page_history, sanitize_history};
use crate::ids::SessionId;
use crate::presentation::SteerReceiptPrompt;
use crate::prompt::ProjectPromptProvider;
use crate::store::{
    Store, StoredCancelReason, StoredLoopOutcome, StoredLoopRecord, StoredModelError,
    SummaryCommit, utc_timestamp,
};
use crate::tool_data::ToolData;
use crate::tools::command::CommandOwners;
use crate::tools::observe::ToolObserver;
use crate::workspace::{Workspace, WorkspaceError};

mod admission;
mod compact;

#[cfg(test)]
static PANIC_WORKERS: OnceLock<Mutex<Vec<SessionId>>> = OnceLock::new();
#[cfg(test)]
type WorkerGateEntry = (SessionId, Arc<WorkerGate>);
#[cfg(test)]
static PAUSE_BEFORE_JOIN: OnceLock<Mutex<Vec<WorkerGateEntry>>> = OnceLock::new();
#[cfg(test)]
static PAUSE_AFTER_COMPACTION_RESULT: OnceLock<Mutex<Vec<WorkerGateEntry>>> = OnceLock::new();
#[cfg(test)]
static PAUSE_AFTER_ADMISSION_RESULT: OnceLock<Mutex<Vec<WorkerGateEntry>>> = OnceLock::new();
#[cfg(test)]
type RuntimeStartGateEntry = (SessionId, Arc<RuntimeStartGate>);
#[cfg(test)]
static PAUSE_AFTER_RUNTIME_START: OnceLock<Mutex<Vec<RuntimeStartGateEntry>>> = OnceLock::new();

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
pub(crate) struct RuntimeStartGate {
    started: tokio::sync::Notify,
    released: Mutex<bool>,
    release: Condvar,
}

#[cfg(test)]
impl RuntimeStartGate {
    pub(crate) fn new() -> Self {
        Self {
            started: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            release: Condvar::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_one();
    }

    pub(crate) fn enter_and_wait(&self) {
        self.started.notify_one();
        self.wait_release();
    }

    fn wait_release(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.release.wait(released).unwrap();
        }
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
pub(crate) fn pause_next_runtime_start_before_bind(
    session_id: SessionId,
    gate: Arc<RuntimeStartGate>,
) {
    PAUSE_AFTER_RUNTIME_START
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, gate));
}

#[cfg(test)]
fn take_pause_after_runtime_start(session_id: SessionId) -> Option<Arc<RuntimeStartGate>> {
    let mut gates = PAUSE_AFTER_RUNTIME_START
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|(candidate, _)| *candidate == session_id)
        .map(|position| gates.remove(position).1)
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

#[cfg(test)]
pub(crate) fn pause_next_compaction_after_result(session_id: SessionId, gate: Arc<WorkerGate>) {
    PAUSE_AFTER_COMPACTION_RESULT
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, gate));
}

#[cfg(test)]
pub(crate) fn pause_next_admission_after_result(session_id: SessionId, gate: Arc<WorkerGate>) {
    PAUSE_AFTER_ADMISSION_RESULT
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, gate));
}

#[cfg(test)]
fn take_pause_after_compaction_result(session_id: SessionId) -> Option<Arc<WorkerGate>> {
    let mut gates = PAUSE_AFTER_COMPACTION_RESULT
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|(candidate, _)| *candidate == session_id)
        .map(|position| gates.remove(position).1)
}

#[cfg(test)]
fn take_pause_after_admission_result(session_id: SessionId) -> Option<Arc<WorkerGate>> {
    let mut gates = PAUSE_AFTER_ADMISSION_RESULT
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPhase {
    Preparing,
    Summarizing,
    Merging,
    Committing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompactionProgress {
    pub operation_id: String,
    pub phase: CompactionPhase,
    pub covered_item_count: usize,
    pub retained_item_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SummaryCoverage {
    pub covered_loop_count: u64,
    pub covered_item_count: usize,
    pub retained_item_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ContextBudget {
    /// Exact item count of the history passed to Runtime as `LoopRequest.history`.
    /// It excludes the summary, system/AGENTS text, tool schemas, and current
    /// User/Steer input.
    pub estimated_history_items: usize,
    /// Bounded estimate of that Runtime history input (the projected suffix,
    /// or full history without a valid summary). `None` means the context
    /// query stopped at its scan budget.
    pub estimated_history_bytes: Option<usize>,
    /// `estimated_history_bytes / 4`; this is not a full request-context token
    /// estimate and does not represent provider-reported usage.
    pub estimated_history_tokens: Option<u64>,
    /// The latest full prepared-request estimate observed by automatic
    /// preparation. While a request is preparing this is its current estimate;
    /// while idle it is the last completed estimate.
    pub estimated_request_context_tokens: Option<u64>,
    /// The model's effective input budget after its own output allowance and
    /// safety margin were already subtracted. `None` when automatic compaction
    /// is disabled or no model descriptor is bound.
    pub input_budget_tokens: Option<u64>,
    /// Automatic-compaction trigger and target thresholds derived from the
    /// effective input budget and the active `[compaction]` policy.
    pub trigger_tokens: Option<u64>,
    pub target_tokens: Option<u64>,
    pub max_history_items: usize,
    pub max_history_bytes: usize,
    /// `None` means the byte estimate was not available; it never guesses that
    /// an unscanned history is within the Runtime limits.
    pub within_runtime_limits: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionContext {
    pub session_id: SessionId,
    pub current_operation: Option<CompactionProgress>,
    pub coverage: SummaryCoverage,
    pub last_result: Option<CompactionResult>,
    pub budget: ContextBudget,
    /// Bounded process-local observations for the current and last automatic
    /// preparation operation. Summary bodies are intentionally not exposed.
    pub automatic: AutomaticCompactionView,
    /// The most recent request-preparation failure kind observed in this
    /// process, e.g. `context_uncompressible`. Cleared by a successful
    /// preparation. It is observation, never durable session state.
    pub last_prepare_failure: Option<String>,
    /// Bounded observation of provider context overflow recovery, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<crate::compaction::RecoveryObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionState {
    pub session_id: SessionId,
    pub status: SessionStatus,
    pub active_loop: Option<minicore_runtime::LoopState>,
    pub block_reason: Option<SessionBlockReason>,
    pub compaction: Option<CompactionProgress>,
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
        let session = self.loaded.remove(&session_id)?;
        // Workspace queries are owned by the loaded Session; closing it stops
        // them instead of reading a Workspace whose Session is gone.
        session.cancel_queries();
        Some(session)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Session> {
        self.loaded.values()
    }

    pub(crate) async fn shutdown_all(&mut self) -> Result<(), AgentError> {
        let mut ids = self.loaded.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        let mut first_error = None;
        for session_id in ids {
            let Some(session) = self.loaded.get(&session_id).cloned() else {
                continue;
            };
            if let Err(error) = session.shutdown().await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            // Keep the Session in the map until its shutdown barrier has
            // joined every owned worker, including manual compaction.
            self.remove(session_id);
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for Sessions {
    fn drop(&mut self) {
        // Ordinary Agent drop is intentionally non-blocking. Manual and
        // startup-admission workers still receive cancellation through their
        // Session-owned token; the async shutdown path above is the complete
        // join barrier.
        for session in self.loaded.values() {
            session.cancel_active_on_drop();
            session.cancel_compaction_on_drop();
            session.cancel_queries();
        }
    }
}

/// A loaded product session. Cheap to clone and shared with the per-loop
/// background task; the mutable product state lives behind the two locks.
#[derive(Clone)]
pub(crate) struct Session {
    shared: Arc<SessionShared>,
}

pub(crate) struct ReadSnapshot {
    pub(crate) info: SessionInfo,
    pub(crate) history: Arc<[HistoryItem]>,
    pub(crate) user_times: HashMap<(LoopId, usize), String>,
}

/// Owned `workspace.status` workers run at most this many at a time.
pub(crate) const MAX_STATUS_QUERY_WORKERS: usize = 4;

/// The Session owns `workspace.status` worker tasks. Each worker keeps its git
/// child until the child is stopped and reaped, so closing the Session joins
/// this set instead of relying on the child's drop behaviour.
struct StatusWorkers {
    /// Set once the Session starts closing: no new worker may register.
    closing: bool,
    workers: Vec<JoinHandle<()>>,
}

impl StatusWorkers {
    fn new() -> Self {
        Self {
            closing: false,
            workers: Vec::new(),
        }
    }

    /// Drops the handles of workers that already finished and reaped.
    fn reap_finished(&mut self) {
        self.workers.retain(|handle| !handle.is_finished());
    }
}

/// One owned `workspace.status` query. Awaiting it observes the worker's
/// result only; dropping it stops that query's child through the child token,
/// never through the Session token, so other queries keep running.
pub(crate) struct StatusQuery<T = crate::WorkspaceStatusResult> {
    receiver: tokio::sync::oneshot::Receiver<Result<T, AgentError>>,
    child_cancel: CancellationToken,
}

impl<T> StatusQuery<T> {
    pub(crate) async fn wait(mut self) -> Result<T, AgentError> {
        match (&mut self.receiver).await {
            Ok(result) => result,
            // The worker ended without publishing a result, so nothing was
            // observed; the failure stays inside the query boundary.
            Err(_) => Err(AgentError::Internal),
        }
    }
}

impl<T> Drop for StatusQuery<T> {
    fn drop(&mut self) {
        self.child_cancel.cancel();
    }
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
    command_owners: Arc<CommandOwners>,
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
    user_times: std::collections::HashMap<(LoopId, usize), String>,
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
    compaction_progress: Option<CompactionProgress>,
    admission: Option<Arc<AdmissionOperation>>,
    last_compaction_result: Option<CompactionResult>,
    policy: CompactionPolicy,
    next_admission_id: u64,
    // Operation IDs stay reserved for this loaded Session so stale cancel
    // requests cannot target a later operation with the same identity.
    used_compaction_ids: BTreeSet<String>,
}

struct ActiveLoop {
    turn: TurnRef,
    handle: LoopHandle,
    completion: watch::Receiver<Option<TurnCompletion>>,
    task: Option<Arc<SessionTask>>,
    execution_summary: Option<BoundedText>,
}

pub(crate) struct ExecutionInput {
    pub(crate) request: LoopRequest,
    pub(crate) options: LoopOptions,
    pub(crate) summary: Option<BoundedText>,
}

/// One accepted submission: either a live loop, or an admission preparation
/// that still owes its loop. The deferred waiter observes the preparation.
pub(crate) enum LoopSubmission {
    Accepted(LoopAccepted),
    Preparing(PreparationWaiter),
}

/// Deferred submission receiver. Dropping it cancels a still-running startup
/// preparation, so an embedded caller that abandons `Agent::send` cannot leave
/// a summary worker running without an owner waiting for its result.
pub(crate) struct PreparationWaiter {
    session: Session,
    operation_id: String,
    receiver: watch::Receiver<Option<PreparedLoop>>,
}

impl PreparationWaiter {
    pub(crate) fn operation_id(&self) -> &str {
        &self.operation_id
    }
}

impl Drop for PreparationWaiter {
    fn drop(&mut self) {
        let _ = self.session.cancel_compaction(&self.operation_id);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparationFailure {
    Cancelled,
    ContextUncompressible,
    HistoryTooLarge,
    SessionBlocked,
    SessionBusy,
    Compaction(&'static str),
    Internal,
}

/// Result of one startup admission preparation: the loop it finally created,
/// or the honest failure kind that prevented it. It never fabricates a LoopId
/// before the summary work succeeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedLoop {
    Started(LoopAccepted),
    Failed(PreparationFailure),
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

/// Owns one Agent worker without taking its JoinHandle into a local across an
/// await. If a join future is cancelled, the handle remains in this object so
/// a later close or cleanup call can join the same worker.
struct SessionTask {
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl SessionTask {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            join: tokio::sync::Mutex::new(Some(handle)),
        }
    }

    async fn join(&self) -> Result<(), AgentError> {
        let mut slot = self.join.lock().await;
        let Some(handle) = slot.as_mut() else {
            return Ok(());
        };
        let result = std::pin::Pin::new(handle).await;
        slot.take();
        result.map_err(|_| AgentError::Internal)
    }

    #[cfg(test)]
    fn abort(&self) {
        if let Ok(slot) = self.join.try_lock() {
            if let Some(handle) = slot.as_ref() {
                handle.abort();
            }
        }
    }
}

struct CompactionOperation {
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
    /// Effective input token target for this operation. Automatic admission
    /// uses the active policy; explicit manual compaction keeps its original
    /// half-window target.
    target_tokens: u64,
    /// Effective model input ceiling for utility requests. Unlike the target,
    /// this is a hard boundary for deciding whether a result is usable.
    hard_tokens: u64,
    /// Startup admission must avoid constructing an invalid full request before
    /// the utility replaces over-limit history; manual compaction keeps exact
    /// before-request accounting.
    safe_before_estimate: bool,
    /// Automatic admission may refresh an existing durable summary when a new
    /// budget makes that summary too large. Manual compaction retains its
    /// established no-op behavior once every history item is covered.
    refresh_summary: bool,
    deadline: Instant,
}

/// Publishes and owns the outcome of one admission preparation. The wait
/// receiver is created before work starts so no completion can be lost.
struct AdmissionOperation {
    result: watch::Sender<Option<PreparedLoop>>,
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    operation: Arc<CompactionOperation>,
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

struct EphemeralSummaryCleanup {
    state: Arc<CompactionState>,
    loop_id: LoopId,
}

impl Drop for EphemeralSummaryCleanup {
    fn drop(&mut self) {
        self.state.clear_ephemeral(self.loop_id);
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
        auto: Option<AutoContext>,
        options: LoopOptions,
        compaction: Arc<CompactionState>,
        policy: CompactionPolicy,
        store: Store,
        events: AgentEventSink,
        tool_data: Arc<ToolData>,
        command_owners: Arc<CommandOwners>,
        tool_observer: Arc<ToolObserver>,
    ) -> Self {
        compaction.note_settings_installed();
        let inner = SessionInner {
            record,
            workspace,
            history,
            user_times,
            presentation,
            config,
            auto,
            options,
            active: None,
            blocked: None,
            closing: false,
            compaction: None,
            compaction_progress: None,
            admission: None,
            last_compaction_result: None,
            policy,
            next_admission_id: 0,
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
                command_owners,
                tool_observer,
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

    #[cfg(test)]
    pub(crate) fn options_for_test(&self) -> LoopOptions {
        let inner = self.shared.inner.lock().unwrap();
        inner.options.clone()
    }

    pub(crate) fn workspace(&self) -> Arc<Workspace> {
        let inner = self.shared.inner.lock().unwrap();
        Arc::clone(&inner.workspace)
    }

    /// Cancellation observed by read queries that are owned by this loaded
    /// Session's Workspace. It fires when the Session is closed or dropped.
    pub(crate) fn query_cancellation(&self) -> CancellationToken {
        self.shared.close.clone()
    }

    /// Completes an explicit `workspace.status` observation. Branch is a
    /// projection of the complete status result, never a separate Git query;
    /// an unavailable or incomplete observation clears the cache. The Session
    /// lock makes close and projection linearizable, while the caller token
    /// prevents a cancelled request from publishing a late result.
    pub(crate) fn complete_status_query(
        &self,
        result: &crate::WorkspaceStatusResult,
        cancellation: &CancellationToken,
    ) {
        let inner = self.shared.inner.lock().unwrap();
        if inner.closing || self.shared.close.is_cancelled() || cancellation.is_cancelled() {
            return;
        }
        let branch = result
            .complete
            .then(|| {
                result
                    .repo_available
                    .then(|| result.branch.clone())
                    .flatten()
            })
            .flatten();
        inner.presentation.set_branch(branch);
    }

    /// Starts one owned `workspace.status` query. Registration and the capacity
    /// check happen under the worker lock, so a Session that starts closing
    /// either observes this worker or refuses it here. The worker captures only
    /// the workspace, the request, the observed tokens, and the result sender;
    /// it never holds the Session.
    pub(crate) fn spawn_status_query(
        &self,
        workspace: Arc<Workspace>,
        request: crate::WorkspaceStatusRequest,
        shutdown_cancellation: CancellationToken,
    ) -> Result<StatusQuery, AgentError> {
        let deadline = crate::workspace::status::status_deadline_at(workspace.root());
        self.spawn_status_query_with_deadline(workspace, request, shutdown_cancellation, deadline)
    }

    pub(crate) fn spawn_status_query_with_deadline(
        &self,
        workspace: Arc<Workspace>,
        request: crate::WorkspaceStatusRequest,
        shutdown_cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<StatusQuery, AgentError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let child_cancel = CancellationToken::new();
        let session_cancellation = self.shared.close.clone();
        let worker_child_cancel = child_cancel.clone();
        let mut workers = self.shared.status_workers.lock().unwrap();
        workers.reap_finished();
        // The token is checked under the same lock as the closing flag, so a
        // registration cannot slip between the flag and the cancellation.
        if workers.closing
            || session_cancellation.is_cancelled()
            || workers.workers.len() >= MAX_STATUS_QUERY_WORKERS
        {
            return Err(AgentError::QueryLimit);
        }
        let handle = tokio::spawn(async move {
            let result = crate::workspace::status::status_with_deadline(
                &workspace,
                &request,
                &session_cancellation,
                &shutdown_cancellation,
                &worker_child_cancel,
                deadline,
            )
            .await;
            let _ = sender.send(result);
        });
        workers.workers.push(handle);
        Ok(StatusQuery {
            receiver,
            child_cancel,
        })
    }

    /// Registers bounded Git source reads in the existing Session-owned worker set.
    pub(crate) fn spawn_workspace_diff(
        &self,
        request: crate::ChangesDiffRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<StatusQuery<crate::diff::DiffSources>, AgentError> {
        request.validate()?;
        let workspace = self.workspace();
        let session_cancel = self.shared.close.clone();
        let child_cancel = CancellationToken::new();
        let worker_cancel = child_cancel.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut workers = self.shared.status_workers.lock().unwrap();
        workers.reap_finished();
        if workers.closing
            || session_cancel.is_cancelled()
            || workers.workers.len() >= MAX_STATUS_QUERY_WORKERS
        {
            return Err(AgentError::QueryLimit);
        }
        workers.workers.push(tokio::spawn(async move {
            let result = crate::workspace::status::diff_sources(
                &workspace,
                &request,
                &session_cancel,
                &cancellation,
                &worker_cancel,
                deadline,
            )
            .await;
            let _ = sender.send(result);
        }));
        Ok(StatusQuery {
            receiver,
            child_cancel,
        })
    }

    /// Refuses new status workers and waits until each has reaped its child.
    async fn join_status_workers(&self) {
        let workers = {
            let mut workers = self.shared.status_workers.lock().unwrap();
            workers.closing = true;
            std::mem::take(&mut workers.workers)
        };
        for handle in workers {
            let _ = handle.await;
        }
    }

    /// Test-only evidence that no owned status worker is still running, used by
    /// the process-level Unix tests.
    #[cfg(all(test, unix))]
    pub(crate) fn active_status_workers(&self) -> usize {
        self.shared
            .status_workers
            .lock()
            .unwrap()
            .workers
            .iter()
            .filter(|handle| !handle.is_finished())
            .count()
    }

    /// Stops in-flight workspace queries owned by this loaded Session.
    pub(crate) fn cancel_queries(&self) {
        self.shared.close.cancel();
    }

    pub(crate) fn compaction_state(&self) -> Arc<CompactionState> {
        Arc::clone(&self.shared.compaction)
    }

    pub(crate) fn policy(&self) -> CompactionPolicy {
        self.shared.inner.lock().unwrap().policy
    }

    pub(crate) fn automatic_compaction_enabled(&self) -> bool {
        self.shared.inner.lock().unwrap().auto.is_some()
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
            compaction: inner.compaction_progress.clone(),
        }
    }

    pub(crate) fn context(&self) -> SessionContext {
        let inner = self.shared.inner.lock().unwrap();
        let (covered_loop_count, covered_item_count) =
            self.shared.compaction.coverage().unwrap_or((0, 0));
        let retained_item_count = inner.history.len().saturating_sub(covered_item_count);
        let projection = self.shared.compaction.project(&inner.history);
        let projected_history = projection
            .as_ref()
            .map_or(&inner.history[..], |projection| projection.suffix);
        let estimated_history_items = projected_history.len();
        let estimated_history_bytes =
            estimate_history_bytes_for_context(projected_history, &inner.options.limits);
        let within_runtime_limits =
            if estimated_history_items > inner.options.limits.max_history_items {
                Some(false)
            } else {
                estimated_history_bytes.map(|bytes| bytes <= inner.options.limits.max_history_bytes)
            };
        let automatic = self.shared.compaction.automatic_view();
        let estimated_request_context_tokens = self.shared.compaction.latest_request_tokens();
        let (input_budget_tokens, trigger_tokens, target_tokens) =
            match (&inner.auto, inner.config.descriptor().context_window) {
                (Some(auto), window) => {
                    let budget = auto.policy.budget(window);
                    (
                        Some(window),
                        Some(budget.trigger_tokens),
                        Some(budget.target_tokens),
                    )
                }
                _ => (None, None, None),
            };
        SessionContext {
            session_id: inner.record.session_id,
            current_operation: inner.compaction_progress.clone(),
            coverage: SummaryCoverage {
                covered_loop_count,
                covered_item_count,
                retained_item_count,
            },
            last_result: inner.last_compaction_result.clone(),
            last_prepare_failure: self.shared.compaction.prepare_failure(),
            budget: ContextBudget {
                estimated_history_items,
                estimated_history_tokens: estimated_history_bytes.map(bytes_to_tokens),
                estimated_history_bytes,
                estimated_request_context_tokens,
                input_budget_tokens,
                trigger_tokens,
                target_tokens,
                max_history_items: inner.options.limits.max_history_items,
                max_history_bytes: inner.options.limits.max_history_bytes,
                within_runtime_limits,
            },
            automatic,
            recovery: self.shared.compaction.recovery_observation(),
        }
    }

    fn bind_execution_config(
        &self,
        config: ExecutionConfig,
        summary: Option<BoundedText>,
        system_prompt: String,
        auto: Option<AutoContext>,
    ) -> Result<ExecutionConfig, AgentError> {
        if summary.is_none() && auto.is_none() {
            return Ok(config);
        }
        let workspace = self.workspace();
        let prompt = match summary {
            Some(summary) => ProjectPromptProvider::new_bound(
                workspace,
                system_prompt,
                summary,
                auto,
                Arc::clone(&self.shared.compaction),
            ),
            None => ProjectPromptProvider::with_auto(
                workspace,
                system_prompt,
                Arc::clone(&self.shared.compaction),
                auto,
            ),
        }
        .map_err(|_| AgentError::InvalidSessionSettings)?;
        let prompt = SteerReceiptPrompt::new(Arc::new(prompt), self.presentation());
        ExecutionConfig::new(
            Arc::clone(config.model()),
            config.reasoning(),
            config.tools().clone(),
            config.policy().cloned(),
            prompt,
        )
        .map_err(|_| AgentError::InvalidSessionSettings)
    }

    pub(crate) fn execution_input(&self, input: UserInput) -> Result<ExecutionInput, AgentError> {
        self.build_execution_input(input, false)
    }

    /// Builds the startup projection for an admission worker that already owns
    /// the Session's compaction slot; that ownership replaces the usual busy
    /// rejection.
    fn build_execution_input(
        &self,
        input: UserInput,
        owned_admission: bool,
    ) -> Result<ExecutionInput, AgentError> {
        let (history, config, options, system_prompt, auto) = {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            let busy = if owned_admission {
                inner.closing || inner.active.is_some()
            } else {
                inner.closing || inner.active.is_some() || inner.compaction.is_some()
            };
            if busy {
                return Err(AgentError::SessionBusy);
            }
            (
                Arc::clone(&inner.history),
                inner.config.clone(),
                inner.options.clone(),
                inner.record.system_prompt.clone(),
                inner.auto.clone(),
            )
        };
        let (history, summary) = match self.shared.compaction.project(&history) {
            Some(projection) => (projection.suffix.to_vec().into(), Some(projection.summary)),
            None => (history, None),
        };
        if !history_fits_runtime_limits(&history, &options.limits) {
            return Err(AgentError::HistoryTooLarge);
        }
        let config = self.bind_execution_config(config, summary.clone(), system_prompt, auto)?;
        Ok(ExecutionInput {
            request: LoopRequest::new(history, input, config),
            options,
            summary,
        })
    }

    /// Starts a new `AgentLoop` for one user message. A session runs at most
    /// one active loop; no queueing or auto-cancellation.
    pub(crate) async fn start_loop(&self, input: UserInput) -> Result<LoopAccepted, AgentError> {
        self.cleanup_finished().await?;
        let execution = self.execution_input(input)?;
        self.install_loop(execution, None)
    }

    /// Admits one user submission. Automatic-enabled sessions first use a
    /// Session-owned preparation to perform the bounded startup decision; the
    /// worker either starts the loop directly or creates a compliant summary
    /// before any `AgentLoop` exists. The caller receives a real `TurnRef` only
    /// after that decision succeeds.
    pub(crate) async fn submit(&self, input: UserInput) -> Result<LoopSubmission, AgentError> {
        self.cleanup_finished().await?;
        let automatic = {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            inner.auto.is_some()
        };
        if !automatic {
            return Ok(LoopSubmission::Accepted(self.start_loop(input).await?));
        }
        // Reserve the Session slot before any workspace read or model work.
        // The admission worker makes the eventual decision, which keeps the
        // RPC reader responsive even when AGENTS.md is slow to read.
        let waiter = self.start_admission(input).await?;
        Ok(LoopSubmission::Preparing(waiter))
    }

    /// Installs a successfully created loop. When `admission` is given, the
    /// admission is re-validated before the loop starts and its terminal state
    /// plus the active loop are published under one lock, so an admission can
    /// never be observed as both busy and started.
    fn install_loop(
        &self,
        execution: ExecutionInput,
        admission: Option<&Arc<AdmissionOperation>>,
    ) -> Result<LoopAccepted, AgentError> {
        let accepted_at = crate::store::utc_timestamp().ok();
        let turn = {
            let mut inner = self.shared.inner.lock().unwrap();
            Self::validate_install_state(&inner, admission)?;
            // Initialize current-loop observation before spawning the Runtime
            // task: the runner may reach Model::start synchronously.
            inner.presentation.reset_before_loop_start();
            self.shared.tool_observer.reset_before_loop_start();
            // AgentLoop::start only validates and spawns the Runtime task; it
            // does not await model work. Keeping this short lock held closes
            // the cancellation/close race between validation and ownership.
            let mut agent_loop = AgentLoop::start(execution.request, execution.options)
                .map_err(map_loop_start_error)?;
            #[cfg(test)]
            let runtime_start_gate = take_pause_after_runtime_start(inner.record.session_id);
            #[cfg(test)]
            if let Some(gate) = runtime_start_gate {
                // The Runtime may need the Session lock to reach Model::start.
                // Release it only while the deterministic test gate waits;
                // production startup keeps the original ownership boundary.
                drop(inner);
                tokio::task::block_in_place(|| gate.enter_and_wait());
                inner = self.shared.inner.lock().unwrap();
            }
            let handle = agent_loop.handle();
            let turn = TurnRef {
                session_id: inner.record.session_id,
                loop_id: handle.id(),
            };
            let events = agent_loop.take_events().map_err(|_| AgentError::Internal)?;
            let (completion_tx, completion_rx) = watch::channel(None);
            if let Some(admission) = admission {
                inner.compaction_progress = None;
                admission.operation.finish();
                // Commit the admission observation before the loop task can
                // begin a request-time observation. This preserves startup
                // utility usage when the first request itself merely fits.
                self.shared.compaction.finish_automatic(
                    &admission.operation.operation_id,
                    "started",
                    None,
                    None,
                );
            }
            inner.active = Some(ActiveLoop {
                turn,
                handle: handle.clone(),
                completion: completion_rx,
                task: None,
                execution_summary: execution.summary,
            });
            inner
                .presentation
                .bind_started_loop(turn.loop_id, accepted_at.clone());
            inner.presentation.record_prompt_time(accepted_at.clone());
            let task = Arc::new(SessionTask::new(tokio::spawn(run_active_loop(
                self.clone(),
                turn,
                agent_loop,
                events,
                CompletionGuard::new(self.clone(), completion_tx),
            ))));
            inner
                .active
                .as_mut()
                .expect("newly inserted active loop")
                .task = Some(task);
            turn
        };
        tracing::info!(
            session_id = %turn.session_id,
            loop_id = %turn.loop_id,
            "turn submitted"
        );
        Ok(LoopAccepted { turn, accepted_at })
    }

    /// Pre-flight for `install_loop`: fails before any `AgentLoop` is created
    /// when the session is blocked, closing, already active, or the admission
    /// was cancelled or replaced.
    fn validate_install_state(
        inner: &SessionInner,
        admission: Option<&Arc<AdmissionOperation>>,
    ) -> Result<(), AgentError> {
        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }
        if inner.closing || inner.active.is_some() {
            return Err(AgentError::SessionBusy);
        }
        match admission {
            Some(admission) => {
                let current = inner
                    .admission
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, admission));
                if !current {
                    return Err(AgentError::SessionBusy);
                }
                if admission.operation.cancellation_requested() {
                    // A preparation cancelled after its summary finished must
                    // not install the loop.
                    return Err(AgentError::InvalidState);
                }
                if !inner
                    .compaction
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission.operation))
                {
                    return Err(AgentError::SessionBusy);
                }
            }
            None => {
                if inner.compaction.is_some() || inner.admission.is_some() {
                    return Err(AgentError::SessionBusy);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn cleanup_finished(&self) -> Result<(), AgentError> {
        let active_task = {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            inner.active.as_ref().and_then(|active| {
                active
                    .completion
                    .borrow()
                    .is_some()
                    .then(|| active.task.clone())
                    .flatten()
            })
        };
        if let Some(task) = active_task {
            if task.join().await.is_err() {
                let mut inner = self.shared.inner.lock().unwrap();
                if inner
                    .active
                    .as_ref()
                    .and_then(|active| active.task.as_ref())
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, &task))
                {
                    inner.active = None;
                }
                inner.blocked = Some(SessionBlockReason::Internal);
                drop(inner);
                self.emit_state();
                return Err(AgentError::SessionBlocked);
            }
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner
                .active
                .as_ref()
                .and_then(|active| active.task.as_ref())
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &task))
            {
                inner.active = None;
            }
        }
        if self.reap_finished_admission().await? {
            return Ok(());
        }
        if self.reap_finished_compaction().await? {
            return Ok(());
        }
        Err(AgentError::SessionBusy)
    }

    fn cancel_active_on_drop(&self) {
        let handle = {
            let inner = self.shared.inner.lock().unwrap();
            inner.active.as_ref().map(|active| active.handle.clone())
        };
        if let Some(handle) = handle {
            let _ = handle.cancel();
        }
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

    pub(crate) fn read_snapshot(&self) -> ReadSnapshot {
        let inner = self.shared.inner.lock().unwrap();
        ReadSnapshot {
            info: SessionInfo::from_record(&inner.record, true),
            history: Arc::clone(&inner.history),
            user_times: inner.user_times.clone(),
        }
    }

    pub(crate) fn turn_result_snapshot(
        &self,
        turn: TurnRef,
    ) -> Result<Option<Option<Arc<TurnResult>>>, AgentError> {
        let inner = self.shared.inner.lock().unwrap();
        let Some(active) = inner.active.as_ref().filter(|active| active.turn == turn) else {
            return Ok(None);
        };
        match active.completion.borrow().as_ref() {
            None => Ok(Some(None)),
            Some(TurnCompletion::Finished(result)) => Ok(Some(Some(Arc::clone(result)))),
            Some(TurnCompletion::Internal) => Err(AgentError::Internal),
        }
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

    pub(crate) fn tool_data(&self) -> Arc<crate::tool_data::ToolData> {
        Arc::clone(&self.shared.tool_data)
    }

    pub(crate) fn command_owners(&self) -> Arc<CommandOwners> {
        Arc::clone(&self.shared.command_owners)
    }

    pub(crate) fn tool_observer(&self) -> Arc<ToolObserver> {
        Arc::clone(&self.shared.tool_observer)
    }

    pub(crate) fn presentation_view(&self) -> crate::presentation::PresentationView {
        self.presentation().snapshot()
    }

    /// Replaces only the future-turn execution snapshot. This never persists
    /// Session metadata and never forwards a config update to an active loop.
    pub(crate) fn replace_future_config(
        &self,
        config: ExecutionConfig,
        auto: Option<AutoContext>,
        policy: CompactionPolicy,
        options: LoopOptions,
    ) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.config = config;
        inner.auto = auto.clone();
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
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing
                || (inner.active.is_none()
                    && (inner.compaction.is_some() || inner.admission.is_some()))
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
            inner.auto = auto.clone();
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
        // Every Session-owned query observes this token, so it is cancelled here
        // with the other owners; the workers are joined further down, after
        // every owner has been told to stop, so slow status cleanup can never
        // delay cancelling the active loop, admission, or compaction.
        self.shared.close.cancel();
        let (active_handle, active_task, compaction, admission) = {
            let mut inner = self.shared.inner.lock().unwrap();
            let active_handle = inner.active.as_ref().map(|active| active.handle.clone());
            let active_task = inner.active.as_ref().and_then(|active| active.task.clone());
            let compaction = inner.compaction.clone();
            let admission = inner.admission.clone();
            inner.closing = true;
            (active_handle, active_task, compaction, admission)
        };
        let mut first_error = None;
        if let Some(handle) = active_handle {
            let _ = handle.cancel();
        }
        if let Some(task) = active_task {
            if task.join().await.is_err() {
                first_error = Some(AgentError::Internal);
            }
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .active
                .as_ref()
                .and_then(|active| active.task.as_ref())
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &task))
            {
                inner.active = None;
            }
        }
        if let Some(admission) = admission {
            let _ = admission.operation.cancel();
            if admission.join().await.is_err() && first_error.is_none() {
                first_error = Some(AgentError::Internal);
            }
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .admission
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission))
            {
                inner.admission = None;
            }
        }
        if let Some(compaction) = compaction {
            let _ = compaction.cancel();
            if compaction.join().await.is_err() && first_error.is_none() {
                first_error = Some(AgentError::Internal);
            }
            let mut inner = self.shared.inner.lock().unwrap();
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
        self.shared.command_owners.join_all().await;
        first_error.map_or(Ok(()), Err)
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

fn history_fits_runtime_limits(
    history: &[HistoryItem],
    limits: &minicore_runtime::LoopLimits,
) -> bool {
    history.len() <= limits.max_history_items
        && estimate_history_bytes(history) <= limits.max_history_bytes
}

/// Mirrors `minicore_runtime::history::estimate_history_bytes` at the pinned
/// Runtime revision `6cd2bdbc634437dea925495c61c7eb0be10ba171`. This narrow
/// copy is only for the pre-start `LoopRequest.history` admission check; it is
/// not a full prompt or provider-context estimate. The over-item and over-byte
/// startup tests are the contract boundary for keeping this copy aligned.
pub(crate) fn estimate_history_bytes(history: &[HistoryItem]) -> usize {
    history.iter().map(estimate_history_item_bytes).sum()
}

/// Produces a bounded context-query estimate. The item count is checked before
/// scanning, and byte accumulation stops once the Runtime byte limit is known
/// to be exceeded, so `session.context` never scans an unbounded history.
fn estimate_history_bytes_for_context(
    history: &[HistoryItem],
    limits: &minicore_runtime::LoopLimits,
) -> Option<usize> {
    if history.len() > limits.max_history_items {
        return None;
    }
    let mut total = 0usize;
    for item in history {
        total = total.saturating_add(estimate_history_item_bytes(item));
        if total > limits.max_history_bytes {
            return Some(total);
        }
    }
    Some(total)
}

fn estimate_history_item_bytes(item: &HistoryItem) -> usize {
    match item {
        HistoryItem::User(user) => user.input.as_text().len(),
        HistoryItem::Assistant(assistant) => assistant
            .content
            .iter()
            .map(estimate_assistant_part_bytes)
            .sum(),
        HistoryItem::ToolResult(result) => {
            result.call_id.as_str().len()
                + result.tool_name.as_str().len()
                + result.output.content().as_str().len()
        }
        HistoryItem::Summary(summary) => summary.content.as_str().len(),
    }
}

fn estimate_assistant_part_bytes(part: &AssistantPart) -> usize {
    match part {
        AssistantPart::Text(text) => text.len(),
        AssistantPart::Reasoning(reasoning) => reasoning
            .text()
            .map_or(0, str::len)
            .saturating_add(reasoning.summary().map_or(0, str::len))
            .saturating_add(reasoning.encrypted().map_or(0, str::len))
            .saturating_add(reasoning.signature().map_or(0, str::len)),
        AssistantPart::ToolCall(call) => {
            call.name().as_str().len() + call.arguments().to_string().len()
        }
    }
}

fn bytes_to_tokens(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX).div_ceil(4)
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
    session.shared.compaction.begin_ephemeral_loop(turn.loop_id);
    let _ephemeral_cleanup = EphemeralSummaryCleanup {
        state: Arc::clone(&session.shared.compaction),
        loop_id: turn.loop_id,
    };
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
    let result = loop {
        tokio::select! {
            biased;
            result = &mut join => break result,
            envelope = events.recv(), if events_open => {
                match envelope {
                    Some(envelope) => forward_loop_event(turn.session_id, envelope, &session),
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
        forward_loop_event(turn.session_id, envelope, &session);
    }
    // Owned commands of this loop are joined before the report is reconciled
    // and persisted: a dropped tool future must not leave a process behind, and
    // the process record must be final before the terminal state is written.
    session.shared.command_owners.join_loop(turn.loop_id).await;
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
    // append succeeds.
    session.presentation().note_loop_finished();
    // The joined report is authoritative: reconcile terminal tool state even
    // when the wrapper future was dropped by an outer Runtime deadline/cancel
    // or its best-effort events were lost.
    session
        .shared
        .tool_data
        .reconcile(turn.session_id, &sanitized);
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

    // Best-effort auxiliary tool persistence: save native tool facts and
    // streams to the store's auxiliary directory before publishing turn completion.
    // Failure is logged as a warning; it does not change TurnResult persistence
    // or ToolOutcome, does not retry the loop, and does not touch history.jsonl.
    persist_loop_tool_records(&session, turn.session_id, turn.loop_id).await;

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

async fn persist_loop_tool_records(session: &Session, session_id: SessionId, loop_id: LoopId) {
    let tool_refs = session.shared.tool_data.loop_tool_refs(session_id, loop_id);
    if tool_refs.is_empty() {
        return;
    }
    let deadline = Instant::now() + crate::store::AUX_PERSIST_DEADLINE;
    let results = session
        .shared
        .store
        .persist_loop_tool_records(&tool_refs, session.shared.tool_data.as_ref(), deadline)
        .await;

    for (tool_ref, outcome) in results {
        let turn = TurnRef {
            session_id: tool_ref.session_id,
            loop_id: tool_ref.loop_id,
        };
        match outcome {
            Ok(()) => {
                session.shared.tool_observer.note_tool_recording(
                    &tool_ref,
                    crate::tool_data::ToolRecordingState::Saved,
                    turn,
                );
            }
            Err(error) => {
                tracing::warn!(
                    session_id = %tool_ref.session_id,
                    loop_id = %tool_ref.loop_id,
                    error_kind = error.kind(),
                    "auxiliary tool record persistence failed"
                );
                session.shared.tool_observer.note_tool_recording(
                    &tool_ref,
                    crate::tool_data::ToolRecordingState::Failed,
                    turn,
                );
            }
        }
    }
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
    // The terminal structured facts are read from the same ToolRef-keyed data
    // source as `tool.read`, before the envelope is consumed below.
    let execution = tool_execution_event(session_id, &envelope.event, session);
    if let Some(event) = map_loop_event(session_id, envelope.event, session) {
        session.shared.events.try_send(event);
    }
    if let Some(event) = execution {
        session.shared.events.try_send(event);
    }
}

/// Best-effort `tool_execution` event built from the same structured record
/// `tool.read` returns, so live and query clients cannot disagree.
fn tool_execution_event(
    session_id: SessionId,
    event: &minicore_runtime::LoopEvent,
    session: &Session,
) -> Option<AgentEvent> {
    let minicore_runtime::LoopEvent::ToolFinished {
        loop_id,
        request_index,
        call_id,
        outcome,
        ..
    } = event
    else {
        return None;
    };
    let tool_ref = crate::tool_data::ToolRef {
        session_id,
        loop_id: *loop_id,
        request_index: *request_index,
        tool_call_id: call_id.clone(),
    };
    // Record the authoritative terminal state before reading it back, so the
    // event never reports a stale non-terminal state.
    let data = session
        .shared
        .tool_data
        .finish_and_snapshot(&tool_ref, *outcome)?;
    Some(AgentEvent::ToolExecution {
        turn: TurnRef {
            session_id,
            loop_id: *loop_id,
        },
        data,
        meta: EventMeta {
            session_id,
            loop_id: Some(*loop_id),
            dropped_before: 0,
        },
    })
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
        } => {
            // Runtime accepted the call; arguments are not available yet.
            session.shared.tool_data.note_requested(
                &crate::tool_data::ToolRef {
                    session_id,
                    loop_id,
                    request_index,
                    tool_call_id: call_id.clone(),
                },
                tool_name.as_str(),
            );
            Some(AgentEvent::ToolStarted {
                turn,
                request_index,
                tool_call_id: call_id,
                tool_name: tool_name.to_string(),
                meta,
            })
        }
        minicore_runtime::LoopEvent::ToolProgress {
            request_index,
            call_id,
            progress,
            ..
        } => {
            // A real `ToolContext.progress` phase is the only thing recorded
            // here; raw stream bytes never travel through progress messages.
            if let Some(message) = progress.message.as_ref() {
                session.shared.tool_data.note_phase(
                    &crate::tool_data::ToolRef {
                        session_id,
                        loop_id,
                        request_index,
                        tool_call_id: call_id.clone(),
                    },
                    message.as_str(),
                );
            }
            Some(AgentEvent::ToolProgress {
                turn,
                request_index,
                tool_call_id: call_id,
                progress: ToolProgressView::from(&progress),
                meta,
            })
        }
        minicore_runtime::LoopEvent::ToolFinished {
            request_index,
            call_id,
            outcome,
            output_bytes,
            ..
        } => {
            // The wrapper normally finishes before Runtime emits this event;
            // the lookup remains best-effort because the event and presentation
            // channels have independent delivery/drop semantics. Terminal
            // structured facts are recorded once by `tool_execution_event`.
            let presentation_result = session.presentation().tool_result(
                crate::tools::observe::RequestKey {
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
            // `awaiting_policy` is recorded by the policy wrapper from the
            // exact ToolRef; the best-effort interaction event is not used to
            // guess which call is waiting.
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

pub(crate) async fn await_compaction_completion(
    mut receiver: watch::Receiver<Option<CompactionResult>>,
) -> Result<CompactionResult, AgentError> {
    loop {
        {
            let value = receiver.borrow();
            if let Some(result) = value.as_ref() {
                return Ok(result.clone());
            }
        }
        if receiver.changed().await.is_err() {
            return Err(AgentError::Internal);
        }
    }
}

/// Awaits one startup admission preparation. It resolves only after the
/// Session published either a real loop or an explicit failure kind.
pub(crate) async fn await_loop_preparation(
    mut waiter: PreparationWaiter,
) -> Result<LoopAccepted, AgentError> {
    loop {
        {
            let value = waiter.receiver.borrow();
            if let Some(prepared) = value.as_ref() {
                return match prepared {
                    PreparedLoop::Started(accepted) => Ok(accepted.clone()),
                    PreparedLoop::Failed(failure) => Err(match failure {
                        PreparationFailure::Cancelled => AgentError::InvalidState,
                        PreparationFailure::ContextUncompressible => {
                            AgentError::ContextUncompressible
                        }
                        PreparationFailure::HistoryTooLarge => AgentError::HistoryTooLarge,
                        PreparationFailure::SessionBlocked => AgentError::SessionBlocked,
                        PreparationFailure::SessionBusy => AgentError::SessionBusy,
                        PreparationFailure::Compaction(_) => AgentError::Internal,
                        PreparationFailure::Internal => AgentError::Internal,
                    }),
                };
            }
        }
        if waiter.receiver.changed().await.is_err() {
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
        StoreError::HistoryChanged => AgentError::InvalidState,
        StoreError::QueryLimit => AgentError::QueryLimit,
        StoreError::InvalidArguments => AgentError::InvalidArguments,
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
    use crate::compaction::EphemeralGroupKey;
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

    #[test]
    fn an_old_loop_cleanup_cannot_clear_a_new_loop_summary() {
        let state = CompactionState::new();
        let old_loop = LoopId::new().unwrap();
        let new_loop = LoopId::new().unwrap();
        let cleanup = EphemeralSummaryCleanup {
            state: Arc::clone(&state),
            loop_id: old_loop,
        };
        assert!(state.cache_ephemeral(
            old_loop,
            EphemeralGroupKey {
                start: 0,
                end: 1,
                source_hash: [1; 32],
            },
            BoundedText::new("old").unwrap(),
        ));
        assert!(state.cache_ephemeral(
            new_loop,
            EphemeralGroupKey {
                start: 0,
                end: 1,
                source_hash: [2; 32],
            },
            BoundedText::new("new").unwrap(),
        ));
        drop(cleanup);
        assert!(state.ephemeral(new_loop).is_some());
    }
}
