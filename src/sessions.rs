use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};
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
use crate::subagents::SubagentService;
use crate::workspace::{Workspace, WorkspaceError};

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
            self.loaded.remove(&session_id);
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

/// Whether one submission needs a startup summary preparation.
enum AdmissionNeed {
    /// The request fits; start the loop directly.
    None,
    /// The request exceeds the runtime limits or the automatic trigger.
    Compact,
    /// System plus current User/Steer plus tool schemas already exceed the
    /// hard input ceiling; no summary can help without dropping constraints.
    Uncompressible,
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

/// Snapshot captured when automatic startup admission is reserved. Reloads
/// and other future-turn updates must not change the model/configuration of a
/// request that is already waiting for its admission decision.
struct AdmissionReservation {
    compaction: CompactionReservation,
    config: ExecutionConfig,
    options: LoopOptions,
    auto: AutoContext,
    system_prompt: Option<BoundedText>,
    /// The raw projected request was known to fit the hard model window before
    /// a trigger-started durable compaction attempt. It is a safe fallback if
    /// the optional summary attempt makes no progress.
    raw_fits_hard: bool,
}

/// Publishes and owns the outcome of one admission preparation. The wait
/// receiver is created before work starts so no completion can be lost.
struct AdmissionOperation {
    result: watch::Sender<Option<PreparedLoop>>,
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    operation: Arc<CompactionOperation>,
}

impl AdmissionOperation {
    fn new(
        operation: Arc<CompactionOperation>,
    ) -> (Arc<Self>, watch::Receiver<Option<PreparedLoop>>) {
        let (result, receiver) = watch::channel(None);
        (
            Arc::new(Self {
                result,
                join: tokio::sync::Mutex::new(None),
                operation,
            }),
            receiver,
        )
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
}

struct CompactionCompletionGuard {
    session: Session,
    operation: Arc<CompactionOperation>,
    armed: bool,
}

impl CompactionOperation {
    fn new(operation_id: String) -> (Arc<Self>, watch::Receiver<Option<CompactionResult>>) {
        let (result, receiver) = watch::channel(None);
        let operation = Arc::new(Self {
            operation_id,
            cancellation: CancellationToken::new(),
            result,
            join: tokio::sync::Mutex::new(None),
            state: AtomicU8::new(COMPACTION_RUNNING),
        });
        (operation, receiver)
    }

    fn cancel(&self) -> bool {
        if self
            .state
            .compare_exchange(
                COMPACTION_RUNNING,
                COMPACTION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        self.cancellation.cancel();
        true
    }

    fn cancellation_requested(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPACTION_CANCELLED
            || self.cancellation.is_cancelled()
    }

    /// Marks the operation terminal without publishing a manual result. Used
    /// by a startup admission after it has taken ownership of its loop, so a
    /// later cancel finds nothing to cancel.
    fn finish(&self) {
        self.state.store(COMPACTION_COMPLETED, Ordering::Release);
    }

    fn try_begin_commit(&self) -> bool {
        self.state
            .compare_exchange(
                COMPACTION_RUNNING,
                COMPACTION_COMMITTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn commit_started(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPACTION_COMMITTING
    }

    fn failed_result(
        &self,
        failure_kind: &'static str,
        utility_usage: Option<CompactionUtilityUsage>,
    ) -> CompactionResult {
        CompactionResult {
            operation_id: self.operation_id.clone(),
            status: CompactionStatus::Failed,
            before_tokens: None,
            after_tokens: None,
            covered_loop_count: 0,
            covered_item_count: 0,
            retained_item_count: 0,
            utility_usage,
            failure_kind: Some(failure_kind.to_owned()),
        }
    }

    fn unknown_write_result(&self) -> CompactionResult {
        CompactionResult {
            operation_id: self.operation_id.clone(),
            status: CompactionStatus::UnknownWrite,
            before_tokens: None,
            after_tokens: None,
            covered_loop_count: 0,
            covered_item_count: 0,
            retained_item_count: 0,
            utility_usage: None,
            failure_kind: Some("write_outcome_unknown".to_owned()),
        }
    }

    fn publish(&self, result: CompactionResult) -> CompactionResult {
        let result = match self.state.compare_exchange(
            COMPACTION_RUNNING,
            COMPACTION_COMPLETED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => result,
            Err(COMPACTION_CANCELLED) => {
                self.state.store(COMPACTION_COMPLETED, Ordering::Release);
                self.failed_result("cancelled", result.utility_usage.clone())
            }
            Err(COMPACTION_COMMITTING) => {
                self.state.store(COMPACTION_COMPLETED, Ordering::Release);
                result
            }
            Err(COMPACTION_COMPLETED) => result,
            Err(_) => result,
        };
        self.result.send_replace(Some(result.clone()));
        result
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
}

impl CompactionCompletionGuard {
    fn new(session: Session, operation: Arc<CompactionOperation>) -> Self {
        Self {
            session,
            operation,
            armed: true,
        }
    }

    fn publish(&mut self, result: CompactionResult) {
        if !self.armed {
            return;
        }
        self.session
            .publish_compaction_result(&self.operation, result);
        self.armed = false;
    }
}

impl Drop for CompactionCompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let result = if self.operation.commit_started() {
            self.operation.unknown_write_result()
        } else if self.operation.cancellation_requested() {
            self.operation.failed_result("cancelled", None)
        } else {
            self.operation.failed_result("internal", None)
        };
        self.session
            .publish_compaction_result(&self.operation, result);
    }
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
        subagents: Arc<SubagentService>,
        compaction: Arc<CompactionState>,
        policy: CompactionPolicy,
        store: Store,
        events: AgentEventSink,
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

    /// Decides whether the next submission must compact its settled history
    /// before a loop can start. The reservation has already captured the
    /// model/configuration, so reloads cannot change this request mid-flight.
    /// The irreducible minimum is checked with the real current input before a
    /// futile summary model call is started.
    async fn admission_needed(
        &self,
        input: &UserInput,
        reservation: &mut AdmissionReservation,
    ) -> Result<AdmissionNeed, PreparationFailure> {
        let operation = &reservation.compaction.operation;
        if operation.cancellation_requested() {
            return Err(PreparationFailure::Cancelled);
        }
        let deadline = reservation.compaction.deadline;
        if Instant::now() >= deadline {
            return Err(PreparationFailure::Compaction("timeout"));
        }
        let history = &reservation.compaction.history;
        let (projected, summary) = match self.shared.compaction.project(history) {
            Some(projection) => (projection.suffix.to_vec(), Some(projection.summary)),
            None => (history.to_vec(), None),
        };
        let read = reservation
            .compaction
            .workspace
            .read_prefix(crate::prompt::AGENTS_PATH, crate::prompt::MAX_AGENTS_BYTES);
        tokio::pin!(read);
        let prefix = tokio::select! {
            biased;
            _ = operation.cancellation.cancelled() => {
                return Err(PreparationFailure::Cancelled);
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(PreparationFailure::Compaction("timeout"));
            }
            result = &mut read => result,
        };
        let agents = match prefix {
            Ok(prefix) => Some(
                crate::prompt::decode_agents(&prefix).map_err(|_| PreparationFailure::Internal)?,
            ),
            Err(WorkspaceError::NotFound) => None,
            Err(_) => return Err(PreparationFailure::Internal),
        };
        if operation.cancellation_requested() {
            return Err(PreparationFailure::Cancelled);
        }
        let system_base = minicore_runtime::value::BoundedText::new(
            reservation.compaction.record.system_prompt.clone(),
        )
        .map_err(|_| PreparationFailure::Internal)?;
        let system = crate::prompt::build_system_prompt(&system_base, agents)
            .map_err(|_| PreparationFailure::Internal)?;
        reservation.system_prompt = Some(system.clone());
        let budget = reservation
            .auto
            .policy
            .budget(reservation.config.descriptor().context_window);
        let tools = &reservation.compaction.tool_schemas;
        // The irreducible request alone may already exceed the budget; no
        // summary could make it fit without dropping current user constraints.
        let minimal_tokens = crate::compaction::estimate_minimal(
            &system,
            &[input.as_text()],
            tools,
            reservation.compaction.record.reasoning,
            &*reservation.auto.budget,
        )
        .map_err(|_| PreparationFailure::Internal)?;
        let minimal_message_count =
            ((!system.is_empty()) as usize).saturating_add((!input.as_text().is_empty()) as usize);
        if Instant::now() >= deadline {
            return Err(PreparationFailure::Compaction("timeout"));
        }
        if minimal_tokens > budget.hard_tokens
            || minimal_message_count > reservation.options.limits.max_prompt_messages
        {
            return Ok(AdmissionNeed::Uncompressible);
        }

        let runtime_over_limit =
            !history_fits_runtime_limits(&projected, &reservation.options.limits);
        let projected_message_count = projected
            .len()
            .saturating_add((!system.is_empty()) as usize)
            .saturating_add(summary.is_some() as usize)
            .saturating_add((!input.as_text().is_empty()) as usize);
        let compacted_message_count =
            (!system.is_empty()) as usize + 1 + (!input.as_text().is_empty()) as usize;
        if (runtime_over_limit
            || projected_message_count > reservation.options.limits.max_prompt_messages)
            && compacted_message_count > reservation.options.limits.max_prompt_messages
        {
            return Ok(AdmissionNeed::Uncompressible);
        }
        // Do not compose/estimate a full request when the Runtime history
        // itself is already outside its structural admission limits. The
        // bounded utility summary is the recovery path for that history.
        if runtime_over_limit {
            return Ok(AdmissionNeed::Compact);
        }
        if projected_message_count > reservation.options.limits.max_prompt_messages {
            return Ok(AdmissionNeed::Compact);
        }

        // With no settled history there is nothing automatic compaction can
        // reduce. A request between trigger and hard is valid and must start
        // normally without a utility call.
        if projected.is_empty() && summary.is_none() {
            let estimate = crate::compaction::estimate_startup_exact(
                &system,
                None,
                &projected,
                input.as_text(),
                tools,
                reservation.compaction.record.reasoning,
                &*reservation.auto.budget,
            )
            .map_err(|_| PreparationFailure::Internal)?;
            self.shared
                .compaction
                .update_automatic(&operation.operation_id, |observation| {
                    observation.before_tokens = Some(estimate);
                    observation.after_tokens = Some(estimate);
                });
            self.shared.compaction.note_request_estimate(estimate);
            if estimate > budget.hard_tokens {
                return Ok(AdmissionNeed::Uncompressible);
            }
            if Instant::now() >= deadline {
                return Err(PreparationFailure::Compaction("timeout"));
            }
            return Ok(AdmissionNeed::None);
        }

        let request_safe = crate::compaction::startup_history_is_request_safe(&projected);
        let estimate = if request_safe {
            match crate::compaction::estimate_startup_exact(
                &system,
                summary.as_ref(),
                &projected,
                input.as_text(),
                tools,
                reservation.compaction.record.reasoning,
                &*reservation.auto.budget,
            ) {
                Ok(estimate) => estimate,
                // A request-safe history can still fail ModelRequest
                // validation because of an invalid value. Let compaction
                // replace it rather than sending it to Runtime.
                Err(_) => return Ok(AdmissionNeed::Compact),
            }
        } else {
            // A malformed/structurally invalid full history is not sent. A
            // durable summary can replace it, provided the minimum above
            // still fits the hard boundary.
            crate::compaction::estimate_startup(
                &system,
                summary.as_ref(),
                &projected,
                input.as_text(),
                tools,
                reservation.compaction.record.reasoning,
            )
            .map_err(|_| PreparationFailure::Internal)?
        };
        self.shared
            .compaction
            .update_automatic(&operation.operation_id, |observation| {
                observation.before_tokens = Some(estimate);
                observation.after_tokens = Some(estimate);
            });
        self.shared.compaction.note_request_estimate(estimate);
        if operation.cancellation_requested() {
            return Err(PreparationFailure::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(PreparationFailure::Compaction("timeout"));
        }
        if request_safe && estimate <= budget.hard_tokens {
            reservation.raw_fits_hard = true;
        }
        if !request_safe {
            return Ok(AdmissionNeed::Compact);
        }
        if estimate <= budget.trigger_tokens {
            Ok(AdmissionNeed::None)
        } else {
            Ok(AdmissionNeed::Compact)
        }
    }

    fn build_admission_execution(
        &self,
        input: UserInput,
        reservation: &AdmissionReservation,
        admission: &Arc<AdmissionOperation>,
    ) -> Result<ExecutionInput, AgentError> {
        {
            let inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing
                || inner.active.is_some()
                || !inner
                    .admission
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, admission))
                || !Arc::ptr_eq(&inner.history, &reservation.compaction.history)
            {
                return Err(AgentError::SessionBusy);
            }
        }
        let (history, summary) = match self
            .shared
            .compaction
            .project(&reservation.compaction.history)
        {
            Some(projection) => (projection.suffix.to_vec().into(), Some(projection.summary)),
            None => (Arc::clone(&reservation.compaction.history), None),
        };
        if !history_fits_runtime_limits(&history, &reservation.options.limits) {
            return Err(AgentError::HistoryTooLarge);
        }
        let system = reservation
            .system_prompt
            .as_ref()
            .ok_or(AgentError::InvalidState)?;
        let message_count = history
            .len()
            .saturating_add((!system.is_empty()) as usize)
            .saturating_add(summary.is_some() as usize)
            .saturating_add((!input.as_text().is_empty()) as usize);
        if message_count > reservation.options.limits.max_prompt_messages {
            return Err(AgentError::ContextUncompressible);
        }
        let estimate = crate::compaction::estimate_startup_exact(
            system,
            summary.as_ref(),
            &history,
            input.as_text(),
            &reservation.compaction.tool_schemas,
            reservation.compaction.record.reasoning,
            &*reservation.auto.budget,
        )
        .map_err(|_| AgentError::ContextUncompressible)?;
        if estimate > reservation.compaction.hard_tokens {
            return Err(AgentError::ContextUncompressible);
        }
        self.shared.compaction.update_automatic(
            &reservation.compaction.operation.operation_id,
            |observation| {
                observation.after_tokens = Some(estimate);
            },
        );
        self.shared.compaction.note_request_estimate(estimate);
        let config = self.bind_execution_config(
            reservation.config.clone(),
            summary.clone(),
            reservation.compaction.record.system_prompt.clone(),
            Some(reservation.auto.clone()),
        )?;
        Ok(ExecutionInput {
            request: LoopRequest::new(history, input, config),
            options: reservation.options.clone(),
            summary,
        })
    }

    /// Reserves a Session-owned admission preparation, spawning the summary
    /// worker. The returned receiver yields the loop it finally created.
    async fn start_admission(&self, input: UserInput) -> Result<PreparationWaiter, AgentError> {
        let operation_id = {
            let mut inner = self.shared.inner.lock().unwrap();
            let Some(next) = inner.next_admission_id.checked_add(1) else {
                return Err(AgentError::InvalidState);
            };
            let operation_id = format!("admission-{}-{}", inner.record.session_id, next);
            inner.next_admission_id = next;
            operation_id
        };
        let (operation, _receiver) = CompactionOperation::new(operation_id.clone());
        let (admission, result_receiver) = AdmissionOperation::new(Arc::clone(&operation));
        let mut join_slot = admission.join.lock().await;
        let reservation = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing
                || inner.active.is_some()
                || inner.compaction.is_some()
                || inner.admission.is_some()
            {
                return Err(AgentError::SessionBusy);
            }
            let auto = inner.auto.clone().ok_or(AgentError::SessionBusy)?;
            // The automatic binding always carries the raw configured model,
            // never the presentation-wrapped one, so the summary utility has
            // its own no-tools identity.
            let model = Arc::clone(&auto.model);
            let descriptor = inner.config.descriptor().clone();
            let history = Arc::clone(&inner.history);
            // Reuse an already-validated durable summary as the fold starting
            // point instead of re-summarizing its covered prefix.
            let previous = self.shared.compaction.project(&history);
            let (previous_summary, previous_covered_item_count) = previous
                .map(|projection| {
                    let covered = history.len().saturating_sub(projection.suffix.len());
                    (Some(projection.summary), covered)
                })
                .unwrap_or((None, 0));
            let retained_item_count = history.len().saturating_sub(previous_covered_item_count);
            let budget = auto.policy.budget(descriptor.context_window);
            self.shared
                .compaction
                .begin_automatic(AutomaticCompactionObservation {
                    operation_id: operation.operation_id.clone(),
                    loop_id: None,
                    request_index: None,
                    before_tokens: None,
                    after_tokens: None,
                    utility_before_tokens: None,
                    utility_after_tokens: None,
                    hard_tokens: budget.hard_tokens,
                    trigger_tokens: budget.trigger_tokens,
                    target_tokens: budget.target_tokens,
                    utility_usage: None,
                    outcome: "preparing".to_owned(),
                });
            let options = inner.options.clone();
            let compaction = CompactionReservation {
                operation: Arc::clone(&operation),
                model,
                record: inner.record.clone(),
                history,
                workspace: Arc::clone(&inner.workspace),
                tool_schemas: frozen_tool_specs(&inner.config, &inner.record),
                previous_summary,
                previous_covered_item_count,
                hard_tokens: budget.hard_tokens,
                target_tokens: budget.target_tokens,
                safe_before_estimate: true,
                refresh_summary: true,
                // Startup compaction has its own operation deadline. The
                // effective automatic preparation budget covers the configured
                // model/prompt timeout floor and is shared by all utility
                // chunks; no individual chunk gets a fresh timeout.
                deadline: Instant::now()
                    .checked_add(options.prompt_timeout.max(options.model_timeout))
                    .unwrap_or_else(Instant::now),
            };
            inner.compaction = Some(Arc::clone(&operation));
            inner.admission = Some(Arc::clone(&admission));
            inner.compaction_progress = Some(CompactionProgress {
                operation_id: operation.operation_id.clone(),
                phase: CompactionPhase::Preparing,
                covered_item_count: previous_covered_item_count,
                retained_item_count,
            });
            AdmissionReservation {
                compaction,
                config: inner.config.clone(),
                options,
                auto,
                system_prompt: None,
                raw_fits_hard: false,
            }
        };
        // Publish the reservation before spawning a worker that may complete
        // synchronously (for example, an already-fitting request). This keeps
        // SessionState notifications in causal order.
        self.emit_state();
        // Do not let a very fast worker finish and clear the Session slot
        // before its JoinHandle has been stored. The gate closes that small
        // shutdown/ownership window without adding an await to admission.
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let worker_session = self.clone();
        let worker_admission = Arc::clone(&admission);
        let task = tokio::spawn(async move {
            if start_rx.await.is_ok() {
                run_admission(worker_session, worker_admission, reservation, input).await;
            }
        });
        *join_slot = Some(task);
        drop(join_slot);
        let _ = start_tx.send(());
        Ok(PreparationWaiter {
            session: self.clone(),
            operation_id,
            receiver: result_receiver,
        })
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
            // AgentLoop::start only validates and spawns the Runtime task; it
            // does not await model work. Keeping this short lock held closes
            // the cancellation/close race between validation and ownership.
            let mut agent_loop = AgentLoop::start(execution.request, execution.options)
                .map_err(map_loop_start_error)?;
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
                .note_loop_started(turn.loop_id, accepted_at.clone());
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

    /// Clears only the wire-visible progress for one admission. The admission
    /// and its JoinHandle remain Session-owned until the worker has actually
    /// been joined by cleanup or shutdown.
    fn clear_admission(&self, admission: &Arc<AdmissionOperation>) {
        let mut inner = self.shared.inner.lock().unwrap();
        let current = inner
            .admission
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, admission));
        if current {
            inner.compaction_progress = None;
        }
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

    async fn reap_finished_admission(&self) -> Result<bool, AgentError> {
        let admission = {
            let inner = self.shared.inner.lock().unwrap();
            inner.admission.clone()
        };
        let Some(admission) = admission else {
            return Ok(false);
        };
        if admission.result.borrow().is_none() {
            return Ok(false);
        }
        if admission.join().await.is_err() {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .admission
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission))
            {
                inner.admission = None;
            }
            if inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission.operation))
            {
                inner.compaction = None;
                inner.compaction_progress = None;
            }
            inner.blocked = Some(SessionBlockReason::Internal);
            drop(inner);
            self.emit_state();
            return Err(AgentError::SessionBlocked);
        }
        let cleared = {
            let mut inner = self.shared.inner.lock().unwrap();
            let same_admission = inner
                .admission
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission));
            if same_admission {
                inner.admission = None;
            }
            let same_compaction = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &admission.operation));
            if same_compaction {
                inner.compaction = None;
            }
            let progress_cleared = inner.compaction_progress.take().is_some();
            same_admission || same_compaction || progress_cleared
        };
        if cleared {
            self.emit_state();
        }
        Ok(true)
    }

    pub(crate) async fn start_compaction(
        &self,
        operation_id: String,
        model: Arc<dyn Model>,
        descriptor: ModelDescriptor,
    ) -> Result<watch::Receiver<Option<CompactionResult>>, AgentError> {
        self.cleanup_finished().await?;
        let _io = self.shared.io.lock().await;
        let (operation, receiver) = CompactionOperation::new(operation_id.clone());

        // Acquire the operation's join slot before publishing the reservation.
        // From the point where the Session becomes busy to the point where the
        // spawned task is stored, there is no further await at which the only
        // JoinHandle could be dropped and detached.
        let mut join_slot = operation.join.lock().await;
        let reservation = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing || inner.active.is_some() || inner.compaction.is_some() {
                return Err(AgentError::SessionBusy);
            }
            if inner.used_compaction_ids.contains(&operation_id) {
                return Err(AgentError::SessionBusy);
            }
            if inner.used_compaction_ids.len() >= MAX_USED_COMPACTION_IDS {
                return Err(AgentError::InvalidInput);
            }
            let previous = self.shared.compaction.project(&inner.history);
            let (previous_summary, previous_covered_item_count) = previous
                .map(|projection| {
                    let covered_item_count =
                        inner.history.len().saturating_sub(projection.suffix.len());
                    (Some(projection.summary), covered_item_count)
                })
                .unwrap_or((None, 0));
            let tool_schemas = frozen_tool_specs(&inner.config, &inner.record);
            let retained_item_count = inner
                .history
                .len()
                .saturating_sub(previous_covered_item_count);
            // Explicit manual compaction keeps its original half-window
            // target. The Agent-global policy controls automatic compaction;
            // changing it must not silently change manual behavior.
            let target_tokens = descriptor.context_window / 2;
            inner.compaction = Some(Arc::clone(&operation));
            inner.used_compaction_ids.insert(operation_id.clone());
            inner.compaction_progress = Some(CompactionProgress {
                operation_id: operation_id.clone(),
                phase: CompactionPhase::Preparing,
                covered_item_count: previous_covered_item_count,
                retained_item_count,
            });
            CompactionReservation {
                operation: Arc::clone(&operation),
                model,
                record: inner.record.clone(),
                history: Arc::clone(&inner.history),
                workspace: Arc::clone(&inner.workspace),
                tool_schemas,
                previous_summary,
                previous_covered_item_count,
                hard_tokens: descriptor.context_window,
                target_tokens,
                safe_before_estimate: false,
                refresh_summary: false,
                deadline: Instant::now()
                    .checked_add(inner.options.model_timeout)
                    .unwrap_or_else(Instant::now),
            }
        };
        drop(_io);
        let task = tokio::spawn(run_compaction(self.clone(), reservation));
        *join_slot = Some(task);
        drop(join_slot);
        self.emit_state();
        Ok(receiver)
    }

    pub(crate) fn cancel_compaction(&self, operation_id: &str) -> Result<bool, AgentError> {
        // A startup preparation reserves the same `compaction` slot and
        // operation id, so this cancels both a manual operation and a
        // preparation that has not started its loop.
        let operation = {
            let inner = self.shared.inner.lock().unwrap();
            inner
                .compaction
                .as_ref()
                .filter(|operation| operation.operation_id == operation_id)
                .cloned()
        };
        let Some(operation) = operation else {
            return Ok(false);
        };
        Ok(operation.cancel())
    }

    fn cancel_compaction_on_drop(&self) {
        let operation = {
            let inner = self.shared.inner.lock().unwrap();
            inner.compaction.clone()
        };
        if let Some(operation) = operation {
            let _ = operation.cancel();
        }
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

    async fn reap_finished_compaction(&self) -> Result<bool, AgentError> {
        let operation = {
            let inner = self.shared.inner.lock().unwrap();
            inner.compaction.clone()
        };
        let Some(operation) = operation else {
            return Ok(true);
        };
        if operation.result.borrow().is_none() {
            return Ok(false);
        }
        if operation.join().await.is_err() {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation))
            {
                inner.compaction = None;
                inner.compaction_progress = None;
            }
            inner.blocked = Some(SessionBlockReason::Internal);
            drop(inner);
            self.emit_state();
            return Err(AgentError::SessionBlocked);
        }
        let cleared = {
            let mut inner = self.shared.inner.lock().unwrap();
            let same = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation));
            if same {
                inner.compaction = None;
            }
            same && inner.compaction_progress.take().is_some()
        };
        if cleared {
            self.emit_state();
        }
        Ok(true)
    }

    fn publish_compaction_result(&self, operation: &CompactionOperation, result: CompactionResult) {
        // Clear wire-visible progress before waking result waiters. The
        // operation handle remains Session-owned until a later join.
        let should_emit = {
            let mut inner = self.shared.inner.lock().unwrap();
            let should_clear = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| std::ptr::eq(candidate.as_ref(), operation));
            let should_emit = should_clear && inner.compaction_progress.take().is_some();
            let result = operation.publish(result);
            inner.last_compaction_result = Some(result);
            should_emit
        };
        if should_emit {
            self.emit_state();
        }
    }

    fn set_compaction_phase(&self, operation: &CompactionOperation, phase: CompactionPhase) {
        let changed = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .compaction
                .as_ref()
                .is_none_or(|candidate| !std::ptr::eq(candidate.as_ref(), operation))
            {
                false
            } else if let Some(progress) = inner.compaction_progress.as_mut() {
                progress.phase = phase;
                true
            } else {
                false
            }
        };
        if changed {
            self.emit_state();
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
        self.presentation().tool_data()
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
        self.shared.subagents.drain_session(session_id).await;
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

    async fn commit_compaction(
        &self,
        reservation: &CompactionReservation,
        source: &crate::store::HistoryPrefix,
        bytes: &[u8],
        summary: &minicore_runtime::value::BoundedText,
    ) -> Result<CompactionCommit, StoreError> {
        if reservation.operation.cancellation_requested() {
            return Ok(CompactionCommit::Cancelled);
        }
        let Some(remaining) = reservation
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return Ok(CompactionCommit::Deadline);
        };
        let _io = match tokio::time::timeout(remaining, self.shared.io.lock()).await {
            Ok(io) => io,
            Err(_) => return Ok(CompactionCommit::Deadline),
        };
        let operation = Arc::clone(&reservation.operation);
        let expected_history = Arc::clone(&reservation.history);
        {
            let inner = self.shared.inner.lock().unwrap();
            if operation.cancellation_requested() {
                return Ok(CompactionCommit::Cancelled);
            }
            if Instant::now() >= reservation.deadline {
                return Ok(CompactionCommit::Deadline);
            }
            let current = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation));
            if inner.closing || !current || !Arc::ptr_eq(&inner.history, &expected_history) {
                return Ok(CompactionCommit::Rejected);
            }
        }

        let session = self.clone();
        let deadline = reservation.deadline;
        let callback_operation = Arc::clone(&operation);
        let callback_history = Arc::clone(&expected_history);
        let commit = self
            .shared
            .store
            .commit_summary(
                session.session_id(),
                source,
                expected_history.as_ref(),
                bytes,
                reservation.deadline,
                move || {
                    let inner = session.shared.inner.lock().unwrap();
                    let current = inner
                        .compaction
                        .as_ref()
                        .is_some_and(|candidate| Arc::ptr_eq(candidate, &callback_operation));
                    if inner.closing
                        || !current
                        || !Arc::ptr_eq(&inner.history, &callback_history)
                        || callback_operation.cancellation_requested()
                        || Instant::now() >= deadline
                    {
                        return false;
                    }
                    callback_operation.try_begin_commit()
                },
            )
            .await?;

        match commit {
            SummaryCommit::Committed => {
                // The write is definite. Publish only if the Session still
                // owns the same settled history and operation at this
                // boundary; otherwise the disk result stays known but the
                // old in-memory projection remains authoritative.
                let publish = {
                    let inner = self.shared.inner.lock().unwrap();
                    let current = inner
                        .compaction
                        .as_ref()
                        .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation));
                    !inner.closing && current && Arc::ptr_eq(&inner.history, &expected_history)
                };
                if publish {
                    self.shared.compaction.publish(
                        summary.clone(),
                        source.covered_loop_count,
                        expected_history.len(),
                    );
                }
                Ok(CompactionCommit::Store(SummaryCommit::Committed))
            }
            SummaryCommit::Rejected if operation.cancellation_requested() => {
                Ok(CompactionCommit::Cancelled)
            }
            SummaryCommit::Rejected if Instant::now() >= reservation.deadline => {
                Ok(CompactionCommit::Deadline)
            }
            SummaryCommit::Rejected => Ok(CompactionCommit::Store(SummaryCommit::Rejected)),
            SummaryCommit::Unknown => Ok(CompactionCommit::Store(SummaryCommit::Unknown)),
        }
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

enum CompactionCommit {
    Cancelled,
    Deadline,
    Rejected,
    Store(SummaryCommit),
}

async fn run_compaction(session: Session, reservation: CompactionReservation) {
    let operation = Arc::clone(&reservation.operation);
    let mut completion = CompactionCompletionGuard::new(session.clone(), Arc::clone(&operation));
    let result = run_compaction_inner(&session, &reservation).await;
    completion.publish(result);
    #[cfg(test)]
    if let Some(gate) = take_pause_after_compaction_result(session.session_id()) {
        gate.started.notify_one();
        gate.release.notified().await;
    }
}

/// The single startup-admission worker: it folds the settled prefix into a
/// durable summary, then creates the loop. It never fabricates a `TurnRef`
/// before the summary is committed, and a cancelled preparation terminates
/// without starting any loop. A drop guard guarantees the waiter always sees a
/// terminal outcome even if the worker panics.
async fn run_admission(
    session: Session,
    admission: Arc<AdmissionOperation>,
    mut reservation: AdmissionReservation,
    input: UserInput,
) {
    let mut guard = AdmissionCompletionGuard {
        session: session.clone(),
        admission: Arc::clone(&admission),
        operation_id: reservation.compaction.operation.operation_id.clone(),
        outcome: None,
    };
    let operation = Arc::clone(&reservation.compaction.operation);
    let outcome = if operation.cancellation_requested() {
        PreparedLoop::Failed(PreparationFailure::Cancelled)
    } else {
        match session.admission_needed(&input, &mut reservation).await {
            Err(failure) => PreparedLoop::Failed(failure),
            Ok(AdmissionNeed::Uncompressible) => {
                PreparedLoop::Failed(PreparationFailure::ContextUncompressible)
            }
            Ok(AdmissionNeed::None) => {
                install_admitted_loop(&session, &admission, input, &reservation)
            }
            Ok(AdmissionNeed::Compact) => {
                let result = run_compaction_inner(&session, &reservation.compaction).await;
                session.shared.compaction.update_automatic(
                    &operation.operation_id,
                    |observation| {
                        if observation.before_tokens.is_none() {
                            observation.before_tokens = result.before_tokens;
                        }
                        if observation.after_tokens.is_none() {
                            observation.after_tokens = result.after_tokens;
                        }
                        if result.before_tokens.is_some() {
                            observation.utility_before_tokens = result.before_tokens;
                        }
                        if result.after_tokens.is_some() {
                            observation.utility_after_tokens = result.after_tokens;
                        }
                        if result.utility_usage.is_some() {
                            observation.utility_usage = result.utility_usage.clone();
                        }
                    },
                );
                match result.status {
                    CompactionStatus::Compacted | CompactionStatus::Noop => {
                        if operation.cancellation_requested() {
                            PreparedLoop::Failed(PreparationFailure::Cancelled)
                        } else {
                            install_admitted_loop(&session, &admission, input, &reservation)
                        }
                    }
                    CompactionStatus::Failed => {
                        if reservation.raw_fits_hard
                            && !operation.cancellation_requested()
                            && Instant::now() < reservation.compaction.deadline
                        {
                            install_admitted_loop(&session, &admission, input, &reservation)
                        } else {
                            PreparedLoop::Failed(admission_failure_kind(
                                result.failure_kind.as_deref(),
                            ))
                        }
                    }
                    CompactionStatus::UnknownWrite => {
                        PreparedLoop::Failed(admission_failure_kind(result.failure_kind.as_deref()))
                    }
                }
            }
        }
    };
    guard.outcome = Some(outcome);
    drop(guard);
    #[cfg(test)]
    if let Some(gate) = take_pause_after_admission_result(session.session_id()) {
        gate.started.notify_one();
        gate.release.notified().await;
    }
}

/// Publishes exactly one terminal outcome for an admission. If the worker is
/// dropped without publishing (panic or task abort), a later admission cannot
/// hang on a sender that will never send.
struct AdmissionCompletionGuard {
    session: Session,
    admission: Arc<AdmissionOperation>,
    operation_id: String,
    outcome: Option<PreparedLoop>,
}

impl Drop for AdmissionCompletionGuard {
    fn drop(&mut self) {
        let mut outcome = self
            .outcome
            .take()
            .unwrap_or(PreparedLoop::Failed(PreparationFailure::Internal));
        if !matches!(&outcome, PreparedLoop::Started(_))
            && self.admission.operation.cancellation_requested()
        {
            outcome = PreparedLoop::Failed(PreparationFailure::Cancelled);
        }
        if matches!(
            &outcome,
            PreparedLoop::Failed(PreparationFailure::ContextUncompressible)
        ) {
            self.session
                .shared
                .compaction
                .note_prepare_failure(crate::compaction::CONTEXT_UNCOMPRESSIBLE);
        } else if let PreparedLoop::Failed(PreparationFailure::Compaction(kind)) = &outcome {
            self.session.shared.compaction.note_prepare_failure(kind);
        } else if matches!(&outcome, PreparedLoop::Started(_)) {
            self.session.shared.compaction.clear_prepare_failure();
        }
        let observation_outcome = match &outcome {
            PreparedLoop::Started(_) => "started",
            PreparedLoop::Failed(PreparationFailure::Cancelled) => "cancelled",
            PreparedLoop::Failed(PreparationFailure::ContextUncompressible) => {
                "context_uncompressible"
            }
            PreparedLoop::Failed(PreparationFailure::HistoryTooLarge) => "history_too_large",
            PreparedLoop::Failed(PreparationFailure::SessionBlocked) => "session_blocked",
            PreparedLoop::Failed(PreparationFailure::SessionBusy) => "session_busy",
            PreparedLoop::Failed(PreparationFailure::Compaction(kind)) => kind,
            PreparedLoop::Failed(PreparationFailure::Internal) => "internal",
        };
        self.session.shared.compaction.finish_automatic(
            &self.operation_id,
            observation_outcome,
            None,
            None,
        );
        if !matches!(&outcome, PreparedLoop::Started(_)) {
            self.admission.operation.finish();
            self.session.clear_admission(&self.admission);
        }
        self.admission.result.send_replace(Some(outcome));
        self.session.emit_state();
    }
}

fn install_admitted_loop(
    session: &Session,
    admission: &Arc<AdmissionOperation>,
    input: UserInput,
    reservation: &AdmissionReservation,
) -> PreparedLoop {
    if reservation.compaction.operation.cancellation_requested() {
        return PreparedLoop::Failed(PreparationFailure::Cancelled);
    }
    if Instant::now() >= reservation.compaction.deadline {
        return PreparedLoop::Failed(PreparationFailure::Compaction("timeout"));
    }
    match session.build_admission_execution(input, reservation, admission) {
        Ok(execution) => match session.install_loop(execution, Some(admission)) {
            Ok(accepted) => PreparedLoop::Started(accepted),
            Err(error) => PreparedLoop::Failed(admission_failure(&error)),
        },
        Err(error) => PreparedLoop::Failed(admission_failure(&error)),
    }
}

fn admission_failure(error: &AgentError) -> PreparationFailure {
    match error {
        AgentError::HistoryTooLarge => PreparationFailure::HistoryTooLarge,
        AgentError::SessionBlocked => PreparationFailure::SessionBlocked,
        AgentError::SessionBusy => PreparationFailure::SessionBusy,
        AgentError::InvalidState => PreparationFailure::Cancelled,
        AgentError::ContextUncompressible => PreparationFailure::ContextUncompressible,
        _ => PreparationFailure::Internal,
    }
}

fn admission_failure_kind(kind: Option<&str>) -> PreparationFailure {
    match kind {
        Some("cancelled") => PreparationFailure::Cancelled,
        Some("history_too_large") => PreparationFailure::HistoryTooLarge,
        Some("context_uncompressible") => PreparationFailure::ContextUncompressible,
        Some("timeout") => PreparationFailure::Compaction("timeout"),
        Some("workspace") => PreparationFailure::Compaction("workspace"),
        Some("prompt") => PreparationFailure::Compaction("prompt"),
        Some("store") => PreparationFailure::Compaction("store"),
        Some("history_changed") => PreparationFailure::Compaction("history_changed"),
        Some("too_large") => PreparationFailure::Compaction("too_large"),
        Some("budget_exceeded") => PreparationFailure::Compaction("budget_exceeded"),
        Some("no_progress") => PreparationFailure::Compaction("no_progress"),
        Some("model_failure") => PreparationFailure::Compaction("model_failure"),
        Some("invalid_model_response") => PreparationFailure::Compaction("invalid_model_response"),
        Some("tool_call_rejected") => PreparationFailure::Compaction("tool_call_rejected"),
        Some("serialization_failure") => PreparationFailure::Compaction("serialization_failure"),
        Some("write_outcome_unknown") => PreparationFailure::Compaction("write_outcome_unknown"),
        _ => PreparationFailure::Compaction("compaction_failure"),
    }
}

async fn run_compaction_inner(
    session: &Session,
    reservation: &CompactionReservation,
) -> CompactionResult {
    let history_len = reservation.history.len();
    let retained_item_count = history_len.saturating_sub(reservation.previous_covered_item_count);
    if reservation.operation.cancellation_requested() {
        return failed_compaction(&reservation.operation, "cancelled", history_len);
    }
    if history_len == 0
        || (retained_item_count == 0
            && (!reservation.refresh_summary || reservation.previous_summary.is_none()))
    {
        return CompactionResult {
            operation_id: reservation.operation.operation_id.clone(),
            status: CompactionStatus::Noop,
            before_tokens: None,
            after_tokens: None,
            covered_loop_count: 0,
            covered_item_count: reservation.previous_covered_item_count,
            retained_item_count: 0,
            utility_usage: None,
            failure_kind: None,
        };
    }

    let Some(remaining) = reservation
        .deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
    else {
        return failed_compaction(&reservation.operation, "timeout", history_len);
    };
    let source = match tokio::time::timeout(
        remaining,
        session
            .shared
            .store
            .capture_history_anchor(session.session_id(), reservation.history.as_ref()),
    )
    .await
    {
        Ok(Ok(Some(source))) => source,
        Ok(Ok(None)) => {
            return failed_compaction(&reservation.operation, "history_changed", history_len);
        }
        Ok(Err(_)) => return failed_compaction(&reservation.operation, "store", history_len),
        Err(_) => return failed_compaction(&reservation.operation, "timeout", history_len),
    };

    if reservation.operation.cancellation_requested() {
        return failed_compaction(&reservation.operation, "cancelled", history_len);
    }
    let Some(remaining) = reservation
        .deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
    else {
        return failed_compaction(&reservation.operation, "timeout", history_len);
    };
    let agents = match tokio::time::timeout(
        remaining,
        reservation
            .workspace
            .read_prefix(crate::prompt::AGENTS_PATH, crate::prompt::MAX_AGENTS_BYTES),
    )
    .await
    {
        Ok(Ok(prefix)) => match crate::prompt::decode_agents(&prefix) {
            Ok(content) => (!content.is_empty()).then_some(content),
            Err(_) => return failed_compaction(&reservation.operation, "workspace", history_len),
        },
        Ok(Err(WorkspaceError::NotFound)) => None,
        Ok(Err(_)) => return failed_compaction(&reservation.operation, "workspace", history_len),
        Err(_) => return failed_compaction(&reservation.operation, "timeout", history_len),
    };
    let system_prompt =
        match minicore_runtime::value::BoundedText::new(reservation.record.system_prompt.clone())
            .ok()
            .and_then(|system| crate::prompt::build_system_prompt(&system, agents).ok())
        {
            Some(system_prompt) => system_prompt,
            None => return failed_compaction(&reservation.operation, "prompt", history_len),
        };

    let input = CompactionInput {
        model: Arc::clone(&reservation.model),
        reasoning: reservation.record.reasoning,
        history: Arc::clone(&reservation.history),
        previous_summary: reservation.previous_summary.clone(),
        previous_covered_item_count: reservation.previous_covered_item_count,
        project_instructions: system_prompt,
        tool_schemas: reservation.tool_schemas.clone(),
        hard_tokens: reservation.hard_tokens,
        target_tokens: reservation.target_tokens,
        safe_before_estimate: reservation.safe_before_estimate,
        operation_deadline: reservation.deadline,
    };
    session.set_compaction_phase(&reservation.operation, CompactionPhase::Summarizing);
    let mut mark_merging =
        || session.set_compaction_phase(&reservation.operation, CompactionPhase::Merging);
    let generated = match generate_summary(
        &input,
        &reservation.operation.cancellation,
        &mut mark_merging,
    )
    .await
    {
        Ok(generated) => generated,
        Err(error) => {
            return failed_compaction_with_usage(
                &reservation.operation,
                error.error.kind(),
                history_len,
                error.utility_usage,
            );
        }
    };
    if reservation.operation.cancellation_requested() {
        return failed_compaction_with_usage(
            &reservation.operation,
            "cancelled",
            history_len,
            generated.utility_usage.clone(),
        );
    }
    if Instant::now() >= reservation.deadline {
        return failed_compaction_with_usage(
            &reservation.operation,
            "timeout",
            history_len,
            generated.utility_usage.clone(),
        );
    }
    session.set_compaction_phase(&reservation.operation, CompactionPhase::Committing);
    let Some(bytes) = crate::compaction::encode_snapshot(
        session.session_id(),
        &reservation.record,
        &source,
        &generated.content,
    ) else {
        return failed_compaction_with_usage(
            &reservation.operation,
            "too_large",
            history_len,
            generated.utility_usage.clone(),
        );
    };
    let commit = match session
        .commit_compaction(reservation, &source, &bytes, &generated.content)
        .await
    {
        Ok(commit) => commit,
        Err(_) => {
            return failed_compaction_with_usage(
                &reservation.operation,
                "store",
                history_len,
                generated.utility_usage.clone(),
            );
        }
    };
    match commit {
        CompactionCommit::Store(SummaryCommit::Committed) => CompactionResult {
            operation_id: reservation.operation.operation_id.clone(),
            status: CompactionStatus::Compacted,
            before_tokens: Some(generated.before_tokens),
            after_tokens: Some(generated.after_tokens),
            covered_loop_count: source.covered_loop_count,
            covered_item_count: history_len,
            retained_item_count: 0,
            utility_usage: generated.utility_usage,
            failure_kind: None,
        },
        CompactionCommit::Cancelled => failed_compaction_with_usage(
            &reservation.operation,
            "cancelled",
            history_len,
            generated.utility_usage.clone(),
        ),
        CompactionCommit::Deadline => failed_compaction_with_usage(
            &reservation.operation,
            "timeout",
            history_len,
            generated.utility_usage.clone(),
        ),
        CompactionCommit::Rejected | CompactionCommit::Store(SummaryCommit::Rejected) => {
            failed_compaction_with_usage(
                &reservation.operation,
                "history_changed",
                history_len,
                generated.utility_usage.clone(),
            )
        }
        CompactionCommit::Store(SummaryCommit::Unknown) => CompactionResult {
            operation_id: reservation.operation.operation_id.clone(),
            status: CompactionStatus::UnknownWrite,
            before_tokens: Some(generated.before_tokens),
            after_tokens: Some(generated.after_tokens),
            covered_loop_count: source.covered_loop_count,
            covered_item_count: reservation.previous_covered_item_count,
            retained_item_count,
            utility_usage: generated.utility_usage,
            failure_kind: Some("write_outcome_unknown".to_owned()),
        },
    }
}

fn failed_compaction(
    operation: &CompactionOperation,
    failure_kind: &'static str,
    history_len: usize,
) -> CompactionResult {
    failed_compaction_with_usage(operation, failure_kind, history_len, None)
}

fn failed_compaction_with_usage(
    operation: &CompactionOperation,
    failure_kind: &'static str,
    history_len: usize,
    utility_usage: Option<CompactionUtilityUsage>,
) -> CompactionResult {
    CompactionResult {
        operation_id: operation.operation_id.clone(),
        status: CompactionStatus::Failed,
        before_tokens: None,
        after_tokens: None,
        covered_loop_count: 0,
        covered_item_count: 0,
        retained_item_count: history_len,
        utility_usage,
        failure_kind: Some(failure_kind.to_owned()),
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
    // The joined report is authoritative: reconcile terminal tool state even
    // when the wrapper future was dropped by an outer Runtime deadline/cancel
    // or its best-effort events were lost.
    session
        .presentation()
        .tool_data()
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
        .presentation()
        .tool_data()
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
        } => {
            // Runtime accepted the call; arguments are not available yet.
            session.presentation().tool_data().note_requested(
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
                session.presentation().tool_data().note_phase(
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
