use super::*;

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
    Unknown,
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
