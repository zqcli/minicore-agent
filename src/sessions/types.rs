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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TurnResult {
    pub turn: TurnRef,
    pub outcome: LoopOutcomeView,
    pub usage: minicore_runtime::model::Usage,
    pub requests: u32,
    pub tool_rounds: u64,
    pub final_config_revision: ConfigRevision,
    pub persistence: TurnPersistence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPersistence {
    Persisted,
    Failed,
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
    /// Estimate of that Runtime history input (the projected suffix, or full
    /// history without a valid summary).
    pub estimated_history_bytes: usize,
    /// `estimated_history_bytes / 4`; this is not a full request-context token
    /// estimate and does not represent provider-reported usage.
    pub estimated_history_tokens: u64,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionExecution {
    None,
    Starting {
        input: QueueItem,
    },
    Running {
        loop_id: LoopId,
        input: QueueItem,
        request_index: u32,
        phase: crate::event::LoopStatusView,
    },
    Finishing {
        loop_id: LoopId,
        input: QueueItem,
        request_index: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotInteraction {
    pub loop_id: LoopId,
    pub request_index: u32,
    #[serde(flatten)]
    pub interaction: crate::event::PendingInteractionView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub load: LoadId,
    pub revision: u64,
    pub execution: SessionExecution,
    pub pending_interaction: Option<SnapshotInteraction>,
    pub queue: Queue,
    pub compaction: Option<CompactionProgress>,
    pub block_reason: Option<SessionBlockReason>,
    pub completions: CompletionWindow,
}
