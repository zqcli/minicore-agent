use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use serde::{Serialize, Serializer};
use tokio::sync::mpsc;

use minicore_runtime::execution::ConfigRevision;
use minicore_runtime::interaction::{InteractionKind, PendingInteraction};
use minicore_runtime::model::{ModelError, Usage};
use minicore_runtime::tools::{ApprovalRisk, ToolInputAnswerKind, ToolProgress, ToolResultOutcome};
use minicore_runtime::{InteractionId, LoopId, ToolCallId};

use crate::agent::SessionInfo;
use crate::ids::SessionId;
use crate::sessions::{
    CompactionProgress, SessionBlockReason, SessionState, SessionStatus, TurnPersistence, TurnRef,
};
use crate::tool_data::{ToolExecutionData, ToolInvocationData, ToolProcessData};

/// Identity and drop accounting attached to every agent event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EventMeta {
    pub session_id: SessionId,
    pub loop_id: Option<LoopId>,
    pub dropped_before: u64,
}

impl EventMeta {
    pub(crate) fn session(session_id: SessionId) -> Self {
        Self {
            session_id,
            loop_id: None,
            dropped_before: 0,
        }
    }

    pub(crate) fn for_turn(turn: TurnRef) -> Self {
        Self {
            loop_id: Some(turn.loop_id),
            ..Self::session(turn.session_id)
        }
    }
}

/// Best-effort live events. Events never participate in correctness; the
/// authoritative results are `turn.wait` and `session.history`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum AgentEvent {
    SessionOpened {
        session: SessionInfo,
        meta: EventMeta,
    },
    SessionClosed {
        session_id: SessionId,
        meta: EventMeta,
    },
    SessionState {
        #[serde(serialize_with = "serialize_session_state")]
        state: SessionState,
        meta: EventMeta,
    },
    TurnStarted {
        turn: TurnRef,
        meta: EventMeta,
    },
    RequestStarted {
        turn: TurnRef,
        request_index: u32,
        config_revision: ConfigRevision,
        model: String,
        reasoning: minicore_runtime::model::ReasoningPreference,
        meta: EventMeta,
    },
    /// Read-only per-request usage, emitted from the model stream when the
    /// provider reports it (spec 9.4/12.4). Best-effort: dropped when the
    /// bounded event queue is full, and never affects execution.
    RequestUsage {
        turn: TurnRef,
        request_index: u32,
        usage: minicore_runtime::model::Usage,
        meta: EventMeta,
    },
    OutputDelta {
        turn: TurnRef,
        request_index: u32,
        channel: OutputChannel,
        delta: String,
        meta: EventMeta,
    },
    ToolStarted {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        tool_name: String,
        meta: EventMeta,
    },
    /// Structured invocation facts for one tool call, published when the
    /// validated request reaches the policy boundary (before the approval
    /// decision and before the work). Same ToolRef-keyed data source as
    /// `tool.read`; read-only.
    ToolInvocation {
        turn: TurnRef,
        data: ToolInvocationData,
        meta: EventMeta,
    },
    /// Terminal structured execution facts for one tool call, emitted from the
    /// Runtime `ToolFinished` boundary. Same data source as `tool.read`.
    ToolExecution {
        turn: TurnRef,
        data: ToolExecutionData,
        meta: EventMeta,
    },
    /// Best-effort live process facts for one owned command, emitted after the
    /// authoritative window or record was written. Missing one loses a
    /// notification only; stored facts and the Runtime outcome are unaffected.
    ToolProcess {
        turn: TurnRef,
        data: ToolProcessData,
        meta: EventMeta,
    },
    /// Bounded display for one tool call, emitted when that call finishes
    /// (may arrive before or after `ToolStarted`). Read-only, never drives
    /// execution.
    ToolPresentation {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        tool_name: String,
        display: crate::presentation::ToolDisplay,
        meta: EventMeta,
    },
    /// Optional, self-contained and disposable input-generation display.
    /// Even `Generated` is not validation, policy approval, or execution.
    ToolArgumentsPreview {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        tool_name: String,
        attempt: u64,
        revision: u64,
        state: crate::presentation::ToolArgumentsPreviewState,
        partial: bool,
        display: crate::presentation::ToolDisplay,
        meta: EventMeta,
    },
    ToolProgress {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        progress: ToolProgressView,
        meta: EventMeta,
    },
    ToolFinished {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        result: ToolResultView,
        meta: EventMeta,
    },
    /// Read-only steering receipt: the number of Steering User items that were
    /// present in the PREPARED prompt history at this request boundary,
    /// emitted at the real `Model::start`. Best-effort and metadata-only; it
    /// never drives execution and never carries steer text.
    SteerProgress {
        turn: TurnRef,
        request_index: u32,
        applied_count: u64,
        meta: EventMeta,
    },
    InteractionRequested {
        turn: TurnRef,
        interaction: PendingInteractionView,
        meta: EventMeta,
    },
    InteractionResolved {
        turn: TurnRef,
        interaction_id: InteractionId,
        meta: EventMeta,
    },
    TurnFinished {
        turn: TurnRef,
        outcome: LoopOutcomeView,
        persistence: TurnPersistence,
        meta: EventMeta,
    },
}

macro_rules! event_meta {
    ($($variant:ident),+ $(,)?) => {
        pub(crate) fn meta(&self) -> &EventMeta {
            match self { $(Self::$variant { meta, .. })|+ => meta }
        }
        pub(crate) fn meta_mut(&mut self) -> &mut EventMeta {
            match self { $(Self::$variant { meta, .. })|+ => meta }
        }
    };
}

impl AgentEvent {
    pub(crate) fn dropped_before(&self) -> u64 {
        self.meta().dropped_before
    }

    pub(crate) fn set_dropped_before(&mut self, dropped_before: u64) {
        self.meta_mut().dropped_before = dropped_before;
    }

    event_meta!(
        SessionOpened,
        SessionClosed,
        SessionState,
        TurnStarted,
        RequestStarted,
        RequestUsage,
        OutputDelta,
        ToolStarted,
        ToolInvocation,
        ToolExecution,
        ToolProcess,
        ToolPresentation,
        ToolArgumentsPreview,
        ToolProgress,
        ToolFinished,
        InteractionRequested,
        InteractionResolved,
        SteerProgress,
        TurnFinished
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum AgentSendResult {
    Sent,
    Dropped,
    Closed,
}

/// Cloneable sender that merges runtime-envelope drops and its own bounded
/// queue drops into the next delivered event's `dropped_before`.
#[derive(Clone)]
pub(crate) struct AgentEventSink {
    sender: mpsc::Sender<AgentEvent>,
    drop_state: Arc<Mutex<DropState>>,
}

struct DropState {
    pending: u64,
}

impl AgentEventSink {
    pub(crate) fn new(sender: mpsc::Sender<AgentEvent>) -> Self {
        Self {
            sender,
            drop_state: Arc::new(Mutex::new(DropState { pending: 0 })),
        }
    }

    pub(crate) fn try_send(&self, mut event: AgentEvent) -> AgentSendResult {
        let mut pending = self
            .drop_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.sender.is_closed() {
            return AgentSendResult::Closed;
        }
        let own_drops = event.dropped_before();
        let reported = pending.pending.saturating_add(own_drops);
        event.set_dropped_before(reported);
        match self.sender.try_send(event) {
            Ok(()) => {
                pending.pending = 0;
                AgentSendResult::Sent
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                pending.pending = pending.pending.saturating_add(own_drops).saturating_add(1);
                AgentSendResult::Dropped
            }
            Err(mpsc::error::TrySendError::Closed(_)) => AgentSendResult::Closed,
        }
    }

    pub(crate) fn record_core_drops(&self, dropped: u64) {
        let mut pending = self
            .drop_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending.pending = pending.pending.saturating_add(dropped);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputChannel {
    Text,
    Reasoning,
}

impl OutputChannel {
    pub(crate) fn from_runtime(channel: minicore_runtime::OutputChannel) -> Self {
        match channel {
            minicore_runtime::OutputChannel::Text => Self::Text,
            minicore_runtime::OutputChannel::Reasoning => Self::Reasoning,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolProgressView {
    pub message: Option<String>,
    pub completed: Option<u64>,
    pub total: Option<u64>,
}

impl fmt::Debug for ToolProgressView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProgressView")
            .field("message_len", &self.message.as_ref().map(String::len))
            .field("completed", &self.completed)
            .field("total", &self.total)
            .finish()
    }
}

impl From<&ToolProgress> for ToolProgressView {
    fn from(progress: &ToolProgress) -> Self {
        Self {
            message: progress
                .message
                .as_ref()
                .map(|message| message.as_str().to_owned()),
            completed: progress.completed,
            total: progress.total,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolResultView {
    pub outcome: ToolResultOutcome,
    pub content_bytes: usize,
    /// Best-effort bounded result content for local display (same cap as the
    /// presentation wrapper); `None` when empty or unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Set when the live result exceeded the display cap and `content` is a
    /// truncated prefix.
    #[serde(default, skip_serializing_if = "is_false")]
    pub content_truncated: bool,
}

impl fmt::Debug for ToolResultView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResultView")
            .field("outcome", &self.outcome)
            .field("content_bytes", &self.content_bytes)
            .field("content_len", &self.content.as_ref().map(String::len))
            .field("content_truncated", &self.content_truncated)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LoopOutcomeView {
    Completed,
    Cancelled {
        reason: CancelReasonView,
    },
    Failed {
        kind: String,
        model_error: Option<ModelErrorView>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReasonView {
    User,
    OwnerDropped,
    Shutdown,
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopStatusView {
    Starting,
    RunningModel,
    RunningTools,
    WaitingForInput,
    Finishing,
    Finished,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LoopStateView {
    pub loop_id: LoopId,
    pub status: LoopStatusView,
    pub request_index: u32,
    pub config_revision: ConfigRevision,
    pub model: Option<String>,
    pub pending_interaction: Option<PendingInteractionView>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStateView {
    pub session_id: SessionId,
    pub status: SessionStatus,
    pub active_loop: Option<LoopStateView>,
    pub block_reason: Option<SessionBlockReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionProgress>,
}

/// Safe numeric evidence from the Agent's local request preflight only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalContextBudgetFailure {
    pub estimated_tokens: u64,
    pub input_budget_tokens: u64,
}

impl LocalContextBudgetFailure {
    pub(crate) fn from_model(error: &ModelError) -> Option<Self> {
        use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode};
        use minicore_runtime::model::{DeliveryState, ModelErrorKind, RetryHint};
        let diagnostic = error.diagnostic();
        if error.kind() != ModelErrorKind::InvalidRequest
            || error.delivery() != DeliveryState::NotStarted
            || !matches!(error.retry_hint(), RetryHint::Never)
            || diagnostic.code != DiagnosticCode::InvalidConfiguration
            || diagnostic.category != DiagnosticCategory::Model
            || diagnostic.retryable
        {
            return None;
        }
        let text = diagnostic.message.as_str();
        let numbers = text
            .strip_prefix(
                "local serialized context estimate exceeds input budget: estimated_tokens=",
            )?
            .strip_suffix("; use /compact or a larger model")?;
        let (estimated, budget) = numbers.split_once(", input_budget_tokens=")?;
        let estimated_tokens = estimated.parse::<u64>().ok()?;
        let input_budget_tokens = budget.parse::<u64>().ok()?;
        if estimated != estimated_tokens.to_string()
            || budget != input_budget_tokens.to_string()
            || estimated_tokens <= input_budget_tokens
        {
            return None;
        }
        Some(Self {
            estimated_tokens,
            input_budget_tokens,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
pub struct ModelErrorView {
    pub kind: String,
    pub delivery: String,
    pub retryable: bool,
    pub retry_after_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_context_budget: Option<LocalContextBudgetFailure>,
}

impl ModelErrorView {
    pub(crate) fn from_model(error: &ModelError) -> Self {
        let retry_after_millis = match error.retry_hint() {
            minicore_runtime::model::RetryHint::Never => None,
            minicore_runtime::model::RetryHint::Retryable { retry_after } => {
                retry_after.and_then(|duration| u64::try_from(duration.as_millis()).ok())
            }
        };
        Self {
            kind: crate::store::model_error_kind(error.kind()),
            delivery: crate::store::delivery_state(error.delivery()),
            retryable: error.diagnostic().retryable,
            retry_after_millis,
            local_context_budget: LocalContextBudgetFailure::from_model(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TurnResultView {
    pub turn: TurnRef,
    pub outcome: LoopOutcomeView,
    pub usage: Usage,
    pub requests: u32,
    pub tool_rounds: u64,
    pub final_config_revision: ConfigRevision,
    pub persistence: TurnPersistence,
}

impl TurnResultView {
    pub(crate) fn from_turn_result(result: &crate::sessions::TurnResult) -> Self {
        Self {
            turn: result.turn,
            outcome: LoopOutcomeView::from_report(&result.report),
            usage: result.report.usage,
            requests: result.report.requests,
            tool_rounds: result.report.tool_rounds,
            final_config_revision: result.report.final_config_revision,
            persistence: result.persistence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum InteractionKindView {
    Approval {
        prompt: String,
        risk: ApprovalRiskView,
    },
    ToolInput {
        prompt: String,
        choices: Vec<ToolInputChoiceView>,
        answer_kind: ToolInputAnswerKindView,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRiskView {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolInputAnswerKindView {
    Text,
    SingleChoice,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolInputChoiceView {
    pub index: usize,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PendingInteractionView {
    pub interaction_id: InteractionId,
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub kind: InteractionKindView,
}

impl From<&PendingInteraction> for PendingInteractionView {
    fn from(interaction: &PendingInteraction) -> Self {
        Self {
            interaction_id: interaction.interaction_id,
            tool_call_id: interaction.tool_call_id.clone(),
            tool_name: interaction.tool_name.to_string(),
            kind: match &interaction.kind {
                InteractionKind::Approval(request) => InteractionKindView::Approval {
                    prompt: request.prompt.as_str().to_owned(),
                    risk: match request.risk {
                        ApprovalRisk::Low => ApprovalRiskView::Low,
                        ApprovalRisk::Medium => ApprovalRiskView::Medium,
                        ApprovalRisk::High => ApprovalRiskView::High,
                    },
                },
                InteractionKind::ToolInput(request) => InteractionKindView::ToolInput {
                    prompt: request.prompt.as_str().to_owned(),
                    choices: request
                        .choices
                        .iter()
                        .enumerate()
                        .map(|(index, text)| ToolInputChoiceView {
                            index,
                            text: text.as_str().to_owned(),
                        })
                        .collect(),
                    answer_kind: match request.answer_kind {
                        ToolInputAnswerKind::Text => ToolInputAnswerKindView::Text,
                        ToolInputAnswerKind::SingleChoice => ToolInputAnswerKindView::SingleChoice,
                    },
                },
            },
        }
    }
}

impl From<&SessionState> for SessionStateView {
    fn from(state: &SessionState) -> Self {
        Self {
            session_id: state.session_id,
            status: state.status,
            active_loop: state.active_loop.as_ref().map(|loop_state| LoopStateView {
                loop_id: loop_state.loop_id,
                status: match loop_state.status {
                    minicore_runtime::LoopStatus::Starting => LoopStatusView::Starting,
                    minicore_runtime::LoopStatus::RunningModel => LoopStatusView::RunningModel,
                    minicore_runtime::LoopStatus::RunningTools => LoopStatusView::RunningTools,
                    minicore_runtime::LoopStatus::WaitingForInput => {
                        LoopStatusView::WaitingForInput
                    }
                    minicore_runtime::LoopStatus::Finishing => LoopStatusView::Finishing,
                    minicore_runtime::LoopStatus::Finished => LoopStatusView::Finished,
                },
                request_index: loop_state.request_index,
                config_revision: loop_state.config_revision,
                model: loop_state
                    .model
                    .as_ref()
                    .map(|model| model.as_str().to_owned()),
                pending_interaction: loop_state
                    .pending_interaction
                    .as_ref()
                    .map(PendingInteractionView::from),
            }),
            block_reason: state.block_reason,
            compaction: state.compaction.clone(),
        }
    }
}

fn serialize_session_state<S: Serializer>(
    state: &SessionState,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    SessionStateView::from(state).serialize(serializer)
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Requests one paginated history slice through an Agent-owned session.
/// Public single-consumer agent event stream.
pub struct AgentEventStream {
    receiver: mpsc::Receiver<AgentEvent>,
}

impl AgentEventStream {
    pub(crate) fn new(receiver: mpsc::Receiver<AgentEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<AgentEvent> {
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod snapshots;
#[cfg(test)]
mod tests;
