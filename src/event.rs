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
use crate::history::{HistoryItemView, HistoryPage};
use crate::ids::SessionId;
use crate::sessions::{SessionBlockReason, SessionState, SessionStatus, TurnPersistence, TurnRef};

/// Identity and drop accounting attached to every agent event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EventMeta {
    pub session_id: SessionId,
    pub loop_id: Option<LoopId>,
    pub dropped_before: u64,
}

/// Best-effort live events. Events never participate in correctness; the
/// authoritative results are `turn.wait` and `session.history`.
#[derive(Clone, Debug, Eq, PartialEq)]
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

impl AgentEvent {
    pub(crate) fn dropped_before(&self) -> u64 {
        self.meta().dropped_before
    }

    pub(crate) fn set_dropped_before(&mut self, dropped_before: u64) {
        self.meta_mut().dropped_before = dropped_before;
    }

    pub(crate) fn meta(&self) -> &EventMeta {
        match self {
            Self::SessionOpened { meta, .. }
            | Self::SessionClosed { meta, .. }
            | Self::SessionState { meta, .. }
            | Self::TurnStarted { meta, .. }
            | Self::RequestStarted { meta, .. }
            | Self::RequestUsage { meta, .. }
            | Self::OutputDelta { meta, .. }
            | Self::ToolStarted { meta, .. }
            | Self::ToolPresentation { meta, .. }
            | Self::ToolProgress { meta, .. }
            | Self::ToolFinished { meta, .. }
            | Self::InteractionRequested { meta, .. }
            | Self::InteractionResolved { meta, .. }
            | Self::SteerProgress { meta, .. }
            | Self::TurnFinished { meta, .. } => meta,
        }
    }

    pub(crate) fn meta_mut(&mut self) -> &mut EventMeta {
        match self {
            Self::SessionOpened { meta, .. }
            | Self::SessionClosed { meta, .. }
            | Self::SessionState { meta, .. }
            | Self::TurnStarted { meta, .. }
            | Self::RequestStarted { meta, .. }
            | Self::RequestUsage { meta, .. }
            | Self::OutputDelta { meta, .. }
            | Self::ToolStarted { meta, .. }
            | Self::ToolPresentation { meta, .. }
            | Self::ToolProgress { meta, .. }
            | Self::ToolFinished { meta, .. }
            | Self::InteractionRequested { meta, .. }
            | Self::InteractionResolved { meta, .. }
            | Self::SteerProgress { meta, .. }
            | Self::TurnFinished { meta, .. } => meta,
        }
    }
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReasonView {
    User,
    OwnerDropped,
    Shutdown,
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatusView {
    Idle,
    Running,
    WaitingForInput,
    Finishing,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionBlockReasonView {
    Persistence,
    Internal,
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
    pub status: SessionStatusView,
    pub active_loop: Option<LoopStateView>,
    pub block_reason: Option<SessionBlockReasonView>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ModelErrorView {
    pub kind: String,
    pub delivery: String,
    pub retryable: bool,
    pub retry_after_millis: Option<u64>,
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
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TurnResultView {
    pub turn: TurnRef,
    pub outcome: LoopOutcomeView,
    pub usage: Usage,
    pub requests: u32,
    pub tool_rounds: u16,
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
pub struct HistoryPageView {
    pub items: Vec<IndexedHistoryItemView>,
    pub next_offset: Option<usize>,
    pub total: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexedHistoryItemView {
    pub index: usize,
    pub item: HistoryItemView,
}

impl From<&HistoryPage> for HistoryPageView {
    fn from(page: &HistoryPage) -> Self {
        Self {
            items: page
                .items
                .iter()
                .map(|item| IndexedHistoryItemView {
                    index: item.index,
                    item: item.item.clone(),
                })
                .collect(),
            next_offset: page.next_offset,
            total: page.total,
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
            status: match state.status {
                SessionStatus::Idle => SessionStatusView::Idle,
                SessionStatus::Running => SessionStatusView::Running,
                SessionStatus::WaitingForInput => SessionStatusView::WaitingForInput,
                SessionStatus::Finishing => SessionStatusView::Finishing,
                SessionStatus::Blocked => SessionStatusView::Blocked,
            },
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
            block_reason: state.block_reason.map(|reason| match reason {
                SessionBlockReason::Persistence => SessionBlockReasonView::Persistence,
                SessionBlockReason::Internal => SessionBlockReasonView::Internal,
            }),
        }
    }
}

impl Serialize for AgentEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::SessionOpened { session, meta } => serialize_event(
                serializer,
                "session_opened",
                SessionOpenedData {
                    session,
                    meta: *meta,
                },
            ),
            Self::SessionClosed { session_id, meta } => serialize_event(
                serializer,
                "session_closed",
                SessionClosedData {
                    session_id: *session_id,
                    meta: *meta,
                },
            ),
            Self::SessionState { state, meta } => serialize_event(
                serializer,
                "session_state",
                SessionStateData {
                    state: SessionStateView::from(state),
                    meta: *meta,
                },
            ),
            Self::TurnStarted { turn, meta } => {
                serialize_event(serializer, "turn_started", TurnData { turn, meta: *meta })
            }
            Self::RequestStarted {
                turn,
                request_index,
                config_revision,
                model,
                reasoning,
                meta,
            } => serialize_event(
                serializer,
                "request_started",
                RequestStartedData {
                    turn,
                    request_index: *request_index,
                    config_revision: *config_revision,
                    model,
                    reasoning: *reasoning,
                    meta: *meta,
                },
            ),
            Self::RequestUsage {
                turn,
                request_index,
                usage,
                meta,
            } => serialize_event(
                serializer,
                "request_usage",
                RequestUsageData {
                    turn,
                    request_index: *request_index,
                    usage,
                    meta: *meta,
                },
            ),
            Self::OutputDelta {
                turn,
                request_index,
                channel,
                delta,
                meta,
            } => serialize_event(
                serializer,
                "output_delta",
                OutputDeltaData {
                    turn,
                    request_index: *request_index,
                    channel,
                    delta,
                    meta: *meta,
                },
            ),
            Self::SteerProgress {
                turn,
                request_index,
                applied_count,
                meta,
            } => serialize_event(
                serializer,
                "steer_progress",
                SteerProgressData {
                    turn,
                    request_index: *request_index,
                    applied_count: *applied_count,
                    meta: *meta,
                },
            ),
            Self::ToolStarted {
                turn,
                request_index,
                tool_call_id,
                tool_name,
                meta,
            } => serialize_event(
                serializer,
                "tool_started",
                ToolStartedData {
                    turn,
                    request_index: *request_index,
                    tool_call_id,
                    tool_name,
                    meta: *meta,
                },
            ),
            Self::ToolPresentation {
                turn,
                request_index,
                tool_call_id,
                tool_name,
                display,
                meta,
            } => serialize_event(
                serializer,
                "tool_presentation",
                ToolPresentationData {
                    turn,
                    request_index: *request_index,
                    tool_call_id,
                    tool_name,
                    display,
                    meta: *meta,
                },
            ),
            Self::ToolProgress {
                turn,
                request_index,
                tool_call_id,
                progress,
                meta,
            } => serialize_event(
                serializer,
                "tool_progress",
                ToolProgressData {
                    turn,
                    request_index: *request_index,
                    tool_call_id,
                    progress,
                    meta: *meta,
                },
            ),
            Self::ToolFinished {
                turn,
                request_index,
                tool_call_id,
                result,
                meta,
            } => serialize_event(
                serializer,
                "tool_finished",
                ToolFinishedData {
                    turn,
                    request_index: *request_index,
                    tool_call_id,
                    result,
                    meta: *meta,
                },
            ),
            Self::InteractionRequested {
                turn,
                interaction,
                meta,
            } => serialize_event(
                serializer,
                "interaction_requested",
                InteractionRequestedData {
                    turn,
                    interaction,
                    meta: *meta,
                },
            ),
            Self::InteractionResolved {
                turn,
                interaction_id,
                meta,
            } => serialize_event(
                serializer,
                "interaction_resolved",
                InteractionResolvedData {
                    turn,
                    interaction_id: *interaction_id,
                    meta: *meta,
                },
            ),
            Self::TurnFinished {
                turn,
                outcome,
                persistence,
                meta,
            } => serialize_event(
                serializer,
                "turn_finished",
                TurnFinishedData {
                    turn,
                    outcome,
                    persistence: *persistence,
                    meta: *meta,
                },
            ),
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Serialize)]
struct EventWire<T> {
    #[serde(rename = "type")]
    event_type: &'static str,
    data: T,
}

fn serialize_event<S, T>(
    serializer: S,
    event_type: &'static str,
    data: T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    EventWire { event_type, data }.serialize(serializer)
}

#[derive(Serialize)]
struct SessionOpenedData<'a> {
    session: &'a SessionInfo,
    meta: EventMeta,
}

#[derive(Serialize)]
struct SessionClosedData {
    session_id: SessionId,
    meta: EventMeta,
}

#[derive(Serialize)]
struct SessionStateData {
    state: SessionStateView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct TurnData<'a> {
    turn: &'a TurnRef,
    meta: EventMeta,
}

#[derive(Serialize)]
struct RequestStartedData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    config_revision: ConfigRevision,
    model: &'a str,
    reasoning: minicore_runtime::model::ReasoningPreference,
    meta: EventMeta,
}

#[derive(Serialize)]
struct RequestUsageData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    usage: &'a minicore_runtime::model::Usage,
    meta: EventMeta,
}

#[derive(Serialize)]
struct SteerProgressData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    applied_count: u64,
    meta: EventMeta,
}

#[derive(Serialize)]
struct OutputDeltaData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    channel: &'a OutputChannel,
    delta: &'a str,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolStartedData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    tool_call_id: &'a ToolCallId,
    tool_name: &'a str,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolPresentationData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    tool_call_id: &'a ToolCallId,
    tool_name: &'a str,
    display: &'a crate::presentation::ToolDisplay,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolProgressData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    tool_call_id: &'a ToolCallId,
    progress: &'a ToolProgressView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolFinishedData<'a> {
    turn: &'a TurnRef,
    request_index: u32,
    tool_call_id: &'a ToolCallId,
    result: &'a ToolResultView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct InteractionRequestedData<'a> {
    turn: &'a TurnRef,
    interaction: &'a PendingInteractionView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct InteractionResolvedData<'a> {
    turn: &'a TurnRef,
    interaction_id: InteractionId,
    meta: EventMeta,
}

#[derive(Serialize)]
struct TurnFinishedData<'a> {
    turn: &'a TurnRef,
    outcome: &'a LoopOutcomeView,
    persistence: TurnPersistence,
    meta: EventMeta,
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
mod tests {
    use super::*;

    #[test]
    fn tool_result_debug_redacts_display_content() {
        let view = ToolResultView {
            outcome: ToolResultOutcome::Success,
            content_bytes: 42,
            content: Some("secret command output".to_owned()),
            content_truncated: true,
        };

        let debug = format!("{view:?}");
        assert!(!debug.contains("secret command output"));
        assert!(debug.contains("content_len"));
        assert!(debug.contains("content_truncated"));
    }

    #[test]
    fn tool_progress_debug_redacts_message() {
        let view = ToolProgressView {
            message: Some("secret progress path".to_owned()),
            completed: Some(1),
            total: Some(2),
        };
        let debug = format!("{view:?}");
        assert!(!debug.contains("secret progress path"));
        assert!(debug.contains("message_len"));
    }
}
