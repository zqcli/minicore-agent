use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Serialize, Serializer};
use tokio::sync::{Notify, mpsc};

use minicore_runtime::error::DiagnosticSummary;
use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, ToolCallId, TurnId};
use minicore_runtime::model::Usage;
use minicore_runtime::session::{
    InteractionKind, SessionEvent, SessionEventEnvelope, SessionHealth, SessionState, SessionStatus,
};
use minicore_runtime::tools::{ApprovalRisk, ToolInputAnswerKind, ToolProgress, ToolResultOutcome};

use crate::agent::{SessionInfo, TurnRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EventMeta {
    pub session_id: SessionId,
    pub instance_id: SessionInstanceId,
    pub dropped_before: u64,
}

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
    OutputDelta {
        turn: TurnRef,
        channel: OutputChannel,
        delta: String,
        meta: EventMeta,
    },
    ToolStarted {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        tool_name: String,
        meta: EventMeta,
    },
    ToolProgress {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        progress: ToolProgressView,
        meta: EventMeta,
    },
    ToolFinished {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        result: ToolResultView,
        meta: EventMeta,
    },
    InteractionRequested {
        session_id: SessionId,
        interaction: minicore_runtime::PendingInteraction,
        meta: EventMeta,
    },
    InteractionResolved {
        session_id: SessionId,
        interaction_id: InteractionId,
        meta: EventMeta,
    },
    TurnFinished {
        turn: TurnRef,
        outcome: minicore_runtime::TurnOutcome,
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

    fn meta(&self) -> &EventMeta {
        match self {
            Self::SessionOpened { meta, .. }
            | Self::SessionClosed { meta, .. }
            | Self::SessionState { meta, .. }
            | Self::TurnStarted { meta, .. }
            | Self::OutputDelta { meta, .. }
            | Self::ToolStarted { meta, .. }
            | Self::ToolProgress { meta, .. }
            | Self::ToolFinished { meta, .. }
            | Self::InteractionRequested { meta, .. }
            | Self::InteractionResolved { meta, .. }
            | Self::TurnFinished { meta, .. } => meta,
        }
    }

    fn meta_mut(&mut self) -> &mut EventMeta {
        match self {
            Self::SessionOpened { meta, .. }
            | Self::SessionClosed { meta, .. }
            | Self::SessionState { meta, .. }
            | Self::TurnStarted { meta, .. }
            | Self::OutputDelta { meta, .. }
            | Self::ToolStarted { meta, .. }
            | Self::ToolProgress { meta, .. }
            | Self::ToolFinished { meta, .. }
            | Self::InteractionRequested { meta, .. }
            | Self::InteractionResolved { meta, .. }
            | Self::TurnFinished { meta, .. } => meta,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentSendResult {
    Sent,
    Dropped,
    Closed,
}

#[derive(Clone)]
pub(crate) struct AgentEventSink {
    sender: mpsc::Sender<AgentEvent>,
    drop_state: Arc<Mutex<DropState>>,
}

struct DropState {
    pending: u64,
}

#[derive(Clone)]
pub(crate) struct CompletionCancellation {
    inner: Arc<CompletionCancellationState>,
}

struct CompletionCancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CompletionCancellation {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CompletionCancellationState {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::Release) {
            self.inner.notify.notify_waiters();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub(crate) async fn cancelled(&self) {
        loop {
            if self.inner.cancelled.load(Ordering::Acquire) {
                return;
            }
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
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
        let core_dropped = event.dropped_before();
        let reported = pending.pending.saturating_add(core_dropped);
        event.set_dropped_before(reported);
        match self.sender.try_send(event) {
            Ok(()) => {
                pending.pending = 0;
                AgentSendResult::Sent
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                pending.pending = pending
                    .pending
                    .saturating_add(core_dropped)
                    .saturating_add(1);
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

    pub(crate) async fn send_durable(
        &self,
        mut event: AgentEvent,
        cancellation: CompletionCancellation,
    ) -> bool {
        let reserved_drops = {
            let mut state = self
                .drop_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self.sender.is_closed() {
                return false;
            }
            let core_dropped = event.dropped_before();
            let reserved_drops = state.pending.saturating_add(core_dropped);
            event.set_dropped_before(reserved_drops);
            state.pending = 0;
            reserved_drops
        };
        let sent = tokio::select! {
            biased;
            _ = cancellation.cancelled() => false,
            result = self.sender.send(event) => result.is_ok(),
        };
        {
            let mut state = self
                .drop_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !sent {
                state.pending = state.pending.saturating_add(reserved_drops);
            }
        }
        sent
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputChannel {
    Text,
    Reasoning,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolProgressView {
    pub message: Option<String>,
    pub completed: Option<u64>,
    pub total: Option<u64>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ToolResultView {
    pub outcome: ToolResultOutcome,
    pub content_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ApprovalRiskView {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ToolInputAnswerKindView {
    Text,
    SingleChoice,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum InteractionKindView {
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ToolInputChoiceView {
    index: usize,
    text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PendingInteractionView {
    pub interaction_id: InteractionId,
    pub turn_id: TurnId,
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub kind: InteractionKindView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SessionStatusView {
    Idle,
    Running,
    WaitingForInput,
    Closing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SessionHealthView {
    Healthy,
    Degraded { diagnostic: DiagnosticSummary },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct TurnOutcomeView {
    pub turn_id: TurnId,
    pub terminal: minicore_runtime::TurnTerminal,
    pub usage: Usage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SessionStateView {
    pub session_id: SessionId,
    pub instance_id: SessionInstanceId,
    pub status: SessionStatusView,
    pub health: SessionHealthView,
    pub active_turn: Option<TurnId>,
    pub pending_interaction: Option<PendingInteractionView>,
    pub conversation_seq: minicore_runtime::ConversationSeq,
    pub last_terminal: Option<TurnOutcomeView>,
}

impl From<&SessionState> for SessionStateView {
    fn from(state: &SessionState) -> Self {
        Self {
            session_id: state.session_id,
            instance_id: state.instance_id,
            status: match state.status {
                SessionStatus::Idle => SessionStatusView::Idle,
                SessionStatus::Running => SessionStatusView::Running,
                SessionStatus::WaitingForInput => SessionStatusView::WaitingForInput,
                SessionStatus::Closing => SessionStatusView::Closing,
            },
            health: match &state.health {
                SessionHealth::Healthy => SessionHealthView::Healthy,
                SessionHealth::Degraded { diagnostic } => SessionHealthView::Degraded {
                    diagnostic: diagnostic.clone(),
                },
            },
            active_turn: state.active_turn,
            pending_interaction: state
                .pending_interaction
                .as_ref()
                .map(PendingInteractionView::from),
            conversation_seq: state.conversation_seq,
            last_terminal: state.last_terminal.as_ref().map(TurnOutcomeView::from),
        }
    }
}

impl From<&minicore_runtime::TurnOutcome> for TurnOutcomeView {
    fn from(outcome: &minicore_runtime::TurnOutcome) -> Self {
        Self {
            turn_id: outcome.turn_id,
            terminal: outcome.terminal.clone(),
            usage: outcome.usage,
        }
    }
}

impl From<&minicore_runtime::PendingInteraction> for PendingInteractionView {
    fn from(interaction: &minicore_runtime::PendingInteraction) -> Self {
        Self {
            interaction_id: interaction.interaction_id,
            turn_id: interaction.turn_id,
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
            Self::OutputDelta {
                turn,
                channel,
                delta,
                meta,
            } => serialize_event(
                serializer,
                "output_delta",
                OutputDeltaData {
                    turn,
                    channel,
                    delta,
                    meta: *meta,
                },
            ),
            Self::ToolStarted {
                turn,
                tool_call_id,
                tool_name,
                meta,
            } => serialize_event(
                serializer,
                "tool_started",
                ToolStartedData {
                    turn,
                    tool_call_id,
                    tool_name,
                    meta: *meta,
                },
            ),
            Self::ToolProgress {
                turn,
                tool_call_id,
                progress,
                meta,
            } => serialize_event(
                serializer,
                "tool_progress",
                ToolProgressData {
                    turn,
                    tool_call_id,
                    progress,
                    meta: *meta,
                },
            ),
            Self::ToolFinished {
                turn,
                tool_call_id,
                result,
                meta,
            } => serialize_event(
                serializer,
                "tool_finished",
                ToolFinishedData {
                    turn,
                    tool_call_id,
                    result,
                    meta: *meta,
                },
            ),
            Self::InteractionRequested {
                session_id,
                interaction,
                meta,
            } => serialize_event(
                serializer,
                "interaction_requested",
                InteractionRequestedData {
                    session_id: *session_id,
                    interaction: PendingInteractionView::from(interaction),
                    meta: *meta,
                },
            ),
            Self::InteractionResolved {
                session_id,
                interaction_id,
                meta,
            } => serialize_event(
                serializer,
                "interaction_resolved",
                InteractionResolvedData {
                    session_id: *session_id,
                    interaction_id: *interaction_id,
                    meta: *meta,
                },
            ),
            Self::TurnFinished {
                turn,
                outcome,
                meta,
            } => serialize_event(
                serializer,
                "turn_finished",
                TurnFinishedData {
                    turn,
                    outcome: TurnOutcomeView::from(outcome),
                    meta: *meta,
                },
            ),
        }
    }
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
struct OutputDeltaData<'a> {
    turn: &'a TurnRef,
    channel: &'a OutputChannel,
    delta: &'a str,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolStartedData<'a> {
    turn: &'a TurnRef,
    tool_call_id: &'a ToolCallId,
    tool_name: &'a str,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolProgressData<'a> {
    turn: &'a TurnRef,
    tool_call_id: &'a ToolCallId,
    progress: &'a ToolProgressView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct ToolFinishedData<'a> {
    turn: &'a TurnRef,
    tool_call_id: &'a ToolCallId,
    result: &'a ToolResultView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct InteractionRequestedData {
    session_id: SessionId,
    interaction: PendingInteractionView,
    meta: EventMeta,
}

#[derive(Serialize)]
struct InteractionResolvedData {
    session_id: SessionId,
    interaction_id: InteractionId,
    meta: EventMeta,
}

#[derive(Serialize)]
struct TurnFinishedData<'a> {
    turn: &'a TurnRef,
    outcome: TurnOutcomeView,
    meta: EventMeta,
}

pub(crate) fn map_session_event(envelope: SessionEventEnvelope) -> Option<AgentEvent> {
    let turn = |turn_id| TurnRef {
        session_id: envelope.session_id,
        instance_id: envelope.instance_id,
        turn_id,
    };
    let meta = EventMeta {
        session_id: envelope.session_id,
        instance_id: envelope.instance_id,
        dropped_before: envelope.dropped_before,
    };
    match envelope.event {
        SessionEvent::TurnStarted { turn_id } => Some(AgentEvent::TurnStarted {
            turn: turn(turn_id),
            meta,
        }),
        SessionEvent::OutputDelta {
            turn_id,
            channel,
            delta,
        } => Some(AgentEvent::OutputDelta {
            turn: turn(turn_id),
            channel: match channel {
                minicore_runtime::session::OutputChannel::Text => OutputChannel::Text,
                minicore_runtime::session::OutputChannel::Reasoning => OutputChannel::Reasoning,
            },
            delta: delta.as_str().to_owned(),
            meta,
        }),
        SessionEvent::ToolStarted {
            turn_id,
            tool_call_id,
            tool_name,
        } => Some(AgentEvent::ToolStarted {
            turn: turn(turn_id),
            tool_call_id,
            tool_name: tool_name.to_string(),
            meta,
        }),
        SessionEvent::ToolProgress {
            turn_id,
            tool_call_id,
            progress,
        } => Some(AgentEvent::ToolProgress {
            turn: turn(turn_id),
            tool_call_id,
            progress: ToolProgressView::from(&progress),
            meta,
        }),
        SessionEvent::ToolFinished {
            turn_id,
            tool_call_id,
            result,
        } => Some(AgentEvent::ToolFinished {
            turn: turn(turn_id),
            tool_call_id,
            result: ToolResultView {
                outcome: result.outcome,
                content_bytes: result.content_bytes,
            },
            meta,
        }),
        SessionEvent::InteractionRequested { interaction } => {
            Some(AgentEvent::InteractionRequested {
                session_id: envelope.session_id,
                interaction,
                meta,
            })
        }
        SessionEvent::InteractionResolved { interaction_id, .. } => {
            Some(AgentEvent::InteractionResolved {
                session_id: envelope.session_id,
                interaction_id,
                meta,
            })
        }
        SessionEvent::TurnFinished { .. }
        | SessionEvent::ModelStarted { .. }
        | SessionEvent::ModelFinished { .. }
        | SessionEvent::HealthChanged { .. } => None,
    }
}

pub(crate) fn forward_core_event(
    envelope: SessionEventEnvelope,
    event_sink: &AgentEventSink,
) -> bool {
    let dropped_before = envelope.dropped_before;
    if matches!(&envelope.event, SessionEvent::TurnFinished { .. }) {
        // SessionEventStream is best-effort. Core TurnFinished is only an observation;
        // authoritative completion comes from TurnHandle::wait. If Core drops this envelope,
        // its own unknown dropped_before count cannot be recovered or safely fabricated.
        event_sink.record_core_drops(dropped_before);
        return !event_sink.is_closed();
    }
    if let Some(event) = map_session_event(envelope) {
        event_sink.try_send(event) != AgentSendResult::Closed
    } else {
        event_sink.record_core_drops(dropped_before);
        !event_sink.is_closed()
    }
}

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
    use minicore_runtime::ids::{InteractionId, SessionId, ToolCallId, TurnId};
    use minicore_runtime::session::{InteractionKind, SessionHealth, SessionState, SessionStatus};
    use minicore_runtime::tools::{
        ApprovalRequest, ApprovalRisk, ToolInputAnswerKind, ToolInputRequest,
    };
    use minicore_runtime::value::BoundedText;
    use serde_json::json;
    use tokio::sync::mpsc;

    use super::*;

    fn ids() -> (SessionId, SessionInstanceId, TurnId) {
        (
            "ses_00000000000000000000000000000001".parse().unwrap(),
            "ins_00000000000000000000000000000001".parse().unwrap(),
            "trn_00000000000000000000000000000001".parse().unwrap(),
        )
    }

    fn meta(dropped_before: u64) -> EventMeta {
        let (session_id, instance_id, _) = ids();
        EventMeta {
            session_id,
            instance_id,
            dropped_before,
        }
    }

    fn turn() -> TurnRef {
        let (session_id, instance_id, turn_id) = ids();
        TurnRef {
            session_id,
            instance_id,
            turn_id,
        }
    }

    fn output(dropped_before: u64) -> AgentEvent {
        AgentEvent::OutputDelta {
            turn: turn(),
            channel: OutputChannel::Text,
            delta: "text".to_owned(),
            meta: meta(dropped_before),
        }
    }

    fn finish(dropped_before: u64) -> AgentEvent {
        AgentEvent::TurnFinished {
            turn: turn(),
            outcome: minicore_runtime::TurnOutcome {
                turn_id: ids().2,
                terminal: minicore_runtime::TurnTerminal::Completed,
                usage: Usage::default(),
            },
            meta: meta(dropped_before),
        }
    }

    fn interaction(kind: InteractionKind) -> minicore_runtime::PendingInteraction {
        let (_, _, turn_id) = ids();
        minicore_runtime::PendingInteraction {
            interaction_id: "int_00000000000000000000000000000001"
                .parse::<InteractionId>()
                .unwrap(),
            turn_id,
            tool_call_id: ToolCallId::new("call-1").unwrap(),
            tool_name: "write".parse().unwrap(),
            kind,
        }
    }

    fn approval_interaction() -> minicore_runtime::PendingInteraction {
        interaction(InteractionKind::Approval(
            ApprovalRequest::new("Allow write?", ApprovalRisk::High).unwrap(),
        ))
    }

    fn tool_input_interaction() -> minicore_runtime::PendingInteraction {
        interaction(InteractionKind::ToolInput(
            ToolInputRequest::new(
                "Choose a format",
                vec![
                    BoundedText::new("Markdown").unwrap(),
                    BoundedText::new("Plain text").unwrap(),
                ],
                ToolInputAnswerKind::SingleChoice,
            )
            .unwrap(),
        ))
    }

    #[test]
    fn approval_interaction_wire_is_bounded_and_answerable() {
        let (_, instance_id, turn_id) = ids();
        let pending = approval_interaction();
        let value = serde_json::to_value(AgentEvent::InteractionRequested {
            session_id: ids().0,
            interaction: pending,
            meta: meta(3),
        })
        .unwrap();
        assert_eq!(
            value,
            json!({
                "type": "interaction_requested",
                "data": {
                    "session_id": ids().0,
                    "interaction": {
                        "interaction_id": "int_00000000000000000000000000000001",
                        "turn_id": turn_id,
                        "tool_call_id": "call-1",
                        "tool_name": "write",
                        "kind": {
                            "type": "approval",
                            "data": {"prompt": "Allow write?", "risk": "high"}
                        }
                    },
                    "meta": {
                        "session_id": ids().0,
                        "instance_id": instance_id,
                        "dropped_before": 3
                    }
                }
            })
        );
        assert!(!value.to_string().contains("arguments"));
    }

    #[test]
    fn tool_input_interaction_wire_preserves_choice_shape_and_state_pending() {
        let (session_id, instance_id, turn_id) = ids();
        let pending = tool_input_interaction();
        let event = AgentEvent::InteractionRequested {
            session_id,
            interaction: pending.clone(),
            meta: meta(0),
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(
            value["data"]["interaction"]["kind"],
            json!({
                "type": "tool_input",
                "data": {
                    "prompt": "Choose a format",
                    "choices": [
                        {"index": 0, "text": "Markdown"},
                        {"index": 1, "text": "Plain text"}
                    ],
                    "answer_kind": "single_choice"
                }
            })
        );

        let state = SessionState {
            session_id,
            instance_id,
            status: SessionStatus::WaitingForInput,
            health: SessionHealth::Healthy,
            active_turn: Some(turn_id),
            pending_interaction: Some(pending),
            conversation_seq: minicore_runtime::ConversationSeq::ZERO,
            last_terminal: None,
        };
        let state_value = serde_json::to_value(AgentEvent::SessionState {
            state,
            meta: meta(2),
        })
        .unwrap();
        assert_eq!(
            state_value["data"]["state"]["pending_interaction"]["kind"],
            value["data"]["interaction"]["kind"]
        );
        assert_eq!(state_value["data"]["meta"]["dropped_before"], 2);
        assert!(!state_value.to_string().contains("arguments"));
    }

    #[test]
    fn core_event_mapping_preserves_complete_event_meta() {
        let (session_id, instance_id, turn_id) = ids();
        let mapped = map_session_event(SessionEventEnvelope {
            session_id,
            instance_id,
            dropped_before: 7,
            event: SessionEvent::TurnStarted { turn_id },
        })
        .unwrap();
        assert!(matches!(
            mapped,
            AgentEvent::TurnStarted {
                meta: EventMeta {
                    session_id: value_session,
                    instance_id: value_instance,
                    dropped_before: 7
                },
                ..
            } if value_session == session_id && value_instance == instance_id
        ));

        let requested = map_session_event(SessionEventEnvelope {
            session_id,
            instance_id,
            dropped_before: 8,
            event: SessionEvent::InteractionRequested {
                interaction: approval_interaction(),
            },
        })
        .unwrap();
        assert!(matches!(
            requested,
            AgentEvent::InteractionRequested {
                meta: EventMeta {
                    dropped_before: 8,
                    ..
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn outer_drop_accounting_adds_core_drop_and_reaches_durable_finish() {
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = AgentEventSink::new(sender);
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Sent);
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Dropped);
        assert_eq!(sink.try_send(output(2)), AgentSendResult::Dropped);
        let _ = receiver.recv().await.unwrap();
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Sent);
        let recovered = receiver.recv().await.unwrap();
        assert!(matches!(
            recovered,
            AgentEvent::OutputDelta {
                meta: EventMeta {
                    dropped_before: 4,
                    ..
                },
                ..
            }
        ));

        assert_eq!(sink.try_send(output(0)), AgentSendResult::Sent);
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Dropped);
        let durable = tokio::spawn({
            let sink = sink.clone();
            async move {
                sink.send_durable(finish(0), CompletionCancellation::new())
                    .await
            }
        });
        let _ = receiver.recv().await.unwrap();
        let durable_event = receiver.recv().await.unwrap();
        assert!(matches!(
            durable_event,
            AgentEvent::TurnFinished {
                meta: EventMeta {
                    dropped_before: 1,
                    ..
                },
                ..
            }
        ));
        assert!(durable.await.unwrap());
    }

    #[tokio::test]
    async fn durable_finish_reports_pending_core_drop_count() {
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = AgentEventSink::new(sender);
        sink.record_core_drops(11);
        assert!(
            sink.send_durable(finish(0), CompletionCancellation::new())
                .await
        );
        assert!(matches!(
            receiver.recv().await.unwrap(),
            AgentEvent::TurnFinished {
                meta: EventMeta {
                    dropped_before: 11,
                    ..
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn durable_send_finishes_when_receiver_is_dropped_or_session_is_cancelled() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let sink = AgentEventSink::new(sender);
        assert!(
            !sink
                .send_durable(finish(0), CompletionCancellation::new())
                .await
        );

        let (sender, mut receiver) = mpsc::channel(1);
        let sink = AgentEventSink::new(sender);
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Sent);
        let cancellation = CompletionCancellation::new();
        let task = tokio::spawn({
            let sink = sink.clone();
            let cancellation = cancellation.clone();
            async move { sink.send_durable(finish(0), cancellation).await }
        });
        cancellation.cancel();
        assert!(!task.await.unwrap());
        let _ = receiver.recv().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_durable_send_restores_pending_drops_and_clears_in_flight() {
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = AgentEventSink::new(sender);
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Sent);
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Dropped);
        let cancellation = CompletionCancellation::new();
        let task = tokio::spawn({
            let sink = sink.clone();
            let cancellation = cancellation.clone();
            async move { sink.send_durable(finish(0), cancellation).await }
        });
        cancellation.cancel();
        assert!(!task.await.unwrap());
        let _ = receiver.recv().await.unwrap();
        assert_eq!(sink.try_send(output(0)), AgentSendResult::Sent);
        let recovered = receiver.recv().await.unwrap();
        assert!(matches!(
            recovered,
            AgentEvent::OutputDelta {
                meta: EventMeta {
                    dropped_before: 1,
                    ..
                },
                ..
            }
        ));
        assert_eq!(
            sink.try_send(AgentEvent::SessionClosed {
                session_id: ids().0,
                meta: meta(0),
            }),
            AgentSendResult::Sent
        );
    }
}
