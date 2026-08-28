use serde::{Serialize, Serializer};
use tokio::sync::mpsc;

use minicore_runtime::error::DiagnosticSummary;
use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, ToolCallId, TurnId};
use minicore_runtime::model::Usage;
use minicore_runtime::session::{
    InteractionKind, SessionEvent, SessionEventEnvelope, SessionHealth, SessionState, SessionStatus,
};
use minicore_runtime::tools::{ApprovalRisk, ToolInputAnswerKind, ToolProgress, ToolResultOutcome};

use crate::agent::{SessionInfo, TurnRef};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentEvent {
    SessionOpened {
        session: SessionInfo,
    },
    SessionClosed {
        session_id: SessionId,
    },
    SessionState {
        state: SessionState,
    },
    TurnStarted {
        turn: TurnRef,
    },
    OutputDelta {
        turn: TurnRef,
        channel: OutputChannel,
        delta: String,
        dropped_before: u64,
    },
    ToolStarted {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        tool_name: String,
        dropped_before: u64,
    },
    ToolProgress {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        progress: ToolProgressView,
        dropped_before: u64,
    },
    ToolFinished {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        result: ToolResultView,
        dropped_before: u64,
    },
    InteractionRequested {
        session_id: SessionId,
        interaction: minicore_runtime::PendingInteraction,
    },
    InteractionResolved {
        session_id: SessionId,
        interaction_id: InteractionId,
    },
    TurnFinished {
        turn: TurnRef,
        outcome: minicore_runtime::TurnOutcome,
    },
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
        risk: ApprovalRiskView,
    },
    ToolInput {
        prompt_bytes: usize,
        choice_count: usize,
        answer_kind: ToolInputAnswerKindView,
    },
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
                    risk: match request.risk {
                        ApprovalRisk::Low => ApprovalRiskView::Low,
                        ApprovalRisk::Medium => ApprovalRiskView::Medium,
                        ApprovalRisk::High => ApprovalRiskView::High,
                    },
                },
                InteractionKind::ToolInput(request) => InteractionKindView::ToolInput {
                    prompt_bytes: request.prompt.byte_len(),
                    choice_count: request.choices.len(),
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
            Self::SessionOpened { session } => {
                serialize_event(serializer, "session_opened", SessionOpenedData { session })
            }
            Self::SessionClosed { session_id } => serialize_event(
                serializer,
                "session_closed",
                SessionClosedData {
                    session_id: *session_id,
                },
            ),
            Self::SessionState { state } => serialize_event(
                serializer,
                "session_state",
                SessionStateData {
                    state: SessionStateView::from(state),
                },
            ),
            Self::TurnStarted { turn } => {
                serialize_event(serializer, "turn_started", TurnData { turn })
            }
            Self::OutputDelta {
                turn,
                channel,
                delta,
                dropped_before,
            } => serialize_event(
                serializer,
                "output_delta",
                OutputDeltaData {
                    turn,
                    channel,
                    delta,
                    dropped_before: *dropped_before,
                },
            ),
            Self::ToolStarted {
                turn,
                tool_call_id,
                tool_name,
                dropped_before,
            } => serialize_event(
                serializer,
                "tool_started",
                ToolStartedData {
                    turn,
                    tool_call_id,
                    tool_name,
                    dropped_before: *dropped_before,
                },
            ),
            Self::ToolProgress {
                turn,
                tool_call_id,
                progress,
                dropped_before,
            } => serialize_event(
                serializer,
                "tool_progress",
                ToolProgressData {
                    turn,
                    tool_call_id,
                    progress,
                    dropped_before: *dropped_before,
                },
            ),
            Self::ToolFinished {
                turn,
                tool_call_id,
                result,
                dropped_before,
            } => serialize_event(
                serializer,
                "tool_finished",
                ToolFinishedData {
                    turn,
                    tool_call_id,
                    result,
                    dropped_before: *dropped_before,
                },
            ),
            Self::InteractionRequested {
                session_id,
                interaction,
            } => serialize_event(
                serializer,
                "interaction_requested",
                InteractionRequestedData {
                    session_id: *session_id,
                    interaction: PendingInteractionView::from(interaction),
                },
            ),
            Self::InteractionResolved {
                session_id,
                interaction_id,
            } => serialize_event(
                serializer,
                "interaction_resolved",
                InteractionResolvedData {
                    session_id: *session_id,
                    interaction_id: *interaction_id,
                },
            ),
            Self::TurnFinished { turn, outcome } => serialize_event(
                serializer,
                "turn_finished",
                TurnFinishedData {
                    turn,
                    outcome: TurnOutcomeView::from(outcome),
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
}

#[derive(Serialize)]
struct SessionClosedData {
    session_id: SessionId,
}

#[derive(Serialize)]
struct SessionStateData {
    state: SessionStateView,
}

#[derive(Serialize)]
struct TurnData<'a> {
    turn: &'a TurnRef,
}

#[derive(Serialize)]
struct OutputDeltaData<'a> {
    turn: &'a TurnRef,
    channel: &'a OutputChannel,
    delta: &'a str,
    dropped_before: u64,
}

#[derive(Serialize)]
struct ToolStartedData<'a> {
    turn: &'a TurnRef,
    tool_call_id: &'a ToolCallId,
    tool_name: &'a str,
    dropped_before: u64,
}

#[derive(Serialize)]
struct ToolProgressData<'a> {
    turn: &'a TurnRef,
    tool_call_id: &'a ToolCallId,
    progress: &'a ToolProgressView,
    dropped_before: u64,
}

#[derive(Serialize)]
struct ToolFinishedData<'a> {
    turn: &'a TurnRef,
    tool_call_id: &'a ToolCallId,
    result: &'a ToolResultView,
    dropped_before: u64,
}

#[derive(Serialize)]
struct InteractionRequestedData {
    session_id: SessionId,
    interaction: PendingInteractionView,
}

#[derive(Serialize)]
struct InteractionResolvedData {
    session_id: SessionId,
    interaction_id: InteractionId,
}

#[derive(Serialize)]
struct TurnFinishedData<'a> {
    turn: &'a TurnRef,
    outcome: TurnOutcomeView,
}

pub(crate) fn map_session_event(envelope: SessionEventEnvelope) -> Option<AgentEvent> {
    let turn = |turn_id| TurnRef {
        session_id: envelope.session_id,
        instance_id: envelope.instance_id,
        turn_id,
    };
    let dropped_before = envelope.dropped_before;
    match envelope.event {
        SessionEvent::TurnStarted { turn_id } => Some(AgentEvent::TurnStarted {
            turn: turn(turn_id),
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
            dropped_before,
        }),
        SessionEvent::ToolStarted {
            turn_id,
            tool_call_id,
            tool_name,
        } => Some(AgentEvent::ToolStarted {
            turn: turn(turn_id),
            tool_call_id,
            tool_name: tool_name.to_string(),
            dropped_before,
        }),
        SessionEvent::ToolProgress {
            turn_id,
            tool_call_id,
            progress,
        } => Some(AgentEvent::ToolProgress {
            turn: turn(turn_id),
            tool_call_id,
            progress: ToolProgressView::from(&progress),
            dropped_before,
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
            dropped_before,
        }),
        SessionEvent::InteractionRequested { interaction } => {
            Some(AgentEvent::InteractionRequested {
                session_id: envelope.session_id,
                interaction,
            })
        }
        SessionEvent::InteractionResolved { interaction_id, .. } => {
            Some(AgentEvent::InteractionResolved {
                session_id: envelope.session_id,
                interaction_id,
            })
        }
        SessionEvent::TurnFinished { .. }
        | SessionEvent::ModelStarted { .. }
        | SessionEvent::ModelFinished { .. }
        | SessionEvent::HealthChanged { .. } => None,
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
