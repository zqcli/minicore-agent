use tokio::sync::mpsc;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub enum AgentEvent {}

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
