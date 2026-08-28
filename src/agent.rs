use tokio::sync::mpsc;

use crate::config::AgentConfig;
use crate::error::AgentError;
use crate::event::{AgentEvent, AgentEventStream};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PingResponse {
    pub version: &'static str,
}

pub struct Agent {
    config: AgentConfig,
    events_tx: mpsc::Sender<AgentEvent>,
    events_rx: Option<mpsc::Receiver<AgentEvent>>,
}

impl Agent {
    pub async fn open(config: AgentConfig) -> Result<Self, AgentError> {
        config.validate().map_err(AgentError::Config)?;
        let (events_tx, events_rx) = mpsc::channel(config.event_capacity);
        Ok(Self {
            config,
            events_tx,
            events_rx: Some(events_rx),
        })
    }

    pub fn config(&self) -> &AgentConfig {
        &self.config
    }

    pub fn take_events(&mut self) -> Result<AgentEventStream, AgentError> {
        self.events_rx
            .take()
            .map(AgentEventStream::new)
            .ok_or(AgentError::EventStreamTaken)
    }

    pub const fn ping(&self) -> PingResponse {
        PingResponse { version: VERSION }
    }

    pub async fn shutdown(self) -> Result<(), AgentError> {
        drop(self.events_tx);
        Ok(())
    }
}

pub const fn agent_version() -> &'static str {
    VERSION
}
