use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::AgentError;

/// Keeps an owned task joinable when an awaiting caller is cancelled.
#[derive(Default)]
pub(crate) struct OwnedTask(Mutex<Option<JoinHandle<()>>>);

impl OwnedTask {
    pub(crate) fn new(handle: JoinHandle<()>) -> Self {
        Self(Mutex::new(Some(handle)))
    }

    pub(crate) fn set(&self, handle: JoinHandle<()>) {
        *self.0.try_lock().expect("a fresh task is unshared") = Some(handle);
    }

    pub(crate) fn is_finished(&self) -> bool {
        match self.0.try_lock() {
            Ok(slot) => slot.as_ref().is_none_or(|handle| handle.is_finished()),
            Err(_) => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn abort(&self) {
        if let Some(handle) = self.0.try_lock().unwrap().as_ref() {
            handle.abort();
        }
    }

    pub(crate) async fn join(&self) -> Result<(), AgentError> {
        let mut slot = self.0.lock().await;
        let Some(handle) = slot.as_mut() else {
            return Ok(());
        };
        let result = std::pin::Pin::new(handle).await;
        slot.take();
        result.map_err(|_| AgentError::Internal)
    }
}
