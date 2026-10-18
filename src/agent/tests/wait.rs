use super::*;
use std::time::Duration;

impl crate::event::AgentEventStream {
    pub(super) async fn next_event(&mut self, timeout: Duration, message: &str) -> AgentEvent {
        tokio::time::timeout(timeout, self.recv())
            .await
            .expect(message)
            .expect("event stream must remain open")
    }

    pub(super) async fn expect<T>(
        &mut self,
        timeout: Duration,
        message: &str,
        mut matching: impl FnMut(AgentEvent) -> Option<T>,
    ) -> T {
        tokio::time::timeout(timeout, async {
            loop {
                let event = self.recv().await.expect("event stream must remain open");
                if let Some(value) = matching(event) {
                    return value;
                }
            }
        })
        .await
        .expect(message)
    }
}

pub(super) async fn wait_until(timeout: Duration, message: &str, mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(timeout, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect(message);
}
