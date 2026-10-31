#[cfg(test)]
use std::sync::Condvar;
use std::sync::Mutex;
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct Gate {
    state: Mutex<GateState>,
    #[cfg(test)]
    changed: Condvar,
    notify: Notify,
}

#[derive(Default)]
struct GateState {
    #[cfg(test)]
    started: bool,
    released: bool,
}

impl Gate {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn pass(&self) {
        #[cfg(test)]
        self.update(|state| state.started = true);
        self.wait_until(|state| state.released).await;
    }

    #[cfg(test)]
    pub(crate) fn pass_blocking(&self) {
        self.update(|state| state.started = true);
        let state = self.state.lock().unwrap();
        drop(
            self.changed
                .wait_while(state, |state| !state.released)
                .unwrap(),
        );
    }

    #[cfg(test)]
    pub(crate) async fn wait_started(&self) {
        self.wait_until(|state| state.started).await;
    }

    pub(crate) fn release(&self) {
        self.update(|state| state.released = true);
    }

    fn update(&self, change: impl FnOnce(&mut GateState)) {
        change(&mut self.state.lock().unwrap());
        #[cfg(test)]
        self.changed.notify_all();
        self.notify.notify_waiters();
    }

    async fn wait_until(&self, done: impl Fn(&GateState) -> bool) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if done(&self.state.lock().unwrap()) {
                return;
            }
            notified.await;
        }
    }
}
