//! Shared test-only infrastructure. Production code observes these hooks only
//! under `cfg(test)`. Registries are keyed so tests running in parallel never
//! observe each other's faults or gates.

pub(crate) mod model;
pub(crate) use model::{ScriptedModel, Step};

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};

use tokio::sync::Notify;

/// Values armed by a test under a key and consumed by production code.
pub(crate) struct Registry<K, V = ()>(Mutex<Vec<(K, V)>>);

impl<K: PartialEq, V> Registry<K, V> {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    pub(crate) fn arm(&self, key: K, value: V) {
        self.entries().push((key, value));
    }

    /// Removes and returns the oldest value armed for `key`.
    pub(crate) fn take(&self, key: &K) -> Option<V> {
        let mut entries = self.entries();
        let index = entries.iter().position(|(candidate, _)| candidate == key)?;
        Some(entries.remove(index).1)
    }

    /// Returns the oldest value armed for `key` without consuming it.
    pub(crate) fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.entries()
            .iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, value)| value.clone())
    }

    fn entries(&self) -> MutexGuard<'_, Vec<(K, V)>> {
        self.0.lock().unwrap()
    }
}

impl<K: PartialEq> Registry<K> {
    /// Consumes one armed occurrence for `key`.
    pub(crate) fn hit(&self, key: &K) -> bool {
        self.take(key).is_some()
    }
}

/// A rendezvous that holds production code at a point until the test releases
/// it. Production code calls [`Gate::pass`] (async) or [`Gate::pass_blocking`]
/// (blocking worker threads); the test awaits [`Gate::wait_started`].
#[derive(Default)]
pub(crate) struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
    notify: Notify,
}

#[derive(Default)]
struct GateState {
    started: bool,
    released: bool,
}

impl Gate {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn pass(&self) {
        self.update(|state| state.started = true);
        self.wait_until(|state| state.released).await;
    }

    pub(crate) fn pass_blocking(&self) {
        self.update(|state| state.started = true);
        let state = self.state.lock().unwrap();
        drop(
            self.changed
                .wait_while(state, |state| !state.released)
                .unwrap(),
        );
    }

    pub(crate) async fn wait_started(&self) {
        self.wait_until(|state| state.started).await;
    }

    pub(crate) fn release(&self) {
        self.update(|state| state.released = true);
    }

    fn update(&self, change: impl FnOnce(&mut GateState)) {
        change(&mut self.state.lock().unwrap());
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

/// A uniquely named, canonicalized temporary directory removed on drop.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "minicore-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
