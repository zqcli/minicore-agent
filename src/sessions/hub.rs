//! Reliable publication of the single control snapshot per loaded Session.
//!
//! Execution paths only mark a Session dirty; one publisher task samples,
//! reserves before sampling and hands the charge to the reliable writer lane.
//! Nothing here runs while a Session's `inner`/`queue` locks are held.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::event::{AgentEvent, AgentEventSink, EventMeta};
use crate::ids::LoadId;

/// Most Sessions one publication round samples, so no Session starves another.
const MAX_BATCH: usize = 16;

#[cfg(test)]
pub(crate) static SNAPSHOT_PUBLISH_GATES: crate::test_support::Registry<
    LoadId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

/// Ceiling for control snapshots held, encoded, or queued for the wire.
pub(crate) const SNAPSHOT_BUDGET_BYTES: usize = 320 * 1024 * 1024;
pub(crate) const SNAPSHOT_RESERVATION_BYTES: usize =
    2 * crate::rpc::MAX_RPC_FRAME_BYTES + 4096 + crate::rpc::WRITER_BUFFER_BYTES;

/// Byte budget for control snapshots in memory or waiting for the wire.
struct SnapshotBudget {
    limit: usize,
    used: AtomicUsize,
    notify: Notify,
}

/// One charge of the snapshot budget, released when the frame is consumed.
pub(crate) struct SnapshotCharge {
    budget: Arc<SnapshotBudget>,
    bytes: usize,
    #[cfg(feature = "e2e-observe")]
    key: Option<crate::e2e_observe::SnapshotKey>,
}

impl SnapshotCharge {
    pub(crate) fn shrink(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes);
        #[cfg(feature = "e2e-observe")]
        crate::e2e_observe::sub(crate::e2e_observe::Gauge::ControlBytes, self.bytes - bytes);
        self.budget
            .used
            .fetch_sub(self.bytes - bytes, Ordering::SeqCst);
        self.bytes = bytes;
        self.budget.notify.notify_waiters();
    }

    #[cfg(feature = "e2e-observe")]
    pub(crate) fn observe(&mut self, snapshot: &crate::SessionSnapshot) {
        self.key = Some(crate::e2e_observe::SnapshotKey {
            session_id: snapshot.session_id,
            load: snapshot.load,
            revision: snapshot.revision,
        });
    }

    #[cfg(feature = "e2e-observe")]
    pub(crate) fn observed_key(&self) -> Option<crate::e2e_observe::SnapshotKey> {
        self.key
    }
}

impl Drop for SnapshotCharge {
    fn drop(&mut self) {
        #[cfg(feature = "e2e-observe")]
        {
            crate::e2e_observe::sub(crate::e2e_observe::Gauge::ControlBytes, self.bytes);
            crate::e2e_observe::sub(crate::e2e_observe::Gauge::Controls, 1);
        }
        self.budget.used.fetch_sub(self.bytes, Ordering::SeqCst);
        self.budget.notify.notify_waiters();
    }
}

impl SnapshotBudget {
    async fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        stopped: &CancellationToken,
    ) -> Option<SnapshotCharge> {
        assert!(bytes <= self.limit);
        loop {
            if stopped.is_cancelled() {
                return None;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(charge) = self.try_reserve(bytes) {
                return Some(charge);
            }
            tokio::select! {
                _ = notified => {}
                _ = stopped.cancelled() => return None,
            }
        }
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<SnapshotCharge> {
        let mut used = self.used.load(Ordering::SeqCst);
        loop {
            if used.checked_add(bytes)? > self.limit {
                return None;
            }
            match self.used.compare_exchange_weak(
                used,
                used + bytes,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    #[cfg(feature = "e2e-observe")]
                    {
                        crate::e2e_observe::add(crate::e2e_observe::Gauge::ControlBytes, bytes);
                        crate::e2e_observe::add(crate::e2e_observe::Gauge::Controls, 1);
                    }
                    return Some(SnapshotCharge {
                        budget: Arc::clone(self),
                        bytes,
                        #[cfg(feature = "e2e-observe")]
                        key: None,
                    });
                }
                Err(current) => used = current,
            }
        }
    }
}

struct Dirty {
    marks: HashSet<LoadId>,
    /// Marked loads in mark order, at most one entry per marked load.
    ready: VecDeque<LoadId>,
    sessions: HashMap<LoadId, Weak<super::SessionShared>>,
}

/// Per-Agent dirty set and publisher handle shared by every loaded Session.
pub(crate) struct SnapshotHub {
    dirty: Mutex<Dirty>,
    budget: Arc<SnapshotBudget>,
    events: AgentEventSink,
    notify: Notify,
    stopped: CancellationToken,
    listening: AtomicBool,
    #[cfg(test)]
    pending_publishes: AtomicUsize,
}

impl SnapshotHub {
    pub(crate) fn new(events: AgentEventSink) -> Arc<Self> {
        Arc::new(Self {
            dirty: Mutex::new(Dirty {
                marks: HashSet::new(),
                ready: VecDeque::new(),
                sessions: HashMap::new(),
            }),
            budget: Arc::new(SnapshotBudget {
                limit: SNAPSHOT_BUDGET_BYTES,
                used: AtomicUsize::new(0),
                notify: Notify::new(),
            }),
            events,
            notify: Notify::new(),
            stopped: CancellationToken::new(),
            listening: AtomicBool::new(false),
            #[cfg(test)]
            pending_publishes: AtomicUsize::new(0),
        })
    }

    /// Registers a loaded Session and publishes its initial state.
    pub(super) fn register(&self, load: LoadId, session: &super::Session) {
        {
            let mut dirty = self.dirty.lock().unwrap_or_else(|error| error.into_inner());
            dirty.sessions.insert(load, Arc::downgrade(&session.shared));
        }
        self.mark(load);
    }

    /// Drops a closed Session and any control state still pending for it.
    pub(super) fn unregister(&self, load: LoadId) {
        let mut dirty = self.dirty.lock().unwrap_or_else(|error| error.into_inner());
        dirty.sessions.remove(&load);
        dirty.marks.remove(&load);
        dirty.ready.retain(|candidate| *candidate != load);
    }

    pub(crate) fn try_reserve(&self) -> Option<SnapshotCharge> {
        self.budget.try_reserve(SNAPSHOT_RESERVATION_BYTES)
    }

    pub(crate) fn listen(&self) {
        self.listening.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn used_for_test(&self) -> usize {
        self.budget.used.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn idle_for_test(&self) -> bool {
        let dirty = self.dirty.lock().unwrap();
        dirty.marks.is_empty() && self.pending_publishes.load(Ordering::SeqCst) == 0
    }

    pub(crate) fn shutdown_now(&self) {
        self.stopped.cancel();
        self.notify.notify_waiters();
    }

    /// Publishes this load's current control state on the next round.
    pub(crate) fn mark(&self, load: LoadId) {
        let mut dirty = self.dirty.lock().unwrap_or_else(|error| error.into_inner());
        if !dirty.sessions.contains_key(&load) {
            return;
        }
        if dirty.marks.insert(load) {
            dirty.ready.push_back(load);
        }
        drop(dirty);
        self.notify.notify_one();
    }

    /// Samples and publishes dirty Sessions until no mark remains.
    pub(crate) async fn run(self: Arc<Self>) {
        let _stopped = self.stopped.clone().drop_guard();
        loop {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = self.stopped.cancelled() => return,
            }
            if !self.listening.load(Ordering::SeqCst) {
                continue;
            }
            loop {
                let batch = self.take_batch();
                if batch.is_empty() {
                    break;
                }
                for (load, session) in batch {
                    if self.stopped.is_cancelled() {
                        return;
                    }
                    let sent = self.publish(load, session).await;
                    #[cfg(test)]
                    self.pending_publishes.fetch_sub(1, Ordering::SeqCst);
                    if !sent {
                        return;
                    }
                }
            }
        }
    }

    pub(super) async fn publish_final(&self, session: super::Session) {
        if self.listening.load(Ordering::SeqCst) {
            self.publish(session.load(), Arc::downgrade(&session.shared))
                .await;
        }
    }

    fn take_batch(&self) -> Vec<(LoadId, Weak<super::SessionShared>)> {
        let mut dirty = self.dirty.lock().unwrap_or_else(|error| error.into_inner());
        let mut batch = Vec::new();
        while batch.len() < MAX_BATCH {
            // A load removed here is no longer marked, so a mark during
            // sampling re-enters the set instead of being cleared afterwards.
            let Some(load) = dirty.ready.pop_front() else {
                break;
            };
            dirty.marks.remove(&load);
            let Some(session) = dirty.sessions.get(&load).cloned() else {
                continue;
            };
            batch.push((load, session));
            #[cfg(test)]
            self.pending_publishes.fetch_add(1, Ordering::SeqCst);
        }
        batch
    }

    /// Returns `false` once the reliable lane is closed.
    async fn publish(&self, load: LoadId, session: Weak<super::SessionShared>) -> bool {
        let Some(mut charge) = self
            .budget
            .reserve(SNAPSHOT_RESERVATION_BYTES, &self.stopped)
            .await
        else {
            return false;
        };
        let Some(session) = session.upgrade().map(super::Session::from_shared) else {
            return true;
        };
        let prepared = tokio::task::spawn_blocking(move || {
            let snapshot = session.sample()?;
            #[cfg(feature = "e2e-observe")]
            charge.observe(&snapshot);
            let session_id = session.session_id();
            drop(session);
            #[cfg(test)]
            if let Some(gate) = SNAPSHOT_PUBLISH_GATES.take(&load) {
                gate.pass_blocking();
            }
            let event = AgentEvent::SessionSnapshot {
                snapshot,
                meta: EventMeta::session(session_id, load),
            };
            let bytes = crate::event::snapshot_frame_len(&event);
            if bytes + 1 > crate::rpc::MAX_RPC_FRAME_BYTES {
                return Err(crate::AgentError::Internal);
            }
            charge.shrink(2 * bytes + 4096 + crate::rpc::WRITER_BUFFER_BYTES);
            Ok((event, charge))
        })
        .await;
        let (event, charge) = match prepared {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(crate::AgentError::StaleLoad)) => return true,
            _ => {
                self.events.close_control();
                return false;
            }
        };
        self.events
            .send_snapshot(event, charge, &self.stopped)
            .await
    }
}

#[cfg(test)]
mod tests;
