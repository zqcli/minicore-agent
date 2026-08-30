use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use minicore_runtime::config::SessionSpec;
use minicore_runtime::error::SessionShutdownError;
use minicore_runtime::ids::{SessionId, TurnId};
use minicore_runtime::session::{
    SessionEventStream, SessionHandle, SessionRuntime, SessionState, TurnHandle,
};
use minicore_runtime::storage::SessionLogErrorKind;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agent::{SessionInfo, TurnRef};
use crate::error::{AgentError, StoreError};
use crate::event::{AgentEvent, AgentEventSink, AgentSendResult, EventMeta, forward_core_event};
use crate::store::{SessionRecord, Store};

const COMPLETION_CHANNEL_CAPACITY: usize = 1;

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, LoadedSession>,
}

pub(crate) struct LoadedSession {
    pub(crate) record: SessionRecord,
    pub(crate) spec: SessionSpec,
    pub(crate) runtime: SessionRuntime,
    pub(crate) handle: SessionHandle,
    pub(crate) active_turn: Option<ActiveTurn>,
    pub(crate) pump: SessionPump,
    pub(crate) metadata: MetadataWorker,
    pub(crate) event_sink: AgentEventSink,
}

pub(crate) struct ActiveTurn {
    pub(crate) handle: TurnHandle,
    pub(crate) completion_task: JoinHandle<()>,
}

pub(crate) struct CompletionReady {
    pub(crate) turn_ref: TurnRef,
    pub(crate) outcome: minicore_runtime::TurnOutcome,
}

pub(crate) struct SessionPump {
    completion_tx: mpsc::Sender<CompletionReady>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

#[cfg(test)]
pub(crate) struct CompletionCapacityGuard {
    _permit: mpsc::OwnedPermit<CompletionReady>,
}

#[cfg(test)]
pub(crate) struct SessionPumpStartupGate {
    pub(crate) started: Arc<tokio::sync::Semaphore>,
    pub(crate) release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl SessionPumpStartupGate {
    pub(crate) fn new() -> Self {
        Self {
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
type SessionPumpStartupGateList = Mutex<Vec<(std::path::PathBuf, Arc<SessionPumpStartupGate>)>>;

#[cfg(test)]
static SESSION_PUMP_STARTUP_GATES: OnceLock<SessionPumpStartupGateList> = OnceLock::new();

impl SessionPump {
    pub(crate) fn new(
        task_runtime: &Handle,
        event_stream: SessionEventStream,
        state: watch::Receiver<SessionState>,
        event_sink: AgentEventSink,
        opened: SessionInfo,
    ) -> Self {
        let (completion_tx, completion_rx) = mpsc::channel(COMPLETION_CHANNEL_CAPACITY);
        let stop = CancellationToken::new();
        let task = task_runtime.spawn(run_session_pump(
            event_stream,
            state,
            completion_rx,
            event_sink,
            opened,
            stop.clone(),
        ));
        Self {
            completion_tx,
            stop,
            task,
        }
    }

    pub(crate) fn completion_sender(&self) -> mpsc::Sender<CompletionReady> {
        self.completion_tx.clone()
    }

    pub(crate) fn stop_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }

    #[cfg(test)]
    pub(crate) fn completion_capacity(&self) -> usize {
        self.completion_tx.max_capacity()
    }

    #[cfg(test)]
    async fn reserve_completion_capacity(&self) -> CompletionCapacityGuard {
        CompletionCapacityGuard {
            _permit: self.completion_tx.clone().reserve_owned().await.unwrap(),
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        let Self {
            completion_tx,
            stop,
            task,
        } = self;
        stop.cancel();
        drop(completion_tx);
        task.await.map_err(|_| AgentError::Internal)
    }
}

async fn run_session_pump(
    mut event_stream: SessionEventStream,
    mut state: watch::Receiver<SessionState>,
    mut completions: mpsc::Receiver<CompletionReady>,
    event_sink: AgentEventSink,
    opened: SessionInfo,
    stop: CancellationToken,
) {
    #[cfg(test)]
    let startup_gate = take_session_pump_startup_gate(&opened.workspace);
    let mut last_emitted_state = None;
    let session_id = opened.session_id;
    let Some(instance_id) = opened.instance_id else {
        stop.cancel();
        return;
    };
    if event_sink.try_send(AgentEvent::SessionOpened {
        session: opened,
        meta: EventMeta {
            session_id,
            instance_id,
            dropped_before: 0,
        },
    }) == AgentSendResult::Closed
        || !emit_latest_state(&mut state, &event_sink, &mut last_emitted_state)
    {
        stop.cancel();
        return;
    }
    #[cfg(test)]
    if let Some(gate) = startup_gate {
        gate.started.add_permits(1);
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            permit = gate.release.acquire() => {
                if let Ok(permit) = permit {
                    permit.forget();
                }
            }
        }
    }

    loop {
        let keep_running = tokio::select! {
            biased;
            _ = stop.cancelled() => false,
            _ = event_sink.closed() => false,
            changed = state.changed() => {
                changed.is_ok()
                    && emit_latest_state(&mut state, &event_sink, &mut last_emitted_state)
            }
            envelope = event_stream.recv() => match envelope {
                Some(envelope) => forward_core_event(envelope, &event_sink),
                None => false,
            },
            ready = completions.recv() => match ready {
                Some(ready) => {
                    event_sink.try_send(AgentEvent::TurnFinished {
                        turn: ready.turn_ref,
                        outcome: ready.outcome,
                        meta: EventMeta {
                            session_id: ready.turn_ref.session_id,
                            instance_id: ready.turn_ref.instance_id,
                            dropped_before: 0,
                        },
                    }) != AgentSendResult::Closed
                }
                None => true,
            },
        };
        if !keep_running {
            break;
        }
    }
    stop.cancel();
}

fn emit_latest_state(
    state: &mut watch::Receiver<SessionState>,
    event_sink: &AgentEventSink,
    last_emitted_state: &mut Option<SessionState>,
) -> bool {
    let latest = state.borrow_and_update().clone();
    if last_emitted_state.as_ref() == Some(&latest) {
        return !event_sink.is_closed();
    }
    let result = event_sink.try_send(AgentEvent::SessionState {
        meta: EventMeta {
            session_id: latest.session_id,
            instance_id: latest.instance_id,
            dropped_before: 0,
        },
        state: latest.clone(),
    });
    if result == AgentSendResult::Sent {
        *last_emitted_state = Some(latest);
    }
    result != AgentSendResult::Closed
}

pub(crate) struct MetadataWorker {
    sender: watch::Sender<Option<String>>,
    cancellation: CancellationToken,
    failed: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl MetadataWorker {
    pub(crate) fn new(task_runtime: &Handle, store: Store, session_id: SessionId) -> Self {
        let (sender, receiver) = watch::channel(None);
        let cancellation = CancellationToken::new();
        let failed = Arc::new(AtomicBool::new(false));
        let task = task_runtime.spawn(metadata_worker(
            receiver,
            store,
            session_id,
            cancellation.clone(),
            Arc::clone(&failed),
        ));
        Self {
            sender,
            cancellation,
            failed,
            task,
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn stop_signal(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) fn update(&self, updated_at: String) -> bool {
        if self.is_failed() || self.cancellation.is_cancelled() || self.sender.is_closed() {
            return false;
        }
        let _ = self.sender.send_replace(Some(updated_at));
        !self.is_failed() && !self.sender.is_closed()
    }

    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        let Self {
            sender,
            cancellation,
            failed: _,
            task,
        } = self;
        drop(sender);
        cancellation.cancel();
        task.await.map_err(|_| AgentError::Internal)
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }
}

async fn metadata_worker(
    mut receiver: watch::Receiver<Option<String>>,
    store: Store,
    session_id: SessionId,
    cancellation: CancellationToken,
    failed: Arc<AtomicBool>,
) {
    loop {
        let changed = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = receiver.changed() => result.is_ok(),
        };
        if !changed {
            return;
        }
        let Some(updated_at) = receiver.borrow_and_update().clone() else {
            continue;
        };
        if cancellation.is_cancelled() {
            return;
        }
        let result = store.touch_at(session_id, updated_at).await;
        if cancellation.is_cancelled() {
            return;
        }
        match result {
            Ok(()) => {}
            Err(error) if metadata_error_is_retryable(&error) => continue,
            Err(error) => {
                tracing::warn!(
                    session_id = %session_id,
                    error_kind = error.kind(),
                    "metadata update failed"
                );
                failed.store(true, Ordering::Release);
                return;
            }
        }
    }
}

fn metadata_error_is_retryable(error: &StoreError) -> bool {
    match error {
        StoreError::Unavailable => true,
        StoreError::Log(error) => error.kind() == SessionLogErrorKind::Unavailable,
        _ => false,
    }
}

impl Sessions {
    pub(crate) fn new() -> Self {
        Self {
            loaded: HashMap::new(),
        }
    }

    pub(crate) fn contains(&self, session_id: SessionId) -> bool {
        self.loaded.contains_key(&session_id)
    }

    pub(crate) fn get(&self, session_id: SessionId) -> Option<&LoadedSession> {
        self.loaded.get(&session_id)
    }

    pub(crate) fn get_mut(&mut self, session_id: SessionId) -> Option<&mut LoadedSession> {
        self.loaded.get_mut(&session_id)
    }

    pub(crate) fn insert(
        &mut self,
        session_id: SessionId,
        loaded: LoadedSession,
    ) -> Option<LoadedSession> {
        self.loaded.insert(session_id, loaded)
    }

    pub(crate) fn remove(&mut self, session_id: SessionId) -> Option<LoadedSession> {
        self.loaded.remove(&session_id)
    }

    pub(crate) async fn shutdown_all(&mut self) -> Result<(), AgentError> {
        let mut ids = self.loaded.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        let mut first_error = None;
        for session_id in ids {
            let Some(loaded) = self.loaded.remove(&session_id) else {
                continue;
            };
            let meta = EventMeta {
                session_id,
                instance_id: loaded.handle.instance_id(),
                dropped_before: 0,
            };
            if let Err(error) = loaded.shutdown(meta).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for Sessions {
    fn drop(&mut self) {
        for loaded in self.loaded.values() {
            loaded.pump.stop();
            loaded.metadata.cancel();
        }
    }
}

impl LoadedSession {
    #[cfg(test)]
    pub(crate) async fn reserve_completion_capacity(&self) -> CompletionCapacityGuard {
        self.pump.reserve_completion_capacity().await
    }

    #[cfg(test)]
    pub(crate) fn session_pump_stop_token(&self) -> CancellationToken {
        self.pump.stop_token()
    }

    #[cfg(test)]
    pub(crate) fn active_completion_task_is_finished(&self) -> Option<bool> {
        self.active_turn
            .as_ref()
            .map(|active| active.completion_task.is_finished())
    }

    pub(crate) async fn shutdown(self, meta: EventMeta) -> Result<(), AgentError> {
        let Self {
            runtime,
            active_turn,
            pump,
            metadata,
            event_sink,
            ..
        } = self;
        let runtime_error = runtime.shutdown().await.err();
        pump.stop();
        let completion_error = match active_turn {
            Some(active) => active.completion_task.await.err(),
            None => None,
        };
        let pump_error = pump.shutdown().await.err();
        let metadata_error = metadata.shutdown().await.err();
        if runtime_error.is_none() {
            let _ = event_sink.try_send(AgentEvent::SessionClosed {
                session_id: meta.session_id,
                meta,
            });
        }
        if let Some(error) = runtime_error {
            return Err(map_session_shutdown_error(error));
        }
        if completion_error.is_some() || pump_error.is_some() || metadata_error.is_some() {
            return Err(AgentError::Internal);
        }
        Ok(())
    }

    pub(crate) fn active_turn_id(&self) -> Option<TurnId> {
        self.active_turn
            .as_ref()
            .map(|active| active.handle.turn_id())
    }
}

#[cfg(test)]
pub(crate) fn block_session_pump_startup(
    workspace: std::path::PathBuf,
    gate: Arc<SessionPumpStartupGate>,
) {
    SESSION_PUMP_STARTUP_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((workspace, gate));
}

#[cfg(test)]
fn take_session_pump_startup_gate(
    workspace: &std::path::Path,
) -> Option<Arc<SessionPumpStartupGate>> {
    let mut gates = SESSION_PUMP_STARTUP_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|(candidate, _)| candidate == workspace)
        .map(|position| gates.remove(position).1)
}

pub(crate) fn map_session_shutdown_error(error: SessionShutdownError) -> AgentError {
    match error {
        SessionShutdownError::Timeout(diagnostic) => AgentError::Core(
            crate::error::CoreErrorView::new("session shutdown timeout", diagnostic.retryable),
        ),
        SessionShutdownError::Durability(diagnostic) => {
            AgentError::Core(crate::error::CoreErrorView::new(
                "session shutdown durability failed",
                diagnostic.retryable,
            ))
        }
        SessionShutdownError::LogClose(diagnostic) => AgentError::Core(
            crate::error::CoreErrorView::new("session log close failed", diagnostic.retryable),
        ),
        SessionShutdownError::ActorTerminated(diagnostic) => AgentError::Core(
            crate::error::CoreErrorView::new("session actor terminated", diagnostic.retryable),
        ),
        _ => AgentError::Internal,
    }
}
