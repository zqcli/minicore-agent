use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use crate::event::{
    AgentEvent, AgentEventSink, CompletionCancellation, EventMeta, emit_state, forward_core_event,
};

use minicore_runtime::error::{SessionError, SessionShutdownError};
use minicore_runtime::ids::{SessionId, TurnId};
use minicore_runtime::session::{
    SessionEventStream, SessionHandle, SessionRuntime, SessionState, TurnHandle,
};
use minicore_runtime::storage::SessionLogErrorKind;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::agent::{SessionInfo, TurnRef};
use crate::error::{AgentError, StoreError};
use crate::store::{SessionRecord, Store};

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, LoadedSession>,
}

pub(crate) struct LoadedSession {
    pub(crate) record: SessionRecord,
    pub(crate) runtime: SessionRuntime,
    pub(crate) handle: SessionHandle,
    pub(crate) active_turn: Option<TurnHandle>,
    pub(crate) sequencer: OutboundSequencer,
    pub(crate) completion: CompletionNotifier,
    pub(crate) metadata: MetadataWorker,
    pub(crate) event_sink: AgentEventSink,
}

pub(crate) struct CompletionReady {
    turn_ref: TurnRef,
    outcome: minicore_runtime::TurnOutcome,
}

pub(crate) struct OutboundSequencer {
    completion_input: mpsc::UnboundedSender<CompletionReady>,
    stop: CompletionCancellation,
    task: JoinHandle<()>,
}

struct SequencerResources {
    event_stream: SessionEventStream,
    state: watch::Receiver<SessionState>,
    handle: SessionHandle,
    completion_input: mpsc::UnboundedReceiver<CompletionReady>,
    event_sink: AgentEventSink,
    opened: SessionInfo,
    stop: CompletionCancellation,
}

#[cfg(test)]
pub(crate) struct SequencerGate {
    pub(crate) started: Arc<tokio::sync::Semaphore>,
    pub(crate) release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl SequencerGate {
    pub(crate) fn new() -> Self {
        Self {
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
type SequencerGateList = Mutex<Vec<(std::path::PathBuf, Arc<SequencerGate>)>>;

#[cfg(test)]
static SEQUENCER_GATES: OnceLock<SequencerGateList> = OnceLock::new();

#[cfg(test)]
pub(crate) struct TranscriptBarrierGate {
    session_id: SessionId,
    pub(crate) started: Arc<tokio::sync::Semaphore>,
    pub(crate) release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl TranscriptBarrierGate {
    pub(crate) fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
static TRANSCRIPT_BARRIER_GATES: OnceLock<Mutex<Vec<Arc<TranscriptBarrierGate>>>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct SessionShutdownGate {
    session_id: SessionId,
    pub(crate) started: Arc<tokio::sync::Semaphore>,
    pub(crate) release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl SessionShutdownGate {
    pub(crate) fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
static SESSION_SHUTDOWN_GATES: OnceLock<Mutex<Vec<Arc<SessionShutdownGate>>>> = OnceLock::new();

impl OutboundSequencer {
    pub(crate) fn new(
        task_runtime: &Handle,
        event_stream: SessionEventStream,
        state: watch::Receiver<SessionState>,
        handle: SessionHandle,
        event_sink: AgentEventSink,
        opened: SessionInfo,
    ) -> Self {
        let (completion_input, completion_receiver) = mpsc::unbounded_channel();
        let stop = CompletionCancellation::new();
        let task = task_runtime.spawn(run_sequencer(SequencerResources {
            event_stream,
            state,
            handle,
            completion_input: completion_receiver,
            event_sink: event_sink.clone(),
            opened,
            stop: stop.clone(),
        }));
        Self {
            completion_input,
            stop,
            task,
        }
    }

    pub(crate) fn completion_sender(&self) -> mpsc::UnboundedSender<CompletionReady> {
        self.completion_input.clone()
    }

    pub(crate) fn stop_signal(&self) -> CompletionCancellation {
        self.stop.clone()
    }

    pub(crate) async fn close(self) -> Result<(), AgentError> {
        let Self {
            completion_input,
            stop,
            task,
        } = self;
        stop.cancel();
        drop(completion_input);
        task.await.map_err(|_| AgentError::Internal).map(|_| ())
    }

    pub(crate) fn cancel(&self) {
        self.stop.cancel();
    }
}

async fn run_sequencer(resources: SequencerResources) {
    let SequencerResources {
        mut event_stream,
        mut state,
        handle,
        mut completion_input,
        event_sink,
        opened,
        stop,
    } = resources;
    #[cfg(test)]
    let sequencer_gate = take_sequencer_gate(&opened.workspace);
    let Some(instance_id) = opened.instance_id else {
        stop.cancel();
        return;
    };
    if event_sink.try_send(AgentEvent::SessionOpened {
        session: opened,
        meta: EventMeta {
            session_id: handle.session_id(),
            instance_id,
            dropped_before: 0,
        },
    }) == crate::event::AgentSendResult::Closed
        || !emit_state(&event_sink, state.borrow_and_update().clone())
    {
        stop.cancel();
        return;
    }
    #[cfg(test)]
    if let Some(gate) = sequencer_gate {
        gate.started.add_permits(1);
        gate.release.acquire().await.unwrap().forget();
    }

    let mut core_open = true;
    let mut state_open = true;
    let mut completion_open = true;
    loop {
        if !core_open && !state_open && !completion_open {
            break;
        }
        let keep_running = tokio::select! {
            biased;
            _ = stop.cancelled() => false,
            _ = event_sink.closed() => false,
            ready = completion_input.recv(), if completion_open => match ready {
                Some(ready) => tokio::select! {
                    biased;
                    _ = stop.cancelled() => false,
                    _ = event_sink.closed() => false,
                    keep_running = process_completion(
                        ready,
                        &handle,
                        &mut event_stream,
                        &mut state,
                        &event_sink,
                        &stop,
                    ) => keep_running,
                },
                None => {
                    completion_open = false;
                    true
                }
            },
            changed = state.changed(), if state_open => match changed {
                Ok(()) => emit_state(&event_sink, state.borrow_and_update().clone()),
                Err(_) => {
                    state_open = false;
                    true
                }
            },
            envelope = event_stream.recv(), if core_open => match envelope {
                Some(envelope) => forward_core_event(envelope, &event_sink),
                None => {
                    core_open = false;
                    true
                }
            },
        };
        if !keep_running {
            break;
        }
    }
    stop.cancel();
}

async fn process_completion(
    ready: CompletionReady,
    handle: &SessionHandle,
    event_stream: &mut SessionEventStream,
    state: &mut watch::Receiver<SessionState>,
    event_sink: &AgentEventSink,
    cancellation: &CompletionCancellation,
) -> bool {
    match transcript_barrier(handle, cancellation).await {
        BarrierResult::Cancelled => return false,
        BarrierResult::Closed => {
            cancellation.cancel();
            return false;
        }
        BarrierResult::Processed => {}
    }

    while let Ok(envelope) = event_stream.try_recv() {
        if !forward_core_event(envelope, event_sink) {
            return false;
        }
    }
    if !emit_state(event_sink, state.borrow_and_update().clone()) {
        return false;
    }
    let sent = event_sink
        .send_durable(
            AgentEvent::TurnFinished {
                turn: ready.turn_ref,
                outcome: ready.outcome,
                meta: EventMeta {
                    session_id: ready.turn_ref.session_id,
                    instance_id: ready.turn_ref.instance_id,
                    dropped_before: 0,
                },
            },
            cancellation.clone(),
        )
        .await;
    sent || !event_sink.is_closed()
}

enum BarrierResult {
    Processed,
    Cancelled,
    Closed,
}

async fn transcript_barrier(
    handle: &SessionHandle,
    cancellation: &CompletionCancellation,
) -> BarrierResult {
    #[cfg(test)]
    if let Some(gate) = take_transcript_barrier_gate(handle.session_id()) {
        gate.started.add_permits(1);
        gate.release.acquire().await.unwrap().forget();
    }
    loop {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return BarrierResult::Cancelled,
            result = handle.transcript(None, 1) => result,
        };
        match result {
            Err(SessionError::Backpressure) => {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return BarrierResult::Cancelled,
                    _ = tokio::task::yield_now() => {}
                }
            }
            Err(SessionError::Closed) => return BarrierResult::Closed,
            Ok(_) | Err(_) => return BarrierResult::Processed,
        }
    }
}

pub(crate) struct CompletionNotifier {
    sender: mpsc::UnboundedSender<CompletionJob>,
    cancellation: CompletionCancellation,
    task: JoinHandle<()>,
}

struct CompletionJob {
    turn: TurnHandle,
    turn_ref: TurnRef,
}

impl CompletionNotifier {
    pub(crate) fn new(
        task_runtime: &Handle,
        completion_input: mpsc::UnboundedSender<CompletionReady>,
        stop: CompletionCancellation,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let cancellation = CompletionCancellation::new();
        let task = task_runtime.spawn(completion_worker(
            receiver,
            completion_input,
            cancellation.clone(),
            stop,
        ));
        Self {
            sender,
            cancellation,
            task,
        }
    }

    pub(crate) fn enqueue(&self, turn: TurnHandle, turn_ref: TurnRef) {
        let _ = self.sender.send(CompletionJob { turn, turn_ref });
    }

    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        let Self {
            sender,
            cancellation,
            task,
        } = self;
        drop(sender);
        cancellation.cancel();
        task.await.map_err(|_| AgentError::Internal).map(|_| ())
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }
}

async fn completion_worker(
    mut receiver: mpsc::UnboundedReceiver<CompletionJob>,
    completion_input: mpsc::UnboundedSender<CompletionReady>,
    cancellation: CompletionCancellation,
    stop: CompletionCancellation,
) {
    loop {
        let Some(job) = (tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            _ = stop.cancelled() => None,
            _ = completion_input.closed() => None,
            job = receiver.recv() => job,
        }) else {
            break;
        };
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = stop.cancelled() => break,
            _ = completion_input.closed() => break,
            outcome = job.turn.wait() => outcome,
        };
        let Ok(outcome) = outcome else {
            // TurnWaitError has no safe TurnOutcome. The runtime state/error surface remains
            // authoritative, and the FIFO notifier proceeds to the next queued turn.
            continue;
        };
        if completion_input
            .send(CompletionReady {
                turn_ref: job.turn_ref,
                outcome,
            })
            .is_err()
        {
            break;
        }
    }
}

pub(crate) struct MetadataWorker {
    sender: watch::Sender<Option<String>>,
    cancellation: CompletionCancellation,
    failed: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl MetadataWorker {
    pub(crate) fn new(task_runtime: &Handle, store: Store, session_id: SessionId) -> Self {
        let (sender, receiver) = watch::channel(None);
        let cancellation = CompletionCancellation::new();
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
    pub(crate) fn stop_signal(&self) -> CompletionCancellation {
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
        task.await.map_err(|_| AgentError::Internal).map(|_| ())
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }
}

async fn metadata_worker(
    mut receiver: watch::Receiver<Option<String>>,
    store: Store,
    session_id: SessionId,
    cancellation: CompletionCancellation,
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
            Err(_) => {
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
            loaded.completion.cancel();
            loaded.sequencer.cancel();
            loaded.metadata.cancel();
        }
    }
}

impl LoadedSession {
    pub(crate) async fn shutdown(self, meta: EventMeta) -> Result<(), AgentError> {
        let Self {
            runtime,
            completion,
            metadata,
            sequencer,
            event_sink,
            ..
        } = self;
        let runtime_error = runtime.shutdown().await.err();
        let completion_error = completion.shutdown().await.err();
        let metadata_error = metadata.shutdown().await.err();
        #[cfg(test)]
        if let Some(gate) = take_session_shutdown_gate(meta.session_id) {
            gate.started.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        let sequencer_error = sequencer.close().await.err();
        if let Some(error) = runtime_error {
            return Err(map_session_shutdown_error(error));
        }
        if completion_error.is_some() || metadata_error.is_some() || sequencer_error.is_some() {
            return Err(AgentError::Internal);
        }
        let _ = event_sink.try_send(AgentEvent::SessionClosed {
            session_id: meta.session_id,
            meta,
        });
        Ok(())
    }

    pub(crate) fn active_turn_id(&self) -> Option<TurnId> {
        self.active_turn.as_ref().map(TurnHandle::turn_id)
    }
}

#[cfg(test)]
pub(crate) fn block_sequencer(workspace: std::path::PathBuf, gate: Arc<SequencerGate>) {
    SEQUENCER_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((workspace, gate));
}

#[cfg(test)]
pub(crate) fn block_transcript_barrier(gate: Arc<TranscriptBarrierGate>) {
    TRANSCRIPT_BARRIER_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(gate);
}

#[cfg(test)]
fn take_transcript_barrier_gate(session_id: SessionId) -> Option<Arc<TranscriptBarrierGate>> {
    let mut gates = TRANSCRIPT_BARRIER_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|gate| gate.session_id == session_id)
        .map(|position| gates.remove(position))
}

#[cfg(test)]
pub(crate) fn block_session_shutdown(gate: Arc<SessionShutdownGate>) {
    SESSION_SHUTDOWN_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(gate);
}

#[cfg(test)]
fn take_session_shutdown_gate(session_id: SessionId) -> Option<Arc<SessionShutdownGate>> {
    let mut gates = SESSION_SHUTDOWN_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    gates
        .iter()
        .position(|gate| gate.session_id == session_id)
        .map(|position| gates.remove(position))
}

#[cfg(test)]
fn take_sequencer_gate(workspace: &std::path::Path) -> Option<Arc<SequencerGate>> {
    let mut gates = SEQUENCER_GATES
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
