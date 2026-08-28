use std::collections::HashMap;

use crate::event::{AgentEvent, AgentEventSink, CompletionCancellation, EventMeta};

use minicore_runtime::error::SessionShutdownError;
use minicore_runtime::ids::{SessionId, TurnId};
use minicore_runtime::session::{SessionHandle, SessionRuntime, TurnHandle};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::agent::TurnRef;
use crate::error::AgentError;
use crate::store::{SessionRecord, Store};

pub(crate) struct Sessions {
    loaded: HashMap<SessionId, LoadedSession>,
}

pub(crate) struct LoadedSession {
    pub(crate) record: SessionRecord,
    pub(crate) runtime: SessionRuntime,
    pub(crate) handle: SessionHandle,
    pub(crate) active_turn: Option<TurnHandle>,
    pub(crate) event_task: JoinHandle<()>,
    pub(crate) state_task: JoinHandle<()>,
    pub(crate) event_sink: AgentEventSink,
    pub(crate) completion: CompletionNotifier,
    pub(crate) metadata: MetadataWorker,
}

struct CompletionJob {
    turn: TurnHandle,
    turn_ref: TurnRef,
}

pub(crate) struct CompletionNotifier {
    sender: mpsc::UnboundedSender<CompletionJob>,
    cancellation: CompletionCancellation,
    task: JoinHandle<()>,
}

impl CompletionNotifier {
    pub(crate) fn new(task_runtime: &Handle, event_sink: AgentEventSink) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let cancellation = CompletionCancellation::new();
        let task = task_runtime.spawn(completion_worker(
            receiver,
            event_sink,
            cancellation.clone(),
        ));
        Self {
            sender,
            cancellation,
            task,
        }
    }

    pub(crate) fn enqueue(&self, turn: TurnHandle, turn_ref: TurnRef) {
        // A closed event stream intentionally stops the notifier; the submitted TurnHandle
        // remains authoritative even when no live AgentEvent consumer exists.
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
    event_sink: AgentEventSink,
    cancellation: CompletionCancellation,
) {
    loop {
        let Some(job) = (tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            _ = event_sink.closed() => None,
            job = receiver.recv() => job,
        }) else {
            break;
        };
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = event_sink.closed() => break,
            outcome = job.turn.wait() => outcome,
        };
        let Ok(outcome) = outcome else {
            // TurnWaitError is reflected by the runtime's safe SessionState/error state. Never
            // invent a TurnOutcome for a turn whose durable result is unavailable.
            continue;
        };
        if !event_sink
            .send_durable(
                AgentEvent::TurnFinished {
                    turn: job.turn_ref,
                    outcome,
                    meta: EventMeta {
                        session_id: job.turn_ref.session_id,
                        instance_id: job.turn_ref.instance_id,
                        dropped_before: 0,
                    },
                },
                cancellation.clone(),
            )
            .await
        {
            break;
        }
    }
}

pub(crate) struct MetadataWorker {
    sender: watch::Sender<Option<String>>,
    cancellation: CompletionCancellation,
    task: JoinHandle<()>,
}

impl MetadataWorker {
    pub(crate) fn new(task_runtime: &Handle, store: Store, session_id: SessionId) -> Self {
        let (sender, receiver) = watch::channel(None);
        let cancellation = CompletionCancellation::new();
        let task = task_runtime.spawn(metadata_worker(
            receiver,
            store,
            session_id,
            cancellation.clone(),
        ));
        Self {
            sender,
            cancellation,
            task,
        }
    }

    pub(crate) fn update(&self, updated_at: String) {
        let _ = self.sender.send_replace(Some(updated_at));
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

async fn metadata_worker(
    mut receiver: watch::Receiver<Option<String>>,
    store: Store,
    session_id: SessionId,
    cancellation: CompletionCancellation,
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
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = store.touch_at(session_id, updated_at) => result,
        };
        if result.is_err() {
            return;
        }
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
            let event_sink = loaded.event_sink.clone();
            let meta = EventMeta {
                session_id,
                instance_id: loaded.handle.instance_id(),
                dropped_before: 0,
            };
            let result = loaded.shutdown().await;
            if result.is_ok() {
                let _ = event_sink.try_send(AgentEvent::SessionClosed { session_id, meta });
            } else if first_error.is_none() {
                first_error = result.err();
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for Sessions {
    fn drop(&mut self) {
        for loaded in self.loaded.values() {
            loaded.completion.cancel();
            loaded.metadata.cancel();
        }
    }
}

impl LoadedSession {
    pub(crate) async fn shutdown(self) -> Result<(), AgentError> {
        let Self {
            runtime,
            event_task,
            state_task,
            completion,
            metadata,
            ..
        } = self;
        let runtime_error = runtime.shutdown().await.err();
        let event_error = event_task.await.err();
        let state_error = state_task.await.err();
        let completion_error = completion.shutdown().await.err();
        let metadata_error = metadata.shutdown().await.err();
        if let Some(error) = runtime_error {
            return Err(map_session_shutdown_error(error));
        }
        if event_error.is_some() || state_error.is_some() {
            return Err(AgentError::Internal);
        }
        if completion_error.is_some() || metadata_error.is_some() {
            return Err(AgentError::Internal);
        }
        Ok(())
    }

    pub(crate) fn active_turn_id(&self) -> Option<TurnId> {
        self.active_turn.as_ref().map(TurnHandle::turn_id)
    }
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
