//! The per-session input queue: accepted user inputs that reach the running
//! turn at a request boundary or start the next turn.
use super::*;

/// Most inputs one Session queues at once.
pub(crate) const MAX_QUEUED_INPUTS: usize = 32;

/// How a pushed input reaches the Session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InputMode {
    /// As soon as possible: the running turn's next request, else a new turn.
    Steer,
    /// After the running turn has ended, as a new turn.
    FollowUp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueueItem {
    pub id: String,
    pub mode: InputMode,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<String>,
}

/// The queued inputs in the order they run (steers first) and whether
/// nothing starts by itself.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Queue {
    pub items: Vec<QueueItem>,
    pub paused: bool,
}

/// A turn the Agent started from the queue, with the entry that became its
/// initial input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StartedTurn {
    pub turn: TurnRef,
    pub input: QueueItem,
}

/// An input a running turn took, with the request preparation it joined.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveredInput {
    pub id: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<String>,
    /// Index of the preparation the input joined. It is the candidate index:
    /// a cancelled or failed preparation can leave it unissued.
    pub request_index: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PushInput {
    pub session_id: SessionId,
    pub mode: InputMode,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveInput {
    pub session_id: SessionId,
    pub id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputPushed {
    pub session_id: SessionId,
    pub load: crate::ids::LoadId,
    pub accepted: bool,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<String>,
    /// The turn this push started; its input is an older entry when queue
    /// order requires.
    pub started: Option<StartedTurn>,
    pub start_error: Option<InputStartError>,
    pub queue: Queue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputStartError {
    pub input_id: String,
    pub kind: &'static str,
    pub message: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputRemoved {
    pub text: String,
    pub queue: Queue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputsRecalled {
    pub texts: Vec<String>,
    pub queue: Queue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputsResumed {
    pub started: Option<StartedTurn>,
    pub start_error: Option<InputStartError>,
    pub queue: Queue,
}

#[derive(Clone)]
pub(super) struct Entry {
    id: String,
    mode: InputMode,
    input: UserInput,
    accepted_at: Option<String>,
}

impl Entry {
    pub(super) fn accepted_at(&self) -> Option<String> {
        self.accepted_at.clone()
    }

    pub(super) fn item(&self) -> QueueItem {
        QueueItem {
            id: self.id.clone(),
            mode: self.mode,
            text: self.input.as_text().to_owned(),
            accepted_at: self.accepted_at.clone(),
        }
    }

    fn delivered(&self, request_index: u32) -> DeliveredInput {
        DeliveredInput {
            id: self.id.clone(),
            text: self.input.as_text().to_owned(),
            accepted_at: self.accepted_at.clone(),
            request_index,
        }
    }
}

#[derive(Default)]
pub(super) struct QueueState {
    /// In acceptance order; the run order is derived from it.
    pub(super) items: Vec<Entry>,
    pub(super) starting: Option<Entry>,
    pub(super) paused: bool,
    /// A turn is active or being started, or a claimed start is under way.
    running: bool,
    /// The loop generation that may take inputs; any other loop is stale.
    binding: Option<u64>,
    generation: u64,
    next_id: u64,
}

impl QueueState {
    fn head(&self) -> Option<usize> {
        self.items
            .iter()
            .position(|entry| entry.mode == InputMode::Steer)
            .or((!self.items.is_empty()).then_some(0))
    }

    pub(super) fn view(&self) -> Queue {
        let steers = self
            .items
            .iter()
            .filter(|entry| entry.mode == InputMode::Steer);
        let follow_ups = self
            .items
            .iter()
            .filter(|entry| entry.mode == InputMode::FollowUp);
        Queue {
            items: steers.chain(follow_ups).map(Entry::item).collect(),
            paused: self.paused,
        }
    }

    /// Hands the head to a new loop generation; the queue stays `running`.
    fn start_head(&mut self) -> Result<Option<(Entry, u64)>, AgentError> {
        let Some(head) = self.head() else {
            return Ok(None);
        };
        let generation = self.generation.checked_add(1).ok_or(AgentError::Internal)?;
        let entry = self.items.remove(head);
        self.running = true;
        self.generation = generation;
        self.binding = Some(self.generation);
        self.starting = Some(entry.clone());
        Ok(Some((entry, self.generation)))
    }

    fn restore(&mut self, entry: Entry) {
        let at = match entry.mode {
            InputMode::Steer => 0,
            InputMode::FollowUp => self
                .items
                .iter()
                .position(|candidate| candidate.mode == InputMode::FollowUp)
                .unwrap_or(self.items.len()),
        };
        self.items.insert(at, entry);
    }

    pub(super) fn end_period(&mut self) {
        self.running = false;
        self.binding = None;
        self.starting = None;
    }

    fn take_steers(&mut self) -> Vec<Entry> {
        let (steers, rest): (Vec<Entry>, Vec<Entry>) = std::mem::take(&mut self.items)
            .into_iter()
            .partition(|entry| entry.mode == InputMode::Steer);
        self.items = rest;
        steers
    }
}

/// A queue head claimed to start a turn. Dropping it unstarted gives the
/// entry back and lets the next push or compaction end start it.
pub(super) struct Claim {
    session: Session,
    entry: Option<Entry>,
    generation: u64,
}

impl Claim {
    fn new(session: &Session, (entry, generation): (Entry, u64)) -> Self {
        Self {
            session: session.clone(),
            entry: Some(entry),
            generation,
        }
    }

    fn entry(&self) -> &Entry {
        self.entry.as_ref().expect("claim is not consumed")
    }

    fn started(mut self) {
        self.entry = None;
    }

    fn failed(mut self) {
        if let Some(entry) = self.entry.take() {
            self.session.abandon_claim(entry, true);
        }
    }

    fn occupied(mut self) {
        if let Some(entry) = self.entry.take() {
            self.session.abandon_claim(entry, false);
        }
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            self.session.abandon_claim(entry, false);
        }
    }
}

/// A push accepted into the queue that may still have to start a turn.
pub(crate) struct PendingPush {
    session: Session,
    id: String,
    accepted_at: Option<String>,
    claim: Option<Claim>,
}

impl PendingPush {
    pub(crate) fn needs_start(&self) -> bool {
        self.claim.is_some()
    }

    pub(crate) fn finish(self, started: Option<StartedTurn>) -> InputPushed {
        self.finish_with_error(started, None)
    }

    fn finish_with_error(
        self,
        started: Option<StartedTurn>,
        start_error: Option<InputStartError>,
    ) -> InputPushed {
        InputPushed {
            session_id: self.session.session_id(),
            load: self.session.load(),
            accepted: true,
            id: self.id,
            accepted_at: self.accepted_at,
            started,
            start_error,
            queue: self.session.queue(),
        }
    }

    pub(crate) async fn start(mut self) -> Result<InputPushed, AgentError> {
        let (started, start_error) = match self.claim.take() {
            Some(claim) => self.session.start_claim(claim).await,
            None => (None, None),
        };
        Ok(self.finish_with_error(started, start_error))
    }
}

/// `resume` that may still have to start a turn.
pub(crate) struct PendingResume {
    session: Session,
    claim: Option<Claim>,
}

impl PendingResume {
    pub(crate) fn needs_start(&self) -> bool {
        self.claim.is_some()
    }

    pub(crate) fn finish(self, started: Option<StartedTurn>) -> InputsResumed {
        self.finish_with_error(started, None)
    }

    fn finish_with_error(
        self,
        started: Option<StartedTurn>,
        start_error: Option<InputStartError>,
    ) -> InputsResumed {
        InputsResumed {
            started,
            start_error,
            queue: self.session.queue(),
        }
    }

    pub(crate) async fn start(mut self) -> Result<InputsResumed, AgentError> {
        let (started, start_error) = match self.claim.take() {
            Some(claim) => self.session.start_claim(claim).await,
            None => (None, None),
        };
        Ok(self.finish_with_error(started, start_error))
    }
}

/// The inputs one loop takes at its request boundaries. It belongs to the
/// loop generation it was created for.
pub(super) struct SessionInputs {
    session: Session,
    presentation: Arc<crate::presentation::Presentation>,
    generation: u64,
    boundary: Arc<RequestBoundary>,
    delivery: Mutex<Delivery>,
}

/// Where `input_delivered` goes: the loop identity is only known after
/// `AgentLoop::start` returns, so earlier takes wait for `bind`.
#[derive(Default)]
struct Delivery {
    turn: Option<TurnRef>,
    early: Vec<DeliveredInput>,
}

impl SessionInputs {
    pub(super) fn new(
        session: Session,
        presentation: Arc<crate::presentation::Presentation>,
        generation: u64,
        boundary: Arc<RequestBoundary>,
    ) -> Self {
        boundary.install(generation);
        Self {
            session,
            presentation,
            generation,
            boundary,
            delivery: Mutex::new(Delivery::default()),
        }
    }

    pub(super) fn bind(&self, turn: TurnRef) {
        let mut delivery = self.delivery.lock().unwrap_or_else(|e| e.into_inner());
        delivery.turn = Some(turn);
        if !delivery.early.is_empty() {
            self.session
                .shared
                .events
                .try_send(AgentEvent::InputDelivered {
                    turn,
                    items: std::mem::take(&mut delivery.early),
                    meta: EventMeta::for_turn(turn, self.session.load()),
                });
        }
    }
}

impl LoopInputs for SessionInputs {
    fn take(&self) -> Vec<UserInput> {
        let mut queue = self.session.lock_queue();
        if queue.binding != Some(self.generation) {
            return Vec::new();
        }
        let taken = queue.take_steers();
        if taken.is_empty() {
            return Vec::new();
        }
        for entry in &taken {
            self.presentation
                .record_prompt_time(entry.accepted_at.clone());
        }
        let request_index = self.boundary.take(self.generation);
        let items = taken
            .iter()
            .map(|entry| entry.delivered(request_index))
            .collect();
        {
            let mut delivery = self.delivery.lock().unwrap_or_else(|e| e.into_inner());
            match delivery.turn {
                Some(turn) => {
                    self.session
                        .shared
                        .events
                        .try_send(AgentEvent::InputDelivered {
                            turn,
                            items,
                            meta: EventMeta::for_turn(turn, self.session.load()),
                        });
                }
                None => delivery.early.extend(items),
            }
        }
        self.session.emit_queue(&queue);
        taken.into_iter().map(|entry| entry.input).collect()
    }
}

impl Session {
    pub(super) fn lock_queue(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.shared
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(super) fn emit_queue(&self, queue: &QueueState) {
        self.shared.events.try_send(AgentEvent::InputQueue {
            queue: queue.view(),
            meta: EventMeta::session(self.shared.session_id, self.shared.load),
        });
        self.mark_dirty();
    }

    pub(crate) fn queue(&self) -> Queue {
        self.lock_queue().view()
    }

    /// Nothing may start while the Session is blocked, closing, or runs or
    /// prepares a manual compaction.
    fn can_start(inner: &SessionInner) -> bool {
        inner.blocked.is_none()
            && !inner.closing
            && inner.pending_compaction.is_none()
            && inner
                .compaction
                .as_ref()
                .is_none_or(|operation| operation.is_completed())
    }

    /// Claims the head for a new busy period when nothing runs and nothing
    /// holds the queue back.
    fn claim_start(
        &self,
        inner: &SessionInner,
        queue: &mut QueueState,
    ) -> Result<Option<Claim>, AgentError> {
        if queue.running || queue.paused || !Self::can_start(inner) {
            return Ok(None);
        }
        Ok(queue.start_head()?.map(|head| Claim::new(self, head)))
    }

    /// Queues one input and claims a turn start when the Session is idle.
    pub(crate) fn accept_input(
        &self,
        mode: InputMode,
        text: String,
    ) -> Result<PendingPush, AgentError> {
        let accepted_at = crate::store::utc_timestamp().ok();
        let inner = self.lock_inner();
        let input = UserInput::text(text)
            .ok()
            .filter(|input| input.as_text().len() <= inner.options.limits.max_user_input_bytes)
            .ok_or(AgentError::InvalidInput)?;
        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }
        if inner.closing {
            return Err(AgentError::SessionBusy);
        }
        let mut queue = self.lock_queue();
        if queue.items.len() >= MAX_QUEUED_INPUTS {
            return Err(AgentError::InputQueueFull);
        }
        if !queue.running && Self::can_start(&inner) && queue.generation == u64::MAX {
            return Err(AgentError::Internal);
        }
        queue.next_id = queue.next_id.checked_add(1).ok_or(AgentError::Internal)?;
        let id = format!("inp_{:032x}", queue.next_id);
        queue.items.push(Entry {
            id: id.clone(),
            mode,
            input,
            accepted_at: accepted_at.clone(),
        });
        queue.paused = false;
        let claim = self.claim_start(&inner, &mut queue)?;
        self.emit_queue(&queue);
        Ok(PendingPush {
            session: self.clone(),
            id,
            accepted_at,
            claim,
        })
    }

    pub(crate) fn remove_input(&self, id: &str) -> Result<InputRemoved, AgentError> {
        let mut queue = self.lock_queue();
        let at = queue
            .items
            .iter()
            .position(|entry| entry.id == id)
            .ok_or(AgentError::InputNotQueued)?;
        let entry = queue.items.remove(at);
        queue.paused &= !queue.items.is_empty();
        self.emit_queue(&queue);
        Ok(InputRemoved {
            text: entry.input.as_text().to_owned(),
            queue: queue.view(),
        })
    }

    pub(crate) fn recall_inputs(&self) -> InputsRecalled {
        let mut queue = self.lock_queue();
        let texts = queue
            .view()
            .items
            .into_iter()
            .map(|item| item.text)
            .collect();
        queue.items.clear();
        queue.paused = false;
        self.emit_queue(&queue);
        InputsRecalled {
            texts,
            queue: queue.view(),
        }
    }

    pub(crate) fn accept_resume(&self) -> Result<PendingResume, AgentError> {
        let inner = self.lock_inner();
        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }
        let mut queue = self.lock_queue();
        if !queue.running
            && !queue.items.is_empty()
            && Self::can_start(&inner)
            && queue.generation == u64::MAX
        {
            return Err(AgentError::Internal);
        }
        let was_paused = std::mem::take(&mut queue.paused);
        let claim = self.claim_start(&inner, &mut queue)?;
        if was_paused || claim.is_some() {
            self.emit_queue(&queue);
        }
        Ok(PendingResume {
            session: self.clone(),
            claim,
        })
    }

    /// Starts the claimed turn. A Session that became busy meanwhile keeps
    /// the entry queued; any other failure also pauses the queue.
    async fn start_claim(&self, claim: Claim) -> (Option<StartedTurn>, Option<InputStartError>) {
        let input_id = claim.entry().id.clone();
        let started = async {
            self.cleanup_finished().await?;
            let entry = claim.entry();
            let boundary = RequestBoundary::new();
            let execution =
                self.execution_config(entry.input.clone(), claim.generation, &boundary)?;
            self.install_loop_with_boundary(execution, boundary, entry, claim.generation, None)
        }
        .await;
        match started {
            Ok((started, _)) => {
                claim.started();
                (Some(started), None)
            }
            Err(AgentError::SessionBusy) => {
                claim.occupied();
                (
                    None,
                    Some(InputStartError {
                        input_id,
                        kind: "session_busy",
                        message: "The queued turn could not start",
                    }),
                )
            }
            Err(error) => {
                claim.failed();
                let kind = match error {
                    AgentError::SessionBlocked => "session_blocked",
                    AgentError::InvalidSessionSettings => "invalid_session_settings",
                    _ => "runtime_start_failed",
                };
                (
                    None,
                    Some(InputStartError {
                        input_id,
                        kind,
                        message: "The queued turn could not start",
                    }),
                )
            }
        }
    }

    /// Gives a claimed entry back and ends the busy period it began.
    fn abandon_claim(&self, entry: Entry, pause: bool) {
        let mut queue = self.lock_queue();
        queue.restore(entry);
        queue.end_period();
        queue.paused |= pause;
        self.emit_queue(&queue);
    }

    /// Starts the head after a manual compaction ended, without waiting for
    /// any task: the caller is the compaction's own worker.
    pub(super) fn start_after_compaction(&self) -> Option<StartedTurn> {
        let claim = {
            let inner = self.lock_inner();
            let mut queue = self.lock_queue();
            let claim = match self.claim_start(&inner, &mut queue) {
                Ok(Some(claim)) => claim,
                Ok(None) => return None,
                Err(_) => {
                    queue.paused = true;
                    self.emit_queue(&queue);
                    return None;
                }
            };
            self.emit_queue(&queue);
            claim
        };
        let boundary = RequestBoundary::new();
        let started = self
            .execution_config(claim.entry().input.clone(), claim.generation, &boundary)
            .and_then(|execution| {
                self.install_loop_with_boundary(
                    execution,
                    boundary,
                    claim.entry(),
                    claim.generation,
                    None,
                )
            });
        match started {
            Ok((started, _)) => {
                claim.started();
                Some(started)
            }
            Err(error) => {
                tracing::warn!(session_id = %self.shared.session_id, error = ?error,
                    "queued input could not start after compaction");
                claim.failed();
                None
            }
        }
    }
}

/// What the driver does once a turn's report is persisted.
pub(super) enum Close {
    /// Settle the finished turn first: a successor would project the history.
    Settle,
    Done {
        result: Arc<TurnResult>,
        run: Option<LoopRun>,
    },
}

impl Session {
    /// Ends the turn: starts the successor when the turn completed and the
    /// queue has a head, otherwise ends the busy period, pausing a non-empty
    /// queue after a turn that did not complete. The successor is installed
    /// before the finished turn's completion is published.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn close_turn(
        &self,
        turn: TurnRef,
        task: &Arc<SessionTask>,
        completion: &mut CompletionGuard,
        eligible: bool,
        settled: bool,
        result: &dyn Fn(Option<StartedTurn>) -> Arc<TurnResult>,
        source: &mut Option<ReservedSource>,
        unavailable: Option<UnavailableReason>,
        uncertain: bool,
    ) -> Close {
        let mut released = Vec::new();
        let publication = self.shared.results.publication();
        let (entry, generation) = {
            let mut inner = self.lock_inner();
            let mut queue = self.lock_queue();
            let successor = eligible && !inner.closing && queue.head().is_some();
            if successor && !settled {
                return Close::Settle;
            }
            let head = if successor {
                match queue.start_head() {
                    Ok(head) => head,
                    Err(_) => {
                        queue.paused = true;
                        None
                    }
                }
            } else {
                None
            };
            let Some(head) = head else {
                queue.paused |= !eligible && !queue.items.is_empty();
                queue.end_period();
                let result = result(None);
                released.extend(self.register_completion(
                    &mut inner,
                    &result,
                    source.take(),
                    unavailable,
                    uncertain,
                ));
                completion.publish_finished(Arc::clone(&result));
                self.emit_queue(&queue);
                drop(queue);
                drop(inner);
                drop(publication);
                drop(released);
                return Close::Done { result, run: None };
            };
            self.emit_queue(&queue);
            head
        };
        drop(publication);
        let mut publish = |inner: &mut SessionInner, next: StartedTurn| {
            let finished = result(Some(next));
            released.extend(self.register_completion(
                inner,
                &finished,
                source.take(),
                unavailable,
                uncertain,
            ));
            finished
        };
        let boundary = RequestBoundary::new();
        let started = self
            .execution_config(entry.input.clone(), generation, &boundary)
            .and_then(|execution| {
                self.install_loop_with_boundary(
                    execution,
                    boundary,
                    &entry,
                    generation,
                    Some(Successor {
                        turn,
                        task,
                        completion,
                        publish: &mut publish,
                    }),
                )
            });
        drop(publish);
        match started {
            Ok((started, run)) => {
                #[cfg(test)]
                if let Some(gate) = PAUSE_AFTER_SUCCESSOR_INSTALL.take(&turn.session_id) {
                    tokio::task::block_in_place(|| gate.pass_blocking());
                }
                let result = result(Some(started));
                drop(released);
                Close::Done { result, run }
            }
            Err(error) => {
                tracing::warn!(session_id = %turn.session_id, error = ?error,
                    "queued input could not start after the turn");
                let publication = self.shared.results.publication();
                let mut inner = self.lock_inner();
                let mut queue = self.lock_queue();
                queue.restore(entry);
                queue.end_period();
                queue.paused = true;
                let result = result(None);
                released.extend(self.register_completion(
                    &mut inner,
                    &result,
                    source.take(),
                    unavailable,
                    uncertain,
                ));
                completion.publish_finished(Arc::clone(&result));
                self.emit_queue(&queue);
                drop(queue);
                drop(inner);
                drop(publication);
                drop(released);
                Close::Done { result, run: None }
            }
        }
    }

    fn register_completion(
        &self,
        inner: &mut SessionInner,
        result: &Arc<TurnResult>,
        source: Option<ReservedSource>,
        unavailable: Option<UnavailableReason>,
        uncertain: bool,
    ) -> Vec<Arc<results::ResultSource>> {
        let outcome = match &result.report.outcome {
            minicore_runtime::LoopOutcome::Completed => CompletionOutcome::Completed,
            minicore_runtime::LoopOutcome::Cancelled(_) => CompletionOutcome::Cancelled,
            minicore_runtime::LoopOutcome::Failed(_) => CompletionOutcome::Failed,
            _ => CompletionOutcome::Unknown,
        };
        let entry = CompletionEntry {
            seq: 0,
            loop_id: result.turn.loop_id,
            input_id: inner
                .active
                .as_ref()
                .filter(|active| active.turn == result.turn)
                .map(|active| active.input.id.clone()),
            completion_kind: CompletionKind::Reported,
            outcome_kind: Some(outcome),
            persistence: Some(result.persistence),
            persistence_uncertain: uncertain,
            availability: if result.persistence == TurnPersistence::Persisted {
                ResultAvailability::Stored
            } else if source.is_some() {
                ResultAvailability::Live
            } else {
                ResultAvailability::Unavailable
            },
            unavailable_reason: unavailable,
            next_loop_id: result.next.as_ref().map(|next| next.turn.loop_id),
            after_saved_loop_id: inner.last_saved,
        };
        let released = self.shared.results.insert(self.load(), entry, source);
        if result.persistence == TurnPersistence::Persisted {
            inner.last_saved = Some(result.turn.loop_id);
        }
        released
    }
}

#[cfg(test)]
mod tests;
