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

/// An input a running turn took.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveredInput {
    pub id: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<String>,
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
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<String>,
    /// The turn this push started; its input is an older entry when queue
    /// order requires.
    pub started: Option<StartedTurn>,
    pub queue: Queue,
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

    fn delivered(&self) -> DeliveredInput {
        DeliveredInput {
            id: self.id.clone(),
            text: self.input.as_text().to_owned(),
            accepted_at: self.accepted_at.clone(),
        }
    }
}

#[derive(Default)]
pub(super) struct QueueState {
    /// In acceptance order; the run order is derived from it.
    items: Vec<Entry>,
    paused: bool,
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

    fn view(&self) -> Queue {
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
    fn start_head(&mut self) -> Option<(Entry, u64)> {
        let entry = self.items.remove(self.head()?);
        self.running = true;
        self.generation += 1;
        self.binding = Some(self.generation);
        Some((entry, self.generation))
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

    fn end_period(&mut self) {
        self.running = false;
        self.binding = None;
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
        InputPushed {
            id: self.id,
            accepted_at: self.accepted_at,
            started,
            queue: self.session.queue(),
        }
    }

    pub(crate) async fn start(mut self) -> Result<InputPushed, AgentError> {
        let started = match self.claim.take() {
            Some(claim) => self.session.start_claim(claim).await?,
            None => None,
        };
        Ok(self.finish(started))
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
        InputsResumed {
            started,
            queue: self.session.queue(),
        }
    }

    pub(crate) async fn start(mut self) -> Result<InputsResumed, AgentError> {
        let started = match self.claim.take() {
            Some(claim) => self.session.start_claim(claim).await?,
            None => None,
        };
        Ok(self.finish(started))
    }
}

/// The inputs one loop takes at its request boundaries. It belongs to the
/// loop generation it was created for.
pub(super) struct SessionInputs {
    session: Session,
    presentation: Arc<crate::presentation::Presentation>,
    generation: u64,
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
    ) -> Self {
        Self {
            session,
            presentation,
            generation,
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
                    meta: EventMeta::for_turn(turn),
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
        let items = taken.iter().map(Entry::delivered).collect();
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
                            meta: EventMeta::for_turn(turn),
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
            meta: EventMeta::session(self.shared.session_id),
        });
    }

    pub(crate) fn queue(&self) -> Queue {
        self.lock_queue().view()
    }

    pub(crate) fn has_queued_inputs(&self) -> bool {
        !self.lock_queue().items.is_empty()
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
    fn claim_start(&self, inner: &SessionInner, queue: &mut QueueState) -> Option<Claim> {
        if queue.running || queue.paused || !Self::can_start(inner) {
            return None;
        }
        queue.start_head().map(|head| Claim::new(self, head))
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
        queue.next_id += 1;
        let id = format!("inp_{:032x}", queue.next_id);
        queue.items.push(Entry {
            id: id.clone(),
            mode,
            input,
            accepted_at: accepted_at.clone(),
        });
        queue.paused = false;
        let claim = self.claim_start(&inner, &mut queue);
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
        let was_paused = std::mem::take(&mut queue.paused);
        let claim = self.claim_start(&inner, &mut queue);
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
    async fn start_claim(&self, claim: Claim) -> Result<Option<StartedTurn>, AgentError> {
        let started = async {
            self.cleanup_finished().await?;
            let entry = claim.entry();
            let execution = self.execution_input(entry.input.clone())?;
            self.install_loop(execution, entry, claim.generation, None)
        }
        .await;
        match started {
            Ok((started, _)) => {
                claim.started();
                Ok(Some(started))
            }
            Err(AgentError::SessionBusy) => Ok(None),
            Err(error) => {
                claim.failed();
                Err(error)
            }
        }
    }

    /// A blocked Session starts nothing, so its busy period ends and a
    /// non-empty queue waits for `input.recall`.
    pub(super) fn end_period_blocked(&self) {
        let mut queue = self.lock_queue();
        queue.paused |= !queue.items.is_empty();
        queue.end_period();
        self.emit_queue(&queue);
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
            let claim = self.claim_start(&inner, &mut queue)?;
            self.emit_queue(&queue);
            claim
        };
        let started = self
            .execution_input(claim.entry().input.clone())
            .and_then(|execution| {
                self.install_loop(execution, claim.entry(), claim.generation, None)
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
    pub(super) fn close_turn(
        &self,
        turn: TurnRef,
        task: &Arc<SessionTask>,
        completion: &mut CompletionGuard,
        eligible: bool,
        settled: bool,
        result: &dyn Fn(Option<StartedTurn>) -> Arc<TurnResult>,
    ) -> Close {
        let (entry, generation) = {
            let inner = self.lock_inner();
            let mut queue = self.lock_queue();
            let successor = eligible && !inner.closing && queue.head().is_some();
            if successor && !settled {
                return Close::Settle;
            }
            let head = if successor { queue.start_head() } else { None };
            let Some(head) = head else {
                queue.paused |= !eligible && !queue.items.is_empty();
                queue.end_period();
                let result = result(None);
                completion.publish_finished(Arc::clone(&result));
                self.emit_queue(&queue);
                return Close::Done { result, run: None };
            };
            self.emit_queue(&queue);
            head
        };
        let started = self
            .execution_input(entry.input.clone())
            .and_then(|execution| {
                self.install_loop(execution, &entry, generation, Some((turn, task)))
            });
        match started {
            Ok((started, run)) => {
                let result = result(Some(started));
                completion.publish_finished(Arc::clone(&result));
                Close::Done { result, run }
            }
            Err(error) => {
                tracing::warn!(session_id = %turn.session_id, error = ?error,
                    "queued input could not start after the turn");
                let mut queue = self.lock_queue();
                queue.restore(entry);
                queue.end_period();
                queue.paused = true;
                let result = result(None);
                completion.publish_finished(Arc::clone(&result));
                self.emit_queue(&queue);
                Close::Done { result, run: None }
            }
        }
    }
}

#[cfg(test)]
mod tests;
