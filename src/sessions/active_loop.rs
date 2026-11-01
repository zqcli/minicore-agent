use super::*;

pub(super) struct ActiveLoop {
    #[cfg(feature = "e2e-observe")]
    pub(super) _observed: crate::e2e_observe::Lease,
    pub(super) turn: TurnRef,
    pub(super) input: QueueItem,
    pub(super) handle: LoopHandle,
    pub(super) completion: watch::Receiver<Option<TurnCompletion>>,
    pub(super) task: Arc<OwnedTask>,
    pub(super) execution_summary: Option<BoundedText>,
    pub(super) execution_history: Arc<[HistoryItem]>,
    pub(super) boundary: Arc<RequestBoundary>,
    pub(super) generation: u64,
}

pub(super) struct FinishedTurn {
    pub(super) turn: TurnRef,
    pub(super) completion: watch::Receiver<Option<TurnCompletion>>,
    #[cfg(feature = "e2e-observe")]
    pub(super) _observed: crate::e2e_observe::Lease,
}

#[cfg(not(feature = "e2e-observe"))]
pub(crate) type TurnWaitReceiver = watch::Receiver<Option<TurnCompletion>>;
#[cfg(feature = "e2e-observe")]
pub(crate) struct TurnWaitReceiver {
    receiver: watch::Receiver<Option<TurnCompletion>>,
    _observed: crate::e2e_observe::Lease,
}

pub(crate) struct ExecutionInput {
    pub(crate) request: LoopRequest,
    pub(crate) options: LoopOptions,
    pub(crate) summary: Option<BoundedText>,
}

pub(super) struct CompletionGuard {
    session: Session,
    turn: TurnRef,
    sender: watch::Sender<Option<TurnCompletion>>,
    armed: bool,
}

impl CompletionGuard {
    fn new(session: Session, turn: TurnRef, sender: watch::Sender<Option<TurnCompletion>>) -> Self {
        Self {
            session,
            turn,
            sender,
            armed: true,
        }
    }

    fn publish_internal(&mut self) {
        if !self.armed {
            return;
        }
        let publication = self.session.shared.results.publication();
        let released = {
            let mut inner = self.session.lock_inner();
            inner.blocked = Some(SessionBlockReason::Internal);
            let mut queue = self.session.lock_queue();
            queue.paused |= !queue.items.is_empty();
            queue.end_period();
            let entry = CompletionEntry {
                seq: 0,
                loop_id: self.turn.loop_id,
                input_id: inner
                    .active
                    .as_ref()
                    .filter(|active| active.turn == self.turn)
                    .map(|active| active.input.id.clone()),
                completion_kind: CompletionKind::InternalError,
                outcome_kind: None,
                persistence: None,
                persistence_uncertain: false,
                availability: ResultAvailability::Unavailable,
                unavailable_reason: Some(UnavailableReason::InternalError),
                after_saved_loop_id: inner.last_saved,
            };
            let released = self
                .session
                .shared
                .results
                .insert(self.session.load(), entry, None);
            self.sender.send_replace(Some(TurnCompletion::Internal));
            released
        };
        drop(publication);
        drop(released);
        self.session.mark_dirty();
        self.armed = false;
    }

    pub(super) fn publish_finished(&mut self, result: Arc<TurnResult>) {
        self.sender.send_replace(Some(TurnCompletion::Finished {
            result,
            #[cfg(feature = "e2e-observe")]
            _observed: Arc::new(crate::e2e_observe::Lease::new(
                crate::e2e_observe::Gauge::Completions,
                1,
            )),
        }));
        self.session.mark_dirty();
        self.armed = false;
    }
}

pub(super) struct EphemeralSummaryCleanup {
    pub(super) state: Arc<CompactionState>,
    pub(super) loop_id: LoopId,
}

impl Drop for EphemeralSummaryCleanup {
    fn drop(&mut self) {
        self.state.clear_ephemeral(self.loop_id);
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.publish_internal();
    }
}

#[derive(Clone)]
pub(crate) enum TurnCompletion {
    Finished {
        result: Arc<TurnResult>,
        #[cfg(feature = "e2e-observe")]
        _observed: Arc<crate::e2e_observe::Lease>,
    },
    Internal,
}

/// One started loop with everything its worker needs to run it.
pub(super) struct LoopRun {
    turn: TurnRef,
    agent_loop: AgentLoop,
    events: minicore_runtime::LoopEventStream,
    completion: CompletionGuard,
}

/// The single Agent-owned task per busy period: runs one turn after another
/// while the input queue supplies successors.
async fn run_driver(session: Session, task: Arc<OwnedTask>, mut run: LoopRun) {
    while let Some(next) = run_turn(&session, &task, run).await {
        run = next;
    }
}

/// Forwards runtime events, joins the loop, persists the report, merges
/// sanitized history, and publishes the Agent-level completion. Returns the
/// successor turn the queue started, already installed.
async fn run_turn(session: &Session, task: &Arc<OwnedTask>, run: LoopRun) -> Option<LoopRun> {
    let LoopRun {
        turn,
        agent_loop,
        events,
        mut completion,
    } = run;
    let _ephemeral_cleanup = EphemeralSummaryCleanup {
        state: Arc::clone(&session.shared.compaction),
        loop_id: turn.loop_id,
    };
    #[cfg(test)]
    tests::panic_worker(turn.session_id);
    #[cfg(test)]
    tests::pause_worker(&PAUSE_BEFORE_JOIN, turn.session_id).await;
    let report = match join_runtime_loop(session, turn, agent_loop, events).await {
        Ok(report) => report,
        Err(_) => {
            completion.publish_internal();
            return None;
        }
    };
    #[cfg(feature = "e2e-observe")]
    let _report_owner = crate::e2e_observe::Lease::new(crate::e2e_observe::Gauge::DriverReports, 1);
    let sanitized = match normalize_history(report.appended.as_ref()) {
        Ok(items) => items,
        Err(_) => {
            completion.publish_internal();
            return None;
        }
    };
    let stored = reconcile_report(session, turn, &report, &sanitized);
    #[cfg(feature = "e2e-observe")]
    crate::e2e_observe::pause(
        crate::e2e_observe::Point::BeforePersist,
        turn.session_id,
        turn.loop_id,
        session.query_cancellation(),
    )
    .await;
    let (persistence, uncertain) = persist_report(session, turn, &stored, &sanitized).await;
    let result = Arc::new(TurnResult {
        turn,
        outcome: LoopOutcomeView::from_report(&report),
        usage: report.usage,
        requests: report.requests,
        tool_rounds: report.tool_rounds,
        final_config_revision: report.final_config_revision,
        persistence,
    });
    let (source, unavailable) = if persistence == TurnPersistence::Failed {
        let catalog = Arc::clone(&session.shared.results);
        let items = Arc::clone(&sanitized);
        let interrupted = stored.interrupted;
        let metadata = result.as_ref().clone();
        let load = session.load();
        match tokio::task::spawn_blocking(
            move || -> Result<Result<ReservedSource, UnavailableReason>, AgentError> {
                let bytes = crate::read::result_source_upper_bound(
                    &items,
                    interrupted.as_ref(),
                    &metadata,
                )?;
                let charge = match catalog.reserve(load, bytes) {
                    Ok(charge) => charge,
                    Err(reason) => return Ok(Err(reason)),
                };
                let source = crate::read::encode_result_source(&items, interrupted, metadata)?;
                Ok(catalog.finish_reservation(source, charge))
            },
        )
        .await
        {
            Ok(Ok(Ok(source))) => (Some(source), None),
            Ok(Ok(Err(reason))) => (None, Some(reason)),
            Ok(Err(_)) | Err(_) => (None, Some(UnavailableReason::InternalError)),
        }
    } else {
        (None, None)
    };
    let continue_period =
        session.complete_turn(&mut completion, result, source, unavailable, uncertain);
    // Waiters see the terminal result before auxiliary writes; a successor
    // needs their settled projection, and close still joins this same driver.
    finish_tail(session, turn, persistence, &report).await;
    continue_period
        .then(|| session.start_successor(turn, task))
        .flatten()
}

async fn finish_tail(
    session: &Session,
    turn: TurnRef,
    persistence: TurnPersistence,
    report: &LoopReport,
) {
    if persistence == TurnPersistence::Persisted {
        settle_persisted_projection(session, turn, report).await;
    }
    persist_loop_tool_records(session, turn.session_id, turn.loop_id).await;
}

async fn join_runtime_loop(
    session: &Session,
    turn: TurnRef,
    agent_loop: AgentLoop,
    mut events: minicore_runtime::LoopEventStream,
) -> Result<Arc<LoopReport>, AgentError> {
    let mut state = agent_loop.handle().watch_state();
    #[cfg(feature = "e2e-observe")]
    let observed = crate::e2e_observe::Lease::new(crate::e2e_observe::Gauge::Watchers, 1);
    #[cfg(test)]
    tests::pause_worker(&PAUSE_OBSERVER_INITIAL, turn.session_id).await;
    state.borrow_and_update();
    session.mark_dirty();
    let join = agent_loop.join();
    tokio::pin!(join);
    #[cfg(feature = "e2e-observe")]
    let dropped_tail = crate::e2e_observe::take_fault(
        crate::e2e_observe::Point::RuntimeEvents,
        turn.session_id,
        turn.loop_id,
    );
    #[cfg(feature = "e2e-observe")]
    if let Some(fault) = &dropped_tail {
        fault.applied("hold_until_runtime_join");
    }
    #[cfg(not(feature = "e2e-observe"))]
    let mut events_open = true;
    #[cfg(feature = "e2e-observe")]
    let mut events_open = dropped_tail.is_none();
    let mut state_open = true;
    let result = loop {
        tokio::select! {
            biased;
            result = &mut join => break result,
            changed = state.changed(), if state_open => {
                state.borrow_and_update();
                state_open = changed.is_ok();
                session.mark_dirty();
            }
            envelope = events.recv(), if events_open => {
                match envelope {
                    Some(envelope) => forward_loop_event(turn.session_id, envelope, session),
                    None => events_open = false,
                }
            }
        }
    };
    // Join can win over the final watch update. Runtime completion is still
    // Agent finishing until persistence and the completion catalog publish.
    session.mark_dirty();
    drop(state);
    #[cfg(feature = "e2e-observe")]
    drop(observed);
    // Runtime join means the producer is finished. Drain only envelopes that
    // are already queued; waiting for channel closure here would add an
    // unnecessary completion dependency. This also accounts for a queued
    // `Finished` envelope without emitting a second control notification.
    #[cfg(feature = "e2e-observe")]
    let mut discarded_events = 0usize;
    while let Ok(envelope) = events.try_recv() {
        #[cfg(feature = "e2e-observe")]
        if dropped_tail.is_some() {
            discarded_events += 1;
            continue;
        }
        forward_loop_event(turn.session_id, envelope, session);
    }
    #[cfg(feature = "e2e-observe")]
    if let Some(fault) = &dropped_tail {
        fault.dropped_events(discarded_events);
    }
    // Owned commands of this loop are joined before the report is reconciled
    // and persisted: a dropped tool future must not leave a process behind, and
    // the process record must be final before the terminal state is written.
    session.shared.tool_owners.join_loop(turn.loop_id).await;
    let report = match result {
        Ok(report) => {
            if let minicore_runtime::LoopOutcome::Failed(failure) = &report.outcome {
                tracing::warn!(
                    session_id = %turn.session_id,
                    loop_id = %turn.loop_id,
                    error_kind = failure.model_error().map(|error| format!("{:?}", error.kind())),
                    diagnostic_code = ?failure.diagnostic.code,
                    "loop failed"
                );
            }
            report
        }
        Err(_) => return Err(AgentError::Internal),
    };
    Ok(report)
}

fn reconcile_report(
    session: &Session,
    turn: TurnRef,
    report: &LoopReport,
    sanitized: &[HistoryItem],
) -> StoredLoopRecord {
    // The runtime loop is complete regardless of whether the later JSONL
    // append succeeds.
    session.presentation().note_loop_finished();
    // The joined report is authoritative: reconcile terminal tool state even
    // when the wrapper future was dropped by an outer Runtime deadline/cancel
    // or its best-effort events were lost.
    session
        .shared
        .tool_data
        .reconcile(turn.session_id, sanitized);
    let user_item_count = sanitized
        .iter()
        .filter(|item| matches!(item, HistoryItem::User(_)))
        .count();
    let user_times = session.presentation().peek_user_times(user_item_count);
    let interrupted = session
        .presentation()
        .take_interrupted(report.loop_id, sanitized)
        .filter(|_| matches!(report.outcome, minicore_runtime::LoopOutcome::Cancelled(_)));
    StoredLoopRecord {
        loop_id: report.loop_id,
        outcome: StoredLoopOutcome::from_report(report),
        items: sanitized.to_vec(),
        usage: report.usage,
        requests: report.requests,
        tool_rounds: report.tool_rounds,
        final_config_revision: report.final_config_revision,
        completed_at: utc_timestamp().unwrap_or_default(),
        user_times: (!user_times.is_empty()).then_some(user_times),
        interrupted,
    }
}

async fn persist_report(
    session: &Session,
    turn: TurnRef,
    stored: &StoredLoopRecord,
    sanitized: &[HistoryItem],
) -> (TurnPersistence, bool) {
    // The same per-session IO lock serializes this metadata touch with
    // rename and settings updates; each write starts from current record state.
    let _io = session.shared.io.lock().await;
    match session
        .shared
        .store
        .append_loop(turn.session_id, stored)
        .await
    {
        Ok(_) => {
            let history = Arc::clone(&session.shared.inner.lock().unwrap().history);
            let mut merged = Vec::with_capacity(history.len() + sanitized.len());
            merged.extend(history.iter().cloned());
            merged.extend(sanitized.iter().cloned());
            let merged = merged.into();
            {
                let mut inner = session.shared.inner.lock().unwrap();
                // Persisted user timestamps align by (loop_id, occurrence).
                let mut occurrence = 0usize;
                for item in sanitized.iter() {
                    if let HistoryItem::User(_) = item {
                        if let Some(Some(time)) = stored
                            .user_times
                            .as_ref()
                            .and_then(|times| times.get(occurrence))
                        {
                            Arc::make_mut(&mut inner.user_times)
                                .insert((stored.loop_id, occurrence), time.clone());
                        }
                        occurrence += 1;
                    }
                }
                inner.history = merged;
                inner.blocked = None;
            }
            session.presentation().clear_user_times();
            touch_updated_at_best_effort(session).await;
            (TurnPersistence::Persisted, false)
        }
        Err(error) => {
            tracing::warn!(
                session_id = %turn.session_id,
                loop_id = %turn.loop_id,
                error_kind = error.kind(),
                "history.jsonl append failed"
            );
            let uncertain = matches!(error, crate::store::AppendError::Unknown);
            if uncertain {
                let mut inner = session.lock_inner();
                inner.blocked = Some(SessionBlockReason::Persistence);
                inner.uncertain_loop = Some(turn.loop_id);
            }
            (TurnPersistence::Failed, uncertain)
        }
    }
}

async fn settle_persisted_projection(session: &Session, turn: TurnRef, report: &LoopReport) {
    if let Err(kind) = session.settle_emergency_projection(turn, report).await {
        session
            .shared
            .compaction
            .note_settlement_failure(turn.loop_id, kind == "emergency_settlement_write_unknown");
        if let Some(observation) = session
            .shared
            .compaction
            .recovery_observation()
            .filter(|observation| observation.loop_id == turn.loop_id)
        {
            session.shared.compaction.record_recovery_failure(
                turn.loop_id,
                observation.request_index,
                observation.before_tokens,
                observation.utility_usage,
                kind,
            );
        }
        tracing::warn!(session_id = %turn.session_id, loop_id = %turn.loop_id,
            failure_kind = kind, "emergency projection could not be durably settled");
    }
}

async fn persist_loop_tool_records(session: &Session, session_id: SessionId, loop_id: LoopId) {
    let tool_refs = session.shared.tool_data.loop_tool_refs(session_id, loop_id);
    if tool_refs.is_empty() {
        return;
    }
    let deadline = Instant::now() + crate::store::AUX_PERSIST_DEADLINE;
    let results = session
        .shared
        .store
        .persist_loop_tool_records(&tool_refs, session.shared.tool_data.as_ref(), deadline)
        .await;

    for (tool_ref, outcome) in results {
        let turn = TurnRef {
            session_id: tool_ref.session_id,
            loop_id: tool_ref.loop_id,
        };
        match outcome {
            Ok(()) => {
                session.shared.tool_observer.note_tool_recording(
                    &tool_ref,
                    crate::tool_data::ToolRecordingState::Saved,
                    turn,
                );
            }
            Err(error) => {
                tracing::warn!(
                    session_id = %tool_ref.session_id,
                    loop_id = %tool_ref.loop_id,
                    error_kind = error.kind(),
                    "auxiliary tool record persistence failed"
                );
                session.shared.tool_observer.note_tool_recording(
                    &tool_ref,
                    crate::tool_data::ToolRecordingState::Failed,
                    turn,
                );
            }
        }
    }
}

async fn touch_updated_at_best_effort(session: &Session) {
    let Ok(updated_at) = utc_timestamp() else {
        return;
    };
    let record = {
        let mut inner = session.lock_inner();
        let mut record = inner.record.clone();
        record.updated_at = updated_at.clone();
        inner.record = record.clone();
        record
    };
    if session.shared.store.write_record(&record).await.is_err() {
        tracing::warn!(
            session_id = %record.session_id,
            "session.json updated_at write failed"
        );
    }
}

/// Awaits an Agent-level turn completion. Used by `Session::wait` and the RPC
/// deferred waiter; never waits on a raw runtime `LoopHandle`.
pub(crate) async fn await_turn_completion(
    receiver: TurnWaitReceiver,
) -> Result<Arc<TurnResult>, AgentError> {
    #[cfg(feature = "e2e-observe")]
    let TurnWaitReceiver {
        receiver,
        _observed,
    } = receiver;
    match await_completion(receiver).await? {
        TurnCompletion::Finished { result, .. } => Ok(result),
        TurnCompletion::Internal => Err(AgentError::Internal),
    }
}

pub(crate) async fn await_compaction_completion(
    receiver: watch::Receiver<Option<CompactionCompletion>>,
) -> Result<CompactionCompletion, AgentError> {
    await_completion(receiver).await
}

async fn await_completion<T: Clone>(
    mut receiver: watch::Receiver<Option<T>>,
) -> Result<T, AgentError> {
    let value = receiver
        .wait_for(Option::is_some)
        .await
        .map_err(|_| AgentError::Internal)?;
    Ok(value.as_ref().expect("waited for completion").clone())
}

fn map_answer_error(error: AnswerError) -> AgentError {
    match error {
        AnswerError::InteractionNotFound => AgentError::InteractionNotFound,
        AnswerError::NotActive => AgentError::TurnNotFound,
        _ => AgentError::InvalidInteraction,
    }
}

impl Session {
    fn complete_turn(
        &self,
        completion: &mut CompletionGuard,
        result: Arc<TurnResult>,
        source: Option<ReservedSource>,
        unavailable: Option<UnavailableReason>,
        uncertain: bool,
    ) -> bool {
        let publication = self.shared.results.publication();
        let (released, continue_period) = {
            let mut inner = self.lock_inner();
            let mut queue = self.lock_queue();
            let eligible = result.persistence == TurnPersistence::Persisted
                && result.outcome == LoopOutcomeView::Completed;
            let continue_period = eligible && !inner.closing && queue.head().is_some();
            if !continue_period {
                queue.paused |= !eligible && !queue.items.is_empty();
                queue.end_period();
            }
            let outcome = match result.outcome {
                LoopOutcomeView::Completed => CompletionOutcome::Completed,
                LoopOutcomeView::Cancelled { .. } => CompletionOutcome::Cancelled,
                LoopOutcomeView::Failed { .. } => CompletionOutcome::Failed,
                LoopOutcomeView::Unknown => CompletionOutcome::Unknown,
            };
            let entry = CompletionEntry {
                seq: 0,
                loop_id: result.turn.loop_id,
                input_id: inner.active.as_ref().map(|active| active.input.id.clone()),
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
                after_saved_loop_id: inner.last_saved,
            };
            let released = self.shared.results.insert(self.load(), entry, source);
            if result.persistence == TurnPersistence::Persisted {
                inner.last_saved = Some(result.turn.loop_id);
            }
            completion.publish_finished(result);
            (released, continue_period)
        };
        drop(publication);
        drop(released);
        continue_period
    }

    fn start_successor(&self, turn: TurnRef, task: &Arc<OwnedTask>) -> Option<LoopRun> {
        let claim = {
            let inner = self.lock_inner();
            let mut queue = self.lock_queue();
            let head = if inner.closing {
                queue.paused |= !queue.items.is_empty();
                None
            } else {
                match queue.start_head() {
                    Ok(head) => head,
                    Err(_) => {
                        queue.paused = true;
                        None
                    }
                }
            };
            let Some(head) = head else {
                queue.end_period();
                self.mark_dirty();
                return None;
            };
            self.mark_dirty();
            Claim::new(self, head)
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
                    Some((turn, task)),
                )
            });
        match started {
            Ok(run) => {
                claim.started();
                #[cfg(test)]
                if let Some(gate) = PAUSE_AFTER_SUCCESSOR_INSTALL.take(&turn.session_id) {
                    tokio::task::block_in_place(|| gate.pass_blocking());
                }
                run
            }
            Err(error) => {
                tracing::warn!(session_id = %turn.session_id, error = ?error,
                    "queued input could not start after the turn");
                claim.failed();
                None
            }
        }
    }

    pub(super) fn bind_execution_config(
        &self,
        config: ExecutionConfig,
        summary: Option<BoundedText>,
        system_prompt: String,
        auto: Option<AutoContext>,
    ) -> Result<ExecutionConfig, AgentError> {
        if summary.is_none() && auto.is_none() {
            return Ok(config);
        }
        let workspace = self.workspace();
        let prompt = match summary {
            Some(summary) => ProjectPromptProvider::new_bound(
                workspace,
                system_prompt,
                summary,
                auto,
                Arc::clone(&self.shared.compaction),
            ),
            None => ProjectPromptProvider::with_auto(
                workspace,
                system_prompt,
                Arc::clone(&self.shared.compaction),
                auto,
            ),
        }
        .map_err(|_| AgentError::InvalidSessionSettings)?;
        ExecutionConfig::new(
            Arc::clone(config.model()),
            config.reasoning(),
            config.tools().clone(),
            config.policy().cloned(),
            Arc::new(prompt),
        )
        .map_err(|_| AgentError::InvalidSessionSettings)
    }

    /// Builds the config this loop serves, with the input-boundary observation
    /// installed on its prompt provider.
    pub(super) fn execution_config(
        &self,
        input: UserInput,
        generation: u64,
        boundary: &Arc<RequestBoundary>,
    ) -> Result<ExecutionInput, AgentError> {
        let mut execution = self.execution_input(input)?;
        execution.request.config =
            boundary::observe_config(&execution.request.config, boundary, generation)?;
        Ok(execution)
    }

    pub(crate) fn execution_input(&self, input: UserInput) -> Result<ExecutionInput, AgentError> {
        let (history, config, options, system_prompt, auto) = {
            let inner = self.lock_inner();
            (
                Arc::clone(&inner.history),
                inner.config.clone(),
                inner.options.clone(),
                inner.record.system_prompt.clone(),
                inner.auto.clone(),
            )
        };
        let (history, summary) = match self.shared.compaction.project(&history) {
            Some(projection) => (projection.suffix.to_vec().into(), Some(projection.summary)),
            None => (history, None),
        };
        let config = self.bind_execution_config(config, summary.clone(), system_prompt, auto)?;
        Ok(ExecutionInput {
            request: LoopRequest::new(history, input, config),
            options,
            summary,
        })
    }

    /// Starts the loop for a queue head. A busy period's first turn gets a
    /// new worker; a successor runs on the worker of the turn it follows and
    /// is returned for it to continue with.
    pub(super) fn install_loop_with_boundary(
        &self,
        execution: ExecutionInput,
        boundary: Arc<RequestBoundary>,
        entry: &Entry,
        generation: u64,
        successor: Option<(TurnRef, &Arc<OwnedTask>)>,
    ) -> Result<Option<LoopRun>, AgentError> {
        let (turn, run, evicted_waiter) = {
            let mut inner = self.lock_inner();
            Self::validate_install_state(&inner, successor.map(|(turn, _)| turn))?;
            #[cfg(test)]
            if FAIL_RUNTIME_START.hit(&inner.record.session_id) {
                return Err(AgentError::Internal);
            }
            // Initialize current-loop observation before spawning the Runtime
            // task: the runner may reach Model::start synchronously.
            inner.presentation.reset_before_loop_start();
            self.shared.tool_observer.reset_before_loop_start();
            inner.presentation.record_prompt_time(entry.accepted_at());
            // AgentLoop::start only validates and spawns the Runtime task; it
            // does not await model work. Keeping this short lock held closes
            // the cancellation/close race between validation and ownership.
            #[cfg(test)]
            let captured_options = execution.options.clone();
            let execution_history = Arc::clone(&execution.request.history);
            let inputs = Arc::new(SessionInputs::new(
                self.clone(),
                Arc::clone(&inner.presentation),
                generation,
                Arc::clone(&boundary),
            ));
            let mut agent_loop = AgentLoop::start(
                execution.request,
                execution.options,
                Arc::clone(&inputs) as Arc<dyn LoopInputs>,
            )
            .map_err(map_loop_start_error)?;
            #[cfg(test)]
            {
                inner = tests::pause_runtime_start(self, inner);
            }
            let handle = agent_loop.handle();
            let turn = TurnRef {
                session_id: inner.record.session_id,
                loop_id: handle.id(),
            };
            #[cfg(feature = "e2e-observe")]
            crate::e2e_observe::installed(turn, self.load(), &entry.item().id);
            inputs.bind(turn);
            #[cfg(test)]
            INSTALLED_LOOP_OPTIONS.arm(turn.loop_id, captured_options);
            let events = agent_loop.take_events().map_err(|_| AgentError::Internal)?;
            let (completion_tx, completion_rx) = watch::channel(None);
            let run = LoopRun {
                turn,
                agent_loop,
                events,
                completion: CompletionGuard::new(self.clone(), turn, completion_tx),
            };
            let (task, run) = match successor {
                Some((_, task)) => (Arc::clone(task), Some(run)),
                None => {
                    let task = Arc::new(OwnedTask::default());
                    task.set(tokio::spawn(run_driver(
                        self.clone(),
                        Arc::clone(&task),
                        run,
                    )));
                    (task, None)
                }
            };
            self.lock_queue().starting = None;
            let previous = inner.active.replace(ActiveLoop {
                #[cfg(feature = "e2e-observe")]
                _observed: crate::e2e_observe::Lease::new(crate::e2e_observe::Gauge::Active, 1),
                turn,
                input: entry.item(),
                handle: handle.clone(),
                completion: completion_rx.clone(),
                task,
                execution_summary: execution.summary,
                execution_history,
                boundary,
                generation,
            });
            let mut evicted_waiter = None;
            if let Some(previous) = previous {
                inner.finished.push_back(FinishedTurn {
                    turn: previous.turn,
                    completion: previous.completion,
                    #[cfg(feature = "e2e-observe")]
                    _observed: crate::e2e_observe::Lease::new(
                        crate::e2e_observe::Gauge::LateWait,
                        1,
                    ),
                });
                if inner.finished.len() > FINISHED_TURNS_KEPT {
                    evicted_waiter = inner.finished.pop_front();
                }
            }
            inner
                .presentation
                .bind_started_loop(turn.loop_id, entry.accepted_at());
            self.mark_dirty();
            (turn, run, evicted_waiter)
        };
        drop(evicted_waiter);
        tracing::info!(
            session_id = %turn.session_id,
            loop_id = %turn.loop_id,
            "turn submitted"
        );
        Ok(run)
    }

    fn validate_install_state(
        inner: &SessionInner,
        successor_of: Option<TurnRef>,
    ) -> Result<(), AgentError> {
        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }
        let active_matches = match (&inner.active, successor_of) {
            (None, None) => true,
            (Some(active), Some(turn)) => active.turn == turn,
            _ => false,
        };
        if inner.closing
            || !active_matches
            || inner.pending_compaction.is_some()
            || inner
                .compaction
                .as_ref()
                .is_some_and(|operation| !operation.is_completed())
        {
            return Err(AgentError::SessionBusy);
        }
        Ok(())
    }

    pub(super) fn clear_active_task(inner: &mut SessionInner, task: &Arc<OwnedTask>) {
        if inner
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(&active.task, task))
        {
            inner.active = None;
        }
    }

    pub(crate) async fn cleanup_finished(&self) -> Result<(), AgentError> {
        let active_task = {
            let inner = self.lock_inner();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            inner.active.as_ref().and_then(|active| {
                active
                    .completion
                    .borrow()
                    .is_some()
                    .then(|| active.task.clone())
            })
        };
        if let Some(task) = active_task {
            if task.join().await.is_err() {
                let mut inner = self.lock_inner();
                Self::clear_active_task(&mut inner, &task);
                inner.blocked = Some(SessionBlockReason::Internal);
                drop(inner);
                self.mark_dirty();
                return Err(AgentError::SessionBlocked);
            }
            let mut inner = self.lock_inner();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            Self::clear_active_task(&mut inner, &task);
        }
        if self.reap_finished_compaction().await? {
            return Ok(());
        }
        Err(AgentError::SessionBusy)
    }

    pub(super) fn cancel_active_on_drop(&self) {
        let handle = {
            let inner = self.lock_inner();
            inner.active.as_ref().map(|active| active.handle.clone())
        };
        if let Some(handle) = handle {
            let _ = handle.cancel();
        }
    }

    /// Agent-level completion: the runtime loop has joined and the report has
    /// been persisted before this resolves.
    pub(crate) fn wait_receiver(&self, turn: TurnRef) -> Result<TurnWaitReceiver, AgentError> {
        let inner = self.lock_inner();
        let receiver = Self::completion_receiver(&inner, turn)
            .cloned()
            .ok_or(AgentError::TurnNotFound)?;
        #[cfg(feature = "e2e-observe")]
        let receiver = TurnWaitReceiver {
            receiver,
            _observed: crate::e2e_observe::Lease::new(crate::e2e_observe::Gauge::Waiters, 1),
        };
        Ok(receiver)
    }

    fn completion_receiver(
        inner: &SessionInner,
        turn: TurnRef,
    ) -> Option<&watch::Receiver<Option<TurnCompletion>>> {
        inner
            .active
            .as_ref()
            .filter(|active| active.turn == turn)
            .map(|active| &active.completion)
            .or_else(|| {
                inner
                    .finished
                    .iter()
                    .find(|finished| finished.turn == turn)
                    .map(|finished| &finished.completion)
            })
    }

    pub(crate) fn is_turn_pending(&self, turn: TurnRef) -> bool {
        self.lock_inner()
            .active
            .as_ref()
            .is_some_and(|active| active.turn == turn && active.completion.borrow().is_none())
    }

    pub(crate) fn completed_turn(&self, turn: TurnRef) -> Option<Arc<TurnResult>> {
        let inner = self.lock_inner();
        match Self::completion_receiver(&inner, turn)?.borrow().as_ref() {
            Some(TurnCompletion::Finished { result, .. }) => Some(Arc::clone(result)),
            _ => None,
        }
    }

    pub(crate) async fn wait(&self, turn: TurnRef) -> Result<Arc<TurnResult>, AgentError> {
        let receiver = self.wait_receiver(turn)?;
        await_turn_completion(receiver).await
    }

    pub(crate) fn cancel(&self, turn: TurnRef) -> Result<bool, AgentError> {
        let inner = self.lock_inner();
        let active = inner
            .active
            .as_ref()
            .filter(|active| active.turn == turn)
            .ok_or(AgentError::TurnNotFound)?;
        if active.handle.is_finished() {
            return Ok(false);
        }
        Ok(active.handle.cancel())
    }

    pub(crate) fn answer(
        &self,
        turn: TurnRef,
        interaction_id: InteractionId,
        answer: InteractionAnswer,
    ) -> Result<(), AgentError> {
        let inner = self.lock_inner();
        let active = inner
            .active
            .as_ref()
            .filter(|active| active.turn == turn)
            .ok_or(AgentError::TurnNotFound)?;
        #[cfg(feature = "e2e-observe")]
        let observed_state = active.handle.state();
        #[cfg(feature = "e2e-observe")]
        let observed_answer = crate::e2e_observe::input_fixture::AnswerClass::of(&answer);
        let result = active
            .handle
            .answer(interaction_id, answer)
            .map_err(map_answer_error);
        #[cfg(feature = "e2e-observe")]
        crate::e2e_observe::input_fixture::answered(
            self.load(),
            &observed_state,
            interaction_id,
            observed_answer,
            result.is_ok(),
        );
        result
    }
}
