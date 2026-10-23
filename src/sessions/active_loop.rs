use super::*;

pub(super) struct ActiveLoop {
    pub(super) turn: TurnRef,
    pub(super) handle: LoopHandle,
    pub(super) completion: watch::Receiver<Option<TurnCompletion>>,
    pub(super) task: Arc<SessionTask>,
    pub(super) execution_summary: Option<BoundedText>,
    pub(super) execution_history: Arc<[HistoryItem]>,
    /// Abandons the post-turn check of this loop once a send needs the session.
    pub(super) abandon_check: CancellationToken,
}

pub(crate) struct ExecutionInput {
    pub(crate) request: LoopRequest,
    pub(crate) options: LoopOptions,
    pub(crate) summary: Option<BoundedText>,
}

/// An accepted submission always starts the ordinary execution path.
pub(crate) enum LoopSubmission {
    Accepted(LoopAccepted),
}

/// Owns one Agent worker without taking its JoinHandle into a local across an
/// await. If a join future is cancelled, the handle remains in this object so
/// a later close or cleanup call can join the same worker.
pub(super) struct SessionTask {
    pub(super) join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl SessionTask {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            join: tokio::sync::Mutex::new(Some(handle)),
        }
    }

    pub(super) async fn join(&self) -> Result<(), AgentError> {
        join_task(&self.join).await
    }
}

struct CompletionGuard {
    session: Session,
    sender: watch::Sender<Option<TurnCompletion>>,
    armed: bool,
}

impl CompletionGuard {
    fn new(session: Session, sender: watch::Sender<Option<TurnCompletion>>) -> Self {
        Self {
            session,
            sender,
            armed: true,
        }
    }

    fn publish_internal(&mut self) {
        if !self.armed {
            return;
        }
        mark_internal(&self.session);
        self.sender.send_replace(Some(TurnCompletion::Internal));
        self.session.emit_state();
        self.armed = false;
    }

    fn publish_finished(&mut self, result: Arc<TurnResult>) {
        self.sender
            .send_replace(Some(TurnCompletion::Finished(result)));
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
    Finished(Arc<TurnResult>),
    Internal,
}

/// The single Agent-owned task per active loop: forwards runtime events, joins
/// the loop, persists the report, merges sanitized history, and publishes the
/// Agent-level completion.
async fn run_active_loop(
    session: Session,
    turn: TurnRef,
    agent_loop: AgentLoop,
    events: minicore_runtime::LoopEventStream,
    mut completion: CompletionGuard,
) {
    let _ephemeral_cleanup = EphemeralSummaryCleanup {
        state: Arc::clone(&session.shared.compaction),
        loop_id: turn.loop_id,
    };
    #[cfg(test)]
    tests::panic_worker(turn.session_id);
    #[cfg(test)]
    tests::pause_worker(&PAUSE_BEFORE_JOIN, turn.session_id).await;
    let report = match join_runtime_loop(&session, turn, agent_loop, events).await {
        Ok(report) => report,
        Err(_) => {
            completion.publish_internal();
            return;
        }
    };
    let sanitized = match normalize_history(report.appended.as_ref()) {
        Ok(items) => items,
        Err(_) => {
            completion.publish_internal();
            return;
        }
    };
    let stored = reconcile_report(&session, turn, &report, &sanitized);
    let persistence = persist_report(&session, turn, &stored, &sanitized).await;
    let result = Arc::new(TurnResult {
        turn,
        report: Arc::clone(&report),
        persistence,
        interrupted: stored.interrupted.clone(),
    });
    publish_and_settle(&session, &result, &mut completion).await;
    // Remain in the owned task so close/shutdown still join auxiliary writes.
    persist_loop_tool_records(&session, turn.session_id, turn.loop_id).await;

    session.shared.events.try_send(AgentEvent::TurnFinished {
        turn,
        outcome: LoopOutcomeView::from_report(&report),
        persistence,
        meta: EventMeta::for_turn(turn),
    });
    session.emit_state();
    if persistence == TurnPersistence::Persisted
        && matches!(report.outcome, minicore_runtime::LoopOutcome::Completed)
    {
        check_post_turn(&session, turn).await;
    }
}

async fn join_runtime_loop(
    session: &Session,
    turn: TurnRef,
    agent_loop: AgentLoop,
    mut events: minicore_runtime::LoopEventStream,
) -> Result<Arc<LoopReport>, AgentError> {
    let join = agent_loop.join();
    tokio::pin!(join);
    let mut events_open = true;
    let result = loop {
        tokio::select! {
            biased;
            result = &mut join => break result,
            envelope = events.recv(), if events_open => {
                match envelope {
                    Some(envelope) => forward_loop_event(turn.session_id, envelope, session),
                    None => events_open = false,
                }
            }
        }
    };
    // Runtime join means the producer is finished. Drain only envelopes that
    // are already queued; waiting for channel closure here would add an
    // unnecessary completion dependency. This also accounts for a queued
    // `Finished` envelope without mapping it to Agent `TurnFinished`.
    while let Ok(envelope) = events.try_recv() {
        forward_loop_event(turn.session_id, envelope, session);
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
    // Durable normalization preserves validated replay but may remove legacy
    // opaque reasoning. Usage from the prior raw projection is then stale.
    session
        .shared
        .compaction
        .note_normalized_projection(report.appended.as_ref(), sanitized);
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
) -> TurnPersistence {
    // The same per-session IO lock serializes this metadata touch with
    // rename and settings updates; each write starts from current record state.
    let _io = session.shared.io.lock().await;
    match session
        .shared
        .store
        .append_loop(turn.session_id, stored)
        .await
    {
        Ok(()) => {
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
            TurnPersistence::Persisted
        }
        Err(error) => {
            tracing::warn!(
                session_id = %turn.session_id,
                loop_id = %turn.loop_id,
                error_kind = error.kind(),
                "history.jsonl append failed"
            );
            if matches!(error, crate::store::AppendError::Unknown) {
                session.shared.inner.lock().unwrap().blocked =
                    Some(SessionBlockReason::Persistence);
            }
            TurnPersistence::Failed
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

/// Publishes completion first: waiters never wait for emergency-projection
/// settlement, which stays owned by this task.
async fn publish_and_settle(
    session: &Session,
    result: &Arc<TurnResult>,
    completion: &mut CompletionGuard,
) {
    completion.publish_finished(Arc::clone(result));
    if result.persistence == TurnPersistence::Persisted {
        settle_persisted_projection(session, result.turn, &result.report).await;
    }
}

/// The owned task measures the settled history against the trigger without
/// publishing anything; only a crossed threshold becomes a compaction operation.
/// A send or a close abandons the check, which has no side effect to undo.
async fn check_post_turn(session: &Session, turn: TurnRef) {
    let (operation, _) = CompactionOperation::new(
        format!("auto-{}", turn.loop_id),
        CompactionOrigin::Automatic,
    );
    let mut join_slot = operation.join.lock().await;
    let Some(abandon) = session
        .lock_inner()
        .active
        .as_ref()
        .map(|active| active.abandon_check.clone())
    else {
        return;
    };
    let Some(reservation) = session.reserve_post_turn(turn, Arc::clone(&operation)) else {
        return;
    };
    let checked = tokio::select! {
        biased;
        () = abandon.cancelled() => return,
        () = session.shared.close.cancelled() => return,
        checked = compact::check_post_turn(session, &reservation) => checked,
    };
    match checked {
        compact::PostTurnCheck::Settled(result) => {
            session.publish_compaction_result(&operation, result);
            #[cfg(test)]
            tests::pause_worker(&PAUSE_AFTER_COMPACTION_RESULT, turn.session_id).await;
        }
        compact::PostTurnCheck::Compact(input) => {
            if session.begin_post_turn(turn, &reservation) {
                *join_slot = Some(tokio::spawn(compact::run_compaction(
                    session.clone(),
                    reservation,
                    Some(input),
                )));
            }
        }
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

fn mark_internal(session: &Session) {
    let mut inner = session.lock_inner();
    inner.blocked = Some(SessionBlockReason::Internal);
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
    receiver: watch::Receiver<Option<TurnCompletion>>,
) -> Result<Arc<TurnResult>, AgentError> {
    match await_completion(receiver).await? {
        TurnCompletion::Finished(result) => Ok(result),
        TurnCompletion::Internal => Err(AgentError::Internal),
    }
}

pub(crate) async fn await_compaction_completion(
    receiver: watch::Receiver<Option<CompactionResult>>,
) -> Result<CompactionResult, AgentError> {
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

fn map_steer_error(error: SteerError) -> AgentError {
    match error {
        SteerError::QueueFull => AgentError::SteerQueueFull,
        SteerError::WaitingForInput => AgentError::InvalidState,
        SteerError::NotActive => AgentError::TurnNotFound,
        SteerError::InvalidInput => AgentError::InvalidInput,
        _ => AgentError::Internal,
    }
}

fn map_answer_error(error: AnswerError) -> AgentError {
    match error {
        AnswerError::InteractionNotFound => AgentError::InteractionNotFound,
        AnswerError::NotActive => AgentError::TurnNotFound,
        _ => AgentError::InvalidInteraction,
    }
}

impl Session {
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
        let prompt = SteerReceiptPrompt::new(Arc::new(prompt), self.presentation());
        ExecutionConfig::new(
            Arc::clone(config.model()),
            config.reasoning(),
            config.tools().clone(),
            config.policy().cloned(),
            prompt,
        )
        .map_err(|_| AgentError::InvalidSessionSettings)
    }

    pub(crate) fn execution_input(&self, input: UserInput) -> Result<ExecutionInput, AgentError> {
        let (history, config, options, system_prompt, auto) = {
            let inner = self.lock_inner();
            Self::validate_install_state(&inner)?;
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

    /// Starts a new `AgentLoop` for one user message. A session runs at most
    /// one active loop; no queueing or auto-cancellation.
    pub(crate) async fn start_loop(&self, input: UserInput) -> Result<LoopAccepted, AgentError> {
        self.cleanup_finished().await?;
        let execution = self.execution_input(input)?;
        self.install_loop(execution)
    }

    /// Submission never starts a summary worker or a pre-turn handoff.
    pub(crate) async fn submit(&self, input: UserInput) -> Result<LoopSubmission, AgentError> {
        Ok(LoopSubmission::Accepted(self.start_loop(input).await?))
    }

    fn install_loop(&self, execution: ExecutionInput) -> Result<LoopAccepted, AgentError> {
        let accepted_at = crate::store::utc_timestamp().ok();
        let turn = {
            let mut inner = self.lock_inner();
            Self::validate_install_state(&inner)?;
            // Initialize current-loop observation before spawning the Runtime
            // task: the runner may reach Model::start synchronously.
            inner.presentation.reset_before_loop_start();
            self.shared.tool_observer.reset_before_loop_start();
            // AgentLoop::start only validates and spawns the Runtime task; it
            // does not await model work. Keeping this short lock held closes
            // the cancellation/close race between validation and ownership.
            #[cfg(test)]
            let captured_options = execution.options.clone();
            let execution_history = Arc::clone(&execution.request.history);
            let mut agent_loop = AgentLoop::start(execution.request, execution.options)
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
            #[cfg(test)]
            INSTALLED_LOOP_OPTIONS.arm(turn.loop_id, captured_options);
            let events = agent_loop.take_events().map_err(|_| AgentError::Internal)?;
            let (completion_tx, completion_rx) = watch::channel(None);
            let task = Arc::new(SessionTask::new(tokio::spawn(run_active_loop(
                self.clone(),
                turn,
                agent_loop,
                events,
                CompletionGuard::new(self.clone(), completion_tx),
            ))));
            inner.active = Some(ActiveLoop {
                turn,
                handle: handle.clone(),
                completion: completion_rx,
                task,
                execution_summary: execution.summary,
                execution_history,
                abandon_check: CancellationToken::new(),
            });
            inner
                .presentation
                .bind_started_loop(turn.loop_id, accepted_at.clone());
            inner.presentation.record_prompt_time(accepted_at.clone());
            turn
        };
        tracing::info!(
            session_id = %turn.session_id,
            loop_id = %turn.loop_id,
            "turn submitted"
        );
        Ok(LoopAccepted { turn, accepted_at })
    }

    fn validate_install_state(inner: &SessionInner) -> Result<(), AgentError> {
        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }
        if inner.closing
            || inner.active.is_some()
            || inner.compaction.is_some()
            || inner.pending_compaction.is_some()
        {
            return Err(AgentError::SessionBusy);
        }
        Ok(())
    }

    pub(super) fn clear_active_task(inner: &mut SessionInner, task: &Arc<SessionTask>) {
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
                    .then(|| (active.task.clone(), active.abandon_check.clone()))
            })
        };
        if let Some((task, abandon_check)) = active_task {
            abandon_check.cancel();
            if task.join().await.is_err() {
                let mut inner = self.lock_inner();
                Self::clear_active_task(&mut inner, &task);
                inner.blocked = Some(SessionBlockReason::Internal);
                drop(inner);
                self.emit_state();
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
    pub(crate) fn wait_receiver(
        &self,
        turn: TurnRef,
    ) -> Result<watch::Receiver<Option<TurnCompletion>>, AgentError> {
        let inner = self.lock_inner();
        inner
            .active
            .as_ref()
            .filter(|active| active.turn == turn)
            .map(|active| active.completion.clone())
            .ok_or(AgentError::TurnNotFound)
    }

    pub(crate) fn turn_result_snapshot(
        &self,
        turn: TurnRef,
    ) -> Result<Option<Option<Arc<TurnResult>>>, AgentError> {
        let inner = self.lock_inner();
        let Some(active) = inner.active.as_ref().filter(|active| active.turn == turn) else {
            return Ok(None);
        };
        match active.completion.borrow().as_ref() {
            None => Ok(Some(None)),
            Some(TurnCompletion::Finished(result)) => Ok(Some(Some(Arc::clone(result)))),
            Some(TurnCompletion::Internal) => Err(AgentError::Internal),
        }
    }

    pub(crate) async fn wait(&self, turn: TurnRef) -> Result<Arc<TurnResult>, AgentError> {
        let receiver = self.wait_receiver(turn)?;
        await_turn_completion(receiver).await
    }

    pub(crate) fn steer(&self, turn: TurnRef, text: String) -> Result<SteerAccepted, AgentError> {
        let input = UserInput::text(text).map_err(|_| AgentError::InvalidInput)?;
        let inner = self.lock_inner();
        let active = inner
            .active
            .as_ref()
            .filter(|active| active.turn == turn)
            .ok_or(AgentError::TurnNotFound)?;
        active.handle.steer(input).map_err(map_steer_error)?;
        let accepted_at = crate::store::utc_timestamp().ok();
        let steer_index = inner.presentation.note_steer_accepted(accepted_at.clone());
        Ok(SteerAccepted {
            accepted_at,
            steer_index,
        })
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
        active
            .handle
            .answer(interaction_id, answer)
            .map_err(map_answer_error)
    }
}
