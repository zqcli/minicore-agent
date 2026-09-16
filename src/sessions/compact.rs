//! Manual compaction work, commit, cancellation, and completion helpers.
use super::*;

struct CompactionCompletionGuard {
    session: Session,
    operation: Arc<CompactionOperation>,
    armed: bool,
}

impl CompactionOperation {
    pub(super) fn new(
        operation_id: String,
    ) -> (Arc<Self>, watch::Receiver<Option<CompactionResult>>) {
        let (result, receiver) = watch::channel(None);
        let operation = Arc::new(Self {
            operation_id,
            cancellation: CancellationToken::new(),
            result,
            join: tokio::sync::Mutex::new(None),
            state: AtomicU8::new(COMPACTION_RUNNING),
        });
        (operation, receiver)
    }

    pub(super) fn cancel(&self) -> bool {
        if self
            .state
            .compare_exchange(
                COMPACTION_RUNNING,
                COMPACTION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        self.cancellation.cancel();
        true
    }

    pub(super) fn cancellation_requested(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPACTION_CANCELLED
            || self.cancellation.is_cancelled()
    }

    /// Marks the operation terminal without publishing a manual result. Used
    /// by a startup admission after it has taken ownership of its loop, so a
    /// later cancel finds nothing to cancel.
    pub(super) fn finish(&self) {
        self.state.store(COMPACTION_COMPLETED, Ordering::Release);
    }

    fn try_begin_commit(&self) -> bool {
        self.state
            .compare_exchange(
                COMPACTION_RUNNING,
                COMPACTION_COMMITTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn commit_started(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPACTION_COMMITTING
    }

    fn failed_result(
        &self,
        failure_kind: &'static str,
        utility_usage: Option<CompactionUtilityUsage>,
    ) -> CompactionResult {
        CompactionResult {
            operation_id: self.operation_id.clone(),
            status: CompactionStatus::Failed,
            before_tokens: None,
            after_tokens: None,
            covered_loop_count: 0,
            covered_item_count: 0,
            retained_item_count: 0,
            utility_usage,
            failure_kind: Some(failure_kind.to_owned()),
        }
    }

    fn unknown_write_result(&self) -> CompactionResult {
        CompactionResult {
            operation_id: self.operation_id.clone(),
            status: CompactionStatus::UnknownWrite,
            before_tokens: None,
            after_tokens: None,
            covered_loop_count: 0,
            covered_item_count: 0,
            retained_item_count: 0,
            utility_usage: None,
            failure_kind: Some("write_outcome_unknown".to_owned()),
        }
    }

    fn publish(&self, result: CompactionResult) -> CompactionResult {
        let result = match self.state.compare_exchange(
            COMPACTION_RUNNING,
            COMPACTION_COMPLETED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => result,
            Err(COMPACTION_CANCELLED) => {
                self.state.store(COMPACTION_COMPLETED, Ordering::Release);
                self.failed_result("cancelled", result.utility_usage.clone())
            }
            Err(COMPACTION_COMMITTING) => {
                self.state.store(COMPACTION_COMPLETED, Ordering::Release);
                result
            }
            Err(COMPACTION_COMPLETED) => result,
            Err(_) => result,
        };
        self.result.send_replace(Some(result.clone()));
        result
    }

    pub(super) async fn join(&self) -> Result<(), AgentError> {
        let mut slot = self.join.lock().await;
        let Some(handle) = slot.as_mut() else {
            return Ok(());
        };
        let result = std::pin::Pin::new(handle).await;
        slot.take();
        result.map_err(|_| AgentError::Internal)
    }
}

impl CompactionCompletionGuard {
    fn new(session: Session, operation: Arc<CompactionOperation>) -> Self {
        Self {
            session,
            operation,
            armed: true,
        }
    }

    fn publish(&mut self, result: CompactionResult) {
        if !self.armed {
            return;
        }
        self.session
            .publish_compaction_result(&self.operation, result);
        self.armed = false;
    }
}

impl Drop for CompactionCompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let result = if self.operation.commit_started() {
            self.operation.unknown_write_result()
        } else if self.operation.cancellation_requested() {
            self.operation.failed_result("cancelled", None)
        } else {
            self.operation.failed_result("internal", None)
        };
        self.session
            .publish_compaction_result(&self.operation, result);
    }
}

impl Session {
    pub(crate) async fn start_compaction(
        &self,
        operation_id: String,
        model: Arc<dyn Model>,
        descriptor: ModelDescriptor,
    ) -> Result<watch::Receiver<Option<CompactionResult>>, AgentError> {
        self.cleanup_finished().await?;
        let _io = self.shared.io.lock().await;
        let (operation, receiver) = CompactionOperation::new(operation_id.clone());

        // Acquire the operation's join slot before publishing the reservation.
        // From the point where the Session becomes busy to the point where the
        // spawned task is stored, there is no further await at which the only
        // JoinHandle could be dropped and detached.
        let mut join_slot = operation.join.lock().await;
        let reservation = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner.blocked.is_some() {
                return Err(AgentError::SessionBlocked);
            }
            if inner.closing || inner.active.is_some() || inner.compaction.is_some() {
                return Err(AgentError::SessionBusy);
            }
            if inner.used_compaction_ids.contains(&operation_id) {
                return Err(AgentError::SessionBusy);
            }
            if inner.used_compaction_ids.len() >= MAX_USED_COMPACTION_IDS {
                return Err(AgentError::InvalidInput);
            }
            let previous = self.shared.compaction.project(&inner.history);
            let (previous_summary, previous_covered_item_count) = previous
                .map(|projection| {
                    let covered_item_count =
                        inner.history.len().saturating_sub(projection.suffix.len());
                    (Some(projection.summary), covered_item_count)
                })
                .unwrap_or((None, 0));
            let tool_schemas = frozen_tool_specs(&inner.config, &inner.record);
            let retained_item_count = inner
                .history
                .len()
                .saturating_sub(previous_covered_item_count);
            // Explicit manual compaction keeps its original half-window
            // target. The Agent-global policy controls automatic compaction;
            // changing it must not silently change manual behavior.
            let target_tokens = descriptor.context_window / 2;
            inner.compaction = Some(Arc::clone(&operation));
            inner.used_compaction_ids.insert(operation_id.clone());
            inner.compaction_progress = Some(CompactionProgress {
                operation_id: operation_id.clone(),
                phase: CompactionPhase::Preparing,
                covered_item_count: previous_covered_item_count,
                retained_item_count,
            });
            CompactionReservation {
                operation: Arc::clone(&operation),
                model,
                record: inner.record.clone(),
                history: Arc::clone(&inner.history),
                workspace: Arc::clone(&inner.workspace),
                tool_schemas,
                previous_summary,
                previous_covered_item_count,
                hard_tokens: descriptor.context_window,
                target_tokens,
                safe_before_estimate: false,
                refresh_summary: false,
                deadline: Instant::now()
                    .checked_add(inner.options.model_timeout)
                    .unwrap_or_else(Instant::now),
            }
        };
        drop(_io);
        let task = tokio::spawn(run_compaction(self.clone(), reservation));
        *join_slot = Some(task);
        drop(join_slot);
        self.emit_state();
        Ok(receiver)
    }
}

impl Session {
    pub(crate) fn cancel_compaction(&self, operation_id: &str) -> Result<bool, AgentError> {
        // A startup preparation reserves the same `compaction` slot and
        // operation id, so this cancels both a manual operation and a
        // preparation that has not started its loop.
        let operation = {
            let inner = self.shared.inner.lock().unwrap();
            inner
                .compaction
                .as_ref()
                .filter(|operation| operation.operation_id == operation_id)
                .cloned()
        };
        let Some(operation) = operation else {
            return Ok(false);
        };
        Ok(operation.cancel())
    }
}

impl Session {
    pub(super) fn cancel_compaction_on_drop(&self) {
        let operation = {
            let inner = self.shared.inner.lock().unwrap();
            inner.compaction.clone()
        };
        if let Some(operation) = operation {
            let _ = operation.cancel();
        }
    }
}

impl Session {
    pub(super) async fn reap_finished_compaction(&self) -> Result<bool, AgentError> {
        let operation = {
            let inner = self.shared.inner.lock().unwrap();
            inner.compaction.clone()
        };
        let Some(operation) = operation else {
            return Ok(true);
        };
        if operation.result.borrow().is_none() {
            return Ok(false);
        }
        if operation.join().await.is_err() {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation))
            {
                inner.compaction = None;
                inner.compaction_progress = None;
            }
            inner.blocked = Some(SessionBlockReason::Internal);
            drop(inner);
            self.emit_state();
            return Err(AgentError::SessionBlocked);
        }
        let cleared = {
            let mut inner = self.shared.inner.lock().unwrap();
            let same = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation));
            if same {
                inner.compaction = None;
            }
            same && inner.compaction_progress.take().is_some()
        };
        if cleared {
            self.emit_state();
        }
        Ok(true)
    }
}

impl Session {
    fn publish_compaction_result(&self, operation: &CompactionOperation, result: CompactionResult) {
        // Clear wire-visible progress before waking result waiters. The
        // operation handle remains Session-owned until a later join.
        let should_emit = {
            let mut inner = self.shared.inner.lock().unwrap();
            let should_clear = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| std::ptr::eq(candidate.as_ref(), operation));
            let should_emit = should_clear && inner.compaction_progress.take().is_some();
            let result = operation.publish(result);
            inner.last_compaction_result = Some(result);
            should_emit
        };
        if should_emit {
            self.emit_state();
        }
    }
}

impl Session {
    fn set_compaction_phase(&self, operation: &CompactionOperation, phase: CompactionPhase) {
        let changed = {
            let mut inner = self.shared.inner.lock().unwrap();
            if inner
                .compaction
                .as_ref()
                .is_none_or(|candidate| !std::ptr::eq(candidate.as_ref(), operation))
            {
                false
            } else if let Some(progress) = inner.compaction_progress.as_mut() {
                progress.phase = phase;
                true
            } else {
                false
            }
        };
        if changed {
            self.emit_state();
        }
    }
}

impl Session {
    async fn commit_compaction(
        &self,
        reservation: &CompactionReservation,
        source: &crate::store::HistoryPrefix,
        bytes: &[u8],
        summary: &minicore_runtime::value::BoundedText,
    ) -> Result<CompactionCommit, StoreError> {
        if reservation.operation.cancellation_requested() {
            return Ok(CompactionCommit::Cancelled);
        }
        let Some(remaining) = reservation
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return Ok(CompactionCommit::Deadline);
        };
        let _io = match tokio::time::timeout(remaining, self.shared.io.lock()).await {
            Ok(io) => io,
            Err(_) => return Ok(CompactionCommit::Deadline),
        };
        let operation = Arc::clone(&reservation.operation);
        let expected_history = Arc::clone(&reservation.history);
        {
            let inner = self.shared.inner.lock().unwrap();
            if operation.cancellation_requested() {
                return Ok(CompactionCommit::Cancelled);
            }
            if Instant::now() >= reservation.deadline {
                return Ok(CompactionCommit::Deadline);
            }
            let current = inner
                .compaction
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation));
            if inner.closing || !current || !Arc::ptr_eq(&inner.history, &expected_history) {
                return Ok(CompactionCommit::Rejected);
            }
        }

        let session = self.clone();
        let deadline = reservation.deadline;
        let callback_operation = Arc::clone(&operation);
        let callback_history = Arc::clone(&expected_history);
        let commit = self
            .shared
            .store
            .commit_summary(
                session.session_id(),
                source,
                expected_history.as_ref(),
                bytes,
                reservation.deadline,
                move || {
                    let inner = session.shared.inner.lock().unwrap();
                    let current = inner
                        .compaction
                        .as_ref()
                        .is_some_and(|candidate| Arc::ptr_eq(candidate, &callback_operation));
                    if inner.closing
                        || !current
                        || !Arc::ptr_eq(&inner.history, &callback_history)
                        || callback_operation.cancellation_requested()
                        || Instant::now() >= deadline
                    {
                        return false;
                    }
                    callback_operation.try_begin_commit()
                },
            )
            .await?;

        match commit {
            SummaryCommit::Committed => {
                // The write is definite. Publish only if the Session still
                // owns the same settled history and operation at this
                // boundary; otherwise the disk result stays known but the
                // old in-memory projection remains authoritative.
                let publish = {
                    let inner = self.shared.inner.lock().unwrap();
                    let current = inner
                        .compaction
                        .as_ref()
                        .is_some_and(|candidate| Arc::ptr_eq(candidate, &operation));
                    !inner.closing && current && Arc::ptr_eq(&inner.history, &expected_history)
                };
                if publish {
                    self.shared.compaction.publish(
                        summary.clone(),
                        source.covered_loop_count,
                        expected_history.len(),
                    );
                }
                Ok(CompactionCommit::Store(SummaryCommit::Committed))
            }
            SummaryCommit::Rejected if operation.cancellation_requested() => {
                Ok(CompactionCommit::Cancelled)
            }
            SummaryCommit::Rejected if Instant::now() >= reservation.deadline => {
                Ok(CompactionCommit::Deadline)
            }
            SummaryCommit::Rejected => Ok(CompactionCommit::Store(SummaryCommit::Rejected)),
            SummaryCommit::Unknown => Ok(CompactionCommit::Store(SummaryCommit::Unknown)),
        }
    }
}

enum CompactionCommit {
    Cancelled,
    Deadline,
    Rejected,
    Store(SummaryCommit),
}

async fn run_compaction(session: Session, reservation: CompactionReservation) {
    let operation = Arc::clone(&reservation.operation);
    let mut completion = CompactionCompletionGuard::new(session.clone(), Arc::clone(&operation));
    let result = run_compaction_inner(&session, &reservation).await;
    completion.publish(result);
    #[cfg(test)]
    if let Some(gate) = take_pause_after_compaction_result(session.session_id()) {
        gate.started.notify_one();
        gate.release.notified().await;
    }
}

pub(super) async fn run_compaction_inner(
    session: &Session,
    reservation: &CompactionReservation,
) -> CompactionResult {
    let history_len = reservation.history.len();
    let retained_item_count = history_len.saturating_sub(reservation.previous_covered_item_count);
    if reservation.operation.cancellation_requested() {
        return failed_compaction(&reservation.operation, "cancelled", history_len);
    }
    if history_len == 0
        || (retained_item_count == 0
            && (!reservation.refresh_summary || reservation.previous_summary.is_none()))
    {
        return CompactionResult {
            operation_id: reservation.operation.operation_id.clone(),
            status: CompactionStatus::Noop,
            before_tokens: None,
            after_tokens: None,
            covered_loop_count: 0,
            covered_item_count: reservation.previous_covered_item_count,
            retained_item_count: 0,
            utility_usage: None,
            failure_kind: None,
        };
    }

    let Some(remaining) = reservation
        .deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
    else {
        return failed_compaction(&reservation.operation, "timeout", history_len);
    };
    let source = match tokio::time::timeout(
        remaining,
        session
            .shared
            .store
            .capture_history_anchor(session.session_id(), reservation.history.as_ref()),
    )
    .await
    {
        Ok(Ok(Some(source))) => source,
        Ok(Ok(None)) => {
            return failed_compaction(&reservation.operation, "history_changed", history_len);
        }
        Ok(Err(_)) => return failed_compaction(&reservation.operation, "store", history_len),
        Err(_) => return failed_compaction(&reservation.operation, "timeout", history_len),
    };

    if reservation.operation.cancellation_requested() {
        return failed_compaction(&reservation.operation, "cancelled", history_len);
    }
    let Some(remaining) = reservation
        .deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
    else {
        return failed_compaction(&reservation.operation, "timeout", history_len);
    };
    let agents = match tokio::time::timeout(
        remaining,
        reservation
            .workspace
            .read_prefix(crate::prompt::AGENTS_PATH, crate::prompt::MAX_AGENTS_BYTES),
    )
    .await
    {
        Ok(Ok(prefix)) => match crate::prompt::decode_agents(&prefix) {
            Ok(content) => (!content.is_empty()).then_some(content),
            Err(_) => return failed_compaction(&reservation.operation, "workspace", history_len),
        },
        Ok(Err(WorkspaceError::NotFound)) => None,
        Ok(Err(_)) => return failed_compaction(&reservation.operation, "workspace", history_len),
        Err(_) => return failed_compaction(&reservation.operation, "timeout", history_len),
    };
    let system_prompt =
        match minicore_runtime::value::BoundedText::new(reservation.record.system_prompt.clone())
            .ok()
            .and_then(|system| crate::prompt::build_system_prompt(&system, agents).ok())
        {
            Some(system_prompt) => system_prompt,
            None => return failed_compaction(&reservation.operation, "prompt", history_len),
        };

    let input = CompactionInput {
        model: Arc::clone(&reservation.model),
        reasoning: reservation.record.reasoning,
        history: Arc::clone(&reservation.history),
        previous_summary: reservation.previous_summary.clone(),
        previous_covered_item_count: reservation.previous_covered_item_count,
        project_instructions: system_prompt,
        tool_schemas: reservation.tool_schemas.clone(),
        hard_tokens: reservation.hard_tokens,
        target_tokens: reservation.target_tokens,
        safe_before_estimate: reservation.safe_before_estimate,
        operation_deadline: reservation.deadline,
    };
    session.set_compaction_phase(&reservation.operation, CompactionPhase::Summarizing);
    let mut mark_merging =
        || session.set_compaction_phase(&reservation.operation, CompactionPhase::Merging);
    let generated = match generate_summary(
        &input,
        &reservation.operation.cancellation,
        &mut mark_merging,
    )
    .await
    {
        Ok(generated) => generated,
        Err(error) => {
            return failed_compaction_with_usage(
                &reservation.operation,
                error.error.kind(),
                history_len,
                error.utility_usage,
            );
        }
    };
    if reservation.operation.cancellation_requested() {
        return failed_compaction_with_usage(
            &reservation.operation,
            "cancelled",
            history_len,
            generated.utility_usage.clone(),
        );
    }
    if Instant::now() >= reservation.deadline {
        return failed_compaction_with_usage(
            &reservation.operation,
            "timeout",
            history_len,
            generated.utility_usage.clone(),
        );
    }
    session.set_compaction_phase(&reservation.operation, CompactionPhase::Committing);
    let Some(bytes) = crate::compaction::encode_snapshot(
        session.session_id(),
        &reservation.record,
        &source,
        &generated.content,
    ) else {
        return failed_compaction_with_usage(
            &reservation.operation,
            "too_large",
            history_len,
            generated.utility_usage.clone(),
        );
    };
    let commit = match session
        .commit_compaction(reservation, &source, &bytes, &generated.content)
        .await
    {
        Ok(commit) => commit,
        Err(_) => {
            return failed_compaction_with_usage(
                &reservation.operation,
                "store",
                history_len,
                generated.utility_usage.clone(),
            );
        }
    };
    match commit {
        CompactionCommit::Store(SummaryCommit::Committed) => CompactionResult {
            operation_id: reservation.operation.operation_id.clone(),
            status: CompactionStatus::Compacted,
            before_tokens: Some(generated.before_tokens),
            after_tokens: Some(generated.after_tokens),
            covered_loop_count: source.covered_loop_count,
            covered_item_count: history_len,
            retained_item_count: 0,
            utility_usage: generated.utility_usage,
            failure_kind: None,
        },
        CompactionCommit::Cancelled => failed_compaction_with_usage(
            &reservation.operation,
            "cancelled",
            history_len,
            generated.utility_usage.clone(),
        ),
        CompactionCommit::Deadline => failed_compaction_with_usage(
            &reservation.operation,
            "timeout",
            history_len,
            generated.utility_usage.clone(),
        ),
        CompactionCommit::Rejected | CompactionCommit::Store(SummaryCommit::Rejected) => {
            failed_compaction_with_usage(
                &reservation.operation,
                "history_changed",
                history_len,
                generated.utility_usage.clone(),
            )
        }
        CompactionCommit::Store(SummaryCommit::Unknown) => CompactionResult {
            operation_id: reservation.operation.operation_id.clone(),
            status: CompactionStatus::UnknownWrite,
            before_tokens: Some(generated.before_tokens),
            after_tokens: Some(generated.after_tokens),
            covered_loop_count: source.covered_loop_count,
            covered_item_count: reservation.previous_covered_item_count,
            retained_item_count,
            utility_usage: generated.utility_usage,
            failure_kind: Some("write_outcome_unknown".to_owned()),
        },
    }
}

fn failed_compaction(
    operation: &CompactionOperation,
    failure_kind: &'static str,
    history_len: usize,
) -> CompactionResult {
    failed_compaction_with_usage(operation, failure_kind, history_len, None)
}

fn failed_compaction_with_usage(
    operation: &CompactionOperation,
    failure_kind: &'static str,
    history_len: usize,
    utility_usage: Option<CompactionUtilityUsage>,
) -> CompactionResult {
    CompactionResult {
        operation_id: operation.operation_id.clone(),
        status: CompactionStatus::Failed,
        before_tokens: None,
        after_tokens: None,
        covered_loop_count: 0,
        covered_item_count: 0,
        retained_item_count: history_len,
        utility_usage,
        failure_kind: Some(failure_kind.to_owned()),
    }
}
