//! Auxiliary tool record, output blob, quota, and cold-read implementation.
use super::*;

#[cfg(test)]
static AUX_BLOB_WRITE_FAILURES: OnceLock<Mutex<Vec<(SessionId, usize)>>> = OnceLock::new();
#[cfg(test)]
type AuxTempCreationGateEntry = (SessionId, Arc<AuxCommitGate>);
#[cfg(test)]
static AUX_TEMP_CREATION_GATES: OnceLock<Mutex<Vec<AuxTempCreationGateEntry>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn fail_aux_blob_write_after(session_id: SessionId, successful_writes: usize) {
    AUX_BLOB_WRITE_FAILURES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, successful_writes));
}

#[cfg(test)]
fn should_fail_aux_blob_write(session_id: SessionId) -> bool {
    let Some(mutex) = AUX_BLOB_WRITE_FAILURES.get() else {
        return false;
    };
    let mut failures = mutex.lock().unwrap();
    let Some(pos) = failures.iter().position(|(item, _)| *item == session_id) else {
        return false;
    };
    if failures[pos].1 == 0 {
        failures.remove(pos);
        true
    } else {
        failures[pos].1 -= 1;
        false
    }
}

#[cfg(test)]
pub(crate) fn register_aux_temp_creation_gate(session_id: SessionId, gate: Arc<AuxCommitGate>) {
    AUX_TEMP_CREATION_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((session_id, gate));
}

#[cfg(test)]
async fn wait_aux_temp_creation_gate(session_id: SessionId) {
    let Some(mutex) = AUX_TEMP_CREATION_GATES.get() else {
        return;
    };
    let gate = {
        let mut entries = mutex.lock().unwrap();
        let Some(pos) = entries.iter().position(|(item, _)| *item == session_id) else {
            return;
        };
        entries.remove(pos).1
    };
    gate.entered.notify_one();
    gate.release.notified().await;
}

impl Store {
    #[cfg(test)]
    pub(crate) fn with_aux_limits(mut self, limits: AuxLimits) -> Self {
        self.aux_limits = Some(limits);
        self
    }

    fn aux_limits(&self) -> AuxLimits {
        #[cfg(test)]
        if let Some(limits) = self.aux_limits {
            return limits;
        }
        DEFAULT_AUX_LIMITS
    }

    /// Atomically persists one completed tool call's metadata and retained
    /// raw stream windows to the auxiliary directory.
    #[cfg(test)]
    pub(crate) async fn commit_tool_record(
        &self,
        snapshot: &ToolPersistenceSnapshot,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        if should_fail_aux_write(snapshot.tool_ref.session_id) {
            return Err(StoreError::Unavailable);
        }
        let _guard = match tokio::time::timeout_at(deadline.into(), self.aux_lock.lock()).await {
            Ok(guard) => guard,
            Err(_) => return Err(StoreError::Unavailable),
        };
        self.commit_tool_record_locked(snapshot, deadline).await
    }

    async fn commit_tool_record_locked(
        &self,
        snapshot: &ToolPersistenceSnapshot,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        if Instant::now() >= deadline {
            return Err(StoreError::Unavailable);
        }
        validate_stored_tool_record(&snapshot.record, &snapshot.tool_ref)?;

        let input_len = snapshot.input_bytes.as_ref().map_or(0, |b| b.len());
        let result_len = snapshot.result_bytes.as_ref().map_or(0, |b| b.len());
        let stdout_len = snapshot.stdout_bytes.as_ref().map_or(0, |b| b.len());
        let stderr_len = snapshot.stderr_bytes.as_ref().map_or(0, |b| b.len());
        let before_len = snapshot
            .file_change_before
            .as_ref()
            .map_or(0, |bytes| bytes.len());
        let after_len = snapshot
            .file_change_after
            .as_ref()
            .map_or(0, |bytes| bytes.len());

        let streams = [
            (
                snapshot.input_bytes.as_deref(),
                snapshot.record.input.file_bytes,
                snapshot.record.input.file_sha256.as_deref(),
            ),
            (
                snapshot.result_bytes.as_deref(),
                snapshot.record.result.file_bytes,
                snapshot.record.result.file_sha256.as_deref(),
            ),
            (
                snapshot.stdout_bytes.as_deref(),
                snapshot.record.stdout.file_bytes,
                snapshot.record.stdout.file_sha256.as_deref(),
            ),
            (
                snapshot.stderr_bytes.as_deref(),
                snapshot.record.stderr.file_bytes,
                snapshot.record.stderr.file_sha256.as_deref(),
            ),
        ];
        for (bytes, expected_len, expected_sha) in streams {
            let actual_len = bytes.map_or(0, |bytes| bytes.len());
            if actual_len != expected_len {
                return Err(StoreError::Corrupt);
            }
            if let Some(bytes) = bytes {
                let actual_sha = hash_bytes(bytes);
                if expected_sha != Some(actual_sha.as_str()) {
                    return Err(StoreError::Corrupt);
                }
            }
        }
        validate_file_change_snapshot(
            snapshot.record.file_change.as_ref(),
            snapshot.file_change_before.as_deref(),
            snapshot.file_change_after.as_deref(),
        )?;
        let raw_bytes = input_len
            .saturating_add(result_len)
            .saturating_add(stdout_len)
            .saturating_add(stderr_len)
            .saturating_add(before_len)
            .saturating_add(after_len);
        if raw_bytes > 3 * 1024 * 1024 {
            return Err(StoreError::RecordTooLarge);
        }

        let session_dir = self
            .require_session_directory(snapshot.tool_ref.session_id)
            .await?;
        let session_meta = fs::symlink_metadata(&session_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if session_meta.file_type().is_symlink() || !session_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        match path_state(&tools_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Directory | PathState::Missing => {}
            _ => return Err(StoreError::Corrupt),
        }

        let hash = tool_ref_hash(&snapshot.tool_ref);
        let target_dir = tools_dir.join(&hash);
        if target_dir.exists() {
            let target_meta = fs::symlink_metadata(&target_dir)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            if target_meta.file_type().is_symlink() || !target_meta.is_dir() {
                return Err(StoreError::Corrupt);
            }
            match self
                .read_tool_record_details(&snapshot.tool_ref, deadline)
                .await
            {
                Ok(Some(existing)) => {
                    if existing.record.has_corrupt_streams() {
                        return Err(StoreError::Corrupt);
                    }
                    let existing_record_bytes =
                        serde_json::to_vec(&existing.metadata).map_err(|_| StoreError::Corrupt)?;
                    let incoming_record_bytes =
                        serde_json::to_vec(&snapshot.record).map_err(|_| StoreError::Corrupt)?;
                    if existing_record_bytes != incoming_record_bytes {
                        return Err(StoreError::Corrupt);
                    }
                    return Ok(());
                }
                _ => return Err(StoreError::Corrupt),
            }
        }

        let record_bytes = serde_json::to_vec(&snapshot.record).map_err(|_| StoreError::Corrupt)?;
        if record_bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Err(StoreError::RecordTooLarge);
        }

        let item_bytes = (record_bytes.len()
            + input_len
            + result_len
            + stdout_len
            + stderr_len
            + before_len
            + after_len) as u64;
        let reserve = item_bytes.saturating_mul(2);

        let limits = self.aux_limits();
        self.enforce_aux_budget_locked(snapshot.tool_ref.session_id, reserve, limits, deadline)
            .await?;

        if Instant::now() >= deadline {
            return Err(StoreError::Unavailable);
        }
        match fs::create_dir(&tools_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if !matches!(path_state(&tools_dir).await, Ok(PathState::Directory)) {
                    return Err(StoreError::Corrupt);
                }
            }
            Err(_) => return Err(StoreError::Unavailable),
        }
        let temp_dir = unique_temp_path(&target_dir);
        if Instant::now() >= deadline {
            let _ = fs::remove_dir(&tools_dir).await;
            return Err(StoreError::Unavailable);
        }
        if fs::create_dir(&temp_dir).await.is_err() {
            let _ = fs::remove_dir(&tools_dir).await;
            return Err(StoreError::Unavailable);
        }

        let write_res = async {
            #[cfg(test)]
            wait_aux_temp_creation_gate(snapshot.tool_ref.session_id).await;

            let blobs = [
                (TOOL_INPUT_FILE, snapshot.input_bytes.as_deref()),
                (TOOL_RESULT_FILE, snapshot.result_bytes.as_deref()),
                (TOOL_STDOUT_FILE, snapshot.stdout_bytes.as_deref()),
                (TOOL_STDERR_FILE, snapshot.stderr_bytes.as_deref()),
                (TOOL_BEFORE_FILE, snapshot.file_change_before.as_deref()),
                (TOOL_AFTER_FILE, snapshot.file_change_after.as_deref()),
            ];
            for (name, bytes) in blobs {
                if let Some(bytes) = bytes {
                    if Instant::now() >= deadline {
                        return Err(StoreError::Unavailable);
                    }
                    #[cfg(test)]
                    if should_fail_aux_blob_write(snapshot.tool_ref.session_id) {
                        return Err(StoreError::Unavailable);
                    }
                    write_sync_file(&temp_dir.join(name), bytes).await?;
                }
            }
            if Instant::now() >= deadline {
                return Err(StoreError::Unavailable);
            }
            write_sync_file(&temp_dir.join(TOOL_RECORD_FILE), &record_bytes).await?;
            if Instant::now() >= deadline {
                return Err(StoreError::Unavailable);
            }
            sync_directory(&temp_dir)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            Ok::<(), StoreError>(())
        }
        .await;

        if write_res.is_err() {
            let _ = remove_aux_directory(&temp_dir).await;
            return Err(StoreError::Unavailable);
        }

        if Instant::now() >= deadline {
            let _ = remove_aux_directory(&temp_dir).await;
            return Err(StoreError::Unavailable);
        }

        if fs::rename(&temp_dir, &target_dir).await.is_err() {
            let _ = remove_aux_directory(&temp_dir).await;
            return Err(StoreError::Unavailable);
        }

        if sync_directory(&tools_dir).await.is_err() {
            return Err(StoreError::Unavailable);
        }

        Ok(())
    }

    /// Persists completed tool records of a loop under a shared lock and deadline.
    pub(crate) async fn persist_loop_tool_records(
        &self,
        tool_refs: &[ToolRef],
        tool_data: &crate::tool_data::ToolData,
        deadline: Instant,
    ) -> Vec<(ToolRef, Result<(), StoreError>)> {
        if tool_refs.is_empty() {
            return Vec::new();
        }

        #[cfg(test)]
        if let Some(first) = tool_refs.first() {
            if should_fail_aux_write(first.session_id) {
                return tool_refs
                    .iter()
                    .cloned()
                    .map(|r| (r, Err(StoreError::Unavailable)))
                    .collect();
            }
        }

        let _guard = match tokio::time::timeout_at(deadline.into(), self.aux_lock.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                return tool_refs
                    .iter()
                    .cloned()
                    .map(|r| (r, Err(StoreError::Unavailable)))
                    .collect();
            }
        };

        #[cfg(test)]
        if let Some(first) = tool_refs.first() {
            wait_aux_commit_gate(first.session_id).await;
        }

        let mut results = Vec::with_capacity(tool_refs.len().min(DEFAULT_SESSION_AUX_RECORDS));
        let mut expired = false;

        for tool_ref in tool_refs {
            if expired || Instant::now() >= deadline {
                expired = true;
                results.push((tool_ref.clone(), Err(StoreError::Unavailable)));
                continue;
            }

            let snapshot = match tool_data.snapshot_for_persistence(tool_ref) {
                Some(snap) => snap,
                None => {
                    results.push((tool_ref.clone(), Err(StoreError::Corrupt)));
                    continue;
                }
            };

            let res = self.commit_tool_record_locked(&snapshot, deadline).await;
            if matches!(res, Err(StoreError::QueryLimit)) || Instant::now() >= deadline {
                expired = true;
            }
            results.push((tool_ref.clone(), res));
        }

        results
    }

    /// Reads one stored tool record and its retained raw blobs from the auxiliary
    /// directory, returning an in-memory `ToolRecord` projection. Returns `Ok(None)`
    /// when the record or auxiliary directory does not exist.
    #[cfg(test)]
    pub(crate) async fn read_tool_record(
        &self,
        tool_ref: &ToolRef,
    ) -> Result<Option<ToolRecord>, StoreError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        self.read_tool_record_with_deadline(tool_ref, deadline)
            .await
    }

    pub(crate) async fn read_tool_record_with_deadline(
        &self,
        tool_ref: &ToolRef,
        deadline: Instant,
    ) -> Result<Option<ToolRecord>, StoreError> {
        self.read_tool_record_details(tool_ref, deadline)
            .await
            .map(|loaded| loaded.map(|loaded| loaded.record))
    }

    /// Lists only persisted file-change metadata for one Session or loop.
    /// Blob contents are deliberately not touched here; callers verify only
    /// the records that survive their encoded page budget.
    pub(crate) async fn list_tool_changes(
        &self,
        session_id: SessionId,
        loop_id: Option<LoopId>,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<ToolChangeScan, StoreError> {
        check_aux_scan(cancellation, deadline)?;
        let session_dir = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(
                deadline.into(),
                self.require_session_directory(session_id),
            ) => result.map_err(|_| StoreError::QueryLimit)??,
        };
        check_aux_scan(cancellation, deadline)?;
        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        let mut budget = ChangeScanBudget::default();
        let mut complete = true;
        let mut skipped = false;
        let mut records = Vec::new();
        match path_state(&tools_dir).await {
            Ok(PathState::Missing) => {}
            Ok(PathState::Directory) => {
                let mut entries = match fs::read_dir(&tools_dir).await {
                    Ok(entries) => entries,
                    Err(_) => {
                        return Ok(make_tool_change_scan(records, false, true, &budget));
                    }
                };
                let limits = self.aux_limits();
                loop {
                    check_aux_scan(cancellation, deadline)?;
                    // Reserve the entry ceiling before the next directory I/O, so
                    // the hard limit is never exceeded by one speculative read.
                    if !budget.entry_available(limits.max_scan_entries) {
                        complete = false;
                        skipped = true;
                        break;
                    }
                    let entry = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
                        result = tokio::time::timeout_at(
                            deadline.into(),
                            entries.next_entry(),
                        ) => match result {
                            Ok(Ok(Some(entry))) => entry,
                            Ok(Ok(None)) => break,
                            Ok(Err(_)) => {
                                complete = false;
                                skipped = true;
                                break;
                            }
                            Err(_) => return Err(StoreError::QueryLimit),
                        },
                    };
                    budget.consume_entry();
                    check_aux_scan(cancellation, deadline)?;
                    let path = entry.path();
                    let metadata = match fs::symlink_metadata(&path).await {
                        Ok(metadata) => metadata,
                        Err(_) => {
                            complete = false;
                            skipped = true;
                            continue;
                        }
                    };
                    let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                        complete = false;
                        skipped = true;
                        continue;
                    };
                    if is_valid_temp_name(&name) {
                        continue;
                    }
                    if !valid_sha256(&name)
                        || metadata.file_type().is_symlink()
                        || !metadata.is_dir()
                    {
                        complete = false;
                        skipped = true;
                        continue;
                    }
                    match self
                        .read_change_metadata_at(
                            &path,
                            session_id,
                            &name,
                            cancellation,
                            deadline,
                            &mut budget,
                        )
                        .await?
                    {
                        ChangeMetadataRead::BudgetExhausted => {
                            complete = false;
                            skipped = true;
                            break;
                        }
                        ChangeMetadataRead::Record(None) => {
                            complete = false;
                            skipped = true;
                        }
                        ChangeMetadataRead::Record(Some(stored)) => {
                            let Some(change) = stored.file_change else {
                                continue;
                            };
                            if loop_id
                                .as_ref()
                                .is_some_and(|wanted| &stored.tool_ref.loop_id != wanted)
                            {
                                continue;
                            }
                            records.push((stored.tool_ref, change));
                            if records.len() > limits.session_records {
                                records.pop();
                                complete = false;
                                skipped = true;
                                break;
                            }
                        }
                    }
                }
            }
            Ok(_) | Err(_) => {
                complete = false;
                skipped = true;
            }
        }
        Ok(make_tool_change_scan(records, complete, skipped, &budget))
    }

    async fn read_change_metadata_at(
        &self,
        path: &Path,
        session_id: SessionId,
        expected_hash: &str,
        cancellation: &CancellationToken,
        deadline: Instant,
        budget: &mut ChangeScanBudget,
    ) -> Result<ChangeMetadataRead, StoreError> {
        let mut entries = match fs::read_dir(path).await {
            Ok(entries) => entries,
            Err(_) => return Ok(ChangeMetadataRead::Record(None)),
        };
        let mut record_path = None;
        let mut malformed = false;
        let max_entries = self.aux_limits().max_scan_entries;
        loop {
            check_aux_scan(cancellation, deadline)?;
            // Same entry ceiling reservation as the outer scan, before I/O.
            if !budget.entry_available(max_entries) {
                return Ok(ChangeMetadataRead::BudgetExhausted);
            }
            let entry = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
                result = tokio::time::timeout_at(
                    deadline.into(),
                    entries.next_entry(),
                ) => match result {
                    Ok(Ok(Some(entry))) => entry,
                    Ok(Ok(None)) => break,
                    Ok(Err(_)) => return Ok(ChangeMetadataRead::Record(None)),
                    Err(_) => return Err(StoreError::QueryLimit),
                },
            };
            budget.consume_entry();
            let metadata = match fs::symlink_metadata(entry.path()).await {
                Ok(metadata) => metadata,
                Err(_) => {
                    malformed = true;
                    continue;
                }
            };
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                malformed = true;
                continue;
            };
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || !ALLOWED_AUX_FILES.contains(&name.as_str())
            {
                malformed = true;
                continue;
            }
            if name == TOOL_RECORD_FILE {
                record_path = Some(entry.path());
            }
        }
        if malformed {
            return Ok(ChangeMetadataRead::Record(None));
        }
        let Some(record_path) = record_path else {
            return Ok(ChangeMetadataRead::Record(None));
        };
        let file = match safe_open_read(&record_path).await {
            Ok(file) => file,
            Err(_) => return Ok(ChangeMetadataRead::Record(None)),
        };
        // Read at most the remaining allowance, capped by the oversized-file
        // probe. If the read fills a cap smaller than that probe the file did
        // not reach EOF, so a truncated read is never accepted as a complete
        // record and the whole read stays inside the cumulative budget.
        let probe = MAX_TOOL_METADATA_BYTES.saturating_add(1);
        let remaining = CHANGE_SCAN_METADATA_BYTES.saturating_sub(budget.metadata_bytes);
        if remaining == 0 {
            return Ok(ChangeMetadataRead::BudgetExhausted);
        }
        let cap = remaining.min(probe);
        let mut bytes = Vec::new();
        let mut reader = file.take(cap as u64);
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(
                deadline.into(),
                reader.read_to_end(&mut bytes),
            ) => result.map_err(|_| StoreError::QueryLimit)?,
        };
        if read.is_err() {
            return Ok(ChangeMetadataRead::Record(None));
        }
        check_aux_scan(cancellation, deadline)?;
        // Charge every byte actually read, including an oversized file, so the
        // cumulative budget bounds total metadata I/O rather than only the
        // records that parse successfully.
        if !budget.consume_metadata(bytes.len(), CHANGE_SCAN_METADATA_BYTES) {
            return Ok(ChangeMetadataRead::BudgetExhausted);
        }
        if bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Ok(ChangeMetadataRead::Record(None));
        }
        if bytes.len() == cap && cap < probe {
            return Ok(ChangeMetadataRead::BudgetExhausted);
        }
        let stored: StoredToolRecord = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Ok(ChangeMetadataRead::Record(None)),
        };
        if stored.tool_ref.session_id != session_id
            || tool_ref_hash(&stored.tool_ref) != expected_hash
            || validate_stored_tool_record(&stored, &stored.tool_ref).is_err()
        {
            return Ok(ChangeMetadataRead::Record(None));
        }
        Ok(ChangeMetadataRead::Record(Some(Box::new(stored))))
    }

    pub(crate) async fn change_blobs_available(
        &self,
        session_id: SessionId,
        tool_ref: &ToolRef,
        change: &StoredFileChange,
        cancellation: &CancellationToken,
        deadline: Instant,
        budget: &mut ChangeBlobBudget,
    ) -> Result<bool, StoreError> {
        check_aux_scan(cancellation, deadline)?;
        if !change.before_captured
            || !change.after_captured
            || (!matches!(
                &change.before,
                ChangeRevision::Missing | ChangeRevision::Content { .. }
            ))
            || !matches!(&change.after, ChangeRevision::Content { .. })
        {
            return Ok(false);
        }
        // Charge the worst-case read cost (expected bytes plus the one-byte
        // lookahead), not just the expected length, so the cumulative budget
        // bounds what is actually read from disk. A `Missing` before-image is
        // not opened and costs 0; a real empty `Content` still costs 1.
        let required = change_blob_read_cost(&change.before)
            .saturating_add(change_blob_read_cost(&change.after));
        if budget.used_bytes.saturating_add(required) > CHANGE_SCAN_BLOB_BYTES {
            budget.exhausted = true;
            return Ok(false);
        }
        budget.used_bytes = budget.used_bytes.saturating_add(required);
        let target_dir = self
            .session_directory(session_id)
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(tool_ref));
        let before = Self::change_blob_present(
            &target_dir.join(TOOL_BEFORE_FILE),
            &change.before,
            cancellation,
            deadline,
        )
        .await?;
        let after = Self::change_blob_present(
            &target_dir.join(TOOL_AFTER_FILE),
            &change.after,
            cancellation,
            deadline,
        )
        .await?;
        Ok(before && after)
    }

    async fn change_blob_present(
        path: &Path,
        revision: &ChangeRevision,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        if !matches!(revision, ChangeRevision::Content { .. }) {
            return Ok(true);
        }
        let (value, corrupt) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(deadline.into(), read_change_blob(path, revision)) => {
                result.map_err(|_| StoreError::QueryLimit)?
            }
        };
        check_aux_scan(cancellation, deadline)?;
        Ok(value.is_some() && !corrupt)
    }

    async fn read_tool_record_details(
        &self,
        tool_ref: &ToolRef,
        deadline: Instant,
    ) -> Result<Option<ReadToolRecord>, StoreError> {
        match tokio::time::timeout_at(deadline.into(), self.read_tool_record_inner(tool_ref)).await
        {
            Ok(res) => res,
            Err(_) => Err(StoreError::Unavailable),
        }
    }

    /// Reads only one retained record's bounded before/after snapshots. It
    /// deliberately skips the larger input/result/stream blobs, so a cold
    /// `changes.diff` never clones an unrelated tool output.
    pub(crate) async fn read_file_change_snapshots(
        &self,
        session_id: SessionId,
        tool_ref: &ToolRef,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::changes::FileChange>, StoreError> {
        if tool_ref.session_id != session_id {
            return Err(StoreError::InvalidArguments);
        }
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        let session_dir = self.session_directory(session_id);
        let session_meta = match fs::symlink_metadata(&session_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        if session_meta.file_type().is_symlink() || !session_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }
        let target_dir = session_dir
            .join(AUX_TOOLS_DIR)
            .join(tool_ref_hash(tool_ref));
        let record_path = target_dir.join(TOOL_RECORD_FILE);
        let file = match safe_open_read(&record_path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        let mut bytes = Vec::new();
        let mut reader = file.take((MAX_TOOL_METADATA_BYTES + 1) as u64);
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(deadline.into(), reader.read_to_end(&mut bytes)) => {
                result.map_err(|_| StoreError::QueryLimit)?
            }
        };
        if read.is_err() || bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Ok(None);
        }
        let stored: StoredToolRecord = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Ok(None),
        };
        if stored.tool_ref != *tool_ref || validate_stored_tool_record(&stored, tool_ref).is_err() {
            return Ok(None);
        }
        let Some(change) = stored.file_change else {
            return Ok(None);
        };
        let (before, before_corrupt) =
            read_change_blob(&target_dir.join(TOOL_BEFORE_FILE), &change.before).await;
        check_aux_scan(cancellation, deadline)?;
        let (after, after_corrupt) =
            read_change_blob(&target_dir.join(TOOL_AFTER_FILE), &change.after).await;
        check_aux_scan(cancellation, deadline)?;
        Ok(Some(crate::changes::FileChange::from_stored(
            change,
            before,
            before_corrupt,
            after,
            after_corrupt,
        )))
    }

    async fn read_tool_record_inner(
        &self,
        tool_ref: &ToolRef,
    ) -> Result<Option<ReadToolRecord>, StoreError> {
        let root_meta = fs::symlink_metadata(&self.root)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if root_meta.file_type().is_symlink() || !root_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let sessions_dir = self.sessions_directory();
        let sessions_meta = fs::symlink_metadata(&sessions_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if sessions_meta.file_type().is_symlink() || !sessions_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let session_dir = self.session_directory(tool_ref.session_id);
        let session_meta = match fs::symlink_metadata(&session_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(StoreError::SessionNotFound);
            }
            Err(_) => return Err(StoreError::Unavailable),
        };
        if session_meta.file_type().is_symlink() || !session_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        let tools_meta = match fs::symlink_metadata(&tools_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        if tools_meta.file_type().is_symlink() || !tools_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let hash = tool_ref_hash(tool_ref);
        let target_dir = tools_dir.join(&hash);
        let target_meta = match fs::symlink_metadata(&target_dir).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        if target_meta.file_type().is_symlink() || !target_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let record_path = target_dir.join(TOOL_RECORD_FILE);
        let file = match safe_open_read(&record_path).await {
            Ok(f) => f,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };

        let mut bytes = Vec::new();
        if file
            .take((MAX_TOOL_METADATA_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .is_err()
        {
            return Err(StoreError::Unavailable);
        }
        if bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Err(StoreError::Corrupt);
        }

        let stored: StoredToolRecord = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Err(StoreError::Corrupt),
        };

        validate_stored_tool_record(&stored, tool_ref)?;

        let (input_bytes, input_corrupt) = read_blob(
            &target_dir.join(TOOL_INPUT_FILE),
            stored.input.file_bytes,
            stored.input.file_sha256.as_deref(),
            MAX_TOOL_INPUT_PERSIST_BYTES,
        )
        .await;

        let (result_bytes, result_corrupt) = read_blob(
            &target_dir.join(TOOL_RESULT_FILE),
            stored.result.file_bytes,
            stored.result.file_sha256.as_deref(),
            MAX_TOOL_RESULT_PERSIST_BYTES,
        )
        .await;

        let (stdout_bytes, stdout_corrupt) = read_blob(
            &target_dir.join(TOOL_STDOUT_FILE),
            stored.stdout.file_bytes,
            stored.stdout.file_sha256.as_deref(),
            crate::tool_data::MAX_TOOL_STREAM_BYTES,
        )
        .await;

        let (stderr_bytes, stderr_corrupt) = read_blob(
            &target_dir.join(TOOL_STDERR_FILE),
            stored.stderr.file_bytes,
            stored.stderr.file_sha256.as_deref(),
            crate::tool_data::MAX_TOOL_STREAM_BYTES,
        )
        .await;

        let (file_change_before, file_change_before_corrupt) = match &stored.file_change {
            Some(change) => {
                read_change_blob(&target_dir.join(TOOL_BEFORE_FILE), &change.before).await
            }
            None => (None, false),
        };
        let (file_change_after, file_change_after_corrupt) = match &stored.file_change {
            Some(change) => {
                read_change_blob(&target_dir.join(TOOL_AFTER_FILE), &change.after).await
            }
            None => (None, false),
        };

        let record = ToolRecord::from_stored(
            stored.clone(),
            (input_bytes, input_corrupt),
            (result_bytes, result_corrupt),
            (stdout_bytes, stdout_corrupt),
            (stderr_bytes, stderr_corrupt),
            (file_change_before, file_change_before_corrupt),
            (file_change_after, file_change_after_corrupt),
        );

        Ok(Some(ReadToolRecord {
            metadata: stored,
            record,
        }))
    }

    async fn enforce_aux_budget_locked(
        &self,
        target_session: SessionId,
        reserve: u64,
        limits: AuxLimits,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        let sessions_dir = self.sessions_directory();
        let sessions_meta = fs::symlink_metadata(&sessions_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if sessions_meta.file_type().is_symlink() || !sessions_meta.is_dir() {
            return Err(StoreError::Corrupt);
        }

        let mut session_entries = fs::read_dir(&sessions_dir)
            .await
            .map_err(|_| StoreError::Unavailable)?;

        let mut total_scanned = 0usize;
        let mut global_bytes = 0u64;
        let mut global_records = 0usize;
        let mut session_bytes = 0u64;
        let mut session_records = 0usize;
        let mut all_entries = Vec::new();
        let mut target_entries = Vec::new();

        while let Some(ses_entry) = {
            if Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            session_entries
                .next_entry()
                .await
                .map_err(|_| StoreError::Unavailable)?
        } {
            total_scanned = total_scanned.saturating_add(1);
            if total_scanned > limits.max_scan_entries || Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            let ses_path = ses_entry.path();
            let ses_meta = fs::symlink_metadata(&ses_path)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            if ses_meta.file_type().is_symlink() {
                return Err(StoreError::Corrupt);
            }
            if !ses_meta.is_dir() {
                global_bytes = global_bytes.saturating_add(ses_meta.len());
                continue;
            }
            let file_name = ses_entry.file_name();
            let Some(name_str) = file_name.to_str() else {
                return Err(StoreError::Corrupt);
            };
            let Ok(current_ses_id) = name_str.parse::<SessionId>() else {
                return Err(StoreError::Corrupt);
            };
            let tools_dir = ses_path.join(AUX_TOOLS_DIR);
            let tools_meta = match fs::symlink_metadata(&tools_dir).await {
                Ok(m) => m,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => return Err(StoreError::Unavailable),
            };
            if tools_meta.file_type().is_symlink() || !tools_meta.is_dir() {
                return Err(StoreError::Corrupt);
            }

            let mut tool_dir_entries = fs::read_dir(&tools_dir)
                .await
                .map_err(|_| StoreError::Unavailable)?;

            while let Some(tool_entry) = {
                if Instant::now() >= deadline {
                    return Err(StoreError::QueryLimit);
                }
                tool_dir_entries
                    .next_entry()
                    .await
                    .map_err(|_| StoreError::Unavailable)?
            } {
                total_scanned = total_scanned.saturating_add(1);
                if total_scanned > limits.max_scan_entries || Instant::now() >= deadline {
                    return Err(StoreError::QueryLimit);
                }
                let tool_path = tool_entry.path();
                let tool_meta = fs::symlink_metadata(&tool_path)
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
                if tool_meta.file_type().is_symlink() {
                    return Err(StoreError::Corrupt);
                }
                let tool_name = tool_entry.file_name();
                let Some(name_str) = tool_name.to_str() else {
                    return Err(StoreError::Corrupt);
                };

                if is_valid_temp_name(name_str) {
                    if !tool_meta.is_dir() {
                        return Err(StoreError::Corrupt);
                    }
                    match verify_and_scan_temp_dir(
                        &tool_path,
                        &mut total_scanned,
                        limits.max_scan_entries,
                        deadline,
                    )
                    .await?
                    {
                        Some(temp_bytes) => {
                            global_bytes = global_bytes.saturating_add(temp_bytes);
                            if current_ses_id == target_session {
                                session_bytes = session_bytes.saturating_add(temp_bytes);
                            }

                            #[cfg(test)]
                            let fail_remove = should_fail_remove_temp(current_ses_id);
                            #[cfg(not(test))]
                            let fail_remove = false;

                            if fail_remove || remove_aux_directory(&tool_path).await.is_err() {
                                return Err(StoreError::Unavailable);
                            }

                            global_bytes = global_bytes.saturating_sub(temp_bytes);
                            if current_ses_id == target_session {
                                session_bytes = session_bytes.saturating_sub(temp_bytes);
                            }
                            continue;
                        }
                        None => {
                            return Err(StoreError::Corrupt);
                        }
                    }
                }

                if !valid_sha256(name_str) || !tool_meta.is_dir() {
                    return Err(StoreError::Corrupt);
                }

                let (dir_bytes, _mtime, verified_entry) = scan_and_verify_tool_dir(
                    &tool_path,
                    current_ses_id,
                    name_str,
                    &mut total_scanned,
                    limits.max_scan_entries,
                    deadline,
                )
                .await?;

                global_bytes = global_bytes.saturating_add(dir_bytes);
                global_records = global_records.saturating_add(1);
                if current_ses_id == target_session {
                    session_bytes = session_bytes.saturating_add(dir_bytes);
                    session_records = session_records.saturating_add(1);
                    target_entries.push(verified_entry.clone());
                }
                all_entries.push(verified_entry);
            }
        }

        // Evict session-level oldest records if exceeding session bounds
        target_entries.sort_by_key(|e| e.mtime);
        while (session_bytes.saturating_add(reserve) > limits.session_bytes
            || session_records.saturating_add(1) > limits.session_records)
            && !target_entries.is_empty()
        {
            if Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            let victim = target_entries.remove(0);
            remove_aux_directory(&victim.path).await?;
            session_bytes = session_bytes.saturating_sub(victim.bytes);
            session_records = session_records.saturating_sub(1);
            global_bytes = global_bytes.saturating_sub(victim.bytes);
            global_records = global_records.saturating_sub(1);
            if let Some(pos) = all_entries.iter().position(|e| e.path == victim.path) {
                all_entries.remove(pos);
            }
        }
        if session_bytes.saturating_add(reserve) > limits.session_bytes
            || session_records.saturating_add(1) > limits.session_records
        {
            return Err(StoreError::Unavailable);
        }

        // Evict global oldest records if exceeding global bounds
        all_entries.sort_by_key(|e| e.mtime);
        while (global_bytes.saturating_add(reserve) > limits.global_bytes
            || global_records.saturating_add(1) > limits.global_records)
            && !all_entries.is_empty()
        {
            if Instant::now() >= deadline {
                return Err(StoreError::QueryLimit);
            }
            let victim = all_entries.remove(0);
            remove_aux_directory(&victim.path).await?;
            global_bytes = global_bytes.saturating_sub(victim.bytes);
            global_records = global_records.saturating_sub(1);
            if victim.session_id == target_session {
                session_bytes = session_bytes.saturating_sub(victim.bytes);
                session_records = session_records.saturating_sub(1);
            }
        }
        if global_bytes.saturating_add(reserve) > limits.global_bytes
            || global_records.saturating_add(1) > limits.global_records
        {
            return Err(StoreError::Unavailable);
        }

        Ok(())
    }
}

fn check_aux_scan(cancellation: &CancellationToken, deadline: Instant) -> Result<(), StoreError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(StoreError::QueryLimit)
    } else {
        Ok(())
    }
}

impl ChangeScanBudget {
    /// True when one more directory entry fits under the scan ceiling. Callers
    /// check this before the next `next_entry()` I/O, then record the consumed
    /// entry only after they really received one.
    pub(super) fn entry_available(&self, max_entries: usize) -> bool {
        self.entries < max_entries
    }

    fn consume_entry(&mut self) {
        self.entries = self.entries.saturating_add(1);
    }

    fn consume_metadata(&mut self, bytes: usize, max_bytes: usize) -> bool {
        self.metadata_bytes = self.metadata_bytes.saturating_add(bytes);
        self.metadata_bytes <= max_bytes
    }
}

/// The worst-case bytes one blob verification reads, including the one-byte
/// lookahead that detects an oversized or truncated file. A `Content` blob is
/// always read with `.take(bytes + 1)`, so even a genuinely empty one costs 1;
/// a `Missing` before-image is not opened at all and costs 0.
pub(super) fn change_blob_read_cost(revision: &ChangeRevision) -> usize {
    match revision {
        ChangeRevision::Content { bytes, .. } => bytes.saturating_add(1),
        ChangeRevision::Missing | ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => 0,
    }
}

fn make_tool_change_scan(
    mut records: Vec<(ToolRef, StoredFileChange)>,
    complete: bool,
    skipped: bool,
    budget: &ChangeScanBudget,
) -> ToolChangeScan {
    records.sort_by_cached_key(|entry| tool_ref_hash(&entry.0));
    let fingerprint = serde_json::to_vec(&(
        &records,
        complete,
        skipped,
        budget.entries,
        budget.metadata_bytes,
    ))
    .map(|bytes| hash_bytes(&bytes))
    .unwrap_or_else(|_| hash_bytes(b"change-scan-serialization-failed"));
    ToolChangeScan {
        records,
        complete,
        skipped,
        #[cfg(test)]
        entries: budget.entries,
        #[cfg(test)]
        metadata_bytes: budget.metadata_bytes,
        observation: fingerprint,
    }
}

#[derive(Clone)]
struct AuxDirectoryEntry {
    session_id: SessionId,
    path: PathBuf,
    mtime: std::time::SystemTime,
    bytes: u64,
}

async fn remove_aux_directory(path: &Path) -> Result<(), StoreError> {
    let parent = path.parent().ok_or(StoreError::Corrupt)?;
    if parent.file_name() != Some(std::ffi::OsStr::new(AUX_TOOLS_DIR)) {
        return Err(StoreError::Corrupt);
    }
    fs::remove_dir_all(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    match fs::remove_dir(parent).await {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        Err(_) => Err(StoreError::Unavailable),
    }
}

async fn write_sync_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if file.write_all(bytes).await.is_err()
        || file.flush().await.is_err()
        || file.sync_all().await.is_err()
    {
        return Err(StoreError::Unavailable);
    }
    Ok(())
}

fn is_valid_temp_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() < 73 || bytes[0] != b'.' {
        return false;
    }
    let hex_part = &bytes[1..65];
    if !hex_part
        .iter()
        .all(|&b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return false;
    }
    if &bytes[65..70] != b".tmp-" {
        return false;
    }
    let remainder = &bytes[70..];
    let Some(dash_pos) = remainder.iter().position(|&b| b == b'-') else {
        return false;
    };
    if dash_pos == 0 || dash_pos == remainder.len() - 1 {
        return false;
    }
    let pid_part = &remainder[..dash_pos];
    let id_part = &remainder[dash_pos + 1..];
    pid_part.iter().all(u8::is_ascii_digit) && id_part.iter().all(u8::is_ascii_digit)
}

fn validate_stored_tool_record(
    stored: &StoredToolRecord,
    tool_ref: &ToolRef,
) -> Result<(), StoreError> {
    if stored.version != TOOL_RECORD_FORMAT_VERSION {
        return Err(StoreError::UnsupportedFormat);
    }
    if stored.tool_ref != *tool_ref {
        return Err(StoreError::Corrupt);
    }
    if !KNOWN_TOOL_NAMES.contains(&stored.name.as_str()) && stored.name != LEGACY_STORED_TOOL_NAME {
        return Err(StoreError::Corrupt);
    }
    if !stored.state.is_terminal() {
        return Err(StoreError::Corrupt);
    }
    if let Some(outcome) = stored.outcome {
        if !stored.state.matches_outcome(outcome) {
            return Err(StoreError::Corrupt);
        }
    }

    if !stored.input.seen {
        if stored.input.total_bytes != 0
            || stored.input.file_bytes != 0
            || stored.input.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        if stored.input.file_bytes > stored.input.total_bytes {
            return Err(StoreError::Corrupt);
        }
        if stored.input.expired
            && (stored.input.file_bytes != 0 || stored.input.file_sha256.is_some())
        {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.input.file_bytes > MAX_TOOL_INPUT_PERSIST_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    if !stored.result.seen {
        if stored.result.total_bytes != 0
            || stored.result.file_bytes != 0
            || stored.result.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        if stored.result.file_bytes > stored.result.total_bytes {
            return Err(StoreError::Corrupt);
        }
        if stored.result.expired
            && (stored.result.file_bytes != 0 || stored.result.file_sha256.is_some())
        {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.result.file_bytes > MAX_TOOL_RESULT_PERSIST_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    if stored.stdout.start_offset > stored.stdout.observed_end {
        return Err(StoreError::Corrupt);
    }
    if !stored.stdout.seen {
        if stored.stdout.start_offset != 0
            || stored.stdout.observed_end != 0
            || stored.stdout.file_bytes != 0
            || stored.stdout.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else if stored.stdout.expired {
        if stored.stdout.start_offset != stored.stdout.observed_end
            || stored.stdout.file_bytes != 0
            || stored.stdout.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        let diff = stored
            .stdout
            .observed_end
            .checked_sub(stored.stdout.start_offset)
            .ok_or(StoreError::Corrupt)?;
        if diff != stored.stdout.file_bytes as u64 {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.stdout.file_bytes > crate::tool_data::MAX_TOOL_STREAM_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    if stored.stderr.start_offset > stored.stderr.observed_end {
        return Err(StoreError::Corrupt);
    }
    if !stored.stderr.seen {
        if stored.stderr.start_offset != 0
            || stored.stderr.observed_end != 0
            || stored.stderr.file_bytes != 0
            || stored.stderr.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else if stored.stderr.expired {
        if stored.stderr.start_offset != stored.stderr.observed_end
            || stored.stderr.file_bytes != 0
            || stored.stderr.file_sha256.is_some()
        {
            return Err(StoreError::Corrupt);
        }
    } else {
        let diff = stored
            .stderr
            .observed_end
            .checked_sub(stored.stderr.start_offset)
            .ok_or(StoreError::Corrupt)?;
        if diff != stored.stderr.file_bytes as u64 {
            return Err(StoreError::Corrupt);
        }
    }
    if stored.stderr.file_bytes > crate::tool_data::MAX_TOOL_STREAM_BYTES {
        return Err(StoreError::RecordTooLarge);
    }

    let change_bytes = stored.file_change.as_ref().map_or(0, |change| {
        let before = match &change.before {
            crate::changes::ChangeRevision::Content { bytes, .. } => *bytes,
            _ => 0,
        };
        let after = match &change.after {
            crate::changes::ChangeRevision::Content { bytes, .. } => *bytes,
            _ => 0,
        };
        before.saturating_add(after)
    });

    let total_file_bytes = stored
        .input
        .file_bytes
        .saturating_add(stored.result.file_bytes)
        .saturating_add(stored.stdout.file_bytes)
        .saturating_add(stored.stderr.file_bytes)
        .saturating_add(change_bytes);
    if total_file_bytes > 3 * 1024 * 1024 {
        return Err(StoreError::RecordTooLarge);
    }

    let check_sha256 = |bytes: usize, hash: Option<&str>| -> bool {
        if bytes == 0 {
            hash.is_none()
        } else {
            hash.is_some_and(valid_sha256)
        }
    };
    if !check_sha256(stored.input.file_bytes, stored.input.file_sha256.as_deref())
        || !check_sha256(
            stored.result.file_bytes,
            stored.result.file_sha256.as_deref(),
        )
        || !check_sha256(
            stored.stdout.file_bytes,
            stored.stdout.file_sha256.as_deref(),
        )
        || !check_sha256(
            stored.stderr.file_bytes,
            stored.stderr.file_sha256.as_deref(),
        )
    {
        return Err(StoreError::Corrupt);
    }

    if let Some(cmd) = &stored.command {
        if !cmd.status.is_terminal() {
            return Err(StoreError::Corrupt);
        }
        if cmd.stdout_base_offset != stored.stdout.start_offset
            || cmd.stdout_observed_end != stored.stdout.observed_end
            || cmd.stderr_base_offset != stored.stderr.start_offset
            || cmd.stderr_observed_end != stored.stderr.observed_end
        {
            return Err(StoreError::Corrupt);
        }
    }

    if let Some(change) = &stored.file_change {
        validate_stored_file_change(change)?;
    }

    Ok(())
}

fn validate_stored_file_change(change: &StoredFileChange) -> Result<(), StoreError> {
    crate::workspace::validate_relative_path(&change.path).map_err(|_| StoreError::Corrupt)?;
    match &change.before {
        ChangeRevision::Missing => {
            if !change.before_captured {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Content { sha256, bytes } => {
            if !change.before_captured
                || *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
                || !valid_sha256(sha256)
            {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => {
            if change.before_captured {
                return Err(StoreError::Corrupt);
            }
        }
    }
    match &change.after {
        ChangeRevision::Content { sha256, bytes } => {
            if !change.after_captured
                || *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
                || !valid_sha256(sha256)
            {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Missing | ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => {
            if change.after_captured {
                return Err(StoreError::Corrupt);
            }
        }
    }
    match change.commit_state {
        crate::changes::ChangeCommitState::Applied
        | crate::changes::ChangeCommitState::Conflict => {
            if !matches!(&change.after, ChangeRevision::Content { .. }) {
                return Err(StoreError::Corrupt);
            }
        }
        crate::changes::ChangeCommitState::NotCommitted
        | crate::changes::ChangeCommitState::Unknown => {}
    }
    Ok(())
}

fn validate_file_change_snapshot(
    stored: Option<&StoredFileChange>,
    before_bytes: Option<&[u8]>,
    after_bytes: Option<&[u8]>,
) -> Result<(), StoreError> {
    let Some(stored) = stored else {
        return if before_bytes.is_none() && after_bytes.is_none() {
            Ok(())
        } else {
            Err(StoreError::Corrupt)
        };
    };
    validate_stored_file_change(stored)?;
    if let Some(bytes) = before_bytes {
        let ChangeRevision::Content {
            sha256,
            bytes: size,
        } = &stored.before
        else {
            return Err(StoreError::Corrupt);
        };
        if !stored.before_captured
            || bytes.len() != *size
            || bytes.len() > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
            || hash_bytes(bytes) != *sha256
        {
            return Err(StoreError::Corrupt);
        }
    }
    if let Some(bytes) = after_bytes {
        let ChangeRevision::Content {
            sha256,
            bytes: size,
        } = &stored.after
        else {
            return Err(StoreError::Corrupt);
        };
        if !stored.after_captured
            || bytes.len() != *size
            || bytes.len() > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
            || hash_bytes(bytes) != *sha256
        {
            return Err(StoreError::Corrupt);
        }
    }
    Ok(())
}

async fn read_blob(
    path: &Path,
    expected_bytes: usize,
    expected_sha256: Option<&str>,
    max_cap: usize,
) -> (Option<Vec<u8>>, bool) {
    if expected_bytes == 0 {
        return (None, false);
    }
    if expected_bytes > max_cap {
        return (None, true);
    }
    let Some(expected_hash) = expected_sha256 else {
        return (None, true);
    };
    let Ok(file) = safe_open_read(path).await else {
        return (None, true);
    };
    let mut bytes = Vec::with_capacity(expected_bytes);
    if file
        .take((expected_bytes.saturating_add(1)) as u64)
        .read_to_end(&mut bytes)
        .await
        .is_err()
    {
        return (None, true);
    }
    if bytes.len() != expected_bytes {
        return (None, true);
    }
    let actual_hash = hash_bytes(&bytes);
    if actual_hash != expected_hash {
        return (None, true);
    }
    (Some(bytes), false)
}

async fn read_change_blob(path: &Path, revision: &ChangeRevision) -> (Option<Vec<u8>>, bool) {
    let ChangeRevision::Content { sha256, bytes } = revision else {
        return (None, false);
    };
    if *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES || !valid_sha256(sha256) {
        return (None, true);
    }
    #[cfg(test)]
    READ_CHANGE_BLOBS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(path.to_path_buf());
    let Ok(file) = safe_open_read(path).await else {
        return (None, true);
    };
    let mut value = Vec::with_capacity(*bytes);
    if file
        .take((*bytes).saturating_add(1) as u64)
        .read_to_end(&mut value)
        .await
        .is_err()
    {
        return (None, true);
    }
    if value.len() != *bytes || hash_bytes(&value) != *sha256 {
        return (None, true);
    }
    (Some(value), false)
}

async fn verify_and_scan_temp_dir(
    path: &Path,
    total_scanned: &mut usize,
    max_scan_entries: usize,
    deadline: Instant,
) -> Result<Option<u64>, StoreError> {
    let mut read_dir = fs::read_dir(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    let mut temp_bytes = 0u64;
    while let Some(entry) = {
        if Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        read_dir
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
    } {
        *total_scanned = total_scanned.saturating_add(1);
        if *total_scanned > max_scan_entries || Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        let meta = fs::symlink_metadata(entry.path())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if meta.file_type().is_symlink() || !meta.is_file() {
            return Ok(None);
        }
        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            return Ok(None);
        };
        if !ALLOWED_AUX_FILES.contains(&name_str) {
            return Ok(None);
        }
        temp_bytes = temp_bytes.saturating_add(meta.len());
    }
    Ok(Some(temp_bytes))
}

async fn scan_and_verify_tool_dir(
    path: &Path,
    session_id: SessionId,
    expected_hash: &str,
    total_scanned: &mut usize,
    max_scan_entries: usize,
    deadline: Instant,
) -> Result<(u64, std::time::SystemTime, AuxDirectoryEntry), StoreError> {
    let dir_meta = fs::symlink_metadata(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if dir_meta.file_type().is_symlink() || !dir_meta.is_dir() {
        return Err(StoreError::Corrupt);
    }
    let mtime = dir_meta
        .modified()
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

    let mut read_dir = fs::read_dir(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    let mut dir_bytes = 0u64;
    let mut has_record = false;

    while let Some(entry) = {
        if Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        read_dir
            .next_entry()
            .await
            .map_err(|_| StoreError::Unavailable)?
    } {
        *total_scanned = total_scanned.saturating_add(1);
        if *total_scanned > max_scan_entries || Instant::now() >= deadline {
            return Err(StoreError::QueryLimit);
        }
        let file_meta = fs::symlink_metadata(entry.path())
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if file_meta.file_type().is_symlink() || !file_meta.is_file() {
            return Err(StoreError::Corrupt);
        }
        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            return Err(StoreError::Corrupt);
        };
        if !ALLOWED_AUX_FILES.contains(&name_str) {
            return Err(StoreError::Corrupt);
        }
        if name_str == TOOL_RECORD_FILE {
            has_record = true;
        }
        dir_bytes = dir_bytes.saturating_add(file_meta.len());
    }

    if !has_record {
        return Err(StoreError::Corrupt);
    }

    let record_path = path.join(TOOL_RECORD_FILE);
    let file = safe_open_read(&record_path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    let mut bytes = Vec::new();
    if file
        .take((MAX_TOOL_METADATA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .is_err()
    {
        return Err(StoreError::Unavailable);
    }
    if bytes.len() > MAX_TOOL_METADATA_BYTES {
        return Err(StoreError::Corrupt);
    }
    let stored: StoredToolRecord =
        serde_json::from_slice(&bytes).map_err(|_| StoreError::Corrupt)?;
    if stored.version != TOOL_RECORD_FORMAT_VERSION
        || stored.tool_ref.session_id != session_id
        || tool_ref_hash(&stored.tool_ref) != expected_hash
    {
        return Err(StoreError::Corrupt);
    }
    validate_stored_tool_record(&stored, &stored.tool_ref)?;

    Ok((
        dir_bytes,
        mtime,
        AuxDirectoryEntry {
            session_id,
            path: path.to_path_buf(),
            mtime,
            bytes: dir_bytes,
        },
    ))
}
