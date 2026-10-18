//! Auxiliary tool record, output blob, quota, and cold-read implementation.
use super::*;

#[cfg(test)]
pub(crate) static AUX_BLOB_WRITE_FAILURES: crate::test_support::Registry<SessionId, usize> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static AUX_TEMP_CREATION_GATES: crate::test_support::Registry<
    SessionId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

impl Store {
    fn aux_limits(&self) -> AuxLimits {
        #[cfg(test)]
        if let Some(limits) = self.aux_limits {
            return limits;
        }
        DEFAULT_AUX_LIMITS
    }

    pub(super) async fn commit_tool_record_locked(
        &self,
        snapshot: &ToolPersistenceSnapshot,
        deadline: Instant,
        ledger: &mut Option<AuxLedger>,
    ) -> Result<(), StoreError> {
        check_aux_deadline(deadline)?;
        validate_stored_tool_record(&snapshot.record, &snapshot.tool_ref)?;

        let blobs = snapshot_blobs(snapshot);
        for ((_, bytes), (_, expected_len, expected_sha)) in
            blobs.into_iter().zip(stored_blobs(&snapshot.record))
        {
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
        let raw_bytes: usize = blobs
            .iter()
            .filter_map(|(_, bytes)| *bytes)
            .map(<[u8]>::len)
            .sum();
        if raw_bytes > 3 * 1024 * 1024 {
            return Err(StoreError::RecordTooLarge);
        }

        let session_dir = self
            .require_session_directory(snapshot.tool_ref.session_id)
            .await?;
        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        match path_state(&tools_dir).await? {
            PathState::Directory | PathState::Missing => {}
            _ => return Err(StoreError::Corrupt),
        }

        let hash = tool_ref_hash(&snapshot.tool_ref);
        let target_dir = tools_dir.join(&hash);
        if real_dir_opt(&target_dir).await? {
            match self
                .read_tool_record_details(&snapshot.tool_ref, deadline)
                .await
            {
                Ok(Some(existing)) => {
                    if existing.record.has_corrupt_streams() {
                        return Err(StoreError::Corrupt);
                    }
                    let existing_record_bytes =
                        serde_json::to_vec(&existing.metadata).map_err(corrupt)?;
                    let incoming_record_bytes =
                        serde_json::to_vec(&snapshot.record).map_err(corrupt)?;
                    if existing_record_bytes != incoming_record_bytes {
                        return Err(StoreError::Corrupt);
                    }
                    return Ok(());
                }
                _ => return Err(StoreError::Corrupt),
            }
        }

        let record_bytes = serde_json::to_vec(&snapshot.record).map_err(corrupt)?;
        if record_bytes.len() > MAX_TOOL_METADATA_BYTES {
            return Err(StoreError::RecordTooLarge);
        }

        let item_bytes = (record_bytes.len() + raw_bytes) as u64;
        let reserve = item_bytes.saturating_mul(2);

        let limits = self.aux_limits();
        self.enforce_aux_budget_locked(
            snapshot.tool_ref.session_id,
            reserve,
            limits,
            deadline,
            ledger,
        )
        .await?;

        check_aux_deadline(deadline)?;
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
            if let Some(gate) = AUX_TEMP_CREATION_GATES.take(&snapshot.tool_ref.session_id) {
                gate.pass().await;
            }

            for (name, bytes) in blobs {
                if let Some(bytes) = bytes {
                    check_aux_deadline(deadline)?;
                    #[cfg(test)]
                    if tests::fail_blob_write(snapshot.tool_ref.session_id) {
                        return Err(StoreError::Unavailable);
                    }
                    write_sync_file(&temp_dir.join(name), bytes).await?;
                }
            }
            check_aux_deadline(deadline)?;
            write_sync_file(&temp_dir.join(TOOL_RECORD_FILE), &record_bytes).await?;
            check_aux_deadline(deadline)?;
            sync_directory(&temp_dir).await.map_err(unavailable)?;
            Ok::<(), StoreError>(())
        }
        .await;

        if write_res.is_err() {
            discard_aux_temp(&temp_dir, ledger).await;
            return Err(StoreError::Unavailable);
        }

        if Instant::now() >= deadline {
            discard_aux_temp(&temp_dir, ledger).await;
            return Err(StoreError::Unavailable);
        }

        if fs::rename(&temp_dir, &target_dir).await.is_err() {
            discard_aux_temp(&temp_dir, ledger).await;
            return Err(StoreError::Unavailable);
        }

        ledger.as_mut().unwrap().insert(AuxDirectoryEntry {
            session_id: snapshot.tool_ref.session_id,
            path: target_dir,
            mtime: std::time::SystemTime::now(),
            bytes: item_bytes,
        });

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
            if AUX_WRITE_FAILURES.hit(&first.session_id) {
                return tool_refs
                    .iter()
                    .cloned()
                    .map(|r| (r, Err(StoreError::Unavailable)))
                    .collect();
            }
        }

        let mut ledger = match tokio::time::timeout_at(deadline.into(), self.aux_lock.lock()).await
        {
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
            if let Some(gate) = AUX_COMMIT_GATES.take(&first.session_id) {
                gate.pass().await;
            }
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
                    results.push((tool_ref.clone(), Err(StoreError::Unavailable)));
                    continue;
                }
            };

            let res = self
                .commit_tool_record_locked(&snapshot, deadline, &mut ledger)
                .await;
            if matches!(res, Err(StoreError::QueryLimit)) || Instant::now() >= deadline {
                expired = true;
            }
            results.push((tool_ref.clone(), res));
        }

        results
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

        let mut skipped = false;
        let mut records = Vec::new();
        match path_state(&tools_dir).await {
            Ok(PathState::Missing) => {}
            Ok(PathState::Directory) => {
                let mut entries = match fs::read_dir(&tools_dir).await {
                    Ok(entries) => entries,
                    Err(_) => {
                        return Ok(make_tool_change_scan(records, true, &budget));
                    }
                };
                let limits = self.aux_limits();
                loop {
                    check_aux_scan(cancellation, deadline)?;
                    // Reserve the entry ceiling before the next directory I/O, so
                    // the hard limit is never exceeded by one speculative read.
                    if !budget.entry_available(limits.max_scan_entries) {
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
                            skipped = true;
                            continue;
                        }
                    };
                    let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
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
                            skipped = true;
                            break;
                        }
                        ChangeMetadataRead::Record(None) => {
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

                                skipped = true;
                                break;
                            }
                        }
                    }
                }
            }
            Ok(_) | Err(_) => {
                skipped = true;
            }
        }
        Ok(make_tool_change_scan(records, skipped, &budget))
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
        check_aux_scan(cancellation, deadline)?;
        self.require_sessions_root().await?;
        let session_dir = self.session_directory(session_id);
        if !real_dir_opt(&session_dir).await? {
            return Ok(None);
        }
        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        if !real_dir_opt(&tools_dir).await? {
            return Ok(None);
        }
        let target_dir = tools_dir.join(tool_ref_hash(tool_ref));
        if !real_dir_opt(&target_dir).await? {
            return Ok(None);
        }
        let record_path = target_dir.join(TOOL_RECORD_FILE);
        let loaded = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(StoreError::QueryLimit),
            result = tokio::time::timeout_at(deadline.into(), load_tool_metadata(&record_path)) => {
                result.map_err(|_| StoreError::QueryLimit)?
            }
        };
        let stored = match loaded {
            Ok(Some(stored)) => stored,
            Ok(None) | Err(StoreError::Corrupt) => return Ok(None),
            Err(error) => return Err(error),
        };
        if validate_stored_tool_record(&stored, tool_ref).is_err() {
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
        self.require_sessions_root().await?;

        let session_dir = self.session_directory(tool_ref.session_id);
        if !real_dir_opt(&session_dir).await? {
            return Err(StoreError::SessionNotFound);
        }
        let tools_dir = session_dir.join(AUX_TOOLS_DIR);
        if !real_dir_opt(&tools_dir).await? {
            return Ok(None);
        }
        let target_dir = tools_dir.join(tool_ref_hash(tool_ref));
        if !real_dir_opt(&target_dir).await? {
            return Ok(None);
        }

        let Some(stored) = load_tool_metadata(&target_dir.join(TOOL_RECORD_FILE)).await? else {
            return Ok(None);
        };

        validate_stored_tool_record(&stored, tool_ref)?;

        let mut blobs = std::array::from_fn::<_, 4, _>(|_| (None, false));
        for (slot, (name, bytes, sha)) in blobs.iter_mut().zip(stored_blobs(&stored)) {
            *slot = read_blob(&target_dir.join(name), bytes, sha).await;
        }
        let [input, result, stdout, stderr] = blobs;

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
            input,
            result,
            stdout,
            stderr,
            (file_change_before, file_change_before_corrupt),
            (file_change_after, file_change_after_corrupt),
        );

        Ok(Some(ReadToolRecord {
            metadata: stored,
            record,
        }))
    }

    async fn scan_aux_ledger(&self, deadline: Instant) -> Result<AuxLedger, StoreError> {
        self.require_sessions_root().await?;
        let mut sessions = fs::read_dir(self.sessions_directory())
            .await
            .map_err(unavailable)?;
        let mut ledger = AuxLedger::default();
        // The read-query entry ceiling must not prevent budget eviction at startup.
        while let Some(entry) = next_aux_entry(&mut sessions, deadline).await? {
            let path = entry.path();
            let meta = fs::symlink_metadata(&path).await.map_err(unavailable)?;
            let session_id = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<SessionId>().ok());
            let Some(session_id) = session_id.filter(|_| meta.is_dir()) else {
                ledger.note_foreign(None, meta.len());
                tracing::warn!(entry = ?path, "skipping foreign auxiliary budget entry");
                continue;
            };
            let tools = path.join(AUX_TOOLS_DIR);
            match real_dir_opt(&tools).await {
                Ok(false) => continue,
                Ok(true) => {}
                Err(_) => {
                    let bytes = fs::symlink_metadata(&tools)
                        .await
                        .map_or(0, |meta| meta.len());
                    ledger.note_foreign(Some(session_id), bytes);
                    tracing::warn!(entry = ?tools, "skipping unreadable auxiliary directory");
                    continue;
                }
            }
            let mut entries = fs::read_dir(&tools).await.map_err(unavailable)?;
            while let Some(entry) = next_aux_entry(&mut entries, deadline).await? {
                let path = entry.path();
                let meta = fs::symlink_metadata(&path).await.map_err(unavailable)?;
                let name = entry.file_name();
                let name = name.to_str().unwrap_or("");
                let (bytes, valid_files) = if meta.is_dir() {
                    scan_aux_files(&path, deadline).await?
                } else {
                    (meta.len(), false)
                };
                if valid_files && is_valid_temp_name(name) {
                    #[cfg(test)]
                    if FAIL_REMOVE_TEMP_SESSIONS.hit(&session_id) {
                        return Err(StoreError::Unavailable);
                    }
                    if remove_aux_directory(&path).await.is_ok() {
                        continue;
                    }
                } else if valid_files && valid_sha256(name) {
                    if let Ok(Some(record)) = load_tool_metadata(&path.join(TOOL_RECORD_FILE)).await
                    {
                        if record.tool_ref.session_id == session_id
                            && tool_ref_hash(&record.tool_ref) == name
                            && validate_stored_tool_record(&record, &record.tool_ref).is_ok()
                        {
                            ledger.insert(AuxDirectoryEntry {
                                session_id,
                                path,
                                bytes,
                                mtime: meta.modified().map_err(unavailable)?,
                            });
                            continue;
                        }
                    }
                }
                ledger.note_foreign(Some(session_id), bytes);
                tracing::warn!(entry = ?path, "skipping foreign or corrupt auxiliary record");
            }
        }
        Ok(ledger)
    }

    async fn enforce_aux_budget_locked(
        &self,
        target_session: SessionId,
        reserve: u64,
        limits: AuxLimits,
        deadline: Instant,
        ledger: &mut Option<AuxLedger>,
    ) -> Result<(), StoreError> {
        if ledger.is_none() {
            *ledger = Some(self.scan_aux_ledger(deadline).await?);
        }
        let ledger = ledger.as_mut().unwrap();
        loop {
            let (bytes, records) = ledger
                .sessions
                .get(&target_session)
                .copied()
                .unwrap_or_default();
            let session_full = bytes.saturating_add(reserve) > limits.session_bytes
                || records + 1 > limits.session_records;
            let global_full = ledger.global_bytes.saturating_add(reserve) > limits.global_bytes
                || ledger.entries.len() + 1 > limits.global_records;
            if !session_full && !global_full {
                return Ok(());
            }
            check_aux_deadline(deadline)?;
            let victim = ledger
                .entries
                .iter()
                .find(|(_, entry)| !session_full || entry.session_id == target_session)
                .map(|(key, entry)| (key.clone(), entry.path.clone()))
                .ok_or(StoreError::Unavailable)?;
            remove_aux_directory(&victim.1).await?;
            ledger.remove(&victim.0);
        }
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
    skipped: bool,
    budget: &ChangeScanBudget,
) -> ToolChangeScan {
    records.sort_by_cached_key(|entry| tool_ref_hash(&entry.0));
    let fingerprint = hash_bytes(
        &serde_json::to_vec(&(
            &records,
            !skipped,
            skipped,
            budget.entries,
            budget.metadata_bytes,
        ))
        .expect("serializing tool change metadata cannot fail"),
    );
    ToolChangeScan {
        records,
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

type AuxKey = (std::time::SystemTime, PathBuf);

#[derive(Default)]
pub(super) struct AuxLedger {
    entries: std::collections::BTreeMap<AuxKey, AuxDirectoryEntry>,
    sessions: HashMap<SessionId, (u64, usize)>,
    global_bytes: u64,
}

impl AuxLedger {
    fn insert(&mut self, entry: AuxDirectoryEntry) {
        let totals = self.sessions.entry(entry.session_id).or_default();
        totals.0 += entry.bytes;
        totals.1 += 1;
        self.global_bytes += entry.bytes;
        self.entries
            .insert((entry.mtime, entry.path.clone()), entry);
    }

    fn note_foreign(&mut self, session_id: Option<SessionId>, bytes: u64) {
        self.global_bytes += bytes;
        if let Some(session_id) = session_id {
            self.sessions.entry(session_id).or_default().0 += bytes;
        }
    }

    fn remove(&mut self, key: &AuxKey) {
        let entry = self.entries.remove(key).unwrap();
        self.global_bytes -= entry.bytes;
        let totals = self.sessions.get_mut(&entry.session_id).unwrap();
        totals.0 -= entry.bytes;
        totals.1 -= 1;
    }

    pub(super) fn remove_session(&mut self, session_id: SessionId) {
        if let Some((bytes, _)) = self.sessions.remove(&session_id) {
            self.global_bytes -= bytes;
        }
        self.entries
            .retain(|_, entry| entry.session_id != session_id);
    }
}

fn check_aux_deadline(deadline: Instant) -> Result<(), StoreError> {
    if Instant::now() >= deadline {
        Err(StoreError::Unavailable)
    } else {
        Ok(())
    }
}

async fn next_aux_entry(
    entries: &mut fs::ReadDir,
    deadline: Instant,
) -> Result<Option<fs::DirEntry>, StoreError> {
    check_aux_deadline(deadline)?;
    entries.next_entry().await.map_err(unavailable)
}

async fn scan_aux_files(path: &Path, deadline: Instant) -> Result<(u64, bool), StoreError> {
    let mut entries = fs::read_dir(path).await.map_err(unavailable)?;
    let mut bytes = 0;
    let mut valid = true;
    while let Some(entry) = next_aux_entry(&mut entries, deadline).await? {
        let meta = fs::symlink_metadata(entry.path())
            .await
            .map_err(unavailable)?;
        bytes += meta.len();
        valid &= meta.is_file()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| ALLOWED_AUX_FILES.contains(&name));
    }
    Ok((bytes, valid))
}

async fn load_tool_metadata(path: &Path) -> Result<Option<StoredToolRecord>, StoreError> {
    let file = match safe_open_read(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(StoreError::Unavailable),
    };
    let mut bytes = Vec::new();
    file.take((MAX_TOOL_METADATA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(unavailable)?;
    if bytes.len() > MAX_TOOL_METADATA_BYTES {
        return Err(StoreError::Corrupt);
    }
    serde_json::from_slice(&bytes).map(Some).map_err(corrupt)
}

async fn remove_aux_directory(path: &Path) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .expect("auxiliary directory has a tools parent");
    fs::remove_dir_all(path).await.map_err(unavailable)?;
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

async fn discard_aux_temp(path: &Path, ledger: &mut Option<AuxLedger>) {
    if remove_aux_directory(path).await.is_err() {
        // A surviving partial directory has unknown size; rebuild before reserving again.
        *ledger = None;
    }
}

async fn write_sync_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(unavailable)?;
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

    for (seen, total_bytes, file_bytes, file_sha256, expired, cap) in [
        (
            stored.input.seen,
            stored.input.total_bytes,
            stored.input.file_bytes,
            &stored.input.file_sha256,
            stored.input.expired,
            MAX_TOOL_INPUT_PERSIST_BYTES,
        ),
        (
            stored.result.seen,
            stored.result.total_bytes,
            stored.result.file_bytes,
            &stored.result.file_sha256,
            stored.result.expired,
            MAX_TOOL_RESULT_PERSIST_BYTES,
        ),
    ] {
        if !seen {
            if total_bytes != 0 || file_bytes != 0 || file_sha256.is_some() {
                return Err(StoreError::Corrupt);
            }
        } else {
            if file_bytes > total_bytes {
                return Err(StoreError::Corrupt);
            }
            if expired && (file_bytes != 0 || file_sha256.is_some()) {
                return Err(StoreError::Corrupt);
            }
        }
        if file_bytes > cap {
            return Err(StoreError::RecordTooLarge);
        }
    }

    for stream in [&stored.stdout, &stored.stderr] {
        if stream.start_offset > stream.observed_end {
            return Err(StoreError::Corrupt);
        }
        if !stream.seen {
            if stream.start_offset != 0
                || stream.observed_end != 0
                || stream.file_bytes != 0
                || stream.file_sha256.is_some()
            {
                return Err(StoreError::Corrupt);
            }
        } else if stream.expired {
            if stream.start_offset != stream.observed_end
                || stream.file_bytes != 0
                || stream.file_sha256.is_some()
            {
                return Err(StoreError::Corrupt);
            }
        } else {
            let diff = stream
                .observed_end
                .checked_sub(stream.start_offset)
                .ok_or(StoreError::Corrupt)?;
            if diff != stream.file_bytes as u64 {
                return Err(StoreError::Corrupt);
            }
        }
        if stream.file_bytes > crate::tool_data::MAX_TOOL_STREAM_BYTES {
            return Err(StoreError::RecordTooLarge);
        }
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

    for (_, bytes, hash) in stored_blobs(stored) {
        if if bytes == 0 {
            hash.is_some()
        } else {
            !hash.is_some_and(valid_sha256)
        } {
            return Err(StoreError::Corrupt);
        }
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
    crate::workspace::validate_relative_path(&change.path).map_err(corrupt)?;
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
    if let Some(bytes) = before_bytes {
        let ChangeRevision::Content {
            sha256,
            bytes: size,
        } = &stored.before
        else {
            return Err(StoreError::Corrupt);
        };
        if bytes.len() != *size || hash_bytes(bytes) != *sha256 {
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
        if bytes.len() != *size || hash_bytes(bytes) != *sha256 {
            return Err(StoreError::Corrupt);
        }
    }
    Ok(())
}

async fn read_blob(
    path: &Path,
    expected_bytes: usize,
    expected_sha256: Option<&str>,
) -> (Option<Vec<u8>>, bool) {
    if expected_bytes == 0 {
        return (None, false);
    }
    let expected_hash = expected_sha256.expect("validated nonempty blob has a digest");
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
    tests::note_change_blob(path);
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

fn snapshot_blobs(snapshot: &ToolPersistenceSnapshot) -> [(&str, Option<&[u8]>); 6] {
    [
        (TOOL_INPUT_FILE, snapshot.input_bytes.as_deref()),
        (TOOL_RESULT_FILE, snapshot.result_bytes.as_deref()),
        (TOOL_STDOUT_FILE, snapshot.stdout_bytes.as_deref()),
        (TOOL_STDERR_FILE, snapshot.stderr_bytes.as_deref()),
        (TOOL_BEFORE_FILE, snapshot.file_change_before.as_deref()),
        (TOOL_AFTER_FILE, snapshot.file_change_after.as_deref()),
    ]
}

// The four core blobs use the same order as snapshot_blobs; change snapshots follow them.
fn stored_blobs(stored: &StoredToolRecord) -> [(&str, usize, Option<&str>); 4] {
    [
        (
            TOOL_INPUT_FILE,
            stored.input.file_bytes,
            stored.input.file_sha256.as_deref(),
        ),
        (
            TOOL_RESULT_FILE,
            stored.result.file_bytes,
            stored.result.file_sha256.as_deref(),
        ),
        (
            TOOL_STDOUT_FILE,
            stored.stdout.file_bytes,
            stored.stdout.file_sha256.as_deref(),
        ),
        (
            TOOL_STDERR_FILE,
            stored.stderr.file_bytes,
            stored.stderr.file_sha256.as_deref(),
        ),
    ]
}
