//! Bounded auxiliary reads and change scans.
use super::super::*;
use super::ledger::is_valid_temp_name;
use super::validate::{stored_blobs, validate_stored_tool_record};

impl Store {
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

    pub(super) async fn read_tool_record_details(
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
    pub(in crate::store) fn entry_available(&self, max_entries: usize) -> bool {
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
pub(in crate::store) fn change_blob_read_cost(revision: &ChangeRevision) -> usize {
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

pub(super) async fn load_tool_metadata(
    path: &Path,
) -> Result<Option<StoredToolRecord>, StoreError> {
    let file = match safe_open_read(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(map_io_store_error(error)),
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
