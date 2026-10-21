//! Auxiliary record commits.
use super::super::*;
use super::ledger::{AuxDirectoryEntry, AuxLedger, check_aux_deadline, discard_aux_temp};
use super::validate::validate_stored_tool_record;

#[cfg(test)]
pub(crate) static AUX_BLOB_WRITE_FAILURES: crate::test_support::Registry<SessionId, usize> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static AUX_TEMP_CREATION_GATES: crate::test_support::Registry<
    SessionId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

impl Store {
    pub(super) fn aux_limits(&self) -> AuxLimits {
        #[cfg(test)]
        if let Some(limits) = self.aux_limits {
            return limits;
        }
        DEFAULT_AUX_LIMITS
    }

    pub(in crate::store) async fn commit_tool_record_locked(
        &self,
        snapshot: &ToolPersistenceSnapshot,
        deadline: Instant,
        ledger: &mut Option<AuxLedger>,
    ) -> Result<(), StoreError> {
        check_aux_deadline(deadline)?;
        validate_stored_tool_record(&snapshot.record, &snapshot.tool_ref)?;

        let blobs = snapshot.blobs();
        // Snapshots are assembled in-process; blob integrity is checked when
        // reading from disk, not by hashing the same owned bytes again here.
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
            Err(error) => return Err(unavailable(error)),
        }
        let temp_dir = unique_temp_path(&target_dir)?;
        if Instant::now() >= deadline {
            let _ = fs::remove_dir(&tools_dir).await;
            return Err(StoreError::Unavailable);
        }
        if let Err(error) = fs::create_dir(&temp_dir).await {
            let _ = fs::remove_dir(&tools_dir).await;
            return Err(unavailable(error));
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

        if let Err(error) = fs::rename(&temp_dir, &target_dir).await {
            discard_aux_temp(&temp_dir, ledger).await;
            return Err(unavailable(error));
        }

        ledger.as_mut().unwrap().insert(AuxDirectoryEntry {
            session_id: snapshot.tool_ref.session_id,
            path: target_dir,
            mtime: std::time::SystemTime::now(),
            bytes: item_bytes,
        });

        sync_directory(&tools_dir).await.map_err(unavailable)
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
        if let Some(first) = tool_refs.first()
            && AUX_WRITE_FAILURES.hit(&first.session_id)
        {
            return tool_refs
                .iter()
                .cloned()
                .map(|r| (r, Err(StoreError::Unavailable)))
                .collect();
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
        if let Some(first) = tool_refs.first()
            && let Some(gate) = AUX_COMMIT_GATES.take(&first.session_id)
        {
            gate.pass().await;
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
}
