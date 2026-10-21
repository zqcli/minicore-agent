//! Auxiliary budget accounting and eviction.
use super::super::*;
use super::cold_read::load_tool_metadata;
use super::validate::validate_stored_tool_record;

#[cfg(test)]
pub(crate) static AUX_PARENT_CLEANUP_GATES: crate::test_support::Registry<
    PathBuf,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

impl Store {
    async fn scan_aux_ledger(&self, deadline: Instant) -> Result<AuxLedger, StoreError> {
        self.require_sessions_root().await?;
        let mut sessions = fs::read_dir(self.sessions_directory())
            .await
            .map_err(unavailable)?;
        let mut ledger = AuxLedger::default();
        // The read-query entry ceiling must not prevent budget eviction at startup.
        while let Some(entry) = next_aux_entry(&mut sessions, deadline).await? {
            if entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix(DELETING_PREFIX))
                .is_some_and(|id| id.parse::<SessionId>().is_ok())
            {
                continue;
            }
            let path = entry.path();
            let meta = match fs::symlink_metadata(&path).await {
                Ok(meta) => meta,
                Err(error) => {
                    tracing::warn!(error_kind = ?error.kind(), "skipping unreadable auxiliary entry");
                    continue;
                }
            };
            let session_id = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<SessionId>().ok());
            let Some(session_id) = session_id.filter(|_| meta.is_dir()) else {
                ledger.note_foreign(None, meta.len());
                tracing::warn!("skipping foreign auxiliary budget entry");
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
                    tracing::warn!("skipping unreadable auxiliary directory");
                    continue;
                }
            }
            let mut entries = match fs::read_dir(&tools).await {
                Ok(entries) => entries,
                Err(error) => {
                    ledger.note_foreign(
                        Some(session_id),
                        fs::symlink_metadata(&tools)
                            .await
                            .map_or(0, |meta| meta.len()),
                    );
                    tracing::warn!(error_kind = ?error.kind(), "skipping unreadable auxiliary directory");
                    continue;
                }
            };
            loop {
                check_aux_deadline(deadline)?;
                let entry = match entries.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(error_kind = ?error.kind(), "stopping unreadable auxiliary directory scan");
                        break;
                    }
                };
                let path = entry.path();
                let meta = match fs::symlink_metadata(&path).await {
                    Ok(meta) => meta,
                    Err(error) => {
                        tracing::warn!(error_kind = ?error.kind(), "skipping unreadable auxiliary entry");
                        continue;
                    }
                };
                let name = entry.file_name();
                let name = name.to_str().unwrap_or("");
                let (bytes, valid_files) = if meta.is_dir() {
                    scan_aux_files(&path, deadline, meta.len()).await?
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
                } else if valid_files
                    && valid_sha256(name)
                    && let Ok(Some(record)) = load_tool_metadata(&path.join(TOOL_RECORD_FILE)).await
                    && record.tool_ref.session_id == session_id
                    && tool_ref_hash(&record.tool_ref) == name
                    && validate_stored_tool_record(&record, &record.tool_ref).is_ok()
                    && let Ok(mtime) = meta.modified()
                {
                    ledger.insert(AuxDirectoryEntry {
                        session_id,
                        path,
                        bytes,
                        mtime,
                    });
                    continue;
                }
                ledger.note_foreign(Some(session_id), bytes);
                tracing::warn!("skipping foreign or corrupt auxiliary record");
            }
        }
        Ok(ledger)
    }

    pub(super) async fn enforce_aux_budget_locked(
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
        loop {
            let current = ledger.as_mut().unwrap();
            let (bytes, records) = current
                .sessions
                .get(&target_session)
                .copied()
                .unwrap_or_default();
            let session_full = bytes.saturating_add(reserve) > limits.session_bytes
                || records + 1 > limits.session_records;
            let global_full = current.global_bytes.saturating_add(reserve) > limits.global_bytes
                || current.entries.len() + 1 > limits.global_records;
            if !session_full && !global_full {
                return Ok(());
            }
            check_aux_deadline(deadline)?;
            let victim = current
                .entries
                .iter()
                .find(|(_, entry)| !session_full || entry.session_id == target_session)
                .map(|(key, entry)| (key.clone(), entry.path.clone()))
                .ok_or(StoreError::Unavailable)?;
            // A failed or cancelled removal may have deleted only part of the record.
            let mut current = ledger.take().unwrap();
            fs::remove_dir_all(&victim.1).await.map_err(unavailable)?;
            current.remove(&victim.0);
            *ledger = Some(current);
            cleanup_aux_parent(&victim.1).await;
        }
    }
}

#[derive(Clone)]
pub(super) struct AuxDirectoryEntry {
    pub(super) session_id: SessionId,
    pub(super) path: PathBuf,
    pub(super) mtime: std::time::SystemTime,
    pub(super) bytes: u64,
}

type AuxKey = (std::time::SystemTime, PathBuf);

#[derive(Default)]
pub(in crate::store) struct AuxLedger {
    entries: std::collections::BTreeMap<AuxKey, AuxDirectoryEntry>,
    sessions: HashMap<SessionId, (u64, usize)>,
    global_bytes: u64,
}

impl AuxLedger {
    pub(super) fn insert(&mut self, entry: AuxDirectoryEntry) {
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

    pub(in crate::store) fn remove_session(&mut self, session_id: SessionId) {
        if let Some((bytes, _)) = self.sessions.remove(&session_id) {
            self.global_bytes -= bytes;
        }
        self.entries
            .retain(|_, entry| entry.session_id != session_id);
    }
}

pub(super) fn check_aux_deadline(deadline: Instant) -> Result<(), StoreError> {
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

async fn scan_aux_files(
    path: &Path,
    deadline: Instant,
    directory_bytes: u64,
) -> Result<(u64, bool), StoreError> {
    let mut entries = match fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(_) => return Ok((directory_bytes, false)),
    };
    let mut bytes = 0;
    let mut valid = true;
    loop {
        check_aux_deadline(deadline)?;
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(_) => return Ok((bytes, false)),
        };
        let meta = match fs::symlink_metadata(entry.path()).await {
            Ok(meta) => meta,
            Err(_) => {
                valid = false;
                continue;
            }
        };
        bytes += meta.len();
        valid &= meta.is_file()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| ALLOWED_AUX_FILES.contains(&name));
    }
    Ok((bytes, valid))
}

async fn remove_aux_directory(path: &Path) -> Result<(), StoreError> {
    fs::remove_dir_all(path).await.map_err(unavailable)?;
    cleanup_aux_parent(path).await;
    Ok(())
}

async fn cleanup_aux_parent(path: &Path) {
    let parent = path
        .parent()
        .expect("auxiliary directory has a tools parent");
    #[cfg(test)]
    if let Some(gate) = AUX_PARENT_CLEANUP_GATES.take(&path.to_path_buf()) {
        gate.pass().await;
    }
    let _ = fs::remove_dir(parent).await;
}

pub(super) async fn discard_aux_temp(path: &Path, ledger: &mut Option<AuxLedger>) {
    if remove_aux_directory(path).await.is_err() {
        // A surviving partial directory has unknown size; rebuild before reserving again.
        *ledger = None;
    }
}

pub(super) fn is_valid_temp_name(name: &str) -> bool {
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
    valid_temp_suffix(&name[70..])
}
