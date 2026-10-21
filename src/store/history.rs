//! JSONL history read, write, scan, and anchor implementation.
use super::*;

#[derive(Clone)]
pub(crate) struct HistoryLine {
    end: u64,
    items: usize,
    hash: Sha256,
    usage: crate::read::CoveredUsage,
}

fn index_line(
    index: &mut Vec<HistoryLine>,
    bytes: &[u8],
    record: &StoredLoopRecord,
    item_count: usize,
) {
    let mut line = index.last().cloned().unwrap_or(HistoryLine {
        end: 0,
        items: 0,
        hash: Sha256::new(),
        usage: crate::read::CoveredUsage::default(),
    });
    line.end += bytes.len() as u64;
    line.items += item_count;
    line.hash.update(bytes);
    line.usage.add(record.loop_id, record.usage);
    index.push(line);
}

struct IndexedPage {
    start: u64,
    end: u64,
    first_item: usize,
    page: HistoryReadPage,
}

fn indexed_page(
    index: &[HistoryLine],
    offset: usize,
    limit: usize,
    visible: Option<usize>,
    captured: Option<u64>,
    revision: Option<&str>,
    usage_boundary: Option<usize>,
) -> Result<IndexedPage, StoreError> {
    let end = captured.map_or_else(
        || {
            visible.map_or(index.len(), |cap| {
                index.partition_point(|line| line.items <= cap)
            })
        },
        |end| index.partition_point(|line| line.end <= end),
    );
    let index = &index[..end];
    let last = index.last();
    let captured_end = last.map_or(0, |line| line.end);
    let total_items = last.map_or(0, |line| line.items);
    if captured.is_some_and(|end| end != captured_end)
        || visible.is_some_and(|cap| total_items > cap)
    {
        return Err(StoreError::HistoryChanged);
    }
    let hash = last.map_or_else(Sha256::new, |line| line.hash.clone());
    let actual_revision = digest_hex(hash);
    if revision.is_some_and(|revision| revision != actual_revision) {
        return Err(StoreError::HistoryChanged);
    }
    let mut page = HistoryReadPage {
        captured_end,
        revision: actual_revision,
        total_items,
        summary_anchor_valid: true,
        ..HistoryReadPage::default()
    };
    if let Some(boundary) = usage_boundary {
        let count = index.partition_point(|line| line.items <= boundary);
        let covered = index[..count].last();
        if covered.map_or(0, |line| line.items) != boundary {
            return Err(StoreError::HistoryChanged);
        }
        if let Some(covered) = covered {
            page.covered_usage = covered.usage.clone();
        }
    }
    let first = index.partition_point(|line| line.items <= offset);
    let start = first.checked_sub(1).map(|i| &index[i]);
    let last = index.partition_point(|line| line.items < offset.saturating_add(limit));
    let end = if limit == 0 || first == index.len() {
        start.map_or(0, |line| line.end)
    } else {
        index[last.min(index.len() - 1)].end
    };
    Ok(IndexedPage {
        start: start.map_or(0, |line| line.end),
        end,
        first_item: start.map_or(0, |line| line.items),
        page,
    })
}

impl Store {
    async fn require_history_file(
        &self,
        session_id: SessionId,
    ) -> Result<(PathBuf, u64), StoreError> {
        let path = self
            .require_session_directory(session_id)
            .await?
            .join(HISTORY_FILE);
        let metadata = match fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(StoreError::Corrupt);
            }
            Err(error) => return Err(unavailable(error)),
        };
        if !metadata.is_file() {
            return Err(StoreError::Corrupt);
        }
        Ok((path, metadata.len()))
    }

    async fn read_indexed_page(
        &self,
        session_id: SessionId,
        mut indexed: IndexedPage,
        offset: usize,
        limit: usize,
        limits: &HistoryScanLimits,
    ) -> Result<HistoryReadPage, StoreError> {
        check_history_scan(limits)?;
        let (path, _) = self.require_history_file(session_id).await?;
        let mut file = File::open(path).await.map_err(unavailable)?;
        file.seek(std::io::SeekFrom::Start(indexed.start))
            .await
            .map_err(unavailable)?;
        let mut reader = BufReader::new(file);
        let mut remaining = indexed.end - indexed.start;
        let mut item_start = indexed.first_item;
        let page = &mut indexed.page;
        while remaining > 0 {
            if page.scanned_lines >= limits.max_lines || page.scanned_bytes >= limits.max_bytes {
                return Err(StoreError::QueryLimit);
            }
            let BoundedLine::Complete(line) =
                read_bounded_line(&mut reader, &mut remaining, Some(limits)).await?
            else {
                return Err(StoreError::HistoryChanged);
            };
            page.scanned_bytes += line.len() as u64 + 1;
            if page.scanned_bytes > limits.max_bytes {
                return Err(StoreError::QueryLimit);
            }
            page.scanned_lines += 1;
            let record: StoredLoopRecord = serde_json::from_slice(&line).map_err(corrupt)?;
            let items = normalize_history(&record.items).map_err(corrupt)?;
            let item_end = item_start + items.len();
            let times = record.normalized_user_times().unwrap_or_default();
            let mut occurrence = 0;
            for (i, item) in items.iter().enumerate() {
                let time = if matches!(item, HistoryItem::User(_)) {
                    let time = times.get(occurrence).cloned().flatten();
                    occurrence += 1;
                    time
                } else {
                    None
                };
                if (offset..offset.saturating_add(limit)).contains(&(item_start + i)) {
                    page.items.push(item.clone());
                    page.user_times.push(time);
                }
            }
            if page.turns.len() < MAX_READ_TURN_SUMMARIES {
                page.turns.push(StoredTurnSummary {
                    item_start,
                    item_end,
                    loop_id: record.loop_id,
                    outcome: record.outcome,
                    usage: record.usage,
                    requests: record.requests,
                    tool_rounds: record.tool_rounds,
                    final_config_revision: record.final_config_revision,
                    completed_at: record.completed_at,
                });
            } else {
                page.turns_truncated = true;
            }
            item_start = item_end;
            tokio::task::yield_now().await;
        }
        Ok(indexed.page)
    }

    /// Scans one raw history prefix without invoking the normal tail repair.
    /// The result exists only when the requested byte offset ends immediately
    /// after a complete StoredLoopRecord line.
    pub(crate) async fn read_history_prefix(
        &self,
        session_id: SessionId,
        prefix_bytes: u64,
        expected_history: &[HistoryItem],
    ) -> Result<Option<HistoryPrefix>, StoreError> {
        let (path, length) = self.require_history_file(session_id).await?;
        if prefix_bytes > length {
            return Ok(None);
        }
        let file = File::open(path).await.map_err(unavailable)?;
        Ok(
            scan_history_prefix(file, prefix_bytes, expected_history, None)
                .await?
                .map(|anchors| anchors.full),
        )
    }

    /// Captures the complete raw history anchor without tail repair. The
    /// loaded sanitized history must account for every stored item before an
    /// anchor can be used for a derived snapshot write.
    pub(crate) async fn capture_history_anchor(
        &self,
        session_id: SessionId,
        expected_history: &[HistoryItem],
    ) -> Result<Option<HistoryPrefix>, StoreError> {
        Ok(self
            .capture_compaction_anchors(session_id, expected_history, None)
            .await?
            .map(|anchors| anchors.full))
    }

    /// Select the last complete loop ending at or before `max_prefix_items`,
    /// while still validating every retained item and the full file length.
    pub(crate) async fn capture_compaction_anchors(
        &self,
        session_id: SessionId,
        expected_history: &[HistoryItem],
        max_prefix_items: Option<usize>,
    ) -> Result<Option<CompactionAnchors>, StoreError> {
        let (path, length) = self.require_history_file(session_id).await?;
        let file = File::open(&path).await.map_err(unavailable)?;
        let Some(anchors) =
            scan_history_prefix(file, length, expected_history, max_prefix_items).await?
        else {
            return Ok(None);
        };
        let final_length = fs::metadata(&path).await.map_err(unavailable)?.len();
        if final_length != length {
            return Ok(None);
        }
        if anchors.full.covered_item_count
            != u64::try_from(expected_history.len()).unwrap_or(u64::MAX)
        {
            return Ok(None);
        }
        Ok(Some(anchors))
    }

    /// Appends one completed loop as a single JSON line. On success the file
    /// content is complete; callers merge the sanitized items into memory.
    pub(crate) async fn append_loop(
        &self,
        session_id: SessionId,
        record: &StoredLoopRecord,
    ) -> Result<(), AppendError> {
        let (path, previous_len) = self.require_history_file(session_id).await?;
        // Keep legacy raw record bytes unchanged. New replay-bearing records
        // must pass durable admission before any append is attempted.
        let normalized_items = normalize_history(&record.items).map_err(corrupt)?;
        let normalized_record = if record.items.iter().any(|item| {
            matches!(item,
            HistoryItem::Assistant(assistant) if assistant.provider_replay.is_some())
        }) {
            let mut normalized = record.clone();
            normalized.items = normalized_items.to_vec();
            Some(normalized)
        } else {
            None
        };
        let record = normalized_record.as_ref().unwrap_or(record);
        let mut bytes = if record.user_times_are_valid() {
            serde_json::to_vec(record)
        } else {
            let mut normalized = record.clone();
            normalized.user_times = record.normalized_user_times();
            serde_json::to_vec(&normalized)
        }
        .map_err(corrupt)?;
        if bytes.len() > MAX_LOOP_RECORD_BYTES && record.user_times.is_some() {
            // Presentation metadata must not make an otherwise storable core
            // loop record cross the single-line limit.
            let mut without_metadata = record.clone();
            without_metadata.user_times = None;
            bytes = serde_json::to_vec(&without_metadata).map_err(corrupt)?;
        }
        if bytes.len() > MAX_LOOP_RECORD_BYTES {
            return Err(StoreError::RecordTooLarge.into());
        }
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .map_err(unavailable)?;
        #[cfg(test)]
        if let Some(rollback_fails) = APPEND_FAILURES.take(&session_id) {
            return tests::fail_append(file, &path, &bytes, previous_len, rollback_fails).await;
        }
        let written = async {
            file.write_all(&bytes).await?;
            file.flush().await?;
            file.sync_data().await
        }
        .await;
        if let Err(error) = written {
            unavailable(error);
            return Err(rollback_append(&path, previous_len).await);
        }
        if let Some(index) = self.history_indexes.lock().unwrap().get_mut(&session_id) {
            index_line(index, &bytes, record, normalized_items.len());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_history_page(
        &self,
        session_id: SessionId,
        item_offset: usize,
        item_limit: usize,
        visible_item_count: Option<usize>,
        captured_end: Option<u64>,
        expected_revision: Option<&str>,
        usage_boundary: Option<usize>,
        limits: &HistoryScanLimits,
    ) -> Result<HistoryReadPage, StoreError> {
        self.scan_history_page(
            session_id,
            item_offset,
            item_limit,
            visible_item_count,
            captured_end,
            expected_revision,
            usage_boundary,
            None,
            limits,
        )
        .await
    }

    /// Cold display uses the same bounded, read-only scan as canonical reads.
    /// A candidate summary is accepted only against this scan's exact bytes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_display_history_page(
        &self,
        session_id: SessionId,
        item_offset: usize,
        item_limit: usize,
        captured_end: Option<u64>,
        expected_revision: Option<&str>,
        summary: Option<&HistoryPrefix>,
        limits: &HistoryScanLimits,
    ) -> Result<HistoryReadPage, StoreError> {
        self.scan_history_page(
            session_id,
            item_offset,
            item_limit,
            None,
            captured_end,
            expected_revision,
            Some(summary.map_or(0, |source| source.covered_item_count as usize)),
            Some(ColdDisplayScan { summary }),
            limits,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_history_page(
        &self,
        session_id: SessionId,
        item_offset: usize,
        item_limit: usize,
        visible_item_count: Option<usize>,
        captured_end: Option<u64>,
        expected_revision: Option<&str>,
        usage_boundary: Option<usize>,
        display: Option<ColdDisplayScan<'_>>,
        limits: &HistoryScanLimits,
    ) -> Result<HistoryReadPage, StoreError> {
        if captured_end.is_some() != expected_revision.is_some() {
            return Err(StoreError::InvalidArguments);
        }
        let indexed = if display.is_none() {
            self.history_indexes
                .lock()
                .unwrap()
                .get(&session_id)
                .map(|index| {
                    indexed_page(
                        index,
                        item_offset,
                        item_limit,
                        visible_item_count,
                        captured_end,
                        expected_revision,
                        usage_boundary,
                    )
                })
                .transpose()?
        } else {
            None
        };
        if let Some(indexed) = indexed {
            return self
                .read_indexed_page(session_id, indexed, item_offset, item_limit, limits)
                .await;
        }
        let (path, length) = self.require_history_file(session_id).await?;
        let target = captured_end.unwrap_or(length);
        if target > length {
            return Err(StoreError::HistoryChanged);
        }
        if let Some(expected) = expected_revision
            && !valid_sha256(expected)
        {
            return Err(StoreError::InvalidArguments);
        }
        check_history_scan(limits)?;
        if visible_item_count == Some(0) {
            if captured_end.is_some_and(|end| end != 0) {
                return Err(StoreError::HistoryChanged);
            }
            return finish_empty_history_page(0, expected_revision);
        }

        let file = File::open(&path).await.map_err(unavailable)?;
        let mut reader = BufReader::new(file);
        let scan_target = target.min(limits.max_bytes);
        let mut remaining = scan_target;
        let mut complete_end = 0_u64;
        let mut hasher = Sha256::new();
        let mut line_count = 0_usize;
        let mut total_items = 0_usize;
        let mut covered_usage = crate::read::CoveredUsage::default();
        let mut items = Vec::new();
        let mut user_times = Vec::new();
        let mut display_items = display.as_ref().map(|_| Vec::new());
        let mut summary_anchor_valid = display.as_ref().is_none_or(|read| read.summary.is_none());
        let page_end = item_offset.saturating_add(item_limit);
        let mut turns = Vec::new();
        let mut turns_truncated = false;
        let mut stopped_at_visible_cap = false;
        let mut trailing_incomplete = false;
        let retained_source_limit = limits
            .max_bytes
            .saturating_sub(MAX_LOOP_RECORD_BYTES as u64);
        let mut retained_source_bytes = 0_u64;

        loop {
            if remaining == 0 {
                break;
            }
            check_history_scan(limits)?;
            if line_count >= limits.max_lines {
                return Err(StoreError::QueryLimit);
            }
            let line = match read_bounded_line(&mut reader, &mut remaining, Some(limits)).await? {
                BoundedLine::Complete(line) => line,
                BoundedLine::Partial => {
                    if captured_end.is_some() {
                        return Err(StoreError::HistoryChanged);
                    }
                    if scan_target < target {
                        return Err(StoreError::QueryLimit);
                    }
                    trailing_incomplete = true;
                    break;
                }
                BoundedLine::End => return Err(StoreError::HistoryChanged),
            };
            if line.is_empty() {
                return Err(StoreError::Corrupt);
            }
            // JSONL has no item counts; legacy-only items can disappear on normalization.
            // Off-page records still establish offsets, covered usage, and corruption status.
            let record: StoredLoopRecord = serde_json::from_slice(&line).map_err(corrupt)?;
            let normalized = normalize_history(&record.items).map_err(corrupt)?;
            let record_start = total_items;
            let record_end = record_start
                .checked_add(normalized.len())
                .ok_or(StoreError::Corrupt)?;
            if let Some(boundary) = usage_boundary {
                if record_start < boundary && record_end > boundary && display.is_none() {
                    return Err(StoreError::HistoryChanged);
                }
                if record_end <= boundary {
                    covered_usage.add(record.loop_id, record.usage);
                }
            }

            if visible_item_count.is_some_and(|cap| record_end > cap) {
                return Err(StoreError::HistoryChanged);
            }
            if record_end > item_offset && record_start < page_end {
                retained_source_bytes = retained_source_bytes
                    .checked_add(u64::try_from(line.len()).map_err(corrupt)?)
                    .ok_or(StoreError::QueryLimit)?;
                if retained_source_bytes > retained_source_limit {
                    return Err(StoreError::QueryLimit);
                }
            }
            let page_item_start = items.len();
            let times = record.normalized_user_times().unwrap_or_default();
            let mut user_occurrence = 0_usize;
            for (index, item) in normalized.iter().enumerate() {
                let timestamp = if matches!(item, HistoryItem::User(_)) {
                    let timestamp = times.get(user_occurrence).cloned().flatten();
                    user_occurrence += 1;
                    timestamp
                } else {
                    None
                };
                if index + record_start >= item_offset && index + record_start < page_end {
                    items.push(item.clone());
                    user_times.push(timestamp);
                }
            }
            if let Some(encoded) = display_items
                .as_mut()
                .filter(|_| items.len() > page_item_start)
            {
                // Tool identities are loop-local. Pair only within this complete
                // stored loop, including counterparts outside the selected page.
                encoded.extend(
                    crate::read::encoded_display_items(
                        session_id,
                        record_start.max(item_offset),
                        &items[page_item_start..],
                        &user_times[page_item_start..],
                        &normalized,
                    )
                    .map_err(corrupt)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(corrupt)?,
                );
            }
            if record_end > item_offset && record_start < page_end {
                if turns.len() < MAX_READ_TURN_SUMMARIES {
                    turns.push(StoredTurnSummary {
                        item_start: record_start,
                        item_end: record_end,
                        loop_id: record.loop_id,
                        outcome: record.outcome.clone(),
                        usage: record.usage,
                        requests: record.requests,
                        tool_rounds: record.tool_rounds,
                        final_config_revision: record.final_config_revision,
                        completed_at: record.completed_at.clone(),
                    });
                } else {
                    turns_truncated = true;
                }
            }
            total_items = record_end;
            line_count += 1;
            hasher.update(&line);
            hasher.update(b"\n");
            complete_end = scan_target - remaining;
            if let Some(expected) = display.as_ref().and_then(|read| read.summary)
                && complete_end == expected.prefix_bytes
            {
                let actual = HistoryPrefix {
                    prefix_bytes: complete_end,
                    covered_loop_count: line_count as u64,
                    covered_item_count: total_items as u64,
                    last_loop_id: Some(record.loop_id),
                    sha256: digest_hex(hasher.clone()),
                };
                summary_anchor_valid = &actual == expected;
            }
            tokio::task::yield_now().await;
            if visible_item_count.is_some_and(|cap| cap > 0 && cap == total_items) {
                stopped_at_visible_cap = true;
                break;
            }
        }

        if !stopped_at_visible_cap && scan_target < target {
            return Err(StoreError::QueryLimit);
        }
        if remaining != 0 && !stopped_at_visible_cap {
            return Err(StoreError::HistoryChanged);
        }
        if captured_end.is_some() && complete_end != target {
            return Err(StoreError::HistoryChanged);
        }

        let revision = digest_hex(hasher);
        if expected_revision.is_some_and(|expected| expected != revision) {
            return Err(StoreError::HistoryChanged);
        }
        Ok(HistoryReadPage {
            captured_end: complete_end,
            revision,
            trailing_incomplete,
            total_items,
            items,
            user_times,
            turns,
            turns_truncated,
            covered_usage,
            display_items,
            summary_anchor_valid,
            scanned_bytes: scan_target - remaining,
            scanned_lines: line_count,
        })
    }

    pub(crate) async fn read_loop_record(
        &self,
        session_id: SessionId,
        loop_id: LoopId,
        limits: &HistoryScanLimits,
    ) -> Result<Option<StoredLoopRecord>, StoreError> {
        check_history_scan(limits)?;
        let indexed = self
            .history_indexes
            .lock()
            .unwrap()
            .get(&session_id)
            .map(|index| {
                index
                    .iter()
                    .position(|line| line.usage.last_loop_id == Some(loop_id))
                    .map(|position| {
                        (
                            position.checked_sub(1).map_or(0, |i| index[i].end),
                            index[position].end,
                        )
                    })
            });
        if let Some(range) = indexed {
            let Some((start, end)) = range else {
                return Ok(None);
            };
            if end - start > limits.max_bytes || limits.max_lines == 0 {
                return Err(StoreError::QueryLimit);
            }
            let (path, _) = self.require_history_file(session_id).await?;
            let mut file = File::open(path).await.map_err(unavailable)?;
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(unavailable)?;
            return match read_bounded_line(
                &mut BufReader::new(file),
                &mut (end - start),
                Some(limits),
            )
            .await?
            {
                BoundedLine::Complete(line) => {
                    serde_json::from_slice(&line).map(Some).map_err(corrupt)
                }
                _ => Err(StoreError::HistoryChanged),
            };
        }
        let (path, length) = self.require_history_file(session_id).await?;
        check_history_scan(limits)?;
        let file = File::open(path).await.map_err(unavailable)?;
        let mut reader = BufReader::new(file);
        let target = length;
        let scan_target = target.min(limits.max_bytes);
        let mut remaining = scan_target;
        let mut line_count = 0_usize;
        loop {
            if remaining == 0 {
                if scan_target < target {
                    return Err(StoreError::QueryLimit);
                }
                return Ok(None);
            }
            check_history_scan(limits)?;
            if line_count >= limits.max_lines {
                return Err(StoreError::QueryLimit);
            }
            let line = match read_bounded_line(&mut reader, &mut remaining, Some(limits)).await? {
                BoundedLine::Complete(line) => line,
                BoundedLine::Partial => {
                    if scan_target < target {
                        return Err(StoreError::QueryLimit);
                    }
                    return Ok(None);
                }
                BoundedLine::End => return Err(StoreError::HistoryChanged),
            };
            if line.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord = serde_json::from_slice(&line).map_err(corrupt)?;
            if record.loop_id == loop_id {
                return Ok(Some(record));
            }
            line_count += 1;
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn load_history(
        &self,
        session_id: SessionId,
    ) -> Result<
        (
            std::sync::Arc<[HistoryItem]>,
            std::collections::HashMap<(LoopId, usize), String>,
            Vec<HistoryLine>,
        ),
        StoreError,
    > {
        let (path, _) = self.require_history_file(session_id).await?;
        let file = File::open(&path).await.map_err(unavailable)?;
        let mut reader = BufReader::new(file);
        let mut items = Vec::new();
        let mut times = std::collections::HashMap::new();
        let mut buffer = Vec::new();
        let mut last_complete_offset = 0u64;
        let mut index = Vec::new();
        let mut saw_partial = false;
        loop {
            buffer.clear();
            // Bounded line assembly: once a line exceeds the ceiling, stop
            // retaining payload bytes but keep scanning to distinguish a
            // complete oversized line from an oversized final partial.
            let mut line_len = 0usize;
            let mut oversized = false;
            let mut is_complete = false;
            let reached_eof = loop {
                let chunk = reader.fill_buf().await.map_err(unavailable)?;
                if chunk.is_empty() {
                    break true;
                }
                if let Some(position) = chunk.iter().position(|byte| *byte == b'\n') {
                    if !oversized {
                        match line_len.checked_add(position) {
                            Some(total) if total <= MAX_LOOP_RECORD_BYTES => {
                                buffer.extend_from_slice(&chunk[..position]);
                                line_len = total;
                            }
                            Some(_) | None => oversized = true,
                        }
                    }
                    reader.consume(position + 1);
                    is_complete = true;
                    break false;
                }
                let length = chunk.len();
                if !oversized {
                    match line_len.checked_add(length) {
                        Some(total) if total <= MAX_LOOP_RECORD_BYTES => {
                            buffer.extend_from_slice(chunk);
                            line_len = total;
                        }
                        Some(_) | None => oversized = true,
                    }
                }
                reader.consume(length);
            };
            if reached_eof && line_len == 0 && !oversized {
                break;
            }
            let parsed = if oversized {
                Err(StoreError::Corrupt)
            } else {
                serde_json::from_slice::<StoredLoopRecord>(&buffer)
                    .map_err(corrupt)
                    .and_then(|record| {
                        let normalized = normalize_history(&record.items).map_err(corrupt)?;
                        Ok((record, normalized))
                    })
            };
            let (record, normalized) = match parsed {
                Ok(parsed) => parsed,
                Err(_) if !is_complete => {
                    saw_partial = true;
                    break;
                }
                Err(error) => return Err(error),
            };
            if !is_complete {
                let mut file = OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .await
                    .map_err(unavailable)?;
                file.write_all(b"\n").await.map_err(unavailable)?;
                file.flush().await.map_err(unavailable)?;
                file.sync_data().await.map_err(unavailable)?;
                tracing::warn!(session_id = %session_id, "history final newline repaired");
            }
            let user_count = record
                .items
                .iter()
                .filter(|item| matches!(item, HistoryItem::User(_)))
                .count();
            let user_times = record
                .user_times
                .as_deref()
                .filter(|times| times.len() <= user_count)
                .unwrap_or(&[]);
            let mut user_occurrence = 0usize;
            for item in &record.items {
                if let HistoryItem::User(_) = item {
                    if let Some(Some(time)) = user_times.get(user_occurrence) {
                        times.insert((record.loop_id, user_occurrence), time.clone());
                    }
                    user_occurrence += 1;
                }
            }
            last_complete_offset += buffer.len() as u64 + 1;
            buffer.push(b'\n');
            index_line(&mut index, &buffer, &record, normalized.len());
            items.extend(normalized.iter().cloned());
            if !is_complete {
                break;
            }
        }
        if saw_partial {
            tail_repair(&path, last_complete_offset).await?;
            tracing::warn!(session_id = %session_id, "history tail repaired");
        }
        let history = items.into();
        Ok((history, times, index))
    }
}

struct ColdDisplayScan<'a> {
    summary: Option<&'a HistoryPrefix>,
}

pub(super) async fn rollback_append(path: &Path, previous_len: u64) -> AppendError {
    // A Windows append handle lacks the write-data right that truncation needs.
    match OpenOptions::new().write(true).open(path).await {
        Ok(file) => truncate_append(&file, previous_len).await,
        Err(error) => {
            unavailable(error);
            AppendError::Unknown
        }
    }
}

pub(super) async fn truncate_append(file: &File, previous_len: u64) -> AppendError {
    match async {
        file.set_len(previous_len).await?;
        file.sync_data().await
    }
    .await
    {
        Ok(()) => AppendError::NotWritten(StoreError::Unavailable),
        Err(error) => {
            unavailable(error);
            AppendError::Unknown
        }
    }
}

enum BoundedLine {
    End,
    Complete(Vec<u8>),
    Partial,
}

async fn read_bounded_line<R>(
    reader: &mut R,
    remaining: &mut u64,
    limits: Option<&HistoryScanLimits>,
) -> Result<BoundedLine, StoreError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        if *remaining == 0 {
            return if line.is_empty() {
                Ok(BoundedLine::End)
            } else {
                Ok(BoundedLine::Partial)
            };
        }
        if let Some(limits) = limits {
            check_history_scan(limits)?;
        }
        let chunk = reader.fill_buf().await.map_err(unavailable)?;
        if chunk.is_empty() {
            return Ok(BoundedLine::End);
        }
        let take = chunk
            .len()
            .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
        let chunk = &chunk[..take];
        if let Some(position) = chunk.iter().position(|byte| *byte == b'\n') {
            if !oversized {
                match line.len().checked_add(position) {
                    Some(length) if length <= MAX_LOOP_RECORD_BYTES => {
                        line.extend_from_slice(&chunk[..position]);
                    }
                    Some(_) | None => oversized = true,
                }
            }
            reader.consume(position + 1);
            *remaining -= u64::try_from(position + 1).map_err(corrupt)?;
            if oversized {
                return Err(StoreError::Corrupt);
            }
            return Ok(BoundedLine::Complete(line));
        }
        if !oversized {
            match line.len().checked_add(chunk.len()) {
                Some(length) if length <= MAX_LOOP_RECORD_BYTES => {
                    line.extend_from_slice(chunk);
                }
                Some(_) | None => oversized = true,
            }
        }
        reader.consume(take);
        *remaining -= u64::try_from(take).map_err(corrupt)?;
        tokio::task::yield_now().await;
    }
}

fn check_history_scan(limits: &HistoryScanLimits) -> Result<(), StoreError> {
    if limits.cancellation.is_cancelled() || Instant::now() >= limits.deadline {
        Err(StoreError::QueryLimit)
    } else {
        Ok(())
    }
}

fn finish_empty_history_page(
    captured_end: u64,
    expected_revision: Option<&str>,
) -> Result<HistoryReadPage, StoreError> {
    let revision = digest_hex(Sha256::new());
    if expected_revision.is_some_and(|expected| expected != revision) {
        return Err(StoreError::HistoryChanged);
    }
    Ok(HistoryReadPage {
        captured_end,
        revision,
        summary_anchor_valid: true,
        ..HistoryReadPage::default()
    })
}

async fn scan_history_prefix(
    file: File,
    prefix_bytes: u64,
    expected_history: &[HistoryItem],
    max_prefix_items: Option<usize>,
) -> Result<Option<CompactionAnchors>, StoreError> {
    let mut reader = BufReader::new(file);
    let mut remaining = prefix_bytes;
    let mut hasher = Sha256::new();
    let mut covered_loop_count = 0_u64;
    let mut covered_item_count = 0_u64;
    let mut last_loop_id = None;
    let mut prefix = None;
    // Bind the raw scan to the already-loaded sanitized history without
    // constructing a second history-sized collection.
    let mut expected_item_index = 0_usize;

    while remaining > 0 {
        let BoundedLine::Complete(line) =
            read_bounded_line(&mut reader, &mut remaining, None).await?
        else {
            return Ok(None);
        };
        hasher.update(&line);
        hasher.update(b"\n");
        if line.is_empty() {
            return Err(StoreError::Corrupt);
        }
        let record: StoredLoopRecord = serde_json::from_slice(&line).map_err(corrupt)?;
        let normalized = normalize_history(&record.items).map_err(corrupt)?;
        for item in normalized.iter() {
            let Some(expected) = expected_history.get(expected_item_index) else {
                return Ok(None);
            };
            if item != expected {
                return Ok(None);
            }
            expected_item_index = expected_item_index
                .checked_add(1)
                .ok_or(StoreError::Corrupt)?;
        }
        covered_loop_count = covered_loop_count
            .checked_add(1)
            .ok_or(StoreError::Corrupt)?;
        covered_item_count = covered_item_count
            .checked_add(u64::try_from(normalized.len()).map_err(corrupt)?)
            .ok_or(StoreError::Corrupt)?;
        last_loop_id = Some(record.loop_id);
        if max_prefix_items.is_some_and(|maximum| expected_item_index <= maximum) {
            prefix = Some(HistoryPrefix {
                prefix_bytes: prefix_bytes - remaining,
                covered_loop_count,
                covered_item_count,
                last_loop_id,
                sha256: digest_hex(hasher.clone()),
            });
        }
        tokio::task::yield_now().await;
    }
    Ok(Some(CompactionAnchors {
        full: HistoryPrefix {
            prefix_bytes,
            covered_loop_count,
            covered_item_count,
            last_loop_id,
            sha256: digest_hex(hasher),
        },
        prefix,
    }))
}

async fn tail_repair(path: &Path, complete_offset: u64) -> Result<(), StoreError> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(unavailable)?;
    file.set_len(complete_offset).await.map_err(unavailable)?;
    file.sync_data().await.map_err(unavailable)
}
