//! JSONL history read, write, scan, and anchor implementation.
use super::*;

impl Store {
    /// Scans one raw history prefix without invoking the normal tail repair.
    /// The result exists only when the requested byte offset ends immediately
    /// after a complete StoredLoopRecord line.
    pub(crate) async fn read_history_prefix(
        &self,
        session_id: SessionId,
        prefix_bytes: u64,
        expected_history: &[HistoryItem],
    ) -> Result<Option<HistoryPrefix>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing | PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if prefix_bytes > metadata.len() {
            return Ok(None);
        }
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        scan_history_prefix(file, prefix_bytes, expected_history).await
    }

    /// Captures the complete raw history anchor without tail repair. The
    /// loaded sanitized history must account for every stored item before an
    /// anchor can be used for a derived snapshot write.
    pub(crate) async fn capture_history_anchor(
        &self,
        session_id: SessionId,
        expected_history: &[HistoryItem],
    ) -> Result<Option<HistoryPrefix>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing | PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let file = File::open(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let length = metadata.len();
        let Some(anchor) = scan_history_prefix(file, length, expected_history).await? else {
            return Ok(None);
        };
        let final_length = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
            .len();
        if final_length != length {
            return Ok(None);
        }
        if anchor.covered_item_count != u64::try_from(expected_history.len()).unwrap_or(u64::MAX) {
            return Ok(None);
        }
        Ok(Some(anchor))
    }

    /// Appends one completed loop as a single JSON line. On success the file
    /// content is complete; callers merge the sanitized items into memory.
    pub(crate) async fn append_loop(
        &self,
        session_id: SessionId,
        record: &StoredLoopRecord,
    ) -> Result<(), StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        #[cfg(test)]
        if should_fail_append(session_id) {
            return Err(StoreError::Unavailable);
        }
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::RegularFile => {}
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let mut bytes = if record.user_times_are_valid() {
            serde_json::to_vec(record)
        } else {
            let mut normalized = record.clone();
            normalized.user_times = record.normalized_user_times();
            serde_json::to_vec(&normalized)
        }
        .map_err(|_| StoreError::Corrupt)?;
        if bytes.len() > MAX_LOOP_RECORD_BYTES && record.user_times.is_some() {
            // Presentation metadata must not make an otherwise storable core
            // loop record cross the single-line limit.
            let mut without_metadata = record.clone();
            without_metadata.user_times = None;
            bytes = serde_json::to_vec(&without_metadata).map_err(|_| StoreError::Corrupt)?;
        }
        if bytes.len() > MAX_LOOP_RECORD_BYTES {
            return Err(StoreError::RecordTooLarge);
        }
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        file.write_all(&bytes)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        file.flush().await.map_err(|_| StoreError::Unavailable)?;
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
        expected_history: Option<&[HistoryItem]>,
        limits: &HistoryScanLimits,
    ) -> Result<HistoryReadPage, StoreError> {
        if captured_end.is_some() != expected_revision.is_some() {
            return Err(StoreError::InvalidArguments);
        }
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let target = captured_end.unwrap_or(metadata.len());
        if target > metadata.len() {
            return Err(StoreError::HistoryChanged);
        }
        if let Some(expected) = expected_revision {
            if !valid_sha256(expected) {
                return Err(StoreError::InvalidArguments);
            }
        }
        check_history_scan(limits)?;
        if visible_item_count == Some(0) {
            if captured_end.is_some_and(|end| end != 0) {
                return Err(StoreError::HistoryChanged);
            }
            return finish_empty_history_page(0, expected_revision);
        }

        let file = File::open(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut reader = BufReader::new(file);
        let scan_target = target.min(limits.max_bytes);
        let mut remaining = scan_target;
        let mut complete_end = 0_u64;
        let mut hasher = Sha256::new();
        let mut line_count = 0_usize;
        let mut total_items = 0_usize;
        let mut items = Vec::new();
        let mut user_times = Vec::new();
        let page_end = item_offset.saturating_add(item_limit);
        let mut turns = Vec::new();
        let mut turns_truncated = false;
        let mut expected_index = 0_usize;
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
            let line = match read_bounded_line(&mut reader, &mut remaining, limits).await? {
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
            let record: StoredLoopRecord =
                serde_json::from_slice(&line).map_err(|_| StoreError::Corrupt)?;
            let normalized = sanitize_history(&record.items).map_err(|_| StoreError::Corrupt)?;
            let record_start = total_items;
            let record_end = record_start
                .checked_add(normalized.len())
                .ok_or(StoreError::Corrupt)?;
            if let Some(expected) = expected_history {
                let compared = visible_item_count
                    .map(|cap| normalized.len().min(cap.saturating_sub(record_start)))
                    .unwrap_or(normalized.len());
                for item in normalized.iter().take(compared) {
                    let Some(expected_item) = expected.get(expected_index) else {
                        return Err(StoreError::HistoryChanged);
                    };
                    if item != expected_item {
                        return Err(StoreError::HistoryChanged);
                    }
                    expected_index = expected_index.checked_add(1).ok_or(StoreError::Corrupt)?;
                }
            }
            if visible_item_count.is_some_and(|cap| record_end > cap) {
                return Err(StoreError::HistoryChanged);
            }
            if record_end > item_offset && record_start < page_end {
                retained_source_bytes = retained_source_bytes
                    .checked_add(u64::try_from(line.len()).map_err(|_| StoreError::Corrupt)?)
                    .ok_or(StoreError::QueryLimit)?;
                if retained_source_bytes > retained_source_limit {
                    return Err(StoreError::QueryLimit);
                }
            }
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
        if let Some(expected) = expected_history {
            if expected_index != total_items || total_items > expected.len() {
                return Err(StoreError::HistoryChanged);
            }
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
        })
    }

    pub(crate) async fn read_loop_record(
        &self,
        session_id: SessionId,
        loop_id: LoopId,
        limits: &HistoryScanLimits,
    ) -> Result<Option<StoredLoopRecord>, StoreError> {
        let directory = self.require_session_directory(session_id).await?;
        let path = directory.join(HISTORY_FILE);
        match path_state(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        check_history_scan(limits)?;
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut reader = BufReader::new(file);
        let target = metadata.len();
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
            let line = match read_bounded_line(&mut reader, &mut remaining, limits).await? {
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
            let record: StoredLoopRecord =
                serde_json::from_slice(&line).map_err(|_| StoreError::Corrupt)?;
            if record.loop_id == loop_id {
                return Ok(Some(record));
            }
            line_count += 1;
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn load_history(
        &self,
        path: &Path,
        session_id: SessionId,
    ) -> Result<
        (
            std::sync::Arc<[HistoryItem]>,
            std::collections::HashMap<(LoopId, usize), String>,
        ),
        StoreError,
    > {
        match path_state(path)
            .await
            .map_err(|_| StoreError::Unavailable)?
        {
            PathState::RegularFile => {}
            // A valid v0.3 record always pairs `session.json` with
            // `history.jsonl`; a missing history for a present record is
            // corrupt data, never an empty conversation.
            PathState::Missing => return Err(StoreError::Corrupt),
            PathState::Directory | PathState::Symlink | PathState::Other => {
                return Err(StoreError::Corrupt);
            }
        }
        let file = File::open(path)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut reader = BufReader::new(file);
        let mut items = Vec::new();
        let mut times = std::collections::HashMap::new();
        let mut buffer = Vec::new();
        let mut last_complete_offset = 0u64;
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
                let chunk = reader
                    .fill_buf()
                    .await
                    .map_err(|_| StoreError::Unavailable)?;
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
            if !is_complete {
                saw_partial = true;
                break;
            }
            if oversized {
                return Err(StoreError::Corrupt);
            }
            if buffer.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord =
                serde_json::from_slice(&buffer).map_err(|_| StoreError::Corrupt)?;
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
            items.extend(record.items);
            last_complete_offset = reader
                .stream_position()
                .await
                .map_err(|_| StoreError::Unavailable)?;
        }
        if saw_partial {
            // The only allowed repair: truncate back to the last complete
            // line. Works for a first-segment partial too (offset 0).
            let had_partial = tail_repair(path, last_complete_offset).await?;
            if had_partial {
                tracing::warn!(session_id = %session_id, "history tail repaired");
            }
        }
        let history = sanitize_history(&items).map_err(|_| StoreError::Corrupt)?;
        Ok((history, times))
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
    limits: &HistoryScanLimits,
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
        check_history_scan(limits)?;
        let chunk = reader
            .fill_buf()
            .await
            .map_err(|_| StoreError::Unavailable)?;
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
            *remaining -= u64::try_from(position + 1).map_err(|_| StoreError::Corrupt)?;
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
        *remaining -= u64::try_from(take).map_err(|_| StoreError::Corrupt)?;
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
        trailing_incomplete: false,
        total_items: 0,
        items: Vec::new(),
        user_times: Vec::new(),
        turns: Vec::new(),
        turns_truncated: false,
    })
}

async fn scan_history_prefix(
    file: File,
    prefix_bytes: u64,
    expected_history: &[HistoryItem],
) -> Result<Option<HistoryPrefix>, StoreError> {
    let mut reader = BufReader::new(file);
    let mut remaining = prefix_bytes;
    let mut hasher = Sha256::new();
    let mut line = Vec::new();
    let mut covered_loop_count = 0_u64;
    let mut covered_item_count = 0_u64;
    let mut last_loop_id = None;
    // Bind the raw scan to the already-loaded sanitized history without
    // constructing a second history-sized collection.
    let mut expected_item_index = 0_usize;

    while remaining > 0 {
        let chunk = reader
            .fill_buf()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if chunk.is_empty() {
            return Ok(None);
        }
        let take = chunk
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let bytes = &chunk[..take];
        hasher.update(bytes);

        let mut segment_start = 0;
        for (index, byte) in bytes.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            let segment = &bytes[segment_start..index];
            let line_length = line
                .len()
                .checked_add(segment.len())
                .ok_or(StoreError::Corrupt)?;
            if line_length > MAX_LOOP_RECORD_BYTES {
                return Err(StoreError::Corrupt);
            }
            line.extend_from_slice(segment);
            if line.is_empty() {
                return Err(StoreError::Corrupt);
            }
            let record: StoredLoopRecord =
                serde_json::from_slice(&line).map_err(|_| StoreError::Corrupt)?;
            let normalized = sanitize_history(&record.items).map_err(|_| StoreError::Corrupt)?;
            for item in normalized.iter() {
                let Some(expected) = expected_history.get(expected_item_index) else {
                    return Ok(None);
                };
                let actual_bytes = serde_json::to_vec(item).map_err(|_| StoreError::Corrupt)?;
                let expected_bytes =
                    serde_json::to_vec(expected).map_err(|_| StoreError::Corrupt)?;
                if actual_bytes != expected_bytes {
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
                .checked_add(u64::try_from(normalized.len()).map_err(|_| StoreError::Corrupt)?)
                .ok_or(StoreError::Corrupt)?;
            last_loop_id = Some(record.loop_id);
            line.clear();
            segment_start = index + 1;
        }
        if segment_start < bytes.len() {
            let segment = &bytes[segment_start..];
            let line_length = line
                .len()
                .checked_add(segment.len())
                .ok_or(StoreError::Corrupt)?;
            if line_length > MAX_LOOP_RECORD_BYTES {
                return Err(StoreError::Corrupt);
            }
            line.extend_from_slice(segment);
        }
        reader.consume(take);
        remaining -= u64::try_from(take).map_err(|_| StoreError::Corrupt)?;
    }

    if !line.is_empty() {
        return Ok(None);
    }
    let digest = hasher.finalize();
    let mut sha256 = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(sha256, "{byte:02x}").expect("writing digest cannot fail");
    }
    Ok(Some(HistoryPrefix {
        prefix_bytes,
        covered_loop_count,
        covered_item_count,
        last_loop_id,
        sha256,
    }))
}

/// If the file ends without a newline, truncate it back to the last complete
/// line. Returns whether a repair happened.
async fn tail_repair(path: &Path, complete_offset: u64) -> Result<bool, StoreError> {
    let metadata = fs::metadata(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    if metadata.len() == complete_offset {
        return Ok(false);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    file.set_len(complete_offset)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    file.flush().await.map_err(|_| StoreError::Unavailable)?;
    Ok(true)
}
