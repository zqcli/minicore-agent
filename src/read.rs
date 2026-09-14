use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::Usage;

use crate::agent::SessionInfo;
use crate::error::AgentError;
use crate::event::LoopOutcomeView;
use crate::history::sanitize_history;
use crate::ids::SessionId;
use crate::sessions::{Session, TurnPersistence, TurnRef};
use crate::store::{HistoryScanLimits, Store, StoredLoopRecord, StoredTurnSummary};

pub(crate) const DEFAULT_READ_MAX_BYTES: usize = 256 * 1024;
pub(crate) const MAX_READ_MAX_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_READ_ITEMS: usize = 100;
pub(crate) const READ_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const MAX_HISTORY_SCAN_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_HISTORY_SCAN_LINES: usize = 100_000;
const JSON_ENCODING: &str = "utf8_json";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadCursor {
    pub item: usize,
    #[serde(default)]
    pub offset: usize,
}

impl ReadCursor {
    pub const fn start() -> Self {
        Self { item: 0, offset: 0 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSession {
    pub session_id: SessionId,
    #[serde(default)]
    pub cursor: Option<ReadCursor>,
    #[serde(default = "default_read_limit")]
    pub limit: usize,
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub captured_end: Option<u64>,
    #[serde(default)]
    pub history_revision: Option<String>,
}

impl ReadSession {
    pub fn validate(&self) -> Result<(), AgentError> {
        if !(1..=MAX_READ_ITEMS).contains(&self.limit) {
            return Err(AgentError::InvalidArguments);
        }
        validate_max_bytes(self.max_bytes)?;
        if self.captured_end.is_some() != self.history_revision.is_some()
            || self
                .history_revision
                .as_deref()
                .is_some_and(|value| !valid_revision(value))
            || (self
                .cursor
                .is_some_and(|cursor| cursor != ReadCursor::start())
                && self.captured_end.is_none())
        {
            return Err(AgentError::InvalidArguments);
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ReadItemChunk {
    pub index: usize,
    pub offset: usize,
    pub total_bytes: usize,
    pub encoding: &'static str,
    pub data: String,
    pub complete: bool,
}

impl fmt::Debug for ReadItemChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadItemChunk")
            .field("index", &self.index)
            .field("offset", &self.offset)
            .field("total_bytes", &self.total_bytes)
            .field("encoding", &self.encoding)
            .field("data_bytes", &self.data.len())
            .field("complete", &self.complete)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadTurnSummary {
    pub loop_id: LoopId,
    pub outcome: LoopOutcomeView,
    pub usage: Usage,
    pub requests: u32,
    pub tool_rounds: u16,
    pub final_config_revision: minicore_runtime::execution::ConfigRevision,
    pub completed_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadSessionResult {
    pub session: SessionInfo,
    pub items: Vec<ReadItemChunk>,
    pub next_cursor: Option<ReadCursor>,
    pub total: usize,
    pub records: Vec<ReadTurnSummary>,
    pub records_truncated: bool,
    pub history_revision: String,
    pub captured_end: u64,
    pub trailing_incomplete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnResultAvailability {
    Pending,
    Live,
    Stored,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnResultRequest {
    pub turn: TurnRef,
    #[serde(default)]
    pub cursor: Option<ReadCursor>,
    #[serde(default = "default_read_limit")]
    pub limit: usize,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

impl TurnResultRequest {
    pub fn validate(&self) -> Result<(), AgentError> {
        if !(1..=MAX_READ_ITEMS).contains(&self.limit) {
            return Err(AgentError::InvalidArguments);
        }
        validate_max_bytes(self.max_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TurnResultPage {
    pub turn: TurnRef,
    pub availability: TurnResultAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<LoopOutcomeView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<TurnPersistence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_rounds: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_config_revision: Option<minicore_runtime::execution::ConfigRevision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    pub items: Vec<ReadItemChunk>,
    pub next_cursor: Option<ReadCursor>,
    pub total: usize,
}

pub(crate) async fn read_session(
    store: Store,
    loaded: Option<Session>,
    request: ReadSession,
    cancellation: CancellationToken,
) -> Result<ReadSessionResult, AgentError> {
    request.validate()?;
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    let cursor = request.cursor.unwrap_or_else(ReadCursor::start);
    let limits = scan_limits(cancellation.clone());

    if cancellation.is_cancelled() {
        return Err(AgentError::QueryLimit);
    }

    let (info, page, source_items, timestamps) = if let Some(session) = loaded {
        let snapshot = session.read_snapshot();
        let info = snapshot.info;
        let history = snapshot.history;
        let user_times = snapshot.user_times;
        let mut page = store
            .read_history_page(
                request.session_id,
                cursor.item,
                request.limit,
                Some(history.len()),
                request.captured_end,
                request.history_revision.as_deref(),
                Some(history.as_ref()),
                &limits,
            )
            .await
            .map_err(map_read_store_error)?;
        let visible = page.total_items.min(history.len());
        validate_cursor(cursor, visible)?;
        let end = cursor.item.saturating_add(request.limit).min(visible);
        let timestamps = timestamps_for_range(history.as_ref(), &user_times, cursor.item, end);
        if page.items.len() != end.saturating_sub(cursor.item) {
            return Err(AgentError::InvalidState);
        }
        let source_items = std::mem::take(&mut page.items);
        drop(std::mem::take(&mut page.user_times));
        (info, page, source_items, timestamps)
    } else {
        let record = store
            .load_record(request.session_id)
            .await
            .map_err(map_read_store_error)?;
        let info = SessionInfo::from_record(&record, false);
        let mut page = store
            .read_history_page(
                request.session_id,
                cursor.item,
                request.limit,
                None,
                request.captured_end,
                request.history_revision.as_deref(),
                None,
                &limits,
            )
            .await
            .map_err(map_read_store_error)?;
        validate_cursor(cursor, page.total_items)?;
        let items = std::mem::take(&mut page.items);
        let timestamps = std::mem::take(&mut page.user_times);
        (info, page, items, timestamps)
    };

    let encoded = encoded_items(cursor.item, &source_items, &timestamps)?;
    let stored_records = page.turns;
    let total = page.total_items;
    let revision = page.revision.clone();
    let captured_end = page.captured_end;
    let trailing_incomplete = page.trailing_incomplete;
    let records_truncated = page.turns_truncated;
    let session = info.clone();
    let (items, next_cursor) = pack_items(
        encoded,
        cursor,
        total,
        max_bytes,
        &limits.cancellation,
        limits.deadline,
        |items, next_cursor| {
            let records = records_for_chunks(&stored_records, items);
            serde_json::to_vec(&ReadSessionResult {
                session: session.clone(),
                items: items.to_vec(),
                next_cursor,
                total,
                records: records.clone(),
                records_truncated,
                history_revision: revision.clone(),
                captured_end,
                trailing_incomplete,
            })
            .map_err(|_| AgentError::RpcSerialization)
        },
    )
    .await?;
    let records = records_for_chunks(&stored_records, &items);

    Ok(ReadSessionResult {
        session: info,
        items,
        next_cursor,
        total,
        records,
        records_truncated,
        history_revision: page.revision,
        captured_end: page.captured_end,
        trailing_incomplete: page.trailing_incomplete,
    })
}

pub(crate) async fn turn_result(
    store: Store,
    loaded: Option<Session>,
    request: TurnResultRequest,
    cancellation: CancellationToken,
) -> Result<TurnResultPage, AgentError> {
    request.validate()?;
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    let cursor = request.cursor.unwrap_or_else(ReadCursor::start);
    let limits = scan_limits(cancellation.clone());

    if cancellation.is_cancelled() {
        return Err(AgentError::QueryLimit);
    }

    if let Some(session) = loaded {
        match session.turn_result_snapshot(request.turn)? {
            Some(None) => {
                validate_cursor(cursor, 0)?;
                return pending_turn_page(request.turn, max_bytes);
            }
            Some(Some(result)) => {
                return live_turn_page(
                    request.turn,
                    result,
                    cursor,
                    request.limit,
                    max_bytes,
                    &limits.cancellation,
                    limits.deadline,
                )
                .await;
            }
            None => {}
        }
    }

    let record = store
        .read_loop_record(request.turn.session_id, request.turn.loop_id, &limits)
        .await
        .map_err(map_read_store_error)?
        .ok_or(AgentError::TurnNotFound)?;
    stored_turn_page(
        request.turn,
        record,
        cursor,
        request.limit,
        max_bytes,
        &limits.cancellation,
        limits.deadline,
    )
    .await
}

fn pending_turn_page(turn: TurnRef, max_bytes: usize) -> Result<TurnResultPage, AgentError> {
    let page = TurnResultPage {
        turn,
        availability: TurnResultAvailability::Pending,
        outcome: None,
        persistence: None,
        usage: None,
        requests: None,
        tool_rounds: None,
        final_config_revision: None,
        completed_at: None,
        items: Vec::new(),
        next_cursor: None,
        total: 0,
    };
    if encoded_len(&page)? > max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    Ok(page)
}

async fn live_turn_page(
    turn: TurnRef,
    result: Arc<crate::sessions::TurnResult>,
    cursor: ReadCursor,
    limit: usize,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<TurnResultPage, AgentError> {
    let sanitized =
        sanitized_live_history(result.report.appended.as_ref(), cancellation, deadline).await?;
    validate_cursor(cursor, sanitized.len())?;
    let end = cursor.item.saturating_add(limit).min(sanitized.len());
    let timestamps = vec![None; end.saturating_sub(cursor.item)];
    let encoded = encoded_items(cursor.item, &sanitized[cursor.item..end], &timestamps)?;
    let outcome = LoopOutcomeView::from_report(&result.report);
    let persistence = result.persistence;
    let usage = result.report.usage;
    let requests = result.report.requests;
    let tool_rounds = result.report.tool_rounds;
    let final_config_revision = result.report.final_config_revision;
    let base = TurnResultPage {
        turn,
        availability: TurnResultAvailability::Live,
        outcome: Some(outcome),
        persistence: Some(persistence),
        usage: Some(usage),
        requests: Some(requests),
        tool_rounds: Some(tool_rounds),
        final_config_revision: Some(final_config_revision),
        completed_at: None,
        items: Vec::new(),
        next_cursor: None,
        total: sanitized.len(),
    };
    let total = sanitized.len();
    let (items, next_cursor) = pack_items(
        encoded,
        cursor,
        total,
        max_bytes,
        cancellation,
        deadline,
        |items, next_cursor| {
            serde_json::to_vec(&TurnResultPage {
                items: items.to_vec(),
                next_cursor,
                ..base.clone()
            })
            .map_err(|_| AgentError::RpcSerialization)
        },
    )
    .await?;
    Ok(TurnResultPage {
        items,
        next_cursor,
        ..base
    })
}

async fn stored_turn_page(
    turn: TurnRef,
    record: StoredLoopRecord,
    cursor: ReadCursor,
    limit: usize,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<TurnResultPage, AgentError> {
    let sanitized = sanitized_record_items(&record, cancellation, deadline).await?;
    validate_cursor(cursor, sanitized.len())?;
    let total = sanitized.len();
    let end = cursor.item.saturating_add(limit).min(total);
    let timestamps = vec![None; end.saturating_sub(cursor.item)];
    let encoded = encoded_items(cursor.item, &sanitized[cursor.item..end], &timestamps)?;
    let base = TurnResultPage {
        turn,
        availability: TurnResultAvailability::Stored,
        outcome: Some(LoopOutcomeView::from_stored(&record.outcome)),
        persistence: Some(TurnPersistence::Persisted),
        usage: Some(record.usage),
        requests: Some(record.requests),
        tool_rounds: Some(record.tool_rounds),
        final_config_revision: Some(record.final_config_revision),
        completed_at: Some(record.completed_at),
        items: Vec::new(),
        next_cursor: None,
        total,
    };
    let (items, next_cursor) = pack_items(
        encoded,
        cursor,
        total,
        max_bytes,
        cancellation,
        deadline,
        |items, next_cursor| {
            serde_json::to_vec(&TurnResultPage {
                items: items.to_vec(),
                next_cursor,
                ..base.clone()
            })
            .map_err(|_| AgentError::RpcSerialization)
        },
    )
    .await?;
    Ok(TurnResultPage {
        items,
        next_cursor,
        ..base
    })
}

fn encoded_items<'a>(
    start: usize,
    items: &'a [HistoryItem],
    timestamps: &'a [Option<String>],
) -> Result<impl Iterator<Item = Result<(usize, String), AgentError>> + 'a, AgentError> {
    if timestamps.len() != items.len() {
        return Err(AgentError::Internal);
    }
    Ok(items.iter().enumerate().map(move |(offset, item)| {
        let timestamp = timestamps[offset].as_deref();
        let envelope = ReadItemEnvelope { item, timestamp };
        let bytes = serde_json::to_vec(&envelope).map_err(|_| AgentError::RpcSerialization)?;
        let data = String::from_utf8(bytes).map_err(|_| AgentError::Internal)?;
        Ok((start.saturating_add(offset), data))
    }))
}

#[derive(Serialize)]
struct ReadItemEnvelope<'a> {
    item: &'a HistoryItem,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<&'a str>,
}

async fn pack_items<I, F>(
    encoded: I,
    start: ReadCursor,
    total: usize,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
    mut build: F,
) -> Result<(Vec<ReadItemChunk>, Option<ReadCursor>), AgentError>
where
    I: IntoIterator<Item = Result<(usize, String), AgentError>>,
    F: FnMut(&[ReadItemChunk], Option<ReadCursor>) -> Result<Vec<u8>, AgentError>,
{
    let mut chunks = Vec::new();
    let mut position = start;
    let mut encoded = encoded.into_iter();

    loop {
        check_read_budget(cancellation, deadline)?;
        let Some(encoded_item) = encoded.next() else {
            break;
        };
        let (index, json) = encoded_item?;
        check_read_budget(cancellation, deadline)?;
        if index < position.item {
            continue;
        }
        let offset = if index == start.item { start.offset } else { 0 };
        if offset > json.len() || !json.is_char_boundary(offset) {
            return Err(AgentError::InvalidArguments);
        }
        let remaining = &json[offset..];
        if remaining.is_empty() {
            position = next_position(index, total).unwrap_or(ReadCursor {
                item: total,
                offset: 0,
            });
            continue;
        }

        let complete_cursor = next_position(index, total);
        if remaining.len() <= max_bytes {
            let full = make_chunk(index, offset, &json, remaining.len());
            if candidate_fits(&chunks, full, complete_cursor, max_bytes, &mut build)? {
                chunks.push(make_chunk(index, offset, &json, remaining.len()));
                position = complete_cursor.unwrap_or(ReadCursor {
                    item: total,
                    offset: 0,
                });
                tokio::task::yield_now().await;
                continue;
            }
        }

        let length = largest_fitting_prefix(
            &chunks,
            index,
            offset,
            &json,
            total,
            max_bytes,
            cancellation,
            deadline,
            &mut build,
        )
        .await?;
        if length == 0 {
            if chunks.is_empty() {
                return Err(AgentError::InvalidArguments);
            }
            break;
        }
        let complete = length == remaining.len();
        let next_cursor = if complete {
            complete_cursor
        } else {
            Some(ReadCursor {
                item: index,
                offset: offset.saturating_add(length),
            })
        };
        chunks.push(make_chunk(index, offset, &json, length));
        position = next_cursor.unwrap_or(ReadCursor {
            item: total,
            offset: 0,
        });
        break;
    }

    let next_cursor = (position.item < total).then_some(position);
    check_read_budget(cancellation, deadline)?;
    let encoded_page = build(&chunks, next_cursor)?;
    check_read_budget(cancellation, deadline)?;
    if encoded_page.len() > max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    Ok((chunks, next_cursor))
}

fn candidate_fits<F>(
    existing: &[ReadItemChunk],
    candidate: ReadItemChunk,
    next_cursor: Option<ReadCursor>,
    max_bytes: usize,
    build: &mut F,
) -> Result<bool, AgentError>
where
    F: FnMut(&[ReadItemChunk], Option<ReadCursor>) -> Result<Vec<u8>, AgentError>,
{
    let mut items = existing.to_vec();
    items.push(candidate);
    Ok(build(&items, next_cursor)?.len() <= max_bytes)
}

#[allow(clippy::too_many_arguments)]
async fn largest_fitting_prefix<F>(
    existing: &[ReadItemChunk],
    index: usize,
    offset: usize,
    json: &str,
    total: usize,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
    build: &mut F,
) -> Result<usize, AgentError>
where
    F: FnMut(&[ReadItemChunk], Option<ReadCursor>) -> Result<Vec<u8>, AgentError>,
{
    let remaining = &json[offset..];
    let mut low = 0_usize;
    // Serialized JSON data is never smaller than its UTF-8 fragment, so a
    // fragment longer than the whole-page budget cannot be a fitting candidate.
    // Keep the full length when it is possible so the final complete cursor is
    // still considered.
    let mut high = remaining.len().min(max_bytes);
    while low < high {
        check_read_budget(cancellation, deadline)?;
        let mut middle = low + (high - low).div_ceil(2);
        middle = previous_boundary(remaining, middle);
        if middle == low {
            let next = next_boundary(remaining, low);
            if next > high {
                high = low;
                continue;
            }
            middle = next;
        }
        let next_cursor = if middle == remaining.len() {
            next_position(index, total)
        } else {
            Some(ReadCursor {
                item: index,
                offset: offset.saturating_add(middle),
            })
        };
        let candidate = make_chunk(index, offset, json, middle);
        if candidate_fits(existing, candidate, next_cursor, max_bytes, build)? {
            low = middle;
        } else {
            high = middle.saturating_sub(1);
        }
        tokio::task::yield_now().await;
    }
    Ok(previous_boundary(remaining, low))
}

fn make_chunk(index: usize, offset: usize, json: &str, length: usize) -> ReadItemChunk {
    ReadItemChunk {
        index,
        offset,
        total_bytes: json.len(),
        encoding: JSON_ENCODING,
        data: json[offset..offset.saturating_add(length)].to_owned(),
        complete: offset.saturating_add(length) == json.len(),
    }
}

fn next_position(index: usize, total: usize) -> Option<ReadCursor> {
    let next = index.saturating_add(1);
    (next < total).then_some(ReadCursor {
        item: next,
        offset: 0,
    })
}

fn previous_boundary(value: &str, mut offset: usize) -> usize {
    offset = offset.min(value.len());
    while offset > 0 && !value.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn next_boundary(value: &str, offset: usize) -> usize {
    if offset >= value.len() {
        return value.len();
    }
    let mut next = offset.saturating_add(1);
    while next < value.len() && !value.is_char_boundary(next) {
        next += 1;
    }
    next
}

fn timestamps_for_range(
    history: &[HistoryItem],
    user_times: &HashMap<(LoopId, usize), String>,
    start: usize,
    end: usize,
) -> Vec<Option<String>> {
    let mut occurrences = HashMap::<LoopId, usize>::new();
    for item in history.iter().take(start) {
        if let HistoryItem::User(user) = item {
            *occurrences.entry(user.loop_id).or_default() += 1;
        }
    }
    history
        .iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|item| match item {
            HistoryItem::User(user) => {
                let occurrence = occurrences.entry(user.loop_id).or_default();
                let timestamp = user_times.get(&(user.loop_id, *occurrence)).cloned();
                *occurrence += 1;
                timestamp
            }
            _ => None,
        })
        .collect()
}

async fn sanitized_record_items(
    record: &StoredLoopRecord,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<Vec<HistoryItem>, AgentError> {
    if record.items.len() > MAX_HISTORY_SCAN_LINES {
        return Err(AgentError::QueryLimit);
    }
    let mut items = Vec::new();
    for raw in &record.items {
        check_read_budget(cancellation, deadline)?;
        let cleaned = sanitize_history(std::slice::from_ref(raw))?;
        if let Some(item) = cleaned.first() {
            items.push(item.clone());
        }
        tokio::task::yield_now().await;
    }
    check_read_budget(cancellation, deadline)?;
    Ok(items)
}

async fn sanitized_live_history(
    items: &[HistoryItem],
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<Vec<HistoryItem>, AgentError> {
    if items.len() > MAX_HISTORY_SCAN_LINES {
        return Err(AgentError::QueryLimit);
    }
    let mut sanitized = Vec::new();
    let mut source_bytes = 0_u64;
    for raw in items {
        check_read_budget(cancellation, deadline)?;
        let cleaned = sanitize_history(std::slice::from_ref(raw))?;
        for item in cleaned.iter() {
            let item_bytes =
                u64::try_from(encoded_len(item)?).map_err(|_| AgentError::QueryLimit)?;
            source_bytes = source_bytes
                .checked_add(item_bytes)
                .ok_or(AgentError::QueryLimit)?;
            if source_bytes > MAX_HISTORY_SCAN_BYTES {
                return Err(AgentError::QueryLimit);
            }
            sanitized.push(item.clone());
            if sanitized.len() > MAX_HISTORY_SCAN_LINES {
                return Err(AgentError::QueryLimit);
            }
        }
        tokio::task::yield_now().await;
    }
    check_read_budget(cancellation, deadline)?;
    Ok(sanitized)
}

impl ReadTurnSummary {
    fn from_stored(summary: &StoredTurnSummary) -> Self {
        Self {
            loop_id: summary.loop_id,
            outcome: LoopOutcomeView::from_stored(&summary.outcome),
            usage: summary.usage,
            requests: summary.requests,
            tool_rounds: summary.tool_rounds,
            final_config_revision: summary.final_config_revision,
            completed_at: summary.completed_at.clone(),
        }
    }
}

fn scan_limits(cancellation: CancellationToken) -> HistoryScanLimits {
    HistoryScanLimits {
        max_bytes: MAX_HISTORY_SCAN_BYTES,
        max_lines: MAX_HISTORY_SCAN_LINES,
        deadline: Instant::now()
            .checked_add(READ_DEADLINE)
            .unwrap_or_else(Instant::now),
        cancellation,
    }
}

fn check_read_budget(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), AgentError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(AgentError::QueryLimit)
    } else {
        Ok(())
    }
}

fn records_for_chunks(
    summaries: &[StoredTurnSummary],
    chunks: &[ReadItemChunk],
) -> Vec<ReadTurnSummary> {
    summaries
        .iter()
        .filter(|summary| {
            chunks
                .iter()
                .any(|chunk| chunk.index >= summary.item_start && chunk.index < summary.item_end)
        })
        .map(ReadTurnSummary::from_stored)
        .collect()
}

fn validate_max_bytes(value: Option<usize>) -> Result<(), AgentError> {
    if value.is_some_and(|value| value == 0 || value > MAX_READ_MAX_BYTES) {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(())
    }
}

fn valid_revision(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_cursor(cursor: ReadCursor, total: usize) -> Result<(), AgentError> {
    if cursor.item > total || (cursor.item == total && cursor.offset != 0) {
        return Err(AgentError::InvalidArguments);
    }
    Ok(())
}

fn default_read_limit() -> usize {
    100
}

fn encoded_len<T: Serialize>(value: &T) -> Result<usize, AgentError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| AgentError::RpcSerialization)
}

fn map_read_store_error(error: crate::error::StoreError) -> AgentError {
    match error {
        crate::error::StoreError::HistoryChanged => AgentError::InvalidState,
        crate::error::StoreError::QueryLimit => AgentError::QueryLimit,
        crate::error::StoreError::InvalidArguments => AgentError::InvalidArguments,
        other => crate::sessions::map_store_error(other),
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Clone, Serialize)]
    struct TestPage {
        metadata: String,
        items: Vec<ReadItemChunk>,
        next_cursor: Option<ReadCursor>,
        total: usize,
    }

    fn encode_page(
        metadata: &str,
        items: &[ReadItemChunk],
        next_cursor: Option<ReadCursor>,
        total: usize,
    ) -> Vec<u8> {
        serde_json::to_vec(&TestPage {
            metadata: metadata.to_owned(),
            items: items.to_vec(),
            next_cursor,
            total,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn fragments_reconstruct_escaped_unicode_json_without_loss() {
        let text = "é🙂\n\t\\\" escaped ".repeat(256);
        let json = serde_json::to_string(&serde_json::json!({ "text": text })).unwrap();
        let metadata = "fixed metadata with a non-trivial prefix";
        let cancellation = CancellationToken::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let budget = 4 * 1024;
        assert!(json.len() > budget);
        let mut cursor = ReadCursor::start();
        let mut fragments = Vec::new();
        let mut fragment_count = 0;
        loop {
            let (page, next) = pack_items(
                std::iter::once(Ok::<_, AgentError>((0, json.clone()))),
                cursor,
                1,
                budget,
                &cancellation,
                deadline,
                |items, next_cursor| Ok(encode_page(metadata, items, next_cursor, 1)),
            )
            .await
            .unwrap();
            let encoded_len = encode_page(metadata, &page, next, 1).len();
            assert!(encoded_len <= budget);
            fragment_count += page.len();
            fragments.extend(page.into_iter().map(|chunk| chunk.data));
            let Some(next) = next else {
                break;
            };
            cursor = next;
        }
        assert!(fragment_count > 1);
        let reconstructed = fragments.into_iter().collect::<String>();
        assert_eq!(reconstructed, json);
    }

    #[tokio::test]
    async fn page_budget_includes_metadata_and_rejects_too_small_pages() {
        let json = "{\"text\":\"small\"}".to_owned();
        let metadata = "metadata";
        let cancellation = CancellationToken::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let one_chunk = make_chunk(0, 0, &json, 1);
        let budget = encode_page(
            metadata,
            &[one_chunk],
            Some(ReadCursor { item: 0, offset: 1 }),
            1,
        )
        .len();
        let (items, next) = pack_items(
            std::iter::once(Ok::<_, AgentError>((0, json.clone()))),
            ReadCursor::start(),
            1,
            budget,
            &cancellation,
            deadline,
            |items, next_cursor| Ok(encode_page(metadata, items, next_cursor, 1)),
        )
        .await
        .unwrap();
        assert!(encode_page(metadata, &items, next, 1).len() <= budget);

        let error = pack_items(
            std::iter::once(Ok::<_, AgentError>((0, json))),
            ReadCursor::start(),
            1,
            1,
            &cancellation,
            deadline,
            |items, next_cursor| Ok(encode_page(metadata, items, next_cursor, 1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::InvalidArguments));
    }

    #[tokio::test]
    async fn cursor_must_end_on_a_utf8_boundary() {
        let cancellation = CancellationToken::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let error = pack_items(
            std::iter::once(Ok::<_, AgentError>((0, "é".to_owned()))),
            ReadCursor { item: 0, offset: 1 },
            1,
            256,
            &cancellation,
            deadline,
            |items, next_cursor| Ok(encode_page("metadata", items, next_cursor, 1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AgentError::InvalidArguments));
    }
}
