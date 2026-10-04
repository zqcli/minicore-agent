use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
use crate::tool_data::{
    ToolData, ToolOutputPage, ToolOutputRequest, ToolReadRequest, ToolReadResult, ToolRecord,
    ToolRef,
};

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
    #[serde(default)]
    pub view: ReadView,
    #[serde(default)]
    pub projection_revision: Option<String>,
}

/// Canonical history remains the default. Display is a read-only projection
/// for loaded or persisted sessions; its offsets never apply to canonical history.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadView {
    #[default]
    Canonical,
    Display,
}

impl ReadSession {
    pub fn validate(&self) -> Result<(), AgentError> {
        if !(1..=MAX_READ_ITEMS).contains(&self.limit) {
            return Err(AgentError::InvalidArguments);
        }
        validate_max_bytes(self.max_bytes)?;
        if (self.view == ReadView::Canonical && self.projection_revision.is_some())
            || self
                .projection_revision
                .as_deref()
                .is_some_and(|v| !valid_revision(v))
            || (self.view == ReadView::Display
                && self.captured_end.is_some() != self.projection_revision.is_some())
        {
            return Err(AgentError::InvalidArguments);
        }
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
    pub tool_rounds: u64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<DisplayProjection>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DisplayProjection {
    pub revision: String,
    pub first_item: usize,
    pub covered_item_count: usize,
    pub covered_usage: CoveredUsage,
}

/// Totals from complete stored loops, independent of the visible transcript.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct CoveredUsage {
    pub usage: Usage,
    pub loop_count: u64,
    pub last_loop_id: Option<LoopId>,
    pub partial: bool,
}

impl CoveredUsage {
    pub(crate) fn add(&mut self, loop_id: LoopId, next: Usage) {
        let mut sum = |a: Option<u64>, b: Option<u64>| {
            self.partial |= b.is_none();
            match (a, b) {
                (Some(a), Some(b)) => a.checked_add(b).or_else(|| {
                    self.partial = true;
                    Some(u64::MAX)
                }),
                (a, b) => a.or(b),
            }
        };
        self.usage = Usage::from_optional(
            sum(self.usage.input_tokens(), next.input_tokens()),
            sum(self.usage.output_tokens(), next.output_tokens()),
            sum(self.usage.reasoning_tokens(), next.reasoning_tokens()),
        )
        .with_cache_read_tokens(sum(
            self.usage.cache_read_tokens(),
            next.cache_read_tokens(),
        ))
        .with_cache_write_tokens(sum(
            self.usage.cache_write_tokens(),
            next.cache_write_tokens(),
        ))
        .with_provider_total_tokens(sum(
            self.usage.provider_total_tokens(),
            next.provider_total_tokens(),
        ));
        self.loop_count += 1;
        self.last_loop_id = Some(loop_id);
    }
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
    pub tool_rounds: Option<u64>,
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
    let mut limits = scan_limits(cancellation);
    check_read_budget(&limits.cancellation, limits.deadline)?;
    let snapshot = loaded.as_ref().map(Session::read_snapshot);
    let display = request.view == ReadView::Display;
    let (mut summary, mut cold_prefix) = if display {
        if let Some(session) = &loaded {
            (session.compaction_state().display_snapshot(), None)
        } else {
            let candidate = store
                .read_summary_bytes(request.session_id)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| {
                    crate::compaction::display_snapshot_candidate(&bytes, request.session_id)
                });
            match candidate {
                Some((summary, source)) => (Some(summary), Some(source)),
                None => (None, None),
            }
        }
    } else {
        (None, None)
    };
    let info = if let Some(snapshot) = &snapshot {
        snapshot.info.clone()
    } else {
        let record = store
            .load_record(request.session_id)
            .await
            .map_err(map_read_store_error)?;
        SessionInfo::from_record(&record, false)
    };
    let (covered, first_item, projection_revision, cursor, source_start, mut page) = loop {
        check_read_budget(&limits.cancellation, limits.deadline)?;
        let covered = summary
            .as_ref()
            .map_or(0, |summary| summary.covered_item_count);
        if snapshot
            .as_ref()
            .is_some_and(|snapshot| covered > snapshot.history.len())
        {
            return Err(AgentError::InvalidState);
        }
        let first_item = covered.saturating_sub(1);
        let projection_revision = display.then(|| {
            let mut hash = Sha256::new();
            hash.update(b"display-v1\0");
            hash.update((covered as u64).to_le_bytes());
            hash.update(std::env::var("HOME").unwrap_or_default().as_bytes());
            hash.update(b"\0");
            if let Some(summary) = &summary {
                hash.update(summary.content.as_str().as_bytes());
            }
            format!("{:x}", hash.finalize())
        });
        if cold_prefix.is_none()
            && request
                .projection_revision
                .as_ref()
                .is_some_and(|expected| Some(expected) != projection_revision.as_ref())
        {
            return Err(AgentError::InvalidState);
        }
        let mut cursor = request.cursor.unwrap_or_else(ReadCursor::start);
        if display && request.captured_end.is_none() && cursor == ReadCursor::start() {
            cursor.item = first_item;
        }
        if display && cold_prefix.is_none() && cursor.item < first_item {
            return Err(AgentError::InvalidArguments);
        }
        let source_start = cursor.item.max(covered);
        let page = if display && snapshot.is_none() {
            store
                .read_display_history_page(
                    request.session_id,
                    source_start,
                    request.limit - usize::from(summary.is_some() && cursor.item == first_item),
                    request.captured_end,
                    request.history_revision.as_deref(),
                    cold_prefix.as_ref(),
                    &limits,
                )
                .await
        } else {
            store
                .read_history_page(
                    request.session_id,
                    source_start,
                    request.limit - usize::from(summary.is_some() && cursor.item == first_item),
                    snapshot.as_ref().map(|snapshot| snapshot.history.len()),
                    request.captured_end,
                    request.history_revision.as_deref(),
                    snapshot.as_ref().map(|snapshot| snapshot.history.as_ref()),
                    display.then_some(covered),
                    &limits,
                )
                .await
        }
        .map_err(map_read_store_error)?;
        if cold_prefix.is_some() && !page.summary_anchor_valid {
            // Invalid optional snapshots do not hide source history. A retry
            // stays inside the original aggregate scan budget and starts from
            // the client's cursor/limit, never from the rejected summary slot.
            // A pinned request fails the projection check on the next pass.
            limits.max_bytes = limits.max_bytes.saturating_sub(page.scanned_bytes);
            limits.max_lines = limits.max_lines.saturating_sub(page.scanned_lines);
            summary = None;
            cold_prefix = None;
            continue;
        }
        // A cold candidate has no authority until the same scan verifies its
        // anchor. In particular, an invalid optional file must not invalidate
        // a continuation already pinned to the no-summary display projection.
        if request
            .projection_revision
            .as_ref()
            .is_some_and(|expected| Some(expected) != projection_revision.as_ref())
        {
            return Err(AgentError::InvalidState);
        }
        if display && cursor.item < first_item {
            return Err(AgentError::InvalidArguments);
        }
        break (
            covered,
            first_item,
            projection_revision,
            cursor,
            source_start,
            page,
        );
    };
    validate_cursor(cursor, page.total_items)?;
    if summary
        .as_ref()
        .is_some_and(|summary| page.covered_usage.loop_count != summary.covered_loop_count)
    {
        return Err(AgentError::InvalidState);
    }
    let projection = projection_revision.map(|revision| DisplayProjection {
        revision,
        first_item,
        covered_item_count: covered,
        covered_usage: page.covered_usage.clone(),
    });
    let timestamps = if let Some(snapshot) = &snapshot {
        timestamps_for_range(
            &snapshot.history,
            &snapshot.user_times,
            source_start,
            source_start + page.items.len(),
        )
    } else {
        page.user_times.clone()
    };
    let derived = summary
        .as_ref()
        .filter(|_| cursor.item == first_item)
        .map(|summary| {
            serde_json::to_string(&serde_json::json!({
                "display": true,
                "derived_summary": true,
                "item": {"type": "summary", "data": {"content": summary.content.as_str()}}
            }))
            .map(|body| (first_item, body))
            .map_err(|_| AgentError::RpcSerialization)
        });
    let tail: Box<dyn Iterator<Item = Result<(usize, String), AgentError>> + Send + '_> = if display
    {
        if let Some(snapshot) = &snapshot {
            Box::new(encoded_display_items(
                request.session_id,
                source_start,
                &page.items,
                &timestamps,
                &snapshot.history[covered..page.total_items],
            )?)
        } else {
            Box::new(
                page.display_items
                    .take()
                    .ok_or(AgentError::Internal)?
                    .into_iter()
                    .map(Ok),
            )
        }
    } else {
        Box::new(encoded_items(source_start, &page.items, &timestamps)?)
    };
    let encoded = derived.into_iter().chain(tail);
    let mut result = ReadSessionResult {
        session: info,
        items: Vec::new(),
        next_cursor: None,
        total: page.total_items,
        records: Vec::new(),
        records_truncated: page.turns_truncated,
        history_revision: page.revision,
        captured_end: page.captured_end,
        trailing_incomplete: page.trailing_incomplete,
        projection,
    };
    let (items, next_cursor) = pack_items(
        encoded,
        cursor,
        result.total,
        max_bytes,
        &limits.cancellation,
        limits.deadline,
        |items, next_cursor| {
            let mut candidate = result.clone();
            candidate.items = items.to_vec();
            candidate.next_cursor = next_cursor;
            candidate.records = records_for_chunks(&page.turns, items);
            serde_json::to_vec(&candidate).map_err(|_| AgentError::RpcSerialization)
        },
    )
    .await?;
    result.records = records_for_chunks(&page.turns, &items);
    result.items = items;
    result.next_cursor = next_cursor;
    Ok(result)
}

/// Canonical tool bodies are never serialized into the display envelope.
/// Whitelisted presentation metadata is regenerated by the one Agent formatter.
pub(crate) fn encoded_display_items<'a>(
    session_id: SessionId,
    start: usize,
    items: &'a [HistoryItem],
    timestamps: &'a [Option<String>],
    history: &'a [HistoryItem],
) -> Result<impl Iterator<Item = Result<(usize, String), AgentError>> + 'a, AgentError> {
    use minicore_runtime::model::AssistantPart;
    if timestamps.len() != items.len() {
        return Err(AgentError::Internal);
    }
    let mut calls = HashMap::new();
    let mut results = HashMap::new();
    for item in history {
        match item {
            HistoryItem::Assistant(assistant) => {
                for part in &assistant.content {
                    if let AssistantPart::ToolCall(call) = part {
                        calls.insert(
                            (
                                assistant.loop_id,
                                assistant.request_index,
                                call.tool_call_id(),
                            ),
                            call,
                        );
                    }
                }
            }
            HistoryItem::ToolResult(result) => {
                results.insert(
                    (result.loop_id, result.request_index, &result.call_id),
                    result,
                );
            }
            _ => {}
        }
    }
    Ok(items.iter().enumerate().map(move |(offset, item)| {
        // Remove the potentially large bodies before serializing the item.
        // Other Runtime fields retain exactly their canonical shape.
        let value = match item {
            HistoryItem::Assistant(assistant) => serde_json::json!({
                "type": "assistant", "data": {
                    "loop_id": assistant.loop_id, "request_index": assistant.request_index,
                    "model": assistant.model, "reasoning": assistant.reasoning,
                    "content": display_assistant_parts(assistant)?,
                    "finish_reason": assistant.finish_reason, "usage": assistant.usage
                }
            }),
            HistoryItem::ToolResult(result) => serde_json::json!({
                "type": "tool_result", "data": {
                    "loop_id": result.loop_id, "request_index": result.request_index,
                    "call_id": result.call_id, "tool_name": result.tool_name, "outcome": result.outcome
                }
            }),
            _ => serde_json::to_value(item).map_err(|_| AgentError::RpcSerialization)?,
        };
        let keys = match item {
            HistoryItem::Assistant(assistant) => assistant.content.iter().filter_map(|part| part.as_tool_call().map(|call|
                ((assistant.loop_id, assistant.request_index, call.tool_call_id()), call.name().as_str()))).collect::<Vec<_>>(),
            HistoryItem::ToolResult(result) => vec![((result.loop_id, result.request_index, &result.call_id), result.tool_name.as_str())],
            _ => Vec::new(),
        };
        let summaries = keys.into_iter().map(|(key, name)| {
            let call = calls.get(&key);
            let result = results.get(&key);
            let mut display = crate::presentation::build_tool_card_display(name, call.map(|call| call.arguments()));
            let input_truncated = display.body_truncated;
            let (output_line_count, output_truncated) = result.map(|result| {
                let (body, truncated) = crate::presentation::bounded_result_content(result.output.content().as_str(), crate::presentation::MAX_RESULT_DISPLAY_BYTES);
                (Some(crate::presentation::count_lines(&body)), truncated)
            }).unwrap_or((None, false));
            let count_state = if result.is_none() {
                if display.input_line_count.unwrap_or(0) > 0 { "lower_bound" } else { "unknown" }
            } else if input_truncated || output_truncated
                || (call.is_none() && matches!(name, "bash" | "write" | "edit" | "patch" | "apply_patch")) {
                "lower_bound"
            } else { "exact" };
            display.expanded_input = None;
            display.hidden_line_count = Some(display.input_line_count.unwrap_or(0) + output_line_count.unwrap_or(0));
            serde_json::json!({
                "tool_ref": ToolRef {session_id, loop_id: key.0, request_index: key.1, tool_call_id: key.2.clone()},
                "tool_call_id": key.2, "name": name, "display": display,
                "output_line_count": output_line_count, "output_truncated": output_truncated,
                "count_state": count_state,
                "state": result.map(|result| crate::tool_data::ToolExecutionState::from_outcome(result.outcome)),
                "input_availability": null, "output_availability": null
            })
        }).collect::<Vec<_>>();
        let envelope = serde_json::json!({"display": true, "item": value, "timestamp": timestamps[offset], "tool_summaries": summaries});
        let encoded = serde_json::to_string(&envelope).map_err(|_| AgentError::RpcSerialization)?;
        Ok((start + offset, encoded))
    }))
}

fn display_assistant_parts(
    assistant: &minicore_runtime::history::AssistantHistory,
) -> Result<Vec<serde_json::Value>, AgentError> {
    use minicore_runtime::model::AssistantPart;
    assistant.content.iter().map(|part| match part {
        AssistantPart::ToolCall(call) => Ok(serde_json::json!({"type": "tool_call", "data": {
            "tool_call_id": call.tool_call_id(), "name": call.name(), "call_index": call.call_index()
        }})),
        _ => serde_json::to_value(part).map_err(|_| AgentError::RpcSerialization),
    }).collect()
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

#[cfg(test)]
pub(crate) struct ToolReadGate {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

#[cfg(test)]
impl ToolReadGate {
    pub(crate) fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
type ToolReadGateEntry = (ToolRef, Arc<ToolReadGate>);
#[cfg(test)]
static TOOL_READ_GATES: std::sync::OnceLock<std::sync::Mutex<Vec<ToolReadGateEntry>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn gate_next_tool_read(tool_ref: ToolRef, gate: Arc<ToolReadGate>) {
    let mutex = TOOL_READ_GATES.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    mutex.lock().unwrap().push((tool_ref, gate));
}

#[cfg(test)]
async fn check_tool_read_gate(tool_ref: &ToolRef) {
    let gate = {
        let Some(mutex) = TOOL_READ_GATES.get() else {
            return;
        };
        let mut entries = mutex.lock().unwrap();
        let Some(pos) = entries.iter().position(|(r, _)| r == tool_ref) else {
            return;
        };
        entries.remove(pos).1
    };
    gate.entered.notify_one();
    gate.release.notified().await;
}

async fn resolve_tool_record(
    store: &Store,
    loaded_tool_data: Option<&ToolData>,
    tool_ref: &ToolRef,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<ToolRecord, AgentError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        return Err(AgentError::QueryLimit);
    }

    let mem_record = loaded_tool_data.and_then(|td| td.get_record(tool_ref));
    if let Some(mem) = mem_record {
        if !mem.needs_stored() {
            return Ok(mem);
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Err(AgentError::QueryLimit),
            res = cold_read_and_merge(store, mem, tool_ref, deadline) => res,
        }
    } else {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Err(AgentError::QueryLimit),
            res = cold_read_only(store, tool_ref, deadline) => res,
        }
    }
}

async fn cold_read_and_merge(
    store: &Store,
    mut mem: ToolRecord,
    tool_ref: &ToolRef,
    deadline: Instant,
) -> Result<ToolRecord, AgentError> {
    #[cfg(test)]
    check_tool_read_gate(tool_ref).await;

    let disk_result = store
        .read_tool_record_with_deadline(tool_ref, deadline)
        .await;

    match disk_result {
        Ok(Some(disk_record)) => {
            mem.merge_stored(disk_record);
            Ok(mem)
        }
        Ok(None) => Ok(mem),
        Err(store_err) => {
            if matches!(store_err, crate::error::StoreError::QueryLimit) {
                Err(AgentError::QueryLimit)
            } else {
                Ok(mem)
            }
        }
    }
}

async fn cold_read_only(
    store: &Store,
    tool_ref: &ToolRef,
    deadline: Instant,
) -> Result<ToolRecord, AgentError> {
    #[cfg(test)]
    check_tool_read_gate(tool_ref).await;

    let disk_result = store
        .read_tool_record_with_deadline(tool_ref, deadline)
        .await;

    match disk_result {
        Ok(Some(disk_record)) => Ok(disk_record),
        Ok(None) => Err(AgentError::ToolNotFound),
        Err(store_err) => {
            if matches!(store_err, crate::error::StoreError::QueryLimit) {
                Err(AgentError::QueryLimit)
            } else {
                Err(AgentError::ToolNotFound)
            }
        }
    }
}

pub(crate) async fn tool_read(
    store: Store,
    loaded_tool_data: Option<Arc<ToolData>>,
    request: ToolReadRequest,
    cancellation: CancellationToken,
) -> Result<ToolReadResult, AgentError> {
    request.validate()?;
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    let deadline = Instant::now() + READ_DEADLINE;
    let record = resolve_tool_record(
        &store,
        loaded_tool_data.as_deref(),
        &request.tool_ref,
        deadline,
        &cancellation,
    )
    .await?;
    if request.display {
        record.project_display_read(&request.tool_ref, max_bytes)
    } else {
        record.project_read(&request.tool_ref, max_bytes)
    }
}

pub(crate) async fn tool_output(
    store: Store,
    loaded_tool_data: Option<Arc<ToolData>>,
    request: ToolOutputRequest,
    cancellation: CancellationToken,
) -> Result<ToolOutputPage, AgentError> {
    request.validate()?;
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    let deadline = Instant::now() + READ_DEADLINE;
    let record = resolve_tool_record(
        &store,
        loaded_tool_data.as_deref(),
        &request.tool_ref,
        deadline,
        &cancellation,
    )
    .await?;
    record.project_output(&request, max_bytes)
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
    let outcome = LoopOutcomeView::from_report(&result.report);
    let persistence = result.persistence;
    let usage = result.report.usage;
    let requests = result.report.requests;
    let tool_rounds = result.report.tool_rounds;
    let final_config_revision = result.report.final_config_revision;
    let total = sanitized.len();
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
        total,
    };
    assemble_turn_page(
        base,
        &sanitized,
        cursor,
        limit,
        max_bytes,
        cancellation,
        deadline,
    )
    .await
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
    let total = sanitized.len();
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
    assemble_turn_page(
        base,
        &sanitized,
        cursor,
        limit,
        max_bytes,
        cancellation,
        deadline,
    )
    .await
}

/// Shared page assembly for Live and Stored turns: cursor validation, slicing,
/// the timestamp array, JSON encoding, and the byte-budget pack.
///
/// The caller cleans its own source and builds the metadata-only base page; the
/// source differences (availability, persistence, `completed_at`, outcome,
/// usage, revision) stay in that base. Pending never reaches this helper.
async fn assemble_turn_page(
    base: TurnResultPage,
    sanitized: &[HistoryItem],
    cursor: ReadCursor,
    limit: usize,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<TurnResultPage, AgentError> {
    validate_cursor(cursor, sanitized.len())?;
    let end = cursor.item.saturating_add(limit).min(sanitized.len());
    let timestamps = vec![None; end.saturating_sub(cursor.item)];
    let encoded = encoded_items(cursor.item, &sanitized[cursor.item..end], &timestamps)?;
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
        let mut item = item.clone();
        if let HistoryItem::Assistant(assistant) = &mut item {
            assistant.provider_replay = None;
        }
        let envelope = ReadItemEnvelope {
            item: &item,
            timestamp,
        };
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

pub(crate) fn validate_max_bytes(value: Option<usize>) -> Result<(), AgentError> {
    if value.is_some_and(|value| value == 0 || value > MAX_READ_MAX_BYTES) {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(())
    }
}

pub(crate) fn valid_revision(value: &str) -> bool {
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
    use crate::store::StoredLoopOutcome;
    use minicore_runtime::execution::ConfigRevision;

    #[test]
    fn display_projection_strips_bodies_and_only_uses_the_captured_prefix() {
        use minicore_runtime::history::{AssistantHistory, ToolResultHistory};
        use minicore_runtime::model::{
            AssistantPart, ModelFinishReason, ReasoningPreference, ToolCall,
        };
        use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
        let session_id = SessionId::new().unwrap();
        let loop_id = LoopId::new().unwrap();
        let call_id = minicore_runtime::ToolCallId::new("display-call").unwrap();
        let call = ToolCall::new(
            call_id.clone(),
            "write".parse().unwrap(),
            serde_json::json!({
                "path": "file.txt", "content": "private body\nsecond", "secret": "raw secret"
            }),
            0,
        )
        .unwrap();
        let mut history = vec![HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: 0,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![
                AssistantPart::Text("visible".to_owned()),
                AssistantPart::ToolCall(call),
            ],
            provider_replay: None,
            finish_reason: ModelFinishReason::ToolCalls,
            usage: Usage::new(1, 2, 0),
        })];
        let times = vec![None];
        let encode = |history: &[HistoryItem], end: usize| {
            encoded_display_items(session_id, 0, &history[..1], &times, &history[..end])
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .1
        };
        let original = encode(&history, 1);
        assert!(!original.contains("private body"));
        assert!(!original.contains("raw secret"));
        assert!(original.contains("visible"));
        history.push(HistoryItem::ToolResult(ToolResultHistory {
            loop_id,
            request_index: 0,
            call_id,
            tool_name: "write".parse().unwrap(),
            outcome: ToolResultOutcome::Success,
            output: ToolOutput::new("result outside pin").unwrap(),
        }));
        assert_eq!(encode(&history, 1), original);
        assert_ne!(encode(&history, 2), original);
        let complete: serde_json::Value = serde_json::from_str(&encode(&history, 2)).unwrap();
        assert_eq!(
            complete["tool_summaries"][0]["display"]["input_line_count"],
            2
        );
        assert_eq!(complete["tool_summaries"][0]["output_line_count"], 1);
        assert_eq!(complete["tool_summaries"][0]["count_state"], "exact");
        assert!(complete["tool_summaries"][0]["input_availability"].is_null());
    }

    #[test]
    fn covered_usage_preserves_known_partial_totals_without_filling_unknown_with_zero() {
        let mut covered = CoveredUsage::default();
        assert_eq!(covered.usage.input_tokens(), None);
        covered.add(
            LoopId::new().unwrap(),
            Usage::from_optional(Some(7), None, Some(0)),
        );
        let last = LoopId::new().unwrap();
        covered.add(last, Usage::from_optional(Some(3), Some(5), Some(0)));
        assert_eq!(covered.usage.input_tokens(), Some(10));
        assert_eq!(covered.usage.output_tokens(), Some(5));
        assert!(covered.partial);
        assert_eq!(covered.loop_count, 2);
        assert_eq!(covered.last_loop_id, Some(last));
    }

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

    fn user_item(loop_id: LoopId, text: &str) -> HistoryItem {
        HistoryItem::User(minicore_runtime::history::UserHistory {
            loop_id,
            kind: minicore_runtime::history::UserMessageKind::Prompt,
            input: minicore_runtime::execution::UserInput::text(text).unwrap(),
        })
    }

    #[tokio::test]
    async fn wide_tool_round_counts_reach_event_and_read_wire_views() {
        for tool_rounds in [65_535_u64, 65_536, 70_000] {
            let loop_id = LoopId::new().unwrap();
            let turn = TurnRef {
                session_id: SessionId::new().unwrap(),
                loop_id,
            };
            let live = Arc::new(crate::sessions::TurnResult {
                turn,
                report: Arc::new(minicore_runtime::LoopReport {
                    loop_id,
                    outcome: minicore_runtime::LoopOutcome::Completed,
                    appended: Arc::from([]),
                    usage: Usage::default(),
                    requests: 1,
                    tool_rounds,
                    final_config_revision: ConfigRevision::INITIAL,
                }),
                persistence: TurnPersistence::Persisted,
            });
            let event = crate::event::TurnResultView::from_turn_result(&live);
            assert_eq!(
                serde_json::to_value(event).unwrap()["tool_rounds"].as_u64(),
                Some(tool_rounds)
            );
            let stored = StoredLoopRecord {
                loop_id,
                outcome: StoredLoopOutcome::Completed,
                items: Vec::new(),
                usage: Usage::default(),
                requests: 1,
                tool_rounds,
                final_config_revision: ConfigRevision::INITIAL,
                completed_at: "2026-10-03T00:00:00Z".to_owned(),
                user_times: None,
            };
            let cancellation = CancellationToken::new();
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            let live_page = live_turn_page(
                turn,
                live,
                ReadCursor::start(),
                100,
                4096,
                &cancellation,
                deadline,
            )
            .await
            .unwrap();
            let stored_page = stored_turn_page(
                turn,
                stored,
                ReadCursor::start(),
                100,
                4096,
                &cancellation,
                deadline,
            )
            .await
            .unwrap();
            for page in [live_page, stored_page] {
                assert_eq!(page.tool_rounds, Some(tool_rounds));
                assert_eq!(
                    serde_json::to_value(page).unwrap()["tool_rounds"].as_u64(),
                    Some(tool_rounds)
                );
            }
            let summary = ReadTurnSummary::from_stored(&StoredTurnSummary {
                item_start: 0,
                item_end: 0,
                loop_id,
                outcome: StoredLoopOutcome::Completed,
                usage: Usage::default(),
                requests: 1,
                tool_rounds,
                final_config_revision: ConfigRevision::INITIAL,
                completed_at: "2026-10-03T00:00:00Z".to_owned(),
            });
            assert_eq!(
                serde_json::to_value(summary).unwrap()["tool_rounds"].as_u64(),
                Some(tool_rounds)
            );
        }
    }

    #[tokio::test]
    async fn live_and_stored_turn_pages_preserve_metadata_and_encoded_items() {
        const MAX_BYTES: usize = 512;
        const COMPLETED_AT: &str = "2026-01-02T03:04:05.000Z";

        let loop_id = LoopId::new().unwrap();
        let turn = TurnRef {
            session_id: SessionId::new().unwrap(),
            loop_id,
        };
        let multi_items: Vec<HistoryItem> = (0..6)
            .map(|index| user_item(loop_id, &format!("turn item {index} é🙂\n\t\\\"")))
            .collect();
        let long_text = "é🙂\n\t\\\"".repeat(256);
        let cases = [
            ("multi-item", multi_items),
            ("long-item", vec![user_item(loop_id, &long_text)]),
            ("empty-items", Vec::new()),
        ];
        let cancellation = CancellationToken::new();

        for (label, items) in cases {
            let sanitized = sanitize_history(&items).unwrap();
            let expected: Vec<String> = sanitized
                .iter()
                .map(|item| {
                    serde_json::to_string(&ReadItemEnvelope {
                        item,
                        timestamp: None,
                    })
                    .unwrap()
                })
                .collect();
            let live = Arc::new(crate::sessions::TurnResult {
                turn,
                report: Arc::new(minicore_runtime::LoopReport {
                    loop_id,
                    outcome: minicore_runtime::LoopOutcome::Completed,
                    appended: items.clone().into(),
                    usage: Usage::default(),
                    requests: 1,
                    tool_rounds: 0,
                    final_config_revision: ConfigRevision::INITIAL,
                }),
                persistence: crate::sessions::TurnPersistence::Failed,
            });
            let stored = StoredLoopRecord {
                loop_id,
                outcome: StoredLoopOutcome::Completed,
                items,
                usage: Usage::default(),
                requests: 1,
                tool_rounds: 0,
                final_config_revision: ConfigRevision::INITIAL,
                completed_at: COMPLETED_AT.to_owned(),
                user_times: None,
            };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

            for live_side in [true, false] {
                let side = if live_side { "live" } else { "stored" };
                let (availability, persistence, completed_at) = if live_side {
                    (
                        TurnResultAvailability::Live,
                        Some(crate::sessions::TurnPersistence::Failed),
                        None,
                    )
                } else {
                    (
                        TurnResultAvailability::Stored,
                        Some(crate::sessions::TurnPersistence::Persisted),
                        Some(COMPLETED_AT),
                    )
                };
                let mut cursor = ReadCursor::start();
                let mut reconstructed = vec![String::new(); expected.len()];
                let mut page_count = 0;
                loop {
                    let page = if live_side {
                        live_turn_page(
                            turn,
                            Arc::clone(&live),
                            cursor,
                            100,
                            MAX_BYTES,
                            &cancellation,
                            deadline,
                        )
                        .await
                        .unwrap()
                    } else {
                        stored_turn_page(
                            turn,
                            stored.clone(),
                            cursor,
                            100,
                            MAX_BYTES,
                            &cancellation,
                            deadline,
                        )
                        .await
                        .unwrap()
                    };
                    assert!(
                        encoded_len(&page).unwrap() <= MAX_BYTES,
                        "{label} {side} page over budget"
                    );
                    assert_eq!(
                        (
                            page.turn,
                            page.availability,
                            page.outcome.clone(),
                            page.persistence,
                            page.usage,
                            page.requests,
                            page.tool_rounds,
                            page.final_config_revision,
                            page.completed_at.as_deref(),
                            page.total,
                        ),
                        (
                            turn,
                            availability,
                            Some(LoopOutcomeView::Completed),
                            persistence,
                            Some(Usage::default()),
                            Some(1),
                            Some(0),
                            Some(ConfigRevision::INITIAL),
                            completed_at,
                            expected.len(),
                        ),
                        "{label} {side} metadata"
                    );
                    for chunk in &page.items {
                        let expected_item = &expected[chunk.index];
                        assert_eq!(chunk.encoding, JSON_ENCODING);
                        assert_eq!(chunk.total_bytes, expected_item.len());
                        assert_eq!(chunk.offset, reconstructed[chunk.index].len());
                        let end = chunk.offset.saturating_add(chunk.data.len());
                        assert!(
                            end <= expected_item.len()
                                && expected_item.is_char_boundary(chunk.offset)
                                && expected_item.is_char_boundary(end)
                        );
                        assert_eq!(chunk.data.as_str(), &expected_item[chunk.offset..end]);
                        reconstructed[chunk.index].push_str(&chunk.data);
                        assert_eq!(chunk.complete, end == expected_item.len());
                    }
                    let next_cursor = page.next_cursor;
                    page_count += 1;
                    match next_cursor {
                        Some(next) => cursor = next,
                        None => break,
                    }
                }
                assert_eq!(reconstructed, expected, "{label} {side} item data");
                if expected.is_empty() {
                    assert_eq!(page_count, 1, "{label} {side} empty page");
                } else {
                    assert!(page_count > 1, "{label} {side} must span pages");
                }
            }
        }
    }
}
