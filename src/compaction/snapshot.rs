use super::*;
use serde::Deserialize;

const SUMMARY_FORMAT_VERSION: u32 = 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SummarySnapshot {
    format_version: u32,
    session_id: SessionId,
    model: String,
    reasoning: ReasoningPreference,
    source: SummarySource,
    summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    superseded_usage: Option<(LoopId, u32)>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SummarySource {
    prefix_bytes: u64,
    covered_loop_count: u64,
    covered_item_count: u64,
    last_loop_id: Option<LoopId>,
    sha256: String,
}

pub(crate) fn validate_summary_content(value: &str) -> Option<BoundedText> {
    let content = BoundedText::new_with_max_bytes(value, MAX_SUMMARY_CONTENT_BYTES).ok()?;
    if content.as_str().trim().is_empty() || summary_data_message(&content).is_err() {
        return None;
    }
    Some(content)
}

/// Generated summaries remain capped at 64 KiB. A settled snapshot can also
/// contain the complete sanitized effective projection, encoded as historical
/// message data. That different object uses the existing Runtime envelope and
/// 256 KiB snapshot-file bounds, not the model's semantic-output limit.
pub(super) fn validate_snapshot_content(value: &str) -> Option<BoundedText> {
    if value.len() <= MAX_SUMMARY_CONTENT_BYTES {
        return validate_summary_content(value);
    }
    let content = BoundedText::new(value).ok()?;
    summary_data_message(&content).ok()?;
    let messages: Vec<ModelMessage> = serde_json::from_str(value).ok()?;
    if messages.is_empty()
        || !recovery::validate_clean_tool_exchanges(&messages)
        || messages.iter().any(|message| match message {
            ModelMessage::AssistantWithReplay { .. } => true,
            ModelMessage::Assistant(parts) => parts.iter().any(|part| {
                matches!(part, minicore_runtime::model::AssistantPart::Reasoning(reasoning)
                    if reasoning.encrypted().is_some() || reasoning.signature().is_some())
            }),
            _ => false,
        })
    {
        return None;
    }
    Some(content)
}

pub(crate) fn encode_snapshot(
    session_id: SessionId,
    record: &SessionRecord,
    source: &HistoryPrefix,
    summary: &BoundedText,
    superseded_usage: Option<(LoopId, u32)>,
) -> Option<Vec<u8>> {
    validate_snapshot_content(summary.as_str())?;
    let snapshot = SummarySnapshot {
        format_version: SUMMARY_FORMAT_VERSION,
        session_id,
        model: record.model.clone(),
        reasoning: record.reasoning,
        source: SummarySource {
            prefix_bytes: source.prefix_bytes,
            covered_loop_count: source.covered_loop_count,
            covered_item_count: source.covered_item_count,
            last_loop_id: source.last_loop_id,
            sha256: source.sha256.clone(),
        },
        summary: summary.as_str().to_owned(),
        superseded_usage,
    };
    let bytes = serde_json::to_vec(&snapshot).ok()?;
    (bytes.len() <= MAX_SUMMARY_FILE_BYTES).then_some(bytes)
}

/// Shape-check a cold display candidate without loading a Session. The bounded
/// history page scan must validate this exact source anchor before using it.
pub(crate) fn display_snapshot_candidate(
    bytes: &[u8],
    session_id: SessionId,
) -> Option<(LoadedSummary, HistoryPrefix)> {
    let snapshot = decode_snapshot(bytes)?;
    let summary = validate_snapshot_shape(&snapshot, session_id, usize::MAX)?;
    let source = HistoryPrefix {
        prefix_bytes: snapshot.source.prefix_bytes,
        covered_loop_count: snapshot.source.covered_loop_count,
        covered_item_count: snapshot.source.covered_item_count,
        last_loop_id: snapshot.source.last_loop_id,
        sha256: snapshot.source.sha256,
    };
    Some((summary, source))
}

/// Loads only a validated derived snapshot. Any snapshot problem returns an
/// empty state; the already-loaded core history remains authoritative.
pub(crate) async fn load_state(
    store: &Store,
    session_id: SessionId,
    history: &[HistoryItem],
) -> Arc<CompactionState> {
    let state = CompactionState::new();
    let Some(bytes) = store.read_summary_bytes(session_id).await.ok().flatten() else {
        return state;
    };
    let Some(snapshot) = decode_snapshot(&bytes) else {
        return state;
    };
    let Some(summary) = validate_snapshot_shape(&snapshot, session_id, history.len()) else {
        return state;
    };
    if let Some(identity) = snapshot.superseded_usage
        && !history.iter().any(|item| {
            matches!(item, HistoryItem::Assistant(assistant)
            if (assistant.loop_id, assistant.request_index) == identity)
        })
    {
        return state;
    }
    let Some(prefix) = store
        .read_history_prefix(session_id, snapshot.source.prefix_bytes, history)
        .await
        .ok()
        .flatten()
    else {
        return state;
    };
    let Some(summary) =
        validate_snapshot_anchor(&snapshot, session_id, history.len(), &prefix, summary)
    else {
        return state;
    };
    state.install(summary);
    state
}

fn decode_snapshot(bytes: &[u8]) -> Option<SummarySnapshot> {
    if bytes.len() > MAX_SUMMARY_FILE_BYTES {
        return None;
    }
    serde_json::from_slice(bytes).ok()
}

fn validate_snapshot_shape(
    snapshot: &SummarySnapshot,
    session_id: SessionId,
    history_len: usize,
) -> Option<LoadedSummary> {
    let _provenance_reasoning = snapshot.reasoning;
    if snapshot.format_version != SUMMARY_FORMAT_VERSION
        || snapshot.session_id != session_id
        || !valid_provenance(&snapshot.model)
        || snapshot
            .model
            .parse::<minicore_runtime::model::ModelRef>()
            .is_err()
        || snapshot.source.prefix_bytes == 0
        || snapshot.source.covered_loop_count == 0
        || snapshot.source.covered_item_count == 0
        || snapshot.source.last_loop_id.is_none()
        || !valid_sha256(&snapshot.source.sha256)
    {
        return None;
    }
    let covered_item_count = usize::try_from(snapshot.source.covered_item_count).ok()?;
    if covered_item_count > history_len {
        return None;
    }
    let content = validate_snapshot_content(&snapshot.summary)?;
    Some(LoadedSummary {
        content,
        covered_loop_count: snapshot.source.covered_loop_count,
        covered_item_count,
        superseded_usage: snapshot.superseded_usage,
    })
}

fn validate_snapshot_anchor(
    snapshot: &SummarySnapshot,
    session_id: SessionId,
    history_len: usize,
    prefix: &HistoryPrefix,
    summary: LoadedSummary,
) -> Option<LoadedSummary> {
    if snapshot.session_id != session_id
        || snapshot.source.prefix_bytes != prefix.prefix_bytes
        || snapshot.source.covered_loop_count != prefix.covered_loop_count
        || snapshot.source.covered_item_count != prefix.covered_item_count
        || snapshot.source.last_loop_id != prefix.last_loop_id
        || snapshot.source.sha256 != prefix.sha256
        || summary.covered_item_count > history_len
    {
        return None;
    }
    Some(summary)
}

fn valid_provenance(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.chars().all(|character| !character.is_control())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
