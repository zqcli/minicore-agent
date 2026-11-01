use super::*;

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

    let continuation = request
        .cursor
        .is_some_and(|cursor| cursor != ReadCursor::start());

    if let Some(session) = loaded.as_ref() {
        let mut active_saved = None;
        let publication = session.result_catalog().publication();
        if let Some(load) = &request.load {
            if load != &session.load().to_string() {
                return Err(AgentError::StaleLoad);
            }
            if let Some((entry, source)) = session
                .result_catalog()
                .lookup(session.load(), request.turn.loop_id)
            {
                if let Some(source) = source {
                    drop(publication);
                    return retained_turn_page(
                        request,
                        entry,
                        source,
                        cursor,
                        max_bytes,
                        &limits.cancellation,
                        limits.deadline,
                    )
                    .await;
                }
                if entry.availability == crate::sessions::results::ResultAvailability::Unavailable {
                    return unavailable_turn_page(
                        request.turn,
                        Some(load.clone()),
                        entry
                            .unavailable_reason
                            .expect("unavailable results have a reason"),
                        if entry.completion_kind == crate::sessions::CompletionKind::InternalError {
                            None
                        } else {
                            session
                                .turn_result_snapshot(request.turn)?
                                .flatten()
                                .map(|result| LoopOutcomeView::from_report(&result.report))
                                .or_else(|| completion_outcome(entry.outcome_kind))
                        },
                        entry.persistence,
                        entry.persistence_uncertain,
                        max_bytes,
                    );
                }
            }
        } else if session
            .result_catalog()
            .lookup(session.load(), request.turn.loop_id)
            .is_some_and(|(entry, _)| entry.persistence != Some(TurnPersistence::Persisted))
        {
            return Err(AgentError::MissingLoad);
        }
        match session.turn_result_snapshot(request.turn)? {
            Some(None) => {
                if request.load.is_none() {
                    return Err(AgentError::MissingLoad);
                }
                validate_cursor(cursor, 0)?;
                return crate::budget::encoded_result(
                    empty_turn_page(request.turn, request.load),
                    max_bytes,
                );
            }
            Some(Some(result)) => {
                if result.persistence == TurnPersistence::Failed {
                    if request.load.is_none() {
                        return Err(AgentError::MissingLoad);
                    }
                    return unavailable_turn_page(
                        request.turn,
                        request.load,
                        crate::sessions::UnavailableReason::Expired,
                        Some(LoopOutcomeView::from_report(&result.report)),
                        Some(TurnPersistence::Failed),
                        session.uncertain_loop() == Some(request.turn.loop_id),
                        max_bytes,
                    );
                }
                if request.load.is_some() && !continuation {
                    active_saved = Some(result);
                }
            }
            None => {}
        }
        if session.uncertain_loop() == Some(request.turn.loop_id) {
            if request.load.is_none() {
                return Err(AgentError::MissingLoad);
            }
            return unavailable_turn_page(
                request.turn,
                request.load,
                crate::sessions::UnavailableReason::Expired,
                None,
                Some(TurnPersistence::Failed),
                true,
                max_bytes,
            );
        }
        drop(publication);
        let snapshot = session.read_snapshot();
        if let Some((range, source_revision)) = store.loop_item_source(
            request.turn.session_id,
            request.turn.loop_id,
            limits.max_lines,
        ) && range.end <= snapshot.history.len()
        {
            let availability = if active_saved.is_some() {
                TurnResultAvailability::Live
            } else {
                TurnResultAvailability::Stored
            };
            return resident_stored_turn_page(
                &store,
                session,
                request,
                snapshot,
                range,
                source_revision,
                availability,
                max_bytes,
                &limits,
            )
            .await;
        }
        if let Some(result) = active_saved {
            return live_turn_page(&request, result, &limits).await;
        }
    } else if request.load.is_some() {
        return Err(AgentError::StaleLoad);
    }

    let record = store
        .read_loop_record(request.turn.session_id, request.turn.loop_id, &limits)
        .await
        .map_err(AgentError::from)?;
    let Some(record) = record else {
        return unavailable_turn_page(
            request.turn,
            request.load,
            crate::sessions::UnavailableReason::NotRetained,
            None,
            None,
            false,
            max_bytes,
        );
    };
    stored_turn_page(&request, record, &limits).await
}

#[allow(clippy::too_many_arguments)]
async fn resident_stored_turn_page(
    store: &Store,
    session: &Session,
    request: TurnResultRequest,
    snapshot: crate::sessions::ReadSnapshot,
    range: std::ops::Range<usize>,
    source_revision: [u8; 32],
    availability: TurnResultAvailability,
    max_bytes: usize,
    limits: &HistoryScanLimits,
) -> Result<TurnResultPage, AgentError> {
    use crate::sessions::results::{StoredIdentity, StoredIdentityKey};
    let key = StoredIdentityKey {
        load: session.load(),
        source_revision,
        loop_id: request.turn.loop_id,
    };
    let catalog = session.result_catalog();
    let cursor = request.cursor.unwrap_or_else(ReadCursor::start);
    let mut prepared = None;
    let identity = if let Some(identity) = catalog.stored_identity(key) {
        identity
    } else {
        let _preparing = tokio::select! {
            guard = catalog.stored_preparation() => guard,
            _ = limits.cancellation.cancelled() => return Err(AgentError::QueryLimit),
        };
        check_budget(&limits.cancellation, limits.deadline)?;
        if let Some(identity) = catalog.stored_identity(key) {
            identity
        } else {
            let record = store
                .read_loop_record(request.turn.session_id, request.turn.loop_id, limits)
                .await
                .map_err(AgentError::from)?;
            let Some(record) = record else {
                return unavailable_turn_page(
                    request.turn,
                    request.load,
                    crate::UnavailableReason::NotRetained,
                    None,
                    None,
                    false,
                    max_bytes,
                );
            };
            if record.loop_id != request.turn.loop_id {
                return Err(AgentError::InvalidState);
            }
            let metadata = tokio::task::spawn_blocking(move || {
                let StoredLoopRecord {
                    loop_id,
                    outcome,
                    items,
                    usage,
                    requests,
                    tool_rounds,
                    final_config_revision,
                    completed_at,
                    user_times: _,
                    interrupted,
                } = record;
                drop(items);
                ReadTurnSummary {
                    loop_id,
                    outcome,
                    usage,
                    requests,
                    tool_rounds,
                    final_config_revision,
                    completed_at,
                    interrupted,
                }
            })
            .await
            .map_err(|_| AgentError::Internal)?;
            let first = prepare_turn_view(
                Arc::clone(&snapshot.history),
                range.clone(),
                metadata.interrupted.clone(),
                cursor,
                request.limit,
                max_bytes,
                limits.cancellation.clone(),
                limits.deadline,
                None,
            )
            .await?;
            check_budget(&limits.cancellation, limits.deadline)?;
            let identity = catalog.retain_stored_identity(StoredIdentity::new(
                key,
                range,
                first.result_id.clone(),
                first.total,
                metadata,
            ));
            prepared = Some(first);
            identity
        }
    };
    if request
        .result_id
        .as_deref()
        .is_some_and(|expected| expected != identity.result_id)
    {
        return Err(AgentError::StaleResult);
    }
    let encoded = if let Some(prepared) = prepared {
        prepared.encoded
    } else {
        prepare_turn_view(
            Arc::clone(&snapshot.history),
            identity.range.clone(),
            None,
            cursor,
            request.limit,
            max_bytes,
            limits.cancellation.clone(),
            limits.deadline,
            Some((identity.result_id.clone(), identity.total)),
        )
        .await?
        .encoded
    };
    let metadata = &identity.metadata;
    let base = TurnResultPage {
        result_id: Some(identity.result_id.clone()),
        availability,
        outcome: Some(metadata.outcome.clone()),
        persistence: Some(TurnPersistence::Persisted),
        usage: Some(metadata.usage),
        requests: Some(metadata.requests),
        tool_rounds: Some(metadata.tool_rounds),
        final_config_revision: Some(metadata.final_config_revision),
        completed_at: (availability == TurnResultAvailability::Stored)
            .then(|| metadata.completed_at.clone()),
        interrupted: metadata.interrupted.clone(),
        total: identity.total,
        ..empty_turn_page(request.turn, request.load)
    };
    assemble_turn_page(
        base,
        encoded,
        cursor,
        max_bytes,
        &limits.cancellation,
        limits.deadline,
    )
    .await
}

fn completion_outcome(
    outcome: Option<crate::sessions::CompletionOutcome>,
) -> Option<LoopOutcomeView> {
    use crate::sessions::CompletionOutcome;
    outcome.map(|outcome| match outcome {
        CompletionOutcome::Completed => LoopOutcomeView::Completed,
        CompletionOutcome::Cancelled => LoopOutcomeView::Cancelled {
            reason: crate::event::CancelReasonView::Unknown,
        },
        CompletionOutcome::Failed => LoopOutcomeView::Failed {
            kind: "unavailable_details".into(),
            model_error: None,
        },
        CompletionOutcome::Unknown => LoopOutcomeView::Unknown,
    })
}

fn empty_turn_page(turn: TurnRef, load: Option<String>) -> TurnResultPage {
    TurnResultPage {
        turn,
        load,
        result_id: None,
        persistence_uncertain: false,
        unavailable_reason: None,
        availability: TurnResultAvailability::Pending,
        outcome: None,
        persistence: None,
        usage: None,
        requests: None,
        tool_rounds: None,
        final_config_revision: None,
        completed_at: None,
        interrupted: None,
        items: Vec::new(),
        next_cursor: None,
        total: 0,
    }
}

fn unavailable_turn_page(
    turn: TurnRef,
    load: Option<String>,
    reason: crate::sessions::UnavailableReason,
    outcome: Option<LoopOutcomeView>,
    persistence: Option<TurnPersistence>,
    uncertain: bool,
    max_bytes: usize,
) -> Result<TurnResultPage, AgentError> {
    let mut page = empty_turn_page(turn, load);
    page.availability = TurnResultAvailability::Unavailable;
    page.unavailable_reason = Some(reason);
    page.outcome = outcome;
    page.persistence = persistence;
    page.persistence_uncertain = uncertain;
    if encoded_len(&page) > max_bytes {
        return Err(AgentError::InvalidArguments);
    }
    Ok(page)
}

#[cfg(test)]
pub(crate) fn prepare_result_source(
    items: &[HistoryItem],
    interrupted: Option<InterruptedRequest>,
) -> Result<crate::sessions::PreparedSource, AgentError> {
    let sanitized = sanitize_history(items)?;
    let times = vec![None; sanitized.len()];
    let encoded = encoded_items(0, &sanitized, &times)?
        .map(|item| item.map(|(_, value)| value))
        .collect::<Result<Vec<_>, _>>()?;
    crate::sessions::PreparedSource::new(encoded, interrupted)
}

#[derive(Serialize)]
struct SourceEnvelope<'a> {
    item: SourceItem<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum SourceItem<'a> {
    User(&'a minicore_runtime::history::UserHistory),
    Assistant(SourceAssistant<'a>),
    ToolResult(&'a minicore_runtime::history::ToolResultHistory),
    Summary(&'a minicore_runtime::history::SummaryHistory),
}

#[derive(Serialize)]
struct SourceAssistant<'a> {
    loop_id: LoopId,
    request_index: u32,
    model: &'a minicore_runtime::model::ModelRef,
    reasoning: minicore_runtime::model::ReasoningPreference,
    content: SourceParts<'a>,
    finish_reason: minicore_runtime::model::ModelFinishReason,
    usage: Usage,
}

impl<'a> SourceItem<'a> {
    fn from_history(item: &'a HistoryItem) -> Option<Self> {
        Some(match item {
            HistoryItem::User(item) => Self::User(item),
            HistoryItem::ToolResult(item) => Self::ToolResult(item),
            HistoryItem::Summary(item) => Self::Summary(item),
            HistoryItem::Assistant(item) if !item.content.iter().any(SourceParts::visible) => {
                return None;
            }
            HistoryItem::Assistant(item) => Self::Assistant(SourceAssistant {
                loop_id: item.loop_id,
                request_index: item.request_index,
                model: &item.model,
                reasoning: item.reasoning,
                content: SourceParts(&item.content),
                finish_reason: item.finish_reason,
                usage: item.usage,
            }),
        })
    }
}

struct SourceParts<'a>(&'a [minicore_runtime::model::AssistantPart]);

impl SourceParts<'_> {
    fn visible(part: &minicore_runtime::model::AssistantPart) -> bool {
        match part {
            minicore_runtime::model::AssistantPart::Reasoning(value) => {
                value.text().is_some() || value.summary().is_some()
            }
            _ => true,
        }
    }
}

impl Serialize for SourceParts<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use minicore_runtime::model::AssistantPart;
        use serde::ser::SerializeSeq as _;
        #[derive(Serialize)]
        #[serde(tag = "type", content = "data", rename_all = "snake_case")]
        enum Part<'a> {
            Text(&'a str),
            Reasoning(Reasoning<'a>),
            ToolCall(&'a minicore_runtime::model::ToolCall),
        }
        #[derive(Serialize)]
        struct Reasoning<'a> {
            text: Option<&'a str>,
            summary: Option<&'a str>,
            encrypted: Option<&'a str>,
            signature: Option<&'a str>,
        }
        let mut sequence = serializer.serialize_seq(None)?;
        for part in self.0.iter().filter(|part| Self::visible(part)) {
            let part = match part {
                AssistantPart::Text(text) => Part::Text(text),
                AssistantPart::ToolCall(call) => Part::ToolCall(call),
                AssistantPart::Reasoning(value) => Part::Reasoning(Reasoning {
                    text: value.text(),
                    summary: value.summary(),
                    encrypted: None,
                    signature: None,
                }),
            };
            sequence.serialize_element(&part)?;
        }
        sequence.end()
    }
}

pub(super) struct PreparedTurnItems {
    pub(super) result_id: String,
    pub(super) total: usize,
    pub(super) encoded: Vec<(usize, String)>,
}

#[cfg(test)]
pub(crate) static RESULT_PREPARE_GATES: crate::test_support::Registry<
    LoopId,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

pub(super) async fn prepare_turn_items(
    items: Arc<[HistoryItem]>,
    interrupted: Option<InterruptedRequest>,
    cursor: ReadCursor,
    limit: usize,
    max_bytes: usize,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<PreparedTurnItems, AgentError> {
    let range = 0..items.len();
    prepare_turn_view(
        items,
        range,
        interrupted,
        cursor,
        limit,
        max_bytes,
        cancellation,
        deadline,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_turn_view(
    items: Arc<[HistoryItem]>,
    range: std::ops::Range<usize>,
    interrupted: Option<InterruptedRequest>,
    cursor: ReadCursor,
    limit: usize,
    max_bytes: usize,
    cancellation: CancellationToken,
    deadline: Instant,
    identity: Option<(String, usize)>,
) -> Result<PreparedTurnItems, AgentError> {
    tokio::task::spawn_blocking(move || {
        let items = &items[range];
        #[cfg(feature = "e2e-observe")]
        let mut observed = crate::e2e_observe::Span::new(if identity.is_some() {
            crate::e2e_observe::Work::ResultRangeEncode
        } else {
            crate::e2e_observe::Work::ResultProjectionHash
        });
        check_budget(&cancellation, deadline)?;
        #[cfg(test)]
        if let Some(gate) = items.first().and_then(|item| match item {
            HistoryItem::User(user) => RESULT_PREPARE_GATES.take(&user.loop_id),
            _ => None,
        }) {
            gate.pass_blocking();
        }
        if items.len() > MAX_HISTORY_SCAN_LINES {
            return Err(AgentError::QueryLimit);
        }
        let total = identity.as_ref().map_or_else(
            || items.iter().filter_map(SourceItem::from_history).count(),
            |(_, total)| *total,
        );
        validate_cursor(cursor, total)?;
        let end = cursor.item.saturating_add(limit).min(total);
        let mut hash = identity.is_none().then(|| {
            let mut hash = Sha256::new();
            hash.update(b"minicore-turn-result-r1\0");
            hash.update((total as u64).to_be_bytes());
            #[cfg(feature = "e2e-observe")]
            observed.add(0, b"minicore-turn-result-r1\0".len() + 8);
            hash
        });
        let mut source_bytes = 0usize;
        let mut encoded = Vec::with_capacity(end.saturating_sub(cursor.item));
        let mut prepared_bytes = 0usize;
        struct HashWriter<'a>(&'a mut Sha256);
        impl std::io::Write for HashWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let range = if hash.is_some() {
            0..total
        } else {
            cursor.item..end
        };
        for (index, item) in items
            .iter()
            .filter_map(SourceItem::from_history)
            .enumerate()
            .skip(range.start)
            .take(range.len())
        {
            check_budget(&cancellation, deadline)?;
            if hash.is_none() && prepared_bytes >= max_bytes {
                break;
            }
            let envelope = SourceEnvelope {
                item,
                timestamp: None,
            };
            let bytes =
                crate::budget::json_len(&envelope).map_err(|_| AgentError::RpcSerialization)?;
            if let Some(hash) = &mut hash {
                source_bytes = source_bytes
                    .checked_add(bytes)
                    .ok_or(AgentError::QueryLimit)?;
                if source_bytes as u64 > MAX_HISTORY_SCAN_BYTES {
                    return Err(AgentError::QueryLimit);
                }
                hash.update((bytes as u64).to_be_bytes());
            }
            #[cfg(feature = "e2e-observe")]
            observed.add(1, bytes + if hash.is_some() { 8 } else { 0 });
            if (cursor.item..end).contains(&index) && prepared_bytes < max_bytes {
                let mut bytes = Vec::with_capacity(bytes);
                serde_json::to_writer(&mut bytes, &envelope)
                    .map_err(|_| AgentError::RpcSerialization)?;
                if let Some(hash) = &mut hash {
                    hash.update(&bytes);
                }
                prepared_bytes += bytes.len().saturating_sub(if index == cursor.item {
                    cursor.offset
                } else {
                    0
                });
                encoded.push((
                    index,
                    String::from_utf8(bytes).map_err(|_| AgentError::RpcSerialization)?,
                ));
            } else if let Some(hash) = &mut hash {
                serde_json::to_writer(HashWriter(hash), &envelope)
                    .map_err(|_| AgentError::RpcSerialization)?;
            }
        }
        let result_id = if let Some(mut hash) = hash {
            let partial_len =
                crate::budget::json_len(&interrupted).map_err(|_| AgentError::RpcSerialization)?;
            hash.update((partial_len as u64).to_be_bytes());
            #[cfg(feature = "e2e-observe")]
            observed.add(0, partial_len + 8);
            serde_json::to_writer(HashWriter(&mut hash), &interrupted)
                .map_err(|_| AgentError::RpcSerialization)?;
            format!("r1:{:x}", hash.finalize())
        } else {
            identity.expect("cached identity").0
        };
        check_budget(&cancellation, deadline)?;
        Ok(PreparedTurnItems {
            result_id,
            total,
            encoded,
        })
    })
    .await
    .map_err(|_| AgentError::Internal)?
}

pub(crate) fn encode_result_source(
    items: &[HistoryItem],
    interrupted: Option<InterruptedRequest>,
    metadata: crate::event::TurnResultView,
) -> Result<crate::sessions::PreparedSource, AgentError> {
    #[cfg(feature = "e2e-observe")]
    let mut observed = crate::e2e_observe::Span::new(crate::e2e_observe::Work::SourcePrepare);
    let mut encoded = Vec::with_capacity(items.len());
    for item in items.iter().filter_map(SourceItem::from_history) {
        let envelope = SourceEnvelope {
            item,
            timestamp: None,
        };
        let bytes =
            crate::budget::encode_json(&envelope).map_err(|_| AgentError::RpcSerialization)?;
        #[cfg(feature = "e2e-observe")]
        observed.add(1, bytes.len());
        encoded.push(String::from_utf8(bytes).map_err(|_| AgentError::RpcSerialization)?);
    }
    crate::sessions::PreparedSource::new(encoded, interrupted)?.with_metadata(metadata)
}

pub(crate) fn result_source_upper_bound(
    items: &[HistoryItem],
    interrupted: Option<&InterruptedRequest>,
    metadata: &crate::event::TurnResultView,
) -> Result<usize, AgentError> {
    let mut bytes = std::mem::size_of::<crate::sessions::results::ResultSource>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .ok_or(AgentError::Internal)?
        .checked_add(items.len().saturating_mul(std::mem::size_of::<String>()))
        .ok_or(AgentError::Internal)?;
    for item in items.iter().filter_map(SourceItem::from_history) {
        bytes = bytes
            .checked_add(crate::budget::encoded_len(&SourceEnvelope {
                item,
                timestamp: None,
            }))
            .ok_or(AgentError::Internal)?;
    }
    let interrupted_bytes = interrupted.map_or(0, |value| {
        value
            .model
            .capacity()
            .saturating_add(value.reasoning.capacity())
            .saturating_add(value.text.capacity())
    });
    bytes
        .checked_add(interrupted_bytes)
        .and_then(|bytes| {
            bytes.checked_add(crate::sessions::results::outcome_heap_bytes(
                &metadata.outcome,
            ))
        })
        .and_then(|bytes| bytes.checked_add(67))
        .ok_or(AgentError::Internal)
}

pub(super) async fn live_turn_page(
    request: &TurnResultRequest,
    result: Arc<crate::sessions::TurnResult>,
    limits: &HistoryScanLimits,
) -> Result<TurnResultPage, AgentError> {
    let metadata = crate::event::TurnResultView::from_turn_result(&result);
    let base = TurnResultPage {
        availability: TurnResultAvailability::Live,
        outcome: Some(metadata.outcome),
        persistence: Some(metadata.persistence),
        usage: Some(metadata.usage),
        requests: Some(metadata.requests),
        tool_rounds: Some(metadata.tool_rounds),
        final_config_revision: Some(metadata.final_config_revision),
        interrupted: result.interrupted.clone(),
        ..empty_turn_page(request.turn, request.load.clone())
    };
    project_turn_page(base, Arc::clone(&result.report.appended), request, limits).await
}

async fn retained_turn_page(
    request: TurnResultRequest,
    entry: crate::sessions::results::CompletionEntry,
    source: Arc<crate::sessions::results::ResultSource>,
    cursor: ReadCursor,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<TurnResultPage, AgentError> {
    #[cfg(feature = "e2e-observe")]
    let _page_refs = crate::e2e_observe::Lease::new(crate::e2e_observe::Gauge::PageRefs, 1);
    #[cfg(feature = "e2e-observe")]
    let _page_bytes = crate::e2e_observe::Lease::new(
        crate::e2e_observe::Gauge::PageRefBytes,
        source.observed_bytes(),
    );
    #[cfg(feature = "e2e-observe")]
    crate::e2e_observe::pause(
        crate::e2e_observe::Point::ResultPage,
        request.turn.session_id,
        request.turn.loop_id,
        cancellation.clone(),
    )
    .await;
    if request
        .result_id
        .as_deref()
        .is_some_and(|id| id != source.result_id)
    {
        return Err(AgentError::StaleResult);
    }
    let total = source.items.len();
    validate_cursor(cursor, total)?;
    let metadata = source.metadata.as_ref().expect("retained result metadata");
    let base = TurnResultPage {
        result_id: Some(source.result_id.clone()),
        persistence_uncertain: entry.persistence_uncertain,
        availability: TurnResultAvailability::Live,
        outcome: Some(metadata.outcome.clone()),
        persistence: entry.persistence,
        usage: Some(metadata.usage),
        requests: Some(metadata.requests),
        tool_rounds: Some(metadata.tool_rounds),
        final_config_revision: Some(metadata.final_config_revision),
        interrupted: source.interrupted.clone(),
        total,
        ..empty_turn_page(request.turn, request.load)
    };
    let end = cursor.item.saturating_add(request.limit).min(total);
    let encoded = source.items[cursor.item..end]
        .iter()
        .enumerate()
        .map(|(offset, item)| (cursor.item + offset, item.as_str()))
        .collect::<Vec<_>>();
    assemble_turn_page(base, encoded, cursor, max_bytes, cancellation, deadline).await
}

pub(super) async fn stored_turn_page(
    request: &TurnResultRequest,
    mut record: StoredLoopRecord,
    limits: &HistoryScanLimits,
) -> Result<TurnResultPage, AgentError> {
    let items = std::mem::take(&mut record.items).into();
    let base = TurnResultPage {
        availability: TurnResultAvailability::Stored,
        outcome: Some(record.outcome),
        persistence: Some(TurnPersistence::Persisted),
        usage: Some(record.usage),
        requests: Some(record.requests),
        tool_rounds: Some(record.tool_rounds),
        final_config_revision: Some(record.final_config_revision),
        completed_at: Some(record.completed_at),
        interrupted: record.interrupted,
        ..empty_turn_page(request.turn, request.load.clone())
    };
    project_turn_page(base, items, request, limits).await
}

async fn project_turn_page(
    mut page: TurnResultPage,
    items: Arc<[HistoryItem]>,
    request: &TurnResultRequest,
    limits: &HistoryScanLimits,
) -> Result<TurnResultPage, AgentError> {
    let cursor = request.cursor.unwrap_or_else(ReadCursor::start);
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    let prepared = prepare_turn_items(
        items,
        page.interrupted.clone(),
        cursor,
        request.limit,
        max_bytes,
        limits.cancellation.clone(),
        limits.deadline,
    )
    .await?;
    if request
        .result_id
        .as_ref()
        .is_some_and(|id| *id != prepared.result_id)
    {
        return Err(AgentError::StaleResult);
    }
    page.result_id = Some(prepared.result_id);
    page.total = prepared.total;
    assemble_turn_page(
        page,
        prepared.encoded,
        cursor,
        max_bytes,
        &limits.cancellation,
        limits.deadline,
    )
    .await
}

/// Packs the prepared range without normalizing the entire turn again.
async fn assemble_turn_page<I, S>(
    base: TurnResultPage,
    encoded: I,
    cursor: ReadCursor,
    max_bytes: usize,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<TurnResultPage, AgentError>
where
    I: IntoIterator<Item = (usize, S)>,
    S: AsRef<str>,
{
    let total = base.total;
    let base_bytes = encoded_len(&base);
    let encoded = encoded
        .into_iter()
        .map(Ok::<_, AgentError>)
        .collect::<Vec<_>>();
    let (items, next_cursor) = pack_items(
        encoded,
        cursor,
        total,
        max_bytes,
        cancellation,
        deadline,
        |_, _, next_cursor| Ok(base_bytes + encoded_len(&next_cursor) - 4),
    )
    .await?;
    Ok(TurnResultPage {
        items,
        next_cursor,
        ..base
    })
}
