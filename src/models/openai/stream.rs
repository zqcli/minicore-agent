use super::*;

pub(super) type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

pub(super) struct StreamState {
    bytes: ByteStream,
    pub(super) cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
    parser: SseParser,
    pub(super) pending: VecDeque<Result<ModelEvent, ModelError>>,
    pub(super) tools: BTreeMap<u32, ToolState>,
    pub(super) tool_ids: BTreeSet<ToolCallId>,
    pub(super) tool_order: Vec<u32>,
    pub(super) output_evidence: BTreeMap<u32, replay::ItemEvidence>,
    pub(super) output_ids: BTreeSet<String>,
    pub(super) output_items: Option<BTreeMap<u32, Value>>,
    pub(super) output_item_bytes: usize,
    replay_identity: Option<(String, String)>,
    pub(super) text: String,
    pub(super) trace: Option<RequestTraceContext>,
    pub(super) provider_event_seen: bool,
    pub(super) output_seen: bool,
    pub(super) reasoning_seen: bool,
    pub(super) refusal_seen: bool,
    pub(super) terminal_seen: bool,
    pub(super) reasoning_parts: ReasoningParts,
    pub(super) done: bool,
}

pub(super) struct ToolState {
    pub(super) tool_call_id: ToolCallId,
    pub(super) name: ToolName,
    pub(super) arguments: String,
    pub(super) arguments_done: bool,
    pub(super) item_done: bool,
    pub(super) ended: bool,
}

impl StreamState {
    pub(super) fn new_with_replay(
        bytes: ByteStream,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: TokioInstant,
        replay_identity: Option<(String, String)>,
        trace: Option<RequestTraceContext>,
    ) -> Self {
        Self {
            bytes,
            cancellation,
            deadline,
            parser: SseParser::default(),
            pending: VecDeque::new(),
            tools: BTreeMap::new(),
            tool_ids: BTreeSet::new(),
            tool_order: Vec::new(),
            output_evidence: BTreeMap::new(),
            output_ids: BTreeSet::new(),
            output_items: Some(BTreeMap::new()),
            output_item_bytes: 0,
            replay_identity,
            text: String::new(),
            trace,
            provider_event_seen: false,
            output_seen: false,
            reasoning_seen: false,
            refusal_seen: false,
            terminal_seen: false,
            reasoning_parts: ReasoningParts::default(),
            done: false,
        }
    }

    pub(super) fn queue(&mut self, event: ModelEvent) {
        self.pending.push_back(Ok(event));
    }

    pub(super) fn fail(&mut self, error: ModelError) {
        if let Some(trace) = self.trace {
            tracing::warn!(
                loop_id = %trace.loop_id,
                request_index = trace.request_index,
                error_kind = ?error.kind(),
                delivery = ?error.delivery(),
                "provider stream failed"
            );
        }
        self.done = true;
        self.pending.push_back(Err(error));
    }

    fn malformed(&mut self) {
        let error = stream_error(
            ModelErrorKind::InvalidProviderResponse,
            self.provider_event_seen,
        );
        self.fail(error);
    }

    pub(super) fn finalize_output_items(
        &mut self,
        terminal_output: Option<Vec<Value>>,
    ) -> Result<(), ()> {
        let Some(terminal_output) = terminal_output else {
            return Ok(());
        };
        if !terminal_output.is_empty() {
            replay::validate_output(&terminal_output)?;
        }
        for (output_index, evidence) in &self.output_evidence {
            let output_index = usize::try_from(*output_index).map_err(|_| ())?;
            if !terminal_output
                .get(output_index)
                .is_some_and(|terminal| evidence.matches(terminal))
            {
                return Err(());
            }
        }
        self.output_items = Some(
            terminal_output
                .into_iter()
                .enumerate()
                .map(|(index, item)| (index as u32, item))
                .collect(),
        );
        Ok(())
    }

    pub(super) fn replay_event(&mut self) -> Result<Option<ModelEvent>, ()> {
        let Some((endpoint, model)) = &self.replay_identity else {
            return Ok(None);
        };
        let Some(output) = self.output_items.as_ref() else {
            return self.without_replay();
        };
        if !self.tools.is_empty()
            && self.reasoning_seen
            && !output
                .values()
                .any(|item| item_type(item) == Some("reasoning"))
        {
            return Err(());
        }
        if output.is_empty() {
            return Ok(None);
        }
        if !output
            .keys()
            .copied()
            .eq(0..u32::try_from(output.len()).map_err(|_| ())?)
        {
            return Err(());
        }
        if !self
            .tools
            .keys()
            .copied()
            .eq(self.tool_order.iter().copied())
        {
            return Err(());
        }
        for (index, item) in output {
            if item_type(item) != Some("function_call") {
                continue;
            }
            let tool = self.tools.get(index).ok_or(())?;
            if item["call_id"] != tool.tool_call_id.as_str()
                || item["name"] != tool.name.as_str()
                || serde_json::from_str::<Value>(item["arguments"].as_str().ok_or(())?)
                    .map_err(|_| ())?
                    != serde_json::from_str::<Value>(&tool.arguments).map_err(|_| ())?
            {
                return Err(());
            }
        }
        for index in self.tools.keys() {
            if output.get(index).and_then(item_type) != Some("function_call") {
                return Err(());
            }
        }
        if replay::visible_text(output.values())? != self.text {
            return Err(());
        }
        let output = self
            .output_items
            .take()
            .expect("checked output")
            .into_values()
            .collect();
        match replay::capture(endpoint, model, output)? {
            Some(replay) => Ok(Some(ModelEvent::ProviderReplay { replay })),
            None => self.without_replay(),
        }
    }

    fn without_replay(&self) -> Result<Option<ModelEvent>, ()> {
        if self.text.is_empty() && self.tools.is_empty() && !self.reasoning_parts.emitted {
            Err(())
        } else {
            Ok(None)
        }
    }
}

pub(super) async fn next_stream_event(
    mut state: StreamState,
) -> Option<(Result<ModelEvent, ModelError>, StreamState)> {
    loop {
        if let Some(event) = state.pending.pop_front() {
            return Some((event, state));
        }
        if state.done {
            return None;
        }
        let next = tokio::select! {
            biased;
            _ = state.cancellation.cancelled() => {
                let error = stream_error(ModelErrorKind::Cancelled, state.provider_event_seen);
                state.fail(error);
                continue;
            }
            _ = tokio::time::sleep_until(state.deadline) => {
                let error = stream_error(ModelErrorKind::Timeout, state.provider_event_seen);
                state.fail(error);
                continue;
            }
            next = state.bytes.next() => next,
        };
        match next {
            Some(Ok(chunk)) => {
                let parser_failed = state.parser.feed(&chunk).is_err();
                while let Some(frame) = state.parser.pop_frame() {
                    if handle_frame(&mut state, &frame).is_err() {
                        state.malformed();
                        break;
                    }
                    if state.done {
                        break;
                    }
                }
                if parser_failed && !state.done {
                    state.malformed();
                }
                if state.terminal_seen {
                    drain_body(&mut state).await;
                    state.bytes = Box::pin(futures_stream::empty());
                }
            }
            Some(Err(error)) => {
                let kind = if error.is_timeout() {
                    ModelErrorKind::Timeout
                } else if state.output_seen {
                    ModelErrorKind::StreamInterrupted
                } else {
                    ModelErrorKind::RequestOutcomeUnknown
                };
                let error = stream_error(kind, state.provider_event_seen);
                state.fail(error);
            }
            None => {
                let kind = if state.output_seen {
                    ModelErrorKind::StreamInterrupted
                } else {
                    ModelErrorKind::IncompleteResponse
                };
                let error = stream_error(kind, state.provider_event_seen);
                state.fail(error);
            }
        }
    }
}

// A terminal result is authoritative; draining is only a pooling optimization.
async fn drain_body(state: &mut StreamState) {
    let deadline = TokioInstant::now() + DRAIN_TIMEOUT;
    if deadline + DRAIN_TIMEOUT >= state.deadline {
        return;
    }
    let mut remaining = MAX_DRAIN_BYTES;
    loop {
        let chunk = tokio::select! {
            biased;
            _ = state.cancellation.cancelled() => break,
            _ = tokio::time::sleep_until(deadline) => break,
            chunk = state.bytes.next() => chunk,
        };
        match chunk {
            Some(Ok(chunk)) if chunk.len() < remaining => remaining -= chunk.len(),
            _ => break,
        }
    }
}

pub(super) fn queue_text(state: &mut StreamState, text: String, reasoning: bool) {
    if !reasoning {
        state.text.push_str(&text);
    }
    for chunk in event_text_chunks(&text) {
        let event = if reasoning {
            ModelEvent::reasoning_delta(chunk)
        } else {
            ModelEvent::text_delta(chunk)
        }
        .expect("event chunk fits the runtime byte limit");
        state.queue(event);
    }
}

pub(super) fn event_text_chunks(value: &str) -> impl Iterator<Item = &str> {
    let mut remaining = Some(value);
    std::iter::from_fn(move || {
        let value = remaining.take()?;
        let (chunk, rest) =
            value.split_at(crate::text::floor_char_boundary(value, MAX_EVENT_BYTES));
        remaining = (!rest.is_empty()).then_some(rest);
        Some(chunk)
    })
}
