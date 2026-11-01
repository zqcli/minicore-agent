//! Query projections own only requested tool data.
use super::paging::ToolOutputView;
use super::record::{ToolRecord, stream_availability};
use super::*;
use crate::budget::check_budget;
use crate::error::{AgentError, StoreError};
use crate::read::{DEFAULT_READ_MAX_BYTES, READ_DEADLINE};
use crate::store::{Store, StoredToolRecord};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(crate) enum ToolReadScope {
    Summary,
    Stream(ToolDataStream),
}

pub(crate) type ToolBlob = (Option<Vec<u8>>, bool);

pub(crate) enum ToolReadPayload {
    Summary { input: ToolBlob, result: ToolBlob },
    Stream(ToolBlob),
}

pub(crate) struct StoredToolRead {
    pub(crate) metadata: StoredToolRecord,
    pub(crate) payload: ToolReadPayload,
}

pub(super) struct ReadMetadata {
    pub(super) execution: ToolExecutionData,
    pub(super) input_seen: bool,
    result_seen: bool,
    stdout_end: Option<u64>,
    stderr_end: Option<u64>,
}

impl ReadMetadata {
    fn from_record(record: &ToolRecord, tool_ref: &ToolRef) -> Self {
        let end = |window: &super::record::StreamWindow| {
            (window.seen || window.complete || window.expired || window.observed_end > 0)
                .then_some(window.observed_end)
        };
        Self {
            execution: record.execution_data(tool_ref),
            input_seen: record.input_seen,
            result_seen: record.result_seen,
            stdout_end: end(&record.stdout),
            stderr_end: end(&record.stderr),
        }
    }

    fn from_stored(record: &StoredToolRecord) -> Self {
        let terminal = record.state.is_terminal();
        let end = |window: &crate::store::StoredStreamWindow| {
            (window.seen || window.complete || window.expired || window.observed_end > 0)
                .then_some(window.observed_end)
        };
        let mut command = record.command.clone();
        if let Some(command) = &mut command {
            command.stdout_base_offset = record.stdout.start_offset;
            command.stdout_observed_end = record.stdout.observed_end;
            command.stderr_base_offset = record.stderr.start_offset;
            command.stderr_observed_end = record.stderr.observed_end;
            command.output_truncated |= record.stdout.truncated
                || record.stderr.truncated
                || record.stdout.expired
                || record.stderr.expired;
        }
        Self {
            execution: ToolExecutionData {
                tool_ref: record.tool_ref.clone(),
                name: record.name.clone(),
                state: record.state,
                phase: record.phase,
                started_at: record.started_at.clone(),
                finished_at: record.finished_at.clone(),
                outcome: record.outcome,
                input_availability: stream_availability(
                    record.input.seen,
                    record.input.expired,
                    terminal,
                    record.input.file_bytes,
                    record.input.total_bytes,
                    record.input.truncated,
                ),
                output_availability: stream_availability(
                    record.result.seen,
                    record.result.expired,
                    terminal,
                    record.result.file_bytes,
                    record.result.total_bytes,
                    record.result.truncated,
                ),
                input_bytes: record.input.total_bytes,
                result_bytes: record.result.total_bytes,
                input_truncated: record.input.truncated
                    || record.input.file_bytes < record.input.total_bytes,
                result_truncated: record.result.truncated
                    || record.result.file_bytes < record.result.total_bytes,
                output_line_count: None,
                command,
                recording: ToolRecordingState::Saved,
            },
            input_seen: record.input.seen,
            result_seen: record.result.seen,
            stdout_end: end(&record.stdout),
            stderr_end: end(&record.stderr),
        }
    }

    fn terminal(&self) -> bool {
        self.execution.state.is_terminal()
            && self
                .execution
                .command
                .as_ref()
                .is_none_or(|command| command.status.is_terminal())
    }

    pub(super) fn compatible(&self, disk: &Self) -> bool {
        let warm = &self.execution;
        let cold = &disk.execution;
        let command = |value: &CommandResult| {
            (
                value.status,
                value.exit_code,
                value.signal,
                value.termination_confirmed,
                value.output_complete,
            )
        };
        warm.name == cold.name
            && (!warm.state.is_terminal() || !cold.state.is_terminal() || warm.state == cold.state)
            && warm.outcome.zip(cold.outcome).is_none_or(|(a, b)| a == b)
            && (!self.input_seen || !disk.input_seen || warm.input_bytes == cold.input_bytes)
            && (!self.result_seen || !disk.result_seen || warm.result_bytes == cold.result_bytes)
            && self
                .stdout_end
                .zip(disk.stdout_end)
                .is_none_or(|(a, b)| a == b)
            && self
                .stderr_end
                .zip(disk.stderr_end)
                .is_none_or(|(a, b)| a == b)
            && warm
                .command
                .as_ref()
                .zip(cold.command.as_ref())
                .is_none_or(|(a, b)| command(a) == command(b))
    }
}

pub(super) struct ToolSummary {
    pub(super) metadata: ReadMetadata,
    pub(super) subject: ToolSubject,
    pub(super) subject_truncated: bool,
    pub(super) input: String,
}

impl ToolSummary {
    pub(super) fn needs_stored(&self) -> bool {
        self.metadata.terminal()
            && (!usable(self.metadata.execution.input_availability)
                || !usable(self.metadata.execution.output_availability))
    }

    fn from_stored(source: StoredToolRead) -> Self {
        let ToolReadPayload::Summary { input, result } = source.payload else {
            unreachable!()
        };
        let (input, input_corrupt) = decode_text(input);
        let (result, result_corrupt) = decode_text(result);
        let mut metadata = ReadMetadata::from_stored(&source.metadata);
        if input_corrupt {
            metadata.execution.input_availability = ToolDataAvailability::Unavailable;
        }
        if result_corrupt {
            metadata.execution.output_availability = ToolDataAvailability::Unavailable;
        } else if metadata.result_seen && !source.metadata.result.expired {
            metadata.execution.output_line_count = Some(crate::presentation::count_lines(&result));
        }
        Self {
            metadata,
            subject: source.metadata.subject,
            subject_truncated: source.metadata.subject_truncated,
            input,
        }
    }

    pub(super) fn merge(&mut self, disk: Self) {
        if !self.metadata.compatible(&disk.metadata) {
            return;
        }
        let warm = &mut self.metadata.execution;
        let cold = disk.metadata.execution;
        if !self.metadata.input_seen && warm.started_at.is_none() {
            warm.phase = warm.phase.or(cold.phase);
            warm.started_at = cold.started_at;
            warm.finished_at = cold.finished_at.or(warm.finished_at.take());
        }
        if (!self.metadata.input_seen && disk.metadata.input_seen)
            || (!usable(warm.input_availability)
                && (usable(cold.input_availability)
                    || (disk.metadata.input_seen
                        && cold.input_availability == ToolDataAvailability::Unavailable)))
        {
            self.input = disk.input;
            self.subject = disk.subject;
            self.subject_truncated = disk.subject_truncated;
            self.metadata.input_seen = disk.metadata.input_seen;
            warm.input_bytes = cold.input_bytes;
            warm.input_truncated = cold.input_truncated;
            warm.input_availability = cold.input_availability;
        }
        if (!self.metadata.result_seen && disk.metadata.result_seen)
            || (!usable(warm.output_availability)
                && (usable(cold.output_availability)
                    || (disk.metadata.result_seen
                        && cold.output_availability == ToolDataAvailability::Unavailable)))
        {
            self.metadata.result_seen = disk.metadata.result_seen;
            warm.result_bytes = cold.result_bytes;
            warm.result_truncated = cold.result_truncated;
            warm.output_line_count = cold.output_line_count;
            warm.output_availability = cold.output_availability;
        }
        if warm.command.is_none() {
            warm.command = cold.command;
        }
        if warm.recording == ToolRecordingState::MemoryOnly {
            warm.recording = cold.recording;
        }
    }
}

impl ToolRecord {
    pub(super) fn read_summary(&self, tool_ref: &ToolRef) -> ToolSummary {
        ToolSummary {
            metadata: ReadMetadata::from_record(self, tool_ref),
            subject: self.subject.clone(),
            subject_truncated: self.subject_truncated,
            input: self.input.clone(),
        }
    }
}

fn decode_text((bytes, corrupt): ToolBlob) -> (String, bool) {
    match String::from_utf8(bytes.unwrap_or_default()) {
        Ok(text) => (text, corrupt),
        Err(_) => (String::new(), true),
    }
}

fn usable(availability: ToolDataAvailability) -> bool {
    matches!(
        availability,
        ToolDataAvailability::Available | ToolDataAvailability::Partial
    )
}

async fn read_source(
    store: &Store,
    tool_ref: &ToolRef,
    scope: ToolReadScope,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<Option<StoredToolRead>, AgentError> {
    check_budget(cancellation, deadline)?;
    #[cfg(test)]
    if let Some(gate) = TOOL_READ_GATES.take(tool_ref) {
        tokio::select! {
            _ = cancellation.cancelled() => return Err(AgentError::QueryLimit),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(AgentError::QueryLimit),
            _ = gate.pass() => {},
        }
    }
    tokio::select! {
        _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
        result = store.read_tool_source(tool_ref, scope, deadline) => {
            result.map_err(|error| match error {
                StoreError::SessionNotFound => AgentError::ToolNotFound,
                error => error.into(),
            })
        }
    }
}

pub(crate) async fn tool_read(
    store: Store,
    tool_data: Option<Arc<ToolData>>,
    request: ToolReadRequest,
    cancellation: CancellationToken,
) -> Result<ToolReadResult, AgentError> {
    request.validate()?;
    let deadline = Instant::now() + READ_DEADLINE;
    check_budget(&cancellation, deadline)?;
    let mut warm = tool_data.as_ref().and_then(|data| {
        data.lock()
            .records
            .get(&request.tool_ref)
            .map(|record| record.read_summary(&request.tool_ref))
    });
    if warm.as_ref().is_none_or(ToolSummary::needs_stored) {
        match read_source(
            &store,
            &request.tool_ref,
            ToolReadScope::Summary,
            &cancellation,
            deadline,
        )
        .await
        {
            Ok(Some(disk)) => match &mut warm {
                Some(warm) => warm.merge(ToolSummary::from_stored(disk)),
                None => warm = Some(ToolSummary::from_stored(disk)),
            },
            Err(error) if warm.is_none() || matches!(error, AgentError::QueryLimit) => {
                return Err(error);
            }
            _ => {}
        }
    }
    let source = warm.ok_or(AgentError::ToolNotFound)?;
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    if request.display {
        source.project_display_read(max_bytes)
    } else {
        source.project_read(max_bytes)
    }
}

pub(crate) async fn tool_output(
    store: Store,
    tool_data: Option<Arc<ToolData>>,
    request: ToolOutputRequest,
    cancellation: CancellationToken,
) -> Result<ToolOutputPage, AgentError> {
    request.validate()?;
    let deadline = Instant::now() + READ_DEADLINE;
    check_budget(&cancellation, deadline)?;
    let max_bytes = request.max_bytes.unwrap_or(DEFAULT_READ_MAX_BYTES);
    let warm = tool_data.as_ref().and_then(|data| {
        let inner = data.lock();
        let record = inner.records.get(&request.tool_ref)?;
        let metadata = ReadMetadata::from_record(record, &request.tool_ref);
        let view = record.output_view(request.stream);
        let needs_stored = metadata.terminal() && !usable(view.availability);
        Some((
            metadata,
            view.known(),
            needs_stored,
            view.project(&request, max_bytes),
        ))
    });
    if warm.as_ref().is_some_and(|(_, _, needed, _)| !needed) {
        return warm.unwrap().3;
    }
    let disk = read_source(
        &store,
        &request.tool_ref,
        ToolReadScope::Stream(request.stream),
        &cancellation,
        deadline,
    )
    .await;
    match disk {
        Ok(Some(source)) => {
            let metadata = ReadMetadata::from_stored(&source.metadata);
            let ToolReadPayload::Stream(blob) = &source.payload else {
                unreachable!()
            };
            let view = ToolOutputView::from_stored(&source.metadata, request.stream, blob);
            if warm.as_ref().is_none_or(|(warm, known, _, _)| {
                warm.compatible(&metadata)
                    && (usable(view.availability) || view.corrupt || (!known && view.known()))
            }) {
                return view.project(&request, max_bytes);
            }
        }
        Err(error) if warm.is_none() || matches!(error, AgentError::QueryLimit) => {
            return Err(error);
        }
        _ => {}
    }
    warm.map(|(_, _, _, page)| page)
        .unwrap_or(Err(AgentError::ToolNotFound))
}

#[cfg(test)]
pub(crate) static TOOL_READ_GATES: crate::test_support::Registry<
    ToolRef,
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();
