//! Retained tool records, stream windows, and durable snapshots.
use super::types::{
    CommandResult, ToolDataAvailability, ToolDataStream, ToolExecutionData, ToolExecutionState,
    ToolPhase, ToolRecordingState, ToolRef, ToolSubject,
};
use super::{COMMAND_METADATA_BYTES, MAX_TOOL_STREAM_BYTES};
use crate::changes::{CHANGE_METADATA_BYTES, FileChange};
use crate::store::{StoredStreamWindow, StoredToolRecord};
use minicore_runtime::tools::ToolResultOutcome;
use std::collections::VecDeque;
use std::fmt;

/// A bounded tail window of one raw process stream. `start_offset` is the raw
/// offset of the first retained byte, so eviction moves `base_offset` forward
/// instead of renumbering bytes; `observed_end` counts every byte the owner
/// really read, whether or not it is still retained.
#[derive(Clone, Default)]
pub(super) struct StreamWindow {
    pub(super) bytes: VecDeque<u8>,
    pub(super) start_offset: u64,
    pub(super) observed_end: u64,
    pub(super) seen: bool,
    /// The stream reached a real end of output.
    pub(super) complete: bool,
    /// Bytes were dropped, either by the window or by an eviction.
    pub(super) truncated: bool,
    /// The retained bytes were freed under the Session budget.
    pub(super) expired: bool,
    /// The backing blob was corrupt or missing on disk during cold read.
    pub(super) corrupt: bool,
}

/// What one pushed chunk changed, measured against the raw stream.
pub(super) struct StreamPush {
    pub(super) base_offset: u64,
    pub(super) next_offset: u64,
    pub(super) dropped: bool,
}

impl StreamWindow {
    fn from_stored(stored: StoredStreamWindow, bytes: Option<Vec<u8>>, corrupt: bool) -> Self {
        Self {
            bytes: bytes.unwrap_or_default().into(),
            start_offset: stored.start_offset,
            observed_end: stored.observed_end,
            seen: stored.seen,
            complete: stored.complete,
            truncated: stored.truncated,
            expired: stored.expired,
            corrupt,
        }
    }

    fn merge_stored(&mut self, stored: Self) {
        if !self.seen && !self.complete && self.observed_end == 0 {
            *self = stored;
        } else if self.expired || self.corrupt {
            if (stored.seen || stored.complete) && !stored.expired && !stored.corrupt {
                *self = stored;
            } else if stored.corrupt {
                self.corrupt = true;
                if stored.observed_end > 0 {
                    self.observed_end = stored.observed_end;
                    self.start_offset = stored.start_offset;
                }
            }
        } else if stored.complete {
            self.complete = true;
        }
    }

    pub(super) fn snapshot(self) -> (StoredStreamWindow, Option<Vec<u8>>) {
        let (bytes, file_bytes, file_sha256) = snapshot_blob(self.bytes.into(), self.expired);
        (
            StoredStreamWindow {
                start_offset: self.start_offset,
                observed_end: self.observed_end,
                seen: self.seen,
                complete: self.complete,
                truncated: self.truncated,
                expired: self.expired,
                file_bytes,
                file_sha256,
            },
            bytes,
        )
    }

    /// Appends one chunk and evicts the oldest bytes beyond the window.
    pub(super) fn push(&mut self, chunk: &[u8]) -> StreamPush {
        self.seen = true;
        if self.bytes.is_empty() {
            // Nothing is retained (first byte, or retention was evicted), so
            // the retained window starts where this chunk starts.
            self.start_offset = self.observed_end;
        }
        let base_offset = self.observed_end;
        self.observed_end = self.observed_end.saturating_add(chunk.len() as u64);
        if chunk.is_empty() {
            return StreamPush {
                base_offset,
                next_offset: self.observed_end,
                dropped: false,
            };
        }
        self.expired = false;
        let old_len = self.bytes.len();
        let total = old_len.saturating_add(chunk.len());
        let dropped;
        if total <= MAX_TOOL_STREAM_BYTES {
            dropped = false;
            if total > self.bytes.capacity() {
                let target = (self.bytes.capacity().saturating_mul(2))
                    .max(total)
                    .min(MAX_TOOL_STREAM_BYTES);
                let additional = target - old_len;
                self.bytes.reserve_exact(additional);
            }
            self.bytes.extend(chunk);
        } else {
            let excess = total - MAX_TOOL_STREAM_BYTES;
            self.start_offset = self.start_offset.saturating_add(excess as u64);
            self.truncated = true;
            dropped = true;
            let old_dropped = excess.min(old_len);
            let chunk_dropped = excess - old_dropped;
            if old_dropped == old_len {
                self.bytes.clear();
            } else if old_dropped > 0 {
                self.bytes.drain(..old_dropped);
            }
            let chunk_slice = &chunk[chunk_dropped..];
            if self.bytes.capacity() < MAX_TOOL_STREAM_BYTES {
                let additional = MAX_TOOL_STREAM_BYTES - self.bytes.len();
                self.bytes.reserve_exact(additional);
            }
            self.bytes.extend(chunk_slice);
        }
        StreamPush {
            base_offset,
            next_offset: self.observed_end,
            dropped,
        }
    }

    pub(super) fn retained(&self) -> usize {
        if self.expired {
            0
        } else {
            self.bytes.capacity()
        }
    }

    /// The retained range as `(base_offset, observed_end)`, even when nothing
    /// is retained or the window was evicted. The observed end never moves
    /// backwards, so a live command record can report progress while running.
    fn live_range(&self) -> (u64, u64) {
        (self.start_offset, self.observed_end)
    }

    /// The retained bytes from one raw offset, when that offset is still
    /// inside the window.
    pub(super) fn page(&self, offset: u64) -> Option<(&[u8], &[u8])> {
        if offset < self.start_offset {
            return None;
        }
        let index = usize::try_from(offset - self.start_offset).ok()?;
        let (first, second) = self.bytes.as_slices();
        if index < first.len() {
            Some((&first[index..], second))
        } else {
            Some((second.get(index - first.len()..)?, &[]))
        }
    }

    pub(super) fn evict(&mut self) {
        self.bytes = VecDeque::new();
        self.start_offset = self.observed_end;
        self.expired = true;
        self.truncated = true;
    }
}

#[derive(Clone)]
pub(crate) struct ToolRecord {
    pub(super) name: String,
    pub(super) subject: ToolSubject,
    pub(super) subject_truncated: bool,
    pub(super) input: String,
    pub(super) input_total: usize,
    pub(super) input_seen: bool,
    pub(super) input_truncated: bool,
    pub(super) input_expired: bool,
    pub(super) result: String,
    pub(super) output_line_count: Option<usize>,
    pub(super) result_total: usize,
    pub(super) result_seen: bool,
    pub(super) result_truncated: bool,
    pub(super) result_expired: bool,
    pub(super) state: ToolExecutionState,
    pub(super) phase: Option<ToolPhase>,
    pub(super) started_at: Option<String>,
    pub(super) finished_at: Option<String>,
    pub(super) outcome: Option<ToolResultOutcome>,
    /// Bounded tail windows of the process streams; empty until a command
    /// really owned a process.
    pub(super) stdout: StreamWindow,
    pub(super) stderr: StreamWindow,
    /// The structured process record; absent until a command was owned.
    pub(super) command: Option<CommandResult>,
    /// One native file mutation, if the bound tool reached its mutation seam.
    pub(super) file_change: Option<FileChange>,
    pub(super) input_corrupt: bool,
    pub(super) result_corrupt: bool,
    pub(super) recording: ToolRecordingState,
}

impl ToolRecord {
    pub(crate) fn has_corrupt_streams(&self) -> bool {
        self.input_corrupt || self.result_corrupt || self.stdout.corrupt || self.stderr.corrupt
    }

    pub(crate) fn is_terminal(&self) -> bool {
        if !self.state.is_terminal() {
            return false;
        }
        if let Some(cmd) = &self.command {
            if !cmd.status.is_terminal() {
                return false;
            }
        }
        true
    }

    pub(crate) fn needs_stored(&self) -> bool {
        self.is_terminal()
            && (self.input_expired
                || self.result_expired
                || self.stdout.expired
                || self.stderr.expired
                || !self.input_seen
                || !self.result_seen
                || self
                    .file_change
                    .as_ref()
                    .is_some_and(FileChange::needs_stored)
                || self.has_corrupt_streams())
    }

    pub(crate) fn merge_stored(&mut self, stored: ToolRecord) -> bool {
        // 1. Conflict checks: refuse merge on any identity or observed range conflict.
        if self.name != stored.name {
            return false;
        }
        if self.state.is_terminal() && stored.state.is_terminal() && self.state != stored.state {
            return false;
        }
        if self.outcome.is_some() && stored.outcome.is_some() && self.outcome != stored.outcome {
            return false;
        }
        if self.input_seen && stored.input_seen && self.input_total != stored.input_total {
            return false;
        }
        if self.result_seen && stored.result_seen && self.result_total != stored.result_total {
            return false;
        }
        let known = |stream: &StreamWindow| {
            stream.seen || stream.complete || stream.expired || stream.observed_end > 0
        };
        for (memory, disk) in [
            (&self.stdout, &stored.stdout),
            (&self.stderr, &stored.stderr),
        ] {
            if known(memory) && known(disk) && memory.observed_end != disk.observed_end {
                return false;
            }
        }
        if self
            .command
            .as_ref()
            .zip(stored.command.as_ref())
            .is_some_and(|(memory, disk)| {
                (
                    memory.status,
                    memory.exit_code,
                    memory.signal,
                    memory.termination_confirmed,
                    memory.output_complete,
                ) != (
                    disk.status,
                    disk.exit_code,
                    disk.signal,
                    disk.termination_confirmed,
                    disk.output_complete,
                )
            })
        {
            return false;
        }
        if self
            .file_change
            .as_ref()
            .zip(stored.file_change.as_ref())
            .is_some_and(|(memory, disk)| memory.stored() != disk.stored())
        {
            return false;
        }

        if !self.input_seen && self.started_at.is_none() {
            self.phase = self.phase.or(stored.phase);
            self.started_at = stored.started_at;
            if stored.finished_at.is_some() {
                self.finished_at = stored.finished_at;
            }
        }

        // 2. Input
        if !self.input_seen {
            if stored.input_seen {
                self.input = stored.input;
                self.input_total = stored.input_total;
                self.input_seen = true;
                self.input_truncated = stored.input_truncated;
                self.input_expired = stored.input_expired;
                self.input_corrupt = stored.input_corrupt;
                self.subject = stored.subject;
                self.subject_truncated = stored.subject_truncated;
            }
        } else if self.input_expired || self.input_corrupt {
            if stored.input_seen && !stored.input_expired && !stored.input_corrupt {
                self.input = stored.input;
                self.input_total = stored.input_total;
                self.input_truncated = stored.input_truncated;
                self.input_expired = false;
                self.input_corrupt = false;
            } else if stored.input_corrupt {
                self.input_corrupt = true;
                self.input_total = stored.input_total;
            }
        }

        // 3. Result
        if !self.result_seen {
            if stored.result_seen {
                self.result = stored.result;
                self.output_line_count = stored.output_line_count;
                self.result_total = stored.result_total;
                self.result_seen = true;
                self.result_truncated = stored.result_truncated;
                self.result_expired = stored.result_expired;
                self.result_corrupt = stored.result_corrupt;
            }
        } else if self.result_expired || self.result_corrupt {
            if stored.result_seen && !stored.result_expired && !stored.result_corrupt {
                self.result = stored.result;
                self.output_line_count = stored.output_line_count;
                self.result_total = stored.result_total;
                self.result_truncated = stored.result_truncated;
                self.result_expired = false;
                self.result_corrupt = false;
            } else if stored.result_corrupt {
                self.result_corrupt = true;
                self.result_total = stored.result_total;
            }
        }

        self.stdout.merge_stored(stored.stdout);
        self.stderr.merge_stored(stored.stderr);

        // 5. Command record
        if self.command.is_none() {
            if let Some(cmd) = stored.command {
                self.command = Some(self.live_command(cmd));
            }
        } else if let Some(cmd) = self.command.take() {
            self.command = Some(self.live_command(cmd));
        }

        // 6. File change snapshots. Metadata from memory wins; disk can only
        // fill an evicted or corrupt blob for the same immutable change.
        if self.file_change.is_none() {
            self.file_change = stored.file_change;
        } else if let (Some(memory), Some(mut disk)) =
            (self.file_change.as_mut(), stored.file_change)
        {
            if memory.before_bytes.is_none() && !memory.before_corrupt {
                memory.before_bytes = disk.before_bytes.take();
                memory.before_corrupt = disk.before_corrupt;
            }
            if memory.after_bytes.is_none() && !memory.after_corrupt {
                memory.after_bytes = disk.after_bytes.take();
                memory.after_corrupt = disk.after_corrupt;
            }
        }

        // 7. Recording state
        if self.recording == ToolRecordingState::MemoryOnly
            && stored.recording != ToolRecordingState::MemoryOnly
        {
            self.recording = stored.recording;
        }

        true
    }

    pub(super) fn new(name: String) -> Self {
        Self {
            name,
            subject: ToolSubject::Other,
            subject_truncated: false,
            input: String::new(),
            input_total: 0,
            input_seen: false,
            input_truncated: false,
            input_expired: false,
            result: String::new(),
            output_line_count: None,
            result_total: 0,
            result_seen: false,
            result_truncated: false,
            result_expired: false,
            state: ToolExecutionState::Requested,
            phase: None,
            started_at: None,
            finished_at: None,
            outcome: None,
            stdout: StreamWindow::default(),
            stderr: StreamWindow::default(),
            command: None,
            file_change: None,
            input_corrupt: false,
            result_corrupt: false,
            recording: ToolRecordingState::MemoryOnly,
        }
    }

    pub(super) fn stream(&self, stream: ToolDataStream) -> Option<&StreamWindow> {
        match stream {
            ToolDataStream::Stdout => Some(&self.stdout),
            ToolDataStream::Stderr => Some(&self.stderr),
            ToolDataStream::Input | ToolDataStream::Output => None,
        }
    }

    pub(super) fn stream_mut(&mut self, stream: ToolDataStream) -> Option<&mut StreamWindow> {
        match stream {
            ToolDataStream::Stdout => Some(&mut self.stdout),
            ToolDataStream::Stderr => Some(&mut self.stderr),
            ToolDataStream::Input | ToolDataStream::Output => None,
        }
    }

    /// Counts retained raw bytes and bounded metadata, so the Session budget
    /// matches what is actually held in memory.
    pub(super) fn retained_bytes(&self) -> usize {
        let command = if self.command.is_some() {
            COMMAND_METADATA_BYTES
        } else {
            0
        };
        let file_change = self.file_change.as_ref().map_or(0, |change| {
            CHANGE_METADATA_BYTES
                .saturating_add(change.path.len())
                .saturating_add(
                    change
                        .before_bytes
                        .as_ref()
                        .map_or(0, |bytes| bytes.capacity())
                        .saturating_add(
                            change
                                .after_bytes
                                .as_ref()
                                .map_or(0, |bytes| bytes.capacity()),
                        ),
                )
        });
        self.name
            .len()
            .saturating_add(subject_len(&self.subject))
            .saturating_add(self.input.len())
            .saturating_add(self.result.len())
            .saturating_add(self.stdout.retained())
            .saturating_add(self.stderr.retained())
            .saturating_add(file_change)
            .saturating_add(command)
    }

    fn input_availability(&self) -> ToolDataAvailability {
        if self.input_corrupt {
            return ToolDataAvailability::Unavailable;
        }
        stream_availability(
            self.input_seen,
            self.input_expired,
            self.state.is_terminal(),
            self.input.len(),
            self.input_total,
            self.input_truncated,
        )
    }

    pub(super) fn output_availability(&self) -> ToolDataAvailability {
        if self.result_corrupt {
            return ToolDataAvailability::Unavailable;
        }
        stream_availability(
            self.result_seen,
            self.result_expired,
            self.state.is_terminal(),
            self.result.len(),
            self.result_total,
            self.result_truncated,
        )
    }

    /// Frees the retained input bytes and marks the stream expired. Capacity is
    /// dropped, not just logically cleared.
    pub(super) fn evict_input(&mut self) {
        self.input = String::new();
        self.input_expired = true;
    }

    pub(super) fn evict_result(&mut self) {
        self.result = String::new();
        self.output_line_count = None;
        self.result_expired = true;
    }

    /// Stream availability for a process stream. A running stream is `Pending`
    /// until its first byte, and never claims an end of output it has not seen.
    /// A stream the owner really ended is `Available` even with zero bytes (a
    /// real empty end); one that was cut short is `Partial`.
    pub(super) fn stream_availability(&self, stream: ToolDataStream) -> ToolDataAvailability {
        let Some(window) = self.stream(stream) else {
            return ToolDataAvailability::Unavailable;
        };
        if window.corrupt {
            return ToolDataAvailability::Unavailable;
        }
        if window.expired {
            return ToolDataAvailability::Expired;
        }
        if window.truncated {
            return ToolDataAvailability::Partial;
        }
        if window.seen || window.complete {
            return ToolDataAvailability::Available;
        }
        if self.stream_final() {
            ToolDataAvailability::Unavailable
        } else {
            ToolDataAvailability::Pending
        }
    }

    /// True when no more bytes can arrive for either process stream. A Runtime
    /// terminal outcome alone is not enough: a dropped tool future can leave
    /// the owner still draining, so the owner's own terminal command record (or
    /// the absence of any command record on a terminal call) decides.
    pub(super) fn stream_final(&self) -> bool {
        match &self.command {
            Some(command) => command.status.is_terminal(),
            None => self.state.is_terminal(),
        }
    }

    pub(super) fn execution_data(&self, tool_ref: &ToolRef) -> ToolExecutionData {
        ToolExecutionData {
            output_line_count: self.output_line_count,
            tool_ref: tool_ref.clone(),
            name: self.name.clone(),
            state: self.state,
            phase: self.phase,
            started_at: self.started_at.clone(),
            finished_at: self.finished_at.clone(),
            outcome: self.outcome,
            input_availability: self.input_availability(),
            output_availability: self.output_availability(),
            input_bytes: self.input_total,
            result_bytes: self.result_total,
            input_truncated: self.input_truncated || self.input.len() < self.input_total,
            result_truncated: self.result_truncated || self.result.len() < self.result_total,
            command: self
                .command
                .clone()
                .map(|command| self.live_command(command)),
            recording: self.recording,
        }
    }

    /// Overlays the ranges and retention flags of the *current* stream windows
    /// on the stored command record. A record stored at start or at the end
    /// must not make a running command report empty ranges while
    /// `tool.output` already returns bytes, and a complete EOF must not hide
    /// that the retained tail was evicted.
    pub(super) fn live_command(&self, mut command: CommandResult) -> CommandResult {
        let (stdout_base, stdout_end) = self.stdout.live_range();
        let (stderr_base, stderr_end) = self.stderr.live_range();
        command.stdout_base_offset = stdout_base;
        command.stdout_observed_end = stdout_end;
        command.stderr_base_offset = stderr_base;
        command.stderr_observed_end = stderr_end;
        // Real end of output and complete retention are different facts:
        // `output_complete` is the owner's end-of-observation decision, while
        // `output_truncated` also covers a dropped tail window.
        command.output_truncated = command.output_truncated
            || self.stdout.truncated
            || self.stderr.truncated
            || self.stdout.expired
            || self.stderr.expired;
        command
    }

    pub(crate) fn from_stored(
        stored: StoredToolRecord,
        (input_bytes, input_corrupt): (Option<Vec<u8>>, bool),
        (result_bytes, result_corrupt): (Option<Vec<u8>>, bool),
        (stdout_bytes, stdout_corrupt): (Option<Vec<u8>>, bool),
        (stderr_bytes, stderr_corrupt): (Option<Vec<u8>>, bool),
        (file_change_before, file_change_before_corrupt): (Option<Vec<u8>>, bool),
        (file_change_after, file_change_after_corrupt): (Option<Vec<u8>>, bool),
    ) -> Self {
        let mut input_corrupt = input_corrupt;
        let input = match input_bytes {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => {
                    input_corrupt = true;
                    String::new()
                }
            },
            None => String::new(),
        };
        let mut result_corrupt = result_corrupt;
        let result = match result_bytes {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => {
                    result_corrupt = true;
                    String::new()
                }
            },
            None => String::new(),
        };
        let stdout = StreamWindow::from_stored(stored.stdout, stdout_bytes, stdout_corrupt);
        let stderr = StreamWindow::from_stored(stored.stderr, stderr_bytes, stderr_corrupt);
        let file_change = stored.file_change.map(|change| {
            FileChange::from_stored(
                change,
                file_change_before,
                file_change_before_corrupt,
                file_change_after,
                file_change_after_corrupt,
            )
        });
        Self {
            name: stored.name,
            subject: stored.subject,
            subject_truncated: stored.subject_truncated,
            input,
            input_total: stored.input.total_bytes,
            input_seen: stored.input.seen,
            input_truncated: stored.input.truncated,
            input_expired: stored.input.expired,
            input_corrupt,
            output_line_count: (!result_corrupt && stored.result.seen && !stored.result.expired)
                .then(|| crate::presentation::count_lines(&result)),
            result,
            result_total: stored.result.total_bytes,
            result_seen: stored.result.seen,
            result_truncated: stored.result.truncated,
            result_expired: stored.result.expired,
            result_corrupt,
            state: stored.state,
            phase: stored.phase,
            started_at: stored.started_at,
            finished_at: stored.finished_at,
            outcome: stored.outcome,
            stdout,
            stderr,
            command: stored.command,
            file_change,
            recording: ToolRecordingState::Saved,
        }
    }
}

/// A snapshot of one completed tool call's metadata and retained buffers,
/// bounded per tool to <= 3 MiB for durable storage.
#[derive(Clone)]
pub(crate) struct ToolPersistenceSnapshot {
    pub(crate) tool_ref: ToolRef,
    pub(crate) record: StoredToolRecord,
    pub(crate) input_bytes: Option<Vec<u8>>,
    pub(crate) result_bytes: Option<Vec<u8>>,
    pub(crate) stdout_bytes: Option<Vec<u8>>,
    pub(crate) stderr_bytes: Option<Vec<u8>>,
    pub(crate) file_change_before: Option<Vec<u8>>,
    pub(crate) file_change_after: Option<Vec<u8>>,
}

impl ToolPersistenceSnapshot {
    pub(crate) fn blobs(&self) -> [(&'static str, Option<&[u8]>); 6] {
        use crate::store::{
            TOOL_AFTER_FILE, TOOL_BEFORE_FILE, TOOL_INPUT_FILE, TOOL_RESULT_FILE, TOOL_STDERR_FILE,
            TOOL_STDOUT_FILE,
        };
        [
            (TOOL_INPUT_FILE, self.input_bytes.as_deref()),
            (TOOL_RESULT_FILE, self.result_bytes.as_deref()),
            (TOOL_STDOUT_FILE, self.stdout_bytes.as_deref()),
            (TOOL_STDERR_FILE, self.stderr_bytes.as_deref()),
            (TOOL_BEFORE_FILE, self.file_change_before.as_deref()),
            (TOOL_AFTER_FILE, self.file_change_after.as_deref()),
        ]
    }
}

impl fmt::Debug for ToolPersistenceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolPersistenceSnapshot")
            .field("tool_ref", &self.tool_ref)
            .field("record", &self.record)
            .field(
                "input_bytes_len",
                &self.input_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "result_bytes_len",
                &self.result_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "stdout_bytes_len",
                &self.stdout_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "stderr_bytes_len",
                &self.stderr_bytes.as_ref().map(|b| b.len()),
            )
            .field(
                "file_change_before_len",
                &self.file_change_before.as_ref().map(|b| b.len()),
            )
            .field(
                "file_change_after_len",
                &self.file_change_after.as_ref().map(|b| b.len()),
            )
            .finish()
    }
}

fn subject_len(subject: &ToolSubject) -> usize {
    match subject {
        ToolSubject::File { path } => path.len(),
        ToolSubject::Command { script, cwd } => script.len().saturating_add(cwd.len()),
        ToolSubject::Other => 0,
    }
}

pub(super) fn stream_availability(
    seen: bool,
    expired: bool,
    terminal: bool,
    retained: usize,
    total: usize,
    truncated: bool,
) -> ToolDataAvailability {
    if expired {
        ToolDataAvailability::Expired
    } else if seen {
        if truncated || retained < total {
            ToolDataAvailability::Partial
        } else {
            ToolDataAvailability::Available
        }
    } else if terminal {
        ToolDataAvailability::Unavailable
    } else {
        ToolDataAvailability::Pending
    }
}

pub(super) fn snapshot_blob(
    bytes: Vec<u8>,
    expired: bool,
) -> (Option<Vec<u8>>, usize, Option<String>) {
    if expired || bytes.is_empty() {
        (None, 0, None)
    } else {
        let len = bytes.len();
        let hash = crate::store::hash_bytes(&bytes);
        (Some(bytes), len, Some(hash))
    }
}
