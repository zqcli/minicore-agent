//! Per-session retention, event capture, and persistence snapshots.
use super::record::{StreamWindow, ToolPersistenceSnapshot, ToolRecord, snapshot_blob};
#[cfg(test)]
use super::tests;
use super::types::{
    CommandResult, ToolDataStream, ToolExecutionData, ToolExecutionState, ToolInputSummary,
    ToolInvocationData, ToolPhase, ToolRecordingState, ToolRef, ToolStreamNotice, ToolSubject,
};
use super::{
    EVENT_INPUT_PREVIEW_BYTES, INPUT_ENCODING, MAX_SUBJECT_BYTES, MAX_TOOL_INPUT_BYTES,
    MAX_TOOL_RECORDS, MAX_TOOL_RESULT_BYTES, MAX_TOOL_TOTAL_BYTES,
};
use crate::changes::{ChangeRecord, FileChange};
use crate::ids::SessionId;
use crate::store::{
    StoredInputSummary, StoredResultSummary, StoredToolRecord, TOOL_RECORD_FORMAT_VERSION,
};
use crate::text::truncate;
use minicore_runtime::LoopId;
use minicore_runtime::history::HistoryItem;
use minicore_runtime::tools::{ToolInvocation, ToolResultOutcome};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

#[derive(Default)]
pub(super) struct ToolDataInner {
    pub(super) records: HashMap<ToolRef, ToolRecord>,
    /// Insertion order; the front is the oldest record.
    order: VecDeque<ToolRef>,
    pub(super) total_bytes: usize,
}

impl ToolDataInner {
    fn track(&mut self, tool_ref: &ToolRef, name: &str) {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.records.entry(tool_ref.clone())
        {
            let record = ToolRecord::new(name.to_owned());
            self.total_bytes = self.total_bytes.saturating_add(record.retained_bytes());
            entry.insert(record);
            self.order.push_back(tool_ref.clone());
        }
    }

    /// Resizes one record's retained bytes while keeping the Session byte
    /// budget exact.
    pub(super) fn resize(&mut self, tool_ref: &ToolRef, resize: impl FnOnce(&mut ToolRecord)) {
        let before = self
            .records
            .get(tool_ref)
            .map_or(0, ToolRecord::retained_bytes);
        if let Some(record) = self.records.get_mut(tool_ref) {
            resize(record);
        }
        let after = self
            .records
            .get(tool_ref)
            .map_or(0, ToolRecord::retained_bytes);
        self.total_bytes = self
            .total_bytes
            .saturating_sub(before)
            .saturating_add(after);
    }

    /// Evicts the oldest still-retained stream bytes. Input is evicted before
    /// result, so a result that arrives after its input was evicted stays
    /// queryable. Returns false when only metadata remains.
    fn evict_some_bytes(&mut self) -> bool {
        let input_victim = self
            .order
            .iter()
            .find(|tool_ref| {
                self.records.get(*tool_ref).is_some_and(|record| {
                    !record.input_expired && (!record.input.is_empty() || record.input_total > 0)
                })
            })
            .cloned();
        if let Some(victim) = input_victim {
            let freed = self
                .records
                .get(&victim)
                .map_or(0, |record| record.input.len());
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                record.evict_input();
            }
            return true;
        }
        let victim = self
            .order
            .iter()
            .find(|tool_ref| {
                self.records.get(*tool_ref).is_some_and(|record| {
                    !record.result_expired && (!record.result.is_empty() || record.result_total > 0)
                })
            })
            .cloned();
        if let Some(victim) = victim {
            let freed = self
                .records
                .get(&victim)
                .map_or(0, |record| record.result.len());
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                record.evict_result();
            }
            return true;
        }
        let victim = self.order.iter().find_map(|tool_ref| {
            let record = self.records.get(tool_ref)?;
            let change = record.file_change.as_ref()?;
            let retained = change
                .before_bytes
                .as_ref()
                .map_or(0, |bytes| bytes.capacity())
                .saturating_add(
                    change
                        .after_bytes
                        .as_ref()
                        .map_or(0, |bytes| bytes.capacity()),
                );
            (retained > 0).then(|| tool_ref.clone())
        });
        if let Some(victim) = victim {
            let freed = self.records.get(&victim).map_or(0, |record| {
                record.file_change.as_ref().map_or(0, |change| {
                    change
                        .before_bytes
                        .as_ref()
                        .map_or(0, |bytes| bytes.capacity())
                        .saturating_add(
                            change
                                .after_bytes
                                .as_ref()
                                .map_or(0, |bytes| bytes.capacity()),
                        )
                })
            });
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                if let Some(change) = record.file_change.as_mut() {
                    change.before_bytes = None;
                    change.after_bytes = None;
                }
            }
            return true;
        }
        // Process windows are evicted last, oldest record first, so a live
        // stream keeps its newest bytes as long as any budget remains.
        let victim = self.order.iter().find_map(|tool_ref| {
            let record = self.records.get(tool_ref)?;
            [ToolDataStream::Stdout, ToolDataStream::Stderr]
                .into_iter()
                .find(|stream| {
                    record.stream(*stream).is_some_and(|window| {
                        !window.expired && (!window.bytes.is_empty() || window.observed_end > 0)
                    })
                })
                .map(|stream| (tool_ref.clone(), stream))
        });
        if let Some((victim, stream)) = victim {
            let freed = self
                .records
                .get(&victim)
                .and_then(|record| record.stream(stream))
                .map_or(0, StreamWindow::retained);
            self.total_bytes = self.total_bytes.saturating_sub(freed);
            if let Some(record) = self.records.get_mut(&victim) {
                if let Some(window) = record.stream_mut(stream) {
                    window.evict();
                }
            }
            return true;
        }
        false
    }

    /// Removes the oldest record (preferring a terminal one) to bound the
    /// record count or a metadata-only byte overrun.
    fn remove_oldest(&mut self) -> bool {
        let victim = self
            .order
            .iter()
            .find(|tool_ref| {
                self.records
                    .get(*tool_ref)
                    .is_some_and(|record| record.state.is_terminal())
            })
            .cloned()
            .or_else(|| self.order.front().cloned());
        let Some(victim) = victim else {
            return false;
        };
        if let Some(record) = self.records.remove(&victim) {
            self.total_bytes = self.total_bytes.saturating_sub(record.retained_bytes());
        }
        self.order.retain(|tool_ref| tool_ref != &victim);
        true
    }

    fn enforce_limits(&mut self) {
        while self.total_bytes > MAX_TOOL_TOTAL_BYTES {
            if !self.evict_some_bytes() && !self.remove_oldest() {
                break;
            }
        }
        while self.records.len() > MAX_TOOL_RECORDS {
            if !self.remove_oldest() {
                break;
            }
        }
        debug_assert_eq!(
            self.total_bytes,
            self.records
                .values()
                .map(ToolRecord::retained_bytes)
                .sum::<usize>(),
            "tool data byte accounting drifted"
        );
    }
}

/// Per-Session bounded store of tool invocation/execution facts.
pub(crate) struct ToolData {
    pub(super) inner: Mutex<ToolDataInner>,
}

impl ToolData {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(ToolDataInner::default()),
        }
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, ToolDataInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns an isolated copy of one tool call's record from memory if present.
    pub(crate) fn get_record(&self, tool_ref: &ToolRef) -> Option<ToolRecord> {
        let inner = self.lock();
        inner.records.get(tool_ref).cloned()
    }

    /// Records that the Runtime accepted a tool call, before any policy
    /// decision. Arguments are not available at this boundary.
    pub(crate) fn note_requested(&self, tool_ref: &ToolRef, name: &str) {
        let mut inner = self.lock();
        inner.track(tool_ref, name);
        inner.enforce_limits();
    }

    /// Publishes the requested invocation exactly once per tool call and
    /// returns the event payload on the first publish. This runs at the policy
    /// boundary, before the approval decision and before the tool parses its
    /// own input; the recorded arguments are what the model requested.
    pub(crate) fn note_invocation(
        &self,
        tool_ref: &ToolRef,
        invocation: &ToolInvocation,
    ) -> Option<ToolInvocationData> {
        let name = invocation.tool_name().as_str().to_owned();
        let raw = serde_json::to_string(invocation.arguments()).expect("JSON arguments serialize");
        let (subject, subject_truncated) = subject_for(&name, invocation.arguments());
        let (input, input_truncated) = truncate(&raw, MAX_TOOL_INPUT_BYTES);

        let mut inner = self.lock();
        inner.track(tool_ref, &name);
        let already_published = inner
            .records
            .get(tool_ref)
            .is_some_and(|record| record.input_seen);
        inner.resize(tool_ref, |record| {
            if record.input_seen {
                return;
            }
            record.subject = subject;
            record.subject_truncated = subject_truncated;
            record.input = input.to_owned();
            record.input_total = raw.len();
            record.input_seen = true;
            record.input_truncated = input_truncated;
            record.input_expired = false;
        });
        inner.enforce_limits();
        if already_published {
            return None;
        }
        let record = inner.records.get(tool_ref)?;
        Some(ToolInvocationData {
            tool_ref: tool_ref.clone(),
            name: record.name.clone(),
            subject: record.subject.clone(),
            subject_truncated: record.subject_truncated,
            input: ToolInputSummary {
                total_bytes: record.input_total,
                preview: truncate(&record.input, EVENT_INPUT_PREVIEW_BYTES)
                    .0
                    .to_owned(),
                truncated: record.input.len() > EVENT_INPUT_PREVIEW_BYTES || record.input_truncated,
                encoding: INPUT_ENCODING,
            },
        })
    }

    /// Marks a call as awaiting a policy/approval decision. Never `Running`:
    /// the tool has not been invoked before the decision.
    pub(crate) fn mark_awaiting_policy(&self, tool_ref: &ToolRef) {
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            if record.state == ToolExecutionState::Requested {
                record.state = ToolExecutionState::AwaitingPolicy;
            }
        }
    }

    /// Marks a call as actually executing and stamps the real start time.
    pub(crate) fn mark_running(&self, tool_ref: &ToolRef) {
        let started_at = crate::store::utc_timestamp().ok();
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            if !record.state.is_terminal() {
                record.state = ToolExecutionState::Running;
            }
            if record.started_at.is_none() {
                record.started_at = started_at;
            }
        }
    }

    /// Records a real execution stage. Unknown progress text is ignored.
    pub(crate) fn note_phase(&self, tool_ref: &ToolRef, phase: &str) {
        let Some(phase) = ToolPhase::from_wire(phase) else {
            return;
        };
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            record.phase = Some(phase);
        }
    }

    /// Retains the raw result text observed by the wrapper. The authoritative
    /// terminal state still comes from the Runtime report.
    pub(crate) fn note_result(&self, tool_ref: &ToolRef, content: &str) {
        let (result, result_truncated) = truncate(content, MAX_TOOL_RESULT_BYTES);
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            record.output_line_count = Some(crate::presentation::count_lines(result));
            record.result = result.to_owned();
            record.result_total = content.len();
            record.result_seen = true;
            record.result_truncated = result_truncated;
            record.result_expired = false;
        });
        inner.enforce_limits();
    }

    /// Marks that cancellation was requested for a still-running call. Never
    /// terminal: the owner reports the real end separately.
    pub(crate) fn mark_cancelling(&self, tool_ref: &ToolRef) {
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            if !record.state.is_terminal() {
                record.state = ToolExecutionState::Cancelling;
            }
        }
    }

    /// Appends one accepted chunk of one process stream. The retained window is
    /// authoritative: live events and every later `tool.output` page describe
    /// these same bytes, so nothing is re-read from a file system. The notice
    /// is built after the Session budget ran, so it reports the state a
    /// consumer will actually observe.
    pub(crate) fn note_stream_chunk(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
        chunk: &[u8],
    ) -> Option<ToolStreamNotice> {
        let mut inner = self.lock();
        if !inner.records.contains_key(tool_ref) {
            return None;
        }
        let mut pushed = None;
        inner.resize(tool_ref, |record| {
            let Some(window) = record.stream_mut(stream) else {
                return;
            };
            pushed = Some(window.push(chunk));
        });
        let pushed = pushed?;
        inner.enforce_limits();
        let window = inner.records.get(tool_ref)?.stream(stream)?;
        Some(ToolStreamNotice {
            stream,
            base_offset: pushed.base_offset,
            next_offset: pushed.next_offset,
            observed_end: window.observed_end,
            // True when the retained window no longer covers every observed
            // byte, so a live consumer knows it has to page `tool.output` for
            // the authoritative range instead of stitching this event alone.
            dropped: pushed.dropped || window.truncated,
            expired: window.expired,
        })
    }

    /// Marks a real end of output for one stream, never a failed read.
    pub(crate) fn note_stream_end(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            if let Some(window) = record.stream_mut(stream) {
                window.complete = true;
            }
        });
    }

    /// Marks an owner-observed end that may be missing bytes: the observation
    /// stopped before a real end of output. `eof` becomes true (the record is
    /// final) and the stream is truncated, never reported as a clean end.
    pub(crate) fn note_stream_cut(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            if let Some(window) = record.stream_mut(stream) {
                window.complete = true;
                window.truncated = true;
            }
        });
    }

    /// Stores the current process record without touching the Runtime outcome.
    /// The ranges and retention flags are overlaid from the current windows and
    /// the built snapshot is returned *after* the Session budget ran, so an
    /// event cannot publish a stale result clone that disagrees with a later
    /// `tool.read`.
    pub(crate) fn note_command(
        &self,
        tool_ref: &ToolRef,
        command: CommandResult,
    ) -> Option<ToolExecutionData> {
        let mut inner = self.lock();
        if !inner.records.contains_key(tool_ref) {
            return None;
        }
        inner.resize(tool_ref, |record| {
            record.command = Some(command);
        });
        inner.enforce_limits();
        inner
            .records
            .get(tool_ref)
            .map(|record| record.execution_data(tool_ref))
    }

    /// Marks durable recording state for one tool call and returns the updated snapshot.
    pub(crate) fn note_recording(
        &self,
        tool_ref: &ToolRef,
        recording: ToolRecordingState,
    ) -> Option<ToolExecutionData> {
        let mut inner = self.lock();
        if let Some(record) = inner.records.get_mut(tool_ref) {
            record.recording = recording;
            Some(record.execution_data(tool_ref))
        } else {
            None
        }
    }

    /// Commits one native file-tool fact at the real mutation boundary. A
    /// duplicate identity is never overwritten with a guessed later value.
    pub(crate) fn note_file_change(&self, tool_ref: &ToolRef, change: FileChange) {
        let mut inner = self.lock();
        inner.resize(tool_ref, |record| {
            if record.file_change.is_none() {
                record.file_change = Some(change);
            }
        });
        inner.enforce_limits();
    }

    /// The retained range of one process stream, as
    /// `(base_offset, observed_end)`, so the process record and `tool.output`
    /// describe the same bytes.
    pub(crate) fn stream_range(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
    ) -> Option<(u64, u64)> {
        let inner = self.lock();
        let window = inner.records.get(tool_ref)?.stream(stream)?;
        Some((window.start_offset, window.observed_end))
    }

    /// Current structured snapshot for a live event. It is not an outcome:
    /// only `finish_and_snapshot` records the authoritative terminal state.
    pub(crate) fn snapshot(&self, tool_ref: &ToolRef) -> Option<ToolExecutionData> {
        let inner = self.lock();
        inner
            .records
            .get(tool_ref)
            .map(|record| record.execution_data(tool_ref))
    }

    /// Records the authoritative terminal outcome and returns the same
    /// snapshot `tool.read` would return, so the live event and the query
    /// cannot disagree.
    pub(crate) fn finish_and_snapshot(
        &self,
        tool_ref: &ToolRef,
        outcome: ToolResultOutcome,
    ) -> Option<ToolExecutionData> {
        let mut inner = self.lock();
        let finished_at = inner.records.get(tool_ref).and_then(|record| {
            if record.finished_at.is_some() {
                None
            } else {
                crate::store::utc_timestamp().ok()
            }
        });
        let record = inner.records.get_mut(tool_ref)?;
        record.state = ToolExecutionState::from_outcome(outcome);
        record.outcome = Some(outcome);
        if record.finished_at.is_none() {
            record.finished_at = finished_at;
        }
        Some(record.execution_data(tool_ref))
    }

    /// Reconciles terminal state from the joined loop report, which is
    /// authoritative. State/outcome are always aligned. Result content is
    /// captured whenever it was not already observed and not evicted, for
    /// every outcome, including failed/denied calls. Evicted content is never
    /// resurrected as available.
    pub(crate) fn reconcile(&self, session_id: SessionId, items: &[HistoryItem]) {
        let finished_at = crate::store::utc_timestamp().ok();
        let mut inner = self.lock();
        for item in items {
            let HistoryItem::ToolResult(result) = item else {
                continue;
            };
            let tool_ref = ToolRef {
                session_id,
                loop_id: result.loop_id,
                request_index: result.request_index,
                tool_call_id: result.call_id.clone(),
            };
            inner.track(&tool_ref, result.tool_name.as_str());
            // The report is the authority for the result text the model saw;
            // replace any best-effort early wrapper copy. Never resurrect bytes
            // that were evicted.
            let content = result.output.content().as_str();
            let captured = inner.records.get(&tool_ref).and_then(|record| {
                (!record.result_expired).then(|| truncate(content, MAX_TOOL_RESULT_BYTES))
            });
            if let Some(record) = inner.records.get_mut(&tool_ref) {
                record.state = ToolExecutionState::from_outcome(result.outcome);
                record.outcome = Some(result.outcome);
                if record.finished_at.is_none() {
                    record.finished_at = finished_at.clone();
                }
            }
            if let Some((prefix, truncated)) = captured {
                inner.resize(&tool_ref, |record| {
                    record.output_line_count = Some(crate::presentation::count_lines(prefix));
                    record.result = prefix.to_owned();
                    record.result_total = content.len();
                    record.result_seen = true;
                    record.result_truncated = truncated;
                });
            }
        }
        inner.enforce_limits();
    }

    /// Returns all tool refs associated with a particular loop in insertion order.
    pub(crate) fn loop_tool_refs(&self, session_id: SessionId, loop_id: LoopId) -> Vec<ToolRef> {
        let inner = self.lock();
        inner
            .order
            .iter()
            .filter(|r| r.session_id == session_id && r.loop_id == loop_id)
            .cloned()
            .collect()
    }

    /// Returns only bounded file-change metadata and retained blob ownership;
    /// callers never receive the larger invocation/result snapshots here.
    pub(crate) fn file_change_records(
        &self,
        session_id: SessionId,
        loop_id: Option<LoopId>,
    ) -> Vec<ChangeRecord> {
        let inner = self.lock();
        inner
            .order
            .iter()
            .filter(|tool_ref| {
                tool_ref.session_id == session_id
                    && loop_id
                        .as_ref()
                        .is_none_or(|loop_id| &tool_ref.loop_id == loop_id)
            })
            .filter_map(|tool_ref| {
                inner
                    .records
                    .get(tool_ref)
                    .and_then(|record| record.file_change.as_ref())
                    .map(|change| change.record(tool_ref))
            })
            .collect()
    }

    /// Returns the full in-memory `FileChange` for one tool call, with its
    /// bounded before/after snapshots, when one was recorded. Callers match on
    /// `file_change_records` metadata first, so only the hit is cloned here.
    pub(crate) fn file_change(&self, tool_ref: &ToolRef) -> Option<crate::changes::FileChange> {
        let inner = self.lock();
        inner
            .records
            .get(tool_ref)
            .and_then(|record| record.file_change.as_ref())
            .cloned()
    }

    /// Exports one completed tool call's facts and retained buffers for durable persistence.
    /// Memory copying is bounded to this single record (<= 3 MiB total).
    pub(crate) fn snapshot_for_persistence(
        &self,
        tool_ref: &ToolRef,
    ) -> Option<ToolPersistenceSnapshot> {
        let record = self.get_record(tool_ref)?;
        if !record.is_terminal() {
            return None;
        }
        #[cfg(test)]
        tests::wait_snapshot_gate(tool_ref);
        let command = record.command.clone().map(|cmd| record.live_command(cmd));
        let (input_bytes, input_file_bytes, input_file_sha256) =
            snapshot_blob(record.input.into_bytes(), record.input_expired);
        let input_summary = StoredInputSummary {
            total_bytes: record.input_total,
            seen: record.input_seen,
            truncated: record.input_truncated,
            expired: record.input_expired,
            file_bytes: input_file_bytes,
            file_sha256: input_file_sha256,
        };

        let (result_bytes, result_file_bytes, result_file_sha256) =
            snapshot_blob(record.result.into_bytes(), record.result_expired);
        let result_summary = StoredResultSummary {
            total_bytes: record.result_total,
            seen: record.result_seen,
            truncated: record.result_truncated,
            expired: record.result_expired,
            file_bytes: result_file_bytes,
            file_sha256: result_file_sha256,
        };

        let (stdout_summary, stdout_bytes) = record.stdout.snapshot();
        let (stderr_summary, stderr_bytes) = record.stderr.snapshot();

        let (file_change, file_change_before, file_change_after) = record
            .file_change
            .map(|change| {
                (
                    Some(change.stored()),
                    change.before_bytes,
                    change.after_bytes,
                )
            })
            .unwrap_or_default();

        let stored_record = StoredToolRecord {
            version: TOOL_RECORD_FORMAT_VERSION,
            tool_ref: tool_ref.clone(),
            name: record.name,
            subject: record.subject,
            subject_truncated: record.subject_truncated,
            state: record.state,
            phase: record.phase,
            started_at: record.started_at,
            finished_at: record.finished_at,
            outcome: record.outcome,
            input: input_summary,
            result: result_summary,
            stdout: stdout_summary,
            stderr: stderr_summary,
            command,
            file_change,
        };

        Some(ToolPersistenceSnapshot {
            tool_ref: tool_ref.clone(),
            record: stored_record,
            input_bytes,
            result_bytes,
            stdout_bytes,
            stderr_bytes,
            file_change_before,
            file_change_after,
        })
    }
}

#[cfg(test)]
pub(super) static SNAPSHOT_GATES: crate::test_support::Registry<
    ToolRef,
    std::sync::Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

/// Structured subject extraction for the known native tools. Unknown tools
/// expose only `Other`; their raw arguments stay readable through
/// `tool.output(Input, ...)`.
fn subject_for(name: &str, arguments: &Value) -> (ToolSubject, bool) {
    match name {
        "read" | "write" | "edit" | "apply_patch" => {
            let path = string_arg(arguments, &["path", "file_path"]);
            let (path, truncated) = truncate(&path, MAX_SUBJECT_BYTES);
            (
                ToolSubject::File {
                    path: path.to_owned(),
                },
                truncated,
            )
        }
        "bash" => {
            let script = string_arg(arguments, &["command"]);
            let cwd = arguments.get("cwd").and_then(Value::as_str).unwrap_or(".");
            let (script, script_truncated) = truncate(&script, MAX_SUBJECT_BYTES);
            let (cwd, cwd_truncated) = truncate(cwd, MAX_SUBJECT_BYTES);
            (
                ToolSubject::Command {
                    script: script.to_owned(),
                    cwd: cwd.to_owned(),
                },
                script_truncated || cwd_truncated,
            )
        }
        _ => (ToolSubject::Other, false),
    }
}

fn string_arg(arguments: &Value, keys: &[&str]) -> String {
    for key in keys {
        if let Some(value) = arguments.get(*key).and_then(Value::as_str) {
            return value.to_owned();
        }
    }
    String::new()
}
