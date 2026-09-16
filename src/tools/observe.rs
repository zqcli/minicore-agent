use std::sync::{Arc, Mutex};

use base64::Engine as _;
use minicore_runtime::LoopId;

use crate::event::{AgentEvent, AgentEventSink, EventMeta};
use crate::ids::SessionId;
use crate::sessions::TurnRef;
use crate::tool_data::{
    CommandResult, ToolData, ToolDataStream, ToolInvocationData, ToolProcessChunk, ToolProcessData,
    ToolRecordingState, ToolRef, ToolStreamNotice,
};

use super::command::{CommandBinding, CommandOwners, CommandStreamSink};

/// Fixed identity of one model request, captured at the real model boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestKey {
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
}

#[derive(Default)]
struct ObserverInner {
    request_key: Option<RequestKey>,
}

/// The sole narrow execution observer. It records authoritative tool facts and
/// publishes best-effort events, but never owns a Session, Agent, or command
/// registry. Command ownership remains explicitly Session-owned.
pub(crate) struct ToolObserver {
    session_id: SessionId,
    tool_data: Arc<ToolData>,
    events: AgentEventSink,
    inner: Mutex<ObserverInner>,
}

impl ToolObserver {
    pub(crate) fn new(
        session_id: SessionId,
        tool_data: Arc<ToolData>,
        events: AgentEventSink,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            tool_data,
            events,
            inner: Mutex::new(ObserverInner::default()),
        })
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn tool_data(&self) -> Arc<ToolData> {
        Arc::clone(&self.tool_data)
    }

    pub(crate) fn command_binding(
        self: &Arc<Self>,
        command_owners: Arc<CommandOwners>,
    ) -> CommandBinding {
        let sink: Arc<dyn CommandStreamSink> = Arc::clone(self) as Arc<dyn CommandStreamSink>;
        CommandBinding::new(command_owners, sink)
    }

    pub(crate) fn note_request_start(&self, key: RequestKey) {
        self.inner.lock().unwrap().request_key = Some(key);
    }

    pub(crate) fn request_key(&self) -> Option<RequestKey> {
        self.inner.lock().unwrap().request_key
    }

    pub(crate) fn reset_before_loop_start(&self) {
        self.inner.lock().unwrap().request_key = None;
    }

    pub(crate) fn publish_tool_invocation(
        &self,
        tool_ref: &ToolRef,
        invocation: &minicore_runtime::tools::ToolInvocation,
    ) {
        let data = self.tool_data.note_invocation(tool_ref, invocation);
        self.tool_data.mark_running(tool_ref);
        if let Some(data) = data {
            self.emit_tool_invocation(data);
        }
    }

    pub(crate) fn emit_tool_execution(&self, tool_ref: &ToolRef) {
        let Some(data) = self.tool_data.snapshot(tool_ref) else {
            return;
        };
        let loop_id = data.tool_ref.loop_id;
        let _ = self.events.try_send(AgentEvent::ToolExecution {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            data,
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }

    pub(crate) fn note_tool_recording(
        &self,
        tool_ref: &ToolRef,
        state: ToolRecordingState,
        turn: TurnRef,
    ) {
        if let Some(data) = self.tool_data.note_recording(tool_ref, state) {
            let loop_id = data.tool_ref.loop_id;
            let _ = self.events.try_send(AgentEvent::ToolExecution {
                turn,
                data,
                meta: EventMeta {
                    session_id: self.session_id,
                    loop_id: Some(loop_id),
                    dropped_before: 0,
                },
            });
        }
    }

    pub(crate) fn emit_tool_invocation(&self, data: ToolInvocationData) {
        let loop_id = data.tool_ref.loop_id;
        let _ = self.events.try_send(AgentEvent::ToolInvocation {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            data,
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }

    fn emit_tool_process(
        &self,
        tool_ref: &ToolRef,
        chunk: Option<ToolProcessChunk>,
        command: Option<CommandResult>,
    ) {
        let loop_id = tool_ref.loop_id;
        let _ = self.events.try_send(AgentEvent::ToolProcess {
            turn: TurnRef {
                session_id: self.session_id,
                loop_id,
            },
            data: ToolProcessData {
                tool_ref: tool_ref.clone(),
                chunk,
                command,
            },
            meta: EventMeta {
                session_id: self.session_id,
                loop_id: Some(loop_id),
                dropped_before: 0,
            },
        });
    }
}

impl CommandStreamSink for ToolObserver {
    fn push_chunk(
        &self,
        tool_ref: &ToolRef,
        stream: ToolDataStream,
        chunk: &[u8],
    ) -> Option<ToolStreamNotice> {
        let notice = self.tool_data.note_stream_chunk(tool_ref, stream, chunk)?;
        let command = self
            .tool_data
            .snapshot(tool_ref)
            .and_then(|execution| execution.command);
        self.emit_tool_process(
            tool_ref,
            Some(ToolProcessChunk {
                stream: notice.stream,
                encoding: notice.stream.encoding(),
                data: base64::engine::general_purpose::STANDARD.encode(chunk),
                base_offset: notice.base_offset,
                next_offset: notice.next_offset,
                observed_end: notice.observed_end,
                dropped: notice.dropped,
                expired: notice.expired,
            }),
            command,
        );
        Some(notice)
    }

    fn note_command(&self, tool_ref: &ToolRef, result: &CommandResult) {
        let Some(snapshot) = self.tool_data.note_command(tool_ref, result.clone()) else {
            return;
        };
        self.emit_tool_process(tool_ref, None, snapshot.command);
    }

    fn mark_cancelling(&self, tool_ref: &ToolRef) {
        self.tool_data.mark_cancelling(tool_ref);
        self.emit_tool_execution(tool_ref);
    }

    fn stream_range(&self, tool_ref: &ToolRef, stream: ToolDataStream) -> Option<(u64, u64)> {
        self.tool_data.stream_range(tool_ref, stream)
    }

    fn note_stream_end(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        self.tool_data.note_stream_end(tool_ref, stream);
    }

    fn note_stream_cut(&self, tool_ref: &ToolRef, stream: ToolDataStream) {
        self.tool_data.note_stream_cut(tool_ref, stream);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::tools::{
        ToolContext, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
    };
    use minicore_runtime::{LoopId, ToolCallId};
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::Workspace;
    use crate::changes::{ChangeCommitState, ChangeKind};
    use crate::event::AgentEventSink;
    use crate::ids::SessionId;
    use crate::tool_data::{
        CommandStatus, ToolData, ToolDataStream, ToolOutputRequest, ToolReadRequest,
    };
    use crate::tools::{CommandEnvironment, NativeWriteTool, OwnedBashTool};

    fn invocation(
        tool_call_id: &str,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> ToolInvocation {
        ToolInvocation {
            tool_call_id: ToolCallId::new(tool_call_id).unwrap(),
            tool_name: tool_name.parse().unwrap(),
            arguments,
        }
    }

    fn context() -> ToolContext {
        ToolContext {
            cancellation: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(5),
            progress: ToolProgressSink::default(),
        }
    }

    #[tokio::test]
    async fn direct_observer_records_bash_streams_and_native_file_changes() {
        let session_id = SessionId::new().unwrap();
        let loop_id = LoopId::new().unwrap();
        let base =
            std::env::temp_dir().join(format!("minicore-agent-direct-observer-{session_id}"));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root.clone()).await.unwrap());

        // No event consumer is present: authoritative facts must still be queryable.
        let (event_sender, event_receiver) = tokio::sync::mpsc::channel(1);
        drop(event_receiver);
        let data = Arc::new(ToolData::new());
        let owners = crate::tools::command::CommandOwners::new();
        let observer = ToolObserver::new(
            session_id,
            Arc::clone(&data),
            AgentEventSink::new(event_sender),
        );
        let bash = Arc::new(OwnedBashTool::with_binding(
            Arc::clone(&workspace),
            CommandEnvironment::new(std::iter::empty::<std::ffi::OsString>()),
            Some(observer.command_binding(Arc::clone(&owners))),
        ));
        let bash_ref = ToolRef {
            session_id,
            loop_id,
            request_index: 0,
            tool_call_id: ToolCallId::new("bash-call").unwrap(),
        };
        let bash_invocation = invocation(
            "bash-call",
            "bash",
            json!({
                "command": "printf 'started\\n'; exec tail -f /dev/null",
                "timeout_seconds": 30
            }),
        );
        observer.publish_tool_invocation(&bash_ref, &bash_invocation);

        let bash_task_ref = bash_ref.clone();
        let bash_task = tokio::spawn({
            let bash = Arc::clone(&bash);
            async move {
                bash.execute_bound(bash_invocation, context(), Some(bash_task_ref))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if data.stream_range(&bash_ref, ToolDataStream::Stdout) == Some((0, 8)) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Bash output was not recorded in time");

        let full = data
            .output(
                &ToolOutputRequest {
                    tool_ref: bash_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 0,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(full.base_offset, 0);
        assert_eq!(full.next_offset, 8);
        assert!(!full.eof);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(full.data)
                .unwrap(),
            b"started\n"
        );

        let suffix = data
            .output(
                &ToolOutputRequest {
                    tool_ref: bash_ref.clone(),
                    stream: ToolDataStream::Stdout,
                    offset: 4,
                    max_bytes: Some(4096),
                },
                4096,
            )
            .unwrap();
        assert_eq!(suffix.base_offset, 4);
        assert_eq!(suffix.next_offset, 8);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(suffix.data)
                .unwrap(),
            b"ted\n"
        );

        tokio::time::timeout(Duration::from_secs(5), owners.join_loop(loop_id))
            .await
            .expect("Session-style command join timed out");
        let bash_result = tokio::time::timeout(Duration::from_secs(5), bash_task)
            .await
            .expect("Bash tool future did not finish after owner join")
            .expect("Bash task panicked");
        assert!(
            bash_result.is_err(),
            "cancelled Bash unexpectedly completed"
        );
        assert_eq!(owners.active(), 0);

        let bash_read = data
            .read(
                &ToolReadRequest {
                    tool_ref: bash_ref.clone(),
                    max_bytes: None,
                },
                4096,
            )
            .unwrap();
        assert_eq!(bash_read.execution.tool_ref, bash_ref);
        let command = bash_read.execution.command.unwrap();
        assert_eq!(command.status, CommandStatus::Cancelled);
        assert!(command.termination_confirmed);
        assert_eq!(command.stdout_base_offset, 0);
        assert_eq!(command.stdout_observed_end, 8);

        let native_ref = ToolRef {
            session_id,
            loop_id,
            request_index: 1,
            tool_call_id: ToolCallId::new("write-call").unwrap(),
        };
        tokio::fs::write(root.join("value.txt"), b"before\n")
            .await
            .unwrap();
        let native_invocation = invocation(
            "write-call",
            "write",
            json!({"path": "value.txt", "content": "after\n"}),
        );
        observer.publish_tool_invocation(&native_ref, &native_invocation);
        let write = NativeWriteTool::new(Arc::clone(&workspace));
        let outcome = write
            .execute_bound(
                native_invocation,
                context(),
                Some(native_ref.clone()),
                Some(Arc::clone(&data)),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, ToolExecutionOutcome::Completed(_)));

        let change = data.file_change(&native_ref).unwrap();
        assert_eq!(change.kind, ChangeKind::Modified);
        assert_eq!(change.commit_state, ChangeCommitState::Applied);
        assert_eq!(change.before_bytes.as_deref(), Some(b"before\n".as_slice()));
        assert_eq!(change.after_bytes.as_deref(), Some(b"after\n".as_slice()));
        let records = data.file_change_records(session_id, Some(loop_id));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool_ref.as_ref(), Some(&native_ref));

        tokio::fs::remove_dir_all(base)
            .await
            .expect("direct observer fixture cleanup failed");
    }
}
