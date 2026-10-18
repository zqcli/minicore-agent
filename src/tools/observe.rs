use std::sync::{Arc, Mutex};

use super::{NativeApplyPatchTool, NativeEditTool, NativeWriteTool};
use base64::Engine as _;
use minicore_runtime::tools::{Tool, ToolContext, ToolExecutionOutcome, ToolInvocation};
use minicore_runtime::{LoopId, ToolCallId};

use crate::event::{AgentEvent, AgentEventSink, EventMeta};
use crate::ids::SessionId;
use crate::sessions::TurnRef;
use crate::tool_data::{
    CommandResult, ToolData, ToolDataStream, ToolInvocationData, ToolProcessChunk, ToolProcessData,
    ToolRecordingState, ToolRef, ToolStreamNotice,
};

use super::command::{CommandBinding, CommandStreamSink, ToolOwners};

/// Fixed identity of one model request, captured at the real model boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestKey {
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
}

/// The sole narrow execution observer. It records authoritative tool facts and
/// publishes best-effort events, but never owns a Session, Agent, or command
/// registry. Command ownership remains explicitly Session-owned.
pub(crate) struct ToolObserver {
    session_id: SessionId,
    tool_data: Arc<ToolData>,
    events: AgentEventSink,
    inner: Mutex<Option<RequestKey>>,
}

impl ToolObserver {
    fn turn(&self, loop_id: LoopId) -> TurnRef {
        TurnRef {
            session_id: self.session_id,
            loop_id,
        }
    }
    fn meta(&self, loop_id: LoopId) -> EventMeta {
        EventMeta {
            session_id: self.session_id,
            loop_id: Some(loop_id),
            dropped_before: 0,
        }
    }

    pub(crate) fn new(
        session_id: SessionId,
        tool_data: Arc<ToolData>,
        events: AgentEventSink,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            tool_data,
            events,
            inner: Mutex::new(None),
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
        tool_owners: Arc<ToolOwners>,
    ) -> CommandBinding {
        let sink: Arc<dyn CommandStreamSink> = Arc::clone(self) as Arc<dyn CommandStreamSink>;
        CommandBinding::new(tool_owners, sink)
    }

    pub(crate) fn note_request_start(&self, key: RequestKey) {
        *self.inner.lock().unwrap() = Some(key);
    }

    pub(crate) fn request_key(&self) -> Option<RequestKey> {
        *self.inner.lock().unwrap()
    }

    pub(crate) fn reset_before_loop_start(&self) {
        *self.inner.lock().unwrap() = None;
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
            turn: self.turn(loop_id),
            data,
            meta: self.meta(loop_id),
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
                meta: self.meta(loop_id),
            });
        }
    }

    pub(crate) fn emit_tool_invocation(&self, data: ToolInvocationData) {
        let loop_id = data.tool_ref.loop_id;
        let _ = self.events.try_send(AgentEvent::ToolInvocation {
            turn: self.turn(loop_id),
            data,
            meta: self.meta(loop_id),
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
            turn: self.turn(loop_id),
            data: ToolProcessData {
                tool_ref: tool_ref.clone(),
                chunk,
                command,
            },
            meta: self.meta(loop_id),
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

#[derive(Clone)]
pub(crate) enum ToolImpl {
    Plain(Arc<dyn Tool>),
    Bash(Arc<crate::tools::OwnedBashTool>),
    Write(Arc<NativeWriteTool>),
    Edit(Arc<NativeEditTool>),
    ApplyPatch(Arc<NativeApplyPatchTool>),
}

impl ToolImpl {
    pub(crate) fn tool(&self) -> &dyn Tool {
        match self {
            ToolImpl::Plain(inner) => inner.as_ref(),
            ToolImpl::Bash(inner) => inner.as_ref(),
            ToolImpl::Write(inner) => inner.as_ref(),
            ToolImpl::Edit(inner) => inner.as_ref(),
            ToolImpl::ApplyPatch(inner) => inner.as_ref(),
        }
    }
    pub(crate) async fn execute_bound(
        self,
        invocation: ToolInvocation,
        context: ToolContext,
        tool_ref: Option<ToolRef>,
        tool_data: Arc<crate::tool_data::ToolData>,
    ) -> Result<ToolExecutionOutcome, minicore_runtime::tools::ToolError> {
        macro_rules! execute_native {
            ($tool:expr) => {
                $tool
                    .execute_bound(invocation, context, tool_ref, Some(tool_data))
                    .await
            };
        }
        match self {
            ToolImpl::Plain(inner) => inner.execute(invocation, context).await,
            ToolImpl::Bash(inner) => inner.execute_bound(invocation, context, tool_ref).await,
            ToolImpl::Write(inner) => execute_native!(inner),
            ToolImpl::Edit(inner) => execute_native!(inner),
            ToolImpl::ApplyPatch(inner) => execute_native!(inner),
        }
    }
}

impl ToolObserver {
    pub(crate) fn tool_ref(
        &self,
        key: Option<RequestKey>,
        tool_call_id: &ToolCallId,
    ) -> Option<ToolRef> {
        key.map(|key| ToolRef {
            session_id: self.session_id(),
            loop_id: key.loop_id,
            request_index: key.request_index,
            tool_call_id: tool_call_id.clone(),
        })
    }
}

/// Reads the request identity from `ToolObserver` at the policy boundary and
/// publishes the validated invocation before any approval decision. This is
/// what makes invocation data obtainable while a tool is still waiting for
/// approval; `Running` is not set here, because the tool has not been invoked
/// yet. Delegation, fallback decisions, and error paths are unchanged.
pub(crate) struct PresentationPolicy {
    inner: Arc<dyn minicore_runtime::tools::ToolPolicy>,
    observer: Arc<ToolObserver>,
}

impl PresentationPolicy {
    pub(crate) fn new_with_observer(
        inner: Arc<dyn minicore_runtime::tools::ToolPolicy>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Arc::new(Self { inner, observer })
    }
}

impl minicore_runtime::tools::ToolPolicy for PresentationPolicy {
    fn decide<'a>(
        &'a self,
        request: minicore_runtime::tools::ToolPolicyRequest,
    ) -> minicore_runtime::tools::ToolPolicyFuture<'a> {
        let tool_ref = self.observer.tool_ref(
            self.observer.request_key(),
            request.invocation.tool_call_id(),
        );
        if let Some(tool_ref) = &tool_ref {
            if let Some(data) = self
                .observer
                .tool_data()
                .note_invocation(tool_ref, &request.invocation)
            {
                self.observer.emit_tool_invocation(data);
            }
        }
        let observer = Arc::clone(&self.observer);
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let decision = inner.decide(request).await;
            if let (Some(tool_ref), Ok(decision)) = (&tool_ref, &decision) {
                if matches!(
                    decision,
                    minicore_runtime::tools::ToolDecision::RequireApproval { .. }
                ) {
                    observer.tool_data().mark_awaiting_policy(tool_ref);
                }
            }
            decision
        })
    }
}

#[cfg(all(test, unix))]
mod tests;
