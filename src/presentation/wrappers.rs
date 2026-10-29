use super::*;

// ---------------------------------------------------------------------------
// Model / Tool wrappers (thin; identity + best-effort display only)
// ---------------------------------------------------------------------------

/// Records the real request key in `ToolObserver` at the model boundary, then
/// delegates unchanged.
pub(crate) struct PresentationModel {
    physical_window: u64,
    inner: Arc<dyn Model>,
    presentation: Arc<Presentation>,
    observer: Arc<ToolObserver>,
}

impl PresentationModel {
    pub(crate) fn new_with_observer(
        inner: Arc<dyn Model>,
        physical_window: u64,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            physical_window,
            presentation,
            observer,
        })
    }
}

impl Model for PresentationModel {
    fn descriptor(&self) -> &ModelDescriptor {
        self.inner.descriptor()
    }

    fn start<'a>(
        &'a self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> minicore_runtime::model::ModelStartFuture<'a> {
        let key = RequestKey {
            loop_id: context.loop_id,
            request_index: context.request_index,
        };
        // Synchronous; no lock is held across an await.
        self.observer.note_request_start(key);
        self.presentation
            .begin_streamed(key, self.descriptor().model_ref.as_str().to_owned());
        let context_generation = self.presentation.context_generation(self);
        let physical_window = self.physical_window;
        // This presentation belongs to a session, so replacing a model cannot
        // reuse an attempt within an active request. Overflow disables only
        // the optional preview; execution and usage observation still work.
        let attempt = self.presentation.next_preview_attempt();
        let inner = self.inner.start(request, context);
        let presentation = Arc::clone(&self.presentation);
        Box::pin(async move {
            let stream = inner.await?;
            // A pass-through stream also observes normal EOF. Merely seeing
            // Finish is insufficient: assembler rejection/drop afterwards must
            // discard speculative cards. There is no extra task or timer.
            Ok(Box::pin(preview::PreviewStream::new(
                stream,
                presentation,
                key,
                attempt,
                context_generation,
                physical_window,
            )) as minicore_runtime::model::ModelStream)
        })
    }
}

/// Captures the current request identity in `ToolObserver` at `Tool::execute`
/// start, computes the bounded ToolDisplay, then delegates execution unchanged
/// (cancellation, deadline, errors, and the result are never altered). Real
/// Bash and native file tools receive the exact `ToolRef` captured here, so
/// commands and mutation records never resolve identity from a table that a
/// dropped future can leave stale.
pub(crate) struct PresentationTool {
    pub(super) inner: ToolImpl,
    pub(super) presentation: Arc<Presentation>,
    pub(super) observer: Arc<ToolObserver>,
}

impl PresentationTool {
    pub(crate) fn new(
        inner: ToolImpl,
        presentation: Arc<Presentation>,
        observer: Arc<ToolObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            presentation,
            observer,
        })
    }
}

impl Tool for PresentationTool {
    fn spec(&self) -> &ToolSpec {
        self.inner.tool().spec()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: ToolContext,
    ) -> minicore_runtime::tools::ToolFuture<'a> {
        let presentation = Arc::clone(&self.presentation);
        let observer = Arc::clone(&self.observer);
        let key = observer.request_key();
        let display = build_tool_display(
            invocation.tool_name().as_str(),
            Some(invocation.arguments()),
            None,
        );
        // Complete identity: only a real `Model::start` request key plus the
        // Runtime's tool-call id. This is the single place the identity is
        // captured; it is passed downward, never recovered from a live table.
        let tool_ref = observer.tool_ref(key, invocation.tool_call_id());
        presentation.begin_tool(key, &invocation, display);
        if let Some(tool_ref) = &tool_ref {
            // Published before the work starts, never after it returns.
            observer.publish_tool_invocation(tool_ref, &invocation);
        }

        let tool_call_id = invocation.tool_call_id().clone();
        let inner = self.inner.clone();
        let tool_data = observer.tool_data();
        Box::pin(async move {
            let result = inner
                .execute_bound(invocation, context, tool_ref.clone(), tool_data)
                .await;
            finish_presentation_tool(
                &presentation,
                &observer,
                key,
                &tool_call_id,
                tool_ref.as_ref(),
                &result,
            );
            result
        })
    }
}

/// Best-effort post-execution presentation bookkeeping. The outcome and error
/// are forwarded unchanged; raw result bytes are retained for `tool.output`
/// while the authoritative terminal state still comes from the Runtime.
fn finish_presentation_tool(
    presentation: &Presentation,
    observer: &ToolObserver,
    request_key: Option<RequestKey>,
    tool_call_id: &ToolCallId,
    tool_ref: Option<&ToolRef>,
    result: &Result<ToolExecutionOutcome, minicore_runtime::tools::ToolError>,
) {
    if let (Some(tool_ref), Ok(ToolExecutionOutcome::Completed(output))) = (tool_ref, result) {
        observer
            .tool_data()
            .note_result(tool_ref, output.content().as_str());
    }
    // Runtime errors are static enum variants, but map them explicitly so a
    // future diagnostic carrying arbitrary text cannot leak into the display
    // cache.
    let result_text = match result {
        Ok(ToolExecutionOutcome::Completed(output)) => {
            bounded_display_copy(output.content().as_str(), MAX_RESULT_DISPLAY_BYTES)
        }
        Err(error) => bounded_display_copy(safe_tool_error_text(*error), MAX_RESULT_DISPLAY_BYTES),
        Ok(ToolExecutionOutcome::RequestInput(_)) => None,
    };
    presentation.finish_tool(request_key, tool_call_id, result_text);
}
