//! Compile-only contract for the MiniCore Runtime API used by the Agent.
//!
//! This function is intentionally referenced but never executed. Its body makes
//! the compiler type-check the direct v0.3 public surface at the pinned path
//! dependency.

mod runtime_public_api_compile_contract {
    use std::sync::Arc;

    use futures_util::StreamExt;
    use tokio::sync::{mpsc, watch};

    use minicore_runtime::compaction::CompactionStrategy;
    use minicore_runtime::context::ContextProvider;
    use minicore_runtime::conversation::TranscriptPage;
    use minicore_runtime::error::{
        EventStreamTakenError, SessionError, SessionOpenError, SessionShutdownError, TurnWaitError,
    };
    use minicore_runtime::ids::TurnId;
    use minicore_runtime::model::{
        Model, ModelCallContext, ModelDescriptor, ModelRequest, ModelStartFuture,
    };
    use minicore_runtime::session::{
        InteractionAnswer, SessionEventEnvelope, SessionEventStream, SessionState, TurnOutcome,
    };
    use minicore_runtime::storage::SessionLog;
    use minicore_runtime::tools::{ApprovalDecision, Tool, ToolInputAnswer, ToolPolicy, ToolSet};
    use minicore_runtime::{
        CompactionConfig, InteractionId, KernelConfig, SessionBindings, SessionHandle, SessionId,
        SessionRuntime, SessionRuntimeOptions, SessionSpec, TurnHandle, TurnOptions, UserInput,
    };

    struct CompileInputs {
        session_id: SessionId,
        spec: SessionSpec,
        create_log: Box<dyn SessionLog>,
        load_log: Box<dyn SessionLog>,
        model: Arc<dyn Model>,
        model_request: ModelRequest,
        model_context: ModelCallContext,
        tools: ToolSet,
        tool: Arc<dyn Tool>,
        policy: Arc<dyn ToolPolicy>,
        context: Arc<dyn ContextProvider>,
        compaction: Arc<dyn CompactionStrategy>,
        runtime: tokio::runtime::Handle,
    }

    async fn compile_contract(inputs: CompileInputs) -> Result<(), Box<dyn std::error::Error>> {
        let CompileInputs {
            session_id,
            spec,
            create_log,
            load_log,
            model,
            model_request,
            model_context,
            tools,
            tool,
            policy,
            context,
            compaction,
            runtime,
        } = inputs;
        let _descriptor: &ModelDescriptor = model.descriptor();
        let start: ModelStartFuture<'_> = model.start(model_request, model_context);
        drop(start);
        let bindings: SessionBindings =
            SessionBindings::new(model, tools, Some(policy), Some(context), Some(compaction));
        let options = SessionRuntimeOptions::new(
            KernelConfig::default_checked()?,
            bindings.clone(),
            runtime.clone(),
        )?;
        let create_future = SessionRuntime::create(session_id, spec.clone(), create_log, options);
        let create_result: Result<SessionRuntime, SessionOpenError> = create_future.await;
        let mut owner = create_result?;

        let owner_session_id = owner.session_id();
        let owner_instance_id = owner.instance_id();
        let take_result: Result<SessionEventStream, EventStreamTakenError> = owner.take_events();
        let mut events = take_result?;
        let received: Option<SessionEventEnvelope> = events.recv().await;
        let tried: Result<SessionEventEnvelope, mpsc::error::TryRecvError> = events.try_recv();
        let streamed: Option<SessionEventEnvelope> = events.next().await;
        let dropped_before = received
            .as_ref()
            .map_or(0, |envelope| envelope.dropped_before);
        let _ = (received, tried, streamed, dropped_before);

        let handle: SessionHandle = owner.handle();
        assert_eq!(handle.session_id(), owner_session_id);
        assert_eq!(handle.instance_id(), owner_instance_id);
        let current: SessionState = handle.state();
        let state_watch: watch::Receiver<SessionState> = handle.watch_state();
        let _ = (current, state_watch.borrow().clone());

        let input = UserInput::text("compile contract")?;
        let submit_result: Result<TurnHandle, SessionError> =
            handle.submit(input, TurnOptions::default()).await;
        let turn = submit_result?;
        assert_eq!(turn.session_id(), owner_session_id);
        assert_eq!(turn.instance_id(), owner_instance_id);
        let turn_id: TurnId = turn.turn_id();
        let cancel_result: bool = turn.cancel();
        let finished_result: bool = turn.is_finished();
        let wait_result: Result<TurnOutcome, TurnWaitError> = turn.wait().await;
        let _ = (turn_id, cancel_result, finished_result, wait_result);

        let approval_result: Result<(), SessionError> = handle
            .answer(
                InteractionId::new()?,
                InteractionAnswer::Approval(ApprovalDecision::Deny),
            )
            .await;
        let input_result: Result<(), SessionError> = handle
            .answer(
                InteractionId::new()?,
                InteractionAnswer::ToolInput(ToolInputAnswer::Text(
                    minicore_runtime::BoundedText::new("answer")?,
                )),
            )
            .await;
        let transcript_result: Result<TranscriptPage, SessionError> =
            handle.transcript(None, 32).await;
        let _ = (approval_result, input_result, transcript_result);

        let shutdown_result: Result<(), SessionShutdownError> = owner.shutdown().await;
        shutdown_result?;

        let load_options =
            SessionRuntimeOptions::new(KernelConfig::default_checked()?, bindings, runtime)?;
        let load_future = SessionRuntime::load(session_id, load_log, load_options);
        let load_result: Result<SessionRuntime, SessionOpenError> = load_future.await;
        let mut loaded = load_result?;
        let _loaded_events: SessionEventStream = loaded.take_events()?;
        let _loaded_page: TranscriptPage = loaded.handle().transcript(None, 32).await?;
        let loaded_shutdown: Result<(), SessionShutdownError> = loaded.shutdown().await;
        loaded_shutdown?;
        let _ = (tool, CompactionConfig::Disabled);
        Ok(())
    }

    fn compile_contract_function_item_reference() {
        let _ = compile_contract;
    }

    const _: fn() = compile_contract_function_item_reference;
}
