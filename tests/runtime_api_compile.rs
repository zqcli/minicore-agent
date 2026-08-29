//! Compile-only contract for the MiniCore Runtime API used by the Agent.
//!
//! The contract owns small public-port implementations and calls their methods,
//! but the contract function is only referenced as an item and is never run.

// This contract is referenced only as a function item for type checking and is intentionally not executed.
#[allow(dead_code)]
mod runtime_public_api_compile_contract {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures_util::{StreamExt, stream};
    use serde_json::json;
    use tokio::runtime::Handle;
    use tokio::sync::watch;
    use tokio_util::sync::CancellationToken;

    use minicore_runtime::compaction::{
        CompactionCandidate, CompactionError, CompactionFuture, CompactionProposal,
        CompactionRequest, CompactionStrategy,
    };
    use minicore_runtime::context::{
        ContextBundle, ContextFuture, ContextProvider, ContextRequest,
    };
    use minicore_runtime::conversation::{
        ConversationEntry, ConversationSeq, ConversationView, TranscriptPage,
    };
    use minicore_runtime::error::{
        DiagnosticCategory, DiagnosticCode, DiagnosticSummary, EventStreamTakenError, SessionError,
        SessionLogError, SessionLogErrorKind, SessionOpenError, SessionShutdownError,
        TurnWaitError,
    };
    use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, ToolCallId, TurnId};
    use minicore_runtime::model::{
        AssistantPart, Model, ModelCallContext, ModelDescriptor, ModelEvent, ModelFinishReason,
        ModelLimits, ModelMessage, ModelRef, ModelRequest, ModelResponse, ModelStartFuture,
        ModelStream, ModelValueError, ReasoningPreference, ToolCall, Usage,
    };
    use minicore_runtime::session::{
        InteractionAnswer, SessionEventEnvelope, SessionEventStream, SessionState, TurnOutcome,
    };
    use minicore_runtime::storage::{AppendReceipt, ConversationPage, LogFuture, SessionLog};
    use minicore_runtime::tools::{
        ApprovalDecision, Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture,
        ToolInputAnswer, ToolInvocation, ToolName, ToolOutput, ToolPolicy, ToolPolicyFuture,
        ToolPolicyRequest, ToolResultOutcome, ToolSet, ToolSpec, ToolValueError,
    };
    use minicore_runtime::value::BoundedText;
    use minicore_runtime::{
        CompactionConfig, KernelConfig, SemanticLimits, SessionBindings, SessionHandle,
        SessionManifest, SessionRuntime, SessionRuntimeOptions, SessionSpec, TurnHandle,
        TurnOptions, UserInput,
    };

    struct DummyModel {
        descriptor: ModelDescriptor,
    }

    impl DummyModel {
        fn new(model_ref: ModelRef) -> Result<Self, ModelValueError> {
            Ok(Self {
                descriptor: ModelDescriptor::new(
                    model_ref,
                    128,
                    BTreeSet::from([ReasoningPreference::Auto]),
                    true,
                )?,
            })
        }
    }

    impl Model for DummyModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn start<'a>(
            &'a self,
            _request: ModelRequest,
            _context: ModelCallContext,
        ) -> ModelStartFuture<'a> {
            Box::pin(async {
                let stream: ModelStream = Box::pin(stream::iter(vec![Ok(ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                })]));
                Ok(stream)
            })
        }
    }

    struct DummyTool {
        spec: ToolSpec,
    }

    impl DummyTool {
        fn new(name: ToolName) -> Result<Self, ToolValueError> {
            Ok(Self {
                spec: ToolSpec::new(name, "compile-only tool", json!({"type": "object"}))?,
            })
        }
    }

    impl Tool for DummyTool {
        fn spec(&self) -> &ToolSpec {
            &self.spec
        }

        fn execute<'a>(
            &'a self,
            _invocation: ToolInvocation,
            _context: ToolContext,
        ) -> ToolFuture<'a> {
            Box::pin(async {
                ToolOutput::new("ok")
                    .map(ToolExecutionOutcome::Completed)
                    .map_err(|_| ToolError::Internal)
            })
        }
    }

    struct DummyPolicy;

    impl ToolPolicy for DummyPolicy {
        fn decide<'a>(&'a self, _request: ToolPolicyRequest) -> ToolPolicyFuture<'a> {
            Box::pin(async { Ok(minicore_runtime::tools::ToolDecision::Allow) })
        }
    }

    struct DummyContext;

    impl ContextProvider for DummyContext {
        fn provide<'a>(&'a self, _request: ContextRequest) -> ContextFuture<'a> {
            Box::pin(async { Ok(ContextBundle { blocks: Vec::new() }) })
        }
    }

    struct DummyCompaction;

    impl CompactionStrategy for DummyCompaction {
        fn compact<'a>(&'a self, _request: CompactionRequest) -> CompactionFuture<'a> {
            Box::pin(async {
                Ok(CompactionProposal {
                    through_seq: ConversationSeq::ZERO,
                    summary: BoundedText::new("summary").map_err(|_| CompactionError::Internal)?,
                })
            })
        }
    }

    struct DummyLog;

    impl DummyLog {
        fn unavailable() -> SessionLogError {
            SessionLogError::new(
                SessionLogErrorKind::Unavailable,
                DiagnosticSummary::new(
                    DiagnosticCode::Internal,
                    DiagnosticCategory::Storage,
                    BoundedText::new("compile-only log").expect("static text fits"),
                    true,
                ),
            )
        }
    }

    impl SessionLog for DummyLog {
        fn initialize<'a>(
            &'a mut self,
            _manifest: SessionManifest,
        ) -> LogFuture<'a, ConversationSeq> {
            Box::pin(async { Ok(ConversationSeq::ZERO) })
        }

        fn load_manifest<'a>(&'a mut self) -> LogFuture<'a, SessionManifest> {
            Box::pin(async { Err(Self::unavailable()) })
        }

        fn read_page<'a>(
            &'a mut self,
            _after: Option<ConversationSeq>,
            _limit: usize,
        ) -> LogFuture<'a, ConversationPage> {
            Box::pin(async {
                Ok(ConversationPage {
                    entries: Vec::new(),
                    next_after: None,
                    observed_head: ConversationSeq::ZERO,
                })
            })
        }

        fn append<'a>(
            &'a mut self,
            expected_head: ConversationSeq,
            entries: Vec<ConversationEntry>,
        ) -> LogFuture<'a, AppendReceipt> {
            let appended = entries.len();
            Box::pin(async move {
                Ok(AppendReceipt {
                    previous_head: expected_head,
                    new_head: expected_head,
                    appended,
                })
            })
        }

        fn close<'a>(&'a mut self) -> LogFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    async fn compile_contract() -> Result<(), Box<dyn std::error::Error>> {
        let session_id = SessionId::new()?;
        let instance_id = SessionInstanceId::new()?;
        let turn_id = TurnId::new()?;
        let model_ref: ModelRef = "compile-model".parse()?;
        let tool_name: ToolName = "dummy".parse()?;
        let tool_call_id = ToolCallId::new("call-1")?;

        let model: Arc<dyn Model> = Arc::new(DummyModel::new(model_ref.clone())?);
        let tool: Arc<dyn Tool> = Arc::new(DummyTool::new(tool_name.clone())?);
        let tool_spec = tool.spec().clone();
        let tool_call = ToolCall::new(tool_call_id.clone(), tool_name.clone(), json!({}), 0)?;
        let _assistant = ModelMessage::assistant(vec![AssistantPart::ToolCall(tool_call)])?;
        let tool_output = ToolOutput::new("ok")?;
        let _tool_message = ModelMessage::tool_with_outcome(
            tool_call_id.clone(),
            tool_output,
            ToolResultOutcome::Success,
        )?;
        let request = ModelRequest::new(
            vec![ModelMessage::user("compile contract")?],
            vec![tool_spec.clone()],
            ModelLimits::new(Some(128), Some(16))?,
            ReasoningPreference::Auto,
        )?;
        let _response = ModelResponse::new(
            vec![AssistantPart::Text("done".to_owned())],
            ModelFinishReason::Stop,
            Usage::new(1, 2, 0),
        )?;
        let _event = ModelEvent::text_delta("delta")?;

        let deadline = Instant::now() + Duration::from_secs(60);
        let model_context = ModelCallContext::new(
            session_id,
            instance_id,
            turn_id,
            0,
            CancellationToken::new(),
            deadline,
        );
        let model_start: ModelStartFuture<'_> = model.start(request, model_context);
        drop(model_start);

        let invocation = ToolInvocation::new(
            session_id,
            instance_id,
            turn_id,
            tool_call_id.clone(),
            tool_name.clone(),
            json!({}),
        )?;
        let tool_future = tool.execute(
            invocation.clone(),
            ToolContext {
                cancellation: CancellationToken::new(),
                deadline,
                progress: minicore_runtime::tools::ToolProgressSink::default(),
            },
        );
        drop(tool_future);

        let policy: Arc<dyn ToolPolicy> = Arc::new(DummyPolicy);
        let policy_future = policy.decide(ToolPolicyRequest {
            invocation: invocation.clone(),
            spec: tool_spec.clone(),
            cancellation: CancellationToken::new(),
            deadline,
        });
        drop(policy_future);

        let context: Arc<dyn ContextProvider> = Arc::new(DummyContext);
        let context_future = context.provide(ContextRequest {
            session_id,
            instance_id,
            turn_id,
            model_round: 0,
            conversation: ConversationView::empty(),
            remaining_context_budget: 128,
            cancellation: CancellationToken::new(),
            deadline,
        });
        drop(context_future);

        let compaction: Arc<dyn CompactionStrategy> = Arc::new(DummyCompaction);
        let compaction_future = compaction.compact(CompactionRequest {
            session_id,
            turn_id,
            candidate: CompactionCandidate::empty(),
            target_tokens: 16,
            cancellation: CancellationToken::new(),
            deadline,
        });
        drop(compaction_future);

        let mut tool_builder = ToolSet::builder();
        tool_builder.register_arc(Arc::clone(&tool));
        let tools = tool_builder.build()?;
        let spec = SessionSpec::new(
            model_ref,
            ReasoningPreference::Auto,
            BoundedText::new("system")?,
            BTreeSet::from([tool_name]),
            1,
            CompactionConfig::Enabled {
                trigger_tokens: 32,
                target_tokens: 16,
            },
        )?;
        let manifest = SessionManifest::new(session_id, spec.clone())?;
        let bindings = SessionBindings::new(
            Arc::clone(&model),
            tools,
            Some(Arc::clone(&policy)),
            Some(Arc::clone(&context)),
            Some(Arc::clone(&compaction)),
        );
        bindings.validate(&spec, &SemanticLimits::default())?;
        let options = SessionRuntimeOptions::new(
            KernelConfig::default_checked()?,
            bindings.clone(),
            Handle::current(),
        )?;

        let mut log = DummyLog;
        drop(log.initialize(manifest.clone()));
        drop(log.load_manifest());
        drop(log.read_page(None, 1));
        drop(log.append(ConversationSeq::ZERO, Vec::new()));
        drop(log.close());

        let create_result: Result<SessionRuntime, SessionOpenError> =
            SessionRuntime::create(session_id, spec.clone(), Box::new(DummyLog), options).await;
        let mut owner = create_result?;
        let owner_session_id = owner.session_id();
        let owner_instance_id = owner.instance_id();
        let take_result: Result<SessionEventStream, EventStreamTakenError> = owner.take_events();
        let mut events = take_result?;
        let received: Option<SessionEventEnvelope> = events.recv().await;
        let _streamed: Option<SessionEventEnvelope> = events.next().await;
        let _ = received;

        let handle: SessionHandle = owner.handle();
        let _state: SessionState = handle.state();
        let _state_watch: watch::Receiver<SessionState> = handle.watch_state();
        let turn: TurnHandle = handle
            .submit(UserInput::text("compile contract")?, TurnOptions::default())
            .await?;
        assert_eq!(turn.session_id(), owner_session_id);
        assert_eq!(turn.instance_id(), owner_instance_id);
        let _cancelled = turn.cancel();
        let _finished = turn.is_finished();
        let _outcome: Result<TurnOutcome, TurnWaitError> = turn.wait().await;
        let _approval = handle
            .answer(
                InteractionId::new()?,
                InteractionAnswer::Approval(ApprovalDecision::Deny),
            )
            .await;
        let _input = handle
            .answer(
                InteractionId::new()?,
                InteractionAnswer::ToolInput(ToolInputAnswer::Text(BoundedText::new("answer")?)),
            )
            .await;
        let _transcript: Result<TranscriptPage, SessionError> = handle.transcript(None, 32).await;
        let shutdown_result: Result<(), SessionShutdownError> = owner.shutdown().await;
        shutdown_result?;

        let load_options = SessionRuntimeOptions::new(
            KernelConfig::default_checked()?,
            bindings,
            Handle::current(),
        )?;
        let load_result: Result<SessionRuntime, SessionOpenError> =
            SessionRuntime::load(session_id, Box::new(DummyLog), load_options).await;
        let mut loaded = load_result?;
        let _loaded_events: SessionEventStream = loaded.take_events()?;
        let _loaded_page: TranscriptPage = loaded.handle().transcript(None, 32).await?;
        loaded.shutdown().await?;
        Ok(())
    }

    fn compile_contract_function_item_reference() {
        let _ = compile_contract;
    }

    const _: fn() = compile_contract_function_item_reference;
}
