use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::pending;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream;
use serde_json::json;
use tokio::sync::Semaphore;

use minicore_runtime::conversation::TurnTerminal;
use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, SessionShutdownError};
use minicore_runtime::ids::{SessionId, ToolCallId};
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason, ModelRef,
    ModelStartFuture, ModelStream, ReasoningPreference, Usage,
};
use minicore_runtime::session::SessionStatus;
use minicore_runtime::tools::{
    Tool, ToolContext, ToolDecision, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation,
    ToolOutput, ToolPolicy, ToolPolicyFuture, ToolPolicyRequest, ToolSet, ToolSpec,
};
use minicore_runtime::value::BoundedText;

use crate::config::{AgentConfig, KernelOverrides, Profile};
use crate::error::{AgentError, CoreErrorView};
use crate::event::AgentEvent;
use crate::models::Models;
use crate::profiles::{ApprovalMode, ProfileCompaction};

use super::{Agent, AnswerInteraction, CreateSession, GetTranscript, SendMessage, TurnRef};

#[derive(Clone)]
enum ModelScript {
    Text(&'static str),
    ReadCalls(usize),
    Block,
}

struct FakeModel {
    descriptor: ModelDescriptor,
    scripts: Arc<Mutex<VecDeque<ModelScript>>>,
    routed_scripts: Arc<Mutex<BTreeMap<SessionId, ModelScript>>>,
    started_sessions: Arc<Mutex<Vec<SessionId>>>,
    started: Option<Arc<Semaphore>>,
    calls: Arc<AtomicUsize>,
}

impl FakeModel {
    fn new(scripts: impl IntoIterator<Item = ModelScript>) -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let model_ref: ModelRef = "fake".parse().unwrap();
        let descriptor = ModelDescriptor::new(
            model_ref,
            16_384,
            BTreeSet::from([
                ReasoningPreference::Auto,
                ReasoningPreference::Disabled,
                ReasoningPreference::Low,
                ReasoningPreference::Medium,
                ReasoningPreference::High,
            ]),
            true,
        )
        .unwrap();
        let model = Arc::new(Self {
            descriptor,
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            routed_scripts: Arc::new(Mutex::new(BTreeMap::new())),
            started_sessions: Arc::new(Mutex::new(Vec::new())),
            started: None,
            calls: Arc::clone(&calls),
        });
        (model, calls)
    }

    fn with_started(mut self: Arc<Self>, started: Arc<Semaphore>) -> Arc<Self> {
        Arc::get_mut(&mut self).unwrap().started = Some(started);
        self
    }

    fn started_sessions(&self) -> Arc<Mutex<Vec<SessionId>>> {
        Arc::clone(&self.started_sessions)
    }

    fn route(&self, session_id: SessionId, script: ModelScript) {
        self.routed_scripts
            .lock()
            .unwrap()
            .insert(session_id, script);
    }
}

impl Model for FakeModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start<'a>(
        &'a self,
        _request: minicore_runtime::model::ModelRequest,
        context: ModelCallContext,
    ) -> ModelStartFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started_sessions
            .lock()
            .unwrap()
            .push(context.session_id);
        let script = self
            .routed_scripts
            .lock()
            .unwrap()
            .get(&context.session_id)
            .cloned()
            .or_else(|| self.scripts.lock().unwrap().pop_front())
            .unwrap_or(ModelScript::Text("default"));
        let started = self.started.clone();
        Box::pin(async move {
            if let Some(started) = started {
                started.add_permits(1);
            }
            match script {
                ModelScript::Block => {
                    let _: Result<ModelStream, ModelError> = pending().await;
                    unreachable!()
                }
                ModelScript::Text(text) => Ok(events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ])),
                ModelScript::ReadCalls(count) => {
                    let mut values = Vec::with_capacity(count.saturating_mul(3) + 2);
                    for index in 0..count {
                        let tool_call_id = ToolCallId::new(format!("read-call-{index}")).unwrap();
                        values.push(ModelEvent::ToolCallStart {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: "read".parse().unwrap(),
                        });
                        values.push(
                            ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), "{}")
                                .unwrap(),
                        );
                        values.push(ModelEvent::ToolCallEnd { tool_call_id });
                    }
                    values.push(ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    });
                    values.push(ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    });
                    Ok(events(values))
                }
            }
        })
    }
}

fn events(values: Vec<ModelEvent>) -> ModelStream {
    Box::pin(stream::iter(values.into_iter().map(Ok)))
}

struct FakeTool {
    spec: ToolSpec,
    started: Option<Arc<Semaphore>>,
    block: bool,
}

impl FakeTool {
    fn read() -> Self {
        Self {
            spec: ToolSpec::new(
                "read".parse().unwrap(),
                "read-like test tool",
                json!({"type": "object"}),
            )
            .unwrap(),
            started: None,
            block: false,
        }
    }

    fn blocking(mut self, started: Arc<Semaphore>) -> Self {
        self.started = Some(started);
        self.block = true;
        self
    }
}

impl Tool for FakeTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute<'a>(&'a self, _invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'a> {
        let started = self.started.clone();
        let block = self.block;
        Box::pin(async move {
            if let Some(started) = started {
                started.add_permits(1);
            }
            if block {
                context.cancellation.cancelled().await;
                return Err(ToolError::Cancelled);
            }
            Ok(ToolExecutionOutcome::Completed(
                ToolOutput::new("tool result").unwrap(),
            ))
        })
    }
}

struct AllowPolicy;

impl ToolPolicy for AllowPolicy {
    fn decide<'a>(&'a self, _request: ToolPolicyRequest) -> ToolPolicyFuture<'a> {
        Box::pin(async { Ok(ToolDecision::Allow) })
    }
}

fn tool_set(tool: Option<FakeTool>) -> ToolSet {
    let mut builder = ToolSet::builder();
    if let Some(tool) = tool {
        builder.register(tool);
    }
    builder.build().unwrap()
}

fn config(data_dir: PathBuf, tools: Vec<&str>) -> AgentConfig {
    let profile = Profile {
        model: "fake".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "You are a test agent.".to_owned(),
        tools: tools.into_iter().map(str::to_owned).collect(),
        max_tool_rounds: 4,
        approval: ApprovalMode::Auto,
        compaction: ProfileCompaction::Disabled,
    };
    AgentConfig {
        data_dir,
        event_capacity: 128,
        default_profile: "test".to_owned(),
        profiles: BTreeMap::from([(String::from("test"), profile)]),
        models: BTreeMap::new(),
        kernel: KernelOverrides::default(),
    }
}

fn models(model: Arc<FakeModel>) -> Models {
    let model: Arc<dyn Model> = model;
    Models::from_values(BTreeMap::from([(String::from("fake"), model)]))
}

async fn agent_fixture(
    label: &str,
    scripts: impl IntoIterator<Item = ModelScript>,
    tools: Vec<&str>,
    tool: Option<FakeTool>,
) -> (Agent, PathBuf, PathBuf, Arc<AtomicUsize>) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-loop-{label}-{}",
        SessionId::new().unwrap()
    ));
    let data_dir = base.join("data");
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, calls) = FakeModel::new(scripts);
    let agent = Agent::open_with_models(
        config(data_dir, tools),
        models(model),
        tool_set(tool),
        Some(Arc::new(AllowPolicy)),
    )
    .await
    .unwrap();
    (agent, base, workspace, calls)
}

async fn next_event(stream: &mut crate::event::AgentEventStream) -> AgentEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn wait_for_event(
    stream: &mut crate::event::AgentEventStream,
    matches: impl Fn(&AgentEvent) -> bool,
) -> AgentEvent {
    loop {
        let event = next_event(stream).await;
        if matches(&event) {
            return event;
        }
    }
}

async fn remove_base(base: &Path) {
    let _ = tokio::fs::remove_dir_all(base).await;
}

fn create_request(workspace: &Path) -> CreateSession {
    CreateSession {
        workspace: workspace.to_path_buf(),
        profile: "test".to_owned(),
        title: Some("loop test".to_owned()),
    }
}

#[tokio::test]
async fn agent_lifecycle_text_turn_and_durable_finish_event() {
    let (mut agent, base, workspace, calls) =
        agent_fixture("lifecycle", [ModelScript::Text("final")], Vec::new(), None).await;
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    assert!(info.loaded);
    assert!(matches!(
        wait_for_event(&mut events, |event| {
            matches!(event, AgentEvent::SessionOpened { session, .. } if session.session_id == info.session_id)
        })
        .await,
        AgentEvent::SessionOpened { .. }
    ));
    assert!(matches!(
        wait_for_event(&mut events, |event| {
            matches!(event, AgentEvent::SessionState { state, .. } if state.session_id == info.session_id && matches!(state.status, SessionStatus::Idle))
        })
        .await,
        AgentEvent::SessionState { .. }
    ));
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        SessionStatus::Idle
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(agent.list_sessions().await.unwrap().len(), 1);
    let reopened = agent.open_session(info.session_id).await.unwrap();
    assert_eq!(reopened.instance_id, info.instance_id);
    assert!(matches!(
        agent.delete_session(info.session_id).await,
        Err(crate::error::AgentError::SessionAlreadyLoaded)
    ));

    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "hello".to_owned(),
        })
        .await
        .unwrap();
    let outcome = agent.turn_handle(turn).unwrap().wait().await.unwrap();
    assert_eq!(outcome.terminal, TurnTerminal::Completed);
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        SessionStatus::Idle
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let finished = wait_for_event(
        &mut events,
        |event| matches!(event, AgentEvent::TurnFinished { turn: value, .. } if *value == turn),
    )
    .await;
    assert!(
        matches!(finished, AgentEvent::TurnFinished { outcome, .. } if outcome.turn_id == turn.turn_id)
    );
    let transcript = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 32,
        })
        .await
        .unwrap();
    assert!(transcript.complete);
    assert!(transcript.entries.len() >= 3);

    assert!(matches!(
        agent
            .answer(AnswerInteraction {
                session_id: info.session_id,
                interaction_id: minicore_runtime::InteractionId::new().unwrap(),
                answer: minicore_runtime::InteractionAnswer::Approval(
                    minicore_runtime::tools::ApprovalDecision::Deny,
                ),
            })
            .await,
        Err(crate::error::AgentError::InteractionNotFound)
    ));
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        SessionStatus::Idle
    );
    agent.close_session(info.session_id).await.unwrap();
    assert!(matches!(
        wait_for_event(&mut events, |event| {
            matches!(event, AgentEvent::SessionClosed { session_id, .. } if *session_id == info.session_id)
        })
        .await,
        AgentEvent::SessionClosed { .. }
    ));
    assert!(!agent.list_sessions().await.unwrap()[0].loaded);
    agent.delete_session(info.session_id).await.unwrap();
    assert!(agent.list_sessions().await.unwrap().is_empty());
    agent.shutdown().await.unwrap();
    while events.recv().await.is_some() {}
    remove_base(&base).await;
}

#[tokio::test]
async fn fake_read_tool_runs_through_runtime_model_tool_model_loop() {
    let (mut agent, base, workspace, calls) = agent_fixture(
        "tool-loop",
        [ModelScript::ReadCalls(2), ModelScript::Text("tool final")],
        vec!["read"],
        Some(FakeTool::read()),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "use the read tool".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent
            .turn_handle(turn)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let mut started_count = 0;
    let mut finished_count = 0;
    while started_count < 2 || finished_count < 2 {
        match next_event(&mut events).await {
            AgentEvent::ToolStarted {
                turn: value,
                tool_name,
                ..
            } if value == turn && tool_name == "read" => started_count += 1,
            AgentEvent::ToolFinished { turn: value, .. } if value == turn => finished_count += 1,
            _ => {}
        }
    }
    wait_for_event(&mut events, |event| {
        matches!(event, AgentEvent::OutputDelta { turn: value, delta, .. } if *value == turn && delta == "tool final")
    })
    .await;
    let page = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 32,
        })
        .await
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .filter(|entry| matches!(
                entry,
                minicore_runtime::ConversationEntry::ToolResult(result)
                    if result.outcome == minicore_runtime::tools::ToolResultOutcome::Success
            ))
            .count(),
        2
    );
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn exact_cancel_and_busy_are_scoped_to_one_session() {
    let started = Arc::new(Semaphore::new(0));
    let (model, calls) = FakeModel::new([ModelScript::Block, ModelScript::Text("after")]);
    let model = model.with_started(Arc::clone(&started));
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-cancel-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let mut agent = Agent::open_with_models(
        config(base.join("data"), Vec::new()),
        models(model),
        ToolSet::default(),
        None,
    )
    .await
    .unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let first = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "long".to_owned(),
        })
        .await
        .unwrap();
    started.acquire().await.unwrap().forget();
    assert!(matches!(
        agent
            .send(SendMessage {
                session_id: info.session_id,
                text: "busy".to_owned(),
            })
            .await,
        Err(crate::error::AgentError::SessionBusy)
    ));
    let wrong = TurnRef {
        turn_id: minicore_runtime::TurnId::new().unwrap(),
        ..first
    };
    assert!(matches!(
        agent.cancel(wrong),
        Err(crate::error::AgentError::TurnNotFound)
    ));
    assert!(agent.cancel(first).unwrap());
    assert_eq!(
        agent
            .turn_handle(first)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::CancelledByUser
    );
    let second = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "after".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent
            .turn_handle(second)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn exact_cancel_interrupts_a_long_running_tool() {
    let started = Arc::new(Semaphore::new(0));
    let (mut agent, base, workspace, _) = agent_fixture(
        "tool-cancel",
        [ModelScript::ReadCalls(1)],
        vec!["read"],
        Some(FakeTool::read().blocking(Arc::clone(&started))),
    )
    .await;
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "cancel tool".to_owned(),
        })
        .await
        .unwrap();
    started.acquire().await.unwrap().forget();
    assert!(agent.cancel(turn).unwrap());
    assert_eq!(
        agent
            .turn_handle(turn)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::CancelledByUser
    );
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn loaded_open_is_idempotent_without_reading_session_metadata() {
    let (mut agent, base, workspace, _) = agent_fixture(
        "loaded-idempotent",
        [ModelScript::Text("unused")],
        Vec::new(),
        None,
    )
    .await;
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let record_path = base
        .join("data")
        .join("sessions")
        .join(info.session_id.to_string())
        .join("session.json");
    tokio::fs::write(&record_path, b"not json").await.unwrap();
    assert_eq!(agent.open_session(info.session_id).await.unwrap(), info);
    tokio::fs::remove_file(&record_path).await.unwrap();
    assert_eq!(agent.open_session(info.session_id).await.unwrap(), info);
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "metadata is unavailable".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent
            .turn_handle(turn)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::Completed
    );
    assert_eq!(
        agent
            .open_session(info.session_id)
            .await
            .unwrap()
            .updated_at,
        info.updated_at
    );
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn old_instance_turn_references_are_rejected_after_reopen() {
    let (mut agent, base, workspace, _) = agent_fixture(
        "old-instance",
        [ModelScript::Text("unused")],
        Vec::new(),
        None,
    )
    .await;
    let created = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let old_instance = created.instance_id.unwrap();
    agent.close_session(created.session_id).await.unwrap();
    let reopened = agent.open_session(created.session_id).await.unwrap();
    assert_ne!(reopened.instance_id, Some(old_instance));
    let stale = TurnRef {
        session_id: created.session_id,
        instance_id: old_instance,
        turn_id: minicore_runtime::TurnId::new().unwrap(),
    };
    assert!(matches!(agent.cancel(stale), Err(AgentError::TurnNotFound)));
    assert!(matches!(
        agent.turn_handle(stale),
        Err(AgentError::TurnNotFound)
    ));
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

async fn assert_profile_drift(label: &str, change: fn(&mut Profile)) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-profile-drift-{label}-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (old_model, _) = FakeModel::new([ModelScript::Text("old")]);
    let mut old_agent = Agent::open_with_models(
        config(base.join("data"), Vec::new()),
        models(old_model),
        ToolSet::default(),
        None,
    )
    .await
    .unwrap();
    let info = old_agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    old_agent.shutdown().await.unwrap();
    let manifest_path = base
        .join("data")
        .join("sessions")
        .join(info.session_id.to_string())
        .join("manifest.json");
    let manifest_before = tokio::fs::read(&manifest_path).await.unwrap();

    let (new_model, _) = FakeModel::new([ModelScript::Text("new")]);
    let mut new_config = config(base.join("data"), Vec::new());
    change(new_config.profiles.get_mut("test").unwrap());
    let mut new_agent =
        Agent::open_with_models(new_config, models(new_model), ToolSet::default(), None)
            .await
            .unwrap();
    assert!(matches!(
        new_agent.open_session(info.session_id).await,
        Err(AgentError::SessionSpecMismatch)
    ));
    assert_eq!(
        tokio::fs::read(&manifest_path).await.unwrap(),
        manifest_before
    );
    new_agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

fn drift_system_prompt(profile: &mut Profile) {
    profile.system_prompt = "changed system prompt".to_owned();
}

fn drift_reasoning(profile: &mut Profile) {
    profile.reasoning = ReasoningPreference::High;
}

fn drift_max_rounds(profile: &mut Profile) {
    profile.max_tool_rounds = 5;
}

#[tokio::test]
async fn opening_rejects_each_supported_profile_spec_drift() {
    assert_profile_drift("system-prompt", drift_system_prompt).await;
    assert_profile_drift("reasoning", drift_reasoning).await;
    assert_profile_drift("max-rounds", drift_max_rounds).await;
}

fn shutdown_diagnostic(retryable: bool) -> minicore_runtime::error::DiagnosticSummary {
    minicore_runtime::error::DiagnosticSummary::new(
        DiagnosticCode::Internal,
        DiagnosticCategory::Internal,
        BoundedText::new("private shutdown detail").unwrap(),
        retryable,
    )
}

fn assert_shutdown_error(error: SessionShutdownError, kind: &'static str, retryable: bool) {
    assert!(matches!(
        crate::sessions::map_session_shutdown_error(error),
        AgentError::Core(CoreErrorView {
            kind: value,
            retryable: actual,
        }) if value == kind && actual == retryable
    ));
}

#[test]
fn shutdown_errors_preserve_safe_public_categories_without_debug_details() {
    assert_shutdown_error(
        SessionShutdownError::Timeout(shutdown_diagnostic(false)),
        "session shutdown timeout",
        false,
    );
    assert_shutdown_error(
        SessionShutdownError::Durability(shutdown_diagnostic(false)),
        "session shutdown durability failed",
        false,
    );
    assert_shutdown_error(
        SessionShutdownError::LogClose(shutdown_diagnostic(true)),
        "session log close failed",
        true,
    );
    assert_shutdown_error(
        SessionShutdownError::ActorTerminated(shutdown_diagnostic(false)),
        "session actor terminated",
        false,
    );
}

#[tokio::test]
async fn small_outer_capacity_still_delivers_durable_turn_finished() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-durable-event-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("final")]);
    let mut config = config(base.join("data"), Vec::new());
    config.event_capacity = 1;
    let mut agent = Agent::open_with_models(config, models(model), ToolSet::default(), None)
        .await
        .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "hello".to_owned(),
        })
        .await
        .unwrap();
    let finished = wait_for_event(
        &mut events,
        |event| matches!(event, AgentEvent::TurnFinished { turn: value, .. } if *value == turn),
    )
    .await;
    assert!(matches!(finished, AgentEvent::TurnFinished { .. }));
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn dropped_event_receiver_does_not_block_agent_shutdown() {
    let (mut agent, base, workspace, _) = agent_fixture(
        "dropped-events",
        [ModelScript::Text("final")],
        Vec::new(),
        None,
    )
    .await;
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "hello".to_owned(),
        })
        .await
        .unwrap();
    turn_handle_wait(&agent, turn).await;
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn shutdown_closes_events_after_cancelling_detached_completion_waiter() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-shutdown-events-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let started = Arc::new(Semaphore::new(0));
    let (model, _) = FakeModel::new([ModelScript::Block]);
    let model = model.with_started(Arc::clone(&started));
    let mut config = config(base.join("data"), Vec::new());
    config.event_capacity = 1;
    let mut agent = Agent::open_with_models(config, models(model), ToolSet::default(), None)
        .await
        .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "cancel before shutdown".to_owned(),
        })
        .await
        .unwrap();
    started.acquire().await.unwrap().forget();
    assert!(agent.cancel(turn).unwrap());
    assert_eq!(
        agent
            .turn_handle(turn)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::CancelledByUser
    );
    agent.shutdown().await.unwrap();
    while tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .is_some()
    {}
    remove_base(&base).await;
}

async fn turn_handle_wait(agent: &Agent, turn: TurnRef) {
    agent.turn_handle(turn).unwrap().wait().await.unwrap();
}

#[tokio::test]
async fn two_loaded_sessions_cancel_independently_and_shutdown_all() {
    let started = Arc::new(Semaphore::new(0));
    let (model, _) = FakeModel::new([ModelScript::Text("unrouted")]);
    let model = model.with_started(Arc::clone(&started));
    let routed_model = Arc::clone(&model);
    let started_sessions = model.started_sessions();
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-two-sessions-{}",
        SessionId::new().unwrap()
    ));
    let workspace_a = base.join("a");
    let workspace_b = base.join("b");
    tokio::fs::create_dir_all(&workspace_a).await.unwrap();
    tokio::fs::create_dir_all(&workspace_b).await.unwrap();
    let mut agent = Agent::open_with_models(
        config(base.join("data"), Vec::new()),
        models(model),
        ToolSet::default(),
        None,
    )
    .await
    .unwrap();
    let a = agent
        .create_session(create_request(&workspace_a))
        .await
        .unwrap();
    let b = agent
        .create_session(create_request(&workspace_b))
        .await
        .unwrap();
    routed_model.route(a.session_id, ModelScript::Block);
    routed_model.route(b.session_id, ModelScript::Block);
    let turn_a = agent
        .send(SendMessage {
            session_id: a.session_id,
            text: "a".to_owned(),
        })
        .await
        .unwrap();
    let turn_b = agent
        .send(SendMessage {
            session_id: b.session_id,
            text: "b".to_owned(),
        })
        .await
        .unwrap();
    started.acquire().await.unwrap().forget();
    started.acquire().await.unwrap().forget();
    let started_sessions = started_sessions.lock().unwrap().clone();
    assert!(started_sessions.contains(&turn_a.session_id));
    assert!(started_sessions.contains(&turn_b.session_id));
    assert!(agent.cancel(turn_a).unwrap());
    assert!(!agent.turn_handle(turn_b).unwrap().is_finished());
    assert_eq!(
        agent
            .turn_handle(turn_a)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::CancelledByUser
    );
    assert!(agent.cancel(turn_b).unwrap());
    assert_eq!(
        agent
            .turn_handle(turn_b)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::CancelledByUser
    );
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn restart_repairs_durable_unfinished_turn_then_accepts_new_turn() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-restart-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let started = Arc::new(Semaphore::new(0));
    let (old_model, _) = FakeModel::new([ModelScript::Block]);
    let old_model = old_model.with_started(Arc::clone(&started));
    let old_config = config(base.join("data"), Vec::new());
    let old_workspace = workspace.clone();
    let old_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let mut agent =
                Agent::open_with_models(old_config, models(old_model), ToolSet::default(), None)
                    .await
                    .unwrap();
            let info = agent
                .create_session(create_request(&old_workspace))
                .await
                .unwrap();
            let _turn = agent
                .send(SendMessage {
                    session_id: info.session_id,
                    text: "unfinished".to_owned(),
                })
                .await
                .unwrap();
            started.acquire().await.unwrap().forget();
            drop(agent);
            info.session_id
        })
    });
    let session_id = old_thread.join().unwrap();

    let (new_model, calls) = FakeModel::new([ModelScript::Text("restarted")]);
    let mut agent = Agent::open_with_models(
        config(base.join("data"), Vec::new()),
        models(new_model),
        ToolSet::default(),
        None,
    )
    .await
    .unwrap();
    let info = agent.open_session(session_id).await.unwrap();
    let state = agent.session_state(session_id).unwrap();
    assert_eq!(state.status, SessionStatus::Idle);
    assert!(state.pending_interaction.is_none());
    assert!(matches!(
        state.last_terminal,
        Some(outcome) if outcome.terminal == TurnTerminal::CancelledByRestart
    ));
    assert!(matches!(
        state.health,
        minicore_runtime::session::SessionHealth::Healthy
    ));
    let transcript = agent
        .transcript(GetTranscript {
            session_id,
            after: None,
            limit: 32,
        })
        .await
        .unwrap();
    assert!(transcript.entries.iter().any(|entry| matches!(
        entry,
        minicore_runtime::ConversationEntry::UserMessage(message)
            if message.input.text.as_str() == "unfinished"
    )));
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "continue".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent
            .turn_handle(turn)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn open_rejects_real_openai_config_instead_of_claiming_provider_support() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-model-config-{}",
        SessionId::new().unwrap()
    ));
    let text = format!(
        r#"
data_dir = "{}"
default_profile = "test"

[profiles.test]
model = "main"
system_prompt = "test"

[models.main]
provider = "open_ai_responses"
model = "gpt-test"
base_url = "https://example.invalid/v1"
api_key_env = "TEST_KEY"
physical_context_window = 1000
output_budget_tokens = 100
safety_margin_tokens = 100
supported_reasoning = ["auto"]
supports_tools = false
"#,
        base.display()
    );
    let config = AgentConfig::from_toml(&text).unwrap();
    assert!(matches!(
        Agent::open(config).await,
        Err(crate::error::AgentError::ModelNotImplemented)
    ));
    remove_base(&base).await;
}
