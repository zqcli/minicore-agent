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
use minicore_runtime::ids::{SessionId, SessionInstanceId, ToolCallId, TurnId};
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason, ModelRef,
    ModelRequest, ModelStartFuture, ModelStream, ReasoningPreference, Usage,
};
use minicore_runtime::session::SessionStatus;
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
    ToolCalls(Vec<ToolCallScript>),
    Block,
}

#[derive(Clone)]
struct ToolCallScript {
    name: &'static str,
    arguments: serde_json::Value,
}

impl ModelScript {
    fn tool_call(name: &'static str, arguments: serde_json::Value) -> Self {
        Self::ToolCalls(vec![ToolCallScript { name, arguments }])
    }

    fn tool_calls(calls: Vec<ToolCallScript>) -> Self {
        Self::ToolCalls(calls)
    }
}

fn scripted_call(name: &'static str, arguments: serde_json::Value) -> ToolCallScript {
    ToolCallScript { name, arguments }
}

struct FakeModel {
    descriptor: ModelDescriptor,
    scripts: Arc<Mutex<VecDeque<ModelScript>>>,
    routed_scripts: Arc<Mutex<BTreeMap<SessionId, ModelScript>>>,
    started_sessions: Arc<Mutex<Vec<SessionId>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
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
            requests: Arc::new(Mutex::new(Vec::new())),
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

    fn requests(&self) -> Arc<Mutex<Vec<ModelRequest>>> {
        Arc::clone(&self.requests)
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
        request: minicore_runtime::model::ModelRequest,
        context: ModelCallContext,
    ) -> ModelStartFuture<'a> {
        let model_call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
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
                ModelScript::ToolCalls(calls) => {
                    let mut values = Vec::with_capacity(calls.len().saturating_mul(3) + 2);
                    for (index, call) in calls.into_iter().enumerate() {
                        let tool_call_id = ToolCallId::new(format!(
                            "{}-call-{model_call_index}-{index}",
                            call.name
                        ))
                        .unwrap();
                        values.push(ModelEvent::ToolCallStart {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: call.name.parse().unwrap(),
                        });
                        values.push(
                            ModelEvent::tool_call_arguments_delta(
                                tool_call_id.clone(),
                                serde_json::to_string(&call.arguments).unwrap(),
                            )
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

fn config_with_approval(
    data_dir: PathBuf,
    tools: Vec<&str>,
    approval: ApprovalMode,
) -> AgentConfig {
    let mut config = config(data_dir, tools);
    config.profiles.get_mut("test").unwrap().approval = approval;
    config
}

fn models(model: Arc<FakeModel>) -> Models {
    let model: Arc<dyn Model> = model;
    Models::from_values(BTreeMap::from([(String::from("fake"), model)]))
}

async fn agent_fixture(
    label: &str,
    scripts: impl IntoIterator<Item = ModelScript>,
    tools: Vec<&str>,
) -> (Agent, PathBuf, PathBuf, Arc<AtomicUsize>) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-loop-{label}-{}",
        SessionId::new().unwrap()
    ));
    let data_dir = base.join("data");
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, calls) = FakeModel::new(scripts);
    let agent = Agent::open_with_models(config(data_dir, tools), models(model))
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

#[cfg(unix)]
async fn read_test_pid(path: &Path) -> u32 {
    for _ in 0..200 {
        if let Ok(value) = tokio::fs::read_to_string(path).await {
            if let Ok(pid) = value.trim().parse() {
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("bash child did not publish its PID");
}

#[cfg(unix)]
async fn wait_for_test_process_exit(pid: u32) {
    for _ in 0..200 {
        if !test_process_exists(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("bash child PID {pid} remained alive");
}

#[cfg(unix)]
fn test_process_exists(pid: u32) -> bool {
    std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -0 {pid} 2>/dev/null"))
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(unix)]
struct TestProcessGuard {
    pid: Option<u32>,
}

#[cfg(unix)]
impl TestProcessGuard {
    fn new(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

#[cfg(unix)]
impl Drop for TestProcessGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            let _ = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(format!("kill -KILL {pid} 2>/dev/null"))
                .status();
        }
    }
}

fn create_request(workspace: &Path) -> CreateSession {
    CreateSession {
        workspace: workspace.to_path_buf(),
        profile: "test".to_owned(),
        title: Some("loop test".to_owned()),
    }
}

fn session_directory(base: &Path, session_id: SessionId) -> PathBuf {
    base.join("data")
        .join("sessions")
        .join(session_id.to_string())
}

fn session_record_path(base: &Path, session_id: SessionId) -> PathBuf {
    session_directory(base, session_id).join("session.json")
}

#[tokio::test]
async fn agent_lifecycle_text_turn_and_durable_finish_event() {
    let (mut agent, base, workspace, calls) =
        agent_fixture("lifecycle", [ModelScript::Text("final")], Vec::new()).await;
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
    let late_finish = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            match events.recv().await {
                Some(AgentEvent::TurnFinished { .. }) => break true,
                Some(_) => {}
                None => break false,
            }
        }
    })
    .await;
    assert!(late_finish.is_err() || !late_finish.unwrap());
    assert!(!agent.list_sessions().await.unwrap()[0].loaded);
    agent.delete_session(info.session_id).await.unwrap();
    assert!(agent.list_sessions().await.unwrap().is_empty());
    agent.shutdown().await.unwrap();
    while events.recv().await.is_some() {}
    remove_base(&base).await;
}

#[tokio::test]
async fn real_read_tool_runs_through_runtime_model_tool_model_loop() {
    let (mut agent, base, workspace, calls) = agent_fixture(
        "tool-loop",
        [
            ModelScript::tool_calls(vec![
                scripted_call("read", json!({"path": "input.txt"})),
                scripted_call("read", json!({"path": "input.txt"})),
            ]),
            ModelScript::Text("tool final"),
            ModelScript::tool_call("read", json!({"path": "input.txt"})),
            ModelScript::Text("reopened tool final"),
        ],
        vec!["read"],
    )
    .await;
    tokio::fs::write(workspace.join("input.txt"), "real workspace content\n")
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
    assert!(page.entries.iter().any(|entry| matches!(
        entry,
        minicore_runtime::ConversationEntry::ToolResult(result)
            if result.content.as_str().contains("real workspace content")
    )));
    agent.close_session(info.session_id).await.unwrap();
    let reopened = agent.open_session(info.session_id).await.unwrap();
    assert_ne!(reopened.instance_id, info.instance_id);
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "read after reopen".to_owned(),
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
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let page = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 64,
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
        3
    );
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn real_write_ask_allow_once_writes_after_safe_approval() {
    const SECRET: &str = "CAPABILITY-SECRET-CONTENT";
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-write-allow-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, calls) = FakeModel::new([
        ModelScript::tool_call("write", json!({"path": "approved.txt", "content": SECRET})),
        ModelScript::Text("write approved final"),
    ]);
    let mut agent = Agent::open_with_models(
        config_with_approval(base.join("data"), vec!["write"], ApprovalMode::Ask),
        models(model),
    )
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
            text: "write with approval".to_owned(),
        })
        .await
        .unwrap();
    let event = wait_for_event(&mut events, |event| {
        matches!(event, AgentEvent::InteractionRequested { interaction, .. } if interaction.turn_id == turn.turn_id)
    })
    .await;
    let serialized = serde_json::to_string(&event).unwrap();
    assert!(!serialized.contains(SECRET));
    assert!(!serialized.contains("arguments"));
    let AgentEvent::InteractionRequested { interaction, .. } = event else {
        unreachable!();
    };
    let minicore_runtime::InteractionKind::Approval(approval) = &interaction.kind else {
        panic!("write did not request approval");
    };
    assert_eq!(
        approval.prompt.as_str(),
        "Allow tool `write` for this call?"
    );
    assert_eq!(approval.risk, minicore_runtime::tools::ApprovalRisk::Medium);
    agent
        .answer(AnswerInteraction {
            session_id: info.session_id,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::AllowOnce,
            ),
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
        tokio::fs::read_to_string(workspace.join("approved.txt"))
            .await
            .unwrap(),
        SECRET
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn real_write_ask_deny_records_denied_and_continues_model_loop() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-write-deny-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, calls) = FakeModel::new([
        ModelScript::tool_call(
            "write",
            json!({"path": "denied.txt", "content": "must-not-exist"}),
        ),
        ModelScript::Text("write denied final"),
    ]);
    let mut agent = Agent::open_with_models(
        config_with_approval(base.join("data"), vec!["write"], ApprovalMode::Ask),
        models(model),
    )
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
            text: "deny write".to_owned(),
        })
        .await
        .unwrap();
    let event = wait_for_event(&mut events, |event| {
        matches!(event, AgentEvent::InteractionRequested { interaction, .. } if interaction.turn_id == turn.turn_id)
    })
    .await;
    let AgentEvent::InteractionRequested { interaction, .. } = event else {
        unreachable!();
    };
    agent
        .answer(AnswerInteraction {
            session_id: info.session_id,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::Deny,
            ),
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
    assert!(
        !tokio::fs::try_exists(workspace.join("denied.txt"))
            .await
            .unwrap()
    );
    let page = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 32,
        })
        .await
        .unwrap();
    assert!(page.entries.iter().any(|entry| matches!(
        entry,
        minicore_runtime::ConversationEntry::ToolResult(result)
            if result.tool_name.as_str() == "write"
                && result.outcome == minicore_runtime::tools::ToolResultOutcome::Denied
    )));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn read_only_allows_read_and_denies_every_mutating_tool_without_side_effects() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-read-only-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::write(workspace.join("target.txt"), "before\n")
        .await
        .unwrap();
    let scripts = vec![
        ModelScript::tool_call("read", json!({"path": "target.txt"})),
        ModelScript::Text("read final"),
        ModelScript::tool_call(
            "write",
            json!({"path": "write-denied.txt", "content": "bad"}),
        ),
        ModelScript::Text("write denied final"),
        ModelScript::tool_call(
            "edit",
            json!({"path": "target.txt", "old_text": "before", "new_text": "edited"}),
        ),
        ModelScript::Text("edit denied final"),
        ModelScript::tool_call(
            "apply_patch",
            json!({
                "path": "target.txt",
                "patch": "@@ -1 +1 @@\n-before\n+patched\n"
            }),
        ),
        ModelScript::Text("patch denied final"),
        ModelScript::tool_call("bash", json!({"command": "printf bad > bash-denied.txt"})),
        ModelScript::Text("bash denied final"),
    ];
    let (model, _) = FakeModel::new(scripts);
    let mut agent = Agent::open_with_models(
        config_with_approval(
            base.join("data"),
            vec!["read", "write", "edit", "apply_patch", "bash"],
            ApprovalMode::ReadOnly,
        ),
        models(model),
    )
    .await
    .unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    for tool in ["read", "write", "edit", "apply_patch", "bash"] {
        let turn = agent
            .send(SendMessage {
                session_id: info.session_id,
                text: format!("run {tool}"),
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
    }
    let page = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 100,
        })
        .await
        .unwrap();
    let results = page
        .entries
        .iter()
        .filter_map(|entry| match entry {
            minicore_runtime::ConversationEntry::ToolResult(result) => {
                Some((result.tool_name.as_str(), result.outcome))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results,
        vec![
            ("read", minicore_runtime::tools::ToolResultOutcome::Success),
            ("write", minicore_runtime::tools::ToolResultOutcome::Denied),
            ("edit", minicore_runtime::tools::ToolResultOutcome::Denied),
            (
                "apply_patch",
                minicore_runtime::tools::ToolResultOutcome::Denied,
            ),
            ("bash", minicore_runtime::tools::ToolResultOutcome::Denied),
        ]
    );
    assert_eq!(
        tokio::fs::read_to_string(workspace.join("target.txt"))
            .await
            .unwrap(),
        "before\n"
    );
    assert!(
        !tokio::fs::try_exists(workspace.join("write-denied.txt"))
            .await
            .unwrap()
    );
    assert!(
        !tokio::fs::try_exists(workspace.join("bash-denied.txt"))
            .await
            .unwrap()
    );
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn auto_executes_real_write_edit_and_patch_sequentially_without_approval() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-auto-mutations-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, calls) = FakeModel::new([
        ModelScript::tool_calls(vec![
            scripted_call("write", json!({"path": "chain.txt", "content": "one\n"})),
            scripted_call(
                "edit",
                json!({"path": "chain.txt", "old_text": "one", "new_text": "two"}),
            ),
            scripted_call(
                "apply_patch",
                json!({
                    "path": "chain.txt",
                    "patch": "@@ -1 +1 @@\n-two\n+three\n"
                }),
            ),
        ]),
        ModelScript::Text("mutation chain final"),
    ]);
    let mut agent = Agent::open_with_models(
        config_with_approval(
            base.join("data"),
            vec!["write", "edit", "apply_patch"],
            ApprovalMode::Auto,
        ),
        models(model),
    )
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
            text: "mutate in order".to_owned(),
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
        tokio::fs::read_to_string(workspace.join("chain.txt"))
            .await
            .unwrap(),
        "three\n"
    );
    let page = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 64,
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
        3
    );
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(10), events.recv()).await
    {
        assert!(!matches!(event, AgentEvent::InteractionRequested { .. }));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn project_context_is_injected_per_workspace_and_missing_file_adds_no_block() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-context-assembly-{}",
        SessionId::new().unwrap()
    ));
    let workspace_with = base.join("with-context");
    let workspace_without = base.join("without-context");
    tokio::fs::create_dir_all(&workspace_with).await.unwrap();
    tokio::fs::create_dir_all(&workspace_without).await.unwrap();
    tokio::fs::write(
        workspace_with.join("AGENTS.md"),
        "Use real project instructions.",
    )
    .await
    .unwrap();
    let (model, _) = FakeModel::new([
        ModelScript::Text("context final"),
        ModelScript::Text("no context final"),
    ]);
    let requests = model.requests();
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let with = agent
        .create_session(create_request(&workspace_with.join(".")))
        .await
        .unwrap();
    assert_eq!(
        with.workspace,
        tokio::fs::canonicalize(&workspace_with).await.unwrap()
    );
    let turn = agent
        .send(SendMessage {
            session_id: with.session_id,
            text: "inspect context".to_owned(),
        })
        .await
        .unwrap();
    agent.turn_handle(turn).unwrap().wait().await.unwrap();

    let without = agent
        .create_session(create_request(&workspace_without))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: without.session_id,
            text: "inspect missing context".to_owned(),
        })
        .await
        .unwrap();
    agent.turn_handle(turn).unwrap().wait().await.unwrap();

    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let first_context = requests[0]
            .messages()
            .iter()
            .filter_map(|message| match message {
                minicore_runtime::model::ModelMessage::System(text)
                    if text.starts_with("[minicore-context ") =>
                {
                    Some(text.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(first_context.len(), 1);
        assert!(first_context[0].contains("slot=project_instructions"));
        assert!(first_context[0].contains("source=agents-md"));
        assert!(first_context[0].contains("Use real project instructions."));
        assert!(!requests[1].messages().iter().any(|message| matches!(
            message,
            minicore_runtime::model::ModelMessage::System(text)
                if text.starts_with("[minicore-context ")
        )));
    }
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn real_tools_are_isolated_by_each_loaded_sessions_workspace() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-session-workspaces-{}",
        SessionId::new().unwrap()
    ));
    let workspace_a = base.join("workspace-a");
    let workspace_b = base.join("workspace-b");
    tokio::fs::create_dir_all(&workspace_a).await.unwrap();
    tokio::fs::create_dir_all(&workspace_b).await.unwrap();
    tokio::fs::write(workspace_a.join("same.txt"), "session-a\n")
        .await
        .unwrap();
    tokio::fs::write(workspace_b.join("same.txt"), "session-b\n")
        .await
        .unwrap();
    let (model, _) = FakeModel::new([
        ModelScript::tool_call("read", json!({"path": "same.txt"})),
        ModelScript::Text("a final"),
        ModelScript::tool_call("read", json!({"path": "same.txt"})),
        ModelScript::Text("b final"),
    ]);
    let mut agent = Agent::open_with_models(config(base.join("data"), vec!["read"]), models(model))
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
    for session_id in [a.session_id, b.session_id] {
        let turn = agent
            .send(SendMessage {
                session_id,
                text: "read isolated file".to_owned(),
            })
            .await
            .unwrap();
        agent.turn_handle(turn).unwrap().wait().await.unwrap();
    }
    for (session_id, own, other) in [
        (a.session_id, "session-a", "session-b"),
        (b.session_id, "session-b", "session-a"),
    ] {
        let page = agent
            .transcript(GetTranscript {
                session_id,
                after: None,
                limit: 32,
            })
            .await
            .unwrap();
        let output = page
            .entries
            .iter()
            .find_map(|entry| match entry {
                minicore_runtime::ConversationEntry::ToolResult(result) => {
                    Some(result.content.as_str())
                }
                _ => None,
            })
            .unwrap();
        assert!(output.contains(own));
        assert!(!output.contains(other));
    }
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_traversal_and_escape_symlink_fail_through_real_read_tool() {
    use std::os::unix::fs::symlink;

    const OUTSIDE_SECRET: &str = "OUTSIDE-SECRET-MUST-NOT-LEAK";
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-workspace-escape-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::write(base.join("outside.txt"), OUTSIDE_SECRET)
        .await
        .unwrap();
    symlink(base.join("outside.txt"), workspace.join("escape-link")).unwrap();
    let (model, _) = FakeModel::new([
        ModelScript::tool_call("read", json!({"path": "../outside.txt"})),
        ModelScript::Text("traversal failed final"),
        ModelScript::tool_call("read", json!({"path": "escape-link"})),
        ModelScript::Text("symlink failed final"),
    ]);
    let mut agent = Agent::open_with_models(config(base.join("data"), vec!["read"]), models(model))
        .await
        .unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    for text in ["try traversal", "try symlink"] {
        let turn = agent
            .send(SendMessage {
                session_id: info.session_id,
                text: text.to_owned(),
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
    }
    let page = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 64,
        })
        .await
        .unwrap();
    let results = page
        .entries
        .iter()
        .filter_map(|entry| match entry {
            minicore_runtime::ConversationEntry::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| {
        result.outcome == minicore_runtime::tools::ToolResultOutcome::Failed
            && !result.content.as_str().contains(OUTSIDE_SECRET)
    }));
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
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
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
    let first_handle = agent.turn_handle(first).unwrap();
    assert_eq!(
        first_handle.wait().await.unwrap().terminal,
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
    assert!(matches!(
        agent.turn_handle(first),
        Err(crate::error::AgentError::TurnNotFound)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn exact_cancel_interrupts_a_long_running_tool() {
    let (mut agent, base, workspace, _) = agent_fixture(
        "tool-cancel",
        [ModelScript::tool_call(
            "bash",
            json!({
                "command": "echo $$ > bash.pid; exec sleep 30",
                "timeout_seconds": 30
            }),
        )],
        vec!["bash"],
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
    let pid = read_test_pid(&workspace.join("bash.pid")).await;
    let mut guard = TestProcessGuard::new(pid);
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
    wait_for_test_process_exit(pid).await;
    guard.disarm();
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        SessionStatus::Idle
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
    assert_ne!(
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

#[cfg(unix)]
#[tokio::test]
async fn unloaded_open_revalidates_deleted_and_symlinked_workspace_roots() {
    use std::os::unix::fs::symlink;

    let base = std::env::temp_dir().join(format!(
        "minicore-agent-workspace-reopen-{}",
        SessionId::new().unwrap()
    ));
    let deleted_workspace = base.join("deleted-workspace");
    let symlink_workspace = base.join("symlink-workspace");
    tokio::fs::create_dir_all(&deleted_workspace).await.unwrap();
    tokio::fs::create_dir_all(&symlink_workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();

    let deleted = agent
        .create_session(create_request(&deleted_workspace.join(".")))
        .await
        .unwrap();
    assert_eq!(
        deleted.workspace,
        tokio::fs::canonicalize(&deleted_workspace).await.unwrap()
    );
    tokio::fs::remove_dir_all(&deleted_workspace).await.unwrap();
    assert_eq!(
        agent.open_session(deleted.session_id).await.unwrap(),
        deleted
    );
    agent.close_session(deleted.session_id).await.unwrap();
    assert!(matches!(
        agent.open_session(deleted.session_id).await,
        Err(AgentError::Workspace)
    ));

    let symlinked = agent
        .create_session(create_request(&symlink_workspace))
        .await
        .unwrap();
    agent.close_session(symlinked.session_id).await.unwrap();
    let moved = base.join("moved-workspace");
    tokio::fs::rename(&symlink_workspace, &moved).await.unwrap();
    symlink(&moved, &symlink_workspace).unwrap();
    assert!(matches!(
        agent.open_session(symlinked.session_id).await,
        Err(AgentError::Workspace)
    ));
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn old_instance_turn_references_are_rejected_after_reopen() {
    let (mut agent, base, workspace, _) =
        agent_fixture("old-instance", [ModelScript::Text("unused")], Vec::new()).await;
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
    let mut old_agent =
        Agent::open_with_models(config(base.join("data"), Vec::new()), models(old_model))
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
    let mut new_agent = Agent::open_with_models(new_config, models(new_model))
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

fn drift_tools(profile: &mut Profile) {
    profile.tools = vec!["read".to_owned()];
}

#[tokio::test]
async fn opening_rejects_each_supported_profile_spec_drift() {
    assert_profile_drift("system-prompt", drift_system_prompt).await;
    assert_profile_drift("reasoning", drift_reasoning).await;
    assert_profile_drift("max-rounds", drift_max_rounds).await;
    assert_profile_drift("tools", drift_tools).await;
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

#[test]
fn core_turn_finished_is_suppressed_and_its_drop_count_reaches_the_next_event() {
    let session_id: SessionId = "ses_00000000000000000000000000000001".parse().unwrap();
    let instance_id: SessionInstanceId = "ins_00000000000000000000000000000001".parse().unwrap();
    let turn_id: TurnId = "trn_00000000000000000000000000000001".parse().unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let sink = crate::event::AgentEventSink::new(sender);
    assert!(crate::event::forward_core_event(
        minicore_runtime::session::SessionEventEnvelope {
            session_id,
            instance_id,
            dropped_before: 5,
            event: minicore_runtime::session::SessionEvent::TurnFinished {
                turn_id,
                outcome: minicore_runtime::TurnOutcome {
                    turn_id,
                    terminal: TurnTerminal::Completed,
                    usage: Usage::default(),
                },
            },
        },
        &sink,
    ));
    assert!(receiver.try_recv().is_err());
    assert_eq!(
        sink.try_send(AgentEvent::SessionClosed {
            session_id,
            meta: crate::event::EventMeta {
                session_id,
                instance_id,
                dropped_before: 0,
            },
        }),
        crate::event::AgentSendResult::Sent
    );
    assert!(matches!(
        receiver.try_recv().unwrap(),
        AgentEvent::SessionClosed {
            meta: crate::event::EventMeta {
                dropped_before: 5,
                ..
            },
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_turn_close_emits_session_closed_after_closed_transcript_barrier() {
    let (mut agent, base, workspace, _) =
        agent_fixture("active-close", [ModelScript::Text("final")], Vec::new()).await;
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    wait_for_event(&mut events, |event| {
        matches!(event, AgentEvent::SessionState { state, .. } if state.session_id == info.session_id && state.status == SessionStatus::Idle)
    })
    .await;

    let barrier_gate = Arc::new(crate::sessions::TranscriptBarrierGate::new(info.session_id));
    crate::sessions::block_transcript_barrier(Arc::clone(&barrier_gate));
    let shutdown_gate = Arc::new(crate::sessions::SessionShutdownGate::new(info.session_id));
    crate::sessions::block_session_shutdown(Arc::clone(&shutdown_gate));
    let sequencer_stop = agent
        .sessions
        .get(info.session_id)
        .unwrap()
        .sequencer
        .stop_signal();

    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "finish while closing".to_owned(),
        })
        .await
        .unwrap();
    agent.turn_handle(turn).unwrap().wait().await.unwrap();
    barrier_gate.started.acquire().await.unwrap().forget();

    let close = tokio::spawn(async move {
        let result = agent.close_session(info.session_id).await;
        (agent, result)
    });
    shutdown_gate.started.acquire().await.unwrap().forget();
    barrier_gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), sequencer_stop.cancelled())
        .await
        .unwrap();
    shutdown_gate.release.add_permits(1);
    let (agent, result) = close.await.unwrap();
    result.unwrap();

    let closed = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            match events.recv().await {
                Some(AgentEvent::SessionClosed { session_id, .. })
                    if session_id == info.session_id =>
                {
                    break true;
                }
                Some(_) => {}
                None => break false,
            }
        }
    })
    .await;
    assert_eq!(closed, Ok(true));
    let late_finish = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(event) = events.recv().await {
            assert!(!matches!(event, AgentEvent::SessionClosed { .. }));
            if matches!(event, AgentEvent::TurnFinished { turn: value, .. } if value == turn) {
                return true;
            }
        }
        false
    })
    .await;
    assert!(late_finish.is_err() || !late_finish.unwrap());
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_for_input_state_precedes_interaction_requested() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-state-order-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::tool_call(
        "write",
        json!({"path": "approval.txt", "content": "approved"}),
    )]);
    let mut agent = Agent::open_with_models(
        config_with_approval(base.join("data"), vec!["write"], ApprovalMode::Ask),
        models(model),
    )
    .await
    .unwrap();
    let gate = Arc::new(crate::sessions::SequencerGate::new());
    crate::sessions::block_sequencer(workspace.clone(), Arc::clone(&gate));
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    gate.started.acquire().await.unwrap().forget();
    assert!(matches!(
        next_event(&mut events).await,
        AgentEvent::SessionOpened { .. }
    ));
    assert!(matches!(
        next_event(&mut events).await,
        AgentEvent::SessionState { state, .. } if state.status == SessionStatus::Idle
    ));

    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "request approval".to_owned(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent.session_state(info.session_id).unwrap().status
                == SessionStatus::WaitingForInput
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.release.add_permits(1);

    let mut waiting_state_seen = false;
    let interaction = loop {
        match next_event(&mut events).await {
            AgentEvent::SessionState { state, .. }
                if state.status == SessionStatus::WaitingForInput =>
            {
                assert!(state.pending_interaction.is_some());
                waiting_state_seen = true;
            }
            AgentEvent::InteractionRequested { interaction, .. } => {
                assert!(waiting_state_seen);
                break interaction;
            }
            _ => {}
        }
    };
    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, SessionStatus::WaitingForInput);
    assert_eq!(
        state
            .pending_interaction
            .as_ref()
            .map(|pending| pending.interaction_id),
        Some(interaction.interaction_id)
    );
    agent
        .answer(AnswerInteraction {
            session_id: info.session_id,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::Deny,
            ),
        })
        .await
        .unwrap();
    agent.turn_handle(turn).unwrap().wait().await.unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_boundary_preserves_cross_turn_interaction_state_order() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-cross-turn-state-order-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([
        ModelScript::Text("first"),
        ModelScript::tool_call(
            "write",
            json!({"path": "approval.txt", "content": "approved"}),
        ),
    ]);
    let mut agent = Agent::open_with_models(
        config_with_approval(base.join("data"), vec!["write"], ApprovalMode::Ask),
        models(model),
    )
    .await
    .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    wait_for_event(&mut events, |event| {
        matches!(event, AgentEvent::SessionState { state, .. } if state.session_id == info.session_id && state.status == SessionStatus::Idle)
    })
    .await;

    let barrier_gate = Arc::new(crate::sessions::TranscriptBarrierGate::new(info.session_id));
    crate::sessions::block_transcript_barrier(Arc::clone(&barrier_gate));
    let first = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "first turn".to_owned(),
        })
        .await
        .unwrap();
    agent.turn_handle(first).unwrap().wait().await.unwrap();
    barrier_gate.started.acquire().await.unwrap().forget();

    let second = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "second turn needs approval".to_owned(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent.session_state(info.session_id).unwrap().status
                == SessionStatus::WaitingForInput
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    barrier_gate.release.add_permits(1);

    let mut first_started = false;
    let mut first_output = false;
    let mut first_finished = false;
    let mut waiting_state_count = 0;
    let mut waiting_interaction = None;
    let interaction = loop {
        match next_event(&mut events).await {
            AgentEvent::TurnStarted { turn, .. } if turn == first => first_started = true,
            AgentEvent::OutputDelta { turn, .. } if turn == first => first_output = true,
            AgentEvent::SessionState { state, .. }
                if state.status == SessionStatus::WaitingForInput
                    && state.active_turn == Some(second.turn_id) =>
            {
                waiting_state_count += 1;
                waiting_interaction = state
                    .pending_interaction
                    .as_ref()
                    .map(|pending| pending.interaction_id);
            }
            AgentEvent::InteractionRequested { interaction, .. }
                if interaction.turn_id == second.turn_id =>
            {
                assert_eq!(waiting_interaction, Some(interaction.interaction_id));
                assert_eq!(waiting_state_count, 1);
                break interaction;
            }
            AgentEvent::TurnFinished { turn, .. } if turn == first => {
                assert!(first_started);
                assert!(first_output);
                first_finished = true;
            }
            _ => {}
        }
    };
    while !first_finished {
        match next_event(&mut events).await {
            AgentEvent::SessionState { state, .. }
                if state.status == SessionStatus::WaitingForInput
                    && state.active_turn == Some(second.turn_id) =>
            {
                waiting_state_count += 1;
            }
            AgentEvent::TurnFinished { turn, .. } if turn == first => {
                assert!(first_started);
                assert!(first_output);
                first_finished = true;
            }
            _ => {}
        }
    }
    assert_eq!(waiting_state_count, 1);

    agent
        .answer(AnswerInteraction {
            session_id: info.session_id,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::Deny,
            ),
        })
        .await
        .unwrap();
    agent.turn_handle(second).unwrap().wait().await.unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_event_refreshes_state_published_after_initial_completion_emit() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-drain-state-race-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([
        ModelScript::Text("first"),
        ModelScript::tool_call(
            "write",
            json!({"path": "approval.txt", "content": "approved"}),
        ),
    ]);
    let mut agent = Agent::open_with_models(
        config_with_approval(base.join("data"), vec!["write"], ApprovalMode::Ask),
        models(model),
    )
    .await
    .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let policy_gate = Arc::new(crate::policy::PolicyDecisionGate::new(
        info.session_id,
        ToolCallId::new("write-call-1-0").unwrap(),
    ));
    let policy_started = Arc::clone(&policy_gate.started);
    let policy_release = Arc::clone(&policy_gate.release);
    crate::policy::block_decision(policy_gate);
    wait_for_event(&mut events, |event| {
        matches!(event, AgentEvent::SessionState { state, .. } if state.session_id == info.session_id && state.status == SessionStatus::Idle)
    })
    .await;

    let barrier_gate = Arc::new(crate::sessions::TranscriptBarrierGate::new(info.session_id));
    crate::sessions::block_transcript_barrier(Arc::clone(&barrier_gate));
    let first = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "first turn".to_owned(),
        })
        .await
        .unwrap();
    agent.turn_handle(first).unwrap().wait().await.unwrap();
    barrier_gate.started.acquire().await.unwrap().forget();

    let second = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "second turn races drain".to_owned(),
        })
        .await
        .unwrap();
    policy_started.acquire().await.unwrap().forget();
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        SessionStatus::Running
    );
    let drain_gate = Arc::new(crate::sessions::CompletionDrainGate::new(info.session_id));
    crate::sessions::block_completion_drain(Arc::clone(&drain_gate));
    barrier_gate.release.add_permits(1);
    drain_gate.started.acquire().await.unwrap().forget();

    policy_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent.session_state(info.session_id).unwrap().status
                == SessionStatus::WaitingForInput
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drain_gate.release.add_permits(1);

    let mut running_seen = false;
    let mut waiting_interaction = None;
    let interaction = loop {
        match next_event(&mut events).await {
            AgentEvent::SessionState { state, .. }
                if state.status == SessionStatus::Running
                    && state.active_turn == Some(second.turn_id) =>
            {
                running_seen = true;
            }
            AgentEvent::SessionState { state, .. }
                if state.status == SessionStatus::WaitingForInput
                    && state.active_turn == Some(second.turn_id) =>
            {
                waiting_interaction = state
                    .pending_interaction
                    .as_ref()
                    .map(|pending| pending.interaction_id);
            }
            AgentEvent::InteractionRequested { interaction, .. }
                if interaction.turn_id == second.turn_id =>
            {
                assert!(running_seen);
                assert_eq!(waiting_interaction, Some(interaction.interaction_id));
                break interaction;
            }
            _ => {}
        }
    };

    agent
        .answer(AnswerInteraction {
            session_id: info.session_id,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::Deny,
            ),
        })
        .await
        .unwrap();
    agent.turn_handle(second).unwrap().wait().await.unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequencer_barrier_orders_successful_live_events_before_finish() {
    let (mut agent, base, workspace, _) = agent_fixture(
        "sequencer-order",
        [
            ModelScript::tool_call("read", json!({"path": "input.txt"})),
            ModelScript::Text("after tool"),
        ],
        vec!["read"],
    )
    .await;
    tokio::fs::write(workspace.join("input.txt"), "sequenced read\n")
        .await
        .unwrap();
    let gate = Arc::new(crate::sessions::SequencerGate::new());
    crate::sessions::block_sequencer(workspace.clone(), Arc::clone(&gate));
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    gate.started.acquire().await.unwrap().forget();
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
    gate.release.add_permits(1);

    let mut started = false;
    let mut output = false;
    let mut tool_started = false;
    let mut tool_finished = false;
    let mut terminal_state = false;
    loop {
        match next_event(&mut events).await {
            AgentEvent::TurnStarted { turn: value, .. } if value == turn => started = true,
            AgentEvent::OutputDelta { turn: value, .. } if value == turn => output = true,
            AgentEvent::ToolStarted { turn: value, .. } if value == turn => tool_started = true,
            AgentEvent::ToolFinished { turn: value, .. } if value == turn => tool_finished = true,
            AgentEvent::SessionState { state, .. }
                if state
                    .last_terminal
                    .as_ref()
                    .is_some_and(|outcome| outcome.turn_id == turn.turn_id) =>
            {
                terminal_state = true
            }
            AgentEvent::TurnFinished { turn: value, .. } if value == turn => break,
            _ => {}
        }
    }
    assert!(started);
    assert!(output);
    assert!(tool_started);
    assert!(tool_finished);
    assert!(terminal_state);
    let duplicate = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            match events.recv().await {
                Some(AgentEvent::TurnFinished { turn: value, .. }) if value == turn => break true,
                Some(_) => {}
                None => break false,
            }
        }
    })
    .await;
    assert!(duplicate.is_err() || !duplicate.unwrap());
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn sequencer_does_not_wait_for_a_dropped_core_terminal_event() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-terminal-drop-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("final")]);
    let mut agent_config = config(base.join("data"), Vec::new());
    agent_config.kernel.event_capacity = Some(1);
    let mut agent = Agent::open_with_models(agent_config, models(model))
        .await
        .unwrap();
    let gate = Arc::new(crate::sessions::SequencerGate::new());
    crate::sessions::block_sequencer(workspace.clone(), Arc::clone(&gate));
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    gate.started.acquire().await.unwrap().forget();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "terminal may be dropped".to_owned(),
        })
        .await
        .unwrap();
    agent.turn_handle(turn).unwrap().wait().await.unwrap();
    gate.release.add_permits(1);
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
async fn completion_notifier_serializes_ready_turns_without_blocking_send() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-durable-event-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("first"), ModelScript::Text("second")]);
    let mut config = config(base.join("data"), Vec::new());
    config.event_capacity = 1;
    let mut agent = Agent::open_with_models(config, models(model))
        .await
        .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let first = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "first request".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent
            .turn_handle(first)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::Completed
    );
    let second = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "second request".to_owned(),
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

    assert!(matches!(
        events.recv().await,
        Some(AgentEvent::SessionOpened { .. })
    ));
    let mut finished = Vec::new();
    while finished.len() < 2 {
        if let AgentEvent::TurnFinished { turn, .. } = next_event(&mut events).await {
            finished.push(turn);
        }
    }
    assert_eq!(finished, vec![first, second]);
    let duplicate = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            match events.recv().await {
                Some(AgentEvent::TurnFinished { .. }) => break true,
                Some(_) => {}
                None => break false,
            }
        }
    })
    .await;
    assert!(duplicate.is_err() || !duplicate.unwrap());
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn dropped_event_receiver_stops_completion_worker() {
    let (mut agent, base, workspace, _) =
        agent_fixture("dropped-events", [ModelScript::Block], Vec::new()).await;
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let _turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "hello".to_owned(),
        })
        .await
        .unwrap();
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn send_returns_before_background_touch_finishes() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-touch-background-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let started = Arc::new(Semaphore::new(0));
    let (model, _) = FakeModel::new([ModelScript::Block]);
    let model = model.with_started(Arc::clone(&started));
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let gate = Arc::new(crate::store::AtomicWriteGate::new(session_record_path(
        &base,
        info.session_id,
    )));
    crate::store::block_next_atomic_write(Arc::clone(&gate));
    let turn = tokio::time::timeout(
        Duration::from_secs(2),
        agent.send(SendMessage {
            session_id: info.session_id,
            text: "touch me asynchronously".to_owned(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    started.acquire().await.unwrap().forget();
    gate.started.acquire().await.unwrap().forget();
    assert_ne!(
        agent
            .open_session(info.session_id)
            .await
            .unwrap()
            .updated_at,
        info.updated_at
    );
    gate.release.add_permits(1);
    gate.finished.acquire().await.unwrap().forget();
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
    drop(events);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn close_joins_blocked_metadata_worker_before_delete() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-touch-close-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let started = Arc::new(Semaphore::new(0));
    let (model, _) = FakeModel::new([ModelScript::Block]);
    let model = model.with_started(Arc::clone(&started));
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let gate = Arc::new(crate::store::AtomicWriteGate::new(session_record_path(
        &base,
        info.session_id,
    )));
    crate::store::block_next_atomic_write(Arc::clone(&gate));
    let _turn = tokio::time::timeout(
        Duration::from_secs(2),
        agent.send(SendMessage {
            session_id: info.session_id,
            text: "close while touching".to_owned(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    started.acquire().await.unwrap().forget();
    gate.started.acquire().await.unwrap().forget();
    let metadata_stop = agent
        .sessions
        .get(info.session_id)
        .unwrap()
        .metadata
        .stop_signal();
    drop(events);
    let close = tokio::spawn(async move {
        let result = agent.close_session(info.session_id).await;
        (agent, result)
    });
    tokio::time::timeout(Duration::from_secs(5), metadata_stop.cancelled())
        .await
        .unwrap();
    assert!(!close.is_finished());
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), gate.finished.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let (mut agent, result) = close.await.unwrap();
    result.unwrap();
    agent.delete_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn metadata_worker_serializes_latest_update_before_close_and_delete() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-touch-order-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("first"), ModelScript::Text("second")]);
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let record_path = session_record_path(&base, info.session_id);
    let first_gate = Arc::new(crate::store::AtomicWriteGate::new(record_path.clone()));
    let second_gate = Arc::new(crate::store::AtomicWriteGate::new(record_path));
    second_gate.release.add_permits(1);
    crate::store::block_next_atomic_write(Arc::clone(&first_gate));
    crate::store::block_next_atomic_write(Arc::clone(&second_gate));

    let first = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "first request".to_owned(),
        })
        .await
        .unwrap();
    first_gate.started.acquire().await.unwrap().forget();
    agent.turn_handle(first).unwrap().wait().await.unwrap();

    let second = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "second request".to_owned(),
        })
        .await
        .unwrap();
    let expected = agent
        .sessions
        .get(info.session_id)
        .expect("session is loaded")
        .record
        .updated_at
        .clone();
    assert_ne!(expected, info.updated_at);

    first_gate.release.add_permits(1);
    first_gate.finished.acquire().await.unwrap().forget();
    second_gate.started.acquire().await.unwrap().forget();
    second_gate.finished.acquire().await.unwrap().forget();
    agent.turn_handle(second).unwrap().wait().await.unwrap();

    assert_eq!(
        agent
            .store
            .load_record(info.session_id)
            .await
            .unwrap()
            .updated_at,
        expected
    );
    agent.close_session(info.session_id).await.unwrap();
    agent.delete_session(info.session_id).await.unwrap();
    drop(events);
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn metadata_unavailable_retry_waits_for_a_new_latest_value() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-touch-retry-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let record_path = session_record_path(&base, info.session_id);
    let first_gate = Arc::new(crate::store::AtomicWriteGate::new(record_path.clone()));
    let second_gate = Arc::new(crate::store::AtomicWriteGate::new(record_path.clone()));
    crate::store::block_next_atomic_write(Arc::clone(&first_gate));
    crate::store::block_next_atomic_write(Arc::clone(&second_gate));
    crate::store::fail_next_atomic_write_before_rename(&record_path);
    let first = "2020-01-02T03:04:05.006Z".to_owned();
    let second = "2020-01-02T03:04:05.007Z".to_owned();
    assert!(
        agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .metadata
            .update(first)
    );
    first_gate.started.acquire().await.unwrap().forget();
    first_gate.release.add_permits(1);
    first_gate.finished.acquire().await.unwrap().forget();
    assert!(
        !agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .metadata
            .is_failed()
    );
    assert!(
        agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .metadata
            .update(second)
    );
    second_gate.started.acquire().await.unwrap().forget();
    second_gate.release.add_permits(1);
    second_gate.finished.acquire().await.unwrap().forget();
    assert_eq!(
        agent
            .store
            .load_record(info.session_id)
            .await
            .unwrap()
            .updated_at,
        "2020-01-02T03:04:05.007Z"
    );
    drop(events);
    agent.close_session(info.session_id).await.unwrap();
    agent.delete_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn metadata_unknown_outcome_stops_future_updates_after_uncertain_write() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-touch-unknown-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let gate = Arc::new(crate::store::AtomicWriteGate::new(session_record_path(
        &base,
        info.session_id,
    )));
    crate::store::block_next_atomic_write(Arc::clone(&gate));
    crate::store::fail_next_directory_sync(&session_directory(&base, info.session_id));
    assert!(
        agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .metadata
            .update("2020-01-02T03:04:05.006Z".to_owned())
    );
    gate.started.acquire().await.unwrap().forget();
    gate.release.add_permits(1);
    gate.finished.acquire().await.unwrap().forget();
    for _ in 0..100 {
        if agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .metadata
            .is_failed()
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let metadata = &agent.sessions.get(info.session_id).unwrap().metadata;
    assert!(metadata.is_failed());
    assert!(!metadata.update("2020-01-02T03:04:05.007Z".to_owned()));
    assert_eq!(
        agent
            .store
            .load_record(info.session_id)
            .await
            .unwrap()
            .updated_at,
        "2020-01-02T03:04:05.006Z"
    );
    drop(events);
    agent.close_session(info.session_id).await.unwrap();
    agent.delete_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn shutdown_with_full_events_cancels_completion_worker() {
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
    let mut agent = Agent::open_with_models(config, models(model))
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
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    while let Some(event) = events.recv().await {
        assert!(!matches!(event, AgentEvent::TurnFinished { .. }));
    }
    remove_base(&base).await;
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
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
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
            let mut agent = Agent::open_with_models(old_config, models(old_model))
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
    let mut agent =
        Agent::open_with_models(config(base.join("data"), Vec::new()), models(new_model))
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
async fn startup_rejects_unknown_and_duplicate_profile_tools() {
    for (label, tools) in [
        ("unknown", vec!["read", "unknown"]),
        ("duplicate", vec!["read", "read"]),
    ] {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-invalid-tools-{label}-{}",
            SessionId::new().unwrap()
        ));
        let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
        assert!(matches!(
            Agent::open_with_models(config(base.join("data"), tools), models(model)).await,
            Err(AgentError::Config(
                crate::config::ConfigError::InvalidProfile
            ))
        ));
        remove_base(&base).await;
    }
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
