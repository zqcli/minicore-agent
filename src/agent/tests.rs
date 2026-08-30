use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::pending;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream;
use serde_json::json;
use tokio::sync::Semaphore;

use minicore_runtime::conversation::{
    ConversationEntry, ConversationSeq, TurnExecutionRecord, TurnTerminal, UserInputRecord,
    UserMessageEntry,
};
use minicore_runtime::error::{
    DiagnosticCategory, DiagnosticCode, SessionLogErrorKind, SessionShutdownError,
};
use minicore_runtime::ids::{SessionId, SessionInstanceId, ToolCallId, TurnId};
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason, ModelRef,
    ModelRequest, ModelStartFuture, ModelStream, ReasoningPreference, Usage,
};
use minicore_runtime::session::SessionStatus;
use minicore_runtime::storage::SessionLog;
use minicore_runtime::value::BoundedText;

use crate::config::{AgentConfig, ConfigError, KernelOverrides, Profile};
use crate::error::{AgentError, CoreErrorView, StoreError};
use crate::event::AgentEvent;
use crate::models::{ModelConfig, Models};
use crate::profiles::{ApprovalMode, ProfileCompaction};

use super::{Agent, AnswerInteraction, CreateSession, GetTranscript, SendMessage, TurnRef};

const TEST_LOG_CAPACITY: usize = 32 * 1024;

#[derive(Clone)]
struct BoundedLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct TestTracingCapture {
    writer: BoundedLogWriter,
    capture_dispatch: tracing::Dispatch,
    _alternate_dispatch: tracing::Dispatch,
}

struct TestDirectoryGuard {
    path: PathBuf,
}

impl TestDirectoryGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TestDirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl TestTracingCapture {
    fn new() -> Self {
        let writer = BoundedLogWriter::new();
        let capture_subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(writer.clone())
            .with_target(false)
            .with_ansi(false)
            .without_time()
            .compact()
            .finish();
        let alternate_subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::level_filters::LevelFilter::OFF)
            .with_writer(io::sink)
            .with_target(false)
            .with_ansi(false)
            .without_time()
            .compact()
            .finish();
        let capture_dispatch = tracing::Dispatch::new(capture_subscriber);
        let alternate_dispatch = tracing::Dispatch::new(alternate_subscriber);
        Self {
            writer,
            capture_dispatch,
            _alternate_dispatch: alternate_dispatch,
        }
    }

    fn enter(&self) -> tracing::dispatcher::DefaultGuard {
        tracing::dispatcher::set_default(&self.capture_dispatch)
    }

    fn contents(&self) -> String {
        self.writer.contents()
    }
}

impl BoundedLogWriter {
    fn new() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contents(&self) -> String {
        let bytes = self
            .bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl Write for BoundedLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let mut bytes = self
            .bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let remaining = TEST_LOG_CAPACITY.saturating_sub(bytes.len());
        bytes.extend_from_slice(&buffer[..buffer.len().min(remaining)]);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for BoundedLogWriter {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

fn test_log_field_equals(line: &str, name: &str, expected: &str) -> bool {
    let prefix = format!("{name}=");
    line.split_ascii_whitespace().any(|token| {
        let token = token.trim_end_matches([',', ';']);
        let Some(value) = token.strip_prefix(&prefix) else {
            return false;
        };
        value == expected
            || value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                == Some(expected)
    })
}

fn session_log_test_entry(seq: u64, turn_id: TurnId, secret: &str) -> ConversationEntry {
    ConversationEntry::UserMessage(UserMessageEntry {
        seq: ConversationSeq::new(seq),
        turn_id,
        input: UserInputRecord::new(BoundedText::new(secret).unwrap()).unwrap(),
        execution: TurnExecutionRecord::new(
            "model:v1".parse().unwrap(),
            ReasoningPreference::Auto,
            4,
        )
        .unwrap(),
        created_at: "2020-01-02T03:04:05.006Z".parse().unwrap(),
    })
}

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

fn fake_supported_reasoning() -> BTreeSet<ReasoningPreference> {
    BTreeSet::from([
        ReasoningPreference::Auto,
        ReasoningPreference::Disabled,
        ReasoningPreference::Low,
        ReasoningPreference::Medium,
        ReasoningPreference::High,
    ])
}

fn configured_model(
    supported_reasoning: BTreeSet<ReasoningPreference>,
    supports_tools: bool,
    api_key_env: String,
) -> ModelConfig {
    ModelConfig::OpenAiResponses {
        model: "provider-model".to_owned(),
        base_url: "https://example.invalid/v1".to_owned(),
        api_key_env,
        physical_context_window: 10_000,
        output_budget_tokens: 1_000,
        safety_margin_tokens: 1_000,
        supported_reasoning,
        supports_tools,
        request_timeout_seconds: Some(30),
    }
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
        Self::with_descriptor("fake", scripts, fake_supported_reasoning(), true)
    }

    fn with_descriptor(
        model_ref: &str,
        scripts: impl IntoIterator<Item = ModelScript>,
        supported_reasoning: BTreeSet<ReasoningPreference>,
        supports_tools: bool,
    ) -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let model_ref: ModelRef = model_ref.parse().unwrap();
        let descriptor =
            ModelDescriptor::new(model_ref, 16_384, supported_reasoning, supports_tools).unwrap();
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
        models: BTreeMap::from([(
            "fake".to_owned(),
            configured_model(
                fake_supported_reasoning(),
                true,
                "MINICORE_UNUSED_FAKE_MODEL_KEY".to_owned(),
            ),
        )]),
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

fn config_with_model_capabilities(
    data_dir: PathBuf,
    reasoning: ReasoningPreference,
    supported_reasoning: BTreeSet<ReasoningPreference>,
    supports_tools: bool,
    api_key_env: String,
) -> AgentConfig {
    let mut config = config(data_dir, Vec::new());
    config.profiles.get_mut("test").unwrap().reasoning = reasoning;
    config.models.insert(
        "fake".to_owned(),
        configured_model(supported_reasoning, supports_tools, api_key_env),
    );
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

fn event_instance_id(event: &AgentEvent) -> SessionInstanceId {
    match event {
        AgentEvent::SessionOpened { meta, .. }
        | AgentEvent::SessionClosed { meta, .. }
        | AgentEvent::SessionState { meta, .. }
        | AgentEvent::TurnStarted { meta, .. }
        | AgentEvent::OutputDelta { meta, .. }
        | AgentEvent::ToolStarted { meta, .. }
        | AgentEvent::ToolProgress { meta, .. }
        | AgentEvent::ToolFinished { meta, .. }
        | AgentEvent::InteractionRequested { meta, .. }
        | AgentEvent::InteractionResolved { meta, .. }
        | AgentEvent::TurnFinished { meta, .. } => meta.instance_id,
    }
}

async fn reserve_completion_capacity(
    agent: &Agent,
    session_id: SessionId,
) -> crate::sessions::CompletionCapacityGuard {
    agent
        .sessions
        .get(session_id)
        .unwrap()
        .reserve_completion_capacity()
        .await
}

fn active_turn_id(agent: &Agent, session_id: SessionId) -> Option<TurnId> {
    agent
        .sessions
        .get(session_id)
        .and_then(crate::sessions::LoadedSession::active_turn_id)
}

fn session_pump_stop_token(
    agent: &Agent,
    session_id: SessionId,
) -> tokio_util::sync::CancellationToken {
    agent
        .sessions
        .get(session_id)
        .unwrap()
        .session_pump_stop_token()
}

fn active_completion_task_is_finished(agent: &Agent, session_id: SessionId) -> Option<bool> {
    agent
        .sessions
        .get(session_id)
        .and_then(crate::sessions::LoadedSession::active_completion_task_is_finished)
}

async fn remove_base(base: &Path) {
    let _ = tokio::fs::remove_dir_all(base).await;
}

async fn cleanup_full_event_channel_fixture(
    agent: Agent,
    gate: &crate::sessions::SessionPumpStartupGate,
    probe_registration: Option<crate::event::TurnFinishedAttemptProbeRegistration>,
    base: &Path,
) {
    gate.release.add_permits(1);
    let _ = tokio::time::timeout(Duration::from_secs(5), agent.shutdown()).await;
    drop(probe_registration);
    remove_base(base).await;
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
        model: None,
        reasoning: None,
        title: Some("loop test".to_owned()),
    }
}

async fn create_closed_session_for_log_test(
    agent: &mut Agent,
    workspace: &Path,
    profile: &str,
    title: &str,
) -> SessionId {
    let mut request = create_request(workspace);
    request.profile = profile.to_owned();
    request.title = Some(title.to_owned());
    let session_id = agent.create_session(request).await.unwrap().session_id;
    agent.close_session(session_id).await.unwrap();
    session_id
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
async fn agent_lifecycle_text_turn_and_finish_event() {
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
    let outcome = agent.wait_turn(turn).await.unwrap();
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
    let AgentEvent::TurnFinished {
        outcome: event_outcome,
        ..
    } = finished
    else {
        unreachable!();
    };
    assert_eq!(event_outcome, outcome);
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
    assert!(!matches!(late_finish, Ok(true)));
    assert!(!agent.list_sessions().await.unwrap()[0].loaded);
    agent.delete_session(info.session_id).await.unwrap();
    assert!(agent.list_sessions().await.unwrap().is_empty());
    agent.shutdown().await.unwrap();
    while events.recv().await.is_some() {}
    remove_base(&base).await;
}

#[tokio::test]
async fn create_session_freezes_defaults_and_overrides_before_store_creation() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-session-settings-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let supported = fake_supported_reasoning();
    let (deep, _) =
        FakeModel::with_descriptor("deep", [ModelScript::Text("deep")], supported.clone(), true);
    let (fast, _) = FakeModel::with_descriptor(
        "fast",
        [ModelScript::Text("fast")],
        BTreeSet::from([ReasoningPreference::Auto, ReasoningPreference::Low]),
        true,
    );
    let (no_tools, _) = FakeModel::with_descriptor(
        "no-tools",
        [ModelScript::Text("no tools")],
        supported.clone(),
        false,
    );
    let mut agent_config = config(base.join("data"), vec!["read"]);
    let profile = agent_config.profiles.get_mut("test").unwrap();
    profile.model = "deep".to_owned();
    profile.reasoning = ReasoningPreference::High;
    agent_config.models.insert(
        "deep".to_owned(),
        configured_model(
            supported.clone(),
            true,
            "MINICORE_UNUSED_DEEP_KEY".to_owned(),
        ),
    );
    agent_config.models.insert(
        "fast".to_owned(),
        configured_model(
            BTreeSet::from([ReasoningPreference::Auto, ReasoningPreference::Low]),
            true,
            "MINICORE_UNUSED_FAST_KEY".to_owned(),
        ),
    );
    agent_config.models.insert(
        "no-tools".to_owned(),
        configured_model(supported, false, "MINICORE_UNUSED_NO_TOOLS_KEY".to_owned()),
    );
    let deep: Arc<dyn Model> = deep;
    let fast: Arc<dyn Model> = fast;
    let no_tools: Arc<dyn Model> = no_tools;
    let session_models = Models::from_values(BTreeMap::from([
        ("deep".to_owned(), deep),
        ("fast".to_owned(), fast),
        ("no-tools".to_owned(), no_tools),
    ]));
    let mut agent = Agent::open_with_models(agent_config, session_models)
        .await
        .unwrap();

    let defaulted = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    assert_eq!(defaulted.model, "deep");
    assert_eq!(defaulted.reasoning, ReasoningPreference::High);

    let mut selected = create_request(&workspace);
    selected.model = Some("fast".to_owned());
    selected.reasoning = Some(ReasoningPreference::Low);
    let selected = agent.create_session(selected).await.unwrap();
    assert_eq!(selected.model, "fast");
    assert_eq!(selected.reasoning, ReasoningPreference::Low);

    for (model, reasoning, expected) in [
        ("fast", None, AgentError::InvalidSessionSettings),
        (
            "fast",
            Some(ReasoningPreference::High),
            AgentError::InvalidSessionSettings,
        ),
        (
            "no-tools",
            Some(ReasoningPreference::Low),
            AgentError::InvalidSessionSettings,
        ),
        (
            "missing",
            Some(ReasoningPreference::Low),
            AgentError::ModelNotFound,
        ),
    ] {
        let mut request = create_request(&workspace);
        request.model = Some(model.to_owned());
        request.reasoning = reasoning;
        let error = agent.create_session(request).await.unwrap_err();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(agent.store.list_sessions().await.unwrap().len(), 2);
    }

    agent.shutdown().await.unwrap();
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
async fn symlinked_workspace_inputs_persist_and_reopen_the_canonical_target() {
    use std::os::unix::fs::symlink;

    let base = std::env::temp_dir().join(format!(
        "minicore-agent-symlinked-workspace-input-{}",
        SessionId::new().unwrap()
    ));
    let target = base.join("target");
    let root_link = base.join("root-link");
    tokio::fs::create_dir_all(&target).await.unwrap();
    symlink(&target, &root_link).unwrap();
    let canonical_target = tokio::fs::canonicalize(&target).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();

    let direct = agent
        .create_session(create_request(&root_link))
        .await
        .unwrap();
    let dotted = agent
        .create_session(create_request(&root_link.join(".")))
        .await
        .unwrap();
    for info in [&direct, &dotted] {
        assert_eq!(info.workspace, canonical_target);
        assert_eq!(
            agent
                .store
                .load_record(info.session_id)
                .await
                .unwrap()
                .workspace,
            canonical_target
        );
        agent.close_session(info.session_id).await.unwrap();
        let reopened = agent.open_session(info.session_id).await.unwrap();
        assert_eq!(reopened.workspace, canonical_target);
    }

    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn unloaded_open_revalidates_missing_and_redirected_workspace_identity() {
    use std::os::unix::fs::symlink;

    let base = std::env::temp_dir().join(format!(
        "minicore-agent-workspace-reopen-{}",
        SessionId::new().unwrap()
    ));
    let deleted_workspace = base.join("deleted-workspace");
    let redirected_workspace = base.join("redirected-workspace");
    let other_workspace = base.join("other-workspace");
    tokio::fs::create_dir_all(&deleted_workspace).await.unwrap();
    tokio::fs::create_dir_all(&redirected_workspace)
        .await
        .unwrap();
    tokio::fs::create_dir_all(&other_workspace).await.unwrap();
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

    let redirected = agent
        .create_session(create_request(&redirected_workspace))
        .await
        .unwrap();
    let moved_original = base.join("moved-original-workspace");
    tokio::fs::rename(&redirected_workspace, &moved_original)
        .await
        .unwrap();
    symlink(&other_workspace, &redirected_workspace).unwrap();
    assert_eq!(
        agent.open_session(redirected.session_id).await.unwrap(),
        redirected
    );
    agent.close_session(redirected.session_id).await.unwrap();
    assert!(matches!(
        agent.open_session(redirected.session_id).await,
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
        agent.wait_turn(stale).await,
        Err(AgentError::TurnNotFound)
    ));
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn reopening_uses_manifest_spec_after_profile_core_settings_drift() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-profile-drift-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (old_model, old_calls) = FakeModel::new([ModelScript::Text("manifest model")]);
    let old_requests = old_model.requests();
    let mut old_agent = Agent::open_with_models(
        config(base.join("data"), Vec::new()),
        models(Arc::clone(&old_model)),
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

    let (new_model, new_calls) = FakeModel::with_descriptor(
        "new",
        [ModelScript::Text("profile model")],
        fake_supported_reasoning(),
        true,
    );
    let mut new_config = config(base.join("data"), Vec::new());
    let profile = new_config.profiles.get_mut("test").unwrap();
    profile.model = "new".to_owned();
    profile.reasoning = ReasoningPreference::High;
    profile.system_prompt = "changed system prompt".to_owned();
    profile.tools = vec!["read".to_owned()];
    profile.max_tool_rounds = 5;
    new_config.models.insert(
        "new".to_owned(),
        configured_model(
            fake_supported_reasoning(),
            true,
            "MINICORE_UNUSED_PROFILE_DRIFT_KEY".to_owned(),
        ),
    );
    let old_runtime_model: Arc<dyn Model> = old_model;
    let new_runtime_model: Arc<dyn Model> = new_model;
    let runtime_models = Models::from_values(BTreeMap::from([
        ("fake".to_owned(), old_runtime_model),
        ("new".to_owned(), new_runtime_model),
    ]));
    let mut new_agent = Agent::open_with_models(new_config, runtime_models)
        .await
        .unwrap();
    let listed = new_agent.list_sessions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].model, "fake");
    assert_eq!(listed[0].reasoning, ReasoningPreference::Auto);
    assert!(!listed[0].loaded);
    let reopened = new_agent.open_session(info.session_id).await.unwrap();
    assert_eq!(reopened.model, "fake");
    assert_eq!(reopened.reasoning, ReasoningPreference::Auto);
    let loaded = new_agent.sessions.get(info.session_id).unwrap();
    assert_eq!(loaded.spec.model.as_str(), "fake");
    assert_eq!(loaded.spec.reasoning, ReasoningPreference::Auto);
    assert_eq!(loaded.spec.system_prompt.as_str(), "You are a test agent.");
    assert!(loaded.spec.enabled_tools.is_empty());
    assert_eq!(loaded.spec.max_tool_rounds, 4);

    let turn = new_agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "continue from manifest".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        new_agent
            .turn_handle(turn)
            .unwrap()
            .wait()
            .await
            .unwrap()
            .terminal,
        TurnTerminal::Completed
    );
    assert_eq!(old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(new_calls.load(Ordering::SeqCst), 0);
    {
        let requests = old_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].tools().is_empty());
        assert!(requests[0].messages().iter().any(|message| matches!(
            message,
            minicore_runtime::model::ModelMessage::System(text)
                if text == "You are a test agent."
        )));
        assert!(!requests[0].messages().iter().any(|message| matches!(
            message,
            minicore_runtime::model::ModelMessage::System(text)
                if text == "changed system prompt"
        )));
    }
    let transcript = new_agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 32,
        })
        .await
        .unwrap();
    assert!(transcript.entries.iter().any(|entry| matches!(
        entry,
        ConversationEntry::UserMessage(message)
            if message.execution.model.as_str() == "fake"
                && message.execution.reasoning == ReasoningPreference::Auto
                && message.execution.max_tool_rounds == 4
    )));
    assert_eq!(
        tokio::fs::read(&manifest_path).await.unwrap(),
        manifest_before
    );
    new_agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "current_thread")]
async fn list_skips_one_bad_manifest_while_explicit_open_stays_strict() {
    const MANIFEST_SECRET: &str = "PRIVATE-MANIFEST-CONTENT";
    let tracing_capture = TestTracingCapture::new();
    let _subscriber_guard = tracing_capture.enter();
    let (mut agent, base, workspace, _) = agent_fixture(
        "bad-manifest-list",
        [ModelScript::Text("unused")],
        Vec::new(),
    )
    .await;
    let healthy = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let corrupt = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    agent.close_session(healthy.session_id).await.unwrap();
    let manifest_path = session_directory(&base, corrupt.session_id).join("manifest.json");
    let manifest_before = tokio::fs::read(&manifest_path).await.unwrap();
    let corrupt_manifest = format!("not-json {MANIFEST_SECRET}");
    tokio::fs::write(&manifest_path, &corrupt_manifest)
        .await
        .unwrap();

    let listed = agent.list_sessions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_id, healthy.session_id);
    assert_eq!(listed[0].model, "fake");
    assert_eq!(listed[0].reasoning, ReasoningPreference::Auto);

    let mut mismatched: minicore_runtime::SessionManifest =
        serde_json::from_slice(&manifest_before).unwrap();
    mismatched.spec.reasoning = ReasoningPreference::High;
    tokio::fs::write(&manifest_path, serde_json::to_vec(&mismatched).unwrap())
        .await
        .unwrap();
    assert_eq!(agent.list_sessions().await.unwrap(), listed);

    tokio::fs::write(&manifest_path, &corrupt_manifest)
        .await
        .unwrap();
    agent.close_session(corrupt.session_id).await.unwrap();
    assert_eq!(
        tokio::fs::read_to_string(&manifest_path).await.unwrap(),
        corrupt_manifest
    );
    assert!(matches!(
        agent.open_session(corrupt.session_id).await,
        Err(AgentError::Store)
    ));
    assert_eq!(
        agent.open_session(healthy.session_id).await.unwrap().model,
        "fake"
    );

    agent.shutdown().await.unwrap();
    remove_base(&base).await;
    let logs = tracing_capture.contents();
    assert!(!logs.contains(MANIFEST_SECRET));
    let corrupt_id = corrupt.session_id.to_string();
    assert!(logs.lines().any(|line| {
        line.contains("skipping session with invalid manifest")
            && test_log_field_equals(line, "session_id", &corrupt_id)
            && test_log_field_equals(line, "error_kind", "corrupt")
    }));
    assert!(logs.lines().any(|line| {
        line.contains("skipping session with invalid manifest")
            && test_log_field_equals(line, "session_id", &corrupt_id)
            && test_log_field_equals(line, "error_kind", "manifest_spec_mismatch")
    }));
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
async fn slow_agent_event_consumer_does_not_block_turn_or_core_state() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-slow-event-consumer-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("final")]);
    let mut agent_config = config(base.join("data"), Vec::new());
    agent_config.event_capacity = 1;
    let mut agent = Agent::open_with_models(agent_config, models(model))
        .await
        .unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "finish without consuming events".to_owned(),
        })
        .await
        .unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        agent.turn_handle(turn).unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome.terminal, TurnTerminal::Completed);
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        SessionStatus::Idle
    );
    let transcript = agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 32,
        })
        .await
        .unwrap();
    assert!(transcript.entries.iter().any(|entry| matches!(
        entry,
        minicore_runtime::ConversationEntry::TurnTerminal(terminal)
            if terminal.turn_id == turn.turn_id
    )));
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_owned_completion_task_uses_capacity_one_channel() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-single-completion-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let started = Arc::new(Semaphore::new(0));
    let (model, _) = FakeModel::new([ModelScript::Block, ModelScript::Text("second")]);
    let model = model.with_started(Arc::clone(&started));
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    assert_eq!(
        agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .pump
            .completion_capacity(),
        1
    );
    let first = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "first active turn".to_owned(),
        })
        .await
        .unwrap();
    let active = agent
        .sessions
        .get(info.session_id)
        .unwrap()
        .active_turn
        .as_ref()
        .unwrap();
    assert_eq!(active.handle.turn_id(), first.turn_id);
    assert!(!active.completion_task.is_finished());
    tokio::time::timeout(Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(agent.cancel(first).unwrap());
    tokio::time::timeout(
        Duration::from_secs(5),
        agent.turn_handle(first).unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();

    let second = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "second active turn".to_owned(),
        })
        .await
        .unwrap();
    let active = agent
        .sessions
        .get(info.session_id)
        .unwrap()
        .active_turn
        .as_ref()
        .unwrap();
    assert_eq!(active.handle.turn_id(), second.turn_id);
    assert_eq!(
        agent
            .sessions
            .get(info.session_id)
            .unwrap()
            .pump
            .completion_capacity(),
        1
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        agent.turn_handle(second).unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome.terminal, TurnTerminal::Completed);
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_cleanup_retains_active_turn_until_completion_task_is_joined() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-cancelled-completion-cleanup-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("first"), ModelScript::Text("second")]);
    let mut agent = Agent::open_with_models(config(base.join("data"), Vec::new()), models(model))
        .await
        .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    let capacity_guard = tokio::time::timeout(
        Duration::from_secs(5),
        reserve_completion_capacity(&agent, info.session_id),
    )
    .await
    .unwrap();
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        agent.send(SendMessage {
            session_id: info.session_id,
            text: "first turn".to_owned(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let first_outcome = tokio::time::timeout(
        Duration::from_secs(5),
        agent.turn_handle(first).unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first_outcome.terminal, TurnTerminal::Completed);

    let cancelled_send = tokio::time::timeout(
        Duration::from_millis(100),
        agent.send(SendMessage {
            session_id: info.session_id,
            text: "cancelled second turn submission".to_owned(),
        }),
    )
    .await;
    let cleanup_was_cancelled = cancelled_send.is_err();
    let unexpectedly_submitted = match cancelled_send {
        Ok(Ok(turn)) => Some(turn),
        Ok(Err(_)) | Err(_) => None,
    };
    let retained_turn_id = active_turn_id(&agent, info.session_id);

    drop(capacity_guard);
    let retried_turn = match tokio::time::timeout(
        Duration::from_secs(5),
        agent.send(SendMessage {
            session_id: info.session_id,
            text: "retried second turn submission".to_owned(),
        }),
    )
    .await
    {
        Ok(Ok(turn)) => Some(turn),
        Ok(Err(_)) | Err(_) => None,
    };
    let first_completion_observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Some(AgentEvent::TurnFinished { turn, .. }) if turn == first => break true,
                Some(_) => {}
                None => break false,
            }
        }
    })
    .await
    .unwrap_or(false);
    let turn_to_wait = retried_turn.or(unexpectedly_submitted);
    let retried_turn_completed = if let Some(turn) = turn_to_wait {
        match agent.turn_handle(turn) {
            Ok(handle) => matches!(
                tokio::time::timeout(Duration::from_secs(5), handle.wait()).await,
                Ok(Ok(outcome)) if outcome.terminal == TurnTerminal::Completed
            ),
            Err(_) => false,
        }
    } else {
        false
    };
    let shutdown_succeeded = matches!(
        tokio::time::timeout(Duration::from_secs(5), agent.shutdown()).await,
        Ok(Ok(()))
    );
    drop(events);
    let cleanup_succeeded = tokio::time::timeout(Duration::from_secs(5), remove_base(&base))
        .await
        .is_ok();

    assert!(cleanup_was_cancelled);
    assert_eq!(retained_turn_id, Some(first.turn_id));
    assert!(first_completion_observed);
    assert!(retried_turn.is_some());
    assert!(retried_turn_completed);
    assert!(shutdown_succeeded);
    assert!(cleanup_succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn congested_outer_events_do_not_couple_session_execution() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-session-event-isolation-{}",
        SessionId::new().unwrap()
    ));
    let workspace_a = base.join("workspace-a");
    let workspace_b = base.join("workspace-b");
    tokio::fs::create_dir_all(&workspace_a).await.unwrap();
    tokio::fs::create_dir_all(&workspace_b).await.unwrap();
    let workspace_a = tokio::fs::canonicalize(&workspace_a).await.unwrap();
    let workspace_b = tokio::fs::canonicalize(&workspace_b).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("a"), ModelScript::Text("b")]);
    let mut agent_config = config(base.join("data"), Vec::new());
    agent_config.event_capacity = 1;
    let mut agent = Agent::open_with_models(agent_config, models(model))
        .await
        .unwrap();
    let gate = Arc::new(crate::sessions::SessionPumpStartupGate::new());
    crate::sessions::block_session_pump_startup(workspace_a.clone(), Arc::clone(&gate));
    let a = agent
        .create_session(create_request(&workspace_a))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), gate.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let b = agent
        .create_session(create_request(&workspace_b))
        .await
        .unwrap();
    gate.release.add_permits(1);
    let turn_a = agent
        .send(SendMessage {
            session_id: a.session_id,
            text: "session A".to_owned(),
        })
        .await
        .unwrap();
    let handle_a = agent.turn_handle(turn_a).unwrap();
    let turn_b = agent
        .send(SendMessage {
            session_id: b.session_id,
            text: "session B".to_owned(),
        })
        .await
        .unwrap();
    let handle_b = agent.turn_handle(turn_b).unwrap();
    let (outcome_a, outcome_b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(handle_a.wait(), handle_b.wait())
    })
    .await
    .unwrap();
    assert_eq!(outcome_a.unwrap().terminal, TurnTerminal::Completed);
    assert_eq!(outcome_b.unwrap().terminal, TurnTerminal::Completed);
    assert_eq!(
        agent.session_state(a.session_id).unwrap().status,
        SessionStatus::Idle
    );
    assert_eq!(
        agent.session_state(b.session_id).unwrap().status,
        SessionStatus::Idle
    );
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_active_turn_reclaims_completion_and_pump_tasks() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-active-close-{}",
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
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "close active turn".to_owned(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let handle = agent.turn_handle(turn).unwrap();
    let pump_stop = agent
        .sessions
        .get(info.session_id)
        .unwrap()
        .pump
        .stop_token();
    tokio::time::timeout(Duration::from_secs(5), agent.close_session(info.session_id))
        .await
        .unwrap()
        .unwrap();
    assert!(pump_stop.is_cancelled());
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.terminal, TurnTerminal::CancelledByShutdown);
    assert!(!agent.sessions.contains(info.session_id));
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_closed_is_last_event_for_its_instance() {
    let (mut agent, base, workspace, _) = agent_fixture(
        "session-closed-last",
        [ModelScript::Text("final")],
        Vec::new(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    wait_for_event(&mut events, |event| {
        matches!(
            event,
            AgentEvent::SessionState { state, .. }
                if state.session_id == info.session_id
                    && state.status == SessionStatus::Idle
        )
    })
    .await;
    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "complete before close".to_owned(),
        })
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        agent.turn_handle(turn).unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    let instance_id = info.instance_id.unwrap();
    tokio::time::timeout(Duration::from_secs(5), agent.close_session(info.session_id))
        .await
        .unwrap()
        .unwrap();
    wait_for_event(&mut events, |event| {
        matches!(
            event,
            AgentEvent::SessionClosed { session_id, meta }
                if *session_id == info.session_id && meta.instance_id == instance_id
        )
    })
    .await;
    let late_same_instance = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            match events.recv().await {
                Some(event) if event_instance_id(&event) == instance_id => break true,
                Some(_) => {}
                None => break false,
            }
        }
    })
    .await;
    assert!(!matches!(late_same_instance, Ok(true)));
    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_interaction_event_leaves_answerable_session_state() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-interaction-event-drop-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let workspace = tokio::fs::canonicalize(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::tool_call(
        "write",
        json!({"path": "approval.txt", "content": "approved"}),
    )]);
    let mut agent_config =
        config_with_approval(base.join("data"), vec!["write"], ApprovalMode::Ask);
    agent_config.event_capacity = 1;
    let mut agent = Agent::open_with_models(agent_config, models(model))
        .await
        .unwrap();
    let gate = Arc::new(crate::sessions::SessionPumpStartupGate::new());
    crate::sessions::block_session_pump_startup(workspace.clone(), Arc::clone(&gate));
    let events = agent.take_events().unwrap();
    let info = agent
        .create_session(create_request(&workspace))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), gate.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    gate.release.add_permits(1);

    let turn = agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "request approval without an event slot".to_owned(),
        })
        .await
        .unwrap();
    let interaction = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = agent.session_state(info.session_id).unwrap();
            if state.status == SessionStatus::WaitingForInput {
                break state
                    .pending_interaction
                    .expect("waiting state needs interaction");
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(interaction.turn_id, turn.turn_id);
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
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        agent.turn_handle(turn).unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome.terminal, TurnTerminal::Completed);
    assert!(
        !tokio::fs::try_exists(workspace.join("approval.txt"))
            .await
            .unwrap()
    );

    tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(events);
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_event_channel_drops_turn_finished_without_late_replay() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-best-effort-finish-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let workspace = tokio::fs::canonicalize(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("final")]);
    let mut agent_config = config(base.join("data"), Vec::new());
    agent_config.event_capacity = 1;
    let mut agent = Agent::open_with_models(agent_config, models(model))
        .await
        .unwrap();
    let gate = Arc::new(crate::sessions::SessionPumpStartupGate::new());
    crate::sessions::block_session_pump_startup(workspace.clone(), Arc::clone(&gate));
    let mut events = agent.take_events().unwrap();
    let info = match agent.create_session(create_request(&workspace)).await {
        Ok(info) => info,
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, None, &base).await;
            panic!("session creation failed");
        }
    };
    match tokio::time::timeout(Duration::from_secs(5), gate.started.acquire()).await {
        Ok(Ok(permit)) => permit.forget(),
        Ok(Err(_)) => {
            cleanup_full_event_channel_fixture(agent, &gate, None, &base).await;
            panic!("initial pump startup gate closed before SessionOpened emission");
        }
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, None, &base).await;
            panic!("initial SessionOpened emission did not reach the pump startup gate");
        }
    }
    let probe = Arc::new(crate::event::TurnFinishedAttemptProbe::new(
        info.session_id,
        info.instance_id.unwrap(),
    ));
    let probe_registration = crate::event::register_turn_finished_attempt_probe(Arc::clone(&probe));
    gate.release.add_permits(1);

    let turn = match agent
        .send(SendMessage {
            session_id: info.session_id,
            text: "complete while events are full".to_owned(),
        })
        .await
    {
        Ok(turn) => turn,
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("turn submission failed");
        }
    };
    let turn_handle = match agent.turn_handle(turn) {
        Ok(handle) => handle,
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("submitted TurnHandle was unavailable");
        }
    };
    let waiter = turn_handle.clone();
    drop(turn_handle);
    let outcome = match tokio::time::timeout(Duration::from_secs(5), waiter.wait()).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(_)) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("authoritative TurnHandle wait failed");
        }
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("authoritative TurnHandle wait depended on event capacity");
        }
    };
    assert_eq!(outcome.turn_id, turn.turn_id);
    assert_eq!(outcome.terminal, TurnTerminal::Completed);

    let state = match agent.session_state(info.session_id) {
        Ok(state) => state,
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("authoritative session state query failed");
        }
    };
    assert_eq!(state.instance_id, turn.instance_id);
    assert_eq!(state.status, SessionStatus::Idle);
    assert!(state.last_terminal.as_ref().is_some_and(|terminal| {
        terminal.turn_id == turn.turn_id && terminal.terminal == TurnTerminal::Completed
    }));
    let transcript = match agent
        .transcript(GetTranscript {
            session_id: info.session_id,
            after: None,
            limit: 32,
        })
        .await
    {
        Ok(transcript) => transcript,
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("authoritative transcript query failed");
        }
    };
    assert!(transcript.entries.iter().any(|entry| matches!(
        entry,
        minicore_runtime::ConversationEntry::TurnTerminal(terminal)
            if terminal.turn_id == turn.turn_id
                && terminal.terminal == TurnTerminal::Completed
    )));

    if tokio::time::timeout(Duration::from_secs(5), probe.wait_started())
        .await
        .is_err()
    {
        cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
        panic!("TurnFinished send attempt did not start");
    }
    let completed_while_full =
        tokio::time::timeout(Duration::from_millis(100), probe.wait_completed()).await;
    if completed_while_full.is_err() {
        cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
        panic!("TurnFinished send attempt did not complete while the event channel remained full");
    }

    let released = match tokio::time::timeout(Duration::from_secs(5), events.recv()).await {
        Ok(Some(event)) => event,
        Ok(None) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("event stream closed before releasing the old full-slot event");
        }
        Err(_) => {
            cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
            panic!("old full-slot event was not available");
        }
    };
    if !matches!(
        released,
        AgentEvent::SessionOpened { session, meta }
            if session.session_id == turn.session_id
                && session.instance_id == Some(turn.instance_id)
                && meta.instance_id == turn.instance_id
    ) {
        cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
        panic!("old full-slot event did not match the created session instance");
    }
    let violation = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            match events.recv().await {
                Some(AgentEvent::TurnFinished {
                    turn: observed,
                    outcome,
                    ..
                }) if observed == turn
                    && outcome.turn_id == turn.turn_id
                    && observed.instance_id == turn.instance_id =>
                {
                    break "same TurnFinished was replayed after capacity returned";
                }
                Some(_) => {}
                None => break "event stream closed during the no-replay window",
            }
        }
    })
    .await
    .ok();

    cleanup_full_event_channel_fixture(agent, &gate, Some(probe_registration), &base).await;
    if let Some(violation) = violation {
        panic!("{violation}");
    }
}

#[tokio::test]
async fn dropped_event_receiver_stops_session_pump_and_completion_task() {
    let (mut agent, base, workspace, _) =
        agent_fixture("dropped-events", [ModelScript::Block], Vec::new()).await;
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
    let pump_stop = session_pump_stop_token(&agent, info.session_id);
    let active_turn_matches = active_turn_id(&agent, info.session_id) == Some(turn.turn_id);
    let completion_task_was_running =
        active_completion_task_is_finished(&agent, info.session_id) == Some(false);
    drop(events);
    let pump_stopped_before_shutdown =
        tokio::time::timeout(Duration::from_secs(5), pump_stop.cancelled())
            .await
            .is_ok();
    let completion_task_stopped_before_shutdown =
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match active_completion_task_is_finished(&agent, info.session_id) {
                    Some(true) => break true,
                    Some(false) => tokio::task::yield_now().await,
                    None => break false,
                }
            }
        })
        .await
        .unwrap_or(false);
    let shutdown_succeeded = tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .is_ok_and(|result| result.is_ok());
    let cleanup_succeeded = tokio::time::timeout(Duration::from_secs(5), remove_base(&base))
        .await
        .is_ok();

    assert!(active_turn_matches);
    assert!(completion_task_was_running);
    assert!(pump_stopped_before_shutdown);
    assert!(completion_task_stopped_before_shutdown);
    assert!(shutdown_succeeded);
    assert!(cleanup_succeeded);
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

#[tokio::test(flavor = "current_thread")]
async fn session_log_errors_emit_safe_classified_logs() {
    const SYSTEM_PROMPT_MARKER: &str = "SESSION-LOG-SYSTEM-PROMPT-MARKER-SECRET";
    const PROFILE_MARKER: &str = "SESSION-LOG-PROFILE-MARKER-SECRET";
    const RECORD_MARKER: &str = "SESSION-LOG-RECORD-MARKER-SECRET";
    const CONVERSATION_MARKER: &str = "SESSION-LOG-CONVERSATION-MARKER-SECRET";

    let tracing_capture = TestTracingCapture::new();
    let _subscriber_guard = tracing_capture.enter();
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-session-log-tracing-{}",
        SessionId::new().unwrap()
    ));
    let _fixture_guard = TestDirectoryGuard::new(base.clone());
    let data_dir = base.join("data");
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let workspace = tokio::fs::canonicalize(&workspace).await.unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let mut test_config = config(data_dir.clone(), Vec::new());
    let mut profile = test_config.profiles.remove("test").unwrap();
    profile.system_prompt = SYSTEM_PROMPT_MARKER.to_owned();
    test_config.default_profile = PROFILE_MARKER.to_owned();
    test_config
        .profiles
        .insert(PROFILE_MARKER.to_owned(), profile);
    let mut agent = Agent::open_with_models(test_config, models(model))
        .await
        .unwrap();
    let canonical_data_dir = tokio::fs::canonicalize(&data_dir).await.unwrap();

    let conflict_session_id =
        create_closed_session_for_log_test(&mut agent, &workspace, PROFILE_MARKER, RECORD_MARKER)
            .await;
    let unknown_session_id =
        create_closed_session_for_log_test(&mut agent, &workspace, PROFILE_MARKER, RECORD_MARKER)
            .await;
    let corrupt_session_id =
        create_closed_session_for_log_test(&mut agent, &workspace, PROFILE_MARKER, RECORD_MARKER)
            .await;
    let persisted_record = tokio::fs::read_to_string(
        session_directory(&base, conflict_session_id).join("session.json"),
    )
    .await
    .unwrap();
    let persisted_manifest = tokio::fs::read_to_string(
        session_directory(&base, conflict_session_id).join("manifest.json"),
    )
    .await
    .unwrap();

    let mut conflict_log = agent.store.open_log(conflict_session_id).await.unwrap();
    let conflict_result = conflict_log
        .append(
            ConversationSeq::new(1),
            vec![session_log_test_entry(
                1,
                TurnId::new().unwrap(),
                CONVERSATION_MARKER,
            )],
        )
        .await;
    let conflict_close_result = conflict_log.close().await;
    drop(conflict_log);

    let mut unknown_log = agent.store.open_log(unknown_session_id).await.unwrap();
    unknown_log.inject_unknown_after_write();
    let unknown_result = unknown_log
        .append(
            ConversationSeq::ZERO,
            vec![session_log_test_entry(
                1,
                TurnId::new().unwrap(),
                CONVERSATION_MARKER,
            )],
        )
        .await;
    let unknown_close_result = unknown_log.close().await;
    drop(unknown_log);

    tokio::fs::write(
        session_directory(&base, corrupt_session_id).join("conversation.log"),
        format!("{{\"entries\":[\"{CONVERSATION_MARKER}\"]}}\n"),
    )
    .await
    .unwrap();
    let corrupt_result = match agent.store.open_log(corrupt_session_id).await {
        Ok(mut log) => log.close().await.map_err(StoreError::Log),
        Err(error) => Err(error),
    };

    let conflict_delete_result = agent.delete_session(conflict_session_id).await;
    let unknown_delete_result = agent.delete_session(unknown_session_id).await;
    let corrupt_delete_result = agent.delete_session(corrupt_session_id).await;
    let shutdown_result = agent.shutdown().await;
    let lexical_data_dir_text = data_dir.to_string_lossy().into_owned();
    let canonical_data_dir_text = canonical_data_dir.to_string_lossy().into_owned();
    let workspace_text = workspace.to_string_lossy().into_owned();
    remove_base(&base).await;
    let logs = tracing_capture.contents();

    assert!(persisted_record.contains(PROFILE_MARKER));
    assert!(persisted_record.contains(RECORD_MARKER));
    assert!(persisted_manifest.contains(SYSTEM_PROMPT_MARKER));
    assert!(matches!(
        conflict_result,
        Err(error) if error.kind() == SessionLogErrorKind::Conflict
    ));
    assert!(conflict_close_result.is_ok());
    assert!(matches!(
        unknown_result,
        Err(error) if error.kind() == SessionLogErrorKind::UnknownOutcome
    ));
    assert!(matches!(
        unknown_close_result,
        Err(error) if error.kind() == SessionLogErrorKind::UnknownOutcome
    ));
    assert!(matches!(
        corrupt_result,
        Err(StoreError::Log(error)) if error.kind() == SessionLogErrorKind::Corrupt
    ));
    assert!(conflict_delete_result.is_ok());
    assert!(unknown_delete_result.is_ok());
    assert!(corrupt_delete_result.is_ok());
    assert!(shutdown_result.is_ok());
    assert!(!logs.contains("session log tail repaired"));
    for private in [
        lexical_data_dir_text.as_str(),
        canonical_data_dir_text.as_str(),
        workspace_text.as_str(),
        SYSTEM_PROMPT_MARKER,
        PROFILE_MARKER,
        RECORD_MARKER,
        CONVERSATION_MARKER,
        "entries",
        "local session log append head conflicts",
        "local session log is corrupt",
        "local session log mutation outcome is unknown",
        "SessionLogError",
        "Conflict",
        "Corrupt",
        "UnknownOutcome",
    ] {
        assert!(!logs.contains(private));
    }
    for (session_id, operation, error_kind) in [
        (conflict_session_id, "append", "session_log_conflict"),
        (unknown_session_id, "append", "session_log_unknown_outcome"),
        (corrupt_session_id, "open_log", "session_log_corrupt"),
    ] {
        let session_id = session_id.to_string();
        assert!(logs.lines().any(|line| {
            test_log_field_equals(line, "session_id", &session_id)
                && test_log_field_equals(line, "operation", operation)
                && test_log_field_equals(line, "error_kind", error_kind)
        }));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_unknown_outcome_stops_future_updates_after_uncertain_write() {
    const RECORD_MARKER: &str = "METADATA-RECORD-MARKER-SECRET";
    const CONTENT_MARKER: &str = "METADATA-CONTENT-MARKER-SECRET";
    const TEST_SECRET_MARKER: &str = "METADATA-TEST-SECRET-MARKER";

    let tracing_capture = TestTracingCapture::new();
    let _subscriber_guard = tracing_capture.enter();
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-touch-unknown-{}",
        SessionId::new().unwrap()
    ));
    let data_dir = base.join("data");
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let workspace = tokio::fs::canonicalize(&workspace).await.unwrap();
    tokio::fs::write(
        workspace.join("metadata-secret.txt"),
        format!("{CONTENT_MARKER}-{TEST_SECRET_MARKER}"),
    )
    .await
    .unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let mut agent = Agent::open_with_models(config(data_dir.clone(), Vec::new()), models(model))
        .await
        .unwrap();
    let events = agent.take_events().unwrap();
    let mut request = create_request(&workspace);
    request.title = Some(format!("{RECORD_MARKER}-{TEST_SECRET_MARKER}"));
    let info = agent.create_session(request).await.unwrap();
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
    let session_id_text = info.session_id.to_string();
    let data_dir_text = data_dir.to_string_lossy().into_owned();
    let workspace_text = workspace.to_string_lossy().into_owned();
    remove_base(&base).await;

    let logs = tracing_capture.contents();
    for private in [
        data_dir_text.as_str(),
        workspace_text.as_str(),
        RECORD_MARKER,
        CONTENT_MARKER,
        TEST_SECRET_MARKER,
        "StoreError",
        "UnknownOutcome",
    ] {
        assert!(!logs.contains(private));
    }
    let metadata_failure_lines = logs
        .lines()
        .filter(|line| line.contains("metadata update failed"))
        .collect::<Vec<_>>();
    assert!(
        !metadata_failure_lines.is_empty(),
        "metadata terminal failure must emit a stable safe log marker"
    );
    let metadata_failure_line = metadata_failure_lines
        .into_iter()
        .find(|line| test_log_field_equals(line, "session_id", &session_id_text))
        .expect("metadata failure log must identify the affected session");
    assert!(test_log_field_equals(
        metadata_failure_line,
        "error_kind",
        "unknown_outcome"
    ));
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
async fn complete_config_opens_with_an_injected_model() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-complete-config-{}",
        SessionId::new().unwrap()
    ));
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let config = config(base.join("data"), Vec::new());

    let agent = Agent::open_with_models(config, models(model))
        .await
        .unwrap();
    agent.shutdown().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test]
async fn capability_mismatch_precedes_missing_credential_at_agent_open() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-capability-before-credential-{}",
        SessionId::new().unwrap()
    ));
    let key_env = format!(
        "MINICORE_MISSING_CAPABILITY_KEY_{}",
        SessionId::new().unwrap()
    );
    assert!(std::env::var_os(&key_env).is_none());
    let config = config_with_model_capabilities(
        base.join("data"),
        ReasoningPreference::High,
        BTreeSet::from([ReasoningPreference::Auto]),
        false,
        key_env,
    );

    let result = Agent::open(config).await;
    remove_base(&base).await;
    assert!(matches!(
        result,
        Err(AgentError::Config(ConfigError::UnsupportedReasoning))
    ));
}

#[tokio::test]
async fn capability_mismatch_precedes_store_open_at_agent_open() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-capability-before-store-{}",
        SessionId::new().unwrap()
    ));
    tokio::fs::create_dir_all(&base).await.unwrap();
    let data_dir = base.join("not-a-directory");
    tokio::fs::write(&data_dir, b"store must not be opened")
        .await
        .unwrap();
    let (model, _) = FakeModel::new([ModelScript::Text("unused")]);
    let config = config_with_model_capabilities(
        data_dir,
        ReasoningPreference::High,
        BTreeSet::from([ReasoningPreference::Auto]),
        false,
        "MINICORE_UNUSED_STORE_PRIORITY_KEY".to_owned(),
    );

    let result = Agent::open_with_models(config, models(model)).await;
    remove_base(&base).await;
    assert!(matches!(
        result,
        Err(AgentError::Config(ConfigError::UnsupportedReasoning))
    ));
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
async fn open_rejects_openai_config_when_its_key_environment_is_missing() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-model-config-{}",
        SessionId::new().unwrap()
    ));
    let key_env = format!("MINICORE_MISSING_KEY_{}", SessionId::new().unwrap());
    let mut config = config(base.join("data"), Vec::new());
    config.models.insert(
        "fake".to_owned(),
        configured_model(fake_supported_reasoning(), true, key_env),
    );
    assert!(matches!(
        Agent::open(config).await,
        Err(crate::error::AgentError::Config(
            crate::config::ConfigError::InvalidModel
        ))
    ));
    remove_base(&base).await;
}
