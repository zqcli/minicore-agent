use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::stream;
use serde_json::json;
use tokio::sync::Notify;

use minicore_runtime::ToolCallId;
use minicore_runtime::history::{HistoryItem, UserMessageKind};
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason,
    ModelMessage, ModelRef, ModelRequest, ModelStartFuture, ModelStream, ReasoningPreference,
    Usage,
};
use minicore_runtime::tools::ToolResultOutcome;

use crate::config::AgentConfig;
use crate::error::AgentError;
use crate::event::{AgentEvent, OutputChannel};
use crate::history::GetHistory;
use crate::models::{ModelConfig, Models};
use crate::profiles::{ApprovalMode, Profile};
use crate::sessions::{WorkerGate, panic_next_worker, pause_next_worker_before_join};
use crate::store::{Store, fail_next_append, fail_next_record_write};

use super::{Agent, CreateSession, SessionUpdateResult, TurnRef};

struct TestDirectoryGuard {
    path: PathBuf,
}

impl Drop for TestDirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn fixture_dir(label: &str) -> (PathBuf, TestDirectoryGuard) {
    let path = std::env::temp_dir().join(format!("minicore-agent-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    (path.clone(), TestDirectoryGuard { path })
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

/// A model call that parks on `release` and signals `entered` once parked.
#[derive(Clone)]
struct BlockGate {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl BlockGate {
    fn new() -> Self {
        Self {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone)]
enum ModelScript {
    Text(&'static str),
    ToolCall(&'static str, serde_json::Value),
    ToolCallAfterGate(BlockGate, &'static str, serde_json::Value),
    /// Parks until `release` fires and signals `entered` when parked.
    BlockUntil(BlockGate, &'static str),
    /// Emits many text deltas back-to-back with no awaits so the runtime can
    /// saturate its best-effort event queue deterministically, then ends the
    /// request with one tool call so the loop runs a tool (with an await)
    /// before its next request.
    BurstTextThenTool(&'static str, usize, &'static str, serde_json::Value),
}

struct FakeModel {
    model_ref: ModelRef,
    descriptor: ModelDescriptor,
    scripts: Arc<Mutex<VecDeque<ModelScript>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    calls: Arc<AtomicUsize>,
}

impl FakeModel {
    fn new(model_ref: &str, scripts: impl IntoIterator<Item = ModelScript>) -> Arc<Self> {
        let model_ref: ModelRef = model_ref.parse().unwrap();
        let descriptor =
            ModelDescriptor::new(model_ref.clone(), 16_384, fake_supported_reasoning(), true)
                .unwrap();
        Arc::new(Self {
            model_ref,
            descriptor,
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn requests(&self) -> Arc<Mutex<Vec<ModelRequest>>> {
        Arc::clone(&self.requests)
    }
}

impl Model for FakeModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start(&self, request: ModelRequest, _context: ModelCallContext) -> ModelStartFuture<'_> {
        let model_ref = self.model_ref.clone();
        self.requests.lock().unwrap().push(request);
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ModelScript::Text("default"));
        Box::pin(async move {
            let _ = model_ref;
            let _ = call_index;
            match script {
                ModelScript::BlockUntil(gate, text) => {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                    events(vec![
                        ModelEvent::text_delta(text).unwrap(),
                        ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ])
                }
                ModelScript::ToolCall(name, arguments) => {
                    let tool_call_id =
                        ToolCallId::new(format!("{name}-call-{call_index}")).unwrap();
                    events(vec![
                        ModelEvent::ToolCallStart {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: name.parse().unwrap(),
                        },
                        ModelEvent::tool_call_arguments_delta(
                            tool_call_id.clone(),
                            arguments.to_string(),
                        )
                        .unwrap(),
                        ModelEvent::ToolCallEnd { tool_call_id },
                        ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::ToolCalls,
                        },
                    ])
                }
                ModelScript::ToolCallAfterGate(gate, name, arguments) => {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                    let tool_call_id =
                        ToolCallId::new(format!("{name}-call-{call_index}")).unwrap();
                    events(vec![
                        ModelEvent::ToolCallStart {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: name.parse().unwrap(),
                        },
                        ModelEvent::tool_call_arguments_delta(
                            tool_call_id.clone(),
                            arguments.to_string(),
                        )
                        .unwrap(),
                        ModelEvent::ToolCallEnd { tool_call_id },
                        ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::ToolCalls,
                        },
                    ])
                }
                ModelScript::BurstTextThenTool(delta, count, name, arguments) => {
                    let tool_call_id =
                        ToolCallId::new(format!("{name}-call-{call_index}")).unwrap();
                    let event_rows: Vec<Result<ModelEvent, ModelError>> = (0..count)
                        .map(move |_| ModelEvent::text_delta(delta).unwrap())
                        .chain([
                            ModelEvent::ToolCallStart {
                                tool_call_id: tool_call_id.clone(),
                                tool_name: name.parse().unwrap(),
                            },
                            ModelEvent::tool_call_arguments_delta(
                                tool_call_id.clone(),
                                arguments.to_string(),
                            )
                            .unwrap(),
                            ModelEvent::ToolCallEnd { tool_call_id },
                            ModelEvent::Usage {
                                usage: Usage::new(1, 1, 0),
                            },
                            ModelEvent::Finish {
                                reason: ModelFinishReason::ToolCalls,
                            },
                        ])
                        .map(Ok::<ModelEvent, ModelError>)
                        .collect();
                    let stream: ModelStream = Box::pin(stream::iter(event_rows));
                    Ok(stream)
                }
                ModelScript::Text(text) => events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ]),
            }
        })
    }
}

fn events(values: Vec<ModelEvent>) -> Result<ModelStream, ModelError> {
    // Stream each model event with a small tick so the best-effort loop event
    // queue does not drop the burst, mirroring real network-timed delivery.
    Ok(Box::pin(stream::unfold(
        values.into_iter().collect::<Vec<_>>(),
        |mut remaining| async move {
            if remaining.is_empty() {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let event = remaining.remove(0);
            Some((Ok(event), remaining))
        },
    )))
}

fn config(
    data_dir: PathBuf,
    models: BTreeMap<String, ModelConfig>,
    profile: Profile,
) -> AgentConfig {
    AgentConfig {
        data_dir,
        event_capacity: 256,
        default_profile: "test".to_owned(),
        profiles: BTreeMap::from([("test".to_owned(), profile)]),
        models,
        loop_options: Default::default(),
    }
}

fn read_profile() -> Profile {
    Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    }
}

fn model_config(api_key_env: &str) -> ModelConfig {
    ModelConfig::OpenAiResponses {
        model: "provider-model".to_owned(),
        base_url: "https://example.invalid/v1".to_owned(),
        api_key_env: api_key_env.to_owned(),
        physical_context_window: 10_000,
        output_budget_tokens: 1_000,
        safety_margin_tokens: 1_000,
        supported_reasoning: fake_supported_reasoning(),
        supports_tools: true,
        request_timeout_seconds: Some(30),
    }
}

async fn open_agent(
    data_dir: &Path,
    fake_models: BTreeMap<String, Arc<FakeModel>>,
    profile: Profile,
) -> Agent {
    let models = fake_models
        .iter()
        .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
        .collect::<BTreeMap<_, _>>();
    let mut models_config = BTreeMap::new();
    for id in fake_models.keys() {
        models_config.insert(id.clone(), model_config("MINICORE_AGENT_TEST_KEY"));
    }
    let config = config(data_dir.to_path_buf(), models_config, profile);
    Agent::open_with_models(config, Models::from_values(models))
        .await
        .unwrap()
}

#[derive(Clone, Copy, Default)]
struct AgentOptions {
    agent_event_capacity: usize,
    loop_event_capacity: Option<usize>,
}

async fn open_agent_with(
    data_dir: &Path,
    fake_models: BTreeMap<String, Arc<FakeModel>>,
    profile: Profile,
    options: AgentOptions,
) -> Agent {
    let models = fake_models
        .iter()
        .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
        .collect::<BTreeMap<_, _>>();
    let mut models_config = BTreeMap::new();
    for id in fake_models.keys() {
        models_config.insert(id.clone(), model_config("MINICORE_AGENT_TEST_KEY"));
    }
    let mut config = config(data_dir.to_path_buf(), models_config, profile);
    config.event_capacity = options.agent_event_capacity;
    config.loop_options.event_capacity = options.loop_event_capacity;
    Agent::open_with_models(config, Models::from_values(models))
        .await
        .unwrap()
}

fn workspace_file(label: &str, file: &str, content: &[u8]) -> (PathBuf, TestDirectoryGuard) {
    let (base, _) = fixture_dir(label);
    let root = base.join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    if let Some(parent) = Path::new(file).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(root.join(parent)).unwrap();
        }
    }
    std::fs::write(root.join(file), content).unwrap();
    (root, TestDirectoryGuard { path: base })
}

async fn create_session(agent: &mut Agent, workspace: &Path) -> crate::agent::SessionInfo {
    agent
        .create_session(CreateSession {
            workspace: workspace.to_path_buf(),
            profile: String::new(),
            model: None,
            reasoning: None,
            title: Some("test session".to_owned()),
        })
        .await
        .unwrap_or_else(|error| panic!("create_session failed: {error:?}"))
}

async fn send_text(agent: &mut Agent, session_id: crate::ids::SessionId, text: &str) -> TurnRef {
    agent
        .send(crate::agent::SendMessage {
            session_id,
            text: text.to_owned(),
        })
        .await
        .unwrap()
}

async fn wait_text(agent: &Agent, turn: TurnRef) -> Arc<crate::sessions::TurnResult> {
    agent.wait_turn(turn).await.unwrap()
}

async fn read_store_history(
    data_dir: &Path,
    session_id: crate::ids::SessionId,
) -> Vec<HistoryItem> {
    let store = Store::open(data_dir.to_path_buf()).await.unwrap();
    store
        .load_session(session_id)
        .await
        .unwrap()
        .history
        .to_vec()
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

fn next_id() -> usize {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[tokio::test]
async fn create_and_open_do_not_start_agent_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("open-no-loop-{}", next_id()));
    let (workspace, _guard) = workspace_file("open-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;
    assert!(agent.list_sessions().await.unwrap()[0].loaded);

    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
    assert!(state.active_loop.is_none());

    // Re-open from disk also starts no loop.
    let info2 = create_session(&mut agent, &workspace).await;
    agent.close_session(info2.session_id).await.unwrap();
    let reopened = agent.open_session(info.session_id).await.unwrap();
    let state = agent.session_state(reopened.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
}

#[tokio::test]
async fn single_active_loop_busy() {
    let (data_dir, _guard) = fixture_dir(&format!("busy-{}", next_id()));
    let (workspace, _guard) = workspace_file("busy-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "first")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let first = send_text(&mut agent, info.session_id, "first").await;
    let busy = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "second".to_owned(),
        })
        .await;
    assert!(matches!(busy, Err(AgentError::SessionBusy)));

    gate.entered.notified().await;
    gate.release.notify_waiters();
    let result = wait_text(&agent, first).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    agent.wait_turn(first).await.unwrap(); // already completed: no error
}

#[tokio::test]
async fn basic_completion_persists_and_merges_history() {
    let (data_dir, _guard) = fixture_dir(&format!("basic-{}", next_id()));
    let (workspace, _guard) = workspace_file("basic-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "hello agent").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    // The history in memory and the durable JSONL agree.
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 2);
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 2);
    let texts = flatten_texts(&stored);
    assert!(texts.contains(&"hello agent".to_owned()));
    assert!(texts.contains(&"done".to_owned()));

    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
}

fn flatten_texts(items: &[HistoryItem]) -> Vec<String> {
    let mut texts = Vec::new();
    for item in items {
        match item {
            HistoryItem::User(user) => texts.push(user.input.as_text().to_owned()),
            HistoryItem::Assistant(assistant) => {
                for part in &assistant.content {
                    if let minicore_runtime::model::AssistantPart::Text(text) = part {
                        texts.push(text.clone());
                    }
                }
            }
            _ => {}
        }
    }
    texts
}

#[tokio::test]
async fn tool_loop_runs_read_and_persists_one_loop_record() {
    let (data_dir, _guard) = fixture_dir(&format!("tool-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-ws", "file.txt", b"file contents");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            ModelScript::Text("final answer"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read the file").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(result.report.requests, 2);
    assert_eq!(result.report.tool_rounds, 1);

    let stored = read_store_history(&data_dir, info.session_id).await;
    // User, Assistant(tool call), ToolResult, Assistant(text)
    assert_eq!(stored.len(), 4);
    let tool_results = stored
        .iter()
        .filter_map(|item| match item {
            HistoryItem::ToolResult(result) => {
                Some((result.outcome, result.output.content().as_str().to_owned()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tool_results,
        vec![(ToolResultOutcome::Success, "1: file contents".to_owned())]
    );
}

#[tokio::test]
async fn project_prompt_reloads_agents_between_requests_in_one_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("dynamic-agents-{}", next_id()));
    let (workspace, _guard) = workspace_file("dynamic-agents-ws", "AGENTS.md", b"before\n");
    std::fs::write(workspace.join("a.txt"), b"file contents").unwrap();
    let gate = BlockGate::new();
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCallAfterGate(
                gate.clone(),
                "read",
                json!({"path": "a.txt", "limit": 32}),
            ),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;

    gate.entered.notified().await;
    std::fs::write(workspace.join("AGENTS.md"), b"after\n").unwrap();
    gate.release.notify_waiters();
    wait_text(&agent, turn).await;

    let requests = model.requests();
    let requests = requests.lock().unwrap();
    let systems = requests
        .iter()
        .map(|request| {
            request
                .messages()
                .iter()
                .find_map(|message| match message {
                    ModelMessage::System(text) => Some(text.clone()),
                    _ => None,
                })
                .expect("each request has a system message")
        })
        .collect::<Vec<_>>();
    assert_eq!(systems.len(), 2);
    assert!(systems[0].contains("before"));
    assert!(!systems[0].contains("after"));
    assert!(systems[1].contains("after"));
    assert!(!systems[1].contains("before"));
}

#[tokio::test]
async fn multi_turn_creates_new_loop_each_time_and_history_grows() {
    let (data_dir, _guard) = fixture_dir(&format!("multi-{}", next_id()));
    let (workspace, _guard) = workspace_file("multi-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::Text("one"),
            ModelScript::Text("two"),
            ModelScript::Text("three"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let mut loop_ids = Vec::new();
    for text in ["one", "two", "three"] {
        let turn = send_text(&mut agent, info.session_id, text).await;
        loop_ids.push(turn.loop_id);
        let result = wait_text(&agent, turn).await;
        assert_eq!(
            result.persistence,
            crate::sessions::TurnPersistence::Persisted
        );
    }
    assert_ne!(loop_ids[0], loop_ids[1]);
    assert_ne!(loop_ids[1], loop_ids[2]);

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    // Three user prompts + three assistant texts.
    assert_eq!(page.total, 6);
    assert!(page.next_offset.is_none());
}

#[tokio::test]
async fn cancel_turn_returns_cancelled_report_and_session_recovers() {
    let (data_dir, _guard) = fixture_dir(&format!("cancel-{}", next_id()));
    let (workspace, _guard) = workspace_file("cancel-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model = FakeModel::new(
        "main",
        [
            ModelScript::BlockUntil(gate.clone(), "blocked"),
            ModelScript::Text("after cancel"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "cancel me").await;

    // Wait until the first model call is actually parked before cancelling,
    // so the first script is consumed and the second turn gets `Text`.
    gate.entered.notified().await;
    assert!(agent.cancel(turn).unwrap());
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    // The session can run a new loop after cleanup.
    let turn = send_text(&mut agent, info.session_id, "next").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
}

#[tokio::test]
async fn close_active_session_cancels_and_waits() {
    let (data_dir, _guard) = fixture_dir(&format!("close-{}", next_id()));
    let (workspace, _guard) = workspace_file("close-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "blocked")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let _turn = send_text(&mut agent, info.session_id, "do work").await;

    agent.close_session(info.session_id).await.unwrap();
    gate.release.notify_waiters();
    // No orphan task: reopening and running a fresh loop keeps working.
    agent.open_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn panicked_agent_worker_publishes_internal_and_blocks_session() {
    let (data_dir, _guard) = fixture_dir(&format!("worker-panic-{}", next_id()));
    let (workspace, _guard) = workspace_file("worker-panic-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("unused")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    panic_next_worker(info.session_id);

    let turn = send_text(&mut agent, info.session_id, "panic").await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("worker panic must complete the waiter");
    assert!(matches!(result, Err(AgentError::Internal)));

    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Blocked);
    assert_eq!(
        state.block_reason,
        Some(crate::sessions::SessionBlockReason::Internal)
    );
    assert!(state.active_loop.is_none());

    let error = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "after panic".to_owned(),
        })
        .await;
    assert!(matches!(error, Err(AgentError::SessionBlocked)));
    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Blocked);
    let close = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        agent.close_session(info.session_id),
    )
    .await
    .expect("close must reclaim a panicked worker");
    assert!(matches!(close, Err(AgentError::Internal)));
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn aborted_agent_worker_wait_close_and_shutdown_reclaim_without_busy_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("worker-abort-{}", next_id()));
    let (workspace, _guard) = workspace_file("worker-abort-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("unused")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "abort").await;
    // Abort immediately, including the case where the worker has not polled
    // yet. CompletionGuard is captured before spawn and still publishes the
    // internal failure when the future is dropped.
    agent.abort_active_task_for_test(info.session_id).unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("worker abort must complete the waiter");
    assert!(matches!(result, Err(AgentError::Internal)));

    let close = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        agent.close_session(info.session_id),
    )
    .await
    .expect("close must reclaim an aborted worker");
    assert!(matches!(close, Err(AgentError::Internal)));
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn unobserved_event_stream_still_persists() {
    let (data_dir, _guard) = fixture_dir(&format!("noevents-{}", next_id()));
    let (workspace, _guard) = workspace_file("noevents-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    // Do not take_events at all.
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 2);
}

#[tokio::test]
async fn next_send_cleans_up_finished_turn_without_explicit_wait() {
    let (data_dir, _guard) = fixture_dir(&format!("cleanup-{}", next_id()));
    let (workspace, _guard) = workspace_file("cleanup-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("one"), ModelScript::Text("two")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "one").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let state = agent.session_state(info.session_id).unwrap();
        if state.status == crate::sessions::SessionStatus::Idle {
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "first turn did not reach Idle; final status: {:?}",
                state.status
            );
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(10))).await;
    }
    // Send again after Agent-level completion, without waiting for the first turn.
    let second = send_text(&mut agent, info.session_id, "two").await;
    assert_ne!(turn.loop_id, second.loop_id);
    let result = wait_text(&agent, second).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
}

#[tokio::test]
async fn append_failure_blocks_session_and_returns_failed_persistence() {
    let (data_dir, _guard) = fixture_dir(&format!("blocked-{}", next_id()));
    let (workspace, _guard) = workspace_file("blocked-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    fail_next_append(info.session_id);

    let turn = send_text(&mut agent, info.session_id, "will fail").await;
    let result = wait_text(&agent, turn).await;
    // The report is still fully returned.
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    // History was not merged into memory.
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 0);

    // Session is blocked: further sends and updates are refused.
    let state = agent.session_state(info.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Blocked);
    assert_eq!(
        state.block_reason,
        Some(crate::sessions::SessionBlockReason::Persistence)
    );
    let busy = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "after".to_owned(),
        })
        .await;
    assert!(matches!(busy, Err(AgentError::SessionBlocked)));
}

#[tokio::test]
async fn blocked_send_does_not_discard_previous_turn_result() {
    let (data_dir, _guard) = fixture_dir(&format!("blocked-discard-{}", next_id()));
    let (workspace, _guard) = workspace_file("blocked-discard-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("persist me")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    fail_next_append(info.session_id);

    let turn = send_text(&mut agent, info.session_id, "first turn").await;

    // Wait deterministically for the session to transition to Blocked.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let state = agent.session_state(info.session_id).unwrap();
        if state.status == crate::sessions::SessionStatus::Blocked {
            assert_eq!(
                state.block_reason,
                Some(crate::sessions::SessionBlockReason::Persistence)
            );
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "session did not reach Blocked; final status: {:?}",
                state.status
            );
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(10))).await;
    }

    // A second send fails with SessionBlocked.
    let second_send = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "second turn".to_owned(),
        })
        .await;
    assert!(matches!(second_send, Err(AgentError::SessionBlocked)));

    // First turn.wait still resolves with the authoritative runtime outcome and persistence failure.
    let first_result = agent
        .wait_turn(turn)
        .await
        .expect("previous turn wait must succeed even after blocked send");
    assert_eq!(
        first_result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        first_result.persistence,
        crate::sessions::TurnPersistence::Failed
    );

    // Repeated wait returns the same result.
    let second_wait = agent
        .wait_turn(turn)
        .await
        .expect("repeated wait on blocked turn must succeed");
    assert_eq!(
        second_wait.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        second_wait.persistence,
        crate::sessions::TurnPersistence::Failed
    );
    assert_eq!(second_wait.turn, turn);

    // Close reclaims the active task cleanly.
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn close_and_reopen_recovers_from_blocked_session() {
    let (data_dir, _guard) = fixture_dir(&format!("recover-{}", next_id()));
    let (workspace, _guard) = workspace_file("recover-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    fail_next_append(info.session_id);
    let turn = send_text(&mut agent, info.session_id, "fail").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);

    agent.close_session(info.session_id).await.unwrap();
    let opened = agent.open_session(info.session_id).await.unwrap();
    let state = agent.session_state(opened.session_id).unwrap();
    assert_eq!(state.status, crate::sessions::SessionStatus::Idle);
    assert!(state.block_reason.is_none());

    let turn = send_text(&mut agent, info.session_id, "after recovery").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
}

#[tokio::test]
async fn metadata_write_failure_does_not_fail_the_turn() {
    let (data_dir, _guard) = fixture_dir(&format!("metadata-{}", next_id()));
    let (workspace, _guard) = workspace_file("metadata-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    fail_next_record_write(info.session_id);

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    // History merged even though session.json updated_at write failed.
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 2);
}

#[tokio::test]
async fn steer_is_applied_and_persisted_in_order() {
    let (data_dir, _guard) = fixture_dir(&format!("steer-{}", next_id()));
    let (workspace, _guard) = workspace_file("steer-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "final")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "main prompt").await;

    agent
        .steer(crate::agent::SteerMessage {
            turn,
            text: "steer one".to_owned(),
        })
        .unwrap();
    agent
        .steer(crate::agent::SteerMessage {
            turn,
            text: "steer two".to_owned(),
        })
        .unwrap();
    gate.entered.notified().await;
    gate.release.notify_waiters();

    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let stored = read_store_history(&data_dir, info.session_id).await;
    let kinds = stored
        .iter()
        .filter_map(|item| match item {
            HistoryItem::User(user) => Some((user.input.as_text().to_owned(), user.kind)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            ("main prompt".to_owned(), UserMessageKind::Prompt),
            ("steer one".to_owned(), UserMessageKind::Steering),
            ("steer two".to_owned(), UserMessageKind::Steering),
        ]
    );
}

#[tokio::test]
async fn stale_turn_ref_is_rejected() {
    let (data_dir, _guard) = fixture_dir(&format!("stale-{}", next_id()));
    let (workspace, _guard) = workspace_file("stale-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("one"), ModelScript::Text("two")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let first = send_text(&mut agent, info.session_id, "one").await;
    wait_text(&agent, first).await;
    let second = send_text(&mut agent, info.session_id, "two").await;

    let stale = TurnRef {
        session_id: info.session_id,
        loop_id: first.loop_id,
    };
    assert!(matches!(
        agent.steer(crate::agent::SteerMessage {
            turn: stale,
            text: "x".to_owned(),
        }),
        Err(AgentError::TurnNotFound)
    ));
    assert!(matches!(agent.cancel(stale), Err(AgentError::TurnNotFound)));
    let _ = second;
}

#[tokio::test]
async fn idle_model_update_affects_next_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("idle-update-{}", next_id()));
    let (workspace, _guard) = workspace_file("idle-update-ws", "a.txt", b"hello");
    let model_a = FakeModel::new("main", [ModelScript::Text("from a")]);
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let updated = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    let SessionUpdateResult {
        active_revision,
        session,
    } = updated;
    assert!(active_revision.is_none());
    assert_eq!(session.model, "other");

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&stored);
    assert!(texts.contains(&"from b".to_owned()));
    let requests = model_b.requests();
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn running_model_update_keeps_current_request_and_reports_revision() {
    let (data_dir, _guard) = fixture_dir(&format!("running-update-{}", next_id()));
    let (workspace, _guard) = workspace_file("running-update-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model_a = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "from a")]);
    let model_b = FakeModel::new("other", [ModelScript::Text("from b")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;

    let updated = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_some());

    gate.entered.notified().await;
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    // The current (unblocked) request used model A.
    let requests_a = model_a.requests();
    assert_eq!(requests_a.lock().unwrap().len(), 1);
    // Next turn uses the new model.
    let turn = send_text(&mut agent, info.session_id, "next").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let requests_b = model_b.requests();
    assert_eq!(requests_b.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn running_model_update_applies_to_next_request_in_same_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("same-loop-update-{}", next_id()));
    let (workspace, _guard) = workspace_file("same-loop-update-ws", "a.txt", b"hello from a.txt");
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "main",
        [ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({"path": "a.txt", "limit": 32}),
        )],
    );
    let model_b = FakeModel::new("other", [ModelScript::Text("from model b")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read then answer").await;

    gate.entered.notified().await;

    let updated = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    let active_revision = updated
        .active_revision
        .expect("running loop must accept update");

    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;

    // Single turn / loop identity preserved.
    assert_eq!(result.turn.loop_id, turn.loop_id);
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(result.report.requests, 2);
    assert_eq!(result.report.tool_rounds, 1);
    assert_eq!(result.report.final_config_revision, active_revision);
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    // Both models were called exactly once in this single loop.
    assert_eq!(model_a.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model_b.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);

    // History contains:
    // [0] User message
    // [1] Assistant from Model A with read tool call (request_index = 0)
    // [2] ToolResult from read tool
    // [3] Final Assistant from Model B with text (request_index = 1)
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 4);
    assert!(matches!(&stored[0], HistoryItem::User(_)));

    let HistoryItem::Assistant(ref assistant_a) = stored[1] else {
        panic!("expected Assistant history item at index 1");
    };
    assert_eq!(assistant_a.model.as_str(), "main");
    assert_eq!(assistant_a.request_index, 0);
    let has_tool_call = assistant_a.content.iter().any(|part| {
        matches!(
            part,
            minicore_runtime::model::AssistantPart::ToolCall(call) if call.name().as_str() == "read"
        )
    });
    assert!(
        has_tool_call,
        "Request 0 must produce read tool call from Model A"
    );

    let HistoryItem::ToolResult(ref tool_result) = stored[2] else {
        panic!("expected ToolResult history item at index 2");
    };
    assert_eq!(tool_result.outcome, ToolResultOutcome::Success);

    let HistoryItem::Assistant(ref assistant_b) = stored[3] else {
        panic!("expected Assistant history item at index 3");
    };
    assert_eq!(assistant_b.model.as_str(), "other");
    assert_eq!(assistant_b.request_index, 1);
    let has_final_text = assistant_b.content.iter().any(|part| {
        matches!(
            part,
            minicore_runtime::model::AssistantPart::Text(text) if text == "from model b"
        )
    });
    assert!(
        has_final_text,
        "Request 1 must produce final text from Model B"
    );

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 4);
    let crate::history::HistoryItemView::Assistant(ref final_view) = page.items[3].item else {
        panic!("expected AssistantHistoryView at index 3");
    };
    assert_eq!(final_view.model, "other");
    assert_eq!(final_view.request_index, 1);
    assert_eq!(final_view.text, "from model b");

    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_update_does_not_write_or_change_memory() {
    let (data_dir, _guard) = fixture_dir(&format!("invalid-update-{}", next_id()));
    let (workspace, _guard) = workspace_file("invalid-update-ws", "a.txt", b"hello");
    let model_a = FakeModel::new("main", [ModelScript::Text("from a")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model_a))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let error = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("missing".to_owned()),
            reasoning: None,
        })
        .await;
    assert!(matches!(error, Err(AgentError::ModelNotFound)));

    let error = agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: None,
            reasoning: None,
        })
        .await;
    assert!(matches!(error, Err(AgentError::InvalidArguments)));

    // Disk record still references the original model.
    let info2 = agent.list_sessions().await.unwrap();
    assert_eq!(info2[0].model, "main");
}

#[tokio::test]
async fn history_view_never_exposes_tool_arguments_or_opaque_reasoning() {
    let (data_dir, _guard) = fixture_dir(&format!("view-{}", next_id()));
    let (workspace, _guard) = workspace_file("view-ws", "file.txt", b"contents");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "file.txt", "limit": 32})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let serialized = serde_json::to_string(&page).unwrap();
    assert!(!serialized.contains("file.txt"));
    assert!(!serialized.contains("arguments"));
    // Safe fields remain visible.
    assert!(serialized.contains("read"));
    assert!(serialized.contains("contents"));
}

#[tokio::test]
async fn two_sessions_write_independently() {
    let (data_dir, _guard) = fixture_dir(&format!("multi-session-{}", next_id()));
    let (workspace_a, _ga) = workspace_file("multi-session-a", "a.txt", b"a");
    let model = FakeModel::new("main", [ModelScript::Text("a1"), ModelScript::Text("a2")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let session_x = create_session(&mut agent, &workspace_a).await;
    let (workspace_b, _gb) = workspace_file("multi-session-b", "b.txt", b"b");
    let session_y = create_session(&mut agent, &workspace_b).await;

    let turn_x = send_text(&mut agent, session_x.session_id, "x").await;
    let turn_y = send_text(&mut agent, session_y.session_id, "y").await;
    let rx = wait_text(&agent, turn_x).await;
    let ry = wait_text(&agent, turn_y).await;
    assert_eq!(rx.persistence, crate::sessions::TurnPersistence::Persisted);
    assert_eq!(ry.persistence, crate::sessions::TurnPersistence::Persisted);
    let hx = agent
        .history(GetHistory {
            session_id: session_x.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let hy = agent
        .history(GetHistory {
            session_id: session_y.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(hx.total, 2);
    assert_eq!(hy.total, 2);
}

#[tokio::test]
async fn send_is_rejected_for_unloaded_session() {
    let (data_dir, _guard) = fixture_dir(&format!("unloaded-{}", next_id()));
    let (workspace, _guard) = workspace_file("unloaded-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();

    let error = agent
        .send(crate::agent::SendMessage {
            session_id: info.session_id,
            text: "hello".to_owned(),
        })
        .await;
    assert!(matches!(error, Err(AgentError::SessionNotLoaded)));
}

#[tokio::test]
async fn delete_rejects_loaded_session_and_removes_unloaded() {
    let (data_dir, _guard) = fixture_dir(&format!("delete-{}", next_id()));
    let (workspace, _guard) = workspace_file("delete-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    assert!(matches!(
        agent.delete_session(info.session_id).await,
        Err(AgentError::SessionAlreadyLoaded)
    ));
    agent.close_session(info.session_id).await.unwrap();
    agent.delete_session(info.session_id).await.unwrap();
    assert!(agent.list_sessions().await.unwrap().is_empty());
}

#[tokio::test]
async fn persisted_history_after_failed_loop_is_still_durable() {
    let (data_dir, _guard) = fixture_dir(&format!("failed-{}", next_id()));
    let (workspace, _guard) = workspace_file("failed-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "partial")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;
    gate.entered.notified().await;
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert!(!stored.is_empty());
}

#[tokio::test]
async fn event_stream_delivers_lifecycle_events() {
    let (data_dir, _guard) = fixture_dir(&format!("events-{}", next_id()));
    let (workspace, _guard) = workspace_file("events-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("final")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "hello").await;

    // Drain the agent event stream concurrently with the turn so the
    // best-effort event queue never overflows.
    let drain = tokio::spawn(async move {
        let mut seen_turn_started = false;
        let mut seen_request_started = false;
        let mut seen_output = false;
        let mut seen_finished = false;
        loop {
            let Some(event) = events.recv().await else {
                break;
            };
            match event {
                AgentEvent::TurnStarted {
                    turn: event_turn, ..
                } => {
                    assert_eq!(event_turn, turn);
                    seen_turn_started = true;
                }
                AgentEvent::RequestStarted { .. } => seen_request_started = true,
                AgentEvent::OutputDelta {
                    channel: OutputChannel::Text,
                    ..
                } => seen_output = true,
                AgentEvent::TurnFinished { .. } => {
                    seen_finished = true;
                    break;
                }
                _ => {}
            }
        }
        (
            seen_turn_started,
            seen_request_started,
            seen_output,
            seen_finished,
        )
    });

    let result = agent.wait_turn(turn).await.unwrap();
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    let (seen_turn_started, seen_request_started, seen_output, seen_finished) =
        drain.await.unwrap();
    assert!(seen_turn_started);
    assert!(seen_request_started);
    assert!(seen_output);
    assert!(seen_finished);
}

#[tokio::test]
async fn join_first_drains_queued_finished_without_duplicate_agent_completion() {
    let (data_dir, _guard) = fixture_dir(&format!("join-first-{}", next_id()));
    let (workspace, _guard) = workspace_file("join-first-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent_with(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
        AgentOptions {
            agent_event_capacity: 256,
            loop_event_capacity: Some(32),
        },
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(WorkerGate::new());
    pause_next_worker_before_join(info.session_id, Arc::clone(&gate));
    let turn = send_text(&mut agent, info.session_id, "join first").await;

    gate.wait_started().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if agent
                .runtime_loop_finished_for_test(info.session_id)
                .unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime must finish while the Agent worker is paused");

    let drain = tokio::spawn(async move {
        let mut turn_finished = 0;
        while let Some(event) = events.recv().await {
            if matches!(event, AgentEvent::TurnFinished { .. }) {
                turn_finished += 1;
            }
        }
        turn_finished
    });
    gate.release();

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("join-first drain must not wait for channel closure")
        .unwrap();
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    agent.shutdown().await.unwrap();
    assert_eq!(drain.await.unwrap(), 1);
}

#[tokio::test]
async fn queued_finished_drops_are_accounted_without_runtime_completion_mapping() {
    let (data_dir, _guard) = fixture_dir(&format!("finished-drops-{}", next_id()));
    let (workspace, _guard) = workspace_file("finished-drops-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("unused")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let _ = events.recv().await;
    let loop_id = minicore_runtime::LoopId::new().unwrap();

    agent
        .forward_runtime_event_for_test(
            info.session_id,
            minicore_runtime::LoopEventEnvelope {
                dropped_before: 7,
                event: minicore_runtime::LoopEvent::Finished {
                    loop_id,
                    outcome: minicore_runtime::LoopOutcomeSummary::Completed,
                },
            },
        )
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), events.recv(),)
            .await
            .is_err()
    );

    let turn = TurnRef {
        session_id: info.session_id,
        loop_id,
    };
    agent.event_sink.try_send(AgentEvent::TurnFinished {
        turn,
        outcome: crate::event::LoopOutcomeView::Completed,
        persistence: crate::sessions::TurnPersistence::Persisted,
        meta: crate::event::EventMeta {
            session_id: turn.session_id,
            loop_id: Some(turn.loop_id),
            dropped_before: 0,
        },
    });
    let Some(AgentEvent::TurnFinished { meta, .. }) = events.recv().await else {
        panic!("expected the Agent completion event");
    };
    assert_eq!(meta.dropped_before, 7);
}

#[tokio::test]
async fn loop_event_drops_surface_in_agent_events_but_turn_result_is_authoritative() {
    let (data_dir, _guard) = fixture_dir(&format!("loop-drops-{}", next_id()));
    let (workspace, _guard) = workspace_file("loop-drops-ws", "a.txt", b"hello");
    // A back-to-back delta burst saturates the runtime's best-effort loop
    // event channel (capacity 1) during the runner's synchronous tail, so the
    // runtime deterministically drops events and reports them on the next
    // delivered envelope.
    let model = FakeModel::new(
        "main",
        [
            ModelScript::BurstTextThenTool("x", 300, "read", json!({"path": "a.txt", "limit": 32})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent_with(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
        AgentOptions {
            agent_event_capacity: 256,
            loop_event_capacity: Some(1),
        },
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "burst").await;

    let drain = tokio::spawn(async move {
        let mut saw_drops = false;
        let mut saw_tool_finished = false;
        loop {
            let Some(event) = events.recv().await else {
                break;
            };
            let AgentEvent::TurnFinished { .. } = &event else {
                let meta = event.meta();
                if meta.dropped_before > 0 {
                    saw_drops = true;
                }
                if let AgentEvent::ToolFinished { .. } = &event {
                    saw_tool_finished = true;
                }
                continue;
            };
            break;
        }
        (saw_drops, saw_tool_finished)
    });

    let result = agent.wait_turn(turn).await.unwrap();
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    // The rejected loop events are accounted for on a delivered Agent event
    // (here the tool round's `tool_finished`) instead of vanishing silently.
    let (saw_drops, _saw_tool_finished) = drain.await.unwrap();
    assert!(
        saw_drops,
        "runtime loop drops must surface as dropped_before on an Agent event"
    );
    // user + assistant(tool call) + tool result + final assistant text
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 4);
}

#[tokio::test]
async fn full_agent_event_channel_keeps_wait_and_history_authoritative() {
    let (data_dir, _guard) = fixture_dir(&format!("full-channel-{}", next_id()));
    let (workspace, _guard) = workspace_file("full-channel-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("one"), ModelScript::Text("two")]);
    let mut agent = open_agent_with(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
        AgentOptions {
            agent_event_capacity: 1,
            loop_event_capacity: None,
        },
    )
    .await;
    // Never read the event stream: the Agent event channel stays full and
    // best-effort events (including a dropped TurnFinished) must not block or
    // disturb the authoritative result.
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "first").await;
    let result = agent.wait_turn(turn).await.unwrap();
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 2);
    let stored = read_store_history(&data_dir, info.session_id).await;
    assert_eq!(stored.len(), 2);
}

#[tokio::test]
async fn immediate_send_after_completion_does_not_report_busy() {
    let (data_dir, _guard) = fixture_dir(&format!("reopen-race-{}", next_id()));
    let (workspace, _guard) = workspace_file("reopen-race-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::Text("one"),
            ModelScript::Text("two"),
            ModelScript::Text("three"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    // Once a turn's agent-level completion is published, an immediate next
    // send must never observe a SessionBusy window.
    for expected in ["one", "two", "three"] {
        let turn = send_text(&mut agent, info.session_id, expected).await;
        let result = agent.wait_turn(turn).await.unwrap();
        assert_eq!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Completed
        );
        assert_eq!(
            result.persistence,
            crate::sessions::TurnPersistence::Persisted
        );
    }
    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(page.total, 6);
}

#[tokio::test]
async fn reopen_rejects_missing_workspace_and_keeps_type_identity() {
    let (data_dir, _guard) = fixture_dir(&format!("ws-identity-{}", next_id()));
    let (workspace, _guard) = workspace_file("ws-identity-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();

    // Deleting the workspace makes reopen fail cleanly instead of running
    // against a different root.
    std::fs::remove_dir_all(&workspace).unwrap();
    assert!(matches!(
        agent.open_session(info.session_id).await,
        Err(AgentError::Workspace)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn reopen_rejects_workspace_redirected_by_symlink() {
    use std::os::unix::fs::symlink;

    let (data_dir, _guard) = fixture_dir(&format!("ws-symlink-{}", next_id()));
    let (workspace, _guard) = workspace_file("ws-symlink-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();
    let canonical = workspace.canonicalize().unwrap();

    // Move the real directory and put a symlink at its former path that
    // redirects elsewhere: reopen must refuse to continue on the wrong root.
    let other = workspace.with_extension("moved");
    std::fs::rename(&canonical, &other).unwrap();
    symlink(&other, &workspace).unwrap();
    assert!(matches!(
        agent.open_session(info.session_id).await,
        Err(AgentError::Workspace)
    ));
}

#[tokio::test]
async fn reopen_after_profile_removal_still_works() {
    let (data_dir, _guard) = fixture_dir(&format!("ws-profile-{}", next_id()));
    let (workspace, _guard) = workspace_file("ws-profile-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();
    drop(agent);

    // A second agent whose config no longer has the "test" profile still
    // reopens the session, because the session record carries its own frozen
    // settings.
    let second_profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "other profile".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 4,
        approval: ApprovalMode::Auto,
    };
    let models = BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]);
    let models_config =
        BTreeMap::from([("main".to_owned(), model_config("MINICORE_AGENT_TEST_KEY"))]);
    let mut second_config = config(
        data_dir.to_path_buf(),
        models_config,
        second_profile.clone(),
    );
    // The second agent has no "test" profile at all.
    second_config.default_profile = "other".to_owned();
    second_config.profiles = BTreeMap::from([("other".to_owned(), second_profile)]);
    let second_models = models
        .iter()
        .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
        .collect();
    let mut second = Agent::open_with_models(second_config, Models::from_values(second_models))
        .await
        .unwrap();
    let reopened = second.open_session(info.session_id).await.unwrap();
    assert!(reopened.loaded);
    assert_eq!(reopened.session_id, info.session_id);
}

#[tokio::test]
async fn invalid_agents_md_fails_the_turn_as_a_prompt_error() {
    let (data_dir, _guard) = fixture_dir(&format!("agents-invalid-{}", next_id()));
    let (workspace, _guard) = workspace_file("agents-invalid-ws", "AGENTS.md", b"\xff\xfe");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let result = agent.wait_turn(turn).await.unwrap();
    match &result.report.outcome {
        minicore_runtime::LoopOutcome::Failed(failure) => {
            assert_eq!(failure.kind, minicore_runtime::LoopFailureKind::Prompt)
        }
        other => panic!("expected a prompt failure, got {other:?}"),
    }
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );
}

#[tokio::test]
async fn invalid_session_json_skipped_by_list_and_fails_open() {
    let (data_dir, _guard) = fixture_dir(&format!("invalid-record-{}", next_id()));
    let (workspace, _guard) = workspace_file("invalid-record-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();

    let record_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("session.json");
    let raw = std::fs::read_to_string(&record_path).unwrap();
    let mut parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
    parsed["system_prompt"] = json!("");
    let updated = serde_json::to_string(&parsed).unwrap();
    std::fs::write(&record_path, updated).unwrap();

    let listed = agent.list_sessions().await.unwrap();
    assert!(listed.is_empty());
    assert!(matches!(
        agent.open_session(info.session_id).await,
        Err(AgentError::Store)
    ));

    agent.shutdown().await.unwrap();
}
