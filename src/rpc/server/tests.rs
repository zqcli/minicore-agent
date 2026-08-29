use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::pending;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode};
use minicore_runtime::ids::{SessionId, ToolCallId};
use minicore_runtime::model::{
    DeliveryState, Model, ModelCallContext, ModelDescriptor, ModelError, ModelErrorKind,
    ModelEvent, ModelFinishReason, ModelRef, ModelRequest, ModelStartFuture, ModelStream,
    ReasoningPreference, Usage,
};
use minicore_runtime::value::BoundedText;

use crate::agent::Agent;
use crate::config::{AgentConfig, KernelOverrides, Profile};
use crate::error::{AgentError, CoreErrorView};
use crate::models::{ModelConfig, Models};
use crate::profiles::{ApprovalMode, ProfileCompaction};

use super::{agent_error, run_with_io, turn_wait_error};

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum ModelScript {
    Text(&'static str),
    ToolCalls(Vec<ToolCallScript>),
    Block,
    Error(&'static str),
}

#[derive(Clone)]
struct ToolCallScript {
    name: &'static str,
    arguments: Value,
}

impl ModelScript {
    fn tool_call(name: &'static str, arguments: Value) -> Self {
        Self::ToolCalls(vec![ToolCallScript { name, arguments }])
    }
}

struct FakeModel {
    descriptor: ModelDescriptor,
    scripts: Mutex<VecDeque<ModelScript>>,
    calls: AtomicUsize,
}

impl FakeModel {
    fn new(scripts: impl IntoIterator<Item = ModelScript>) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ModelDescriptor::new(
                "fake".parse::<ModelRef>().unwrap(),
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
            .unwrap(),
            scripts: Mutex::new(scripts.into_iter().collect()),
            calls: AtomicUsize::new(0),
        })
    }
}

impl Model for FakeModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start<'a>(
        &'a self,
        _request: ModelRequest,
        _context: ModelCallContext,
    ) -> ModelStartFuture<'a> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ModelScript::Text("default"));
        Box::pin(async move {
            match script {
                ModelScript::Text(text) => Ok(model_events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ])),
                ModelScript::ToolCalls(calls) => {
                    let mut events = Vec::new();
                    for (index, call) in calls.into_iter().enumerate() {
                        let tool_call_id =
                            ToolCallId::new(format!("{}-rpc-{call_index}-{index}", call.name))
                                .unwrap();
                        events.push(ModelEvent::ToolCallStart {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: call.name.parse().unwrap(),
                        });
                        events.push(
                            ModelEvent::tool_call_arguments_delta(
                                tool_call_id.clone(),
                                serde_json::to_string(&call.arguments).unwrap(),
                            )
                            .unwrap(),
                        );
                        events.push(ModelEvent::ToolCallEnd { tool_call_id });
                    }
                    events.push(ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    });
                    events.push(ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    });
                    Ok(model_events(events))
                }
                ModelScript::Block => {
                    let _: Result<ModelStream, ModelError> = pending().await;
                    unreachable!()
                }
                ModelScript::Error(message) => Err(ModelError::permanent(
                    ModelErrorKind::ProviderUnavailable,
                    DeliveryState::Started,
                    minicore_runtime::error::DiagnosticSummary::new(
                        DiagnosticCode::ModelUnavailable,
                        DiagnosticCategory::Model,
                        BoundedText::new(message).unwrap(),
                        false,
                    ),
                )),
            }
        })
    }
}

fn model_events(events: Vec<ModelEvent>) -> ModelStream {
    Box::pin(stream::iter(events.into_iter().map(Ok)))
}

fn test_config(data_dir: PathBuf, tools: &[&str], approval: ApprovalMode) -> AgentConfig {
    AgentConfig {
        data_dir,
        event_capacity: 256,
        default_profile: "test".to_owned(),
        profiles: BTreeMap::from([(
            "test".to_owned(),
            Profile {
                model: "fake".to_owned(),
                reasoning: ReasoningPreference::Medium,
                system_prompt: "RPC test system prompt".to_owned(),
                tools: tools.iter().map(|name| (*name).to_owned()).collect(),
                max_tool_rounds: 8,
                approval,
                compaction: ProfileCompaction::Disabled,
            },
        )]),
        models: BTreeMap::new(),
        kernel: KernelOverrides::default(),
    }
}

fn test_models(model: Arc<FakeModel>) -> Models {
    let model: Arc<dyn Model> = model;
    Models::from_values(BTreeMap::from([("fake".to_owned(), model)]))
}

async fn test_agent(
    label: &str,
    scripts: impl IntoIterator<Item = ModelScript>,
    tools: &[&str],
    approval: ApprovalMode,
) -> (Agent, PathBuf, PathBuf) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-{label}-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = FakeModel::new(scripts);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), tools, approval),
        test_models(model),
    )
    .await
    .unwrap();
    (agent, base, workspace)
}

struct RpcHarness {
    input: Option<DuplexStream>,
    output: BufReader<DuplexStream>,
    task: Option<JoinHandle<Result<(), AgentError>>>,
    pending: VecDeque<Value>,
    events: Vec<Value>,
    observed: Vec<Value>,
}

impl RpcHarness {
    fn spawn(agent: Agent) -> Self {
        let (client_input, server_input) = tokio::io::duplex(2 * 1024 * 1024);
        let (server_output, client_output) = tokio::io::duplex(2 * 1024 * 1024);
        let task = tokio::spawn(run_with_io(
            agent,
            BufReader::new(server_input),
            server_output,
            pending::<io::Result<()>>(),
        ));
        Self {
            input: Some(client_input),
            output: BufReader::new(client_output),
            task: Some(task),
            pending: VecDeque::new(),
            events: Vec::new(),
            observed: Vec::new(),
        }
    }

    fn spawn_with_switch_writer(agent: Agent, fail: Arc<AtomicBool>) -> Self {
        let (client_input, server_input) = tokio::io::duplex(2 * 1024 * 1024);
        let (server_output, client_output) = tokio::io::duplex(2 * 1024 * 1024);
        let task = tokio::spawn(run_with_io(
            agent,
            BufReader::new(server_input),
            SwitchWriter {
                inner: server_output,
                fail,
            },
            pending::<io::Result<()>>(),
        ));
        Self {
            input: Some(client_input),
            output: BufReader::new(client_output),
            task: Some(task),
            pending: VecDeque::new(),
            events: Vec::new(),
            observed: Vec::new(),
        }
    }

    fn spawn_with_signal(agent: Agent) -> (Self, oneshot::Sender<()>) {
        let (client_input, server_input) = tokio::io::duplex(2 * 1024 * 1024);
        let (server_output, client_output) = tokio::io::duplex(2 * 1024 * 1024);
        let (signal_tx, signal_rx) = oneshot::channel();
        let task = tokio::spawn(run_with_io(
            agent,
            BufReader::new(server_input),
            server_output,
            async move {
                signal_rx.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::Interrupted, "test signal sender dropped")
                })
            },
        ));
        (
            Self {
                input: Some(client_input),
                output: BufReader::new(client_output),
                task: Some(task),
                pending: VecDeque::new(),
                events: Vec::new(),
                observed: Vec::new(),
            },
            signal_tx,
        )
    }

    async fn send(&mut self, id: Value, method: &str, params: Option<Value>) {
        let mut request = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if let Some(params) = params {
            request["params"] = params;
        }
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        self.input
            .as_mut()
            .unwrap()
            .write_all(&encoded)
            .await
            .unwrap();
    }

    async fn response(&mut self, id: Value) -> Value {
        if let Some(index) = self
            .pending
            .iter()
            .position(|frame| frame.get("id") == Some(&id))
        {
            return self.pending.remove(index).unwrap();
        }
        loop {
            let frame = self.next_frame().await.expect("RPC output ended");
            if frame.get("method") == Some(&json!("agent.event")) {
                self.events.push(frame);
            } else if frame.get("id") == Some(&id) {
                return frame;
            } else {
                self.pending.push_back(frame);
            }
        }
    }

    async fn event(&mut self, event_type: &str) -> Value {
        if let Some(index) = self.events.iter().position(|frame| {
            frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type)
        }) {
            return self.events.remove(index);
        }
        loop {
            let frame = self.next_frame().await.expect("RPC output ended");
            if frame.get("method") == Some(&json!("agent.event")) {
                if frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type) {
                    return frame;
                }
                self.events.push(frame);
            } else {
                self.pending.push_back(frame);
            }
        }
    }

    async fn next_frame(&mut self) -> Option<Value> {
        let mut line = String::new();
        let read = tokio::time::timeout(TIMEOUT, self.output.read_line(&mut line))
            .await
            .expect("RPC output timed out")
            .unwrap();
        if read == 0 {
            return None;
        }
        assert!(line.ends_with('\n'));
        let value: Value = serde_json::from_str(&line).unwrap();
        self.observed.push(value.clone());
        Some(value)
    }

    fn close_input(&mut self) {
        drop(self.input.take());
    }

    async fn join(&mut self) -> Result<(), AgentError> {
        tokio::time::timeout(TIMEOUT, self.task.take().unwrap())
            .await
            .expect("RPC server task leaked")
            .expect("RPC server task panicked")
    }

    async fn shutdown(&mut self) {
        self.send(json!("shutdown"), "agent.shutdown", Some(json!({})))
            .await;
        let response = self.response(json!("shutdown")).await;
        assert_eq!(response["result"], json!({"ok": true}));
        assert!(self.next_frame().await.is_none());
        self.join().await.unwrap();
    }
}

struct SwitchWriter<W> {
    inner: W,
    fail: Arc<AtomicBool>,
}

impl<W> AsyncWrite for SwitchWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.fail.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected writer failure",
            )))
        } else {
            Pin::new(&mut this.inner).poll_write(context, buffer)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.fail.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected writer failure",
            )))
        } else {
            Pin::new(&mut this.inner).poll_flush(context)
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

fn session_id(response: &Value) -> Value {
    response
        .pointer("/result/session/session_id")
        .unwrap()
        .clone()
}

fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "instance_id": turn["instance_id"],
        "turn_id": turn["turn_id"],
    })
}

fn assert_error(response: &Value, code: i64, kind: &str) {
    assert_eq!(response["error"]["code"], json!(code));
    assert_eq!(response["error"]["data"]["kind"], json!(kind));
    assert!(response["error"]["data"]["retryable"].is_boolean());
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key(key) || object.values().any(|value| contains_key(value, key))
        }
        Value::Array(values) => values.iter().any(|value| contains_key(value, key)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

async fn remove_base(path: &Path) {
    let _ = tokio::fs::remove_dir_all(path).await;
}

#[tokio::test]
async fn profile_and_model_lists_are_btree_ordered() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-order-{}",
        SessionId::new().unwrap()
    ));
    let model = FakeModel::new([ModelScript::Text("unused")]);
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    let profile = config.profiles.get("test").unwrap().clone();
    config.profiles.clear();
    config.profiles.insert("zeta".to_owned(), profile.clone());
    config.profiles.insert("alpha".to_owned(), profile);
    config.default_profile = "alpha".to_owned();
    let model: Arc<dyn Model> = model;
    let models = Models::from_values(BTreeMap::from([
        ("zeta".to_owned(), Arc::clone(&model)),
        ("fake".to_owned(), Arc::clone(&model)),
        ("alpha".to_owned(), model),
    ]));
    let agent = Agent::open_with_models(config, models).await.unwrap();
    let mut rpc = RpcHarness::spawn(agent);

    rpc.send(json!("profiles"), "profile.list", None).await;
    let profiles = rpc.response(json!("profiles")).await;
    assert_eq!(
        profiles["result"]["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|profile| profile["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
    rpc.send(json!("models"), "model.list", None).await;
    let models = rpc.response(json!("models")).await;
    assert_eq!(
        models["result"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["alpha", "fake", "zeta"]
    );

    rpc.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_rpc_method_runs_through_the_real_agent_lifecycle() {
    let (agent, base, workspace) = test_agent(
        "methods",
        [ModelScript::Text("RPC final")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut rpc = RpcHarness::spawn(agent);

    rpc.send(json!("ping"), "agent.ping", None).await;
    assert_eq!(
        rpc.response(json!("ping")).await["result"]["version"],
        "0.1.0"
    );

    rpc.send(json!("profiles"), "profile.list", Some(json!({})))
        .await;
    let profiles = rpc.response(json!("profiles")).await;
    assert_eq!(profiles["result"]["profiles"][0]["id"], "test");
    assert_eq!(profiles["result"]["profiles"][0]["reasoning"], "medium");
    assert!(
        profiles["result"]["profiles"][0]
            .get("system_prompt")
            .is_none()
    );

    rpc.send(json!("models"), "model.list", Some(json!({})))
        .await;
    let models = rpc.response(json!("models")).await;
    assert_eq!(models["result"]["models"][0]["id"], "fake");
    assert_eq!(models["result"]["models"][0]["context_window"], 16_384);

    rpc.send(json!("list-empty"), "session.list", None).await;
    assert_eq!(
        rpc.response(json!("list-empty")).await["result"]["sessions"],
        json!([])
    );

    let unknown_session = SessionId::new().unwrap();
    rpc.send(
        json!("open-missing"),
        "session.open",
        Some(json!({"session_id": unknown_session})),
    )
    .await;
    assert_error(
        &rpc.response(json!("open-missing")).await,
        -32_001,
        "session_not_found",
    );
    rpc.send(
        json!("state-unloaded"),
        "session.state",
        Some(json!({"session_id": unknown_session})),
    )
    .await;
    assert_error(
        &rpc.response(json!("state-unloaded")).await,
        -32_002,
        "session_not_loaded",
    );

    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace, "title": "RPC session"})),
    )
    .await;
    let created = rpc.response(json!("create")).await;
    let session_id = session_id(&created);
    assert_eq!(created["result"]["session"]["profile"], "test");
    assert_eq!(created["result"]["session"]["loaded"], true);

    rpc.send(
        json!("open-loaded"),
        "session.open",
        Some(json!({"session_id": session_id})),
    )
    .await;
    let opened = rpc.response(json!("open-loaded")).await;
    assert_eq!(
        opened["result"]["session"]["instance_id"],
        created["result"]["session"]["instance_id"]
    );

    rpc.send(json!("list-loaded"), "session.list", None).await;
    assert_eq!(
        rpc.response(json!("list-loaded")).await["result"]["sessions"][0]["loaded"],
        true
    );

    rpc.send(
        json!("state"),
        "session.state",
        Some(json!({"session_id": session_id})),
    )
    .await;
    assert_eq!(
        rpc.response(json!("state")).await["result"]["status"],
        "idle"
    );

    rpc.send(
        json!("transcript-empty"),
        "session.transcript",
        Some(json!({"session_id": session_id, "after": null})),
    )
    .await;
    assert_eq!(
        rpc.response(json!("transcript-empty")).await["result"]["entries"],
        json!([])
    );

    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "hello"})),
    )
    .await;
    let sent = rpc.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();

    rpc.send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = rpc.response(json!("wait")).await;
    assert_eq!(waited["result"]["terminal"], "completed");
    let finished = rpc.event("turn_finished").await;
    assert_eq!(
        finished.pointer("/params/data/turn/turn_id"),
        Some(&turn["turn_id"])
    );

    rpc.send(
        json!("cancel-finished"),
        "turn.cancel",
        Some(turn_params(&turn)),
    )
    .await;
    assert_eq!(
        rpc.response(json!("cancel-finished")).await["result"]["cancelled"],
        false
    );

    rpc.send(
        json!("transcript"),
        "session.transcript",
        Some(json!({"session_id": session_id, "limit": 100})),
    )
    .await;
    assert!(
        rpc.response(json!("transcript")).await["result"]["entries"]
            .as_array()
            .unwrap()
            .len()
            >= 3
    );

    rpc.send(
        json!("delete-loaded"),
        "session.delete",
        Some(json!({"session_id": session_id})),
    )
    .await;
    assert_error(
        &rpc.response(json!("delete-loaded")).await,
        -32_005,
        "invalid_state",
    );

    rpc.send(
        json!("close"),
        "session.close",
        Some(json!({"session_id": session_id})),
    )
    .await;
    assert_eq!(
        rpc.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    rpc.send(
        json!("open"),
        "session.open",
        Some(json!({"session_id": session_id})),
    )
    .await;
    assert_eq!(
        rpc.response(json!("open")).await["result"]["session"]["loaded"],
        true
    );
    rpc.send(
        json!("close-again"),
        "session.close",
        Some(json!({"session_id": session_id})),
    )
    .await;
    rpc.response(json!("close-again")).await;
    rpc.send(
        json!("delete"),
        "session.delete",
        Some(json!({"session_id": session_id})),
    )
    .await;
    assert_eq!(
        rpc.response(json!("delete")).await["result"],
        json!({"ok": true})
    );
    rpc.send(json!("list-final"), "session.list", None).await;
    assert_eq!(
        rpc.response(json!("list-final")).await["result"]["sessions"],
        json!([])
    );

    rpc.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_wait_is_async_multiple_waiters_and_events_share_one_writer() {
    let (agent, base, workspace) =
        test_agent("waiters", [ModelScript::Block], &[], ApprovalMode::Auto).await;
    let mut rpc = RpcHarness::spawn(agent);
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);
    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "block"})),
    )
    .await;
    let turn = rpc.response(json!("send")).await["result"]["turn"].clone();

    for index in 0..8 {
        rpc.send(
            json!(format!("wait-{index}")),
            "turn.wait",
            Some(turn_params(&turn)),
        )
        .await;
    }
    rpc.send(json!("ping-after-waits"), "agent.ping", None)
        .await;
    assert_eq!(
        rpc.response(json!("ping-after-waits")).await["result"]["version"],
        "0.1.0"
    );

    rpc.send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        rpc.response(json!("cancel")).await["result"]["cancelled"],
        true
    );
    for index in 0..8 {
        let response = rpc.response(json!(format!("wait-{index}"))).await;
        assert_eq!(response["result"]["terminal"], "cancelled_by_user");
    }
    assert_eq!(
        rpc.event("turn_finished").await["params"]["data"]["outcome"]["terminal"],
        "cancelled_by_user"
    );

    rpc.shutdown().await;
    assert!(rpc.observed.iter().all(|frame| frame.is_object()));
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interaction_answers_are_strict_and_approval_allow_and_deny_resume_real_tools() {
    const SECRET: &str = "WRITE-ARGUMENT-MUST-NOT-LEAK";
    let (agent, base, workspace) = test_agent(
        "approval",
        [
            ModelScript::tool_call("write", json!({"path": "allowed.txt", "content": SECRET})),
            ModelScript::Text("allowed final"),
            ModelScript::tool_call("write", json!({"path": "denied.txt", "content": "denied"})),
            ModelScript::Text("denied final"),
        ],
        &["write"],
        ApprovalMode::Ask,
    )
    .await;
    let mut rpc = RpcHarness::spawn(agent);
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);

    rpc.send(
        json!("send-allow"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "allow write"})),
    )
    .await;
    let allow_turn = rpc.response(json!("send-allow")).await["result"]["turn"].clone();
    let interaction = rpc.event("interaction_requested").await;
    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
    assert!(
        !serde_json::to_string(&interaction)
            .unwrap()
            .contains(SECRET)
    );

    for (id, answer) in [
        ("empty-text", json!({"type": "text", "text": ""})),
        ("control-text", json!({"type": "text", "text": "bad\ntext"})),
        ("negative-choice", json!({"type": "choice", "index": -1})),
        ("unknown-type", json!({"type": "unknown"})),
        (
            "extra",
            json!({"type": "approval", "decision": "deny", "extra": true}),
        ),
    ] {
        rpc.send(
            json!(id),
            "interaction.answer",
            Some(json!({
                "session_id": session_id,
                "interaction_id": interaction_id,
                "answer": answer,
            })),
        )
        .await;
        assert_error(&rpc.response(json!(id)).await, -32_602, "invalid_params");
    }

    for (id, answer) in [
        ("wrong-text", json!({"type": "text", "text": "valid"})),
        ("wrong-choice", json!({"type": "choice", "index": 0})),
    ] {
        rpc.send(
            json!(id),
            "interaction.answer",
            Some(json!({
                "session_id": session_id,
                "interaction_id": interaction_id,
                "answer": answer,
            })),
        )
        .await;
        assert_error(&rpc.response(json!(id)).await, -32_005, "invalid_state");
    }

    rpc.send(
        json!("allow"),
        "interaction.answer",
        Some(json!({
            "session_id": session_id,
            "interaction_id": interaction_id,
            "answer": {"type": "approval", "decision": "allow_once"},
        })),
    )
    .await;
    assert_eq!(
        rpc.response(json!("allow")).await["result"],
        json!({"ok": true})
    );
    rpc.send(
        json!("wait-allow"),
        "turn.wait",
        Some(turn_params(&allow_turn)),
    )
    .await;
    assert_eq!(
        rpc.response(json!("wait-allow")).await["result"]["terminal"],
        "completed"
    );
    assert_eq!(
        tokio::fs::read_to_string(workspace.join("allowed.txt"))
            .await
            .unwrap(),
        SECRET
    );

    rpc.send(
        json!("send-deny"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "deny write"})),
    )
    .await;
    let deny_turn = rpc.response(json!("send-deny")).await["result"]["turn"].clone();
    let interaction = rpc.event("interaction_requested").await;
    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
    rpc.send(
        json!("deny"),
        "interaction.answer",
        Some(json!({
            "session_id": session_id,
            "interaction_id": interaction_id,
            "answer": {"type": "approval", "decision": "deny"},
        })),
    )
    .await;
    rpc.response(json!("deny")).await;
    rpc.send(
        json!("wait-deny"),
        "turn.wait",
        Some(turn_params(&deny_turn)),
    )
    .await;
    assert_eq!(
        rpc.response(json!("wait-deny")).await["result"]["terminal"],
        "completed"
    );
    assert!(!workspace.join("denied.txt").exists());

    rpc.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn params_methods_ids_and_domain_errors_are_stable() {
    let (agent, base, workspace) =
        test_agent("errors", [ModelScript::Block], &[], ApprovalMode::Auto).await;
    let mut rpc = RpcHarness::spawn(agent);

    rpc.send(json!("unknown"), "unknown.method", Some(json!({})))
        .await;
    assert_error(
        &rpc.response(json!("unknown")).await,
        -32_601,
        "method_not_found",
    );
    rpc.send(json!("null"), "session.list", Some(Value::Null))
        .await;
    assert_error(
        &rpc.response(json!("null")).await,
        -32_602,
        "invalid_params",
    );
    rpc.send(
        json!("extra-create"),
        "session.create",
        Some(json!({"workspace": workspace, "extra": true})),
    )
    .await;
    assert_error(
        &rpc.response(json!("extra-create")).await,
        -32_602,
        "invalid_params",
    );
    rpc.send(
        json!("missing-profile"),
        "session.create",
        Some(json!({"workspace": workspace, "profile": "missing"})),
    )
    .await;
    assert_error(
        &rpc.response(json!("missing-profile")).await,
        -32_008,
        "profile_not_found",
    );
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);

    for limit in [0, 101] {
        let id = format!("limit-{limit}");
        rpc.send(
            json!(id),
            "session.transcript",
            Some(json!({"session_id": session_id, "limit": limit})),
        )
        .await;
        assert_error(&rpc.response(json!(id)).await, -32_602, "invalid_params");
    }
    rpc.send(
        json!("empty-send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": ""})),
    )
    .await;
    assert_error(
        &rpc.response(json!("empty-send")).await,
        -32_602,
        "invalid_params",
    );
    rpc.send(
        json!("answer-missing"),
        "interaction.answer",
        Some(json!({
            "session_id": session_id,
            "interaction_id": minicore_runtime::InteractionId::new().unwrap(),
            "answer": {"type": "approval", "decision": "deny"},
        })),
    )
    .await;
    assert_error(
        &rpc.response(json!("answer-missing")).await,
        -32_006,
        "interaction_not_found",
    );

    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "busy"})),
    )
    .await;
    let turn = rpc.response(json!("send")).await["result"]["turn"].clone();
    rpc.send(
        json!("busy"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "second"})),
    )
    .await;
    let busy = rpc.response(json!("busy")).await;
    assert_error(&busy, -32_003, "session_busy");
    assert_eq!(busy["error"]["data"]["retryable"], true);

    let mut wrong_turn = turn_params(&turn);
    wrong_turn["turn_id"] = json!(minicore_runtime::TurnId::new().unwrap());
    rpc.send(json!("wrong-turn"), "turn.wait", Some(wrong_turn))
        .await;
    assert_error(
        &rpc.response(json!("wrong-turn")).await,
        -32_007,
        "turn_not_found",
    );
    rpc.send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    rpc.response(json!("cancel")).await;

    rpc.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_provider_phase_maps_to_provider_error() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-provider-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = FakeModel::new([ModelScript::Text("unused")]);
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.profiles.get_mut("test").unwrap().compaction = ProfileCompaction::Model {
        trigger_tokens: 1_000,
        target_tokens: 500,
    };
    let agent = Agent::open_with_models(config, test_models(model))
        .await
        .unwrap();
    let mut rpc = RpcHarness::spawn(agent);
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    assert_error(
        &rpc.response(json!("create")).await,
        -32_012,
        "provider_error",
    );
    rpc.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_response_is_last_after_pending_waiter_and_event_drain() {
    let (agent, base, workspace) = test_agent(
        "shutdown-order",
        [ModelScript::Block],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut rpc = RpcHarness::spawn(agent);
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);
    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "active"})),
    )
    .await;
    let turn = rpc.response(json!("send")).await["result"]["turn"].clone();
    rpc.send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    rpc.send(json!("shutdown"), "agent.shutdown", Some(json!({})))
        .await;

    let mut frames = Vec::new();
    while let Some(frame) = rpc.next_frame().await {
        let is_shutdown = frame.get("id") == Some(&json!("shutdown"));
        frames.push(frame);
        if is_shutdown {
            break;
        }
    }
    assert_eq!(frames.last().unwrap()["id"], "shutdown");
    assert!(
        frames
            .iter()
            .any(|frame| frame.get("id") == Some(&json!("wait")))
    );
    assert!(frames.iter().any(|frame| {
        frame.get("id") == Some(&json!("wait"))
            && frame["result"]["terminal"] == "cancelled_by_shutdown"
    }));
    assert!(
        frames
            .iter()
            .any(|frame| frame.pointer("/params/type") == Some(&json!("session_closed")))
    );
    assert!(rpc.next_frame().await.is_none());
    rpc.join().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_runs_the_same_active_turn_shutdown_barrier() {
    let (agent, base, workspace) =
        test_agent("eof", [ModelScript::Block], &[], ApprovalMode::Auto).await;
    let mut rpc = RpcHarness::spawn(agent);
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);
    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "active"})),
    )
    .await;
    let turn = rpc.response(json!("send")).await["result"]["turn"].clone();
    rpc.send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    rpc.close_input();

    let mut wait_seen = false;
    let mut closed_seen = false;
    while let Some(frame) = rpc.next_frame().await {
        wait_seen |= frame.get("id") == Some(&json!("wait"))
            && frame["result"]["terminal"] == "cancelled_by_shutdown";
        closed_seen |= frame.pointer("/params/type") == Some(&json!("session_closed"));
    }
    assert!(wait_seen);
    assert!(closed_seen);
    rpc.join().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_signal_runs_the_same_active_turn_barrier_without_global_signal_state() {
    let (agent, base, workspace) =
        test_agent("signal", [ModelScript::Block], &[], ApprovalMode::Auto).await;
    let (mut rpc, signal) = RpcHarness::spawn_with_signal(agent);
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);
    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "active"})),
    )
    .await;
    let turn = rpc.response(json!("send")).await["result"]["turn"].clone();
    rpc.send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    rpc.send(json!("wait-registered"), "agent.ping", None).await;
    rpc.response(json!("wait-registered")).await;
    signal.send(()).unwrap();

    let mut wait_seen = false;
    let mut closed_seen = false;
    while let Some(frame) = rpc.next_frame().await {
        wait_seen |= frame.get("id") == Some(&json!("wait"))
            && frame["result"]["terminal"] == "cancelled_by_shutdown";
        closed_seen |= frame.pointer("/params/type") == Some(&json!("session_closed"));
    }
    assert!(wait_seen);
    assert!(closed_seen);
    rpc.join().await.unwrap();
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_writer_stops_reader_shuts_down_agent_and_joins_waiters() {
    let (agent, base, workspace) = test_agent(
        "writer-failure",
        [ModelScript::Block],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let fail = Arc::new(AtomicBool::new(false));
    let mut rpc = RpcHarness::spawn_with_switch_writer(agent, Arc::clone(&fail));
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);
    rpc.send(
        json!("send"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "active"})),
    )
    .await;
    let turn = rpc.response(json!("send")).await["result"]["turn"].clone();
    rpc.send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    fail.store(true, Ordering::SeqCst);
    rpc.send(json!("trigger"), "agent.ping", None).await;

    let result = rpc.join().await;
    assert!(matches!(result, Err(AgentError::Io(_))));
    remove_base(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_info_errors_and_events_never_serialize_secrets() {
    const SYSTEM_SECRET: &str = "SECRET-SYSTEM-PROMPT";
    const ENV_SECRET: &str = "SECRET-API-KEY-ENV-VALUE";
    const BASE_URL_SECRET: &str = "https://secret.invalid/v1";
    const WRITE_SECRET: &str = "SECRET-WRITE-CONTENT";
    const COMMAND_SECRET: &str = "curl https://secret.invalid/private";
    const PROVIDER_SECRET: &str = "SECRET-RAW-PROVIDER-RESPONSE";

    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-secret-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = FakeModel::new([
        ModelScript::ToolCalls(vec![
            ToolCallScript {
                name: "write",
                arguments: json!({"path": "secret.txt", "content": WRITE_SECRET}),
            },
            ToolCallScript {
                name: "bash",
                arguments: json!({"command": COMMAND_SECRET}),
            },
        ]),
        ModelScript::Text("after denials"),
        ModelScript::Error(PROVIDER_SECRET),
    ]);
    let mut config = test_config(base.join("data"), &["write", "bash"], ApprovalMode::Ask);
    config.profiles.get_mut("test").unwrap().system_prompt = SYSTEM_SECRET.to_owned();
    config.models.insert(
        "fake".to_owned(),
        ModelConfig::OpenAiResponses {
            model: "private-model".to_owned(),
            base_url: BASE_URL_SECRET.to_owned(),
            api_key_env: ENV_SECRET.to_owned(),
            physical_context_window: 32_000,
            output_budget_tokens: 2_000,
            safety_margin_tokens: 1_000,
            supported_reasoning: BTreeSet::from([ReasoningPreference::Auto]),
            supports_tools: true,
            request_timeout_seconds: Some(30),
        },
    );
    let agent = Agent::open_with_models(config, test_models(model))
        .await
        .unwrap();
    let mut rpc = RpcHarness::spawn(agent);

    rpc.send(json!("profiles"), "profile.list", None).await;
    rpc.response(json!("profiles")).await;
    rpc.send(json!("models"), "model.list", None).await;
    rpc.response(json!("models")).await;
    rpc.send(
        json!("create"),
        "session.create",
        Some(json!({"workspace": workspace})),
    )
    .await;
    let session_id = session_id(&rpc.response(json!("create")).await);
    rpc.send(
        json!("send-tools"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "request tools"})),
    )
    .await;
    let tool_turn = rpc.response(json!("send-tools")).await["result"]["turn"].clone();
    for index in 0..2 {
        let interaction = rpc.event("interaction_requested").await;
        let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
        rpc.send(
            json!(format!("deny-{index}")),
            "interaction.answer",
            Some(json!({
                "session_id": session_id,
                "interaction_id": interaction_id,
                "answer": {"type": "approval", "decision": "deny"},
            })),
        )
        .await;
        rpc.response(json!(format!("deny-{index}"))).await;
    }
    rpc.send(
        json!("wait-tools"),
        "turn.wait",
        Some(turn_params(&tool_turn)),
    )
    .await;
    rpc.response(json!("wait-tools")).await;
    rpc.event("turn_finished").await;

    rpc.send(
        json!("send-error"),
        "turn.send",
        Some(json!({"session_id": session_id, "text": "provider failure"})),
    )
    .await;
    let error_turn = rpc.response(json!("send-error")).await["result"]["turn"].clone();
    rpc.send(
        json!("wait-error"),
        "turn.wait",
        Some(turn_params(&error_turn)),
    )
    .await;
    let outcome = rpc.response(json!("wait-error")).await;
    assert!(outcome["result"]["terminal"]["failed"]["diagnostic"].is_object());
    rpc.event("turn_finished").await;

    rpc.send(
        json!("transcript"),
        "session.transcript",
        Some(json!({"session_id": session_id, "limit": 100})),
    )
    .await;
    let transcript = rpc.response(json!("transcript")).await;
    let page = &transcript["result"];
    assert!(page["entries"].as_array().is_some_and(|entries| {
        entries
            .iter()
            .any(|entry| entry.get("user_message").is_some())
            && entries
                .iter()
                .any(|entry| entry.get("assistant_message").is_some())
            && entries
                .iter()
                .any(|entry| entry.get("tool_result").is_some())
            && entries
                .iter()
                .any(|entry| entry.get("turn_terminal").is_some())
    }));
    assert!(page.get("next_after").is_some());
    assert!(page.get("observed_head").is_some());
    assert_eq!(page["complete"], true);
    assert!(!contains_key(page, "arguments"));
    assert!(!contains_key(page, "message"));

    let encoded = serde_json::to_string(&rpc.observed).unwrap();
    for secret in [
        SYSTEM_SECRET,
        ENV_SECRET,
        BASE_URL_SECRET,
        WRITE_SECRET,
        COMMAND_SECRET,
        PROVIDER_SECRET,
    ] {
        assert!(!encoded.contains(secret), "RPC output leaked {secret}");
    }

    rpc.shutdown().await;
    remove_base(&base).await;
}

#[test]
fn agent_error_table_is_stable_and_never_serializes_sources() {
    let id = super::super::protocol::RpcId::String("error".to_owned());
    let cases = [
        (AgentError::SessionNotFound, -32_001, "session_not_found"),
        (AgentError::SessionNotLoaded, -32_002, "session_not_loaded"),
        (AgentError::SessionBusy, -32_003, "session_busy"),
        (AgentError::SessionClosed, -32_004, "session_closed"),
        (AgentError::SessionAlreadyLoaded, -32_005, "invalid_state"),
        (AgentError::SessionSpecMismatch, -32_005, "invalid_state"),
        (AgentError::InvalidInteraction, -32_005, "invalid_state"),
        (
            AgentError::InteractionNotFound,
            -32_006,
            "interaction_not_found",
        ),
        (AgentError::TurnNotFound, -32_007, "turn_not_found"),
        (AgentError::ProfileNotFound, -32_008, "profile_not_found"),
        (AgentError::ModelNotFound, -32_009, "model_not_found"),
        (AgentError::Workspace, -32_010, "workspace_error"),
        (AgentError::Store, -32_011, "store_error"),
        (AgentError::ModelNotImplemented, -32_012, "provider_error"),
        (
            AgentError::Core(CoreErrorView::new("PRIVATE-CORE-SOURCE", true)),
            -32_013,
            "core_error",
        ),
    ];
    for (error, code, kind) in cases {
        let value = serde_json::to_value(agent_error(id.clone(), &error)).unwrap();
        assert_eq!(value["error"]["code"], code);
        assert_eq!(value["error"]["data"]["kind"], kind);
        assert!(!value.to_string().contains("PRIVATE-CORE-SOURCE"));
    }

    let invalid = serde_json::to_value(agent_error(id.clone(), &AgentError::InvalidInput)).unwrap();
    assert_eq!(invalid["error"]["code"], -32_602);
    assert_eq!(invalid["error"]["data"]["kind"], "invalid_params");
    let internal = serde_json::to_value(agent_error(id.clone(), &AgentError::Internal)).unwrap();
    assert_eq!(internal["error"]["code"], -32_603);
    assert_eq!(internal["error"]["data"]["kind"], "internal_error");

    let wait_error = minicore_runtime::error::TurnWaitError::DurabilityUnavailable(
        minicore_runtime::error::DiagnosticSummary::new(
            DiagnosticCode::Internal,
            DiagnosticCategory::Storage,
            BoundedText::new("PRIVATE-TURN-WAIT-SOURCE").unwrap(),
            true,
        ),
    );
    let value = serde_json::to_value(turn_wait_error(id, &wait_error)).unwrap();
    assert_eq!(value["error"]["code"], -32_013);
    assert_eq!(value["error"]["data"]["kind"], "core_error");
    assert_eq!(value["error"]["data"]["retryable"], true);
    assert!(!value.to_string().contains("PRIVATE-TURN-WAIT-SOURCE"));
}
