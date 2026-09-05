use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::pending;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::task::JoinHandle;

use crate::ids::SessionId;
use minicore_runtime::ToolCallId;
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason, ModelRef,
    ModelRequest, ModelStartFuture, ModelStream, ReasoningPreference, Usage,
};

use crate::agent::Agent;
use crate::config::{AgentConfig, LoopOverrides, Profile};
use crate::error::AgentError;
use crate::models::{ModelConfig, Models};
use crate::profiles::ApprovalMode;
use crate::store::fail_next_append;

use super::run_with_io;

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum ModelScript {
    Text(&'static str),
    ToolCalls(Vec<ToolCallScript>),
    Block,
}

#[derive(Clone)]
struct ToolCallScript {
    name: &'static str,
    arguments: Value,
}

fn fake_supported_reasoning() -> BTreeSet<ReasoningPreference> {
    BTreeSet::from([
        ReasoningPreference::Auto,
        ReasoningPreference::Disabled,
        ReasoningPreference::Low,
        ReasoningPreference::Medium,
        ReasoningPreference::High,
        ReasoningPreference::XHigh,
        ReasoningPreference::Max,
        ReasoningPreference::Ultra,
    ])
}

fn configured_model(model: &str, base_url: &str, api_key_env: &str) -> ModelConfig {
    ModelConfig::OpenAiResponses {
        model: model.to_owned(),
        base_url: base_url.to_owned(),
        api_key_env: api_key_env.to_owned(),
        physical_context_window: 32_000,
        output_budget_tokens: 2_000,
        safety_margin_tokens: 1_000,
        supported_reasoning: fake_supported_reasoning(),
        supports_tools: true,
        request_timeout_seconds: Some(30),
    }
}

struct FakeModel {
    descriptor: ModelDescriptor,
    scripts: Mutex<VecDeque<ModelScript>>,
    calls: AtomicUsize,
}

impl FakeModel {
    fn new(scripts: impl IntoIterator<Item = ModelScript>) -> Arc<Self> {
        Self::with_capabilities(scripts, fake_supported_reasoning(), true)
    }

    fn with_capabilities(
        scripts: impl IntoIterator<Item = ModelScript>,
        supported_reasoning: BTreeSet<ReasoningPreference>,
        supports_tools: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ModelDescriptor::new(
                "fake".parse::<ModelRef>().unwrap(),
                16_384,
                supported_reasoning,
                supports_tools,
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

    fn start(&self, _request: ModelRequest, _context: ModelCallContext) -> ModelStartFuture<'_> {
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
            },
        )]),
        models: BTreeMap::from([(
            "fake".to_owned(),
            configured_model(
                "provider-model",
                "https://example.invalid/v1",
                "MINICORE_UNUSED_RPC_FAKE_KEY",
            ),
        )]),
        loop_options: LoopOverrides::default(),
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

fn session_id(response: &Value) -> Value {
    response
        .pointer("/result/session/session_id")
        .unwrap()
        .clone()
}

fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "loop_id": turn["loop_id"],
    })
}

async fn remove_base(path: &Path) {
    let _ = tokio::fs::remove_dir_all(path).await;
}

#[tokio::test]
async fn capability_discovery_returns_ordered_lists() {
    let (agent, base, _workspace) =
        test_agent("capability", [], &["read"], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    harness
        .send(json!("p"), "agent.ping", Some(json!({})))
        .await;
    let ping = harness.response(json!("p")).await;
    assert_eq!(ping["result"]["version"], env!("CARGO_PKG_VERSION"));

    harness
        .send(json!("profiles"), "profile.list", Some(json!({})))
        .await;
    let profiles = harness.response(json!("profiles")).await;
    assert_eq!(profiles["result"]["profiles"][0]["id"], json!("test"));

    harness
        .send(json!("models"), "model.list", Some(json!({})))
        .await;
    let models = harness.response(json!("models")).await;
    assert_eq!(models["result"]["models"][0]["id"], json!("fake"));
    assert_eq!(
        models["result"]["models"][0]["supported_reasoning"],
        json!([
            "auto", "disabled", "low", "medium", "high", "xhigh", "max", "ultra"
        ])
    );

    harness
        .send(json!("sessions"), "session.list", Some(json!({})))
        .await;
    let sessions = harness.response(json!("sessions")).await;
    assert_eq!(sessions["result"]["sessions"], json!([]));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn extended_reasoning_round_trips_through_rpc_and_reopen() {
    let (agent, base, workspace) =
        test_agent("extended-reasoning", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);

    harness
        .send(
            json!("create-xhigh"),
            "session.create",
            Some(json!({"workspace": workspace, "reasoning": "xhigh"})),
        )
        .await;
    let created = harness.response(json!("create-xhigh")).await;
    assert_eq!(created["result"]["session"]["reasoning"], json!("xhigh"));
    assert_eq!(
        serde_json::from_value::<ReasoningPreference>(
            created["result"]["session"]["reasoning"].clone()
        )
        .unwrap(),
        ReasoningPreference::XHigh
    );
    let opened = harness.event("session_opened").await;
    assert_eq!(
        opened["params"]["data"]["session"]["reasoning"],
        json!("xhigh")
    );
    let session_id = session_id(&created);

    for (wire, expected) in [
        ("max", ReasoningPreference::Max),
        ("ultra", ReasoningPreference::Ultra),
    ] {
        let request_id = json!(format!("update-{wire}"));
        harness
            .send(
                request_id.clone(),
                "session.update",
                Some(json!({"session_id": session_id, "reasoning": wire})),
            )
            .await;
        let updated = harness.response(request_id).await;
        assert_eq!(updated["result"]["session"]["reasoning"], json!(wire));
        assert_eq!(
            serde_json::from_value::<ReasoningPreference>(
                updated["result"]["session"]["reasoning"].clone()
            )
            .unwrap(),
            expected
        );
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    harness
        .send(
            json!("reopen"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let reopened = harness.response(json!("reopen")).await;
    assert_eq!(reopened["result"]["session"]["reasoning"], json!("ultra"));
    let reopened_event = harness.event("session_opened").await;
    assert_eq!(
        reopened_event["params"]["data"]["session"]["reasoning"],
        json!("ultra")
    );

    harness
        .send(
            json!("close-again"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close-again")).await["result"],
        json!({"ok": true})
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

async fn create_and_open(harness: &mut RpcHarness, workspace: &Path) -> Value {
    harness
        .send(
            json!("create"),
            "session.create",
            Some(json!({"workspace": workspace, "title": "rpc session"})),
        )
        .await;
    let created = harness.response(json!("create")).await;
    assert_eq!(created["result"]["session"]["model"], json!("fake"));
    let session_id = session_id(&created);
    harness
        .send(
            json!("open"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    harness.response(json!("open")).await;
    session_id
}

#[tokio::test]
async fn create_open_history_and_send_wait_deferred() {
    let (agent, base, workspace) = test_agent(
        "flow",
        [ModelScript::Text("complete")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    assert_eq!(history["result"]["total"], 0);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello rpc"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    assert_eq!(turn["session_id"], session_id);
    assert!(turn["loop_id"].as_str().unwrap().starts_with("lup_"));

    // Deferred wait must not block other requests.
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    let ping = harness.response(json!("ping")).await;
    assert_eq!(ping["result"]["version"], env!("CARGO_PKG_VERSION"));

    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited["result"]["persistence"], json!("persisted"));
    assert_eq!(waited["result"]["turn"]["loop_id"], turn["loop_id"]);

    harness
        .send(
            json!("history2"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history2 = harness.response(json!("history2")).await;
    assert_eq!(history2["result"]["total"], 2);
    assert!(history2["result"]["next_offset"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn steer_and_update_and_cancel_work_while_wait_is_pending() {
    let (agent, base, workspace) = test_agent(
        "wait-concurrent",
        [ModelScript::Block],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();

    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;

    // steer while wait pending
    harness
        .send(
            json!("steer"),
            "turn.steer",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "text": "stay on track"
            })),
        )
        .await;
    let steer = harness.response(json!("steer")).await;
    assert_eq!(steer["result"], json!({"ok": true}));

    // session.update while wait pending
    harness
        .send(
            json!("update"),
            "session.update",
            Some(json!({"session_id": session_id, "reasoning": "high"})),
        )
        .await;
    let update = harness.response(json!("update")).await;
    assert!(update["result"]["active_revision"].is_number());
    assert_eq!(update["result"]["session"]["reasoning"], json!("high"));

    // cancel while wait pending
    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    let cancel = harness.response(json!("cancel")).await;
    assert_eq!(cancel["result"]["cancelled"], json!(true));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_update_requires_an_unknown_field_free_shape_and_model_change() {
    let (agent, base, workspace) = test_agent(
        "update",
        [ModelScript::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("noop"),
            "session.update",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let noop = harness.response(json!("noop")).await;
    assert_eq!(noop["error"]["code"], json!(-32602));

    harness
        .send(
            json!("missing-model"),
            "session.update",
            Some(json!({"session_id": session_id, "model": "nope"})),
        )
        .await;
    let missing = harness.response(json!("missing-model")).await;
    assert_eq!(missing["error"]["code"], json!(-32009));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn history_view_is_safe_and_pages() {
    let (agent, base, workspace) = test_agent(
        "safe-view",
        [ModelScript::ToolCalls(vec![ToolCallScript {
            name: "read",
            arguments: json!({"path": "secret.txt"}),
        }])],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "read file"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    assert!(!serialized.contains("secret.txt"));
    assert!(!serialized.contains("arguments"));
    // Tool lifecycle is visible through the safe view.
    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert_eq!(state["result"]["status"], json!("idle"));
    assert!(state["result"]["active_loop"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn persistence_failure_returns_failed_and_blocked_state() {
    let (agent, base, workspace) = test_agent(
        "persist-fail",
        [ModelScript::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    fail_next_append(session_id.as_str().unwrap().parse().unwrap());

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert_eq!(state["result"]["status"], json!("blocked"));
    assert_eq!(state["result"]["block_reason"], json!("persistence"));

    // Blocked sessions refuse further sends.
    harness
        .send(
            json!("send2"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "again"})),
        )
        .await;
    let send2 = harness.response(json!("send2")).await;
    assert_eq!(send2["error"]["code"], json!(-32004));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn blocked_send_does_not_discard_previous_turn_result_rpc() {
    let (agent, base, workspace) = test_agent(
        "blocked-discard",
        [ModelScript::Text("hello")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    fail_next_append(session_id.as_str().unwrap().parse().unwrap());

    // 1. send first turn
    harness
        .send(
            json!("send1"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    let sent = harness.response(json!("send1")).await;
    let turn = sent["result"]["turn"].clone();

    // 2. Deterministically wait for session to reach blocked state before waiting turn.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        harness
            .send(
                json!("poll_state"),
                "session.state",
                Some(json!({"session_id": session_id})),
            )
            .await;
        let state = harness.response(json!("poll_state")).await;
        if state["result"]["status"] == json!("blocked") {
            assert_eq!(state["result"]["block_reason"], json!("persistence"));
            break;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "session did not reach blocked; final state: {:?}",
                state["result"]
            );
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
    }

    // 3. Second send fails with -32004 session_blocked
    harness
        .send(
            json!("send2"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "again"})),
        )
        .await;
    let send2 = harness.response(json!("send2")).await;
    assert_eq!(send2["error"]["code"], json!(-32004));

    // 4. First turn.wait still succeeds (not -32007 turn_not_found)
    harness
        .send(json!("wait1"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited1 = harness.response(json!("wait1")).await;
    assert_eq!(waited1["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited1["result"]["persistence"], json!("failed"));

    // 5. Repeated turn.wait also succeeds
    harness
        .send(json!("wait2"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited2 = harness.response(json!("wait2")).await;
    assert_eq!(waited2["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(waited2["result"]["persistence"], json!("failed"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn interaction_answer_resolves_an_approval() {
    let (agent, base, workspace) = test_agent(
        "approval",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "out.txt", "content": "approved"}),
            }]),
            ModelScript::Text("wrote it"),
        ],
        &["write"],
        ApprovalMode::Ask,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "write the file"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();

    let interaction = harness.event("interaction_requested").await;
    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
    assert_eq!(
        interaction["params"]["data"]["interaction"]["kind"]["type"],
        json!("approval")
    );

    harness
        .send(
            json!("answer"),
            "interaction.answer",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "interaction_id": interaction_id,
                "answer": {"type": "approval", "decision": "allow_once"}
            })),
        )
        .await;
    let answer = harness.response(json!("answer")).await;
    assert_eq!(answer["result"], json!({"ok": true}));

    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn shutdown_cancels_and_awaits_an_active_loop() {
    let (agent, base, workspace) = test_agent(
        "shutdown-active",
        [ModelScript::Block],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "hello"})),
        )
        .await;
    harness.response(json!("send")).await;
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn error_mapping_covers_blocked_and_stale_turn() {
    let (agent, base, workspace) = test_agent(
        "errors",
        [ModelScript::Text("done")],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    // stale loop id
    harness
        .send(
            json!("wait-stale"),
            "turn.wait",
            Some(json!({
                "session_id": session_id,
                "loop_id": "lup_00000000000000000000000000000001"
            })),
        )
        .await;
    let stale = harness.response(json!("wait-stale")).await;
    assert_eq!(stale["error"]["code"], json!(-32007));

    // unknown session
    harness
        .send(
            json!("state-missing"),
            "session.state",
            Some(json!({"session_id": "ses_00000000000000000000000000000001"})),
        )
        .await;
    let missing = harness.response(json!("state-missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32002));

    harness.shutdown().await;
    remove_base(&base).await;
}
