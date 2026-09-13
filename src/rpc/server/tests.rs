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
use tokio_util::sync::CancellationToken;

use crate::ids::SessionId;
use minicore_runtime::ToolCallId;
use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{
    Model, ModelCallContext, ModelDescriptor, ModelError, ModelErrorKind, ModelEvent,
    ModelFinishReason, ModelMessage, ModelRef, ModelRequest, ModelStartFuture, ModelStream,
    ReasoningPreference, Usage,
};

use crate::agent::Agent;
use crate::config::{AgentConfig, LoopOverrides, Profile};
use crate::error::AgentError;
use crate::models::{ModelConfig, Models};
use crate::profiles::ApprovalMode;
use crate::sessions::{WorkerGate, pause_next_compaction_after_result};
use crate::store::{
    SummaryCommitGate, fail_next_append, fail_next_record_write, fail_next_summary_write,
    force_unknown_summary_write, gate_next_summary_commit,
};

use super::run_with_io;

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum ModelScript {
    Text(&'static str),
    NoUsage(&'static str),
    Capture {
        inputs: Arc<Mutex<Vec<String>>>,
        text: &'static str,
    },
    Observe {
        calls: Arc<Mutex<Vec<ObservedCall>>>,
        text: &'static str,
    },
    Fail,
    Gate(Arc<ConcurrencyProbe>),
    ToolCalls(Vec<ToolCallScript>),
    NoFinish,
    WrongFinish,
    LateContent,
    Whitespace,
    Oversize,
    Block,
}

struct ObservedCall {
    request: ModelRequest,
    context: ModelCallContext,
}

struct ConcurrencyProbe {
    started: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    release: CancellationToken,
}

impl ConcurrencyProbe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            release: CancellationToken::new(),
        })
    }

    fn observe_active(&self, active: usize) {
        let mut current = self.max_active.load(Ordering::SeqCst);
        while active > current {
            match self.max_active.compare_exchange_weak(
                current,
                active,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

fn fake_model_error() -> ModelError {
    ModelError::started(
        ModelErrorKind::Internal,
        DiagnosticSummary::new(
            DiagnosticCode::Internal,
            DiagnosticCategory::Internal,
            minicore_runtime::BoundedText::new("fake model failure").unwrap(),
            false,
        ),
    )
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
        Self::with_context_and_capabilities(scripts, 16_384, fake_supported_reasoning(), true)
    }

    fn with_context_window(
        scripts: impl IntoIterator<Item = ModelScript>,
        context_window: u64,
    ) -> Arc<Self> {
        Self::with_context_and_capabilities(
            scripts,
            context_window,
            fake_supported_reasoning(),
            true,
        )
    }

    fn with_context_and_capabilities(
        scripts: impl IntoIterator<Item = ModelScript>,
        context_window: u64,
        supported_reasoning: BTreeSet<ReasoningPreference>,
        supports_tools: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ModelDescriptor::new(
                "fake".parse::<ModelRef>().unwrap(),
                context_window,
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

    fn start(&self, request: ModelRequest, context: ModelCallContext) -> ModelStartFuture<'_> {
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
                ModelScript::NoUsage(text) => Ok(model_events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ])),
                ModelScript::Capture { inputs, text } => {
                    let input = request
                        .messages()
                        .iter()
                        .rev()
                        .find_map(|message| match message {
                            ModelMessage::User(text) => Some(text.clone()),
                            _ => None,
                        })
                        .unwrap_or_default();
                    inputs.lock().unwrap().push(input);
                    Ok(model_events(vec![
                        ModelEvent::text_delta(text).unwrap(),
                        ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]))
                }
                ModelScript::Observe { calls, text } => {
                    calls
                        .lock()
                        .unwrap()
                        .push(ObservedCall { request, context });
                    Ok(model_events(vec![
                        ModelEvent::text_delta(text).unwrap(),
                        ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]))
                }
                ModelScript::Fail => Err(fake_model_error()),
                ModelScript::Gate(probe) => {
                    probe.started.fetch_add(1, Ordering::SeqCst);
                    let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
                    probe.observe_active(active);
                    probe.release.cancelled().await;
                    probe.active.fetch_sub(1, Ordering::SeqCst);
                    Ok(model_events(vec![
                        ModelEvent::text_delta("gated answer").unwrap(),
                        ModelEvent::Usage {
                            usage: Usage::new(1, 1, 0),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]))
                }
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
                ModelScript::NoFinish => Ok(model_events(vec![
                    ModelEvent::text_delta("incomplete").unwrap(),
                ])),
                ModelScript::WrongFinish => Ok(model_events(vec![
                    ModelEvent::text_delta("wrong finish").unwrap(),
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Length,
                    },
                ])),
                ModelScript::LateContent => Ok(model_events(vec![
                    ModelEvent::text_delta("late").unwrap(),
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                    ModelEvent::text_delta("after finish").unwrap(),
                ])),
                ModelScript::Whitespace => Ok(model_events(vec![
                    ModelEvent::text_delta(" \n\t").unwrap(),
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ])),
                ModelScript::Oversize => {
                    let chunk = "x".repeat(64 * 1024);
                    Ok(model_events(vec![
                        ModelEvent::text_delta(chunk.clone()).unwrap(),
                        ModelEvent::text_delta(chunk).unwrap(),
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]))
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
async fn agent_reload_requires_empty_params_and_a_file_source() {
    let (agent, base, _workspace) = test_agent("reload-rpc", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);

    harness
        .send(json!("reload"), "agent.reload", Some(json!({})))
        .await;
    let reload = harness.response(json!("reload")).await;
    assert_eq!(reload["error"]["code"], json!(-32018));
    assert_eq!(
        reload["error"]["data"],
        json!({
            "kind": "reload_unavailable",
            "retryable": false,
        })
    );

    harness
        .send(
            json!("reload-path"),
            "agent.reload",
            Some(json!({"path": "/tmp/other.agent.toml"})),
        )
        .await;
    let reload_path = harness.response(json!("reload-path")).await;
    assert_eq!(reload_path["error"]["code"], json!(-32602));
    assert_eq!(
        reload_path["error"]["data"]["kind"],
        json!("invalid_params")
    );
    assert!(!reload_path.to_string().contains("other.agent.toml"));

    harness
        .send(json!("reload-omitted"), "agent.reload", None)
        .await;
    let reload_omitted = harness.response(json!("reload-omitted")).await;
    assert_eq!(reload_omitted["error"]["code"], json!(-32018));

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

#[tokio::test]
async fn session_rename_is_a_separate_rpc_and_persists_title_without_config_revision() {
    let (agent, base, workspace) = test_agent(
        "rename-basic",
        [ModelScript::Text("done")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "  Unicode 名称  "})),
        )
        .await;
    let renamed = harness.response(json!("rename")).await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Unicode 名称"));
    assert!(renamed["result"].get("active_revision").is_none());
    assert_eq!(renamed["result"]["session"]["model"], json!("fake"));
    assert_eq!(renamed["result"]["session"]["reasoning"], json!("medium"));

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
    assert_eq!(
        reopened["result"]["session"]["title"],
        json!("Unicode 名称")
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_supports_unloaded_sessions_and_clears_title() {
    let (agent, base, workspace) = test_agent("rename-unloaded", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

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
            json!("rename-unloaded"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "  Unloaded  "})),
        )
        .await;
    let renamed = harness.response(json!("rename-unloaded")).await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Unloaded"));
    assert_eq!(renamed["result"]["session"]["loaded"], json!(false));

    harness
        .send(
            json!("clear"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": " \t "})),
        )
        .await;
    let cleared = harness.response(json!("clear")).await;
    assert!(cleared["result"]["session"]["title"].is_null());

    harness
        .send(
            json!("open"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let reopened = harness.response(json!("open")).await;
    assert!(reopened["result"]["session"]["title"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_rejects_invalid_titles_and_unknown_fields() {
    let (agent, base, workspace) = test_agent("rename-invalid", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let titles = ["bad\nname", "bad\0name", &"x".repeat(4_097)];
    for (index, title) in titles.iter().enumerate() {
        let id = json!(format!("invalid-{index}"));
        harness
            .send(
                id.clone(),
                "session.rename",
                Some(json!({"session_id": session_id, "title": title})),
            )
            .await;
        let response = harness.response(id).await;
        assert_eq!(response["error"]["code"], json!(-32602));
        assert_eq!(response["error"]["data"]["kind"], json!("invalid_params"));
        assert!(!response.to_string().contains("bad"));
    }

    harness
        .send(
            json!("unknown"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "ok", "extra": true})),
        )
        .await;
    let unknown = harness.response(json!("unknown")).await;
    assert_eq!(unknown["error"]["code"], json!(-32602));

    let missing_id = SessionId::new().unwrap();
    harness
        .send(
            json!("missing"),
            "session.rename",
            Some(json!({
                "session_id": missing_id,
                "title": "missing"
            })),
        )
        .await;
    let missing = harness.response(json!("missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32001));
    assert_eq!(missing["error"]["data"]["kind"], json!("session_not_found"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_write_failure_does_not_change_loaded_state() {
    let (agent, base, workspace) =
        test_agent("rename-write-failure", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    fail_next_record_write(typed_id);

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "not persisted"})),
        )
        .await;
    let failure = harness.response(json!("rename")).await;
    assert_eq!(failure["error"]["code"], json!(-32011));
    assert_eq!(failure["error"]["data"]["kind"], json!("store_error"));
    assert!(!failure.to_string().contains("not persisted"));

    harness
        .send(json!("list"), "session.list", Some(json!({})))
        .await;
    let listed = harness.response(json!("list")).await;
    assert_eq!(
        listed["result"]["sessions"][0]["title"],
        json!("rpc session")
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_is_allowed_while_a_loop_is_busy() {
    let (agent, base, workspace) = test_agent(
        "rename-busy-rpc",
        [ModelScript::Block],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "busy"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "Busy title"})),
        )
        .await;
    let renamed = harness.response(json!("rename")).await;
    assert_eq!(renamed["result"]["session"]["title"], json!("Busy title"));
    assert!(renamed["result"].get("active_revision").is_none());

    harness
        .send(
            json!("cancel"),
            "turn.cancel",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"]
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    harness
        .send(
            json!("wait"),
            "turn.wait",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"]
            })),
        )
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["persistence"], json!("persisted"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_rename_is_allowed_for_a_blocked_session() {
    let (agent, base, workspace) = test_agent(
        "rename-blocked-rpc",
        [ModelScript::Text("done")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    fail_next_append(typed_id);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "block"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(
            json!("wait"),
            "turn.wait",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"]
            })),
        )
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    harness
        .send(
            json!("rename"),
            "session.rename",
            Some(json!({"session_id": session_id, "title": "Blocked title"})),
        )
        .await;
    let renamed = harness.response(json!("rename")).await;
    assert_eq!(
        renamed["result"]["session"]["title"],
        json!("Blocked title")
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

async fn assert_manual_failure_case(label: &str, script: ModelScript, expected: &str) {
    let (agent, base, workspace) = test_agent(
        label,
        [ModelScript::Text("settled"), script],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": format!("compact-{label}")
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!(expected));
    harness.shutdown().await;
    remove_base(&base).await;
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
            json!("presentation"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let presentation = harness.response(json!("presentation")).await;
    assert_eq!(presentation["result"]["session_id"], session_id);
    assert_eq!(presentation["result"]["model_label"], json!("fake"));
    assert_eq!(presentation["result"]["context"]["kind"], json!("unknown"));
    assert!(presentation["result"]["context"]["tokens"].is_null());
    assert!(presentation["result"]["cost_usd"].is_null());
    assert!(presentation["result"]["using_subscription"].is_null());
    assert!(presentation["result"]["git_branch"].is_null());
    assert!(presentation["result"]["last_loop"].is_null());

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
    assert!(sent["result"]["accepted_at"].is_string());
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
            json!("presentation-after"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let presentation_after = harness.response(json!("presentation-after")).await;
    assert_eq!(
        presentation_after["result"]["last_loop"]["loop_id"],
        turn["loop_id"]
    );
    assert!(presentation_after["result"]["last_loop"]["started_at"].is_string());
    assert!(presentation_after["result"]["last_loop"]["finished_at"].is_string());

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
async fn manual_compaction_generates_no_tools_summary_and_reopens_atomic_snapshot() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-compaction-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();

    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut scripts = vec![
        ModelScript::Text("settled answer one"),
        ModelScript::Text("settled answer two"),
        ModelScript::Text("settled answer three"),
    ];
    scripts.extend((0..64).map(|_| ModelScript::Observe {
        calls: Arc::clone(&observations),
        text: "manual compact summary",
    }));
    let model = FakeModel::new(scripts);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &["read"], ApprovalMode::Auto),
        test_models(model),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let settled_texts = (0..3)
        .map(|index| format!("settled-{index} {}", "bounded settled history ".repeat(128)))
        .collect::<Vec<_>>();
    let mut original_loop_id = None;
    for (index, text) in settled_texts.iter().enumerate() {
        let send_id = json!(format!("settled-send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({"session_id": session_id, "text": text})),
            )
            .await;
        let sent = harness.response(send_id).await;
        let turn = sent["result"]["turn"].clone();
        if original_loop_id.is_none() {
            original_loop_id = Some(turn["loop_id"].as_str().unwrap().to_owned());
        }
        let wait_id = json!(format!("settled-wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        let waited = harness.response(wait_id).await;
        assert_eq!(waited["result"]["persistence"], json!("persisted"));
    }
    let original_loop_id = original_loop_id.expect("at least one settled turn is required");

    let session_key = session_id.as_str().unwrap();
    let session_dir = base.join("data").join("sessions").join(session_key);
    let history_path = session_dir.join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-test-1"
            })),
        )
        .await;
    let compacted = harness.response(json!("compact")).await;
    assert_eq!(compacted["result"]["status"], json!("compacted"));

    harness
        .send(
            json!("state-after-compact"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert!(harness.response(json!("state-after-compact")).await["result"]["compaction"].is_null());

    let summary_path = session_dir.join("summary.json");
    assert!(summary_path.is_file());
    let summary: Value = serde_json::from_slice(&std::fs::read(&summary_path).unwrap()).unwrap();
    assert_eq!(summary["session_id"], session_id);
    assert!(
        summary["summary"]
            .as_str()
            .is_some_and(|text| text.contains("manual compact summary"))
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

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
    assert_eq!(
        harness.response(json!("reopen")).await["result"]["session"]["session_id"],
        session_id
    );

    let current_user = "current-after-reopen";
    harness
        .send(
            json!("next-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": current_user})),
        )
        .await;
    let next_turn = harness.response(json!("next-send")).await["result"]["turn"].clone();
    harness
        .send(
            json!("next-wait"),
            "turn.wait",
            Some(turn_params(&next_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("next-wait")).await["result"]["persistence"],
        json!("persisted")
    );

    {
        let observations = observations.lock().unwrap();
        let utility = observations
            .iter()
            .find(|call| {
                call.request.messages().iter().any(|message| {
                    matches!(message, ModelMessage::User(text) if text.contains("settled-0"))
                }) && !call.request.messages().iter().any(|message| {
                    matches!(message, ModelMessage::User(text) if text == current_user)
                })
            })
            .expect("manual compaction must issue an observable utility request");
        assert!(utility.request.tools().is_empty());
        assert!(utility.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::System(text) if text.contains("read") && text.contains("input_schema"))
        }));
        assert_eq!(utility.context.request_index, 0);
        assert_ne!(utility.context.loop_id.to_string(), original_loop_id);
        assert!(
            !utility
                .request
                .messages()
                .iter()
                .any(|message| { matches!(message, ModelMessage::Tool { .. }) })
        );
        assert!(!utility.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text == current_user)
        }));

        let next = observations
            .iter()
            .find(|call| {
                call.request.messages().iter().any(
                    |message| matches!(message, ModelMessage::User(text) if text == current_user),
                )
            })
            .expect("reopened session must issue the next real model request");
        let systems = next
            .request
            .messages()
            .iter()
            .filter_map(|message| match message {
                ModelMessage::System(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        assert!(systems[0].contains("RPC test system prompt"));
        assert!(!systems[0].contains("manual compact summary"));
        assert!(!systems[0].contains(current_user));
        let summary_message = next
            .request
            .messages()
            .iter()
            .find_map(|message| match message {
                ModelMessage::User(text) if text.contains("manual compact summary") => Some(text),
                _ => None,
            })
            .expect("next real request must consume the summary data message");
        assert!(summary_message.starts_with("[BEGIN MINICORE HISTORICAL SUMMARY DATA]"));
        assert!(summary_message.contains("not a new user instruction"));
        assert!(summary_message.ends_with("[END MINICORE HISTORICAL SUMMARY DATA]"));
        assert!(next.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text == current_user)
        }));
        assert!(!next.request.tools().is_empty());
        assert!(!next.request.messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text.contains("settled-0"))
        }));
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_completion_clears_authoritative_busy_state() {
    let (agent, base, workspace) =
        test_agent("manual-completion-state", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-completion-state"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("noop"));

    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert!(
        state["result"]["compaction"].is_null(),
        "completed compaction must clear authoritative busy state: {state}"
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_rejects_fixed_schema_budget_before_model_call() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-fixed-budget-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = FakeModel::with_context_window(
        [ModelScript::Text("settled"), ModelScript::Text("unused")],
        256,
    );
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &["read"], ApprovalMode::Auto),
        test_models(Arc::clone(&model)),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "fixed-budget"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("budget_exceeded"));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_budget_counts_utf8_and_json_escaping() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-utf8-budget-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let model = FakeModel::new([
        ModelScript::Text("settled"),
        ModelScript::Observe {
            calls: Arc::clone(&observations),
            text: "summary",
        },
    ]);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &[], ApprovalMode::Auto),
        test_models(Arc::clone(&model)),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let text = "é🙂\n\t\\\" escaped ".repeat(2_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utf8-budget"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    {
        let observations = observations.lock().unwrap();
        assert!(observations.iter().any(|call| {
            call.request.messages().iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.contains("é🙂") && text.contains("\\n") && text.contains("\\\\"))
            })
        }));
        assert!(
            observations
                .iter()
                .all(|call| call.request.tools().is_empty())
        );
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_chunks_large_source_and_merges_within_call_bound() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-chunk-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut scripts = vec![ModelScript::Text("settled")];
    scripts.extend((0..32).map(|_| ModelScript::Observe {
        calls: Arc::clone(&observations),
        text: "partial summary",
    }));
    let model = FakeModel::new(scripts);
    let agent = Agent::open_with_models(
        test_config(base.join("data"), &[], ApprovalMode::Auto),
        test_models(model),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let large_text = format!("large-history {}", "utf8 source ".repeat(4_000));
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": large_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-chunk"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    assert!(result["result"]["before_tokens"].is_number());
    assert!(result["result"]["after_tokens"].is_number());
    assert!(
        result["result"]["after_tokens"].as_u64().unwrap()
            < result["result"]["before_tokens"].as_u64().unwrap()
    );
    {
        let observations = observations.lock().unwrap();
        assert!(observations.len() >= 2);
        assert!(observations.len() <= 32);
        assert!(observations.iter().all(|call| {
            call.request.tools().is_empty()
                && call.context.request_index == 0
                && !call
                    .request
                    .messages()
                    .iter()
                    .any(|message| matches!(message, ModelMessage::Tool { .. }))
        }));
        assert!(observations.iter().any(|call| {
            call.request.messages().iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.contains("MINICORE HISTORICAL SOURCE DATA"))
            })
        }));
        assert!(observations.iter().any(|call| {
            call.request.messages().iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text.contains("MINICORE SUMMARY PARTS DATA"))
            })
        }));
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_empty_history_is_a_noop_without_a_model_call() {
    let (agent, base, workspace) = test_agent("manual-noop", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("invalid"),
            "session.compact",
            Some(json!({"session_id": session_id, "operation_id": ""})),
        )
        .await;
    assert_eq!(
        harness.response(json!("invalid")).await["error"]["data"]["kind"],
        json!("invalid_params")
    );

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-noop"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("noop"));
    assert_eq!(result["result"]["covered_item_count"], json!(0));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_rejects_busy_and_blocked_sessions() {
    let (agent, base, workspace) = test_agent(
        "manual-admission",
        [ModelScript::Block],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "busy"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-busy"
            })),
        )
        .await;
    let busy = harness.response(json!("compact")).await;
    assert_eq!(busy["error"]["data"]["kind"], json!("session_busy"));

    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    // A persistence failure blocks the Session, and compaction must preserve
    // the existing blocked admission semantics.
    let (agent, base2, workspace2) = test_agent(
        "manual-blocked",
        [ModelScript::Text("done")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut blocked = RpcHarness::spawn(agent);
    let blocked_id = create_and_open(&mut blocked, &workspace2).await;
    fail_next_append(blocked_id.as_str().unwrap().parse().unwrap());
    blocked
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": blocked_id, "text": "block"})),
        )
        .await;
    let blocked_turn = blocked.response(json!("send")).await["result"]["turn"].clone();
    blocked
        .send(json!("wait"), "turn.wait", Some(turn_params(&blocked_turn)))
        .await;
    let _ = blocked.response(json!("wait")).await;
    blocked
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": blocked_id,
                "operation_id": "compact-blocked"
            })),
        )
        .await;
    let rejected = blocked.response(json!("compact")).await;
    assert_eq!(rejected["error"]["data"]["kind"], json!("session_blocked"));

    harness.shutdown().await;
    blocked.shutdown().await;
    remove_base(&base).await;
    remove_base(&base2).await;
}

#[tokio::test]
async fn manual_compaction_cancel_is_exact_and_keeps_ping_responsive() {
    let (agent, base, workspace) = test_agent(
        "manual-cancel",
        [
            ModelScript::Text("settled"),
            ModelScript::Block,
            ModelScript::Text("summary"),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(4_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-1"
            })),
        )
        .await;
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    harness
        .send(
            json!("duplicate"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-2"
            })),
        )
        .await;
    let duplicate = harness.response(json!("duplicate")).await;
    assert_eq!(duplicate["error"]["data"]["kind"], json!("session_busy"));

    harness
        .send(
            json!("busy-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "not admitted"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("busy-send")).await["error"]["data"]["kind"],
        json!("session_busy")
    );
    harness
        .send(
            json!("busy-update"),
            "session.update",
            Some(json!({"session_id": session_id, "reasoning": "high"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("busy-update")).await["error"]["data"]["kind"],
        json!("session_busy")
    );

    harness
        .send(
            json!("wrong"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-old"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("wrong")).await["result"]["cancelled"],
        json!(false)
    );
    harness
        .send(
            json!("cancel"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-cancel-1"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("cancelled"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_keeps_owned_handle_until_delayed_worker_drain() {
    let (agent, base, workspace) = test_agent(
        "manual-owned-handle",
        [ModelScript::Text("settled"), ModelScript::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    let gate = Arc::new(WorkerGate::new());
    pause_next_compaction_after_result(
        session_id.as_str().unwrap().parse().unwrap(),
        Arc::clone(&gate),
    );
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-owned-handle"
            })),
        )
        .await;
    gate.wait_started().await;
    assert_eq!(
        harness.response(json!("compact")).await["result"]["status"],
        json!("compacted")
    );

    harness
        .send(
            json!("send-busy"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "must wait"})),
        )
        .await;
    let pending = tokio::time::timeout(
        Duration::from_millis(25),
        harness.response(json!("send-busy")),
    )
    .await;
    assert!(
        pending.is_err(),
        "next work must wait for the owned worker join"
    );
    gate.release();
    assert!(harness.response(json!("send-busy")).await["result"]["turn"].is_object());
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

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_write_failure_preserves_history_and_old_snapshot() {
    let (agent, base, workspace) = test_agent(
        "manual-write-failure",
        [
            ModelScript::Text("settled"),
            ModelScript::Text("initial summary"),
            ModelScript::Text("new turn answer"),
            ModelScript::Text("new summary"),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    let session_dir = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap());
    let history_path = session_dir.join("history.jsonl");
    harness
        .send(
            json!("compact-initial"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-initial"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("compact-initial")).await["result"]["status"],
        json!("compacted")
    );
    let summary_path = session_dir.join("summary.json");
    let summary_before = std::fs::read(&summary_path).unwrap();

    harness
        .send(
            json!("next-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "new turn"})),
        )
        .await;
    let next_turn = harness.response(json!("next-send")).await["result"]["turn"].clone();
    harness
        .send(
            json!("next-wait"),
            "turn.wait",
            Some(turn_params(&next_turn)),
        )
        .await;
    let _ = harness.response(json!("next-wait")).await;
    let history_before = std::fs::read(&history_path).unwrap();
    let typed_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    fail_next_summary_write(typed_id);

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-write-failure"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("store"));
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_before);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_rejects_model_failures_and_tool_events() {
    let (agent, base, workspace) = test_agent(
        "manual-model-failure",
        [ModelScript::Text("settled"), ModelScript::Fail],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-model-failure"
            })),
        )
        .await;
    let failure = harness.response(json!("compact")).await;
    assert_eq!(failure["result"]["status"], json!("failed"));
    assert_eq!(failure["result"]["failure_kind"], json!("model_failure"));
    harness.shutdown().await;
    remove_base(&base).await;

    let (agent, base, workspace) = test_agent(
        "manual-tool-event",
        [
            ModelScript::Text("settled"),
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "read",
                arguments: json!({}),
            }]),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-tool-event"
            })),
        )
        .await;
    let failure = harness.response(json!("compact")).await;
    assert_eq!(failure["result"]["status"], json!("failed"));
    assert_eq!(
        failure["result"]["failure_kind"],
        json!("tool_call_rejected")
    );
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_reports_unknown_write_without_publishing_state() {
    let (agent, base, workspace) = test_agent(
        "manual-unknown-write",
        [ModelScript::Text("settled"), ModelScript::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    force_unknown_summary_write(session_id.as_str().unwrap().parse().unwrap());
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-unknown-write"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("unknown_write"));
    assert_eq!(
        result["result"]["failure_kind"],
        json!("write_outcome_unknown")
    );
    assert!(
        !base
            .join("data")
            .join("sessions")
            .join(session_id.as_str().unwrap())
            .join("summary.json")
            .exists()
    );
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_revalidates_history_before_commit() {
    let (agent, base, workspace) = test_agent(
        "manual-revalidate",
        [ModelScript::Text("settled"), ModelScript::Text("summary")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "settled"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let newline = history_before
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("settled history has one complete line");
    let first_line = history_before[..=newline].to_vec();
    let gate = Arc::new(SummaryCommitGate::new());
    gate_next_summary_commit(
        session_id.as_str().unwrap().parse().unwrap(),
        Arc::clone(&gate),
    );
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-revalidate"
            })),
        )
        .await;
    gate.wait_started().await;
    let mut mutated = history_before.clone();
    mutated.extend(first_line);
    std::fs::write(&history_path, mutated).unwrap();
    gate.release();
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("history_changed"));
    assert!(!history_path.parent().unwrap().join("summary.json").exists());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_deadline_covers_commit_wait() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-manual-deadline-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.loop_options.model_timeout_seconds = Some(1);
    let model = FakeModel::new([ModelScript::Text("settled"), ModelScript::Text("summary")]);
    let agent = Agent::open_with_models(config, test_models(model))
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled_text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    let gate = Arc::new(SummaryCommitGate::new());
    gate_next_summary_commit(
        session_id.as_str().unwrap().parse().unwrap(),
        Arc::clone(&gate),
    );
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-deadline"
            })),
        )
        .await;
    gate.wait_started().await;
    let result = tokio::time::timeout(Duration::from_secs(3), harness.response(json!("compact")))
        .await
        .expect("commit wait must be bounded by the operation deadline");
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("timeout"));
    gate.release();

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_rejects_incomplete_late_empty_and_oversize_output() {
    assert_manual_failure_case(
        "manual-no-finish",
        ModelScript::NoFinish,
        "invalid_model_response",
    )
    .await;
    assert_manual_failure_case(
        "manual-wrong-finish",
        ModelScript::WrongFinish,
        "invalid_model_response",
    )
    .await;
    assert_manual_failure_case(
        "manual-late-content",
        ModelScript::LateContent,
        "invalid_model_response",
    )
    .await;
    assert_manual_failure_case("manual-whitespace", ModelScript::Whitespace, "no_progress").await;
    assert_manual_failure_case("manual-oversize", ModelScript::Oversize, "too_large").await;
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
    assert_eq!(steer["result"]["ok"], json!(true));
    assert!(steer["result"]["accepted_at"].is_string());

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
    let presentation_event = harness.event("tool_presentation").await;
    assert_eq!(
        presentation_event["params"]["data"]["turn"]["session_id"],
        session_id
    );
    assert_eq!(
        presentation_event["params"]["data"]["request_index"],
        json!(0)
    );
    assert_eq!(
        presentation_event["params"]["data"]["display"]["detail"],
        json!("secret.txt")
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    // Whitelisted path detail is available to the local TUI, but raw tool
    // argument object fields are not.
    assert!(serialized.contains("secret.txt"));
    assert!(!serialized.contains("arguments"));
    assert!(!serialized.contains("\"path\""));
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
async fn subagent_tool_runs_a_stateless_child_and_reports_stage_metadata() {
    let (agent, base, workspace) = test_agent(
        "subagent-single",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "subagent",
                arguments: json!({
                    "model": null,
                    "reasoning": null,
                    "task": "Answer from the child",
                    "tasks": null,
                    "chain": null,
                    "cwd": null
                }),
            }]),
            ModelScript::Text("child answer"),
            ModelScript::Text("parent answer"),
        ],
        &["subagent"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "delegate"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    let progress = harness.event("tool_progress").await;
    assert!(
        progress["params"]["data"]["progress"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("stage 1"))
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    assert!(serialized.contains("child answer"));
    assert!(serialized.contains("\\\"status\\\":\\\"completed\\\""));
    assert!(serialized.contains("\\\"model\\\":\\\"fake\\\""));
    assert!(serialized.contains("\\\"loop_id\\\":\\\"lup_"));
    assert!(serialized.contains("\\\"usage\\\""));

    harness
        .send(json!("sessions"), "session.list", Some(json!({})))
        .await;
    let sessions = harness.response(json!("sessions")).await;
    assert_eq!(sessions["result"]["sessions"].as_array().unwrap().len(), 1);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn subagent_real_child_without_usage_serializes_null_usage() {
    let (agent, base, workspace) = test_agent(
        "subagent-no-usage",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "subagent",
                arguments: json!({
                    "model": null,
                    "reasoning": null,
                    "task": "child without usage",
                    "tasks": null,
                    "chain": null,
                    "cwd": null
                }),
            }]),
            ModelScript::NoUsage("child answer"),
            ModelScript::Text("parent answer"),
        ],
        &["subagent"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "delegate"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let tool_result = history["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["item"]["type"].as_str() == Some("tool_result"))
        .unwrap();
    let details: Value =
        serde_json::from_str(tool_result["item"]["data"]["content"].as_str().unwrap()).unwrap();
    assert!(details["stages"][0]["usage"].is_null());
    assert!(details["usage"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn subagent_chain_replaces_previous_and_reports_completion() {
    let inputs = Arc::new(Mutex::new(Vec::new()));
    let (agent, base, workspace) = test_agent(
        "subagent-chain",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "subagent",
                arguments: json!({
                    "model": null,
                    "reasoning": null,
                    "task": null,
                    "tasks": null,
                    "chain": [
                        {"model": null, "reasoning": null, "task": "first", "cwd": null},
                        {"model": null, "reasoning": null, "task": "second {previous}", "cwd": null},
                        {"model": null, "reasoning": null, "task": "third {previous}", "cwd": null}
                    ],
                    "cwd": null
                }),
            }]),
            ModelScript::Capture {
                inputs: Arc::clone(&inputs),
                text: "first output",
            },
            ModelScript::Capture {
                inputs: Arc::clone(&inputs),
                text: "second output",
            },
            ModelScript::Capture {
                inputs: Arc::clone(&inputs),
                text: "third output",
            },
            ModelScript::Text("parent answer"),
        ],
        &["subagent"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "delegate"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    assert!(serialized.contains("\\\"mode\\\":\\\"chain\\\""));
    assert!(serialized.contains("first output"));
    assert!(
        serialized.contains("second output"),
        "subagent result: {serialized}"
    );
    assert!(serialized.contains("third output"));
    let tool_result = history["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["item"]["type"].as_str() == Some("tool_result"))
        .unwrap();
    let details: Value = serde_json::from_str(
        tool_result["item"]["data"]["content"]
            .as_str()
            .expect("subagent details are serialized as tool output"),
    )
    .unwrap();
    assert_eq!(details["status"], "completed");
    assert_eq!(details["stages"][0]["status"], "completed");
    assert_eq!(details["stages"][0]["output"], "first output");
    assert_eq!(details["stages"][1]["status"], "completed");
    assert_eq!(details["stages"][1]["output"], "second output");
    assert_eq!(details["stages"][2]["status"], "completed");
    assert_eq!(details["stages"][2]["output"], "third output");
    assert_eq!(
        inputs.lock().unwrap().clone(),
        vec![
            "first".to_owned(),
            "second first output".to_owned(),
            "third second output".to_owned()
        ]
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn subagent_chain_stops_after_failure_and_marks_remaining_stages_skipped() {
    let parent_inputs = Arc::new(Mutex::new(Vec::new()));
    let (agent, base, workspace) = test_agent(
        "subagent-chain-failure",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "subagent",
                arguments: json!({
                    "model": null,
                    "reasoning": null,
                    "task": null,
                    "tasks": null,
                    "chain": [
                        {"model": null, "reasoning": null, "task": "first", "cwd": null},
                        {"model": null, "reasoning": null, "task": "second {previous}", "cwd": null},
                        {"model": null, "reasoning": null, "task": "third {previous}", "cwd": null}
                    ],
                    "cwd": null
                }),
            }]),
            ModelScript::Fail,
            ModelScript::Capture {
                inputs: Arc::clone(&parent_inputs),
                text: "parent answer",
            },
        ],
        &["subagent"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "delegate"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    assert!(serialized.contains("\\\"status\\\":\\\"failed\\\""));
    assert!(serialized.contains("\\\"status\\\":\\\"skipped\\\""));
    let tool_result = history["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["item"]["type"].as_str() == Some("tool_result"))
        .unwrap();
    let details: Value = serde_json::from_str(
        tool_result["item"]["data"]["content"]
            .as_str()
            .expect("subagent details are serialized as tool output"),
    )
    .unwrap();
    assert_eq!(details["status"], "failed");
    assert_eq!(details["stages"][0]["status"], "failed");
    assert_eq!(details["stages"][0]["error"], "child model failed");
    assert_eq!(details["stages"][1]["status"], "skipped");
    assert_eq!(details["stages"][2]["status"], "skipped");
    assert_eq!(
        parent_inputs.lock().unwrap().clone(),
        vec!["delegate".to_owned()]
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn subagent_parallel_starts_at_most_four_workers() {
    let probe = ConcurrencyProbe::new();
    let tasks = (0..8)
        .map(|index| {
            json!({
                "model": null,
                "reasoning": null,
                "task": format!("child {index}"),
                "cwd": null
            })
        })
        .collect::<Vec<_>>();
    let mut scripts = vec![ModelScript::ToolCalls(vec![ToolCallScript {
        name: "subagent",
        arguments: json!({
            "model": null,
            "reasoning": null,
            "task": null,
            "tasks": tasks,
            "chain": null,
            "cwd": null
        }),
    }])];
    scripts.extend((0..8).map(|_| ModelScript::Gate(Arc::clone(&probe))));
    scripts.push(ModelScript::Text("parent answer"));
    let (agent, base, workspace) = test_agent(
        "subagent-concurrency",
        scripts,
        &["subagent"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "delegate"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;

    tokio::time::timeout(TIMEOUT, async {
        while probe.started.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("four child model requests did not start");
    assert_eq!(probe.started.load(Ordering::SeqCst), 4);
    assert_eq!(probe.active.load(Ordering::SeqCst), 4);
    assert_eq!(probe.max_active.load(Ordering::SeqCst), 4);

    probe.release.cancel();
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );
    assert_eq!(probe.started.load(Ordering::SeqCst), 8);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    assert_eq!(probe.max_active.load(Ordering::SeqCst), 4);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn subagent_tool_runs_parallel_tasks_with_a_bounded_stage_count() {
    let (agent, base, workspace) = test_agent(
        "subagent-parallel",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "subagent",
                arguments: json!({
                    "model": null,
                    "reasoning": null,
                    "task": null,
                    "tasks": [
                        {"model": null, "reasoning": null, "task": "first child", "cwd": null},
                        {"model": null, "reasoning": null, "task": "second child", "cwd": null}
                    ],
                    "chain": null,
                    "cwd": null
                }),
            }]),
            ModelScript::Text("first answer"),
            ModelScript::Text("second answer"),
            ModelScript::Text("parent answer"),
        ],
        &["subagent"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "delegate"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );

    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session_id, "offset": 0, "limit": 100})),
        )
        .await;
    let history = harness.response(json!("history")).await;
    let serialized = history["result"].to_string();
    assert!(serialized.contains("first answer"));
    assert!(serialized.contains("second answer"));
    assert!(serialized.contains("\\\"stages\\\":["));
    assert!(serialized.contains("\\\"stage_index\\\":1"));
    assert!(serialized.contains("\\\"stage_index\\\":2"));

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
