use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::pending;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream;
use serde_json::{Value, json};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream,
    ReadBuf,
};
use tokio::sync::Notify;
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
use crate::config::{AgentConfig, CompactionConfig, LoopOverrides, Profile};
use crate::error::AgentError;
use crate::models::{ModelConfig, Models};
use crate::profiles::ApprovalMode;
use crate::sessions::{WorkerGate, pause_next_compaction_after_result};
use crate::store::{
    SummaryCommitGate, fail_next_append, fail_next_record_write, fail_next_summary_write,
    force_unknown_summary_write, gate_next_summary_commit,
};
use crate::workspace::query::{WorkspaceReadGate, gate_next_workspace_read};
use crate::workspace::scan::{ScanHold, hold_next_scan};

use super::{
    Frame, MAX_DEFERRED_QUERIES, MAX_DEFERRED_WAITERS, MAX_RPC_LINE_BYTES, RpcServer, read_frame,
    run_with_io,
};

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum ModelScript {
    Text(&'static str),
    TextWithUsage(&'static str, u64, u64),
    DuplicateUsage(&'static str),
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
    /// Emits usage, then fails. The observed usage must be retained as a
    /// known partial total rather than reported as unknown.
    UsageThenFail(u64, u64),
    Gate(Arc<ConcurrencyProbe>),
    ToolCalls(Vec<ToolCallScript>),
    NoFinish,
    WrongFinish,
    LateContent,
    Whitespace,
    BlockWithSignal(Arc<Notify>),
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
                ModelScript::TextWithUsage(text, input, output) => Ok(model_events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(input, output, 0),
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ])),
                ModelScript::DuplicateUsage(text) => Ok(model_events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(7, 11, 0),
                    },
                    ModelEvent::Usage {
                        usage: Usage::new(13, 17, 0),
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
                ModelScript::UsageThenFail(input, output) => Ok(Box::pin(stream::iter(vec![
                    Ok(ModelEvent::Usage {
                        usage: Usage::new(input, output, 0),
                    }),
                    Err(fake_model_error()),
                ])) as ModelStream),
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
                ModelScript::BlockWithSignal(started) => {
                    started.notify_one();
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
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
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

async fn test_agent_with_auto(
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
    let mut config = test_config(base.join("data"), tools, approval);
    config.compaction = CompactionConfig {
        enabled: true,
        trigger_percent: 80,
        target_percent: 50,
    };
    let agent = Agent::open_with_models(config, test_models(model))
        .await
        .unwrap();
    (agent, base, workspace)
}

struct RpcHarness {
    input: Option<Box<dyn AsyncWrite + Unpin + Send>>,
    output: BufReader<DuplexStream>,
    task: Option<JoinHandle<Result<(), AgentError>>>,
    pending: VecDeque<Value>,
    events: Vec<Value>,
    observed: Vec<Value>,
}

impl RpcHarness {
    fn spawn(agent: Agent) -> Self {
        let (client_input, server_input) = tokio::io::duplex(2 * 1024 * 1024);
        Self::spawn_with(agent, BufReader::new(server_input), Box::new(client_input))
    }

    /// Spawns the server over a caller-supplied reader, which lets a test
    /// observe exactly when the server has consumed input.
    fn spawn_with<R>(agent: Agent, reader: R, input: Box<dyn AsyncWrite + Unpin + Send>) -> Self
    where
        R: AsyncBufRead + Send + Unpin + 'static,
    {
        let (server_output, client_output) = tokio::io::duplex(2 * 1024 * 1024);
        let task = tokio::spawn(run_with_io(
            agent,
            reader,
            server_output,
            pending::<io::Result<()>>(),
        ));
        Self {
            input: Some(input),
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
    assert_eq!(ping["result"]["protocol_version"], json!(1));
    assert_eq!(
        ping["result"]["capabilities"],
        json!([
            "session.read",
            "session.context",
            "turn.result",
            "tool.read",
            "tool.output",
            "session.history",
            "workspace.read",
            "workspace.files",
            "workspace.search",
            "deferred.waiter_limit"
        ])
    );

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

async fn assert_manual_failure_case(label: &str, script: ModelScript, expected: &str) -> Value {
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
    result
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
async fn session_context_reports_manual_busy_state_and_last_result() {
    let utility_started = Arc::new(Notify::new());
    let (agent, base, workspace) = test_agent(
        "context-manual-busy",
        [
            ModelScript::Text("settled"),
            ModelScript::BlockWithSignal(Arc::clone(&utility_started)),
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
                "operation_id": "context-busy-operation"
            })),
        )
        .await;
    // Wait until the manual utility call has really started; otherwise the
    // cancel races the utility and the observed usage total is not
    // deterministic.
    tokio::time::timeout(TIMEOUT, utility_started.notified())
        .await
        .expect("manual utility call must be started before cancelling");
    harness
        .send(
            json!("context-busy"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context-busy")).await;
    assert_eq!(
        context["result"]["current_operation"]["operation_id"],
        json!("context-busy-operation")
    );
    assert_eq!(
        context["result"]["coverage"]["covered_item_count"],
        json!(0)
    );
    assert!(context["result"]["last_result"].is_null());
    assert!(context["result"]["budget"]["estimated_history_items"].is_number());
    assert_eq!(
        context["result"]["budget"]["within_runtime_limits"],
        json!(true)
    );

    harness
        .send(
            json!("cancel"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "context-busy-operation"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    // The utility call really started before the cancel, so it is counted as
    // one incomplete call and its yet-unknown usage stays absent.
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(false));
    assert!(result["result"]["utility_usage"]["usage"].is_null());

    harness
        .send(
            json!("context-after"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context-after")).await;
    assert!(context["result"]["current_operation"].is_null());
    assert_eq!(
        context["result"]["last_result"]["operation_id"],
        json!("context-busy-operation")
    );
    // `session.context` must expose exactly the utility accounting returned by
    // the compact result.
    assert_eq!(
        context["result"]["last_result"]["utility_usage"],
        result["result"]["utility_usage"]
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_reports_independent_utility_usage() {
    let (agent, base, workspace) = test_agent(
        "context-utility-usage",
        [
            ModelScript::TextWithUsage("settled", 41, 43),
            ModelScript::TextWithUsage("summary", 7, 11),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["usage"]["input_tokens"], json!(41));

    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-usage"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["output_tokens"],
        json!(11)
    );

    harness
        .send(
            json!("context"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context")).await;
    assert_eq!(
        context["result"]["last_result"]["utility_usage"]["call_count"],
        json!(1)
    );
    assert_eq!(
        context["result"]["last_result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );
    assert_eq!(
        context["result"]["last_result"]["utility_usage"]["usage"]["output_tokens"],
        json!(11)
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn failed_manual_utility_stream_keeps_known_partial_usage() {
    let (agent, base, workspace) = test_agent(
        "context-utility-partial",
        [
            ModelScript::TextWithUsage("settled", 41, 43),
            ModelScript::UsageThenFail(7, 11),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
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
                "operation_id": "utility-partial"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(false));
    // The failed stream's own usage is a known partial total, not unknown.
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
    );
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["output_tokens"],
        json!(11)
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn missing_manual_utility_usage_remains_unknown() {
    let (agent, base, workspace) = test_agent(
        "context-utility-unknown",
        [
            ModelScript::TextWithUsage("settled", 41, 43),
            ModelScript::NoUsage("summary"),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
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
                "operation_id": "utility-unknown"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(false));
    assert!(result["result"]["utility_usage"]["usage"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_aggregates_usage_across_multiple_utility_calls() {
    let mut scripts = vec![ModelScript::Text("settled")];
    scripts.extend((0..32).map(|_| ModelScript::TextWithUsage("summary-part", 7, 11)));
    let (agent, base, workspace) =
        test_agent("context-utility-multiple", scripts, &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled = "settled history ".repeat(7_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
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
                "operation_id": "utility-multiple"
            })),
        )
        .await;
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("compacted"));
    let call_count = result["result"]["utility_usage"]["call_count"]
        .as_u64()
        .expect("utility call count must be reported");
    assert!(call_count > 1);
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(call_count * 7)
    );
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["output_tokens"],
        json!(call_count * 11)
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn manual_compaction_cancel_after_utility_call_keeps_usage_accounting() {
    let (agent, base, workspace) = test_agent(
        "context-utility-cancel",
        [
            ModelScript::TextWithUsage("settled", 2, 3),
            ModelScript::TextWithUsage("summary", 7, 11),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let settled = "settled history ".repeat(128);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": settled})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    let gate = Arc::new(SummaryCommitGate::new());
    let typed_session_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    gate_next_summary_commit(typed_session_id, Arc::clone(&gate));
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-cancel"
            })),
        )
        .await;
    gate.wait_started().await;
    harness
        .send(
            json!("cancel"),
            "session.compact.cancel",
            Some(json!({
                "session_id": session_id,
                "operation_id": "utility-cancel"
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    gate.release();
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], json!("failed"));
    assert_eq!(result["result"]["failure_kind"], json!("cancelled"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(7)
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
    let result = harness.response(json!("compact")).await;
    assert_eq!(
        result["result"]["status"],
        json!("compacted"),
        "unexpected manual compaction result: {result}"
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
            ModelScript::TextWithUsage("settled", 2, 3),
            ModelScript::TextWithUsage("initial summary", 5, 7),
            ModelScript::TextWithUsage("new turn answer", 11, 13),
            ModelScript::TextWithUsage("new summary", 17, 19),
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
    let initial = harness.response(json!("compact-initial")).await;
    assert_eq!(
        initial["result"]["status"],
        json!("compacted"),
        "unexpected initial compaction result: {initial}"
    );
    let summary_path = session_dir.join("summary.json");
    let summary_before = std::fs::read(&summary_path).unwrap();

    let new_turn_text = "new turn history ".repeat(128);
    harness
        .send(
            json!("next-send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": new_turn_text})),
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
    assert_eq!(
        result["result"]["status"],
        json!("failed"),
        "unexpected write-failure compaction result: {result}"
    );
    assert_eq!(result["result"]["failure_kind"], json!("store"));
    assert_eq!(result["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(result["result"]["utility_usage"]["complete"], json!(true));
    assert_eq!(
        result["result"]["utility_usage"]["usage"]["input_tokens"],
        json!(17)
    );
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
    assert_eq!(
        result["result"]["status"],
        json!("unknown_write"),
        "unexpected unknown-write compaction result: {result}"
    );
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
    if tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .is_err()
    {
        gate.release();
        let response = tokio::time::timeout(TIMEOUT, harness.response(json!("compact"))).await;
        panic!("manual compaction did not reach commit gate: {response:?}");
    }
    let mut mutated = history_before.clone();
    mutated.extend(first_line);
    std::fs::write(&history_path, mutated).unwrap();
    gate.release();
    let result = harness.response(json!("compact")).await;
    assert_eq!(
        result["result"]["status"],
        json!("failed"),
        "unexpected history revalidation result: {result}"
    );
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
    let duplicate = assert_manual_failure_case(
        "manual-duplicate-usage",
        ModelScript::DuplicateUsage("duplicate usage"),
        "invalid_model_response",
    )
    .await;
    assert_eq!(duplicate["result"]["utility_usage"]["call_count"], json!(1));
    assert_eq!(
        duplicate["result"]["utility_usage"]["complete"],
        json!(false)
    );
    assert!(duplicate["result"]["utility_usage"]["usage"].is_null());
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

/// A pull-based `AsyncRead` source fed by a separate writer handle. The server
/// reads whatever bytes have been fed, so a test can split a request across the
/// read boundary. `delivered` counts bytes handed to the server, which makes
/// "the partial prefix has been consumed" directly observable instead of
/// timing-dependent.
struct ChunkedReader {
    shared: Arc<ChunkedShared>,
}

struct ChunkedShared {
    buffer: Mutex<Vec<u8>>,
    fed: AtomicUsize,
    delivered: AtomicUsize,
    delivered_notify: Notify,
    waker: Mutex<Option<std::task::Waker>>,
}

fn chunked_input() -> (ChunkedReader, ChunkedInput) {
    let shared = Arc::new(ChunkedShared {
        buffer: Mutex::new(Vec::new()),
        fed: AtomicUsize::new(0),
        delivered: AtomicUsize::new(0),
        delivered_notify: Notify::new(),
        waker: Mutex::new(None),
    });
    (
        ChunkedReader {
            shared: Arc::clone(&shared),
        },
        ChunkedInput { shared },
    )
}

impl AsyncRead for ChunkedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut buffer = self.shared.buffer.lock().unwrap();
        if buffer.is_empty() {
            *self.shared.waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let take = buffer.len().min(buf.remaining());
        buf.put_slice(&buffer[..take]);
        buffer.drain(..take);
        drop(buffer);
        self.shared.delivered.fetch_add(take, Ordering::SeqCst);
        self.shared.delivered_notify.notify_one();
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone)]
struct ChunkedInput {
    shared: Arc<ChunkedShared>,
}

impl AsyncWrite for ChunkedInput {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Infallible and immediate; `run_with_io` never blocks on this writer.
        self.shared.buffer.lock().unwrap().extend_from_slice(buf);
        self.shared.fed.fetch_add(buf.len(), Ordering::SeqCst);
        if let Some(waker) = self.shared.waker.lock().unwrap().take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl ChunkedInput {
    async fn feed(&self, bytes: &[u8]) {
        self.shared.buffer.lock().unwrap().extend_from_slice(bytes);
        self.shared.fed.fetch_add(bytes.len(), Ordering::SeqCst);
        if let Some(waker) = self.shared.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    /// Total bytes ever written into this source, including earlier requests.
    /// Callers wait for this count so the wait proves the server consumed the
    /// latest bytes, not merely some prefix of previous traffic.
    fn total_fed(&self) -> usize {
        self.shared.fed.load(Ordering::SeqCst)
    }

    /// Waits until at least `target` total bytes have been handed to the server.
    /// `read_frame` copies whatever a successful read returns before it can
    /// yield, so reaching the target proves the prefix is retained in its buffer.
    async fn wait_delivered(&self, target: usize) {
        tokio::time::timeout(TIMEOUT, async {
            while self.shared.delivered.load(Ordering::SeqCst) < target {
                self.shared.delivered_notify.notified().await;
            }
        })
        .await
        .expect("server never consumed the fragmented prefix");
    }
}

/// Fragmented input must survive a `select!` cancellation of the read future.
/// A deferred waiter resolving between two chunks previously discarded the
/// already-consumed prefix, leaving the request unparseable.
#[tokio::test]
async fn fragmented_frame_survives_deferred_waiter_interleaving() {
    let probe = ConcurrencyProbe::new();
    let (agent, base, workspace) = test_agent(
        "half-frame-interleave",
        [
            ModelScript::Gate(Arc::clone(&probe)),
            ModelScript::Text("done"),
        ],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let (reader, input) = chunked_input();
    let mut harness =
        RpcHarness::spawn_with(agent, BufReader::new(reader), Box::new(input.clone()));

    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "gated turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;

    // Confirm the gated model call is actually in flight before relying on the
    // waiter branch of `select!` becoming ready.
    tokio::time::timeout(TIMEOUT, async {
        while probe.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gated model request did not start");

    // Deliver only the first half of the next request and wait until the server
    // has consumed it, so the partial frame is parked inside `read_frame`.
    let ping = json!({"jsonrpc": "2.0", "id": "after", "method": "agent.ping", "params": {}});
    let mut encoded = serde_json::to_vec(&ping).unwrap();
    encoded.push(b'\n');
    let split = encoded.len() / 2;
    input.feed(&encoded[..split]).await;
    input.wait_delivered(input.total_fed()).await;

    // Release the model gate so the deferred `turn.wait` resolves while the
    // partial frame is in flight. The waiter branch of `select!` cancels the
    // in-flight read future.
    probe.release.cancel();
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));

    input.feed(&encoded[split..]).await;
    let ping_response = harness.response(json!("after")).await;
    assert!(ping_response["result"]["version"].is_string());

    harness.shutdown().await;
    remove_base(&base).await;
}

/// Direct regression for the same cancellation: a frame split across a
/// cancelled poll must still be reassembled from the retained prefix.
#[tokio::test]
async fn read_frame_accumulates_across_a_cancelled_poll() {
    let request = br#"{"jsonrpc":"2.0","id":"split","method":"agent.ping","params":{}}"#.to_vec();
    let mut terminated = request.clone();
    terminated.push(b'\n');
    let split = terminated.len() / 2;
    let (reader, input) = chunked_input();
    let mut reader = BufReader::new(reader);
    let mut frame = Vec::new();

    let mut first = Box::pin(read_frame(&mut reader, &mut frame));
    assert!(futures_util::poll!(&mut first).is_pending());
    input.feed(&terminated[..split]).await;
    assert!(futures_util::poll!(&mut first).is_pending());
    input.wait_delivered(input.total_fed()).await;
    drop(first);
    assert_eq!(frame.as_slice(), &terminated[..split]);

    input.feed(&terminated[split..]).await;
    match tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader, &mut frame))
        .await
        .expect("fragmented frame was never completed")
        .unwrap()
    {
        Frame::Data(bytes) => assert_eq!(bytes, terminated),
        Frame::Eof | Frame::Oversized => panic!("fragmented frame was not reconstructed"),
    }
}

/// A frame that exceeds the limit only after an earlier cancellation is still
/// rejected, proving the limit covers the cumulative length.
#[tokio::test]
async fn read_frame_limit_covers_cumulative_length_after_cancellation() {
    let first = vec![b'x'; 1024];
    let second = vec![b'y'; MAX_RPC_LINE_BYTES];
    let (reader, input) = chunked_input();
    let mut reader = BufReader::new(reader);
    let mut frame = Vec::new();

    let mut first_read = Box::pin(read_frame(&mut reader, &mut frame));
    assert!(futures_util::poll!(&mut first_read).is_pending());
    input.feed(&first).await;
    assert!(futures_util::poll!(&mut first_read).is_pending());
    input.wait_delivered(input.total_fed()).await;
    drop(first_read);
    assert_eq!(frame.len(), 1024);

    input.feed(&second).await;
    match tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader, &mut frame))
        .await
        .expect("oversized frame was never observed")
        .unwrap()
    {
        Frame::Oversized => {}
        Frame::Data(bytes) => panic!("oversized frame was accepted: {} bytes", bytes.len()),
        Frame::Eof => panic!("oversized frame reported as EOF"),
    }
    assert!(frame.is_empty(), "oversized frame must release its buffer");
}

/// The deferred waiter pool is bounded. Filling it with parked `turn.wait`
/// requests rejects further `turn.wait`/`session.compact` with
/// `-32019` without launching the compaction, while ping and cancel still work.
#[tokio::test]
async fn deferred_waiter_limit_rejects_new_waiters_but_keeps_control_methods() {
    let probe = ConcurrencyProbe::new();
    let (agent, base, workspace) = test_agent(
        "waiter-limit",
        [ModelScript::Gate(Arc::clone(&probe))],
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
            Some(json!({"session_id": session_id, "text": "gated turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, async {
        while probe.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gated model request did not start");

    // Fill every waiter slot with a parked turn.wait.
    for index in 0..MAX_DEFERRED_WAITERS {
        harness
            .send(
                json!(format!("wait-{index}")),
                "turn.wait",
                Some(turn_params(&turn)),
            )
            .await;
    }

    harness
        .send(json!("wait-over"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let over = harness.response(json!("wait-over")).await;
    assert_eq!(over["error"]["code"], json!(-32019));
    assert_eq!(over["error"]["data"]["kind"], json!("resource_exhausted"));
    assert_eq!(over["error"]["data"]["retryable"], json!(true));

    // The capped compaction request is rejected before the Session-owned
    // operation starts, while the waiter slots are still full and the turn is
    // still gated.
    harness
        .send(
            json!("compact-over"),
            "session.compact",
            Some(json!({
                "session_id": session_id,
                "operation_id": "compact-over-limit"
            })),
        )
        .await;
    let compact = harness.response(json!("compact-over")).await;
    assert_eq!(compact["error"]["code"], json!(-32019));

    // The reader still serves control methods, and no compaction was launched.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert!(state["result"]["compaction"].is_null());
    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );

    // Releasing the model lets the parked waiters drain.
    probe.release.cancel();
    for index in 0..MAX_DEFERRED_WAITERS {
        let waited = harness.response(json!(format!("wait-{index}"))).await;
        assert_eq!(waited["result"]["outcome"]["type"], json!("cancelled"));
    }

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_read_pages_are_bounded_lossless_and_do_not_need_workspace() {
    let (agent, base, workspace) = test_agent(
        "session-read-pages",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "read",
                arguments: json!({"path": "huge.txt", "limit": 400}),
            }]),
            ModelScript::Oversize,
        ],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let tool_text = (0..400)
        .map(|index| format!("tool {index} é🙂 \\\"\n"))
        .collect::<String>();
    tokio::fs::write(workspace.join("huge.txt"), &tool_text)
        .await
        .unwrap();
    let text = "prefix é🙂\n\t\\\" ".repeat(2_000);

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
    assert_eq!(
        harness.response(json!("wait")).await["result"]["persistence"],
        json!("persisted")
    );

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
    let session_dir = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap());
    let session_path = session_dir.join("session.json");
    let mut metadata: Value =
        serde_json::from_slice(&std::fs::read(&session_path).unwrap()).unwrap();
    metadata["model"] = json!("vendor/model-name:v1.0");
    std::fs::write(&session_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
    let metadata_before = std::fs::read(&session_path).unwrap();
    let history_before = std::fs::read(session_dir.join("history.jsonl")).unwrap();
    std::fs::remove_dir_all(&workspace).unwrap();

    let mut cursor = None;
    let mut captured_end = None;
    let mut revision = None;
    let mut reconstructed = BTreeMap::<usize, String>::new();
    let mut complete = BTreeSet::new();
    for page_index in 0..128 {
        let request_id = json!(format!("read-{page_index}"));
        let mut params = json!({
            "session_id": session_id,
            "limit": 100,
            "max_bytes": 4_096,
        });
        if let Some(page_cursor) = cursor.clone() {
            params["cursor"] = page_cursor;
            params["captured_end"] = json!(captured_end.unwrap());
            params["history_revision"] = json!(revision.clone().unwrap());
        }
        harness
            .send(request_id.clone(), "session.read", Some(params))
            .await;
        let response = harness.response(request_id).await;
        assert!(
            response.get("error").is_none(),
            "unexpected read error: {response}"
        );
        let result = &response["result"];
        assert!(serde_json::to_vec(result).unwrap().len() <= 4_096);
        if captured_end.is_none() {
            captured_end = result["captured_end"].as_u64();
            revision = result["history_revision"].as_str().map(str::to_owned);
        } else {
            assert_eq!(result["captured_end"].as_u64(), captured_end);
            assert_eq!(result["history_revision"].as_str(), revision.as_deref());
        }
        for item in result["items"].as_array().unwrap() {
            let index = item["index"].as_u64().unwrap() as usize;
            let offset = item["offset"].as_u64().unwrap() as usize;
            let data = item["data"].as_str().unwrap();
            let entry = reconstructed.entry(index).or_default();
            assert_eq!(entry.len(), offset);
            entry.push_str(data);
            if item["complete"].as_bool().unwrap() {
                assert_eq!(
                    offset + data.len(),
                    item["total_bytes"].as_u64().unwrap() as usize
                );
                complete.insert(index);
            }
        }
        if result["next_cursor"].is_null() {
            break;
        }
        cursor = Some(result["next_cursor"].clone());
        assert!(page_index < 127, "read pagination did not make progress");
    }
    assert_eq!(complete.len(), reconstructed.len());
    let user = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .find(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("user"))
        .expect("the user item must be present");
    assert_eq!(user["item"]["data"]["input"]["text"], json!(text));
    let tool = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .find(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("tool_result"))
        .expect("the tool result must be present");
    let expected_tool = tool_text
        .lines()
        .enumerate()
        .map(|(index, line)| format!("{}: {line}", index + 1))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        tool["item"]["data"]["output"]["content"],
        json!(expected_tool)
    );
    let tool_call = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .filter(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("assistant"))
        .flat_map(|item| {
            item["item"]["data"]["content"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .find(|part| part["type"] == json!("tool_call"))
        .expect("the structured tool call must be present");
    assert_eq!(tool_call["data"]["arguments"]["path"], json!("huge.txt"));
    let assistant_text_bytes = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .filter(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("assistant"))
        .flat_map(|item| {
            item["item"]["data"]["content"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .filter(|part| part["type"] == json!("text"))
        .filter_map(|part| part["data"].as_str().map(str::len))
        .sum::<usize>();
    assert_eq!(assistant_text_bytes, 128 * 1024);
    assert_eq!(
        std::fs::read(session_dir.join("session.json")).unwrap(),
        metadata_before
    );
    assert_eq!(
        std::fs::read(session_dir.join("history.jsonl")).unwrap(),
        history_before
    );

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn unloaded_session_read_pages_short_turns_under_small_budget() {
    let (agent, base, workspace) =
        test_agent("session-read-short-pages", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    for index in 0..100 {
        let send_id = json!(format!("short-send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({
                    "session_id": session_id,
                    "text": format!("short prompt {index}"),
                })),
            )
            .await;
        let turn = harness.response(send_id).await["result"]["turn"].clone();
        let wait_id = json!(format!("short-wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        assert_eq!(
            harness.response(wait_id).await["result"]["persistence"],
            json!("persisted")
        );
    }
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    let mut cursor = None;
    let mut captured_end = None;
    let mut history_revision = None;
    let mut pages = 0;
    loop {
        let request_id = json!(format!("short-read-{pages}"));
        let mut params = json!({
            "session_id": session_id,
            "limit": 100,
            "max_bytes": 4_096,
        });
        if let Some(next) = cursor.clone() {
            params["cursor"] = next;
            params["captured_end"] = json!(captured_end.unwrap());
            params["history_revision"] = json!(history_revision.clone().unwrap());
        }
        harness
            .send(request_id.clone(), "session.read", Some(params))
            .await;
        let response = harness.response(request_id).await;
        assert!(
            response["error"].is_null(),
            "unexpected read error: {response}"
        );
        assert!(serde_json::to_vec(&response["result"]).unwrap().len() <= 4_096);
        assert!(
            response["result"]["records"].as_array().unwrap().len()
                <= response["result"]["items"].as_array().unwrap().len()
        );
        if captured_end.is_none() {
            captured_end = response["result"]["captured_end"].as_u64();
            history_revision = response["result"]["history_revision"]
                .as_str()
                .map(str::to_owned);
        }
        pages += 1;
        if response["result"]["next_cursor"].is_null() {
            break;
        }
        cursor = Some(response["result"]["next_cursor"].clone());
        assert!(pages < 128, "short-turn pagination did not make progress");
    }
    assert!(pages > 1);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_read_rejects_same_length_prefix_replacement() {
    let (agent, base, workspace) = test_agent(
        "session-read-revision",
        [ModelScript::Text("answer")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let text = "prefix-".to_owned() + &"é🙂 escaped ".repeat(2_000);
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
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    let first_id = json!("read-first");
    harness
        .send(
            first_id.clone(),
            "session.read",
            Some(json!({
                "session_id": session_id,
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let first = harness.response(first_id).await;
    assert!(first["result"]["next_cursor"].is_object());

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let mut history = std::fs::read(&history_path).unwrap();
    let prefix = history
        .windows(b"prefix".len())
        .position(|window| window == b"prefix")
        .expect("test history must contain the selected prefix");
    history[prefix] = b'P';
    std::fs::write(&history_path, &history).unwrap();

    harness
        .send(
            json!("read-second"),
            "session.read",
            Some(json!({
                "session_id": session_id,
                "cursor": first["result"]["next_cursor"],
                "limit": 100,
                "max_bytes": 4_096,
                "captured_end": first["result"]["captured_end"],
                "history_revision": first["result"]["history_revision"],
            })),
        )
        .await;
    let second = harness.response(json!("read-second")).await;
    assert_eq!(second["error"]["code"], json!(-32_005));
    assert_eq!(second["error"]["data"]["kind"], json!("invalid_state"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn loaded_session_read_does_not_expose_a_disk_tail() {
    let (agent, base, workspace) = test_agent(
        "session-read-loaded-prefix",
        [ModelScript::Text("answer")],
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
            Some(json!({"session_id": session_id, "text": "committed"})),
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
    let original = std::fs::read(&history_path).unwrap();
    let newline = original
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("a persisted turn must have one JSONL line");
    let mut with_tail = original.clone();
    with_tail.extend_from_slice(&original[..=newline]);
    std::fs::write(&history_path, with_tail).unwrap();

    harness
        .send(
            json!("read"),
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    let read = harness.response(json!("read")).await;
    assert_eq!(read["result"]["session"]["loaded"], json!(true));
    assert_eq!(read["result"]["total"], json!(2));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn loaded_session_read_keeps_a_captured_prefix_across_append() {
    let (agent, base, workspace) = test_agent(
        "session-read-loaded-pagination",
        [ModelScript::Oversize, ModelScript::Text("new answer")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send-first"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "first"})),
        )
        .await;
    let first_turn = harness.response(json!("send-first")).await["result"]["turn"].clone();
    harness
        .send(
            json!("wait-first"),
            "turn.wait",
            Some(turn_params(&first_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("wait-first")).await["result"]["persistence"],
        json!("persisted")
    );

    harness
        .send(
            json!("read-first"),
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    let first_page = harness.response(json!("read-first")).await;
    assert!(first_page["result"]["next_cursor"].is_object());
    let captured_end = first_page["result"]["captured_end"].as_u64().unwrap();
    let history_revision = first_page["result"]["history_revision"].clone();
    let captured_total = first_page["result"]["total"].as_u64().unwrap();
    let mut cursor = first_page["result"]["next_cursor"].clone();
    let mut response_text = first_page["result"].to_string();
    let mut page_count = 1;

    harness
        .send(
            json!("send-second"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "second"})),
        )
        .await;
    let second_turn = harness.response(json!("send-second")).await["result"]["turn"].clone();
    harness
        .send(
            json!("wait-second"),
            "turn.wait",
            Some(turn_params(&second_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("wait-second")).await["result"]["persistence"],
        json!("persisted")
    );

    while !cursor.is_null() {
        let request_id = json!(format!("read-next-{page_count}"));
        harness
            .send(
                request_id.clone(),
                "session.read",
                Some(json!({
                    "session_id": session_id,
                    "cursor": cursor,
                    "limit": 100,
                    "max_bytes": 4_096,
                    "captured_end": captured_end,
                    "history_revision": history_revision,
                })),
            )
            .await;
        let page = harness.response(request_id).await;
        assert!(
            page["error"].is_null(),
            "unexpected continuation error: {page}"
        );
        assert_eq!(page["result"]["total"].as_u64(), Some(captured_total));
        assert_eq!(page["result"]["captured_end"].as_u64(), Some(captured_end));
        assert_eq!(page["result"]["history_revision"], history_revision);
        assert!(!page.to_string().contains("new answer"));
        response_text.push_str(&page["result"].to_string());
        cursor = page["result"]["next_cursor"].clone();
        page_count += 1;
        assert!(page_count < 128, "loaded pagination did not make progress");
    }
    assert!(page_count > 1);
    assert!(!response_text.contains("new answer"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_read_reports_incomplete_tail_and_corrupt_middle_without_repair() {
    let (agent, base, workspace) = test_agent(
        "session-read-integrity",
        [ModelScript::Text("first"), ModelScript::Text("second")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    for (index, text) in ["one", "two"].into_iter().enumerate() {
        let send_id = json!(format!("send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({"session_id": session_id, "text": text})),
            )
            .await;
        let turn = harness.response(send_id).await["result"]["turn"].clone();
        let wait_id = json!(format!("wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        assert_eq!(
            harness.response(wait_id).await["result"]["persistence"],
            json!("persisted")
        );
    }
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let original = std::fs::read(&history_path).unwrap();
    let mut with_tail = original.clone();
    with_tail.extend_from_slice(b"{\"incomplete\"");
    std::fs::write(&history_path, &with_tail).unwrap();

    harness
        .send(
            json!("tail"),
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    let tail = harness.response(json!("tail")).await;
    assert_eq!(tail["result"]["trailing_incomplete"], json!(true));
    assert_eq!(tail["result"]["total"], json!(4));
    assert_eq!(std::fs::read(&history_path).unwrap(), with_tail);

    let second_line = original
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| offset + 1)
        .expect("two turns must produce a second JSONL line");
    assert_eq!(original[second_line], b'{');
    let mut corrupt = with_tail;
    corrupt[second_line] = b'X';
    std::fs::write(&history_path, &corrupt).unwrap();
    harness
        .send(
            json!("corrupt"),
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    let corrupt_response = harness.response(json!("corrupt")).await;
    assert_eq!(corrupt_response["error"]["code"], json!(-32_011));
    assert_eq!(
        corrupt_response["error"]["data"]["kind"],
        json!("store_error")
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), corrupt);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn turn_result_reports_pending_and_stored_availability() {
    let model_started = Arc::new(Notify::new());
    let (agent, base, workspace) = test_agent(
        "turn-result-lifecycle",
        [
            ModelScript::BlockWithSignal(Arc::clone(&model_started)),
            ModelScript::Text("stored answer"),
        ],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send-blocked"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "wait"})),
        )
        .await;
    let blocked_turn = harness.response(json!("send-blocked")).await["result"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, model_started.notified())
        .await
        .expect("blocked model request must be started before querying its result");

    harness
        .send(
            json!("pending"),
            "turn.result",
            Some(json!({
                "session_id": blocked_turn["session_id"],
                "loop_id": blocked_turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let pending = harness.response(json!("pending")).await;
    assert_eq!(pending["result"]["availability"], json!("pending"));

    harness
        .send(
            json!("cancel-blocked"),
            "turn.cancel",
            Some(turn_params(&blocked_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel-blocked")).await["result"]["cancelled"],
        json!(true)
    );
    harness
        .send(
            json!("wait-blocked"),
            "turn.wait",
            Some(turn_params(&blocked_turn)),
        )
        .await;
    let _ = harness.response(json!("wait-blocked")).await;

    harness
        .send(
            json!("send-stored"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "persist"})),
        )
        .await;
    let stored_turn = harness.response(json!("send-stored")).await["result"]["turn"].clone();
    harness
        .send(
            json!("wait-stored"),
            "turn.wait",
            Some(turn_params(&stored_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("wait-stored")).await["result"]["persistence"],
        json!("persisted")
    );
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    harness
        .send(
            json!("stored-result"),
            "turn.result",
            Some(json!({
                "session_id": stored_turn["session_id"],
                "loop_id": stored_turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let stored = harness.response(json!("stored-result")).await;
    assert_eq!(stored["result"]["availability"], json!("stored"));
    assert_eq!(stored["result"]["persistence"], json!("persisted"));
    assert!(stored["result"]["items"].as_array().unwrap().len() >= 2);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn turn_result_can_continue_from_live_to_stored_items() {
    let (agent, base, workspace) = test_agent(
        "turn-result-live-stored-pages",
        [ModelScript::Oversize],
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
            Some(json!({"session_id": session_id, "text": "live"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    harness
        .send(
            json!("live-first"),
            "turn.result",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let first = harness.response(json!("live-first")).await;
    assert_eq!(first["result"]["availability"], json!("live"));
    assert!(first["result"]["next_cursor"].is_object());

    let mut cursor = first["result"]["next_cursor"].clone();
    let mut reconstructed = BTreeMap::<usize, String>::new();
    for item in first["result"]["items"].as_array().unwrap() {
        let index = item["index"].as_u64().unwrap() as usize;
        let offset = item["offset"].as_u64().unwrap() as usize;
        let entry = reconstructed.entry(index).or_default();
        assert_eq!(entry.len(), offset);
        entry.push_str(item["data"].as_str().unwrap());
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    let mut pages = 1;
    while !cursor.is_null() {
        let request_id = json!(format!("stored-{pages}"));
        harness
            .send(
                request_id.clone(),
                "turn.result",
                Some(json!({
                    "session_id": turn["session_id"],
                    "loop_id": turn["loop_id"],
                    "cursor": cursor,
                    "limit": 100,
                    "max_bytes": 4_096,
                })),
            )
            .await;
        let page = harness.response(request_id).await;
        assert_eq!(page["result"]["availability"], json!("stored"));
        for item in page["result"]["items"].as_array().unwrap() {
            let index = item["index"].as_u64().unwrap() as usize;
            let offset = item["offset"].as_u64().unwrap() as usize;
            let entry = reconstructed.entry(index).or_default();
            assert_eq!(entry.len(), offset);
            entry.push_str(item["data"].as_str().unwrap());
            if item["complete"].as_bool().unwrap() {
                assert_eq!(entry.len(), item["total_bytes"].as_u64().unwrap() as usize);
            }
        }
        cursor = page["result"]["next_cursor"].clone();
        pages += 1;
        assert!(pages < 128, "turn result pagination did not make progress");
    }
    assert!(pages > 1);
    assert_eq!(reconstructed.len(), 2);

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn turn_result_reads_failed_live_report_without_history() {
    let (agent, base, workspace) = test_agent(
        "turn-result-failed-append",
        [ModelScript::Text("answer retained in memory")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let typed_session_id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    fail_next_append(typed_session_id);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "failed persistence"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let waited = harness.response(json!("wait")).await;
    assert_eq!(waited["result"]["persistence"], json!("failed"));

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    std::fs::remove_file(history_path).unwrap();
    harness
        .send(
            json!("result"),
            "turn.result",
            Some(json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let result = harness.response(json!("result")).await;
    assert_eq!(result["result"]["availability"], json!("live"));
    assert_eq!(result["result"]["persistence"], json!("failed"));
    assert_eq!(result["result"]["outcome"]["type"], json!("completed"));
    assert!(result.to_string().contains("answer retained in memory"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn deferred_query_capacity_is_four_with_shared_total_limit() {
    let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::channel(1);
    let mut server = RpcServer {
        agent: None,
        outbound_tx,
        waiters: tokio::task::JoinSet::new(),
        queries: tokio::task::JoinSet::new(),
        query_cancellation: CancellationToken::new(),
    };
    for _ in 0..MAX_DEFERRED_QUERIES {
        server.queries.spawn(async { pending::<()>().await });
    }
    assert!(!server.query_capacity_available());

    server.queries.abort_all();
    while server.queries.join_next().await.is_some() {}

    for _ in 0..(MAX_DEFERRED_QUERIES - 1) {
        server.queries.spawn(async { pending::<()>().await });
    }
    for _ in 0..(MAX_DEFERRED_WAITERS - (MAX_DEFERRED_QUERIES - 1)) {
        server.waiters.spawn(async { pending::<()>().await });
    }
    assert!(!server.query_capacity_available());

    server.queries.abort_all();
    server.waiters.abort_all();
    while server.queries.join_next().await.is_some() {}
    while server.waiters.join_next().await.is_some() {}
}

#[tokio::test]
async fn tool_read_and_output_query_recorded_facts_by_full_identity() {
    let (agent, base, workspace) = test_agent(
        "tool-data-rpc",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "read",
                arguments: json!({"path": "a.txt", "limit": 4}),
            }]),
            ModelScript::Text("done"),
        ],
        &["read"],
        ApprovalMode::Auto,
    )
    .await;
    tokio::fs::write(workspace.join("a.txt"), b"alpha\nbeta")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "read a"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness.response(json!("wait")).await;

    // Derive the ToolRef from authoritative history rather than guessing.
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
        .find_map(|item| {
            (item["item"]["type"] == json!("tool_result")).then(|| item["item"]["data"].clone())
        })
        .expect("tool result history item");
    let tool_ref = json!({
        "session_id": session_id,
        "loop_id": tool_result["loop_id"],
        "request_index": tool_result["request_index"],
        "tool_call_id": tool_result["tool_call_id"],
    });

    harness
        .send(
            json!("read"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_result["loop_id"],
                "request_index": tool_result["request_index"],
                "tool_call_id": tool_result["tool_call_id"],
            })),
        )
        .await;
    let read = harness.response(json!("read")).await;
    assert_eq!(read["result"]["execution"]["state"], json!("succeeded"));
    assert_eq!(read["result"]["invocation"]["name"], json!("read"));
    assert!(
        read["result"]["invocation"]["input"]["preview"]
            .as_str()
            .unwrap()
            .contains("a.txt")
    );

    harness
        .send(
            json!("output"),
            "tool.output",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_ref["loop_id"],
                "request_index": tool_ref["request_index"],
                "tool_call_id": tool_ref["tool_call_id"],
                "stream": "output",
                "offset": 0,
                "max_bytes": 8192,
            })),
        )
        .await;
    let output = harness.response(json!("output")).await;
    assert_eq!(output["result"]["stream"], json!("output"));
    assert_eq!(output["result"]["encoding"], json!("utf8"));
    assert!(
        output["result"]["data"]
            .as_str()
            .unwrap()
            .starts_with("1: alpha")
    );

    // An unknown identity is an explicit `tool_not_found`, never the nearest
    // matching name.
    harness
        .send(
            json!("missing"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": tool_ref["loop_id"],
                "request_index": 999,
                "tool_call_id": tool_ref["tool_call_id"],
            })),
        )
        .await;
    let missing = harness.response(json!("missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32021));
    assert_eq!(missing["error"]["data"]["kind"], json!("tool_not_found"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn tool_read_exposes_approval_time_invocation_without_running() {
    let (agent, base, workspace) = test_agent(
        "tool-data-approval",
        [
            ModelScript::ToolCalls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "out.txt", "content": "approved"}),
            }]),
            ModelScript::Text("done"),
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
            Some(json!({"session_id": session_id, "text": "write it"})),
        )
        .await;
    let sent = harness.response(json!("send")).await;
    let turn = sent["result"]["turn"].clone();
    let interaction = harness.event("interaction_requested").await;
    let tool_call_id = interaction["params"]["data"]["interaction"]["tool_call_id"].clone();

    harness
        .send(
            json!("read"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "request_index": 0,
                "tool_call_id": tool_call_id,
            })),
        )
        .await;
    let read = harness.response(json!("read")).await;
    assert_eq!(
        read["result"]["execution"]["state"],
        json!("awaiting_policy")
    );
    assert_eq!(
        read["result"]["invocation"]["subject"]["kind"],
        json!("file")
    );
    assert_eq!(
        read["result"]["invocation"]["subject"]["path"],
        json!("out.txt")
    );
    assert!(read["result"]["execution"].get("started_at").is_none());

    // The output stream of a call that has not run is pending, not a false
    // empty complete page.
    harness
        .send(
            json!("output"),
            "tool.output",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "request_index": 0,
                "tool_call_id": interaction["params"]["data"]["interaction"]["tool_call_id"],
                "stream": "output",
                "offset": 0,
            })),
        )
        .await;
    let pending = harness.response(json!("output")).await;
    assert_eq!(pending["result"]["availability"], json!("pending"));
    assert_eq!(pending["result"]["eof"], json!(false));
    assert_eq!(pending["result"]["data"], json!(""));

    let interaction_id = interaction["params"]["data"]["interaction"]["interaction_id"].clone();
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
    harness.response(json!("answer")).await;
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    harness.response(json!("wait")).await;

    harness
        .send(
            json!("read2"),
            "tool.read",
            Some(json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "request_index": 0,
                "tool_call_id": interaction["params"]["data"]["interaction"]["tool_call_id"],
            })),
        )
        .await;
    let finished = harness.response(json!("read2")).await;
    assert_eq!(finished["result"]["execution"]["state"], json!("succeeded"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_context_reports_effective_input_budget_when_auto_enabled() {
    let (agent, base, workspace) = test_agent_with_auto(
        "context-budget",
        [ModelScript::Text("answer")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("context"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context")).await;
    assert_eq!(
        context["result"]["budget"]["input_budget_tokens"],
        json!(16_384)
    );
    assert_eq!(
        context["result"]["budget"]["trigger_tokens"],
        json!(16_384 * 80 / 100)
    );
    assert_eq!(
        context["result"]["budget"]["target_tokens"],
        json!(16_384 * 50 / 100)
    );
    assert!(context["result"]["last_prepare_failure"].is_null());
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn session_context_omits_budget_when_auto_disabled() {
    let (agent, base, workspace) = test_agent(
        "context-disabled",
        [ModelScript::Text("answer")],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("context"),
            "session.context",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let context = harness.response(json!("context")).await;
    assert!(context["result"]["budget"]["input_budget_tokens"].is_null());
    assert!(context["result"]["budget"]["trigger_tokens"].is_null());
    assert!(context["result"]["budget"]["target_tokens"].is_null());
    harness.shutdown().await;
    remove_base(&base).await;
}

/// An uncompressible request is rejected with a distinct domain error before
/// any model call. The system prompt plus the current input alone exceed the
/// tiny effective budget.
#[tokio::test]
async fn turn_send_reports_context_uncompressible_for_oversized_current_input() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-uncompressible-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = FakeModel::with_context_window([ModelScript::Text("answer")], 1_000);
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.compaction = CompactionConfig {
        enabled: true,
        trigger_percent: 80,
        target_percent: 50,
    };
    let agent = Agent::open_with_models(config, test_models(model))
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let oversized = "x".repeat(20_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": oversized})),
        )
        .await;
    let response = harness.response(json!("send")).await;
    assert_eq!(response["error"]["code"], json!(-32022));
    assert_eq!(
        response["error"]["data"]["kind"],
        json!("context_uncompressible")
    );
    assert_eq!(response["error"]["data"]["retryable"], json!(false));
    // The reader still serves control methods and the session is idle.
    harness
        .send(
            json!("state"),
            "session.state",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let state = harness.response(json!("state")).await;
    assert_eq!(state["result"]["status"], json!("idle"));
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn automatic_startup_accepts_minimum_between_trigger_and_hard_limit() {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-minimum-window-{}",
        SessionId::new().unwrap()
    ));
    let workspace = base.join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let model = FakeModel::with_context_window([ModelScript::Text("answer")], 1_000);
    let mut config = test_config(base.join("data"), &[], ApprovalMode::Auto);
    config.compaction = CompactionConfig {
        enabled: true,
        trigger_percent: 80,
        target_percent: 50,
    };
    let system = minicore_runtime::value::BoundedText::new("test system prompt").unwrap();
    let text = (1..10_000)
        .map(|size| "x".repeat(size))
        .find(|candidate| {
            crate::compaction::estimate_minimal(
                &system,
                &[candidate.as_str()],
                &[],
                ReasoningPreference::Auto,
                &crate::models::DefaultProviderBudget,
            )
            .is_ok_and(|tokens| (801..=900).contains(&tokens))
        })
        .expect("test input must land above trigger and below hard limit");
    let agent = Agent::open_with_models(config, test_models(Arc::clone(&model)))
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": text})),
        )
        .await;
    let response = harness.response(json!("send")).await;
    assert!(response["result"]["turn"]["loop_id"].is_string());
    let turn = response["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let wait = harness.response(json!("wait")).await;
    assert_eq!(wait["result"]["outcome"]["type"], json!("completed"));
    assert_eq!(wait["result"]["persistence"], json!("persisted"));
    // turn.send promises loop creation, not that the model has started yet.
    // The call count is meaningful only after the turn has completed.
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn workspace_read_query_serves_raw_content_and_rejects_invalid_requests() {
    let (agent, base, workspace) = test_agent("workspace-read", [], &[], ApprovalMode::Auto).await;
    tokio::fs::write(workspace.join("note.txt"), b"hello\nworld\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("ws-read"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "note.txt"})),
        )
        .await;
    let read = harness.response(json!("ws-read")).await;
    let result = &read["result"];
    assert_eq!(result["path"], json!("note.txt"));
    assert_eq!(result["content"], json!("hello\nworld\n"));
    assert_eq!(result["start_line"], json!(1));
    assert_eq!(result["returned_lines"], json!(2));
    assert_eq!(result["status"], json!("ok"));
    assert_eq!(result["encoding"], json!("utf8"));
    assert_eq!(result["revision"].as_str().unwrap().len(), 64);
    assert_eq!(result["truncated"], json!(false));
    assert_eq!(result["line_truncated"], json!(false));
    assert_eq!(result["next_range"], json!(null));
    assert_eq!(result["file_bytes"], json!(12));

    harness
        .send(
            json!("ws-range"),
            "workspace.read",
            Some(json!({
                "session_id": session_id,
                "path": "note.txt",
                "start_line": 2,
                "max_lines": 1,
            })),
        )
        .await;
    let ranged = harness.response(json!("ws-range")).await;
    assert_eq!(ranged["result"]["content"], json!("world\n"));
    assert_eq!(ranged["result"]["start_line"], json!(2));

    harness
        .send(
            json!("ws-fresh"),
            "workspace.read",
            Some(json!({
                "session_id": session_id,
                "path": "note.txt",
                "if_revision": result["revision"].clone(),
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("ws-fresh")).await["result"]["status"],
        json!("ok")
    );
    harness
        .send(
            json!("ws-stale"),
            "workspace.read",
            Some(json!({
                "session_id": session_id,
                "path": "note.txt",
                "if_revision": "0".repeat(64),
            })),
        )
        .await;
    let stale = harness.response(json!("ws-stale")).await;
    assert_eq!(stale["result"]["status"], json!("changed"));
    assert_eq!(stale["result"]["content"], json!(""));

    // Only a loaded Session locates its Workspace.
    let unloaded = SessionId::new().unwrap();
    harness
        .send(
            json!("ws-unloaded"),
            "workspace.read",
            Some(json!({"session_id": unloaded, "path": "note.txt"})),
        )
        .await;
    let unloaded = harness.response(json!("ws-unloaded")).await;
    assert_eq!(unloaded["error"]["code"], json!(-32002));
    assert_eq!(
        unloaded["error"]["data"]["kind"],
        json!("session_not_loaded")
    );

    harness
        .send(
            json!("ws-escape"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "../note.txt"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("ws-escape")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .send(
            json!("ws-lines"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "note.txt", "max_lines": 0})),
        )
        .await;
    assert_eq!(
        harness.response(json!("ws-lines")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .send(
            json!("ws-missing"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "missing.txt"})),
        )
        .await;
    let missing = harness.response(json!("ws-missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32010));
    assert_eq!(missing["error"]["data"]["kind"], json!("workspace_error"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn pending_workspace_read_does_not_block_ping_cancel_or_shutdown() {
    let turn_started = Arc::new(Notify::new());
    let (agent, base, workspace) = test_agent(
        "workspace-read-pending",
        [ModelScript::BlockWithSignal(Arc::clone(&turn_started))],
        &[],
        ApprovalMode::Auto,
    )
    .await;
    tokio::fs::write(workspace.join("pending-a.txt"), b"a\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "blocked turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, turn_started.notified())
        .await
        .expect("blocked model request did not start");

    let gate = Arc::new(WorkspaceReadGate::new());
    gate_next_workspace_read("pending-a.txt", Arc::clone(&gate));
    harness
        .send(
            json!("ws-pending"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "pending-a.txt"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .expect("workspace read did not start");

    // The read query is genuinely pending while control methods stay live.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
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

    // Shutdown cancels the pending read and still completes.
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn closing_a_session_cancels_pending_workspace_reads_and_frees_capacity() {
    let (agent, base, workspace) =
        test_agent("workspace-read-capacity", [], &[], ApprovalMode::Auto).await;
    for index in 0..MAX_DEFERRED_QUERIES {
        tokio::fs::write(
            workspace.join(format!("cap-{index}.txt")),
            format!("cap-{index}\n"),
        )
        .await
        .unwrap();
    }
    tokio::fs::write(workspace.join("cap-fresh.txt"), b"fresh\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let closing = create_and_open(&mut harness, &workspace).await;
    let surviving = create_and_open(&mut harness, &workspace).await;

    let mut gates = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let path = format!("cap-{index}.txt");
        let gate = Arc::new(WorkspaceReadGate::new());
        gate_next_workspace_read(&path, Arc::clone(&gate));
        gates.push(gate);
        harness
            .send(
                json!(format!("cap-{index}")),
                "workspace.read",
                Some(json!({"session_id": closing, "path": path})),
            )
            .await;
    }

    harness
        .send(
            json!("cap-over"),
            "workspace.read",
            Some(json!({"session_id": surviving, "path": "cap-fresh.txt"})),
        )
        .await;
    let over = harness.response(json!("cap-over")).await;
    assert_eq!(over["error"]["code"], json!(-32019));
    assert_eq!(over["error"]["data"]["kind"], json!("resource_exhausted"));

    // The reads really started before the Session is closed.
    for gate in &gates {
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .expect("gated workspace read did not start");
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": closing})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    // Closing the owning Session cancels the four started queries.
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("cap-{index}"))).await;
        assert_eq!(response["error"]["code"], json!(-32020));
        assert_eq!(response["error"]["data"]["kind"], json!("query_limit"));
    }

    // The reader reaps the finished queries and admits a new one, and the
    // closed Session no longer owns a Workspace.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness
        .send(
            json!("cap-closed"),
            "workspace.read",
            Some(json!({"session_id": closing, "path": "cap-fresh.txt"})),
        )
        .await;
    let closed = harness.response(json!("cap-closed")).await;
    assert_eq!(closed["error"]["code"], json!(-32002));
    harness
        .send(
            json!("cap-reuse"),
            "workspace.read",
            Some(json!({"session_id": surviving, "path": "cap-fresh.txt"})),
        )
        .await;
    let reuse = harness.response(json!("cap-reuse")).await;
    assert_eq!(reuse["result"]["content"], json!("fresh\n"));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn workspace_files_and_search_serve_entries_and_literal_matches() {
    let (agent, base, workspace) = test_agent("workspace-scan", [], &[], ApprovalMode::Auto).await;
    tokio::fs::write(workspace.join("note.txt"), b"alpha\nneedle here\n")
        .await
        .unwrap();
    tokio::fs::create_dir_all(workspace.join("sub"))
        .await
        .unwrap();
    tokio::fs::write(workspace.join("sub/deep.txt"), b"needle too\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("files"),
            "workspace.files",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let files = harness.response(json!("files")).await;
    let result = &files["result"];
    assert_eq!(result["directory"], json!(""));
    assert_eq!(result["consistency"], json!("live"));
    assert_eq!(result["scan_complete"], json!(true));
    assert_eq!(result["truncated"], json!(false));
    assert_eq!(result["stopped_by"], json!("end"));
    assert_eq!(result["skipped_count"], json!(0));
    assert_eq!(result["next_cursor"], json!(null));
    assert!(result["observed_at_unix_ms"].as_u64().unwrap() > 0);
    let entries = result["entries"].as_array().unwrap();
    let mut paths: Vec<&str> = entries
        .iter()
        .map(|entry| entry["path"].as_str().unwrap())
        .collect();
    paths.sort();
    assert_eq!(paths, vec!["note.txt", "sub"]);
    let note = entries
        .iter()
        .find(|entry| entry["path"] == json!("note.txt"))
        .unwrap();
    assert_eq!(note["kind"], json!("file"));
    assert_eq!(note["size"], json!(18));
    let directory = entries
        .iter()
        .find(|entry| entry["path"] == json!("sub"))
        .unwrap();
    assert_eq!(directory["kind"], json!("directory"));
    assert_eq!(directory["size"], json!(null));

    // A recursive query filters entry paths and still descends.
    harness
        .send(
            json!("files-recursive"),
            "workspace.files",
            Some(json!({
                "session_id": session_id,
                "recursive": true,
                "query": "deep",
            })),
        )
        .await;
    let nested = harness.response(json!("files-recursive")).await;
    assert_eq!(
        nested["result"]["entries"][0]["path"],
        json!("sub/deep.txt")
    );
    assert!(nested["result"]["skipped_count"].as_u64().unwrap() > 0);

    // A literal, case-insensitive search reports line metadata, not contents
    // spliced into the text.
    harness
        .send(
            json!("search"),
            "workspace.search",
            Some(json!({"session_id": session_id, "query": "NEEDLE"})),
        )
        .await;
    let search = harness.response(json!("search")).await;
    let result = &search["result"];
    assert_eq!(result["consistency"], json!("live"));
    assert_eq!(result["scan_complete"], json!(true));
    assert_eq!(result["skipped_files"], json!(0));
    let matches = result["matches"].as_array().unwrap();
    let mut matched: Vec<&str> = matches
        .iter()
        .map(|record| record["path"].as_str().unwrap())
        .collect();
    matched.sort();
    assert_eq!(matched, vec!["note.txt", "sub/deep.txt"]);
    let record = matches
        .iter()
        .find(|record| record["path"] == json!("note.txt"))
        .unwrap();
    assert_eq!(record["line_number"], json!(2));
    assert_eq!(record["line_text"], json!("needle here"));
    assert_eq!(record["match_byte_ranges"], json!([{"start": 0, "end": 6}]));
    assert_eq!(record["line_truncated"], json!(false));

    // Lexical errors are rejected before a query slot is reserved, and a
    // missing root is a workspace error.
    harness
        .send(
            json!("files-escape"),
            "workspace.files",
            Some(json!({"session_id": session_id, "directory": "../escape"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("files-escape")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .send(
            json!("search-empty"),
            "workspace.search",
            Some(json!({"session_id": session_id, "query": ""})),
        )
        .await;
    assert_eq!(
        harness.response(json!("search-empty")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .send(
            json!("files-missing"),
            "workspace.files",
            Some(json!({"session_id": session_id, "directory": "missing"})),
        )
        .await;
    let missing = harness.response(json!("files-missing")).await;
    assert_eq!(missing["error"]["code"], json!(-32010));
    assert_eq!(missing["error"]["data"]["kind"], json!("workspace_error"));
    let unloaded = SessionId::new().unwrap();
    harness
        .send(
            json!("search-unloaded"),
            "workspace.search",
            Some(json!({"session_id": unloaded, "query": "needle"})),
        )
        .await;
    let unloaded = harness.response(json!("search-unloaded")).await;
    assert_eq!(unloaded["error"]["code"], json!(-32002));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn pending_workspace_scans_do_not_block_ping_or_shutdown() {
    let (agent, base, workspace) =
        test_agent("workspace-scan-pending", [], &[], ApprovalMode::Auto).await;
    tokio::fs::write(workspace.join("a.txt"), b"pending hit\n")
        .await
        .unwrap();
    let root = std::fs::canonicalize(&workspace).unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let files_hold = Arc::new(ScanHold::new());
    hold_next_scan(root.clone(), Arc::clone(&files_hold));
    harness
        .send(
            json!("files"),
            "workspace.files",
            Some(json!({"session_id": session_id})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, files_hold.wait_started())
        .await
        .expect("gated files scan did not start");

    let search_hold = Arc::new(ScanHold::new());
    hold_next_scan(root, Arc::clone(&search_hold));
    harness
        .send(
            json!("search"),
            "workspace.search",
            Some(json!({"session_id": session_id, "query": "hit"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, search_hold.wait_started())
        .await
        .expect("gated search scan did not start");

    // Both scans are genuinely pending while control methods stay live.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Shutdown cancels both scans, joins their blocking workers, and still
    // queues its own response last.
    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn closing_a_session_cancels_pending_workspace_scans_and_frees_capacity() {
    let (agent, base, workspace) =
        test_agent("workspace-scan-capacity", [], &[], ApprovalMode::Auto).await;
    for index in 0..MAX_DEFERRED_QUERIES {
        tokio::fs::write(
            workspace.join(format!("cap-{index}.txt")),
            format!("cap-{index}\n"),
        )
        .await
        .unwrap();
    }
    tokio::fs::write(workspace.join("cap-fresh.txt"), b"fresh\n")
        .await
        .unwrap();
    let root = std::fs::canonicalize(&workspace).unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let closing = create_and_open(&mut harness, &workspace).await;
    let surviving = create_and_open(&mut harness, &workspace).await;

    let mut holds = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let hold = Arc::new(ScanHold::new());
        hold_next_scan(root.clone(), Arc::clone(&hold));
        holds.push(hold);
        harness
            .send(
                json!(format!("scan-{index}")),
                "workspace.files",
                Some(json!({"session_id": closing})),
            )
            .await;
    }

    harness
        .send(
            json!("scan-over"),
            "workspace.search",
            Some(json!({"session_id": surviving, "query": "fresh"})),
        )
        .await;
    let over = harness.response(json!("scan-over")).await;
    assert_eq!(over["error"]["code"], json!(-32019));
    assert_eq!(over["error"]["data"]["kind"], json!("resource_exhausted"));

    for hold in &holds {
        tokio::time::timeout(TIMEOUT, hold.wait_started())
            .await
            .expect("gated scan did not start");
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": closing})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    // Closing the owning Session cancels the four started scans.
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("scan-{index}"))).await;
        assert_eq!(response["error"]["code"], json!(-32020));
        assert_eq!(response["error"]["data"]["kind"], json!("query_limit"));
    }

    // The reader reaps the finished queries and admits a new one, and the
    // closed Session no longer owns a Workspace.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness
        .send(
            json!("scan-closed"),
            "workspace.files",
            Some(json!({"session_id": closing})),
        )
        .await;
    assert_eq!(
        harness.response(json!("scan-closed")).await["error"]["code"],
        json!(-32002)
    );
    harness
        .send(
            json!("scan-reuse"),
            "workspace.search",
            Some(json!({"session_id": surviving, "query": "cap"})),
        )
        .await;
    let reuse = harness.response(json!("scan-reuse")).await;
    let paths: Vec<&str> = reuse["result"]["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths.len(), MAX_DEFERRED_QUERIES);
    assert!(paths.contains(&"cap-0.txt"));
    assert!(!paths.contains(&"cap-fresh.txt"));

    harness.shutdown().await;
    remove_base(&base).await;
}
