mod changes;
mod compaction;
mod compaction_budget;
mod compaction_failures;
mod core;
mod discovery;
mod framing;
mod review;
mod session_read;
mod sessions;
mod status;
mod tools;
mod turn_result;
mod turns;
mod workspace;

pub(super) async fn pause_operation(key: PathBuf) {
    if let Some(gate) = super::OPERATION_GATES.take(&key) {
        gate.pass().await;
    }
}

use crate::test_support::Gate;
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
use crate::changes::CHANGE_LIST_GATES;
use crate::config::{AgentConfig, CompactionConfig, LoopOverrides, Profile};
use crate::error::AgentError;
use crate::models::{ModelConfig, Models};
use crate::profiles::ApprovalMode;
use crate::read::TOOL_READ_GATES;
use crate::sessions::PAUSE_AFTER_COMPACTION_RESULT;
use crate::workspace::query::READ_GATES;
use crate::workspace::scan::{SCAN_HOLDS, tests::ScanHold};

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
        )
        .unwrap(),
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

    fn for_compaction(scripts: impl IntoIterator<Item = ModelScript>) -> Arc<Self> {
        Self::with_context_window(scripts, 40_000)
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

async fn test_compaction_agent(
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
    let model = FakeModel::for_compaction(scripts);
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

/// The complete `resource_exhausted` error object, so a regression that only
/// preserved the code/kind is caught.
#[cfg(unix)]
fn resource_exhausted_error(id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32019,
            "message": "too many deferred requests",
            "data": {"kind": "resource_exhausted", "retryable": true},
        },
    })
}

/// A syntactically valid `workspace:` change reference for one path, built the
/// same way the live projection does. Used to pass `validate` without a real
/// Git fixture when the point is the capacity check, not the diff content.
#[cfg(unix)]
fn untracked_workspace_ref(session_id: SessionId, path: &str) -> String {
    use base64::Engine;
    let entry = crate::workspace::status::WorkspaceStatusEntry {
        path: path.to_owned(),
        kind: crate::workspace::status::WorkspaceStatusEntryKind::Untracked,
        index_status: None,
        worktree_status: None,
        original_path: None,
    };
    let bytes = serde_json::to_vec(&(session_id, String::new(), entry)).unwrap();
    format!(
        "workspace:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
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

/// Give lifecycle/budget tests a real legal prefix plus a complete recent
/// loop above the default 20k retention target, without consuming fake model
/// scripts. Reload through the public RPC so memory and durable bytes agree.
async fn append_compaction_tail(harness: &mut RpcHarness, base: &Path, session_id: &Value) {
    harness
        .send(
            json!("fixture-close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("fixture-close")).await["result"]["ok"],
        true
    );
    let store = crate::store::Store::open(base.join("data")).await.unwrap();
    let id: SessionId = session_id.as_str().unwrap().parse().unwrap();
    let loop_id = minicore_runtime::LoopId::new().unwrap();
    let record = crate::store::StoredLoopRecord {
        loop_id,
        outcome: crate::store::StoredLoopOutcome::Completed,
        items: vec![
            minicore_runtime::history::HistoryItem::User(minicore_runtime::history::UserHistory {
                loop_id,
                kind: minicore_runtime::history::UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(format!(
                    "RETAINED_TAIL {}",
                    "recent ".repeat(12_000)
                ))
                .unwrap(),
            }),
            minicore_runtime::history::HistoryItem::Assistant(
                minicore_runtime::history::AssistantHistory {
                    loop_id,
                    request_index: 0,
                    model: "fake".parse().unwrap(),
                    reasoning: ReasoningPreference::Auto,
                    content: vec![minicore_runtime::model::AssistantPart::Text(
                        "RECENT_TAIL_ANSWER".into(),
                    )],
                    provider_replay: None,
                    finish_reason: ModelFinishReason::Stop,
                    usage: Usage::new(1, 1, 0),
                },
            ),
        ],
        usage: Usage::new(1, 1, 0),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: minicore_runtime::execution::ConfigRevision::INITIAL,
        completed_at: "2026-01-02T03:04:05.000Z".to_owned(),
        user_times: None,
    };
    store.append_loop(id, &record).await.unwrap();
    harness
        .send(
            json!("fixture-open"),
            "session.open",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert!(harness.response(json!("fixture-open")).await["error"].is_null());
}

async fn assert_manual_failure_case(label: &str, script: ModelScript, expected: &str) -> Value {
    let (agent, base, workspace) = test_compaction_agent(
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
    append_compaction_tail(&mut harness, &base, &session_id).await;
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
