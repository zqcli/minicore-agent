use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::stream;
use serde_json::json;
use tokio::sync::Notify;

use minicore_runtime::execution::ConfigRevision;
use minicore_runtime::history::{
    AssistantHistory, HistoryItem, ToolResultHistory, UserHistory, UserMessageKind,
};
use minicore_runtime::model::{
    AssistantPart, Model, ModelCallContext, ModelDescriptor, ModelError, ModelEvent,
    ModelFinishReason, ModelMessage, ModelRef, ModelRequest, ModelStartFuture, ModelStream,
    ReasoningContent, ReasoningPreference, ToolCall, Usage,
};
use minicore_runtime::tools::{ToolOutput, ToolResultOutcome};
use minicore_runtime::{LoopId, ToolCallId};

use crate::config::{AgentConfig, CompactionConfig};
use crate::error::AgentError;
use crate::event::{AgentEvent, OutputChannel};
use crate::history::GetHistory;
use crate::ids::SessionId;
use crate::models::{ModelConfig, Models};
use crate::profiles::{ApprovalMode, Profile};
use crate::sessions::{
    LoopSubmission, RuntimeStartGate, WorkerGate, panic_next_worker,
    pause_next_admission_after_result, pause_next_compaction_after_result,
    pause_next_runtime_start_before_bind, pause_next_worker_before_join,
};
use crate::store::{
    SESSION_FORMAT_VERSION, SessionRecord, Store, StoredLoopOutcome, StoredLoopRecord,
    fail_next_append, fail_next_record_write,
};

use super::{
    Agent, CompactSession, CreateSession, RenameSession, SessionUpdateResult, TurnRef,
    UpdateSession,
};

use crate::openai_mock::{CapturedRequest, MockResponse, MockServer};

struct TestDirectoryGuard {
    path: PathBuf,
}

impl Drop for TestDirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn fixture_dir(label: &str) -> (PathBuf, TestDirectoryGuard) {
    let temp = std::fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
    let path = temp.join(format!("minicore-agent-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    (path.clone(), TestDirectoryGuard { path })
}

fn toml_path(path: &Path) -> String {
    toml::Value::String(path.to_string_lossy().into_owned()).to_string()
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

/// A model call that parks on `release` and signals `entered` once parked.
#[derive(Clone)]
struct BlockGate {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
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
    /// Text answer with a distinct per-request usage so per-request identity
    /// (loop_id, request_index) can be asserted (spec 9.4/12.4).
    TextWithUsage(&'static str, u64, u64),
    ToolCall(&'static str, serde_json::Value),
    ToolCallAfterGate(BlockGate, &'static str, serde_json::Value),
    /// Parks until `release` fires and signals `entered` when parked.
    BlockUntil(BlockGate, &'static str),
    BlockUntilDrop(BlockGate, Arc<AtomicBool>, &'static str),
    /// Delays a response long enough to exceed the old prompt default in the
    /// automatic-preparation timeout regression test.
    DelayedText(u64, &'static str),
    /// Emits many text deltas back-to-back with no awaits so the runtime can
    /// saturate its best-effort event queue deterministically, then ends the
    /// request with one tool call so the loop runs a tool (with an await)
    /// before its next request.
    BurstTextThenTool(&'static str, usize, &'static str, serde_json::Value),
    ContextOverflowNotStarted,
    Error(ModelError),
    StreamErrorAfterText(&'static str, ModelError),
}

struct FakeModel {
    model_ref: ModelRef,
    descriptor: ModelDescriptor,
    scripts: Arc<Mutex<VecDeque<ModelScript>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    contexts: Arc<Mutex<Vec<ModelCallContext>>>,
    start_entered: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}

impl FakeModel {
    fn new(model_ref: &str, scripts: impl IntoIterator<Item = ModelScript>) -> Arc<Self> {
        Self::with_window(model_ref, 16_384, scripts)
    }

    fn with_window(
        model_ref: &str,
        context_window: u64,
        scripts: impl IntoIterator<Item = ModelScript>,
    ) -> Arc<Self> {
        Self::with_reasoning(
            model_ref,
            context_window,
            fake_supported_reasoning(),
            scripts,
        )
    }

    fn with_reasoning(
        model_ref: &str,
        context_window: u64,
        supported_reasoning: BTreeSet<ReasoningPreference>,
        scripts: impl IntoIterator<Item = ModelScript>,
    ) -> Arc<Self> {
        let model_ref: ModelRef = model_ref.parse().unwrap();
        let descriptor =
            ModelDescriptor::new(model_ref.clone(), context_window, supported_reasoning, true)
                .unwrap();
        Arc::new(Self {
            model_ref,
            descriptor,
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
            contexts: Arc::new(Mutex::new(Vec::new())),
            start_entered: Arc::new(Notify::new()),
            calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn requests(&self) -> Arc<Mutex<Vec<ModelRequest>>> {
        Arc::clone(&self.requests)
    }

    fn contexts(&self) -> Arc<Mutex<Vec<ModelCallContext>>> {
        Arc::clone(&self.contexts)
    }

    fn start_entered(&self) -> Arc<Notify> {
        Arc::clone(&self.start_entered)
    }
}

impl Model for FakeModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn start(&self, request: ModelRequest, context: ModelCallContext) -> ModelStartFuture<'_> {
        self.start_entered.notify_one();
        let model_ref = self.model_ref.clone();
        self.requests.lock().unwrap().push(request);
        self.contexts.lock().unwrap().push(context);
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
                ModelScript::BlockUntilDrop(gate, dropped, text) => {
                    let _signal = DropSignal(dropped);
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
                ModelScript::DelayedText(delay_millis, text) => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_millis)).await;
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
                ModelScript::TextWithUsage(text, input, output) => events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(input, output, 0),
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ]),
                ModelScript::ContextOverflowNotStarted => {
                    let diagnostic = minicore_runtime::error::DiagnosticSummary::new(
                        minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
                        minicore_runtime::error::DiagnosticCategory::Model,
                        minicore_runtime::value::BoundedText::new("context window exceeded")
                            .unwrap(),
                        false,
                    );
                    Err(ModelError::permanent(
                        minicore_runtime::model::ModelErrorKind::ContextOverflow,
                        minicore_runtime::model::DeliveryState::NotStarted,
                        diagnostic,
                    ))
                }
                ModelScript::Error(err) => Err(err),
                ModelScript::StreamErrorAfterText(text, err) => {
                    let event_rows = vec![Ok(ModelEvent::text_delta(text).unwrap()), Err(err)];
                    let stream: ModelStream = Box::pin(stream::iter(event_rows));
                    Ok(stream)
                }
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
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
    }
}

fn auto_config(
    data_dir: PathBuf,
    models: BTreeMap<String, ModelConfig>,
    profile: Profile,
) -> AgentConfig {
    let mut config = config(data_dir, models, profile);
    config.compaction = CompactionConfig {
        enabled: true,
        trigger_percent: 80,
        target_percent: 50,
    };
    config
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

fn bash_profile() -> Profile {
    Profile {
        tools: vec!["bash".to_owned()],
        ..read_profile()
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
        // Fake models do not enforce a provider timeout. Leaving this unset
        // keeps tests that configure a short automatic deadline from inheriting
        // a synthetic 30-second provider floor.
        request_timeout_seconds: None,
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

async fn open_agent_auto(
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
    let config = auto_config(data_dir.to_path_buf(), models_config, profile);
    Agent::open_with_models(config, Models::from_values(models))
        .await
        .unwrap()
}

fn synthetic_loop_record(
    loop_id: minicore_runtime::LoopId,
    user_text: &str,
    assistant_text: &str,
    include_opaque_reasoning: bool,
) -> StoredLoopRecord {
    let mut items = vec![
        HistoryItem::User(UserHistory {
            loop_id,
            kind: UserMessageKind::Prompt,
            input: minicore_runtime::execution::UserInput::text(user_text).unwrap(),
        }),
        HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: 0,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::Text(assistant_text.to_owned())],
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::new(1, 2, 0),
        }),
    ];
    if include_opaque_reasoning {
        items.push(HistoryItem::Assistant(AssistantHistory {
            loop_id,
            request_index: 1,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::Reasoning(
                ReasoningContent::new(
                    None,
                    None,
                    Some("enc::snapshot-opaque".to_owned()),
                    Some("sig::snapshot-opaque".to_owned()),
                )
                .unwrap(),
            )],
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::new(1, 2, 0),
        }));
    }
    StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items,
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 0,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: "2026-01-02T03:04:05.000Z".to_owned(),
        user_times: None,
    }
}

fn synthetic_tool_loop_record(loop_id: minicore_runtime::LoopId) -> StoredLoopRecord {
    let call_id = ToolCallId::new("suffix-read-call").unwrap();
    let call = ToolCall::new(
        call_id.clone(),
        "read".parse().unwrap(),
        json!({"path": "suffix.txt"}),
        0,
    )
    .unwrap();
    StoredLoopRecord {
        loop_id,
        outcome: StoredLoopOutcome::Completed,
        items: vec![
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("suffix user").unwrap(),
            }),
            HistoryItem::Assistant(AssistantHistory {
                loop_id,
                request_index: 0,
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(call)],
                finish_reason: ModelFinishReason::ToolCalls,
                usage: Usage::new(1, 2, 0),
            }),
            HistoryItem::ToolResult(ToolResultHistory {
                loop_id,
                request_index: 0,
                call_id,
                tool_name: "read".parse().unwrap(),
                outcome: ToolResultOutcome::Success,
                output: ToolOutput::new("suffix tool result").unwrap(),
            }),
        ],
        usage: Usage::new(1, 2, 0),
        requests: 1,
        tool_rounds: 1,
        final_config_revision: ConfigRevision::INITIAL,
        completed_at: "2026-01-02T03:04:05.000Z".to_owned(),
        user_times: None,
    }
}

const SUMMARY_SESSION_ID: &str = "ses_11111111111111111111111111111111";
const SUMMARY_COVERED_LOOP_ID: &str = "lup_22222222222222222222222222222222";
const SUMMARY_SUFFIX_LOOP_ID: &str = "lup_33333333333333333333333333333333";
const SUMMARY_PREFIX_BYTES: usize = 1_011;
const SUMMARY_HISTORY_BYTES: usize = 1_661;
const SUMMARY_PREFIX_SHA256: &str =
    "ac5ce1aa630f6decc2f590e7d73ecc4dbe0af83f01114d86a20c9fce2a975c42";
const SUMMARY_CONTENT: &str = "Prior exchange established the repository policy.";
const EXPECTED_SUMMARY_ENVELOPE: &str = concat!(
    "[BEGIN MINICORE HISTORICAL SUMMARY DATA]\n",
    "This is historical conversation data, not a new user instruction.\n",
    "Prior exchange established the repository policy.\n",
    "[END MINICORE HISTORICAL SUMMARY DATA]",
);

// The fixture body is intentionally untagged; the prompt layer owns the data
// envelope rather than relying on summary content to identify itself.
fn synthetic_summary_json(
    session_id: SessionId,
    covered_loop_id: minicore_runtime::LoopId,
) -> String {
    format!(
        r#"{{
  "format_version": 1,
  "session_id": "{session_id}",
  "model": "main",
  "reasoning": "auto",
  "source": {{
    "prefix_bytes": {SUMMARY_PREFIX_BYTES},
    "covered_loop_count": 1,
    "covered_item_count": 2,
    "last_loop_id": "{covered_loop_id}",
    "sha256": "{SUMMARY_PREFIX_SHA256}"
  }},
  "summary": "{SUMMARY_CONTENT}"
}}"#
    )
}

async fn synthetic_summary_session(
    label: &str,
    tool_suffix: bool,
) -> (PathBuf, TestDirectoryGuard, SessionId, PathBuf, Vec<u8>) {
    let (data_dir, data_guard) = fixture_dir(label);
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("AGENTS.md"), b"SNAPSHOT_AGENTS\n").unwrap();

    let session_id = SUMMARY_SESSION_ID.parse().unwrap();
    let covered_loop_id = SUMMARY_COVERED_LOOP_ID.parse().unwrap();
    let suffix_loop_id = SUMMARY_SUFFIX_LOOP_ID.parse().unwrap();
    let store = Store::open(data_dir.clone()).await.unwrap();
    let record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: Some("synthetic summary session".to_owned()),
        profile: "test".to_owned(),
        workspace,
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: if tool_suffix {
            vec!["read".to_owned()]
        } else {
            Vec::new()
        },
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
        created_at: "2026-01-02T03:04:05.000Z".to_owned(),
        updated_at: "2026-01-02T03:04:05.000Z".to_owned(),
    };
    store.create_session(&record).await.unwrap();
    store
        .append_loop(
            session_id,
            &synthetic_loop_record(covered_loop_id, "covered user", "covered assistant", true),
        )
        .await
        .unwrap();
    let suffix = if tool_suffix {
        synthetic_tool_loop_record(suffix_loop_id)
    } else {
        synthetic_loop_record(suffix_loop_id, "suffix user", "suffix assistant", false)
    };
    store.append_loop(session_id, &suffix).await.unwrap();

    // The first raw record has three items, but sanitize_history drops its
    // opaque-only Assistant item. The snapshot covers two normalized items.
    let loaded = store.load_session(session_id).await.unwrap();
    assert_eq!(loaded.history.len(), if tool_suffix { 5 } else { 4 });
    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    if tool_suffix {
        assert!(history_before.len() > SUMMARY_PREFIX_BYTES);
    } else {
        assert_eq!(history_before.len(), SUMMARY_HISTORY_BYTES);
    }
    assert_eq!(history_before[SUMMARY_PREFIX_BYTES - 1], b'\n');

    let summary_path = history_path.parent().unwrap().join("summary.json");
    std::fs::write(
        summary_path,
        synthetic_summary_json(session_id, covered_loop_id),
    )
    .unwrap();
    (
        data_dir,
        data_guard,
        session_id,
        history_path,
        history_before,
    )
}

async fn install_valid_summary(data_dir: &Path, session_id: SessionId) {
    let store = Store::open(data_dir.to_path_buf()).await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    let source = store
        .capture_history_anchor(session_id, &loaded.history)
        .await
        .unwrap()
        .unwrap();
    let summary = minicore_runtime::value::BoundedText::new("bounded historical summary").unwrap();
    let bytes =
        crate::compaction::encode_snapshot(session_id, &loaded.record, &source, &summary).unwrap();
    std::fs::write(
        data_dir
            .join("sessions")
            .join(session_id.to_string())
            .join("summary.json"),
        bytes,
    )
    .unwrap();
}

async fn assert_runtime_limit_behavior(items: Vec<HistoryItem>, install_summary: bool) {
    let (data_dir, _data_guard) = fixture_dir(&format!("summary-runtime-limit-{}", next_id()));
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let session_id = SessionId::new().unwrap();
    let record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: None,
        profile: "test".to_owned(),
        workspace,
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: Vec::new(),
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
        created_at: "2026-01-02T03:04:05.000Z".to_owned(),
        updated_at: "2026-01-02T03:04:05.000Z".to_owned(),
    };
    let store = Store::open(data_dir.clone()).await.unwrap();
    store.create_session(&record).await.unwrap();
    let loop_id = LoopId::new().unwrap();
    store
        .append_loop(
            session_id,
            &StoredLoopRecord {
                loop_id,
                outcome: StoredLoopOutcome::Completed,
                items,
                usage: Usage::default(),
                requests: 1,
                tool_rounds: 0,
                final_config_revision: ConfigRevision::INITIAL,
                completed_at: "2026-01-02T03:04:05.000Z".to_owned(),
                user_times: None,
            },
        )
        .await
        .unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    let history_len = loaded.history.len();
    let limits = minicore_runtime::LoopOptions::default_checked()
        .unwrap()
        .limits;
    assert!(
        loaded.history.len() > limits.max_history_items
            || crate::sessions::estimate_history_bytes(&loaded.history) > limits.max_history_bytes
    );
    if install_summary {
        install_valid_summary(&data_dir, session_id).await;
    }
    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();

    let model = FakeModel::new("main", [ModelScript::Text("started")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.budget.estimated_request_context_tokens, None);
    if install_summary {
        assert_eq!(context.budget.estimated_history_items, 0);
        assert_eq!(context.budget.estimated_history_bytes, Some(0));
        assert_eq!(context.budget.estimated_history_tokens, Some(0));
        assert_eq!(context.budget.within_runtime_limits, Some(true));
    } else {
        assert_eq!(context.budget.estimated_history_items, history_len);
        assert_eq!(context.budget.estimated_history_bytes, None);
        assert_eq!(context.budget.estimated_history_tokens, None);
        assert_eq!(context.budget.within_runtime_limits, Some(false));
    }
    if !install_summary {
        assert!(matches!(
            agent
                .send(super::SendMessage {
                    session_id,
                    text: "current after unbounded history".to_owned(),
                })
                .await,
            Err(AgentError::HistoryTooLarge)
        ));
        assert_eq!(model.requests().lock().unwrap().len(), 0);
        assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
        return;
    }
    let turn = send_text(&mut agent, session_id, "current after bounded history").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(model.requests().lock().unwrap().len(), 1);
    let after = std::fs::read(&history_path).unwrap();
    assert!(after.starts_with(&history_before));
}

#[tokio::test]
async fn valid_summary_allows_history_over_runtime_item_limit_to_start() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|index| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(format!("old-{index}"))
                    .unwrap(),
            })
        })
        .collect();
    assert_runtime_limit_behavior(items, true).await;
}

#[tokio::test]
async fn valid_summary_allows_history_over_runtime_byte_limit_to_start() {
    let loop_id = LoopId::new().unwrap();
    let limits = minicore_runtime::LoopOptions::default_checked()
        .unwrap()
        .limits;
    let chunk_len = limits.max_model_text_bytes / 2;
    let item_count = limits.max_history_bytes / chunk_len + 1;
    let items = (0..item_count)
        .map(|index| {
            HistoryItem::Assistant(AssistantHistory {
                loop_id,
                request_index: u32::try_from(index).unwrap(),
                model: "main".parse::<ModelRef>().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::Text("x".repeat(chunk_len))],
                finish_reason: ModelFinishReason::Stop,
                usage: Usage::default(),
            })
        })
        .collect();
    assert_runtime_limit_behavior(items, true).await;
}

#[tokio::test]
async fn over_runtime_history_without_valid_summary_is_rejected() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|index| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(format!("old-{index}"))
                    .unwrap(),
            })
        })
        .collect();
    assert_runtime_limit_behavior(items, false).await;
}

/// Builds a stored session whose history exceeds the runtime item limit, then
/// opens it with automatic compaction enabled or disabled.
async fn auto_history_fixture(
    label: &str,
    enabled: bool,
) -> (
    PathBuf,
    TestDirectoryGuard,
    SessionId,
    Arc<FakeModel>,
    Agent,
) {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|_| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("x").unwrap(),
            })
        })
        .collect();
    auto_admission_fixture(
        label,
        enabled,
        items,
        [
            ModelScript::Text("folded summary"),
            ModelScript::Text("answer"),
        ],
        None,
        None,
    )
    .await
}

async fn auto_admission_fixture(
    label: &str,
    enabled: bool,
    items: Vec<HistoryItem>,
    scripts: impl IntoIterator<Item = ModelScript>,
    prompt_timeout_seconds: Option<u64>,
    model_timeout_seconds: Option<u64>,
) -> (
    PathBuf,
    TestDirectoryGuard,
    SessionId,
    Arc<FakeModel>,
    Agent,
) {
    auto_admission_fixture_with_window(
        label,
        enabled,
        1_000_000,
        items,
        scripts,
        prompt_timeout_seconds,
        model_timeout_seconds,
    )
    .await
}

async fn auto_admission_fixture_with_window(
    label: &str,
    enabled: bool,
    context_window: u64,
    items: Vec<HistoryItem>,
    scripts: impl IntoIterator<Item = ModelScript>,
    prompt_timeout_seconds: Option<u64>,
    model_timeout_seconds: Option<u64>,
) -> (
    PathBuf,
    TestDirectoryGuard,
    SessionId,
    Arc<FakeModel>,
    Agent,
) {
    let (data_dir, guard) = fixture_dir(label);
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let session_id = SessionId::new().unwrap();
    let record = SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title: None,
        profile: "test".to_owned(),
        workspace,
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["read".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
        created_at: "2026-01-02T03:04:05.000Z".to_owned(),
        updated_at: "2026-01-02T03:04:05.000Z".to_owned(),
    };
    let store = Store::open(data_dir.clone()).await.unwrap();
    store.create_session(&record).await.unwrap();
    let mut groups: Vec<(LoopId, Vec<HistoryItem>)> = Vec::new();
    for item in items {
        let loop_id = match &item {
            HistoryItem::User(user) => user.loop_id,
            HistoryItem::Assistant(assistant) => assistant.loop_id,
            HistoryItem::ToolResult(result) => result.loop_id,
            HistoryItem::Summary(_) => LoopId::new().unwrap(),
        };
        if groups
            .last()
            .is_some_and(|(candidate, _)| *candidate == loop_id)
        {
            groups.last_mut().unwrap().1.push(item);
        } else {
            groups.push((loop_id, vec![item]));
        }
    }
    for (loop_id, items) in groups {
        let request_count = items
            .iter()
            .filter_map(|item| match item {
                HistoryItem::Assistant(assistant) => assistant.request_index.checked_add(1),
                HistoryItem::ToolResult(result) => result.request_index.checked_add(1),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let tool_round_count = items
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    HistoryItem::Assistant(assistant)
                        if assistant
                            .content
                            .iter()
                            .any(|part| part.as_tool_call().is_some())
                )
            })
            .count();
        store
            .append_loop(
                session_id,
                &StoredLoopRecord {
                    loop_id,
                    outcome: StoredLoopOutcome::Completed,
                    items,
                    usage: Usage::default(),
                    requests: request_count,
                    tool_rounds: u16::try_from(tool_round_count).unwrap(),
                    final_config_revision: ConfigRevision::INITIAL,
                    completed_at: "2026-01-02T03:04:05.000Z".to_owned(),
                    user_times: None,
                },
            )
            .await
            .unwrap();
    }
    let model = FakeModel::with_window("main", context_window, scripts);
    let profile = read_profile();
    let models = BTreeMap::from([("main".to_owned(), Arc::clone(&model))]);
    let models_config =
        BTreeMap::from([("main".to_owned(), model_config("MINICORE_AGENT_TEST_KEY"))]);
    let mut config = if enabled {
        auto_config(data_dir.clone(), models_config, profile)
    } else {
        config(data_dir.clone(), models_config, profile)
    };
    config.loop_options.prompt_timeout_seconds = prompt_timeout_seconds;
    config.loop_options.model_timeout_seconds = model_timeout_seconds;
    let mut agent = Agent::open_with_models(
        config,
        Models::from_values(
            models
                .into_iter()
                .map(|(id, model)| (id, model as Arc<dyn Model>))
                .collect(),
        ),
    )
    .await
    .unwrap();
    agent.open_session(session_id).await.unwrap();
    (data_dir, guard, session_id, model, agent)
}

#[tokio::test]
async fn auto_compaction_starts_over_runtime_item_limit_history() {
    let (_data_dir, _guard, session_id, model, mut agent) =
        auto_history_fixture(&format!("auto-startup-{}", next_id()), true).await;
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.budget.input_budget_tokens, Some(1_000_000));
    assert_eq!(context.budget.trigger_tokens, Some(1_000_000 * 80 / 100));
    assert_eq!(context.budget.target_tokens, Some(1_000_000 * 50 / 100));
    let turn = send_text(&mut agent, session_id, "after auto summary").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    // Every utility call is a no-tools request; the final request is the real
    // loop request and carries the folded summary instead of the raw prefix.
    let requests = model.requests();
    let requests = requests.lock().unwrap();
    let utility_calls = requests
        .iter()
        .filter(|request| request.tools().is_empty())
        .count();
    let main_calls = requests
        .iter()
        .filter(|request| !request.tools().is_empty())
        .count();
    assert!(utility_calls >= 1);
    assert_eq!(main_calls, 1);
    assert!(
        requests[..requests.len() - 1]
            .iter()
            .all(|request| request.tools().is_empty())
    );
    let last = requests.last().unwrap();
    assert!(!last.tools().is_empty());
    assert!(last.messages().iter().any(|message| {
        matches!(message, minicore_runtime::model::ModelMessage::User(text) if text.contains("[BEGIN MINICORE HISTORICAL SUMMARY DATA]"))
    }));
    assert!(!last.messages().iter().any(|message| {
        matches!(message, minicore_runtime::model::ModelMessage::User(text) if text == "x")
    }));
}

#[tokio::test]
async fn auto_compaction_recovers_from_runtime_bytes_with_huge_tool_results() {
    let limits = minicore_runtime::LoopOptions::default_checked()
        .unwrap()
        .limits;
    let output = "x".repeat(256 * 1024);
    let group_count = limits.max_history_bytes / output.len() + 1;
    let items = (0..group_count)
        .flat_map(|index| {
            let loop_id = LoopId::new().unwrap();
            let call_id = ToolCallId::new(format!("huge-{index}")).unwrap();
            let call = ToolCall::new(
                call_id.clone(),
                "read".parse().unwrap(),
                json!({"path": "huge.txt"}),
                0,
            )
            .unwrap();
            [
                HistoryItem::Assistant(AssistantHistory {
                    loop_id,
                    request_index: u32::try_from(index).unwrap(),
                    model: "main".parse::<ModelRef>().unwrap(),
                    reasoning: ReasoningPreference::Auto,
                    content: vec![AssistantPart::ToolCall(call)],
                    finish_reason: ModelFinishReason::ToolCalls,
                    usage: Usage::default(),
                }),
                HistoryItem::ToolResult(ToolResultHistory {
                    loop_id,
                    request_index: u32::try_from(index).unwrap(),
                    call_id,
                    tool_name: "read".parse().unwrap(),
                    outcome: ToolResultOutcome::Success,
                    output: ToolOutput::new(output.clone()).unwrap(),
                }),
            ]
        })
        .collect();
    let (_data_dir, _guard, session_id, model, mut agent) = auto_admission_fixture(
        &format!("auto-bytes-{}", next_id()),
        true,
        items,
        [
            ModelScript::Text("folded summary"),
            ModelScript::Text("answer"),
        ],
        None,
        None,
    )
    .await;
    let turn = send_text(&mut agent, session_id, "after huge tool results").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let requests = model.requests();
    let requests = requests.lock().unwrap();
    let last = requests.last().unwrap();
    assert!(last.messages().iter().any(|message| {
        matches!(message, ModelMessage::User(text) if text.contains("[BEGIN MINICORE HISTORICAL SUMMARY DATA]"))
    }));
    assert!(!last.messages().iter().any(|message| {
        matches!(message, ModelMessage::Tool { output, .. } if output.content().byte_len() >= 256 * 1024)
    }));
    let observation = agent
        .session_context(session_id)
        .unwrap()
        .automatic
        .last
        .expect("automatic preparation must record an observation");
    assert_eq!(observation.outcome.as_str(), "started");
    assert!(observation.before_tokens.is_some());
    assert!(
        observation
            .utility_usage
            .as_ref()
            .is_some_and(|usage| usage.call_count >= 1)
    );
}

#[tokio::test]
async fn automatic_startup_preparation_can_outlive_the_old_prompt_timeout() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|_| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("x").unwrap(),
            })
        })
        .collect();
    let (_data_dir, _guard, session_id, model, mut agent) = auto_admission_fixture(
        &format!("auto-prompt-timeout-{}", next_id()),
        true,
        items,
        [
            ModelScript::DelayedText(60, "folded summary"),
            ModelScript::Text("answer"),
        ],
        Some(10),
        Some(100),
    )
    .await;
    let turn = send_text(&mut agent, session_id, "after delayed summary").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert!(model.requests().lock().unwrap().len() >= 2);
}

#[tokio::test]
async fn automatic_startup_total_deadline_cancels_the_utility_worker() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..8)
        .map(|index| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text(format!(
                    "history-{index} {}",
                    "x".repeat(2_048)
                ))
                .unwrap(),
            })
        })
        .collect();
    let gate = BlockGate::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let (_data_dir, _guard, session_id, _model, mut agent) = auto_admission_fixture_with_window(
        &format!("auto-deadline-{}", next_id()),
        true,
        4_000,
        items,
        [ModelScript::BlockUntilDrop(
            gate.clone(),
            Arc::clone(&dropped),
            "summary",
        )],
        Some(2),
        Some(2),
    )
    .await;
    let (result, gate_started) = tokio::join!(
        agent.send(super::SendMessage {
            session_id,
            text: "deadline".to_owned(),
        }),
        tokio::time::timeout(std::time::Duration::from_secs(1), gate.entered.notified())
    );
    assert!(
        gate_started.is_ok(),
        "automatic deadline test must reach the utility model before timing out"
    );
    assert!(matches!(result, Err(AgentError::Internal)));
    agent.close_session(session_id).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn automatic_request_compaction_rederives_after_a_smaller_model_update() {
    let (data_dir, _guard) = fixture_dir(&format!("auto-hot-window-{}", next_id()));
    let file = (0..16)
        .map(|_| "x".repeat(1_024))
        .collect::<Vec<_>>()
        .join("\n");
    let (workspace, _workspace_guard) =
        workspace_file("auto-hot-window-ws", "a.txt", file.as_bytes());
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "main",
        [ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({"path": "a.txt", "limit": 16}),
        )],
    );
    let mut model_b_scripts =
        std::iter::repeat_n(ModelScript::Text("folded hot summary"), 8).collect::<Vec<_>>();
    model_b_scripts.push(ModelScript::Text("answer"));
    let model_b = FakeModel::with_window("other", 4_000, model_b_scripts);
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read the file").await;
    gate.entered.notified().await;
    agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(model_a.calls.load(Ordering::SeqCst), 1);
    let stored = read_store_history(&data_dir, info.session_id).await;
    let tool_results = stored
        .iter()
        .filter(|item| {
            matches!(
                item,
                HistoryItem::ToolResult(result) if result.output.content().byte_len() > 16 * 1024
            )
        })
        .count();
    assert_eq!(tool_results, 1);
    let requests = model_b.requests();
    let requests = requests.lock().unwrap();
    let utility_calls = requests
        .iter()
        .filter(|request| request.tools().is_empty())
        .count();
    let main_requests = requests
        .iter()
        .filter(|request| !request.tools().is_empty())
        .collect::<Vec<_>>();
    assert!(utility_calls >= 1);
    assert_eq!(
        model_b.calls.load(Ordering::SeqCst),
        utility_calls + main_requests.len()
    );
    assert_eq!(main_requests.len(), 1);
    assert!(main_requests[0].messages().iter().any(|message| {
        matches!(message, ModelMessage::User(text) if text.contains("folded hot summary"))
    }));
    let observation = agent
        .session_context(info.session_id)
        .unwrap()
        .automatic
        .last
        .expect("model update must record automatic preparation");
    assert!(observation.before_tokens.unwrap() > observation.trigger_tokens);
    assert!(
        observation
            .utility_usage
            .as_ref()
            .is_some_and(|usage| usage.call_count >= 1)
    );
}

#[tokio::test]
async fn automatic_request_compaction_preserves_a_steer_alongside_tool_summary() {
    let (data_dir, _guard) = fixture_dir(&format!("auto-steer-compress-{}", next_id()));
    let file = (0..16)
        .map(|_| "x".repeat(1_024))
        .collect::<Vec<_>>()
        .join("\n");
    let (workspace, _workspace_guard) =
        workspace_file("auto-steer-compress-ws", "a.txt", file.as_bytes());
    let gate = BlockGate::new();
    let mut model_scripts = vec![ModelScript::ToolCallAfterGate(
        gate.clone(),
        "read",
        json!({"path": "a.txt", "limit": 16}),
    )];
    model_scripts.extend(std::iter::repeat_n(
        ModelScript::Text("folded steer summary"),
        8,
    ));
    model_scripts.push(ModelScript::Text("answer"));
    let model = FakeModel::with_window("main", 4_000, model_scripts);
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read then steer").await;
    gate.entered.notified().await;
    agent
        .steer(super::SteerMessage {
            turn,
            text: "keep the final answer concise".to_owned(),
        })
        .unwrap();
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let stored = read_store_history(&data_dir, info.session_id).await;
    let tool_results = stored
        .iter()
        .filter(|item| {
            matches!(
                item,
                HistoryItem::ToolResult(result) if result.output.content().byte_len() > 16 * 1024
            )
        })
        .count();
    assert_eq!(tool_results, 1);
    let requests = model.requests();
    let requests = requests.lock().unwrap();
    let utility_calls = requests
        .iter()
        .filter(|request| request.tools().is_empty())
        .count();
    let main_requests = requests
        .iter()
        .filter(|request| !request.tools().is_empty())
        .collect::<Vec<_>>();
    assert!(utility_calls >= 1);
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        utility_calls + main_requests.len()
    );
    assert_eq!(main_requests.len(), 2);
    let final_request = main_requests.last().unwrap();
    assert!(final_request.messages().iter().any(|message| {
        matches!(message, ModelMessage::User(text) if text == "keep the final answer concise")
    }));
    assert!(final_request.messages().iter().any(|message| {
        matches!(message, ModelMessage::User(text) if text.contains("folded steer summary"))
    }));
    let observation = agent
        .session_context(info.session_id)
        .unwrap()
        .automatic
        .last
        .expect("steer compaction must record an automatic observation");
    assert!(observation.before_tokens.unwrap() > observation.trigger_tokens);
    assert!(
        observation
            .utility_usage
            .as_ref()
            .is_some_and(|usage| usage.call_count >= 1)
    );
}

#[tokio::test]
async fn disabled_auto_keeps_over_runtime_history_rejected() {
    let (data_dir, _guard, session_id, model, mut agent) =
        auto_history_fixture(&format!("auto-disabled-{}", next_id()), false).await;
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.budget.input_budget_tokens, None);
    assert_eq!(context.budget.trigger_tokens, None);
    assert_eq!(context.budget.target_tokens, None);
    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let before = std::fs::read(&history_path).unwrap();
    assert!(matches!(
        agent
            .send(super::SendMessage {
                session_id,
                text: "should be rejected".to_owned(),
            })
            .await,
        Err(AgentError::HistoryTooLarge)
    ));
    assert_eq!(model.requests().lock().unwrap().len(), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), before);
    assert!(
        !data_dir
            .join("sessions")
            .join(session_id.to_string())
            .join("summary.json")
            .exists()
    );
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
async fn rename_loaded_session_trims_clears_and_rejects_invalid_titles() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-loaded-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-loaded-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    let lower_bound = crate::store::utc_timestamp().unwrap();

    let renamed = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "\u{2003} Renamed session \u{3000}".to_owned(),
        })
        .await
        .unwrap();
    assert!(renamed.loaded);
    assert_eq!(renamed.title.as_deref(), Some("Renamed session"));
    let after = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(after.title.as_deref(), Some("Renamed session"));
    let upper_bound = crate::store::utc_timestamp().unwrap();
    assert!(after.updated_at >= lower_bound);
    assert!(after.updated_at <= upper_bound);
    assert_eq!(after.profile, before.profile);
    assert_eq!(after.workspace, before.workspace);
    assert_eq!(after.model, before.model);
    assert_eq!(after.reasoning, before.reasoning);
    assert_eq!(after.system_prompt, before.system_prompt);
    assert_eq!(after.tools, before.tools);
    assert_eq!(after.max_tool_rounds, before.max_tool_rounds);
    assert_eq!(after.approval, before.approval);
    assert_eq!(after.created_at, before.created_at);

    let cleared = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: " \t ".to_owned(),
        })
        .await
        .unwrap();
    assert!(cleared.title.is_none());

    let exact_multibyte = "é".repeat(2_048);
    let exact = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: exact_multibyte,
        })
        .await
        .unwrap();
    assert_eq!(exact.title.as_deref().unwrap().len(), 4_096);
    let over_limit = format!("{}a", exact.title.unwrap());
    let result = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: over_limit,
        })
        .await;
    assert!(matches!(result, Err(AgentError::InvalidInput)));

    for title in ["bad\nname", "bad\0name", &"x".repeat(4_097)] {
        let result = agent
            .rename_session(RenameSession {
                session_id: info.session_id,
                title: title.to_string(),
            })
            .await;
        assert!(matches!(result, Err(AgentError::InvalidInput)));
    }
}

#[tokio::test]
async fn rename_unloaded_session_updates_only_the_persistent_metadata() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-unloaded-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-unloaded-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "keep history").await;
    wait_text(&agent, turn).await;
    let store = Store::open(data_dir.clone()).await.unwrap();
    let before = store.load_session(info.session_id).await.unwrap();
    assert!(!before.history.is_empty());
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    agent.close_session(info.session_id).await.unwrap();
    std::fs::remove_dir_all(&workspace).unwrap();
    std::fs::remove_file(&history_path).unwrap();

    let renamed = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "Unloaded title".to_owned(),
        })
        .await
        .unwrap();
    assert!(!renamed.loaded);
    assert_eq!(renamed.title.as_deref(), Some("Unloaded title"));

    let after = store.load_record(info.session_id).await.unwrap();
    assert_eq!(after.title.as_deref(), Some("Unloaded title"));
    assert_eq!(after.system_prompt, before.record.system_prompt);
    assert!(!history_path.exists());
}

#[tokio::test]
async fn rename_serializes_with_busy_completion_and_preserves_a_blocked_session() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-busy-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-busy-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "busy").await;
    gate.entered.notified().await;

    let renamed = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "While busy".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(renamed.title.as_deref(), Some("While busy"));
    gate.release.notify_waiters();
    wait_text(&agent, turn).await;
    let record = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(record.title.as_deref(), Some("While busy"));

    let (blocked_data, _blocked_guard) = fixture_dir(&format!("rename-blocked-{}", next_id()));
    let (blocked_workspace, _blocked_workspace_guard) =
        workspace_file("rename-blocked-ws", "a.txt", b"hello");
    let blocked_model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut blocked_agent = open_agent(
        &blocked_data,
        BTreeMap::from([("main".to_owned(), blocked_model)]),
        read_profile(),
    )
    .await;
    let blocked_info = create_session(&mut blocked_agent, &blocked_workspace).await;
    fail_next_append(blocked_info.session_id);
    let blocked_turn = send_text(&mut blocked_agent, blocked_info.session_id, "block").await;
    let result = wait_text(&blocked_agent, blocked_turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(
        blocked_agent
            .session_state(blocked_info.session_id)
            .unwrap()
            .status,
        crate::sessions::SessionStatus::Blocked
    );
    let renamed_blocked = blocked_agent
        .rename_session(RenameSession {
            session_id: blocked_info.session_id,
            title: "Blocked title".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(renamed_blocked.title.as_deref(), Some("Blocked title"));
    assert_eq!(
        blocked_agent
            .session_state(blocked_info.session_id)
            .unwrap()
            .status,
        crate::sessions::SessionStatus::Blocked
    );
}

#[tokio::test]
async fn rename_persistence_failure_leaves_memory_and_disk_unchanged() {
    let (data_dir, _guard) = fixture_dir(&format!("rename-failure-{}", next_id()));
    let (workspace, _guard) = workspace_file("rename-failure-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let before = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    fail_next_record_write(info.session_id);

    let result = agent
        .rename_session(RenameSession {
            session_id: info.session_id,
            title: "Should not persist".to_owned(),
        })
        .await;
    assert!(matches!(result, Err(AgentError::Store)));
    let in_memory = agent.list_sessions().await.unwrap()[0].clone();
    assert_eq!(in_memory.title, info.title);
    assert_eq!(in_memory.updated_at, before.updated_at);
    let record = Store::open(data_dir)
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(record.title, info.title);
    assert_eq!(record.updated_at, before.updated_at);
}

#[tokio::test]
async fn system_prompt_file_is_loaded_once_and_session_snapshot_survives_reopen() {
    let (base, _guard) = fixture_dir(&format!("prompt-file-snapshot-{}", next_id()));
    std::fs::create_dir_all(&base).unwrap();
    let (workspace, _workspace_guard) = workspace_file("prompt-file-ws", "a.txt", b"hello");
    let prompt_path = base.join("prompt");
    let config_path = base.join("agent.toml");
    std::fs::write(&prompt_path, "file prompt\r\noriginal").unwrap();
    let text = format!(
        r#"
data_dir = {data_dir}
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = {{ file = {prompt_path} }}
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_PROMPT_FILE_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 30
"#,
        data_dir = toml_path(&base.join("data")),
        prompt_path = toml_path(&prompt_path)
    );
    std::fs::write(&config_path, text).unwrap();
    let config = AgentConfig::load(&config_path).unwrap();
    assert_eq!(
        config.profiles["test"].system_prompt,
        "file prompt\noriginal"
    );
    let model = FakeModel::new("main", []);
    let mut agent = Agent::open_with_models(
        config,
        Models::from_values(BTreeMap::from([(
            "main".to_owned(),
            model as Arc<dyn Model>,
        )])),
    )
    .await
    .unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let store = Store::open(base.join("data")).await.unwrap();
    assert_eq!(
        store
            .load_record(info.session_id)
            .await
            .unwrap()
            .system_prompt,
        "file prompt\noriginal"
    );

    std::fs::write(&prompt_path, "changed after Agent startup").unwrap();
    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    let reopened = store.load_record(info.session_id).await.unwrap();
    assert_eq!(reopened.system_prompt, "file prompt\noriginal");
}

#[test]
fn rename_request_debug_does_not_include_the_title() {
    let request = RenameSession {
        session_id: crate::ids::SessionId::new().unwrap(),
        title: "private title".to_owned(),
    };
    assert!(!format!("{request:?}").contains("private title"));
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

#[tokio::test]
async fn reopened_session_projects_external_summary_as_bounded_user_data() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-snapshot-{}", next_id()), false).await;

    let model = FakeModel::new("main", []);
    let next_model = FakeModel::new("other", [ModelScript::Text("next answer")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model)),
            ("other".to_owned(), Arc::clone(&next_model)),
        ]),
        read_profile(),
    )
    .await;
    let reopened = agent.open_session(session_id).await.unwrap();
    assert_eq!(reopened.session_id, session_id);
    let context = agent.session_context(session_id).unwrap();
    assert_eq!(context.coverage.covered_loop_count, 1);
    assert_eq!(context.coverage.covered_item_count, 2);
    assert_eq!(context.coverage.retained_item_count, 2);
    assert_eq!(context.budget.estimated_history_items, 2);
    assert_eq!(context.budget.within_runtime_limits, Some(true));
    // Loading and projecting a derived snapshot must not rewrite core history.
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let candidate = agent.config().clone();
    let candidate_models = Models::from_values(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model) as Arc<dyn Model>),
        (
            "other".to_owned(),
            Arc::clone(&next_model) as Arc<dyn Model>,
        ),
    ]));
    agent
        .reload_settings_with_models(candidate, candidate_models)
        .unwrap();
    agent
        .update_session(UpdateSession {
            session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

    let turn = send_text(&mut agent, session_id, "current user").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let requests = next_model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let messages = requests[0].messages();

    let systems = messages
        .iter()
        .filter_map(|message| match message {
            ModelMessage::System(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(systems.len(), 1);
    assert!(systems[0].contains("test system prompt"));
    assert!(systems[0].contains("SNAPSHOT_AGENTS"));
    assert!(!systems[0].contains(SUMMARY_CONTENT));
    assert!(!systems[0].contains("current user"));
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::System(text) if text.contains(SUMMARY_CONTENT))
    }));

    let summary_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT)),
        )
        .expect("valid summary snapshot must become a non-system data message");
    let summary_text = match &messages[summary_index] {
        ModelMessage::User(text) => text,
        _ => unreachable!("summary index is selected from User messages"),
    };
    assert_eq!(summary_text.as_str(), EXPECTED_SUMMARY_ENVELOPE);
    assert_ne!(summary_text.as_str(), SUMMARY_CONTENT);

    let suffix_user_index = messages
        .iter()
        .position(|message| matches!(message, ModelMessage::User(text) if text == "suffix user"))
        .expect("uncovered suffix user must remain in the request");
    let suffix_assistant_index = messages
        .iter()
        .position(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text == "suffix assistant")))
        })
        .expect("uncovered suffix assistant must remain in the request");
    let current_user_index = messages
        .iter()
        .position(|message| matches!(message, ModelMessage::User(text) if text == "current user"))
        .expect("current user must remain in the request");
    assert!(summary_index < suffix_user_index);
    assert!(suffix_user_index < suffix_assistant_index);
    assert!(suffix_assistant_index < current_user_index);
    assert!(
        !messages.iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text == "covered user")
        })
    );
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text == "covered assistant")))
    }));

    // The request reached a completed Agent loop, so the final projected
    // ModelRequest passed Runtime's exchange validator.
    assert!(
        std::fs::read(&history_path)
            .unwrap()
            .starts_with(&history_before)
    );
}

#[tokio::test]
async fn projected_summary_binding_survives_a_model_update_in_the_same_loop() {
    let (data_dir, _data_guard, session_id, _history_path, _history_before) =
        synthetic_summary_session(&format!("summary-same-loop-update-{}", next_id()), true).await;
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "main",
        [ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({"path": "suffix.txt"}),
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
    agent.open_session(session_id).await.unwrap();

    let turn = send_text(&mut agent, session_id, "same loop current").await;
    gate.entered.notified().await;
    let updated = agent
        .update_session(UpdateSession {
            session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    assert!(updated.active_revision.is_some());
    gate.release.notify_waiters();
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let requests = model_b.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let messages = requests[0].messages();
    assert_eq!(
        messages
            .iter()
            .filter(|message| {
                matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT))
            })
            .count(),
        1
    );
    assert!(
        messages
            .iter()
            .any(|message| matches!(message, ModelMessage::User(text) if text == "suffix user"))
    );
    assert!(messages.iter().any(|message| {
        matches!(message, ModelMessage::Tool { tool_call_id, .. } if tool_call_id.as_str() == "suffix-read-call")
    }));
    assert!(
        messages.iter().any(
            |message| matches!(message, ModelMessage::User(text) if text == "same loop current")
        )
    );
    assert!(
        !messages
            .iter()
            .any(|message| matches!(message, ModelMessage::User(text) if text == "covered user"))
    );
}

#[tokio::test]
async fn snapshot_with_same_count_different_loaded_history_is_ignored() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(
            &format!("summary-loaded-history-mismatch-{}", next_id()),
            false,
        )
        .await;
    let summary_path = history_path.parent().unwrap().join("summary.json");
    let summary_before = std::fs::read(&summary_path).unwrap();
    let store = Store::open(data_dir).await.unwrap();
    let loaded = store.load_session(session_id).await.unwrap();
    let mut different_history = loaded.history.to_vec();
    let mut changed = false;
    for item in &mut different_history {
        let HistoryItem::User(user) = item else {
            continue;
        };
        let loop_id = user.loop_id;
        let kind = user.kind;
        user.input =
            minicore_runtime::execution::UserInput::text("different loaded history user").unwrap();
        assert_eq!(user.loop_id, loop_id);
        assert_eq!(user.kind, kind);
        changed = true;
        break;
    }
    assert!(changed);
    assert_eq!(different_history.len(), loaded.history.len());

    let state = crate::compaction::load_state(&store, session_id, &different_history).await;
    assert!(state.project(&different_history).is_none());
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&summary_path).unwrap(), summary_before);
}

#[tokio::test]
async fn invalid_external_summary_falls_back_to_full_history() {
    for variant in [
        "hash",
        "hash-shape",
        "truncated",
        "boundary",
        "count",
        "last-loop",
        "version",
        "unknown",
        "wrong-session",
        "blank",
        "body-oversized",
        "oversized",
        "directory",
    ] {
        let (data_dir, _data_guard, session_id, history_path, history_before) =
            synthetic_summary_session(&format!("summary-fallback-{variant}-{}", next_id()), false)
                .await;
        let summary_path = history_path.parent().unwrap().join("summary.json");
        match variant {
            "hash" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                let old = format!("\"sha256\": \"{SUMMARY_PREFIX_SHA256}\"");
                let new = format!("\"sha256\": \"b{}\"", &SUMMARY_PREFIX_SHA256[1..]);
                assert!(raw.contains(&old));
                std::fs::write(&summary_path, raw.replacen(&old, &new, 1)).unwrap();
            }
            "hash-shape" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &format!("\"sha256\": \"{SUMMARY_PREFIX_SHA256}\""),
                        "\"sha256\": \"bad\"",
                        1,
                    ),
                )
                .unwrap();
            }
            "truncated" => {
                std::fs::write(&summary_path, b"{\"format_version\":1").unwrap();
            }
            "boundary" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &format!("\"prefix_bytes\": {SUMMARY_PREFIX_BYTES}"),
                        "\"prefix_bytes\": 1010",
                        1,
                    ),
                )
                .unwrap();
            }
            "count" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen("\"covered_item_count\": 2", "\"covered_item_count\": 3", 1),
                )
                .unwrap();
            }
            "last-loop" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &format!("\"last_loop_id\": \"{SUMMARY_COVERED_LOOP_ID}\""),
                        "\"last_loop_id\": \"lup_44444444444444444444444444444444\"",
                        1,
                    ),
                )
                .unwrap();
            }
            "version" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen("\"format_version\": 1", "\"format_version\": 2", 1),
                )
                .unwrap();
            }
            "unknown" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                let raw = raw.replace(SUMMARY_CONTENT, "DERIVED_SECRET");
                let insert_at = raw.rfind('}').unwrap();
                let mut raw = raw;
                raw.insert_str(insert_at, ",\n  \"unknown\": true");
                std::fs::write(&summary_path, raw).unwrap();
            }
            "wrong-session" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(
                    &summary_path,
                    raw.replacen(
                        &session_id.to_string(),
                        "ses_44444444444444444444444444444444",
                        1,
                    ),
                )
                .unwrap();
            }
            "blank" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                std::fs::write(&summary_path, raw.replacen(SUMMARY_CONTENT, r#" \n\t "#, 1))
                    .unwrap();
            }
            "body-oversized" => {
                let raw = std::fs::read_to_string(&summary_path).unwrap();
                let oversized = "x".repeat(64 * 1024 + 1);
                std::fs::write(&summary_path, raw.replacen(SUMMARY_CONTENT, &oversized, 1))
                    .unwrap();
            }
            "oversized" => {
                std::fs::write(&summary_path, vec![b'x'; 256 * 1024 + 1]).unwrap();
            }
            "directory" => {
                std::fs::remove_file(&summary_path).unwrap();
                std::fs::create_dir(&summary_path).unwrap();
            }
            _ => unreachable!("all summary fallback variants are listed above"),
        }

        let model = FakeModel::new("main", [ModelScript::Text("fallback answer")]);
        let mut agent = open_agent(
            &data_dir,
            BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
            read_profile(),
        )
        .await;
        agent.open_session(session_id).await.unwrap();
        assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
        let turn = send_text(&mut agent, session_id, "fallback current user").await;
        wait_text(&agent, turn).await;

        let requests = model.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "fallback variant: {variant}");
        let messages = requests[0].messages();
        assert!(
            messages.iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text == "covered user")
            }),
            "covered user missing for {variant}"
        );
        assert!(messages.iter().any(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::Text(text) if text == "covered assistant")))
        }), "covered assistant missing for {variant}");
        assert!(
            messages.iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text == "suffix user")
            }),
            "suffix user missing for {variant}"
        );
        assert!(
            messages.iter().any(|message| {
                matches!(message, ModelMessage::User(text) if text == "fallback current user")
            }),
            "current user missing for {variant}"
        );
        assert!(!messages.iter().any(|message| {
            matches!(message, ModelMessage::User(text) | ModelMessage::System(text) if text.contains(SUMMARY_CONTENT) || text.contains("DERIVED_SECRET"))
        }), "invalid derived body leaked for {variant}");
        assert!(
            std::fs::read(&history_path)
                .unwrap()
                .starts_with(&history_before)
        );
    }
}

#[tokio::test]
async fn invalid_summary_does_not_mask_core_history_corruption() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-core-corrupt-{}", next_id()), false).await;
    let summary_path = history_path.parent().unwrap().join("summary.json");
    std::fs::write(&summary_path, vec![b'x'; 256 * 1024 + 1]).unwrap();

    let mut corrupt_history = history_before;
    corrupt_history.extend_from_slice(b"{\"broken\":true}\n");
    std::fs::write(&history_path, corrupt_history).unwrap();

    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    assert!(matches!(
        agent.open_session(session_id).await,
        Err(AgentError::Store)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn summary_symlink_is_ignored_but_core_symlink_is_not() {
    use std::os::unix::fs::symlink;

    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-symlink-{}", next_id()), false).await;
    let summary_path = history_path.parent().unwrap().join("summary.json");
    let summary_target = data_dir.join("summary-target.json");
    std::fs::copy(&summary_path, &summary_target).unwrap();
    std::fs::remove_file(&summary_path).unwrap();
    symlink(&summary_target, &summary_path).unwrap();

    let model = FakeModel::new("main", [ModelScript::Text("symlink fallback")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let turn = send_text(&mut agent, session_id, "symlink current user").await;
    wait_text(&agent, turn).await;
    let requests = model.requests();
    {
        let requests = requests.lock().unwrap();
        assert!(
            requests[0].messages().iter().any(
                |message| matches!(message, ModelMessage::User(text) if text == "covered user")
            )
        );
        assert!(!requests[0].messages().iter().any(|message| {
            matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT))
        }));
    }

    agent.close_session(session_id).await.unwrap();
    let record_path = history_path.parent().unwrap().join("session.json");
    let record_target = data_dir.join("record-target.json");
    std::fs::copy(&record_path, &record_target).unwrap();
    std::fs::remove_file(&record_path).unwrap();
    symlink(&record_target, &record_path).unwrap();
    assert!(matches!(
        agent.open_session(session_id).await,
        Err(AgentError::Store)
    ));
}

#[tokio::test]
async fn projected_summary_keeps_suffix_tool_exchange_and_current_user() {
    let (data_dir, _data_guard, session_id, history_path, history_before) =
        synthetic_summary_session(&format!("summary-tool-suffix-{}", next_id()), true).await;
    let model = FakeModel::new("main", [ModelScript::Text("tool suffix answer")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    agent.open_session(session_id).await.unwrap();
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let turn = send_text(&mut agent, session_id, "tool current user").await;
    wait_text(&agent, turn).await;

    let requests = model.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.tools().len(), 1);
    let messages = request.messages();
    let summary_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text.contains(SUMMARY_CONTENT)),
        )
        .expect("tool suffix request must contain the summary data message");
    let suffix_user_index = messages
        .iter()
        .position(|message| matches!(message, ModelMessage::User(text) if text == "suffix user"))
        .expect("tool suffix user must remain");
    let call_index = messages
        .iter()
        .position(|message| {
            matches!(message, ModelMessage::Assistant(parts) if parts.iter().any(|part| matches!(part, AssistantPart::ToolCall(call) if call.tool_call_id().as_str() == "suffix-read-call")))
        })
        .expect("tool call assistant must remain");
    let result_index = messages
        .iter()
        .position(|message| {
            matches!(message, ModelMessage::Tool { tool_call_id, .. } if tool_call_id.as_str() == "suffix-read-call")
        })
        .expect("tool result must remain");
    let current_index = messages
        .iter()
        .position(
            |message| matches!(message, ModelMessage::User(text) if text == "tool current user"),
        )
        .expect("current user must remain");
    assert!(summary_index < suffix_user_index);
    assert!(suffix_user_index < call_index);
    assert!(call_index < result_index);
    assert!(result_index < current_index);
    assert!(!messages.iter().any(|message| {
        matches!(message, ModelMessage::System(text) if text.contains(SUMMARY_CONTENT))
    }));
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

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let crate::history::HistoryItemView::Assistant(assistant) = &page.items[1].item else {
        panic!("expected the first assistant history item");
    };
    assert_eq!(assistant.parts.as_ref().unwrap().len(), 1);
    let display = assistant.tool_calls[0]
        .display
        .as_ref()
        .expect("history tool display");
    assert_eq!(display.detail, "file.txt:1-32");
    assert_eq!(display.hidden_line_count, Some(5));
}

#[tokio::test]
async fn live_tool_presentation_and_result_keep_runtime_identity() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-live-{}", next_id()));
    let (workspace, _guard) = workspace_file("presentation-live-ws", "file.txt", b"file contents");
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
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let mut presentation = None;
    let mut tool_result = None;
    for _ in 0..32 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("live Agent event must arrive")
            .expect("event stream must remain open");
        match event {
            AgentEvent::ToolPresentation {
                turn: event_turn,
                request_index,
                tool_call_id,
                display,
                ..
            } => presentation = Some((event_turn, request_index, tool_call_id, display)),
            AgentEvent::ToolFinished {
                turn: event_turn,
                request_index,
                tool_call_id,
                result,
                ..
            } => tool_result = Some((event_turn, request_index, tool_call_id, result)),
            AgentEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }

    let (event_turn, request_index, tool_call_id, display) =
        presentation.expect("tool presentation event");
    assert_eq!(event_turn, turn);
    assert_eq!(request_index, 0);
    assert_eq!(display.detail, "file.txt:1-32");
    assert_eq!(display.hidden_line_count, Some(5));
    let (result_turn, result_request_index, result_tool_call_id, result) =
        tool_result.expect("tool result event");
    assert_eq!(result_turn, turn);
    assert_eq!(result_request_index, 0);
    assert_eq!(result_tool_call_id, tool_call_id);
    assert_eq!(result.content.as_deref(), Some("1: file contents"));
    assert!(!result.content_truncated);
}

#[tokio::test]
async fn failed_tool_presentation_exposes_safe_error_and_history_fallback() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-failed-{}", next_id()));
    let (workspace, _guard) =
        workspace_file("presentation-failed-ws", "file.txt", b"file contents");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "missing-secret.txt"})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read the missing file").await;
    wait_text(&agent, turn).await;

    let mut finished = None;
    while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
        .await
        .expect("failed tool events must arrive")
    {
        if let AgentEvent::ToolFinished { result, .. } = event {
            finished = Some(result);
            break;
        }
    }
    let result = finished.expect("failed tool must emit ToolFinished");
    assert_eq!(result.outcome, ToolResultOutcome::Failed);
    assert_eq!(result.content.as_deref(), Some("tool execution failed"));
    assert!(
        !result
            .content
            .as_deref()
            .unwrap_or_default()
            .contains("missing-secret.txt")
    );

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let tool_result = page
        .items
        .iter()
        .find_map(|item| match &item.item {
            crate::history::HistoryItemView::ToolResult(result) => Some(result),
            _ => None,
        })
        .expect("failed tool must be present in history");
    assert_eq!(tool_result.outcome, ToolResultOutcome::Failed);
    assert_eq!(tool_result.content, "tool failed");
    assert!(!tool_result.content.contains("missing-secret.txt"));
}

#[tokio::test]
async fn request_usage_events_carry_real_per_request_usage_across_two_sessions() {
    let (data_dir, _guard) = fixture_dir(&format!("usage-two-sessions-{}", next_id()));
    let (workspace_a, _guard_a) = workspace_file("usage-session-a", "a.txt", b"a");
    let (workspace_b, _guard_b) = workspace_file("usage-session-b", "b.txt", b"b");
    let model = FakeModel::new(
        "provider/model-id",
        [
            ModelScript::TextWithUsage("a answer", 10, 20),
            ModelScript::TextWithUsage("b answer", 300, 400),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let session_a = create_session(&mut agent, &workspace_a).await;
    let session_b = create_session(&mut agent, &workspace_b).await;
    let turn_a = send_text(&mut agent, session_a.session_id, "a").await;
    wait_text(&agent, turn_a).await;
    let turn_b = send_text(&mut agent, session_b.session_id, "b").await;
    wait_text(&agent, turn_b).await;

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("request_usage event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::RequestUsage {
            turn,
            request_index,
            usage,
            ..
        } = event
        {
            seen.push((
                turn,
                request_index,
                usage.input_tokens(),
                usage.output_tokens(),
            ));
        }
    }
    assert!(
        seen.contains(&(turn_a, 0, Some(10), Some(20))),
        "seen: {seen:?}"
    );
    assert!(
        seen.contains(&(turn_b, 0, Some(300), Some(400))),
        "seen: {seen:?}"
    );
}

#[tokio::test]
async fn request_usage_is_keyed_per_request_index_within_one_loop() {
    let (data_dir, _guard) = fixture_dir(&format!("usage-requests-{}", next_id()));
    let (workspace, _guard) = workspace_file("usage-requests-ws", "a.txt", b"file contents");
    // Request 0 finishes with a tool call (its Usage is (1,1,0)); the tool
    // runs; request 1 reports a distinct usage (5,6,0). Both must surface as
    // separate RequestUsage events under the same loop with distinct indexes.
    let model = FakeModel::new(
        "deep",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 32})),
            ModelScript::TextWithUsage("final", 5, 6),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;

    let mut by_index = std::collections::BTreeMap::new();
    while by_index.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("request_usage event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::RequestUsage {
            turn: event_turn,
            request_index,
            usage,
            ..
        } = event
        {
            assert_eq!(event_turn, turn, "usage must stay on the same turn");
            by_index.insert(request_index, (usage.input_tokens(), usage.output_tokens()));
        }
    }
    assert_eq!(by_index.get(&0), Some(&(Some(1), Some(1))));
    assert_eq!(by_index.get(&1), Some(&(Some(5), Some(6))));
    let _ = format!("{:?}", by_index); // Debug formatting must not panic (no sensitive fields)
}

#[tokio::test]
async fn shared_model_keeps_live_presentation_identity_per_session() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-sessions-{}", next_id()));
    let (workspace_a, _guard_a) = workspace_file("presentation-session-a", "a.txt", b"a");
    let (workspace_b, _guard_b) = workspace_file("presentation-session-b", "b.txt", b"b");
    let model = FakeModel::new(
        "provider/model-id",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt"})),
            ModelScript::Text("a done"),
            ModelScript::ToolCall("read", json!({"path": "b.txt"})),
            ModelScript::Text("b done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let session_a = create_session(&mut agent, &workspace_a).await;
    let session_b = create_session(&mut agent, &workspace_b).await;
    let turn_a = send_text(&mut agent, session_a.session_id, "read a").await;
    wait_text(&agent, turn_a).await;
    let turn_b = send_text(&mut agent, session_b.session_id, "read b").await;
    wait_text(&agent, turn_b).await;

    let mut presentations = Vec::new();
    while presentations.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("presentation event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::ToolPresentation {
            turn,
            request_index,
            display,
            ..
        } = event
        {
            presentations.push((turn, request_index, display.detail));
        }
    }
    assert!(presentations.contains(&(turn_a, 0, "a.txt".to_owned())));
    assert!(presentations.contains(&(turn_b, 0, "b.txt".to_owned())));
    assert_eq!(
        agent
            .session_presentation(session_a.session_id)
            .unwrap()
            .model_label
            .as_deref(),
        Some("main")
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
async fn cancelling_wrapped_tool_preserves_runtime_outcome_and_recovery() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-cancel-{}", next_id()));
    let (workspace, _guard) = workspace_file("presentation-cancel-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": "sleep 30"})),
            ModelScript::Text("after cancel"),
        ],
    );
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run slowly").await;
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .expect("tool start event must arrive")
            .expect("event stream must remain open");
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn) {
            break;
        }
    }
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

    // Dropping the wrapped execute future must not leave the Session busy or
    // change the next loop's model/tool ownership.
    let next = send_text(&mut agent, info.session_id, "next").await;
    let next_result = wait_text(&agent, next).await;
    assert_eq!(
        next_result.report.outcome,
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
async fn agent_drop_cancels_an_active_model_future() {
    let (data_dir, _guard) = fixture_dir(&format!("drop-cancel-{}", next_id()));
    let (workspace, _guard) = workspace_file("drop-cancel-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let model = FakeModel::new(
        "main",
        [ModelScript::BlockUntilDrop(
            gate.clone(),
            Arc::clone(&dropped),
            "never returned",
        )],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let _turn = send_text(&mut agent, info.session_id, "drop me").await;
    gate.entered.notified().await;

    drop(agent);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("Agent drop must cancel the active model future");
    gate.release.notify_waiters();
}

#[tokio::test]
async fn agent_drop_cancels_a_manual_compaction_model_future() {
    let (data_dir, _guard) = fixture_dir(&format!("drop-compact-cancel-{}", next_id()));
    let (workspace, _guard) = workspace_file("drop-compact-cancel-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let model = FakeModel::new(
        "main",
        [
            ModelScript::Text("settled"),
            ModelScript::BlockUntilDrop(gate.clone(), Arc::clone(&dropped), "never returned"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "settled").await;
    wait_text(&agent, turn).await;
    let _receiver = agent
        .compact_session(CompactSession {
            session_id: info.session_id,
            operation_id: "drop-compaction".to_owned(),
        })
        .await
        .unwrap();
    gate.entered.notified().await;

    drop(agent);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("Agent drop must cancel the manual model future");
    gate.release.notify_waiters();
}

#[tokio::test]
async fn active_shutdown_waits_for_the_session_owned_worker() {
    let (data_dir, _guard) = fixture_dir(&format!("shutdown-active-{}", next_id()));
    let (workspace, _guard) = workspace_file("shutdown-active-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(WorkerGate::new());
    pause_next_worker_before_join(info.session_id, Arc::clone(&gate));
    let _turn = send_text(&mut agent, info.session_id, "shutdown").await;
    gate.wait_started().await;

    let mut shutdown = tokio::spawn(agent.shutdown());
    let pending = tokio::time::timeout(std::time::Duration::from_millis(25), &mut shutdown).await;
    assert!(pending.is_err(), "shutdown must wait for the owned worker");
    gate.release();
    assert!(shutdown.await.unwrap().is_ok());
}

#[tokio::test]
async fn close_after_admission_result_still_joins_the_preparation_worker() {
    let (data_dir, _guard) = fixture_dir(&format!("shutdown-admission-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file("shutdown-admission-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(WorkerGate::new());
    pause_next_admission_after_result(info.session_id, Arc::clone(&gate));
    let session = agent.loaded_session(info.session_id).unwrap();
    let submission = session
        .submit(minicore_runtime::execution::UserInput::text("wait").unwrap())
        .await
        .unwrap();
    assert!(matches!(&submission, LoopSubmission::Preparing(_)));
    gate.wait_started().await;

    let mut close = tokio::spawn(async move { agent.close_session(info.session_id).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut close)
            .await
            .is_err()
    );
    gate.release();
    assert!(close.await.unwrap().is_ok());
}

#[tokio::test]
async fn dropping_an_admission_waiter_cancels_its_worker() {
    let loop_id = LoopId::new().unwrap();
    let items = (0..4_097)
        .map(|_| {
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: minicore_runtime::execution::UserInput::text("x").unwrap(),
            })
        })
        .collect();
    let gate = BlockGate::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let (_data_dir, _guard, session_id, _model, mut agent) = auto_admission_fixture(
        &format!("drop-admission-waiter-{}", next_id()),
        true,
        items,
        [ModelScript::BlockUntilDrop(
            gate.clone(),
            Arc::clone(&dropped),
            "summary",
        )],
        None,
        None,
    )
    .await;
    let session = agent.loaded_session(session_id).unwrap();
    let submission = session
        .submit(minicore_runtime::execution::UserInput::text("cancel").unwrap())
        .await
        .unwrap();
    let operation_id = match &submission {
        LoopSubmission::Preparing(waiter) => waiter.operation_id().to_owned(),
        LoopSubmission::Accepted(_) => panic!("automatic admission must defer"),
    };
    gate.entered.notified().await;
    drop(submission);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("dropping the waiter must cancel the utility model future");
    assert!(
        agent
            .cancel_compaction(CompactSession {
                session_id,
                operation_id,
            })
            .is_ok()
    );
    gate.release.notify_waiters();
    agent.close_session(session_id).await.unwrap();
}

#[tokio::test]
async fn cancelling_close_future_keeps_normal_worker_owned_until_second_join() {
    let (data_dir, _guard) = fixture_dir(&format!("close-cancel-owned-{}", next_id()));
    let (workspace, _guard) = workspace_file("close-cancel-owned-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", [ModelScript::Text("done")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(WorkerGate::new());
    pause_next_worker_before_join(info.session_id, Arc::clone(&gate));

    let turn = send_text(&mut agent, info.session_id, "close later").await;
    gate.wait_started().await;
    let first = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        agent.close_session(info.session_id),
    )
    .await;
    assert!(
        first.is_err(),
        "cancelled close must still be waiting for the worker"
    );
    assert!(
        agent
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|session| session.session_id == info.session_id && session.loaded)
    );

    gate.release();
    let _ = agent.wait_turn(turn).await;
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_close_future_keeps_manual_worker_owned_until_second_join() {
    let (data_dir, _guard) = fixture_dir(&format!("compact-close-cancel-{}", next_id()));
    let (workspace, _guard) = workspace_file("compact-close-cancel-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [ModelScript::Text("settled"), ModelScript::Text("summary")],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    let turn = send_text(&mut agent, info.session_id, &settled_text).await;
    wait_text(&agent, turn).await;

    let gate = Arc::new(WorkerGate::new());
    pause_next_compaction_after_result(info.session_id, Arc::clone(&gate));
    let result = agent
        .compact(CompactSession {
            session_id: info.session_id,
            operation_id: "close-cancel-compaction".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        result.status,
        crate::compaction::CompactionStatus::Compacted,
        "unexpected manual compaction result: {result:?}"
    );
    gate.wait_started().await;

    let first = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        agent.close_session(info.session_id),
    )
    .await;
    assert!(
        first.is_err(),
        "cancelled close must still be waiting for compaction"
    );
    assert!(
        agent
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|session| session.session_id == info.session_id && session.loaded)
    );
    gate.release();
    agent.close_session(info.session_id).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn compaction_operation_ids_are_bounded_and_reset_on_reopen() {
    let (data_dir, _guard) = fixture_dir(&format!("compact-id-cap-{}", next_id()));
    let (workspace, _guard) = workspace_file("compact-id-cap-ws", "a.txt", b"hello");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    for index in 0..4_096 {
        let result = agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: format!("bounded-{index}"),
            })
            .await
            .unwrap();
        assert_eq!(result.status, crate::compaction::CompactionStatus::Noop);
    }
    assert!(matches!(
        agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: "bounded-over-cap".to_owned(),
            })
            .await,
        Err(AgentError::InvalidInput)
    ));

    agent.close_session(info.session_id).await.unwrap();
    agent.open_session(info.session_id).await.unwrap();
    assert_eq!(
        agent
            .compact(CompactSession {
                session_id: info.session_id,
                operation_id: "bounded-0".to_owned(),
            })
            .await
            .unwrap()
            .status,
        crate::compaction::CompactionStatus::Noop
    );
    agent.shutdown().await.unwrap();
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

    let page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    let timestamps = page
        .items
        .iter()
        .filter_map(|item| match &item.item {
            crate::history::HistoryItemView::User(user) => user.timestamp.clone(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(timestamps.len(), 3);
    assert!(timestamps.iter().all(|timestamp| timestamp.ends_with('Z')));

    // Paging must retain the loop-local occurrence instead of restarting at
    // zero on every page.
    let second_page = agent
        .history(GetHistory {
            session_id: info.session_id,
            offset: 1,
            limit: 1,
        })
        .unwrap();
    let crate::history::HistoryItemView::User(second_user) = &second_page.items[0].item else {
        panic!("expected the second page to contain a User item");
    };
    assert_eq!(
        second_user.timestamp.as_deref(),
        Some(timestamps[1].as_str())
    );

    let loaded = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_session(info.session_id)
        .await
        .unwrap();
    assert_eq!(loaded.user_times.len(), 3);
    assert!(
        loaded
            .user_times
            .keys()
            .all(|(loop_id, _)| *loop_id == turn.loop_id)
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
async fn embedded_open_without_a_file_source_cannot_reload() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-no-source-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-no-source-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;

    assert!(matches!(
        agent.reload().await,
        Err(AgentError::ReloadUnavailable)
    ));
    let _ = create_session(&mut agent, &workspace).await;
}

#[tokio::test]
async fn reload_keeps_active_loop_snapshot_and_updates_the_next_turn() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-active-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-active-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model_a = FakeModel::new("main", [ModelScript::BlockUntil(gate.clone(), "old")]);
    let model_b = FakeModel::new("main", [ModelScript::Text("new")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model_a))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let first = send_text(&mut agent, info.session_id, "first").await;
    gate.entered.notified().await;

    let candidate = agent.config().clone();
    let candidate_models = Models::from_values(BTreeMap::from([(
        "main".to_owned(),
        Arc::clone(&model_b) as Arc<dyn Model>,
    )]));
    assert_eq!(
        agent
            .reload_settings_with_models(candidate, candidate_models)
            .unwrap(),
        crate::agent::ReloadResult { ok: true }
    );

    gate.release.notify_waiters();
    wait_text(&agent, first).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);

    let second = send_text(&mut agent, info.session_id, "second").await;
    wait_text(&agent, second).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 1);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reload_does_not_change_a_running_compaction_binding() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-compaction-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-compaction-ws", "a.txt", b"hello");
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "main",
        [
            ModelScript::Text("settled"),
            ModelScript::BlockUntil(gate.clone(), "old summary"),
        ],
    );
    let model_b = FakeModel::new("main", [ModelScript::Text("new turn")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("replacement".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let settled_text = "settled history ".repeat(128);
    let turn = send_text(&mut agent, info.session_id, &settled_text).await;
    wait_text(&agent, turn).await;

    let mut result = agent
        .compact_session(CompactSession {
            session_id: info.session_id,
            operation_id: "reload-compaction-binding".to_owned(),
        })
        .await
        .unwrap();
    gate.entered.notified().await;

    let candidate = agent.config().clone();
    let candidate_models = Models::from_values(BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_b) as Arc<dyn Model>),
        (
            "replacement".to_owned(),
            Arc::clone(&model_b) as Arc<dyn Model>,
        ),
    ]));
    agent
        .reload_settings_with_models(candidate, candidate_models)
        .unwrap();

    gate.release.notify_waiters();
    let compacted = loop {
        if let Some(result) = result.borrow().clone() {
            break result;
        }
        result.changed().await.unwrap();
    };
    assert_eq!(
        compacted.status,
        crate::compaction::CompactionStatus::Compacted
    );

    let next = send_text(&mut agent, info.session_id, "after reload").await;
    wait_text(&agent, next).await;
    assert_eq!(model_a.requests().lock().unwrap().len(), 2);
    assert_eq!(model_b.requests().lock().unwrap().len(), 1);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn reload_command_environment_accumulates_all_prior_credential_names() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-env-merge-{}", next_id()));
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;

    let mut candidate_b = agent.config().clone();
    if let Some(ModelConfig::OpenAiResponses { api_key_env, .. }) =
        candidate_b.models.get_mut("main")
    {
        *api_key_env = "MINICORE_RELOAD_UNIT_KEY_B".to_owned();
    }
    agent
        .reload_settings_with_models(
            candidate_b,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                Arc::clone(&model) as Arc<dyn Model>,
            )])),
        )
        .unwrap();

    let mut candidate_c = agent.config().clone();
    if let Some(ModelConfig::OpenAiResponses { api_key_env, .. }) =
        candidate_c.models.get_mut("main")
    {
        *api_key_env = "MINICORE_RELOAD_UNIT_KEY_C".to_owned();
    }
    agent
        .reload_settings_with_models(
            candidate_c,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                Arc::clone(&model) as Arc<dyn Model>,
            )])),
        )
        .unwrap();

    let names = agent
        .command_environment
        .names()
        .iter()
        .map(|name| name.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "MINICORE_AGENT_TEST_KEY",
            "MINICORE_RELOAD_UNIT_KEY_B",
            "MINICORE_RELOAD_UNIT_KEY_C",
        ]
    );
}

#[tokio::test]
async fn reload_preserves_session_snapshots_history_and_is_atomic_on_failure() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-atomic-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-atomic-ws", "a.txt", b"hello");
    let model_main = FakeModel::new("main", [ModelScript::Text("main")]);
    let model_other = FakeModel::new("other", [ModelScript::Text("other")]);
    let model_reloaded = FakeModel::new("main", [ModelScript::Text("reloaded")]);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_main)),
            ("other".to_owned(), Arc::clone(&model_other)),
        ]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let first = send_text(&mut agent, info.session_id, "before reload").await;
    wait_text(&agent, first).await;
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let record_before = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    let models_before = agent
        .list_models()
        .into_iter()
        .map(|model| model.id)
        .collect::<Vec<_>>();

    let mut candidate = agent.config().clone();
    candidate.profiles.get_mut("test").unwrap().model = "other".to_owned();
    candidate.profiles.get_mut("test").unwrap().system_prompt = "new profile prompt".to_owned();
    candidate.models.remove("main");
    let candidate_models = Models::from_values(BTreeMap::from([(
        "other".to_owned(),
        Arc::clone(&model_other) as Arc<dyn Model>,
    )]));
    assert!(matches!(
        agent.reload_settings_with_models(candidate, candidate_models),
        Err(AgentError::ModelNotFound)
    ));
    assert_eq!(
        agent
            .list_models()
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>(),
        models_before
    );
    assert_eq!(agent.list_profiles()[0].model, "main");
    assert_eq!(agent.config().default_profile, "test");
    assert_eq!(agent.list_sessions().await.unwrap()[0].model, "main");
    let record_disk_after = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&record_before).unwrap(),
        serde_json::to_vec(&record_disk_after).unwrap()
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);

    let mut valid = agent.config().clone();
    valid.profiles.get_mut("test").unwrap().model = "other".to_owned();
    valid.profiles.get_mut("test").unwrap().system_prompt = "new profile prompt".to_owned();
    valid.profiles.insert(
        "new-default".to_owned(),
        Profile {
            model: "other".to_owned(),
            reasoning: ReasoningPreference::Auto,
            system_prompt: "new default prompt".to_owned(),
            tools: Vec::new(),
            max_tool_rounds: 8,
            approval: ApprovalMode::Auto,
        },
    );
    valid.default_profile = "new-default".to_owned();
    let valid_models = Models::from_values(BTreeMap::from([
        (
            "main".to_owned(),
            Arc::clone(&model_reloaded) as Arc<dyn Model>,
        ),
        (
            "other".to_owned(),
            Arc::clone(&model_other) as Arc<dyn Model>,
        ),
    ]));
    agent
        .reload_settings_with_models(valid, valid_models)
        .unwrap();
    let profiles = agent.list_profiles();
    assert!(
        profiles
            .iter()
            .any(|profile| profile.id == "new-default" && profile.model == "other")
    );
    assert!(
        profiles
            .iter()
            .any(|profile| profile.id == "test" && profile.model == "other")
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    let record_after = Store::open(data_dir.clone())
        .await
        .unwrap()
        .load_record(info.session_id)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&record_before).unwrap(),
        serde_json::to_vec(&record_after).unwrap()
    );

    let second = send_text(&mut agent, info.session_id, "after reload").await;
    wait_text(&agent, second).await;
    {
        let requests = model_reloaded.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let system_prompt = requests[0]
            .messages()
            .iter()
            .find_map(|message| match message {
                ModelMessage::System(text) => Some(text.as_str()),
                _ => None,
            })
            .expect("reloaded request must have a system prompt");
        assert!(system_prompt.contains("test system prompt"));
        assert!(!system_prompt.contains("new profile prompt"));
    }

    let (new_workspace, _new_workspace_guard) =
        workspace_file("reload-atomic-new-ws", "b.txt", b"new");
    let new_info = create_session(&mut agent, &new_workspace).await;
    assert_eq!(new_info.profile, "new-default");
    assert_eq!(new_info.model, "other");
    let third = send_text(&mut agent, new_info.session_id, "new session").await;
    wait_text(&agent, third).await;
    let requests = model_other.requests();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let new_system_prompt = requests[0]
        .messages()
        .iter()
        .find_map(|message| match message {
            ModelMessage::System(text) => Some(text.as_str()),
            _ => None,
        })
        .expect("new-session request must have a system prompt");
    assert!(new_system_prompt.contains("new default prompt"));
    assert!(!new_system_prompt.contains("test system prompt"));
}

#[tokio::test]
async fn reload_rejects_store_and_event_capacity_changes_before_swap() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-restart-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-restart-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        read_profile(),
    )
    .await;
    let _ = create_session(&mut agent, &workspace).await;

    let mut data_dir_candidate = agent.config().clone();
    data_dir_candidate.data_dir = data_dir.join("other-data");
    assert!(matches!(
        agent.reload_settings_with_models(
            data_dir_candidate,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                FakeModel::new("main", []) as Arc<dyn Model>,
            )]))
        ),
        Err(AgentError::ReloadRequiresRestart)
    ));

    let mut event_capacity_candidate = agent.config().clone();
    event_capacity_candidate.event_capacity += 1;
    assert!(matches!(
        agent.reload_settings_with_models(
            event_capacity_candidate,
            Models::from_values(BTreeMap::from([(
                "main".to_owned(),
                FakeModel::new("main", []) as Arc<dyn Model>,
            )]))
        ),
        Err(AgentError::ReloadRequiresRestart)
    ));
    assert_eq!(agent.config().event_capacity, 256);
}

#[tokio::test]
async fn reload_preserves_a_blocked_session_without_unblocking_or_persisting() {
    let (data_dir, _guard) = fixture_dir(&format!("reload-blocked-{}", next_id()));
    let (workspace, _guard) = workspace_file("reload-blocked-ws", "a.txt", b"hello");
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([(
            "main".to_owned(),
            FakeModel::new("main", [ModelScript::Text("done")]),
        )]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    fail_next_append(info.session_id);
    let turn = send_text(&mut agent, info.session_id, "block").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(result.persistence, crate::sessions::TurnPersistence::Failed);
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        crate::sessions::SessionStatus::Blocked
    );
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();

    let candidate_models = Models::from_values(BTreeMap::from([(
        "main".to_owned(),
        FakeModel::new("main", []) as Arc<dyn Model>,
    )]));
    let candidate = agent.config().clone();
    agent
        .reload_settings_with_models(candidate, candidate_models)
        .unwrap();
    assert_eq!(
        agent.session_state(info.session_id).unwrap().status,
        crate::sessions::SessionStatus::Blocked
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert!(matches!(
        agent
            .send(crate::agent::SendMessage {
                session_id: info.session_id,
                text: "still blocked".to_owned(),
            })
            .await,
        Err(AgentError::SessionBlocked)
    ));
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
async fn model_swap_keeps_request_identity_for_live_tool_display() {
    let (data_dir, _guard) = fixture_dir(&format!("presentation-model-swap-{}", next_id()));
    let (workspace, _guard) = workspace_file("presentation-model-swap-ws", "a.txt", b"a");
    std::fs::write(workspace.join("b.txt"), b"b").unwrap();
    let gate = BlockGate::new();
    let model_a = FakeModel::new(
        "provider/model-a",
        [ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({"path": "a.txt"}),
        )],
    );
    let model_b = FakeModel::new(
        "provider/model-b",
        [
            ModelScript::ToolCall("read", json!({"path": "b.txt"})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read both").await;
    gate.entered.notified().await;
    agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();
    gate.release.notify_waiters();
    wait_text(&agent, turn).await;

    let mut presentations = Vec::new();
    while presentations.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("model-swap presentation event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::ToolPresentation {
            turn: event_turn,
            request_index,
            display,
            ..
        } = event
        {
            presentations.push((event_turn, request_index, display.detail));
        }
    }
    assert!(presentations.contains(&(turn, 0, "a.txt".to_owned())));
    assert!(presentations.contains(&(turn, 1, "b.txt".to_owned())));
    assert_eq!(
        agent
            .session_presentation(info.session_id)
            .unwrap()
            .model_label
            .as_deref(),
        Some("other")
    );
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
async fn history_view_exposes_whitelisted_tool_detail_without_raw_arguments() {
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
    // A path is an explicitly permitted local-display detail; the raw
    // invocation object and unrelated argument keys are not.
    assert!(serialized.contains("file.txt"));
    assert!(!serialized.contains("arguments"));
    assert!(!serialized.contains("\"path\""));
    assert!(!serialized.contains("\"limit\""));
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

// ---------------------------------------------------------------------------
// P2: structured tool invocation/execution data (spec 3.1 and 7)
// ---------------------------------------------------------------------------

/// Authoritative ToolRefs derived from persisted history, so tests never guess
/// a tool-call id or request index.
fn history_tool_refs(agent: &Agent, session_id: SessionId) -> Vec<crate::tool_data::ToolRef> {
    let page = agent
        .history(GetHistory {
            session_id,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    page.items
        .iter()
        .filter_map(|item| match &item.item {
            crate::history::HistoryItemView::ToolResult(result) => {
                Some(crate::tool_data::ToolRef {
                    session_id,
                    loop_id: result.loop_id,
                    request_index: result.request_index,
                    tool_call_id: result.tool_call_id.clone(),
                })
            }
            _ => None,
        })
        .collect()
}

async fn read_tool(
    agent: &Agent,
    tool_ref: crate::tool_data::ToolRef,
) -> crate::tool_data::ToolReadResult {
    agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref,
            max_bytes: None,
        })
        .await
        .unwrap()
}

async fn read_output(agent: &Agent, tool_ref: crate::tool_data::ToolRef) -> String {
    let mut collected = String::new();
    let mut offset = 0_u64;
    loop {
        let page = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: crate::tool_data::ToolDataStream::Output,
                offset,
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        collected.push_str(&page.data);
        offset = page.next_offset;
        if page.eof {
            break;
        }
        assert!(page.next_offset > 0, "a non-eof page must advance");
    }
    collected
}

#[tokio::test]
async fn tool_invocation_separates_requests_not_just_names() {
    use crate::tool_data::{ToolExecutionState, ToolSubject};

    let (data_dir, _guard) = fixture_dir(&format!("tool-data-identity-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-identity-ws", "a.txt", b"alpha");
    std::fs::write(workspace.join("b.txt"), b"beta").unwrap();
    // The same tool name runs in two different requests within one loop.
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            ModelScript::ToolCall("read", json!({"path": "b.txt", "limit": 8})),
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
    let turn = send_text(&mut agent, info.session_id, "read both").await;
    wait_text(&agent, turn).await;

    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 2, "both reads must be recorded: {refs:?}");
    assert_ne!(refs[0].request_index, refs[1].request_index);
    let first = read_tool(&agent, refs[0].clone()).await;
    let second = read_tool(&agent, refs[1].clone()).await;
    assert_eq!(first.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(second.execution.state, ToolExecutionState::Succeeded);
    let first_subject = first.invocation.expect("first invocation").subject;
    let second_subject = second.invocation.expect("second invocation").subject;
    assert_ne!(
        first_subject, second_subject,
        "each request keeps its own subject"
    );
    assert!(matches!(first_subject, ToolSubject::File { .. }));
    assert!(matches!(second_subject, ToolSubject::File { .. }));

    // A wrong request index is a distinct unknown identity, never a fallback
    // to the most recent read of the same name.
    let guessed = crate::tool_data::ToolRef {
        request_index: refs[0].request_index + 100,
        ..refs[0].clone()
    };
    assert!(matches!(
        agent
            .tool_read(crate::tool_data::ToolReadRequest {
                tool_ref: guessed,
                max_bytes: None,
            })
            .await,
        Err(AgentError::ToolNotFound)
    ));
}

#[tokio::test]
async fn tool_invocation_is_isolated_across_sessions() {
    let (data_dir, _guard) = fixture_dir(&format!("tool-data-sessions-{}", next_id()));
    let (workspace_a, _guard_a) = workspace_file("tool-data-session-a", "a.txt", b"alpha");
    let base = workspace_a.parent().unwrap();
    let workspace_b = base.join("session-b");
    std::fs::create_dir_all(&workspace_b).unwrap();
    std::fs::write(workspace_b.join("b.txt"), b"beta").unwrap();
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            ModelScript::Text("a done"),
            ModelScript::ToolCall("read", json!({"path": "b.txt", "limit": 8})),
            ModelScript::Text("b done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let session_a = create_session(&mut agent, &workspace_a).await;
    let turn_a = send_text(&mut agent, session_a.session_id, "read a").await;
    wait_text(&agent, turn_a).await;
    let session_b = create_session(&mut agent, &workspace_b).await;
    let turn_b = send_text(&mut agent, session_b.session_id, "read b").await;
    wait_text(&agent, turn_b).await;

    let ref_a = history_tool_refs(&agent, session_a.session_id);
    let ref_b = history_tool_refs(&agent, session_b.session_id);
    assert_eq!(ref_a.len(), 1);
    assert_eq!(ref_b.len(), 1);
    assert_eq!(ref_a[0].session_id, session_a.session_id);
    assert_eq!(ref_b[0].session_id, session_b.session_id);
    let invocation_a = read_tool(&agent, ref_a[0].clone())
        .await
        .invocation
        .expect("session-a invocation");
    let invocation_b = read_tool(&agent, ref_b[0].clone())
        .await
        .invocation
        .expect("session-b invocation");
    assert!(invocation_a.input.preview.contains("a.txt"));
    assert!(invocation_b.input.preview.contains("b.txt"));
    // The session is part of the identity: a lookup that keeps the loop-local
    // values but swaps the session must not resolve to the other session's
    // record. A session-insensitive key would find `ref_a` here.
    let cross_session = crate::tool_data::ToolRef {
        session_id: session_b.session_id,
        ..ref_a[0].clone()
    };
    assert!(matches!(
        agent
            .tool_read(crate::tool_data::ToolReadRequest {
                tool_ref: cross_session,
                max_bytes: None,
            })
            .await,
        Err(AgentError::ToolNotFound)
    ));
}

fn workspace_read_request(session_id: SessionId, path: &str) -> crate::WorkspaceReadRequest {
    crate::WorkspaceReadRequest {
        session_id,
        path: path.to_owned(),
        start_line: None,
        line_byte_offset: None,
        max_lines: None,
        max_bytes: None,
        if_revision: None,
    }
}

#[tokio::test]
async fn workspace_read_requires_a_loaded_session_and_touches_nothing_else() {
    let (data_dir, _guard) = fixture_dir(&format!("workspace-read-{}", next_id()));
    let (workspace, _guard) = workspace_file("workspace-read-ws", "note.txt", b"raw\ncontent\n");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session_id = info.session_id;

    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let file_path = workspace.join("note.txt");
    let file_before = std::fs::read(&file_path).unwrap();

    let result = agent
        .workspace_read(workspace_read_request(session_id, "note.txt"))
        .await
        .unwrap();
    assert_eq!(result.status, crate::WorkspaceReadStatus::Ok);
    assert_eq!(result.content, "raw\ncontent\n");
    assert_eq!(result.returned_lines, 2);
    assert_eq!(result.revision.as_deref().map(str::len), Some(64));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&file_path).unwrap(), file_before);

    // A closed Session no longer locates its Workspace, and an unknown Session
    // ID is never guessed into one.
    agent.close_session(session_id).await.unwrap();
    assert!(matches!(
        agent
            .workspace_read(workspace_read_request(session_id, "note.txt"))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert!(matches!(
        agent
            .workspace_read(workspace_read_request(
                SessionId::new().unwrap(),
                "note.txt"
            ))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
}

fn workspace_files_request(session_id: SessionId) -> crate::WorkspaceFilesRequest {
    crate::WorkspaceFilesRequest {
        session_id,
        directory: None,
        recursive: None,
        query: None,
        cursor: None,
        limit: None,
        max_bytes: None,
    }
}

fn workspace_search_request(session_id: SessionId, query: &str) -> crate::WorkspaceSearchRequest {
    crate::WorkspaceSearchRequest {
        session_id,
        query: query.to_owned(),
        paths: None,
        case_sensitive: None,
        cursor: None,
        max_matches: None,
        max_bytes: None,
    }
}

#[tokio::test]
async fn workspace_files_and_search_require_a_loaded_session_and_touch_nothing_else() {
    let (data_dir, _guard) = fixture_dir(&format!("workspace-scan-{}", next_id()));
    let (workspace, _guard) = workspace_file("workspace-scan-ws", "note.txt", b"raw\nneedle\n");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session_id = info.session_id;

    let history_path = data_dir
        .join("sessions")
        .join(session_id.to_string())
        .join("history.jsonl");
    let history_before = std::fs::read(&history_path).unwrap();
    let file_path = workspace.join("note.txt");
    let file_before = std::fs::read(&file_path).unwrap();

    let files = agent
        .workspace_files(workspace_files_request(session_id))
        .await
        .unwrap();
    assert_eq!(files.directory, "");
    assert_eq!(files.entries.len(), 1);
    assert_eq!(files.entries[0].path, "note.txt");
    assert_eq!(files.entries[0].kind, crate::WorkspaceFileKind::File);
    assert!(files.scan_complete);

    let search = agent
        .workspace_search(workspace_search_request(session_id, "needle"))
        .await
        .unwrap();
    assert_eq!(search.matches.len(), 1);
    assert_eq!(search.matches[0].path, "note.txt");
    assert_eq!(search.matches[0].line_number, 2);
    assert_eq!(search.matches[0].line_text, "needle");
    assert!(search.scan_complete);

    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
    assert_eq!(std::fs::read(&file_path).unwrap(), file_before);

    // A closed Session no longer locates its Workspace, and an unknown Session
    // ID is never guessed into one.
    agent.close_session(session_id).await.unwrap();
    assert!(matches!(
        agent
            .workspace_files(workspace_files_request(session_id))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert!(matches!(
        agent
            .workspace_search(workspace_search_request(session_id, "needle"))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    let unknown = SessionId::new().unwrap();
    assert!(matches!(
        agent
            .workspace_files(workspace_files_request(unknown))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert!(matches!(
        agent
            .workspace_search(workspace_search_request(unknown, "needle"))
            .await,
        Err(AgentError::SessionNotLoaded)
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&history_path).unwrap(), history_before);
}

#[tokio::test]
async fn tool_read_exposes_approval_time_data_without_running() {
    use crate::tool_data::{ToolExecutionState, ToolSubject};

    let (data_dir, _guard) = fixture_dir(&format!("tool-data-policy-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-policy-ws", "a.txt", b"hello");
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["write".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Ask,
    };
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("write", json!({"path": "out.txt", "content": "written"})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "write it").await;

    // Wait until the runtime reports the approval request, then query while
    // the tool is still blocked before execution.
    let interaction = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .expect("interaction event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::InteractionRequested { interaction, .. } = event {
            break interaction;
        }
    };
    let tool_ref = crate::tool_data::ToolRef {
        session_id: info.session_id,
        loop_id: turn.loop_id,
        request_index: 0,
        tool_call_id: interaction.tool_call_id.clone(),
    };
    let waiting = read_tool(&agent, tool_ref.clone()).await;
    assert_eq!(waiting.execution.state, ToolExecutionState::AwaitingPolicy);
    assert_ne!(
        waiting.execution.state,
        ToolExecutionState::Running,
        "policy wait must never report running"
    );
    // Approval-time data is obtainable before the tool executes.
    let waiting_invocation = waiting.invocation.expect("approval-time invocation data");
    assert_eq!(
        waiting_invocation.subject,
        ToolSubject::File {
            path: "out.txt".to_owned()
        }
    );
    assert!(waiting_invocation.input.preview.contains("written"));

    agent
        .answer(crate::agent::AnswerInteraction {
            turn,
            interaction_id: interaction.interaction_id,
            answer: minicore_runtime::interaction::InteractionAnswer::Approval(
                minicore_runtime::tools::ApprovalDecision::AllowOnce,
            ),
        })
        .await
        .unwrap();
    wait_text(&agent, turn).await;
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let finished = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(finished.execution.state, ToolExecutionState::Succeeded);
    let invocation = finished.invocation.expect("invocation after approval");
    assert_eq!(
        invocation.subject,
        ToolSubject::File {
            path: "out.txt".to_owned()
        }
    );
}

#[tokio::test]
async fn tool_execution_event_matches_the_tool_read_query() {
    let (data_dir, _guard) = fixture_dir(&format!("tool-data-events-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-events-ws", "a.txt", b"alpha\nbeta");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 2})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;

    let mut invocation_event = None;
    let mut execution_event = None;
    while execution_event.is_none() {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("tool data events must arrive")
            .expect("event stream must remain open");
        match event {
            AgentEvent::ToolInvocation { data, .. } => invocation_event = Some(data),
            AgentEvent::ToolExecution { data, .. } => execution_event = Some(data),
            AgentEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }
    let invocation_event = invocation_event.expect("tool_invocation event");
    let execution_event = execution_event.expect("tool_execution event");
    let queried = read_tool(&agent, execution_event.tool_ref.clone()).await;
    assert_eq!(execution_event.tool_ref, queried.execution.tool_ref);
    assert_eq!(execution_event.state, queried.execution.state);
    assert_eq!(execution_event.outcome, queried.execution.outcome);
    assert_eq!(invocation_event.tool_ref, execution_event.tool_ref);
    assert_eq!(
        invocation_event.subject,
        queried.invocation.expect("queried invocation").subject
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_request_observation_survives_runtime_start_binding() {
    use crate::tool_data::{CommandStatus, ToolDataStream, ToolExecutionState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("startup-observation-{}", next_id()));
    let (workspace, _guard) = workspace_file("startup-observation-ws", "a.txt", b"hello");
    let model_gate = BlockGate::new();
    let model = FakeModel::new(
        "main",
        [ModelScript::ToolCallAfterGate(
            model_gate.clone(),
            "bash",
            json!({"command": "printf 'started\\n'; exec tail -f /dev/null"}),
        )],
    );
    let model_started = model.start_entered();
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        bash_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let gate = Arc::new(RuntimeStartGate::new());
    pause_next_runtime_start_before_bind(info.session_id, Arc::clone(&gate));

    let agent = Arc::new(tokio::sync::Mutex::new(agent));
    let send_agent = Arc::clone(&agent);
    let mut send_task = tokio::spawn(async move {
        let mut agent = send_agent.lock().await;
        send_text(&mut agent, info.session_id, "run bash").await
    });

    // Keep every startup wait bounded. Always release both test gates before
    // joining the sender so a failed observation cannot strand its Session.
    let startup = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        gate.wait_started().await;
        model_started.notified().await;
        model_gate.entered.notified().await;
    })
    .await;

    // The sender must still be waiting for install_loop to bind the started
    // loop. This is the observation that the startup gate is between
    // AgentLoop::start and bind_started_loop, not merely before startup.
    let startup_boundary_observed = startup.is_ok() && !send_task.is_finished();
    gate.release();
    model_gate.release.notify_one();

    let send_result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut send_task).await;
    let turn = match send_result {
        Ok(Ok(turn)) => turn,
        Ok(Err(error)) => {
            let mut agent = agent.lock().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                agent.close_session(info.session_id),
            )
            .await;
            panic!("send task failed: {error}");
        }
        Err(_) => {
            send_task.abort();
            let _ = send_task.await;
            let mut agent = agent.lock().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                agent.close_session(info.session_id),
            )
            .await;
            panic!("install_loop did not leave the startup gate in time");
        }
    };

    if !startup_boundary_observed {
        let mut agent = agent.lock().await;
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agent.close_session(info.session_id),
        )
        .await;
        panic!("startup observation did not prove the bind boundary");
    }

    let mut agent = agent.lock().await;
    let body_result: Result<(), String> = async {
        let mut invocation = None;
        let mut process_ref = None;
        let mut output_chunk = None;
        let mut tool_started = false;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let event = events
                    .recv()
                    .await
                    .ok_or_else(|| "event stream closed before Bash started".to_owned())?;
                match event {
                    AgentEvent::ToolInvocation {
                        turn: event_turn,
                        data,
                        ..
                    } if event_turn == turn => {
                        invocation = Some(data);
                    }
                    AgentEvent::ToolStarted {
                        turn: event_turn, ..
                    } if event_turn == turn => {
                        tool_started = true;
                    }
                    AgentEvent::ToolProcess {
                        turn: event_turn,
                        data,
                        ..
                    } if event_turn == turn => {
                        if let Some(chunk) = data.chunk {
                            if chunk.stream == ToolDataStream::Stdout {
                                let bytes = base64::engine::general_purpose::STANDARD
                                    .decode(&chunk.data)
                                    .map_err(|error| format!("decode Bash output: {error}"))?;
                                process_ref = Some(data.tool_ref);
                                output_chunk = Some(bytes);
                            }
                        }
                    }
                    _ => {}
                }
                if invocation.is_some()
                    && tool_started
                    && process_ref.is_some()
                    && output_chunk.is_some()
                {
                    return Ok::<(), String>(());
                }
            }
        })
        .await
        .map_err(|_| "Bash startup events timed out".to_owned())??;

        let invocation = invocation.ok_or_else(|| "missing ToolInvocation event".to_owned())?;
        let tool_ref = invocation.tool_ref.clone();
        if invocation.name != "bash" {
            return Err(format!("expected Bash invocation, got {}", invocation.name));
        }
        if tool_ref.session_id != info.session_id
            || tool_ref.loop_id != turn.loop_id
            || tool_ref.request_index != 0
            || tool_ref.tool_call_id.as_str() != "bash-call-0"
        {
            return Err(format!("unexpected first Bash ToolRef: {tool_ref:?}"));
        }
        if process_ref != Some(tool_ref.clone()) {
            return Err("tool_process used a different ToolRef".to_owned());
        }

        let queried = agent
            .tool_read(crate::tool_data::ToolReadRequest {
                tool_ref: tool_ref.clone(),
                max_bytes: None,
            })
            .await
            .map_err(|error| format!("tool.read failed: {error}"))?;
        if queried.execution.tool_ref != tool_ref
            || queried
                .invocation
                .as_ref()
                .is_none_or(|data| data.tool_ref != tool_ref)
            || queried.execution.state != ToolExecutionState::Running
        {
            return Err("tool.read did not preserve the complete running ToolRef".to_owned());
        }
        let command = queried
            .execution
            .command
            .ok_or_else(|| "tool.read did not contain a Bash command".to_owned())?;
        if command.status != CommandStatus::Running {
            return Err(format!(
                "Bash command was not running: {:?}",
                command.status
            ));
        }

        let running_output = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .map_err(|error| format!("tool/output failed: {error}"))?;
        if running_output.tool_ref != tool_ref || running_output.stream != ToolDataStream::Stdout {
            return Err("tool/output did not preserve the complete ToolRef".to_owned());
        }
        let running_bytes = base64::engine::general_purpose::STANDARD
            .decode(&running_output.data)
            .map_err(|error| format!("decode tool/output: {error}"))?;
        if running_bytes != b"started\n" || running_output.eof {
            return Err(format!(
                "tool/output did not expose the live Bash output: {:?}, eof={}",
                running_bytes, running_output.eof
            ));
        }
        if agent
            .loaded_session(info.session_id)
            .ok_or_else(|| "Session was unloaded while Bash was running".to_owned())?
            .presentation()
            .command_owners()
            .active()
            == 0
        {
            return Err("Bash command owner was not active before cancellation".to_owned());
        }

        if !agent
            .cancel(turn)
            .map_err(|error| format!("cancel failed: {error}"))?
        {
            return Err("cancel did not find the active Bash turn".to_owned());
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), agent.wait_turn(turn))
            .await
            .map_err(|_| "turn.wait timed out after Bash cancellation".to_owned())?
            .map_err(|error| format!("turn.wait failed: {error}"))?;
        if result.report.outcome
            != minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
        {
            return Err(format!(
                "unexpected cancelled turn outcome: {:?}",
                result.report.outcome
            ));
        }

        let terminal = agent
            .tool_read(crate::tool_data::ToolReadRequest {
                tool_ref: tool_ref.clone(),
                max_bytes: None,
            })
            .await
            .map_err(|error| format!("terminal tool.read failed: {error}"))?;
        if terminal.execution.tool_ref != tool_ref
            || terminal.execution.state != ToolExecutionState::Cancelled
        {
            return Err("terminal tool.read changed the Bash ToolRef or state".to_owned());
        }
        let terminal_command = terminal
            .execution
            .command
            .ok_or_else(|| "terminal tool.read lost the Bash command".to_owned())?;
        if terminal_command.status != CommandStatus::Cancelled
            || !terminal_command.termination_confirmed
        {
            return Err("cancelled Bash command was not confirmed terminated".to_owned());
        }

        let terminal_output = agent
            .tool_output(crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: Some(4096),
            })
            .await
            .map_err(|error| format!("terminal tool/output failed: {error}"))?;
        if terminal_output.tool_ref != tool_ref
            || (terminal_output.eof && terminal_output.data.is_empty())
        {
            return Err("terminal tool/output lost the observed Bash output".to_owned());
        }
        if agent
            .loaded_session(info.session_id)
            .ok_or_else(|| "Session was unloaded before owner join check".to_owned())?
            .presentation()
            .command_owners()
            .active()
            != 0
        {
            return Err("turn.wait returned before the Bash command owner joined".to_owned());
        }
        Ok(())
    }
    .await;

    let close_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.close_session(info.session_id),
    )
    .await;
    if let Err(error) = body_result {
        let _ = close_result;
        panic!("{error}");
    }
    close_result
        .expect("Session cleanup timed out")
        .expect("Session cleanup failed");
}

#[tokio::test]
async fn tool_result_raw_text_is_readable_by_offset_without_escaping() {
    let (data_dir, _guard) = fixture_dir(&format!("tool-data-offset-{}", next_id()));
    let expected = (1..=24)
        .map(|line| format!("line-{line}: 你好 café"))
        .collect::<Vec<_>>()
        .join("\n");
    let (workspace, _guard) = workspace_file("tool-data-offset-ws", "a.txt", expected.as_bytes());
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 64})),
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
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);

    // `read` renders each source line with a "N: " prefix; reassembling the
    // paged original must be byte-exact, including non-ASCII and newlines.
    let expected_rendered = (1..=24)
        .map(|line| format!("{line}: line-{line}: 你好 café"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        read_output(&agent, refs[0].clone()).await,
        expected_rendered
    );
}

#[tokio::test]
async fn tool_read_answers_without_consuming_live_events() {
    // A capacity-1 Agent event queue drops nearly every best-effort event, yet
    // the authoritative join report still reconciles terminal tool state.
    let (data_dir, _guard) = fixture_dir(&format!("tool-data-lost-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-lost-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent_with(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
        AgentOptions {
            agent_event_capacity: 1,
            loop_event_capacity: Some(1),
        },
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;
    wait_text(&agent, turn).await;
    // The live stream is never consumed: events were dropped, not delivered.
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let result = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(
        result.execution.state,
        crate::tool_data::ToolExecutionState::Succeeded
    );
    assert!(
        read_output(&agent, refs[0].clone())
            .await
            .starts_with("1: hello")
    );
}

#[tokio::test]
async fn failed_tool_result_text_is_queryable_from_the_authoritative_report() {
    use crate::tool_data::{ToolDataAvailability, ToolExecutionState};

    let (data_dir, _guard) = fixture_dir(&format!("tool-data-failed-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-failed-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "missing.txt"})),
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
    let turn = send_text(&mut agent, info.session_id, "read missing").await;
    wait_text(&agent, turn).await;

    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let result = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(result.execution.state, ToolExecutionState::Failed);
    assert_eq!(
        result.execution.outcome,
        Some(minicore_runtime::tools::ToolResultOutcome::Failed)
    );
    assert_eq!(
        result.execution.output_availability,
        ToolDataAvailability::Available
    );
    // The failed call's real text is recorded from the report, not left empty.
    let output = read_output(&agent, refs[0].clone()).await;
    assert_eq!(output, "tool failed");
    assert!(!output.contains("missing.txt"));
}

#[tokio::test]
async fn running_tool_output_stream_is_never_a_false_empty_eof() {
    use crate::tool_data::{ToolDataAvailability, ToolExecutionState};

    let (data_dir, _guard) = fixture_dir(&format!("tool-data-pending-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-pending-ws", "a.txt", b"hello");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt", "limit": 8})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        read_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read it").await;

    // Wait for the invocation event, then query while the tool may still be
    // running. The page must never claim an empty, complete (`eof`) available
    // stream: an unobserved output is `pending`.
    let data = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .expect("tool invocation event must arrive")
            .expect("event stream must remain open");
        if let AgentEvent::ToolInvocation { data, .. } = event {
            break data;
        }
    };
    let page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: data.tool_ref.clone(),
            stream: crate::tool_data::ToolDataStream::Output,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    match page.availability {
        ToolDataAvailability::Pending => {
            assert_eq!(page.observed_end, 0);
            assert!(!page.eof);
            assert!(page.data.is_empty());
        }
        ToolDataAvailability::Available => {
            assert!(page.eof);
            assert!(!page.data.is_empty());
        }
        other => panic!("unexpected early output availability: {other:?}"),
    }

    wait_text(&agent, turn).await;
    let finished = read_tool(&agent, data.tool_ref).await;
    assert_eq!(finished.execution.state, ToolExecutionState::Succeeded);
}

#[tokio::test]
async fn rejected_tool_input_never_fabricates_a_file_operation() {
    use crate::tool_data::{ToolExecutionState, ToolSubject};

    let (data_dir, _guard) = fixture_dir(&format!("tool-data-invalid-{}", next_id()));
    let (workspace, _guard) = workspace_file("tool-data-invalid-ws", "a.txt", b"hello");
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["write".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };
    // The schema requires `content`; the arguments are structurally valid JSON
    // but the tool's own parse rejects them.
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("write", json!({"path": "out.txt"})),
            ModelScript::Text("done"),
        ],
    );
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "write it").await;
    wait_text(&agent, turn).await;

    // The file was never created, and the recorded facts say the call failed
    // rather than claiming a write happened.
    assert!(!workspace.join("out.txt").exists());
    let refs = history_tool_refs(&agent, info.session_id);
    assert_eq!(refs.len(), 1);
    let result = read_tool(&agent, refs[0].clone()).await;
    assert_eq!(result.execution.state, ToolExecutionState::Failed);
    // The requested input is recorded as requested, not as applied.
    let invocation = result.invocation.expect("requested input is recorded");
    assert_eq!(
        invocation.subject,
        ToolSubject::File {
            path: "out.txt".to_owned()
        }
    );
    assert!(invocation.input.preview.contains("out.txt"));
    assert!(!invocation.input.preview.contains("content"));
}

// ---------------------------------------------------------------------------
// P3b2: Provider Replay Budget And ContextOverflow Recovery Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_provider_overflow_recovery_success() {
    let (data_dir, _guard) = fixture_dir("p3b2-recovery-success");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // A tool result large enough that replacing the whole exchange with the
    // labeled summary envelope strictly shrinks the provider body.
    std::fs::write(workspace.join("note.txt"), "recovery note ".repeat(400)).unwrap();

    let profile = read_profile();

    // One loop: request 0 runs the tool batch, request 1 rejects with
    // ContextOverflowNotStarted, utility summarizes the exchange, and the
    // same logical request is retried once with the clean projection.
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of prior read tool exchange"),
            ModelScript::Text("recovered answer"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);

    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "recovered answer"));
    let user_messages: Vec<_> = history
        .iter()
        .filter_map(|item| match item {
            HistoryItem::User(u) => Some(u.input.as_text().to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(user_messages, vec!["read note"]);
    // The original tool exchange stays in durable history exactly once even
    // though the retried request projected a summary instead.
    let tool_results = history
        .iter()
        .filter(|item| matches!(item, HistoryItem::ToolResult(_)))
        .count();
    assert_eq!(tool_results, 1);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovered");
    assert!(recovery.before_tokens.is_some());
    assert!(recovery.after_tokens.is_some());
    assert!(recovery.utility_usage.is_some());
}

#[tokio::test]
async fn test_provider_overflow_stops_on_second_failure() {
    let (data_dir, _guard) = fixture_dir("p3b2-second-overflow");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("note.txt"), "recovery note ".repeat(400)).unwrap();

    let profile = read_profile();

    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of prior read exchange"),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("should not be reached"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Failed(_)
    ));
    // overflow, summary, second overflow: the retry never runs a third raw
    // start and the unused script is not consumed.
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovery_failed");
    assert_eq!(recovery.failure_kind.as_deref(), Some("context_overflow"));
    // The utility usage of the failed recovery attempt is retained.
    assert!(recovery.utility_usage.is_some());
}

#[tokio::test]
async fn test_recovery_refuses_when_summary_cannot_shrink() {
    let (data_dir, _guard) = fixture_dir("p3b2-no-shrink");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // A tiny exchange is smaller than the labeled summary envelope, so the
    // utility can never produce a strictly smaller projection for it. The
    // first summary starts the reduce loop, the second shrinks the content
    // without winning the budget, and the third cannot shrink further.
    std::fs::write(workspace.join("tiny.txt"), "x").unwrap();

    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "tiny.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of the tiny read exchange"),
            ModelScript::Text("S"),
            ModelScript::Text("never"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "read tiny").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Failed(_)
    ));
    // overflow plus three utility calls; no second raw start happens.
    assert_eq!(model.calls.load(Ordering::SeqCst), 5);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovery_failed");
    assert_eq!(recovery.failure_kind.as_deref(), Some("no_progress"));
    assert!(recovery.utility_usage.is_some());
}

#[tokio::test]
async fn non_recoverable_model_failures_are_not_retried_as_recovery() {
    let cases = [
        (
            "unknown-provider-unavailable",
            minicore_runtime::model::ModelErrorKind::ProviderUnavailable,
            minicore_runtime::model::DeliveryState::Unknown,
            false,
        ),
        (
            "started-provider-unavailable",
            minicore_runtime::model::ModelErrorKind::ProviderUnavailable,
            minicore_runtime::model::DeliveryState::Started,
            false,
        ),
        (
            "unknown-context-overflow",
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::model::DeliveryState::Unknown,
            false,
        ),
        (
            "started-context-overflow",
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::model::DeliveryState::Started,
            false,
        ),
        (
            "started-mid-stream-context-overflow",
            minicore_runtime::model::ModelErrorKind::ContextOverflow,
            minicore_runtime::model::DeliveryState::Started,
            true,
        ),
    ];
    for (label, kind, delivery, mid_stream) in cases {
        let (data_dir, _guard) = fixture_dir(label);
        let workspace = data_dir.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("note.txt"), "provider note ".repeat(200)).unwrap();

        let diagnostic = minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::InvalidConfiguration,
            minicore_runtime::error::DiagnosticCategory::Model,
            minicore_runtime::value::BoundedText::new("injected model failure").unwrap(),
            false,
        );
        let error = match delivery {
            minicore_runtime::model::DeliveryState::Unknown => {
                ModelError::unknown(kind, diagnostic)
            }
            minicore_runtime::model::DeliveryState::Started => {
                ModelError::started(kind, diagnostic)
            }
            _ => unreachable!("only unknown and started deliveries are exercised"),
        };

        // The failing request follows a real tool exchange, so a compressible
        // recovery source exists; only `ContextOverflow + NotStarted` may use
        // it.
        let failing = if mid_stream {
            ModelScript::StreamErrorAfterText("partial text", error)
        } else {
            ModelScript::Error(error)
        };
        let model = FakeModel::with_window(
            "main",
            16_384,
            [
                ModelScript::ToolCall("read", json!({"path": "note.txt"})),
                failing,
                ModelScript::Text("must not be reached"),
            ],
        );

        let mut agent = open_agent_auto(
            &data_dir,
            BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
            read_profile(),
        )
        .await;

        let info = create_session(&mut agent, &workspace).await;

        let turn = send_text(&mut agent, info.session_id, "read note").await;
        let res = wait_text(&agent, turn).await;
        assert!(
            matches!(res.report.outcome, minicore_runtime::LoopOutcome::Failed(_)),
            "{label} must fail the turn"
        );
        // Tool request plus the injected failure; recovery never starts.
        assert_eq!(model.calls.load(Ordering::SeqCst), 2, "{label}");
        let context = agent.session_context(info.session_id).unwrap();
        assert!(context.recovery.is_none(), "{label}");
    }
}

#[tokio::test]
async fn test_driver_retry_reentry_does_not_reset_recovery_quota() {
    let (data_dir, _guard) = fixture_dir("p3b2-retry-reentry");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("note.txt"), "recovery note ".repeat(400)).unwrap();

    let profile = read_profile();

    let retryable = ModelError::not_started(
        minicore_runtime::model::ModelErrorKind::RateLimited,
        Some(std::time::Duration::from_millis(1)),
        minicore_runtime::error::DiagnosticSummary::new(
            minicore_runtime::error::DiagnosticCode::ModelUnavailable,
            minicore_runtime::error::DiagnosticCategory::Model,
            minicore_runtime::value::BoundedText::new("retry shortly").unwrap(),
            true,
        ),
    );

    // The second start fails with a Driver-retryable error, so the Driver
    // re-enters the same logical request. That retry must not grant another
    // ContextOverflow recovery even though the next attempt overflows again.
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary of prior read exchange"),
            ModelScript::Error(retryable),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("never reached"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert!(matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Failed(_)
    ));
    // tool call, overflow, summary, retryable start failure, re-entered
    // overflow: the unused script is never consumed by a second recovery.
    assert_eq!(model.calls.load(Ordering::SeqCst), 5);

    // Main requests keep one logical identity (loop id + request index) and
    // one deadline across the recovery retry and the Driver retry; the utility
    // runs on its own loop with its own attempt window.
    let contexts = model.contexts();
    let contexts = contexts.lock().unwrap();
    assert_eq!(contexts.len(), 5);
    let loop_id = contexts[0].loop_id;
    assert_eq!(contexts[0].request_index, 0);
    for index in [1, 3, 4] {
        assert_eq!(contexts[index].loop_id, loop_id);
        assert_eq!(contexts[index].request_index, 1);
        assert_eq!(contexts[index].deadline, contexts[1].deadline);
    }
    assert_ne!(contexts[2].loop_id, loop_id);
    assert_eq!(contexts[2].request_index, 0);

    let context = agent.session_context(info.session_id).unwrap();
    let recovery = context
        .recovery
        .expect("recovery observation must be recorded");
    assert_eq!(recovery.outcome, "recovery_failed");
    // The observation belongs to the failed recovery attempt, not to a new one.
    assert_eq!(recovery.failure_kind.as_deref(), Some("rate_limited"));
    assert!(recovery.utility_usage.is_some());
}

#[tokio::test]
async fn test_model_update_before_turn_uses_new_model() {
    let (data_dir, _guard) = fixture_dir("p3b2-stale-ticket-update");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    let profile = read_profile();

    let model_a = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("not reached because stale"),
        ],
    );
    let model_b = FakeModel::with_window("other", 16_384, [ModelScript::Text("from model b")]);

    let mut models_map = BTreeMap::new();
    models_map.insert("main".to_owned(), model_config("PATH"));
    models_map.insert("other".to_owned(), model_config("PATH"));

    let config = auto_config(data_dir.to_path_buf(), models_map, profile);

    let runtime_models = BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a) as Arc<dyn Model>),
        ("other".to_owned(), Arc::clone(&model_b) as Arc<dyn Model>),
    ]);

    let mut agent = Agent::open_with_models(config, Models::from_values(runtime_models))
        .await
        .unwrap();

    let info = create_session(&mut agent, &workspace).await;

    let updated = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::Auto),
        })
        .await
        .unwrap();
    assert_eq!(updated.active_revision, None);

    let turn = send_text(&mut agent, info.session_id, "hello").await;
    let res = wait_text(&agent, turn).await;
    assert_eq!(res.report.outcome, minicore_runtime::LoopOutcome::Completed);
    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "from model b"));
}

#[tokio::test]
async fn test_failed_update_does_not_publish_a_new_recovery_binding() {
    let (data_dir, _guard) = fixture_dir("p3b2-failed-update");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("note.txt"), "note ".repeat(200)).unwrap();

    let profile = read_profile();

    // Model A serves the session and its recovery utility.
    let model_a = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "note.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("summary from a"),
            ModelScript::Text("recovered by a"),
        ],
    );
    // Model B cannot serve High reasoning, so the update fails inside the
    // config factory after the model lookup succeeded.
    let model_b = FakeModel::with_reasoning(
        "other",
        16_384,
        BTreeSet::from([ReasoningPreference::Auto]),
        [ModelScript::Text("summary from b")],
    );

    let mut models_map = BTreeMap::new();
    models_map.insert("main".to_owned(), model_config("PATH"));
    models_map.insert("other".to_owned(), model_config("PATH"));
    let config = auto_config(data_dir.to_path_buf(), models_map, profile);

    let runtime_models = BTreeMap::from([
        ("main".to_owned(), Arc::clone(&model_a) as Arc<dyn Model>),
        ("other".to_owned(), Arc::clone(&model_b) as Arc<dyn Model>),
    ]);
    let mut agent = Agent::open_with_models(config, Models::from_values(runtime_models))
        .await
        .unwrap();

    let info = create_session(&mut agent, &workspace).await;
    let failed = agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: Some(ReasoningPreference::High),
        })
        .await;
    assert!(failed.is_err(), "the update must fail");

    // The session keeps model A and its recovery binding: the overflow
    // recovery must be summarized by A, never by the failed update's model.
    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "recovered by a"));
    assert_eq!(model_b.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_recovery_two_sessions_isolated() {
    let (data_dir, _guard) = fixture_dir("p3b2-sessions-isolated");
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("a.txt"), "content a ".repeat(400)).unwrap();

    let profile = read_profile();

    // Session A overflows inside the same loop that ran its tool batch;
    // session B must see no recovery observation and no shared ticket.
    let model = FakeModel::with_window(
        "main",
        16_384,
        [
            ModelScript::ToolCall("read", json!({"path": "a.txt"})),
            ModelScript::ContextOverflowNotStarted,
            ModelScript::Text("Summary for A"),
            ModelScript::Text("recovered a"),
            ModelScript::Text("session b answer"),
        ],
    );

    let mut agent = open_agent_auto(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        profile,
    )
    .await;

    let info_a = create_session(&mut agent, &workspace).await;
    let info_b = create_session(&mut agent, &workspace).await;

    let turn_a = send_text(&mut agent, info_a.session_id, "read a").await;
    let result_a = wait_text(&agent, turn_a).await;
    assert_eq!(
        result_a.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let turn_b = send_text(&mut agent, info_b.session_id, "hello from b").await;
    let result_b = wait_text(&agent, turn_b).await;
    assert_eq!(
        result_b.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let context_a = agent.session_context(info_a.session_id).unwrap();
    let recovery_a = context_a.recovery.expect("session A has recovery");
    assert_eq!(recovery_a.outcome, "recovered");

    let context_b = agent.session_context(info_b.session_id).unwrap();
    assert!(context_b.recovery.is_none());

    let history_a = read_store_history(&data_dir, info_a.session_id).await;
    let history_b = read_store_history(&data_dir, info_b.session_id).await;
    assert!(
        flatten_texts(&history_a)
            .iter()
            .any(|text| text == "recovered a")
    );
    assert!(
        !flatten_texts(&history_b)
            .iter()
            .any(|text| text == "recovered a")
    );
}

/// Marker carried by every opaque provider replay item in the loopback
/// fixture.
const REPLAY_MARKER: &str = "provider-replay-opaque-";

/// Real-HTTP loopback for provider replay budgeting.
///
/// Both modes answer the tool round with the same opaque replay item. With
/// `preflight` the replay alone exceeds the effective hard budget, so the
/// preparation plan must summarize before sending and the server sees
/// tool -> utility -> recovered. Without it the replaying request is sent and
/// the provider itself rejects it with a structured 400 before recovery
/// summarizes.
async fn openai_overflow_loopback(preflight: bool) -> MockServer {
    let replay_units = if preflight { 3_000 } else { 120 };
    let opaque_replay = REPLAY_MARKER.repeat(replay_units);

    let tool_call_event = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "id": "fc_001",
            "call_id": "call_read_1",
            "name": "read",
            "arguments": "{\"path\":\"file.txt\"}",
            "status": "in_progress"
        }
    });
    let tool_call_done = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "id": "fc_001",
            "call_id": "call_read_1",
            "name": "read",
            "arguments": "{\"path\":\"file.txt\"}",
            "status": "completed",
            "provider": {"opaque": opaque_replay}
        }
    });
    let tool_turn_done = json!({
        "type": "response.completed",
        "response": {
            "status": "completed",
            "output": [{
                "type": "function_call",
                "id": "fc_001",
                "call_id": "call_read_1",
                "name": "read",
                "arguments": "{\"path\":\"file.txt\"}",
                "status": "completed",
                "provider": {"opaque": opaque_replay}
            }]
        }
    });

    let overflow_error_body = json!({
        "error": {
            "message": "This model's maximum context length is 16384 tokens. However, your messages resulted in 17000 tokens.",
            "type": "invalid_request_error",
            "code": "context_length_exceeded"
        }
    });

    let summary_events = vec![
        json!({
            "type": "response.output_text.delta",
            "delta": "Summary of prior read execution"
        }),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Summary of prior read execution"}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Summary of prior read execution"}]
                }]
            }
        }),
    ];

    let recovered_events = vec![
        json!({
            "type": "response.output_text.delta",
            "delta": "Recovered: file contains hello"
        }),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Recovered: file contains hello"}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Recovered: file contains hello"}]
                }]
            }
        }),
    ];

    let tool_move = MockResponse::sse(&[tool_call_event, tool_call_done, tool_turn_done]);
    let responses = if preflight {
        // No over-budget replay body is ever expected on the wire.
        vec![
            tool_move,
            MockResponse::sse(&summary_events),
            MockResponse::sse(&recovered_events),
        ]
    } else {
        vec![
            tool_move,
            MockResponse::json(400, serde_json::to_vec(&overflow_error_body).unwrap()),
            MockResponse::sse(&summary_events),
            MockResponse::sse(&recovered_events),
        ]
    };
    MockServer::spawn(responses).await
}

/// Extracts the JSON-lines history records embedded in one utility source
/// chunk message. `compaction/utility.rs` frames each chunk as the source
/// prefix, a fixed instruction line, `chunk=<index>`, the records, and the
/// source suffix; only the bytes between the chunk header and the suffix are
/// returned, so consecutive chunks concatenate back into one record stream.
fn utility_source_payload(text: &str) -> Option<String> {
    const BEGIN: &str = "[BEGIN MINICORE HISTORICAL SOURCE DATA]";
    const END: &str = "[END MINICORE HISTORICAL SOURCE DATA]";
    let start = text.find(BEGIN)? + BEGIN.len();
    let end = text.rfind(END)?;
    let inner = text.get(start..end)?;
    let header = inner.find("chunk=")?;
    let newline = inner[header..].find('\n')?;
    inner.get(header + newline + 1..).map(str::to_owned)
}

/// Drives one agent turn through the loopback fixture and asserts the
/// provider-aware budget invariants shared by both modes.
async fn run_openai_overflow_loopback(preflight: bool) {
    let label = if preflight {
        "p3b2-openai-preflight"
    } else {
        "p3b2-openai-real-loop"
    };
    let (data_dir, _guard) = fixture_dir(label);
    let workspace = data_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let file_content = "hello ".repeat(800);
    std::fs::write(workspace.join("file.txt"), &file_content).unwrap();

    let server = openai_overflow_loopback(preflight).await;

    let mut m_config = model_config("PATH");
    let ModelConfig::OpenAiResponses {
        base_url,
        physical_context_window,
        ..
    } = &mut m_config;
    *base_url = server.base_url().to_owned();
    *physical_context_window = 16_384;

    let config = auto_config(
        data_dir.to_path_buf(),
        BTreeMap::from([("main".to_owned(), m_config)]),
        read_profile(),
    );

    let mut agent = Agent::open(config).await.unwrap();

    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "read file").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );

    let history = read_store_history(&data_dir, info.session_id).await;
    let texts = flatten_texts(&history);
    assert!(texts.iter().any(|t| t == "Recovered: file contains hello"));
    // Provider-owned metadata stays in the request continuation and is never
    // persisted into durable history.
    assert!(
        !serde_json::to_string(&history)
            .unwrap()
            .contains(REPLAY_MARKER)
    );

    let context = agent.session_context(info.session_id).unwrap();
    if preflight {
        // No provider rejection is needed: the plan compressed the over-budget
        // replaying request before it was sent.
        assert!(context.recovery.is_none());
        let automatic = context
            .automatic
            .last
            .expect("preflight plan observation must be retained");
        assert_eq!(automatic.outcome, "compacted");
        let before = automatic.before_tokens.expect("plan before tokens");
        let after = automatic.after_tokens.expect("plan after tokens");
        assert!(after < before, "preflight body must strictly shrink");
        assert!(
            after <= automatic.hard_tokens,
            "preflight body must fit the hard window"
        );
        assert!(automatic.utility_usage.is_some());
    } else {
        let recovery = context.recovery.expect("recovery observation must exist");
        assert_eq!(recovery.outcome, "recovered");
        let before = recovery.before_tokens.expect("recovery before tokens");
        let after = recovery.after_tokens.expect("recovery after tokens");
        assert!(after < before, "recovered body must strictly shrink");
        assert!(recovery.utility_usage.is_some());
    }

    let captured = server.finish().await;
    assert_eq!(captured.len(), if preflight { 3 } else { 4 });
    let bodies = captured
        .iter()
        .map(CapturedRequest::json_body)
        .collect::<Vec<_>>();
    let encoded = |index: usize| serde_json::to_string(&bodies[index]).unwrap();
    // The utility summarizer is the only tool-free request; it carries the
    // source data but never a provider replay.
    let utility = bodies
        .iter()
        .position(|body| body.get("tools").is_none())
        .expect("utility request must be tool-free");
    assert_eq!(utility, if preflight { 1 } else { 2 });
    assert!(encoded(utility).contains("MINICORE HISTORICAL SOURCE DATA"));
    assert!(!encoded(utility).contains(REPLAY_MARKER));
    // The utility summarizer embeds real Runtime history records as JSON lines
    // inside its source messages, so check that stream structurally instead of
    // looking for provider wire items there.
    let utility_payload = bodies[utility]["input"]
        .as_array()
        .expect("utility input array")
        .iter()
        .flat_map(|item| item["content"].as_array().into_iter().flatten())
        .filter_map(|content| content["text"].as_str())
        .filter_map(utility_source_payload)
        .collect::<String>();
    let utility_records = utility_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<HistoryItem>(line)
                .expect("utility source records must be serialized history items")
        })
        .collect::<Vec<_>>();
    let tool_results = utility_records
        .iter()
        .filter_map(|record| match record {
            HistoryItem::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tool_results.len(),
        1,
        "the utility source must contain the real tool result exactly once"
    );
    assert_eq!(tool_results[0].outcome, ToolResultOutcome::Success);
    assert_eq!(tool_results[0].call_id.as_str(), "call_read_1");
    assert_eq!(tool_results[0].tool_name.as_str(), "read");
    assert_eq!(
        tool_results[0].output.content().as_str(),
        format!("1: {file_content}")
    );
    let matching_calls = utility_records
        .iter()
        .filter_map(|record| match record {
            HistoryItem::Assistant(assistant) => Some(assistant),
            _ => None,
        })
        .flat_map(|assistant| assistant.content.iter())
        .filter_map(AssistantPart::as_tool_call)
        .filter(|call| call.tool_call_id().as_str() == "call_read_1")
        .count();
    assert_eq!(matching_calls, 1, "the matching tool call must appear once");

    let folded = encoded(bodies.len() - 1);
    assert!(folded.contains("MINICORE HISTORICAL SUMMARY DATA"));
    assert!(!folded.contains(REPLAY_MARKER));
    assert!(!folded.contains("\"function_call_output\""));
    // The folded request retains the original user prompt exactly once and
    // does not resend the summarized tool result.
    let folded_input = bodies[bodies.len() - 1]["input"]
        .as_array()
        .expect("input array")
        .clone();
    let repeated_prompt = folded_input
        .iter()
        .filter(|item| item["type"] == json!("message") && item["role"] == json!("user"))
        .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
        .filter(|content| content["text"] == json!("read file"))
        .count();
    assert_eq!(
        repeated_prompt, 1,
        "the original user prompt must appear once"
    );
    if preflight {
        // The over-budget replay body was never sent: only the tool request,
        // the utility call, and the folded request reached the provider.
        assert!(bodies[0].get("tools").is_some());
    } else {
        // The rejected attempt replayed the provider-owned raw item exactly as
        // the provider returned it, and the folded retry strictly shrank.
        let replayed = encoded(1);
        assert!(replayed.contains(REPLAY_MARKER));
        assert!(replayed.contains("\"function_call_output\""));
        let rejected_len = serde_json::to_vec(&bodies[1]).unwrap().len();
        let folded_len = serde_json::to_vec(&bodies[bodies.len() - 1]).unwrap().len();
        assert!(
            folded_len < rejected_len,
            "folded body must strictly shrink"
        );
    }
}

#[tokio::test]
async fn test_openai_loopback_http_overflow_recovery_real_loop() {
    run_openai_overflow_loopback(false).await;
}

#[tokio::test]
async fn test_openai_loopback_preflight_compresses_over_budget_replay() {
    run_openai_overflow_loopback(true).await;
}

/// `workspace.status` is owned by the loaded Session: the public Agent path
/// registers an owned worker for it, dropping the awaiter leaves the worker in
/// charge of the git child, and closing the Session joins that worker only
/// after its child was stopped and reaped.
#[cfg(unix)]
#[tokio::test]
async fn an_owned_status_worker_outlives_its_awaiter_until_the_session_closes() {
    use crate::workspace::status::{StatusReapGate, set_status_program, set_status_reap_gate};
    use std::os::unix::fs::PermissionsExt;

    let (data_dir, _data_guard) = fixture_dir(&format!("status-owner-{}", next_id()));
    let (workspace, _workspace_guard) =
        workspace_file("status-owner-ws", "note.txt", b"raw\ncontent\n");
    let model = FakeModel::new("main", []);
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), Arc::clone(&model))]),
        read_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let session_id = info.session_id;

    // The public Agent path runs through the Session-owned worker.
    let plain = agent
        .workspace_status(crate::WorkspaceStatusRequest {
            session_id,
            max_bytes: None,
        })
        .await
        .expect("the public status path answers");
    assert!(!plain.repo_available);
    assert!(!plain.complete);

    // A child that reports its own process id and then holds its pipes open.
    let pid_path = data_dir.join("status-child-pids");
    let script = data_dir.join("slow-status-git");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> '{}'\nsleep 30\n",
            pid_path.display()
        ),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    set_status_program(std::fs::canonicalize(&workspace).unwrap(), script);

    let session = agent.loaded_session(session_id).unwrap();
    let spawn = |session: &crate::sessions::Session| {
        session
            .spawn_status_query(
                session.workspace(),
                crate::WorkspaceStatusRequest {
                    session_id,
                    max_bytes: None,
                },
                tokio_util::sync::CancellationToken::new(),
            )
            .unwrap()
    };
    let first = spawn(&session);
    let first_pid = wait_for_started_child(&pid_path, 0).await;
    assert!(
        process_is_listed(first_pid),
        "the owned child is not running"
    );
    assert_eq!(session.active_status_workers(), 1);

    // The awaiter goes away first: the Session's worker still owns the child
    // and reaps it, so nothing is left behind by a dropped waiter.
    drop(first);
    for _ in 0..200 {
        if !process_is_listed(first_pid) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        !process_is_listed(first_pid),
        "a dropped status awaiter left its owned child unreaped"
    );

    // Closing the Session is the barrier for a query whose awaiter is still
    // alive: while the owner still waits for its child, the close cannot return,
    // and it returns only after that child was actually reaped. The reap gate
    // parks the owner so both facts are observed without sleeping.
    let gate = StatusReapGate::new();
    set_status_reap_gate(
        std::fs::canonicalize(&workspace).unwrap(),
        Arc::clone(&gate),
    );
    let second = spawn(&session);
    let second_pid = wait_for_started_child(&pid_path, 1).await;
    assert!(
        process_is_listed(second_pid),
        "the second owned child is not running"
    );

    let close = agent.close_session(session_id);
    let mut close = std::pin::pin!(close);
    // The first poll cancels the Session's queries, including this worker.
    assert!(futures_util::poll!(&mut close).is_pending());
    gate.entered().await;
    assert!(
        process_is_listed(second_pid),
        "the child was reaped before its owner was released"
    );
    gate.release();
    close.await.unwrap();
    assert!(
        !process_is_listed(second_pid),
        "the closing Session returned before its owned child was reaped"
    );
    assert!(matches!(second.wait().await, Err(AgentError::QueryLimit)));
}

/// Waits until a fake git child has appended its process id, and returns it.
#[cfg(unix)]
async fn wait_for_started_child(pid_path: &Path, index: usize) -> i32 {
    for _ in 0..200 {
        if let Ok(text) = std::fs::read_to_string(pid_path) {
            if let Some(pid) = text.lines().nth(index) {
                return pid.trim().parse().unwrap();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the status query never started child {index}");
}

/// `ps` lists a process that exists, including one that is still a zombie, so
/// an empty listing is evidence that it was reaped rather than merely signalled.
#[cfg(unix)]
fn process_is_listed(pid: i32) -> bool {
    let output = std::process::Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("pid=")
        .output()
        .expect("ps runs");
    !String::from_utf8_lossy(&output.stdout).trim().is_empty()
}

/// End-to-end Bash ownership through a real Agent Session: a running command is
/// cancelled with its loop, the stored process record matches the live
/// `tool_process` event and the `tool.read`/`tool.output` queries, and
/// `close_session` returns only after the owned command was reaped.
#[cfg(unix)]
#[tokio::test]
async fn bash_owned_command_matches_events_queries_and_the_close_join() {
    use crate::tool_data::{CommandStatus, ToolDataStream};

    let (data_dir, _guard) = fixture_dir(&format!("bash-owned-turn-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-owned-turn-ws", "a.txt", b"hello");
    let pid_file = workspace.join("child.pid");
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall(
                "bash",
                json!({"command": "echo $$ > child.pid; exec sleep 30"}),
            ),
            ModelScript::Text("cancelled"),
        ],
    );
    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "run slowly").await;
    // Wait for the public ToolStarted boundary, then for the pid file, so the
    // cancel happens while a process is really owned.
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("bash events must arrive")
            .expect("event stream must remain open");
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn) {
            break;
        }
    }
    for _ in 0..500 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(pid_file.exists(), "the command really started");

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

    // Drain the live process events for this turn and keep the running and
    // terminal records, which must come from the same stored source as queries.
    let mut running = None;
    let mut terminal = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(1), events.recv()).await
    {
        if let AgentEvent::ToolProcess { data, .. } = event {
            if let Some(command) = data.command {
                match command.status {
                    CommandStatus::Running => running = Some((data.tool_ref, command)),
                    CommandStatus::Cancelled => {
                        terminal = Some((data.tool_ref, command));
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    let (owner, running) = running.expect("a running record was published before the first byte");
    let (terminal_ref, terminal) = terminal.expect("a terminal cancelled record was published");
    assert_eq!(terminal_ref, owner);
    assert!(!running.output_complete);

    let queried = read_tool(&agent, owner.clone()).await;
    let command = queried.execution.command.expect("a queried command record");
    assert_eq!(command.status, CommandStatus::Cancelled);
    assert!(command.termination_confirmed);
    assert!(command.exit_code.is_none());
    assert_eq!(command.stdout_base_offset, terminal.stdout_base_offset);
    assert_eq!(command.stdout_observed_end, terminal.stdout_observed_end);

    // `tool.output` and the process record describe the same retained range.
    let page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: owner.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(page.encoding, "base64");
    assert!(page.eof);
    assert_eq!(page.base_offset, command.stdout_base_offset);
    assert_eq!(page.observed_end, command.stdout_observed_end);

    // The cancelled loop already stopped and reaped its owned command.
    let child_pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("the command published its pid")
        .trim()
        .parse()
        .expect("a pid");
    assert!(
        !process_is_listed(child_pid),
        "the cancelled command was not reaped by its owner"
    );

    // Closing the Session is the join barrier and must return cleanly.
    agent.close_session(info.session_id).await.unwrap();
}

/// Two turns across model change: the first Bash command is cancelled, the
/// second model reuses the same tool call id ("bash-call-0") under a new loop,
/// the second Bash command actually runs without mixing up stdout/stderr/queries,
/// and close_session reaps the active second process before returning.
#[cfg(unix)]
#[tokio::test]
async fn bash_two_turns_reuse_tool_call_id_across_loops_and_close_reaps_active() {
    use crate::tool_data::{CommandStatus, ToolDataStream};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("bash-two-turns-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-two-turns-ws", "a.txt", b"hello");
    let first_pid_file = workspace.join("first.pid");
    let second_pid_file = workspace.join("second.pid");

    let model_a = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall(
                "bash",
                json!({"command": "echo $$ > first.pid; exec sleep 30"}),
            ),
            ModelScript::Text("first turn cancelled"),
        ],
    );

    let model_b = FakeModel::new(
        "other",
        [
            ModelScript::ToolCall(
                "bash",
                json!({
                    "command": "printf 'turn2-stdout\\n'; printf 'turn2-stderr\\n' >&2; echo $$ > second.pid; exec sleep 30"
                }),
            ),
            ModelScript::Text("second turn done"),
        ],
    );

    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([
            ("main".to_owned(), Arc::clone(&model_a)),
            ("other".to_owned(), Arc::clone(&model_b)),
        ]),
        profile,
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;

    // --- Turn 1 ---
    let turn_1 = send_text(&mut agent, info.session_id, "first run slowly").await;
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("bash events must arrive")
            .expect("event stream must remain open");
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn_1)
        {
            break;
        }
    }
    for _ in 0..500 {
        if first_pid_file.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(first_pid_file.exists(), "first command started");
    let first_pid: i32 = std::fs::read_to_string(&first_pid_file)
        .expect("read first pid")
        .trim()
        .parse()
        .expect("parse first pid");
    assert!(process_is_listed(first_pid));

    assert!(agent.cancel(turn_1).unwrap());
    let result_1 = wait_text(&agent, turn_1).await;
    assert_eq!(
        result_1.report.outcome,
        minicore_runtime::LoopOutcome::Cancelled(minicore_runtime::CancelReason::User)
    );
    assert!(
        !process_is_listed(first_pid),
        "first process reaped after turn 1 cancelled"
    );

    // Drain turn 1 events to capture turn 1 tool_ref
    let mut turn_1_ref = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(300), events.recv()).await
    {
        if let AgentEvent::ToolProcess { turn, data, .. } = event {
            if turn == turn_1 {
                turn_1_ref = Some(data.tool_ref);
            }
        }
    }
    let tool_ref_1 = turn_1_ref.expect("turn 1 produced tool process event");
    assert_eq!(tool_ref_1.tool_call_id.as_str(), "bash-call-0");

    // --- Switch model to model_b ("other") ---
    agent
        .update_session(crate::agent::UpdateSession {
            session_id: info.session_id,
            model: Some("other".to_owned()),
            reasoning: None,
        })
        .await
        .unwrap();

    // --- Turn 2 ---
    let turn_2 = send_text(
        &mut agent,
        info.session_id,
        "second run with same tool call id",
    )
    .await;
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("bash events must arrive")
            .expect("event stream must remain open");
        if matches!(event, AgentEvent::ToolStarted { turn: event_turn, .. } if event_turn == turn_2)
        {
            break;
        }
    }
    for _ in 0..500 {
        if second_pid_file.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(second_pid_file.exists(), "second command started");
    let second_pid: i32 = std::fs::read_to_string(&second_pid_file)
        .expect("read second pid")
        .trim()
        .parse()
        .expect("parse second pid");
    assert!(process_is_listed(second_pid));

    // Drain turn 2 events until we get tool_ref for turn 2 and chunks
    let mut turn_2_ref = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), events.recv()).await
    {
        if let AgentEvent::ToolProcess { turn, data, .. } = event {
            if turn == turn_2 {
                turn_2_ref = Some(data.tool_ref);
            }
        }
    }
    let tool_ref_2 = turn_2_ref.expect("turn 2 produced tool process event");

    // Both calls used the exact same tool_call_id ("bash-call-0"), but different loops!
    assert_eq!(tool_ref_2.tool_call_id.as_str(), "bash-call-0");
    assert_ne!(
        tool_ref_1.loop_id, tool_ref_2.loop_id,
        "turns must have distinct loops"
    );

    // Queries for tool_ref_2 must return turn 2's distinct stdout/stderr and not mix with turn 1
    let stdout_page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref_2.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    let stdout_bytes = base64::engine::general_purpose::STANDARD
        .decode(&stdout_page.data)
        .unwrap();
    let stdout_str = String::from_utf8_lossy(&stdout_bytes);
    assert!(
        stdout_str.contains("turn2-stdout"),
        "second bash stdout must contain 'turn2-stdout', got: {stdout_str}"
    );

    let stderr_page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref_2.clone(),
            stream: ToolDataStream::Stderr,
            offset: 0,
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    let stderr_bytes = base64::engine::general_purpose::STANDARD
        .decode(&stderr_page.data)
        .unwrap();
    let stderr_str = String::from_utf8_lossy(&stderr_bytes);
    assert!(
        stderr_str.contains("turn2-stderr"),
        "second bash stderr must contain 'turn2-stderr', got: {stderr_str}"
    );

    let queried_2 = read_tool(&agent, tool_ref_2.clone()).await;
    assert_eq!(queried_2.execution.tool_ref, tool_ref_2);
    let cmd_2 = queried_2.execution.command.expect("command record exists");
    assert_eq!(cmd_2.status, CommandStatus::Running);

    // The second command is STILL running (active). Calling close_session must reap it before returning.
    assert!(
        process_is_listed(second_pid),
        "second process must still be running before close_session"
    );
    agent.close_session(info.session_id).await.unwrap();
    assert!(
        !process_is_listed(second_pid),
        "active second command was not reaped by close_session join barrier"
    );
}

/// Proves that when auxiliary tool persistence fails, the core loop outcome is
/// still completed, main history is persisted, the bash process is reaped,
/// and no duplicate tool execution occurs.
#[cfg(unix)]
#[tokio::test]
async fn bash_turn_auxiliary_write_failure_preserves_main_outcome_and_reap() {
    let (data_dir, _guard) = fixture_dir(&format!("bash-aux-fail-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-aux-fail-ws", "a.txt", b"hello");
    let pid_file = workspace.join("child.pid");

    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall(
                "bash",
                json!({"command": "echo $$ > child.pid; printf 'done' > output.txt"}),
            ),
            ModelScript::Text("all done"),
        ],
    );

    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    // Inject auxiliary write failure on this session
    crate::store::fail_next_aux_write(info.session_id);

    let turn = send_text(&mut agent, info.session_id, "run with aux failure").await;
    let result = wait_text(&agent, turn).await;

    // Core loop outcome must be Completed and core history Persisted
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    // Wait for the process to exit and confirm output
    for _ in 0..500 {
        if workspace.join("output.txt").exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(workspace.join("output.txt").exists());

    if pid_file.exists() {
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(!process_is_listed(pid), "bash process was reaped");
    }

    // In-memory tool records are preserved and marked Failed, core outcome intact
    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    assert!(!tool_refs.is_empty());
    for tool_ref in tool_refs {
        let read_res = agent
            .tool_read(crate::tool_data::ToolReadRequest {
                tool_ref: tool_ref.clone(),
                max_bytes: Some(4096),
            })
            .await
            .unwrap();
        assert_eq!(
            read_res.execution.recording,
            crate::tool_data::ToolRecordingState::Failed
        );
        assert_eq!(
            read_res.execution.outcome,
            Some(minicore_runtime::tools::ToolResultOutcome::Success)
        );
    }

    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn bash_turn_auxiliary_write_success_marks_recording_saved_and_persists_to_disk() {
    let (data_dir, _guard) = fixture_dir(&format!("bash-aux-success-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-aux-success-ws", "a.txt", b"hello");

    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall(
                "bash",
                json!({"command": "printf 'persisted cold output' > output.txt"}),
            ),
            ModelScript::Text("all done"),
        ],
    );

    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let turn = send_text(&mut agent, info.session_id, "run with aux success").await;
    let result = wait_text(&agent, turn).await;

    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    assert!(!tool_refs.is_empty());
    let tool_ref = &tool_refs[0];

    // 1. In-memory record is marked Saved
    let live_read = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(
        live_read.execution.recording,
        crate::tool_data::ToolRecordingState::Saved
    );

    // 2. Real cold read from Store disk
    let store = agent.store_handle();
    let recovered = store
        .read_tool_record(tool_ref)
        .await
        .unwrap()
        .expect("persisted tool record must exist on disk");

    let cold_read = recovered.project_read(tool_ref, 4096).unwrap();
    assert_eq!(
        cold_read.execution.recording,
        crate::tool_data::ToolRecordingState::Saved
    );
    assert_eq!(cold_read.execution.name, "bash");

    agent.close_session(info.session_id).await.unwrap();
}

#[tokio::test]
async fn session_close_while_aux_write_held_still_joins_owned_worker() {
    let (data_dir, _guard) = fixture_dir(&format!("bash-aux-close-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-aux-close-ws", "a.txt", b"hello");

    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": "echo done > output.txt"})),
            ModelScript::Text("finished"),
        ],
    );

    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let gate = Arc::new(crate::store::AuxCommitGate::new());
    crate::store::register_aux_commit_gate(info.session_id, Arc::clone(&gate));

    let _turn = send_text(&mut agent, info.session_id, "run with aux notify").await;

    // 1. Prove that execution has reached the aux write phase under lock
    gate.entered.notified().await;

    // 2. Poll close session while aux write is held: must remain pending on owned worker join
    let close_task = tokio::spawn(async move { agent.close_session(info.session_id).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !close_task.is_finished(),
        "close must be waiting for active loop worker"
    );

    // 3. Release gate to let aux write complete
    gate.release.notify_one();

    // 4. Worker joins cleanly and close completes
    let close_res = close_task.await.unwrap();
    assert!(close_res.is_ok());
}

#[tokio::test]
async fn deadline_lock_timeout_marks_records_failed_without_alloc() {
    let (data_dir, _guard) = fixture_dir(&format!("bash-aux-deadline-{}", next_id()));
    let (workspace, _guard) = workspace_file("bash-aux-deadline-ws", "a.txt", b"hello");

    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": "printf 'fast'"})),
            ModelScript::Text("done"),
        ],
    );

    let profile = Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: vec!["bash".to_owned()],
        max_tool_rounds: 8,
        approval: ApprovalMode::Auto,
    };

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        profile,
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;

    let session = agent.loaded_session(info.session_id).unwrap();
    let presentation = session.presentation();
    let loop_id = minicore_runtime::LoopId::new().unwrap();
    let tool_ref = crate::tool_data::ToolRef {
        session_id: info.session_id,
        loop_id,
        request_index: 0,
        tool_call_id: minicore_runtime::ToolCallId::new("call-dl-timeout").unwrap(),
    };

    presentation.tool_data().note_requested(&tool_ref, "bash");
    presentation.tool_data().mark_running(&tool_ref);
    presentation.tool_data().note_result(&tool_ref, "hi\n");
    presentation.tool_data().finish_and_snapshot(
        &tool_ref,
        minicore_runtime::tools::ToolResultOutcome::Success,
    );

    // Initial state is MemoryOnly
    let initial_read = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: Some(4096),
        })
        .await
        .unwrap();
    assert_eq!(
        initial_read.execution.recording,
        crate::tool_data::ToolRecordingState::MemoryOnly
    );

    // Call persist_loop_tool_records with an already-expired deadline
    let store = agent.store_handle();
    let expired_deadline = std::time::Instant::now() - std::time::Duration::from_millis(10);

    let results = store
        .persist_loop_tool_records(
            std::slice::from_ref(&tool_ref),
            presentation.tool_data().as_ref(),
            expired_deadline,
        )
        .await;

    assert_eq!(results.len(), 1);
    assert!(results[0].1.is_err());

    agent.close_session(info.session_id).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn bash_dual_stream_cold_read_closure_after_restart_without_session_loaded() {
    use crate::tool_data::{CommandStatus, ToolDataStream, ToolExecutionState, ToolRecordingState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-cold-read-closure-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-cold-read-closure-ws", "marker.txt", b"");

    let script = r"i=0; while [ $i -lt 512 ]; do printf '\316\273\000\377\033[31mX\033[0m'; printf '\347\225\214\033[2K\r' >&2; i=$((i+1)); done";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.persistence,
        crate::sessions::TurnPersistence::Persisted
    );

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    assert_eq!(tool_refs.len(), 1);
    let tool_ref = tool_refs[0].clone();

    // Verify in-memory state before closing
    let live_read = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(live_read.execution.recording, ToolRecordingState::Saved);
    assert_eq!(live_read.execution.state, ToolExecutionState::Succeeded);

    // Close session and drop agent instance to simulate full restart
    agent.close_session(info.session_id).await.unwrap();
    drop(agent);

    // Re-open fresh agent without loading the session
    let agent2 = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        bash_profile(),
    )
    .await;
    assert!(agent2.loaded_session(info.session_id).is_none());

    // 1. tool_read cold projection
    let cold_read = agent2
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(cold_read.execution.tool_ref, tool_ref);
    assert_eq!(cold_read.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(cold_read.execution.recording, ToolRecordingState::Saved);

    let command = cold_read
        .execution
        .command
        .expect("command record preserved on cold read");
    assert_eq!(command.status, CommandStatus::Exited);
    assert_eq!(command.exit_code, Some(0));
    assert!(command.termination_confirmed);
    assert!(command.output_complete);
    assert!(command.stdout_observed_end > 0);
    assert!(command.stderr_observed_end > 0);

    for (stream, expected) in [
        (
            ToolDataStream::Stdout,
            b"\xce\xbb\x00\xff\x1b[31mX\x1b[0m".repeat(512),
        ),
        (ToolDataStream::Stderr, b"\xe7\x95\x8c\x1b[2K\r".repeat(512)),
    ] {
        let mut bytes = Vec::new();
        let mut offset = 0_u64;
        let mut pages = 0;
        loop {
            let page = agent2
                .tool_output(crate::tool_data::ToolOutputRequest {
                    tool_ref: tool_ref.clone(),
                    stream,
                    offset,
                    max_bytes: Some(1024),
                })
                .await
                .unwrap();
            assert_eq!(page.encoding, "base64");
            assert!(serde_json::to_vec(&page).unwrap().len() <= 1024);
            assert_eq!(page.base_offset, offset);
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&page.data)
                .unwrap();
            assert_eq!(page.next_offset - offset, decoded.len() as u64);
            bytes.extend_from_slice(&decoded);
            offset = page.next_offset;
            pages += 1;
            if page.eof {
                break;
            }
        }
        assert!(pages > 1);
        assert_eq!(bytes, expected);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cold_read_is_strictly_read_only_and_survives_missing_workspace_and_partial_history() {
    use crate::tool_data::{ToolDataStream, ToolExecutionState, ToolRecordingState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-readonly-guard-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-readonly-guard-ws", "marker.txt", b"");

    let script = "echo hello-cold-read";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    agent.close_session(info.session_id).await.unwrap();
    drop(agent);

    // Tamper with environment to prove cold-read requires NO valid workspace
    std::fs::remove_dir_all(&workspace).unwrap();
    assert!(!workspace.exists());

    // Tamper with history.jsonl by appending a trailing truncated JSON fragment
    let history_path = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join("history.jsonl");
    let initial_history_len = std::fs::metadata(&history_path).unwrap().len();
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .unwrap();
        file.write_all(b"{\"partial_corrupt_line\": true").unwrap();
    }
    let tampered_history_len = std::fs::metadata(&history_path).unwrap().len();
    assert!(tampered_history_len > initial_history_len);

    // Record tool aux directory contents using real store layout
    let hash = crate::store::tool_ref_hash(&tool_ref);
    let aux_dir = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join(crate::store::AUX_TOOLS_DIR)
        .join(&hash);
    let mut before_aux_files = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&aux_dir).unwrap() {
        let entry = entry.unwrap();
        let bytes = std::fs::read(entry.path()).unwrap();
        before_aux_files.insert(entry.file_name(), bytes);
    }
    assert!(!before_aux_files.is_empty());

    // Re-open agent without loading the session
    let agent2 = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        bash_profile(),
    )
    .await;

    // Cold read must succeed seamlessly
    let cold_read = agent2
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(cold_read.execution.state, ToolExecutionState::Succeeded);
    assert_eq!(cold_read.execution.recording, ToolRecordingState::Saved);

    let page = agent2
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&page.data)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&decoded), "hello-cold-read\n");

    // Strictly read-only: history.jsonl length must NOT be rewritten or repaired
    assert_eq!(
        std::fs::metadata(&history_path).unwrap().len(),
        tampered_history_len,
        "history.jsonl must remain byte-identical without repair side effects"
    );

    // Strictly read-only: aux directory files must remain byte-identical
    for (name, expected_bytes) in before_aux_files {
        let current = std::fs::read(aux_dir.join(name)).unwrap();
        assert_eq!(current, expected_bytes, "aux files must not be altered");
    }
}

#[tokio::test]
async fn cold_read_missing_aux_or_unknown_tool_maps_to_tool_not_found() {
    let (data_dir, _guard) = fixture_dir(&format!("p5b2-not-found-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-not-found-ws", "marker.txt", b"");

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), FakeModel::new("main", []))]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    agent.close_session(info.session_id).await.unwrap();

    let non_existent_ref = crate::tool_data::ToolRef {
        session_id: info.session_id,
        loop_id: minicore_runtime::LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: minicore_runtime::ToolCallId::new("call-missing").unwrap(),
    };

    let read_err = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: non_existent_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(read_err, AgentError::ToolNotFound));

    let output_err = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: non_existent_ref,
            stream: crate::tool_data::ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(output_err, AgentError::ToolNotFound));
}

#[cfg(unix)]
#[tokio::test]
async fn warm_record_served_immediately_without_disk_or_gate() {
    use crate::read::{ToolReadGate, gate_next_tool_read};
    use crate::tool_data::{ToolDataStream, ToolExecutionState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-warm-gate-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-warm-gate-ws", "marker.txt", b"");

    let script = "printf 'warm-content\n'";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;

    let tool_refs = agent
        .loaded_session(info.session_id)
        .unwrap()
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    // 1. Install a gate on the cold path for this tool_ref.
    let gate = Arc::new(ToolReadGate::new());
    gate_next_tool_read(tool_ref.clone(), Arc::clone(&gate));

    // 2. Corrupt disk record to ensure disk read would fail if attempted
    let hash = crate::store::tool_ref_hash(&tool_ref);
    let record_file = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join(crate::store::AUX_TOOLS_DIR)
        .join(&hash)
        .join(crate::store::TOOL_RECORD_FILE);
    std::fs::write(&record_file, b"corrupted-not-json").unwrap();

    // 3. Warm tool_read must return immediately without blocking on gate or failing from corrupted disk!
    let read_res = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        agent.tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        }),
    )
    .await
    .expect("warm read must not block on disk gate")
    .unwrap();
    assert_eq!(read_res.execution.state, ToolExecutionState::Succeeded);

    // 4. Warm tool_output must also return immediately
    let output_page = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        agent.tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        }),
    )
    .await
    .expect("warm output must not block on disk gate")
    .unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&output_page.data)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&decoded), "warm-content\n");
}

#[cfg(unix)]
#[tokio::test]
async fn evicted_stream_restored_via_narrow_merge_from_disk() {
    use crate::tool_data::{ToolDataAvailability, ToolDataStream, ToolRecordingState};
    use base64::Engine;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-evicted-restore-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-evicted-restore-ws", "marker.txt", b"");

    let script = "printf 'persisted-stream-data\n'";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;

    let session = agent.loaded_session(info.session_id).unwrap();
    let tool_refs = session
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    // Verify initially saved
    let read_1 = agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: tool_ref.clone(),
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(read_1.execution.recording, ToolRecordingState::Saved);

    // Push 9 records with 1 MiB chunk each to exceed MAX_TOOL_TOTAL_BYTES (8 MiB) and trigger eviction
    let filler_loop = minicore_runtime::LoopId::new().unwrap();
    for i in 1..=9 {
        let filler = crate::tool_data::ToolRef {
            session_id: info.session_id,
            loop_id: filler_loop,
            request_index: i,
            tool_call_id: minicore_runtime::ToolCallId::new(format!("filler-{i}")).unwrap(),
        };
        session
            .presentation()
            .tool_data()
            .note_requested(&filler, "bash");
        session.presentation().tool_data().note_stream_chunk(
            &filler,
            ToolDataStream::Stdout,
            &vec![b'x'; 1024 * 1024],
        );
    }

    // Direct memory check on session shows availability is Expired
    let mem_output = session
        .presentation()
        .tool_data()
        .output(
            &crate::tool_data::ToolOutputRequest {
                tool_ref: tool_ref.clone(),
                stream: ToolDataStream::Stdout,
                offset: 0,
                max_bytes: None,
            },
            4096,
        )
        .unwrap();
    assert_eq!(mem_output.availability, ToolDataAvailability::Expired);

    // But querying through Agent::tool_output uses narrow merge with disk!
    let merged_output = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(merged_output.availability, ToolDataAvailability::Available);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&merged_output.data)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&decoded), "persisted-stream-data\n");
}

#[cfg(unix)]
#[tokio::test]
async fn both_sources_unavailable_preserves_known_offsets() {
    use crate::tool_data::{ToolDataAvailability, ToolDataStream};

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-unavailable-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-unavailable-ws", "marker.txt", b"");

    let script = "printf 'should-be-unavailable\n'";
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "run bash").await;
    wait_text(&agent, turn).await;

    let session = agent.loaded_session(info.session_id).unwrap();
    let tool_refs = session
        .presentation()
        .tool_data()
        .loop_tool_refs(info.session_id, turn.loop_id);
    let tool_ref = tool_refs[0].clone();

    // Push 9 records with 1 MiB chunk each to evict stdout
    let filler_loop = minicore_runtime::LoopId::new().unwrap();
    for i in 1..=9 {
        let filler = crate::tool_data::ToolRef {
            session_id: info.session_id,
            loop_id: filler_loop,
            request_index: i,
            tool_call_id: minicore_runtime::ToolCallId::new(format!("filler-{i}")).unwrap(),
        };
        session
            .presentation()
            .tool_data()
            .note_requested(&filler, "bash");
        session.presentation().tool_data().note_stream_chunk(
            &filler,
            ToolDataStream::Stdout,
            &vec![b'x'; 1024 * 1024],
        );
    }

    // Corrupt disk aux record so disk cannot restore it
    let hash = crate::store::tool_ref_hash(&tool_ref);
    let record_file = data_dir
        .join("sessions")
        .join(info.session_id.to_string())
        .join(crate::store::AUX_TOOLS_DIR)
        .join(&hash)
        .join(crate::store::TOOL_RECORD_FILE);
    std::fs::write(&record_file, b"corrupted-record").unwrap();

    // Query tool_output: both sources cannot provide data, availability is Expired/Unavailable,
    // but observed_end retains the observed range (not 0!)
    let page = agent
        .tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        page.availability,
        ToolDataAvailability::Expired | ToolDataAvailability::Unavailable
    ));
    assert_eq!(page.observed_end, b"should-be-unavailable\n".len() as u64);
    assert!(page.data.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn reader_drop_or_timeout_does_not_cancel_running_bash() {
    use crate::tool_data::ToolDataStream;

    let (data_dir, _guard) = fixture_dir(&format!("p5b2-reader-drop-{}", next_id()));
    let (workspace, _guard) = workspace_file("p5b2-reader-drop-ws", "marker.txt", b"");

    let pid_file = workspace.join("child.pid");
    let script = format!("echo $$ > '{}'; sleep 5", pid_file.display());
    let model = FakeModel::new(
        "main",
        [
            ModelScript::ToolCall("bash", json!({"command": script})),
            ModelScript::Text("done"),
        ],
    );

    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        bash_profile(),
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "sleep bash").await;

    // Wait for the tool invocation to start running
    let tool_ref = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap();
        if let AgentEvent::ToolInvocation { data, .. } = event {
            break data.tool_ref;
        }
    };

    // Wait until child PID is written
    while !pid_file.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Query tool_output with an immediately expiring timeout to simulate reader drop/timeout
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(1),
        agent.tool_output(crate::tool_data::ToolOutputRequest {
            tool_ref: tool_ref.clone(),
            stream: ToolDataStream::Stdout,
            offset: 0,
            max_bytes: None,
        }),
    )
    .await;

    // The reader dropping/timing out MUST NOT kill the running process!
    assert!(
        process_is_listed(pid),
        "child bash process must still be running after reader timeout"
    );

    // Clean up
    let _ = wait_text(&agent, turn).await;
}
