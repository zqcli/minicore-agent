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
    LoopSubmission, WorkerGate, panic_next_worker, pause_next_admission_after_result,
    pause_next_compaction_after_result, pause_next_worker_before_join,
};
use crate::store::{
    SESSION_FORMAT_VERSION, SessionRecord, Store, StoredLoopOutcome, StoredLoopRecord,
    fail_next_append, fail_next_record_write,
};

use super::{
    Agent, CompactSession, CreateSession, RenameSession, SessionUpdateResult, TurnRef,
    UpdateSession,
};

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
        Self::with_window(model_ref, 16_384, scripts)
    }

    fn with_window(
        model_ref: &str,
        context_window: u64,
        scripts: impl IntoIterator<Item = ModelScript>,
    ) -> Arc<Self> {
        let model_ref: ModelRef = model_ref.parse().unwrap();
        let descriptor = ModelDescriptor::new(
            model_ref.clone(),
            context_window,
            fake_supported_reasoning(),
            true,
        )
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

fn read_tool(
    agent: &Agent,
    tool_ref: crate::tool_data::ToolRef,
) -> crate::tool_data::ToolReadResult {
    agent
        .tool_read(crate::tool_data::ToolReadRequest {
            tool_ref,
            max_bytes: None,
        })
        .unwrap()
}

fn read_output(agent: &Agent, tool_ref: crate::tool_data::ToolRef) -> String {
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
    let first = read_tool(&agent, refs[0].clone());
    let second = read_tool(&agent, refs[1].clone());
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
        agent.tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: guessed,
            max_bytes: None,
        }),
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
        .invocation
        .expect("session-a invocation");
    let invocation_b = read_tool(&agent, ref_b[0].clone())
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
        agent.tool_read(crate::tool_data::ToolReadRequest {
            tool_ref: cross_session,
            max_bytes: None,
        }),
        Err(AgentError::ToolNotFound)
    ));
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
    let waiting = read_tool(&agent, tool_ref.clone());
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
    let finished = read_tool(&agent, refs[0].clone());
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
    let queried = read_tool(&agent, execution_event.tool_ref.clone());
    assert_eq!(execution_event.tool_ref, queried.execution.tool_ref);
    assert_eq!(execution_event.state, queried.execution.state);
    assert_eq!(execution_event.outcome, queried.execution.outcome);
    assert_eq!(invocation_event.tool_ref, execution_event.tool_ref);
    assert_eq!(
        invocation_event.subject,
        queried.invocation.expect("queried invocation").subject
    );
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
    assert_eq!(read_output(&agent, refs[0].clone()), expected_rendered);
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
    let result = read_tool(&agent, refs[0].clone());
    assert_eq!(
        result.execution.state,
        crate::tool_data::ToolExecutionState::Succeeded
    );
    assert!(read_output(&agent, refs[0].clone()).starts_with("1: hello"));
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
    let result = read_tool(&agent, refs[0].clone());
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
    let output = read_output(&agent, refs[0].clone());
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
    let finished = read_tool(&agent, data.tool_ref);
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
    let result = read_tool(&agent, refs[0].clone());
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
