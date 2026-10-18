mod admission;
mod bash;
mod cancellation;
mod changes;
mod cold_read;
mod compaction;
mod display;
mod events;
mod legacy;
mod lifecycle;
mod model_update;
mod open;
mod openai_recovery;
mod persistence;
mod post_turn;
mod preview;
mod recovery;
mod reload;
mod running_update;
mod settlement;
mod steering;
mod summary;
mod tool_data;
mod tool_loop;
mod workers;
mod workflows;
use compaction::pi_history_exchange;
use display::continue_display_read;
use display::display_read;
use summary::flatten_texts;
use tool_data::history_tool_refs;
use tool_data::read_tool;
use tool_data::wait_tool_stream_bytes;
use workers::process_is_listed;

mod core;

use super::{ReloadResult, SteerMessage};
use crate::test_support::Gate;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

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
use minicore_runtime::value::BoundedText;
use minicore_runtime::{LoopId, ToolCallId};

use crate::config::{AgentConfig, CompactionConfig};
use crate::error::AgentError;
use crate::event::{AgentEvent, OutputChannel};
use crate::history::GetHistory;
use crate::ids::SessionId;
use crate::models::{ModelConfig, Models};
use crate::profiles::{ApprovalMode, Profile};
use crate::read::ReadSession;
use crate::sessions::{
    LoopSubmission, PANIC_WORKERS, PAUSE_AFTER_COMPACTION_RESULT, PAUSE_AFTER_RUNTIME_START,
    PAUSE_BEFORE_JOIN,
};
use crate::store::{
    SESSION_FORMAT_VERSION, SessionRecord, Store, StoredLoopOutcome, StoredLoopRecord,
};
use crate::tool_data::ToolData;
use crate::tools::command::CommandOwners;
use crate::tools::observe::ToolObserver;

use super::{
    Agent, CompactSession, CreateSession, RenameSession, SendMessage, SessionUpdateResult, TurnRef,
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
    ReplayOnly(minicore_runtime::model::ProviderReplay),
    PreviewEvents(Vec<Result<ModelEvent, ModelError>>, Option<BlockGate>),
    Text(&'static str),
    /// Text answer with a distinct per-request usage so per-request identity
    /// (loop_id, request_index) can be asserted (spec 9.4/12.4).
    TextWithUsage(&'static str, u64, u64),
    ToolCall(&'static str, serde_json::Value),
    ToolCallAfterGate(BlockGate, &'static str, serde_json::Value),
    /// Parks until `release` fires and signals `entered` when parked.
    BlockUntil(BlockGate, &'static str),
    BlockUntilDrop(BlockGate, Arc<AtomicBool>, &'static str),

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
                ModelScript::PreviewEvents(events, gate) => {
                    let stream: ModelStream = Box::pin(stream::unfold(
                        (events.into_iter(), gate),
                        |(mut events, gate)| async move {
                            if let Some(event) = events.next() {
                                tokio::task::yield_now().await;
                                Some((event, (events, gate)))
                            } else {
                                if let Some(gate) = gate {
                                    gate.entered.notify_one();
                                    gate.release.notified().await;
                                }
                                None
                            }
                        },
                    ));
                    Ok(stream)
                }
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
                ModelScript::ReplayOnly(replay) => events(vec![
                    ModelEvent::ProviderReplay { replay },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ]),
                ModelScript::Text(text) => {
                    // Respect ModelEvent's delta bound while allowing fixtures
                    // to exercise the independently bounded assembled response.
                    let mut rows = Vec::new();
                    let mut remaining = text;
                    while !remaining.is_empty() {
                        let mut end = remaining.len().min(4_096);
                        while !remaining.is_char_boundary(end) {
                            end -= 1;
                        }
                        rows.push(ModelEvent::text_delta(&remaining[..end]).unwrap());
                        remaining = &remaining[end..];
                    }
                    rows.push(ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    });
                    rows.push(ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    });
                    events(rows)
                }
                ModelScript::TextWithUsage(text, input, output) => events(vec![
                    ModelEvent::text_delta(text).unwrap(),
                    ModelEvent::Usage {
                        usage: Usage::new(input, output, 0)
                            .with_provider_total_tokens(Some(input + output)),
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
                    )
                    .unwrap();
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

fn session_options(agent: &Agent, session_id: SessionId) -> minicore_runtime::LoopOptions {
    agent
        .sessions
        .get(session_id)
        .expect("session must be loaded")
        .options_for_test()
}

fn session_config_generation(agent: &Agent, session_id: SessionId) -> u64 {
    agent
        .sessions
        .get(session_id)
        .expect("session must be loaded")
        .compaction_state()
        .request_settings()
        .config_generation
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

fn model_config_with_timeout(api_key_env: &str, seconds: u64) -> ModelConfig {
    let mut config = model_config(api_key_env);
    let ModelConfig::OpenAiResponses {
        request_timeout_seconds,
        ..
    } = &mut config;
    *request_timeout_seconds = Some(seconds);
    config
}

async fn open_agent(
    data_dir: &Path,
    fake_models: BTreeMap<String, Arc<FakeModel>>,
    profile: Profile,
) -> Agent {
    let mut models_config = BTreeMap::new();
    for id in fake_models.keys() {
        models_config.insert(id.clone(), model_config("MINICORE_AGENT_TEST_KEY"));
    }
    open_agent_with_configs(data_dir, fake_models, models_config, profile).await
}

async fn open_agent_with_configs(
    data_dir: &Path,
    fake_models: BTreeMap<String, Arc<FakeModel>>,
    models_config: BTreeMap<String, ModelConfig>,
    profile: Profile,
) -> Agent {
    let models = fake_models
        .iter()
        .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
        .collect::<BTreeMap<_, _>>();
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
    let mut models_config = BTreeMap::new();
    for id in fake_models.keys() {
        models_config.insert(id.clone(), model_config("MINICORE_AGENT_TEST_KEY"));
    }
    open_agent_auto_with_configs(data_dir, fake_models, models_config, profile).await
}

async fn open_agent_auto_with_configs(
    data_dir: &Path,
    fake_models: BTreeMap<String, Arc<FakeModel>>,
    models_config: BTreeMap<String, ModelConfig>,
    profile: Profile,
) -> Agent {
    let models = fake_models
        .iter()
        .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
        .collect::<BTreeMap<_, _>>();
    let use_model_specific_timeouts = models_config
        .values()
        .any(|model| model.request_timeout().is_some());
    let mut config = auto_config(data_dir.to_path_buf(), models_config, profile);
    // Keep the global model timeout below the per-model provider timeouts so
    // the candidate model remains observable in derived prompt options.
    if use_model_specific_timeouts {
        config.loop_options.prompt_timeout_seconds = Some(1);
        config.loop_options.model_timeout_seconds = Some(5);
    }
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
            provider_replay: None,
            loop_id,
            request_index: 0,
            model: "main".parse::<ModelRef>().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::Text(assistant_text.into())],
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::new(1, 2, 0),
        }),
    ];
    if include_opaque_reasoning {
        items.push(HistoryItem::Assistant(AssistantHistory {
            provider_replay: None,
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
                provider_replay: None,
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
            || minicore_runtime::history::estimate_history_bytes(&loaded.history)
                > limits.max_history_bytes
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
                    tool_rounds: u64::try_from(tool_round_count).unwrap(),
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

async fn wait_post_turn_noop(agent: &Agent, session_id: SessionId, loop_id: LoopId) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let context = agent.session_context(session_id).unwrap();
            if context.current_operation.is_none() {
                let result = context
                    .last_result
                    .expect("automatic budget check must publish its result");
                assert_eq!(result.operation_id, format!("auto-{loop_id}"));
                assert_eq!(result.status, crate::compaction::CompactionStatus::Noop);
                assert!(result.utility_usage.is_none());
                agent
                    .loaded_session(session_id)
                    .unwrap()
                    .cleanup_finished()
                    .await
                    .unwrap();
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("post-turn Noop must settle without model work");
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

mod recent_tail;

impl Agent {
    pub(crate) async fn open_file_with_models(
        path: PathBuf,
        models: Models,
    ) -> Result<Self, AgentError> {
        let config = AgentConfig::load(&path).map_err(AgentError::Config)?;
        Self::open_parts(config, models, Some(path)).await
    }

    pub(crate) async fn open_with_models(
        config: AgentConfig,
        models: Models,
    ) -> Result<Self, AgentError> {
        config.validate().map_err(AgentError::Config)?;
        Self::open_parts(config, models, None).await
    }
    pub(crate) fn reload_settings_with_models(
        &mut self,
        candidate: AgentConfig,
        models: Models,
    ) -> Result<ReloadResult, AgentError> {
        candidate.validate().map_err(AgentError::Config)?;
        if candidate.data_dir != self.config.data_dir
            || candidate.event_capacity != self.config.event_capacity
        {
            return Err(AgentError::ReloadRequiresRestart);
        }
        self.apply_reload_settings(candidate, models)
    }
    pub(crate) fn abort_active_task_for_test(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<(), AgentError> {
        let session = self.session(session_id)?;
        session.abort_active_task()
    }
    pub(crate) fn runtime_loop_finished_for_test(
        &self,
        session_id: crate::ids::SessionId,
    ) -> Result<bool, AgentError> {
        let session = self.session(session_id)?;
        Ok(session.runtime_loop_finished())
    }
    pub(crate) fn forward_runtime_event_for_test(
        &self,
        session_id: crate::ids::SessionId,
        envelope: minicore_runtime::LoopEventEnvelope,
    ) -> Result<(), AgentError> {
        let session = self.session(session_id)?;
        crate::sessions::forward_loop_event(session_id, envelope, session);
        Ok(())
    }
}
