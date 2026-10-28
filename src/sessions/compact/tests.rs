use super::*;
use crate::test_support::{Gate, TempDir};
use minicore_runtime::history::{UserHistory, UserMessageKind};
use minicore_runtime::model::{
    AssistantPart, ModelCallContext, ModelEvent, ModelFinishReason, ModelRequest, ModelStartFuture,
    ModelStream, ReasoningPreference,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicUsize;

struct SummaryModel {
    descriptor: ModelDescriptor,
    output: String,
    calls: AtomicUsize,
}
impl Model for SummaryModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }
    fn start(&self, _: ModelRequest, _: ModelCallContext) -> ModelStartFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = vec![
            Ok(ModelEvent::text_delta(self.output.clone()).unwrap()),
            Ok(ModelEvent::Usage {
                usage: minicore_runtime::model::Usage::default()
                    .with_provider_total_tokens(Some(8_000)),
            }),
            Ok(ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            }),
        ];
        Box::pin(async move { Ok(Box::pin(futures_util::stream::iter(events)) as ModelStream) })
    }
}

async fn fixture(output: &str, window: u64) -> (TempDir, crate::Agent, Session, Arc<SummaryModel>) {
    let temp = TempDir::new("compaction-worker");
    let model = Arc::new(SummaryModel {
        descriptor: ModelDescriptor::new(
            "main".parse().unwrap(),
            window,
            BTreeSet::from([ReasoningPreference::Auto, ReasoningPreference::Low]),
            true,
        )
        .unwrap(),
        output: output.into(),
        calls: AtomicUsize::new(0),
    });
    let config = crate::config::AgentConfig {
        data_dir: temp.path().join("data"),
        event_capacity: 256,
        default_profile: "test".into(),
        profiles: BTreeMap::from([(
            "test".into(),
            crate::profiles::Profile {
                model: "main".into(),
                reasoning: ReasoningPreference::Auto,
                system_prompt: "system".into(),
                tools: vec![],
                max_tool_rounds: 8,
                approval: crate::profiles::ApprovalMode::Auto,
            },
        )]),
        models: BTreeMap::from([(
            "main".into(),
            crate::models::ModelConfig::OpenAiResponses {
                model: "main".into(),
                base_url: "https://example.invalid/v1".into(),
                api_key_env: "TEST_KEY".into(),
                physical_context_window: window.try_into().unwrap(),
                output_budget_tokens: 100,
                safety_margin_tokens: 100,
                supported_reasoning: model.descriptor.supported_reasoning.clone(),
                supports_tools: true,
                request_timeout_seconds: None,
            },
        )]),
        loop_options: Default::default(),
        compaction: crate::config::CompactionConfig::default(),
    };
    let models = crate::models::Models::from_values(BTreeMap::from([(
        "main".into(),
        model.clone() as Arc<dyn Model>,
    )]));
    let mut agent = crate::Agent::open_with_models(config, models)
        .await
        .unwrap();
    let info = agent
        .create_session(crate::CreateSession {
            workspace: temp.path().to_owned(),
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let session = agent.loaded_session(info.session_id).unwrap();
    (temp, agent, session, model)
}

async fn append_source(session: &Session, items: Vec<HistoryItem>, loop_id: LoopId) {
    session
        .shared
        .store
        .append_loop(
            session.session_id(),
            &StoredLoopRecord {
                loop_id,
                outcome: StoredLoopOutcome::Completed,
                items: items.clone(),
                usage: Default::default(),
                requests: 1,
                tool_rounds: 0,
                final_config_revision: ConfigRevision::INITIAL,
                completed_at: "2026-01-01T00:00:00.000Z".into(),
                user_times: None,
                interrupted: None,
            },
        )
        .await
        .unwrap();
    let mut inner = session.lock_inner();
    inner.history = inner
        .history
        .iter()
        .cloned()
        .chain(items)
        .collect::<Vec<_>>()
        .into();
}

async fn append_text(session: &Session, bytes: usize) {
    let loop_id = LoopId::new().unwrap();
    append_source(
        session,
        vec![
            HistoryItem::User(UserHistory {
                loop_id,
                kind: UserMessageKind::Prompt,
                input: UserInput::text("x".repeat(bytes)).unwrap(),
            }),
            HistoryItem::Assistant(minicore_runtime::history::AssistantHistory {
                loop_id,
                request_index: 0,
                model: "main".parse().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::Text("answer".into())],
                provider_replay: None,
                finish_reason: ModelFinishReason::Stop,
                usage: minicore_runtime::model::Usage::default()
                    .with_provider_total_tokens(Some(4_000)),
            }),
        ],
        loop_id,
    )
    .await;
}

#[tokio::test]
async fn high_final_usage_waits_until_the_next_turn_to_compact() {
    let (_temp, mut agent, session, model) = fixture("answer", 8_000).await;
    for _ in 0..3 {
        append_text(&session, 6_000).await;
    }
    let first = agent
        .send(crate::SendMessage {
            session_id: session.session_id(),
            text: "continue".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent.wait_turn(first).await.unwrap().report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let task = session.lock_inner().active.as_ref().unwrap().task.clone();
    task.join().await.unwrap();
    let operation = session.lock_inner().compaction.clone();
    if let Some(operation) = operation {
        operation.join().await.unwrap();
    }
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        1,
        "turn completion must not call the summary model"
    );
    assert!(session.context().last_result.is_none());
    assert!(session.shared.compaction.coverage().is_none());
    let second = agent
        .send(crate::SendMessage {
            session_id: session.session_id(),
            text: "next turn".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        agent.wait_turn(second).await.unwrap().report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let task = session.lock_inner().active.as_ref().unwrap().task.clone();
    task.join().await.unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    let automatic = session.context().automatic.last.unwrap();
    assert_eq!(automatic.loop_id, Some(second.loop_id));
    assert_eq!(automatic.request_index, Some(0));
    assert_eq!(automatic.before_tokens, Some(8_000));
    assert_eq!(automatic.outcome, "compacted");
    assert!(session.context().last_result.is_none());
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn turn_wait_does_not_wait_for_projection_settlement() {
    let (_temp, mut agent, session, model) = fixture("summary", 8_000).await;
    let old = LoopId::new().unwrap();
    let call_id = "read-old".parse().unwrap();
    append_source(
        &session,
        vec![
            HistoryItem::Assistant(minicore_runtime::history::AssistantHistory {
                loop_id: old,
                request_index: 0,
                model: "main".parse().unwrap(),
                reasoning: ReasoningPreference::Auto,
                content: vec![AssistantPart::ToolCall(
                    minicore_runtime::model::ToolCall::new(
                        call_id,
                        "read".parse().unwrap(),
                        serde_json::json!({"path":"old"}),
                        0,
                    )
                    .unwrap(),
                )],
                provider_replay: None,
                finish_reason: ModelFinishReason::ToolCalls,
                usage: minicore_runtime::model::Usage::default()
                    .with_provider_total_tokens(Some(8_000)),
            }),
            HistoryItem::ToolResult(minicore_runtime::history::ToolResultHistory {
                loop_id: old,
                request_index: 0,
                call_id: "read-old".parse().unwrap(),
                tool_name: "read".parse().unwrap(),
                outcome: minicore_runtime::tools::ToolResultOutcome::Success,
                output: minicore_runtime::tools::ToolOutput::new("x".repeat(40_000)).unwrap(),
            }),
        ],
        old,
    )
    .await;
    let gate = Arc::new(Gate::new());
    crate::store::SUMMARY_COMMIT_GATES.arm(session.session_id(), Arc::clone(&gate));
    let turn = agent
        .send(crate::SendMessage {
            session_id: session.session_id(),
            text: "continue".into(),
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.wait_turn(turn))
        .await
        .expect("wait blocked on settlement")
        .unwrap();
    assert_eq!(result.persistence, TurnPersistence::Persisted);
    assert!(session.shared.compaction.coverage().is_none());
    gate.release();
    session.cleanup_finished().await.unwrap();
    assert_eq!(
        session.shared.compaction.coverage().unwrap().1,
        session.lock_inner().history.len()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
}
