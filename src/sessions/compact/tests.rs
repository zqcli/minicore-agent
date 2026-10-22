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
        vec![HistoryItem::User(UserHistory {
            loop_id,
            kind: UserMessageKind::Prompt,
            input: UserInput::text("x".repeat(bytes)).unwrap(),
        })],
        loop_id,
    )
    .await;
}

fn reservation(
    session: &Session,
    model: &Arc<SummaryModel>,
    trigger: u64,
) -> CompactionReservation {
    let inner = session.lock_inner();
    CompactionReservation {
        operation: CompactionOperation::new("auto-test".into(), CompactionOrigin::Automatic).0,
        model: model.clone(),
        record: inner.record.clone(),
        history: Arc::clone(&inner.history),
        workspace: Arc::clone(&inner.workspace),
        tool_schemas: vec![],
        previous_summary: None,
        previous_covered_item_count: 0,
        hard_tokens: model.descriptor.context_window,
        automatic_budget: Some(Arc::new(crate::models::tests::DefaultProviderBudget)),
        trigger_tokens: Some(trigger),
        history_triggered: false,
        fit_limits: None,
        deadline: Instant::now() + std::time::Duration::from_secs(5),
    }
}

#[tokio::test]
async fn below_trigger_does_not_scan_history_anchors() {
    let (temp, agent, session, model) = fixture("summary", 100_000).await;
    append_text(&session, 100).await;
    std::fs::write(
        temp.path()
            .join("data/sessions")
            .join(session.session_id().to_string())
            .join("history.jsonl"),
        "not history\n",
    )
    .unwrap();
    let result = run_compaction_inner(&session, &reservation(&session, &model, 95_000)).await;
    assert_eq!(result.status, CompactionStatus::Noop);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn post_turn_retries_only_when_a_new_complete_prefix_is_available() {
    let (_temp, agent, session, model) = fixture(&"s".repeat(8_000), 4_000).await;
    append_text(&session, 6_000).await;
    append_text(&session, 4_000).await;
    let first = run_compaction_inner(&session, &reservation(&session, &model, 1_000)).await;
    assert_eq!(first.failure_kind.as_deref(), Some("no_progress"));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    append_text(&session, 100).await;
    let same = run_compaction_inner(&session, &reservation(&session, &model, 1_000)).await;
    assert_eq!(same.status, CompactionStatus::Noop);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    append_text(&session, 4_000).await;
    run_compaction_inner(&session, &reservation(&session, &model, 1_000)).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_post_turn_commit_allows_the_same_source_again() {
    let (_temp, agent, session, model) = fixture("summary", 4_000).await;
    append_text(&session, 6_000).await;
    append_text(&session, 4_000).await;
    let first = reservation(&session, &model, 1_000);
    let operation = Arc::clone(&first.operation);
    session.lock_inner().compaction = Some(Arc::clone(&operation));
    let gate = Arc::new(Gate::new());
    crate::store::SUMMARY_COMMIT_GATES.arm(session.session_id(), Arc::clone(&gate));
    let worker = tokio::spawn(run_compaction(session.clone(), first));
    gate.wait_started().await;
    assert!(operation.cancel());
    gate.release();
    worker.await.unwrap();
    assert_eq!(
        operation
            .result
            .borrow()
            .as_ref()
            .unwrap()
            .failure_kind
            .as_deref(),
        Some("cancelled")
    );
    assert!(session.shared.compaction.post_turn_source().is_none());
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let retry = reservation(&session, &model, 1_000);
    session.lock_inner().compaction = Some(Arc::clone(&retry.operation));
    run_compaction(session.clone(), retry).await;
    assert_eq!(
        session.context().last_result.unwrap().status,
        CompactionStatus::Compacted
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
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
                usage: Default::default(),
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
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while session.context().current_operation.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        session.shared.compaction.coverage().unwrap().1,
        session.lock_inner().history.len()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    agent.shutdown().await.unwrap();
}
#[test]
fn origin_is_owned_by_operation_not_inferred_from_identifier() {
    for (id, origin) in [
        ("auto-looking-manual", CompactionOrigin::Manual),
        ("opaque-id", CompactionOrigin::Automatic),
    ] {
        let (operation, _) = CompactionOperation::new(id.into(), origin);
        assert_eq!(operation.failed_result("cancelled", None).origin, origin);
        assert_eq!(operation.unknown_write_result().origin, origin);
        assert_eq!(failed_compaction(&operation, "timeout", 0).origin, origin);
    }
}
