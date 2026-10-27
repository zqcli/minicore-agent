use super::*;
use crate::event::{LoopOutcomeView, ProviderErrorView, TurnResultView};
use crate::read::{ReadView, TurnResultRequest};
use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{DeliveryState, ModelError, ModelErrorKind};

fn provider_diagnostic(message: &str, retryable: bool) -> DiagnosticSummary {
    let provider = ProviderErrorView::new(
        Some(429),
        Some("rate_limit_exceeded"),
        Some("requests"),
        Some(message),
    )
    .unwrap();
    DiagnosticSummary::new(
        DiagnosticCode::ModelUnavailable,
        DiagnosticCategory::Model,
        BoundedText::new(provider.to_diagnostic_text()).unwrap(),
        retryable,
    )
    .unwrap()
}

fn rate_limited(message: &str) -> ModelError {
    ModelError::not_started(
        ModelErrorKind::RateLimited,
        None,
        provider_diagnostic(message, true),
    )
}

fn context_overflow(message: &str) -> ModelError {
    ModelError::permanent(
        ModelErrorKind::ContextOverflow,
        DeliveryState::NotStarted,
        provider_diagnostic(message, false),
    )
}

fn provider_of(outcome: &LoopOutcomeView) -> &ProviderErrorView {
    match outcome {
        LoopOutcomeView::Failed {
            model_error: Some(error),
            ..
        } => error.provider.as_ref().expect("provider text is kept"),
        other => panic!("expected a model failure, got {other:?}"),
    }
}

fn expected(message: &str) -> ProviderErrorView {
    ProviderErrorView {
        status: Some(429),
        code: Some("rate_limit_exceeded".to_owned()),
        error_type: Some("requests".to_owned()),
        message: Some(message.to_owned()),
    }
}

#[tokio::test]
async fn the_last_attempts_provider_text_reaches_wait_event_and_reloaded_history() {
    let fixture = AgentFixture::new("provider-error").script([
        Step::Fail(rate_limited("first attempt")),
        Step::Fail(rate_limited("last attempt")),
    ]);
    let mut agent = fixture.open().await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &fixture.workspace).await;
    let session_id = info.session_id;

    let turn = send_text(&mut agent, session_id, "hello").await;
    let result = agent.wait_turn(turn).await.unwrap();
    let expected = expected("last attempt");
    let waited = TurnResultView::from_turn_result(&result).outcome;
    assert_eq!(provider_of(&waited), &expected);

    let finished = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        if let AgentEvent::TurnFinished { outcome, .. } = event {
            break outcome;
        }
    };
    assert_eq!(provider_of(&finished), &expected);

    agent.close_session(session_id).await.unwrap();
    drop(agent);
    let mut agent = fixture.open().await;
    agent.open_session(session_id).await.unwrap();
    let stored = agent
        .turn_result(TurnResultRequest {
            turn,
            cursor: None,
            limit: 100,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(stored.availability, crate::TurnResultAvailability::Stored);
    assert_eq!(provider_of(&stored.outcome.unwrap()), &expected);
    let read = agent
        .read_session(ReadSession {
            view: ReadView::Canonical,
            ..display_read(session_id)
        })
        .await
        .unwrap();
    assert_eq!(provider_of(&read.records[0].outcome), &expected);
}

#[tokio::test]
async fn a_failed_overflow_recovery_reports_the_second_providers_text() {
    let fixture = AgentFixture::new("provider-error-recovery")
        .file("note.txt", "recovery note ".repeat(400).as_bytes())
        .models(BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::with_window(
                "main",
                16_384,
                [
                    Step::ToolCall("read", json!({"path": "note.txt"})),
                    Step::Fail(context_overflow("first overflow")),
                    Step::Text("Summary of prior read exchange"),
                    Step::Fail(context_overflow("second overflow")),
                ],
            )
            .with_pacing(std::time::Duration::from_millis(2)),
        )]))
        .auto();
    let (mut agent, info) = fixture.start().await;

    let turn = send_text(&mut agent, info.session_id, "read note").await;
    let result = agent.wait_turn(turn).await.unwrap();

    let outcome = TurnResultView::from_turn_result(&result).outcome;
    assert_eq!(provider_of(&outcome), &expected("second overflow"));
}
