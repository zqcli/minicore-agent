use super::*;

#[test]
fn tool_result_debug_redacts_display_content() {
    let view = ToolResultView {
        outcome: ToolResultOutcome::Success,
        content_bytes: 42,
        content: Some("secret command output".to_owned()),
        content_truncated: true,
    };

    let debug = format!("{view:?}");
    assert!(!debug.contains("secret command output"));
    assert!(debug.contains("content_len"));
    assert!(debug.contains("content_truncated"));
}

#[test]
fn tool_progress_debug_redacts_message() {
    let view = ToolProgressView {
        message: Some("secret progress path".to_owned()),
        completed: Some(1),
        total: Some(2),
    };
    let debug = format!("{view:?}");
    assert!(!debug.contains("secret progress path"));
    assert!(debug.contains("message_len"));
}

use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
use minicore_runtime::model::{DeliveryState, ModelErrorKind};
use minicore_runtime::value::BoundedText;

fn error(kind: ModelErrorKind, delivery: DeliveryState, text: &str) -> ModelError {
    ModelError::permanent(
        kind,
        delivery,
        DiagnosticSummary::new(
            DiagnosticCode::InvalidConfiguration,
            DiagnosticCategory::Model,
            BoundedText::new(text).unwrap(),
            false,
        ),
    )
}
const VALID: &str = "local serialized context estimate exceeds input budget: estimated_tokens=32001, input_budget_tokens=32000; use /compact or a larger model";

#[test]
fn only_exact_local_numeric_preflight_is_exposed_and_persisted() {
    let error = error(
        ModelErrorKind::InvalidRequest,
        DeliveryState::NotStarted,
        VALID,
    );
    let view = ModelErrorView::from_model(&error);
    assert_eq!(
        view.local_context_budget,
        Some(LocalContextBudgetFailure {
            estimated_tokens: 32001,
            input_budget_tokens: 32000
        })
    );
    let stored = ModelErrorView::from_model(&error);
    let encoded = serde_json::to_vec(&stored).unwrap();
    let restored: ModelErrorView = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored, view);
    assert!(
        !String::from_utf8(encoded)
            .unwrap()
            .contains("serialized context estimate")
    );
}
#[test]
fn provider_diagnostics_and_lookalike_numbers_are_not_exposed() {
    for text in [
        "provider private body",
        "local serialized context estimate exceeds input budget; use /compact or a larger model",
        &VALID.replace("32001", "032001"),
        &VALID.replace("32001", "+32001"),
        &VALID.replace("32001", "32000"),
        &VALID.replace("32001", "18446744073709551616"),
        &format!("{VALID} secret"),
    ] {
        assert!(
            LocalContextBudgetFailure::from_model(&error(
                ModelErrorKind::InvalidRequest,
                DeliveryState::NotStarted,
                text
            ))
            .is_none()
        );
    }
    for delivery in [DeliveryState::Started, DeliveryState::Unknown] {
        assert!(
            LocalContextBudgetFailure::from_model(&error(
                ModelErrorKind::InvalidRequest,
                delivery,
                VALID
            ))
            .is_none()
        );
    }
    assert!(
        LocalContextBudgetFailure::from_model(&error(
            ModelErrorKind::ContextOverflow,
            DeliveryState::NotStarted,
            VALID
        ))
        .is_none()
    );
}
#[test]
fn legacy_and_nullable_stored_errors_keep_optional_field_absent() {
    for suffix in ["", ",\"local_context_budget\":null"] {
        let old = format!(
            "{{\"kind\":\"invalid_request\",\"delivery\":\"not_started\",\"retryable\":false,\"retry_after_millis\":null{suffix}}}"
        );
        let stored: ModelErrorView = serde_json::from_str(&old).unwrap();
        assert!(stored.local_context_budget.is_none());
        assert!(
            serde_json::to_value(stored)
                .unwrap()
                .get("local_context_budget")
                .is_none()
        );
    }
}
