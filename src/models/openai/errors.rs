use super::*;

pub(super) fn local_error(kind: ModelErrorKind) -> ModelError {
    ModelError::permanent(kind, DeliveryState::NotStarted, diagnostic(kind, false))
}

pub(super) fn unknown_error(kind: ModelErrorKind) -> ModelError {
    ModelError::unknown(kind, diagnostic(kind, false))
}

pub(super) fn started_error(kind: ModelErrorKind) -> ModelError {
    ModelError::started(kind, diagnostic(kind, false))
}

pub(super) fn retryable_error(kind: ModelErrorKind, retry_after: Option<Duration>) -> ModelError {
    ModelError::not_started(kind, retry_after, diagnostic(kind, true))
}

pub(super) fn stream_error(kind: ModelErrorKind, provider_event_seen: bool) -> ModelError {
    if provider_event_seen {
        started_error(kind)
    } else {
        unknown_error(kind)
    }
}

/// Keeps the classification of `error` and carries the provider's text in its diagnostic.
pub(super) fn with_provider(error: ModelError, provider: Option<ProviderErrorView>) -> ModelError {
    let Some(provider) = provider else {
        return error;
    };
    let base = error.diagnostic();
    let diagnostic = DiagnosticSummary::new(
        base.code,
        base.category,
        BoundedText::new(provider.to_diagnostic_text()).expect("provider text is bounded"),
        base.retryable,
    )
    .expect("provider text has no control characters");
    match *error.retry_hint() {
        RetryHint::Retryable { retry_after } => {
            ModelError::not_started(error.kind(), retry_after, diagnostic)
        }
        RetryHint::Never => ModelError::permanent(error.kind(), error.delivery(), diagnostic),
    }
}

fn diagnostic(kind: ModelErrorKind, retryable: bool) -> DiagnosticSummary {
    let (code, message) = match kind {
        ModelErrorKind::Cancelled => (DiagnosticCode::RuntimeTerminated, "model request cancelled"),
        ModelErrorKind::Timeout => (DiagnosticCode::ModelTimeout, "model request timed out"),
        ModelErrorKind::InvalidRequest | ModelErrorKind::ContextOverflow => (
            DiagnosticCode::InvalidConfiguration,
            "model request is invalid",
        ),
        ModelErrorKind::InvalidProviderResponse
        | ModelErrorKind::IncompleteResponse
        | ModelErrorKind::StreamInterrupted
        | ModelErrorKind::RequestOutcomeUnknown
        | ModelErrorKind::UnexpectedToolCall => (
            DiagnosticCode::ModelMalformedResponse,
            "model provider response is unavailable",
        ),
        ModelErrorKind::AuthMissing
        | ModelErrorKind::AuthRejected
        | ModelErrorKind::RateLimited
        | ModelErrorKind::QuotaExceeded
        | ModelErrorKind::TransportUnavailable
        | ModelErrorKind::ProviderUnavailable
        | ModelErrorKind::Unavailable => (
            DiagnosticCode::ModelUnavailable,
            "model provider is unavailable",
        ),
        ModelErrorKind::Panicked | ModelErrorKind::Internal => {
            (DiagnosticCode::Internal, "model operation failed")
        }
    };
    DiagnosticSummary::new(
        code,
        DiagnosticCategory::Model,
        BoundedText::new(message).expect("static model diagnostic must fit"),
        retryable,
    )
    .expect("valid diagnostic text")
}
