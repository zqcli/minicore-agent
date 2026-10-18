use super::*;

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: Option<ProviderErrorCode>,
}

#[derive(Deserialize)]
struct ProviderErrorCode {
    code: Option<String>,
    #[serde(rename = "type")]
    error_type: Option<String>,
}

pub(super) async fn classify_http_error(
    response: reqwest::Response,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
    received_monotonic: Instant,
    received_system: SystemTime,
) -> ModelError {
    let status = response.status();
    let retry_target = retry_target(response.headers(), received_monotonic, received_system);
    let body = match collect_error_body(response, cancellation, deadline).await {
        Ok(body) => body,
        Err(error)
            if matches!(status.as_u16(), 503 | 529)
                && error.kind() == ModelErrorKind::RequestOutcomeUnknown =>
        {
            Vec::new()
        }
        Err(error) => return error,
    };
    let error = serde_json::from_slice::<ErrorEnvelope>(&body)
        .ok()
        .and_then(|envelope| envelope.error);
    let code = error.as_ref().and_then(|error| error.code.as_deref());
    let error_type = error.as_ref().and_then(|error| error.error_type.as_deref());
    let context_overflow = [code, error_type].into_iter().flatten().any(|value| {
        matches!(
            value,
            "context_length_exceeded" | "context_window_exceeded" | "context_overflow"
        )
    });
    let quota_exceeded = [code, error_type].into_iter().flatten().any(is_quota_code);

    match status.as_u16() {
        400 | 422 if context_overflow => local_error(ModelErrorKind::ContextOverflow),
        400 | 422 => local_error(ModelErrorKind::InvalidRequest),
        401 | 403 => local_error(ModelErrorKind::AuthRejected),
        408 => unknown_error(ModelErrorKind::Timeout),
        413 => local_error(ModelErrorKind::InvalidRequest),
        429 if quota_exceeded => local_error(ModelErrorKind::QuotaExceeded),
        429 => retryable_error(
            ModelErrorKind::RateLimited,
            retry_target.and_then(|target| target.remaining(Instant::now(), SystemTime::now())),
        ),
        503 | 529 => retryable_error(
            ModelErrorKind::ProviderUnavailable,
            retry_target.and_then(|target| target.remaining(Instant::now(), SystemTime::now())),
        ),
        400..=499 => local_error(ModelErrorKind::InvalidRequest),
        _ => unknown_error(ModelErrorKind::ProviderUnavailable),
    }
}

pub(super) fn http_status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() {
        400..=499 => "client_error",
        500..=599 => "server_error",
        _ => "other",
    }
}

pub(super) fn model_error_retryable(error: &ModelError) -> bool {
    matches!(error.retry_hint(), RetryHint::Retryable { .. })
}

pub(super) fn log_provider_request_failure(
    trace: RequestTraceContext,
    error: &ModelError,
    status_class: &'static str,
) {
    tracing::warn!(
        loop_id = %trace.loop_id,
        request_index = trace.request_index,
        error_kind = ?error.kind(),
        delivery = ?error.delivery(),
        retryable = model_error_retryable(error),
        status_class = status_class,
        "provider request failed"
    );
}

fn is_quota_code(value: &str) -> bool {
    matches!(
        value,
        "insufficient_quota"
            | "quota_exceeded"
            | "credit_balance_exhausted"
            | "billing_hard_limit_reached"
            | "billing_hard_limit_exceeded"
            | "billing_limit_reached"
            | "usage_limit_reached"
            | "usage_limit_exceeded"
            | "organization_quota_exceeded"
            | "project_quota_exceeded"
            | "organization_usage_limit_reached"
            | "organization_usage_limit_exceeded"
            | "project_usage_limit_reached"
            | "project_usage_limit_exceeded"
            | "organization_limit_reached"
            | "organization_limit_exceeded"
            | "project_limit_reached"
            | "project_limit_exceeded"
            | "budget_exceeded"
            | "organization_budget_exceeded"
            | "project_budget_exceeded"
            | "spend_limit_reached"
            | "spend_limit_exceeded"
            | "organization_spend_limit_reached"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_reached"
            | "project_spend_limit_exceeded"
    )
}

#[derive(Clone, Copy)]
pub(super) enum RetryTarget {
    Monotonic {
        received_at: Instant,
        retry_at: Instant,
    },
    System {
        received_at: SystemTime,
        retry_at: SystemTime,
    },
}

impl RetryTarget {
    pub(super) fn remaining(
        self,
        current_monotonic: Instant,
        current_system: SystemTime,
    ) -> Option<Duration> {
        let remaining = match self {
            Self::Monotonic {
                received_at,
                retry_at,
            } => {
                current_monotonic.checked_duration_since(received_at)?;
                retry_at.checked_duration_since(current_monotonic)?
            }
            Self::System {
                received_at,
                retry_at,
            } => {
                current_system.duration_since(received_at).ok()?;
                retry_at.duration_since(current_system).ok()?
            }
        };
        (!remaining.is_zero()).then_some(remaining)
    }
}

pub(super) fn retry_target(
    headers: &reqwest::header::HeaderMap,
    received_monotonic: Instant,
    received_system: SystemTime,
) -> Option<RetryTarget> {
    let monotonic_target = |duration: Duration| {
        Some(RetryTarget::Monotonic {
            received_at: received_monotonic,
            retry_at: received_monotonic.checked_add(duration)?,
        })
    };
    headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| positive_duration(value, 1_000.0))
        .and_then(monotonic_target)
        .or_else(|| {
            let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
            if let Some(duration) = positive_duration(value, 1.0) {
                return monotonic_target(duration);
            }
            Some(RetryTarget::System {
                received_at: received_system,
                retry_at: httpdate::parse_http_date(value).ok()?,
            })
        })
}

fn positive_duration(value: &str, divisor: f64) -> Option<Duration> {
    let seconds = value.trim().parse::<f64>().ok()? / divisor;
    Duration::try_from_secs_f64(seconds)
        .ok()
        .filter(|duration| !duration.is_zero())
}

async fn collect_error_body(
    response: reqwest::Response,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: TokioInstant,
) -> Result<Vec<u8>, ModelError> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while body.len() < MAX_ERROR_BODY_BYTES {
        let chunk = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(unknown_error(ModelErrorKind::Cancelled));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(unknown_error(ModelErrorKind::Timeout));
            }
            chunk = stream.next() => chunk,
        };
        match chunk {
            Some(Ok(chunk)) => {
                let remaining = MAX_ERROR_BODY_BYTES - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    break;
                }
            }
            Some(Err(error)) if error.is_timeout() => {
                return Err(unknown_error(ModelErrorKind::Timeout));
            }
            Some(Err(_)) => return Err(unknown_error(ModelErrorKind::RequestOutcomeUnknown)),
            None => break,
        }
    }
    Ok(body)
}

pub(super) fn classify_send_error(error: reqwest::Error) -> ModelError {
    if error.is_builder() {
        local_error(ModelErrorKind::InvalidRequest)
    } else if error.is_connect() {
        retryable_error(ModelErrorKind::TransportUnavailable, None)
    } else if error.is_timeout() {
        unknown_error(ModelErrorKind::Timeout)
    } else {
        unknown_error(ModelErrorKind::RequestOutcomeUnknown)
    }
}
