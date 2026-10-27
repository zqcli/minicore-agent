use super::*;
use crate::event::ModelErrorView;

fn provider(error: &ModelError) -> Option<ProviderErrorView> {
    ModelErrorView::from_model(error).provider
}

fn view(
    status: Option<u16>,
    code: Option<&str>,
    error_type: Option<&str>,
    message: Option<&str>,
) -> Option<ProviderErrorView> {
    ProviderErrorView::new(status, code, error_type, message)
}

async fn http_error(response: MockResponse) -> ModelError {
    let server = MockServer::spawn([response]).await;
    let error = start_error(
        &model(server.base_url()),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    server.finish().await;
    error
}

async fn stream_error_for(frame: Value) -> ModelError {
    let server = MockServer::spawn([MockResponse::sse(&[
        json!({"type":"response.created"}),
        frame,
    ])])
    .await;
    let (_, error) = run_until_error(
        &model(server.base_url()),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    server.finish().await;
    error
}

#[tokio::test]
async fn an_openai_rate_limit_envelope_is_exposed_with_its_status() {
    let error = http_error(MockResponse::json(
        429,
        json!({"error": {
            "message": "Rate limit reached for gpt-test in organization org-1",
            "type": "requests",
            "code": "rate_limit_exceeded",
            "private": "NOT-LISTED"
        }})
        .to_string(),
    ))
    .await;

    assert_error(
        &error,
        ModelErrorKind::RateLimited,
        DeliveryState::NotStarted,
        true,
    );
    let wire = serde_json::to_value(ModelErrorView::from_model(&error)).unwrap();
    assert_eq!(
        wire["provider"],
        json!({
            "status": 429,
            "code": "rate_limit_exceeded",
            "type": "requests",
            "message": "Rate limit reached for gpt-test in organization org-1"
        })
    );
    assert!(!wire.to_string().contains("NOT-LISTED"));
}

#[tokio::test]
async fn an_anthropic_style_envelope_is_exposed_without_a_code() {
    let error = http_error(MockResponse::json(
        529,
        r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
    ))
    .await;

    assert_eq!(error.kind(), ModelErrorKind::ProviderUnavailable);
    assert_eq!(
        provider(&error),
        view(
            Some(529),
            None,
            Some("overloaded_error"),
            Some("Overloaded")
        )
    );
}

#[tokio::test]
async fn a_plain_text_or_html_error_body_becomes_the_message() {
    for (status, body, message) in [
        (502, "  upstream connect error\n", "upstream connect error"),
        (
            504,
            "<html><body><h1>504 Gateway Time-out</h1></body></html>",
            "<html><body><h1>504 Gateway Time-out</h1></body></html>",
        ),
        // An envelope without a message still shows what the relay sent.
        (
            500,
            r#"{"error":{"code":"x"}}"#,
            r#"{"error":{"code":"x"}}"#,
        ),
    ] {
        let error = http_error(MockResponse::json(status, body)).await;
        let provider = provider(&error).unwrap();
        assert_eq!(provider.status, Some(status));
        assert_eq!(provider.message.as_deref(), Some(message));
    }
}

#[tokio::test]
async fn an_empty_error_body_exposes_only_the_status() {
    let error = http_error(MockResponse::json(500, b"".to_vec())).await;

    assert_eq!(provider(&error), view(Some(500), None, None, None));
}

#[tokio::test]
async fn a_quota_rejection_stays_quota_exceeded_and_keeps_the_provider_text() {
    let error = http_error(MockResponse::json(
        429,
        r#"{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}"#,
    ))
    .await;

    assert_error(
        &error,
        ModelErrorKind::QuotaExceeded,
        DeliveryState::NotStarted,
        false,
    );
    assert_eq!(
        provider(&error),
        view(
            Some(429),
            Some("insufficient_quota"),
            Some("insufficient_quota"),
            Some("You exceeded your current quota")
        )
    );
}

#[tokio::test]
async fn stream_failure_events_expose_code_and_message_without_a_status() {
    let failed = stream_error_for(json!({"type":"response.failed","response":{"error":{
        "code":"rate_limit_exceeded","type":"requests","message":"slow down"
    }}}))
    .await;
    assert_eq!(failed.kind(), ModelErrorKind::RateLimited);
    assert_eq!(
        provider(&failed),
        view(
            None,
            Some("rate_limit_exceeded"),
            Some("requests"),
            Some("slow down")
        )
    );

    let flat =
        stream_error_for(json!({"type":"error","code":"server_error","message":"boom"})).await;
    assert_eq!(
        provider(&flat),
        view(None, Some("server_error"), None, Some("boom"))
    );

    let nested = stream_error_for(json!({"type":"error","error":{
        "type":"api_error","message":"inner"
    }}))
    .await;
    assert_eq!(
        provider(&nested),
        view(None, None, Some("api_error"), Some("inner"))
    );
}

#[tokio::test]
async fn a_stream_failure_without_text_exposes_no_provider_object() {
    let error = stream_error_for(json!({"type":"response.failed","response":{}})).await;

    assert_eq!(error.kind(), ModelErrorKind::ProviderUnavailable);
    assert_eq!(provider(&error), None);
}

#[tokio::test]
async fn a_transport_failure_exposes_no_provider_object() {
    let error = start_error(
        &model("http://127.0.0.1:0"),
        basic_request(ReasoningPreference::Auto),
        context(CancellationToken::new(), Duration::from_secs(5)),
    )
    .await;
    assert_eq!(error.kind(), ModelErrorKind::TransportUnavailable);
    assert_eq!(provider(&error), None);
}
