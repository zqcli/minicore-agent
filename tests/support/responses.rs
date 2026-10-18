use serde_json::{Value, json};

use super::openai_mock::MockResponse;

pub fn done() -> Value {
    json!({"type":"response.completed", "response":{"status":"completed"}})
}

pub fn completed() -> Value {
    let mut event = done();
    event["response"]["usage"] = json!({
        "input_tokens": 5,
        "output_tokens": 3,
        "total_tokens": 8,
        "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
        "output_tokens_details": {"reasoning_tokens": 0}
    });
    event
}

pub fn text_response(text: impl Into<String>) -> MockResponse {
    MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": text.into()}),
        completed(),
    ])
}

pub fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "loop_id": turn["loop_id"],
    })
}
