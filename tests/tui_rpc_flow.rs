use std::path::{Path, PathBuf};

use minicore_agent::SessionId;
use serde_json::{Value, json};

#[path = "support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{ChunkGate, MockResponse, MockServer};
#[path = "support/rpc_process.rs"]
mod rpc_process;
use rpc_process::RpcProcess;

const KEY_ENV: &str = "MINICORE_TUI_FLOW_KEY";
const KEY: &str = "TUI-FLOW-API-KEY-SECRET";
const FAST_PROVIDER: &str = "tui-fast-provider-private";
const DEEP_PROVIDER: &str = "tui-deep-provider-private";

fn test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "minicore-agent-tui-{label}-{}",
        SessionId::new().unwrap()
    ))
}

fn write_config(
    base: &Path,
    base_url: &str,
    event_capacity: usize,
    tools: &[&str],
    coding_model: &str,
    coding_reasoning: &str,
) -> PathBuf {
    std::fs::create_dir_all(base).unwrap();
    let path = base.join("agent.toml");
    let tools = tools
        .iter()
        .map(|tool| format!("\"{tool}\""))
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        &path,
        format!(
            r#"data_dir = {:?}
event_capacity = {event_capacity}
default_profile = "coding"

[profiles.quick]
model = "fast"
reasoning = "low"
system_prompt = "Fast TUI flow profile."
tools = [{tools}]
max_tool_rounds = 4
approval = "auto"

[profiles.coding]
model = "{coding_model}"
reasoning = "{coding_reasoning}"
system_prompt = "Coding TUI flow profile."
tools = [{tools}]
max_tool_rounds = 4
approval = "auto"

[models.fast]
provider = "open_ai_responses"
model = "{FAST_PROVIDER}"
base_url = "{base_url}"
api_key_env = "{KEY_ENV}"
physical_context_window = 16000
output_budget_tokens = 1024
safety_margin_tokens = 1000
supported_reasoning = ["auto", "disabled", "low"]
supports_tools = true
request_timeout_seconds = 5

[models.deep]
provider = "open_ai_responses"
model = "{DEEP_PROVIDER}"
base_url = "{base_url}"
api_key_env = "{KEY_ENV}"
physical_context_window = 32000
output_budget_tokens = 2048
safety_margin_tokens = 1000
supported_reasoning = ["auto", "low", "medium", "high"]
supports_tools = true
request_timeout_seconds = 5
"#,
            base.join("data")
        ),
    )
    .unwrap();
    path
}

fn completed() -> Value {
    json!({
        "type": "response.completed",
        "response": {
            "status": "completed",
            "usage": {
                "input_tokens": 5,
                "output_tokens": 3,
                "total_tokens": 8,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0}
            }
        }
    })
}

fn text_response(text: &str) -> MockResponse {
    MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": text}),
        completed(),
    ])
}

fn tool_response(call_id: &str, name: &str, arguments: Value) -> MockResponse {
    MockResponse::sse(&[
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "call_id": call_id,
                "name": name,
                "arguments": serde_json::to_string(&arguments).unwrap()
            }
        }),
        completed(),
    ])
}

fn gated_sse(events: Vec<Value>) -> (MockResponse, ChunkGate) {
    MockResponse::sse(&events).with_chunk_gate()
}

fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "loop_id": turn["loop_id"],
    })
}

async fn create_session(
    process: &mut RpcProcess,
    id: &str,
    workspace: &Path,
    model: &str,
    reasoning: &str,
) -> Value {
    process
        .send(
            id,
            "session.create",
            json!({
                "workspace": workspace,
                "profile": "coding",
                "model": model,
                "reasoning": reasoning,
            }),
        )
        .await;
    process.response(id).await["result"]["session"].clone()
}

async fn history(process: &mut RpcProcess, id: &str, session_id: &Value) -> Value {
    process
        .send(
            id,
            "session.history",
            json!({"session_id": session_id, "offset": 0, "limit": 100}),
        )
        .await;
    process.response(id).await["result"].clone()
}

async fn state(process: &mut RpcProcess, id: &str, session_id: &Value) -> Value {
    process
        .send(id, "session.state", json!({"session_id": session_id}))
        .await;
    process.response(id).await["result"].clone()
}

fn event_for_turn(frame: &Value, turn: &Value) -> bool {
    frame.pointer("/params/data/turn/session_id") == Some(&turn["session_id"])
        && frame.pointer("/params/data/turn/loop_id") == Some(&turn["loop_id"])
}

fn assert_error(response: &Value, code: i64, kind: &str) {
    assert_eq!(response["error"]["code"], code);
    assert_eq!(response["error"]["data"]["kind"], kind);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_discovery_create_history_and_reopen_flow() {
    let server = MockServer::spawn([text_response("first"), text_response("second")]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("discovery");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, &base_url, 64, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    process.send("ping", "agent.ping", json!({})).await;
    assert_eq!(
        process.response("ping").await["result"]["version"],
        env!("CARGO_PKG_VERSION")
    );
    process.send("model.list", "model.list", json!({})).await;
    let models = process.response("model.list").await;
    let model_ids = models["result"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(model_ids.contains(&"fast") && model_ids.contains(&"deep"));
    process
        .send("profile.list", "profile.list", json!({}))
        .await;
    let profiles = process.response("profile.list").await;
    let profile_ids = profiles["result"]["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|profile| profile["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(profile_ids.contains(&"coding") && profile_ids.contains(&"quick"));
    process
        .send("session.list", "session.list", json!({}))
        .await;
    assert_eq!(
        process.response("session.list").await["result"]["sessions"],
        json!([])
    );

    let session = create_session(&mut process, "create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();
    assert_eq!(session["model"], "deep");
    assert_eq!(session["reasoning"], "high");

    let initial = history(&mut process, "history", &session_id).await;
    assert_eq!(initial["total"], 0);

    // First turn: send, wait deferred, stream, then inspect durable history.
    let (first_turn, first_wait) = {
        process
            .send(
                "send1",
                "turn.send",
                json!({"session_id": session_id, "text": "first prompt"}),
            )
            .await;
        let turn = process.response("send1").await["result"]["turn"].clone();
        process.send("wait1", "turn.wait", turn_params(&turn)).await;
        (turn, "wait1")
    };
    let started = process.event("turn_started").await;
    assert!(event_for_turn(&started, &first_turn));
    let waited = process.response(first_wait).await;
    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    assert_eq!(waited["result"]["persistence"], "persisted");

    let page = history(&mut process, "history2", &session_id).await;
    assert_eq!(page["total"], 2);
    assert!(
        page["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["item"]["type"] == "user")
    );

    // Second turn on the same session: history carries the first turn forward.
    process
        .send(
            "send2",
            "turn.send",
            json!({"session_id": session_id, "text": "second prompt"}),
        )
        .await;
    let second_turn = process.response("send2").await["result"]["turn"].clone();
    assert_ne!(second_turn["loop_id"], first_turn["loop_id"]);
    process
        .send("wait2", "turn.wait", turn_params(&second_turn))
        .await;
    let waited = process.response("wait2").await;
    assert_eq!(waited["result"]["persistence"], "persisted");
    let page = history(&mut process, "history3", &session_id).await;
    assert_eq!(page["total"], 4);

    // Close and reopen: history loads from the JSONL.
    process
        .send("close", "session.close", json!({"session_id": session_id}))
        .await;
    assert_eq!(process.response("close").await["result"]["ok"], true);
    process
        .send("open", "session.open", json!({"session_id": session_id}))
        .await;
    let reopened = process.response("open").await;
    assert_eq!(reopened["result"]["session"]["loaded"], true);
    let page = history(&mut process, "history4", &session_id).await;
    assert_eq!(page["total"], 4);

    let (observed, stderr) = process.shutdown().await;
    assert!(!serde_json::to_string(&observed).unwrap().contains(KEY));
    assert!(!stderr.contains(KEY));
    assert!(!stderr.contains(FAST_PROVIDER));
    assert!(!stderr.contains(DEEP_PROVIDER));
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_steer_update_cancel_while_wait_is_pending() {
    let gate_events = vec![
        json!({"type": "response.output_text.delta", "delta": "steered"}),
        completed(),
    ];
    let (gated, gate) = gated_sse(gate_events);
    let server = MockServer::spawn([gated, text_response("after cancel")]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("control");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, &base_url, 256, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session = create_session(&mut process, "create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "do the thing"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    process.send("wait", "turn.wait", turn_params(&turn)).await;

    // The loop is running; the reader stays responsive.
    process.send("ping", "agent.ping", json!({})).await;
    process.response("ping").await;

    // Steer while wait is pending.
    process
        .send(
            "steer",
            "turn.steer",
            json!({
                "session_id": session_id,
                "loop_id": turn["loop_id"],
                "text": "do not touch config files"
            }),
        )
        .await;
    assert_eq!(process.response("steer").await["result"]["ok"], true);

    // Session model/reasoning update while wait is pending.
    process
        .send(
            "update",
            "session.update",
            json!({"session_id": session_id, "reasoning": "low"}),
        )
        .await;
    let updated = process.response("update").await;
    assert!(updated["result"]["active_revision"].is_number());
    assert_eq!(updated["result"]["session"]["reasoning"], "low");

    let running = state(&mut process, "state", &session_id).await;
    assert_eq!(running["status"], "running");

    // Cancel while wait is pending.
    process
        .send("cancel", "turn.cancel", turn_params(&turn))
        .await;
    assert_eq!(
        process.response("cancel").await["result"]["cancelled"],
        true
    );
    let waited = process.response("wait").await;
    assert_eq!(waited["result"]["outcome"]["type"], "cancelled");
    assert_eq!(waited["result"]["persistence"], "persisted");
    gate.release();

    // A further turn succeeds after the cancel.
    process
        .send(
            "send2",
            "turn.send",
            json!({"session_id": session_id, "text": "next task"}),
        )
        .await;
    let turn2 = process.response("send2").await["result"]["turn"].clone();
    process
        .send("wait2", "turn.wait", turn_params(&turn2))
        .await;
    let waited = process.response("wait2").await;
    assert_eq!(waited["result"]["persistence"], "persisted");

    let (observed, stderr) = process.shutdown().await;
    assert!(!serde_json::to_string(&observed).unwrap().contains(KEY));
    assert!(!stderr.contains(KEY));
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_streams_reasoning_and_text_channels() {
    let reasoning = MockResponse::sse(&[
        json!({"type": "response.reasoning_text.delta", "delta": "think carefully"}),
        json!({"type": "response.output_text.delta", "delta": "final answer text"}),
        completed(),
    ]);
    let server = MockServer::spawn([reasoning]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("stream");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, &base_url, 256, &[], "deep", "medium");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session = create_session(&mut process, "create", &workspace, "deep", "medium").await;
    let session_id = session["session_id"].clone();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "reason out loud"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    process.send("wait", "turn.wait", turn_params(&turn)).await;

    // Streaming deltas are best-effort; the exact text is asserted via the
    // authoritative turn result and history.
    let waited = process.response("wait").await;
    assert_eq!(waited["result"]["outcome"]["type"], "completed");
    assert_eq!(waited["result"]["persistence"], "persisted");
    if let Some(delta) = process.try_event("output_delta").await {
        let channel = delta["params"]["data"]["channel"].as_str().unwrap_or("");
        let value = delta["params"]["data"]["delta"].as_str().unwrap_or("");
        assert!(
            (channel == "reasoning" && value == "think carefully")
                || (channel == "text" && value == "final answer text"),
            "unexpected delta {channel}={value}"
        );
    }
    let finished = process.event("turn_finished").await;
    assert!(event_for_turn(&finished, &turn));
    assert_eq!(finished["params"]["data"]["persistence"], "persisted");

    let page = history(&mut process, "history", &session_id).await;
    assert!(page.to_string().contains("final answer text"));
    assert!(page.to_string().contains("reason out loud"));

    process.shutdown().await;
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_tool_loop_shows_lifecycle_and_safe_history() {
    let server = MockServer::spawn([
        tool_response(
            "tui-read-call",
            "read",
            json!({"path": "SECRET-PATH.txt", "limit": 32}),
        ),
        text_response("read finished"),
    ])
    .await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("tool");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("SECRET-PATH.txt"), "TOOL-CONTENT-TOKEN").unwrap();
    let config = write_config(&base, &base_url, 256, &["read"], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session = create_session(&mut process, "create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "read the file"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    process.send("wait", "turn.wait", turn_params(&turn)).await;
    let tool_started = process.event("tool_started").await;
    assert_eq!(tool_started["params"]["data"]["tool_name"], "read");
    let tool_finished = process.event("tool_finished").await;
    assert_eq!(
        tool_finished["params"]["data"]["result"]["outcome"],
        "success"
    );
    let waited = process.response("wait").await;
    assert_eq!(waited["result"]["outcome"]["type"], "completed");

    let page = history(&mut process, "history", &session_id).await;
    let serialized = page.to_string();
    assert!(serialized.contains("TOOL-CONTENT-TOKEN"));
    assert!(!serialized.contains("arguments"));
    // The tool argument (secret path) never leaks into the safe history view.
    assert!(!serialized.contains("SECRET-PATH.txt"));
    let tool_result_count = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["item"]["type"] == "tool_result")
        .count();
    assert_eq!(tool_result_count, 1);

    process.shutdown().await;
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_two_sessions_run_independently() {
    let server = MockServer::spawn([text_response("session-a"), text_response("session-b")]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("two-sessions");
    let workspace_a = base.join("workspace-a");
    let workspace_b = base.join("workspace-b");
    std::fs::create_dir_all(&workspace_a).unwrap();
    std::fs::create_dir_all(&workspace_b).unwrap();
    let config = write_config(&base, &base_url, 64, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session_a = create_session(&mut process, "create-a", &workspace_a, "deep", "high").await;
    let session_b = create_session(&mut process, "create-b", &workspace_b, "deep", "high").await;
    assert_ne!(session_a["session_id"], session_b["session_id"]);

    for (id, session, prompt) in [
        ("a", &session_a, "work on a"),
        ("b", &session_b, "work on b"),
    ] {
        process
            .send(
                &format!("send-{id}"),
                "turn.send",
                json!({"session_id": session["session_id"], "text": prompt}),
            )
            .await;
        let turn = process.response(&format!("send-{id}")).await["result"]["turn"].clone();
        let wait_id = format!("wait-{id}");
        process
            .send(&wait_id, "turn.wait", turn_params(&turn))
            .await;
        let waited = process.response(&wait_id).await;
        assert_eq!(waited["result"]["persistence"], "persisted");
    }

    for (id, session) in [("a", &session_a), ("b", &session_b)] {
        let page = history(
            &mut process,
            &format!("history-{id}"),
            &session["session_id"],
        )
        .await;
        assert_eq!(page["total"], 2);
    }

    process.shutdown().await;
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_error_mappings_cover_new_wire_types() {
    // No turn is run in this test: an empty script keeps the mock idle.
    let server = MockServer::spawn([]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("errors");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, &base_url, 64, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session = create_session(&mut process, "create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();

    process
        .send(
            "bad-history",
            "session.history",
            json!({"session_id": session_id, "offset": 0, "limit": 0}),
        )
        .await;
    assert_error(
        &process.response("bad-history").await,
        -32603,
        "internal_error",
    );

    process
        .send(
            "unknown-session",
            "session.state",
            json!({"session_id": SessionId::new().unwrap()}),
        )
        .await;
    assert_error(
        &process.response("unknown-session").await,
        -32002,
        "session_not_loaded",
    );

    process.shutdown().await;
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}
