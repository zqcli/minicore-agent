use std::path::{Path, PathBuf};

use minicore_agent::SessionId;
use serde_json::{Value, json};

#[path = "support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{MockResponse, MockServer};
#[path = "support/rpc_process.rs"]
mod rpc_process;
use rpc_process::RpcProcess;

const KEY_ENV: &str = "MINICORE_RPC_SOAK_KEY";
const KEY: &str = "RPC-SOAK-API-KEY-SECRET";

fn test_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "minicore-agent-rpc-soak-{}",
        SessionId::new().unwrap()
    ))
}

fn write_config(base: &Path, base_url: &str) -> PathBuf {
    let path = base.join("agent.toml");
    std::fs::write(
        &path,
        format!(
            r#"data_dir = {:?}
event_capacity = 4
default_profile = "soak"

[profiles.soak]
model = "soak"
reasoning = "high"
system_prompt = "Run the offline RPC soak."
tools = ["read", "write"]
max_tool_rounds = 4
approval = "auto"

[models.soak]
provider = "open_ai_responses"
model = "soak-provider"
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

fn text_response(text: impl Into<String>) -> MockResponse {
    MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": text.into()}),
        completed(),
    ])
}

fn read_response(call_id: &str) -> MockResponse {
    MockResponse::sse(&[
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "call_id": call_id,
                "name": "read",
                "arguments": r#"{ "path": "soak.txt", "limit": 32 }"#
            }
        }),
        completed(),
    ])
}

fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "loop_id": turn["loop_id"],
    })
}

async fn create_session(process: &mut RpcProcess, id: &str, workspace: &Path) -> Value {
    process
        .send(
            id,
            "session.create",
            json!({"workspace": workspace, "profile": "soak"}),
        )
        .await;
    process.response(id).await["result"]["session"].clone()
}

async fn history_total(process: &mut RpcProcess, id: &str, session_id: &Value) -> usize {
    process
        .send(
            id,
            "session.history",
            json!({"session_id": session_id, "offset": 0, "limit": 100}),
        )
        .await;
    process.response(id).await["result"]["total"]
        .as_u64()
        .unwrap() as usize
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_turns_on_two_sessions_persist_contiguous_histories() {
    // 20-turn RPC soak: interleave 10 turns per session; each turn runs a
    // read tool loop (20 provider responses queued ahead of time).
    let responses = (0..20)
        .flat_map(|index| {
            [
                read_response(&format!("soak-read-{index}")),
                text_response(format!("soak-final-{index}")),
            ]
        })
        .collect::<Vec<_>>();
    let server = MockServer::spawn(responses).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir();
    std::fs::create_dir_all(&base).unwrap();
    std::fs::create_dir_all(base.join("workspace-a")).unwrap();
    std::fs::create_dir_all(base.join("workspace-b")).unwrap();
    std::fs::write(base.join("workspace-a/soak.txt"), "SOAK-A-CONTENT").unwrap();
    std::fs::write(base.join("workspace-b/soak.txt"), "SOAK-B-CONTENT").unwrap();
    let config = write_config(&base, &base_url);
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session_a = create_session(&mut process, "create-a", &base.join("workspace-a")).await;
    let session_b = create_session(&mut process, "create-b", &base.join("workspace-b")).await;
    let session_a_id = session_a["session_id"].clone();
    let session_b_id = session_b["session_id"].clone();

    // Interleave 10 turns per session; each turn runs a read tool loop.
    for index in 0..10 {
        for (label, session_id) in [("a", &session_a_id), ("b", &session_b_id)] {
            let send_id = format!("send-{label}-{index}");
            let wait_id = format!("wait-{label}-{index}");
            process
                .send(
                    &send_id,
                    "turn.send",
                    json!({"session_id": session_id, "text": format!("turn {index}")}),
                )
                .await;
            let turn = process.response(&send_id).await["result"]["turn"].clone();
            process
                .send(&wait_id, "turn.wait", turn_params(&turn))
                .await;
            let waited = process.response(&wait_id).await;
            assert_eq!(waited["result"]["persistence"], "persisted");
            assert_eq!(waited["result"]["outcome"]["type"], "completed");
        }
    }

    // Each turn contributes 4 durable items (user, toolcall, tool result, final).
    assert_eq!(
        history_total(&mut process, "history-a", &session_a_id).await,
        40
    );
    assert_eq!(
        history_total(&mut process, "history-b", &session_b_id).await,
        40
    );

    // Reload from disk preserves both histories.
    for (label, session_id) in [("a", &session_a_id), ("b", &session_b_id)] {
        process
            .send(
                &format!("close-{label}"),
                "session.close",
                json!({"session_id": session_id}),
            )
            .await;
        assert_eq!(
            process.response(&format!("close-{label}")).await["result"]["ok"],
            true
        );
        process
            .send(
                &format!("open-{label}"),
                "session.open",
                json!({"session_id": session_id}),
            )
            .await;
        process.response(&format!("open-{label}")).await;
        assert_eq!(
            history_total(&mut process, &format!("history-{label}-2"), session_id).await,
            40
        );
    }

    let (observed, stderr) = process.shutdown().await;
    assert!(!serde_json::to_string(&observed).unwrap().contains(KEY));
    assert!(!stderr.contains(KEY));
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_does_not_break_subsequent_turns_or_history() {
    let (gated, gate) = MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "will cancel"}),
        completed(),
    ])
    .with_chunk_gate();
    let server = MockServer::spawn([text_response("first"), gated, text_response("third")]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir();
    std::fs::create_dir_all(&base).unwrap();
    std::fs::create_dir_all(base.join("workspace")).unwrap();
    let config = write_config(&base, &base_url);
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    let session = create_session(&mut process, "create", &base.join("workspace")).await;
    let session_id = session["session_id"].clone();

    // First turn completes.
    process
        .send(
            "send1",
            "turn.send",
            json!({"session_id": session_id, "text": "one"}),
        )
        .await;
    let turn1 = process.response("send1").await["result"]["turn"].clone();
    process
        .send("wait1", "turn.wait", turn_params(&turn1))
        .await;
    assert_eq!(
        process.response("wait1").await["result"]["persistence"],
        "persisted"
    );

    // Second turn is cancelled.
    process
        .send(
            "send2",
            "turn.send",
            json!({"session_id": session_id, "text": "two"}),
        )
        .await;
    let turn2 = process.response("send2").await["result"]["turn"].clone();
    process
        .send("wait2", "turn.wait", turn_params(&turn2))
        .await;
    // Only cancel once the second model request was fully read by the mock,
    // so the gated response is deterministically tied to this turn.
    server.wait_for_requests(2).await;
    process
        .send("cancel", "turn.cancel", turn_params(&turn2))
        .await;
    assert_eq!(
        process.response("cancel").await["result"]["cancelled"],
        true
    );
    let waited = process.response("wait2").await;
    assert_eq!(waited["result"]["outcome"]["type"], "cancelled");
    assert_eq!(waited["result"]["persistence"], "persisted");
    gate.release();

    // Third turn still succeeds on the same session.
    process
        .send(
            "send3",
            "turn.send",
            json!({"session_id": session_id, "text": "three"}),
        )
        .await;
    let turn3 = process.response("send3").await["result"]["turn"].clone();
    assert_ne!(turn3["loop_id"], turn2["loop_id"]);
    process
        .send("wait3", "turn.wait", turn_params(&turn3))
        .await;
    assert_eq!(
        process.response("wait3").await["result"]["persistence"],
        "persisted"
    );

    // History contains all three user messages even though turn two was cancelled.
    let total = history_total(&mut process, "history", &session_id).await;
    assert!(
        total >= 4,
        "expected at least the three user prompts plus assistant text"
    );

    process.shutdown().await;
    server.finish().await;
    let _ = std::fs::remove_dir_all(base);
}
