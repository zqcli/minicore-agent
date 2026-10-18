pub mod support;

use crate::support::config;
use crate::support::fixture::Fixture;
use crate::support::responses::{completed, text_response, turn_params};

use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use openai_mock::{MockResponse, MockServer};
use rpc_process::RpcProcess;
use support::openai_mock;
use support::rpc_process;

const KEY_ENV: &str = "MINICORE_RPC_SOAK_KEY";
const KEY: &str = "RPC-SOAK-API-KEY-SECRET";

fn write_config(base: &Path, base_url: &str) -> PathBuf {
    let mut config = config::provider_config(base_url, KEY_ENV, &["read", "write"]);
    config["event_capacity"] = json!(4);
    config["default_profile"] = json!("soak");
    let mut profile = config["profiles"]["test"].take();
    profile["model"] = json!("soak");
    profile["reasoning"] = json!("high");
    profile["system_prompt"] = json!("Run the offline RPC soak.");
    config["profiles"] = json!({"soak": profile});
    let mut model = config["models"]["main"].take();
    model["model"] = json!("soak-provider");
    model["physical_context_window"] = json!(32000);
    model["output_budget_tokens"] = json!(2048);
    model["supported_reasoning"] = json!(["auto", "low", "medium", "high"]);
    config["models"] = json!({"soak": model});
    config::write_config(base, config)
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

async fn create_session(process: &mut RpcProcess, id: &str, workspace: &Path) -> Value {
    process
        .create_session(id, json!({"workspace": workspace, "profile": "soak"}))
        .await
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
    let fixture = Fixture::new("rpc-soak");
    let base = fixture.path().to_owned();
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
    let fixture = Fixture::new("rpc-soak");
    let base = fixture.path().to_owned();
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

    process
        .wait_post_turn_noop("first-settled", &session_id, &turn1)
        .await;

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
}
