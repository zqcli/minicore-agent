use std::path::{Path, PathBuf};

use minicore_runtime::SessionId;
use serde_json::{Value, json};

#[path = "support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{ChunkGate, ConcurrentMockServer, MockResponse, MockServer, sse_body};
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

fn gated_sse(events: Vec<Value>, split_events: bool) -> (MockResponse, ChunkGate) {
    let response = if split_events {
        MockResponse::sse_bytes(Vec::new()).with_chunks(
            events
                .into_iter()
                .map(|event| sse_body(std::slice::from_ref(&event)).into_bytes()),
        )
    } else {
        MockResponse::sse(&events)
    };
    response.with_chunk_gate()
}

fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "instance_id": turn["instance_id"],
        "turn_id": turn["turn_id"],
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

async fn send_and_register_wait(
    process: &mut RpcProcess,
    prefix: &str,
    session_id: &Value,
    text: &str,
) -> (Value, String) {
    let send_id = format!("{prefix}-send");
    process
        .send(
            &send_id,
            "turn.send",
            json!({"session_id": session_id, "text": text}),
        )
        .await;
    let turn = process.response(&send_id).await["result"]["turn"].clone();
    let wait_id = format!("{prefix}-wait");
    process
        .send(&wait_id, "turn.wait", turn_params(&turn))
        .await;
    let dispatch_ping_id = format!("{wait_id}-dispatch-ping");
    process
        .send(&dispatch_ping_id, "agent.ping", json!({}))
        .await;
    assert_eq!(
        process.response(&dispatch_ping_id).await["result"]["version"],
        "0.1.0"
    );
    (turn, wait_id)
}

async fn transcript(process: &mut RpcProcess, id: &str, session_id: &Value) -> Value {
    process
        .send(
            id,
            "session.transcript",
            json!({"session_id": session_id, "limit": 100}),
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

fn entries_of<'a>(transcript: &'a Value, kind: &str) -> Vec<&'a Value> {
    transcript["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.get(kind))
        .collect()
}

fn event_for_turn(frame: &Value, turn_id: &Value) -> bool {
    frame.pointer("/params/data/turn/turn_id") == Some(turn_id)
}

fn turn_finished_was_observed(process: &RpcProcess, turn_id: &Value) -> bool {
    process.observed().iter().any(|frame| {
        frame.pointer("/params/type").and_then(Value::as_str) == Some("turn_finished")
            && event_for_turn(frame, turn_id)
    })
}

async fn release_output_chunk(
    process: &mut RpcProcess,
    gate: &ChunkGate,
    turn_id: &Value,
    channel: &str,
    expected: &str,
) {
    gate.release();
    let delta = process
        .event_matching("output_delta", |frame| event_for_turn(frame, turn_id))
        .await;
    assert_eq!(delta["params"]["data"]["channel"], channel);
    assert_eq!(delta["params"]["data"]["delta"], expected);
    assert!(!turn_finished_was_observed(process, turn_id));
}

fn has_event(frames: &[Value], event_type: &str) -> bool {
    frames
        .iter()
        .any(|frame| frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type))
}

fn session_dir_count(base: &Path) -> usize {
    std::fs::read_dir(base.join("data/sessions"))
        .map(|entries| entries.count())
        .unwrap_or(0)
}

fn assert_error(response: &Value, code: i64, kind: &str) {
    assert_eq!(response["error"]["code"], code);
    assert_eq!(response["error"]["data"]["kind"], kind);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flows_a_b_i_j_l_discovery_settings_validation_and_manifest_reopen() {
    let server = MockServer::spawn([text_response("deep high final")]).await;
    let base_url = server.base_url().to_owned();
    let base = test_dir("a-b-i-j-l");
    let workspace_a = base.join("workspace-a");
    let workspace_b = base.join("workspace-b");
    let invalid_workspace = base.join("workspace-invalid");
    for workspace in [&workspace_a, &workspace_b, &invalid_workspace] {
        std::fs::create_dir_all(workspace).unwrap();
    }
    let config = write_config(&base, &base_url, 64, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    // Flow A: discovery is sorted, complete, and secret-safe.
    process.send("a-ping", "agent.ping", json!({})).await;
    assert_eq!(
        process.response("a-ping").await["result"]["version"],
        "0.1.0"
    );
    process.send("a-profiles", "profile.list", json!({})).await;
    let profiles = process.response("a-profiles").await;
    assert_eq!(
        profiles["result"]["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|profile| profile["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["coding", "quick"]
    );
    assert_eq!(profiles["result"]["profiles"][0]["model"], "deep");
    assert_eq!(profiles["result"]["profiles"][0]["reasoning"], "high");
    assert_eq!(profiles["result"]["profiles"][0]["approval"], "auto");
    process.send("a-models", "model.list", json!({})).await;
    let models = process.response("a-models").await;
    assert_eq!(
        models["result"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["deep", "fast"]
    );
    assert_eq!(
        models["result"]["models"][0]["supported_reasoning"],
        json!(["auto", "low", "medium", "high"])
    );
    assert_eq!(
        models["result"]["models"][1]["supported_reasoning"],
        json!(["auto", "disabled", "low"])
    );
    let discovery = format!("{profiles}{models}");
    for private in [
        KEY,
        KEY_ENV,
        base_url.as_str(),
        FAST_PROVIDER,
        DEEP_PROVIDER,
    ] {
        assert!(!discovery.contains(private));
    }
    process.send("a-sessions", "session.list", json!({})).await;
    assert_eq!(
        process.response("a-sessions").await["result"]["sessions"],
        json!([])
    );

    // Flow B: explicit deep/high settings reach SessionInfo and durable execution.
    let session_a =
        create_session(&mut process, "b-create-deep", &workspace_a, "deep", "high").await;
    assert_eq!(session_a["model"], "deep");
    assert_eq!(session_a["reasoning"], "high");
    let session_a_id = session_a["session_id"].clone();
    let old_instance = session_a["instance_id"].clone();

    // Flow I: a new fast/low Session is isolated and no set RPC exists.
    let session_b =
        create_session(&mut process, "i-create-fast", &workspace_b, "fast", "low").await;
    assert_ne!(session_a["session_id"], session_b["session_id"]);
    assert_eq!(session_b["model"], "fast");
    assert_eq!(session_b["reasoning"], "low");
    process.send("i-list", "session.list", json!({})).await;
    let listed = process.response("i-list").await["result"]["sessions"]
        .as_array()
        .unwrap()
        .clone();
    assert!(listed.iter().any(|session| {
        session["session_id"] == session_a_id
            && session["model"] == "deep"
            && session["reasoning"] == "high"
    }));
    assert!(listed.iter().any(|session| {
        session["session_id"] == session_b["session_id"]
            && session["model"] == "fast"
            && session["reasoning"] == "low"
    }));
    for (index, method) in [
        "session.set_model",
        "session.set_reasoning",
        "turn.set_model",
        "turn.set_reasoning",
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("i-no-set-{index}");
        process.send(&id, method, json!({})).await;
        assert_error(&process.response(&id).await, -32_601, "method_not_found");
    }

    // Flow J: fast/high fails before creating another durable Session directory.
    let before_invalid = session_dir_count(&base);
    process
        .send(
            "j-invalid",
            "session.create",
            json!({
                "workspace": invalid_workspace,
                "profile": "coding",
                "model": "fast",
                "reasoning": "high"
            }),
        )
        .await;
    assert_error(
        &process.response("j-invalid").await,
        -32_014,
        "invalid_session_settings",
    );
    assert_eq!(session_dir_count(&base), before_invalid);

    let (_turn, wait_id) =
        send_and_register_wait(&mut process, "b-deep-turn", &session_a_id, "run deep high").await;
    assert_eq!(
        process.response(&wait_id).await["result"]["terminal"],
        "completed"
    );
    let before_reopen = transcript(&mut process, "b-transcript", &session_a_id).await;
    let user = entries_of(&before_reopen, "user_message")[0];
    assert_eq!(user["execution"]["model"], "deep");
    assert_eq!(user["execution"]["reasoning"], "high");

    // Flow L: a new process has changed Profile defaults, but manifest settings win.
    process
        .send(
            "l-close",
            "session.close",
            json!({"session_id": session_a_id}),
        )
        .await;
    assert_eq!(
        process.response("l-close").await["result"],
        json!({"ok": true})
    );
    let (first_frames, first_stderr) = process.shutdown().await;
    let requests = server.finish().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].json_body()["model"], DEEP_PROVIDER);
    assert_eq!(
        requests[0].json_body()["reasoning"],
        json!({"effort": "high", "summary": "auto"})
    );

    write_config(&base, &base_url, 64, &[], "fast", "low");
    let mut reopened_process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    reopened_process
        .send("l-profiles", "profile.list", json!({}))
        .await;
    let current_profile = reopened_process.response("l-profiles").await;
    assert_eq!(current_profile["result"]["profiles"][0]["model"], "fast");
    assert_eq!(current_profile["result"]["profiles"][0]["reasoning"], "low");
    reopened_process
        .send(
            "l-open",
            "session.open",
            json!({"session_id": session_a_id}),
        )
        .await;
    let reopened = reopened_process.response("l-open").await["result"]["session"].clone();
    assert_ne!(reopened["instance_id"], old_instance);
    assert_eq!(reopened["model"], "deep");
    assert_eq!(reopened["reasoning"], "high");
    assert_eq!(
        transcript(&mut reopened_process, "l-transcript", &session_a_id).await,
        before_reopen
    );
    let (second_frames, second_stderr) = reopened_process.shutdown().await;
    let output = format!(
        "{}{}{}{}",
        serde_json::to_string(&first_frames).unwrap(),
        first_stderr,
        serde_json::to_string(&second_frames).unwrap(),
        second_stderr
    );
    for private in [KEY, KEY_ENV, base_url.as_str()] {
        assert!(!output.contains(private));
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flows_c_d_gated_text_and_separate_reasoning_channels() {
    let (text_stream, text_gate) = gated_sse(
        vec![
            json!({"type": "response.output_text.delta", "delta": "hello"}),
            json!({"type": "response.output_text.delta", "delta": " "}),
            json!({"type": "response.output_text.delta", "delta": "world"}),
            completed(),
        ],
        true,
    );
    let (reasoning_stream, reasoning_gate) = gated_sse(
        vec![
            json!({"type": "response.reasoning_summary_text.delta", "delta": "Reasoning A"}),
            json!({"type": "response.reasoning_summary_text.delta", "delta": "Reasoning B"}),
            json!({"type": "response.output_text.delta", "delta": "Text Final"}),
            completed(),
        ],
        true,
    );
    let server = MockServer::spawn([text_stream, reasoning_stream]).await;
    let base = test_dir("c-d");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, server.base_url(), 64, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let session = create_session(&mut process, "cd-create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();

    // Flow C: each gated TextDelta is observed before the terminal chunk is released.
    let (text_turn, text_wait) =
        send_and_register_wait(&mut process, "c", &session_id, "stream text").await;
    server.wait_for_requests(1).await;
    let mut text = String::new();
    for expected in ["hello", " ", "world"] {
        release_output_chunk(
            &mut process,
            &text_gate,
            &text_turn["turn_id"],
            "text",
            expected,
        )
        .await;
        text.push_str(expected);
    }
    assert_eq!(text, "hello world");
    text_gate.release();
    assert_eq!(
        process.response(&text_wait).await["result"]["terminal"],
        "completed"
    );
    process
        .event_matching("turn_finished", |frame| {
            event_for_turn(frame, &text_turn["turn_id"])
        })
        .await;
    let text_transcript = transcript(&mut process, "c-transcript", &session_id).await;
    assert_eq!(
        entries_of(&text_transcript, "assistant_message")[0]["text"],
        "hello world"
    );

    // Flow D: reasoning and text retain independent channels and durable fields.
    let (reasoning_turn, reasoning_wait) =
        send_and_register_wait(&mut process, "d", &session_id, "stream reasoning").await;
    server.wait_for_requests(2).await;
    let mut reasoning = String::new();
    for expected in ["Reasoning A", "Reasoning B"] {
        release_output_chunk(
            &mut process,
            &reasoning_gate,
            &reasoning_turn["turn_id"],
            "reasoning",
            expected,
        )
        .await;
        reasoning.push_str(expected);
    }
    release_output_chunk(
        &mut process,
        &reasoning_gate,
        &reasoning_turn["turn_id"],
        "text",
        "Text Final",
    )
    .await;
    reasoning_gate.release();
    assert_eq!(
        process.response(&reasoning_wait).await["result"]["terminal"],
        "completed"
    );
    let reasoning_transcript = transcript(&mut process, "d-transcript", &session_id).await;
    let assistant = entries_of(&reasoning_transcript, "assistant_message")
        .into_iter()
        .last()
        .unwrap();
    assert_eq!(assistant["reasoning"], reasoning);
    assert_eq!(assistant["text"], "Text Final");

    let (frames, _) = process.shutdown().await;
    assert!(!has_event(&frames, "interaction_requested"));
    assert_eq!(server.finish().await.len(), 2);
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flows_e_f_auto_read_and_write_tool_loops() {
    let server = MockServer::spawn([
        tool_response("e-read-call", "read", json!({"path": "input.txt"})),
        text_response("read final"),
        tool_response(
            "f-write-call",
            "write",
            json!({"path": "written.txt", "content": "FLOW-F-WRITTEN"}),
        ),
        text_response("write final"),
    ])
    .await;
    let base = test_dir("e-f");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("input.txt"), "FLOW-E-READ-CONTENT").unwrap();
    let config = write_config(
        &base,
        server.base_url(),
        64,
        &["read", "write"],
        "deep",
        "high",
    );
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let session = create_session(&mut process, "ef-create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();

    // Flow E: auto approval runs read, emits Tool events, and starts round two.
    let (read_turn, read_wait) =
        send_and_register_wait(&mut process, "e", &session_id, "read input").await;
    let read_started = process
        .event_matching("tool_started", |frame| {
            event_for_turn(frame, &read_turn["turn_id"])
        })
        .await;
    assert_eq!(read_started["params"]["data"]["tool_name"], "read");
    let read_finished = process
        .event_matching("tool_finished", |frame| {
            event_for_turn(frame, &read_turn["turn_id"])
        })
        .await;
    assert_eq!(
        read_finished["params"]["data"]["result"]["outcome"],
        "success"
    );
    assert_eq!(
        process.response(&read_wait).await["result"]["terminal"],
        "completed"
    );
    let read_transcript = transcript(&mut process, "e-transcript", &session_id).await;
    assert!(read_transcript.to_string().contains("FLOW-E-READ-CONTENT"));
    assert!(read_transcript.to_string().contains("read final"));

    // Flow F: auto write mutates the real Workspace and then completes round two.
    let (write_turn, write_wait) =
        send_and_register_wait(&mut process, "f", &session_id, "write output").await;
    process
        .event_matching("tool_started", |frame| {
            event_for_turn(frame, &write_turn["turn_id"])
        })
        .await;
    let write_finished = process
        .event_matching("tool_finished", |frame| {
            event_for_turn(frame, &write_turn["turn_id"])
        })
        .await;
    assert_eq!(
        write_finished["params"]["data"]["result"]["outcome"],
        "success"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("written.txt")).unwrap(),
        "FLOW-F-WRITTEN"
    );
    assert_eq!(
        process.response(&write_wait).await["result"]["terminal"],
        "completed"
    );

    let (frames, _) = process.shutdown().await;
    assert!(!has_event(&frames, "interaction_requested"));
    let requests = server.finish().await;
    assert_eq!(requests.len(), 4);
    assert!(
        requests[1].json_body()["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "function_call_output")
    );
    assert!(
        requests[3].json_body()["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "function_call_output")
    );
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_g_three_turns_wait_immediately_and_persist_contiguous_sequence() {
    let server = MockServer::spawn([
        text_response("turn one"),
        tool_response("g-read-call", "read", json!({"path": "input.txt"})),
        text_response("turn two"),
        text_response("turn three"),
    ])
    .await;
    let base = test_dir("g");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("input.txt"), "FLOW-G-READ-CONTENT").unwrap();
    let config = write_config(&base, server.base_url(), 64, &["read"], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let session = create_session(&mut process, "g-create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();

    for (index, prompt) in ["turn one", "turn two read", "turn three"]
        .into_iter()
        .enumerate()
    {
        let prefix = format!("g-{index}");
        let (_turn, wait_id) =
            send_and_register_wait(&mut process, &prefix, &session_id, prompt).await;
        assert_eq!(
            process.response(&wait_id).await["result"]["terminal"],
            "completed"
        );
        assert_eq!(
            state(&mut process, &format!("g-state-{index}"), &session_id).await["status"],
            "idle"
        );
    }

    let durable = transcript(&mut process, "g-transcript", &session_id).await;
    assert_eq!(entries_of(&durable, "user_message").len(), 3);
    assert_eq!(entries_of(&durable, "turn_terminal").len(), 3);
    let sequences = durable["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            entry.as_object().unwrap().values().next().unwrap()["seq"]
                .as_u64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sequences,
        (1..=u64::try_from(sequences.len()).unwrap()).collect::<Vec<_>>()
    );
    assert!(durable.to_string().contains("FLOW-G-READ-CONTENT"));

    let (frames, _) = process.shutdown().await;
    assert!(!has_event(&frames, "interaction_requested"));
    assert_eq!(server.finish().await.len(), 4);
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_h_exact_cancel_wait_idle_then_next_turn_succeeds() {
    let (blocked, gate) = gated_sse(vec![completed()], false);
    let server = MockServer::spawn([blocked, text_response("after cancel")]).await;
    let base = test_dir("h");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, server.base_url(), 64, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let session = create_session(&mut process, "h-create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();

    // Flow H: the Provider stream is blocked by a gate, never by a timer.
    let (blocked_turn, blocked_wait) =
        send_and_register_wait(&mut process, "h-blocked", &session_id, "block").await;
    server.wait_for_requests(1).await;
    process
        .send("h-cancel", "turn.cancel", turn_params(&blocked_turn))
        .await;
    assert_eq!(
        process.response("h-cancel").await["result"]["cancelled"],
        true
    );
    assert_eq!(
        process.response(&blocked_wait).await["result"]["terminal"],
        "cancelled_by_user"
    );
    assert_eq!(
        state(&mut process, "h-idle", &session_id).await["status"],
        "idle"
    );
    gate.release();

    let (_next_turn, next_wait) =
        send_and_register_wait(&mut process, "h-next", &session_id, "continue").await;
    assert_eq!(
        process.response(&next_wait).await["result"]["terminal"],
        "completed"
    );
    assert_eq!(
        state(&mut process, "h-next-idle", &session_id).await["status"],
        "idle"
    );

    process.shutdown().await;
    assert_eq!(server.finish().await.len(), 2);
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_k_event_loss_does_not_break_wait_state_or_transcript() {
    const DELTAS: usize = 4_096;
    let mut events = (0..DELTAS)
        .map(|_| json!({"type": "response.output_text.delta", "delta": "x"}))
        .collect::<Vec<_>>();
    events.push(completed());
    let (burst, gate) = gated_sse(events, false);
    let server = MockServer::spawn([burst]).await;
    let base = test_dir("k");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&base, server.base_url(), 4, &[], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let session = create_session(&mut process, "k-create", &workspace, "deep", "high").await;
    let session_id = session["session_id"].clone();

    // Flow K: drain startup events, checkpoint, then release the burst without reading stdout.
    let (turn, wait_id) =
        send_and_register_wait(&mut process, "k-burst", &session_id, "burst").await;
    server.wait_for_requests(1).await;
    process
        .event_matching("turn_started", |frame| {
            event_for_turn(frame, &turn["turn_id"])
        })
        .await;
    process
        .event_matching("session_state", |frame| {
            frame
                .pointer("/params/data/state/status")
                .and_then(Value::as_str)
                == Some("running")
                && frame.pointer("/params/data/state/active_turn") == Some(&turn["turn_id"])
        })
        .await;
    let burst_checkpoint = process.observed().len();
    assert!(
        process.observed()[..burst_checkpoint]
            .iter()
            .filter(|frame| frame["method"] == "agent.event")
            .all(|frame| {
                frame
                    .pointer("/params/data/meta/dropped_before")
                    .and_then(Value::as_u64)
                    == Some(0)
            })
    );
    gate.release();
    assert_eq!(server.finish().await.len(), 1);
    assert_eq!(
        process.response(&wait_id).await["result"]["terminal"],
        "completed"
    );
    assert_eq!(
        state(&mut process, "k-state", &session_id).await["status"],
        "idle"
    );
    let durable = transcript(&mut process, "k-transcript", &session_id).await;
    assert_eq!(
        entries_of(&durable, "assistant_message")[0]["text"]
            .as_str()
            .unwrap()
            .len(),
        DELTAS
    );
    assert_eq!(entries_of(&durable, "turn_terminal").len(), 1);

    process
        .send(
            "k-close",
            "session.close",
            json!({"session_id": session_id}),
        )
        .await;
    assert_eq!(
        process.response("k-close").await["result"],
        json!({"ok": true})
    );
    process.event("session_closed").await;
    assert!(process.observed()[burst_checkpoint..].iter().any(|frame| {
        frame["method"] == "agent.event"
            && frame
                .pointer("/params/data/meta/dropped_before")
                .and_then(Value::as_u64)
                .is_some_and(|dropped| dropped > 0)
    }));

    process.shutdown().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_m_two_sessions_run_independently_and_cancel_exact_blocked_turn() {
    let (blocked, gate) = gated_sse(vec![completed()], false);
    let server = ConcurrentMockServer::spawn([
        blocked,
        tool_response("m-read-call", "read", json!({"path": "input.txt"})),
        text_response("session b final"),
    ])
    .await;
    let base = test_dir("m");
    let workspace_a = base.join("workspace-a");
    let workspace_b = base.join("workspace-b");
    std::fs::create_dir_all(&workspace_a).unwrap();
    std::fs::create_dir_all(&workspace_b).unwrap();
    std::fs::write(workspace_b.join("input.txt"), "FLOW-M-B-READ-CONTENT").unwrap();
    let config = write_config(&base, server.base_url(), 128, &["read"], "deep", "high");
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let session_a = create_session(&mut process, "m-create-a", &workspace_a, "deep", "high").await;
    let session_b = create_session(&mut process, "m-create-b", &workspace_b, "fast", "low").await;
    let session_a_id = session_a["session_id"].clone();
    let session_b_id = session_b["session_id"].clone();

    // Flow M: A remains blocked while B completes a two-round read loop.
    let (turn_a, wait_a) =
        send_and_register_wait(&mut process, "m-a", &session_a_id, "block a").await;
    server.wait_for_requests(1).await;
    let (turn_b, wait_b) =
        send_and_register_wait(&mut process, "m-b", &session_b_id, "read b").await;
    process
        .event_matching("tool_started", |frame| {
            event_for_turn(frame, &turn_b["turn_id"])
        })
        .await;
    process
        .event_matching("tool_finished", |frame| {
            event_for_turn(frame, &turn_b["turn_id"])
        })
        .await;
    assert_eq!(
        process.response(&wait_b).await["result"]["terminal"],
        "completed"
    );
    assert_eq!(
        state(&mut process, "m-b-idle", &session_b_id).await["status"],
        "idle"
    );
    assert_eq!(
        state(&mut process, "m-a-running", &session_a_id).await["status"],
        "running"
    );
    assert!(
        transcript(&mut process, "m-b-transcript", &session_b_id)
            .await
            .to_string()
            .contains("FLOW-M-B-READ-CONTENT")
    );

    process
        .send("m-cancel-a", "turn.cancel", turn_params(&turn_a))
        .await;
    assert_eq!(
        process.response("m-cancel-a").await["result"]["cancelled"],
        true
    );
    assert_eq!(
        process.response(&wait_a).await["result"]["terminal"],
        "cancelled_by_user"
    );
    assert_eq!(
        state(&mut process, "m-a-idle", &session_a_id).await["status"],
        "idle"
    );
    assert_eq!(
        state(&mut process, "m-b-still-idle", &session_b_id).await["status"],
        "idle"
    );
    gate.release();

    let (frames, _) = process.shutdown().await;
    assert!(!has_event(&frames, "interaction_requested"));
    assert_eq!(server.finish().await.len(), 3);
    let _ = std::fs::remove_dir_all(base);
}
