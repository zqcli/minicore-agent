use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use minicore_runtime::SessionId;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

#[path = "support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{MockResponse, MockServer};

struct RpcProcess {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    stderr: Option<ChildStderr>,
    pending: VecDeque<Value>,
    events: Vec<Value>,
    observed: Vec<Value>,
}

impl RpcProcess {
    async fn spawn(config_path: &Path, key_env: &str, key: &str) -> Self {
        Self::spawn_with_extra_env(config_path, key_env, key, &[]).await
    }

    async fn spawn_with_extra_env(
        config_path: &Path,
        key_env: &str,
        key: &str,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_minicore-agent"));
        command
            .args(["--config", config_path.to_str().unwrap(), "--stdio"])
            .env(key_env, key);
        for &(name, value) in extra_env {
            command.env(name, value);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            stderr: child.stderr.take(),
            child,
            pending: VecDeque::new(),
            events: Vec::new(),
            observed: Vec::new(),
        }
    }

    async fn send(&mut self, id: &str, method: &str, params: Value) {
        let mut frame = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .unwrap();
        frame.push(b'\n');
        self.input.write_all(&frame).await.unwrap();
        self.input.flush().await.unwrap();
    }

    async fn response(&mut self, id: &str) -> Value {
        if let Some(index) = self.pending.iter().position(|frame| frame["id"] == id) {
            return self.pending.remove(index).unwrap();
        }
        loop {
            let frame = self.next_frame().await.expect("process stdout ended");
            if frame["method"] == "agent.event" {
                self.events.push(frame);
            } else if frame["id"] == id {
                return frame;
            } else {
                self.pending.push_back(frame);
            }
        }
    }

    async fn event(&mut self, event_type: &str) -> Value {
        if let Some(index) = self.events.iter().position(|frame| {
            frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type)
        }) {
            return self.events.remove(index);
        }
        loop {
            let frame = self.next_frame().await.expect("process stdout ended");
            if frame["method"] == "agent.event" {
                if frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type) {
                    return frame;
                }
                self.events.push(frame);
            } else {
                self.pending.push_back(frame);
            }
        }
    }

    async fn next_frame(&mut self) -> Option<Value> {
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(10), self.output.read_line(&mut line))
            .await
            .expect("process stdout timed out")
            .unwrap();
        if read == 0 {
            return None;
        }
        assert!(line.ends_with('\n'));
        let frame: Value = serde_json::from_str(&line).unwrap();
        self.observed.push(frame.clone());
        Some(frame)
    }

    async fn shutdown(mut self) -> (Vec<Value>, String) {
        self.send("shutdown", "agent.shutdown", json!({})).await;
        assert_eq!(
            self.response("shutdown").await["result"],
            json!({"ok": true})
        );
        assert!(self.next_frame().await.is_none());
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("process did not exit")
            .unwrap();
        assert!(status.success());
        let mut stderr = Vec::new();
        self.stderr
            .take()
            .unwrap()
            .read_to_end(&mut stderr)
            .await
            .unwrap();
        (self.observed, String::from_utf8_lossy(&stderr).into_owned())
    }
}

fn write_config(temp_dir: &Path, base_url: &str, key_env: &str, tools: &[&str]) -> PathBuf {
    write_config_with_additional_models(temp_dir, base_url, key_env, tools, "")
}

fn write_two_model_config(
    temp_dir: &Path,
    base_url: &str,
    first_key_env: &str,
    second_key_env: &str,
    tools: &[&str],
) -> PathBuf {
    let additional_models = format!(
        r#"
[models.secondary]
provider = "open_ai_responses"
model = "secondary-provider-model"
base_url = "{base_url}"
api_key_env = "{second_key_env}"
physical_context_window = 16000
output_budget_tokens = 1024
safety_margin_tokens = 1000
supported_reasoning = ["auto", "disabled", "low", "medium", "high"]
supports_tools = true
request_timeout_seconds = 5
"#
    );
    write_config_with_additional_models(
        temp_dir,
        base_url,
        first_key_env,
        tools,
        &additional_models,
    )
}

fn write_config_with_additional_models(
    temp_dir: &Path,
    base_url: &str,
    key_env: &str,
    tools: &[&str],
    additional_models: &str,
) -> PathBuf {
    let config_path = temp_dir.join("agent.toml");
    let tools = tools
        .iter()
        .map(|tool| format!("\"{tool}\""))
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        &config_path,
        format!(
            r#"data_dir = {:?}
event_capacity = 256
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = "Use configured tools and finish the task."
tools = [{tools}]
max_tool_rounds = 4
approval = "auto"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "{base_url}"
api_key_env = "{key_env}"
physical_context_window = 16000
output_budget_tokens = 1024
safety_margin_tokens = 1000
supported_reasoning = ["auto", "disabled", "low", "medium", "high"]
supports_tools = true
request_timeout_seconds = 5
{additional_models}
"#,
            temp_dir.join("data")
        ),
    )
    .unwrap();
    config_path
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

#[cfg(any(unix, windows))]
const PROCESS_BASH_KEY_ENV: &str = "MINICORE_PROCESS_BASH_MODEL_KEY";
#[cfg(any(unix, windows))]
const PROCESS_BASH_SECRET: &str = "PROCESS-BASH-MODEL-KEY-SECRET";
#[cfg(any(unix, windows))]
const PROCESS_BASH_SECOND_KEY_ENV: &str = "MINICORE_PROCESS_BASH_SECOND_MODEL_KEY";
#[cfg(any(unix, windows))]
const PROCESS_BASH_SECOND_SECRET: &str = "PROCESS-BASH-SECOND-MODEL-KEY-SECRET";
#[cfg(any(unix, windows))]
const PROCESS_BASH_AUTHORIZATION: &str = "Bearer PROCESS-BASH-MODEL-KEY-SECRET";
#[cfg(any(unix, windows))]
const PROCESS_BASH_PUBLIC_ENV: &str = "MINICORE_PROCESS_BASH_PUBLIC";
#[cfg(any(unix, windows))]
const PROCESS_BASH_PUBLIC_VALUE: &str = "process-bash-public-value";

#[cfg(unix)]
fn environment_probe_command() -> &'static str {
    r#"printf 'credential_one=<%s>\ncredential_two=<%s>\npublic=<%s>\nmarker=<%s>\n' "$MINICORE_PROCESS_BASH_MODEL_KEY" "$MINICORE_PROCESS_BASH_SECOND_MODEL_KEY" "$MINICORE_PROCESS_BASH_PUBLIC" "$MINICORE_AGENT""#
}

#[cfg(windows)]
fn environment_probe_command() -> &'static str {
    r#"[Console]::WriteLine(('credential_one=<{0}>' -f [string]$env:MINICORE_PROCESS_BASH_MODEL_KEY)); [Console]::WriteLine(('credential_two=<{0}>' -f [string]$env:MINICORE_PROCESS_BASH_SECOND_MODEL_KEY)); [Console]::WriteLine(('public=<{0}>' -f [string]$env:MINICORE_PROCESS_BASH_PUBLIC)); [Console]::WriteLine(('marker=<{0}>' -f [string]$env:MINICORE_AGENT))"#
}

#[cfg(any(unix, windows))]
fn assert_isolated_environment_probe(value: &str) {
    assert!(
        value.contains("credential_one=<>"),
        "first credential was not removed"
    );
    assert!(
        value.contains("credential_two=<>"),
        "second credential was not removed"
    );
    assert!(value.contains(&format!("public=<{PROCESS_BASH_PUBLIC_VALUE}>")));
    assert!(value.contains("marker=<1>"));
    assert_process_bash_secrets_absent(value);
}

#[cfg(any(unix, windows))]
fn assert_process_bash_secrets_absent(value: &str) {
    assert!(!value.contains(PROCESS_BASH_SECRET));
    assert!(!value.contains(PROCESS_BASH_SECOND_SECRET));
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key(key) || object.values().any(|value| contains_key(value, key))
        }
        Value::Array(values) => values.iter().any(|value| contains_key(value, key)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_process_runs_openai_read_tool_loop_and_redacted_transcript() {
    const KEY_ENV: &str = "MINICORE_PROCESS_OPENAI_KEY";
    const KEY: &str = "PROCESS-API-KEY-SECRET";
    const ARGUMENT_SECRET: &str = "PROCESS-TOOL-ARGUMENT-SECRET";

    let path = format!("{ARGUMENT_SECRET}.txt");
    let first = MockResponse::sse(&[
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "call_id": "process-read-call",
                "name": "read",
                "arguments": serde_json::to_string(&json!({"path": path})).unwrap()
            }
        }),
        completed(),
    ]);
    let second = MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "process final"}),
        completed(),
    ]);
    let server = MockServer::spawn([first, second]).await;
    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-openai-process-{}",
        SessionId::new().unwrap()
    ));
    let workspace = temp_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join(&path), "PROCESS-READ-CONTENT").unwrap();
    let config = write_config(&temp_dir, server.base_url(), KEY_ENV, &["read"]);
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;

    process.send("models", "model.list", json!({})).await;
    let models = process.response("models").await;
    assert_eq!(models["result"]["models"][0]["id"], "main");
    assert_eq!(models["result"]["models"][0]["model_ref"], "main");
    assert_eq!(models["result"]["models"][0]["context_window"], 13_976);
    assert!(models["result"]["models"][0].get("base_url").is_none());
    assert!(models["result"]["models"][0].get("api_key").is_none());
    assert!(!models.to_string().contains(server.base_url()));
    assert!(!models.to_string().contains(KEY));

    process
        .send("create", "session.create", json!({"workspace": workspace}))
        .await;
    let created = process.response("create").await;
    let session_id = created["result"]["session"]["session_id"].clone();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "read the input"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    process.event("tool_started").await;
    process.event("tool_finished").await;
    let output = process.event("output_delta").await;
    assert_eq!(output["params"]["data"]["delta"], "process final");
    process.event("turn_finished").await;

    process
        .send(
            "wait",
            "turn.wait",
            json!({
                "session_id": turn["session_id"],
                "instance_id": turn["instance_id"],
                "turn_id": turn["turn_id"],
            }),
        )
        .await;
    assert_eq!(
        process.response("wait").await["result"]["terminal"],
        "completed"
    );
    process
        .send(
            "transcript",
            "session.transcript",
            json!({"session_id": session_id, "limit": 100}),
        )
        .await;
    let transcript = process.response("transcript").await;
    assert!(!contains_key(&transcript["result"], "arguments"));
    let transcript_text = transcript.to_string();
    assert!(transcript_text.contains("PROCESS-READ-CONTENT"));
    assert!(transcript_text.contains("process final"));
    assert!(!transcript_text.contains(ARGUMENT_SECRET));
    process
        .send("close", "session.close", json!({"session_id": session_id}))
        .await;
    process.response("close").await;
    let (observed, stderr) = process.shutdown().await;
    assert!(observed.iter().all(Value::is_object));
    assert!(!stderr.contains(KEY));
    assert!(!serde_json::to_string(&observed).unwrap().contains(KEY));

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].header("authorization"),
        Some("Bearer PROCESS-API-KEY-SECRET")
    );
    assert!(!String::from_utf8_lossy(requests[0].body()).contains(KEY));
    assert!(
        requests[1].json_body()["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "function_call_output")
    );
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_process_bash_removes_model_credentials_and_preserves_public_environment() {
    let first = MockResponse::sse(&[
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "call_id": "process-bash-environment-call",
                "name": "bash",
                "arguments": serde_json::to_string(&json!({
                    "command": environment_probe_command()
                }))
                .unwrap()
            }
        }),
        completed(),
    ]);
    let second = MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "environment isolated"}),
        completed(),
    ]);
    let server = MockServer::spawn([first, second]).await;
    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-openai-bash-environment-{}",
        SessionId::new().unwrap()
    ));
    let workspace = temp_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_two_model_config(
        &temp_dir,
        server.base_url(),
        PROCESS_BASH_KEY_ENV,
        PROCESS_BASH_SECOND_KEY_ENV,
        &["bash"],
    );
    let mut process = RpcProcess::spawn_with_extra_env(
        &config,
        PROCESS_BASH_KEY_ENV,
        PROCESS_BASH_SECRET,
        &[
            (PROCESS_BASH_SECOND_KEY_ENV, PROCESS_BASH_SECOND_SECRET),
            (PROCESS_BASH_PUBLIC_ENV, PROCESS_BASH_PUBLIC_VALUE),
        ],
    )
    .await;

    process
        .send("create", "session.create", json!({"workspace": workspace}))
        .await;
    let session_id = process.response("create").await["result"]["session"]["session_id"].clone();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "inspect the command environment"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    process.event("tool_started").await;
    process.event("tool_finished").await;
    assert_eq!(
        process.event("output_delta").await["params"]["data"]["delta"],
        "environment isolated"
    );
    process.event("turn_finished").await;
    process
        .send(
            "wait",
            "turn.wait",
            json!({
                "session_id": turn["session_id"],
                "instance_id": turn["instance_id"],
                "turn_id": turn["turn_id"],
            }),
        )
        .await;
    assert_eq!(
        process.response("wait").await["result"]["terminal"],
        "completed"
    );

    process
        .send(
            "transcript",
            "session.transcript",
            json!({"session_id": session_id, "limit": 100}),
        )
        .await;
    let transcript = process.response("transcript").await;
    let tool_result = transcript["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|entry| entry.get("tool_result"))
        .expect("transcript must contain the Bash tool result");
    assert_eq!(tool_result["outcome"], "success");
    assert_isolated_environment_probe(
        tool_result["content"]
            .as_str()
            .expect("Bash tool result content must be text"),
    );
    let transcript_text = transcript.to_string();
    assert_process_bash_secrets_absent(&transcript_text);
    process
        .send("close", "session.close", json!({"session_id": session_id}))
        .await;
    process.response("close").await;
    let (observed, stderr) = process.shutdown().await;
    let observed = serde_json::to_string(&observed).unwrap();
    assert_process_bash_secrets_absent(&observed);
    assert_process_bash_secrets_absent(&stderr);

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(
            request.header("authorization"),
            Some(PROCESS_BASH_AUTHORIZATION)
        );
        for (name, value) in request.headers() {
            assert!(!name.contains(PROCESS_BASH_SECOND_SECRET));
            assert!(!value.contains(PROCESS_BASH_SECOND_SECRET));
            if !name.eq_ignore_ascii_case("authorization") {
                assert!(!name.contains(PROCESS_BASH_SECRET));
                assert!(!value.contains(PROCESS_BASH_SECRET));
            }
        }
        assert_process_bash_secrets_absent(&String::from_utf8_lossy(request.body()));
    }
    let second_body = requests[1].json_body();
    let tool_output = second_body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "function_call_output")
        .and_then(|item| item["output"].as_str())
        .expect("second request must contain the Bash tool output");
    assert_isolated_environment_probe(tool_output);
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_error_body_secret_never_reaches_process_rpc_or_stderr() {
    const KEY_ENV: &str = "MINICORE_PROCESS_ERROR_KEY";
    const KEY: &str = "PROCESS-ERROR-API-KEY-SECRET";
    const PROVIDER_SECRET: &str = "PROCESS-RAW-PROVIDER-SECRET";

    let server = MockServer::spawn([MockResponse::json(
        500,
        format!(r#"{{"error":{{"message":"{PROVIDER_SECRET}","type":"server_error"}}}}"#),
    )])
    .await;
    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-openai-error-process-{}",
        SessionId::new().unwrap()
    ));
    let workspace = temp_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&temp_dir, server.base_url(), KEY_ENV, &[]);
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    process
        .send("create", "session.create", json!({"workspace": workspace}))
        .await;
    let session_id = process.response("create").await["result"]["session"]["session_id"].clone();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "fail safely"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    process.event("turn_finished").await;
    process
        .send(
            "wait",
            "turn.wait",
            json!({
                "session_id": turn["session_id"],
                "instance_id": turn["instance_id"],
                "turn_id": turn["turn_id"],
            }),
        )
        .await;
    assert!(process.response("wait").await["result"]["terminal"]["failed"].is_object());
    process
        .send(
            "transcript",
            "session.transcript",
            json!({"session_id": session_id, "limit": 100}),
        )
        .await;
    let transcript = process.response("transcript").await;
    assert!(!contains_key(&transcript["result"], "message"));
    process
        .send(
            "missing",
            "session.open",
            json!({"session_id": SessionId::new().unwrap()}),
        )
        .await;
    assert_eq!(
        process.response("missing").await["error"]["data"]["kind"],
        "session_not_found"
    );
    let (observed, stderr) = process.shutdown().await;
    let output = serde_json::to_string(&observed).unwrap();
    assert!(!output.contains(PROVIDER_SECRET));
    assert!(!output.contains(KEY));
    assert!(!stderr.contains(PROVIDER_SECRET));
    assert!(!stderr.contains(KEY));
    server.finish().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}
