use std::path::{Path, PathBuf};

use minicore_runtime::SessionId;
use serde_json::{Value, json};

#[path = "support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{MockResponse, MockServer};

#[path = "support/rpc_process.rs"]
mod rpc_process;
use rpc_process::RpcProcess;

fn write_config(temp_dir: &Path, base_url: &str, key_env: &str, tools: &[&str]) -> PathBuf {
    write_config_with_additional_models(temp_dir, base_url, key_env, tools, "")
}

fn write_config_with_reasoning(
    temp_dir: &Path,
    base_url: &str,
    key_env: &str,
    tools: &[&str],
    reasoning: &str,
) -> PathBuf {
    write_config_with_reasoning_and_additional_models(
        temp_dir, base_url, key_env, tools, reasoning, "",
    )
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
    write_config_with_reasoning_and_additional_models(
        temp_dir,
        base_url,
        key_env,
        tools,
        "auto",
        additional_models,
    )
}

fn write_config_with_reasoning_and_additional_models(
    temp_dir: &Path,
    base_url: &str,
    key_env: &str,
    tools: &[&str],
    reasoning: &str,
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
reasoning = "{reasoning}"
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
#[cfg(any(unix, windows))]
const PROCESS_BASH_COMMAND_MARKER: &str = "PROCESS-B2-BASH-COMMAND-MARKER";
#[cfg(any(unix, windows))]
const PROCESS_BASH_PATH_MARKER: &str = "process-b2-bash-private-path-marker.txt";
#[cfg(any(unix, windows))]
const PROCESS_BASH_CONTENT_MARKER: &str = "PROCESS-B2-BASH-PRIVATE-CONTENT-MARKER";

#[cfg(unix)]
fn environment_probe_command() -> &'static str {
    concat!(
        "# PROCESS-B2-BASH-COMMAND-MARKER\n",
        "probe_path='process-b2-bash-private-path-marker.txt'\n",
        "printf '%s' 'PROCESS-B2-BASH-PRIVATE-CONTENT-MARKER' > \"$probe_path\"\n",
        "rm -f \"$probe_path\"\n",
        "printf 'credential_one=<%s>\\ncredential_two=<%s>\\npublic=<%s>\\nmarker=<%s>\\n' ",
        "\"$MINICORE_PROCESS_BASH_MODEL_KEY\" ",
        "\"$MINICORE_PROCESS_BASH_SECOND_MODEL_KEY\" ",
        "\"$MINICORE_PROCESS_BASH_PUBLIC\" \"$MINICORE_AGENT\""
    )
}

#[cfg(windows)]
fn environment_probe_command() -> &'static str {
    concat!(
        "# PROCESS-B2-BASH-COMMAND-MARKER\r\n",
        "$probePath = 'process-b2-bash-private-path-marker.txt'; ",
        "[System.IO.File]::WriteAllText($probePath, ",
        "'PROCESS-B2-BASH-PRIVATE-CONTENT-MARKER'); ",
        "Remove-Item -LiteralPath $probePath -Force; ",
        "[Console]::WriteLine(('credential_one=<{0}>' -f ",
        "[string]$env:MINICORE_PROCESS_BASH_MODEL_KEY)); ",
        "[Console]::WriteLine(('credential_two=<{0}>' -f ",
        "[string]$env:MINICORE_PROCESS_BASH_SECOND_MODEL_KEY)); ",
        "[Console]::WriteLine(('public=<{0}>' -f ",
        "[string]$env:MINICORE_PROCESS_BASH_PUBLIC)); ",
        "[Console]::WriteLine(('marker=<{0}>' -f [string]$env:MINICORE_AGENT))"
    )
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

#[cfg(any(unix, windows))]
fn assert_process_bash_command_markers_absent(value: &str) {
    for marker in [
        PROCESS_BASH_COMMAND_MARKER,
        PROCESS_BASH_PATH_MARKER,
        PROCESS_BASH_CONTENT_MARKER,
    ] {
        assert!(!value.contains(marker));
    }
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

fn log_field_equals(line: &str, name: &str, expected: &str) -> bool {
    let prefix = format!("{name}=");
    line.split_ascii_whitespace().any(|token| {
        let token = token.trim_end_matches([',', ';']);
        let Some(value) = token.strip_prefix(&prefix) else {
            return false;
        };
        value == expected
            || value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                == Some(expected)
    })
}

fn assert_log_field_equals(line: &str, name: &str, expected: &str) {
    assert!(
        log_field_equals(line, name, expected),
        "provider log field {name} did not equal expected safe value {expected}"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_process_reasoning_replay_is_exact_in_http_and_private_everywhere_else() {
    const KEY_ENV: &str = "MINICORE_PROCESS_REASONING_KEY";
    const KEY: &str = "PROCESS-REASONING-API-KEY-SECRET";
    const CALL_ID: &str = "process-reasoning-read-call";
    const FUNCTION_ARGUMENTS: &str = r#"{ "path": "phase4-process.txt" }"#;
    const ENCRYPTED_MARKER: &str = "enc::PROCESS-A4-T09-T11::AAECAwQFBgcICQ==";
    const OPAQUE_MARKER: &str = "PROCESS-PROVIDER-OPAQUE-A4-T09-T11";
    const SYSTEM_MARKER: &str = "PROCESS-B2-REASONING-SYSTEM-MARKER";
    const USER_MARKER: &str = "PROCESS-B2-REASONING-USER-MARKER";

    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_process_a4_t09_t11",
        "encrypted_content": ENCRYPTED_MARKER,
        "summary": [{"type": "summary_text", "text": "inspect the process fixture"}],
        "status": "completed",
        "provider": {
            "opaque": OPAQUE_MARKER,
            "trace": [1, true, "preserve only in next HTTP request"]
        }
    });
    let function_call_item = json!({
        "type": "function_call",
        "id": "fc_process_a4_t09_t11",
        "call_id": CALL_ID,
        "name": "read",
        "arguments": FUNCTION_ARGUMENTS,
        "status": "completed",
        "provider": {
            "opaque": OPAQUE_MARKER,
            "future": {"preserve": "exactly"}
        }
    });
    let first = MockResponse::sse(&[
        json!({
            "type": "response.reasoning_summary_text.delta",
            "delta": "inspect the process fixture"
        }),
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": reasoning_item.clone()
        }),
        json!({
            "type": "response.output_item.done",
            "output_index": 1,
            "item": function_call_item.clone()
        }),
        completed(),
    ]);
    let second = MockResponse::sse(&[
        json!({"type": "response.output_text.delta", "delta": "reasoning process final"}),
        completed(),
    ]);
    let server = MockServer::spawn([first, second]).await;
    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-openai-reasoning-process-{}",
        SessionId::new().unwrap()
    ));
    let workspace = temp_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(
        workspace.join("phase4-process.txt"),
        "PROCESS-REASONING-READ-CONTENT",
    )
    .unwrap();
    let config =
        write_config_with_reasoning(&temp_dir, server.base_url(), KEY_ENV, &["read"], "high");
    let system_prompt = format!("Use configured tools and finish the task. {SYSTEM_MARKER}");
    let config_text = std::fs::read_to_string(&config).unwrap();
    let default_system_prompt = "system_prompt = \"Use configured tools and finish the task.\"";
    assert!(config_text.contains(default_system_prompt));
    std::fs::write(
        &config,
        config_text.replace(
            default_system_prompt,
            &format!("system_prompt = {system_prompt:?}"),
        ),
    )
    .unwrap();
    let mut process = RpcProcess::spawn_with_extra_env(
        &config,
        KEY_ENV,
        KEY,
        &[("RUST_LOG", "minicore_agent=debug")],
    )
    .await;

    process
        .send("create", "session.create", json!({"workspace": workspace}))
        .await;
    let session_id = process.response("create").await["result"]["session"]["session_id"].clone();
    let session_id_text = session_id
        .as_str()
        .expect("created session ID must be text")
        .to_owned();
    let user_prompt = format!("read the phase4 process fixture {USER_MARKER}");
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": user_prompt.clone()}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    let reasoning_output = process.event("output_delta").await;
    assert_eq!(reasoning_output["params"]["data"]["channel"], "reasoning");
    assert_eq!(
        reasoning_output["params"]["data"]["delta"],
        "inspect the process fixture"
    );
    process.event("tool_started").await;
    process.event("tool_finished").await;
    let final_output = process.event("output_delta").await;
    assert_eq!(final_output["params"]["data"]["channel"], "text");
    assert_eq!(
        final_output["params"]["data"]["delta"],
        "reasoning process final"
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
    let transcript_text = transcript.to_string();
    assert!(transcript_text.contains("PROCESS-REASONING-READ-CONTENT"));
    assert!(transcript_text.contains("reasoning process final"));
    assert!(!transcript_text.contains(ENCRYPTED_MARKER));
    assert!(!transcript_text.contains(OPAQUE_MARKER));
    process
        .send("close", "session.close", json!({"session_id": session_id}))
        .await;
    process.response("close").await;
    let (observed, stderr) = process.shutdown().await;

    for frame in &observed {
        assert_eq!(frame["jsonrpc"], "2.0");
        let notification = frame["method"] == "agent.event" && frame.get("id").is_none();
        let response = frame.get("id").is_some()
            && (frame.get("result").is_some() ^ frame.get("error").is_some());
        assert!(
            notification || response,
            "every process stdout line must be a valid JSON-RPC notification or response"
        );
    }
    let rpc_text = serde_json::to_string(&observed).unwrap();
    for private in [ENCRYPTED_MARKER, OPAQUE_MARKER] {
        assert!(!rpc_text.contains(private));
        assert!(!stderr.contains(private));
    }
    for private in [SYSTEM_MARKER, USER_MARKER] {
        assert!(!stderr.contains(private));
    }
    assert!(!rpc_text.contains(KEY));
    assert!(!stderr.contains(KEY));

    let session_dir = temp_dir.join("data").join("sessions").join(session_id_text);
    for file_name in ["conversation.log", "session.json", "manifest.json"] {
        let contents = std::fs::read(session_dir.join(file_name))
            .unwrap_or_else(|error| panic!("failed to read persisted {file_name}: {error}"));
        let contents = String::from_utf8_lossy(&contents);
        assert!(!contents.contains(ENCRYPTED_MARKER));
        assert!(!contents.contains(OPAQUE_MARKER));
        assert!(!contents.contains(KEY));
    }

    let requests = server.finish().await;
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(
            request.header("authorization"),
            Some("Bearer PROCESS-REASONING-API-KEY-SECRET")
        );
        assert!(!String::from_utf8_lossy(request.body()).contains(KEY));
    }
    let second_body = requests[1].json_body();
    assert_eq!(second_body["store"], false);
    let expected_second_input = json!([
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": concat!(
                    "Honor message roles and the tool-call protocol. ",
                    "Use only declared tools and match every tool result to its call."
                )
            }]
        },
        {
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": system_prompt
            }]
        },
        {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": user_prompt
            }]
        },
        {
            "type": "reasoning",
            "id": "rs_process_a4_t09_t11",
            "encrypted_content": "enc::PROCESS-A4-T09-T11::AAECAwQFBgcICQ==",
            "summary": [{
                "type": "summary_text",
                "text": "inspect the process fixture"
            }],
            "status": "completed",
            "provider": {
                "opaque": "PROCESS-PROVIDER-OPAQUE-A4-T09-T11",
                "trace": [1, true, "preserve only in next HTTP request"]
            }
        },
        {
            "type": "function_call",
            "id": "fc_process_a4_t09_t11",
            "call_id": "process-reasoning-read-call",
            "name": "read",
            "arguments": r#"{ "path": "phase4-process.txt" }"#,
            "status": "completed",
            "provider": {
                "opaque": "PROCESS-PROVIDER-OPAQUE-A4-T09-T11",
                "future": {"preserve": "exactly"}
            }
        },
        {
            "type": "function_call_output",
            "call_id": "process-reasoning-read-call",
            "output": "1: PROCESS-REASONING-READ-CONTENT",
            "status": "completed"
        }
    ]);
    assert_eq!(second_body["input"], expected_second_input);
    assert!(String::from_utf8_lossy(requests[1].body()).contains(ENCRYPTED_MARKER));
    assert!(String::from_utf8_lossy(requests[1].body()).contains(OPAQUE_MARKER));
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_process_bash_removes_model_credentials_and_preserves_public_environment() {
    for marker in [
        PROCESS_BASH_COMMAND_MARKER,
        PROCESS_BASH_PATH_MARKER,
        PROCESS_BASH_CONTENT_MARKER,
    ] {
        assert!(environment_probe_command().contains(marker));
    }
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
            ("RUST_LOG", "minicore_agent=debug"),
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
    let tool_result_content = tool_result["content"]
        .as_str()
        .expect("Bash tool result content must be text");
    assert_isolated_environment_probe(tool_result_content);
    assert_process_bash_command_markers_absent(tool_result_content);
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
    assert_process_bash_command_markers_absent(&stderr);

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
    const RAW_BODY_SECRET: &str = "PROCESS-RAW-PROVIDER-BODY-SECRET";
    const PROVIDER_MESSAGE: &str = "PROCESS-PROVIDER-ERROR-MESSAGE-SECRET";
    const PROVIDER_ERROR_LOG_MARKER: &str = "provider request failed";

    let server = MockServer::spawn([MockResponse::json(
        500,
        format!(
            r#"{{"error":{{"message":"{PROVIDER_MESSAGE}","type":"server_error","private":"{RAW_BODY_SECRET}"}}}}"#
        ),
    )])
    .await;
    let base_url = server.base_url().to_owned();
    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-openai-error-process-{}",
        SessionId::new().unwrap()
    ));
    let workspace = temp_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config = write_config(&temp_dir, &base_url, KEY_ENV, &[]);
    let mut process =
        RpcProcess::spawn_with_extra_env(&config, KEY_ENV, KEY, &[("RUST_LOG", "trace")]).await;
    process
        .send("create", "session.create", json!({"workspace": workspace}))
        .await;
    let session_id = process.response("create").await["result"]["session"]["session_id"].clone();
    let session_id_text = session_id
        .as_str()
        .expect("created session ID must be text")
        .to_owned();
    process
        .send(
            "send",
            "turn.send",
            json!({"session_id": session_id, "text": "fail safely"}),
        )
        .await;
    let turn = process.response("send").await["result"]["turn"].clone();
    assert_eq!(turn["session_id"], session_id);
    let instance_id_text = turn["instance_id"]
        .as_str()
        .expect("submitted instance ID must be text")
        .to_owned();
    let turn_id_text = turn["turn_id"]
        .as_str()
        .expect("submitted turn ID must be text")
        .to_owned();
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
    server.finish().await;
    let _ = std::fs::remove_dir_all(temp_dir);

    for private in [KEY, RAW_BODY_SECRET, PROVIDER_MESSAGE, base_url.as_str()] {
        assert!(!output.contains(private));
        assert!(!stderr.contains(private));
    }
    assert!(!stderr.to_ascii_lowercase().contains("authorization"));
    let provider_error_lines = stderr
        .lines()
        .filter(|line| line.contains(PROVIDER_ERROR_LOG_MARKER))
        .collect::<Vec<_>>();
    assert!(
        !provider_error_lines.is_empty(),
        "stderr must contain a safe provider request failure marker"
    );
    let provider_error_line = provider_error_lines
        .into_iter()
        .find(|line| {
            log_field_equals(line, "session_id", &session_id_text)
                && log_field_equals(line, "instance_id", &instance_id_text)
                && log_field_equals(line, "turn_id", &turn_id_text)
        })
        .expect("provider failure log must identify the failed session and turn");
    assert_log_field_equals(provider_error_line, "error_kind", "ProviderUnavailable");
    assert_log_field_equals(provider_error_line, "delivery", "Unknown");
    assert_log_field_equals(provider_error_line, "status_class", "server_error");
    assert_log_field_equals(provider_error_line, "session_id", &session_id_text);
    assert_log_field_equals(provider_error_line, "instance_id", &instance_id_text);
    assert_log_field_equals(provider_error_line, "turn_id", &turn_id_text);
    assert_log_field_equals(provider_error_line, "round", "0");
}
