use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use minicore_agent::{AgentError, ConfigError, SessionId};
use serde_json::Value;

const TEST_CREDENTIAL_ENV: &str = "MINICORE_STARTUP_TEST_KEY";
const SECRET_MARKER: &str = "STARTUP-PARSE-SECRET-MARKER-9F2A";

fn fresh_temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "minicore-agent-startup-{label}-{}",
        SessionId::new().unwrap()
    ));
    std::fs::create_dir_all(dir.join("data")).unwrap();
    dir
}

fn write_config(temp_dir: &Path, body: &str) -> PathBuf {
    let config_path = temp_dir.join("agent.toml");
    std::fs::write(
        &config_path,
        format!("data_dir = {:?}\n{body}", temp_dir.join("data")),
    )
    .unwrap();
    config_path
}

/// Structurally mirrors the user's `cus-resp` startup config (compaction block
/// commented out): same profile/model names, reasoning, tool list, budget
/// values and approval mode, with a mock base URL and a test credential env.
fn fixed_cus_resp_config() -> String {
    format!(
        r#"event_capacity = 256
default_profile = "coding"

[profiles.coding]
model = "cus-resp-luna"
reasoning = "high"
system_prompt = """
You are a coding agent. Inspect the workspace, use tools when useful,
make focused changes, run relevant checks, and explain the result.
"""
tools = ["read", "write", "edit", "apply_patch", "bash"]
max_tool_rounds = 32
approval = "ask"

[models.cus-resp-luna]
provider = "open_ai_responses"
model = "gpt-5.6-luna"
base_url = "https://example.invalid/v1"
api_key_env = "{TEST_CREDENTIAL_ENV}"
physical_context_window = 372000
output_budget_tokens = 128000
safety_margin_tokens = 4000
supported_reasoning = ["auto", "disabled", "low", "medium", "high"]
supports_tools = true
request_timeout_seconds = 600
"#
    )
}

fn spawn_static(mut command: Command) -> (PathBuf, String, String, std::process::ExitStatus) {
    let temp_dir = command
        .get_envs()
        .find(|(name, _)| *name == "MINICORE_STARTUP_TEMP")
        .map(|(_, value)| PathBuf::from(value.expect("temp dir")))
        .expect("temp dir was not set on the command");
    let output = command.output().unwrap();
    (
        temp_dir,
        String::from_utf8(output.stdout).unwrap_or_default(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        output.status,
    )
}

fn command_with_config(config_path: &Path, temp_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_minicore-agent"));
    command
        .args(["--config", config_path.to_str().unwrap(), "--stdio"])
        .env("MINICORE_STARTUP_TEMP", temp_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Static message constants are part of the diagnostic contract: the cause must
/// reach stderr without ever embedding parsed config text.
#[test]
fn config_error_display_carries_predefined_static_cause() {
    let parse = AgentError::Config(ConfigError::Parse);
    assert_eq!(
        parse.to_string(),
        "invalid configuration: configuration could not be parsed (invalid syntax or unsupported fields)"
    );
    let key = AgentError::Config(ConfigError::MissingModelApiKey);
    assert_eq!(
        key.to_string(),
        "invalid configuration: Model API key environment variable is missing or empty"
    );
}

/// The unsupported `compaction` profile field must still be rejected (no silent
/// accept), and the startup diagnostic must never echo the offending TOML or
/// any config value.
#[test]
fn unsupported_compaction_field_rejected_without_leaking_config_text() {
    let temp_dir = fresh_temp_dir("compaction-reject");
    let config_path = write_config(
        &temp_dir,
        &format!(
            "{}\n[profiles.coding.compaction]\nmode = \"{SECRET_MARKER}\"\n",
            fixed_cus_resp_config()
        ),
    );
    let command = command_with_config(&config_path, &temp_dir);
    let (_dir, stdout, stderr, status) = spawn_static(command);

    assert!(!status.success());
    assert!(
        stdout.is_empty(),
        "stdout must stay empty on startup failure"
    );
    assert!(
        stderr.contains(
            "minicore-agent: invalid configuration: configuration could not be parsed (invalid syntax or unsupported fields)"
        ),
        "stderr must carry the predefined parse cause, got: {stderr}"
    );
    assert!(
        !stderr.contains(SECRET_MARKER)
            && !stderr.contains("compaction")
            && !stderr.contains("[profiles")
            && !stderr.contains("mode ="),
        "stderr must not leak config text or the secret marker, got: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&temp_dir);
}

/// A missing, empty or whitespace-only credential env must map to the distinct
/// static `MissingModelApiKey` diagnostic instead of the generic model error.
#[test]
fn missing_or_empty_api_key_reports_distinct_safe_message() {
    let expected = "minicore-agent: invalid configuration: Model API key environment variable is missing or empty";
    for (label, env_kind) in [
        ("unset", None),
        ("empty", Some("")),
        ("whitespace", Some("   ")),
    ] {
        let temp_dir = fresh_temp_dir(&format!("missing-key-{label}"));
        let config_path = write_config(&temp_dir, &fixed_cus_resp_config());
        let mut command = command_with_config(&config_path, &temp_dir);
        match env_kind {
            None => {
                command.env_remove(TEST_CREDENTIAL_ENV);
            }
            Some(value) => {
                command.env(TEST_CREDENTIAL_ENV, value);
            }
        }
        let (_dir, stdout, stderr, status) = spawn_static(command);
        assert!(
            !status.success(),
            "{label}: startup must fail without a key"
        );
        assert!(stdout.is_empty());
        assert!(
            stderr.contains(expected),
            "{label}: stderr must carry the distinct key diagnostic, got: {stderr}"
        );
        assert!(
            !stderr.contains("model is invalid"),
            "{label}: the generic model diagnostic must no longer be used, got: {stderr}"
        );
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

/// With the unsupported block commented out and a placeholder key supplied,
/// the exact configured startup shape must boot: JSON-RPC ping and shutdown
/// over stdio, clean exit, and no provider call.
#[test]
fn fixed_config_boots_ping_and_shutdown_with_placeholder_key() {
    let temp_dir = fresh_temp_dir("boot-health");
    let config_path = write_config(&temp_dir, &fixed_cus_resp_config());
    let mut command = command_with_config(&config_path, &temp_dir);
    command.env(TEST_CREDENTIAL_ENV, "startup-health-placeholder-key");
    let mut child = command.spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    for request in [
        serde_json::json!({"jsonrpc": "2.0", "id": "ping", "method": "agent.ping", "params": {}}),
        serde_json::json!({"jsonrpc": "2.0", "id": "shutdown", "method": "agent.shutdown", "params": {}}),
    ] {
        serde_json::to_writer(&mut input, &request).unwrap();
        input.write_all(b"\n").unwrap();
    }
    input.flush().unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "startup/stdio must exit cleanly");
    let mut responses = Vec::new();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let frame: Value = serde_json::from_str(line).expect("stdout line must be JSON-RPC");
        if frame.get("id").is_some() {
            responses.push(frame);
        }
    }
    fn response_with_id<'a>(responses: &'a [Value], id: &str) -> &'a Value {
        responses
            .iter()
            .find(|frame| frame["id"] == serde_json::json!(id))
            .expect("expected response id was not observed")
    }
    assert_eq!(
        response_with_id(&responses, "ping")["result"]["version"],
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(
        response_with_id(&responses, "shutdown")["result"]["ok"],
        serde_json::json!(true)
    );
    let _ = std::fs::remove_dir_all(&temp_dir);
}
