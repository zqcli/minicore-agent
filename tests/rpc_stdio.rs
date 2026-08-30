use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};

use minicore_runtime::SessionId;

use serde_json::{Value, json};

const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;
const TEST_CREDENTIAL_ENV: &str = "MINICORE_RPC_STDIO_TEST_KEY";

struct RpcProcess {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    temp_dir: PathBuf,
}

impl RpcProcess {
    fn send_raw(&mut self, frame: &[u8]) {
        self.input.write_all(frame).unwrap();
        self.input.flush().unwrap();
    }

    fn send_json(&mut self, request: Value) {
        let mut frame = serde_json::to_vec(&request).unwrap();
        frame.push(b'\n');
        self.send_raw(&frame);
    }

    fn response(&mut self) -> Value {
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).unwrap() > 0);
        serde_json::from_str(&line).unwrap()
    }

    fn finish(mut self) -> ExitStatus {
        drop(self.input);
        let status = self.child.wait().unwrap();
        let _ = std::fs::remove_dir_all(&self.temp_dir);
        status
    }
}

fn spawn_server() -> RpcProcess {
    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-test-{}",
        SessionId::new().unwrap()
    ));
    let config_path = write_server_config(&temp_dir);
    let mut child = Command::new(env!("CARGO_BIN_EXE_minicore-agent"))
        .args(["--config", config_path.to_str().unwrap(), "--stdio"])
        .env(TEST_CREDENTIAL_ENV, "dummy-test-credential")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    RpcProcess {
        input: child.stdin.take().unwrap(),
        output: BufReader::new(child.stdout.take().unwrap()),
        child,
        temp_dir,
    }
}

fn write_server_config(temp_dir: &std::path::Path) -> PathBuf {
    let data_dir = temp_dir.join("data");
    let config_path = temp_dir.join("agent.toml");
    std::fs::create_dir_all(temp_dir).unwrap();
    std::fs::write(
        &config_path,
        format!(
            r#"data_dir = {:?}
event_capacity = 256
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = "RPC framing test system prompt"
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "{TEST_CREDENTIAL_ENV}"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = false
request_timeout_seconds = 30
"#,
            data_dir
        ),
    )
    .unwrap();
    config_path
}

fn assert_error(response: Value, code: i64, id: Value) {
    assert_eq!(response["jsonrpc"], json!("2.0"));
    assert_eq!(response["id"], id);
    assert_eq!(response["error"]["code"], json!(code));
    assert!(response["error"]["data"]["retryable"].is_boolean());
}

fn assert_domain_error(response: Value, code: i64, id: Value, kind: &str) {
    assert_eq!(response["error"]["data"]["kind"], json!(kind));
    assert_error(response, code, id);
}

fn assert_ping(response: Value, id: Value) {
    assert_eq!(response["jsonrpc"], json!("2.0"));
    assert_eq!(response["id"], id);
    assert_eq!(response["result"]["version"], json!("0.1.0"));
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

fn response_with_id<'response>(responses: &'response [Value], id: &Value) -> &'response Value {
    responses
        .iter()
        .find(|response| response.get("id") == Some(id))
        .expect("expected JSON-RPC response ID was not observed")
}

#[test]
fn debug_logging_keeps_stdout_json_rpc_only_and_uses_safe_stderr_markers() {
    const MALFORMED_FRAME_MARKER: &str = "PROCESS-B2-RPC-MALFORMED-FRAME-SECRET";
    const INVALID_SHAPE_MARKER: &str = "PROCESS-B2-RPC-INVALID-SHAPE-SECRET";
    const UNKNOWN_METHOD_MARKER: &str = "PROCESS-B2-RPC-UNKNOWN-METHOD-SECRET";
    const UNKNOWN_PARAMS_MARKER: &str = "PROCESS-B2-RPC-UNKNOWN-PARAMS-SECRET";
    const STARTUP_LOG_MARKER: &str = "agent startup";
    const RPC_DISPATCH_LOG_MARKER: &str = "rpc dispatch";

    let temp_dir = std::env::temp_dir().join(format!(
        "minicore-agent-rpc-debug-log-test-{}",
        SessionId::new().unwrap()
    ));
    let config_path = write_server_config(&temp_dir);
    let mut child = Command::new(env!("CARGO_BIN_EXE_minicore-agent"))
        .args(["--config", config_path.to_str().unwrap(), "--stdio"])
        .env(TEST_CREDENTIAL_ENV, "dummy-test-credential")
        .env("RUST_LOG", "minicore_agent=debug")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input
        .write_all(format!(r#"{{"private":"{MALFORMED_FRAME_MARKER}""#).as_bytes())
        .unwrap();
    input.write_all(b"\n").unwrap();
    for request in [
        json!({
            "jsonrpc": "2.0",
            "id": "invalid-shape",
            "method": 7,
            "private": INVALID_SHAPE_MARKER
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "unknown-method",
            "method": UNKNOWN_METHOD_MARKER,
            "params": {"private": UNKNOWN_PARAMS_MARKER}
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "ping",
            "method": "agent.ping",
            "params": {}
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "shutdown",
            "method": "agent.shutdown",
            "params": {}
        }),
    ] {
        serde_json::to_writer(&mut input, &request).unwrap();
        input.write_all(b"\n").unwrap();
    }
    input.flush().unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let mut response_ids = Vec::new();
    let mut responses = Vec::new();
    for line in stdout.lines() {
        let frame: Value = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("stdout line was not JSON-RPC JSON: {error}: {line}"));
        assert_eq!(frame["jsonrpc"], "2.0");
        let notification = frame["method"] == "agent.event"
            && frame.get("id").is_none()
            && frame.get("params").is_some()
            && frame.get("result").is_none()
            && frame.get("error").is_none();
        let response = frame.get("id").is_some()
            && frame.get("method").is_none()
            && (frame.get("result").is_some() ^ frame.get("error").is_some());
        assert!(
            notification || response,
            "stdout contained JSON that was not a JSON-RPC response or Agent event"
        );
        if let Some(id) = frame.get("id") {
            response_ids.push(id.clone());
            responses.push(frame);
        }
    }
    assert!(response_ids.contains(&Value::Null));
    assert!(response_ids.contains(&json!("invalid-shape")));
    assert!(response_ids.contains(&json!("unknown-method")));
    assert!(response_ids.contains(&json!("ping")));
    assert!(response_ids.contains(&json!("shutdown")));
    assert_eq!(
        response_with_id(&responses, &Value::Null)["error"]["code"],
        -32_700
    );
    assert_eq!(
        response_with_id(&responses, &json!("invalid-shape"))["error"]["code"],
        -32_600
    );
    assert_eq!(
        response_with_id(&responses, &json!("unknown-method"))["error"]["code"],
        -32_601
    );
    assert_eq!(
        response_with_id(&responses, &json!("ping"))["result"]["version"],
        "0.1.0"
    );
    assert_eq!(
        response_with_id(&responses, &json!("shutdown"))["result"]["ok"],
        true
    );
    let has_safe_log_marker =
        stderr.contains(STARTUP_LOG_MARKER) || stderr.contains(RPC_DISPATCH_LOG_MARKER);
    let has_safe_parse_marker = stderr.lines().any(|line| {
        line.contains("rpc request rejected") && log_field_equals(line, "kind", "parse_error")
    });
    let has_safe_invalid_marker = stderr.lines().any(|line| {
        line.contains("rpc request rejected") && log_field_equals(line, "kind", "invalid_request")
    });
    let has_safe_unknown_marker = stderr.lines().any(|line| {
        line.contains(RPC_DISPATCH_LOG_MARKER) && log_field_equals(line, "method", "unknown")
    });
    let config_path_text = config_path.to_string_lossy().into_owned();
    let stderr_is_redacted = !stderr.contains(&config_path_text)
        && !stderr.contains(MALFORMED_FRAME_MARKER)
        && !stderr.contains(INVALID_SHAPE_MARKER)
        && !stderr.contains(UNKNOWN_METHOD_MARKER)
        && !stderr.contains(UNKNOWN_PARAMS_MARKER);
    let stdout_is_redacted = [
        MALFORMED_FRAME_MARKER,
        INVALID_SHAPE_MARKER,
        UNKNOWN_METHOD_MARKER,
        UNKNOWN_PARAMS_MARKER,
    ]
    .into_iter()
    .all(|marker| !stdout.contains(marker));
    let _ = std::fs::remove_dir_all(&temp_dir);

    assert!(stderr_is_redacted);
    assert!(stdout_is_redacted);
    assert!(
        has_safe_log_marker,
        "stderr must contain a stable safe startup or RPC dispatch marker"
    );
    assert!(has_safe_parse_marker);
    assert!(has_safe_invalid_marker);
    assert!(has_safe_unknown_marker);
}

#[test]
fn stdio_classifies_frames_and_preserves_valid_ids() {
    let mut process = spawn_server();

    process.send_raw(b"not-json\n");
    assert_error(process.response(), -32_700, Value::Null);

    process.send_json(json!([]));
    assert_error(process.response(), -32_600, Value::Null);

    process.send_json(json!({"jsonrpc": "2.0", "id": 7}));
    assert_error(process.response(), -32_600, json!(7));

    process.send_json(json!({
        "jsonrpc": "1.0",
        "id": "wrong-version",
        "method": "agent.ping"
    }));
    assert_error(process.response(), -32_600, json!("wrong-version"));

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "null-params",
        "method": "agent.ping",
        "params": null
    }));
    assert_error(process.response(), -32_602, json!("null-params"));

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "nonempty-params",
        "method": "agent.ping",
        "params": {"unexpected": true}
    }));
    assert_error(process.response(), -32_602, json!("nonempty-params"));

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "array-params",
        "method": "agent.ping",
        "params": []
    }));
    assert_error(process.response(), -32_602, json!("array-params"));

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "omitted-params",
        "method": "agent.ping"
    }));
    assert_ping(process.response(), json!("omitted-params"));

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": -7,
        "method": "agent.ping",
        "params": {}
    }));
    assert_ping(process.response(), json!(-7));

    for invalid_id in [json!(1.5), json!(null), json!(true), json!([]), json!({})] {
        process.send_json(json!({
            "jsonrpc": "2.0",
            "id": invalid_id,
            "method": "agent.ping"
        }));
        assert_error(process.response(), -32_600, Value::Null);
    }

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "shutdown",
        "method": "agent.shutdown",
        "params": {}
    }));
    let response = process.response();
    assert_eq!(response["id"], json!("shutdown"));
    assert_eq!(response["result"]["ok"], json!(true));
    assert!(process.finish().success());
}

#[test]
fn eof_shuts_down_without_an_extra_frame() {
    let mut process = spawn_server();
    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "before-eof",
        "method": "agent.ping"
    }));
    assert_ping(process.response(), json!("before-eof"));
    let temp_dir = process.temp_dir.clone();
    drop(process.input);

    let mut trailing = String::new();
    assert_eq!(process.output.read_line(&mut trailing).unwrap(), 0);
    assert!(process.child.wait().unwrap().success());
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[test]
fn complete_config_exposes_stable_profile_model_and_session_behavior() {
    let mut process = spawn_server();

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "complete-ping",
        "method": "agent.ping",
        "params": {}
    }));
    assert_ping(process.response(), json!("complete-ping"));

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "profiles",
        "method": "profile.list",
        "params": {}
    }));
    let profiles = process.response();
    assert_eq!(profiles["result"]["profiles"][0]["id"], "test");
    assert_eq!(profiles["result"]["profiles"][0]["model"], "main");

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "models",
        "method": "model.list",
        "params": {}
    }));
    let models = process.response();
    assert_eq!(models["result"]["models"][0]["id"], "main");
    assert_eq!(models["result"]["models"][0]["context_window"], 8_000);
    assert_eq!(models["result"]["models"][0]["supports_tools"], false);

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "sessions",
        "method": "session.list",
        "params": {}
    }));
    assert_eq!(process.response()["result"]["sessions"], json!([]));

    let missing = SessionId::new().unwrap();
    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "open-missing",
        "method": "session.open",
        "params": {"session_id": missing}
    }));
    assert_domain_error(
        process.response(),
        -32_001,
        json!("open-missing"),
        "session_not_found",
    );

    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "state-unloaded",
        "method": "session.state",
        "params": {"session_id": missing}
    }));
    assert_domain_error(
        process.response(),
        -32_002,
        json!("state-unloaded"),
        "session_not_loaded",
    );

    let workspace = process.temp_dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    process.send_json(json!({
        "jsonrpc": "2.0",
        "id": "profile-missing",
        "method": "session.create",
        "params": {"workspace": workspace, "profile": "missing"}
    }));
    assert_domain_error(
        process.response(),
        -32_008,
        json!("profile-missing"),
        "profile_not_found",
    );

    assert!(process.finish().success());
}

#[test]
fn oversized_frame_is_rejected_before_unbounded_buffering_and_exits() {
    let mut process = spawn_server();
    let mut frame = vec![b'x'; MAX_RPC_LINE_BYTES + 1];
    frame.push(b'\n');
    process.send_raw(&frame);
    assert_error(process.response(), -32_700, Value::Null);
    assert!(process.finish().success());
}
