use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};

use minicore_runtime::SessionId;

use serde_json::{Value, json};

const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;

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
    let data_dir = temp_dir.join("data");
    let config_path = temp_dir.join("agent.toml");
    std::fs::create_dir_all(&temp_dir).unwrap();
    std::fs::write(
        &config_path,
        format!("data_dir = {:?}\nevent_capacity = 256\n", data_dir),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_minicore-agent"))
        .args(["--config", config_path.to_str().unwrap(), "--stdio"])
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
fn empty_config_exposes_stable_profile_model_and_session_lists() {
    let mut process = spawn_server();

    for (id, method, field) in [
        ("profiles", "profile.list", "profiles"),
        ("models", "model.list", "models"),
        ("sessions", "session.list", "sessions"),
    ] {
        process.send_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": {}
        }));
        let response = process.response();
        assert_eq!(response["id"], json!(id));
        assert_eq!(response["result"][field], json!([]));
    }

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
        "params": {"workspace": workspace}
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
