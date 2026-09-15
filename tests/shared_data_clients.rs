#![cfg(unix)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::Engine;
use minicore_agent::SessionId;
use serde_json::{Value, json};

#[path = "support/openai_mock.rs"]
mod openai_mock;
#[path = "support/rpc_process.rs"]
mod rpc_process;
use openai_mock::{MockResponse, MockServer};
use rpc_process::RpcProcess;

const KEY_ENV: &str = "MINICORE_SHARED_DATA_TEST_KEY";
const KEY: &str = "shared-data-fixture-secret";

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn call(process: &mut RpcProcess, method: &str, params: Value) -> Value {
    process.send("query", method, params).await;
    let response = process.response("query").await;
    assert!(response.get("error").is_none(), "{method}: {response}");
    response["result"].clone()
}

fn done() -> Value {
    json!({"type":"response.completed", "response":{"status":"completed"}})
}

fn tool(name: &str, id: &str, args: Value) -> MockResponse {
    MockResponse::sse(&[
        json!({"type":"response.output_item.done", "output_index":0,
            "item":{"type":"function_call","call_id":id,"name":name,"arguments":args.to_string()}}),
        done(),
    ])
}

fn git(root: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git fixture failed");
}

// A hunk-oriented consumer retains pages, then extracts the changed sides.
#[derive(Default)]
struct HunkClient {
    pages: Vec<Value>,
}
impl HunkClient {
    fn contents(&self, kind: &str) -> String {
        self.pages
            .iter()
            .flat_map(|page| page["hunks"].as_array().unwrap())
            .flat_map(|hunk| hunk["lines"].as_array().unwrap())
            .filter(|line| line["kind"] == kind)
            .map(|line| line["text"].as_str().unwrap())
            .collect()
    }
}

// A streaming consumer reconstructs logical lines without retaining pages.
#[derive(Default)]
struct LineClient {
    lines: BTreeMap<(String, u64), String>,
}
impl LineClient {
    fn consume(&mut self, page: &Value) {
        for hunk in page["hunks"].as_array().unwrap() {
            for line in hunk["lines"].as_array().unwrap() {
                let kind = line["kind"].as_str().unwrap();
                let index = if kind == "removed" {
                    &line["old_index"]
                } else {
                    &line["new_index"]
                };
                let text = self
                    .lines
                    .entry((kind.to_owned(), index.as_u64().unwrap()))
                    .or_default();
                assert_eq!(
                    text.len() as u64,
                    line["line_byte_offset"].as_u64().unwrap()
                );
                text.push_str(line["text"].as_str().unwrap());
                if line["line_complete"] == true {
                    assert_eq!(text.len() as u64, line["line_byte_len"].as_u64().unwrap());
                }
            }
        }
    }
    fn contents(&self, kind: &str) -> String {
        self.lines
            .iter()
            .filter(|((tag, _), _)| tag == kind)
            .map(|(_, text)| text.as_str())
            .collect()
    }
}

async fn read_diff(
    process: &mut RpcProcess,
    session: &Value,
    reference: &Value,
    budget: usize,
) -> Vec<Value> {
    let mut cursor = Value::Null;
    let mut pages = Vec::new();
    loop {
        let page = call(
            process,
            "changes.diff",
            json!({"session_id":session,"change_ref":reference,
            "context_lines":0,"cursor":cursor,"max_bytes":budget}),
        )
        .await;
        assert!(serde_json::to_vec(&page).unwrap().len() <= budget);
        assert_eq!(page["stale"], false);
        cursor = page["next_cursor"].clone();
        let complete = page["complete"] == true;
        pages.push(page);
        if complete {
            break;
        }
        assert!(!cursor.is_null());
        assert!(pages.len() < 1000);
    }
    pages
}

async fn history(process: &mut RpcProcess, session: &Value) -> Vec<Value> {
    let mut params = json!({"session_id":session,"max_bytes":2048});
    let mut items: BTreeMap<u64, String> = BTreeMap::new();
    loop {
        let page = call(process, "session.read", params.clone()).await;
        assert!(serde_json::to_vec(&page).unwrap().len() <= 2048);
        for chunk in page["items"].as_array().unwrap() {
            let text = items.entry(chunk["index"].as_u64().unwrap()).or_default();
            assert_eq!(text.len() as u64, chunk["offset"].as_u64().unwrap());
            text.push_str(chunk["data"].as_str().unwrap());
        }
        if page["next_cursor"].is_null() {
            break;
        }
        params["cursor"] = page["next_cursor"].clone();
        params["history_revision"] = page["history_revision"].clone();
        params["captured_end"] = page["captured_end"].clone();
    }
    items
        .into_values()
        .map(|text| serde_json::from_str(&text).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_public_clients_share_queries_and_recover_after_process_restart() {
    let fixture = Fixture(
        std::env::temp_dir().join(format!("minicore-clients-{}", SessionId::new().unwrap())),
    );
    let workspace = fixture.0.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    git(&workspace, &["init", "-q"]);
    std::fs::write(workspace.join("value.txt"), b"committed\n").unwrap();
    git(&workspace, &["add", "."]);
    git(&workspace, &["commit", "-qm", "base"]);
    let user = "user dirty\r\n";
    std::fs::write(workspace.join("value.txt"), user).unwrap();
    let output = format!("{}\r\nlast", "é🙂\"\\".repeat(1000));
    let server = MockServer::spawn([
        tool("write", "native-write", json!({"path":"value.txt","content":output})),
        tool("bash", "shell", json!({"command":"printf 'out\\000\\377'; printf 'err' >&2; printf 'external' > external.txt"})),
        MockResponse::sse(&[json!({"type":"response.output_text.delta","delta":"done"}), done()]),
    ]).await;
    let config = fixture.0.join("agent.toml");
    std::fs::write(
        &config,
        format!(
            r#"data_dir = {:?}
default_profile = "test"
event_capacity = 256
[profiles.test]
model = "test"
reasoning = "auto"
system_prompt = "Test data contracts."
tools = ["write", "bash"]
max_tool_rounds = 4
approval = "auto"
[models.test]
provider = "open_ai_responses"
model = "fixture"
base_url = "{}"
api_key_env = "{}"
physical_context_window = 100000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 5
"#,
            fixture.0.join("data"),
            server.base_url(),
            KEY_ENV
        ),
    )
    .unwrap();
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let ping = call(&mut process, "agent.ping", json!({})).await;
    assert!(
        ping["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("changes.diff"))
    );
    let created = call(
        &mut process,
        "session.create",
        json!({"workspace":workspace,"profile":"test"}),
    )
    .await;
    let session = created["session"]["session_id"].clone();
    assert!(session.is_string());
    let (turn, wait) = process
        .send_turn_and_register_wait("run", &session, "update and run")
        .await;
    let _write = process.event("tool_invocation").await;
    let bash = process.event("tool_invocation").await;
    assert_eq!(bash["params"]["data"]["data"]["name"], "bash");
    let bash_ref = bash["params"]["data"]["data"]["tool_ref"].clone();
    let waited = process.response(&wait).await;
    assert_eq!(waited["result"]["persistence"], "persisted");
    let tool_read = call(&mut process, "tool.read", bash_ref.clone()).await;
    assert_eq!(tool_read["execution"]["command"]["exit_code"], 0);
    for (stream, expected) in [
        ("stdout", b"out\x00\xff".as_slice()),
        ("stderr", b"err".as_slice()),
    ] {
        let mut params = bash_ref.clone();
        params["stream"] = json!(stream);
        params["offset"] = json!(0);
        let page = call(&mut process, "tool.output", params).await;
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(page["data"].as_str().unwrap())
                .unwrap(),
            expected
        );
        assert_eq!(page["eof"], true);
    }
    let listed = call(
        &mut process,
        "changes.list",
        json!({"session_id":session,"scope":{"turn":{"loop_id":turn["loop_id"]}}}),
    )
    .await;
    assert_eq!(
        listed["records"].as_array().unwrap().len(),
        1,
        "Bash changes must not gain native attribution"
    );
    let reference = listed["records"][0]["change_ref"].clone();
    let hunk_client = HunkClient {
        pages: read_diff(&mut process, &session, &reference, 65536).await,
    };
    let mut line_client = LineClient::default();
    let small = read_diff(&mut process, &session, &reference, 2048).await;
    assert!(small.len() > 1);
    for page in &small {
        line_client.consume(page);
    }
    for client in [
        hunk_client.contents("removed"),
        line_client.contents("removed"),
    ] {
        assert_eq!(client, user);
    }
    for client in [hunk_client.contents("added"), line_client.contents("added")] {
        assert_eq!(client, output);
    }
    let workspace_changes = call(
        &mut process,
        "changes.list",
        json!({"session_id":session,"scope":"workspace"}),
    )
    .await;
    let external = workspace_changes["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["path"] == "external.txt")
        .unwrap();
    assert!(external["tool_ref"].is_null());
    let external_diff = call(
        &mut process,
        "changes.diff",
        json!({"session_id":session,"change_ref":external["change_ref"]}),
    )
    .await;
    assert_eq!(external_diff["versions_refreshed"], true);
    assert_eq!(external_diff["base_version"]["kind"], "missing");
    assert_eq!(external_diff["hunks"][0]["lines"][0]["text"], "external");
    for (method, extra) in [
        ("workspace.read", json!({"path":"value.txt"})),
        ("workspace.files", json!({})),
        ("workspace.search", json!({"query":"last"})),
        ("session.context", json!({})),
        ("turn.result", json!({"loop_id":turn["loop_id"]})),
    ] {
        let mut params = extra;
        params["session_id"] = session.clone();
        assert!(call(&mut process, method, params).await.is_object());
    }
    let warm_history = history(&mut process, &session).await;
    let (_, stderr) = process.shutdown().await;
    assert!(!stderr.contains(KEY));
    server.finish().await;
    std::fs::remove_dir_all(&workspace).unwrap();
    let mut cold = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    assert_eq!(history(&mut cold, &session).await, warm_history);
    let pages = read_diff(&mut cold, &session, &reference, 2048).await;
    let mut recovered = LineClient::default();
    for page in &pages {
        recovered.consume(page);
    }
    assert_eq!(recovered.contents("added"), output);
    let saved = call(&mut cold, "tool.read", bash_ref).await;
    assert_eq!(saved["execution"]["command"]["exit_code"], 0);
    assert!(!workspace.exists());
    cold.shutdown().await;
}
