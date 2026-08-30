use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use minicore_runtime::SessionId;
use serde_json::{Value, json};

#[path = "support/openai_mock.rs"]
mod openai_mock;
use openai_mock::{ChunkGate, MockResponse, MockServer};
#[path = "support/rpc_process.rs"]
mod rpc_process;
use rpc_process::RpcProcess;

const KEY_ENV: &str = "MINICORE_RPC_SOAK_KEY";
const KEY: &str = "RPC-SOAK-API-KEY-SECRET";
const CANCEL_INDEX: usize = 4;
const PAUSE_INDEX: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Plan {
    Text,
    Read,
    Write,
    Cancel,
    Pause,
}

struct TurnExpectation {
    turn: Value,
    prompt: String,
    terminal: &'static str,
    assistant: Option<String>,
    tool: Option<ToolExpectation>,
}

struct ToolExpectation {
    call_id: String,
    name: &'static str,
    content: String,
}

const A_PLANS: [Plan; 10] = [
    Plan::Text,
    Plan::Read,
    Plan::Text,
    Plan::Write,
    Plan::Text,
    Plan::Text,
    Plan::Read,
    Plan::Text,
    Plan::Write,
    Plan::Text,
];
const B_PLANS: [Plan; 10] = [
    Plan::Text,
    Plan::Write,
    Plan::Text,
    Plan::Read,
    Plan::Cancel,
    Plan::Text,
    Plan::Pause,
    Plan::Write,
    Plan::Read,
    Plan::Text,
];

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

fn tool_response(call_id: String, name: &str, arguments: Value) -> MockResponse {
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

fn gated_response(events: &[Value]) -> (MockResponse, ChunkGate) {
    MockResponse::sse(events).with_chunk_gate()
}

fn assistant_text(session: &str, index: usize, plan: Plan) -> Option<String> {
    match plan {
        Plan::Text => Some(format!("{session} final {index}")),
        Plan::Read => Some(format!("{session} read final {index}")),
        Plan::Write => Some(format!("{session} write final {index}")),
        Plan::Pause => Some("p".repeat(1_024)),
        Plan::Cancel => None,
    }
}

fn tool_expectation(session: &str, index: usize, plan: Plan) -> Option<ToolExpectation> {
    match plan {
        Plan::Read => Some(ToolExpectation {
            call_id: format!("{session}-read-{index}"),
            name: "read",
            content: format!(
                "1: {}",
                if session == "session-a" {
                    "SESSION-A-READ-CONTENT"
                } else {
                    "SESSION-B-READ-CONTENT"
                }
            ),
        }),
        Plan::Write => {
            let content = format!("{session}-written-{index}");
            Some(ToolExpectation {
                call_id: format!("{session}-write-{index}"),
                name: "write",
                content: format!(
                    "wrote {} bytes to {session}-write-{index}.txt",
                    content.len()
                ),
            })
        }
        Plan::Text | Plan::Cancel | Plan::Pause => None,
    }
}

fn responses_for(
    session: &str,
    index: usize,
    plan: Plan,
    cancel: &MockResponse,
    pause: &MockResponse,
) -> Vec<MockResponse> {
    match plan {
        Plan::Text => vec![text_response(assistant_text(session, index, plan).unwrap())],
        Plan::Read => vec![
            tool_response(
                tool_expectation(session, index, plan).unwrap().call_id,
                "read",
                json!({"path": format!("{session}-input.txt")}),
            ),
            text_response(assistant_text(session, index, plan).unwrap()),
        ],
        Plan::Write => vec![
            tool_response(
                tool_expectation(session, index, plan).unwrap().call_id,
                "write",
                json!({
                    "path": format!("{session}-write-{index}.txt"),
                    "content": format!("{session}-written-{index}")
                }),
            ),
            text_response(assistant_text(session, index, plan).unwrap()),
        ],
        Plan::Cancel => vec![cancel.clone()],
        Plan::Pause => vec![pause.clone()],
    }
}

fn provider_rounds(plan: Plan) -> usize {
    match plan {
        Plan::Read | Plan::Write => 2,
        Plan::Text | Plan::Cancel | Plan::Pause => 1,
    }
}

async fn create_session(
    process: &mut RpcProcess,
    id: &str,
    workspace: &Path,
    title: &str,
) -> Value {
    process
        .send(
            id,
            "session.create",
            json!({"workspace": workspace, "profile": "soak", "title": title}),
        )
        .await;
    process.response(id).await["result"]["session"].clone()
}

async fn state(process: &mut RpcProcess, id: &str, session_id: &Value) -> Value {
    process
        .send(id, "session.state", json!({"session_id": session_id}))
        .await;
    process.response(id).await["result"].clone()
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

fn turn_params(turn: &Value) -> Value {
    json!({
        "session_id": turn["session_id"],
        "instance_id": turn["instance_id"],
        "turn_id": turn["turn_id"]
    })
}

struct SoakControl<'a> {
    server: &'a MockServer,
    cancel_gate: &'a ChunkGate,
    pause_gate: &'a ChunkGate,
    handler_finished: usize,
}

async fn run_turn(
    control: &mut SoakControl<'_>,
    process: &mut RpcProcess,
    session: &str,
    index: usize,
    plan: Plan,
    session_id: &Value,
) -> TurnExpectation {
    let prefix = format!("{session}-{index}");
    let prompt = format!("{session}-turn-{index}");
    let (turn, wait_id) = process
        .send_turn_and_register_wait(&prefix, session_id, &prompt)
        .await;
    let rounds = provider_rounds(plan);
    let handler_target = control.handler_finished.saturating_add(rounds);
    let expected_terminal = if plan == Plan::Cancel {
        "cancelled_by_user"
    } else {
        "completed"
    };
    let outcome = match plan {
        Plan::Cancel => {
            control.server.wait_for_requests(handler_target).await;
            process
                .send(
                    &format!("{prefix}-cancel"),
                    "turn.cancel",
                    turn_params(&turn),
                )
                .await;
            assert_eq!(
                process.response(&format!("{prefix}-cancel")).await["result"]["cancelled"],
                true
            );
            let outcome = process.response(&wait_id).await["result"].clone();
            control.cancel_gate.release();
            control
                .server
                .wait_for_handler_finished(handler_target)
                .await;
            outcome
        }
        Plan::Pause => {
            control.server.wait_for_requests(handler_target).await;
            // The server is serial and this response is gated, so finishing every prior
            // handler leaves only the captured Pause handler active.
            control
                .server
                .wait_for_handler_finished(control.handler_finished)
                .await;
            let fully_written_baseline = control.server.fully_written_count();
            let checkpoint = process.observed().len();
            control.pause_gate.release();
            control
                .server
                .wait_for_fully_written(fully_written_baseline + 1)
                .await;
            assert_eq!(process.observed().len(), checkpoint);
            process.response(&wait_id).await["result"].clone()
        }
        Plan::Text | Plan::Read | Plan::Write => process.response(&wait_id).await["result"].clone(),
    };
    control.handler_finished = handler_target;
    assert_eq!(outcome["turn_id"], turn["turn_id"]);
    assert_eq!(outcome["terminal"], expected_terminal);
    let state = state(process, &format!("{prefix}-state"), session_id).await;
    assert_eq!(state["status"], "idle");
    assert_eq!(state["last_terminal"]["turn_id"], turn["turn_id"]);
    assert_eq!(state["last_terminal"]["terminal"], expected_terminal);
    TurnExpectation {
        turn,
        prompt,
        terminal: expected_terminal,
        assistant: assistant_text(session, index, plan),
        tool: tool_expectation(session, index, plan),
    }
}

fn entry_seq(entry: &Value) -> u64 {
    entry["seq"].as_u64().expect("transcript sequence")
}

fn exact_turn_entry<'a>(entries: &[&'a Value], turn_id: &str, kind: &str) -> &'a Value {
    let matches = entries
        .iter()
        .copied()
        .filter(|entry| entry["turn_id"] == turn_id)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one {kind} for {turn_id}"
    );
    matches[0]
}

fn assert_final_assistant(entry: &Value, text: &str) -> u64 {
    assert_eq!(entry["model"], "soak");
    assert_eq!(entry["text"], text);
    assert!(entry["reasoning"].is_null());
    assert!(entry["tool_calls"].as_array().unwrap().is_empty());
    entry_seq(entry)
}

fn assert_tool_assistant(entry: &Value, tool: &ToolExpectation) -> u64 {
    assert_eq!(entry["model"], "soak");
    assert!(entry["text"].is_null());
    assert!(entry["reasoning"].is_null());
    let calls = entry["tool_calls"].as_array().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["tool_call_id"], tool.call_id);
    assert_eq!(calls[0]["name"], tool.name);
    entry_seq(entry)
}

fn validate_transcript(transcript: &Value, expectations: &[TurnExpectation]) {
    assert_eq!(transcript["complete"], true);
    assert_eq!(expectations.len(), 10);
    let expected_ids = expectations
        .iter()
        .map(|expected| expected.turn["turn_id"].as_str().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(expected_ids.len(), 10, "expected Turn IDs must be unique");

    let entries = transcript["entries"].as_array().unwrap();
    let (mut users, mut assistants, mut tool_results, mut summaries, mut terminals) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for tagged in entries {
        let object = tagged.as_object().expect("tagged transcript entry");
        assert_eq!(object.len(), 1, "transcript entry must have one variant");
        let (kind, entry) = object.iter().next().unwrap();
        match kind.as_str() {
            "user_message" => users.push(entry),
            "assistant_message" => assistants.push(entry),
            "tool_result" => tool_results.push(entry),
            "summary" => summaries.push(entry),
            "turn_terminal" => terminals.push(entry),
            other => panic!("unexpected transcript variant {other}"),
        }
    }

    let expected_assistants = expectations
        .iter()
        .map(|expected| {
            usize::from(expected.assistant.is_some()) + usize::from(expected.tool.is_some())
        })
        .sum::<usize>();
    assert!(summaries.is_empty());
    assert_eq!(users.len(), 10);
    assert_eq!(terminals.len(), 10);
    assert_eq!(tool_results.len(), 4);
    assert_eq!(assistants.len(), expected_assistants);
    assert_eq!(entries.len(), 24 + expected_assistants);

    for entry in users
        .iter()
        .chain(&assistants)
        .chain(&tool_results)
        .chain(&terminals)
    {
        let turn_id = entry["turn_id"].as_str().expect("entry Turn ID");
        assert!(expected_ids.contains(turn_id), "unknown Turn ID {turn_id}");
    }

    let mut previous_terminal_seq = None;
    for expected in expectations {
        let turn_id = expected.turn["turn_id"].as_str().unwrap();
        let user = exact_turn_entry(&users, turn_id, "UserMessage");
        let terminal = exact_turn_entry(&terminals, turn_id, "TurnTerminal");
        assert_eq!(user["text"], expected.prompt);
        assert_eq!(user["execution"]["model"], "soak");
        assert_eq!(user["execution"]["reasoning"], "high");
        assert_eq!(user["execution"]["max_tool_rounds"], 4);
        assert_eq!(terminal["terminal"], expected.terminal);
        let user_seq = entry_seq(user);
        let terminal_seq = entry_seq(terminal);
        if let Some(previous) = previous_terminal_seq {
            assert!(previous < user_seq);
        }
        let turn_assistants = assistants
            .iter()
            .copied()
            .filter(|entry| entry["turn_id"] == turn_id)
            .collect::<Vec<_>>();
        let turn_results = tool_results
            .iter()
            .copied()
            .filter(|entry| entry["turn_id"] == turn_id)
            .collect::<Vec<_>>();

        match (&expected.assistant, &expected.tool) {
            (None, None) => {
                assert!(turn_assistants.is_empty());
                assert!(turn_results.is_empty());
                assert!(user_seq < terminal_seq);
            }
            (Some(text), None) => {
                assert_eq!(turn_assistants.len(), 1);
                assert!(turn_results.is_empty());
                let final_seq = assert_final_assistant(turn_assistants[0], text);
                assert!(user_seq < final_seq && final_seq < terminal_seq);
            }
            (Some(text), Some(tool)) => {
                assert_eq!(turn_assistants.len(), 2);
                assert_eq!(turn_results.len(), 1);
                let tool_assistant = turn_assistants
                    .iter()
                    .find(|entry| !entry["tool_calls"].as_array().unwrap().is_empty())
                    .expect("tool-call AssistantMessage");
                let final_assistant = turn_assistants
                    .iter()
                    .find(|entry| entry["text"].is_string())
                    .expect("final AssistantMessage");
                let result = turn_results[0];
                assert_eq!(result["tool_call_id"], tool.call_id);
                assert_eq!(result["tool_name"], tool.name);
                assert_eq!(result["outcome"], "success");
                assert_eq!(result["content"], tool.content);
                let tool_seq = assert_tool_assistant(tool_assistant, tool);
                let final_seq = assert_final_assistant(final_assistant, text);
                assert!(
                    user_seq < tool_seq
                        && tool_seq < entry_seq(result)
                        && entry_seq(result) < final_seq
                        && final_seq < terminal_seq
                );
            }
            (None, Some(_)) => panic!("tool Turn must have a final AssistantMessage"),
        }
        previous_terminal_seq = Some(terminal_seq);
    }

    let sequences = entries
        .iter()
        .map(|entry| entry_seq(entry.as_object().unwrap().values().next().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(
        sequences,
        (1..=u64::try_from(sequences.len()).unwrap()).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_session_twenty_turn_rpc_soak_is_durable_and_shutdown_clean() {
    let (cancel_response, cancel_gate) = gated_response(&[completed()]);
    let mut pause_events = (0..1_024)
        .map(|_| json!({"type": "response.output_text.delta", "delta": "p"}))
        .collect::<Vec<_>>();
    pause_events.push(completed());
    let (pause_response, pause_gate) = gated_response(&pause_events);
    let mut responses = Vec::new();
    for index in 0..10 {
        responses.extend(responses_for(
            "session-a",
            index,
            A_PLANS[index],
            &cancel_response,
            &pause_response,
        ));
        responses.extend(responses_for(
            "session-b",
            index,
            B_PLANS[index],
            &cancel_response,
            &pause_response,
        ));
    }
    let server = MockServer::spawn(responses).await;
    let base = test_dir();
    let workspace_a = base.join("workspace-a");
    let workspace_b = base.join("workspace-b");
    std::fs::create_dir_all(&workspace_a).unwrap();
    std::fs::create_dir_all(&workspace_b).unwrap();
    std::fs::write(
        workspace_a.join("session-a-input.txt"),
        "SESSION-A-READ-CONTENT",
    )
    .unwrap();
    std::fs::write(
        workspace_b.join("session-b-input.txt"),
        "SESSION-B-READ-CONTENT",
    )
    .unwrap();
    let config = write_config(&base, server.base_url());
    let mut process = RpcProcess::spawn(&config, KEY_ENV, KEY).await;
    let mut session_a = create_session(&mut process, "create-a", &workspace_a, "A").await;
    let session_b = create_session(&mut process, "create-b", &workspace_b, "B").await;
    assert_ne!(session_a["session_id"], session_b["session_id"]);
    let session_a_id = session_a["session_id"].clone();
    let session_b_id = session_b["session_id"].clone();
    let mut expected_a = Vec::new();
    let mut expected_b = Vec::new();
    let handler_finished = {
        let mut control = SoakControl {
            server: &server,
            cancel_gate: &cancel_gate,
            pause_gate: &pause_gate,
            handler_finished: 0,
        };
        for index in 0..10 {
            expected_a.push(
                run_turn(
                    &mut control,
                    &mut process,
                    "session-a",
                    index,
                    A_PLANS[index],
                    &session_a_id,
                )
                .await,
            );
            expected_b.push(
                run_turn(
                    &mut control,
                    &mut process,
                    "session-b",
                    index,
                    B_PLANS[index],
                    &session_b_id,
                )
                .await,
            );

            if index == 4 {
                let old_instance = session_a["instance_id"].clone();
                process
                    .send(
                        "mid-close-a",
                        "session.close",
                        json!({"session_id": session_a_id}),
                    )
                    .await;
                assert_eq!(
                    process.response("mid-close-a").await["result"],
                    json!({"ok": true})
                );
                process
                    .send(
                        "mid-open-a",
                        "session.open",
                        json!({"session_id": session_a_id}),
                    )
                    .await;
                session_a = process.response("mid-open-a").await["result"]["session"].clone();
                assert_ne!(session_a["instance_id"], old_instance);
                assert_eq!(session_a["model"], "soak");
                assert_eq!(session_a["reasoning"], "high");
            }
        }
        control
            .server
            .wait_for_handler_finished(control.handler_finished)
            .await;
        control.handler_finished
    };
    assert!(
        expected_a
            .iter()
            .all(|expected| expected.terminal == "completed")
    );
    assert_eq!(
        expected_b
            .iter()
            .filter(|expected| expected.terminal == "cancelled_by_user")
            .count(),
        1
    );
    assert_eq!(B_PLANS[CANCEL_INDEX], Plan::Cancel);
    assert_eq!(B_PLANS[PAUSE_INDEX], Plan::Pause);

    let transcript_a = transcript(&mut process, "transcript-a", &session_a_id).await;
    let transcript_b = transcript(&mut process, "transcript-b", &session_b_id).await;
    validate_transcript(&transcript_a, &expected_a);
    validate_transcript(&transcript_b, &expected_b);
    let transcript_a_text = transcript_a.to_string();
    let transcript_b_text = transcript_b.to_string();
    assert!(!transcript_a_text.contains("session-b-turn"));
    assert!(!transcript_b_text.contains("session-a-turn"));
    assert!(transcript_a_text.contains("SESSION-A-READ-CONTENT"));
    assert!(transcript_b_text.contains("SESSION-B-READ-CONTENT"));
    for index in [3, 8] {
        assert_eq!(
            std::fs::read_to_string(workspace_a.join(format!("session-a-write-{index}.txt")))
                .unwrap(),
            format!("session-a-written-{index}")
        );
    }
    for index in [1, 7] {
        assert_eq!(
            std::fs::read_to_string(workspace_b.join(format!("session-b-write-{index}.txt")))
                .unwrap(),
            format!("session-b-written-{index}")
        );
    }

    let (frames, stderr) = process.shutdown().await;
    assert!(!frames.iter().any(|frame| {
        frame.pointer("/params/type").and_then(Value::as_str) == Some("interaction_requested")
    }));
    assert!(!stderr.contains(KEY));
    assert_eq!(handler_finished, 28);
    let requests = server.finish().await;
    assert_eq!(requests.len(), 28);
    let observed_call_ids = requests
        .iter()
        .flat_map(|request| {
            request.json_body()["input"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .filter(|item| item["type"] == "function_call_output")
        .map(|item| item["call_id"].as_str().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    let expected_call_ids = expected_a
        .iter()
        .chain(&expected_b)
        .filter_map(|expected| expected.tool.as_ref())
        .map(|tool| tool.call_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(expected_call_ids.len(), 8);
    assert_eq!(observed_call_ids, expected_call_ids);
    let _ = std::fs::remove_dir_all(base);
}
