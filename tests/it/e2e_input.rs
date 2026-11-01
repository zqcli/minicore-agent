use crate::support::{
    config,
    fixture::Fixture,
    openai_mock::{MockResponse, MockServer},
    responses::{completed, text_response, turn_params},
    rpc_process::RpcProcess,
};
use serde_json::{Value, json};
use std::time::Duration;

fn input_response(id: &str, kind: &str) -> MockResponse {
    MockResponse::sse(&[
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":id,"name":"e2e_input","arguments":serde_json::to_string(&json!({"kind":kind})).unwrap()}}),
        completed(),
    ])
}

async fn pending(process: &mut RpcProcess, session: &Value, kind: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..512 {
            let state = process
                .call_ok("session.state", json!({"session_id":session}))
                .await;
            let pending = &state["snapshot"]["pending_interaction"];
            if !pending.is_null() && pending["kind"]["data"]["answer_kind"] == kind {
                return pending.clone();
            }
            tokio::task::yield_now().await;
        }
        panic!("real Runtime input did not become pending")
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn fixture_uses_real_runtime_text_and_choice_validation_and_same_tool_outputs() {
    let temp = Fixture::new("e2e-input");
    let server = MockServer::spawn([
        input_response("input-text", "text"),
        input_response("input-choice", "single_choice"),
        text_response("done"),
    ])
    .await;
    let path = config::write_config(
        temp.path(),
        config::provider_config(server.base_url(), "MINICORE_E2E_INPUT_KEY", &["read"]),
    );
    let trace = temp.path().join("trace.jsonl");
    let plan = temp.path().join("plan.json");
    std::fs::write(&plan, b"{\"schema\":1,\"input_fixture\":true}").unwrap();
    let mut process = RpcProcess::spawn_with_extra_env(
        &path,
        "MINICORE_E2E_INPUT_KEY",
        "dummy-key",
        &[
            ("MINICORE_AGENT_E2E_TRACE", trace.to_str().unwrap()),
            ("MINICORE_AGENT_E2E_PLAN", plan.to_str().unwrap()),
        ],
    )
    .await;
    let created = process
        .create_session("create", json!({"workspace":temp.path(),"profile":"test"}))
        .await;
    let session = created["session_id"].clone();
    let pushed = process
        .call_ok(
            "input.push",
            json!({"session_id":session,"mode":"steer","text":"test"}),
        )
        .await;
    let turn = process.turn_for_input(&pushed).await;
    let text = pending(&mut process, &session, "text").await;
    assert_eq!(text["loop_id"], turn["loop_id"]);
    assert_eq!(text["request_index"], 0);
    assert_eq!(text["tool_call_id"], "input-text");
    assert_eq!(text["tool_name"], "e2e_input");
    let mut params = turn_params(&turn);
    params["interaction_id"] = text["interaction_id"].clone();
    params["answer"] = json!({"type":"choice","index":0});
    process
        .send("invalid-text-kind", "interaction.answer", params.clone())
        .await;
    assert_eq!(
        process.response("invalid-text-kind").await["error"]["data"]["kind"],
        "invalid_state"
    );
    assert_eq!(
        pending(&mut process, &session, "text").await["interaction_id"],
        text["interaction_id"]
    );
    const ANSWER: &str = "private-e2e-text-answer";
    params["answer"] = json!({"type":"text","text":ANSWER});
    process.call_ok("interaction.answer", params).await;
    let choice = pending(&mut process, &session, "single_choice").await;
    assert_eq!(choice["loop_id"], turn["loop_id"]);
    assert_eq!(choice["request_index"], 1);
    assert_eq!(choice["tool_call_id"], "input-choice");
    assert_ne!(choice["interaction_id"], text["interaction_id"]);
    assert_eq!(
        choice["kind"]["data"]["choices"].as_array().unwrap().len(),
        2
    );
    let mut params = turn_params(&turn);
    params["interaction_id"] = choice["interaction_id"].clone();
    params["answer"] = json!({"type":"choice","index":99});
    process
        .send("invalid-choice-index", "interaction.answer", params.clone())
        .await;
    assert_eq!(
        process.response("invalid-choice-index").await["error"]["data"]["kind"],
        "invalid_state"
    );
    assert_eq!(
        pending(&mut process, &session, "single_choice").await["interaction_id"],
        choice["interaction_id"]
    );
    params["answer"] = json!({"type":"choice","index":1});
    process.call_ok("interaction.answer", params).await;
    let result = process.call_ok("turn.wait", turn_params(&turn)).await;
    assert_eq!(result["outcome"]["type"], "completed");
    assert_eq!(result["persistence"], "persisted");
    assert_eq!(result["requests"], 3);
    process.shutdown().await;
    let requests = server.finish().await;
    assert!(
        requests[0].json_body()["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "e2e_input")
    );
    for (request, id, expected) in [
        (1, "input-text", json!({"answer":ANSWER})),
        (
            2,
            "input-choice",
            json!({"choice_index":1,"choice":"Second choice"}),
        ),
    ] {
        let body = requests[request].json_body();
        let item = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call_output" && item["call_id"] == id)
            .unwrap();
        let output: Value = serde_json::from_str(item["output"].as_str().unwrap()).unwrap();
        assert!(output == expected, "actual Runtime continuation differs");
    }
    let raw = std::fs::read_to_string(&trace).unwrap();
    assert!(!raw.contains(ANSWER));
    let rows = raw
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let answers = rows
        .iter()
        .filter(|row| row["event"] == "input_fixture_answer")
        .collect::<Vec<_>>();
    assert_eq!(answers.len(), 4);
    assert_eq!(
        answers.iter().filter(|row| row["accepted"] == true).count(),
        2
    );
    assert_eq!(
        answers
            .iter()
            .filter(|row| row["accepted"] == false)
            .count(),
        2
    );
    for row in answers {
        assert_eq!(row["loop_id"], turn["loop_id"]);
        assert!(row["load"].is_string());
        assert!(row["interaction_id"].is_string());
        assert_eq!(row["tool_name"], "e2e_input");
    }
    assert_eq!(rows.last().unwrap()["invalid"], false);
    if let Some(directory) = std::env::var_os("MINICORE_AGENT_E2E_TEST_EVIDENCE") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::copy(
            trace,
            std::path::Path::new(&directory).join("input-fixture.agent.jsonl"),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn observer_without_fixture_opt_in_has_only_product_tools() {
    let temp = Fixture::new("e2e-no-input");
    let server = MockServer::spawn([text_response("done")]).await;
    let path = config::write_config(
        temp.path(),
        config::provider_config(server.base_url(), "MINICORE_E2E_INPUT_KEY", &["read"]),
    );
    let trace = temp.path().join("trace.jsonl");
    let mut process = RpcProcess::spawn_with_extra_env(
        &path,
        "MINICORE_E2E_INPUT_KEY",
        "dummy-key",
        &[("MINICORE_AGENT_E2E_TRACE", trace.to_str().unwrap())],
    )
    .await;
    let session = process
        .create_session("create", json!({"workspace":temp.path(),"profile":"test"}))
        .await;
    let pushed = process
        .call_ok(
            "input.push",
            json!({"session_id":session["session_id"],"mode":"steer","text":"test"}),
        )
        .await;
    let turn = process.turn_for_input(&pushed).await;
    process.call_ok("turn.wait", turn_params(&turn)).await;
    process.shutdown().await;
    let requests = server.finish().await;
    assert!(
        !requests[0].json_body()["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "e2e_input")
    );
}
