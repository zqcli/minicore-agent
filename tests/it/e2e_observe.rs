use crate::support::{
    config,
    fixture::Fixture,
    openai_mock::MockServer,
    responses::{text_response, turn_params},
    rpc_process::RpcProcess,
};
#[cfg(feature = "e2e-observe")]
use serde_json::Value;
use serde_json::json;
#[cfg(feature = "e2e-observe")]
use std::path::{Path, PathBuf};
#[cfg(feature = "e2e-observe")]
use std::time::Duration;

const KEY_ENV: &str = "MINICORE_AGENT_E2E_DUMMY_KEY";
const KEY: &str = "e2e-dummy-secret-sentinel";
const QUESTION: &str = "E2E-PRIVATE-PROMPT-SENTINEL";
const ANSWER: &str = "E2E-PRIVATE-ANSWER-SENTINEL";

#[cfg(not(feature = "e2e-observe"))]
#[tokio::test]
async fn observation_and_fault_environment_are_inert_in_default_builds() {
    let temp = Fixture::new("no-observer-default");
    let server = MockServer::spawn([text_response(ANSWER)]).await;
    let path = config::write_config(
        temp.path(),
        config::provider_config(server.base_url(), KEY_ENV, &[]),
    );
    let trace = temp.path().join("must-not-exist.jsonl");
    let missing = temp.path().join("missing-plan.json");
    let mut process = RpcProcess::spawn_with_extra_env(
        &path,
        KEY_ENV,
        KEY,
        &[
            ("MINICORE_AGENT_E2E_TRACE", trace.to_str().unwrap()),
            ("MINICORE_AGENT_E2E_PLAN", missing.to_str().unwrap()),
            ("MINICORE_AGENT_E2E_RELEASE_DIR", missing.to_str().unwrap()),
        ],
    )
    .await;
    let session = process
        .create_session("create", json!({"workspace":temp.path(),"profile":"test"}))
        .await;
    let pushed = process
        .call_ok(
            "input.push",
            json!({"session_id":session["session_id"],"mode":"steer","text":QUESTION}),
        )
        .await;
    let result = process
        .call_ok("turn.wait", turn_params(&pushed["started"]["turn"]))
        .await;
    assert_eq!(result["outcome"]["type"], "completed");
    assert_eq!(result["persistence"], "persisted");
    process.shutdown().await;
    server.finish().await;
    assert!(!trace.exists());
}

#[cfg(feature = "e2e-observe")]
mod enabled {
    use super::*;

    struct Trace {
        path: PathBuf,
        releases: PathBuf,
        label: &'static str,
    }
    impl Trace {
        async fn spawn(
            temp: &Fixture,
            server: &MockServer,
            label: &'static str,
            plan: Value,
        ) -> (Self, RpcProcess, Value) {
            Self::spawn_tools(temp, server, label, plan, &[]).await
        }

        async fn spawn_tools(
            temp: &Fixture,
            server: &MockServer,
            label: &'static str,
            plan: Value,
            tools: &[&str],
        ) -> (Self, RpcProcess, Value) {
            Self::spawn_options(
                temp,
                label,
                plan,
                config::provider_config(server.base_url(), KEY_ENV, tools),
            )
            .await
        }

        async fn spawn_options(
            temp: &Fixture,
            label: &'static str,
            plan: Value,
            mut options: Value,
        ) -> (Self, RpcProcess, Value) {
            let trace = Self {
                path: temp.path().join("agent.jsonl"),
                releases: temp.path().join("releases"),
                label,
            };
            std::fs::create_dir(&trace.releases).unwrap();
            let plan_path = temp.path().join("plan.json");
            std::fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();
            options["loop"] = json!({"event_capacity":1});
            let path = config::write_config(temp.path(), options);
            let mut process = RpcProcess::spawn_with_extra_env(
                &path,
                KEY_ENV,
                KEY,
                &[
                    ("MINICORE_AGENT_E2E_TRACE", trace.path.to_str().unwrap()),
                    ("MINICORE_AGENT_E2E_PLAN", plan_path.to_str().unwrap()),
                    (
                        "MINICORE_AGENT_E2E_RELEASE_DIR",
                        trace.releases.to_str().unwrap(),
                    ),
                ],
            )
            .await;
            trace.wait(|row| row["event"] == "observer_ready").await;
            let session = process
                .create_session("create", json!({"workspace":temp.path(),"profile":"test"}))
                .await;
            (trace, process, session["session_id"].clone())
        }
        async fn rows(&self) -> Vec<Value> {
            let bytes = match tokio::fs::read(&self.path).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
                Err(error) => panic!("trace read failed: {:?}", error.kind()),
            };
            assert!(bytes.len() <= 16 * 1024 * 1024);
            let mut rows = Vec::new();
            for line in bytes.split_inclusive(|b| *b == b'\n') {
                if !line.ends_with(b"\n") {
                    break;
                }
                assert!(line.len() <= 8192);
                rows.push(serde_json::from_slice(line).unwrap());
                assert!(rows.len() <= 65536);
            }
            rows
        }
        async fn wait(&self, predicate: impl Fn(&Value) -> bool) -> Value {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(row) = self.rows().await.into_iter().rev().find(&predicate) {
                        return row;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("selected observer boundary must be reached")
        }
        async fn release(&self, id: &str) {
            tokio::fs::write(self.releases.join(format!("{id}.release")), b"")
                .await
                .unwrap();
        }
        async fn finished(&self) -> Vec<Value> {
            let rows = self.rows().await;
            assert_eq!(rows.first().unwrap()["event"], "observer_ready");
            assert_eq!(rows.last().unwrap()["event"], "observer_end");
            for (index, row) in rows.iter().enumerate() {
                assert_eq!(row["event_seq"], index + 1);
                assert_eq!(row["schema"], "agent-e2e-1");
            }
            let end = rows.last().unwrap();
            assert_eq!(end["invalid"], false);
            assert_eq!(end["trace_dropped"], 0);
            assert_eq!(end["unconsumed_steps"], 0);
            let counters = &end["counters"];
            for (name, gauge) in counters["gauges"].as_object().unwrap() {
                assert_eq!(gauge["current"], 0, "{name}: {gauge}");
                assert!(gauge["peak"].is_u64());
            }
            assert!(
                counters["gauges"]["control_staging_charged_bytes"]["peak"]
                    .as_u64()
                    .unwrap()
                    <= 320 * 1024 * 1024
            );
            assert!(
                counters["gauges"]["source_charged_bytes"]["peak"]
                    .as_u64()
                    .unwrap()
                    <= 256 * 1024 * 1024
            );
            assert!(counters["work"]["normalize_durable"].get("bytes").is_none());
            assert!(counters["work"]["normalize_public"].get("bytes").is_none());
            assert_eq!(
                counters["result_queries"].as_u64().unwrap(),
                counters["result_pages"].as_u64().unwrap()
                    + counters["result_errors"].as_u64().unwrap()
            );
            assert_eq!(
                counters["result_pages"].as_u64().unwrap(),
                counters["result_available_pages"].as_u64().unwrap()
                    + counters["result_pending_pages"].as_u64().unwrap()
                    + counters["result_unavailable_pages"].as_u64().unwrap()
            );
            let body = tokio::fs::read_to_string(&self.path).await.unwrap();
            for private in [KEY, QUESTION, ANSWER, self.path.to_str().unwrap()] {
                assert!(!body.contains(private), "observer leaked a private value");
            }
            if let Some(directory) = std::env::var_os("MINICORE_AGENT_E2E_TEST_EVIDENCE") {
                let directory = Path::new(&directory);
                std::fs::create_dir_all(directory).unwrap();
                std::fs::copy(
                    &self.path,
                    directory.join(format!("{}.agent.jsonl", self.label)),
                )
                .unwrap();
            }
            println!(
                "agent_e2e_observation={}",
                json!({"case":self.label,"counters":counters,"trace_rows":rows.len()})
            );
            rows
        }
    }

    fn step(id: &str, turn: u64, at: &str, action: &str) -> Value {
        json!({"id":id,"session":1,"turn":turn,"at":at,"action":action})
    }
    async fn push(process: &mut RpcProcess, session: &Value, text: &str, mode: &str) -> Value {
        process
            .call_ok(
                "input.push",
                json!({"session_id":session,"mode":mode,"text":text}),
            )
            .await
    }
    fn peak(rows: &[Value], name: &str) -> u64 {
        rows.last().unwrap()["counters"]["gauges"][name]["peak"]
            .as_u64()
            .unwrap()
    }

    #[tokio::test]
    async fn real_stdio_terminal_loss_still_has_result_and_measured_snapshot_send_and_hash_work() {
        let temp = Fixture::new("observed-terminal-loss");
        let answer = format!("{ANSWER}{}", "x".repeat(24 * 1024));
        let server = MockServer::spawn([text_response(&answer), text_response("second")]).await;
        let (trace,mut process,session)=Trace::spawn(&temp,&server,"terminal-loss",json!({"schema":1,"steps":[
        step("drop-tail",1,"runtime_events","drop_terminal"),step("persist-ready",1,"before_persist","pause")]})).await;
        let pushed = push(&mut process, &session, QUESTION, "steer").await;
        let turn = &pushed["started"]["turn"];
        trace
            .wait(|row| row["event"] == "fault_ready" && row["id"] == "persist-ready")
            .await;
        let pending = process.call_ok("turn.result", turn_params(turn)).await;
        assert_eq!(pending["availability"], "pending");
        assert!(pending["result_id"].is_null());
        assert!(pending["items"].as_array().unwrap().is_empty());
        let state = process
            .call_ok("session.state", json!({"session_id":session}))
            .await;
        assert_eq!(state["snapshot"]["execution"]["type"], "finishing");
        assert_eq!(state["snapshot"]["completions"]["through_seq"], 0);
        let queued = push(&mut process, &session, "automatic successor", "follow_up").await;
        assert!(queued["started"].is_null());
        trace.release("persist-ready").await;
        let waited = process.call_ok("turn.wait", turn_params(turn)).await;
        assert_eq!(waited["outcome"]["type"], "completed");
        assert_eq!(waited["persistence"], "persisted");
        assert_eq!(waited["next"]["input"]["id"], queued["id"]);
        process
            .call_ok("turn.wait", turn_params(&waited["next"]["turn"]))
            .await;
        let mut cursor = Value::Null;
        let mut result_id = Value::Null;
        let mut reconstructed = std::collections::BTreeMap::<u64, String>::new();
        let mut pages = 0;
        loop {
            let mut request = turn_params(turn);
            request["max_bytes"] = json!(4096);
            request["cursor"] = cursor.clone();
            if !result_id.is_null() {
                request["result_id"] = result_id.clone();
            }
            let page = process.call_ok("turn.result", request).await;
            if result_id.is_null() {
                result_id = page["result_id"].clone();
            } else {
                assert_eq!(page["result_id"], result_id);
            }
            assert_eq!(page["total"], 2);
            for chunk in page["items"].as_array().unwrap() {
                let value = reconstructed
                    .entry(chunk["index"].as_u64().unwrap())
                    .or_default();
                assert_eq!(value.len() as u64, chunk["offset"].as_u64().unwrap());
                value.push_str(chunk["data"].as_str().unwrap());
                if chunk["complete"] == true {
                    assert_eq!(value.len() as u64, chunk["total_bytes"].as_u64().unwrap());
                }
            }
            pages += 1;
            let next = page["next_cursor"].clone();
            if next.is_null() {
                break;
            }
            assert!(pages < 64);
            assert_ne!(next, cursor);
            cursor = next;
        }
        assert!(pages > 1);
        let item: Value = serde_json::from_str(&reconstructed[&1]).unwrap();
        assert!(
            item["item"]["data"]["content"][0]["data"] == answer,
            "canonical body differs"
        );
        trace
            .wait(|row| {
                row["event"] == "counters"
                    && row["work"]["result_range_encode"]["calls"]
                        .as_u64()
                        .is_some_and(|calls| calls >= pages - 1)
                    && row["gauges"]["watchers"]["current"] == 0
                    && row["gauges"]["active_owner_objects"]["current"] == 1
            })
            .await;
        let (observed, _) = process.shutdown().await;
        server.finish().await;
        assert!(!observed.iter().any(|frame| frame.pointer("/params/type")
            == Some(&json!("output_delta"))
            && frame.pointer("/params/data/turn/loop_id") == Some(&turn["loop_id"])));
        let rows = trace.finished().await;
        assert!(rows.iter().any(|row| row["event"] == "runtime_tail_dropped"
            && row["buffered_events_discarded"].as_u64().unwrap() > 0));
        let counters = &rows.last().unwrap()["counters"];
        assert_eq!(counters["work"]["result_projection_hash"]["calls"], 1);
        assert_eq!(counters["work"]["result_range_encode"]["calls"], pages - 1);
        assert!(
            counters["work"]["page_prefix_scan"]["calls"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            counters["work"]["page_prefix_scan"]["bytes"]
                .as_u64()
                .unwrap()
                <= pages * 4096
        );
        assert!(
            counters["work"]["result_projection_hash"]["bytes"]
                .as_u64()
                .unwrap()
                > answer.len() as u64
        );
        assert!(
            counters["work"]["normalize_durable"]["calls"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(peak(&rows, "watchers") > 0);
        assert!(peak(&rows, "control_staging_charged_bytes") > 0);
        assert!(peak(&rows, "completion_values") >= 2);
        assert!(peak(&rows, "late_wait_slots") >= 1);
        let sent: Vec<_> = rows
            .iter()
            .filter(|row| row["event"] == "snapshot_sent")
            .collect();
        assert!(!sent.is_empty());
        for send in sent {
            let sample = rows
                .iter()
                .find(|row| {
                    row["event"] == "snapshot_sample"
                        && row["load"] == send["load"]
                        && row["revision"] == send["revision"]
                })
                .unwrap();
            assert!(
                sample["sampled_ns"].as_u64().unwrap()
                    <= send["write_started_ns"].as_u64().unwrap()
            );
            assert!(
                send["write_started_ns"].as_u64().unwrap() <= send["written_ns"].as_u64().unwrap()
            );
            assert!(rows.iter().any(|row| row["event"] == "snapshot_encoded"
                && row["load"] == send["load"]
                && row["revision"] == send["revision"]));
        }
    }

    #[tokio::test]
    async fn real_stdio_eviction_keeps_source_charged_until_the_held_page_releases() {
        let temp = Fixture::new("observed-held-source");
        let server = MockServer::spawn([
            text_response(format!("{ANSWER}{}", "a".repeat(12 * 1024))),
            text_response("second"),
        ])
        .await;
        let plan = json!({"schema":1,"limits":{"sources_per_load":1,"sources":1,"source_bytes_per_load":65536,"all_source_bytes":65536},"steps":[
        step("first-ready",1,"before_persist","pause"),step("rollback-a",1,"append","rollback"),step("rollback-b",2,"append","rollback"),step("page-a",1,"result_page","pause")]});
        let (trace, mut process, session) = Trace::spawn(&temp, &server, "held-source", plan).await;
        let first = push(&mut process, &session, QUESTION, "steer").await;
        let turn = first["started"]["turn"].clone();
        trace
            .wait(|row| row["event"] == "fault_ready" && row["id"] == "first-ready")
            .await;
        let queued = push(&mut process, &session, "follow up", "follow_up").await;
        assert!(queued["started"].is_null());
        trace.release("first-ready").await;
        let result = process.call_ok("turn.wait", turn_params(&turn)).await;
        assert_eq!(result["persistence"], "failed");
        assert!(result["next"].is_null());
        assert_eq!(result["queue"]["paused"], true);
        let mut request = turn_params(&turn);
        request["max_bytes"] = json!(1024);
        process.send("held-page", "turn.result", request).await;
        trace
            .wait(|row| row["event"] == "fault_ready" && row["id"] == "page-a")
            .await;
        let before = trace
            .wait(|row| {
                row["event"] == "counters" && row["gauges"]["retained_page_refs"]["current"] == 1
            })
            .await;
        let charged = before["gauges"]["source_charged_bytes"]["current"]
            .as_u64()
            .unwrap();
        assert!(charged > 0);
        assert_eq!(before["gauges"]["catalog_source_bytes"]["current"], charged);
        let resumed = process
            .call_ok("input.resume", json!({"session_id":session}))
            .await;
        assert_eq!(resumed["started"]["input"]["id"], queued["id"]);
        let second = resumed["started"]["turn"].clone();
        let second_result = process.call_ok("turn.wait", turn_params(&second)).await;
        assert_eq!(second_result["persistence"], "failed");
        let state = process
            .call_ok("session.state", json!({"session_id":session}))
            .await;
        let entries = state["snapshot"]["completions"]["entries"]
            .as_array()
            .unwrap();
        assert_eq!(entries[0]["unavailable_reason"], "expired");
        assert_eq!(entries[1]["unavailable_reason"], "resource_limit");
        trace
            .wait(|row| {
                row["event"] == "gauge"
                    && row["name"] == "catalog_source_bytes"
                    && row["current"] == 0
            })
            .await;
        let held = trace.rows().await;
        assert_eq!(
            held.iter()
                .rev()
                .find(|row| row["event"] == "gauge" && row["name"] == "source_charged_bytes")
                .unwrap()["current"],
            charged
        );
        trace.release("page-a").await;
        let page = process.response("held-page").await;
        assert_eq!(page["result"]["availability"], "live");
        assert!(page["result"]["next_cursor"].is_object());
        trace
            .wait(|row| {
                row["event"] == "gauge"
                    && row["name"] == "source_charged_bytes"
                    && row["current"] == 0
            })
            .await;
        let mut next = turn_params(&turn);
        next["cursor"] = page["result"]["next_cursor"].clone();
        next["result_id"] = page["result"]["result_id"].clone();
        let expired = process.call_ok("turn.result", next).await;
        assert_eq!(expired["availability"], "unavailable");
        assert_eq!(expired["unavailable_reason"], "expired");
        process.shutdown().await;
        server.finish().await;
        let rows = trace.finished().await;
        assert_eq!(peak(&rows, "source_charged_count"), 1);
        assert_eq!(peak(&rows, "retained_page_refs"), 1);
        assert_eq!(
            rows.last().unwrap()["counters"]["work"]["result_projection_hash"]["calls"],
            0
        );
        assert_eq!(
            rows.last().unwrap()["counters"]["work"]["source_hash"]["calls"],
            1
        );
    }

    #[tokio::test]
    async fn real_stdio_unknown_append_remains_blocked_and_uncertain_until_reopen() {
        let temp = Fixture::new("observed-unknown");
        let server = MockServer::spawn([text_response(ANSWER)]).await;
        let (trace, mut process, session) = Trace::spawn(
            &temp,
            &server,
            "unknown",
            json!({"schema":1,"steps":[step("unknown",1,"append","unknown")]}),
        )
        .await;
        let first = push(&mut process, &session, QUESTION, "steer").await;
        let turn = first["started"]["turn"].clone();
        let result = process.call_ok("turn.wait", turn_params(&turn)).await;
        assert_eq!(result["persistence"], "failed");
        assert!(result["next"].is_null());
        let state = process
            .call_ok("session.state", json!({"session_id":session}))
            .await;
        assert_eq!(state["snapshot"]["block_reason"], "persistence");
        assert_eq!(
            state["snapshot"]["completions"]["entries"][0]["persistence_uncertain"],
            true
        );
        let page = process.call_ok("turn.result", turn_params(&turn)).await;
        assert_eq!(page["persistence"], "failed");
        assert_eq!(page["persistence_uncertain"], true);
        process
            .send(
                "blocked-resume",
                "input.resume",
                json!({"session_id":session}),
            )
            .await;
        assert_eq!(
            process.response("blocked-resume").await["error"]["data"]["kind"],
            "session_blocked"
        );
        process
            .call_ok("session.close", json!({"session_id":session}))
            .await;
        let opened = process
            .call_ok("session.open", json!({"session_id":session}))
            .await;
        assert_ne!(opened["load"], state["snapshot"]["load"]);
        let history = process.call_ok("turn.result", turn_params(&turn)).await;
        assert_eq!(history["availability"], "unavailable");
        assert_eq!(history["unavailable_reason"], "not_retained");
        process.shutdown().await;
        server.finish().await;
        let rows = trace.finished().await;
        assert!(
            rows.iter()
                .any(|row| row["event"] == "fault_applied" && row["reason"] == "append_unknown")
        );
    }

    #[tokio::test]
    async fn real_stdio_close_releases_a_persistence_pause_and_joins_observers() {
        let temp = Fixture::new("observed-close-pause");
        let server = MockServer::spawn([text_response(ANSWER)]).await;
        let (trace, mut process, session) = Trace::spawn(
            &temp,
            &server,
            "close-pause",
            json!({"schema":1,"steps":[step("close-pause",1,"before_persist","pause")]}),
        )
        .await;
        let first = push(&mut process, &session, QUESTION, "steer").await;
        let turn = first["started"]["turn"].clone();
        trace.wait(|row| row["event"] == "fault_ready").await;
        process
            .send("wait-before-close", "turn.wait", turn_params(&turn))
            .await;
        process
            .call_ok("session.close", json!({"session_id":session}))
            .await;
        let result = process.response("wait-before-close").await;
        assert_eq!(result["result"]["persistence"], "persisted");
        assert_eq!(result["result"]["outcome"]["type"], "completed");
        process.shutdown().await;
        server.finish().await;
        let rows = trace.finished().await;
        assert!(
            rows.iter()
                .any(|row| row["event"] == "fault_released" && row["reason"] == "close_or_cancel")
        );
        assert!(!trace.releases.join("close-pause.release").exists());
    }

    #[tokio::test]
    async fn real_stdio_close_cancels_a_held_page_and_releases_its_source() {
        let temp = Fixture::new("observed-page-close");
        let server = MockServer::spawn([text_response(ANSWER)]).await;
        let (trace, mut process, session) = Trace::spawn(
            &temp,
            &server,
            "page-close",
            json!({"schema":1,"steps":[
            step("rollback",1,"append","rollback"),step("page-close",1,"result_page","pause")]}),
        )
        .await;
        let first = push(&mut process, &session, QUESTION, "steer").await;
        let turn = first["started"]["turn"].clone();
        assert_eq!(
            process.call_ok("turn.wait", turn_params(&turn)).await["persistence"],
            "failed"
        );
        process
            .send("held-page", "turn.result", turn_params(&turn))
            .await;
        trace
            .wait(|row| row["event"] == "fault_ready" && row["id"] == "page-close")
            .await;
        process
            .call_ok("session.close", json!({"session_id":session}))
            .await;
        assert_eq!(
            process.response("held-page").await["error"]["data"]["kind"],
            "session_not_loaded"
        );
        process.shutdown().await;
        server.finish().await;
        let rows = trace.finished().await;
        assert!(peak(&rows, "source_charged_bytes") > 0);
        assert_eq!(peak(&rows, "retained_page_refs"), 1);
        assert_eq!(rows.last().unwrap()["counters"]["result_errors"], 1);
        assert!(rows.iter().any(|row| row["event"] == "fault_released"
            && row["id"] == "page-close"
            && row["reason"] == "close_or_cancel"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn observer_transport_variables_are_scrubbed_from_owned_tool_processes() {
        use crate::support::openai_mock::MockResponse;
        use crate::support::responses::completed;
        let temp = Fixture::new("observed-tool-env");
        let command = "if [ -n \"${MINICORE_AGENT_E2E_TRACE-}${MINICORE_AGENT_E2E_PLAN-}${MINICORE_AGENT_E2E_RELEASE_DIR-}\" ]; then printf e2e-env-leaked; else printf e2e-env-scrubbed; fi";
        let server=MockServer::spawn([MockResponse::sse(&[json!({"type":"response.output_item.done","output_index":0,"item":{
            "type":"function_call","call_id":"e2e-tool-env","name":"bash","arguments":serde_json::to_string(&json!({"command":command})).unwrap()}}),completed()]),text_response(ANSWER)]).await;
        let (trace, mut process, session) =
            Trace::spawn_tools(&temp, &server, "tool-env", json!({"schema":1}), &["bash"]).await;
        let first = push(&mut process, &session, QUESTION, "steer").await;
        let result = process
            .call_ok("turn.wait", turn_params(&first["started"]["turn"]))
            .await;
        assert_eq!(result["outcome"]["type"], "completed");
        process.shutdown().await;
        let requests = server.finish().await;
        let body = requests[1].json_body();
        let output = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call_output")
            .unwrap()["output"]
            .as_str()
            .unwrap();
        assert!(output.contains("e2e-env-scrubbed"));
        assert!(!output.contains("e2e-env-leaked"));
        let rows = trace.finished().await;
        assert!(
            rows.iter()
                .any(|row| row["event"] == "completion_registered")
        );
    }

    #[tokio::test]
    async fn enabled_observer_rejects_non_loopback_provider_configuration_before_execution() {
        let temp = Fixture::new("observed-provider-boundary");
        let config = config::write_config(
            temp.path(),
            config::provider_config("https://example.invalid", KEY_ENV, &[]),
        );
        let trace = temp.path().join("agent.jsonl");
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_minicore-agent"))
                .args(["--config", config.to_str().unwrap(), "--stdio"])
                .env(KEY_ENV, KEY)
                .env("MINICORE_AGENT_E2E_TRACE", &trace)
                .stdin(std::process::Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let rows = tokio::fs::read_to_string(trace)
            .await
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.first().unwrap()["event"], "observer_ready");
        assert_eq!(rows.last().unwrap()["event"], "observer_end");
        assert_eq!(rows.last().unwrap()["invalid"], true);
        assert!(!rows.iter().any(|row| row["event"] == "loop_installed"));
    }

    #[tokio::test]
    async fn real_stdio_megabyte_unsaved_source_is_prepared_once_and_reused_across_pages() {
        use crate::support::openai_mock::MockResponse;
        use crate::support::responses::completed;
        let temp = Fixture::new("observed-large-source");
        std::fs::write(temp.path().join("file.txt"), "content").unwrap();
        let chunk = "x".repeat(55 * 1024);
        let mut responses = Vec::new();
        for index in 0..6 {
            let mut events = (0..4)
                .map(|_| json!({"type":"response.output_text.delta","delta":chunk}))
                .collect::<Vec<_>>();
            let mut output = vec![
                json!({"type":"message","id":format!("msg-{index}"),"role":"assistant",
                "content":[{"type":"output_text","text":chunk.repeat(4)}]}),
            ];
            if index < 5 {
                let call = json!({"type":"function_call","id":format!("fc-{index}"),"call_id":format!("read-{index}"),"name":"read","arguments":"{\"path\":\"file.txt\"}"});
                events
                    .push(json!({"type":"response.output_item.done","output_index":1,"item":call}));
                output.push(call);
            }
            let mut terminal = completed();
            terminal["response"]["output"] = json!(output);
            events.push(terminal);
            responses.push(MockResponse::sse(&events));
        }
        let server = MockServer::spawn(responses).await;
        let mut options = config::provider_config(server.base_url(), KEY_ENV, &["read"]);
        options["models"]["main"]["physical_context_window"] = json!(10_000_000);
        options["profiles"]["test"]["max_tool_rounds"] = json!(8);
        options["compaction"] = json!({"enabled":false});
        let (trace,mut process,session)=Trace::spawn_options(&temp,"large-source",json!({"schema":1,"steps":[
            step("large-rollback",1,"append","rollback"),step("large-page",1,"result_page","pause")]}),options).await;
        let pushed = push(&mut process, &session, QUESTION, "steer").await;
        let turn = pushed["started"]["turn"].clone();
        let result = process.call_ok("turn.wait", turn_params(&turn)).await;
        assert_eq!(
            result["outcome"]["type"], "completed",
            "failure_kind={} model_error_kind={} requests={}",
            result["outcome"]["kind"], result["outcome"]["model_error"]["kind"], result["requests"]
        );
        assert_eq!(result["persistence"], "failed");
        assert_eq!(result["requests"], 6);
        let mut request = turn_params(&turn);
        request["max_bytes"] = json!(1024 * 1024);
        process.send("first-page", "turn.result", request).await;
        trace
            .wait(|row| row["event"] == "fault_ready" && row["id"] == "large-page")
            .await;
        let held = trace
            .wait(|row| {
                row["event"] == "counters" && row["gauges"]["retained_page_refs"]["current"] == 1
            })
            .await;
        let charged = held["gauges"]["source_charged_bytes"]["current"]
            .as_u64()
            .unwrap();
        assert!(charged > 1024 * 1024);
        assert_eq!(
            held["gauges"]["retained_page_reference_weighted_bytes"]["current"],
            charged
        );
        trace.release("large-page").await;
        let first = process.response("first-page").await;
        assert!(first.get("error").is_none());
        let mut page = first["result"].clone();
        let result_id = page["result_id"].clone();
        let total = page["total"].as_u64().unwrap();
        let mut reconstructed = std::collections::BTreeMap::<u64, String>::new();
        let mut pages = 0;
        loop {
            assert_eq!(page["availability"], "live");
            assert_eq!(page["result_id"], result_id);
            assert_eq!(page["total"], total);
            for item in page["items"].as_array().unwrap() {
                let text = reconstructed
                    .entry(item["index"].as_u64().unwrap())
                    .or_default();
                assert_eq!(text.len() as u64, item["offset"].as_u64().unwrap());
                text.push_str(item["data"].as_str().unwrap());
                if item["complete"] == true {
                    assert_eq!(text.len() as u64, item["total_bytes"].as_u64().unwrap());
                }
            }
            pages += 1;
            if page["next_cursor"].is_null() {
                break;
            }
            assert!(pages < 32);
            let mut next = turn_params(&turn);
            next["cursor"] = page["next_cursor"].clone();
            next["result_id"] = result_id.clone();
            next["max_bytes"] = json!(1024 * 1024);
            page = process.call_ok("turn.result", next).await;
        }
        assert!(pages > 1);
        assert_eq!(reconstructed.len() as u64, total);
        let mut assistants = 0;
        for body in reconstructed.values() {
            let item: Value = serde_json::from_str(body).unwrap();
            if item["item"]["type"] == "assistant" {
                let content = item["item"]["data"]["content"].as_array().unwrap();
                let text = content
                    .iter()
                    .filter(|part| part["type"] == "text")
                    .map(|part| part["data"].as_str().unwrap())
                    .collect::<String>();
                assert!(text == chunk.repeat(4), "canonical text differs");
                assistants += 1;
            }
        }
        assert_eq!(assistants, 6);
        trace
            .wait(|row| {
                row["event"] == "result_query"
                    && row["result_id"] == result_id
                    && row["has_next"] == false
            })
            .await;
        let after = trace.rows().await;
        let normalize_before = &held["work"]["normalize_durable"]["calls"];
        process
            .call_ok("session.close", json!({"session_id":session}))
            .await;
        process.shutdown().await;
        server.finish().await;
        let rows = trace.finished().await;
        let counters = &rows.last().unwrap()["counters"];
        assert_eq!(counters["work"]["source_prepare"]["calls"], 1);
        assert_eq!(counters["work"]["source_hash"]["calls"], 1);
        assert_eq!(counters["work"]["result_projection_hash"]["calls"], 0);
        assert_eq!(
            &counters["work"]["normalize_durable"]["calls"],
            normalize_before
        );
        assert_eq!(counters["result_available_pages"], pages);
        assert!(
            after
                .iter()
                .filter(|row| row["event"] == "result_query")
                .count()
                >= pages as usize
        );
        assert_eq!(peak(&rows, "source_charged_bytes"), charged);
    }
}
