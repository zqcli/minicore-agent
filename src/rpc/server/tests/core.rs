use super::super::{OPERATION_GATES, Reservation};
use super::*;

#[tokio::test]
async fn lifecycle_disk_work_is_deferred_and_reserves_the_session() {
    for method in [
        "session.open",
        "session.close",
        "session.delete",
        "session.compact",
    ] {
        let fixture = RpcFixture::new("deferred-lifecycle", [], &[], ApprovalMode::Auto);
        let base = fixture.base.path().to_owned();
        let workspace = fixture.workspace.clone();
        let agent = fixture.open().await;
        let mut harness = RpcHarness::spawn(agent);
        let session = create_and_open(&mut harness, &workspace).await;
        if matches!(method, "session.open" | "session.delete") {
            harness
                .send(
                    json!("unload"),
                    "session.close",
                    Some(json!({"session_id": session})),
                )
                .await;
            assert_eq!(
                harness.response(json!("unload")).await["result"],
                json!({"ok": true})
            );
        }
        let gate = Arc::new(Gate::new());
        OPERATION_GATES.arm(base.join("data"), gate.clone());
        let mut params = json!({"session_id": session});
        if method == "session.compact" {
            params["operation_id"] = json!("held-start");
        }
        harness.send(json!("held"), method, Some(params)).await;
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .unwrap();
        harness.send(json!("ping"), "agent.ping", None).await;
        assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
        for conflict in ["session.open", "session.close", "session.delete"] {
            harness
                .send(
                    json!(conflict),
                    conflict,
                    Some(json!({"session_id": session})),
                )
                .await;
            let response = harness.response(json!(conflict)).await;
            assert_eq!(
                response["error"]["data"],
                json!({"kind": "session_busy", "retryable": true})
            );
        }
        gate.release();
        assert!(
            harness
                .response(json!("held"))
                .await
                .get("result")
                .is_some()
        );
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn create_reserves_its_id_before_the_record_can_be_discovered() {
    use super::super::{Dispatch, RpcId, RpcRequest};
    let fixture = RpcFixture::new("create-reservation", [], &[], ApprovalMode::Auto);
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(8);
    let mut server = RpcServer {
        agent: Some(agent),
        outbound_tx,
        waiters: tokio::task::JoinSet::new(),
        queries: tokio::task::JoinSet::new(),
        operations: tokio::task::JoinSet::new(),
        reservations: Default::default(),
        query_cancellation: CancellationToken::new(),
    };
    assert!(matches!(
        server.dispatch(RpcRequest {
            id: RpcId::String("create".into()),
            method: "session.create".into(),
            params: Some(json!({"workspace":workspace,"profile":"test"})),
        }),
        Dispatch::Deferred
    ));
    let Reservation::Session(session_id) = *server.reservations.iter().next().unwrap() else {
        panic!("creation must reserve the new session ID");
    };
    let gate = Arc::new(Gate::new());
    OPERATION_GATES.arm(base.join("data"), gate.clone());
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    let Dispatch::Response(response) = server.dispatch(RpcRequest {
        id: RpcId::String("delete".into()),
        method: "session.delete".into(),
        params: Some(json!({"session_id":session_id})),
    }) else {
        panic!("delete must reject an in-progress creation");
    };
    assert_eq!(
        response.json()["error"]["data"],
        json!({"kind":"session_busy","retryable":true})
    );
    gate.release();
    let result = server.operations.join_next().await.unwrap().unwrap();
    server.complete_operation(result).await.unwrap();
    let response = outbound_rx.recv().await.unwrap().json();
    assert_eq!(
        response["result"]["session"]["session_id"],
        json!(session_id)
    );
    server.agent.take().unwrap().shutdown().await.unwrap();
}

#[tokio::test]
async fn slow_close_allows_other_turn_cancellation_and_shutdown_waits_for_it() {
    let fixture = RpcFixture::new(
        "close-responsive",
        [ModelScript::Block, ModelScript::Block],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let first = create_and_open(&mut harness, &workspace).await;
    let second = create_and_open(&mut harness, &workspace).await;
    let gate = Arc::new(Gate::new());
    crate::sessions::PAUSE_BEFORE_JOIN.arm(first.as_str().unwrap().parse().unwrap(), gate.clone());
    for (id, session) in [("first", &first), ("second", &second)] {
        harness
            .send(
                json!(id),
                "turn.send",
                Some(json!({"session_id": session, "text": "wait"})),
            )
            .await;
    }
    let _ = harness.response(json!("first")).await;
    let second_turn = harness.response(json!("second")).await["result"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": first})),
        )
        .await;
    harness
        .send(
            json!("cancel"),
            "turn.cancel",
            Some(turn_params(&second_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"],
        json!({"cancelled": true})
    );
    harness.send(json!("ping"), "agent.ping", None).await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness.send(json!("stop"), "agent.shutdown", None).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), harness.response(json!("stop")))
            .await
            .is_err()
    );
    gate.release();
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    assert_eq!(
        harness.response(json!("stop")).await["result"],
        json!({"ok": true})
    );
    assert!(harness.next_frame().await.is_none());
    harness.join().await.unwrap();
}

#[tokio::test]
async fn answers_after_completion_report_turn_not_found() {
    let fixture = RpcFixture::new(
        "ended-answer",
        [ModelScript::Text("done")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id":session,"text":"finish"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert!(
        harness
            .response(json!("wait"))
            .await
            .get("result")
            .is_some()
    );
    harness
        .send(
            json!("answer"),
            "interaction.answer",
            Some(json!({
                "session_id":turn["session_id"], "loop_id":turn["loop_id"],
                "interaction_id":"int_00000000000000000000000000000001",
                "answer":{"type":"approval","decision":"allow_once"}
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("answer")).await["error"]["data"],
        json!({"kind":"turn_not_found","retryable":false})
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn oversized_frame_reports_parse_error_then_stops() {
    let fixture = RpcFixture::new("oversized-service", [], &[], ApprovalMode::Auto);
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let mut frame = vec![b' '; MAX_RPC_LINE_BYTES + 1];
    frame.push(b'\n');
    harness
        .input
        .as_mut()
        .unwrap()
        .write_all(&frame)
        .await
        .unwrap();
    let response = harness.next_frame().await.unwrap();
    assert_eq!(
        response["error"]["data"],
        json!({"kind":"parse_error","retryable":false})
    );
    assert!(harness.next_frame().await.is_none());
    harness.join().await.unwrap();
}

#[tokio::test]
async fn shutdown_signal_is_observed_while_a_close_is_pending() {
    let fixture = RpcFixture::new("signal-during-close", [], &[], ApprovalMode::Auto);
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let mut agent = fixture.open().await;
    let info = agent
        .create_session(crate::CreateSession {
            workspace,
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let second = agent
        .create_session(crate::CreateSession {
            workspace: info.workspace.clone(),
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let cancellation = agent
        .loaded_session(second.session_id)
        .unwrap()
        .query_cancellation();
    let gate = Arc::new(Gate::new());
    OPERATION_GATES.arm(base.join("data"), gate.clone());
    let (mut input, reader) = tokio::io::duplex(8192);
    let (writer, output) = tokio::io::duplex(8192);
    let (signal, received) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(run_with_io(agent, BufReader::new(reader), writer, async {
        received.await.unwrap();
        Ok(())
    }));
    let request = json!({"jsonrpc":"2.0","id":"close","method":"session.close","params":{"session_id":info.session_id}});
    input
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    assert!(!cancellation.is_cancelled());
    signal.send(()).unwrap();
    tokio::time::timeout(TIMEOUT, cancellation.cancelled())
        .await
        .expect("signal must cancel other sessions before deferred close finishes");
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut server)
            .await
            .is_err()
    );
    gate.release();
    let mut output = BufReader::new(output);
    let mut line = String::new();
    while tokio::time::timeout(TIMEOUT, output.read_line(&mut line))
        .await
        .unwrap()
        .unwrap()
        != 0
    {
        line.clear();
    }
    tokio::time::timeout(TIMEOUT, server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // stdin remained open: only the signal could have initiated shutdown.
    drop(input);
}

#[test]
fn method_labels_cover_exactly_the_dispatch_table() {
    let source = include_str!("../../server/dispatch.rs");
    let dispatch = source
        .split("match method.as_str() {")
        .nth(1)
        .unwrap()
        .split("fn defer_query")
        .next()
        .unwrap();
    let methods: BTreeSet<_> = dispatch
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let (method, _) = line.strip_prefix('"')?.split_once("\" => {")?;
            Some(method)
        })
        .collect();
    assert_eq!(methods, super::super::METHODS.iter().copied().collect());
    assert_eq!(super::super::canonical_method("unknown-secret"), "unknown");
}

#[test]
fn transient_store_and_configuration_errors_have_stable_retryability() {
    use crate::error::StoreError;
    let id = super::super::RpcId::String("error".into());
    for (error, kind, retryable) in [
        (
            AgentError::from(StoreError::Unavailable),
            "store_error",
            true,
        ),
        (AgentError::from(StoreError::Corrupt), "store_error", false),
        (
            AgentError::Config(crate::config::ConfigError::MissingModelApiKey),
            "config_error",
            false,
        ),
    ] {
        let response = super::super::agent_error(id.clone(), &error).json();
        assert_eq!(
            response["error"]["data"],
            json!({"kind": kind, "retryable": retryable})
        );
    }
}

#[test]
fn public_request_defaults_and_strict_shapes_match_the_wire() {
    use crate::agent::{CompactSession, CreateSession, RenameSession, SendMessage, UpdateSession};
    use crate::read::ReadSession;
    fn defaults<T: serde::de::DeserializeOwned + serde::Serialize>(input: Value, expected: Value) {
        let value: T = super::super::decode_params(Some(input.clone())).unwrap();
        assert_eq!(serde_json::to_value(value).unwrap(), expected);
        let mut extra = input;
        extra["unknown"] = json!(true);
        assert!(super::super::decode_params::<T>(Some(extra)).is_err());
    }
    let session = "ses_00000000000000000000000000000001";
    defaults::<CreateSession>(
        json!({"workspace":"/tmp"}),
        json!({"workspace":"/tmp", "profile":"", "model":null, "reasoning":null, "title":null}),
    );
    defaults::<UpdateSession>(
        json!({"session_id":session}),
        json!({"session_id":session, "model":null, "reasoning":null}),
    );
    defaults::<ReadSession>(
        json!({"session_id":session}),
        json!({"session_id":session, "cursor":null, "limit":100, "max_bytes":null, "captured_end":null, "history_revision":null, "view":"canonical", "projection_revision":null}),
    );
    for (input, which) in [
        (json!({"session_id":session,"operation_id":"compact-1"}), 0),
        (json!({"session_id":session,"title":"title"}), 1),
        (json!({"session_id":session,"text":"hello"}), 2),
    ] {
        match which {
            0 => defaults::<CompactSession>(input.clone(), input),
            1 => defaults::<RenameSession>(input.clone(), input),
            _ => defaults::<SendMessage>(input.clone(), input),
        }
    }
}
