use super::super::{Dispatch, OPERATION_GATES, RpcId, RpcOutbound, RpcRequest};
use super::*;
use crate::test_support::TempDir;

fn request(method: &str, params: Value) -> RpcRequest {
    RpcRequest {
        id: RpcId::String(method.into()),
        method: method.into(),
        params: Some(params),
    }
}

fn server(agent: Agent) -> (RpcServer, tokio::sync::mpsc::Receiver<RpcOutbound>) {
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(16);
    (
        RpcServer {
            agent: Some(agent),
            outbound_tx,
            waiters: Default::default(),
            queries: Default::default(),
            operations: Default::default(),
            reservations: Default::default(),
            query_cancellation: CancellationToken::new(),
        },
        outbound_rx,
    )
}

async fn hold(server: &mut RpcServer, request: RpcRequest) -> Arc<Gate> {
    let gate = Arc::new(Gate::new());
    OPERATION_GATES.arm(server.agent().config().data_dir.clone(), gate.clone());
    assert!(matches!(server.dispatch(request), Dispatch::Deferred));
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    gate
}

async fn complete(
    server: &mut RpcServer,
    output: &mut tokio::sync::mpsc::Receiver<RpcOutbound>,
) -> Value {
    let result = tokio::time::timeout(TIMEOUT, server.operations.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.complete_operation(result).await.unwrap();
    output.recv().await.unwrap().json()
}

fn assert_busy(dispatch: Dispatch) {
    let Dispatch::Response(response) = dispatch else {
        panic!("conflicting operation was admitted");
    };
    assert_eq!(
        response.json()["error"]["data"],
        json!({"kind":"session_busy", "retryable":true})
    );
}

fn write_config(base: &Path, revision: u64) {
    let data_dir = toml::Value::String(base.join("data").to_string_lossy().into_owned());
    std::fs::write(
        base.join("agent.toml"),
        format!(
            r#"
data_dir = {data_dir}
event_capacity = 256
default_profile = "test"
[profiles.test]
model = "fake"
reasoning = "medium"
system_prompt = "revision-{revision}"
tools = []
approval = "auto"
[models.fake]
provider = "open_ai_responses"
model = "provider-model-{revision}"
base_url = "https://example.invalid/v1"
api_key_env = "PATH"
physical_context_window = 32000
output_budget_tokens = 2000
safety_margin_tokens = 1000
supported_reasoning = ["medium"]
supports_tools = true
[loop]
model_timeout_seconds = {timeout}
[compaction]
enabled = false
"#,
            timeout = 17 + revision
        ),
    )
    .unwrap();
}

async fn reload_agent() -> (Agent, TempDir, PathBuf) {
    let base = TempDir::new("core-review-reload");
    let workspace = base.path().join("workspace");
    tokio::fs::create_dir(&workspace).await.unwrap();
    write_config(base.path(), 0);
    let agent = Agent::open_file_with_models(
        base.path().join("agent.toml"),
        test_models(FakeModel::new([])),
    )
    .await
    .unwrap();
    (agent, base, workspace)
}

async fn create(agent: &mut Agent, workspace: &Path) -> crate::SessionInfo {
    agent
        .create_session(crate::CreateSession {
            workspace: workspace.into(),
            profile: "test".into(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn compaction_cancel_before_preparation_prevents_the_summary_call() {
    let base = TempDir::new("core-review-compaction");
    let workspace = base.path().join("workspace");
    tokio::fs::create_dir(&workspace).await.unwrap();
    let model =
        FakeModel::for_compaction([ModelScript::Text("settled"), ModelScript::Text("summary")]);
    let agent = Agent::open_with_models(
        test_config(base.path().join("data"), &[], ApprovalMode::Auto),
        test_models(model.clone()),
    )
    .await
    .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id":session,"text":"settled history ".repeat(4000)})),
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
    append_compaction_tail(&mut harness, base.path(), &session).await;
    let gate = Arc::new(Gate::new());
    OPERATION_GATES.arm(base.path().join("data"), gate.clone());
    harness
        .send(
            json!("compact"),
            "session.compact",
            Some(json!({"session_id":session,"operation_id":"admitted"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .unwrap();
    for (id, operation_id, cancelled) in [
        ("wrong", "other", false),
        ("cancel", "admitted", true),
        ("again", "admitted", false),
    ] {
        harness
            .send(
                json!(id),
                "session.compact.cancel",
                Some(json!({"session_id":session,"operation_id":operation_id})),
            )
            .await;
        assert_eq!(
            harness.response(json!(id)).await["result"],
            json!({"cancelled":cancelled})
        );
    }
    gate.release();
    let result = harness.response(json!("compact")).await;
    assert_eq!(result["result"]["status"], "failed");
    assert_eq!(result["result"]["failure_kind"], "cancelled");
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    // The same history really requires a summary when a fresh operation is not cancelled.
    harness
        .send(
            json!("next"),
            "session.compact",
            Some(json!({"session_id":session,"operation_id":"next"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("next")).await["result"]["status"],
        "compacted"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    harness.shutdown().await;
}

#[tokio::test]
async fn dropping_unpolled_compaction_preparation_releases_admission() {
    let fixture = RpcFixture::new("dropped-compaction", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let mut agent = fixture.open().await;
    let session = create(&mut agent, &workspace).await;
    let request = crate::CompactSession {
        session_id: session.session_id,
        operation_id: "dropped".into(),
    };
    let future = agent.prepare_compact(request.clone()).unwrap();
    assert!(agent.cancel_compaction(request.clone()).unwrap());
    drop(future);
    assert!(!agent.cancel_compaction(request.clone()).unwrap());
    assert!(matches!(
        agent.prepare_compact(request),
        Err(AgentError::InvalidState)
    ));
    let result = agent
        .compact(crate::CompactSession {
            session_id: session.session_id,
            operation_id: "next".into(),
        })
        .await
        .unwrap();
    assert_eq!(result.status, crate::CompactionStatus::Noop);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_reload_application_preserves_loaded_snapshots_and_releases_reservation() {
    let (mut agent, base, workspace) = reload_agent().await;
    let session = create(&mut agent, &workspace).await;
    write_config(base.path(), 1);
    let path = base.path().join("agent.toml");
    let mut candidate: toml::Value = std::fs::read_to_string(&path).unwrap().parse().unwrap();
    candidate["profiles"]["test"]["model"] = toml::Value::String("other".into());
    let models = candidate["models"].as_table_mut().unwrap();
    let model = models.remove("fake").unwrap();
    models.insert("other".into(), model);
    std::fs::write(&path, toml::to_string(&candidate).unwrap()).unwrap();
    let (mut server, mut output) = server(agent);
    let gate = hold(&mut server, request("agent.reload", json!({}))).await;
    gate.release();
    assert_eq!(
        complete(&mut server, &mut output).await["error"]["data"]["kind"],
        "model_not_found"
    );
    assert_eq!(
        server.agent().config().loop_options.model_timeout_seconds,
        Some(17)
    );
    assert_eq!(
        server
            .agent()
            .loaded_session(session.session_id)
            .unwrap()
            .options_for_test()
            .model_timeout,
        Duration::from_secs(17)
    );
    write_config(base.path(), 1);
    assert!(matches!(
        server.dispatch(request("agent.reload", json!({}))),
        Dispatch::Deferred
    ));
    assert_eq!(
        complete(&mut server, &mut output).await["result"],
        json!({"ok":true})
    );
    assert_eq!(
        server
            .agent()
            .loaded_session(session.session_id)
            .unwrap()
            .options_for_test()
            .model_timeout,
        Duration::from_secs(18)
    );
    server.agent.take().unwrap().shutdown().await.unwrap();
}

#[tokio::test]
async fn reload_excludes_open_create_and_send_in_both_directions() {
    for method in ["session.open", "session.create", "turn.send"] {
        let (mut agent, base, workspace) = reload_agent().await;
        let session = create(&mut agent, &workspace).await;
        if method == "session.open" {
            agent.close_session(session.session_id).await.unwrap();
        }
        let params = match method {
            "session.create" => json!({"workspace":workspace,"profile":"test"}),
            "turn.send" => json!({"session_id":session.session_id,"text":"hello"}),
            _ => json!({"session_id":session.session_id}),
        };
        let (mut server, mut output) = server(agent);
        write_config(base.path(), 1);
        let gate = hold(&mut server, request(method, params.clone())).await;
        assert_busy(server.dispatch(request("agent.reload", json!({}))));
        assert_eq!(
            server.agent().config().loop_options.model_timeout_seconds,
            Some(17)
        );
        gate.release();
        assert!(
            complete(&mut server, &mut output)
                .await
                .get("result")
                .is_some()
        );
        assert_eq!(
            server
                .agent()
                .loaded_session(session.session_id)
                .unwrap()
                .options_for_test()
                .model_timeout,
            Duration::from_secs(17)
        );

        let gate = hold(&mut server, request("agent.reload", json!({}))).await;
        assert_busy(server.dispatch(request(method, params)));
        gate.release();
        assert_eq!(
            complete(&mut server, &mut output).await["result"],
            json!({"ok":true})
        );
        assert_eq!(
            server.agent().config().loop_options.model_timeout_seconds,
            Some(18)
        );
        assert_eq!(
            server
                .agent()
                .loaded_session(session.session_id)
                .unwrap()
                .options_for_test()
                .model_timeout,
            Duration::from_secs(18)
        );
        let fresh = create(server.agent_mut(), &workspace).await;
        assert_eq!(
            server
                .agent()
                .loaded_session(fresh.session_id)
                .unwrap()
                .record()
                .system_prompt,
            "revision-1"
        );
        server.agent.take().unwrap().shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn failed_preparations_release_reservations_without_installing_candidates() {
    let (agent, base, workspace) = reload_agent().await;
    let (mut server, mut output) = server(agent);
    for method in ["session.open", "session.create", "agent.reload"] {
        let missing = SessionId::new().unwrap();
        let (params, kind) = match method {
            "session.open" => (json!({"session_id":missing}), "session_not_found"),
            "session.create" => (
                json!({"workspace":workspace.join("missing"),"profile":"test"}),
                "workspace_error",
            ),
            _ => {
                std::fs::write(base.path().join("agent.toml"), "invalid TOML").unwrap();
                (json!({}), "config_error")
            }
        };
        let gate = hold(&mut server, request(method, params)).await;
        let reservation = *server.reservations.iter().next().unwrap();
        gate.release();
        assert_eq!(
            complete(&mut server, &mut output).await["error"]["data"]["kind"],
            kind
        );
        assert_eq!(
            server.agent().config().loop_options.model_timeout_seconds,
            Some(17)
        );
        match reservation {
            super::super::Reservation::Session(id) => {
                assert!(server.agent().loaded_session(id).is_none());
                assert!(matches!(
                    server.dispatch(request("session.open", json!({"session_id":id}))),
                    Dispatch::Deferred
                ));
                assert_eq!(
                    complete(&mut server, &mut output).await["error"]["data"]["kind"],
                    "session_not_found"
                );
            }
            super::super::Reservation::Reload => {
                write_config(base.path(), 1);
                assert!(matches!(
                    server.dispatch(request("agent.reload", json!({}))),
                    Dispatch::Deferred
                ));
                assert_eq!(
                    complete(&mut server, &mut output).await["result"],
                    json!({"ok":true})
                );
                assert_eq!(
                    server.agent().config().loop_options.model_timeout_seconds,
                    Some(18)
                );
            }
        }
    }
    server.agent.take().unwrap().shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_open_publishes_its_index_only_when_installed() {
    for mode in ["abort", "discard", "install"] {
        let (mut agent, _base, workspace) = reload_agent().await;
        let session = create(&mut agent, &workspace).await;
        let id = session.session_id;
        let store = agent.store_handle();
        assert!(store.has_history_index(id));
        let turn = agent
            .send(crate::SendMessage {
                session_id: id,
                text: "saved history".into(),
            })
            .await
            .unwrap();
        agent.wait_turn(turn).await.unwrap();
        agent.close_session(id).await.unwrap();
        assert!(!store.has_history_index(id));
        let (mut server, mut output) = server(agent);
        let gate = Arc::new(Gate::new());
        if mode == "abort" {
            crate::store::SESSION_LOAD_GATES.arm(id, gate.clone());
        }
        assert!(matches!(
            server.dispatch(request("session.open", json!({"session_id":id}))),
            Dispatch::Deferred
        ));
        if mode == "abort" {
            tokio::time::timeout(TIMEOUT, gate.wait_started())
                .await
                .unwrap();
            assert!(!store.has_history_index(id));
            server.operations.abort_all();
            assert!(
                matches!(server.operations.join_next().await.unwrap(), Err(error) if error.is_cancelled())
            );
        } else {
            let prepared = tokio::time::timeout(TIMEOUT, server.operations.join_next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(!store.has_history_index(id));
            assert!(server.agent().loaded_session(id).is_none());
            if mode == "install" {
                server.complete_operation(prepared).await.unwrap();
                let response = output.recv().await.unwrap().json();
                assert_eq!(response["result"]["session"]["loaded"], true);
                assert!(store.has_history_index(id));
                assert!(matches!(
                    server.dispatch(request("session.close", json!({"session_id":id}))),
                    Dispatch::Deferred
                ));
                assert_eq!(
                    complete(&mut server, &mut output).await["result"],
                    json!({"ok":true})
                );
            } else {
                drop(prepared);
            }
        }
        assert!(!store.has_history_index(id));
        server.agent.take().unwrap().shutdown().await.unwrap();
        assert!(!store.has_history_index(id));
    }
}

#[tokio::test]
async fn shutdown_drains_pending_open_create_and_reload_before_its_response() {
    for method in ["session.open", "session.create", "agent.reload"] {
        let (mut agent, base, workspace) = reload_agent().await;
        let witness = create(&mut agent, &workspace).await;
        let closed = agent
            .loaded_session(witness.session_id)
            .unwrap()
            .query_cancellation();
        let target = create(&mut agent, &workspace).await;
        agent.close_session(target.session_id).await.unwrap();
        let store = agent.store_handle();
        assert!(store.has_history_index(witness.session_id));
        assert!(!store.has_history_index(target.session_id));
        write_config(base.path(), 1);
        let params = match method {
            "session.open" => json!({"session_id":target.session_id}),
            "session.create" => json!({"workspace":workspace,"profile":"test"}),
            _ => json!({}),
        };
        let mut harness = RpcHarness::spawn(agent);
        let gate = Arc::new(Gate::new());
        if method == "session.open" {
            crate::store::SESSION_LOAD_GATES.arm(target.session_id, gate.clone());
        } else {
            OPERATION_GATES.arm(base.path().join("data"), gate.clone());
        }
        harness.send(json!("held"), method, Some(params)).await;
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .unwrap();
        harness
            .send(json!("shutdown"), "agent.shutdown", None)
            .await;
        tokio::time::timeout(TIMEOUT, closed.cancelled())
            .await
            .unwrap();
        assert!(!store.has_history_index(target.session_id));
        gate.release();
        let held = harness.response(json!("held")).await;
        assert!(held.get("result").is_some());
        assert_eq!(
            harness.response(json!("shutdown")).await["result"],
            json!({"ok":true})
        );
        assert!(harness.next_frame().await.is_none());
        harness.join().await.unwrap();
        let ids: Vec<_> = harness
            .observed
            .iter()
            .filter_map(|frame| frame.get("id"))
            .collect();
        assert_eq!(ids, vec![&json!("held"), &json!("shutdown")]);
        assert_eq!(harness.observed.last().unwrap()["id"], "shutdown");
        assert!(!store.has_history_index(witness.session_id));
        assert!(!store.has_history_index(target.session_id));
        if let Some(id) = held.pointer("/result/session/session_id") {
            assert!(!store.has_history_index(id.as_str().unwrap().parse().unwrap()));
        }
    }
}

#[tokio::test]
#[cfg(unix)]
async fn all_query_paths_prefer_caller_cancellation_over_session_closure() {
    use crate::queries::*;
    use futures_util::FutureExt;
    fn decode<T: serde::de::DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).unwrap()
    }
    fn erase<T: Send + 'static>(future: QueryFuture<T>) -> QueryFuture<()> {
        future.map(|result| result.map(|_| ())).boxed()
    }
    let fixture = RpcFixture::new("query-precedence", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let mut agent = fixture.open().await;
    for (cancel_caller, cancel_session) in [(true, false), (false, true), (true, true)] {
        let info = create(&mut agent, &workspace).await;
        let id = info.session_id;
        let session = agent.loaded_session(id).unwrap();
        let caller = CancellationToken::new();
        let store = agent.store_handle();
        let deadline = std::time::Instant::now() + TIMEOUT;
        let mut queries = vec![
            erase(prepare_session_read(
                store.clone(),
                Some(session.clone()),
                decode(json!({"session_id":id})),
                caller.clone(),
            )),
            erase(prepare_turn_result(
                store.clone(),
                Some(session.clone()),
                decode(
                    json!({"turn":{"session_id":id,"loop_id":"lup_00000000000000000000000000000001"}}),
                ),
                caller.clone(),
            )),
            erase(prepare_workspace_read(
                session.clone(),
                decode(json!({"session_id":id,"path":"file.txt"})),
                caller.clone(),
            )),
            erase(prepare_workspace_files(
                session.clone(),
                decode(json!({"session_id":id})),
                caller.clone(),
            )),
            erase(prepare_workspace_search(
                session.clone(),
                decode(json!({"session_id":id,"query":"text"})),
                caller.clone(),
            )),
            erase(
                prepare_workspace_status(
                    session.clone(),
                    decode(json!({"session_id":id})),
                    caller.clone(),
                )
                .unwrap(),
            ),
        ];
        for scope in [json!("workspace"), json!("session")] {
            queries.push(erase(
                prepare_changes_list(
                    store.clone(),
                    Some(session.clone()),
                    decode(json!({"session_id":id,"scope":scope})),
                    caller.clone(),
                    deadline,
                )
                .unwrap(),
            ));
        }
        for change_ref in [
            format!("tool:{}", "0".repeat(64)),
            untracked_workspace_ref(id, "file.txt"),
        ] {
            queries.push(erase(
                prepare_changes_diff(
                    store.clone(),
                    Some(session.clone()),
                    decode(json!({"session_id":id,"change_ref":change_ref})),
                    caller.clone(),
                    deadline,
                )
                .unwrap(),
            ));
        }
        if cancel_caller {
            caller.cancel();
        }
        if cancel_session {
            session.cancel_queries();
        }
        for future in queries {
            let error = tokio::time::timeout(TIMEOUT, future)
                .await
                .unwrap()
                .unwrap_err();
            if cancel_caller {
                assert!(matches!(error, AgentError::QueryLimit));
            } else {
                assert!(matches!(error, AgentError::SessionNotLoaded));
            }
        }
        agent.close_session(id).await.unwrap();
    }
    agent.shutdown().await.unwrap();
}
