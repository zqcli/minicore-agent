use super::*;

#[tokio::test]
async fn capability_discovery_returns_ordered_lists() {
    let (agent, base, _workspace) =
        test_agent("capability", [], &["read"], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);
    harness
        .send(json!("p"), "agent.ping", Some(json!({})))
        .await;
    let ping = harness.response(json!("p")).await;
    assert_eq!(ping["result"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(ping["result"]["protocol_version"], json!(1));
    assert_eq!(
        ping["result"]["capabilities"],
        json!([
            "session.read",
            "session.read.display",
            "session.context",
            "turn.result",
            "tool.read",
            "tool.read.display",
            "tool.output",
            "session.history",
            "workspace.read",
            "workspace.files",
            "workspace.search",
            "workspace.status",
            "changes.list",
            "changes.diff",
            "deferred.waiter_limit",
            "tool.arguments.preview"
        ])
    );

    harness
        .send(json!("profiles"), "profile.list", Some(json!({})))
        .await;
    let profiles = harness.response(json!("profiles")).await;
    assert_eq!(profiles["result"]["profiles"][0]["id"], json!("test"));

    harness
        .send(json!("models"), "model.list", Some(json!({})))
        .await;
    let models = harness.response(json!("models")).await;
    assert_eq!(models["result"]["models"][0]["id"], json!("fake"));
    assert_eq!(
        models["result"]["models"][0]["supported_reasoning"],
        json!([
            "auto", "disabled", "low", "medium", "high", "xhigh", "max", "ultra"
        ])
    );

    harness
        .send(json!("sessions"), "session.list", Some(json!({})))
        .await;
    let sessions = harness.response(json!("sessions")).await;
    assert_eq!(sessions["result"]["sessions"], json!([]));

    harness.shutdown().await;
    remove_base(&base).await;
}

#[tokio::test]
async fn agent_reload_requires_empty_params_and_a_file_source() {
    let (agent, base, _workspace) = test_agent("reload-rpc", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);

    harness
        .send(json!("reload"), "agent.reload", Some(json!({})))
        .await;
    let reload = harness.response(json!("reload")).await;
    assert_eq!(reload["error"]["code"], json!(-32018));
    assert_eq!(
        reload["error"]["data"],
        json!({
            "kind": "reload_unavailable",
            "retryable": false,
        })
    );

    harness
        .send(
            json!("reload-path"),
            "agent.reload",
            Some(json!({"path": "/tmp/other.agent.toml"})),
        )
        .await;
    let reload_path = harness.response(json!("reload-path")).await;
    assert_eq!(reload_path["error"]["code"], json!(-32602));
    assert_eq!(
        reload_path["error"]["data"]["kind"],
        json!("invalid_params")
    );
    assert!(!reload_path.to_string().contains("other.agent.toml"));

    harness
        .send(json!("reload-omitted"), "agent.reload", None)
        .await;
    let reload_omitted = harness.response(json!("reload-omitted")).await;
    assert_eq!(reload_omitted["error"]["code"], json!(-32018));

    harness.shutdown().await;
    remove_base(&base).await;
}

/// P1 regression: the private `Result` boundary must return the exact same
/// error object a caller saw before, with the request id preserved and no
/// `result` field. It drives a plain method, a query entry's param decode, and
/// the unknown-method fallback through `dispatch_inner`.
#[tokio::test]
async fn dispatch_inner_propagates_the_full_error_object() {
    let (agent, base, _workspace) = test_agent("dispatch-error", [], &[], ApprovalMode::Auto).await;
    let mut harness = RpcHarness::spawn(agent);

    let cases = [
        (
            json!("ping-bad"),
            "agent.ping",
            Some(json!({"extra": true})),
        ),
        (json!("shutdown-bad"), "agent.shutdown", Some(json!([]))),
        (
            json!("status-bad"),
            "workspace.status",
            Some(json!({"session_id": 7})),
        ),
    ];
    for (id, method, params) in cases {
        harness.send(id.clone(), method, params).await;
        let response = harness.response(id.clone()).await;
        assert_eq!(
            response,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32602,
                    "message": "invalid params",
                    "data": {"kind": "invalid_params", "retryable": false},
                },
            }),
            "{method} invalid params changed shape"
        );
    }

    harness
        .send(json!("no-such-method"), "no.such.method", None)
        .await;
    let unknown = harness.response(json!("no-such-method")).await;
    assert_eq!(
        unknown,
        json!({
            "jsonrpc": "2.0",
            "id": "no-such-method",
            "error": {
                "code": -32601,
                "message": "method not found",
                "data": {"kind": "method_not_found", "retryable": false},
            },
        })
    );

    harness.shutdown().await;
    remove_base(&base).await;
}
