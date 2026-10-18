use super::*;

#[tokio::test]
async fn capability_discovery_returns_ordered_lists() {
    let mut fixture = RpcFixture::new("capability", [], &["read"], ApprovalMode::Auto);
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let ping = harness.call_ok("agent.ping", Some(json!({}))).await;
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

    let profiles = harness.call_ok("profile.list", Some(json!({}))).await;
    assert_eq!(profiles["result"]["profiles"][0]["id"], json!("test"));

    let models = harness.call_ok("model.list", Some(json!({}))).await;
    assert_eq!(models["result"]["models"][0]["id"], json!("fake"));
    assert_eq!(
        models["result"]["models"][0]["supported_reasoning"],
        json!([
            "auto", "disabled", "low", "medium", "high", "xhigh", "max", "ultra"
        ])
    );

    let sessions = harness.call_ok("session.list", Some(json!({}))).await;
    assert_eq!(sessions["result"]["sessions"], json!([]));

    harness.shutdown().await;
}

#[tokio::test]
async fn agent_reload_requires_empty_params_and_a_file_source() {
    let mut fixture = RpcFixture::new("reload-rpc", [], &[], ApprovalMode::Auto);
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);

    let reload = harness.call("agent.reload", Some(json!({}))).await;
    assert_eq!(reload["error"]["code"], json!(-32018));
    assert_eq!(
        reload["error"]["data"],
        json!({
            "kind": "reload_unavailable",
            "retryable": false,
        })
    );

    let reload_path = harness
        .call_err(
            "agent.reload",
            Some(json!({"path": "/tmp/other.agent.toml"})),
            -32602,
            "invalid_params",
        )
        .await;
    assert!(!reload_path.to_string().contains("other.agent.toml"));

    let reload_omitted = harness.call("agent.reload", None).await;
    assert_eq!(reload_omitted["error"]["code"], json!(-32018));

    harness.shutdown().await;
}

/// P1 regression: the private `Result` boundary must return the exact same
/// error object a caller saw before, with the request id preserved and no
/// `result` field. It drives a plain method, a query entry's param decode, and
/// the unknown-method fallback through `dispatch_inner`.
#[tokio::test]
async fn dispatch_inner_propagates_the_full_error_object() {
    let mut fixture = RpcFixture::new("dispatch-error", [], &[], ApprovalMode::Auto);
    let agent = fixture.open().await;
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
}
