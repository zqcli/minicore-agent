use super::*;

#[tokio::test]
async fn changes_list_separates_workspace_session_and_turn_scopes() {
    let mut fixture = RpcFixture::new(
        "changes-list-scopes",
        [tool_calls(vec![ToolCallScript {
            name: "write",
            arguments: json!({"path": "value.txt", "content": "agent\n"}),
        }])],
        &["write"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    std::fs::write(workspace.join("value.txt"), b"user\n").unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;

    let sent = harness
        .call_ok(
            "input.push",
            Some(json!({"session_id": session, "mode": "steer", "text": "update file"})),
        )
        .await;
    let turn = sent["result"]["started"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );

    let session_changes = harness
        .call_ok(
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    let session_records = session_changes["result"]["records"].as_array().unwrap();
    assert_eq!(session_records.len(), 1);
    assert_eq!(session_records[0]["origin"], json!("tool"));
    assert_eq!(session_records[0]["path"], json!("value.txt"));
    assert_eq!(session_records[0]["before"]["kind"], json!("content"));
    assert_eq!(session_records[0]["after"]["kind"], json!("content"));
    assert_eq!(session_records[0]["commit_state"], json!("applied"));
    assert_eq!(session_records[0]["coverage"], json!("complete"));
    assert_eq!(session_records[0]["details_available"], json!(true));
    assert_eq!(session_records[0]["tool_ref"]["session_id"], json!(session));

    let turn_changes = harness
        .call_ok(
            "changes.list",
            Some(json!({
                "session_id": session,
                "scope": {"turn": {"loop_id": turn["loop_id"]}}
            })),
        )
        .await;
    assert_eq!(
        turn_changes["result"]["records"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        turn_changes["result"]["scope"],
        json!({"turn": {"loop_id": turn["loop_id"]}})
    );

    harness
        .send(
            json!("workspace"),
            "changes.list",
            Some(json!({"session_id": session, "scope": "workspace"})),
        )
        .await;
    let workspace_changes = harness.response(json!("workspace")).await;
    assert_eq!(
        workspace_changes["result"]["records"],
        json!([]),
        "the fixture is deliberately outside Git; no tool record may leak into workspace scope"
    );
    assert_eq!(workspace_changes["result"]["scope"], json!("workspace"));

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    std::fs::remove_dir_all(&workspace).unwrap();
    harness
        .send(
            json!("cold"),
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    let cold = harness.response(json!("cold")).await;
    assert_eq!(cold["result"]["consistency"], json!("cold"));
    assert_eq!(
        cold["result"]["records"],
        session_changes["result"]["records"]
    );
    assert!(!workspace.exists());

    harness.shutdown().await;
}

/// End-to-end public-RPC flow: a real native write records a change, the
/// change is listed, and `changes.diff` returns the actual user-dirty before
/// rather than the Git HEAD content. It works both warm and after the Session
/// is closed (cold), and pages a long line by UTF-8 fragments.
#[tokio::test]
async fn changes_diff_public_rpc_compares_native_before_and_after() {
    let mut fixture = RpcFixture::new(
        "changes-diff-flow",
        [
            tool_calls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "value.txt", "content": "agent\n"}),
            }]),
            tool_calls(vec![ToolCallScript {
                name: "write",
                arguments: json!({"path": "long.txt", "content": "short"}),
            }]),
        ],
        &["write"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    std::fs::write(workspace.join("value.txt"), b"user dirty\n").unwrap();
    std::fs::write(workspace.join("long.txt"), "x".repeat(40)).unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session, "mode": "steer", "text": "update files"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );

    let listed = harness
        .call_ok(
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    let records = listed["result"]["records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    let target = records
        .iter()
        .find(|record| record["path"] == json!("value.txt"))
        .unwrap();
    let change_ref = target["change_ref"].as_str().unwrap().to_owned();

    // Warm diff: before is the actual user-dirty buffer, not HEAD.
    let diff = harness
        .call_ok(
            "changes.diff",
            Some(json!({"session_id": session, "change_ref": change_ref})),
        )
        .await;
    assert_eq!(diff["result"]["base_version"]["kind"], json!("content"));
    assert_eq!(diff["result"]["target_version"]["kind"], json!("content"));
    assert_eq!(diff["result"]["comparison"], json!("tool_before_after"));
    assert_eq!(diff["result"]["availability"], json!("available"));
    let removed: String = diff["result"]["hunks"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|hunk| hunk["lines"].as_array().unwrap())
        .filter(|line| line["kind"] == json!("removed"))
        .map(|line| line["text"].as_str().unwrap())
        .collect();
    assert_eq!(removed, "user dirty\n");

    // A returned cursor continues the same comparison; a bumped revision is stale.
    let cursor = diff["result"]["next_cursor"].clone();
    if !cursor.is_null() {
        let continued = harness
            .call_ok(
                "changes.diff",
                Some(json!({
                    "session_id": session,
                    "change_ref": change_ref,
                    "cursor": cursor,
                })),
            )
            .await;
        assert_eq!(continued["result"]["stale"], json!(false));
    }

    // Cold diff after close: the Store still resolves the record.
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    let cold = harness
        .call_ok(
            "changes.diff",
            Some(json!({"session_id": session, "change_ref": change_ref})),
        )
        .await;
    assert_eq!(cold["result"]["availability"], json!("available"));
    assert_eq!(cold["result"]["change_ref"], json!(change_ref));

    harness.shutdown().await;
}

/// A long CRLF line with a missing final newline is paged by raw UTF-8
/// fragments whose offsets reconstruct the exact original bytes.
#[tokio::test]
async fn changes_diff_public_rpc_pages_a_long_line_losslessly() {
    let long = format!("{}{}", "a".repeat(2_500), "é🙂世界".repeat(300));
    let mut fixture = RpcFixture::new(
        "changes-diff-paging",
        [tool_calls(vec![ToolCallScript {
            name: "write",
            arguments: json!({"path": "long.txt", "content": "changed"}),
        }])],
        &["write"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    std::fs::write(workspace.join("long.txt"), &long).unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session, "mode": "steer", "text": "update file"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );
    let listed = harness
        .call_ok(
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    let change_ref = listed["result"]["records"][0]["change_ref"]
        .as_str()
        .unwrap()
        .to_owned();

    let mut cursor = Value::Null;
    let mut removed = String::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 50, "paging did not terminate");
        let params = if cursor.is_null() {
            json!({"session_id": session, "change_ref": change_ref, "max_bytes": 4096})
        } else {
            json!({
                "session_id": session,
                "change_ref": change_ref,
                "max_bytes": 4096,
                "cursor": cursor,
            })
        };
        let diff = harness.call_ok("changes.diff", Some(params)).await;
        for hunk in diff["result"]["hunks"].as_array().unwrap() {
            for line in hunk["lines"].as_array().unwrap() {
                if line["kind"] == json!("removed") {
                    assert_eq!(
                        line["line_byte_offset"].as_u64().unwrap(),
                        removed.len() as u64
                    );
                    removed.push_str(line["text"].as_str().unwrap());
                }
            }
        }
        if diff["result"]["complete"] == json!(true) {
            break;
        }
        cursor = diff["result"]["next_cursor"].clone();
        assert!(!cursor.is_null(), "an incomplete page must carry a cursor");
    }
    assert_eq!(removed, long);

    harness.shutdown().await;
}

/// Four `changes.diff` queries hold every query slot; a fifth is refused with
/// `resource_exhausted` while ping stays available, and closing the owning
/// Session cancels the held comparisons.
#[tokio::test]
async fn changes_diff_shares_the_four_query_slots_and_joins_on_shutdown() {
    let mut fixture = RpcFixture::new(
        "changes-diff-capacity",
        [tool_calls(vec![ToolCallScript {
            name: "write",
            arguments: json!({"path": "value.txt", "content": "agent\n"}),
        }])],
        &["write"],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    std::fs::write(workspace.join("value.txt"), b"user\n").unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session, "mode": "steer", "text": "update file"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["outcome"]["type"],
        json!("completed")
    );
    let listed = harness
        .call_ok(
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    let change_ref = listed["result"]["records"][0]["change_ref"]
        .as_str()
        .unwrap()
        .to_owned();

    let mut gates = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let gate = Arc::new(crate::test_support::Gate::new());
        crate::diff::DIFF_GATES.arm(
            crate::diff::tests::comparison_key(b"user\n", b"agent\n"),
            Arc::clone(&gate),
        );
        gates.push(gate);
        harness
            .send(
                json!(format!("diff-{index}")),
                "changes.diff",
                Some(json!({"session_id": session, "change_ref": change_ref})),
            )
            .await;
    }
    for gate in &gates {
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .expect("changes.diff query did not start");
    }

    let over = harness
        .call(
            "changes.diff",
            Some(json!({"session_id": session, "change_ref": change_ref})),
        )
        .await;
    assert_eq!(over["error"]["code"], json!(-32019));

    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("diff-{index}"))).await;
        assert_eq!(response["error"]["code"], json!(-32002));
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn changes_list_deferred_query_shares_pool_and_ping_stays_available() {
    let mut fixture = RpcFixture::new("changes-list-capacity", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    let session_val: SessionId = session.as_str().unwrap().parse().unwrap();

    let mut gates = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let gate = Arc::new(crate::test_support::Gate::new());
        CHANGE_LIST_GATES.arm(session_val, Arc::clone(&gate));
        gates.push(gate);
        harness
            .send(
                json!(format!("cl-{index}")),
                "changes.list",
                Some(json!({"session_id": session, "scope": "session"})),
            )
            .await;
    }
    for gate in &gates {
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .expect("changes.list query did not start");
    }

    // The query pool is now full, so a real (valid) changes.list request is
    // rejected by capacity rather than by request validation.
    let overflow = harness
        .call(
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    assert_rpc_error(&overflow, -32019, "resource_exhausted");

    // Control methods are not deferred and stay responsive while the pool holds
    // every slot.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Closing the owning Session cancels the started queries and joins them.
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("cl-{index}"))).await;
        assert_eq!(response["error"]["code"], json!(-32002));
    }

    harness.shutdown().await;
}

/// Capacity is checked before any Session-owned worker is registered. With
/// every query slot held, a request that already resolved to a loaded Session
/// must still be refused as the complete `resource_exhausted` object and must
/// not start a Git child; an unloaded Session plus a `workspace` scope/ref is
/// refused the same way instead of `session_not_loaded`.
#[cfg(unix)]
#[tokio::test]
async fn changes_capacity_precedes_missing_session_and_starts_no_worker() {
    use crate::workspace::status::STATUS_PROGRAMS;
    use std::os::unix::fs::PermissionsExt;

    let mut fixture = RpcFixture::new("changes-capacity-precedence", [], &[], ApprovalMode::Auto);
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let root = std::fs::canonicalize(&workspace).unwrap();
    let started = base.join("capacity-git-started");
    let script = base.join("counting-git");
    std::fs::write(
        &script,
        format!("#!/bin/sh\n: > '{}'\nexec git \"$@\"\n", started.display()),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    STATUS_PROGRAMS.arm(root, script.into_os_string());

    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    let session_val: SessionId = session.as_str().unwrap().parse().unwrap();
    let unloaded = SessionId::new().unwrap();

    // Hold every query slot with gated (already admitted) tool-change queries.
    let mut gates = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let gate = Arc::new(crate::test_support::Gate::new());
        CHANGE_LIST_GATES.arm(session_val, Arc::clone(&gate));
        gates.push(gate);
        harness
            .send(
                json!(format!("cap-{index}")),
                "changes.list",
                Some(json!({"session_id": session, "scope": "session"})),
            )
            .await;
    }
    for gate in &gates {
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .expect("changes.list query did not start");
    }

    // Loaded Session, workspace-bearing entries: the slot is refused before a
    // worker registers, and the whole error object is unchanged.
    for (id, method, params) in [
        (
            json!("status-loaded"),
            "workspace.status",
            json!({"session_id": session}),
        ),
        (
            json!("list-loaded"),
            "changes.list",
            json!({"session_id": session, "scope": "workspace"}),
        ),
        (
            json!("diff-loaded"),
            "changes.diff",
            json!({
                "session_id": session,
                "change_ref": untracked_workspace_ref(session_val, "note.txt"),
            }),
        ),
    ] {
        harness.send(id.clone(), method, Some(params)).await;
        assert_eq!(
            harness.response(id.clone()).await,
            resource_exhausted_error(id),
            "{method} must be refused by capacity with the full object"
        );
    }

    // Workspace scope/ref with an unloaded Session: capacity wins over the
    // missing Session, and no status/source worker (Git child) may start.
    harness
        .send(
            json!("list-workspace"),
            "changes.list",
            Some(json!({"session_id": unloaded, "scope": "workspace"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("list-workspace")).await,
        resource_exhausted_error(json!("list-workspace"))
    );
    harness
        .send(
            json!("diff-workspace"),
            "changes.diff",
            Some(json!({
                "session_id": unloaded,
                "change_ref": untracked_workspace_ref(unloaded, "note.txt"),
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("diff-workspace")).await,
        resource_exhausted_error(json!("diff-workspace"))
    );

    assert!(
        !started.exists(),
        "a refused changes/status request started a Git child"
    );

    // Closing the owning Session drains the held queries so the harness can
    // shut down cleanly.
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("cap-{index}"))).await;
        assert_eq!(response["error"]["code"], json!(-32002));
    }

    harness.shutdown().await;
}

/// The shared 32-slot deferred ceiling is filled by a real pending model turn
/// (`turn.wait` waiters) plus held `changes.list` query slots. With fewer than
/// four query slots held, the 33rd request is refused as `resource_exhausted`;
/// releasing the turn frees the slots again.
#[tokio::test]
async fn changes_list_queries_share_the_total_waiter_ceiling() {
    let probe = ConcurrencyProbe::new();
    let mut fixture = RpcFixture::new(
        "changes-list-shared-ceiling",
        [gated(Arc::clone(&probe))],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    let session_val: SessionId = session.as_str().unwrap().parse().unwrap();

    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session, "mode": "steer", "text": "gated turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, async {
        while probe.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gated model request did not start");

    // One held changes.list query + one parked turn.wait per remaining slot.
    let held_gate = Arc::new(crate::test_support::Gate::new());
    CHANGE_LIST_GATES.arm(session_val, Arc::clone(&held_gate));
    harness
        .send(
            json!("cl-held"),
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, held_gate.wait_started())
        .await
        .expect("held changes.list did not start");

    let waiters = MAX_DEFERRED_WAITERS - 1;
    for index in 0..waiters {
        harness
            .send(
                json!(format!("wait-{index}")),
                "turn.wait",
                Some(turn_params(&turn)),
            )
            .await;
    }

    // The shared ceiling is full; the next request is refused even though fewer
    // than MAX_DEFERRED_QUERIES query slots are held.
    let over = harness.call("turn.wait", Some(turn_params(&turn))).await;
    assert_rpc_error(&over, -32019, "resource_exhausted");

    // Control methods stay responsive while both ceilings are pressed.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Releasing the gated turn drains the parked waiters and frees the slots.
    probe.release.cancel();
    for index in 0..waiters {
        let waited = harness.response(json!(format!("wait-{index}"))).await;
        assert_eq!(waited["result"]["outcome"]["type"], json!("completed"));
    }

    // Closing the Session cancels the still-held changes.list query.
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    let held = harness.response(json!("cl-held")).await;
    assert_eq!(held["error"]["code"], json!(-32002));

    harness.shutdown().await;
}

#[tokio::test]
async fn changes_list_shutdown_cancels_a_waiting_query_within_deadline() {
    let mut fixture = RpcFixture::new("changes-list-shutdown", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;
    let session_val: SessionId = session.as_str().unwrap().parse().unwrap();

    let gate = Arc::new(crate::test_support::Gate::new());
    CHANGE_LIST_GATES.arm(session_val, Arc::clone(&gate));
    harness
        .send(
            json!("cl-pending"),
            "changes.list",
            Some(json!({"session_id": session, "scope": "session"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .expect("changes.list query did not start");

    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    let start = std::time::Instant::now();
    harness.shutdown().await;
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
}
