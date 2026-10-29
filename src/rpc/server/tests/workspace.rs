use super::*;

#[tokio::test]
async fn workspace_read_query_serves_raw_content_and_rejects_invalid_requests() {
    let mut fixture = RpcFixture::new("workspace-read", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    tokio::fs::write(workspace.join("note.txt"), b"hello\nworld\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let read = harness
        .call_ok(
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "note.txt"})),
        )
        .await;
    let result = &read["result"];
    assert_eq!(result["path"], json!("note.txt"));
    assert_eq!(result["content"], json!("hello\nworld\n"));
    assert_eq!(result["start_line"], json!(1));
    assert_eq!(result["returned_lines"], json!(2));
    assert_eq!(result["status"], json!("ok"));
    assert_eq!(result["encoding"], json!("utf8"));
    assert_eq!(result["revision"].as_str().unwrap().len(), 64);
    assert_eq!(result["truncated"], json!(false));
    assert_eq!(result["line_truncated"], json!(false));
    assert_eq!(result["next_range"], json!(null));
    assert_eq!(result["file_bytes"], json!(12));

    let ranged = harness
        .call_ok(
            "workspace.read",
            Some(json!({
                "session_id": session_id,
                "path": "note.txt",
                "start_line": 2,
                "max_lines": 1,
            })),
        )
        .await;
    assert_eq!(ranged["result"]["content"], json!("world\n"));
    assert_eq!(ranged["result"]["start_line"], json!(2));

    harness
        .send(
            json!("ws-fresh"),
            "workspace.read",
            Some(json!({
                "session_id": session_id,
                "path": "note.txt",
                "if_revision": result["revision"].clone(),
            })),
        )
        .await;
    assert_eq!(
        harness.response(json!("ws-fresh")).await["result"]["status"],
        json!("ok")
    );
    let stale = harness
        .call_ok(
            "workspace.read",
            Some(json!({
                "session_id": session_id,
                "path": "note.txt",
                "if_revision": "0".repeat(64),
            })),
        )
        .await;
    assert_eq!(stale["result"]["status"], json!("changed"));
    assert_eq!(stale["result"]["content"], json!(""));

    // Only a loaded Session locates its Workspace.
    let unloaded = SessionId::new().unwrap();
    harness
        .call_err(
            "workspace.read",
            Some(json!({"session_id": unloaded, "path": "note.txt"})),
            -32002,
            "session_not_loaded",
        )
        .await;

    harness
        .send(
            json!("ws-escape"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "../note.txt"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("ws-escape")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .send(
            json!("ws-lines"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "note.txt", "max_lines": 0})),
        )
        .await;
    assert_eq!(
        harness.response(json!("ws-lines")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .call_err(
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "missing.txt"})),
            -32010,
            "workspace_error",
        )
        .await;

    harness.shutdown().await;
}

#[tokio::test]
async fn pending_workspace_read_does_not_block_ping_cancel_or_shutdown() {
    let turn_started = Arc::new(Notify::new());
    let mut fixture = RpcFixture::new(
        "workspace-read-pending",
        [block_with_signal(Arc::clone(&turn_started))],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    tokio::fs::write(workspace.join("pending-a.txt"), b"a\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("send"),
            "input.push",
            Some(json!({"session_id": session_id, "mode": "steer", "text": "blocked turn"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["started"]["turn"].clone();
    tokio::time::timeout(TIMEOUT, turn_started.notified())
        .await
        .expect("blocked model request did not start");

    let gate = Arc::new(Gate::new());
    READ_GATES.arm("pending-a.txt".to_owned(), Arc::clone(&gate));
    harness
        .send(
            json!("ws-pending"),
            "workspace.read",
            Some(json!({"session_id": session_id, "path": "pending-a.txt"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, gate.wait_started())
        .await
        .expect("workspace read did not start");

    // The read query is genuinely pending while control methods stay live.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness
        .send(json!("cancel"), "turn.cancel", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("cancel")).await["result"]["cancelled"],
        json!(true)
    );
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;

    // Both the RPC and Session tokens are cancelled; caller cancellation wins.
    harness
        .send(json!("shutdown"), "agent.shutdown", None)
        .await;
    let response = harness.response(json!("ws-pending")).await;
    assert_eq!(
        response["error"]["data"],
        json!({"kind":"query_limit","retryable":true})
    );
    assert_eq!(
        harness.response(json!("shutdown")).await["result"],
        json!({"ok":true})
    );
    assert!(harness.next_frame().await.is_none());
    harness.join().await.unwrap();
}

#[tokio::test]
async fn closing_a_session_cancels_pending_workspace_reads_and_frees_capacity() {
    let mut fixture = RpcFixture::new("workspace-read-capacity", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    for index in 0..MAX_DEFERRED_QUERIES {
        tokio::fs::write(
            workspace.join(format!("cap-{index}.txt")),
            format!("cap-{index}\n"),
        )
        .await
        .unwrap();
    }
    tokio::fs::write(workspace.join("cap-fresh.txt"), b"fresh\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let closing = create_and_open(&mut harness, &workspace).await;
    let surviving = create_and_open(&mut harness, &workspace).await;

    let mut gates = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let path = format!("cap-{index}.txt");
        let gate = Arc::new(Gate::new());
        READ_GATES.arm(path.clone(), Arc::clone(&gate));
        gates.push(gate);
        harness
            .send(
                json!(format!("cap-{index}")),
                "workspace.read",
                Some(json!({"session_id": closing, "path": path})),
            )
            .await;
    }

    harness
        .call_err(
            "workspace.read",
            Some(json!({"session_id": surviving, "path": "cap-fresh.txt"})),
            -32019,
            "resource_exhausted",
        )
        .await;

    // The reads really started before the Session is closed.
    for gate in &gates {
        tokio::time::timeout(TIMEOUT, gate.wait_started())
            .await
            .expect("gated workspace read did not start");
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": closing})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    // Closing the owning Session cancels the four started queries.
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("cap-{index}"))).await;
        assert_rpc_error(&response, -32002, "session_not_loaded");
    }

    // The reader reaps the finished queries and admits a new one, and the
    // closed Session no longer owns a Workspace.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    let closed = harness
        .call(
            "workspace.read",
            Some(json!({"session_id": closing, "path": "cap-fresh.txt"})),
        )
        .await;
    assert_eq!(closed["error"]["code"], json!(-32002));
    let reuse = harness
        .call_ok(
            "workspace.read",
            Some(json!({"session_id": surviving, "path": "cap-fresh.txt"})),
        )
        .await;
    assert_eq!(reuse["result"]["content"], json!("fresh\n"));

    harness.shutdown().await;
}

#[tokio::test]
async fn workspace_files_and_search_serve_entries_and_literal_matches() {
    let mut fixture = RpcFixture::new("workspace-scan", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    tokio::fs::write(workspace.join("note.txt"), b"alpha\nneedle here\n")
        .await
        .unwrap();
    tokio::fs::create_dir_all(workspace.join("sub"))
        .await
        .unwrap();
    tokio::fs::write(workspace.join("sub/deep.txt"), b"needle too\n")
        .await
        .unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let files = harness
        .call_ok("workspace.files", Some(json!({"session_id": session_id})))
        .await;
    let result = &files["result"];
    assert_eq!(result["directory"], json!(""));
    assert_eq!(result["consistency"], json!("live"));
    assert_eq!(result["scan_complete"], json!(true));
    assert_eq!(result["truncated"], json!(false));
    assert_eq!(result["stopped_by"], json!("end"));
    assert_eq!(result["skipped_count"], json!(0));
    assert_eq!(result["next_cursor"], json!(null));
    assert!(result["observed_at_unix_ms"].as_u64().unwrap() > 0);
    let entries = result["entries"].as_array().unwrap();
    let mut paths: Vec<&str> = entries
        .iter()
        .map(|entry| entry["path"].as_str().unwrap())
        .collect();
    paths.sort();
    assert_eq!(paths, vec!["note.txt", "sub"]);
    let note = entries
        .iter()
        .find(|entry| entry["path"] == json!("note.txt"))
        .unwrap();
    assert_eq!(note["kind"], json!("file"));
    assert_eq!(note["size"], json!(18));
    let directory = entries
        .iter()
        .find(|entry| entry["path"] == json!("sub"))
        .unwrap();
    assert_eq!(directory["kind"], json!("directory"));
    assert_eq!(directory["size"], json!(null));

    // A recursive query filters entry paths and still descends.
    let nested = harness
        .call_ok(
            "workspace.files",
            Some(json!({
                "session_id": session_id,
                "recursive": true,
                "query": "deep",
            })),
        )
        .await;
    assert_eq!(
        nested["result"]["entries"][0]["path"],
        json!("sub/deep.txt")
    );
    assert!(nested["result"]["skipped_count"].as_u64().unwrap() > 0);

    // A literal, case-insensitive search reports line metadata, not contents
    // spliced into the text.
    let search = harness
        .call_ok(
            "workspace.search",
            Some(json!({"session_id": session_id, "query": "NEEDLE"})),
        )
        .await;
    let result = &search["result"];
    assert_eq!(result["consistency"], json!("live"));
    assert_eq!(result["scan_complete"], json!(true));
    assert_eq!(result["skipped_files"], json!(0));
    let matches = result["matches"].as_array().unwrap();
    let mut matched: Vec<&str> = matches
        .iter()
        .map(|record| record["path"].as_str().unwrap())
        .collect();
    matched.sort();
    assert_eq!(matched, vec!["note.txt", "sub/deep.txt"]);
    let record = matches
        .iter()
        .find(|record| record["path"] == json!("note.txt"))
        .unwrap();
    assert_eq!(record["line_number"], json!(2));
    assert_eq!(record["line_text"], json!("needle here"));
    assert_eq!(record["match_byte_ranges"], json!([{"start": 0, "end": 6}]));
    assert_eq!(record["line_truncated"], json!(false));

    // Lexical errors are rejected before a query slot is reserved, and a
    // missing root is a workspace error.
    harness
        .send(
            json!("files-escape"),
            "workspace.files",
            Some(json!({"session_id": session_id, "directory": "../escape"})),
        )
        .await;
    assert_eq!(
        harness.response(json!("files-escape")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .send(
            json!("search-empty"),
            "workspace.search",
            Some(json!({"session_id": session_id, "query": ""})),
        )
        .await;
    assert_eq!(
        harness.response(json!("search-empty")).await["error"]["code"],
        json!(-32602)
    );
    harness
        .call_err(
            "workspace.files",
            Some(json!({"session_id": session_id, "directory": "missing"})),
            -32010,
            "workspace_error",
        )
        .await;
    let unloaded = SessionId::new().unwrap();
    let unloaded = harness
        .call(
            "workspace.search",
            Some(json!({"session_id": unloaded, "query": "needle"})),
        )
        .await;
    assert_eq!(unloaded["error"]["code"], json!(-32002));

    harness.shutdown().await;
}

#[tokio::test]
async fn pending_workspace_scans_do_not_block_ping_or_shutdown() {
    let mut fixture = RpcFixture::new("workspace-scan-pending", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    tokio::fs::write(workspace.join("a.txt"), b"pending hit\n")
        .await
        .unwrap();
    let root = std::fs::canonicalize(&workspace).unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    let files_hold = Arc::new(ScanHold::new());
    SCAN_HOLDS.arm(root.clone(), Arc::clone(&files_hold));
    harness
        .send(
            json!("files"),
            "workspace.files",
            Some(json!({"session_id": session_id})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, files_hold.wait_started())
        .await
        .expect("gated files scan did not start");

    let search_hold = Arc::new(ScanHold::new());
    SCAN_HOLDS.arm(root, Arc::clone(&search_hold));
    harness
        .send(
            json!("search"),
            "workspace.search",
            Some(json!({"session_id": session_id, "query": "hit"})),
        )
        .await;
    tokio::time::timeout(TIMEOUT, search_hold.wait_started())
        .await
        .expect("gated search scan did not start");

    // Both scans are genuinely pending while control methods stay live.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());

    // Shutdown cancels both scans, joins their blocking workers, and still
    // queues its own response last.
    harness.shutdown().await;
}

#[tokio::test]
async fn closing_a_session_cancels_pending_workspace_scans_and_frees_capacity() {
    let mut fixture = RpcFixture::new("workspace-scan-capacity", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    for index in 0..MAX_DEFERRED_QUERIES {
        tokio::fs::write(
            workspace.join(format!("cap-{index}.txt")),
            format!("cap-{index}\n"),
        )
        .await
        .unwrap();
    }
    tokio::fs::write(workspace.join("cap-fresh.txt"), b"fresh\n")
        .await
        .unwrap();
    let root = std::fs::canonicalize(&workspace).unwrap();
    let mut harness = RpcHarness::spawn(agent);
    let closing = create_and_open(&mut harness, &workspace).await;
    let surviving = create_and_open(&mut harness, &workspace).await;

    let mut holds = Vec::new();
    for index in 0..MAX_DEFERRED_QUERIES {
        let hold = Arc::new(ScanHold::new());
        SCAN_HOLDS.arm(root.clone(), Arc::clone(&hold));
        holds.push(hold);
        harness
            .send(
                json!(format!("scan-{index}")),
                "workspace.files",
                Some(json!({"session_id": closing})),
            )
            .await;
    }

    harness
        .call_err(
            "workspace.search",
            Some(json!({"session_id": surviving, "query": "fresh"})),
            -32019,
            "resource_exhausted",
        )
        .await;

    for hold in &holds {
        tokio::time::timeout(TIMEOUT, hold.wait_started())
            .await
            .expect("gated scan did not start");
    }

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": closing})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );

    // Closing the owning Session cancels the four started scans.
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("scan-{index}"))).await;
        assert_rpc_error(&response, -32002, "session_not_loaded");
    }

    // The reader reaps the finished queries and admits a new one, and the
    // closed Session no longer owns a Workspace.
    harness
        .send(json!("ping"), "agent.ping", Some(json!({})))
        .await;
    assert!(harness.response(json!("ping")).await["result"]["version"].is_string());
    harness
        .send(
            json!("scan-closed"),
            "workspace.files",
            Some(json!({"session_id": closing})),
        )
        .await;
    assert_eq!(
        harness.response(json!("scan-closed")).await["error"]["code"],
        json!(-32002)
    );
    let reuse = harness
        .call_ok(
            "workspace.search",
            Some(json!({"session_id": surviving, "query": "cap"})),
        )
        .await;
    let paths: Vec<&str> = reuse["result"]["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths.len(), MAX_DEFERRED_QUERIES);
    assert!(paths.contains(&"cap-0.txt"));
    assert!(!paths.contains(&"cap-fresh.txt"));

    harness.shutdown().await;
}

#[tokio::test]
async fn workspace_status_answers_for_the_session_workspace() {
    let mut fixture = RpcFixture::new("workspace-status", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session = create_and_open(&mut harness, &workspace).await;

    let status = harness
        .call_ok("workspace.status", Some(json!({"session_id": session})))
        .await;
    // A temporary directory outside any repository is answered conservatively:
    // git refused to resolve a work tree, so the observation stays incomplete
    // instead of claiming a definite missing repository.
    assert_eq!(status["result"]["repo_available"], json!(false));
    assert_eq!(status["result"]["complete"], json!(false));
    assert_eq!(status["result"]["head_oid"], json!(null));
    assert_eq!(status["result"]["branch"], json!(null));
    assert_eq!(status["result"]["detached"], json!(false));
    assert_eq!(status["result"]["entries"], json!([]));
    assert_eq!(status["result"]["warnings"], json!(["status_failed"]));
    assert_eq!(status["result"]["consistency"], json!("live"));
    assert!(status["result"]["observed_at_unix_ms"].is_u64());

    // A status query is an observation: it appends nothing to history and
    // never starts a model call.
    harness
        .send(
            json!("history"),
            "session.history",
            Some(json!({"session_id": session, "offset": 0, "limit": 10})),
        )
        .await;
    assert_eq!(
        harness.response(json!("history")).await["result"]["total"],
        json!(0)
    );

    let bad = harness
        .call(
            "workspace.status",
            Some(json!({"session_id": session, "max_bytes": 16})),
        )
        .await;
    assert_eq!(bad["error"]["code"], json!(-32602));

    harness.shutdown().await;
}
