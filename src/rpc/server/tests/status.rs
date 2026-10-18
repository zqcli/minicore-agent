use super::*;

#[cfg(unix)]
#[tokio::test]
async fn workspace_status_projects_the_shared_presentation_branch_and_clears_failures() {
    use crate::workspace::status::STATUS_PROGRAMS;
    use std::os::unix::fs::PermissionsExt;

    let (agent, base, workspace) =
        test_agent("workspace-status-projection", [], &[], ApprovalMode::Auto).await;
    let init = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .arg(&workspace)
        .status()
        .expect("git must be available for the status projection regression");
    assert!(init.success());
    let head = std::process::Command::new("git")
        .args(["-C"])
        .arg(&workspace)
        .args(["symbolic-ref", "HEAD", "refs/heads/rpc-status"])
        .status()
        .unwrap();
    assert!(head.success());

    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;

    harness
        .send(
            json!("before"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert!(harness.response(json!("before")).await["result"]["git_branch"].is_null());

    harness
        .send(
            json!("status"),
            "workspace.status",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let status = harness.response(json!("status")).await;
    assert_eq!(status["result"]["complete"], json!(true));
    assert_eq!(status["result"]["repo_available"], json!(true));
    assert_eq!(status["result"]["branch"], json!("rpc-status"));

    harness
        .send(
            json!("after"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("after")).await["result"]["git_branch"],
        json!("rpc-status")
    );

    let marker = base.join("status-fails");
    let script = base.join("status-wrapper");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ -f '{}' ]; then exit 1; fi\nexec git \"$@\"\n",
            marker.display()
        ),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    STATUS_PROGRAMS.arm(
        std::fs::canonicalize(&workspace).unwrap(),
        script.into_os_string(),
    );
    std::fs::write(&marker, b"fail\n").unwrap();

    harness
        .send(
            json!("failed-status"),
            "workspace.status",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let failed = harness.response(json!("failed-status")).await;
    assert_eq!(failed["result"]["complete"], json!(false));
    assert_eq!(failed["result"]["repo_available"], json!(false));
    assert_eq!(failed["result"]["branch"], json!(null));

    harness
        .send(
            json!("cleared"),
            "session.presentation",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert!(harness.response(json!("cleared")).await["result"]["git_branch"].is_null());

    harness.shutdown().await;
    remove_base(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn closing_a_session_stops_pending_status_queries_and_frees_capacity() {
    use crate::workspace::status::STATUS_PROGRAMS;
    use std::os::unix::fs::PermissionsExt;

    let (agent, base, workspace) =
        test_agent("workspace-status-capacity", [], &[], ApprovalMode::Auto).await;
    let root = std::fs::canonicalize(&workspace).unwrap();
    let started = base.join("status-started");
    let script = base.join("slow-git");
    std::fs::write(
        &script,
        format!("#!/bin/sh\n: > '{}'\nsleep 30\n", started.display()),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    STATUS_PROGRAMS.arm(root, script.into_os_string());

    let mut harness = RpcHarness::spawn(agent);
    let closing = create_and_open(&mut harness, &workspace).await;
    let surviving = create_and_open(&mut harness, &workspace).await;

    for index in 0..MAX_DEFERRED_QUERIES {
        harness
            .send(
                json!(format!("status-{index}")),
                "workspace.status",
                Some(json!({"session_id": closing})),
            )
            .await;
    }
    for _ in 0..200 {
        if started.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(started.exists(), "no status query started its git child");

    // Every query slot is held, so the pool refuses one more.
    harness
        .send(
            json!("over"),
            "workspace.files",
            Some(json!({"session_id": surviving})),
        )
        .await;
    let over = harness.response(json!("over")).await;
    assert_eq!(over["error"]["code"], json!(-32019));

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
    // Closing the owning Session cancels the started status queries.
    for index in 0..MAX_DEFERRED_QUERIES {
        let response = harness.response(json!(format!("status-{index}"))).await;
        assert_eq!(response["error"]["code"], json!(-32002));
    }
    // The freed slots are usable again by the surviving Session.
    harness
        .send(
            json!("after"),
            "workspace.files",
            Some(json!({"session_id": surviving})),
        )
        .await;
    let after = harness.response(json!("after")).await;
    assert_eq!(after["result"]["stopped_by"], json!("end"));

    harness.shutdown().await;
    remove_base(&base).await;
}
