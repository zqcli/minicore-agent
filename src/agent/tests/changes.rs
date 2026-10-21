use super::*;

/// F1 native-file identity: a first request forced to reach `Model::start`
/// before `bind_started_loop` must still associate its `write` ToolRef and
/// change records. This is the native-tool counterpart of the Bash F1
/// regression, and it reads the facts back through `changes.list` /
/// `changes.diff` rather than only through final History.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f1_native_write_changes_keep_the_first_request_identity() {
    use crate::changes::{ChangeScope, ChangesListRequest};
    use crate::diff::{ChangesDiffRequest, DiffComparison};

    let fixture = AgentFixture::new("f1-native-write").file("value.txt", b"user\n");
    let model_gate = Arc::new(Gate::new());
    let model = ScriptedModel::new(
        "main",
        [Step::gated_tool(
            model_gate.clone(),
            "write",
            json!({"path": "value.txt", "content": "agent\n"}),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let model_started = model.start_entered();
    let profile = Profile {
        tools: vec!["write".to_owned()],
        ..read_profile()
    };
    let fixture = fixture
        .models(BTreeMap::from([("main".to_owned(), Arc::clone(&model))]))
        .profile(profile);
    let (agent, info) = fixture.start().await;
    let gate = Arc::new(Gate::new());
    PAUSE_AFTER_RUNTIME_START.arm(info.session_id, Arc::clone(&gate));

    let agent = Arc::new(tokio::sync::Mutex::new(agent));
    let send_agent = Arc::clone(&agent);
    let mut send_task = tokio::spawn(async move {
        let mut agent = send_agent.lock().await;
        send_text(&mut agent, info.session_id, "write the file").await
    });

    let startup = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        gate.wait_started().await;
        model_started.notified().await;
        model_gate.wait_started().await;
    })
    .await;
    let startup_boundary_observed = startup.is_ok() && !send_task.is_finished();
    gate.release();
    model_gate.release();

    let send_result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut send_task).await;
    let turn = match send_result {
        Ok(Ok(turn)) => turn,
        Ok(Err(error)) => {
            let mut agent = agent.lock().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                agent.close_session(info.session_id),
            )
            .await;
            panic!("send task failed: {error}");
        }
        Err(_) => {
            send_task.abort();
            let _ = send_task.await;
            let mut agent = agent.lock().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                agent.close_session(info.session_id),
            )
            .await;
            panic!("install_loop did not leave the startup gate in time");
        }
    };
    assert!(
        startup_boundary_observed,
        "startup observation did not prove the bind boundary"
    );

    let mut agent = agent.lock().await;
    wait_text(&agent, turn).await;
    let listed = agent
        .changes_list(ChangesListRequest {
            session_id: info.session_id,
            scope: ChangeScope::Turn {
                loop_id: turn.loop_id,
            },
            cursor: None,
            limit: 10,
            max_bytes: None,
        })
        .await
        .unwrap();
    assert_eq!(listed.records.len(), 1, "the native write must be recorded");
    let record = &listed.records[0];
    let tool_ref = record
        .tool_ref
        .clone()
        .expect("a native change keeps its ToolRef");
    assert_eq!(tool_ref.session_id, info.session_id);
    assert_eq!(tool_ref.loop_id, turn.loop_id);
    assert_eq!(
        tool_ref.request_index, 0,
        "the first request owns the write"
    );
    assert_eq!(tool_ref.tool_call_id.as_str(), "write-call-0");

    let diff = agent
        .changes_diff(ChangesDiffRequest {
            session_id: info.session_id,
            change_ref: record.change_ref.clone(),
            comparison: Some(DiffComparison::ToolBeforeAfter),
            context_lines: None,
            cursor: None,
            max_bytes: None,
        })
        .await
        .unwrap();
    let removed = diff
        .hunks
        .iter()
        .flat_map(|hunk| hunk.lines.iter())
        .filter(|line| line.kind == crate::diff::DiffLineKind::Removed)
        .map(|line| line.text.as_str())
        .collect::<String>();
    let added = diff
        .hunks
        .iter()
        .flat_map(|hunk| hunk.lines.iter())
        .filter(|line| line.kind == crate::diff::DiffLineKind::Added)
        .map(|line| line.text.as_str())
        .collect::<String>();
    assert_eq!(removed, "user\n");
    assert_eq!(added, "agent\n");

    agent.close_session(info.session_id).await.unwrap();
}
