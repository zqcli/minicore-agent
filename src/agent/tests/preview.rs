use super::*;

fn preview_write_events(source: &str, complete: bool) -> Vec<Result<ModelEvent, ModelError>> {
    let call = ToolCallId::new("preview-write").unwrap();
    let mut events = vec![
        Ok(ModelEvent::ToolCallStart {
            tool_call_id: call.clone(),
            tool_name: "write".parse().unwrap(),
        }),
        Ok(ModelEvent::tool_call_arguments_delta(call.clone(), source).unwrap()),
    ];
    if complete {
        events.push(Ok(ModelEvent::ToolCallEnd { tool_call_id: call }));
        events.push(Ok(ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        }));
    }
    events
}

#[tokio::test]
async fn tool_argument_preview_cancel_leaves_existing_and_new_files_untouched() {
    use crate::ToolArgumentsPreviewState;
    for (name, existing) in [("existing.txt", true), ("new.txt", false)] {
        let (data_dir, _data_guard) = fixture_dir(&format!("preview-cancel-data-{}", next_id()));
        let (workspace, _workspace_guard) = workspace_file(
            &format!("preview-cancel-ws-{}", next_id()),
            "existing.txt",
            b"original bytes\n",
        );
        let path = workspace.join(name);
        let before = std::fs::metadata(&path)
            .ok()
            .and_then(|meta| meta.modified().ok());
        let gate = Arc::new(Gate::new());
        let source = format!(r#"{{"path":"{name}","content":"UNFINISHED"#);
        let model = ScriptedModel::new(
            "main",
            [Step::PreviewEvents(
                preview_write_events(&source, false),
                Some(gate.clone()),
            )],
        )
        .with_pacing(std::time::Duration::from_millis(2));
        let profile = Profile {
            tools: vec!["write".to_owned()],
            ..read_profile()
        };
        let mut agent = open_agent(
            &data_dir,
            BTreeMap::from([("main".to_owned(), model)]),
            profile,
        )
        .await;
        let mut events = agent.take_events().unwrap();
        let info = create_session(&mut agent, &workspace).await;
        let turn = send_text(
            &mut agent,
            info.session_id,
            "synthetic half write then cancel",
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
            .await
            .unwrap();
        if existing {
            assert_eq!(std::fs::read(&path).unwrap(), b"original bytes\n");
            assert_eq!(std::fs::metadata(&path).unwrap().modified().ok(), before);
        } else {
            assert!(!path.exists());
        }
        assert!(agent.cancel(turn).unwrap());
        let result = wait_text(&agent, turn).await;
        assert!(matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Cancelled(_)
        ));
        let mut generating = false;
        let mut discarded = false;
        loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .unwrap()
                .unwrap();
            match event {
                AgentEvent::ToolArgumentsPreview { state, display, .. } => match state {
                    ToolArgumentsPreviewState::Generating => {
                        if display.expanded_input.as_deref() == Some("UNFINISHED") {
                            generating = true;
                        }
                    }
                    ToolArgumentsPreviewState::Discarded => discarded = true,
                    ToolArgumentsPreviewState::Generated => panic!("partial input cannot finish"),
                },
                AgentEvent::ToolInvocation { .. } | AgentEvent::ToolStarted { .. } => {
                    panic!("partial JSON reached execution")
                }
                AgentEvent::TurnFinished { .. } => break,
                _ => {}
            }
        }
        assert!(generating && discarded);
        assert!(
            !result
                .report
                .appended
                .iter()
                .any(|item| matches!(item, HistoryItem::ToolResult(_)))
        );
        if existing {
            assert_eq!(std::fs::read(&path).unwrap(), b"original bytes\n");
            assert_eq!(std::fs::metadata(&path).unwrap().modified().ok(), before);
        } else {
            assert!(!path.exists());
        }
        agent.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn tool_argument_preview_assembler_rejection_and_post_finish_event_never_execute() {
    use crate::ToolArgumentsPreviewState;
    for (source, extra) in [
        (r#"{"path":"target.txt","content":"bad\q"}"#, false),
        (
            r#"{"path":"target.txt","content":"valid but extra event"}"#,
            true,
        ),
    ] {
        let (data_dir, _data_guard) = fixture_dir(&format!("preview-reject-data-{}", next_id()));
        let (workspace, _workspace_guard) = workspace_file(
            &format!("preview-reject-ws-{}", next_id()),
            "target.txt",
            b"original",
        );
        let path = workspace.join("target.txt");
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut raw = preview_write_events(source, true);
        if extra {
            raw.push(Ok(
                ModelEvent::text_delta("unexpected after finish").unwrap()
            ));
        }
        let model = ScriptedModel::new("main", [Step::PreviewEvents(raw, None)])
            .with_pacing(std::time::Duration::from_millis(2));
        let mut agent = open_agent(
            &data_dir,
            BTreeMap::from([("main".to_owned(), model)]),
            Profile {
                tools: vec!["write".to_owned()],
                ..read_profile()
            },
        )
        .await;
        let mut events = agent.take_events().unwrap();
        let info = create_session(&mut agent, &workspace).await;
        let turn = send_text(&mut agent, info.session_id, "synthetic malformed stream").await;
        let result = wait_text(&agent, turn).await;
        assert!(!matches!(
            result.report.outcome,
            minicore_runtime::LoopOutcome::Completed
        ));
        let mut discarded = false;
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                AgentEvent::ToolArgumentsPreview {
                    state: ToolArgumentsPreviewState::Discarded,
                    ..
                } => discarded = true,
                AgentEvent::ToolInvocation { .. } | AgentEvent::ToolStarted { .. } => {
                    panic!("invalid model response reached execution")
                }
                AgentEvent::TurnFinished { .. } => break,
                _ => {}
            }
        }
        assert!(discarded);
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            before
        );
        agent.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn tool_argument_preview_normal_completion_is_followed_by_authoritative_invocation() {
    use crate::ToolArgumentsPreviewState;
    let (data_dir, _data_guard) = fixture_dir(&format!("preview-upgrade-data-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file(
        &format!("preview-upgrade-ws-{}", next_id()),
        "target.txt",
        b"original",
    );
    let source = r#"{"path":"target.txt","content":"new content\n"}"#;
    let model = ScriptedModel::new(
        "main",
        [
            Step::PreviewEvents(preview_write_events(source, true), None),
            Step::Text("done"),
        ],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let mut agent = open_agent(
        &data_dir,
        BTreeMap::from([("main".to_owned(), model)]),
        Profile {
            tools: vec!["write".to_owned()],
            ..read_profile()
        },
    )
    .await;
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "synthetic complete write").await;
    let result = wait_text(&agent, turn).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    let mut generated = false;
    let mut invoked = false;
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentEvent::ToolArgumentsPreview {
                state: ToolArgumentsPreviewState::Generated,
                partial,
                ..
            } => {
                generated = true;
                assert!(!partial);
            }
            AgentEvent::ToolArgumentsPreview {
                state: ToolArgumentsPreviewState::Discarded,
                ..
            } => panic!("normal EOF discarded preview"),
            AgentEvent::ToolInvocation { data, .. } => {
                invoked = true;
                assert!(generated);
                assert_eq!(data.tool_ref.tool_call_id.as_str(), "preview-write");
            }
            AgentEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }
    assert!(generated && invoked);
    assert_eq!(
        std::fs::read(workspace.join("target.txt")).unwrap(),
        b"new content\n"
    );
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn tool_argument_preview_model_deadline_discards_without_workspace_io() {
    use crate::ToolArgumentsPreviewState;
    let (data_dir, _data_guard) = fixture_dir(&format!("preview-deadline-data-{}", next_id()));
    let (workspace, _workspace_guard) = workspace_file(
        &format!("preview-deadline-ws-{}", next_id()),
        "target.txt",
        b"unchanged",
    );
    let gate = Arc::new(Gate::new());
    let model = ScriptedModel::new(
        "main",
        [Step::PreviewEvents(
            preview_write_events(r#"{"path":"target.txt","content":"half"#, false),
            Some(gate.clone()),
        )],
    )
    .with_pacing(std::time::Duration::from_millis(2));
    let profile = Profile {
        tools: vec!["write".to_owned()],
        ..read_profile()
    };
    let mut config = config(
        data_dir,
        BTreeMap::from([("main".to_owned(), model_config("SYNTHETIC_PREVIEW_KEY"))]),
        profile,
    );
    config.loop_options.model_timeout_seconds = Some(1);
    let mut agent = Agent::open_with_models(
        config,
        Models::from_values(BTreeMap::from([(
            "main".to_owned(),
            model as Arc<dyn Model>,
        )])),
    )
    .await
    .unwrap();
    let mut events = agent.take_events().unwrap();
    let info = create_session(&mut agent, &workspace).await;
    let turn = send_text(&mut agent, info.session_id, "synthetic deadline").await;
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), wait_text(&agent, turn))
        .await
        .unwrap();
    assert!(!matches!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    ));
    let mut discarded = false;
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentEvent::ToolArgumentsPreview {
                state: ToolArgumentsPreviewState::Discarded,
                ..
            } => discarded = true,
            AgentEvent::ToolInvocation { .. } | AgentEvent::ToolStarted { .. } => {
                panic!("timed out prefix reached execution")
            }
            AgentEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }
    assert!(discarded);
    assert_eq!(
        std::fs::read(workspace.join("target.txt")).unwrap(),
        b"unchanged"
    );
    agent.shutdown().await.unwrap();
}
