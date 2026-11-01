use super::*;

#[tokio::test]
async fn delivered_steers_join_the_next_preparation_across_all_active_config_replacements() {
    let first = Arc::new(Gate::new());
    let second = Arc::new(Gate::new());
    let third = Arc::new(Gate::new());
    let main = ScriptedModel::new(
        "main",
        [
            Step::gated_tool(first.clone(), "read", json!({"path":"a.txt"})),
            Step::Text("done"),
        ],
    );
    let other = ScriptedModel::new(
        "other",
        [
            Step::gated_tool(second.clone(), "read", json!({"path":"a.txt"})),
            Step::gated_tool(third.clone(), "read", json!({"path":"a.txt"})),
        ],
    );
    let fixture = AgentFixture::new("delivered-boundaries")
        .file("a.txt", b"content")
        .models(BTreeMap::from([
            ("main".into(), main.clone()),
            ("other".into(), other.clone()),
        ]));
    let (mut agent, info) = fixture.start().await;
    let load = agent.loaded_session(info.session_id).unwrap().load();
    let mut events = agent.take_events().unwrap();
    let turn = send_text(&mut agent, info.session_id, "read").await;
    let updates = [
        (&first, Some("other"), None),
        (&second, None, Some(ReasoningPreference::High)),
        (&third, Some("main"), None),
    ];
    let mut accepted = Vec::new();
    for (index, (gate, model, reasoning)) in updates.into_iter().enumerate() {
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
            .await
            .unwrap();
        agent
            .update_session(UpdateSession {
                session_id: info.session_id,
                model: model.map(str::to_owned),
                reasoning,
            })
            .await
            .unwrap();
        accepted.push(
            push(
                &agent,
                info.session_id,
                InputMode::Steer,
                &format!("steer-{index}"),
            )
            .await
            .id,
        );
        gate.release();
    }
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), agent.wait_turn(turn))
        .await
        .unwrap()
        .unwrap();
    assert_completed(&result);
    assert_eq!(result.requests, 4);
    let mut delivered = Vec::new();
    while delivered.len() < 3 {
        let event = events
            .next_event(std::time::Duration::from_secs(5), "delivered steer")
            .await;
        if let AgentEvent::InputDelivered {
            turn: identity,
            items,
            meta,
        } = event
        {
            assert_eq!(identity, turn);
            assert_eq!(meta.load, load);
            assert_eq!(meta.loop_id, Some(turn.loop_id));
            delivered.extend(items);
        }
    }
    assert_eq!(
        delivered
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
        accepted
    );
    assert_eq!(
        delivered
            .iter()
            .map(|item| item.request_index)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(main.requests().lock().unwrap().len(), 2);
    assert_eq!(other.requests().lock().unwrap().len(), 2);
    agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_prompt_rebuilds_and_replaced_prompts_keep_the_preparation_identity() {
    let first = Arc::new(Gate::new());
    let rebuilt = Arc::new(Gate::new());
    let model_gate = Arc::new(Gate::new());
    let after_tools = Arc::new(Gate::new());
    let main = ScriptedModel::new("main", []);
    let other = ScriptedModel::new(
        "other",
        [
            Step::gated_tool(model_gate.clone(), "read", json!({"path":"a.txt"})),
            Step::Text("done"),
        ],
    );
    let fixture = AgentFixture::new("rebuilt-boundaries")
        .file("a.txt", b"content")
        .models(BTreeMap::from([
            ("main".into(), main.clone()),
            ("other".into(), other.clone()),
        ]));
    let (mut agent, info) = fixture.start().await;
    let mut events = agent.take_events().unwrap();
    let root = agent
        .loaded_session(info.session_id)
        .unwrap()
        .workspace()
        .root()
        .to_owned();
    crate::prompt::READ_GATE.arm(root.clone(), first.clone());
    let turn = send_text(&mut agent, info.session_id, "read").await;
    let mut ids = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), first.wait_started())
        .await
        .unwrap();
    ids.push(
        push(
            &agent,
            info.session_id,
            InputMode::Steer,
            "during original prompt",
        )
        .await
        .id,
    );
    crate::prompt::READ_GATE.arm(root.clone(), rebuilt.clone());
    first.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), rebuilt.wait_started())
        .await
        .unwrap();
    agent
        .update_session(UpdateSession {
            session_id: info.session_id,
            model: Some("other".into()),
            reasoning: Some(ReasoningPreference::High),
        })
        .await
        .unwrap();
    ids.push(
        push(
            &agent,
            info.session_id,
            InputMode::Steer,
            "during stale rebuild",
        )
        .await
        .id,
    );
    rebuilt.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), model_gate.wait_started())
        .await
        .unwrap();
    ids.push(
        push(
            &agent,
            info.session_id,
            InputMode::Steer,
            "before next preparation",
        )
        .await
        .id,
    );
    crate::prompt::READ_GATE.arm(root, after_tools.clone());
    model_gate.release();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        after_tools.wait_started(),
    )
    .await
    .unwrap();
    ids.push(
        push(
            &agent,
            info.session_id,
            InputMode::Steer,
            "during next preparation",
        )
        .await
        .id,
    );
    after_tools.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), agent.wait_turn(turn))
        .await
        .unwrap()
        .unwrap();
    assert_completed(&result);
    assert_eq!(result.requests, 2);
    let mut delivered = Vec::new();
    while delivered.len() < 4 {
        if let AgentEvent::InputDelivered {
            turn: identity,
            items,
            ..
        } = events
            .next_event(
                std::time::Duration::from_secs(5),
                "preparation input delivery",
            )
            .await
        {
            assert_eq!(identity, turn);
            delivered.extend(items);
        }
    }
    assert_eq!(
        delivered
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(
        delivered
            .iter()
            .map(|item| item.request_index)
            .collect::<Vec<_>>(),
        [0, 0, 1, 1]
    );
    assert!(main.requests().lock().unwrap().is_empty());
    {
        let requests = other.requests();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let user_texts = |request: &minicore_runtime::model::ModelRequest| {
            request
                .messages()
                .iter()
                .filter_map(|message| match message {
                    ModelMessage::User(text) => Some(text.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            user_texts(&requests[0]),
            ["read", "during original prompt", "during stale rebuild"]
        );
        assert_eq!(
            user_texts(&requests[1]),
            [
                "read",
                "during original prompt",
                "during stale rebuild",
                "before next preparation",
                "during next preparation"
            ]
        );
    }
    agent.shutdown().await.unwrap();
}
