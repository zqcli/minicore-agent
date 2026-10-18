use super::*;

#[tokio::test]
async fn session_read_pages_are_bounded_lossless_and_do_not_need_workspace() {
    let mut fixture = RpcFixture::new(
        "session-read-pages",
        [
            tool_calls(vec![ToolCallScript {
                name: "read",
                arguments: json!({"path": "huge.txt", "limit": 400}),
            }]),
            oversize(),
        ],
        &["read"],
        ApprovalMode::Auto,
    );
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let tool_text = (0..400)
        .map(|index| format!("tool {index} é🙂 \\\"\n"))
        .collect::<String>();
    tokio::fs::write(workspace.join("huge.txt"), &tool_text)
        .await
        .unwrap();
    let text = "prefix é🙂\n\t\\\" ".repeat(2_000);

    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    assert_eq!(
        harness.response(json!("wait")).await["result"]["persistence"],
        json!("persisted")
    );

    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    assert_eq!(
        harness.response(json!("close")).await["result"],
        json!({"ok": true})
    );
    let session_dir = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap());
    let session_path = session_dir.join("session.json");
    let mut metadata: Value =
        serde_json::from_slice(&std::fs::read(&session_path).unwrap()).unwrap();
    metadata["model"] = json!("vendor/model-name:v1.0");
    std::fs::write(&session_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
    let metadata_before = std::fs::read(&session_path).unwrap();
    let history_before = std::fs::read(session_dir.join("history.jsonl")).unwrap();
    std::fs::remove_dir_all(&workspace).unwrap();

    let mut cursor = None;
    let mut captured_end = None;
    let mut revision = None;
    let mut reconstructed = BTreeMap::<usize, String>::new();
    let mut complete = BTreeSet::new();
    for page_index in 0..128 {
        let request_id = json!(format!("read-{page_index}"));
        let mut params = json!({
            "session_id": session_id,
            "limit": 100,
            "max_bytes": 4_096,
        });
        if let Some(page_cursor) = cursor.clone() {
            params["cursor"] = page_cursor;
            params["captured_end"] = json!(captured_end.unwrap());
            params["history_revision"] = json!(revision.clone().unwrap());
        }
        harness
            .send(request_id.clone(), "session.read", Some(params))
            .await;
        let response = harness.response(request_id).await;
        assert!(
            response.get("error").is_none(),
            "unexpected read error: {response}"
        );
        let result = &response["result"];
        assert!(serde_json::to_vec(result).unwrap().len() <= 4_096);
        if captured_end.is_none() {
            captured_end = result["captured_end"].as_u64();
            revision = result["history_revision"].as_str().map(str::to_owned);
        } else {
            assert_eq!(result["captured_end"].as_u64(), captured_end);
            assert_eq!(result["history_revision"].as_str(), revision.as_deref());
        }
        for item in result["items"].as_array().unwrap() {
            let index = item["index"].as_u64().unwrap() as usize;
            let offset = item["offset"].as_u64().unwrap() as usize;
            let data = item["data"].as_str().unwrap();
            let entry = reconstructed.entry(index).or_default();
            assert_eq!(entry.len(), offset);
            entry.push_str(data);
            if item["complete"].as_bool().unwrap() {
                assert_eq!(
                    offset + data.len(),
                    item["total_bytes"].as_u64().unwrap() as usize
                );
                complete.insert(index);
            }
        }
        if result["next_cursor"].is_null() {
            break;
        }
        cursor = Some(result["next_cursor"].clone());
        assert!(page_index < 127, "read pagination did not make progress");
    }
    assert_eq!(complete.len(), reconstructed.len());
    let user = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .find(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("user"))
        .expect("the user item must be present");
    assert_eq!(user["item"]["data"]["input"]["text"], json!(text));
    let tool = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .find(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("tool_result"))
        .expect("the tool result must be present");
    let expected_tool = tool_text
        .lines()
        .enumerate()
        .map(|(index, line)| format!("{}: {line}", index + 1))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        tool["item"]["data"]["output"]["content"],
        json!(expected_tool)
    );
    let tool_call = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .filter(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("assistant"))
        .flat_map(|item| {
            item["item"]["data"]["content"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .find(|part| part["type"] == json!("tool_call"))
        .expect("the structured tool call must be present");
    assert_eq!(tool_call["data"]["arguments"]["path"], json!("huge.txt"));
    let assistant_text_bytes = reconstructed
        .values()
        .map(|item| serde_json::from_str::<Value>(item).unwrap())
        .filter(|item| item.pointer("/item/type").and_then(Value::as_str) == Some("assistant"))
        .flat_map(|item| {
            item["item"]["data"]["content"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .filter(|part| part["type"] == json!("text"))
        .filter_map(|part| part["data"].as_str().map(str::len))
        .sum::<usize>();
    assert_eq!(assistant_text_bytes, 128 * 1024);
    assert_eq!(
        std::fs::read(session_dir.join("session.json")).unwrap(),
        metadata_before
    );
    assert_eq!(
        std::fs::read(session_dir.join("history.jsonl")).unwrap(),
        history_before
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn unloaded_session_read_pages_short_turns_under_small_budget() {
    let mut fixture = RpcFixture::new("session-read-short-pages", [], &[], ApprovalMode::Auto);
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    for index in 0..100 {
        let send_id = json!(format!("short-send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({
                    "session_id": session_id,
                    "text": format!("short prompt {index}"),
                })),
            )
            .await;
        let turn = harness.response(send_id).await["result"]["turn"].clone();
        let wait_id = json!(format!("short-wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        assert_eq!(
            harness.response(wait_id).await["result"]["persistence"],
            json!("persisted")
        );
    }
    harness
        .send(
            json!("close"),
            "session.close",
            Some(json!({"session_id": session_id})),
        )
        .await;
    let _ = harness.response(json!("close")).await;

    let mut cursor = None;
    let mut captured_end = None;
    let mut history_revision = None;
    let mut pages = 0;
    loop {
        let request_id = json!(format!("short-read-{pages}"));
        let mut params = json!({
            "session_id": session_id,
            "limit": 100,
            "max_bytes": 4_096,
        });
        if let Some(next) = cursor.clone() {
            params["cursor"] = next;
            params["captured_end"] = json!(captured_end.unwrap());
            params["history_revision"] = json!(history_revision.clone().unwrap());
        }
        harness
            .send(request_id.clone(), "session.read", Some(params))
            .await;
        let response = harness.response(request_id).await;
        assert!(
            response["error"].is_null(),
            "unexpected read error: {response}"
        );
        assert!(serde_json::to_vec(&response["result"]).unwrap().len() <= 4_096);
        assert!(
            response["result"]["records"].as_array().unwrap().len()
                <= response["result"]["items"].as_array().unwrap().len()
        );
        if captured_end.is_none() {
            captured_end = response["result"]["captured_end"].as_u64();
            history_revision = response["result"]["history_revision"]
                .as_str()
                .map(str::to_owned);
        }
        pages += 1;
        if response["result"]["next_cursor"].is_null() {
            break;
        }
        cursor = Some(response["result"]["next_cursor"].clone());
        assert!(pages < 128, "short-turn pagination did not make progress");
    }
    assert!(pages > 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn session_read_rejects_same_length_prefix_replacement() {
    let mut fixture = RpcFixture::new(
        "session-read-revision",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    );
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    let text = "prefix-".to_owned() + &"é🙂 escaped ".repeat(2_000);
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": text})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    harness
        .send(json!("wait"), "turn.wait", Some(turn_params(&turn)))
        .await;
    let _ = harness.response(json!("wait")).await;
    let _ = harness
        .call("session.close", Some(json!({"session_id": session_id})))
        .await;

    let first_id = json!("read-first");
    harness
        .send(
            first_id.clone(),
            "session.read",
            Some(json!({
                "session_id": session_id,
                "limit": 100,
                "max_bytes": 4_096,
            })),
        )
        .await;
    let first = harness.response(first_id).await;
    assert!(first["result"]["next_cursor"].is_object());

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let mut history = std::fs::read(&history_path).unwrap();
    let prefix = history
        .windows(b"prefix".len())
        .position(|window| window == b"prefix")
        .expect("test history must contain the selected prefix");
    history[prefix] = b'P';
    std::fs::write(&history_path, &history).unwrap();

    let second = harness
        .call(
            "session.read",
            Some(json!({
                "session_id": session_id,
                "cursor": first["result"]["next_cursor"],
                "limit": 100,
                "max_bytes": 4_096,
                "captured_end": first["result"]["captured_end"],
                "history_revision": first["result"]["history_revision"],
            })),
        )
        .await;
    assert_eq!(second["error"]["code"], json!(-32_005));
    assert_eq!(second["error"]["data"]["kind"], json!("invalid_state"));

    harness.shutdown().await;
}

#[tokio::test]
async fn loaded_session_read_does_not_expose_a_disk_tail() {
    let mut fixture = RpcFixture::new(
        "session-read-loaded-prefix",
        [Step::Text("answer")],
        &[],
        ApprovalMode::Auto,
    );
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "committed"})),
        )
        .await;
    let turn = harness.response(json!("send")).await["result"]["turn"].clone();
    let _ = harness.call("turn.wait", Some(turn_params(&turn))).await;

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let original = std::fs::read(&history_path).unwrap();
    let newline = original
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("a persisted turn must have one JSONL line");
    let mut with_tail = original.clone();
    with_tail.extend_from_slice(&original[..=newline]);
    std::fs::write(&history_path, with_tail).unwrap();

    let read = harness
        .call_ok(
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    assert_eq!(read["result"]["session"]["loaded"], json!(true));
    assert_eq!(read["result"]["total"], json!(2));

    harness.shutdown().await;
}

#[tokio::test]
async fn loaded_session_read_keeps_a_captured_prefix_across_append() {
    let mut fixture = RpcFixture::new(
        "session-read-loaded-pagination",
        [oversize(), Step::Text("new answer")],
        &[],
        ApprovalMode::Auto,
    );
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    harness
        .send(
            json!("send-first"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "first"})),
        )
        .await;
    let first_turn = harness.response(json!("send-first")).await["result"]["turn"].clone();
    harness
        .send(
            json!("wait-first"),
            "turn.wait",
            Some(turn_params(&first_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("wait-first")).await["result"]["persistence"],
        json!("persisted")
    );

    let first_page = harness
        .call_ok(
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    assert!(first_page["result"]["next_cursor"].is_object());
    let captured_end = first_page["result"]["captured_end"].as_u64().unwrap();
    let history_revision = first_page["result"]["history_revision"].clone();
    let captured_total = first_page["result"]["total"].as_u64().unwrap();
    let mut cursor = first_page["result"]["next_cursor"].clone();
    let mut response_text = first_page["result"].to_string();
    let mut page_count = 1;

    harness
        .send(
            json!("send-second"),
            "turn.send",
            Some(json!({"session_id": session_id, "text": "second"})),
        )
        .await;
    let second_turn = harness.response(json!("send-second")).await["result"]["turn"].clone();
    harness
        .send(
            json!("wait-second"),
            "turn.wait",
            Some(turn_params(&second_turn)),
        )
        .await;
    assert_eq!(
        harness.response(json!("wait-second")).await["result"]["persistence"],
        json!("persisted")
    );

    while !cursor.is_null() {
        let request_id = json!(format!("read-next-{page_count}"));
        harness
            .send(
                request_id.clone(),
                "session.read",
                Some(json!({
                    "session_id": session_id,
                    "cursor": cursor,
                    "limit": 100,
                    "max_bytes": 4_096,
                    "captured_end": captured_end,
                    "history_revision": history_revision,
                })),
            )
            .await;
        let page = harness.response(request_id).await;
        assert!(
            page["error"].is_null(),
            "unexpected continuation error: {page}"
        );
        assert_eq!(page["result"]["total"].as_u64(), Some(captured_total));
        assert_eq!(page["result"]["captured_end"].as_u64(), Some(captured_end));
        assert_eq!(page["result"]["history_revision"], history_revision);
        assert!(!page.to_string().contains("new answer"));
        response_text.push_str(&page["result"].to_string());
        cursor = page["result"]["next_cursor"].clone();
        page_count += 1;
        assert!(page_count < 128, "loaded pagination did not make progress");
    }
    assert!(page_count > 1);
    assert!(!response_text.contains("new answer"));

    harness.shutdown().await;
}

#[tokio::test]
async fn session_read_reports_incomplete_tail_and_corrupt_middle_without_repair() {
    let mut fixture = RpcFixture::new(
        "session-read-integrity",
        [Step::Text("first"), Step::Text("second")],
        &[],
        ApprovalMode::Auto,
    );
    let base = fixture.base.path().to_owned();
    let workspace = fixture.workspace.clone();
    let agent = fixture.open().await;
    let mut harness = RpcHarness::spawn(agent);
    let session_id = create_and_open(&mut harness, &workspace).await;
    for (index, text) in ["one", "two"].into_iter().enumerate() {
        let send_id = json!(format!("send-{index}"));
        harness
            .send(
                send_id.clone(),
                "turn.send",
                Some(json!({"session_id": session_id, "text": text})),
            )
            .await;
        let turn = harness.response(send_id).await["result"]["turn"].clone();
        let wait_id = json!(format!("wait-{index}"));
        harness
            .send(wait_id.clone(), "turn.wait", Some(turn_params(&turn)))
            .await;
        assert_eq!(
            harness.response(wait_id).await["result"]["persistence"],
            json!("persisted")
        );
    }
    let _ = harness
        .call("session.close", Some(json!({"session_id": session_id})))
        .await;

    let history_path = base
        .join("data")
        .join("sessions")
        .join(session_id.as_str().unwrap())
        .join("history.jsonl");
    let original = std::fs::read(&history_path).unwrap();
    let mut with_tail = original.clone();
    with_tail.extend_from_slice(b"{\"incomplete\"");
    std::fs::write(&history_path, &with_tail).unwrap();

    let tail = harness
        .call_ok(
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    assert_eq!(tail["result"]["trailing_incomplete"], json!(true));
    assert_eq!(tail["result"]["total"], json!(4));
    assert_eq!(std::fs::read(&history_path).unwrap(), with_tail);

    let second_line = original
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| offset + 1)
        .expect("two turns must produce a second JSONL line");
    assert_eq!(original[second_line], b'{');
    let mut corrupt = with_tail;
    corrupt[second_line] = b'X';
    std::fs::write(&history_path, &corrupt).unwrap();
    let corrupt_response = harness
        .call(
            "session.read",
            Some(json!({"session_id": session_id, "limit": 100, "max_bytes": 4_096})),
        )
        .await;
    assert_eq!(corrupt_response["error"]["code"], json!(-32_011));
    assert_eq!(
        corrupt_response["error"]["data"]["kind"],
        json!("store_error")
    );
    assert_eq!(std::fs::read(&history_path).unwrap(), corrupt);

    harness.shutdown().await;
}
