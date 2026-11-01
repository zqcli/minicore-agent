use serde_json::Value;

pub fn read_items(page: &Value) -> Vec<Value> {
    page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|chunk| {
            assert_eq!(chunk["offset"], 0);
            assert_eq!(chunk["complete"], true, "fixture must fit in one page");
            serde_json::from_str(chunk["data"].as_str().unwrap()).unwrap()
        })
        .collect()
}

pub fn input_loop<'a>(snapshot: &'a Value, input_id: &Value) -> Option<&'a Value> {
    let execution = &snapshot["execution"];
    if execution["input"]["id"] == *input_id
        && let Some(loop_id) = execution.get("loop_id")
    {
        return Some(loop_id);
    }
    snapshot["completions"]["entries"]
        .as_array()?
        .iter()
        .find(|entry| entry["input_id"] == *input_id)
        .and_then(|entry| entry.get("loop_id"))
}

pub fn first_tool_ref(page: &Value) -> Value {
    read_items(page)
        .iter()
        .find_map(|item| {
            item["tool_summaries"]
                .as_array()
                .unwrap()
                .first()
                .map(|tool| tool["tool_ref"].clone())
        })
        .expect("fixture contains a tool call")
}
