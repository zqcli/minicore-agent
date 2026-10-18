use super::*;

#[derive(Deserialize)]
pub(super) struct FunctionCallItem {
    call_id: String,
    name: String,
    #[serde(default)]
    arguments: String,
}

pub(super) fn start_tool(
    state: &mut StreamState,
    output_index: u32,
    item: FunctionCallItem,
) -> Result<(), ()> {
    if state.tools.contains_key(&output_index) {
        return Err(());
    }
    let tool_call_id = openai_tool_call_id(&item.call_id)?;
    let name = item.name.parse::<ToolName>().map_err(|_| ())?;
    if !state.tool_ids.insert(tool_call_id.clone()) {
        return Err(());
    }
    state.queue(ModelEvent::ToolCallStart {
        tool_call_id: tool_call_id.clone(),
        tool_name: name.clone(),
    });
    state.tool_order.push(output_index);
    state.tools.insert(
        output_index,
        ToolState {
            tool_call_id,
            name,
            arguments: String::new(),
            arguments_done: false,
            item_done: false,
            ended: false,
        },
    );
    append_tool_arguments(state, output_index, &item.arguments)
}

pub(super) fn append_tool_arguments(
    state: &mut StreamState,
    output_index: u32,
    delta: &str,
) -> Result<(), ()> {
    let tool = state.tools.get_mut(&output_index).ok_or(())?;
    if tool.arguments_done || tool.item_done {
        return Err(());
    }
    let length = tool.arguments.len().checked_add(delta.len()).ok_or(())?;
    if length > MAX_JSON_BYTES {
        return Err(());
    }
    tool.arguments.push_str(delta);
    let tool_call_id = tool.tool_call_id.clone();
    for chunk in event_text_chunks(delta) {
        let event = ModelEvent::tool_call_arguments_delta(tool_call_id.clone(), chunk)
            .expect("event chunk fits the runtime byte limit");
        state.queue(event);
    }
    Ok(())
}

fn reconcile_arguments(
    state: &mut StreamState,
    output_index: u32,
    complete: &str,
) -> Result<(), ()> {
    let current = &state.tools.get(&output_index).ok_or(())?.arguments;
    let remainder = complete.strip_prefix(current).ok_or(())?;
    // Done events repeat the complete arguments after streaming has ended.
    if remainder.is_empty() {
        return Ok(());
    }
    append_tool_arguments(state, output_index, remainder)
}

pub(super) fn finish_tool_arguments(
    state: &mut StreamState,
    output_index: u32,
    arguments: &str,
) -> Result<(), ()> {
    if state
        .tools
        .get(&output_index)
        .is_some_and(|tool| tool.arguments_done)
    {
        return Err(());
    }
    reconcile_arguments(state, output_index, arguments)?;
    let tool = state.tools.get_mut(&output_index).ok_or(())?;
    tool.arguments_done = true;
    end_tool_once(state, output_index)
}

pub(super) fn finish_tool_item(
    state: &mut StreamState,
    output_index: u32,
    item: FunctionCallItem,
) -> Result<(), ()> {
    if let Some(tool) = state.tools.get(&output_index) {
        if tool.tool_call_id.as_str() != item.call_id || tool.name.as_str() != item.name {
            return Err(());
        }
        if tool.item_done {
            return Err(());
        }
        reconcile_arguments(state, output_index, &item.arguments)?;
    } else {
        start_tool(state, output_index, item)?;
    }
    state.tools.get_mut(&output_index).ok_or(())?.item_done = true;
    end_tool_once(state, output_index)
}

pub(super) fn openai_tool_call_id(value: &str) -> Result<ToolCallId, ()> {
    if value.len() > MAX_OPENAI_CALL_ID_BYTES {
        return Err(());
    }
    ToolCallId::new(value).map_err(|_| ())
}

pub(super) fn openai_request_call_id(value: &ToolCallId) -> Result<String, ModelError> {
    if value.as_str().len() > MAX_OPENAI_CALL_ID_BYTES {
        return Err(local_error(ModelErrorKind::InvalidRequest));
    }
    Ok(value.to_string())
}

fn end_tool_once(state: &mut StreamState, output_index: u32) -> Result<(), ()> {
    let tool = state.tools.get_mut(&output_index).ok_or(())?;
    if tool.ended {
        return Ok(());
    }
    if tool.arguments.is_empty() {
        return Err(());
    }
    tool.ended = true;
    let tool_call_id = tool.tool_call_id.clone();
    state.queue(ModelEvent::ToolCallEnd { tool_call_id });
    Ok(())
}
