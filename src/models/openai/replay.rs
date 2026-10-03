//! Provider-owned, durable Responses output. Never serialize this as prose.
use super::*;
use minicore_runtime::model::ProviderReplay;

const FORMAT: &str = "openai-responses-v1";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    endpoint: String,
    model: String,
    output: Vec<Value>,
}

pub(super) fn capture(
    endpoint: &str,
    model: &str,
    output: Vec<Value>,
) -> Result<ProviderReplay, ()> {
    validate_output(&output)?;
    ProviderReplay::new(
        FORMAT,
        serde_json::to_value(Payload {
            endpoint: endpoint.to_owned(),
            model: model.to_owned(),
            output,
        })
        .map_err(|_| ())?,
    )
    .map_err(|_| ())
}

fn payload(replay: &ProviderReplay) -> Result<Option<Payload>, ()> {
    if replay.format() != FORMAT {
        return Ok(None);
    }
    let payload: Payload = serde_json::from_value(replay.payload().clone()).map_err(|_| ())?;
    let url = reqwest::Url::parse(&payload.endpoint).map_err(|_| ())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str() != payload.endpoint
        || payload.model.is_empty()
        || payload.model.len() > 256
    {
        return Err(());
    }
    validate_output(&payload.output)?;
    Ok(Some(payload))
}

pub(crate) fn validate_history(replay: &ProviderReplay, parts: &[AssistantPart]) -> Result<(), ()> {
    if let Some(payload) = payload(replay)? {
        validate_parts(&payload.output, parts)?;
    }
    Ok(())
}

pub(super) fn projection(
    replay: &ProviderReplay,
    parts: &[AssistantPart],
    endpoint: Option<&str>,
    model: &str,
) -> Result<Option<Vec<Value>>, ()> {
    let Some(payload) = payload(replay)? else {
        return Ok(None);
    };
    validate_parts(&payload.output, parts)?;
    // None is only the conservative generic utility estimator; it must include
    // replay rather than underestimate it through a synthetic model mismatch.
    if endpoint.is_some_and(|endpoint| endpoint != payload.endpoint || model != payload.model) {
        return Ok(None);
    }
    Ok(Some(payload.output))
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ()> {
    value.get(key).and_then(Value::as_str).ok_or(())
}

pub(super) fn validate_item(item: &Value) -> Result<(), ()> {
    let object = item.as_object().ok_or(())?;
    let kind = string(item, "type")?;
    // Output only: an input message or tool result can never hide in replay.
    if !matches!(kind, "message" | "reasoning" | "function_call") {
        return Err(());
    }
    if let Some(role) = object.get("role") {
        if role.as_str() != Some("assistant") {
            return Err(());
        }
    }
    if let Some(id) = object.get("id") {
        let id = id.as_str().ok_or(())?;
        if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            return Err(());
        }
    }
    if let Some(status) = object.get("status") {
        if !matches!(status.as_str(), Some("completed" | "incomplete")) {
            return Err(());
        }
    }
    match kind {
        "message" => {
            if string(item, "role")? != "assistant" || string(item, "id")?.is_empty() {
                return Err(());
            }
            let content = item.get("content").and_then(Value::as_array).ok_or(())?;
            if content.is_empty() {
                return Err(());
            }
            for part in content {
                match string(part, "type")? {
                    "output_text" => {
                        string(part, "text")?;
                    }
                    "refusal" => {
                        string(part, "refusal")?;
                    }
                    _ => return Err(()),
                }
            }
        }
        "reasoning" => {
            if string(item, "id")?.is_empty() {
                return Err(());
            }
            if let Some(encrypted) = item.get("encrypted_content") {
                if !encrypted.is_null() && !encrypted.is_string() {
                    return Err(());
                }
            }
            if let Some(summary) = item.get("summary") {
                for part in summary.as_array().ok_or(())? {
                    if string(part, "type")? != "summary_text" {
                        return Err(());
                    }
                    string(part, "text")?;
                }
            }
        }
        "function_call" => {
            validate_openai_call_id(string(item, "call_id")?).map_err(|_| ())?;
            string(item, "name")?.parse::<ToolName>().map_err(|_| ())?;
            let arguments: Value =
                serde_json::from_str(string(item, "arguments")?).map_err(|_| ())?;
            if !arguments.is_object() {
                return Err(());
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn validate_output(output: &[Value]) -> Result<(), ()> {
    if output.is_empty() || output.len() > MAX_REPLAY_ITEMS_PER_RESPONSE {
        return Err(());
    }
    let mut ids = BTreeSet::new();
    let mut calls = BTreeSet::new();
    let mut meaningful = false;
    for item in output {
        validate_item(item)?;
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            if !ids.insert(id) {
                return Err(());
            }
        }
        match item_type(item) {
            Some("function_call") => {
                if !calls.insert(string(item, "call_id")?) {
                    return Err(());
                }
                meaningful = true;
            }
            Some("message") => meaningful |= !visible_text(std::slice::from_ref(item))?.is_empty(),
            Some("reasoning") => {
                meaningful |= item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty());
                meaningful |= item
                    .get("summary")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| {
                        parts.iter().any(|p| {
                            p.get("text")
                                .and_then(Value::as_str)
                                .is_some_and(|s| !s.is_empty())
                        })
                    });
            }
            _ => return Err(()),
        }
    }
    if !meaningful {
        return Err(());
    }
    Ok(())
}

pub(super) fn visible_text(output: &[Value]) -> Result<String, ()> {
    let mut text = String::new();
    for item in output
        .iter()
        .filter(|item| item_type(item) == Some("message"))
    {
        for part in item.get("content").and_then(Value::as_array).ok_or(())? {
            let key = if item_type(part) == Some("refusal") {
                "refusal"
            } else {
                "text"
            };
            text.push_str(string(part, key)?);
        }
    }
    Ok(text)
}

fn validate_parts(output: &[Value], parts: &[AssistantPart]) -> Result<(), ()> {
    let calls = output
        .iter()
        .filter(|item| item_type(item) == Some("function_call"))
        .collect::<Vec<_>>();
    let canonical = parts
        .iter()
        .filter_map(AssistantPart::as_tool_call)
        .collect::<Vec<_>>();
    if calls.len() != canonical.len() {
        return Err(());
    }
    for (item, call) in calls.into_iter().zip(canonical) {
        if string(item, "call_id")? != call.tool_call_id().as_str()
            || string(item, "name")? != call.name().as_str()
            || serde_json::from_str::<Value>(string(item, "arguments")?).map_err(|_| ())?
                != *call.arguments()
        {
            return Err(());
        }
    }
    let text = parts
        .iter()
        .filter_map(|part| match part {
            AssistantPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    if visible_text(output)? != text {
        return Err(());
    }
    Ok(())
}

/// The terminal event may supply late ciphertext, but cannot rewrite anything
/// already observed. No other late changes are accepted.
pub(super) fn terminal_matches(done: &Value, terminal: &Value) -> bool {
    if done == terminal {
        return true;
    }
    if item_type(done) != Some("reasoning")
        || item_type(terminal) != Some("reasoning")
        || !done.get("encrypted_content").is_none_or(Value::is_null)
        || !terminal
            .get("encrypted_content")
            .is_some_and(Value::is_string)
    {
        return false;
    }
    let mut enriched = done.clone();
    enriched.as_object_mut().expect("reasoning object").insert(
        "encrypted_content".to_owned(),
        terminal["encrypted_content"].clone(),
    );
    enriched == *terminal
}
