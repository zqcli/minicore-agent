use super::*;

pub(in crate::models) fn endpoint(base_url: &str) -> Result<reqwest::Url, ModelConfigError> {
    let parsed =
        reqwest::Url::parse(base_url).map_err(|_| ModelConfigError::InvalidConfiguration)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.host_str().is_none()
    {
        return Err(ModelConfigError::InvalidConfiguration);
    }
    let endpoint = format!("{}/responses", base_url.trim_end_matches('/'));
    reqwest::Url::parse(&endpoint).map_err(|_| ModelConfigError::InvalidConfiguration)
}

#[derive(Serialize)]
pub(super) struct ResponsesRequest<'a> {
    model: &'a str,
    input: Vec<InputItem>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<FunctionTool>,
    stream: bool,
    store: bool,
    truncation: &'static str,
    max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    include: Option<[&'static str; 1]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ReasoningWire>,
}

impl<'a> ResponsesRequest<'a> {
    pub(super) fn from_runtime(
        model: &'a str,
        output_budget_tokens: u32,
        request: &ModelRequest,
        endpoint: Option<&str>,
        supports_reasoning: bool,
    ) -> Result<Self, ModelError> {
        let mut input = Vec::new();
        for (message_index, message) in request.messages().iter().enumerate() {
            match message {
                ModelMessage::System(text) | ModelMessage::User(text) => {
                    input.push(InputItem::Message(InputMessage {
                        item_type: "message",
                        role: if matches!(message, ModelMessage::System(_)) {
                            "developer"
                        } else {
                            "user"
                        },
                        content: vec![InputContent {
                            content_type: "input_text",
                            text: text.clone(),
                            annotations: None,
                        }],
                        status: None,
                        id: None,
                    }))
                }
                ModelMessage::Assistant(parts)
                | ModelMessage::AssistantWithReplay { parts, .. } => {
                    if let ModelMessage::AssistantWithReplay {
                        provider_replay, ..
                    } = message
                    {
                        match replay::projection(provider_replay, parts, endpoint, model)
                            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?
                        {
                            Some(output) => {
                                input.extend(output.into_iter().map(InputItem::Raw));
                                continue;
                            }
                            None if parts.is_empty() => {
                                return Err(local_error(ModelErrorKind::InvalidRequest));
                            }
                            None => {}
                        }
                    }

                    for (part_index, part) in parts.iter().enumerate() {
                        match part {
                            AssistantPart::Text(text) => {
                                input.push(InputItem::Message(InputMessage::assistant(
                                    Some(format!("msg_minicore_{message_index}_{part_index}")),
                                    text.clone(),
                                )));
                            }
                            AssistantPart::Reasoning(_) => {
                                // MiniCore's public ReasoningContent does not preserve the exact
                                // provider item identity needed for safe Responses replay.
                            }
                            AssistantPart::ToolCall(call) => {
                                input.push(InputItem::FunctionCall(
                                    FunctionCallInput::from_runtime(call)?,
                                ));
                            }
                        }
                    }
                }
                ModelMessage::Tool {
                    tool_call_id,
                    output,
                    ..
                } => input.push(InputItem::FunctionCallOutput(
                    FunctionCallOutputInput::from_runtime(tool_call_id, output)?,
                )),
            }
        }
        let tools = request
            .tools()
            .iter()
            .map(|tool| FunctionTool {
                tool_type: "function",
                name: tool.name().as_str().to_owned(),
                description: tool.description().as_str().to_owned(),
                parameters: tool.input_schema().clone(),
            })
            .collect();
        let effort = match request.reasoning() {
            ReasoningPreference::Auto => None,
            ReasoningPreference::Disabled => Some("none"),
            ReasoningPreference::Low => Some("low"),
            ReasoningPreference::Medium => Some("medium"),
            ReasoningPreference::High => Some("high"),
            ReasoningPreference::XHigh => Some("xhigh"),
            ReasoningPreference::Max => Some("max"),
            ReasoningPreference::Ultra => Some("ultra"),
        };
        let reasoning = effort.map(|effort| ReasoningWire {
            effort,
            summary: (effort != "none").then_some("auto"),
        });
        Ok(Self {
            model,
            input,
            tools,
            stream: true,
            store: false,
            truncation: "disabled",
            max_output_tokens: output_budget_tokens,
            include: (supports_reasoning && request.reasoning() != ReasoningPreference::Disabled)
                .then_some(["reasoning.encrypted_content"]),
            reasoning,
        })
    }
}

pub(in crate::models) fn serialize_request_for_budget(
    request: &ModelRequest,
    writer: &mut dyn std::io::Write,
) -> Result<(), ModelError> {
    let model = "m".repeat(256);
    let body = ResponsesRequest::from_runtime(&model, u32::MAX, request, None, true)?;
    serde_json::to_writer(writer, &body).map_err(|_| local_error(ModelErrorKind::InvalidRequest))
}

#[derive(Serialize)]
#[serde(untagged)]
enum InputItem {
    Raw(Value),
    Message(InputMessage),
    FunctionCall(FunctionCallInput),
    FunctionCallOutput(FunctionCallOutputInput),
}

#[derive(Serialize)]
pub(super) struct InputMessage {
    #[serde(rename = "type")]
    item_type: &'static str,
    role: &'static str,
    content: Vec<InputContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

impl InputMessage {
    pub(super) fn assistant(id: Option<String>, text: Arc<str>) -> Self {
        Self {
            item_type: "message",
            role: "assistant",
            content: vec![InputContent {
                content_type: "output_text",
                text,
                annotations: Some(Vec::new()),
            }],
            status: Some("completed"),
            id,
        }
    }
}

#[derive(Serialize)]
struct InputContent {
    #[serde(rename = "type")]
    content_type: &'static str,
    text: Arc<str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    annotations: Option<Vec<Value>>,
}

#[derive(Serialize)]
pub(super) struct FunctionCallInput {
    #[serde(rename = "type")]
    item_type: &'static str,
    pub(super) call_id: String,
    name: String,
    arguments: String,
    status: &'static str,
}

impl FunctionCallInput {
    pub(super) fn from_runtime(
        call: &minicore_runtime::model::ToolCall,
    ) -> Result<Self, ModelError> {
        let call_id = openai_request_call_id(call.tool_call_id())?;
        let arguments = serde_json::to_string(call.arguments())
            .map_err(|_| local_error(ModelErrorKind::InvalidRequest))?;
        Ok(Self {
            item_type: "function_call",
            call_id,
            name: call.name().as_str().to_owned(),
            arguments,
            status: "completed",
        })
    }
}

#[derive(Serialize)]
pub(super) struct FunctionCallOutputInput {
    #[serde(rename = "type")]
    item_type: &'static str,
    pub(super) call_id: String,
    output: String,
    status: &'static str,
}

impl FunctionCallOutputInput {
    pub(super) fn from_runtime(
        tool_call_id: &ToolCallId,
        output: &minicore_runtime::tools::ToolOutput,
    ) -> Result<Self, ModelError> {
        Ok(Self {
            item_type: "function_call_output",
            call_id: openai_request_call_id(tool_call_id)?,
            output: output.content().as_str().to_owned(),
            status: "completed",
        })
    }
}

#[derive(Serialize)]
struct FunctionTool {
    #[serde(rename = "type")]
    tool_type: &'static str,
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Serialize)]
struct ReasoningWire {
    effort: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<&'static str>,
}
