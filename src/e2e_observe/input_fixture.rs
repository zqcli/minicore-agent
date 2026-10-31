use super::*;
use crate::tools::observe::ToolObserver;
use minicore_runtime::interaction::{InteractionAnswer, InteractionKind};
use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInputAnswer,
    ToolInputAnswerKind, ToolInputRequest, ToolInvocation, ToolSpec,
};
use std::sync::Arc;

pub(crate) const NAME: &str = "e2e_input";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    kind: Kind,
}
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Text,
    SingleChoice,
}

pub(crate) struct InputFixture {
    spec: ToolSpec,
    observer: Arc<ToolObserver>,
}
impl InputFixture {
    pub(crate) fn new(observer: Arc<ToolObserver>) -> Self {
        Self { spec:ToolSpec::new(NAME.parse().unwrap(),"Isolated test input",json!({"type":"object","properties":{"kind":{"type":"string","enum":["text","single_choice"]}},"required":["kind"],"additionalProperties":false})).expect("fixed fixture schema"),observer }
    }
}
impl Tool for InputFixture {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn execute<'a>(&'a self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'a> {
        Box::pin(async move {
            crate::tools::precheck_control(&context)?;
            let arguments: Arguments = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            let (prompt, choices, answer_kind) = match arguments.kind {
                Kind::Text => ("E2E text input", Vec::new(), ToolInputAnswerKind::Text),
                Kind::SingleChoice => (
                    "E2E single choice",
                    vec![
                        minicore_runtime::BoundedText::new("First choice").unwrap(),
                        minicore_runtime::BoundedText::new("Second choice").unwrap(),
                    ],
                    ToolInputAnswerKind::SingleChoice,
                ),
            };
            let request =
                ToolInputRequest::new(prompt, choices, answer_kind).expect("fixed input request");
            if let Some(observer) = get()
                && let Some(key) = self.observer.request_key()
            {
                let mut data = observer
                    .identity(self.observer.session_id(), key.loop_id)
                    .map(|identity| serde_json::to_value(identity).unwrap())
                    .unwrap_or_else(
                        || json!({"session_id":self.observer.session_id(),"loop_id":key.loop_id}),
                    );
                data["request_index"] = json!(key.request_index);
                data["tool_call_id"] = json!(invocation.tool_call_id());
                data["tool_name"] = json!(NAME);
                data["answer_kind"] = json!(arguments.kind);
                data["choices_count"] = json!(request.choices().len());
                observer.emit("input_fixture_request", data);
            }
            Ok(ToolExecutionOutcome::RequestInput(request))
        })
    }
}

pub(crate) enum AnswerClass {
    Text(usize),
    Choice(usize),
    Approval,
}
impl AnswerClass {
    pub(crate) fn of(answer: &InteractionAnswer) -> Self {
        match answer {
            InteractionAnswer::ToolInput(ToolInputAnswer::Text(text)) => {
                Self::Text(text.byte_len())
            }
            InteractionAnswer::ToolInput(ToolInputAnswer::Choice { index }) => Self::Choice(*index),
            InteractionAnswer::Approval(_) => Self::Approval,
        }
    }
}

pub(crate) fn answered(
    load: LoadId,
    state: &minicore_runtime::LoopState,
    interaction_id: minicore_runtime::InteractionId,
    answer: AnswerClass,
    accepted: bool,
) {
    let Some(observer) = get() else { return };
    let Some(pending) = state
        .pending_interaction
        .as_ref()
        .filter(|pending| pending.tool_name.as_str() == NAME)
    else {
        return;
    };
    let InteractionKind::ToolInput(request) = &pending.kind else {
        return;
    };
    let mut data = json!({"load":load,"loop_id":state.loop_id,"request_index":state.request_index,"tool_call_id":pending.tool_call_id,
        "tool_name":NAME,"interaction_id":interaction_id,"pending_interaction_id":pending.interaction_id,"choices_count":request.choices().len(),"accepted":accepted});
    match answer {
        AnswerClass::Text(bytes) => {
            data["answer_class"] = json!("text");
            data["answer_bytes"] = json!(bytes);
        }
        AnswerClass::Choice(index) => {
            data["answer_class"] = json!("single_choice");
            data["choice_index"] = json!(index);
        }
        AnswerClass::Approval => data["answer_class"] = json!("approval"),
    }
    let session = observer
        .identities
        .lock()
        .unwrap()
        .loads
        .iter()
        .find(|entry| entry.load == load)
        .map(|entry| entry.session_id);
    data["session_id"] = json!(session);
    observer.emit("input_fixture_answer", data);
}
