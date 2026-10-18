use std::time::Instant;

use minicore_runtime::tools::{
    ApprovalRequest, ApprovalRisk, ToolDecision, ToolPolicy, ToolPolicyError, ToolPolicyFuture,
    ToolPolicyRequest,
};

use crate::ApprovalMode;

const READ_ONLY_DENIAL: &str = "tool is not allowed in read-only mode";
const UNKNOWN_TOOL_DENIAL: &str = "tool is not allowed by policy";

pub(crate) struct Policy {
    mode: ApprovalMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolClass {
    ReadOnly,
    Mutating,
}

impl Policy {
    pub(crate) fn new(mode: ApprovalMode) -> Self {
        Self { mode }
    }

    fn decide_now(&self, request: &ToolPolicyRequest) -> Result<ToolDecision, ToolPolicyError> {
        if request.cancellation.is_cancelled() {
            return Err(ToolPolicyError::Cancelled);
        }
        if Instant::now() >= request.deadline {
            return Err(ToolPolicyError::Failed);
        }
        if request.invocation.tool_name() != request.spec.name() {
            return deny(UNKNOWN_TOOL_DENIAL);
        }
        let name = request.invocation.tool_name().as_str();
        let Some(class) = classify(name) else {
            return deny(UNKNOWN_TOOL_DENIAL);
        };
        let decision = match (self.mode, class) {
            (ApprovalMode::Auto, _) | (_, ToolClass::ReadOnly) => ToolDecision::Allow,
            (ApprovalMode::Ask, ToolClass::Mutating) => {
                let prompt = format!("Allow tool `{name}` for this call?");
                let approval = ApprovalRequest::new(prompt, ApprovalRisk::Medium)
                    .map_err(|_| ToolPolicyError::Internal)?;
                ToolDecision::require_approval(approval).map_err(|_| ToolPolicyError::Internal)?
            }
            (ApprovalMode::ReadOnly, ToolClass::Mutating) => deny(READ_ONLY_DENIAL)?,
        };
        decision.validate().map_err(|_| ToolPolicyError::Internal)?;
        Ok(decision)
    }
}

impl ToolPolicy for Policy {
    fn decide(&self, request: ToolPolicyRequest) -> ToolPolicyFuture<'_> {
        Box::pin(std::future::ready(self.decide_now(&request)))
    }
}

fn classify(name: &str) -> Option<ToolClass> {
    match name {
        "read" => Some(ToolClass::ReadOnly),
        "write" | "edit" | "apply_patch" | "bash" => Some(ToolClass::Mutating),
        _ => None,
    }
}

fn deny(reason: &str) -> Result<ToolDecision, ToolPolicyError> {
    ToolDecision::deny(reason).map_err(|_| ToolPolicyError::Internal)
}

#[cfg(test)]
mod tests;
