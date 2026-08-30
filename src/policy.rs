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
    fn decide<'a>(&'a self, request: ToolPolicyRequest) -> ToolPolicyFuture<'a> {
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
mod tests {
    use std::time::{Duration, Instant};

    use minicore_runtime::ids::{InteractionId, SessionId, SessionInstanceId, ToolCallId, TurnId};
    use minicore_runtime::session::{InteractionKind, PendingInteraction};
    use minicore_runtime::tools::{
        ApprovalRequest, ApprovalRisk, ToolDecision, ToolInvocation, ToolPolicy, ToolPolicyError,
        ToolPolicyRequest, ToolSpec,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::event::{AgentEvent, EventMeta};

    const KNOWN_TOOLS: &[&str] = &["read", "write", "edit", "apply_patch", "bash"];
    const MUTATING_TOOLS: &[&str] = &["write", "edit", "apply_patch", "bash"];
    const SECRET: &str = "TOP-SECRET-ARGUMENT";

    fn ids() -> (SessionId, SessionInstanceId, TurnId) {
        (
            "ses_00000000000000000000000000000001".parse().unwrap(),
            "ins_00000000000000000000000000000001".parse().unwrap(),
            "trn_00000000000000000000000000000001".parse().unwrap(),
        )
    }

    fn arguments() -> Value {
        json!({
            "secret": SECRET,
            "path": "/private/project",
            "content": "PRIVATE-CONTENT",
            "command": "curl https://private.invalid"
        })
    }

    fn request(name: &str) -> ToolPolicyRequest {
        request_with_names(
            name,
            name,
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
    }

    fn request_with_control(
        name: &str,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> ToolPolicyRequest {
        request_with_names(name, name, cancellation, deadline)
    }

    fn request_with_names(
        invocation_name: &str,
        spec_name: &str,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> ToolPolicyRequest {
        let (session_id, instance_id, turn_id) = ids();
        ToolPolicyRequest {
            invocation: ToolInvocation::new(
                session_id,
                instance_id,
                turn_id,
                ToolCallId::new("policy-call").unwrap(),
                invocation_name.parse().unwrap(),
                arguments(),
            )
            .unwrap(),
            spec: ToolSpec::new(
                spec_name.parse().unwrap(),
                "policy test tool",
                json!({"type": "object", "additionalProperties": true}),
            )
            .unwrap(),
            cancellation,
            deadline,
        }
    }

    fn expected_prompt(name: &str) -> String {
        format!("Allow tool `{name}` for this call?")
    }

    fn assert_approval(decision: &ToolDecision, name: &str) {
        let ToolDecision::RequireApproval { request } = decision else {
            panic!("expected approval for {name}");
        };
        assert_eq!(request.prompt.as_str(), expected_prompt(name));
        assert_eq!(request.risk, ApprovalRisk::Medium);
        assert!(request.validate().is_ok());
    }

    fn approval_from(decision: &ToolDecision) -> ApprovalRequest {
        match decision {
            ToolDecision::RequireApproval { request } => request.clone(),
            _ => panic!("expected approval decision"),
        }
    }

    fn serialized_approval_event(name: &str, request: ApprovalRequest) -> serde_json::Value {
        let (session_id, instance_id, turn_id) = ids();
        let interaction = PendingInteraction {
            interaction_id: "int_00000000000000000000000000000001"
                .parse::<InteractionId>()
                .unwrap(),
            turn_id,
            tool_call_id: ToolCallId::new("policy-call").unwrap(),
            tool_name: name.parse().unwrap(),
            kind: InteractionKind::Approval(request),
        };
        serde_json::to_value(AgentEvent::InteractionRequested {
            session_id,
            interaction,
            meta: EventMeta {
                session_id,
                instance_id,
                dropped_before: 0,
            },
        })
        .unwrap()
    }

    #[tokio::test]
    async fn known_tools_follow_the_three_mode_decision_table() {
        assert_eq!(ApprovalMode::default(), ApprovalMode::Ask);
        for mode in [
            ApprovalMode::Auto,
            ApprovalMode::Ask,
            ApprovalMode::ReadOnly,
        ] {
            let policy = Policy::new(mode);
            for name in KNOWN_TOOLS {
                let decision = policy.decide(request(name)).await.unwrap();
                assert!(decision.validate().is_ok());
                match (mode, *name, &decision) {
                    (ApprovalMode::Auto, _, ToolDecision::Allow)
                    | (ApprovalMode::Ask, "read", ToolDecision::Allow)
                    | (ApprovalMode::ReadOnly, "read", ToolDecision::Allow) => {}
                    (ApprovalMode::Ask, _, ToolDecision::RequireApproval { .. }) => {
                        assert_approval(&decision, name);
                    }
                    (ApprovalMode::ReadOnly, _, ToolDecision::Deny { reason }) => {
                        assert_eq!(reason.as_str(), READ_ONLY_DENIAL);
                    }
                    _ => panic!("unexpected policy decision for mode {mode:?} tool {name}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn approval_prompt_debug_and_agent_wire_do_not_expose_arguments() {
        let policy = Policy::new(ApprovalMode::Ask);
        for name in MUTATING_TOOLS {
            let policy_request = request(name);
            let request_debug = format!("{policy_request:?}");
            let decision = policy.decide(policy_request).await.unwrap();
            assert_approval(&decision, name);
            let approval = approval_from(&decision);
            let event = serialized_approval_event(name, approval.clone());
            let event_text = event.to_string();
            let decision_debug = format!("{decision:?}");
            let approval_debug = format!("{approval:?}");
            assert!(request_debug.contains("<redacted>"));

            assert_eq!(
                event["data"]["interaction"]["kind"],
                json!({
                    "type": "approval",
                    "data": {
                        "prompt": expected_prompt(name),
                        "risk": "medium"
                    }
                })
            );
            for text in [
                approval.prompt.as_str(),
                request_debug.as_str(),
                decision_debug.as_str(),
                approval_debug.as_str(),
                event_text.as_str(),
            ] {
                assert!(!text.contains(SECRET));
                assert!(!text.contains("PRIVATE-CONTENT"));
                assert!(!text.contains("curl https://private.invalid"));
                assert!(!text.contains("/private/project"));
            }
            for text in [
                approval.prompt.as_str(),
                decision_debug.as_str(),
                approval_debug.as_str(),
                event_text.as_str(),
            ] {
                assert!(!text.contains("arguments"));
            }
        }
    }

    #[tokio::test]
    async fn unknown_and_mismatched_tools_are_denied_in_every_mode() {
        for mode in [
            ApprovalMode::Auto,
            ApprovalMode::Ask,
            ApprovalMode::ReadOnly,
        ] {
            let policy = Policy::new(mode);
            for request in [
                request("unknown"),
                request_with_names(
                    "read",
                    "write",
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5),
                ),
            ] {
                let decision = policy.decide(request).await.unwrap();
                let ToolDecision::Deny { reason } = decision else {
                    panic!("unknown or mismatched tool was not denied");
                };
                assert_eq!(reason.as_str(), UNKNOWN_TOOL_DENIAL);
                assert!(ToolDecision::Deny { reason }.validate().is_ok());
            }
        }
    }

    #[tokio::test]
    async fn pre_cancel_wins_and_expired_deadline_fails_immediately() {
        let policy = Policy::new(ApprovalMode::Auto);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert_eq!(
            policy
                .decide(request_with_control(
                    "read",
                    cancellation,
                    Instant::now() - Duration::from_millis(1),
                ))
                .await,
            Err(ToolPolicyError::Cancelled)
        );
        assert_eq!(
            policy
                .decide(request_with_control(
                    "read",
                    CancellationToken::new(),
                    Instant::now() - Duration::from_millis(1),
                ))
                .await,
            Err(ToolPolicyError::Failed)
        );
        let result =
            tokio::time::timeout(Duration::from_millis(100), policy.decide(request("read")))
                .await
                .unwrap();
        assert_eq!(result, Ok(ToolDecision::Allow));
    }

    #[tokio::test]
    async fn ask_mode_has_no_approval_cache_or_cross_instance_state() {
        for policy in [
            Policy::new(ApprovalMode::Ask),
            Policy::new(ApprovalMode::Ask),
        ] {
            let first = policy.decide(request("write")).await.unwrap();
            let second = policy.decide(request("write")).await.unwrap();
            assert_approval(&first, "write");
            assert_approval(&second, "write");
            assert_eq!(first, second);
        }
    }
}
