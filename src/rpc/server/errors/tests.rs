use super::*;

#[test]
fn input_receipt_errors_after_acceptance_are_unknown_never_rejected() {
    let id = RpcId::String("input".into());
    let response =
        accepted_input_response::<crate::InputPushed>(&id, Err(AgentError::Internal)).json();
    assert_eq!(response["error"]["data"]["acceptance"], "unknown");
    assert_eq!(response["error"]["data"]["kind"], "internal_error");
    assert!(response.get("result").is_none());
    let rejected = agent_error(id, &AgentError::InputQueueFull)
        .rejected_input()
        .json();
    assert_eq!(rejected["error"]["data"]["acceptance"], "rejected");
    assert_eq!(rejected["error"]["data"]["kind"], "input_queue_full");
}

#[test]
fn failed_encoding_after_acceptance_does_not_claim_the_input_was_rejected() {
    struct FailedReceipt;
    impl Serialize for FailedReceipt {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("private details"))
        }
    }
    let response = accepted_input_response(&RpcId::String("input".into()), Ok(FailedReceipt));
    assert!(response.is_error());
    assert_eq!(response.json()["error"]["data"]["acceptance"], "unknown");
    assert!(!String::from_utf8_lossy(response.encoded()).contains("private details"));
}
