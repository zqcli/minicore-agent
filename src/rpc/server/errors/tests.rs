use super::*;

#[test]
fn failed_encoding_after_acceptance_does_not_claim_the_input_was_rejected() {
    struct FailedReceipt;
    impl Serialize for FailedReceipt {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("private details"))
        }
    }
    let response = accepted_input_response(&RpcId::String("input".into()), FailedReceipt);
    assert!(response.is_error());
    assert_eq!(response.json()["error"]["data"]["acceptance"], "unknown");
    assert!(!String::from_utf8_lossy(response.encoded()).contains("private details"));
}
