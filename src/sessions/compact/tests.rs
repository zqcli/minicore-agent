use super::*;
#[test]
fn origin_is_owned_by_operation_not_inferred_from_identifier() {
    for (id, origin) in [
        ("auto-looking-manual", CompactionOrigin::Manual),
        ("opaque-id", CompactionOrigin::Automatic),
    ] {
        let (operation, _) = CompactionOperation::new(id.into(), origin);
        assert_eq!(operation.failed_result("cancelled", None).origin, origin);
        assert_eq!(operation.unknown_write_result().origin, origin);
        assert_eq!(failed_compaction(&operation, "timeout", 0).origin, origin);
    }
}

pub(super) async fn pause_after_result(session_id: SessionId) {
    if let Some(gate) = PAUSE_AFTER_RESULT.take(&session_id) {
        gate.pass().await;
    }
}
