use super::*;

fn identity(load: LoadId, generation: u64) -> StoredIdentity {
    let loop_id = LoopId::new().unwrap();
    StoredIdentity::new(
        StoredIdentityKey {
            load,
            source_revision: [generation as u8; 32],
            loop_id,
        },
        0..2,
        format!("r1:{}", "0".repeat(64)),
        2,
        crate::read::ReadTurnSummary {
            loop_id,
            outcome: LoopOutcomeView::Completed,
            usage: minicore_runtime::model::Usage::default(),
            requests: 1,
            tool_rounds: 0,
            final_config_revision: minicore_runtime::execution::ConfigRevision::INITIAL,
            completed_at: "2026-10-31T00:00:00Z".into(),
            interrupted: None,
        },
    )
}

#[tokio::test]
async fn compact_identity_is_exactly_load_and_immutable_stored_source_bound() {
    let catalog = ResultCatalog::new();
    let load = crate::test_support::load_id();
    catalog.register(load);
    let entry = identity(load, 0);
    let key = entry.key;
    let retained = catalog.retain_stored_identity(entry);
    assert_eq!(
        catalog.stored_identity(key).unwrap().result_id,
        retained.result_id
    );
    assert!(
        catalog
            .stored_identity(StoredIdentityKey {
                source_revision: [1; 32],
                ..key
            })
            .is_none()
    );
    assert!(
        catalog
            .stored_identity(StoredIdentityKey {
                load: crate::test_support::load_id(),
                ..key
            })
            .is_none()
    );
    catalog.remove(load);
    assert!(catalog.stored_identity(key).is_none());
    let late = catalog.retain_stored_identity(identity(load, 1));
    assert!(late._charge.is_none());
    assert!(catalog.stored_identity(late.key).is_none());
}

#[test]
fn eviction_and_close_keep_compact_metadata_charged_until_last_page_reference() {
    let catalog = ResultCatalog::new();
    let load = crate::test_support::load_id();
    catalog.register(load);
    let first = catalog.retain_stored_identity(identity(load, 0));
    let first_key = first.key;
    let second = catalog.retain_stored_identity(identity(load, 1));
    drop(second);
    let third = catalog.retain_stored_identity(identity(load, 2));
    assert!(catalog.stored_identity(first_key).is_none());
    let stored = catalog.stored.lock().unwrap();
    assert_eq!(stored.count.load(Ordering::SeqCst), 2);
    let bytes = Arc::clone(&stored.used);
    let count = Arc::clone(&stored.count);
    drop(stored);
    catalog.remove(load);
    drop(third);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(bytes.load(Ordering::SeqCst) > 0);
    drop(first);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(bytes.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.source_count.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.source_bytes.load(Ordering::SeqCst), 0);
}

#[test]
fn oversized_metadata_cannot_turn_compact_identity_storage_into_a_body_cache() {
    let catalog = ResultCatalog::new();
    let load = crate::test_support::load_id();
    catalog.register(load);
    let mut entry = identity(load, 0);
    let key = entry.key;
    entry.metadata.completed_at = String::with_capacity(MAX_STORED_IDENTITY_BYTES);
    entry.metadata.completed_at.push_str("2026-10-31T00:00:00Z");
    let returned = catalog.retain_stored_identity(entry);
    assert!(returned._charge.is_none());
    assert!(catalog.stored_identity(key).is_none());
    assert_eq!(
        catalog.stored.lock().unwrap().used.load(Ordering::SeqCst),
        0
    );
}

#[test]
fn interrupted_body_is_not_retained_in_the_compact_identity_index() {
    let catalog = ResultCatalog::new();
    let load = crate::test_support::load_id();
    catalog.register(load);
    let mut entry = identity(load, 0);
    let key = entry.key;
    entry.metadata.interrupted = Some(crate::read::InterruptedRequest {
        stop_reason: crate::read::PartialStopReason::Aborted,
        request_index: 0,
        model: "test".into(),
        reasoning: String::new(),
        text: "interrupted body".into(),
    });
    let returned = catalog.retain_stored_identity(entry);
    assert!(returned._charge.is_none());
    assert!(catalog.stored_identity(key).is_none());
}

#[test]
fn held_metadata_prevents_overbooking_the_byte_budget() {
    let catalog = ResultCatalog::new();
    let load = crate::test_support::load_id();
    catalog.register(load);
    let mut entry = identity(load, 0);
    entry.metadata.completed_at = String::with_capacity(MAX_STORED_IDENTITY_BYTES * 3 / 4);
    let first = catalog.retain_stored_identity(entry);
    let mut entry = identity(load, 1);
    entry.metadata.completed_at = String::with_capacity(MAX_STORED_IDENTITY_BYTES / 2);
    let key = entry.key;
    let second = catalog.retain_stored_identity(entry);
    assert!(catalog.stored_identity(first.key).is_none());
    assert!(catalog.stored_identity(key).is_none());
    assert!(second._charge.is_none());
    let stored = catalog.stored.lock().unwrap();
    assert_eq!(stored.count.load(Ordering::SeqCst), 1);
    assert!(stored.used.load(Ordering::SeqCst) <= MAX_STORED_IDENTITY_BYTES);
    drop(stored);
    drop(first);
    let retry = catalog.retain_stored_identity(identity(load, 2));
    assert!(retry._charge.is_some());
}
