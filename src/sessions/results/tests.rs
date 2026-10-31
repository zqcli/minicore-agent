use super::*;

fn limits() -> ResultLimits {
    ResultLimits {
        entries_per_load: 2,
        entries: 3,
        metadata_bytes: 8 * 1024,
        source_bytes_per_load: 4096,
        sources_per_load: 1,
        all_source_bytes: 4096,
        sources: 1,
    }
}

fn entry(loop_id: LoopId) -> CompletionEntry {
    CompletionEntry {
        seq: 0,
        loop_id,
        input_id: None,
        completion_kind: CompletionKind::Reported,
        outcome_kind: Some(CompletionOutcome::Completed),
        persistence: Some(TurnPersistence::Failed),
        persistence_uncertain: false,
        availability: ResultAvailability::Live,
        unavailable_reason: None,
        next_loop_id: None,
        after_saved_loop_id: None,
    }
}

fn source(text: &str) -> PreparedSource {
    PreparedSource::new(vec![text.to_owned()], None).unwrap()
}

#[test]
fn held_page_keeps_source_budget_after_eviction() {
    let catalog = ResultCatalog::new();
    *catalog.limits.lock().unwrap() = limits();
    let load = crate::test_support::load_id();
    catalog.register(load);
    let first = LoopId::new().unwrap();
    let first_source = source("first");
    let charge = catalog.reserve(load, first_source.bytes).unwrap();
    let reservation = catalog.finish_reservation(first_source, charge).unwrap();
    drop(catalog.insert(load, entry(first), Some(reservation)));
    let held = catalog.lookup(load, first).unwrap().1.unwrap();
    assert!(matches!(
        catalog.reserve(load, source("second").bytes),
        Err(UnavailableReason::ResourceLimit)
    ));
    let expired = catalog.lookup(load, first).unwrap().0;
    assert_eq!(expired.availability, ResultAvailability::Unavailable);
    assert_eq!(expired.unavailable_reason, Some(UnavailableReason::Expired));
    assert!(catalog.source_bytes.load(Ordering::SeqCst) > 0);
    drop(held);
    assert_eq!(catalog.source_bytes.load(Ordering::SeqCst), 0);
    let reservation = catalog.reserve(load, source("second").bytes).unwrap();
    drop(reservation);
    assert_eq!(catalog.source_bytes.load(Ordering::SeqCst), 0);
}

#[test]
fn global_metadata_eviction_keeps_each_load_as_a_contiguous_suffix() {
    let catalog = ResultCatalog::new();
    *catalog.limits.lock().unwrap() = limits();
    let a_load = crate::test_support::load_id();
    let b_load = crate::test_support::load_id();
    catalog.register(a_load);
    catalog.register(b_load);
    for load in [a_load, b_load, a_load, b_load, b_load] {
        let mut description = entry(LoopId::new().unwrap());
        description.availability = ResultAvailability::Unavailable;
        description.unavailable_reason = Some(UnavailableReason::TooLarge);
        drop(catalog.insert(load, description, None));
    }
    let a = catalog.window(a_load);
    let b = catalog.window(b_load);
    assert_eq!(a.through_seq, 2);
    assert_eq!(a.retained_from_seq, 2);
    assert_eq!(
        a.entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        [2]
    );
    assert_eq!(b.through_seq, 3);
    assert_eq!(b.retained_from_seq, 2);
    assert_eq!(
        b.entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        [2, 3]
    );
    assert!(a.truncated && b.truncated);
}

#[test]
fn metadata_eviction_and_unregister_do_not_release_a_held_pages_source_charge() {
    let catalog = ResultCatalog::new();
    let mut limits = limits();
    limits.entries = 1;
    catalog.set_limits_for_test(limits);
    let a = crate::test_support::load_id();
    let b = crate::test_support::load_id();
    catalog.register(a);
    catalog.register(b);
    let turn = LoopId::new().unwrap();
    let prepared = source("held source");
    let charge = catalog.reserve(a, prepared.bytes).unwrap();
    let reservation = catalog.finish_reservation(prepared, charge).unwrap();
    drop(catalog.insert(a, entry(turn), Some(reservation)));
    let page = catalog.lookup(a, turn).unwrap().1.unwrap();
    let mut next = entry(LoopId::new().unwrap());
    next.availability = ResultAvailability::Unavailable;
    next.unavailable_reason = Some(UnavailableReason::TooLarge);
    drop(catalog.insert(b, next, None));
    assert!(catalog.window(a).entries.is_empty());
    assert!(catalog.lookup(a, turn).is_none());
    catalog.remove(a);
    assert!(matches!(
        catalog.reserve(b, source("next").bytes),
        Err(UnavailableReason::ResourceLimit)
    ));
    assert!(catalog.source_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(catalog.source_count.load(Ordering::SeqCst), 1);
    drop(page);
    assert_eq!(catalog.source_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.source_count.load(Ordering::SeqCst), 0);
    assert!(catalog.reserve(b, source("next").bytes).is_ok());
}
