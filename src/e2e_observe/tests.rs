use super::*;
use crate::test_support::TempDir;

pub(super) fn local(file: File) -> Observer {
    let (sender, receiver) = mpsc::sync_channel(QUEUED_ROWS);
    Observer {
        origin: Instant::now(),
        metrics: metrics::Metrics::default(),
        identities: Mutex::new(Identities::default()),
        faults: Mutex::new(faults::Faults::load(None, None).unwrap()),
        sender,
        writer: Mutex::new(Some(std::thread::spawn(move || {
            write_trace(file, receiver)
        }))),
        invalid: AtomicBool::new(false),
        dropped: AtomicU64::new(0),
    }
}

#[test]
fn source_charge_and_reference_weighted_gauges_are_distinct_and_have_real_peaks() {
    let temp = TempDir::new("e2e-meters");
    let path = temp.path().join("trace.jsonl");
    let observer = local(File::create(&path).unwrap());
    observer.change(Gauge::SourceBytes, 4096, true);
    observer.change(Gauge::Sources, 1, true);
    observer.change(Gauge::CatalogSourceBytes, 4096, true);
    observer.change(Gauge::PageRefs, 2, true);
    observer.change(Gauge::PageRefBytes, 8192, true);
    observer.change(Gauge::CatalogSourceBytes, 4096, false);
    let held = observer.counters();
    assert_eq!(held["gauges"]["source_charged_bytes"]["current"], 4096);
    assert_eq!(held["gauges"]["catalog_source_bytes"]["current"], 0);
    assert_eq!(
        held["gauges"]["retained_page_reference_weighted_bytes"]["current"],
        8192
    );
    observer.change(Gauge::PageRefs, 2, false);
    observer.change(Gauge::PageRefBytes, 8192, false);
    observer.change(Gauge::SourceBytes, 4096, false);
    observer.change(Gauge::Sources, 1, false);
    let end = observer.counters();
    assert_eq!(end["gauges"]["source_charged_bytes"]["current"], 0);
    assert_eq!(end["gauges"]["source_charged_bytes"]["peak"], 4096);
    observer
        .sender
        .send(Message::End(
            json!({"ended_ns":observer.now(),"counters":end,"invalid":false}),
        ))
        .unwrap();
    assert!(
        observer
            .writer
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap()
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["event_seq"], index + 1);
        assert_eq!(row["schema"], "agent-e2e-1");
    }
    assert_eq!(rows.last().unwrap()["event"], "observer_end");
}

#[test]
fn malformed_or_unbounded_fault_plans_are_rejected_before_a_writer_starts() {
    let temp = TempDir::new("e2e-plans");
    let path = temp.path().join("plan.json");
    for value in [
        json!({"schema":2}),
        json!({"schema":1,"timeout_ms":0}),
        json!({"schema":1,"limits":{"sources":0}}),
        json!({"schema":1,"limits":{"sources":129}}),
        json!({"schema":1,"unknown":"field"}),
        json!({"schema":1,"steps":[{"id":"../escape","session":1,"turn":1,"at":"append","action":"rollback"}]}),
        json!({"schema":1,"steps":[{"id":"bad-action","session":1,"turn":1,"at":"append","action":"pause"}]}),
        json!({"schema":1,"steps":[{"id":"no-release-dir","session":1,"turn":1,"at":"result_page","action":"pause"}]}),
    ] {
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(faults::Faults::load(Some(path.clone()), None).is_err());
    }
    std::fs::write(&path, vec![b' '; 65537]).unwrap();
    assert!(faults::Faults::load(Some(path), None).is_err());
}

#[test]
fn oversized_observation_rows_fail_closed_without_writing_truncated_json() {
    let temp = TempDir::new("e2e-row-bound");
    let path = temp.path().join("trace.jsonl");
    let observer = local(File::create(&path).unwrap());
    observer.emit("test", json!({"bounded_test": "x".repeat(MAX_ROW)}));
    assert!(
        !observer
            .writer
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap()
    );
    assert!(std::fs::read(path).unwrap().is_empty());
}

#[test]
fn counter_overflow_and_underflow_mark_observation_invalid_without_wrapping() {
    let temp = TempDir::new("e2e-counter-bound");
    let path = temp.path().join("trace.jsonl");
    let observer = local(File::create(&path).unwrap());
    observer.change(Gauge::SourceBytes, u64::MAX, true);
    observer.change(Gauge::SourceBytes, 1, true);
    assert_eq!(
        observer.counters()["gauges"]["source_charged_bytes"]["current"],
        u64::MAX
    );
    assert!(observer.invalid.load(Ordering::SeqCst));
    observer.change(Gauge::SourceBytes, u64::MAX, false);
    observer.change(Gauge::SourceBytes, 1, false);
    assert_eq!(
        observer.counters()["gauges"]["source_charged_bytes"]["current"],
        0
    );
    assert_eq!(
        observer.counters()["gauges"]["source_charged_bytes"],
        json!({"revision":2,"current":0,"peak":u64::MAX})
    );
    observer
        .sender
        .send(Message::End(
            json!({"ended_ns":observer.now(),"invalid":true,"counters":observer.counters()}),
        ))
        .unwrap();
    assert!(
        observer
            .writer
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap()
    );
}
