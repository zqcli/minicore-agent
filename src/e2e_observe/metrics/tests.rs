use super::*;
use crate::test_support::{Gate, TempDir};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Barrier};

fn finish(observer: &Observer, path: &Path) -> Vec<Value> {
    observer
        .sender
        .send(Message::End(json!({
            "ended_ns":observer.now(), "invalid":observer.invalid.load(Ordering::SeqCst),
            "trace_dropped":observer.dropped.load(Ordering::SeqCst), "counters":observer.counters()
        })))
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
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn gauge_history_is_valid(rows: &[Value]) -> bool {
    let mut states = BTreeMap::new();
    for gauge in Gauge::ALL {
        let mut updates = rows
            .iter()
            .filter(|row| row["event"] == "gauge" && row["name"] == gauge.name())
            .collect::<Vec<_>>();
        updates.sort_by_key(|row| row["revision"].as_u64());
        let mut revision = 0u64;
        let mut current = 0u64;
        let mut peak = 0u64;
        let mut versions = BTreeMap::from([(0, json!({"revision":0,"current":0,"peak":0}))]);
        for row in updates {
            let Some(amount) = row["amount"].as_u64().filter(|amount| *amount > 0) else {
                return false;
            };
            let next = match row["action"].as_str() {
                Some("add") => current.checked_add(amount),
                Some("sub") => current.checked_sub(amount),
                _ => return false,
            };
            let Some(next) = next else { return false };
            revision += 1;
            peak = peak.max(next);
            if row["revision"] != revision
                || row["previous_current"] != current
                || row["current"] != next
                || row["peak"] != peak
            {
                return false;
            }
            current = next;
            versions.insert(
                revision,
                json!({"revision":revision,"current":current,"peak":peak}),
            );
        }
        states.insert(gauge.name(), (revision, versions));
    }
    for row in rows {
        let counters = match row["event"].as_str() {
            Some("counters") => row,
            Some("observer_end") => &row["counters"],
            Some("gauge") if !states.contains_key(row["name"].as_str().unwrap()) => return false,
            _ => continue,
        };
        let Some(gauges) = counters["gauges"].as_object() else {
            return false;
        };
        if gauges.len() != states.len() {
            return false;
        }
        for (name, snapshot) in gauges {
            let Some(revision) = snapshot["revision"].as_u64() else {
                return false;
            };
            let Some((last, versions)) = states.get(name.as_str()) else {
                return false;
            };
            if versions.get(&revision) != Some(snapshot) {
                return false;
            }
            if row["event"] == "observer_end" && (revision != *last || snapshot["current"] != 0) {
                return false;
            }
        }
    }
    true
}

struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn reversed_gauge_enqueue_reconstructs_each_checkpoint_and_final_owner_state() {
    let temp = TempDir::new("causal-gauge-reverse");
    let path = temp.path().join("trace.jsonl");
    let observer = Arc::new(super::super::tests::local(File::create(&path).unwrap()));
    let gate = Arc::new(Gate::new());
    let _release = Release(gate.clone());
    observer.metrics.gauges[Gauge::Watchers as usize]
        .emit_gates
        .arm(1, gate.clone());
    let worker = {
        let observer = observer.clone();
        tokio::task::spawn_blocking(move || observer.change(Gauge::Watchers, 2, true))
    };
    tokio::time::timeout(Duration::from_secs(5), gate.wait_started())
        .await
        .unwrap();
    observer.checkpoint();
    observer.change(Gauge::Watchers, 2, false);
    observer.checkpoint();
    gate.release();
    worker.await.unwrap();
    let rows = finish(&observer, &path);
    let updates = rows
        .iter()
        .filter(|row| row["event"] == "gauge")
        .collect::<Vec<_>>();
    assert_eq!(
        updates
            .iter()
            .map(|row| row["revision"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(updates.last().unwrap()["current"], 2);
    assert_eq!(
        rows.last().unwrap()["counters"]["gauges"]["watchers"],
        json!({"revision":2,"current":0,"peak":2})
    );
    assert!(gauge_history_is_valid(&rows));
    for field in ["revision", "previous_current", "amount", "current", "peak"] {
        let mut wrong = rows.clone();
        let row = wrong
            .iter_mut()
            .find(|row| row["event"] == "gauge" && row["revision"] == 1)
            .unwrap();
        row[field] = json!(row[field].as_u64().unwrap() + 1);
        assert!(!gauge_history_is_valid(&wrong), "{field}");
    }
    let mut wrong = rows.clone();
    wrong[0]["gauges"]["watchers"]["current"] = json!(0);
    assert!(
        !gauge_history_is_valid(&wrong),
        "a checkpoint cannot borrow another revision's zero"
    );
    let mut wrong = rows.clone();
    wrong.last_mut().unwrap()["counters"]["gauges"]["watchers"] =
        json!({"revision":0,"current":0,"peak":0});
    assert!(
        !gauge_history_is_valid(&wrong),
        "terminal checkpoint must use the last causal revision"
    );
}

#[test]
fn concurrent_gauge_updates_bind_all_checkpoint_versions_without_global_snapshot_claims() {
    let temp = TempDir::new("causal-gauge-concurrent");
    let path = temp.path().join("trace.jsonl");
    let observer = super::super::tests::local(File::create(&path).unwrap());
    let barrier = Barrier::new(4);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..8 {
                    observer.change(Gauge::Watchers, 1, true);
                    observer.change(Gauge::SourceBytes, 4096, true);
                    observer.checkpoint();
                    observer.change(Gauge::SourceBytes, 4096, false);
                    observer.change(Gauge::Watchers, 1, false);
                }
            });
        }
    });
    let rows = finish(&observer, &path);
    assert!(!observer.invalid.load(Ordering::SeqCst));
    assert_eq!(observer.dropped.load(Ordering::SeqCst), 0);
    assert!(gauge_history_is_valid(&rows));
    let end = &rows.last().unwrap()["counters"]["gauges"];
    assert_eq!(end["watchers"]["revision"], 64);
    assert_eq!(end["source_charged_bytes"]["revision"], 64);
}

#[test]
fn gauge_revision_overflow_preserves_the_last_valid_state_and_invalidates() {
    let temp = TempDir::new("causal-revision-limit");
    let path = temp.path().join("trace.jsonl");
    let observer = super::super::tests::local(File::create(&path).unwrap());
    *observer.metrics.gauges[Gauge::Watchers as usize]
        .state
        .lock()
        .unwrap() = MeterState {
        revision: u64::MAX,
        current: 1,
        peak: 1,
    };
    observer.change(Gauge::Watchers, 1, false);
    assert_eq!(
        observer.counters()["gauges"]["watchers"],
        json!({"revision":u64::MAX,"current":1,"peak":1})
    );
    assert!(observer.invalid.load(Ordering::SeqCst));
    let rows = finish(&observer, &path);
    assert!(!rows.iter().any(|row| row["event"] == "gauge"));
}

fn request(index: u32) -> crate::TurnResultRequest {
    crate::TurnResultRequest {
        turn: crate::TurnRef {
            session_id: SessionId::new().unwrap(),
            loop_id: LoopId::new().unwrap(),
        },
        load: Some(crate::test_support::load_id().to_string()),
        result_id: Some(format!("r1:{index:064x}")),
        cursor: Some(crate::ReadCursor {
            item: index as usize,
            offset: 7,
        }),
        limit: 100,
        max_bytes: None,
    }
}

fn page(
    request: &crate::TurnResultRequest,
    availability: crate::read::TurnResultAvailability,
) -> crate::TurnResultPage {
    crate::TurnResultPage {
        turn: request.turn,
        load: request.load.clone(),
        result_id: Some(format!("r1:{:064x}", 99)),
        persistence_uncertain: false,
        unavailable_reason: None,
        availability,
        outcome: None,
        persistence: None,
        usage: None,
        requests: None,
        tool_rounds: None,
        final_config_revision: None,
        completed_at: None,
        interrupted: None,
        items: vec![crate::read::ReadItemChunk {
            index: 2,
            offset: 0,
            total_bytes: 2,
            encoding: "utf8_json",
            data: "{}".into(),
            complete: true,
        }],
        next_cursor: Some(crate::ReadCursor { item: 3, offset: 0 }),
        total: 4,
    }
}

fn query_targets_match(rows: &[Value], requests: &[crate::TurnResultRequest]) -> bool {
    let mut queries = rows
        .iter()
        .filter(|row| row["event"] == "result_query")
        .collect::<Vec<_>>();
    queries.sort_by_key(|row| row["query_id"].as_u64());
    queries.len() == requests.len()
        && queries
            .iter()
            .zip(requests)
            .enumerate()
            .all(|(index, (row, request))| {
                row["query_id"] == index + 1
                    && row["session_id"] == json!(request.turn.session_id)
                    && row["loop_id"] == json!(request.turn.loop_id)
                    && row["load"] == json!(request.load)
                    && row["cursor"] == json!(request.cursor)
                    && row["requested_result_id"] == json!(request.result_id)
            })
}

#[test]
fn reversed_query_outcomes_keep_unique_admissions_exact_targets_and_actual_reply_fields() {
    use crate::read::TurnResultAvailability;
    let temp = TempDir::new("causal-query-targets");
    let path = temp.path().join("trace.jsonl");
    let observer = super::super::tests::local(File::create(&path).unwrap());
    let requests = (0..6).map(request).collect::<Vec<_>>();
    let mut queries = requests
        .iter()
        .map(|request| Query::observe(request, Some(&observer)))
        .collect::<Vec<_>>();
    let mut expired = page(&requests[3], TurnResultAvailability::Unavailable);
    expired.result_id = None;
    expired.next_cursor = None;
    expired.total = 0;
    expired.items.clear();
    expired.unavailable_reason = Some(crate::UnavailableReason::Expired);
    let mut pending = page(&requests[5], TurnResultAvailability::Pending);
    pending.result_id = None;
    pending.next_cursor = None;
    pending.total = 0;
    pending.items.clear();
    queries[5].finish(&Ok(pending));
    queries[4].finish(&Err(AgentError::QueryLimit));
    queries[3].finish(&Ok(expired));
    drop(queries.remove(2));
    queries[1].finish(&Err(AgentError::StaleResult));
    let returned = page(&requests[0], TurnResultAvailability::Stored);
    queries[0].finish(&Ok(returned.clone()));
    assert_eq!(
        observer.counters()["gauges"]["result_queries_inflight"]["current"],
        5
    );
    drop(queries);
    let rows = finish(&observer, &path);
    assert!(gauge_history_is_valid(&rows));
    assert!(query_targets_match(&rows, &requests));
    let outcomes = rows
        .iter()
        .filter(|row| row["event"] == "result_query")
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes
            .iter()
            .map(|row| row["query_id"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [6, 5, 4, 3, 2, 1]
    );
    let available = outcomes.last().unwrap();
    assert_eq!(available["result_id"], json!(returned.result_id));
    assert_ne!(available["requested_result_id"], available["result_id"]);
    assert_eq!(available["next_cursor"], json!(returned.next_cursor));
    assert_eq!(available["total"], 4);
    assert_eq!(available["availability"], "stored");
    assert_eq!(outcomes[2]["unavailable_reason"], "expired");
    assert_eq!(outcomes[3]["error"], "awaiter_dropped");
    assert_eq!(outcomes[4]["error"], "stale_result");
    assert_eq!(outcomes[1]["error"], "query_limit");
    let counters = &rows.last().unwrap()["counters"];
    for (name, value) in [
        ("result_queries", 6),
        ("result_pages", 3),
        ("result_errors", 3),
        ("result_available_pages", 1),
        ("result_pending_pages", 1),
        ("result_unavailable_pages", 1),
    ] {
        assert_eq!(counters[name], value);
    }
    for field in [
        "session_id",
        "load",
        "loop_id",
        "cursor",
        "requested_result_id",
    ] {
        let mut wrong = rows.clone();
        let row = wrong
            .iter_mut()
            .find(|row| row["event"] == "result_query" && row["query_id"] == 1)
            .unwrap();
        row[field] = outcomes[0][field].clone();
        assert!(!query_targets_match(&wrong, &requests), "{field}");
    }
}

#[test]
fn concurrent_query_ids_are_the_actual_admitted_count_not_outcome_order() {
    let temp = TempDir::new("causal-query-concurrent");
    let path = temp.path().join("trace.jsonl");
    let observer = super::super::tests::local(File::create(&path).unwrap());
    let barrier = Barrier::new(8);
    std::thread::scope(|scope| {
        for index in 0..8 {
            let observer = &observer;
            let barrier = &barrier;
            scope.spawn(move || {
                let mut query = Query::observe(&request(index), Some(observer));
                barrier.wait();
                query.finish(&Err(AgentError::QueryLimit));
            });
        }
    });
    let rows = finish(&observer, &path);
    assert!(gauge_history_is_valid(&rows));
    let mut ids = rows
        .iter()
        .filter(|row| row["event"] == "result_query")
        .map(|row| row["query_id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, (1..=8).collect::<Vec<_>>());
    assert_eq!(rows.last().unwrap()["counters"]["result_queries"], 8);
    assert_eq!(rows.last().unwrap()["counters"]["result_errors"], 8);
    assert!(!observer.invalid.load(Ordering::SeqCst));
}

#[test]
fn admitted_query_count_overflow_invalidates_without_duplicate_or_wrapped_ids() {
    let temp = TempDir::new("causal-query-limit");
    let path = temp.path().join("trace.jsonl");
    let observer = super::super::tests::local(File::create(&path).unwrap());
    observer.metrics.queries.store(u64::MAX, Ordering::SeqCst);
    let query = Query::observe(&request(0), Some(&observer));
    assert!(query.active.is_none());
    drop(query);
    assert!(observer.invalid.load(Ordering::SeqCst));
    assert_eq!(observer.counters()["result_queries"], u64::MAX);
    let rows = finish(&observer, &path);
    assert!(!rows.iter().any(|row| row["event"] == "result_query"));
}
