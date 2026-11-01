use super::*;
use std::sync::atomic::AtomicU64;

macro_rules! names {
    ($name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        #[derive(Clone, Copy)]
        pub(crate) enum $name { $($variant),+ }
        impl $name {
            pub(super) const ALL: &[Self] = &[$(Self::$variant),+];
            pub(super) fn name(self) -> &'static str { match self { $(Self::$variant => $label),+ } }
        }
    };
}

names!(Gauge {
    Watchers => "watchers",
    SourceBytes => "source_charged_bytes",
    Sources => "source_charged_count",
    CatalogSourceBytes => "catalog_source_bytes",
    CatalogSources => "catalog_source_count",
    MetadataBytes => "catalog_metadata_bytes",
    Entries => "catalog_entries",
    ControlBytes => "control_staging_charged_bytes",
    Controls => "control_reservations",
    PageRefs => "retained_page_refs",
    PageRefBytes => "retained_page_reference_weighted_bytes",
    Active => "active_owner_objects",
    Completions => "completion_values",
    LateWait => "late_wait_slots",
    Waiters => "registered_wait_receivers",
    DriverReports => "driver_report_owners",
    Queries => "result_queries_inflight",
    StoredIdentityBytes => "stored_identity_charged_bytes",
    StoredIdentities => "stored_identity_charged_count",
    PagePrefixBytes => "paging_prefix_workspace_bytes",
});

names!(Work {
    NormalizeDurable => "normalize_durable",
    NormalizePublic => "normalize_public",
    SourcePrepare => "source_prepare",
    SourceHash => "source_hash",
    ResultProjectionHash => "result_projection_hash",
    StoredDecode => "stored_record_decode",
    ResultRangeEncode => "result_range_encode",
    QueryEncode => "query_encode",
    PagePrefixScan => "page_prefix_scan"
});

impl Work {
    fn counts_bytes(self) -> bool {
        !matches!(self, Self::NormalizeDurable | Self::NormalizePublic)
    }
}

#[derive(Clone, Copy, Default, Serialize)]
struct MeterState {
    revision: u64,
    current: u64,
    peak: u64,
}

struct Meter {
    state: Mutex<MeterState>,
    #[cfg(test)]
    emit_gates: crate::test_support::Registry<u64, std::sync::Arc<crate::test_support::Gate>>,
}

impl Default for Meter {
    fn default() -> Self {
        Self {
            state: Mutex::new(MeterState::default()),
            #[cfg(test)]
            emit_gates: crate::test_support::Registry::new(),
        }
    }
}

#[derive(Serialize)]
struct GaugeChange {
    #[serde(flatten)]
    state: MeterState,
    previous_current: u64,
    action: &'static str,
    amount: u64,
}

impl Meter {
    fn change(&self, amount: u64, add: bool) -> Option<GaugeChange> {
        let mut state = self.state.lock().unwrap();
        let revision = state.revision.checked_add(1)?;
        let previous_current = state.current;
        let current = if add {
            previous_current.checked_add(amount)?
        } else {
            previous_current.checked_sub(amount)?
        };
        *state = MeterState {
            revision,
            current,
            peak: state.peak.max(current),
        };
        Some(GaugeChange {
            state: *state,
            previous_current,
            action: if add { "add" } else { "sub" },
            amount,
        })
    }

    fn snapshot(&self) -> MeterState {
        *self.state.lock().unwrap()
    }
}
#[derive(Default)]
struct Totals {
    calls: AtomicU64,
    items: AtomicU64,
    bytes: AtomicU64,
    duration_ns: AtomicU64,
    peak_ns: AtomicU64,
}

#[derive(Default)]
pub(super) struct Metrics {
    gauges: [Meter; Gauge::ALL.len()],
    work: [Totals; Work::ALL.len()],
    queries: AtomicU64,
    pages: AtomicU64,
    available_pages: AtomicU64,
    pending_pages: AtomicU64,
    unavailable_pages: AtomicU64,
    errors: AtomicU64,
}

fn increment(counter: &AtomicU64, amount: u64, observer: &Observer) -> u64 {
    let previous = counter
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            Some(value.saturating_add(amount))
        })
        .unwrap();
    if previous.checked_add(amount).is_none() {
        observer.invalid.store(true, Ordering::SeqCst);
    }
    previous.saturating_add(amount)
}

impl Observer {
    pub(super) fn change(&self, gauge: Gauge, amount: u64, add: bool) {
        if amount == 0 {
            return;
        }
        let meter = &self.metrics.gauges[gauge as usize];
        let Some(change) = meter.change(amount, add) else {
            self.invalid.store(true, Ordering::SeqCst);
            return;
        };
        #[cfg(test)]
        if let Some(gate) = meter.emit_gates.take(&change.state.revision) {
            gate.pass_blocking();
        }
        let mut row = serde_json::to_value(change).unwrap();
        row["name"] = json!(gauge.name());
        self.emit("gauge", row);
    }

    pub(super) fn counters(&self) -> Value {
        let gauges = Gauge::ALL
            .iter()
            .map(|gauge| {
                let meter = &self.metrics.gauges[*gauge as usize];
                (
                    gauge.name().to_owned(),
                    serde_json::to_value(meter.snapshot()).unwrap(),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let work = Work::ALL.iter().map(|work| {
            let meter = &self.metrics.work[*work as usize];
            let mut value=json!({"calls":meter.calls.load(Ordering::SeqCst),"items":meter.items.load(Ordering::SeqCst),
                "duration_ns":meter.duration_ns.load(Ordering::SeqCst),"peak_ns":meter.peak_ns.load(Ordering::SeqCst)});
            if work.counts_bytes() { value["bytes"]=json!(meter.bytes.load(Ordering::SeqCst)); }
            (work.name().to_owned(),value)
        }).collect::<serde_json::Map<_,_>>();
        json!({"gauges":gauges,"work":work,"result_queries":self.metrics.queries.load(Ordering::SeqCst),
            "result_pages":self.metrics.pages.load(Ordering::SeqCst),"result_errors":self.metrics.errors.load(Ordering::SeqCst),
            "result_available_pages":self.metrics.available_pages.load(Ordering::SeqCst),"result_pending_pages":self.metrics.pending_pages.load(Ordering::SeqCst),
            "result_unavailable_pages":self.metrics.unavailable_pages.load(Ordering::SeqCst),
            "invalid":self.invalid.load(Ordering::SeqCst),"trace_dropped":self.dropped.load(Ordering::SeqCst)})
    }
}

pub(crate) fn add(gauge: Gauge, amount: usize) {
    if let Some(observer) = get() {
        observer.change(gauge, amount as u64, true);
    }
}
pub(crate) fn sub(gauge: Gauge, amount: usize) {
    if let Some(observer) = get() {
        observer.change(gauge, amount as u64, false);
    }
}

pub(crate) struct Lease {
    gauge: Gauge,
    amount: usize,
    enabled: bool,
}
impl Lease {
    pub(crate) fn new(gauge: Gauge, amount: usize) -> Self {
        let enabled = get().is_some();
        if enabled {
            add(gauge, amount);
        }
        Self {
            gauge,
            amount,
            enabled,
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if self.enabled {
            sub(self.gauge, self.amount);
        }
    }
}

pub(crate) struct Span {
    active: Option<(&'static Observer, u64, Work)>,
    items: u64,
    bytes: u64,
}
impl Span {
    pub(crate) fn new(work: Work) -> Self {
        Self {
            active: get().map(|o| (o, o.now(), work)),
            items: 0,
            bytes: 0,
        }
    }
    pub(crate) fn add(&mut self, items: usize, bytes: usize) {
        if let Some((observer, _, _)) = self.active {
            if self.items.checked_add(items as u64).is_none()
                || self.bytes.checked_add(bytes as u64).is_none()
            {
                observer.invalid.store(true, Ordering::SeqCst);
            }
            self.items = self.items.saturating_add(items as u64);
            self.bytes = self.bytes.saturating_add(bytes as u64);
        }
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        if let Some((observer, start, work)) = self.active {
            let finished = observer.now();
            let duration = finished.saturating_sub(start);
            let totals = &observer.metrics.work[work as usize];
            increment(&totals.calls, 1, observer);
            increment(&totals.items, self.items, observer);
            increment(&totals.bytes, self.bytes, observer);
            increment(&totals.duration_ns, duration, observer);
            totals.peak_ns.fetch_max(duration, Ordering::SeqCst);
            let mut value = json!({"kind":work.name(),"started_ns":start,"finished_ns":finished,"duration_ns":duration,"items":self.items});
            if work.counts_bytes() {
                value["bytes"] = json!(self.bytes);
            }
            observer.emit("work", value);
        }
    }
}

pub(crate) struct Query<'a> {
    active: Option<QueryObservation<'a>>,
}

struct QueryObservation<'a> {
    observer: &'a Observer,
    query_id: u64,
    turn: crate::TurnRef,
    load: Option<String>,
    cursor: Option<crate::ReadCursor>,
    requested_result_id: Option<String>,
    start: u64,
    finished: bool,
}

impl Query<'static> {
    pub(crate) fn new(request: &crate::read::TurnResultRequest) -> Self {
        Self::observe(request, get())
    }
}

impl<'a> Query<'a> {
    fn observe(request: &crate::read::TurnResultRequest, observer: Option<&'a Observer>) -> Self {
        let active = observer.and_then(|observer| {
            let query_id = match observer.metrics.queries.try_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |count| count.checked_add(1),
            ) {
                Ok(previous) => previous + 1,
                Err(_) => {
                    observer.invalid.store(true, Ordering::SeqCst);
                    return None;
                }
            };
            observer.change(Gauge::Queries, 1, true);
            Some(QueryObservation {
                observer,
                query_id,
                turn: request.turn,
                load: request.load.clone(),
                cursor: request.cursor,
                requested_result_id: request.result_id.clone(),
                start: observer.now(),
                finished: false,
            })
        });
        Self { active }
    }

    pub(crate) fn finish(&mut self, result: &Result<crate::read::TurnResultPage, AgentError>) {
        if let Some(active) = &mut self.active {
            active.finished = true;
            active.outcome(result.as_ref().map_err(query_error));
        }
    }
}

impl Drop for QueryObservation<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.outcome(Err("awaiter_dropped"));
        }
        self.observer.change(Gauge::Queries, 1, false);
    }
}

impl QueryObservation<'_> {
    fn outcome(&self, result: Result<&crate::read::TurnResultPage, &'static str>) {
        let observer = self.observer;
        let (availability, error, chunks, bytes, total, next) = match result {
            Ok(page) => {
                increment(&observer.metrics.pages, 1, observer);
                use crate::read::TurnResultAvailability;
                let kind = match page.availability {
                    TurnResultAvailability::Live | TurnResultAvailability::Stored => {
                        &observer.metrics.available_pages
                    }
                    TurnResultAvailability::Pending => &observer.metrics.pending_pages,
                    TurnResultAvailability::Unavailable => &observer.metrics.unavailable_pages,
                };
                increment(kind, 1, observer);
                (
                    serde_json::to_value(page.availability).unwrap(),
                    Value::Null,
                    page.items.len(),
                    page.items
                        .iter()
                        .map(|chunk| chunk.data.len())
                        .sum::<usize>(),
                    page.total,
                    page.next_cursor.is_some(),
                )
            }
            Err(error) => {
                increment(&observer.metrics.errors, 1, observer);
                (Value::Null, json!(error), 0, 0, 0, false)
            }
        };
        observer.emit("result_query",json!({"query_id":self.query_id,"session_id":self.turn.session_id,"load":self.load,"cursor":self.cursor,
        "requested_result_id":self.requested_result_id,
        "result_id":result.as_ref().ok().and_then(|page|page.result_id.as_deref()),"next_cursor":result.as_ref().ok().and_then(|page|page.next_cursor),
        "unavailable_reason":result.as_ref().ok().and_then(|page|page.unavailable_reason),
        "loop_id":self.turn.loop_id,"duration_ns":observer.now().saturating_sub(self.start),"availability":availability,"error":error,
        "chunks":chunks,"chunk_bytes":bytes,"total":total,"has_next":next}));
        observer.checkpoint();
    }
}

fn query_error(error: &AgentError) -> &'static str {
    match error {
        AgentError::QueryLimit => "query_limit",
        AgentError::StaleLoad => "stale_load",
        AgentError::MissingLoad => "missing_load",
        AgentError::StaleResult => "stale_result",
        AgentError::InvalidArguments => "invalid_arguments",
        AgentError::Store => "store_error",
        AgentError::StoreUnavailable => "store_unavailable",
        AgentError::TurnNotFound => "turn_not_found",
        AgentError::Internal => "internal_error",
        AgentError::SessionNotLoaded => "session_not_loaded",
        AgentError::SessionNotFound => "session_not_found",
        AgentError::InvalidState => "invalid_state",
        AgentError::RpcSerialization => "rpc_serialization",
        AgentError::Io(_) => "io_error",
        _ => "other_agent_error",
    }
}

#[cfg(test)]
mod tests;
