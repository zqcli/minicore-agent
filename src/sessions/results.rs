use super::*;
use crate::read::InterruptedRequest;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
mod stored;
pub(crate) use stored::{StoredIdentity, StoredIdentityKey};

const MAX_ENTRIES_PER_LOAD: usize = 256;
const MAX_ENTRIES: usize = 4_096;
const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;
const MAX_DESCRIPTOR_BYTES: usize = 2 * 1024;
const _: () = assert!(std::mem::size_of::<CompletionEntry>() + 36 * 6 + 512 < MAX_DESCRIPTOR_BYTES);
const MAX_SOURCE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SOURCES_PER_LOAD: usize = 32;
const MAX_ALL_SOURCE_BYTES: usize = 256 * 1024 * 1024;
const MAX_SOURCES: usize = 128;

#[derive(Clone, Copy)]
#[cfg_attr(feature = "e2e-observe", derive(Serialize))]
pub(crate) struct ResultLimits {
    pub(crate) entries_per_load: usize,
    pub(crate) entries: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) source_bytes_per_load: usize,
    pub(crate) sources_per_load: usize,
    pub(crate) all_source_bytes: usize,
    pub(crate) sources: usize,
}

impl Default for ResultLimits {
    fn default() -> Self {
        Self {
            entries_per_load: MAX_ENTRIES_PER_LOAD,
            entries: MAX_ENTRIES,
            metadata_bytes: MAX_METADATA_BYTES,
            source_bytes_per_load: MAX_SOURCE_BYTES,
            sources_per_load: MAX_SOURCES_PER_LOAD,
            all_source_bytes: MAX_ALL_SOURCE_BYTES,
            sources: MAX_SOURCES,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionKind {
    Reported,
    InternalError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionOutcome {
    Completed,
    Cancelled,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultAvailability {
    Live,
    Stored,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    Expired,
    TooLarge,
    ResourceLimit,
    InternalError,
    NotRetained,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompletionEntry {
    pub seq: u64,
    pub loop_id: LoopId,
    pub input_id: Option<String>,
    pub completion_kind: CompletionKind,
    pub outcome_kind: Option<CompletionOutcome>,
    pub persistence: Option<TurnPersistence>,
    pub persistence_uncertain: bool,
    pub availability: ResultAvailability,
    pub unavailable_reason: Option<UnavailableReason>,
    pub next_loop_id: Option<LoopId>,
    pub after_saved_loop_id: Option<LoopId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompletionWindow {
    pub through_seq: u64,
    pub retained_from_seq: u64,
    pub entries: Vec<CompletionEntry>,
    pub truncated: bool,
}

pub(crate) struct ResultSource {
    pub(crate) items: Vec<String>,
    pub(crate) interrupted: Option<InterruptedRequest>,
    pub(crate) result_id: String,
    pub(crate) metadata: Option<crate::event::TurnResultView>,
    _charge: SourceCharge,
}

pub(crate) struct SourceCharge {
    bytes: usize,
    global: Arc<AtomicUsize>,
    per_load: Arc<AtomicUsize>,
    global_count: Arc<AtomicUsize>,
    per_load_count: Arc<AtomicUsize>,
}

impl Drop for SourceCharge {
    fn drop(&mut self) {
        #[cfg(feature = "e2e-observe")]
        {
            crate::e2e_observe::sub(crate::e2e_observe::Gauge::SourceBytes, self.bytes);
            crate::e2e_observe::sub(crate::e2e_observe::Gauge::Sources, 1);
        }
        self.global.fetch_sub(self.bytes, Ordering::SeqCst);
        self.per_load.fetch_sub(self.bytes, Ordering::SeqCst);
        self.global_count.fetch_sub(1, Ordering::SeqCst);
        self.per_load_count.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Retained {
    entry: CompletionEntry,
    source: Option<Arc<ResultSource>>,
    order: u64,
    metadata_bytes: usize,
}

#[derive(Default)]
struct LoadResults {
    through: u64,
    entries: VecDeque<Retained>,
    source_bytes: Arc<AtomicUsize>,
    source_count: Arc<AtomicUsize>,
}

#[derive(Default)]
struct CatalogState {
    loads: HashMap<LoadId, LoadResults>,
    order: u64,
    metadata_bytes: usize,
    entries: usize,
}

pub(crate) struct ResultCatalog {
    stored: Mutex<stored::StoredIdentities>,
    stored_preparation: tokio::sync::Mutex<()>,
    state: Mutex<CatalogState>,
    source_bytes: Arc<AtomicUsize>,
    source_count: Arc<AtomicUsize>,
    limits: Mutex<ResultLimits>,
    publication: Mutex<()>,
    hub: Mutex<std::sync::Weak<SnapshotHub>>,
}

pub(crate) struct PreparedSource {
    pub(crate) items: Vec<String>,
    pub(crate) interrupted: Option<InterruptedRequest>,
    pub(crate) result_id: String,
    pub(crate) metadata: Option<crate::event::TurnResultView>,
    bytes: usize,
}

pub(crate) struct ReservedSource {
    source: PreparedSource,
    charge: SourceCharge,
}

impl PreparedSource {
    pub(crate) fn new(
        items: Vec<String>,
        interrupted: Option<InterruptedRequest>,
    ) -> Result<Self, AgentError> {
        #[cfg(feature = "e2e-observe")]
        let mut observation = crate::e2e_observe::Span::new(crate::e2e_observe::Work::SourceHash);
        let mut hash = Sha256::new();
        hash.update(b"minicore-turn-result-r1\0");
        hash.update((items.len() as u64).to_be_bytes());
        let mut bytes = std::mem::size_of::<ResultSource>()
            + 2 * std::mem::size_of::<usize>()
            + items.capacity() * std::mem::size_of::<String>();
        for item in &items {
            #[cfg(feature = "e2e-observe")]
            observation.add(1, item.len() + 8);
            hash.update((item.len() as u64).to_be_bytes());
            hash.update(item.as_bytes());
            bytes = bytes
                .checked_add(item.capacity())
                .ok_or(AgentError::Internal)?;
        }
        let partial_len =
            crate::budget::json_len(&interrupted).map_err(|_| AgentError::RpcSerialization)?;
        #[cfg(feature = "e2e-observe")]
        observation.add(0, b"minicore-turn-result-r1\0".len() + 8 + partial_len + 8);
        hash.update((partial_len as u64).to_be_bytes());
        struct HashWriter<'a>(&'a mut Sha256);
        impl std::io::Write for HashWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        serde_json::to_writer(HashWriter(&mut hash), &interrupted)
            .map_err(|_| AgentError::RpcSerialization)?;
        if let Some(partial) = &interrupted {
            bytes = bytes
                .checked_add(partial.model.capacity())
                .and_then(|bytes| bytes.checked_add(partial.reasoning.capacity()))
                .and_then(|bytes| bytes.checked_add(partial.text.capacity()))
                .ok_or(AgentError::Internal)?;
        }
        use std::fmt::Write as _;
        let mut result_id = String::with_capacity(67);
        write!(result_id, "r1:{:x}", hash.finalize()).expect("writing a result ID");
        bytes = bytes
            .checked_add(result_id.capacity())
            .ok_or(AgentError::Internal)?;
        Ok(Self {
            items,
            interrupted,
            result_id,
            metadata: None,
            bytes,
        })
    }

    pub(crate) fn with_metadata(
        mut self,
        metadata: crate::event::TurnResultView,
    ) -> Result<Self, AgentError> {
        self.bytes = self
            .bytes
            .checked_add(metadata_heap_bytes(&metadata))
            .ok_or(AgentError::Internal)?;
        self.metadata = Some(metadata);
        Ok(self)
    }
}

pub(crate) fn metadata_heap_bytes(metadata: &crate::event::TurnResultView) -> usize {
    outcome_heap_bytes(&metadata.outcome)
}

pub(crate) fn outcome_heap_bytes(outcome: &LoopOutcomeView) -> usize {
    let LoopOutcomeView::Failed { kind, model_error } = outcome else {
        return 0;
    };
    let mut bytes = kind.capacity();
    if let Some(error) = model_error {
        bytes += error.kind.capacity() + error.delivery.capacity();
        if let Some(provider) = &error.provider {
            bytes += [&provider.code, &provider.error_type, &provider.message]
                .into_iter()
                .map(|text| text.as_ref().map_or(0, String::capacity))
                .sum::<usize>();
        }
    }
    bytes
}

impl ResultCatalog {
    pub(crate) fn new() -> Arc<Self> {
        let limits = ResultLimits::default();
        #[cfg(feature = "e2e-observe")]
        let limits = crate::e2e_observe::result_limits(limits);
        Arc::new(Self {
            stored: Mutex::new(stored::StoredIdentities::default()),
            stored_preparation: tokio::sync::Mutex::new(()),
            state: Mutex::new(CatalogState::default()),
            source_bytes: Arc::new(AtomicUsize::new(0)),
            source_count: Arc::new(AtomicUsize::new(0)),
            limits: Mutex::new(limits),
            publication: Mutex::new(()),
            hub: Mutex::new(std::sync::Weak::new()),
        })
    }

    pub(crate) fn bind_hub(&self, hub: &Arc<SnapshotHub>) {
        *self.hub.lock().unwrap() = Arc::downgrade(hub);
    }

    fn mark(&self, load: LoadId) {
        if let Some(hub) = self.hub.lock().unwrap().upgrade() {
            hub.mark(load);
        }
    }

    pub(crate) fn publication(&self) -> std::sync::MutexGuard<'_, ()> {
        self.publication.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn set_limits_for_test(&self, limits: ResultLimits) {
        *self.limits.lock().unwrap() = limits;
    }

    pub(crate) fn register(&self, load: LoadId) {
        self.state
            .lock()
            .unwrap()
            .loads
            .insert(load, LoadResults::default());
    }

    pub(crate) fn accepts_source(&self, bytes: usize) -> bool {
        bytes <= self.limits.lock().unwrap().source_bytes_per_load
    }

    pub(crate) fn remove(&self, load: LoadId) {
        let released = {
            let mut state = self.state.lock().unwrap();
            let mut removed = state.loads.remove(&load);
            if let Some(results) = &removed {
                #[cfg(feature = "e2e-observe")]
                {
                    crate::e2e_observe::sub(
                        crate::e2e_observe::Gauge::Entries,
                        results.entries.len(),
                    );
                    crate::e2e_observe::sub(
                        crate::e2e_observe::Gauge::MetadataBytes,
                        results
                            .entries
                            .iter()
                            .map(|entry| entry.metadata_bytes)
                            .sum(),
                    );
                }
                state.entries -= results.entries.len();
                state.metadata_bytes -= results
                    .entries
                    .iter()
                    .map(|entry| entry.metadata_bytes)
                    .sum::<usize>();
            }
            let released = removed.as_mut().map(|results| {
                results
                    .entries
                    .iter_mut()
                    .filter_map(|entry| {
                        let source = entry.source.take()?;
                        #[cfg(feature = "e2e-observe")]
                        observe_catalog_source(&source, false);
                        Some(source)
                    })
                    .collect::<Vec<_>>()
            });
            drop(removed);
            released
        };
        self.remove_stored_identities(load);
        drop(released);
    }

    pub(crate) fn reserve(
        &self,
        load: LoadId,
        bytes: usize,
    ) -> Result<SourceCharge, UnavailableReason> {
        let limits = *self.limits.lock().unwrap();
        if bytes > limits.source_bytes_per_load {
            return Err(UnavailableReason::TooLarge);
        }
        loop {
            let mut state = self.state.lock().unwrap();
            let results = state.loads.get(&load).expect("registered load");
            let load_bytes = Arc::clone(&results.source_bytes);
            let load_count = Arc::clone(&results.source_count);
            let local_count = load_count.load(Ordering::SeqCst);
            let local_full = local_count >= limits.sources_per_load
                || load_bytes.load(Ordering::SeqCst).saturating_add(bytes)
                    > limits.source_bytes_per_load;
            let global_full = self.source_count.load(Ordering::SeqCst) >= limits.sources
                || self
                    .source_bytes
                    .load(Ordering::SeqCst)
                    .saturating_add(bytes)
                    > limits.all_source_bytes;
            if !local_full && !global_full {
                self.source_bytes.fetch_add(bytes, Ordering::SeqCst);
                load_bytes.fetch_add(bytes, Ordering::SeqCst);
                self.source_count.fetch_add(1, Ordering::SeqCst);
                load_count.fetch_add(1, Ordering::SeqCst);
                #[cfg(feature = "e2e-observe")]
                {
                    crate::e2e_observe::add(crate::e2e_observe::Gauge::SourceBytes, bytes);
                    crate::e2e_observe::add(crate::e2e_observe::Gauge::Sources, 1);
                }
                return Ok(SourceCharge {
                    bytes,
                    global: Arc::clone(&self.source_bytes),
                    per_load: load_bytes,
                    global_count: Arc::clone(&self.source_count),
                    per_load_count: load_count,
                });
            }
            let candidate = state
                .loads
                .iter()
                .flat_map(|(key, results)| {
                    results
                        .entries
                        .iter()
                        .filter(|entry| entry.source.is_some())
                        .map(move |entry| (*key, entry.order))
                })
                .filter(|(key, _)| !local_full || *key == load)
                .min_by_key(|(_, order)| *order);
            let Some((key, oldest)) = candidate else {
                return Err(UnavailableReason::ResourceLimit);
            };
            let retained = state
                .loads
                .get_mut(&key)
                .unwrap()
                .entries
                .iter_mut()
                .find(|entry| entry.order == oldest)
                .unwrap();
            let removed = retained.source.take().unwrap();
            #[cfg(feature = "e2e-observe")]
            observe_catalog_source(&removed, false);
            retained.entry.availability = ResultAvailability::Unavailable;
            retained.entry.unavailable_reason = Some(UnavailableReason::Expired);
            drop(state);
            self.mark(key);
            drop(removed);
        }
    }

    pub(crate) fn finish_reservation(
        &self,
        source: PreparedSource,
        mut charge: SourceCharge,
    ) -> Result<ReservedSource, UnavailableReason> {
        if source.bytes > charge.bytes {
            return Err(UnavailableReason::TooLarge);
        }
        let excess = charge.bytes - source.bytes;
        charge.global.fetch_sub(excess, Ordering::SeqCst);
        charge.per_load.fetch_sub(excess, Ordering::SeqCst);
        #[cfg(feature = "e2e-observe")]
        crate::e2e_observe::sub(crate::e2e_observe::Gauge::SourceBytes, excess);
        charge.bytes = source.bytes;
        Ok(ReservedSource { source, charge })
    }

    pub(super) fn insert(
        &self,
        load: LoadId,
        mut entry: CompletionEntry,
        source: Option<ReservedSource>,
    ) -> Vec<Arc<ResultSource>> {
        let limits = *self.limits.lock().unwrap();
        let mut released = Vec::new();
        let mut affected = BTreeSet::from([load]);
        let mut state = self.state.lock().unwrap();
        state.order = state
            .order
            .checked_add(1)
            .expect("completion order overflow");
        let order = state.order;
        let results = state.loads.get_mut(&load).expect("registered load");
        results.through = results
            .through
            .checked_add(1)
            .expect("completion sequence overflow");
        entry.seq = results.through;
        #[cfg(feature = "e2e-observe")]
        crate::e2e_observe::completion(load, &entry);
        let metadata_bytes = 4 * std::mem::size_of::<Retained>()
            + entry.input_id.as_ref().map_or(0, String::capacity);
        let stored_source = source.map(|reserved| {
            let source = Arc::new(ResultSource {
                items: reserved.source.items,
                interrupted: reserved.source.interrupted,
                result_id: reserved.source.result_id,
                metadata: reserved.source.metadata,
                _charge: reserved.charge,
            });
            #[cfg(feature = "e2e-observe")]
            observe_catalog_source(&source, true);
            source
        });
        state
            .loads
            .get_mut(&load)
            .unwrap()
            .entries
            .push_back(Retained {
                entry,
                source: stored_source,
                order,
                metadata_bytes,
            });
        state.entries += 1;
        state.metadata_bytes += metadata_bytes;
        #[cfg(feature = "e2e-observe")]
        {
            crate::e2e_observe::add(crate::e2e_observe::Gauge::Entries, 1);
            crate::e2e_observe::add(crate::e2e_observe::Gauge::MetadataBytes, metadata_bytes);
        }
        loop {
            let local_full = state.loads[&load].entries.len() > limits.entries_per_load;
            if !local_full
                && state.entries <= limits.entries
                && state.metadata_bytes <= limits.metadata_bytes
            {
                break;
            }
            let candidate = if local_full {
                load
            } else {
                *state
                    .loads
                    .iter()
                    .filter_map(|(key, results)| {
                        results.entries.front().map(|entry| (key, entry.order))
                    })
                    .min_by_key(|(_, order)| *order)
                    .unwrap()
                    .0
            };
            let mut oldest = state
                .loads
                .get_mut(&candidate)
                .unwrap()
                .entries
                .pop_front()
                .unwrap();
            affected.insert(candidate);
            if state.loads[&candidate].entries.capacity()
                > 4 * state.loads[&candidate].entries.len()
            {
                state
                    .loads
                    .get_mut(&candidate)
                    .unwrap()
                    .entries
                    .shrink_to_fit();
            }
            state.entries -= 1;
            state.metadata_bytes -= oldest.metadata_bytes;
            #[cfg(feature = "e2e-observe")]
            {
                crate::e2e_observe::sub(crate::e2e_observe::Gauge::Entries, 1);
                crate::e2e_observe::sub(
                    crate::e2e_observe::Gauge::MetadataBytes,
                    oldest.metadata_bytes,
                );
            }
            if let Some(source) = oldest.source.take() {
                #[cfg(feature = "e2e-observe")]
                observe_catalog_source(&source, false);
                released.push(source);
            }
            // Descriptor identities are small; only large sources retire beyond business locks.
            drop(oldest);
        }
        drop(state);
        for load in affected {
            self.mark(load);
        }
        released
    }

    pub(crate) fn window(&self, load: LoadId) -> CompletionWindow {
        let state = self.state.lock().unwrap();
        let Some(results) = state.loads.get(&load) else {
            return CompletionWindow {
                through_seq: 0,
                retained_from_seq: 1,
                entries: Vec::new(),
                truncated: false,
            };
        };
        let retained_from_seq = results.entries.front().map_or_else(
            || {
                results
                    .through
                    .checked_add(1)
                    .expect("completion sequence overflow")
            },
            |entry| entry.entry.seq,
        );
        CompletionWindow {
            through_seq: results.through,
            retained_from_seq,
            entries: results
                .entries
                .iter()
                .map(|entry| entry.entry.clone())
                .collect(),
            truncated: retained_from_seq > 1,
        }
    }

    pub(crate) fn lookup(
        &self,
        load: LoadId,
        loop_id: LoopId,
    ) -> Option<(CompletionEntry, Option<Arc<ResultSource>>)> {
        let state = self.state.lock().unwrap();
        state
            .loads
            .get(&load)?
            .entries
            .iter()
            .find(|entry| entry.entry.loop_id == loop_id)
            .map(|entry| (entry.entry.clone(), entry.source.clone()))
    }
}

#[cfg(feature = "e2e-observe")]
fn observe_catalog_source(source: &ResultSource, add: bool) {
    let change = if add {
        crate::e2e_observe::add
    } else {
        crate::e2e_observe::sub
    };
    change(
        crate::e2e_observe::Gauge::CatalogSourceBytes,
        source._charge.bytes,
    );
    change(crate::e2e_observe::Gauge::CatalogSources, 1);
}

#[cfg(feature = "e2e-observe")]
impl ResultSource {
    pub(crate) fn observed_bytes(&self) -> usize {
        self._charge.bytes
    }
}

#[cfg(test)]
mod tests;
