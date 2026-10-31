use super::*;
use std::ops::Range;

pub(crate) const MAX_STORED_IDENTITIES: usize = 2;
pub(crate) const MAX_STORED_IDENTITY_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct StoredIdentityKey {
    pub(crate) load: LoadId,
    // The indexed committed prefix stays immutable when later turns append.
    pub(crate) source_revision: [u8; 32],
    pub(crate) loop_id: LoopId,
}

pub(crate) struct StoredIdentity {
    pub(crate) key: StoredIdentityKey,
    pub(crate) range: Range<usize>,
    pub(crate) result_id: String,
    pub(crate) total: usize,
    pub(crate) metadata: crate::read::ReadTurnSummary,
    _charge: Option<IdentityCharge>,
}

struct IdentityCharge {
    bytes: usize,
    used: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
}
impl Drop for IdentityCharge {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::SeqCst);
        self.count.fetch_sub(1, Ordering::SeqCst);
        #[cfg(feature = "e2e-observe")]
        {
            crate::e2e_observe::sub(crate::e2e_observe::Gauge::StoredIdentityBytes, self.bytes);
            crate::e2e_observe::sub(crate::e2e_observe::Gauge::StoredIdentities, 1);
        }
    }
}

#[derive(Default)]
pub(super) struct StoredIdentities {
    entries: VecDeque<Arc<StoredIdentity>>,
    used: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
}

impl StoredIdentity {
    pub(crate) fn new(
        key: StoredIdentityKey,
        range: Range<usize>,
        result_id: String,
        total: usize,
        metadata: crate::read::ReadTurnSummary,
    ) -> Self {
        Self {
            key,
            range,
            result_id,
            total,
            metadata,
            _charge: None,
        }
    }
    fn bytes(&self) -> usize {
        let metadata = &self.metadata;
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + 2 * std::mem::size_of::<Arc<Self>>()
            + self.result_id.capacity()
            + metadata.completed_at.capacity()
            + outcome_heap_bytes(&metadata.outcome)
            + metadata.interrupted.as_ref().map_or(0, |partial| {
                partial.model.capacity() + partial.reasoning.capacity() + partial.text.capacity()
            })
    }
}

impl ResultCatalog {
    pub(crate) async fn stored_preparation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.stored_preparation.lock().await
    }
    pub(crate) fn stored_identity(&self, key: StoredIdentityKey) -> Option<Arc<StoredIdentity>> {
        self.stored
            .lock()
            .unwrap()
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .cloned()
    }
    pub(crate) fn retain_stored_identity(
        &self,
        mut identity: StoredIdentity,
    ) -> Arc<StoredIdentity> {
        let bytes = identity.bytes();
        // Interrupted text stays in its existing owner or on disk, never in this index.
        if identity.metadata.interrupted.is_some() || bytes > MAX_STORED_IDENTITY_BYTES {
            return Arc::new(identity);
        }
        loop {
            let registered = self.state.lock().unwrap();
            if !registered.loads.contains_key(&identity.key.load) {
                return Arc::new(identity);
            }
            let mut stored = self.stored.lock().unwrap();
            if stored.used.load(Ordering::SeqCst).saturating_add(bytes) <= MAX_STORED_IDENTITY_BYTES
                && stored.count.load(Ordering::SeqCst) < MAX_STORED_IDENTITIES
            {
                stored.used.fetch_add(bytes, Ordering::SeqCst);
                stored.count.fetch_add(1, Ordering::SeqCst);
                identity._charge = Some(IdentityCharge {
                    bytes,
                    used: Arc::clone(&stored.used),
                    count: Arc::clone(&stored.count),
                });
                #[cfg(feature = "e2e-observe")]
                {
                    crate::e2e_observe::add(crate::e2e_observe::Gauge::StoredIdentityBytes, bytes);
                    crate::e2e_observe::add(crate::e2e_observe::Gauge::StoredIdentities, 1);
                }
                let identity = Arc::new(identity);
                stored.entries.push_back(Arc::clone(&identity));
                return identity;
            }
            let retired = stored.entries.pop_front();
            drop(stored);
            drop(registered);
            if retired.is_none() {
                return Arc::new(identity);
            }
            drop(retired);
        }
    }
    pub(super) fn remove_stored_identities(&self, load: LoadId) {
        let retired = {
            let mut stored = self.stored.lock().unwrap();
            let mut retired = Vec::new();
            let mut index = 0;
            while index < stored.entries.len() {
                if stored.entries[index].key.load == load {
                    retired.push(stored.entries.remove(index).unwrap());
                } else {
                    index += 1;
                }
            }
            retired
        };
        drop(retired);
    }
}

#[cfg(test)]
mod tests;
