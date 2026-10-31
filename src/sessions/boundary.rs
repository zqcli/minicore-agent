//! The request boundary a delivered input belongs to.
//!
//! Runtime hands inputs over at two points of one request preparation, and the
//! index they belong to is the preparation they join. The Runtime documents
//! that its `take` carries no identity, so the host derives it from the config
//! it serves: every prompt and model wrapper observes the index of the
//! preparation the loop is running, and `take` uses the observation instead of
//! counting calls or reading text.

use super::*;

/// Which preparation the loop is at, per installed loop generation.
pub(crate) struct RequestBoundary {
    state: Mutex<BoundaryState>,
}

#[derive(Default)]
struct BoundaryState {
    generation: Option<u64>,
    next: u32,
}

impl RequestBoundary {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BoundaryState::default()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BoundaryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Starts a new loop generation at its first request.
    pub(super) fn install(&self, generation: u64) {
        let mut state = self.lock();
        state.generation = Some(generation);
        state.next = 0;
    }

    /// Records the preparation the loop is now serving.
    pub(super) fn observed(&self, generation: u64, request_index: u32) {
        let mut state = self.lock();
        if state.generation != Some(generation) {
            return;
        }
        state.next = request_index;
    }

    fn issued(&self, generation: u64, request_index: u32) {
        let mut state = self.lock();
        if state.generation == Some(generation) {
            state.next = request_index
                .checked_add(1)
                .expect("request index overflow");
        }
    }

    /// Returns the preparation joined, including the take before its prompt.
    pub(super) fn take(&self, generation: u64) -> u32 {
        let state = self.lock();
        assert_eq!(state.generation, Some(generation));
        state.next
    }
}

/// Observes the request index of every prompt preparation the loop serves.
///
/// It wraps the config's prompt provider, so an update that replaces the config
/// installs the observation again for the new provider.
pub(crate) struct BoundaryPrompt {
    inner: Arc<dyn minicore_runtime::prompt::PromptProvider>,
    boundary: Arc<RequestBoundary>,
    generation: u64,
}

impl BoundaryPrompt {
    pub(super) fn new(
        inner: Arc<dyn minicore_runtime::prompt::PromptProvider>,
        boundary: Arc<RequestBoundary>,
        generation: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            boundary,
            generation,
        })
    }
}

/// Observes preparation and issue boundaries on initial and replacement configs.
pub(super) fn observe_config(
    config: &ExecutionConfig,
    boundary: &Arc<RequestBoundary>,
    generation: u64,
) -> Result<ExecutionConfig, AgentError> {
    ExecutionConfig::new(
        Arc::new(BoundaryModel {
            inner: Arc::clone(config.model()),
            boundary: Arc::clone(boundary),
            generation,
        }),
        config.reasoning(),
        config.tools().clone(),
        config.policy().cloned(),
        BoundaryPrompt::new(
            Arc::clone(config.prompt()),
            Arc::clone(boundary),
            generation,
        ),
    )
    .map_err(|_| AgentError::InvalidSessionSettings)
}

impl minicore_runtime::prompt::PromptProvider for BoundaryPrompt {
    fn prepare<'a>(
        &'a self,
        request: minicore_runtime::prompt::PromptRequest<'a>,
    ) -> minicore_runtime::prompt::PromptFuture<'a> {
        self.boundary
            .observed(self.generation, request.request_index);
        self.inner.prepare(request)
    }
}

struct BoundaryModel {
    inner: Arc<dyn Model>,
    boundary: Arc<RequestBoundary>,
    generation: u64,
}

impl Model for BoundaryModel {
    fn descriptor(&self) -> &ModelDescriptor {
        self.inner.descriptor()
    }

    fn start<'a>(
        &'a self,
        request: minicore_runtime::model::ModelRequest,
        context: minicore_runtime::model::ModelCallContext,
    ) -> minicore_runtime::model::ModelStartFuture<'a> {
        // After an issued request, Runtime's next take precedes the next prompt.
        self.boundary.issued(self.generation, context.request_index);
        self.inner.start(request, context)
    }
}
