use super::*;
use crate::sessions::results::ResultLimits;
use crate::test_gate::Gate;
use std::io::Read;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Point {
    Append,
    RuntimeEvents,
    BeforePersist,
    ResultPage,
}
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Rollback,
    Unknown,
    DropTerminal,
    Pause,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Limits {
    entries_per_load: Option<usize>,
    entries: Option<usize>,
    metadata_bytes: Option<usize>,
    source_bytes_per_load: Option<usize>,
    sources_per_load: Option<usize>,
    all_source_bytes: Option<usize>,
    sources: Option<usize>,
}
impl Limits {
    fn apply(&self, mut limits: ResultLimits) -> Result<ResultLimits, AgentError> {
        macro_rules! limit {
            ($field:ident) => {
                if let Some(value) = self.$field {
                    if value == 0 || value > limits.$field {
                        return Err(AgentError::InvalidArguments);
                    }
                    limits.$field = value;
                }
            };
        }
        limit!(entries_per_load);
        limit!(entries);
        limit!(metadata_bytes);
        limit!(source_bytes_per_load);
        limit!(sources_per_load);
        limit!(all_source_bytes);
        limit!(sources);
        Ok(limits)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Step {
    id: String,
    session: u64,
    turn: u64,
    at: Point,
    action: Action,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema: u8,
    #[serde(default)]
    input_fixture: bool,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default)]
    limits: Limits,
    #[serde(default)]
    steps: Vec<Step>,
}
fn default_timeout() -> u64 {
    10_000
}

struct Planned {
    step: Step,
    taken: bool,
    applied: bool,
}
pub(super) struct Faults {
    pub(super) input_fixture: bool,
    steps: Vec<Planned>,
    limits: ResultLimits,
    releases: Option<PathBuf>,
    timeout: Duration,
}

impl Faults {
    pub(super) fn load(
        path: Option<PathBuf>,
        releases: Option<PathBuf>,
    ) -> Result<Self, AgentError> {
        let plan = match path {
            Some(path) => {
                let mut bytes = Vec::new();
                File::open(path)
                    .map_err(|_| AgentError::InvalidArguments)?
                    .take(65537)
                    .read_to_end(&mut bytes)
                    .map_err(|_| AgentError::InvalidArguments)?;
                if bytes.len() > 65536 {
                    return Err(AgentError::InvalidArguments);
                }
                serde_json::from_slice::<Plan>(&bytes).map_err(|_| AgentError::InvalidArguments)?
            }
            None => Plan {
                schema: 1,
                input_fixture: false,
                timeout_ms: default_timeout(),
                limits: Limits::default(),
                steps: Vec::new(),
            },
        };
        if plan.schema != 1 || !(50..=30_000).contains(&plan.timeout_ms) || plan.steps.len() > 64 {
            return Err(AgentError::InvalidArguments);
        }
        let mut ids = std::collections::HashSet::new();
        for step in &plan.steps {
            if step.id.is_empty()
                || step.id.len() > 32
                || !step
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                || !ids.insert(step.id.as_str())
                || step.session == 0
                || step.turn == 0
                || !matches!(
                    (step.at, step.action),
                    (Point::Append, Action::Rollback | Action::Unknown)
                        | (Point::RuntimeEvents, Action::DropTerminal)
                        | (Point::BeforePersist | Point::ResultPage, Action::Pause)
                )
            {
                return Err(AgentError::InvalidArguments);
            }
        }
        if plan.steps.iter().any(|step| step.action == Action::Pause)
            && !releases.as_ref().is_some_and(|path| path.is_dir())
        {
            return Err(AgentError::InvalidArguments);
        }
        Ok(Self {
            input_fixture: plan.input_fixture,
            steps: plan
                .steps
                .into_iter()
                .map(|step| Planned {
                    step,
                    taken: false,
                    applied: false,
                })
                .collect(),
            limits: plan.limits.apply(ResultLimits::default())?,
            releases,
            timeout: Duration::from_millis(plan.timeout_ms),
        })
    }
    pub(super) fn unused(&self) -> usize {
        self.steps.iter().filter(|step| !step.applied).count()
    }
    pub(super) fn limits(&self, _default: ResultLimits) -> ResultLimits {
        self.limits
    }
}

pub(crate) struct Selected {
    observer: &'static Observer,
    index: usize,
    id: String,
    point: Point,
    pub(crate) action: Action,
    identity: Identity,
    releases: Option<PathBuf>,
    timeout: Duration,
}
impl Selected {
    fn emit(&self, event: &'static str, reason: &'static str) {
        let mut data = serde_json::to_value(self.identity).unwrap();
        data["id"] = json!(self.id);
        data["point"] = json!(self.point);
        data["action"] = json!(self.action);
        data["reason"] = json!(reason);
        self.observer.emit(event, data);
    }
    pub(crate) fn applied(&self, reason: &'static str) {
        self.observer.faults.lock().unwrap().steps[self.index].applied = true;
        self.emit("fault_applied", reason);
    }
    pub(crate) fn dropped_events(&self, count: usize) {
        let mut data = serde_json::to_value(self.identity).unwrap();
        data["id"] = json!(self.id);
        data["buffered_events_discarded"] = json!(count);
        self.observer.emit("runtime_tail_dropped", data);
    }
}

pub(crate) fn take_fault(point: Point, session: SessionId, loop_id: LoopId) -> Option<Selected> {
    let observer = get()?;
    let identity = observer.identity(session, loop_id)?;
    let mut faults = observer.faults.lock().unwrap();
    let index = faults.steps.iter().position(|entry| {
        !entry.taken
            && entry.step.at == point
            && entry.step.session == identity.session
            && entry.step.turn == identity.turn
    })?;
    faults.steps[index].taken = true;
    let step = &faults.steps[index].step;
    Some(Selected {
        observer,
        index,
        id: step.id.clone(),
        point,
        action: step.action,
        identity,
        releases: faults.releases.clone(),
        timeout: faults.timeout,
    })
}

pub(crate) async fn pause(
    point: Point,
    session: SessionId,
    loop_id: LoopId,
    closed: CancellationToken,
) {
    let Some(selected) = take_fault(point, session, loop_id) else {
        return;
    };
    let gate = Arc::new(Gate::new());
    let path = selected
        .releases
        .as_ref()
        .unwrap()
        .join(format!("{}.release", selected.id));
    selected.emit("fault_ready", "paused");
    selected.applied("paused");
    selected.observer.checkpoint();
    struct Release<'a> {
        selected: &'a Selected,
        closed: CancellationToken,
        done: bool,
    }
    impl Drop for Release<'_> {
        fn drop(&mut self) {
            if !self.done {
                self.selected.emit(
                    "fault_released",
                    if self.closed.is_cancelled() {
                        "close_or_cancel"
                    } else {
                        "awaiter_dropped"
                    },
                );
                self.selected.observer.checkpoint();
            }
        }
    }
    let mut release_guard = Release {
        selected: &selected,
        closed: closed.clone(),
        done: false,
    };
    let release = async {
        loop {
            match tokio::fs::metadata(&path).await {
                Ok(metadata) if metadata.is_file() => return "release",
                Ok(_) => return "invalid_release",
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return "release_io_error",
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let monitor = async {
        let reason = tokio::select! {
            reason=release=>reason,
            _=closed.cancelled()=>"close_or_cancel",
            _=tokio::time::sleep(selected.timeout)=>"timeout",
        };
        gate.release();
        reason
    };
    let (reason, ()) = tokio::join!(monitor, gate.pass());
    if !matches!(reason, "release" | "close_or_cancel") {
        selected.observer.invalid.store(true, Ordering::SeqCst);
    }
    release_guard.done = true;
    selected.emit("fault_released", reason);
    selected.observer.checkpoint();
}
