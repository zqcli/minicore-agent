//! Compile-time-only observation of the real stdio Agent in isolated tests.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::{AgentError, LoadId, SessionId};
use minicore_runtime::LoopId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

mod faults;
pub(crate) mod input_fixture;
mod metrics;
pub(crate) use faults::{Action, Point, pause, take_fault};
pub(crate) use metrics::{Gauge, Lease, Query, Span, Work, add, sub};

pub(crate) const ENV_NAMES: [&str; 3] = [
    "MINICORE_AGENT_E2E_TRACE",
    "MINICORE_AGENT_E2E_PLAN",
    "MINICORE_AGENT_E2E_RELEASE_DIR",
];
const MAX_ROW: usize = 8192;
const MAX_TRACE: usize = 16 * 1024 * 1024;
const MAX_ROWS: u64 = 65_536;
const QUEUED_ROWS: usize = 256;
const MAX_LOADS: usize = 64;
const MAX_TURNS: usize = 64;
static OBSERVER: OnceLock<Observer> = OnceLock::new();

pub(super) fn get() -> Option<&'static Observer> {
    OBSERVER.get()
}
fn ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn clock() -> Option<u64> {
    get().map(Observer::now)
}

pub(crate) fn responsiveness_probe(kind: &'static str) {
    if let Some(observer) = get() {
        observer.emit("responsiveness_probe", json!({"kind":kind}));
    }
}

pub(crate) fn input_fixture_enabled() -> bool {
    get().is_some_and(|observer| observer.faults.lock().unwrap().input_fixture)
}

#[derive(Clone, Copy, Serialize)]
pub(crate) struct SnapshotKey {
    pub(crate) session_id: SessionId,
    pub(crate) load: LoadId,
    pub(crate) revision: u64,
}

#[derive(Clone, Copy, Serialize)]
pub(super) struct Identity {
    session_id: SessionId,
    load: LoadId,
    loop_id: LoopId,
    session: u64,
    turn: u64,
}

struct Loaded {
    session_id: SessionId,
    load: LoadId,
    ordinal: u64,
    turns: u64,
    recent: VecDeque<(LoopId, u64)>,
}

#[derive(Default)]
struct Identities {
    next: u64,
    loads: Vec<Loaded>,
}

struct Row {
    event: &'static str,
    mono_ns: u64,
    data: Value,
}
enum Message {
    Row(Row),
    End(Value),
}

pub(super) struct Observer {
    origin: Instant,
    metrics: metrics::Metrics,
    identities: Mutex<Identities>,
    faults: Mutex<faults::Faults>,
    sender: mpsc::SyncSender<Message>,
    writer: Mutex<Option<JoinHandle<bool>>>,
    invalid: AtomicBool,
    dropped: AtomicU64,
}

impl Observer {
    fn now(&self) -> u64 {
        ns(self.origin.elapsed())
    }
    fn emit(&self, event: &'static str, data: Value) {
        let row = Row {
            event,
            mono_ns: self.now(),
            data,
        };
        if self.sender.try_send(Message::Row(row)).is_err() {
            self.dropped
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    Some(value.saturating_add(1))
                })
                .unwrap();
            self.invalid.store(true, Ordering::SeqCst);
        }
    }
    fn checkpoint(&self) {
        self.emit("counters", self.counters());
    }
    fn identity(&self, session: SessionId, loop_id: LoopId) -> Option<Identity> {
        let identities = self.identities.lock().unwrap();
        identities
            .loads
            .iter()
            .filter(|load| load.session_id == session)
            .find_map(|load| {
                load.recent
                    .iter()
                    .find(|(id, _)| *id == loop_id)
                    .map(|(_, turn)| Identity {
                        session_id: session,
                        load: load.load,
                        loop_id,
                        session: load.ordinal,
                        turn: *turn,
                    })
            })
    }
}

pub(crate) async fn start_from_env() -> Result<(), AgentError> {
    if get().is_some() {
        return Err(AgentError::InvalidState);
    }
    let Some(trace) = std::env::var_os(ENV_NAMES[0]) else {
        if ENV_NAMES[1..]
            .iter()
            .any(|name| std::env::var_os(name).is_some())
        {
            return Err(AgentError::InvalidArguments);
        }
        return Ok(());
    };
    let plan = std::env::var_os(ENV_NAMES[1]).map(PathBuf::from);
    let releases = std::env::var_os(ENV_NAMES[2]).map(PathBuf::from);
    tokio::task::spawn_blocking(move || {
        let faults=faults::Faults::load(plan,releases)?;
        let file=OpenOptions::new().create_new(true).write(true).open(PathBuf::from(trace)).map_err(|_|AgentError::InvalidArguments)?;
        let (sender,receiver)=mpsc::sync_channel(QUEUED_ROWS);
        let observer=Observer {origin:Instant::now(),metrics:metrics::Metrics::default(),identities:Mutex::new(Identities::default()),
            faults:Mutex::new(faults),sender,writer:Mutex::new(Some(std::thread::spawn(move ||write_trace(file,receiver)))),
            invalid:AtomicBool::new(false),dropped:AtomicU64::new(0)};
        OBSERVER.set(observer).map_err(|_|AgentError::InvalidState)?;
        let observer = get().unwrap();
        observer.emit("observer_ready",json!({"pid":std::process::id(),"max_row_bytes":MAX_ROW,"max_trace_bytes":MAX_TRACE,"max_rows":MAX_ROWS,
            "gauge_order":"per-gauge-revision-v1","query_order":"query-id-v1",
            "limits":observer.faults.lock().unwrap().limits(crate::sessions::results::ResultLimits::default()),
            "control_staging_limit_bytes":crate::sessions::hub::SNAPSHOT_BUDGET_BYTES,"control_frame_limit_bytes":crate::rpc::MAX_RPC_FRAME_BYTES}));
        Ok(())
    }).await.map_err(|_|AgentError::Internal)?
}

fn write_trace(file: File, receiver: mpsc::Receiver<Message>) -> bool {
    let mut writer = BufWriter::with_capacity(16 * 1024, file);
    let mut seq = 0u64;
    let mut bytes = 0usize;
    let write_row = |writer: &mut BufWriter<File>,
                     seq: &mut u64,
                     bytes: &mut usize,
                     row: Row|
     -> std::io::Result<()> {
        *seq = seq
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("trace counter overflow"))?;
        let mut data = row.data.as_object().cloned().unwrap_or_default();
        data.insert("schema".into(), json!("agent-e2e-1"));
        data.insert("event_seq".into(), json!(*seq));
        data.insert("event".into(), json!(row.event));
        data.insert("mono_ns".into(), json!(row.mono_ns));
        let encoded = serde_json::to_vec(&data).map_err(std::io::Error::other)?;
        if encoded.len() + 1 > MAX_ROW
            || bytes.saturating_add(encoded.len() + 1) > MAX_TRACE
            || *seq > MAX_ROWS
        {
            return Err(std::io::Error::other("trace capacity exceeded"));
        }
        writer.write_all(&encoded)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        *bytes += encoded.len() + 1;
        Ok(())
    };
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Row(row) => {
                if write_row(&mut writer, &mut seq, &mut bytes, row).is_err() {
                    return false;
                }
            }
            Message::End(data) => {
                let mono_ns = data["ended_ns"].as_u64().unwrap();
                if write_row(
                    &mut writer,
                    &mut seq,
                    &mut bytes,
                    Row {
                        event: "counters",
                        mono_ns,
                        data: data["counters"].clone(),
                    },
                )
                .is_err()
                {
                    return false;
                }
                return write_row(
                    &mut writer,
                    &mut seq,
                    &mut bytes,
                    Row {
                        event: "observer_end",
                        mono_ns,
                        data,
                    },
                )
                .is_ok();
            }
        }
    }
    false
}

pub(crate) async fn finish(success: bool) -> bool {
    let Some(observer) = get() else { return true };
    tokio::task::spawn_blocking(move || {
        let unused=observer.faults.lock().unwrap().unused();
        if unused!=0 || !success { observer.invalid.store(true,Ordering::SeqCst); }
        let invalid=observer.invalid.load(Ordering::SeqCst);
        let end=json!({"ended_ns":observer.now(),"invalid":invalid,"trace_dropped":observer.dropped.load(Ordering::SeqCst),
            "unconsumed_steps":unused,"counters":observer.counters()});
        let sent=observer.sender.send(Message::End(end)).is_ok();
        let joined=observer.writer.lock().unwrap().take().is_some_and(|thread|thread.join().unwrap_or(false));
        sent && joined && !invalid
    }).await.unwrap_or(false)
}

pub(crate) fn validate_models(
    config: &crate::AgentConfig,
) -> Result<(), crate::models::ModelConfigError> {
    if get().is_none() {
        return Ok(());
    }
    for model in config.models.values() {
        let crate::ModelConfig::OpenAiResponses { base_url, .. } = model;
        let url = reqwest::Url::parse(base_url)
            .map_err(|_| crate::models::ModelConfigError::InvalidConfiguration)?;
        let host = url.host_str().unwrap_or("").trim_matches(['[', ']']);
        if !host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
        {
            return Err(crate::models::ModelConfigError::InvalidConfiguration);
        }
    }
    Ok(())
}

pub(crate) fn result_limits(
    default: crate::sessions::results::ResultLimits,
) -> crate::sessions::results::ResultLimits {
    get().map_or(default, |observer| {
        observer.faults.lock().unwrap().limits(default)
    })
}

pub(crate) fn register(session_id: SessionId, load: LoadId) {
    let Some(observer) = get() else { return };
    let ordinal = {
        let mut state = observer.identities.lock().unwrap();
        if state.loads.len() == MAX_LOADS || state.next == u64::MAX {
            observer.invalid.store(true, Ordering::SeqCst);
            return;
        }
        state.next += 1;
        let ordinal = state.next;
        state.loads.push(Loaded {
            session_id,
            load,
            ordinal,
            turns: 0,
            recent: VecDeque::new(),
        });
        ordinal
    };
    observer.emit(
        "load_registered",
        json!({"session_id":session_id,"load":load,"session":ordinal}),
    );
}

pub(crate) fn unregister(session_id: SessionId, load: LoadId) {
    let Some(observer) = get() else { return };
    observer
        .identities
        .lock()
        .unwrap()
        .loads
        .retain(|entry| entry.load != load);
    observer.emit(
        "load_unregistered",
        json!({"session_id":session_id,"load":load}),
    );
    observer.checkpoint();
}

pub(crate) fn installed(turn: crate::TurnRef, load: LoadId, input_id: &str) {
    let Some(observer) = get() else { return };
    let identity = {
        let mut state = observer.identities.lock().unwrap();
        let Some(entry) = state.loads.iter_mut().find(|entry| entry.load == load) else {
            return;
        };
        let Some(next) = entry.turns.checked_add(1) else {
            observer.invalid.store(true, Ordering::SeqCst);
            return;
        };
        entry.turns = next;
        if entry.recent.len() == MAX_TURNS {
            entry.recent.pop_front();
        }
        entry.recent.push_back((turn.loop_id, next));
        Identity {
            session_id: turn.session_id,
            load,
            loop_id: turn.loop_id,
            session: entry.ordinal,
            turn: next,
        }
    };
    let mut data = serde_json::to_value(identity).unwrap();
    data["input_id"] = json!(input_id);
    observer.emit("loop_installed", data);
}

pub(crate) fn sample(snapshot: &crate::SessionSnapshot, started: Option<u64>) {
    let Some(observer) = get() else { return };
    let sampled = observer.now();
    let (kind, loop_id, input_id) = match &snapshot.execution {
        crate::SessionExecution::None => ("none", None, None),
        crate::SessionExecution::Starting { input } => ("starting", None, Some(input.id.as_str())),
        crate::SessionExecution::Running { loop_id, input, .. } => {
            ("running", Some(*loop_id), Some(input.id.as_str()))
        }
        crate::SessionExecution::Finishing { loop_id, input, .. } => {
            ("finishing", Some(*loop_id), Some(input.id.as_str()))
        }
    };
    observer.emit("snapshot_sample",json!({"session_id":snapshot.session_id,"load":snapshot.load,"revision":snapshot.revision,
        "sample_started_ns":started,"sampled_ns":sampled,"duration_ns":sampled.saturating_sub(started.unwrap_or(sampled)),
        "execution":kind,"loop_id":loop_id,"input_id":input_id,"queue_count":snapshot.queue.items.len(),
        "completion_count":snapshot.completions.entries.len(),"through_seq":snapshot.completions.through_seq,"retained_from_seq":snapshot.completions.retained_from_seq}));
}

pub(crate) fn encoded(key: SnapshotKey, start: Option<u64>, bytes: usize) {
    let Some(observer) = get() else { return };
    let mut data = serde_json::to_value(key).unwrap();
    data["duration_ns"] = json!(observer.now().saturating_sub(start.unwrap_or(0)));
    data["encoded_bytes"] = json!(bytes);
    observer.emit("snapshot_encoded", data);
}

pub(crate) fn sent(key: SnapshotKey, start: Option<u64>, bytes: usize) {
    let Some(observer) = get() else { return };
    let written = observer.now();
    let mut data = serde_json::to_value(key).unwrap();
    data["write_started_ns"] = json!(start);
    data["written_ns"] = json!(written);
    data["duration_ns"] = json!(written.saturating_sub(start.unwrap_or(written)));
    data["encoded_bytes"] = json!(bytes);
    observer.emit("snapshot_sent", data);
}

pub(crate) fn completion(load: LoadId, entry: &crate::CompletionEntry) {
    if let Some(observer) = get() {
        let session_id = observer
            .identities
            .lock()
            .unwrap()
            .loads
            .iter()
            .find(|entry| entry.load == load)
            .map(|entry| entry.session_id);
        observer.emit("completion_registered",json!({"session_id":session_id,"load":load,"seq":entry.seq,"loop_id":entry.loop_id,
        "input_id":entry.input_id,"completion_kind":entry.completion_kind,"outcome_kind":entry.outcome_kind,"persistence":entry.persistence,
        "persistence_uncertain":entry.persistence_uncertain,"availability":entry.availability,"unavailable_reason":entry.unavailable_reason}));
    }
}

#[cfg(test)]
mod tests;
