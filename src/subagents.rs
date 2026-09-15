use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use minicore_runtime::execution::{ExecutionConfig, UserInput};
use minicore_runtime::history::HistoryItem;
use minicore_runtime::model::{AssistantPart, Model, ReasoningPreference, Usage};
use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolProgress, ToolProgressSink, ToolSpec,
};
use minicore_runtime::{AgentLoop, LoopEvent, LoopOptions, LoopOutcome, LoopRequest, LoopStatus};

use crate::compaction::CompactionState;
use crate::event::AgentEventSink;
use crate::ids::SessionId;
use crate::models::Models;
use crate::policy::Policy;
use crate::presentation::{Presentation, PresentationModel};
use crate::profiles::ApprovalMode;
use crate::prompt::ProjectPromptProvider;
use crate::tools::command::CommandOwners;
use crate::tools::{BuildToolsError, CommandEnvironment};
use crate::workspace::Workspace;

pub(crate) const TOOL_NAME: &str = "subagent";
const MAX_TASKS: usize = 8;
const MAX_CONCURRENCY: usize = 4;
const MAX_TASK_BYTES: usize = 256 * 1024;
const MAX_MODEL_REFERENCE_BYTES: usize = 256;
const MAX_CWD_BYTES: usize = 4 * 1024;
const MAX_PROGRESS_BYTES: usize = 2 * 1024;
const MAX_OUTPUT_BYTES: usize = 50 * 1024;
const MAX_DETAILS_BYTES: usize = 512 * 1024;

struct TrackedChild {
    id: u64,
    session_id: SessionId,
    cancellation: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Agent-owned child-loop lifetime service. It deliberately tracks only the
/// child workers started by this Tool; it is not a general task tree or a
/// persistent subagent registry.
pub(crate) struct SubagentService {
    next_id: AtomicU64,
    tasks: Mutex<Vec<Arc<TrackedChild>>>,
}

impl SubagentService {
    pub(crate) const fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            tasks: Mutex::new(Vec::new()),
        }
    }

    fn start<T, F, Fut>(
        self: &Arc<Self>,
        session_id: SessionId,
        parent_cancellation: CancellationToken,
        operation: F,
    ) -> (u64, oneshot::Receiver<Result<T, SubagentWorkerError>>)
    where
        T: Send + 'static,
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, SubagentWorkerError>> + Send + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let cancellation = parent_cancellation.child_token();
        let worker_cancellation = cancellation.clone();
        let (sender, receiver) = oneshot::channel();
        let mut tasks = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Keep registration and spawning in one synchronous critical section:
        // a concurrent Session/Agent drain must not observe the service before
        // this child is discoverable.
        let task = tokio::spawn(async move {
            let result = operation(worker_cancellation).await;
            let _ = sender.send(result);
        });
        let entry = Arc::new(TrackedChild {
            id,
            session_id,
            cancellation,
            task: tokio::sync::Mutex::new(Some(task)),
        });
        tasks.push(entry);
        (id, receiver)
    }

    async fn join_entry(entry: &Arc<TrackedChild>) {
        let mut task = entry.task.lock().await;
        if let Some(handle) = task.as_mut() {
            let _ = std::pin::Pin::new(handle).await;
            task.take();
        }
    }

    fn remove_entry(&self, entry: &Arc<TrackedChild>) {
        let mut tasks = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tasks.retain(|candidate| !Arc::ptr_eq(candidate, entry));
    }

    fn entries_for(
        &self,
        session_id: Option<&SessionId>,
        ids: Option<&[u64]>,
    ) -> Vec<Arc<TrackedChild>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tasks
            .iter()
            .filter(|entry| {
                session_id.is_none_or(|session| entry.session_id == *session)
                    && ids.is_none_or(|ids| ids.contains(&entry.id))
            })
            .cloned()
            .collect()
    }

    async fn join_entries(&self, entries: Vec<Arc<TrackedChild>>, cancel: bool) {
        for entry in entries {
            if cancel {
                entry.cancellation.cancel();
            }
            Self::join_entry(&entry).await;
            self.remove_entry(&entry);
        }
    }

    pub(crate) async fn drain_session(&self, session_id: SessionId) {
        loop {
            let entries = self.entries_for(Some(&session_id), None);
            if entries.is_empty() {
                return;
            }
            self.join_entries(entries, true).await;
        }
    }

    pub(crate) async fn drain_all(&self) {
        loop {
            let entries = self.entries_for(None, None);
            if entries.is_empty() {
                return;
            }
            self.join_entries(entries, true).await;
        }
    }

    async fn join_ids(&self, ids: &[u64], cancel: bool) -> Vec<u64> {
        if ids.is_empty() {
            return Vec::new();
        }
        let entries = self.entries_for(None, Some(ids));
        self.join_entries(entries, cancel).await;
        ids.to_vec()
    }

    fn cancel_ids(&self, ids: &[u64]) {
        let entries = self.entries_for(None, Some(ids));
        for entry in entries {
            entry.cancellation.cancel();
        }
    }

    #[cfg(test)]
    fn contains_id(&self, id: u64) -> bool {
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|entry| entry.id == id)
    }
}

struct ChildScope {
    service: Arc<SubagentService>,
    ids: Mutex<Vec<u64>>,
}

impl ChildScope {
    fn new(service: Arc<SubagentService>) -> Self {
        Self {
            service,
            ids: Mutex::new(Vec::new()),
        }
    }

    fn track(&self, id: u64) {
        self.ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(id);
    }

    async fn join(&self, cancel: bool) {
        let ids = self
            .ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let completed = self.service.join_ids(&ids, cancel).await;
        if !completed.is_empty() {
            self.ids
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .retain(|id| !completed.contains(id));
        }
    }
}

impl Drop for ChildScope {
    fn drop(&mut self) {
        // Dropping a cancelled Tool scope only requests cancellation. The
        // service retains each join handle until a later owner drains it.
        let ids = self
            .ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        self.service.cancel_ids(&ids);
    }
}

#[derive(Clone)]
pub(crate) struct SubagentFactory {
    pub(crate) service: Arc<SubagentService>,
    pub(crate) session_id: SessionId,
    pub(crate) parent_workspace: Arc<Workspace>,
    pub(crate) models: Arc<Models>,
    pub(crate) command_environment: CommandEnvironment,
    pub(crate) system_prompt: String,
    pub(crate) parent_tools: Vec<String>,
    pub(crate) approval: ApprovalMode,
    pub(crate) options: LoopOptions,
    pub(crate) default_model: String,
    pub(crate) default_reasoning: ReasoningPreference,
}

impl SubagentFactory {
    fn child_tools(&self) -> Vec<String> {
        child_tool_names(&self.parent_tools)
    }

    async fn prepare_task(&self, task: TaskInput) -> Result<PreparedTask, ToolError> {
        validate_task_text(&task.task)?;
        let workspace = self.workspace_for(task.cwd.as_deref()).await?;
        let (model, model_id, reasoning) = self.resolve_model(&task)?;
        let child_tools = self.child_tools();
        if !child_tools.is_empty() && !model.descriptor().supports_tools {
            return Err(ToolError::InvalidInvocation);
        }

        let child_session_id = SessionId::new().map_err(|_| ToolError::Internal)?;
        let (events_tx, events_rx) = mpsc::channel(8);
        let presentation = Presentation::new(child_session_id, AgentEventSink::new(events_tx));
        drop(events_rx);
        presentation.set_model_label(model_id.clone());
        let tools = crate::tools::build_tools_for_child_with_presentation(
            &child_tools,
            Arc::clone(&workspace),
            self.command_environment.clone(),
            &presentation,
        )
        .map_err(map_build_tools_error)?;
        let policy = if child_tools.is_empty() {
            None
        } else {
            Some(Arc::new(Policy::new(self.approval))
                as Arc<dyn minicore_runtime::tools::ToolPolicy>)
        };
        let prompt: Arc<dyn minicore_runtime::prompt::PromptProvider> = Arc::new(
            ProjectPromptProvider::new(
                Arc::clone(&workspace),
                self.system_prompt.clone(),
                CompactionState::new(),
            )
            .map_err(|_| ToolError::InvalidInvocation)?,
        );
        let model = PresentationModel::new(model, Arc::clone(&presentation));
        let config = ExecutionConfig::new(model, reasoning, tools, policy, prompt)
            .map_err(|_| ToolError::InvalidInvocation)?;
        Ok(PreparedTask {
            task: task.task,
            model_id,
            reasoning,
            cwd: workspace.root().to_string_lossy().into_owned(),
            config,
            options: self.options.clone(),
            owners: Arc::clone(presentation.command_owners()),
        })
    }

    async fn workspace_for(&self, cwd: Option<&str>) -> Result<Arc<Workspace>, ToolError> {
        let Some(cwd) = cwd else {
            return Ok(Arc::clone(&self.parent_workspace));
        };
        validate_cwd(cwd)?;
        let path = Path::new(cwd);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.parent_workspace.root().join(path)
        };
        let workspace = Workspace::open(path)
            .await
            .map_err(|_| ToolError::InvalidInvocation)?;
        if !workspace.root().starts_with(self.parent_workspace.root()) {
            return Err(ToolError::InvalidInvocation);
        }
        Ok(Arc::new(workspace))
    }

    fn resolve_model(
        &self,
        task: &TaskInput,
    ) -> Result<(Arc<dyn Model>, String, ReasoningPreference), ToolError> {
        let reference = task
            .model
            .as_deref()
            .unwrap_or(self.default_model.as_str())
            .trim();
        if reference.is_empty() || reference.len() > MAX_MODEL_REFERENCE_BYTES {
            return Err(ToolError::InvalidInvocation);
        }

        let (model_id, suffix_reasoning) = if self.models.get(reference).is_ok() {
            (reference.to_owned(), None)
        } else {
            let Some((candidate, suffix)) = reference.rsplit_once(':') else {
                return Err(ToolError::InvalidInvocation);
            };
            let Some(reasoning) = parse_reasoning_suffix(suffix) else {
                return Err(ToolError::InvalidInvocation);
            };
            if candidate.is_empty() || self.models.get(candidate).is_err() {
                return Err(ToolError::InvalidInvocation);
            }
            (candidate.to_owned(), Some(reasoning))
        };
        if suffix_reasoning.is_some() && task.reasoning.is_some() {
            return Err(ToolError::InvalidInvocation);
        }
        let model = self
            .models
            .get(&model_id)
            .map_err(|_| ToolError::InvalidInvocation)?;
        let reasoning = task
            .reasoning
            .or(suffix_reasoning)
            .unwrap_or(self.default_reasoning);
        if !model.descriptor().supports_reasoning(reasoning) {
            return Err(ToolError::InvalidInvocation);
        }
        Ok((model, model_id, reasoning))
    }
}

fn child_tool_names(parent_tools: &[String]) -> Vec<String> {
    parent_tools
        .iter()
        .filter(|name| name.as_str() != TOOL_NAME)
        .cloned()
        .collect()
}

fn map_build_tools_error(error: BuildToolsError) -> ToolError {
    match error {
        BuildToolsError::InvalidConfiguration => ToolError::InvalidInvocation,
        BuildToolsError::Internal => ToolError::Internal,
    }
}

#[derive(Clone)]
struct PreparedTask {
    task: String,
    model_id: String,
    reasoning: ReasoningPreference,
    cwd: String,
    config: ExecutionConfig,
    options: LoopOptions,
    /// Owned commands of the child loop. Joined when the stage ends, so a Bash
    /// process cannot outlive the child that started it.
    owners: Arc<CommandOwners>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubagentMode {
    Single,
    Parallel,
    Chain,
}

impl SubagentMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Parallel => "parallel",
            Self::Chain => "chain",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskInput {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    reasoning: Option<ReasoningPreference>,
    task: String,
    #[serde(default)]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubagentInput {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    reasoning: Option<ReasoningPreference>,
    #[serde(default)]
    task: Option<String>,
    #[serde(default)]
    tasks: Option<Vec<TaskInput>>,
    #[serde(default)]
    chain: Option<Vec<TaskInput>>,
    #[serde(default)]
    cwd: Option<String>,
}

pub(crate) struct SubagentTool {
    factory: SubagentFactory,
    spec: ToolSpec,
}

impl SubagentTool {
    pub(crate) fn new(factory: SubagentFactory) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("subagent is a valid tool name"),
            "Run bounded stateless child model loops in the current workspace. Child sessions are not stored and cannot call subagent recursively.",
            subagent_schema(),
        )
        .expect("static subagent tool specification is valid");
        Self { factory, spec }
    }
}

impl Tool for SubagentTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute(&self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'_> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            if Instant::now() >= context.deadline {
                return Err(ToolError::TimedOut);
            }
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input = parse_subagent_input(invocation.arguments())?;
            let (mode, tasks) = validate_input(input)?;
            let factory = self.factory.clone();
            let stage_context = context.clone();
            let scope = Arc::new(ChildScope::new(Arc::clone(&factory.service)));
            let operation_scope = Arc::clone(&scope);
            let operation = async move {
                let prepared = prepare_tasks(&factory, tasks).await?;
                if prepared.is_empty() {
                    return Err(ToolError::InvalidInvocation);
                }
                let total = prepared.len();
                let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let results = match mode {
                    SubagentMode::Single => vec![
                        dispatch_stage(
                            &factory,
                            prepared.into_iter().next().expect("one task"),
                            1,
                            total,
                            stage_context.clone(),
                            Arc::clone(&completed),
                            Arc::clone(&operation_scope),
                        )
                        .await?,
                    ],
                    SubagentMode::Parallel => {
                        let mut slots: Vec<Option<StageResult>> =
                            (0..total).map(|_| None).collect();
                        let stream = futures_util::stream::iter(
                            prepared.into_iter().enumerate().map(|(index, task)| {
                                let factory = factory.clone();
                                let stage_context = stage_context.clone();
                                let completed = Arc::clone(&completed);
                                let scope = Arc::clone(&operation_scope);
                                async move {
                                    let result = dispatch_stage(
                                        &factory,
                                        task,
                                        index + 1,
                                        total,
                                        stage_context,
                                        completed,
                                        scope,
                                    )
                                    .await;
                                    (index, result)
                                }
                            }),
                        )
                        .buffer_unordered(MAX_CONCURRENCY);
                        tokio::pin!(stream);
                        while let Some((index, result)) = stream.next().await {
                            slots[index] = Some(result?);
                        }
                        slots
                            .into_iter()
                            .map(|result| result.expect("parallel result slot is filled"))
                            .collect()
                    }
                    SubagentMode::Chain => {
                        let mut results = Vec::with_capacity(total);
                        let mut previous = String::new();
                        let mut remaining = prepared.into_iter().enumerate();
                        while let Some((index, task)) = remaining.next() {
                            let task_text = match replace_previous(&task.task, &previous) {
                                Ok(task_text) => task_text,
                                Err(()) => {
                                    results.push(StageResult::failed(
                                        index + 1,
                                        &task,
                                        "chain task exceeds the bounded input limit",
                                    ));
                                    for (remaining_index, remaining_task) in remaining {
                                        results.push(StageResult::skipped(
                                            remaining_index + 1,
                                            &remaining_task,
                                            "previous chain stage failed",
                                        ));
                                    }
                                    break;
                                }
                            };
                            let stage = dispatch_stage(
                                &factory,
                                PreparedTask {
                                    task: task_text,
                                    ..task
                                },
                                index + 1,
                                total,
                                stage_context.clone(),
                                Arc::clone(&completed),
                                Arc::clone(&operation_scope),
                            )
                            .await?;
                            let failed = stage.status != "completed";
                            previous = stage.output.clone();
                            results.push(stage);
                            if failed {
                                for (remaining_index, remaining_task) in remaining {
                                    results.push(StageResult::skipped(
                                        remaining_index + 1,
                                        &remaining_task,
                                        "previous chain stage failed",
                                    ));
                                }
                                break;
                            }
                        }
                        results
                    }
                };
                let output = encode_result(mode, results)?;
                ToolOutput::new(output).map_err(|_| ToolError::Internal)
            };
            let result = tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(ToolError::Cancelled),
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(context.deadline)) => {
                    Err(ToolError::TimedOut)
                }
                result = operation => {
                    match result.as_ref() {
                        Ok(_) => scope.join(false).await,
                        Err(error)
                            if matches!(*error, ToolError::Cancelled | ToolError::TimedOut) => {}
                        Err(_) => scope.join(true).await,
                    }
                    result
                }
            }?;
            Ok(ToolExecutionOutcome::Completed(result))
        })
    }
}

async fn prepare_tasks(
    factory: &SubagentFactory,
    tasks: Vec<TaskInput>,
) -> Result<Vec<PreparedTask>, ToolError> {
    let mut prepared = Vec::with_capacity(tasks.len());
    for task in tasks {
        prepared.push(factory.prepare_task(task).await?);
    }
    Ok(prepared)
}

async fn dispatch_stage(
    factory: &SubagentFactory,
    task: PreparedTask,
    stage_index: usize,
    total: usize,
    context: ToolContext,
    completed: Arc<std::sync::atomic::AtomicUsize>,
    scope: Arc<ChildScope>,
) -> Result<StageResult, ToolError> {
    let cancellation = context.cancellation.clone();
    let deadline = context.deadline;
    let progress = context.progress.clone();
    emit_progress(
        &progress,
        format!("stage {stage_index} started"),
        None,
        Some(total as u64),
    );
    let task_for_worker = task.clone();
    let child_progress = progress.clone();
    let (child_id, receiver) = factory.service.start(
        factory.session_id,
        cancellation.clone(),
        move |worker_cancellation| {
            run_child(
                task_for_worker,
                stage_index,
                deadline,
                worker_cancellation,
                child_progress.clone(),
            )
        },
    );
    scope.track(child_id);
    let stage = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(ToolError::Cancelled),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            return Err(ToolError::TimedOut);
        }
        result = receiver => match result {
            Ok(Ok(stage)) => stage,
            Ok(Err(SubagentWorkerError::Cancelled)) => return Err(ToolError::Cancelled),
            Ok(Err(SubagentWorkerError::TimedOut)) => return Err(ToolError::TimedOut),
            Ok(Err(SubagentWorkerError::Internal)) | Err(_) => {
                StageResult::internal(stage_index, &task, "child worker failed")
            }
        }
    };
    let done = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    emit_progress(
        &progress,
        format!("stage {stage_index} {}", stage.status),
        Some(done as u64),
        Some(total as u64),
    );
    Ok(stage)
}

/// Owns one child stage: the child loop, its commands, and the join that keeps
/// both from outliving the stage.
async fn run_child(
    task: PreparedTask,
    stage_index: usize,
    deadline: Instant,
    cancellation: CancellationToken,
    progress: ToolProgressSink,
) -> Result<StageResult, SubagentWorkerError> {
    let owners = Arc::clone(&task.owners);
    let result = run_stage(task, stage_index, deadline, cancellation, progress).await;
    // Every exit path joins the child's command owners; the child loop already
    // ended, so this only waits for a process that was really started.
    owners.join_all().await;
    result
}

async fn run_stage(
    task: PreparedTask,
    stage_index: usize,
    deadline: Instant,
    cancellation: CancellationToken,
    progress: ToolProgressSink,
) -> Result<StageResult, SubagentWorkerError> {
    if cancellation.is_cancelled() {
        return Err(SubagentWorkerError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(SubagentWorkerError::TimedOut);
    }
    let input = UserInput::text(&task.task).map_err(|_| SubagentWorkerError::Internal)?;
    let mut options = task.options.clone();
    options.deadline = Some(tokio::time::Instant::from_std(deadline));
    let history: Arc<[HistoryItem]> = Vec::new().into();
    let request = LoopRequest::new(history, input, task.config.clone());
    let mut child =
        AgentLoop::start(request, options).map_err(|_| SubagentWorkerError::Internal)?;
    let handle = child.handle();
    let mut events = match child.take_events() {
        Ok(events) => events,
        Err(_) => {
            let _ = child.join().await;
            return Err(SubagentWorkerError::Internal);
        }
    };
    let mut join = Box::pin(child.join());
    let mut state = handle.watch_state();
    let mut state_open = true;
    let mut events_open = true;
    let mut cancellation_requested = false;
    let mut deadline_requested = false;
    let mut interaction_requested = false;

    let initial_waiting = {
        let current = state.borrow();
        current.status == LoopStatus::WaitingForInput
    };
    if initial_waiting {
        interaction_requested = true;
        cancellation_requested = true;
        handle.cancel();
        emit_progress(
            &progress,
            format!("stage {stage_index} child interaction unavailable"),
            None,
            None,
        );
    }

    let report = loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled(), if !cancellation_requested => {
                cancellation_requested = true;
                handle.cancel();
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)), if !cancellation_requested => {
                cancellation_requested = true;
                deadline_requested = true;
                handle.cancel();
            }
            changed = state.changed(), if state_open => match changed {
                Ok(()) => {
                    let waiting = {
                        let current = state.borrow();
                        current.status == LoopStatus::WaitingForInput
                    };
                    if waiting && !interaction_requested {
                        interaction_requested = true;
                        cancellation_requested = true;
                        handle.cancel();
                        emit_progress(
                            &progress,
                            format!("stage {stage_index} child interaction unavailable"),
                            None,
                            None,
                        );
                    }
                }
                Err(_) => state_open = false,
            },
            result = &mut join => break result.map_err(|_| SubagentWorkerError::Internal)?,
            event = events.recv(), if events_open => match event {
                Some(event) => forward_progress(&progress, stage_index, &event.event),
                None => events_open = false,
            }
        }
    };
    while let Ok(event) = events.try_recv() {
        forward_progress(&progress, stage_index, &event.event);
    }

    if cancellation.is_cancelled() && !deadline_requested && !interaction_requested {
        return Err(SubagentWorkerError::Cancelled);
    }
    let mut stage = StageResult::from_report(stage_index, &task, &report);
    if interaction_requested {
        stage.status = "failed";
        stage.error = Some("child interaction requires approval or input");
    } else if deadline_requested {
        stage.status = "failed";
        stage.error = Some("child deadline exceeded");
    }
    Ok(stage)
}

fn forward_progress(progress: &ToolProgressSink, stage_index: usize, event: &LoopEvent) {
    match event {
        LoopEvent::OutputDelta {
            channel: minicore_runtime::OutputChannel::Text,
            ..
        } => emit_progress(
            progress,
            format!("stage {stage_index} output updated"),
            None,
            None,
        ),
        LoopEvent::RequestStarted { request_index, .. } => emit_progress(
            progress,
            format!("stage {stage_index} request {request_index}"),
            None,
            None,
        ),
        LoopEvent::ToolStarted { tool_name, .. } => emit_progress(
            progress,
            format!("stage {stage_index} tool {} started", tool_name.as_str()),
            None,
            None,
        ),
        LoopEvent::ToolFinished { .. } => emit_progress(
            progress,
            format!("stage {stage_index} tool finished"),
            None,
            None,
        ),
        LoopEvent::ToolProgress {
            progress: child, ..
        } => emit_progress(
            progress,
            format!("stage {stage_index} tool progress"),
            child.completed,
            child.total,
        ),
        _ => {}
    }
}

fn emit_progress(
    sink: &ToolProgressSink,
    message: String,
    completed: Option<u64>,
    total: Option<u64>,
) {
    let message = truncate_utf8(&message, MAX_PROGRESS_BYTES).0;
    let Ok(message) =
        minicore_runtime::BoundedText::new_with_max_bytes(message, MAX_PROGRESS_BYTES)
    else {
        return;
    };
    let Ok(progress) = ToolProgress::new(Some(message), completed, total) else {
        return;
    };
    let _ = sink.emit(progress);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubagentWorkerError {
    Cancelled,
    TimedOut,
    Internal,
}

#[derive(Clone, Debug, Serialize)]
struct StageResult {
    stage_index: usize,
    status: &'static str,
    model: String,
    reasoning: ReasoningPreference,
    cwd: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    loop_id: Option<minicore_runtime::LoopId>,
    output: String,
    output_truncated: bool,
    usage: Option<Usage>,
    requests: Option<u32>,
    tool_rounds: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'static str>,
}

impl StageResult {
    fn from_report(
        stage_index: usize,
        task: &PreparedTask,
        report: &minicore_runtime::LoopReport,
    ) -> Self {
        let (status, error) = match &report.outcome {
            LoopOutcome::Completed => ("completed", None),
            LoopOutcome::Cancelled(reason) => (
                "cancelled",
                Some(match reason {
                    minicore_runtime::CancelReason::Deadline => "child deadline exceeded",
                    _ => "child loop cancelled",
                }),
            ),
            LoopOutcome::Failed(failure) => {
                let status = matches!(failure.kind, minicore_runtime::LoopFailureKind::Internal)
                    .then_some("internal")
                    .unwrap_or("failed");
                (status, Some(loop_failure_message(failure)))
            }
        };
        let (output, output_truncated) = child_output(report);
        Self {
            stage_index,
            status,
            model: task.model_id.clone(),
            reasoning: task.reasoning,
            cwd: task.cwd.clone(),
            loop_id: Some(report.loop_id),
            output,
            output_truncated,
            usage: (!is_empty_usage(&report.usage)).then_some(report.usage),
            requests: Some(report.requests),
            tool_rounds: Some(report.tool_rounds),
            error,
        }
    }

    fn failed(stage_index: usize, task: &PreparedTask, error: &'static str) -> Self {
        Self {
            stage_index,
            status: "failed",
            model: task.model_id.clone(),
            reasoning: task.reasoning,
            cwd: task.cwd.clone(),
            loop_id: None,
            output: String::new(),
            output_truncated: false,
            usage: None,
            requests: None,
            tool_rounds: None,
            error: Some(error),
        }
    }

    fn internal(stage_index: usize, task: &PreparedTask, error: &'static str) -> Self {
        Self {
            stage_index,
            status: "internal",
            model: task.model_id.clone(),
            reasoning: task.reasoning,
            cwd: task.cwd.clone(),
            loop_id: None,
            output: String::new(),
            output_truncated: false,
            usage: None,
            requests: None,
            tool_rounds: None,
            error: Some(error),
        }
    }

    fn skipped(stage_index: usize, task: &PreparedTask, error: &'static str) -> Self {
        Self {
            stage_index,
            status: "skipped",
            model: task.model_id.clone(),
            reasoning: task.reasoning,
            cwd: task.cwd.clone(),
            loop_id: None,
            output: String::new(),
            output_truncated: false,
            usage: None,
            requests: None,
            tool_rounds: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct SubagentResult {
    status: &'static str,
    mode: &'static str,
    stages: Vec<StageResult>,
    usage: Option<Usage>,
    requests: Option<u32>,
    tool_rounds: Option<u16>,
}

fn encode_result(mode: SubagentMode, mut stages: Vec<StageResult>) -> Result<String, ToolError> {
    for stage in &mut stages {
        if stage.usage.as_ref().is_some_and(is_empty_usage) {
            stage.usage = None;
        }
    }
    let completed = stages
        .iter()
        .filter(|stage| stage.status == "completed")
        .count();
    let status = if completed == stages.len() {
        "completed"
    } else if completed > 0 {
        "partial"
    } else {
        "failed"
    };
    let result = SubagentResult {
        status,
        mode: mode.as_str(),
        usage: aggregate_usage(&stages),
        requests: aggregate_counter(&stages, |stage| stage.requests, u32::checked_add),
        tool_rounds: aggregate_counter(&stages, |stage| stage.tool_rounds, u16::checked_add),
        stages,
    };
    let total_output = result
        .stages
        .iter()
        .map(|stage| stage.output.len())
        .sum::<usize>();
    let full_encoded = serde_json::to_string(&result).map_err(|_| ToolError::Internal)?;
    if total_output <= MAX_OUTPUT_BYTES && full_encoded.len() <= MAX_DETAILS_BYTES {
        return Ok(full_encoded);
    }
    let mut low = 0usize;
    let mut high = total_output.min(MAX_OUTPUT_BYTES);
    let mut best = result.clone();
    while low <= high {
        let candidate_budget = low.saturating_add(high).div_ceil(2);
        let mut candidate = result.clone();
        apply_output_budget(&mut candidate.stages, candidate_budget);
        let encoded = serde_json::to_string(&candidate).map_err(|_| ToolError::Internal)?;
        if encoded.len() <= MAX_DETAILS_BYTES {
            best = candidate;
            low = candidate_budget.saturating_add(1);
        } else if candidate_budget == 0 {
            break;
        } else {
            high = candidate_budget - 1;
        }
    }
    let encoded = serde_json::to_string(&best).map_err(|_| ToolError::Internal)?;
    let output_bytes = best
        .stages
        .iter()
        .map(|stage| stage.output.len())
        .sum::<usize>();
    if output_bytes > MAX_OUTPUT_BYTES || encoded.len() > MAX_DETAILS_BYTES {
        return Err(ToolError::Internal);
    }
    Ok(encoded)
}

fn apply_output_budget(stages: &mut [StageResult], budget: usize) {
    let total_output = stages.iter().map(|stage| stage.output.len()).sum::<usize>();
    if total_output <= budget {
        return;
    }
    let mut remaining = budget;
    let mut stages_left = stages.len();
    for stage in stages {
        let share = remaining / stages_left.max(1);
        let (output, cut) = truncate_utf8(&stage.output, share);
        stage.output = output;
        stage.output_truncated |= cut;
        remaining = remaining.saturating_sub(stage.output.len());
        stages_left = stages_left.saturating_sub(1);
    }
}

fn is_empty_usage(usage: &Usage) -> bool {
    usage.input_tokens().is_none()
        && usage.output_tokens().is_none()
        && usage.reasoning_tokens().is_none()
        && usage.cache_read_tokens().is_none()
        && usage.cache_write_tokens().is_none()
        && usage.provider_total_tokens().is_none()
}

fn aggregate_usage(stages: &[StageResult]) -> Option<Usage> {
    let attempted = stages.iter().filter(|stage| stage.status != "skipped");
    if attempted.clone().next().is_none()
        || attempted.clone().any(|stage| match stage.usage.as_ref() {
            Some(usage) => is_empty_usage(usage),
            None => true,
        })
    {
        return None;
    }

    fn sum_known(stages: &[StageResult], get: fn(&Usage) -> Option<u64>) -> Option<u64> {
        let mut total = 0u64;
        let mut included = false;
        for stage in stages.iter().filter(|stage| stage.status != "skipped") {
            let usage = stage.usage.as_ref()?;
            let value = get(usage)?;
            total = total.checked_add(value)?;
            included = true;
        }
        included.then_some(total)
    }
    let usage = Usage::from_optional(
        sum_known(stages, Usage::input_tokens),
        sum_known(stages, Usage::output_tokens),
        sum_known(stages, Usage::reasoning_tokens),
    )
    .with_cache_read_tokens(sum_known(stages, Usage::cache_read_tokens))
    .with_cache_write_tokens(sum_known(stages, Usage::cache_write_tokens))
    .with_provider_total_tokens(sum_known(stages, Usage::provider_total_tokens));
    (!is_empty_usage(&usage)).then_some(usage)
}

fn aggregate_counter<T>(
    stages: &[StageResult],
    get: fn(&StageResult) -> Option<T>,
    add: fn(T, T) -> Option<T>,
) -> Option<T>
where
    T: Copy + Default,
{
    let mut total = T::default();
    let mut included = false;
    for stage in stages.iter().filter(|stage| stage.status != "skipped") {
        let value = get(stage)?;
        total = add(total, value)?;
        included = true;
    }
    included.then_some(total)
}

fn child_output(report: &minicore_runtime::LoopReport) -> (String, bool) {
    let mut output = String::new();
    for item in report.appended.iter() {
        let HistoryItem::Assistant(assistant) = item else {
            continue;
        };
        let text = assistant
            .content
            .iter()
            .filter_map(|part| match part {
                AssistantPart::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        // The last assistant response is the child's final output. Replace
        // even when its text projection is empty; retaining an earlier round
        // would leak stale output into chain `{previous}` substitution after
        // a reasoning-only or tool-call-only final response.
        output = text;
    }
    truncate_utf8(&output, MAX_OUTPUT_BYTES)
}

fn loop_failure_message(failure: &minicore_runtime::LoopFailure) -> &'static str {
    match failure.kind {
        minicore_runtime::LoopFailureKind::Prompt => "child prompt failed",
        minicore_runtime::LoopFailureKind::Model => "child model failed",
        minicore_runtime::LoopFailureKind::InvalidModelResponse => {
            "child model response was invalid"
        }
        minicore_runtime::LoopFailureKind::OutputLimit => "child model output limit reached",
        minicore_runtime::LoopFailureKind::Refused => "child model refused",
        minicore_runtime::LoopFailureKind::ContentFiltered => "child model response was filtered",
        minicore_runtime::LoopFailureKind::Policy => "child tool policy failed",
        minicore_runtime::LoopFailureKind::Interaction => "child interaction failed",
        minicore_runtime::LoopFailureKind::MaxToolRounds => "child tool round limit reached",
        minicore_runtime::LoopFailureKind::Internal => "child loop failed internally",
        _ => "child loop failed",
    }
}

fn validate_input(input: SubagentInput) -> Result<(SubagentMode, Vec<TaskInput>), ToolError> {
    let SubagentInput {
        model,
        reasoning,
        task,
        tasks,
        chain,
        cwd,
    } = input;
    let has_task = task.is_some();
    let has_tasks = tasks.is_some();
    let has_chain = chain.is_some();
    let count = usize::from(has_task) + usize::from(has_tasks) + usize::from(has_chain);
    if count != 1 {
        return Err(ToolError::InvalidInvocation);
    }
    if let Some(task) = task {
        let task = TaskInput {
            model,
            reasoning,
            task,
            cwd,
        };
        validate_task(&task)?;
        return Ok((SubagentMode::Single, vec![task]));
    }
    let (mode, mut tasks) = if let Some(tasks) = tasks {
        (SubagentMode::Parallel, tasks)
    } else {
        (SubagentMode::Chain, chain.expect("one mode is present"))
    };
    if tasks.is_empty() || tasks.len() > MAX_TASKS {
        return Err(ToolError::InvalidInvocation);
    }
    for task in &mut tasks {
        if task.model.is_none() {
            task.model = model.clone();
        }
        if task.reasoning.is_none() {
            task.reasoning = reasoning;
        }
        if task.cwd.is_none() {
            task.cwd = cwd.clone();
        }
        validate_task(task)?;
    }
    if mode == SubagentMode::Chain && !chain_fits_bound(&tasks) {
        return Err(ToolError::InvalidInvocation);
    }
    Ok((mode, tasks))
}

fn parse_subagent_input(value: &Value) -> Result<SubagentInput, ToolError> {
    let object = value.as_object().ok_or(ToolError::InvalidInvocation)?;
    for name in ["model", "reasoning", "task", "tasks", "chain", "cwd"] {
        if !object.contains_key(name) {
            return Err(ToolError::InvalidInvocation);
        }
    }
    for name in ["tasks", "chain"] {
        let Some(Value::Array(items)) = object.get(name) else {
            continue;
        };
        for item in items {
            let item = item.as_object().ok_or(ToolError::InvalidInvocation)?;
            for field in ["model", "reasoning", "task", "cwd"] {
                if !item.contains_key(field) {
                    return Err(ToolError::InvalidInvocation);
                }
            }
        }
    }
    serde_json::from_value(value.clone()).map_err(|_| ToolError::InvalidInvocation)
}

fn chain_fits_bound(tasks: &[TaskInput]) -> bool {
    const PLACEHOLDER: &str = "{previous}";
    tasks.iter().all(|task| {
        let occurrences = task.task.match_indices(PLACEHOLDER).count();
        let static_bytes = task
            .task
            .len()
            .saturating_sub(occurrences.saturating_mul(PLACEHOLDER.len()));
        static_bytes.saturating_add(occurrences.saturating_mul(MAX_OUTPUT_BYTES)) <= MAX_TASK_BYTES
    })
}

fn validate_task(task: &TaskInput) -> Result<(), ToolError> {
    validate_task_text(&task.task)?;
    if let Some(model) = &task.model {
        if model.trim().is_empty() || model.len() > MAX_MODEL_REFERENCE_BYTES {
            return Err(ToolError::InvalidInvocation);
        }
    }
    if let Some(cwd) = &task.cwd {
        validate_cwd(cwd)?;
    }
    Ok(())
}

fn validate_task_text(task: &str) -> Result<(), ToolError> {
    if task.trim().is_empty()
        || task.len() > MAX_TASK_BYTES
        || task
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(())
}

fn validate_cwd(cwd: &str) -> Result<(), ToolError> {
    if cwd.trim().is_empty()
        || cwd.len() > MAX_CWD_BYTES
        || cwd.contains('\0')
        || cwd.chars().any(char::is_control)
    {
        return Err(ToolError::InvalidInvocation);
    }
    if Path::new(cwd)
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(())
}

fn parse_reasoning_suffix(value: &str) -> Option<ReasoningPreference> {
    Some(match value {
        "auto" => ReasoningPreference::Auto,
        "disabled" => ReasoningPreference::Disabled,
        "low" => ReasoningPreference::Low,
        "medium" => ReasoningPreference::Medium,
        "high" => ReasoningPreference::High,
        "xhigh" => ReasoningPreference::XHigh,
        "max" => ReasoningPreference::Max,
        "ultra" => ReasoningPreference::Ultra,
        _ => return None,
    })
}

fn replace_previous(task: &str, previous: &str) -> Result<String, ()> {
    if !task.contains("{previous}") {
        return Ok(task.to_owned());
    }
    let mut result = String::new();
    let mut rest = task;
    while let Some(index) = rest.find("{previous}") {
        result.push_str(&rest[..index]);
        result.push_str(previous);
        if result.len() > MAX_TASK_BYTES {
            return Err(());
        }
        rest = &rest[index + "{previous}".len()..];
    }
    result.push_str(rest);
    if result.len() > MAX_TASK_BYTES {
        Err(())
    } else {
        Ok(result)
    }
}

fn truncate_utf8(value: &str, maximum: usize) -> (String, bool) {
    if value.len() <= maximum {
        return (value.to_owned(), false);
    }
    let mut end = maximum.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_owned(), true)
}

fn subagent_schema() -> Value {
    let reasoning = json!({
        "type": ["string", "null"],
        "enum": ["auto", "disabled", "low", "medium", "high", "xhigh", "max", "ultra", null]
    });
    let task_item = json!({
        "type": "object",
        "properties": {
            "model": {"type": ["string", "null"]},
            "reasoning": reasoning.clone(),
            "task": {"type": "string"},
            "cwd": {"type": ["string", "null"]}
        },
        "required": ["model", "reasoning", "task", "cwd"],
        "additionalProperties": false
    });
    json!({
        "type": "object",
        "properties": {
            "model": {"type": ["string", "null"]},
            "reasoning": reasoning,
            "task": {"type": ["string", "null"]},
            "tasks": {"type": ["array", "null"], "items": task_item.clone()},
            "chain": {"type": ["array", "null"], "items": task_item},
            "cwd": {"type": ["string", "null"]}
        },
        "required": ["model", "reasoning", "task", "tasks", "chain", "cwd"],
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::pending;
    use std::time::Duration;

    use super::*;
    use futures_util::stream;
    use minicore_runtime::ToolCallId;
    use minicore_runtime::model::{
        ModelCallContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason, ModelRef,
        ModelRequest, ModelStartFuture, ModelStream,
    };
    use minicore_runtime::prompt::DefaultPromptProvider;
    use minicore_runtime::tools::{
        Tool, ToolContext, ToolExecutionOutcome, ToolInvocation, ToolProgressSink, ToolSet,
    };

    struct PendingModel {
        descriptor: ModelDescriptor,
        started: Arc<tokio::sync::Notify>,
    }

    impl PendingModel {
        fn new(started: Arc<tokio::sync::Notify>) -> Self {
            Self {
                descriptor: ModelDescriptor::new(
                    "pending".parse::<ModelRef>().unwrap(),
                    16_384,
                    BTreeSet::from([ReasoningPreference::Auto]),
                    false,
                )
                .unwrap(),
                started,
            }
        }
    }

    impl Model for PendingModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn start(
            &self,
            _request: ModelRequest,
            _context: ModelCallContext,
        ) -> ModelStartFuture<'_> {
            let started = Arc::clone(&self.started);
            Box::pin(async move {
                started.notify_one();
                pending::<Result<ModelStream, ModelError>>().await
            })
        }
    }

    struct InteractionModel {
        descriptor: ModelDescriptor,
    }

    impl InteractionModel {
        fn new() -> Self {
            Self {
                descriptor: ModelDescriptor::new(
                    "interaction".parse::<ModelRef>().unwrap(),
                    16_384,
                    BTreeSet::from([ReasoningPreference::Auto]),
                    true,
                )
                .unwrap(),
            }
        }
    }

    impl Model for InteractionModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn start(
            &self,
            _request: ModelRequest,
            _context: ModelCallContext,
        ) -> ModelStartFuture<'_> {
            Box::pin(async move {
                let tool_call_id = ToolCallId::new("write-child-call").unwrap();
                let mut events: Vec<Result<ModelEvent, ModelError>> = (0..300)
                    .map(|_| Ok(ModelEvent::text_delta("x").unwrap()))
                    .collect();
                events.extend([
                    Ok(ModelEvent::ToolCallStart {
                        tool_call_id: tool_call_id.clone(),
                        tool_name: "write".parse().unwrap(),
                    }),
                    Ok(ModelEvent::tool_call_arguments_delta(
                        tool_call_id.clone(),
                        r#"{"path":"blocked.txt","content":"must not write"}"#,
                    )
                    .unwrap()),
                    Ok(ModelEvent::ToolCallEnd { tool_call_id }),
                    Ok(ModelEvent::Usage {
                        usage: Usage::new(1, 1, 0),
                    }),
                    Ok(ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    }),
                ]);
                Ok(Box::pin(stream::iter(events)) as ModelStream)
            })
        }
    }

    fn pending_task(options: LoopOptions, started: Arc<tokio::sync::Notify>) -> PreparedTask {
        let config = ExecutionConfig::new(
            Arc::new(PendingModel::new(started)),
            ReasoningPreference::Auto,
            ToolSet::default(),
            None,
            Arc::new(DefaultPromptProvider::new(None)),
        )
        .unwrap();
        PreparedTask {
            task: "pending task".to_owned(),
            model_id: "pending".to_owned(),
            reasoning: ReasoningPreference::Auto,
            cwd: "/workspace".to_owned(),
            config,
            options,
            owners: CommandOwners::new(),
        }
    }

    fn stage(
        status: &'static str,
        usage: Option<Usage>,
        requests: Option<u32>,
        tool_rounds: Option<u16>,
    ) -> StageResult {
        StageResult {
            stage_index: 1,
            status,
            model: "test".to_owned(),
            reasoning: ReasoningPreference::Auto,
            cwd: "/workspace".to_owned(),
            loop_id: None,
            output: String::new(),
            output_truncated: false,
            usage,
            requests,
            tool_rounds,
            error: None,
        }
    }

    fn output_stage(stage_index: usize, output: impl Into<String>) -> StageResult {
        StageResult {
            stage_index,
            status: "completed",
            model: "test".to_owned(),
            reasoning: ReasoningPreference::Auto,
            cwd: "/workspace".to_owned(),
            loop_id: None,
            output: output.into(),
            output_truncated: false,
            usage: Some(Usage::new(1, 1, 1)),
            requests: Some(1),
            tool_rounds: Some(1),
            error: None,
        }
    }

    #[test]
    fn schema_is_strict_and_nested_optional_fields_are_nullable() {
        let schema = subagent_schema();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"].as_array().unwrap().len(), 6);
        assert_eq!(
            schema["required"],
            json!(["model", "reasoning", "task", "tasks", "chain", "cwd"])
        );
        let item = &schema["properties"]["tasks"]["items"];
        assert_eq!(item["additionalProperties"], false);
        assert_eq!(
            item["required"],
            json!(["model", "reasoning", "task", "cwd"])
        );
        assert_eq!(
            item["properties"]["model"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(
            item["properties"]["reasoning"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(item["properties"]["cwd"]["type"], json!(["string", "null"]));
        assert_eq!(
            schema["properties"]["tasks"]["type"],
            json!(["array", "null"])
        );
        assert_eq!(
            schema["properties"]["chain"]["type"],
            json!(["array", "null"])
        );
    }

    #[test]
    fn schema_and_deserializer_reject_unknown_fields() {
        let schema = subagent_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert!(parse_subagent_input(&json!({"task": "one"})).is_err());
        assert!(
            parse_subagent_input(&json!({
                "model": null,
                "reasoning": null,
                "task": "one",
                "tasks": null,
                "chain": null,
                "cwd": null,
                "unexpected": true
            }))
            .is_err()
        );
        assert!(
            parse_subagent_input(&json!({
                "model": null,
                "reasoning": null,
                "task": "one",
                "tasks": null,
                "chain": null,
                "cwd": null
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<SubagentInput>(json!({
                "task": "one",
                "unexpected": true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<SubagentInput>(json!({
                "tasks": [{"task": "one", "unexpected": true}]
            }))
            .is_err()
        );
    }

    #[test]
    fn input_requires_exactly_one_nonempty_mode_and_bounds_tasks() {
        let parse = |value: Value| serde_json::from_value::<SubagentInput>(value).unwrap();
        assert!(validate_input(parse(json!({"task": "one"}))).is_ok());
        assert!(validate_input(parse(json!({"tasks": [{"task": "one"}]}))).is_ok());
        assert!(validate_input(parse(json!({"chain": [{"task": "one"}]}))).is_ok());
        assert!(validate_input(parse(json!({}))).is_err());
        assert!(validate_input(parse(json!({"task": "one", "tasks": []}))).is_err());
        assert!(validate_input(parse(json!({"tasks": []}))).is_err());
        assert!(validate_input(parse(json!({"task": ""}))).is_err());
        let too_many = (0..=MAX_TASKS)
            .map(|_| json!({"task": "one"}))
            .collect::<Vec<_>>();
        assert!(validate_input(parse(json!({"tasks": too_many}))).is_err());
        assert!(
            validate_input(parse(json!({
                "tasks": [{"model": null, "reasoning": null, "task": "one", "cwd": null}]
            })))
            .is_ok()
        );
    }

    #[test]
    fn model_suffix_and_chain_replacement_are_bounded() {
        assert_eq!(
            parse_reasoning_suffix("high"),
            Some(ReasoningPreference::High)
        );
        assert_eq!(parse_reasoning_suffix("unknown"), None);
        assert_eq!(
            replace_previous("a {previous} b", "answer").unwrap(),
            "a answer b"
        );
        let prefix = "prefix:";
        let task = format!("{prefix}{{previous}}");
        let below_limit = MAX_TASK_BYTES - prefix.len() - 1;
        let at_limit = MAX_TASK_BYTES - prefix.len();
        assert_eq!(
            replace_previous(&task, &"x".repeat(below_limit))
                .unwrap()
                .len(),
            MAX_TASK_BYTES - 1
        );
        assert_eq!(
            replace_previous(&task, &"x".repeat(at_limit))
                .unwrap()
                .len(),
            MAX_TASK_BYTES
        );
        assert!(replace_previous(&task, &"x".repeat(at_limit + 1)).is_err());
        assert!(validate_cwd("../outside").is_err());
        assert!(validate_cwd("inside/../outside").is_err());
        assert!(validate_cwd("nested/project").is_ok());
    }

    #[test]
    fn child_tools_intersect_parent_tools_without_recursive_delegation() {
        let names =
            child_tool_names(&["read".to_owned(), "subagent".to_owned(), "bash".to_owned()]);
        assert_eq!(names, vec!["read".to_owned(), "bash".to_owned()]);
    }

    #[test]
    fn usage_aggregation_propagates_unknown_fields_and_checked_overflow() {
        let first = stage(
            "completed",
            Some(Usage::from_optional(Some(2), None, Some(4)).with_cache_read_tokens(Some(1))),
            Some(2),
            Some(1),
        );
        let second = stage(
            "completed",
            Some(Usage::from_optional(Some(3), Some(5), Some(6)).with_cache_read_tokens(Some(2))),
            Some(3),
            Some(2),
        );
        let aggregate = aggregate_usage(&[first, second]).unwrap();
        assert_eq!(aggregate.input_tokens(), Some(5));
        assert_eq!(aggregate.output_tokens(), None);
        assert_eq!(aggregate.reasoning_tokens(), Some(10));
        assert_eq!(aggregate.cache_read_tokens(), Some(3));

        let overflow = aggregate_usage(&[
            stage(
                "completed",
                Some(Usage::from_optional(Some(u64::MAX), Some(2), Some(3))),
                Some(1),
                Some(1),
            ),
            stage(
                "completed",
                Some(Usage::from_optional(Some(1), Some(3), Some(4))),
                Some(1),
                Some(1),
            ),
        ])
        .unwrap();
        assert_eq!(overflow.input_tokens(), None);
        assert_eq!(overflow.output_tokens(), Some(5));
        assert_eq!(overflow.reasoning_tokens(), Some(7));

        let disjoint = [
            stage(
                "completed",
                Some(Usage::from_optional(Some(1), None, None)),
                Some(1),
                Some(1),
            ),
            stage(
                "completed",
                Some(Usage::from_optional(None, Some(2), None)),
                Some(1),
                Some(1),
            ),
        ];
        assert!(aggregate_usage(&disjoint).is_none());

        let only_overflowing_field = [
            stage(
                "completed",
                Some(Usage::from_optional(Some(u64::MAX), None, None)),
                Some(1),
                Some(1),
            ),
            stage(
                "completed",
                Some(Usage::from_optional(Some(1), None, None)),
                Some(1),
                Some(1),
            ),
        ];
        assert!(aggregate_usage(&only_overflowing_field).is_none());
    }

    #[test]
    fn empty_usage_is_null_and_unknown_attempted_counters_are_not_summed() {
        assert!(
            aggregate_usage(&[stage("completed", Some(Usage::default()), Some(0), Some(0),)])
                .is_none()
        );

        let unknown = stage("completed", Some(Usage::new(1, 1, 1)), None, Some(1));
        assert!(aggregate_counter(&[unknown], |stage| stage.requests, u32::checked_add).is_none());
        assert!(
            aggregate_counter(
                &[stage("completed", Some(Usage::new(1, 1, 1)), Some(2), None)],
                |stage| stage.tool_rounds,
                u16::checked_add,
            )
            .is_none()
        );
        assert_eq!(
            aggregate_counter(
                &[
                    stage("completed", None, Some(2), Some(1)),
                    stage("skipped", None, None, None),
                ],
                |stage| stage.requests,
                u32::checked_add,
            ),
            Some(2)
        );
    }

    #[test]
    fn aggregate_usage_requires_evidence_for_each_attempted_stage() {
        let known = stage("completed", Some(Usage::new(10, 20, 30)), Some(1), Some(1));
        for status in ["completed", "failed"] {
            let encoded = encode_result(
                SubagentMode::Parallel,
                vec![known.clone(), stage(status, None, None, None)],
            )
            .unwrap();
            let value: Value = serde_json::from_str(&encoded).unwrap();
            assert!(value["usage"].is_null(), "{status}: {value}");
        }

        let empty = encode_result(
            SubagentMode::Single,
            vec![stage("completed", Some(Usage::default()), Some(1), Some(1))],
        )
        .unwrap();
        let empty_value: Value = serde_json::from_str(&empty).unwrap();
        assert!(empty_value["stages"][0]["usage"].is_null());
        assert!(empty_value["usage"].is_null());

        let all_known = aggregate_usage(&[
            known,
            stage("completed", Some(Usage::new(30, 40, 50)), Some(1), Some(1)),
        ])
        .unwrap();
        assert_eq!(all_known.input_tokens(), Some(40));
        assert_eq!(all_known.output_tokens(), Some(60));
        assert_eq!(all_known.reasoning_tokens(), Some(80));
    }

    #[test]
    fn output_budget_preserves_uneven_short_stage_outputs() {
        let expected = ["first output", "second output", "third output"];
        let encoded = encode_result(
            SubagentMode::Chain,
            expected
                .iter()
                .enumerate()
                .map(|(index, output)| output_stage(index + 1, *output))
                .collect(),
        )
        .unwrap();
        let value: Value = serde_json::from_str(&encoded).unwrap();
        for (index, output) in expected.iter().enumerate() {
            assert_eq!(value["stages"][index]["output"], *output);
            assert_eq!(value["stages"][index]["output_truncated"], false);
        }
    }

    #[test]
    fn output_budget_handles_large_utf8_and_json_escaped_outputs() {
        let pattern = "é\"\\\n🙂";
        let under = pattern.repeat(MAX_OUTPUT_BYTES / pattern.len() - 1);
        assert!(under.len() < MAX_OUTPUT_BYTES);
        let under_encoded =
            encode_result(SubagentMode::Single, vec![output_stage(1, under.as_str())]).unwrap();
        let under_value: Value = serde_json::from_str(&under_encoded).unwrap();
        assert_eq!(under_value["stages"][0]["output"], under);
        assert_eq!(under_value["stages"][0]["output_truncated"], false);

        let over = pattern.repeat(MAX_OUTPUT_BYTES / pattern.len() + 2);
        let (expected, cut) = truncate_utf8(&over, MAX_OUTPUT_BYTES);
        assert!(cut);
        let over_encoded =
            encode_result(SubagentMode::Single, vec![output_stage(1, over.as_str())]).unwrap();
        let over_value: Value = serde_json::from_str(&over_encoded).unwrap();
        assert_eq!(over_value["stages"][0]["output"], expected);
        assert_eq!(over_value["stages"][0]["output_truncated"], true);
        assert!(over_encoded.len() <= MAX_DETAILS_BYTES);
    }

    #[test]
    fn aggregate_status_reports_failed_stages_and_bounds_output() {
        let stage = StageResult {
            stage_index: 1,
            status: "failed",
            model: "main".to_owned(),
            reasoning: ReasoningPreference::Auto,
            cwd: "/workspace".to_owned(),
            loop_id: None,
            output: "failure".to_owned(),
            output_truncated: false,
            usage: None,
            requests: None,
            tool_rounds: None,
            error: Some("child failed"),
        };
        let encoded = encode_result(SubagentMode::Single, vec![stage]).unwrap();
        let value: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["status"], "failed");
        assert_eq!(value["stages"][0]["status"], "failed");
        assert!(value["stages"][0]["usage"].is_null());
        assert!(value["stages"][0]["requests"].is_null());
        assert!(value["stages"][0]["tool_rounds"].is_null());
        assert!(value["usage"].is_null());
        assert!(value["requests"].is_null());
        assert!(value["tool_rounds"].is_null());
        assert!(encoded.len() <= MAX_OUTPUT_BYTES);
    }

    #[test]
    fn aggregate_status_is_partial_with_known_completed_stage_data() {
        let completed = StageResult {
            stage_index: 1,
            status: "completed",
            model: "main".to_owned(),
            reasoning: ReasoningPreference::Auto,
            cwd: "/workspace".to_owned(),
            loop_id: None,
            output: "done".to_owned(),
            output_truncated: false,
            usage: Some(Usage::default()),
            requests: Some(2),
            tool_rounds: Some(1),
            error: None,
        };
        let skipped = StageResult {
            stage_index: 2,
            status: "skipped",
            model: "main".to_owned(),
            reasoning: ReasoningPreference::Auto,
            cwd: "/workspace".to_owned(),
            loop_id: None,
            output: String::new(),
            output_truncated: false,
            usage: None,
            requests: None,
            tool_rounds: None,
            error: Some("previous chain stage failed"),
        };
        let encoded = encode_result(SubagentMode::Chain, vec![completed, skipped]).unwrap();
        let value: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["status"], "partial");
        assert_eq!(value["requests"], 2);
        assert_eq!(value["tool_rounds"], 1);
        assert!(value["stages"][0]["usage"].is_null());
        assert!(value["stages"][1]["requests"].is_null());
    }

    #[test]
    fn partial_status_keeps_attempted_unknown_totals_null() {
        let encoded = encode_result(
            SubagentMode::Parallel,
            vec![
                stage("completed", Some(Usage::new(1, 1, 1)), Some(2), Some(1)),
                stage("failed", None, None, None),
            ],
        )
        .unwrap();
        let value: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["status"], "partial");
        assert!(value["requests"].is_null());
        assert!(value["tool_rounds"].is_null());
        assert_eq!(value["stages"][0]["requests"], 2);
        assert!(value["stages"][1]["requests"].is_null());
    }

    #[test]
    fn child_output_uses_the_latest_assistant_response() {
        let loop_id = minicore_runtime::LoopId::new().unwrap();
        let model = "fake".parse::<ModelRef>().unwrap();
        let reasoning = minicore_runtime::model::ReasoningContent::new(
            Some("thinking".to_owned()),
            None,
            None,
            None,
        )
        .unwrap();
        let report = minicore_runtime::LoopReport {
            loop_id,
            outcome: LoopOutcome::Completed,
            appended: vec![
                HistoryItem::Assistant(minicore_runtime::AssistantHistory {
                    loop_id,
                    request_index: 0,
                    model: model.clone(),
                    reasoning: ReasoningPreference::Auto,
                    content: vec![AssistantPart::Text("stale output".to_owned())],
                    finish_reason: minicore_runtime::model::ModelFinishReason::Stop,
                    usage: Usage::default(),
                }),
                HistoryItem::Assistant(minicore_runtime::AssistantHistory {
                    loop_id,
                    request_index: 1,
                    model,
                    reasoning: ReasoningPreference::Auto,
                    content: vec![AssistantPart::Reasoning(reasoning)],
                    finish_reason: minicore_runtime::model::ModelFinishReason::Stop,
                    usage: Usage::default(),
                }),
            ]
            .into(),
            usage: Usage::default(),
            requests: 2,
            tool_rounds: 1,
            final_config_revision: minicore_runtime::ConfigRevision::INITIAL,
        };
        assert_eq!(child_output(&report), (String::new(), false));
    }

    #[tokio::test]
    async fn native_child_interaction_uses_state_when_request_event_is_dropped() {
        let session_id = SessionId::new().unwrap();
        let base =
            std::env::temp_dir().join(format!("minicore-agent-subagent-interaction-{session_id}"));
        let _ = tokio::fs::remove_dir_all(&base).await;
        tokio::fs::create_dir_all(&base).await.unwrap();
        let workspace = Arc::new(Workspace::open(base.clone()).await.unwrap());
        let service = Arc::new(SubagentService::new());
        let model: Arc<dyn Model> = Arc::new(InteractionModel::new());
        let models = Arc::new(Models::from_values(BTreeMap::from([(
            "interaction".to_owned(),
            model,
        )])));
        let mut options = LoopOptions::default_checked().unwrap();
        // The fake model emits a synchronous burst before requesting approval,
        // filling this best-effort event channel so InteractionRequested may be
        // absent while the authoritative watch state remains available.
        options.event_capacity = 1;
        let factory = SubagentFactory {
            service: Arc::clone(&service),
            session_id,
            parent_workspace: Arc::clone(&workspace),
            models,
            command_environment: CommandEnvironment::new(std::iter::empty()),
            system_prompt: "test system prompt".to_owned(),
            parent_tools: vec!["write".to_owned()],
            approval: ApprovalMode::Ask,
            options,
            default_model: "interaction".to_owned(),
            default_reasoning: ReasoningPreference::Auto,
        };
        let tool = SubagentTool::new(factory);
        let invocation = ToolInvocation::new(
            ToolCallId::new("subagent-call").unwrap(),
            TOOL_NAME.parse().unwrap(),
            json!({
                "model": null,
                "reasoning": null,
                "task": "ask the child to write a file",
                "tasks": null,
                "chain": null,
                "cwd": null
            }),
        )
        .unwrap();
        let context = ToolContext {
            cancellation: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            progress: ToolProgressSink::default(),
        };
        let result =
            tokio::time::timeout(Duration::from_secs(1), tool.execute(invocation, context))
                .await
                .expect("child interaction must fail promptly")
                .unwrap();
        let ToolExecutionOutcome::Completed(output) = result else {
            panic!("subagent interaction should be represented in stage output");
        };
        let details: Value = serde_json::from_str(output.content().as_str()).unwrap();
        assert_eq!(details["status"], "failed");
        assert_eq!(details["stages"][0]["status"], "failed");
        assert_eq!(
            details["stages"][0]["error"],
            "child interaction requires approval or input"
        );
        assert!(!base.join("blocked.txt").exists());
        assert!(service.entries_for(None, None).is_empty());
        let _ = tokio::fs::remove_dir_all(base).await;
    }

    #[tokio::test]
    async fn child_stage_cancellation_returns_cancelled() {
        let started = Arc::new(tokio::sync::Notify::new());
        let cancellation = CancellationToken::new();
        let task = pending_task(
            LoopOptions::default_checked().unwrap(),
            Arc::clone(&started),
        );
        let worker = tokio::spawn(run_child(
            task,
            1,
            Instant::now() + Duration::from_secs(1),
            cancellation.clone(),
            ToolProgressSink::default(),
        ));
        started.notified().await;
        cancellation.cancel();
        assert!(matches!(
            worker.await.unwrap(),
            Err(SubagentWorkerError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn child_stage_timeout_returns_failed_without_usage() {
        let started = Arc::new(tokio::sync::Notify::new());
        let mut options = LoopOptions::default_checked().unwrap();
        options.model_timeout = Duration::from_millis(1);
        let task = pending_task(options, Arc::clone(&started));
        let result = run_child(
            task,
            1,
            Instant::now() + Duration::from_secs(1),
            CancellationToken::new(),
            ToolProgressSink::default(),
        )
        .await
        .unwrap();
        assert_eq!(result.status, "failed");
        assert_eq!(result.error, Some("child model failed"));
        assert!(result.usage.is_none());
        assert_eq!(result.requests, Some(1));
        assert_eq!(result.tool_rounds, Some(0));
    }

    #[tokio::test]
    async fn cancelling_one_child_does_not_cancel_a_sibling() {
        let service = Arc::new(SubagentService::new());
        let session_id: SessionId = "ses_00000000000000000000000000000006".parse().unwrap();
        let first = service.start(
            session_id,
            CancellationToken::new(),
            |cancellation| async move {
                cancellation.cancelled().await;
                Err::<(), SubagentWorkerError>(SubagentWorkerError::Cancelled)
            },
        );
        let second = service.start(
            session_id,
            CancellationToken::new(),
            |cancellation| async move {
                cancellation.cancelled().await;
                Err::<(), SubagentWorkerError>(SubagentWorkerError::Cancelled)
            },
        );

        service.cancel_ids(&[first.0]);
        let entries = service.entries_for(None, Some(&[first.0, second.0]));
        assert!(
            entries
                .iter()
                .find(|entry| entry.id == first.0)
                .is_some_and(|entry| entry.cancellation.is_cancelled())
        );
        assert!(
            entries
                .iter()
                .find(|entry| entry.id == second.0)
                .is_some_and(|entry| !entry.cancellation.is_cancelled())
        );

        service.drain_all().await;
        assert_eq!(first.1.await.unwrap(), Err(SubagentWorkerError::Cancelled));
        assert_eq!(second.1.await.unwrap(), Err(SubagentWorkerError::Cancelled));
    }

    #[tokio::test]
    async fn dropping_child_scope_cancels_without_waiting_for_join() {
        let service = Arc::new(SubagentService::new());
        let session_id: SessionId = "ses_00000000000000000000000000000003".parse().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let worker_started = Arc::clone(&started);
        let worker_cancelled = Arc::clone(&cancelled);
        let worker_release = Arc::clone(&release);
        let (id, receiver) = service.start(
            session_id,
            CancellationToken::new(),
            move |cancellation| async move {
                worker_started.notify_one();
                cancellation.cancelled().await;
                worker_cancelled.notify_one();
                worker_release.notified().await;
                Err::<(), SubagentWorkerError>(SubagentWorkerError::Cancelled)
            },
        );
        started.notified().await;

        let scope = Arc::new(ChildScope::new(Arc::clone(&service)));
        scope.track(id);
        drop(scope);
        cancelled.notified().await;
        assert!(service.contains_id(id));

        let draining_service = Arc::clone(&service);
        let drain = tokio::spawn(async move {
            draining_service.drain_session(session_id).await;
        });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        release.notify_one();
        drain.await.unwrap();
        assert!(!service.contains_id(id));
        assert_eq!(receiver.await.unwrap(), Err(SubagentWorkerError::Cancelled));
    }

    #[tokio::test]
    async fn child_service_drain_all_joins_workers_from_all_sessions() {
        let service = Arc::new(SubagentService::new());
        let session_ids: [SessionId; 2] = [
            "ses_00000000000000000000000000000004".parse().unwrap(),
            "ses_00000000000000000000000000000005".parse().unwrap(),
        ];
        let mut children = Vec::new();
        for session_id in session_ids {
            children.push(service.start(
                session_id,
                CancellationToken::new(),
                move |cancellation| async move {
                    cancellation.cancelled().await;
                    Err::<(), SubagentWorkerError>(SubagentWorkerError::Cancelled)
                },
            ));
        }
        service.drain_all().await;
        for (id, receiver) in children {
            assert!(!service.contains_id(id));
            assert_eq!(receiver.await.unwrap(), Err(SubagentWorkerError::Cancelled));
        }
    }

    #[tokio::test]
    async fn child_service_cancels_and_joins_session_workers() {
        let service = Arc::new(SubagentService::new());
        let session_id: SessionId = "ses_00000000000000000000000000000001".parse().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let worker_started = Arc::clone(&started);
        let (_id, receiver) = service.start(
            session_id,
            CancellationToken::new(),
            move |cancellation| async move {
                worker_started.notify_one();
                cancellation.cancelled().await;
                Err::<(), SubagentWorkerError>(SubagentWorkerError::Cancelled)
            },
        );
        started.notified().await;
        service.drain_session(session_id).await;
        assert_eq!(receiver.await.unwrap(), Err(SubagentWorkerError::Cancelled));
    }

    #[tokio::test]
    async fn cancelled_scope_join_keeps_registry_until_a_later_drain() {
        let service = Arc::new(SubagentService::new());
        let session_id: SessionId = "ses_00000000000000000000000000000002".parse().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let worker_started = Arc::clone(&started);
        let worker_cancelled = Arc::clone(&cancelled);
        let worker_release = Arc::clone(&release);
        let (id, receiver) = service.start(
            session_id,
            CancellationToken::new(),
            move |cancellation| async move {
                worker_started.notify_one();
                cancellation.cancelled().await;
                worker_cancelled.notify_one();
                worker_release.notified().await;
                Err::<(), SubagentWorkerError>(SubagentWorkerError::Cancelled)
            },
        );
        started.notified().await;

        let scope = Arc::new(ChildScope::new(Arc::clone(&service)));
        scope.track(id);
        let joining_scope = Arc::clone(&scope);
        let join = tokio::spawn(async move {
            joining_scope.join(true).await;
        });
        cancelled.notified().await;
        tokio::task::yield_now().await;
        join.abort();
        assert!(join.await.is_err());
        assert!(service.contains_id(id));
        assert!(
            scope
                .ids
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(&id)
        );

        let draining_service = Arc::clone(&service);
        let drain = tokio::spawn(async move {
            draining_service.drain_session(session_id).await;
        });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        release.notify_one();
        drain.await.unwrap();
        assert!(!service.contains_id(id));

        scope.join(false).await;
        assert!(
            scope
                .ids
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
        assert_eq!(receiver.await.unwrap(), Err(SubagentWorkerError::Cancelled));
    }
}
