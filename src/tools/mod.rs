mod apply_patch;
mod bash;
pub(crate) mod command;
mod edit;
mod read;
mod write;

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use minicore_runtime::tools::{ToolError, ToolSet, ToolSetBuilder};
use thiserror::Error;

use crate::changes::{
    ChangeCommitState, ChangeCoverage, ChangeKind, ChangeRevision, FileChange, content_revision,
    metadata_revision,
};
use crate::presentation::{Presentation, PresentationTool};
use crate::subagents::{SubagentFactory, SubagentTool};
use crate::tool_data::{ToolData, ToolRef};
use crate::workspace::{
    AtomicWriteObservation, ExpectedFileState, FileCapture, FileStamp, FileState,
};
use crate::{Workspace, WorkspaceError};

use apply_patch::ApplyPatchTool;
pub(crate) use apply_patch::ApplyPatchTool as NativeApplyPatchTool;
use bash::BashTool;
pub(crate) use bash::{BashTool as OwnedBashTool, CommandEnvironment};
use edit::EditTool;
pub(crate) use edit::EditTool as NativeEditTool;
use read::ReadTool;
use write::WriteTool;
pub(crate) use write::WriteTool as NativeWriteTool;

pub(crate) const MAX_READ_BYTES: usize = 512 * 1024;
pub(crate) const MAX_WRITE_BYTES: usize = 512 * 1024;
pub(crate) const MAX_PATCH_BYTES: usize = 512 * 1024;
pub(crate) const MAX_COMMAND_OUTPUT: usize = 1024 * 1024;
pub(crate) const DEFAULT_COMMAND_TIMEOUT: u64 = 120;
pub(crate) const MAX_COMMAND_TIMEOUT: u64 = 1_800;
pub(crate) const DEFAULT_READ_OFFSET: usize = 1;
pub(crate) const DEFAULT_READ_LIMIT: usize = 400;
pub(crate) const MAX_READ_LINES: usize = 2_000;
pub(crate) const MAX_DIRECTORY_ENTRIES: usize = 1_000;
pub(crate) const KNOWN_TOOL_NAMES: &[&str] =
    &["read", "write", "edit", "apply_patch", "bash", "subagent"];

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BuildToolsError {
    #[error("tool configuration is invalid")]
    InvalidConfiguration,
    #[error("tool registration failed internally")]
    Internal,
}

#[cfg(test)]
pub(crate) fn build_tools(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
) -> Result<ToolSet, BuildToolsError> {
    build_tools_with(names, workspace, command_environment, None, None)
}

pub(crate) fn build_tools_for_child_with_presentation(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: &Arc<Presentation>,
) -> Result<ToolSet, BuildToolsError> {
    build_tools_with(
        names,
        workspace,
        command_environment,
        Some(presentation),
        None,
    )
}

/// Builds the parent Session tool set, optionally enabling the native
/// stateless child-loop Tool. The option is deliberately explicit: a profile
/// that does not name `subagent` never receives it implicitly.
pub(crate) fn build_tools_with_presentation_and_subagent(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: &Arc<Presentation>,
    subagent: Option<&SubagentFactory>,
) -> Result<ToolSet, BuildToolsError> {
    build_tools_with(
        names,
        workspace,
        command_environment,
        Some(presentation),
        subagent,
    )
}

fn build_tools_with(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: Option<&Arc<Presentation>>,
    subagent: Option<&SubagentFactory>,
) -> Result<ToolSet, BuildToolsError> {
    let mut builder = ToolSet::builder();
    let mut seen = BTreeSet::new();
    for name in names {
        if !seen.insert(name.as_str()) {
            return Err(BuildToolsError::InvalidConfiguration);
        }
        let register = |builder: &mut ToolSetBuilder,
                        tool: Arc<dyn minicore_runtime::tools::Tool>| {
            if let Some(presentation) = presentation {
                builder.register_arc(PresentationTool::new(tool, Arc::clone(presentation)));
            } else {
                builder.register_arc(tool);
            }
        };
        match name.as_str() {
            "apply_patch" => {
                let tool = Arc::new(ApplyPatchTool::new(Arc::clone(&workspace)));
                if let Some(presentation) = presentation {
                    builder.register_arc(PresentationTool::new_apply_patch(
                        tool,
                        Arc::clone(presentation),
                    ));
                } else {
                    builder.register_arc(tool);
                }
            }
            "bash" => {
                let tool = Arc::new(BashTool::with_binding(
                    Arc::clone(&workspace),
                    command_environment.clone(),
                    presentation.map(|presentation| presentation.command_binding()),
                ));
                match presentation {
                    Some(presentation) => {
                        // Bash and native file tools receive captured identity
                        // through explicit wrapper variants.
                        builder.register_arc(PresentationTool::new_bash(
                            Arc::clone(&tool),
                            Arc::clone(presentation),
                        ));
                    }
                    None => {
                        builder.register_arc(tool);
                    }
                }
            }
            "edit" => {
                let tool = Arc::new(EditTool::new(Arc::clone(&workspace)));
                if let Some(presentation) = presentation {
                    builder
                        .register_arc(PresentationTool::new_edit(tool, Arc::clone(presentation)));
                } else {
                    builder.register_arc(tool);
                }
            }
            "read" => {
                register(
                    &mut builder,
                    Arc::new(ReadTool::new(Arc::clone(&workspace))),
                );
            }
            "write" => {
                let tool = Arc::new(WriteTool::new(Arc::clone(&workspace)));
                if let Some(presentation) = presentation {
                    builder
                        .register_arc(PresentationTool::new_write(tool, Arc::clone(presentation)));
                } else {
                    builder.register_arc(tool);
                }
            }
            "subagent" => {
                let Some(subagent) = subagent else {
                    return Err(BuildToolsError::InvalidConfiguration);
                };
                register(&mut builder, Arc::new(SubagentTool::new(subagent.clone())));
            }
            _ => return Err(BuildToolsError::InvalidConfiguration),
        }
    }
    builder.build().map_err(|_| BuildToolsError::Internal)
}

pub(super) fn escape_control_characters(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control() {
            output.extend(character.escape_default());
        } else {
            output.push(character);
        }
    }
    output
}

pub(super) fn precheck_control(
    context: &minicore_runtime::tools::ToolContext,
) -> Result<(), ToolError> {
    if context.cancellation.is_cancelled() {
        return Err(ToolError::Cancelled);
    }
    if Instant::now() >= context.deadline {
        return Err(ToolError::TimedOut);
    }
    Ok(())
}

/// Emits one real execution phase through the Runtime `ToolContext.progress`.
/// The phase is a static stage name, never raw stdout/stderr or file content;
/// streamed bytes have their own typed channel (P5). Best-effort: a full
/// progress queue is ignored.
pub(super) fn emit_phase(context: &minicore_runtime::tools::ToolContext, phase: &'static str) {
    let message = minicore_runtime::value::BoundedText::new(phase)
        .expect("static phase name fits the bounded text limit");
    let _ = context.progress.emit(
        minicore_runtime::tools::ToolProgress::new(Some(message), None, None)
            .expect("phase progress carries no completed/total pair"),
    );
}

pub(super) async fn run_controlled<T, F>(
    context: &minicore_runtime::tools::ToolContext,
    future: F,
) -> Result<T, ToolError>
where
    F: Future<Output = Result<T, ToolError>>,
{
    precheck_control(context)?;
    let deadline = tokio::time::Instant::from_std(context.deadline);
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(ToolError::Cancelled),
        _ = tokio::time::sleep_until(deadline) => Err(ToolError::TimedOut),
        result = future => result,
    }
}

pub(super) fn map_workspace_error(error: WorkspaceError) -> ToolError {
    match error {
        WorkspaceError::InvalidPath | WorkspaceError::Escape | WorkspaceError::TooLarge => {
            ToolError::InvalidInvocation
        }
        WorkspaceError::NotFound
        | WorkspaceError::NotFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::Unavailable
        | WorkspaceError::UnknownOutcome
        | WorkspaceError::Binary => ToolError::Failed,
    }
}

pub(crate) enum FileBefore {
    Missing,
    Content {
        bytes: Vec<u8>,
        stamp: FileStamp,
        stable: bool,
    },
    Metadata(FileStamp),
    Unknown,
}

pub(crate) fn file_before_from_capture(capture: FileCapture) -> FileBefore {
    match capture {
        FileCapture::Missing => FileBefore::Missing,
        FileCapture::Complete(snapshot) => FileBefore::Content {
            bytes: snapshot.bytes,
            stamp: snapshot.stamp,
            stable: snapshot.stable,
        },
        FileCapture::TooLarge(stamp) => FileBefore::Metadata(stamp),
        FileCapture::Error(_) => FileBefore::Unknown,
    }
}

pub(crate) fn expected_file_state(before: &FileBefore) -> Option<ExpectedFileState> {
    match before {
        FileBefore::Missing => Some(ExpectedFileState::Missing),
        FileBefore::Content { stamp, .. } | FileBefore::Metadata(stamp) => {
            Some(ExpectedFileState::Present(*stamp))
        }
        FileBefore::Unknown => None,
    }
}

pub(crate) fn record_file_change(
    tool_data: Option<&ToolData>,
    tool_ref: Option<&ToolRef>,
    path: &str,
    before: FileBefore,
    after_bytes: &[u8],
    observation: &AtomicWriteObservation,
) {
    let (Some(tool_data), Some(tool_ref)) = (tool_data, tool_ref) else {
        return;
    };
    let (before, before_captured, before_bytes, stable, before_unknown) = match before {
        FileBefore::Missing => (ChangeRevision::Missing, true, None, true, false),
        FileBefore::Content {
            bytes,
            stamp: _,
            stable,
        } => (content_revision(&bytes), true, Some(bytes), stable, false),
        FileBefore::Metadata(stamp) => (
            metadata_revision(stamp.len(), stamp.modified_unix_ms()),
            false,
            None,
            true,
            false,
        ),
        FileBefore::Unknown => (ChangeRevision::Unknown, false, None, false, true),
    };
    let renamed = observation.renamed;
    let after = if renamed {
        content_revision(after_bytes)
    } else {
        ChangeRevision::Unknown
    };
    let after_missing = matches!(observation.after, FileState::Missing);
    let unknown_facts = before_unknown
        || observation.expected_unknown
        || matches!(observation.before, FileState::Unknown)
        || (renamed && matches!(observation.after, FileState::Unknown));
    let conflict = !unknown_facts
        && ((!stable && !before_unknown)
            || observation.expected_matches == Some(false)
            || (renamed && after_missing));
    let commit_state = match &observation.outcome {
        Err(WorkspaceError::UnknownOutcome) => ChangeCommitState::Unknown,
        Err(_) if !renamed && observation.expected_matches == Some(false) => {
            ChangeCommitState::Conflict
        }
        Err(_) if !renamed => ChangeCommitState::NotCommitted,
        Err(_) => ChangeCommitState::Unknown,
        Ok(()) if unknown_facts => ChangeCommitState::Unknown,
        Ok(()) if conflict => ChangeCommitState::Conflict,
        Ok(()) => ChangeCommitState::Applied,
    };
    let coverage = if !renamed {
        ChangeCoverage::Unavailable
    } else if unknown_facts
        || !before_captured
        || !stable
        || conflict
        || commit_state != ChangeCommitState::Applied
    {
        if matches!(&before, ChangeRevision::Unknown) {
            ChangeCoverage::Unavailable
        } else {
            ChangeCoverage::Partial
        }
    } else {
        ChangeCoverage::Complete
    };
    let change = FileChange {
        path: path.to_owned(),
        kind: match &before {
            ChangeRevision::Missing => ChangeKind::Added,
            ChangeRevision::Unknown => ChangeKind::Unknown,
            _ => ChangeKind::Modified,
        },
        before,
        after,
        commit_state,
        coverage,
        before_captured,
        after_captured: renamed,
        before_bytes,
        after_bytes: renamed.then(|| after_bytes.to_vec()),
        before_corrupt: false,
        after_corrupt: false,
    };
    tool_data.note_file_change(tool_ref, change);
}

#[cfg(test)]
pub(crate) struct ToolIoGate {
    tool_name: &'static str,
    path: String,
    pub(crate) started: Arc<tokio::sync::Semaphore>,
    pub(crate) release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl ToolIoGate {
    pub(crate) fn new(tool_name: &'static str, path: impl Into<String>) -> Self {
        Self {
            tool_name,
            path: path.into(),
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
static IO_GATES: OnceLock<Mutex<Vec<Arc<ToolIoGate>>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn block_next_io(gate: Arc<ToolIoGate>) {
    IO_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(gate);
}

pub(super) async fn wait_for_test_io(tool_name: &'static str, path: &str) {
    #[cfg(test)]
    {
        let gate = {
            let mut gates = IO_GATES
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap();
            gates
                .iter()
                .position(|gate| gate.tool_name == tool_name && gate.path == path)
                .map(|position| gates.remove(position))
        };
        if let Some(gate) = gate {
            gate.started.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
    }
    #[cfg(not(test))]
    let _ = (tool_name, path);
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use super::*;

    async fn workspace(label: &str) -> (PathBuf, Arc<Workspace>) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-tools-build-{label}-{}",
            crate::ids::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        (base, Arc::new(Workspace::open(root).await.unwrap()))
    }

    #[tokio::test]
    async fn build_tools_registers_exact_subsets_and_rejects_unknown_or_duplicate_names() {
        let (base, workspace) = workspace("subset").await;
        let apply_patch = "apply_patch".parse().unwrap();
        let bash = "bash".parse().unwrap();
        let edit = "edit".parse().unwrap();
        let read = "read".parse().unwrap();
        let subagent = "subagent".parse().unwrap();
        let write = "write".parse().unwrap();
        let command_environment = CommandEnvironment::new(std::iter::empty::<OsString>());

        let empty = build_tools(&[], Arc::clone(&workspace), command_environment.clone()).unwrap();
        assert!(!empty.contains(&apply_patch));
        assert!(!empty.contains(&bash));
        assert!(!empty.contains(&edit));
        assert!(!empty.contains(&read));
        assert!(!empty.contains(&subagent));
        assert!(!empty.contains(&write));

        assert_eq!(
            KNOWN_TOOL_NAMES,
            &["read", "write", "edit", "apply_patch", "bash", "subagent"]
        );

        for mask in 0..(1usize << KNOWN_TOOL_NAMES.len()) {
            let names = KNOWN_TOOL_NAMES
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1usize << index) != 0)
                .map(|(_, name)| (*name).to_owned())
                .collect::<Vec<_>>();
            if names.iter().any(|name| name == "subagent") {
                assert_eq!(
                    build_tools(&names, Arc::clone(&workspace), command_environment.clone()).err(),
                    Some(BuildToolsError::InvalidConfiguration)
                );
                continue;
            }
            let tools =
                build_tools(&names, Arc::clone(&workspace), command_environment.clone()).unwrap();
            for (index, name) in KNOWN_TOOL_NAMES.iter().enumerate() {
                let name = name.parse().unwrap();
                assert_eq!(tools.contains(&name), mask & (1usize << index) != 0);
            }
        }

        assert_eq!(
            build_tools(
                &["unknown".to_owned()],
                Arc::clone(&workspace),
                command_environment.clone(),
            )
            .err(),
            Some(BuildToolsError::InvalidConfiguration)
        );
        for name in KNOWN_TOOL_NAMES {
            assert_eq!(
                build_tools(
                    &[(*name).to_owned(), (*name).to_owned()],
                    Arc::clone(&workspace),
                    command_environment.clone(),
                )
                .err(),
                Some(BuildToolsError::InvalidConfiguration)
            );
        }
        let _ = tokio::fs::remove_dir_all(base).await;
    }

    #[test]
    fn unknown_before_facts_are_not_reported_as_a_modified_file() {
        let data = ToolData::new();
        let tool_ref = ToolRef {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
            request_index: 0,
            tool_call_id: "unknown-before".parse().unwrap(),
        };
        data.note_requested(&tool_ref, "write");
        record_file_change(
            Some(&data),
            Some(&tool_ref),
            "value.txt",
            FileBefore::Unknown,
            b"after",
            &AtomicWriteObservation {
                outcome: Ok(()),
                before: FileState::Unknown,
                after: FileState::Unknown,
                renamed: true,
                expected_matches: None,
                expected_unknown: false,
            },
        );
        let record = data
            .file_change_records(tool_ref.session_id, Some(tool_ref.loop_id))
            .pop()
            .unwrap();
        assert_eq!(record.kind, ChangeKind::Unknown);
        assert_eq!(record.commit_state, ChangeCommitState::Unknown);
        assert_eq!(record.coverage, ChangeCoverage::Unavailable);
        assert!(!record.details_available);
    }

    #[test]
    fn command_environment_merge_keeps_previous_and_candidate_names() {
        let previous = CommandEnvironment::new([
            OsString::from("MINICORE_RELOAD_UNIT_KEY_A"),
            OsString::from("MINICORE_RELOAD_UNIT_KEY_B"),
        ]);
        let candidate = CommandEnvironment::new([
            OsString::from("MINICORE_RELOAD_UNIT_KEY_B"),
            OsString::from("MINICORE_RELOAD_UNIT_KEY_C"),
        ]);
        let merged = previous.extended(candidate.names().iter().cloned());
        assert_eq!(
            merged.names().to_vec(),
            vec![
                OsString::from("MINICORE_RELOAD_UNIT_KEY_A"),
                OsString::from("MINICORE_RELOAD_UNIT_KEY_B"),
                OsString::from("MINICORE_RELOAD_UNIT_KEY_C"),
            ]
        );
    }
}
