mod apply_patch;
mod bash;
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

use crate::presentation::{Presentation, PresentationTool};
use crate::{Workspace, WorkspaceError};

use apply_patch::ApplyPatchTool;
use bash::BashTool;
pub(crate) use bash::CommandEnvironment;
use edit::EditTool;
use read::ReadTool;
use write::WriteTool;

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
pub(crate) const KNOWN_TOOL_NAMES: &[&str] = &["read", "write", "edit", "apply_patch", "bash"];

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
    build_tools_with(names, workspace, command_environment, None)
}

/// Builds the tool set with the per-session presentation wrappers active.
/// The wrappers are display-only and never change tool execution semantics.
pub(crate) fn build_tools_with_presentation(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: &Arc<Presentation>,
) -> Result<ToolSet, BuildToolsError> {
    build_tools_with(names, workspace, command_environment, Some(presentation))
}

fn build_tools_with(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: Option<&Arc<Presentation>>,
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
                register(
                    &mut builder,
                    Arc::new(ApplyPatchTool::new(Arc::clone(&workspace))),
                );
            }
            "bash" => {
                register(
                    &mut builder,
                    Arc::new(BashTool::new(
                        Arc::clone(&workspace),
                        command_environment.clone(),
                    )),
                );
            }
            "edit" => {
                register(
                    &mut builder,
                    Arc::new(EditTool::new(Arc::clone(&workspace))),
                );
            }
            "read" => {
                register(
                    &mut builder,
                    Arc::new(ReadTool::new(Arc::clone(&workspace))),
                );
            }
            "write" => {
                register(
                    &mut builder,
                    Arc::new(WriteTool::new(Arc::clone(&workspace))),
                );
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
        let write = "write".parse().unwrap();
        let command_environment = CommandEnvironment::new(std::iter::empty::<OsString>());

        let empty = build_tools(&[], Arc::clone(&workspace), command_environment.clone()).unwrap();
        assert!(!empty.contains(&apply_patch));
        assert!(!empty.contains(&bash));
        assert!(!empty.contains(&edit));
        assert!(!empty.contains(&read));
        assert!(!empty.contains(&write));

        assert_eq!(
            KNOWN_TOOL_NAMES,
            &["read", "write", "edit", "apply_patch", "bash"]
        );

        for mask in 0..(1usize << KNOWN_TOOL_NAMES.len()) {
            let names = KNOWN_TOOL_NAMES
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1usize << index) != 0)
                .map(|(_, name)| (*name).to_owned())
                .collect::<Vec<_>>();
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
