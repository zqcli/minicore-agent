mod apply_patch;
mod bash;
pub(crate) mod command;
mod edit;
mod file_change;
pub(crate) mod observe;
mod read;
mod write;

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use minicore_runtime::tools::{Tool, ToolError, ToolSet};
use thiserror::Error;

use crate::presentation::{Presentation, PresentationTool, ToolImpl};
use crate::tools::command::CommandOwners;
use crate::tools::observe::ToolObserver;
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
pub(crate) const KNOWN_TOOL_NAMES: &[&str] = &["read", "write", "edit", "apply_patch", "bash"];

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BuildToolsError {
    #[error("tool configuration is invalid")]
    InvalidConfiguration,
    #[error("tool registration failed internally")]
    Internal,
}

pub(crate) fn build_tools_with_presentation(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: &Arc<Presentation>,
    observer: &Arc<ToolObserver>,
    command_owners: &Arc<CommandOwners>,
) -> Result<ToolSet, BuildToolsError> {
    build_tools_with(
        names,
        workspace,
        command_environment,
        Some((presentation, observer, command_owners)),
    )
}

fn build_tools_with(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
    presentation: Option<(&Arc<Presentation>, &Arc<ToolObserver>, &Arc<CommandOwners>)>,
) -> Result<ToolSet, BuildToolsError> {
    let mut builder = ToolSet::builder();
    let mut seen = BTreeSet::new();
    for name in names {
        if !seen.insert(name.as_str()) {
            return Err(BuildToolsError::InvalidConfiguration);
        }
        let tool = match name.as_str() {
            "apply_patch" => {
                ToolImpl::ApplyPatch(Arc::new(ApplyPatchTool::new(Arc::clone(&workspace))))
            }
            "bash" => ToolImpl::Bash(Arc::new(BashTool::with_binding(
                Arc::clone(&workspace),
                command_environment.clone(),
                presentation
                    .map(|(_, observer, owners)| observer.command_binding(Arc::clone(owners))),
            ))),
            "edit" => ToolImpl::Edit(Arc::new(EditTool::new(Arc::clone(&workspace)))),
            "read" => ToolImpl::Plain(Arc::new(ReadTool::new(Arc::clone(&workspace)))),
            "write" => ToolImpl::Write(Arc::new(WriteTool::new(Arc::clone(&workspace)))),
            _ => return Err(BuildToolsError::InvalidConfiguration),
        };
        let tool: Arc<dyn Tool> = match presentation {
            Some((presentation, observer, _)) => {
                PresentationTool::new(tool, Arc::clone(presentation), Arc::clone(observer))
            }
            None => match tool {
                ToolImpl::Plain(tool) => tool,
                ToolImpl::Bash(tool) => tool,
                ToolImpl::Write(tool) => tool,
                ToolImpl::Edit(tool) => tool,
                ToolImpl::ApplyPatch(tool) => tool,
            },
        };
        builder.register_arc(tool);
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

pub(super) fn map_source_error(error: WorkspaceError) -> ToolError {
    if error == WorkspaceError::TooLarge {
        ToolError::Failed
    } else {
        map_workspace_error(error)
    }
}

pub(super) fn decode_input<T: serde::de::DeserializeOwned>(
    spec: &minicore_runtime::tools::ToolSpec,
    invocation: &minicore_runtime::tools::ToolInvocation,
    context: &minicore_runtime::tools::ToolContext,
) -> Result<T, ToolError> {
    precheck_control(context)?;
    if invocation.tool_name() != spec.name() {
        return Err(ToolError::InvalidInvocation);
    }
    serde_json::from_value(invocation.arguments().clone()).map_err(|_| ToolError::InvalidInvocation)
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
pub(crate) static IO_GATES: crate::test_support::Registry<
    (&'static str, String),
    Arc<crate::test_support::Gate>,
> = crate::test_support::Registry::new();

#[cfg(test)]
pub(crate) mod tests;
