use super::command::ToolOwners;
use super::{map_source_error, map_workspace_error};
use crate::changes::{
    ChangeCommitState, ChangeCoverage, ChangeKind, ChangeRevision, FileChange, content_revision,
    metadata_revision,
};
use crate::tool_data::{ToolData, ToolRef};
use crate::workspace::{
    AtomicWriteObservation, ExpectedFileState, FileCapture, FileStamp, FileState,
};
use crate::{Workspace, WorkspaceError};
use minicore_runtime::tools::ToolError;
use std::sync::Arc;

pub(super) async fn read_source_before(
    workspace: &Workspace,
    path: &str,
    max_bytes: usize,
    bound: bool,
) -> Result<(String, FileBefore), WorkspaceError> {
    let (source, stamp, stable) = workspace.read_text_snapshot(path, max_bytes).await?;
    let before = if bound {
        FileBefore::Content {
            bytes: source.as_bytes().to_vec(),
            stamp,
            stable,
        }
    } else {
        FileBefore::Unknown
    };
    Ok((source, before))
}

pub(super) fn source_failure(
    error: WorkspaceError,
) -> Result<minicore_runtime::tools::ToolExecutionOutcome, ToolError> {
    if error == WorkspaceError::Binary {
        let output = minicore_runtime::tools::ToolOutput::new(
            "Cannot edit binary or non-UTF-8 content as text.",
        )
        .map_err(|_| ToolError::Internal)?;
        Ok(minicore_runtime::tools::ToolExecutionOutcome::Completed(
            output,
        ))
    } else {
        Err(map_source_error(error))
    }
}

/// Commits text and records its observation. With `owners`, a started commit
/// is joined by the loop and Session barriers even if this future is dropped,
/// so tool records are never persisted without the change they observed.
pub(super) async fn commit_text(
    workspace: &Workspace,
    owners: Option<&ToolOwners>,
    tool_ref: Option<ToolRef>,
    tool_data: Option<Arc<ToolData>>,
    path: String,
    before: FileBefore,
    bytes: Vec<u8>,
) -> Result<(), ToolError> {
    let owner_ref = tool_ref.clone();
    let commit = workspace
        .prepare_atomic_write(
            path,
            bytes,
            expected_file_state(&before),
            move |path, bytes, observation| {
                record_file_change(
                    tool_data.as_deref(),
                    tool_ref.as_ref(),
                    path,
                    before,
                    bytes,
                    observation,
                );
            },
        )
        .await;
    let observation = match owners {
        Some(owners) => owners.run_blocking(owner_ref.as_ref(), commit).await?,
        None => tokio::task::spawn_blocking(commit)
            .await
            .map_err(|_| ToolError::Failed)?,
    };
    observation.outcome.map_err(map_workspace_error)
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
