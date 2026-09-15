//! Raw Git objects and bounded worktree reads; no diff driver or clean filter.
use super::*;
use crate::changes::{ChangeCoverage, ChangeKind, ChangeRevision, content_revision};
use crate::diff::{ChangesDiffRequest, DiffComparison, DiffSources, workspace_unavailable};
use crate::workspace::FileCapture;

pub(crate) async fn diff_sources(
    workspace: &Workspace,
    request: &ChangesDiffRequest,
    session_cancellation: &CancellationToken,
    shutdown_cancellation: &CancellationToken,
    child_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<DiffSources, AgentError> {
    let (head, entry) =
        crate::changes::parse_workspace_ref(&request.change_ref, request.session_id)?;
    let mut base = workspace_unavailable(request);
    base.path = entry.path.clone();
    base.kind = match entry.kind {
        WorkspaceStatusEntryKind::Untracked => ChangeKind::Added,
        WorkspaceStatusEntryKind::Renamed => ChangeKind::Renamed,
        WorkspaceStatusEntryKind::Unmerged => ChangeKind::Conflict,
        WorkspaceStatusEntryKind::Ordinary => ChangeKind::Modified,
    };
    if entry.kind == WorkspaceStatusEntryKind::Unmerged {
        return Ok(DiffSources { base, bytes: None });
    }
    if entry.kind == WorkspaceStatusEntryKind::Renamed && entry.original_path.is_none() {
        return Ok(DiffSources { base, bytes: None });
    }
    let program = status_program(workspace.root());
    let env = status_env(workspace.root());
    let mut run = GitRun {
        program: program.as_os_str(),
        root: workspace.root(),
        env: &env,
        caps: status_caps(workspace.root()),
        deadline,
        session_cancellation,
        shutdown_cancellation,
        child_cancellation,
    };
    let probe = run_git(&run, &os_args(&["rev-parse", "--show-toplevel"])).await?;
    if !usable(&probe) {
        return Ok(DiffSources { base, bytes: None });
    }
    let Some(toplevel) = repository_root(&probe.stdout) else {
        return Ok(DiffSources { base, bytes: None });
    };
    let Ok(prefix) = workspace.root().strip_prefix(&toplevel) else {
        return Ok(DiffSources { base, bytes: None });
    };
    let path = prefix.join(&entry.path);
    run.root = &toplevel;
    run.caps.stdout_bytes = crate::changes::MAX_CHANGE_SNAPSHOT_BYTES + 1;
    let unstaged = entry
        .worktree_status
        .as_deref()
        .is_some_and(|value| value != ".");
    base.comparison = request.comparison.unwrap_or(
        if unstaged || entry.kind == WorkspaceStatusEntryKind::Untracked {
            DiffComparison::IndexToWorktree
        } else {
            DiffComparison::HeadToIndex
        },
    );
    let worktree = base.comparison != DiffComparison::HeadToIndex;
    let index = index_blob(&run, &path).await?;
    let (before, after) = if worktree {
        let old = if base.comparison == DiffComparison::HeadToWorktree {
            let old_path = prefix.join(entry.original_path.as_ref().unwrap_or(&entry.path));
            if head.is_empty() {
                Blob::Missing
            } else {
                head_blob(&run, &head, &old_path).await?
            }
        } else {
            index
        };
        let (before, version) = load_side(&run, old).await?;
        base.base_version = version;
        if matches!(entry.kind, WorkspaceStatusEntryKind::Untracked)
            && before.as_ref().is_some_and(|v| !v.is_empty())
        {
            base.stale = true;
            return Ok(DiffSources { base, bytes: None });
        }
        let capture = tokio::select! {
            biased;
            _ = session_cancellation.cancelled() => return Err(AgentError::QueryLimit),
            _ = shutdown_cancellation.cancelled() => return Err(AgentError::QueryLimit),
            _ = child_cancellation.cancelled() => return Err(AgentError::QueryLimit),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(AgentError::QueryLimit),
            capture = async {
                if tokio::fs::symlink_metadata(workspace.root().join(&entry.path)).await
                    .is_ok_and(|metadata| metadata.file_type().is_symlink()) {
                    FileCapture::Error(crate::WorkspaceError::NotFile)
                } else {
                    workspace.capture_file(&entry.path, crate::changes::MAX_CHANGE_SNAPSHOT_BYTES).await
                }
            } => capture,
        };
        let after = match capture {
            FileCapture::Missing => {
                base.target_version = ChangeRevision::Missing;
                Some(Vec::new())
            }
            FileCapture::Complete(snapshot) if snapshot.stable => {
                base.target_version = content_revision(&snapshot.bytes);
                Some(snapshot.bytes)
            }
            _ => None,
        };
        (before, after)
    } else {
        let old_path = prefix.join(entry.original_path.as_ref().unwrap_or(&entry.path));
        let old = if head.is_empty() {
            Blob::Missing
        } else {
            head_blob(&run, &head, &old_path).await?
        };
        let (before, before_version) = load_side(&run, old).await?;
        let (after, after_version) = load_side(&run, index).await?;
        base.base_version = before_version;
        base.target_version = after_version;
        (before, after)
    };
    let bytes = before
        .zip(after)
        .map(|(before, after)| (Arc::from(before), Arc::from(after)));
    if bytes.is_some() {
        base.coverage = ChangeCoverage::Complete;
        if base.kind != ChangeKind::Renamed {
            base.kind = if base.base_version == ChangeRevision::Missing {
                ChangeKind::Added
            } else if base.target_version == ChangeRevision::Missing {
                ChangeKind::Deleted
            } else {
                ChangeKind::Modified
            };
        }
    }
    Ok(DiffSources { base, bytes })
}

fn usable(output: &GitOutput) -> bool {
    output.succeeded() && !output.truncated && !output.timed_out && !output.io_failed
}

enum Blob {
    Missing,
    Object(String),
    Unavailable,
}

fn literal(path: &Path) -> OsString {
    path.as_os_str().to_owned()
}

async fn index_blob(run: &GitRun<'_>, path: &Path) -> Result<Blob, AgentError> {
    let output = run_git(
        run,
        &[
            "--literal-pathspecs".into(),
            "ls-files".into(),
            "--stage".into(),
            "--full-name".into(),
            "-z".into(),
            "--".into(),
            literal(path),
        ],
    )
    .await?;
    if !usable(&output) {
        return Ok(Blob::Unavailable);
    }
    parse_blob(&output.stdout, true)
}

async fn head_blob(run: &GitRun<'_>, head: &str, path: &Path) -> Result<Blob, AgentError> {
    let output = run_git(
        run,
        &[
            "--literal-pathspecs".into(),
            "ls-tree".into(),
            "-z".into(),
            head.into(),
            "--".into(),
            literal(path),
        ],
    )
    .await?;
    if !usable(&output) {
        return Ok(Blob::Unavailable);
    }
    parse_blob(&output.stdout, false)
}

fn parse_blob(bytes: &[u8], index: bool) -> Result<Blob, AgentError> {
    if bytes.is_empty() {
        return Ok(Blob::Missing);
    }
    let mut records = bytes
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty());
    let Some(record) = records.next() else {
        return Ok(Blob::Unavailable);
    };
    if records.next().is_some() {
        return Ok(Blob::Unavailable);
    }
    let Some(tab) = record.iter().position(|byte| *byte == b'\t') else {
        return Ok(Blob::Unavailable);
    };
    let fields: Vec<_> = record[..tab].split(|byte| *byte == b' ').collect();
    if fields.len() != 3 || !matches!(fields[0], b"100644" | b"100755") {
        return Ok(Blob::Unavailable);
    }
    if (index && fields[2] != b"0") || (!index && fields[1] != b"blob") {
        return Ok(Blob::Unavailable);
    }
    let oid = if index { fields[1] } else { fields[2] };
    if ![40, 64].contains(&oid.len()) || !oid.iter().all(u8::is_ascii_hexdigit) {
        return Ok(Blob::Unavailable);
    }
    Ok(Blob::Object(
        String::from_utf8(oid.to_vec()).map_err(|_| AgentError::Internal)?,
    ))
}

async fn read_object(run: &GitRun<'_>, oid: &str) -> Result<Option<Vec<u8>>, AgentError> {
    let output = run_git(run, &["cat-file".into(), "blob".into(), oid.into()]).await?;
    Ok(
        (usable(&output) && output.stdout.len() <= crate::changes::MAX_CHANGE_SNAPSHOT_BYTES)
            .then_some(output.stdout),
    )
}

async fn load_side(
    run: &GitRun<'_>,
    blob: Blob,
) -> Result<(Option<Vec<u8>>, ChangeRevision), AgentError> {
    match blob {
        Blob::Missing => Ok((Some(Vec::new()), ChangeRevision::Missing)),
        Blob::Unavailable => Ok((None, ChangeRevision::Unknown)),
        Blob::Object(oid) => {
            let bytes = read_object(run, &oid).await?;
            let version = bytes
                .as_deref()
                .map(content_revision)
                .unwrap_or(ChangeRevision::Unknown);
            Ok((bytes, version))
        }
    }
}
