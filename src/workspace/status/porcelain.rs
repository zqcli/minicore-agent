//! Porcelain v2 decoding and workspace-relative path filtering.

use super::{StatusCaps, WorkspaceStatusEntry, WorkspaceStatusEntryKind};
use crate::workspace::page::MAX_PATH_BYTES;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

/// Branch names are filesystem-bound; a longer value cannot be a ref git
/// printed, so it is omitted and reported as a malformed header instead of
/// failing the whole query.
pub(super) const MAX_STATUS_BRANCH_BYTES: usize = 1024;
/// Parsed records one query accepts. The summary counters cover exactly the
/// records a query accepted.
pub(super) const MAX_STATUS_RECORDS: u64 = 100_000;

/// What one parsing pass over the porcelain stream found.
#[derive(Default)]
pub(super) struct Observation {
    pub(super) head_oid: Option<String>,
    pub(super) branch: Option<String>,
    pub(super) detached: bool,
    pub(super) saw_oid_header: bool,
    pub(super) saw_head_header: bool,
    pub(super) staged: u64,
    pub(super) unstaged: u64,
    pub(super) untracked: u64,
    pub(super) conflicted: u64,
    pub(super) entries: Vec<WorkspaceStatusEntry>,
    pub(super) skipped_paths: u64,
    pub(super) records: u64,
    pub(super) truncated: bool,
    pub(super) malformed: u64,
}

impl Observation {
    /// `--branch` prints both headers together. One without the other means the
    /// stream was cut by something this parser cannot account for, so the
    /// answer is not treated as complete.
    pub(super) fn branch_headers_complete(&self) -> bool {
        self.saw_oid_header && self.saw_head_header
    }
}

/// Parses one porcelain v2 (`-z`) stream, keeping only paths inside the
/// workspace. `toplevel` is where git was run, so every reported path is
/// relative to it.
pub(super) fn observe(
    stdout: &[u8],
    toplevel: &Path,
    workspace_root: &Path,
    caps: StatusCaps,
) -> Observation {
    let mut found = Observation::default();
    // A record cut in half by the output ceiling or the deadline is not a
    // record: parse only up to the last NUL, so no truncated path is reported.
    let stream: &[u8] = match stdout.iter().rposition(|byte| *byte == 0) {
        Some(last) => &stdout[..=last],
        None => &[],
    };
    let mut chunks = stream.split(|byte| *byte == 0);
    while let Some(chunk) = chunks.next() {
        if chunk.is_empty() {
            continue;
        }
        if found.records >= caps.records {
            found.truncated = true;
            break;
        }
        match chunk.first().copied() {
            Some(b'#') => header(chunk, &mut found),
            Some(b'1') => {
                let Some(fields) = split_fields(chunk, 9) else {
                    found.malformed += 1;
                    continue;
                };
                // A record whose codes are not git's documented status
                // characters is not an entry this parser can report.
                let Some(statuses) = xy(fields[1]) else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, _)) =
                    map_entry(fields[8], None, toplevel, workspace_root, &mut found)
                {
                    found.records += 1;
                    count(&mut found, statuses);
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Ordinary,
                        index_status: Some(statuses.0.to_string()),
                        worktree_status: Some(statuses.1.to_string()),
                        original_path: None,
                    });
                }
            }
            Some(b'2') => {
                // A rename record is followed by a second NUL-terminated path.
                // The chunk is consumed before the record is validated, so a
                // malformed record cannot shift the stream.
                let original = chunks.next();
                let Some(fields) = split_fields(chunk, 10) else {
                    found.malformed += 1;
                    continue;
                };
                let statuses = xy(fields[1]);
                let Some(original) = original.filter(|original| !original.is_empty()) else {
                    // Without the previous name this is not a rename, and a
                    // half record never becomes a complete rename entry.
                    found.malformed += 1;
                    continue;
                };
                let Some(statuses) = statuses else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, original_path)) = map_entry(
                    fields[9],
                    Some(original),
                    toplevel,
                    workspace_root,
                    &mut found,
                ) {
                    found.records += 1;
                    count(&mut found, statuses);
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Renamed,
                        index_status: Some(statuses.0.to_string()),
                        worktree_status: Some(statuses.1.to_string()),
                        original_path,
                    });
                }
            }
            Some(b'u') => {
                let Some(fields) = split_fields(chunk, 11) else {
                    found.malformed += 1;
                    continue;
                };
                let Some(statuses) = xy(fields[1]) else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, _)) =
                    map_entry(fields[10], None, toplevel, workspace_root, &mut found)
                {
                    found.records += 1;
                    // A conflicted path is counted as a conflict, not as a
                    // staged or unstaged change.
                    found.conflicted += 1;
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Unmerged,
                        index_status: Some(statuses.0.to_string()),
                        worktree_status: Some(statuses.1.to_string()),
                        original_path: None,
                    });
                }
            }
            Some(b'?') => {
                let Some(fields) = split_fields(chunk, 2) else {
                    found.malformed += 1;
                    continue;
                };
                if let Some((path, _)) =
                    map_entry(fields[1], None, toplevel, workspace_root, &mut found)
                {
                    found.records += 1;
                    found.untracked += 1;
                    found.entries.push(WorkspaceStatusEntry {
                        path,
                        kind: WorkspaceStatusEntryKind::Untracked,
                        index_status: None,
                        worktree_status: None,
                        original_path: None,
                    });
                }
            }
            // Ignored entries are never requested; anything else is not a
            // record this parser knows.
            Some(b'!') => {}
            _ => found.malformed += 1,
        }
    }
    found
}

/// Counts one ordinary or renamed entry in the summary buckets.
fn count(found: &mut Observation, (x, y): (char, char)) {
    if x != '.' {
        found.staged += 1;
    }
    if y != '.' {
        found.unstaged += 1;
    }
}

/// The status characters git documents for a porcelain v2 `XY` field.
const STATUS_CODES: [char; 8] = ['.', 'M', 'T', 'A', 'D', 'R', 'C', 'U'];

/// The two status characters of an ordinary, renamed, or unmerged record.
/// Anything outside git's documented codes is not a record this parser accepts.
fn xy(field: &[u8]) -> Option<(char, char)> {
    let text = std::str::from_utf8(field).ok()?;
    let mut chars = text.chars();
    let x = chars.next()?;
    let y = chars.next()?;
    if chars.next().is_some() || !STATUS_CODES.contains(&x) || !STATUS_CODES.contains(&y) {
        return None;
    }
    Some((x, y))
}

/// A full object id: 40 hex digits (SHA-1) or 64 (SHA-256).
fn valid_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn header(chunk: &[u8], found: &mut Observation) {
    let Ok(text) = std::str::from_utf8(chunk) else {
        found.malformed += 1;
        return;
    };
    let Some(rest) = text.strip_prefix("# ") else {
        found.malformed += 1;
        return;
    };
    let Some((key, value)) = rest.split_once(' ') else {
        found.malformed += 1;
        return;
    };
    match key {
        "branch.oid" => {
            found.saw_oid_header = true;
            found.head_oid = match value {
                "(initial)" => None,
                value if valid_oid(value) => Some(value.to_owned()),
                // Only a hash git could have printed or the unborn marker is
                // accepted; anything else stays absent and malformed.
                _ => {
                    found.malformed += 1;
                    None
                }
            };
        }
        "branch.head" => {
            found.saw_head_header = true;
            if value == "(detached)" {
                found.detached = true;
                found.branch = None;
            } else if value.is_empty() || value.len() > MAX_STATUS_BRANCH_BYTES {
                found.malformed += 1;
            } else {
                found.branch = Some(value.to_owned());
            }
        }
        // `branch.upstream` and `branch.ab` are read but not reported.
        _ => {}
    }
}

/// Splits `fields` space-separated porcelain fields, so the last one keeps any
/// spaces the path contains.
fn split_fields(chunk: &[u8], fields: usize) -> Option<Vec<&[u8]>> {
    let parts: Vec<&[u8]> = chunk.splitn(fields, |byte| *byte == b' ').collect();
    if parts.len() != fields {
        return None;
    }
    Some(parts)
}

/// Maps one reported path into workspace-relative form, skipping or filtering
/// it. Returns the new path and, for renames, the previous one.
pub(super) fn map_entry(
    path: &[u8],
    original: Option<&[u8]>,
    toplevel: &Path,
    workspace_root: &Path,
    found: &mut Observation,
) -> Option<(String, Option<String>)> {
    let mapped = match relative_to_workspace(path, toplevel, workspace_root) {
        Mapped::Inside(path) => path,
        Mapped::Outside => return None,
        Mapped::Skipped => {
            found.skipped_paths += 1;
            return None;
        }
    };
    let original_path = match original {
        None => None,
        Some(original) => match relative_to_workspace(original, toplevel, workspace_root) {
            Mapped::Inside(path) => Some(path),
            // A rename whose previous name cannot be represented or lies
            // outside the workspace keeps the entry and reports the current
            // name only.
            Mapped::Outside => None,
            Mapped::Skipped => {
                found.skipped_paths += 1;
                None
            }
        },
    };
    Some((mapped, original_path))
}

enum Mapped {
    Inside(String),
    Outside,
    Skipped,
}

/// Converts one git path, relative to the work tree root, into a
/// workspace-relative path. Paths outside the workspace are filtered out and
/// never counted; paths that cannot be represented are reported as skipped.
fn relative_to_workspace(path: &[u8], toplevel: &Path, workspace_root: &Path) -> Mapped {
    if path.len() > MAX_PATH_BYTES {
        return Mapped::Skipped;
    }
    let Ok(text) = std::str::from_utf8(path) else {
        return Mapped::Skipped;
    };
    let full = if Path::new(text).is_absolute() {
        PathBuf::from(text)
    } else {
        toplevel.join(text)
    };
    let mut normal = PathBuf::new();
    for component in full.components() {
        match component {
            Component::Prefix(prefix) => normal.push(prefix.as_os_str()),
            Component::RootDir => normal.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normal.pop() {
                    return Mapped::Skipped;
                }
            }
            Component::Normal(name) => normal.push(name),
        }
    }
    let Ok(stripped) = normal.strip_prefix(workspace_root) else {
        return Mapped::Outside;
    };
    let mut relative = String::new();
    for component in stripped.components() {
        let Some(name) = component.as_os_str().to_str() else {
            return Mapped::Skipped;
        };
        if !relative.is_empty() {
            relative.push('/');
        }
        relative.push_str(name);
    }
    Mapped::Inside(relative)
}

/// The path git is given to limit status to the workspace itself. `.` is used
/// when the workspace is the work tree root.
pub(super) fn workspace_pathspec(workspace_root: &Path, toplevel: &Path) -> Option<OsString> {
    let stripped = workspace_root.strip_prefix(toplevel).ok()?;
    let mut spec = OsString::new();
    for component in stripped.components() {
        let name = component.as_os_str();
        if name.is_empty() {
            continue;
        }
        if !spec.is_empty() {
            spec.push("/");
        }
        spec.push(name);
    }
    if spec.is_empty() {
        spec.push(".");
    }
    Some(spec)
}
