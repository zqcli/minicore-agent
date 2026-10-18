//! Shared scope, path and encoded-page limits for workspace queries.

use crate::error::AgentError;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::{Component, Path};
use std::time::{SystemTime, UNIX_EPOCH};

/// Largest accepted literal query or path filter.
pub(crate) const MAX_QUERY_BYTES: usize = 1024;
/// Largest accepted search `paths` list.
pub(crate) const MAX_SEARCH_PATHS: usize = 32;
/// Cursor scope fingerprints are truncated SHA-256 hex prefixes.
pub(crate) const SCOPE_HEX_LEN: usize = 16;

/// A request path longer than this is rejected before any query slot or
/// serialization work. The generic workspace path checks stay uncapped.
pub(crate) const MAX_PATH_BYTES: usize = 4096;

/// A result budget smaller than this cannot hold any useful envelope plus
/// content. `workspace.files` and `workspace.search` share the same range.
pub(crate) const MIN_RESULT_BYTES: usize = 1024;
pub(crate) const DEFAULT_RESULT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_RESULT_BYTES: usize = 256 * 1024;

pub(crate) fn validate_result_bytes(max_bytes: Option<usize>) -> Result<(), AgentError> {
    if max_bytes.is_some_and(|bytes| !(MIN_RESULT_BYTES..=MAX_RESULT_BYTES).contains(&bytes)) {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(())
    }
}

pub(crate) fn envelope_size<T: Serialize>(
    probe: &T,
    max_bytes: usize,
) -> Result<usize, AgentError> {
    let size = crate::budget::encoded_len(probe);
    if size >= max_bytes {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(size)
    }
}

/// Truncated SHA-256 fingerprint that binds a cursor to its session, method,
/// and traversal parameters. It is a mismatch detector, not a secret.
pub(crate) fn scope_digest(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    crate::store::digest_hex(hasher)[..SCOPE_HEX_LEN].to_owned()
}

pub(crate) fn valid_scope(value: &str) -> bool {
    value.len() == SCOPE_HEX_LEN && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// A literal query or path filter: bounded and single line.
pub(crate) fn valid_query_text(query: &str) -> bool {
    query.len() <= MAX_QUERY_BYTES && !query.contains('\n') && !query.contains('\r')
}

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Joins a native relative path into the `/`-separated form used by results.
pub(crate) fn display_path(relative: &Path) -> String {
    let mut display = String::new();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            continue;
        };
        if !display.is_empty() {
            display.push('/');
        }
        display.push_str(&part.to_string_lossy());
    }
    display
}

/// Rejects two requested roots that overlap, which would search one entry
/// twice.
pub(crate) fn roots_overlap(left: &str, right: &str) -> bool {
    if left.is_empty() || right.is_empty() {
        return true;
    }
    let left_path = Path::new(left);
    let right_path = Path::new(right);
    let (short, long) = if left_path.components().count() <= right_path.components().count() {
        (left_path, right_path)
    } else {
        (right_path, left_path)
    };
    long.starts_with(short)
}

/// Normalized, de-duplicated search roots, or the Workspace root when the
/// request names none. Overlapping roots are rejected so no entry is searched
/// twice.
pub(crate) fn normalized_roots(paths: Option<&[String]>) -> Result<Vec<String>, AgentError> {
    let mut roots: Vec<String> = Vec::new();
    for path in paths.unwrap_or_default() {
        let relative = crate::workspace::normalize_relative(path, true)
            .map_err(|_| AgentError::InvalidArguments)?;
        let normalized = display_path(&relative);
        if !roots.contains(&normalized) {
            roots.push(normalized);
        }
    }
    if roots.is_empty() {
        return Ok(vec![String::new()]);
    }
    for (index, root) in roots.iter().enumerate() {
        let overlaps = roots
            .iter()
            .enumerate()
            .any(|(other, candidate)| other != index && roots_overlap(root, candidate));
        if overlaps {
            return Err(AgentError::InvalidArguments);
        }
    }
    Ok(roots)
}
