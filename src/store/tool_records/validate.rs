//! Stored auxiliary metadata and snapshot validation.
use super::super::*;

pub(super) fn validate_stored_tool_record(
    stored: &StoredToolRecord,
    tool_ref: &ToolRef,
) -> Result<(), StoreError> {
    if stored.version != TOOL_RECORD_FORMAT_VERSION {
        return Err(StoreError::UnsupportedFormat);
    }
    if stored.tool_ref != *tool_ref {
        return Err(StoreError::Corrupt);
    }
    if !KNOWN_TOOL_NAMES.contains(&stored.name.as_str()) && stored.name != LEGACY_STORED_TOOL_NAME {
        return Err(StoreError::Corrupt);
    }
    if !stored.state.is_terminal() {
        return Err(StoreError::Corrupt);
    }
    if let Some(outcome) = stored.outcome {
        if !stored.state.matches_outcome(outcome) {
            return Err(StoreError::Corrupt);
        }
    }

    for (seen, total_bytes, file_bytes, file_sha256, expired, cap) in [
        (
            stored.input.seen,
            stored.input.total_bytes,
            stored.input.file_bytes,
            &stored.input.file_sha256,
            stored.input.expired,
            MAX_TOOL_INPUT_PERSIST_BYTES,
        ),
        (
            stored.result.seen,
            stored.result.total_bytes,
            stored.result.file_bytes,
            &stored.result.file_sha256,
            stored.result.expired,
            MAX_TOOL_RESULT_PERSIST_BYTES,
        ),
    ] {
        if !seen {
            if total_bytes != 0 || file_bytes != 0 || file_sha256.is_some() {
                return Err(StoreError::Corrupt);
            }
        } else {
            if file_bytes > total_bytes {
                return Err(StoreError::Corrupt);
            }
            if expired && (file_bytes != 0 || file_sha256.is_some()) {
                return Err(StoreError::Corrupt);
            }
        }
        if file_bytes > cap {
            return Err(StoreError::RecordTooLarge);
        }
    }

    for stream in [&stored.stdout, &stored.stderr] {
        if stream.start_offset > stream.observed_end {
            return Err(StoreError::Corrupt);
        }
        if !stream.seen {
            if stream.start_offset != 0
                || stream.observed_end != 0
                || stream.file_bytes != 0
                || stream.file_sha256.is_some()
            {
                return Err(StoreError::Corrupt);
            }
        } else if stream.expired {
            if stream.start_offset != stream.observed_end
                || stream.file_bytes != 0
                || stream.file_sha256.is_some()
            {
                return Err(StoreError::Corrupt);
            }
        } else {
            let diff = stream
                .observed_end
                .checked_sub(stream.start_offset)
                .ok_or(StoreError::Corrupt)?;
            if diff != stream.file_bytes as u64 {
                return Err(StoreError::Corrupt);
            }
        }
        if stream.file_bytes > crate::tool_data::MAX_TOOL_STREAM_BYTES {
            return Err(StoreError::RecordTooLarge);
        }
    }

    let change_bytes = stored.file_change.as_ref().map_or(0, |change| {
        let before = match &change.before {
            crate::changes::ChangeRevision::Content { bytes, .. } => *bytes,
            _ => 0,
        };
        let after = match &change.after {
            crate::changes::ChangeRevision::Content { bytes, .. } => *bytes,
            _ => 0,
        };
        before.saturating_add(after)
    });

    let total_file_bytes = stored
        .input
        .file_bytes
        .saturating_add(stored.result.file_bytes)
        .saturating_add(stored.stdout.file_bytes)
        .saturating_add(stored.stderr.file_bytes)
        .saturating_add(change_bytes);
    if total_file_bytes > 3 * 1024 * 1024 {
        return Err(StoreError::RecordTooLarge);
    }

    for (_, bytes, hash) in stored_blobs(stored) {
        if if bytes == 0 {
            hash.is_some()
        } else {
            !hash.is_some_and(valid_sha256)
        } {
            return Err(StoreError::Corrupt);
        }
    }

    if let Some(cmd) = &stored.command {
        if !cmd.status.is_terminal() {
            return Err(StoreError::Corrupt);
        }
        if cmd.stdout_base_offset != stored.stdout.start_offset
            || cmd.stdout_observed_end != stored.stdout.observed_end
            || cmd.stderr_base_offset != stored.stderr.start_offset
            || cmd.stderr_observed_end != stored.stderr.observed_end
        {
            return Err(StoreError::Corrupt);
        }
    }

    if let Some(change) = &stored.file_change {
        validate_stored_file_change(change)?;
    }

    Ok(())
}

fn validate_stored_file_change(change: &StoredFileChange) -> Result<(), StoreError> {
    crate::workspace::validate_relative_path(&change.path).map_err(corrupt)?;
    match &change.before {
        ChangeRevision::Missing => {
            if !change.before_captured {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Content { sha256, bytes } => {
            if !change.before_captured
                || *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
                || !valid_sha256(sha256)
            {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => {
            if change.before_captured {
                return Err(StoreError::Corrupt);
            }
        }
    }
    match &change.after {
        ChangeRevision::Content { sha256, bytes } => {
            if !change.after_captured
                || *bytes > crate::changes::MAX_CHANGE_SNAPSHOT_BYTES
                || !valid_sha256(sha256)
            {
                return Err(StoreError::Corrupt);
            }
        }
        ChangeRevision::Missing | ChangeRevision::Metadata { .. } | ChangeRevision::Unknown => {
            if change.after_captured {
                return Err(StoreError::Corrupt);
            }
        }
    }
    match change.commit_state {
        crate::changes::ChangeCommitState::Applied
        | crate::changes::ChangeCommitState::Conflict => {
            if !matches!(&change.after, ChangeRevision::Content { .. }) {
                return Err(StoreError::Corrupt);
            }
        }
        crate::changes::ChangeCommitState::NotCommitted
        | crate::changes::ChangeCommitState::Unknown => {}
    }
    Ok(())
}

// The core blobs match ToolPersistenceSnapshot::blobs; change snapshots follow them.
pub(super) fn stored_blobs(stored: &StoredToolRecord) -> [(&str, usize, Option<&str>); 4] {
    [
        (
            TOOL_INPUT_FILE,
            stored.input.file_bytes,
            stored.input.file_sha256.as_deref(),
        ),
        (
            TOOL_RESULT_FILE,
            stored.result.file_bytes,
            stored.result.file_sha256.as_deref(),
        ),
        (
            TOOL_STDOUT_FILE,
            stored.stdout.file_bytes,
            stored.stdout.file_sha256.as_deref(),
        ),
        (
            TOOL_STDERR_FILE,
            stored.stderr.file_bytes,
            stored.stderr.file_sha256.as_deref(),
        ),
    ]
}
