use serde_json::json;

use super::*;
use crate::workspace::status::{WorkspaceStatusEntry, WorkspaceStatusEntryKind};

fn session() -> crate::ids::SessionId {
    "ses_00000000000000000000000000000001".parse().unwrap()
}

fn status_entry(path: &str) -> WorkspaceStatusEntry {
    WorkspaceStatusEntry {
        path: path.to_owned(),
        kind: WorkspaceStatusEntryKind::Ordinary,
        index_status: Some("M".to_owned()),
        worktree_status: Some(".".to_owned()),
        original_path: None,
    }
}

#[test]
fn revisions_keep_missing_empty_and_unknown_distinct() {
    let empty = content_revision(b"");
    assert_ne!(ChangeRevision::Missing, empty);
    assert_ne!(empty, ChangeRevision::Unknown);
}

#[test]
fn scopes_use_string_units_and_an_explicit_turn_object() {
    assert_eq!(
        serde_json::to_value(ChangeScope::Workspace).unwrap(),
        json!("workspace")
    );
    assert_eq!(
        serde_json::to_value(ChangeScope::Session).unwrap(),
        json!("session")
    );
    let turn = ChangeScope::Turn {
        loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
    };
    assert_eq!(
        serde_json::to_value(turn).unwrap(),
        json!({"turn": {"loop_id": "lup_00000000000000000000000000000001"}})
    );
}

#[tokio::test]
async fn pages_honor_limit_and_reject_a_changed_observation_as_stale() {
    let status = WorkspaceStatusResult {
        repo_available: true,
        head_oid: None,
        branch: None,
        detached: false,
        staged: 2,
        unstaged: 0,
        untracked: 0,
        conflicted: 0,
        entries: vec![status_entry("a.txt"), status_entry("b.txt")],
        skipped_paths: 0,
        complete: true,
        warnings: Vec::new(),
        consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
        observed_at_unix_ms: 1,
    };
    let (records, observation, warnings) = workspace_records(session(), &status);
    let request = ChangesListRequest {
        session_id: session(),
        scope: ChangeScope::Workspace,
        cursor: None,
        limit: 1,
        max_bytes: Some(64 * 1024),
    };
    let first = page_records(
        &request,
        records.clone(),
        observation.clone(),
        ChangeListConsistency::Live,
        Some(1),
        warnings.clone(),
        true,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(1),
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.records.len(), 1);
    let cursor = first.next_cursor.unwrap();
    let continued = page_records(
        &ChangesListRequest {
            cursor: Some(cursor),
            ..request.clone()
        },
        records,
        "0".repeat(64),
        ChangeListConsistency::Live,
        Some(2),
        warnings,
        true,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(1),
        None,
    )
    .await
    .unwrap();
    assert!(continued.stale);
    assert!(continued.records.is_empty());
    assert!(continued.warnings.contains(&ChangeListWarning::StaleCursor));
}

#[tokio::test]
async fn incomplete_pages_keep_a_cursor_for_retained_records() {
    let status = WorkspaceStatusResult {
        repo_available: true,
        head_oid: None,
        branch: None,
        detached: false,
        staged: 0,
        unstaged: 0,
        untracked: 0,
        conflicted: 0,
        entries: vec![status_entry("a.txt"), status_entry("b.txt")],
        skipped_paths: 1,
        complete: false,
        warnings: Vec::new(),
        consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
        observed_at_unix_ms: 1,
    };
    let (records, observation, warnings) = workspace_records(session(), &status);
    let result = page_records(
        &ChangesListRequest {
            session_id: session(),
            scope: ChangeScope::Workspace,
            cursor: None,
            limit: 1,
            max_bytes: Some(64 * 1024),
        },
        records,
        observation,
        ChangeListConsistency::Live,
        Some(1),
        warnings,
        false,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(1),
        None,
    )
    .await
    .unwrap();
    assert!(!result.complete);
    assert!(result.next_cursor.is_some());
}

#[tokio::test]
async fn blob_verification_warnings_cannot_push_a_selected_record_past_the_budget() {
    // Two records whose details are initially unavailable. Choosing the page
    // end must already reserve the `RecordsSkipped` warning that blob
    // verification can add, so a later `complete = false` cannot grow the
    // encoded page past `max_bytes` and drop an already-selected record.
    let long_a = format!("{}.txt", "a".repeat(400));
    let long_b = format!("{}.txt", "b".repeat(400));
    let status = WorkspaceStatusResult {
        repo_available: true,
        head_oid: None,
        branch: None,
        detached: false,
        staged: 0,
        unstaged: 0,
        untracked: 0,
        conflicted: 0,
        entries: vec![status_entry(&long_a), status_entry(&long_b)],
        skipped_paths: 0,
        complete: true,
        warnings: Vec::new(),
        consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
        observed_at_unix_ms: 1,
    };
    let (records, observation, _) = workspace_records(session(), &status);
    assert!(records.iter().all(|record| !record.details_available));
    let request = ChangesListRequest {
        session_id: session(),
        scope: ChangeScope::Workspace,
        cursor: None,
        limit: 10,
        max_bytes: Some(64 * 1024),
    };
    // The best case the planner must be able to reach: both records present
    // with both blob-verification warnings reserved.
    let best = page_records(
        &request,
        records.clone(),
        observation.clone(),
        ChangeListConsistency::Live,
        Some(1),
        Vec::new(),
        false,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(1),
        Some(2),
    )
    .await
    .unwrap();
    assert_eq!(best.records.len(), 2);
    let max_bytes = serde_json::to_vec(&best).unwrap().len() - 1;
    let result = page_records(
        &ChangesListRequest {
            max_bytes: Some(max_bytes),
            ..request
        },
        records,
        observation,
        ChangeListConsistency::Live,
        Some(1),
        Vec::new(),
        false,
        &CancellationToken::new(),
        Instant::now() + Duration::from_secs(1),
        Some(2),
    )
    .await
    .expect("the request stays answerable under the real encoded budget");
    assert_eq!(result.records.len(), 1);
    assert!(result.next_cursor.is_some());
    assert!(serde_json::to_vec(&result).unwrap().len() <= max_bytes);
}

#[test]
fn retained_snapshot_integrity_controls_detail_availability() {
    let mut change = FileChange {
        path: "value.txt".to_owned(),
        kind: ChangeKind::Modified,
        before: content_revision(b"before"),
        after: content_revision(b"after"),
        commit_state: ChangeCommitState::Applied,
        coverage: ChangeCoverage::Complete,
        before_captured: true,
        after_captured: true,
        before_bytes: Some(b"before".to_vec()),
        after_bytes: Some(b"after".to_vec()),
        before_corrupt: false,
        after_corrupt: false,
    };
    assert!(change.details_available());
    change.after_bytes = Some(b"tampered".to_vec());
    assert!(!change.details_available());
}

#[test]
fn a_different_disk_revision_cannot_upgrade_a_warm_tool_record() {
    let tool_ref = ToolRef {
        session_id: session(),
        loop_id: "lup_00000000000000000000000000000001".parse().unwrap(),
        request_index: 3,
        tool_call_id: "call-3".parse().unwrap(),
    };
    let warm_change = StoredFileChange {
        path: "value.txt".to_owned(),
        kind: ChangeKind::Modified,
        before: content_revision(b"before"),
        after: content_revision(b"after"),
        commit_state: ChangeCommitState::Applied,
        coverage: ChangeCoverage::Complete,
        before_captured: true,
        after_captured: true,
    };
    let mut disk_change = warm_change.clone();
    disk_change.after = content_revision(b"different");
    let warm_record = warm_change.record(&tool_ref, true);
    assert!(!compatible_change(&warm_record, &tool_ref, &disk_change));
}

#[test]
fn workspace_records_never_attribute_a_tool() {
    let status = WorkspaceStatusResult {
        repo_available: true,
        head_oid: None,
        branch: None,
        detached: false,
        staged: 1,
        unstaged: 0,
        untracked: 0,
        conflicted: 0,
        entries: vec![status_entry("value.txt")],
        skipped_paths: 0,
        complete: true,
        warnings: Vec::new(),
        consistency: crate::workspace::scan::WorkspaceScanConsistency::Live,
        observed_at_unix_ms: 1,
    };
    let (records, _, _) = workspace_records(session(), &status);
    assert_eq!(records[0].origin, ChangeOrigin::WorkspaceUnknown);
    assert!(records[0].tool_ref.is_none());
    assert_eq!(&records[0].before, &ChangeRevision::Unknown);
    assert_eq!(&records[0].after, &ChangeRevision::Unknown);
}
