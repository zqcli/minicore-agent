use std::ffi::OsString;
use std::path::PathBuf;

use super::file_change::*;
use super::*;
pub(crate) fn build_tools(
    names: &[String],
    workspace: Arc<Workspace>,
    command_environment: CommandEnvironment,
) -> Result<ToolSet, BuildToolsError> {
    build_tools_with(names, workspace, command_environment, None)
}

use crate::changes::{ChangeCommitState, ChangeCoverage, ChangeKind};
use crate::tool_data::{ToolData, ToolRef};
use crate::workspace::{AtomicWriteObservation, FileState};

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
    for name in ["subagent", "unknown"] {
        assert_eq!(
            build_tools(
                &[name.to_owned()],
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
