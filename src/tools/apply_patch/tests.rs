use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
};
use minicore_runtime::{LoopId, ToolCallId};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::tool_data::{ToolData, ToolRef};
use crate::tools::IO_GATES;
use crate::workspace::{BEFORE_RENAME_FAILURES, DIRECTORY_SYNC_FAILURES};

async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, ApplyPatchTool) {
    let base = std::env::temp_dir().join(format!(
        "minicore-agent-apply-patch-tool-{label}-{}",
        crate::ids::SessionId::new().unwrap()
    ));
    let root = base.join("root");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let workspace = Arc::new(Workspace::open(root).await.unwrap());
    let tool = ApplyPatchTool::new(Arc::clone(&workspace));
    (base, workspace, tool)
}

fn invocation(arguments: Value) -> ToolInvocation {
    ToolInvocation {
        tool_call_id: ToolCallId::new("apply-patch-call").unwrap(),
        tool_name: TOOL_NAME.parse().unwrap(),
        arguments,
    }
}

fn context(cancellation: CancellationToken, deadline: Instant) -> ToolContext {
    ToolContext {
        cancellation,
        deadline,
        progress: ToolProgressSink::default(),
    }
}

async fn execute(tool: &ApplyPatchTool, arguments: Value) -> Result<String, ToolError> {
    match tool
        .execute(
            invocation(arguments),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await?
    {
        ToolExecutionOutcome::Completed(output) => Ok(output.content().as_str().to_owned()),
        ToolExecutionOutcome::RequestInput(_) => {
            panic!("apply_patch must not request input")
        }
    }
}

fn bound_ref() -> ToolRef {
    ToolRef {
        session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
        loop_id: LoopId::new().unwrap(),
        request_index: 0,
        tool_call_id: ToolCallId::new("apply-patch-call").unwrap(),
    }
}

async fn cleanup(base: &Path) {
    let _ = tokio::fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn bound_patch_records_the_same_source_and_computed_result() {
    let (base, _, tool) = fixture("bound-record").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), b"old\n")
        .await
        .unwrap();
    let data = Arc::new(ToolData::new());
    let tool_ref = bound_ref();
    data.note_requested(&tool_ref, TOOL_NAME);
    let result = tool
        .execute_bound(
            invocation(json!({
                "path": "value.txt",
                "patch": "--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-old\n+new\n"
            })),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            ),
            Some(tool_ref.clone()),
            Some(Arc::clone(&data)),
        )
        .await
        .unwrap();
    assert!(matches!(result, ToolExecutionOutcome::Completed(_)));
    let records = data.file_change_records(tool_ref.session_id, Some(tool_ref.loop_id));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].commit_state, crate::ChangeCommitState::Applied);
    data.finish_and_snapshot(
        &tool_ref,
        minicore_runtime::tools::ToolResultOutcome::Success,
    )
    .expect("bound patch record finishes");
    let snapshot = data.snapshot_for_persistence(&tool_ref).unwrap();
    assert_eq!(snapshot.file_change_before, Some(b"old\n".to_vec()));
    assert_eq!(snapshot.file_change_after, Some(b"new\n".to_vec()));
    cleanup(&base).await;
}

#[tokio::test]
async fn schema_is_strict_and_headerless_patch_is_applied() {
    let (base, _, tool) = fixture("headerless").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), "one\ntwo\nthree\n")
        .await
        .unwrap();
    let schema = tool.spec().input_schema();
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["path", "patch"]));
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["patch"]["minLength"], 1);
    let description = tool.spec().description().as_str();
    assert!(description.contains("exactly one *** Update File operation"));
    assert!(description.contains("Add File, Delete File, Move to"));
    let patch_description = schema["properties"]["patch"]["description"]
        .as_str()
        .unwrap();
    assert!(patch_description.contains("*** Begin Patch"));
    assert!(patch_description.contains("*** Update File: value.txt"));
    assert!(patch_description.contains("Add/Delete/Move/multifile forms are rejected"));
    for arguments in [
        json!({"path": "value.txt"}),
        json!({"path": "value.txt", "patch": "@@ -1 +1 @@\n one\n", "extra": true}),
    ] {
        assert_eq!(
            tool.execute(
                invocation(arguments),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
    }

    let patch = "@@ -1,3 +1,3 @@\n one\n-two\n+TWO\n three\n";
    assert_eq!(
        execute(&tool, json!({"path": "value.txt", "patch": patch}))
            .await
            .unwrap(),
        "patched 14 bytes to 14 bytes at value.txt"
    );
    assert_eq!(
        tokio::fs::read_to_string(root.join("value.txt"))
            .await
            .unwrap(),
        "one\nTWO\nthree\n"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn codex_update_envelope_applies_to_the_declared_existing_file() {
    let (base, _, tool) = fixture("codex-envelope").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), "old\n")
        .await
        .unwrap();
    let patch = "*** Begin Patch\n*** Update File: value.txt\n@@\n-old\n+new\n*** End Patch\n";

    assert_eq!(
        execute(&tool, json!({"path": "value.txt", "patch": patch}))
            .await
            .unwrap(),
        "patched 4 bytes to 4 bytes at value.txt"
    );
    assert_eq!(
        tokio::fs::read_to_string(root.join("value.txt"))
            .await
            .unwrap(),
        "new\n"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn codex_update_envelope_supports_anchors_ranges_blank_lines_and_eof() {
    let (base, _, tool) = fixture("codex-features").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    tokio::fs::write(&target, b"header\r\nanchor\r\nold\r\n\r\nfinal")
        .await
        .unwrap();
    let patch = "*** Begin Patch\r\n*** Update File: value.txt\r\n@@ anchor\r\n anchor\r\n-old\r\n+new\r\n@@ -4,2 +4,2 @@\r\n \r\n-final\r\n+tail\r\n*** End of File\r\n*** End Patch\r\n";

    execute(&tool, json!({"path": "value.txt", "patch": patch}))
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(&target).await.unwrap(),
        b"header\r\nanchor\r\nnew\r\n\r\ntail"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn codex_envelope_rejects_unsafe_ambiguous_and_overlapping_shapes_without_writing() {
    let (base, _, tool) = fixture("codex-invalid").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    let source = "a\nb\nc\n";
    tokio::fs::write(&target, source).await.unwrap();
    let invalid = [
        "*** Begin Patch\n*** Add File: value.txt\n+a\n*** End Patch\n",
        "*** Begin Patch\n*** Delete File: value.txt\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: value.txt\n*** Move to: other.txt\n@@\n-a\n+A\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: value.txt\n@@\n-a\n+A\n*** Update File: other.txt\n@@\n-b\n+B\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: other.txt\n@@\n-a\n+A\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: value.txt\n@@ -1 +1 @@\n-a\n+A\n@@ -1 +1 @@\n-a\n+A2\n*** End Patch\n",
    ];
    for patch in invalid {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation),
            "Codex shape should be rejected: {patch:?}"
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
    }

    tokio::fs::write(&target, "same\nsame\n").await.unwrap();
    let ambiguous =
        "*** Begin Patch\n*** Update File: value.txt\n@@\n-same\n+changed\n*** End Patch\n";
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": ambiguous})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    assert_eq!(
        tokio::fs::read_to_string(&target).await.unwrap(),
        "same\nsame\n"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn codex_envelope_oversize_is_rejected_without_reading_or_writing() {
    let (base, _, tool) = fixture("codex-oversize").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    tokio::fs::write(&target, "old\n").await.unwrap();
    let patch = format!(
        "*** Begin Patch\n*** Update File: value.txt\n@@\n-old\n+{}\n*** End Patch\n",
        "x".repeat(MAX_PATCH_BYTES)
    );
    assert!(patch.len() > MAX_PATCH_BYTES);
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": patch})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "old\n");
    cleanup(&base).await;
}

#[test]
fn codex_matching_handles_repeated_prefixes_without_guessing() {
    let source = "same\nnoise\nsame\nold\n";
    let patch =
        "*** Begin Patch\n*** Update File: value.txt\n@@\n same\n-old\n+new\n*** End Patch\n";
    let (result, _) = apply_single_file_patch_with_stats("value.txt", source, patch).unwrap();
    assert_eq!(result, "same\nnoise\nsame\nnew\n");
}

#[test]
fn codex_review_regression_consecutive_header_only_hunks() {
    let source = "outer\ninner\nold\n";
    let patch = "*** Begin Patch\n*** Update File: value.txt\n@@ outer\n@@ inner\n-old\n+new\n*** End Patch\n";
    assert_eq!(
        apply_single_file_patch("value.txt", source, patch),
        Ok("outer\ninner\nnew\n".to_owned())
    );
}

#[test]
fn codex_review_regression_noeol_context_append_at_eof() {
    let source = "old";
    let patch = "*** Begin Patch\n*** Update File: value.txt\n@@\n old\n+new\n*** End of File\n*** End Patch\n";
    assert_eq!(
        apply_single_file_patch("value.txt", source, patch),
        Ok("old\nnew".to_owned())
    );
}

#[test]
fn codex_review_regression_rejects_unanchored_empty_and_eof_hunks() {
    let invalid = [
        "*** Begin Patch\n*** Update File: value.txt\n@@ outer\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: value.txt\n@@\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: value.txt\n@@ outer\n*** End of File\n*** End Patch\n",
        "*** Begin Patch\n*** Update File: value.txt\n@@ -0,0 +0,0 @@\n*** End Patch\n",
    ];
    for patch in invalid {
        assert_eq!(
            apply_single_file_patch("value.txt", "outer\n", patch),
            Err(ToolError::InvalidInvocation)
        );
    }
}

#[test]
fn codex_review_regression_preserves_anchor_order_and_range_cursor() {
    let source = "outer\ninner\nold\n";
    let cases = [
        (
            "*** Begin Patch\n*** Update File: value.txt\n@@ inner\n-old\n+new\n@@ outer\n-inner\n+INNER\n*** End Patch\n",
            ToolError::Failed,
        ),
        (
            "*** Begin Patch\n*** Update File: value.txt\n@@ outer\n-old\n+new\n*** End of File\n@@ inner\n-old\n+new\n*** End Patch\n",
            ToolError::InvalidInvocation,
        ),
        (
            "*** Begin Patch\n*** Update File: value.txt\n@@ -3 +2 @@\n-old\n+new\n*** End Patch\n",
            ToolError::InvalidInvocation,
        ),
    ];
    for (patch, error) in cases {
        assert_eq!(
            apply_single_file_patch("value.txt", source, patch),
            Err(error)
        );
    }
}

#[test]
fn codex_hunks_use_bounded_forward_matching() {
    let mut source = String::new();
    let mut patch = String::from("*** Begin Patch\n*** Update File: value.txt\n");
    let mut expected = String::new();
    for index in 0..3_000usize {
        source.push_str(&format!("v{index:04}\n"));
        expected.push_str(&format!("V{index:04}\n"));
        patch.push_str(&format!("@@\n-v{index:04}\n+V{index:04}\n"));
    }
    patch.push_str("*** End Patch\n");
    assert!(patch.len() <= MAX_PATCH_BYTES);
    let (result, stats) = apply_single_file_patch_with_stats("value.txt", &source, &patch).unwrap();
    assert_eq!(result, expected);
    assert!(stats.codex_match_visits <= stats.source_lines);
    assert!(stats.total_steps() <= 4 * (stats.source_lines + stats.patch_lines));
}

#[tokio::test]
async fn standard_matching_headers_and_multiple_hunks_apply_unicode_changes() {
    let (base, _, tool) = fixture("standard-multi-hunk").await;
    let root = base.join("root");
    let source = "alpha\nbeta\nmiddle\ngamma\ndelta\n";
    let expected = "alpha\nβeta\nmiddle\ngamma\n世界\n";
    tokio::fs::write(root.join("value.txt"), source)
        .await
        .unwrap();
    let patch = "--- a/value.txt\t2026-01-01\n+++ b/value.txt\t2026-01-02\n@@ -1,2 +1,2 @@\n alpha\n-beta\n+βeta\n@@ -4,2 +4,2 @@\n gamma\n-delta\n+世界\n";

    assert_eq!(
        execute(&tool, json!({"path": "value.txt", "patch": patch}))
            .await
            .unwrap(),
        format!(
            "patched {} bytes to {} bytes at value.txt",
            source.len(),
            expected.len()
        )
    );
    assert_eq!(
        tokio::fs::read_to_string(root.join("value.txt"))
            .await
            .unwrap(),
        expected
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn headerless_and_standard_hunks_accept_section_headings() {
    let (base, _, tool) = fixture("section-headings").await;
    let root = base.join("root");
    tokio::fs::write(root.join("headerless.txt"), "old\n")
        .await
        .unwrap();
    let headerless = "@@ -1 +1 @@ fn execute() @@ nested\n-old\n+new\n";
    execute(
        &tool,
        json!({"path": "headerless.txt", "patch": headerless}),
    )
    .await
    .unwrap();
    assert_eq!(
        tokio::fs::read_to_string(root.join("headerless.txt"))
            .await
            .unwrap(),
        "new\n"
    );

    tokio::fs::write(root.join("standard.txt"), "old\n")
        .await
        .unwrap();
    let standard =
        "--- a/standard.txt\n+++ b/standard.txt\n@@ -1 +1 @@   fn execute()\n-old\n+new\n";
    execute(&tool, json!({"path": "standard.txt", "patch": standard}))
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read_to_string(root.join("standard.txt"))
            .await
            .unwrap(),
        "new\n"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn context_failure_and_partial_second_hunk_leave_the_source_unchanged() {
    let (base, _, tool) = fixture("hunk-failure").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    let source = "one\ntwo\nthree\nfour\n";
    tokio::fs::write(&target, source).await.unwrap();

    for patch in [
        "@@ -1,2 +1,2 @@\n one\n-mismatch\n+TWO\n",
        "@@ -1,2 +1,2 @@\n one\n-two\n+TWO\n@@ -3,2 +3,2 @@\n three\n-mismatch\n+FOUR\n",
    ] {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
    }
    cleanup(&base).await;
}

#[tokio::test]
async fn hunk_content_at_the_wrong_declared_position_does_not_fuzzy_apply() {
    let (base, _, tool) = fixture("exact-position").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    let source = "zero\none\ntarget\n";
    tokio::fs::write(&target, source).await.unwrap();
    let patch = "@@ -1 +1 @@\n-target\n+changed\n";

    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": patch})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
    cleanup(&base).await;
}

#[tokio::test]
async fn hunk_ranges_are_strict_ordered_and_support_start_zero_insertions() {
    let (base, _, tool) = fixture("strict-ranges").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    tokio::fs::write(&target, "tail\n").await.unwrap();
    assert_eq!(
        execute(
            &tool,
            json!({"path": "value.txt", "patch": "@@ -0,0 +1 @@\n+head\n"})
        )
        .await
        .unwrap(),
        "patched 5 bytes to 10 bytes at value.txt"
    );
    assert_eq!(
        tokio::fs::read_to_string(&target).await.unwrap(),
        "head\ntail\n"
    );

    let invalid = [
        "@@ -0 +1 @@\n-tail\n+TAIL\n",
        "@@ -1,2 +1 @@\n-tail\n+TAIL\n",
        "@@ -1 +1\n-tail\n+TAIL\n",
        "@@@ -1 +1 @@\n-tail\n+TAIL\n",
        "@@ -x +1 @@ fn execute()\n-tail\n+TAIL\n",
        "@@ -1 +1 @@fn execute()\n-tail\n+TAIL\n",
        "@@ -1 +1 @@@\n-tail\n+TAIL\n",
        "@@ -1,0 +1,0 @@\n",
        "@@ -184467440737095516160,0 +1,0 @@\n+x\n",
        "@@ -2 +2 @@\n-b\n+B\n@@ -1 +1 @@\n-a\n+A\n",
        "@@ -1,2 +1,2 @@\n a\n b\n@@ -2 +2 @@\n-b\n+B\n",
    ];
    for patch in invalid {
        tokio::fs::write(&target, "a\nb\n").await.unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation),
            "patch should be rejected as invalid: {patch:?}"
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "a\nb\n");
    }

    tokio::fs::write(&target, "a\n").await.unwrap();
    let wrong_new_position = "@@ -1 +2 @@\n-a\n+A\n";
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": wrong_new_position})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "a\n");
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn c_quoted_headers_decode_tab_quote_backslash_and_octal_utf8() {
    let (base, _, tool) = fixture("quoted-headers").await;
    let root = base.join("root");
    let cases = [
        (
            "tab\tname.txt",
            r#""a/tab\tname.txt""#,
            r#""b/tab\tname.txt""#,
        ),
        (
            "quote\"name.txt",
            r#""a/quote\"name.txt""#,
            r#""b/quote\"name.txt""#,
        ),
        (
            "slash\\name.txt",
            r#""a/slash\\name.txt""#,
            r#""b/slash\\name.txt""#,
        ),
        ("é.txt", r#""a/\303\251.txt""#, r#""b/\303\251.txt""#),
    ];
    for (path, old_header, new_header) in cases {
        tokio::fs::write(root.join(path), "old\n").await.unwrap();
        let patch = format!(
            "--- {old_header}\told timestamp\n+++ {new_header}\tnew timestamp\n@@ -1 +1 @@\n-old\n+new\n"
        );
        execute(&tool, json!({"path": path, "patch": patch}))
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(root.join(path)).await.unwrap(),
            "new\n"
        );
    }

    tokio::fs::write(root.join("value.txt"), "old\n")
        .await
        .unwrap();
    let invalid_headers = [
        r#""/absolute""#,
        r#""a/../value.txt""#,
        r#""a/value\x.txt""#,
        r#""a/\377.txt""#,
        r#""a/\000value.txt""#,
        r#""a/value.txt"junk"#,
        r#""a/value.txt"#,
    ];
    for old_header in invalid_headers {
        let patch = format!("--- {old_header}\n+++ \"b/value.txt\"\n@@ -1 +1 @@\n-old\n+new\n");
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(
            tokio::fs::read_to_string(root.join("value.txt"))
                .await
                .unwrap(),
            "old\n"
        );
    }
    cleanup(&base).await;
}

#[tokio::test]
async fn newline_markers_are_side_specific_and_preserve_crlf_final_newline_state() {
    let (base, _, tool) = fixture("newline-markers").await;
    let root = base.join("root");
    let target = root.join("value.txt");

    tokio::fs::write(&target, b"keep\r\nold").await.unwrap();
    let old_only = "@@ -2 +2 @@\r\n-old\r\n\\ No newline at end of file\r\n+new\r\n";
    execute(&tool, json!({"path": "value.txt", "patch": old_only}))
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"keep\r\nnew\r\n");

    tokio::fs::write(&target, b"keep\r\nold\r\n").await.unwrap();
    let new_only = "--- a/value.txt\r\n+++ b/value.txt\r\n@@ -2 +2 @@\r\n-old\r\n+new\r\n\\ No newline at end of file\r\n";
    execute(&tool, json!({"path": "value.txt", "patch": new_only}))
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"keep\r\nnew");

    tokio::fs::write(&target, b"keep\r\nlast").await.unwrap();
    let context_patch = "@@ -2 +2 @@\r\n last\r\n\\ No newline at end of file\r\n";
    execute(&tool, json!({"path": "value.txt", "patch": context_patch}))
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"keep\r\nlast");

    for patch in [
        "\\ No newline at end of file\n@@ -1 +1 @@\n-old\n+new\n",
        "@@ -1 +1 @@\n-old\n\\ No newline at end of file\n\\ No newline at end of file\n+new\n",
    ] {
        tokio::fs::write(&target, b"old\n").await.unwrap();
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"old\n");
    }

    tokio::fs::write(&target, b"old\n").await.unwrap();
    let wrong_old_marker = "@@ -1 +1 @@\n-old\n\\ No newline at end of file\n+new\n";
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": wrong_old_marker})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"old\n");
    cleanup(&base).await;
}

#[tokio::test]
async fn mixed_source_endings_are_preserved_outside_changes_and_additions_use_majority() {
    let (base, _, tool) = fixture("mixed-endings").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), b"one\r\ntwo\nthree\r\n")
        .await
        .unwrap();
    let patch = "@@ -2 +2 @@\n-two\n+TWO\n";
    execute(&tool, json!({"path": "value.txt", "patch": patch}))
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(root.join("value.txt")).await.unwrap(),
        b"one\r\nTWO\r\nthree\r\n"
    );
    cleanup(&base).await;
}

#[test]
fn thousands_of_hunks_use_single_forward_source_and_patch_steps() {
    let mut source = String::new();
    let mut replacement_patch = String::new();
    let mut expected = String::new();
    for index in 0..3_000usize {
        source.push_str(&format!("v{index:04}\n"));
        expected.push_str(&format!("V{index:04}\n"));
        let line = index + 1;
        replacement_patch.push_str(&format!(
            "@@ -{line} +{line} @@\n-v{index:04}\n+V{index:04}\n"
        ));
    }
    assert!(replacement_patch.len() <= MAX_PATCH_BYTES);
    let (result, stats) =
        apply_single_file_patch_with_stats("value.txt", &source, &replacement_patch).unwrap();
    assert_eq!(result, expected);
    assert_eq!(stats.source_line_visits, stats.source_lines);
    assert_eq!(stats.patch_lines, replacement_patch.lines().count());
    assert!(stats.total_steps() <= stats.source_lines + replacement_patch.lines().count());

    let mut insertion_patch = String::new();
    let mut inserted = String::new();
    for index in 0..3_000usize {
        let new_line = index + 1;
        insertion_patch.push_str(&format!("@@ -0,0 +{new_line} @@\n+i{index:04}\n"));
        inserted.push_str(&format!("i{index:04}\n"));
    }
    assert!(insertion_patch.len() <= MAX_PATCH_BYTES);
    let (result, stats) =
        apply_single_file_patch_with_stats("value.txt", "", &insertion_patch).unwrap();
    assert_eq!(result, inserted);
    assert_eq!(stats.source_line_visits, 0);
    assert_eq!(stats.patch_lines, insertion_patch.lines().count());
    assert!(stats.total_steps() <= insertion_patch.lines().count());

    let source = "x".repeat(MAX_PATCH_BYTES - 8_000);
    let mut over_limit_patch = String::new();
    for index in 0..5_000usize {
        let new_line = index + 1;
        over_limit_patch.push_str(&format!("@@ -0,0 +{new_line} @@\n+x\n"));
    }
    assert!(over_limit_patch.len() <= MAX_PATCH_BYTES);
    assert_eq!(
        apply_single_file_patch("value.txt", &source, &over_limit_patch),
        Err(ToolError::InvalidInvocation)
    );
}

#[tokio::test]
async fn unsafe_or_multi_file_patch_shapes_are_invalid_and_do_not_write() {
    let (base, _, tool) = fixture("invalid-shapes").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    let source = "old\n";
    tokio::fs::write(&target, source).await.unwrap();
    let invalid = [
        "--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-old\n+new\n--- a/other.txt\n+++ b/other.txt\n@@ -1 +1 @@\n-other\n+OTHER\n",
        "--- a/value.txt\n+++ b/renamed.txt\n@@ -1 +1 @@\n-old\n+new\n",
        "--- /dev/null\n+++ b/value.txt\n@@ -0,0 +1 @@\n+new\n",
        "--- a/value.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n",
        "diff --git a/value.txt b/value.txt\nrename from value.txt\nrename to renamed.txt\n",
        "diff --git a/value.txt b/value.txt\nGIT binary patch\nliteral 4\n",
        "--- a/value.txt\n+++ b/value.txt\nindex 1111111..2222222 100644\n@@ -1 +1 @@\n-old\n+new\n",
        "--- a/value.txt\n+++ b/value.txt\n",
        "--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-old\n+new\ntrailing junk\n",
        "@@ -1 +1 @@\n-old\n+new\n--- a/other.txt\n+++ b/other.txt\n",
    ];
    for patch in invalid {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation),
            "patch shape should be rejected"
        );
        assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
        assert!(
            tokio::fs::symlink_metadata(root.join("other.txt"))
                .await
                .is_err()
        );
    }

    let mismatched = "--- a/other.txt\n+++ b/other.txt\n@@ -1 +1 @@\n-old\n+new\n";
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": mismatched})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), source);
    cleanup(&base).await;
}

#[tokio::test]
async fn patch_source_and_result_limits_are_enforced_without_writing() {
    let (base, _, tool) = fixture("limits").await;
    let root = base.join("root");
    let target = root.join("value.txt");
    tokio::fs::write(&target, "old\n").await.unwrap();

    for patch in [
        String::new(),
        "x".repeat(MAX_PATCH_BYTES + 1),
        "@@ -1 +1 @@\n-old\n+new\0\n".to_owned(),
    ] {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": "value.txt", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::InvalidInvocation)
        );
    }
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "old\n");

    tokio::fs::write(root.join("too-large"), "a".repeat(MAX_PATCH_BYTES + 1))
        .await
        .unwrap();
    tokio::fs::write(root.join("binary"), [0xff, 0xfe])
        .await
        .unwrap();
    tokio::fs::write(root.join("nul"), b"text\0value")
        .await
        .unwrap();
    tokio::fs::create_dir(root.join("directory")).await.unwrap();
    let patch = "@@ -1 +1 @@\n-a\n+b\n";
    for path in ["binary", "nul"] {
        let before = tokio::fs::read(root.join(path)).await.unwrap();
        assert_eq!(
            execute(
                &tool,
                json!({"path": path, "patch": "@@ -1 +1 @@\n-text\n+new\n"})
            )
            .await
            .unwrap(),
            "Cannot edit binary or non-UTF-8 content as text."
        );
        assert_eq!(tokio::fs::read(root.join(path)).await.unwrap(), before);
    }
    for path in ["too-large", "directory", "missing"] {
        assert_eq!(
            tool.execute(
                invocation(json!({"path": path, "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5)
                )
            )
            .await,
            Err(ToolError::Failed)
        );
    }

    let at_limit = format!("a\n{}", "x".repeat(MAX_PATCH_BYTES - 2));
    tokio::fs::write(&target, &at_limit).await.unwrap();
    let too_large_result = "@@ -1 +1,2 @@\n a\n+b\n";
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value.txt", "patch": too_large_result})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), at_limit);

    let at_limit_patch = "@@ -1 +1 @@\n-a\n+b\n";
    assert_eq!(
        execute(&tool, json!({"path": "value.txt", "patch": at_limit_patch}))
            .await
            .unwrap(),
        format!("patched {MAX_PATCH_BYTES} bytes to {MAX_PATCH_BYTES} bytes at value.txt")
    );
    assert_eq!(
        tokio::fs::metadata(&target).await.unwrap().len(),
        MAX_PATCH_BYTES as u64
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn matching_crlf_patch_preserves_crlf_content() {
    let (base, _, tool) = fixture("crlf").await;
    let root = base.join("root");
    tokio::fs::write(root.join("value.txt"), b"one\r\ntwo\r\n")
        .await
        .unwrap();
    let patch = "@@ -1,2 +1,2 @@\r\n one\r\n-two\r\n+TWO\r\n";
    execute(&tool, json!({"path": "value.txt", "patch": patch}))
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(root.join("value.txt")).await.unwrap(),
        b"one\r\nTWO\r\n"
    );
    cleanup(&base).await;
}

#[cfg(unix)]
#[tokio::test]
async fn paths_are_contained_final_symlinks_are_rejected_and_output_is_single_line() {
    use std::os::unix::fs::symlink;

    let (base, _, tool) = fixture("paths").await;
    let root = base.join("root");
    let control_path = "control-你-\r-\n-\t-\u{1b}.txt";
    tokio::fs::write(root.join(control_path), "old\n")
        .await
        .unwrap();
    let patch = "@@ -1 +1 @@\n-old\n+new\n";
    let output = execute(&tool, json!({"path": control_path, "patch": patch}))
        .await
        .unwrap();
    assert_eq!(
        output,
        "patched 4 bytes to 4 bytes at control-你-\\r-\\n-\\t-\\u{1b}.txt"
    );
    assert_eq!(output.lines().count(), 1);
    assert!(!output.chars().any(char::is_control));

    tokio::fs::write(root.join("real"), "old\n").await.unwrap();
    symlink("real", root.join("link")).unwrap();
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "link", "patch": patch})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    assert_eq!(
        tokio::fs::read_to_string(root.join("real")).await.unwrap(),
        "old\n"
    );
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "../escape", "patch": patch})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::InvalidInvocation)
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn cancellation_and_deadline_before_commit_do_not_modify_the_source() {
    let (base, _, tool) = fixture("control").await;
    let root = base.join("root");
    tokio::fs::write(root.join("cancelled"), "old\n")
        .await
        .unwrap();
    tokio::fs::write(root.join("timed-out"), "old\n")
        .await
        .unwrap();
    let tool = Arc::new(tool);
    let patch = "@@ -1 +1 @@\n-old\n+new\n";

    let cancellation = CancellationToken::new();
    let gate = Arc::new(crate::test_support::Gate::new());
    IO_GATES.arm((TOOL_NAME, "cancelled".to_owned()), Arc::clone(&gate));
    let cancelled_tool = Arc::clone(&tool);
    let cancelled_context = context(
        cancellation.clone(),
        Instant::now() + Duration::from_secs(5),
    );
    let cancelled = tokio::spawn(async move {
        cancelled_tool
            .execute(
                invocation(json!({"path": "cancelled", "patch": patch})),
                cancelled_context,
            )
            .await
    });
    gate.wait_started().await;
    cancellation.cancel();
    assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));
    assert_eq!(
        tokio::fs::read_to_string(root.join("cancelled"))
            .await
            .unwrap(),
        "old\n"
    );

    let gate = Arc::new(crate::test_support::Gate::new());
    IO_GATES.arm((TOOL_NAME, "timed-out".to_owned()), Arc::clone(&gate));
    let deadline_tool = Arc::clone(&tool);
    let deadline = tokio::spawn(async move {
        deadline_tool
            .execute(
                invocation(json!({"path": "timed-out", "patch": patch})),
                context(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_millis(25),
                ),
            )
            .await
    });
    gate.wait_started().await;
    assert_eq!(deadline.await.unwrap(), Err(ToolError::TimedOut));
    assert_eq!(
        tokio::fs::read_to_string(root.join("timed-out"))
            .await
            .unwrap(),
        "old\n"
    );
    cleanup(&base).await;
}

#[tokio::test]
async fn atomic_failures_and_unknown_outcomes_never_report_success() {
    let (base, workspace, tool) = fixture("atomic-errors").await;
    let root = base.join("root");
    let target = root.join("value");
    tokio::fs::write(&target, "old\n").await.unwrap();
    let resolved = workspace.resolve_for_write("value").await.unwrap();
    let patch = "@@ -1 +1 @@\n-old\n+new\n";

    BEFORE_RENAME_FAILURES.arm(resolved.clone(), ());
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value", "patch": patch})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "old\n");

    DIRECTORY_SYNC_FAILURES.arm(resolved.parent().unwrap().to_path_buf(), ());
    assert_eq!(
        tool.execute(
            invocation(json!({"path": "value", "patch": patch})),
            context(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5)
            )
        )
        .await,
        Err(ToolError::Failed)
    );
    assert_eq!(tokio::fs::read_to_string(&target).await.unwrap(), "new\n");
    cleanup(&base).await;
}

#[cfg(test)]
impl ApplyStats {
    fn total_steps(&self) -> usize {
        self.source_line_visits
            .saturating_add(self.patch_content_visits)
            .saturating_add(self.codex_match_visits)
    }
}
