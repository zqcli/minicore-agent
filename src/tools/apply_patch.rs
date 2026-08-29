use std::sync::Arc;

use diffy::patch_set::{FileOperation, ParseOptions, PatchSet};
use diffy::{Line, Patch};
use minicore_runtime::tools::{
    Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolFuture, ToolInvocation, ToolOutput,
    ToolSpec,
};
use serde::Deserialize;
use serde_json::json;

use crate::{Workspace, WorkspaceError};

use super::{
    MAX_PATCH_BYTES, escape_control_characters, map_workspace_error, precheck_control,
    run_controlled, wait_for_test_io,
};

const TOOL_NAME: &str = "apply_patch";

pub(super) struct ApplyPatchTool {
    workspace: Arc<Workspace>,
    spec: ToolSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyPatchInput {
    path: String,
    patch: String,
}

impl ApplyPatchTool {
    pub(super) fn new(workspace: Arc<Workspace>) -> Self {
        let spec = ToolSpec::new(
            TOOL_NAME.parse().expect("apply_patch is a valid tool name"),
            "Apply one complete unified text patch to one existing workspace file.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative existing file path."
                    },
                    "patch": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Single-file unified text patch, limited to 512 KiB."
                    }
                },
                "required": ["path", "patch"],
                "additionalProperties": false
            }),
        )
        .expect("static apply_patch tool specification is valid");
        Self { workspace, spec }
    }
}

impl Tool for ApplyPatchTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation, context: ToolContext) -> ToolFuture<'a> {
        Box::pin(async move {
            precheck_control(&context)?;
            if invocation.tool_name() != self.spec.name() {
                return Err(ToolError::InvalidInvocation);
            }
            let input: ApplyPatchInput = serde_json::from_value(invocation.arguments().clone())
                .map_err(|_| ToolError::InvalidInvocation)?;
            if input.patch.is_empty()
                || input.patch.len() > MAX_PATCH_BYTES
                || input.patch.contains('\0')
            {
                return Err(ToolError::InvalidInvocation);
            }
            self.workspace
                .validate_write_path(&input.path)
                .map_err(map_workspace_error)?;
            let path_display = escape_control_characters(&input.path);

            run_controlled(&context, async {
                wait_for_test_io(TOOL_NAME, &input.path).await;
                let source = self
                    .workspace
                    .read_text(&input.path, MAX_PATCH_BYTES)
                    .await
                    .map_err(map_source_error)?;
                let result = apply_single_file_patch(&input.path, &source, &input.patch)?;
                if result.len() > MAX_PATCH_BYTES {
                    return Err(ToolError::InvalidInvocation);
                }
                let output = ToolOutput::new(format!(
                    "patched {} bytes to {} bytes at {path_display}",
                    source.len(),
                    result.len()
                ))
                .map_err(|_| ToolError::Internal)?;
                self.workspace
                    .write_atomic(&input.path, result.as_bytes())
                    .await
                    .map_err(map_workspace_error)?;
                Ok(ToolExecutionOutcome::Completed(output))
            })
            .await
        })
    }
}

#[derive(Clone, Copy)]
enum PatchFormat {
    Headerless,
    Standard,
}

fn apply_single_file_patch(path: &str, source: &str, patch: &str) -> Result<String, ToolError> {
    let format = validate_patch_shape(path, patch)?;
    let normalized;
    let patch = match format {
        PatchFormat::Headerless => {
            normalized = format!("--- minicore-source\n+++ minicore-source\n{patch}");
            normalized.as_str()
        }
        PatchFormat::Standard => patch,
    };
    let mut patches = PatchSet::parse(patch, ParseOptions::unidiff());
    let file_patch = patches
        .next()
        .ok_or(ToolError::InvalidInvocation)?
        .map_err(|_| ToolError::InvalidInvocation)?;
    if patches.next().is_some() {
        return Err(ToolError::InvalidInvocation);
    }
    if !matches!(file_patch.operation(), FileOperation::Modify { .. })
        || file_patch.old_mode().is_some()
        || file_patch.new_mode().is_some()
    {
        return Err(ToolError::InvalidInvocation);
    }
    let text_patch = file_patch
        .patch()
        .as_text()
        .ok_or(ToolError::InvalidInvocation)?;
    if text_patch.hunks().is_empty() {
        return Err(ToolError::InvalidInvocation);
    }
    let result_len = patched_len(source.len(), text_patch)?;
    if result_len > MAX_PATCH_BYTES {
        return Err(ToolError::InvalidInvocation);
    }
    let result = diffy::apply(source, text_patch).map_err(|_| ToolError::Failed)?;
    if result.len() != result_len {
        return Err(ToolError::Internal);
    }
    Ok(result)
}

fn patched_len(source_len: usize, patch: &Patch<'_, str>) -> Result<usize, ToolError> {
    let mut result_len = source_len;
    for line in patch.hunks().iter().flat_map(|hunk| hunk.lines()) {
        match line {
            Line::Context(_) => {}
            Line::Delete(line) => {
                result_len = result_len
                    .checked_sub(line.len())
                    .ok_or(ToolError::Failed)?;
            }
            Line::Insert(line) => {
                result_len = result_len
                    .checked_add(line.len())
                    .ok_or(ToolError::InvalidInvocation)?;
            }
        }
    }
    Ok(result_len)
}

fn validate_patch_shape(path: &str, patch: &str) -> Result<PatchFormat, ToolError> {
    let mut lines = patch.split_inclusive('\n');
    let first = lines.next().ok_or(ToolError::InvalidInvocation)?;
    let format = if strip_line_ending(first).starts_with("@@ ") {
        PatchFormat::Headerless
    } else if strip_line_ending(first).starts_with("--- ") {
        let original = patch_header_path(first, "--- ")?;
        let modified_line = lines.next().ok_or(ToolError::InvalidInvocation)?;
        let modified = patch_header_path(modified_line, "+++ ")?;
        if !header_path_matches(original, path, "a/") || !header_path_matches(modified, path, "b/")
        {
            return Err(ToolError::InvalidInvocation);
        }
        PatchFormat::Standard
    } else {
        return Err(ToolError::InvalidInvocation);
    };

    let first_hunk = matches!(format, PatchFormat::Headerless).then_some(first);
    validate_hunks(first_hunk.into_iter().chain(lines))?;
    Ok(format)
}

fn patch_header_path<'a>(line: &'a str, prefix: &str) -> Result<&'a str, ToolError> {
    let line = strip_line_ending(line);
    let path = line
        .strip_prefix(prefix)
        .ok_or(ToolError::InvalidInvocation)?;
    let path = path.split_once('\t').map_or(path, |(path, _)| path);
    if path.is_empty() || path == "/dev/null" {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(path)
}

fn header_path_matches(header: &str, path: &str, prefix: &str) -> bool {
    header == path || header.strip_prefix(prefix) == Some(path)
}

fn validate_hunks<'a>(lines: impl IntoIterator<Item = &'a str>) -> Result<(), ToolError> {
    let mut lines = lines.into_iter().peekable();
    let mut hunk_count = 0usize;
    while let Some(header) = lines.next() {
        let (expected_old, expected_new) = parse_hunk_counts(strip_line_ending(header))?;
        let mut old_count = 0usize;
        let mut new_count = 0usize;
        let mut marker_allowed = false;
        while old_count < expected_old || new_count < expected_new {
            let line = lines.next().ok_or(ToolError::InvalidInvocation)?;
            let content = strip_line_ending(line);
            if content == "\\ No newline at end of file" {
                if !marker_allowed {
                    return Err(ToolError::InvalidInvocation);
                }
                marker_allowed = false;
                continue;
            }
            match line.as_bytes().first().copied() {
                Some(b' ') => {
                    old_count = old_count.saturating_add(1);
                    new_count = new_count.saturating_add(1);
                }
                Some(b'-') => old_count = old_count.saturating_add(1),
                Some(b'+') => new_count = new_count.saturating_add(1),
                Some(b'\n') | None => {
                    old_count = old_count.saturating_add(1);
                    new_count = new_count.saturating_add(1);
                }
                _ => return Err(ToolError::InvalidInvocation),
            }
            if old_count > expected_old || new_count > expected_new {
                return Err(ToolError::InvalidInvocation);
            }
            marker_allowed = true;
        }
        if lines
            .peek()
            .is_some_and(|line| strip_line_ending(line) == "\\ No newline at end of file")
        {
            if !marker_allowed {
                return Err(ToolError::InvalidInvocation);
            }
            lines.next();
        }
        hunk_count = hunk_count.saturating_add(1);
    }
    if hunk_count == 0 {
        return Err(ToolError::InvalidInvocation);
    }
    Ok(())
}

fn parse_hunk_counts(header: &str) -> Result<(usize, usize), ToolError> {
    let ranges = header
        .strip_prefix("@@ ")
        .and_then(|header| header.split_once(" @@"))
        .map(|(ranges, _)| ranges)
        .ok_or(ToolError::InvalidInvocation)?;
    let mut ranges = ranges.split_whitespace();
    let old = ranges.next().ok_or(ToolError::InvalidInvocation)?;
    let new = ranges.next().ok_or(ToolError::InvalidInvocation)?;
    if ranges.next().is_some() {
        return Err(ToolError::InvalidInvocation);
    }
    Ok((parse_range_len(old, '-')?, parse_range_len(new, '+')?))
}

fn parse_range_len(range: &str, prefix: char) -> Result<usize, ToolError> {
    let range = range
        .strip_prefix(prefix)
        .ok_or(ToolError::InvalidInvocation)?;
    let (start, len) = range.split_once(',').unwrap_or((range, "1"));
    let start = start
        .parse::<usize>()
        .map_err(|_| ToolError::InvalidInvocation)?;
    let len = len
        .parse::<usize>()
        .map_err(|_| ToolError::InvalidInvocation)?;
    start.checked_add(len).ok_or(ToolError::InvalidInvocation)?;
    Ok(len)
}

fn strip_line_ending(line: &str) -> &str {
    let line = line.strip_suffix('\n').unwrap_or(line);
    line.strip_suffix('\r').unwrap_or(line)
}

fn map_source_error(error: WorkspaceError) -> ToolError {
    match error {
        WorkspaceError::InvalidPath | WorkspaceError::Escape => ToolError::InvalidInvocation,
        WorkspaceError::NotFound
        | WorkspaceError::NotFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::Unavailable
        | WorkspaceError::UnknownOutcome
        | WorkspaceError::TooLarge
        | WorkspaceError::Binary => ToolError::Failed,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::ids::{SessionInstanceId, ToolCallId, TurnId};
    use minicore_runtime::tools::{
        Tool, ToolContext, ToolError, ToolExecutionOutcome, ToolInvocation, ToolProgressSink,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::tools::{ToolIoGate, block_next_io};
    use crate::workspace::{fail_next_before_rename, fail_next_directory_sync};

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, ApplyPatchTool) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-apply-patch-tool-{label}-{}",
            minicore_runtime::SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let tool = ApplyPatchTool::new(Arc::clone(&workspace));
        (base, workspace, tool)
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            instance_id: "ins_00000000000000000000000000000001"
                .parse::<SessionInstanceId>()
                .unwrap(),
            turn_id: "trn_00000000000000000000000000000001"
                .parse::<TurnId>()
                .unwrap(),
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

    async fn cleanup(base: &Path) {
        let _ = tokio::fs::remove_dir_all(base).await;
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
        for path in ["too-large", "binary", "nul", "directory", "missing"] {
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
        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "cancelled"));
        block_next_io(Arc::clone(&gate));
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
        gate.started.acquire().await.unwrap().forget();
        cancellation.cancel();
        assert_eq!(cancelled.await.unwrap(), Err(ToolError::Cancelled));
        assert_eq!(
            tokio::fs::read_to_string(root.join("cancelled"))
                .await
                .unwrap(),
            "old\n"
        );

        let gate = Arc::new(ToolIoGate::new(TOOL_NAME, "timed-out"));
        block_next_io(Arc::clone(&gate));
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
        gate.started.acquire().await.unwrap().forget();
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

        fail_next_before_rename(resolved.clone());
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

        fail_next_directory_sync(resolved.parent().unwrap().to_path_buf());
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
}
