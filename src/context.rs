use std::io::{self, Write};
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use minicore_runtime::context::{
    ContextBlock, ContextBundle, ContextError, ContextFuture, ContextProvider, ContextRequest,
    ContextSlot,
};
use minicore_runtime::model::ModelMessage;
use minicore_runtime::value::BoundedText;

use crate::workspace::ReadPrefix;
use crate::{Workspace, WorkspaceError};

const AGENTS_PATH: &str = "AGENTS.md";
const SOURCE_ID: &str = "agents-md";
const PRIORITY: i16 = 100;
const TRUNCATED: &str = "[truncated]";
const CONTEXT_ENVELOPE: &str = "[minicore-context slot=project_instructions source=agents-md]\n";
const MAX_CONTEXT_CONTENT_BYTES: usize = BoundedText::MAX_BYTES - CONTEXT_ENVELOPE.len();

pub(crate) struct ProjectContext {
    workspace: Arc<Workspace>,
}

impl ProjectContext {
    pub(crate) fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

impl ContextProvider for ProjectContext {
    fn provide<'a>(&'a self, request: ContextRequest) -> ContextFuture<'a> {
        Box::pin(async move {
            precheck_control(&request)?;
            let remaining_context_budget = request.remaining_context_budget;
            let read = async {
                wait_for_test_read(&self.workspace).await;
                self.workspace
                    .read_prefix(AGENTS_PATH, BoundedText::MAX_BYTES)
                    .await
            };
            tokio::pin!(read);
            let prefix = tokio::select! {
                biased;
                _ = request.cancellation.cancelled() => return Err(ContextError::Cancelled),
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(request.deadline)) => {
                    return Err(ContextError::DeadlineExceeded);
                }
                result = &mut read => result,
            };
            let prefix = match prefix {
                Ok(prefix) => prefix,
                Err(WorkspaceError::NotFound) => return Ok(empty_bundle()),
                Err(_) => return Err(ContextError::Unavailable),
            };
            let (content, read_truncated) = decode_text_prefix(prefix)?;
            build_bundle(&content, read_truncated, remaining_context_budget)
        })
    }
}

fn precheck_control(request: &ContextRequest) -> Result<(), ContextError> {
    if request.cancellation.is_cancelled() {
        return Err(ContextError::Cancelled);
    }
    if Instant::now() >= request.deadline {
        return Err(ContextError::DeadlineExceeded);
    }
    Ok(())
}

fn decode_text_prefix(prefix: ReadPrefix) -> Result<(String, bool), ContextError> {
    let visible = &prefix.bytes[..prefix.visible_len];
    let visible_end = match std::str::from_utf8(visible) {
        Ok(_) => prefix.visible_len,
        Err(error) if error.error_len().is_none() => {
            let start = error.valid_up_to();
            let width = prefix
                .bytes
                .get(start)
                .copied()
                .and_then(utf8_sequence_width)
                .ok_or(ContextError::Unavailable)?;
            let end = start.checked_add(width).ok_or(ContextError::Unavailable)?;
            if end > prefix.bytes.len()
                || end <= prefix.visible_len
                || std::str::from_utf8(&prefix.bytes[start..end]).is_err()
            {
                return Err(ContextError::Unavailable);
            }
            start
        }
        Err(_) => return Err(ContextError::Unavailable),
    };

    for index in 0..visible_end {
        if prefix.bytes[index] == b'\r' && prefix.bytes.get(index + 1) != Some(&b'\n') {
            return Err(ContextError::Unavailable);
        }
    }
    let split_crlf_at_cap = visible_end > 0
        && prefix.bytes[visible_end - 1] == b'\r'
        && prefix.bytes.get(visible_end) == Some(&b'\n');
    let mut content = std::str::from_utf8(&prefix.bytes[..visible_end])
        .map_err(|_| ContextError::Unavailable)?
        .replace("\r\n", "\n");
    if split_crlf_at_cap {
        debug_assert!(content.ends_with('\r'));
        content.pop();
        content.push('\n');
    }
    if content
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(ContextError::Unavailable);
    }
    Ok((content, prefix.has_more))
}

fn build_bundle(
    content: &str,
    read_truncated: bool,
    remaining_context_budget: u64,
) -> Result<ContextBundle, ContextError> {
    if !read_truncated
        && content.len() <= MAX_CONTEXT_CONTENT_BYTES
        && context_budget_tokens(content)? <= remaining_context_budget
    {
        return one_block(content);
    }

    if !truncated_content_fits("", remaining_context_budget)? {
        return Ok(empty_bundle());
    }
    let end = largest_truncated_prefix(content, remaining_context_budget)?;
    let content = with_truncated_marker(&content[..end])?;
    one_block(&content)
}

fn largest_truncated_prefix(
    content: &str,
    remaining_context_budget: u64,
) -> Result<usize, ContextError> {
    let mut lower = 0usize;
    let mut upper = content.len().checked_add(1).ok_or(ContextError::Internal)?;
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        let end = floor_char_boundary(content, middle);
        if truncated_content_fits(&content[..end], remaining_context_budget)? {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(floor_char_boundary(content, lower.saturating_sub(1)))
}

fn truncated_content_fits(body: &str, remaining_context_budget: u64) -> Result<bool, ContextError> {
    let content = with_truncated_marker(body)?;
    if content.len() > MAX_CONTEXT_CONTENT_BYTES {
        return Ok(false);
    }
    Ok(context_budget_tokens(&content)? <= remaining_context_budget)
}

fn with_truncated_marker(body: &str) -> Result<String, ContextError> {
    let separator = usize::from(!body.is_empty() && !body.ends_with('\n'));
    let capacity = body
        .len()
        .checked_add(separator)
        .and_then(|length| length.checked_add(TRUNCATED.len()))
        .ok_or(ContextError::Internal)?;
    let mut content = String::with_capacity(capacity);
    content.push_str(body);
    if separator != 0 {
        content.push('\n');
    }
    content.push_str(TRUNCATED);
    Ok(content)
}

fn one_block(content: &str) -> Result<ContextBundle, ContextError> {
    let content = BoundedText::new(content).map_err(|_| ContextError::Internal)?;
    let source = SOURCE_ID.parse().map_err(|_| ContextError::Internal)?;
    Ok(ContextBundle {
        blocks: vec![ContextBlock {
            source,
            slot: ContextSlot::ProjectInstructions,
            priority: PRIORITY,
            content,
        }],
    })
}

fn empty_bundle() -> ContextBundle {
    ContextBundle { blocks: Vec::new() }
}

fn context_budget_tokens(content: &str) -> Result<u64, ContextError> {
    let message = context_message(content)?;
    let mut writer = ByteCountingWriter::default();
    serde_json::to_writer(&mut writer, &message).map_err(|_| ContextError::Internal)?;
    // ModelRequest always has at least one fixed message. Inserting this context
    // message therefore adds exactly its serialized bytes plus one array comma.
    // For fixed bytes F and delta D,
    // ceil((F + D) / 4) - ceil(F / 4) <= ceil(D / 4),
    // with equality when F is divisible by four.
    let delta = writer
        .written
        .checked_add(1)
        .ok_or(ContextError::Internal)?;
    let rounded = delta.checked_add(3).ok_or(ContextError::Internal)?;
    u64::try_from(rounded / 4).map_err(|_| ContextError::Internal)
}

fn context_message(content: &str) -> Result<ModelMessage, ContextError> {
    let capacity = CONTEXT_ENVELOPE
        .len()
        .checked_add(content.len())
        .ok_or(ContextError::Internal)?;
    let mut text = String::with_capacity(capacity);
    text.push_str(CONTEXT_ENVELOPE);
    text.push_str(content);
    ModelMessage::system(text).map_err(|_| ContextError::Internal)
}

#[derive(Default)]
struct ByteCountingWriter {
    written: usize,
}

impl Write for ByteCountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("context serialization byte count overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn utf8_sequence_width(first: u8) -> Option<usize> {
    match first {
        0xc2..=0xdf => Some(2),
        0xe0..=0xef => Some(3),
        0xf0..=0xf4 => Some(4),
        _ => None,
    }
}

fn floor_char_boundary(value: &str, maximum: usize) -> usize {
    let mut end = maximum.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}

#[cfg(test)]
struct ContextReadGate {
    workspace: usize,
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl ContextReadGate {
    fn new(provider: &ProjectContext) -> Self {
        Self {
            workspace: Arc::as_ptr(&provider.workspace) as usize,
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

#[cfg(test)]
static READ_GATES: OnceLock<Mutex<Vec<Arc<ContextReadGate>>>> = OnceLock::new();

#[cfg(test)]
fn block_next_read(gate: Arc<ContextReadGate>) {
    READ_GATES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(gate);
}

async fn wait_for_test_read(workspace: &Arc<Workspace>) {
    #[cfg(test)]
    {
        let workspace = Arc::as_ptr(workspace) as usize;
        let gate = {
            let mut gates = READ_GATES
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap();
            gates
                .iter()
                .position(|gate| gate.workspace == workspace)
                .map(|position| gates.remove(position))
        };
        if let Some(gate) = gate {
            gate.started.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
    }
    #[cfg(not(test))]
    let _ = workspace;
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use minicore_runtime::config::SemanticLimits;
    use minicore_runtime::context::{ContextProvider, ContextRequest, ContextSlot};
    use minicore_runtime::conversation::ConversationView;
    use minicore_runtime::ids::{SessionId, SessionInstanceId, TurnId};
    use minicore_runtime::model::{ModelLimits, ModelRequest, ReasoningPreference};
    use tokio_util::sync::CancellationToken;

    use super::*;

    async fn fixture(label: &str) -> (PathBuf, Arc<Workspace>, ProjectContext) {
        let base = std::env::temp_dir().join(format!(
            "minicore-agent-project-context-{label}-{}",
            SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        let provider = ProjectContext::new(Arc::clone(&workspace));
        (base, workspace, provider)
    }

    fn request(remaining_context_budget: u64) -> ContextRequest {
        request_with_control(
            remaining_context_budget,
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
    }

    fn request_with_control(
        remaining_context_budget: u64,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> ContextRequest {
        ContextRequest {
            session_id: "ses_00000000000000000000000000000001".parse().unwrap(),
            instance_id: "ins_00000000000000000000000000000001"
                .parse::<SessionInstanceId>()
                .unwrap(),
            turn_id: "trn_00000000000000000000000000000001"
                .parse::<TurnId>()
                .unwrap(),
            model_round: 1,
            conversation: ConversationView::empty(),
            remaining_context_budget,
            cancellation,
            deadline,
        }
    }

    async fn provide(
        provider: &ProjectContext,
        remaining_context_budget: u64,
    ) -> Result<ContextBundle, ContextError> {
        provider.provide(request(remaining_context_budget)).await
    }

    async fn write_agents(base: &Path, content: impl AsRef<[u8]>) {
        tokio::fs::write(base.join("root/AGENTS.md"), content)
            .await
            .unwrap();
    }

    async fn cleanup(base: &Path) {
        let _ = tokio::fs::remove_dir_all(base).await;
    }

    fn only_content(bundle: &ContextBundle) -> &str {
        assert_eq!(bundle.blocks.len(), 1);
        bundle.blocks[0].content.as_str()
    }

    fn serialized_model_message_bytes(content: &str) -> usize {
        serde_json::to_vec(&context_message(content).unwrap())
            .unwrap()
            .len()
    }

    fn conservative_context_tokens(content: &str) -> u64 {
        u64::try_from(
            serialized_model_message_bytes(content)
                .checked_add(1)
                .unwrap()
                .div_ceil(4),
        )
        .unwrap()
    }

    fn request_tokens(request: &ModelRequest) -> u64 {
        u64::try_from(serde_json::to_vec(request).unwrap().len().div_ceil(4)).unwrap()
    }

    fn fixed_request_with_byte_residue(residue: usize) -> ModelRequest {
        for length in 1..=64 {
            let request = ModelRequest::new(
                vec![ModelMessage::system("f".repeat(length)).unwrap()],
                Vec::new(),
                ModelLimits::default(),
                ReasoningPreference::Auto,
            )
            .unwrap();
            if serde_json::to_vec(&request).unwrap().len() % 4 == residue {
                return request;
            }
        }
        panic!("could not construct fixed request byte residue {residue}");
    }

    fn request_with_context(fixed: &ModelRequest, content: &str) -> ModelRequest {
        let mut messages = fixed.messages().to_vec();
        messages.push(context_message(content).unwrap());
        ModelRequest::new(
            messages,
            fixed.tools().to_vec(),
            *fixed.limits(),
            fixed.reasoning(),
        )
        .unwrap()
    }

    fn assert_bundle_fits_full_request_budget(bundle: &ContextBundle, budget: u64) {
        for residue in 0..4 {
            let fixed = fixed_request_with_byte_residue(residue);
            let fixed_tokens = request_tokens(&fixed);
            let final_request = match bundle.blocks.as_slice() {
                [] => fixed.clone(),
                [block] => request_with_context(&fixed, block.content.as_str()),
                _ => panic!("ProjectContext returned more than one block"),
            };
            assert!(request_tokens(&final_request) <= fixed_tokens + budget);
        }
    }

    #[tokio::test]
    async fn missing_empty_and_present_have_stable_project_metadata_and_validate() {
        let (base, _, provider) = fixture("basic").await;
        assert!(
            provide(&provider, u64::MAX)
                .await
                .unwrap()
                .blocks
                .is_empty()
        );

        write_agents(&base, "").await;
        let empty = provide(&provider, context_budget_tokens("").unwrap())
            .await
            .unwrap();
        assert_eq!(only_content(&empty), "");

        write_agents(&base, "Build carefully.\nUse tests.\tOK").await;
        let bundle = provide(&provider, u64::MAX).await.unwrap();
        assert_eq!(bundle.blocks.len(), 1);
        let block = &bundle.blocks[0];
        assert_eq!(block.source.as_str(), SOURCE_ID);
        assert_eq!(block.slot, ContextSlot::ProjectInstructions);
        assert_eq!(block.priority, PRIORITY);
        assert_eq!(block.content.as_str(), "Build carefully.\nUse tests.\tOK");
        assert!(
            bundle
                .clone()
                .validate_and_sort(&SemanticLimits::default())
                .is_ok()
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn provider_rereads_each_call_and_ignores_nested_agents_files() {
        let (base, _, provider) = fixture("reread-root-only").await;
        write_agents(&base, "first").await;
        assert_eq!(
            only_content(&provide(&provider, u64::MAX).await.unwrap()),
            "first"
        );
        write_agents(&base, "second").await;
        assert_eq!(
            only_content(&provide(&provider, u64::MAX).await.unwrap()),
            "second"
        );

        tokio::fs::remove_file(base.join("root/AGENTS.md"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(base.join("root/nested"))
            .await
            .unwrap();
        tokio::fs::write(base.join("root/nested/AGENTS.md"), "nested")
            .await
            .unwrap();
        assert!(
            provide(&provider, u64::MAX)
                .await
                .unwrap()
                .blocks
                .is_empty()
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn large_file_uses_bounded_prefix_utf8_lookahead_and_marker() {
        let (base, _, provider) = fixture("large-prefix").await;
        let mut content = vec![b'a'; BoundedText::MAX_BYTES - 1];
        content.extend_from_slice("🙂tail".as_bytes());
        write_agents(&base, content).await;
        let bundle = provide(&provider, u64::MAX).await.unwrap();
        let content = only_content(&bundle);
        assert!(content.ends_with(TRUNCATED));
        assert!(!content.contains('�'));
        assert!(content.len() <= MAX_CONTEXT_CONTENT_BYTES);
        assert!(context_message(content).is_ok());
        assert!(bundle.validate_and_sort(&SemanticLimits::default()).is_ok());
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn zero_tiny_and_empty_file_budget_boundaries_return_empty() {
        let (base, _, provider) = fixture("tiny-budget").await;
        write_agents(&base, "content that requires truncation").await;
        assert!(provide(&provider, 0).await.unwrap().blocks.is_empty());
        let marker_budget = context_budget_tokens(TRUNCATED).unwrap();
        assert!(
            provide(&provider, marker_budget - 1)
                .await
                .unwrap()
                .blocks
                .is_empty()
        );

        write_agents(&base, "").await;
        let empty_budget = context_budget_tokens("").unwrap();
        assert_eq!(
            only_content(&provide(&provider, empty_budget).await.unwrap()),
            ""
        );
        assert!(
            provide(&provider, empty_budget - 1)
                .await
                .unwrap()
                .blocks
                .is_empty()
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn empty_full_marker_and_unicode_candidates_fit_every_fixed_request_residue() {
        let (base, _, provider) = fixture("full-request-residues").await;

        write_agents(&base, "").await;
        let budget = conservative_context_tokens("");
        let bundle = provide(&provider, budget).await.unwrap();
        assert_eq!(only_content(&bundle), "");
        assert_bundle_fits_full_request_budget(&bundle, budget);

        let full = "full project instructions";
        write_agents(&base, full).await;
        let budget = conservative_context_tokens(full);
        let bundle = provide(&provider, budget).await.unwrap();
        assert_eq!(only_content(&bundle), full);
        assert_bundle_fits_full_request_budget(&bundle, budget);

        let marker_source = "m".repeat(2_000);
        write_agents(&base, &marker_source).await;
        let marker_target = with_truncated_marker(&"m".repeat(24)).unwrap();
        let budget = conservative_context_tokens(&marker_target);
        let bundle = provide(&provider, budget).await.unwrap();
        assert!(only_content(&bundle).ends_with(TRUNCATED));
        assert_bundle_fits_full_request_budget(&bundle, budget);

        let unicode_source = "🙂".repeat(200);
        write_agents(&base, &unicode_source).await;
        let unicode_target = with_truncated_marker(&"🙂".repeat(20)).unwrap();
        let budget = conservative_context_tokens(&unicode_target);
        let bundle = provide(&provider, budget).await.unwrap();
        assert!(only_content(&bundle).ends_with(TRUNCATED));
        assert!(!only_content(&bundle).contains('�'));
        assert_bundle_fits_full_request_budget(&bundle, budget);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn exact_conservative_budget_keeps_full_content_and_minus_one_truncates() {
        let (base, _, provider) = fixture("exact-budget").await;
        let content = "a".repeat(2_000);
        write_agents(&base, &content).await;
        let exact = context_budget_tokens(&content).unwrap();
        assert_eq!(
            only_content(&provide(&provider, exact).await.unwrap()),
            content
        );

        let below = exact - 1;
        let truncated = provide(&provider, below).await.unwrap();
        let returned = only_content(&truncated);
        assert!(returned.ends_with(TRUNCATED));
        assert_ne!(returned, content);
        assert_bundle_fits_full_request_budget(&truncated, below);
        let body_len = returned.len() - "\n[truncated]".len();
        let next = with_truncated_marker(&content[..body_len + 1]).unwrap();
        assert!(context_budget_tokens(&next).unwrap() > below);
        let fixed = fixed_request_with_byte_residue(0);
        assert!(
            request_tokens(&request_with_context(&fixed, &next)) > request_tokens(&fixed) + below
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn comma_is_reserved_at_the_fixed_request_mod_four_zero_boundary() {
        let (base, _, provider) = fixture("comma-boundary").await;
        let content = (1..=64)
            .map(|length| "x".repeat(length))
            .find(|candidate| serialized_model_message_bytes(candidate) % 4 == 0)
            .unwrap();
        write_agents(&base, &content).await;

        let message_bytes = serialized_model_message_bytes(&content);
        let old_budget = u64::try_from(message_bytes / 4).unwrap();
        let fixed = fixed_request_with_byte_residue(0);
        assert!(
            request_tokens(&request_with_context(&fixed, &content))
                > request_tokens(&fixed) + old_budget
        );

        let bundle = provide(&provider, old_budget).await.unwrap();
        assert!(bundle.blocks.is_empty() || only_content(&bundle).ends_with(TRUNCATED));
        assert_bundle_fits_full_request_budget(&bundle, old_budget);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn quotes_backslashes_and_newlines_use_conservative_full_request_budget() {
        let (base, _, provider) = fixture("serialized-budget").await;
        let content = "quote=\"value\" path=C:\\tmp\nnext\n".repeat(80);
        write_agents(&base, &content).await;
        let exact = conservative_context_tokens(&content);
        assert_eq!(context_budget_tokens(&content).unwrap(), exact);
        assert!(
            exact > u64::try_from((CONTEXT_ENVELOPE.len() + content.len()).div_ceil(4)).unwrap()
        );
        assert_eq!(
            only_content(&provide(&provider, exact).await.unwrap()),
            content
        );
        let below = provide(&provider, exact - 1).await.unwrap();
        assert!(only_content(&below).ends_with(TRUNCATED));
        assert_bundle_fits_full_request_budget(&below, exact - 1);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn unicode_budget_truncation_stays_on_utf8_boundaries() {
        let (base, _, provider) = fixture("unicode-budget").await;
        let content = "🙂".repeat(200);
        write_agents(&base, &content).await;
        let target = with_truncated_marker(&"🙂".repeat(20)).unwrap();
        let budget = context_budget_tokens(&target).unwrap();
        let bundle = provide(&provider, budget).await.unwrap();
        let returned = only_content(&bundle);
        assert!(returned.ends_with(TRUNCATED));
        assert!(!returned.contains('�'));
        assert!(
            returned
                .strip_suffix(TRUNCATED)
                .unwrap()
                .trim_end_matches('\n')
                .chars()
                .all(|character| character == '🙂')
        );
        assert_bundle_fits_full_request_budget(&bundle, budget);
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn crlf_is_normalized_and_binary_or_unsafe_controls_are_unavailable() {
        let (base, _, provider) = fixture("text-validation").await;
        write_agents(&base, b"one\r\ntwo\r\n").await;
        assert_eq!(
            only_content(&provide(&provider, u64::MAX).await.unwrap()),
            "one\ntwo\n"
        );

        for bytes in [
            b"nul\0byte".as_slice(),
            &[0xff, b'a'][..],
            b"escape\x1bcontrol".as_slice(),
            b"bare\rreturn".as_slice(),
        ] {
            write_agents(&base, bytes).await;
            assert_eq!(
                provide(&provider, u64::MAX).await,
                Err(ContextError::Unavailable)
            );
        }
        cleanup(&base).await;
    }

    #[test]
    fn crlf_split_at_visible_cap_preserves_the_logical_lf() {
        for (bytes, visible_len, expected) in [
            (b"\n\r\n".as_slice(), 2, "\n\n"),
            (b"text\r\n".as_slice(), 5, "text\n"),
            ("🙂\r\n".as_bytes(), "🙂\r".len(), "🙂\n"),
            (
                "🙂\r\nnext\r\n".as_bytes(),
                "🙂\r\nnext\r".len(),
                "🙂\nnext\n",
            ),
        ] {
            let (content, truncated) = decode_text_prefix(ReadPrefix {
                bytes: bytes.to_vec(),
                visible_len,
                has_more: true,
            })
            .unwrap();
            assert_eq!(content, expected);
            assert!(truncated);
        }
    }

    #[tokio::test]
    async fn agents_directory_is_unavailable() {
        let (base, _, provider) = fixture("directory").await;
        tokio::fs::create_dir(base.join("root/AGENTS.md"))
            .await
            .unwrap();
        assert_eq!(
            provide(&provider, u64::MAX).await,
            Err(ContextError::Unavailable)
        );
        cleanup(&base).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inside_symlink_is_read_and_escape_symlink_is_unavailable() {
        use std::os::unix::fs::symlink;

        let (base, _, provider) = fixture("symlinks").await;
        tokio::fs::write(base.join("root/rules.md"), "inside")
            .await
            .unwrap();
        symlink("rules.md", base.join("root/AGENTS.md")).unwrap();
        assert_eq!(
            only_content(&provide(&provider, u64::MAX).await.unwrap()),
            "inside"
        );

        tokio::fs::remove_file(base.join("root/AGENTS.md"))
            .await
            .unwrap();
        tokio::fs::write(base.join("outside.md"), "outside")
            .await
            .unwrap();
        symlink(base.join("outside.md"), base.join("root/AGENTS.md")).unwrap();
        assert_eq!(
            provide(&provider, u64::MAX).await,
            Err(ContextError::Unavailable)
        );
        cleanup(&base).await;
    }

    #[tokio::test]
    async fn pre_cancel_deadline_and_slow_read_control_are_exact() {
        let (base, workspace, provider) = fixture("control").await;
        let deadline_provider = ProjectContext::new(workspace);
        write_agents(&base, "rules").await;

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            provider
                .provide(request_with_control(
                    u64::MAX,
                    cancelled,
                    Instant::now() - Duration::from_millis(1),
                ))
                .await,
            Err(ContextError::Cancelled)
        );
        assert_eq!(
            provider
                .provide(request_with_control(
                    u64::MAX,
                    CancellationToken::new(),
                    Instant::now() - Duration::from_millis(1),
                ))
                .await,
            Err(ContextError::DeadlineExceeded)
        );

        let gate = Arc::new(ContextReadGate::new(&provider));
        block_next_read(Arc::clone(&gate));
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            provider
                .provide(request_with_control(
                    u64::MAX,
                    task_cancellation,
                    Instant::now() + Duration::from_secs(5),
                ))
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        cancellation.cancel();
        assert_eq!(task.await.unwrap(), Err(ContextError::Cancelled));
        gate.release.add_permits(1);

        let gate = Arc::new(ContextReadGate::new(&deadline_provider));
        block_next_read(Arc::clone(&gate));
        let task = tokio::spawn(async move {
            deadline_provider
                .provide(request_with_control(
                    u64::MAX,
                    CancellationToken::new(),
                    Instant::now() + Duration::from_millis(100),
                ))
                .await
        });
        gate.started.acquire().await.unwrap().forget();
        assert_eq!(task.await.unwrap(), Err(ContextError::DeadlineExceeded));
        gate.release.add_permits(1);
        cleanup(&base).await;
    }
}
