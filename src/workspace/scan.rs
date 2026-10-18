//! Shared bounded traversal for the read-only Workspace listing queries.
//!
//! `workspace.files` and `workspace.search` walk the same ignore-filtered tree
//! through this module. The traversal is a depth-first `std::fs::read_dir`
//! walk: rules come from `ignore::gitignore` matchers built from bounded reads
//! of `.ignore`, `.gitignore`, and `.git/info/exclude`, never from a
//! hand-written pattern parser and never from the global git configuration.
//! Each page visits names after its path cursor; ancestor directories are
//! reopened only to descend to that position. Budgets count this page's visits,
//! content and rule bytes. Cancellation gives the blocking worker a bounded
//! join grace, so a stalled filesystem cannot hold session shutdown forever.

use std::ffi::OsStr;
use std::fs::{self as std_fs};
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ignore::Match as IgnoreMatch;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::workspace::Workspace;

/// Wall-clock budget for one listing or search query, matching the other
/// workspace queries. It is checked between bounded operations, so a single
/// blocking read on a stalled remote filesystem can outlast it.
pub(crate) const WORKSPACE_SCAN_DEADLINE: Duration = Duration::from_secs(10);
/// Largest accepted literal query or path filter.
pub(crate) const MAX_QUERY_BYTES: usize = 1024;
/// Largest accepted search `paths` list.
pub(crate) const MAX_SEARCH_PATHS: usize = 32;
/// Cursor scope fingerprints are truncated SHA-256 hex prefixes.
pub(crate) const SCOPE_HEX_LEN: usize = 16;

/// Entries one page may visit after its cursor.
const MAX_SCAN_ENTRIES: u64 = 100_000;
/// Path or content bytes one request may examine for results.
const MAX_SCAN_BYTES: u64 = 16 * 1024 * 1024;
/// Depth one recursive traversal may descend below its requested root.
const MAX_SCAN_DEPTH: usize = 64;
/// Ignore-rule budget: files, total bytes, and bytes read from one file.
const MAX_RULE_FILES: u64 = 128;
const MAX_RULE_BYTES: u64 = 1024 * 1024;
const MAX_RULE_FILE_BYTES: u64 = 256 * 1024;
/// Bytes requested from one blocking read, so cancellation and the deadline are
/// observed inside a large read instead of only around it.
pub(crate) const READ_CHUNK_BYTES: usize = 64 * 1024;

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

const IGNORE_FILES: [&str; 2] = [".ignore", ".gitignore"];
const GIT_EXCLUDE: &str = ".git/info/exclude";
/// Directory names a Workspace that is not a git repository excludes by
/// default. Local `.ignore` and `.gitignore` rules still apply and take
/// precedence over this list.
const DEFAULT_EXCLUDED_DIRECTORIES: &[&str] = &[
    "node_modules",
    "bower_components",
    "vendor",
    "target",
    "dist",
    "build",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".tox",
    ".gradle",
    ".next",
    ".nuxt",
];

/// The stop that ended a bounded scan.
///
/// `page`, `entries` and `bytes` can continue from the path cursor. `depth`
/// and `rules` describe incomplete traversal; rule exhaustion skips further
/// rule files without stopping the walk. `deadline` keeps partial results but
/// has no cursor. Cancellation fails the query instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceScanStop {
    End,
    Page,
    Entries,
    Bytes,
    Depth,
    Rules,
    Deadline,
}

/// How current a listing or search result is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceScanConsistency {
    /// A live observation of the tree, not a snapshot.
    Live,
}

/// Entry kind, reported without following symlinks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceFileKind {
    File,
    Directory,
    Symlink,
    Other,
}

/// Bounds one scan request enforces. Tests may lower them.
#[derive(Clone, Copy)]
pub(crate) struct ScanLimits {
    pub(crate) entries: u64,
    pub(crate) bytes: u64,
    pub(crate) depth: usize,
    pub(crate) rule_files: u64,
    pub(crate) rule_bytes: u64,
}

impl ScanLimits {
    pub(crate) const fn standard() -> Self {
        Self {
            entries: MAX_SCAN_ENTRIES,
            bytes: MAX_SCAN_BYTES,
            depth: MAX_SCAN_DEPTH,
            rule_files: MAX_RULE_FILES,
            rule_bytes: MAX_RULE_BYTES,
        }
    }
}

/// Live budget of one scan worker: cancellation, deadline, and the examined
/// entry, byte, and rule ceilings. One budget is shared by every root a query
/// walks, so the rule and entry ceilings are per query, not per root.
pub(crate) struct ScanBudget {
    limits: ScanLimits,
    entries: u64,
    bytes: u64,
    rule_files: u64,
    rule_bytes: u64,
    rules_exhausted: bool,
    stop: CancellationToken,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
    deadline: Instant,
    /// Set once a check observes the deadline, so the scan reports
    /// `WorkspaceScanStop::Deadline` and keeps its partial result.
    pub(super) expired: bool,
}

impl ScanBudget {
    /// True when the caller, its session, or the process cancelled the query.
    fn cancelled(&self) -> bool {
        self.stop.is_cancelled()
            || self.session_cancellation.is_cancelled()
            || self.shutdown_cancellation.is_cancelled()
    }

    /// True when the query deadline passed.
    pub(crate) fn time_expired(&self) -> bool {
        self.expired || Instant::now() >= self.deadline
    }

    /// Fails the query only when it was cancelled, which must not produce a
    /// partial result. A timeout is recorded instead, so the scan keeps what it
    /// already found and reports `WorkspaceScanStop::Deadline`. Called at every
    /// examined entry and read chunk, never only between phases.
    pub(crate) fn check(&mut self) -> Result<(), AgentError> {
        if self.cancelled() {
            return Err(AgentError::QueryLimit);
        }
        if self.time_expired() {
            self.expired = true;
        }
        Ok(())
    }

    pub(crate) fn limits(&self) -> ScanLimits {
        self.limits
    }

    /// Claims a new entry on this page; cursor ancestors are not charged.
    pub(crate) fn claim_entry(&mut self) -> bool {
        if self.entries >= self.limits.entries {
            return false;
        }
        self.entries += 1;
        true
    }

    /// Claims `bytes`; false once the byte ceiling would be exceeded.
    pub(crate) fn byte_available(&mut self, bytes: u64) -> bool {
        match self.bytes.checked_add(bytes) {
            Some(total) if total <= self.limits.bytes => {
                self.bytes = total;
                true
            }
            _ => false,
        }
    }

    /// Claims one ignore file; false once the rule-file ceiling is reached.
    pub(crate) fn claim_rule_file(&mut self) -> bool {
        if self.rule_files >= self.limits.rule_files {
            return false;
        }
        self.rule_files += 1;
        true
    }

    /// Charges bytes actually read from ignore files; false once the rule-byte
    /// ceiling would be exceeded.
    pub(crate) fn charge_rule_bytes(&mut self, bytes: u64) -> bool {
        match self.rule_bytes.checked_add(bytes) {
            Some(total) if total <= self.limits.rule_bytes => {
                self.rule_bytes = total;
                true
            }
            _ => false,
        }
    }
}

/// What a visitor wants after examining one entry.
pub(crate) enum Visit {
    /// Examined; continue with the next entry.
    Next,
    /// Stop after this entry; the cursor continues at the following entry.
    Stop(WorkspaceScanStop),
    /// Stop inside this entry; the cursor resumes it at `line`/`offset`.
    StopInside {
        reason: WorkspaceScanStop,
        line: u32,
        offset: u32,
    },
    /// Stop before examining this entry; the cursor re-examines it.
    StopBefore(WorkspaceScanStop),
}

/// One traversed entry, always relative to the Workspace root and valid UTF-8.
pub(crate) struct WalkEntry {
    pub(crate) relative: String,
    pub(crate) kind: WorkspaceFileKind,
    pub(crate) size: Option<u64>,
}

/// How one traversal was driven.
pub(crate) struct WalkOptions {
    pub(crate) recursive: bool,
    pub(crate) want_size: bool,
    /// Reject a requested root that is not a directory.
    pub(crate) directory_only: bool,
    pub(crate) resume_file: bool,
}

/// Result of one traversal of one requested root.
pub(crate) struct WalkOutcome {
    pub(crate) reason: WorkspaceScanStop,
    /// Last processed path, or the partially searched file when `resume` is set.
    pub(crate) next: Option<String>,
    /// Within-entry resume position when the scan stopped inside an entry.
    pub(crate) resume: Option<(u32, u32)>,
    /// Entries whose path is not valid UTF-8.
    pub(crate) unrepresentable: u64,
    /// Directories or entries the walker could not read or type.
    pub(crate) unreadable: u64,
    /// Explicitly requested roots excluded by the same rules as a walk.
    pub(crate) excluded: u64,
}

/// Rule categories, resolved in this order: every `.ignore` rule first, then
/// every `.gitignore` rule, then `.git/info/exclude`. Within a category the
/// closest directory wins.
#[derive(Clone, Copy)]
enum RuleCategory {
    Ignore,
    GitIgnore,
}

/// The rule files loaded from one directory.
struct DirRules {
    relative: PathBuf,
    dot_ignore: Option<Gitignore>,
    gitignore: Option<Gitignore>,
}

/// Walks one requested root with the shared rules.
///
/// Depth-first pre-order with sorted directory names. A resume descends along
/// `start_path` and skips earlier siblings without visiting them. `.git` is
/// never expanded, global ignore rules are never read, and symlinked directories
/// are never followed. Intermediate aliases use their resolved relative path.
pub(crate) fn walk_scope<F>(
    workspace: &Workspace,
    directory: &str,
    options: WalkOptions,
    start_path: Option<&str>,
    budget: &mut ScanBudget,
    visit: F,
) -> Result<WalkOutcome, AgentError>
where
    F: FnMut(&WalkEntry, &mut ScanBudget) -> Result<Visit, AgentError>,
{
    // A cancelled or expired query must not start canonicalizing, reading
    // rules, or walking anything.
    budget.check()?;
    let relative = crate::workspace::normalize_relative(directory, true)
        .map_err(|_| AgentError::InvalidArguments)?;
    let resolved = workspace
        .resolve_inside_sync(directory, true)
        .map_err(|_| AgentError::Workspace)?;
    let metadata = std_fs::symlink_metadata(&resolved).map_err(|_| AgentError::Workspace)?;
    let root_is_directory = metadata.is_dir();
    if !root_is_directory && options.directory_only {
        return Err(AgentError::Workspace);
    }
    // A requested root that is itself a symlinked directory is rejected rather
    // than silently expanded through the alias. A symlinked file root is read
    // through the Workspace boundary, which keeps the canonical check.
    let requested = workspace.root().join(&relative);
    let requested_symlink = std_fs::symlink_metadata(&requested)
        .map_err(|_| AgentError::Workspace)?
        .file_type()
        .is_symlink();
    if root_is_directory && requested_symlink {
        return Err(AgentError::Workspace);
    }
    let relative = resolved
        .strip_prefix(workspace.root())
        .map_err(|_| AgentError::Workspace)?
        .to_path_buf();
    // Display paths are also used to reopen search files. A lossy root could
    // redirect that read to a different, replacement-character sibling.
    relative.to_str().ok_or(AgentError::Workspace)?;
    let display = display_path(&relative);
    let is_repo = std_fs::symlink_metadata(workspace.root().join(".git")).is_ok();
    let limits = budget.limits();
    let mut walker = Walker {
        workspace,
        budget,
        visit,
        limits,
        options,
        is_repo,
        start_path: start_path.map(PathBuf::from),
        last: start_path.unwrap_or("").to_owned(),
        reason: WorkspaceScanStop::End,
        next: None,
        resume: None,
        unrepresentable: 0,
        unreadable: 0,
        excluded: 0,
        depth_capped: false,
        rules: Vec::new(),
        exclude: None,
    };
    // An already expired or cancelled budget stops before any rule is read, so
    // the scan reports a deadline instead of a false end.
    walker.check()?;
    if !walker.stopped() {
        walker.load_ancestor_rules(&relative, root_is_directory)?;
    }
    if !walker.stopped() {
        if root_is_directory {
            if walker.excluded_root(&relative, true) {
                walker.excluded += 1;
            } else {
                walker.walk_directory(&resolved, &relative, &display, 0, false)?;
            }
        } else {
            walker.visit_root_file(&resolved, &relative)?;
        }
    }
    Ok(walker.finish())
}

struct Walker<'a, F> {
    workspace: &'a Workspace,
    budget: &'a mut ScanBudget,
    visit: F,
    limits: ScanLimits,
    options: WalkOptions,
    is_repo: bool,
    start_path: Option<PathBuf>,
    last: String,
    reason: WorkspaceScanStop,
    next: Option<String>,
    resume: Option<(u32, u32)>,
    unrepresentable: u64,
    unreadable: u64,
    excluded: u64,
    depth_capped: bool,
    rules: Vec<DirRules>,
    exclude: Option<Gitignore>,
}

impl<F> Walker<'_, F>
where
    F: FnMut(&WalkEntry, &mut ScanBudget) -> Result<Visit, AgentError>,
{
    fn finish(mut self) -> WalkOutcome {
        // A deadline that passed even as the walk ended keeps the result
        // partial: an expired budget must not be reported as a clean end.
        if self.budget.time_expired() {
            self.reason = WorkspaceScanStop::Deadline;
            self.next = None;
            self.resume = None;
        }
        let reason = if self.reason == WorkspaceScanStop::End && self.budget.rules_exhausted {
            WorkspaceScanStop::Rules
        } else if self.reason == WorkspaceScanStop::End && self.depth_capped {
            WorkspaceScanStop::Depth
        } else {
            self.reason
        };
        WalkOutcome {
            reason,
            next: if reason == WorkspaceScanStop::End {
                None
            } else {
                self.next
            },
            resume: self.resume,
            unrepresentable: self.unrepresentable,
            unreadable: self.unreadable,
            excluded: self.excluded,
        }
    }

    /// Records a budget stop. A timeout keeps the partial result and never
    /// continues; cancellation is raised by `self.check`.
    fn check(&mut self) -> Result<(), AgentError> {
        self.budget.check()?;
        if self.budget.time_expired() {
            self.stop(WorkspaceScanStop::End, None);
        }
        Ok(())
    }

    fn stop(&mut self, reason: WorkspaceScanStop, next: Option<String>) {
        // A timeout outranks whichever bound noticed it and never continues: the
        // caller is asked to refine the request instead of resuming it.
        if self.budget.time_expired() {
            self.reason = WorkspaceScanStop::Deadline;
            self.next = None;
            self.resume = None;
            return;
        }
        if self.reason == WorkspaceScanStop::End {
            self.reason = reason;
            self.next = next.filter(|path| {
                self.resume.is_some()
                    || (!path.is_empty() && self.start_path.as_deref() != Some(Path::new(path)))
            });
        }
    }

    fn stopped(&self) -> bool {
        self.reason != WorkspaceScanStop::End
    }

    /// Loads rules for every directory between the Workspace root and the walk
    /// root, so a requested subdirectory inherits its ancestors' rules and an
    /// explicitly named path cannot bypass them.
    fn load_ancestor_rules(
        &mut self,
        relative: &Path,
        root_is_directory: bool,
    ) -> Result<(), AgentError> {
        let mut prefix = PathBuf::new();
        self.load_directory_rules(self.workspace.root(), &prefix)?;
        if self.stopped() {
            return Ok(());
        }
        // A directory root's own rules are loaded here; a file root has none.
        let limit = relative
            .components()
            .count()
            .saturating_sub(usize::from(!root_is_directory));
        for (index, component) in relative.components().enumerate() {
            if index >= limit {
                break;
            }
            prefix.push(component);
            let dir = self.workspace.root().join(&prefix);
            self.load_directory_rules(&dir, &prefix)?;
            if self.stopped() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Loads complete rule files within the remaining budget.
    fn load_directory_rules(&mut self, dir: &Path, relative: &Path) -> Result<bool, AgentError> {
        let dot_ignore = self.load_rules_file(&dir.join(IGNORE_FILES[0]), dir)?;
        if self.stopped() {
            return Ok(false);
        }
        let gitignore = self.load_rules_file(&dir.join(IGNORE_FILES[1]), dir)?;
        if self.stopped() {
            return Ok(false);
        }
        if relative.as_os_str().is_empty() && self.is_repo {
            self.exclude = self.load_rules_file(&dir.join(GIT_EXCLUDE), dir)?;
            if self.stopped() {
                return Ok(false);
            }
        }
        if dot_ignore.is_none() && gitignore.is_none() {
            return Ok(false);
        }
        self.rules.push(DirRules {
            relative: relative.to_path_buf(),
            dot_ignore,
            gitignore,
        });
        Ok(true)
    }

    /// Reads one ignore file inside the Workspace with the shared rule budget.
    ///
    /// A missing file simply contributes no rules. A file that exists but
    /// cannot be read, or that resolves outside the Workspace, is counted as
    /// unreadable so the response is not reported as complete, and bytes read
    /// are charged in chunks as they are read, so a growing or oversized file
    /// cannot spend the budget uncharged.
    fn load_rules_file(
        &mut self,
        path: &Path,
        base: &Path,
    ) -> Result<Option<Gitignore>, AgentError> {
        self.check()?;
        if self.stopped() || self.budget.rules_exhausted {
            return Ok(None);
        }
        let metadata = match std_fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(None);
            }
            Err(_) => {
                self.unreadable += 1;
                return Ok(None);
            }
        };
        let path = if metadata.file_type().is_symlink() {
            match std_fs::canonicalize(path) {
                Ok(canonical) if canonical.starts_with(self.workspace.root()) => canonical,
                Ok(_) => {
                    self.unreadable += 1;
                    return Ok(None);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(_) => {
                    self.unreadable += 1;
                    return Ok(None);
                }
            }
        } else {
            path.to_path_buf()
        };
        if !std_fs::symlink_metadata(&path)
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
        {
            return Ok(None);
        }
        if !self.budget.claim_rule_file() {
            self.budget.rules_exhausted = true;
            self.unreadable += 1;
            return Ok(None);
        }
        let Some(text) = self.read_bounded(&path)? else {
            return Ok(None);
        };
        let mut builder = GitignoreBuilder::new(base);
        for line in text.lines() {
            // An invalid pattern is ignored, exactly as git ignores it.
            let _ = builder.add_line(Some(path.clone()), line);
        }
        match builder.build() {
            Ok(matcher) => Ok(Some(matcher)),
            Err(_) => {
                self.unreadable += 1;
                Ok(None)
            }
        }
    }

    /// Reads a bounded UTF-8 ignore file in chunks, charging the shared rule
    /// byte budget for what is actually read and observing cancellation and the
    /// deadline between reads.
    fn read_bounded(&mut self, path: &Path) -> Result<Option<String>, AgentError> {
        let mut file = match crate::workspace::open_read_only_sync(path) {
            Ok(file) => file,
            Err(_) => {
                self.unreadable += 1;
                return Ok(None);
            }
        };
        let mut bytes = Vec::new();
        let mut chunk = [0u8; READ_CHUNK_BYTES];
        loop {
            self.check()?;
            if self.stopped() {
                break;
            }
            let read = match file.read(&mut chunk) {
                Ok(read) => read,
                Err(_) => {
                    self.unreadable += 1;
                    return Ok(None);
                }
            };
            if read == 0 {
                break;
            }
            let charged = u64::try_from(read).unwrap_or(u64::MAX);
            let read_cap = usize::try_from(MAX_RULE_FILE_BYTES).unwrap_or(usize::MAX);
            if !self.budget.charge_rule_bytes(charged) || bytes.len() + read > read_cap {
                // Never apply a partial rule file; continue the walk without
                // loading further rules and report incomplete coverage.
                self.budget.rules_exhausted = true;
                self.unreadable += 1;
                return Ok(None);
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        match String::from_utf8(bytes) {
            Ok(text) => Ok(Some(text)),
            Err(_) => {
                self.unreadable += 1;
                Ok(None)
            }
        }
    }

    /// True when `relative` is excluded by the loaded rules or by the default
    /// non-repository exclusions. `name` is its final component.
    ///
    /// Every rule set only ever matches a strict descendant of the directory it
    /// was loaded from, so a rule from one directory cannot leak onto an
    /// ancestor or a sibling, and a requested root is never filtered by its own
    /// rule file.
    fn excluded(&self, relative: &Path, name: &OsStr, is_dir: bool) -> bool {
        if name == OsStr::new(".git") {
            return true;
        }
        for category in [RuleCategory::Ignore, RuleCategory::GitIgnore] {
            if let Some(excluded) = self.decide(relative, is_dir, category) {
                return excluded;
            }
        }
        if let Some(exclude) = &self.exclude {
            match exclude.matched(relative, is_dir) {
                IgnoreMatch::Ignore(_) => return true,
                IgnoreMatch::Whitelist(_) => return false,
                IgnoreMatch::None => {}
            }
        }
        if !self.is_repo
            && is_dir
            && DEFAULT_EXCLUDED_DIRECTORIES
                .iter()
                .any(|candidate| name == OsStr::new(candidate))
        {
            return true;
        }
        false
    }

    /// Resolves one rule category, closest directory first.
    fn decide(&self, relative: &Path, is_dir: bool, category: RuleCategory) -> Option<bool> {
        for rules in self.rules.iter().rev() {
            let Ok(candidate) = relative.strip_prefix(&rules.relative) else {
                continue;
            };
            if candidate.as_os_str().is_empty() {
                continue;
            }
            let matcher = match category {
                RuleCategory::Ignore => rules.dot_ignore.as_ref(),
                RuleCategory::GitIgnore => rules.gitignore.as_ref(),
            };
            let Some(matcher) = matcher else {
                continue;
            };
            match matcher.matched(candidate, is_dir) {
                IgnoreMatch::Ignore(_) => return Some(true),
                IgnoreMatch::Whitelist(_) => return Some(false),
                IgnoreMatch::None => {}
            }
        }
        None
    }

    /// True when an explicitly requested root is excluded, checking every
    /// ancestor directory first so an explicit path cannot bypass the rules a
    /// walk from the Workspace root would apply.
    fn excluded_root(&self, relative: &Path, is_dir: bool) -> bool {
        let mut prefix = PathBuf::new();
        for component in relative.components() {
            prefix.push(component);
            let last = prefix.as_path() == relative;
            if self.excluded(&prefix, component.as_os_str(), !last || is_dir) {
                return true;
            }
        }
        false
    }

    /// Walks one directory: one raw entry at a time, charging the entry ceiling
    /// and the resume position before anything else.
    fn walk_directory(
        &mut self,
        dir: &Path,
        relative: &Path,
        display: &str,
        depth: usize,
        load_rules: bool,
    ) -> Result<(), AgentError> {
        let pushed_rules = if load_rules {
            self.load_directory_rules(dir, relative)?
        } else {
            false
        };
        let walked = self.walk_entries(dir, relative, display, depth);
        if pushed_rules {
            self.rules.pop();
        }
        walked
    }

    fn walk_entries(
        &mut self,
        dir: &Path,
        relative: &Path,
        display: &str,
        depth: usize,
    ) -> Result<(), AgentError> {
        if self.stopped() {
            return Ok(());
        }
        let entries = match std_fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => {
                self.unreadable += 1;
                if !self.budget.claim_entry() {
                    self.stop(WorkspaceScanStop::Entries, None);
                }
                return Ok(());
            }
        };
        // Keep only this page's earliest names plus lookahead and the cursor
        // ancestor. A huge directory must not require an unbounded allocation.
        let capacity = self.limits.entries.saturating_sub(self.budget.entries) as usize + 2;
        let mut sorted = std::collections::BTreeMap::new();
        for entry in entries {
            self.check()?;
            if self.stopped() {
                return Ok(());
            }
            match entry {
                Ok(entry) => {
                    let name = entry.file_name();
                    let child = relative.join(&name);
                    if self
                        .start_path
                        .as_ref()
                        .is_some_and(|start| child < *start && !start.starts_with(&child))
                    {
                        continue;
                    }
                    sorted.insert(name, entry);
                    if sorted.len() > capacity {
                        sorted.pop_last();
                    }
                }
                Err(_) => self.unreadable += 1,
            }
        }
        for entry in sorted.into_values() {
            self.check()?;
            if self.stopped() {
                break;
            }
            let child = relative.join(entry.file_name());
            let positioning = self.start_path.as_ref().is_some_and(|start| {
                child < *start || (child == *start && !self.options.resume_file)
            });
            if positioning
                && !self
                    .start_path
                    .as_ref()
                    .is_some_and(|start| start.starts_with(&child))
            {
                continue;
            }
            if !positioning && !self.budget.claim_entry() {
                self.stop(WorkspaceScanStop::Entries, Some(self.last.clone()));
                break;
            }
            self.examine(relative, display, depth, &entry, positioning)?;
        }
        Ok(())
    }

    /// Classifies one raw entry, applies the rules, visits it, and descends
    /// into it when the visitor did not stop the scan.
    fn examine(
        &mut self,
        relative: &Path,
        display: &str,
        depth: usize,
        entry: &std_fs::DirEntry,
        positioning: bool,
    ) -> Result<(), AgentError> {
        let name = entry.file_name();
        let absolute = entry.path();
        let mut child_relative = relative.to_path_buf();
        child_relative.push(&name);
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => match std_fs::symlink_metadata(&absolute) {
                Ok(metadata) => metadata.file_type(),
                Err(_) => {
                    self.unreadable += 1;
                    if !positioning && name.to_str().is_some() {
                        self.last = display_path(&child_relative);
                    }
                    return Ok(());
                }
            },
        };
        let is_dir = file_type.is_dir();
        if self.excluded(&child_relative, &name, is_dir) {
            if !positioning && name.to_str().is_some() {
                self.last = display_path(&child_relative);
            }
            return Ok(());
        }
        let Some(part) = name.to_str() else {
            self.unrepresentable += 1;
            return Ok(());
        };
        let mut child_display = display.to_owned();
        if !child_display.is_empty() {
            child_display.push('/');
        }
        child_display.push_str(part);
        let kind = entry_kind(file_type);
        let size = if !positioning && self.options.want_size && kind == WorkspaceFileKind::File {
            std_fs::symlink_metadata(&absolute)
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.len())
        } else {
            None
        };
        let child_depth = depth + 1;
        let view = WalkEntry {
            relative: child_display.clone(),
            kind,
            size,
        };
        if !positioning {
            let visit = (self.visit)(&view, self.budget)?;
            self.apply_visit(visit, &child_display);
        }
        if is_dir && self.options.recursive && !self.stopped() {
            if child_depth >= self.limits.depth {
                self.depth_capped = true;
            } else {
                self.walk_directory(
                    &absolute,
                    &child_relative,
                    &child_display,
                    child_depth,
                    true,
                )?;
            }
        }
        Ok(())
    }

    fn apply_visit(&mut self, visit: Visit, path: &str) {
        match visit {
            Visit::Next => self.last = path.to_owned(),
            Visit::Stop(reason) => self.stop(reason, Some(path.to_owned())),
            Visit::StopBefore(reason) => self.stop(reason, Some(self.last.clone())),
            Visit::StopInside {
                reason,
                line,
                offset,
            } => {
                self.resume = Some((line, offset));
                self.stop(reason, Some(path.to_owned()));
            }
        }
    }

    /// Visits a requested root that is a single file.
    fn visit_root_file(&mut self, absolute: &Path, relative: &Path) -> Result<(), AgentError> {
        if self.start_path.as_ref().is_some_and(|start| {
            relative < start.as_path() || (relative == start && !self.options.resume_file)
        }) {
            return Ok(());
        }
        if !self.budget.claim_entry() {
            self.stop(WorkspaceScanStop::Entries, None);
            return Ok(());
        }
        if self.excluded_root(relative, false) {
            self.excluded += 1;
            return Ok(());
        }
        let metadata = match std_fs::symlink_metadata(absolute) {
            Ok(metadata) => metadata,
            Err(_) => {
                self.unreadable += 1;
                return Ok(());
            }
        };
        let kind = entry_kind(metadata.file_type());
        let view = WalkEntry {
            relative: display_path(relative),
            kind,
            size: if self.options.want_size && kind == WorkspaceFileKind::File {
                Some(metadata.len())
            } else {
                None
            },
        };
        let visit = (self.visit)(&view, self.budget)?;
        self.apply_visit(visit, &view.relative);
        Ok(())
    }
}

fn entry_kind(file_type: std_fs::FileType) -> WorkspaceFileKind {
    if file_type.is_dir() {
        WorkspaceFileKind::Directory
    } else if file_type.is_file() {
        WorkspaceFileKind::File
    } else if file_type.is_symlink() {
        WorkspaceFileKind::Symlink
    } else {
        WorkspaceFileKind::Other
    }
}

/// Runs one scan on a retained blocking worker.
///
/// Cancellation is checked between filesystem operations. A cancelled awaiter
/// gives the worker a short grace, then detaches with a warning if an operation
/// is still stuck. Dropping the awaiter also signals cancellation.
pub(crate) async fn run_scan<T, F>(
    workspace: Arc<Workspace>,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
    scan: F,
) -> Result<T, AgentError>
where
    T: Send + 'static,
    F: FnOnce(&Workspace, &mut ScanBudget) -> Result<T, AgentError> + Send + 'static,
{
    let deadline = Instant::now()
        .checked_add(workspace.scan_deadline)
        .unwrap_or_else(Instant::now);
    let stop = CancellationToken::new();
    let _guard = ScanGuard { stop: stop.clone() };
    let mut handle = {
        let workspace = Arc::clone(&workspace);
        let worker_stop = stop.clone();
        let worker_session = session_cancellation.clone();
        let worker_shutdown = shutdown_cancellation.clone();
        tokio::task::spawn_blocking(move || {
            let mut budget = ScanBudget {
                limits: workspace.scan_limits,
                entries: 0,
                bytes: 0,
                rule_files: 0,
                rule_bytes: 0,
                rules_exhausted: false,
                stop: worker_stop,
                session_cancellation: worker_session,
                shutdown_cancellation: worker_shutdown,
                deadline,
                expired: false,
            };
            #[cfg(test)]
            let _exit = tests::hold_worker(workspace.root(), &budget);
            budget.check()?;
            scan(&workspace, &mut budget)
        })
    };
    // The worker applies the deadline between bounded operations and returns its
    // partial result, so joining it is what preserves that result. Only session
    // or caller cancellation ends the query with an error.
    let joined = tokio::select! {
        biased;
        _ = session_cancellation.cancelled() => None,
        _ = shutdown_cancellation.cancelled() => None,
        result = &mut handle => Some(result),
    };
    let outcome = match joined {
        Some(result) => result,
        None => {
            stop.cancel();
            if tokio::time::timeout(super::WORKSPACE_CANCEL_GRACE, &mut handle)
                .await
                .is_err()
            {
                tracing::warn!(
                    "detaching cancelled workspace scan: worker did not stop within grace"
                );
            }
            return Err(AgentError::QueryLimit);
        }
    };
    match outcome {
        Ok(result) => result,
        Err(_) => Err(AgentError::Internal),
    }
}

/// Stops the scan worker when the awaiting future is dropped.
struct ScanGuard {
    stop: CancellationToken,
}

impl Drop for ScanGuard {
    fn drop(&mut self) {
        self.stop.cancel();
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

#[cfg(test)]
pub(crate) static SCAN_HOLDS: crate::test_support::Registry<PathBuf, Arc<tests::ScanHold>> =
    crate::test_support::Registry::new();
#[cfg(test)]
pub(crate) static SCAN_RESULT_EXPIRIES: crate::test_support::Registry<PathBuf> =
    crate::test_support::Registry::new();

#[cfg(test)]
pub(crate) mod tests;
