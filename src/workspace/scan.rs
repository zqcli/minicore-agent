//! Shared bounded traversal for the read-only Workspace listing queries.
//!
//! `workspace.files` and `workspace.search` walk the same ignore-filtered tree
//! through this module. The traversal is a depth-first `std::fs::read_dir`
//! walk: rules come from `ignore::gitignore` matchers built from bounded reads
//! of `.ignore`, `.gitignore`, and `.git/info/exclude`, never from a
//! hand-written pattern parser and never from the global git configuration.
//! Nothing here trusts the tree to bound its own work: every raw directory
//! entry, every positioning step, every unreadable path, and every rule byte is
//! charged against the request budget, checked against the deadline and
//! cancellation as it happens, and reported through an explicit stop reason
//! instead of a guessed total. One traversal runs on a single blocking worker
//! per query, which is always joined before the query returns.

use std::ffi::OsStr;
use std::fs::{self as std_fs};
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
#[cfg(test)]
use std::sync::{Condvar, Mutex, OnceLock};
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

/// Raw entries one request may consume, including entries consumed while
/// positioning at a cursor.
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
/// `end` reached the end of the requested scope, and `page`, `entries`, `bytes`,
/// `depth`, `rules`, and `deadline` are bounds that cut it short. `page` and
/// `bytes` continue from the reported cursor; `entries` stops without a
/// continuation because a resumed request would spend its whole budget reaching
/// the same ceiling; `depth` and `rules` have nothing further to report;
/// `deadline` keeps the partial result, returns no cursor, and asks for a
/// narrower request or a later retry. Cancellation is never a stop: it fails the
/// query instead.
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
    stop: CancellationToken,
    session_cancellation: CancellationToken,
    shutdown_cancellation: CancellationToken,
    deadline: Instant,
    /// Set once a check observes the deadline, so the scan reports
    /// `WorkspaceScanStop::Deadline` and keeps its partial result.
    expired: bool,
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

    /// True when the query was cancelled or its deadline passed. Only the test
    /// hold consults it; production stops go through `check` and `time_expired`.
    #[cfg(test)]
    pub(crate) fn stopped(&self) -> bool {
        self.cancelled() || self.time_expired()
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

    /// Marks this budget as timed out so the rest of the scan behaves as if the
    /// deadline had passed. Tests place the timeout at a chosen point with it.
    #[cfg(test)]
    pub(crate) fn expire_now(&mut self) {
        self.expired = true;
    }

    pub(crate) fn limits(&self) -> ScanLimits {
        self.limits
    }

    /// Claims one raw entry, including entries consumed while positioning at a
    /// cursor. False once the entry ceiling is reached.
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
}

/// Result of one traversal of one requested root.
pub(crate) struct WalkOutcome {
    pub(crate) reason: WorkspaceScanStop,
    /// Continuation ordinal, or `None` when the scan cannot continue: the stop
    /// allows no continuation, or the entry budget ran out before the requested
    /// position was reached.
    pub(crate) next: Option<u64>,
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
/// The traversal is a depth-first pre-order over the rule-filtered tree in the
/// filesystem's directory order. `start_entry` positions a resumed request by
/// ordinal; that positioning is charged against the entry ceiling exactly like
/// examined entries, and when it cannot finish the scan stops without a
/// continuation instead of returning a cursor that would not advance. `.git`
/// metadata is never returned or expanded, `.ignore`, `.gitignore`, and
/// `.git/info/exclude` inside the Workspace are applied, and the global git
/// configuration is never read. Symlinked directories are never followed or
/// expanded, including when one is the requested root.
pub(crate) fn walk_scope<F>(
    workspace: &Workspace,
    directory: &str,
    options: WalkOptions,
    start_entry: u64,
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
        local: 0,
        start_entry,
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
    /// Ordinal of the next raw entry in this walk, starting at zero. The cursor
    /// is scoped to one requested root, so this is not the query-wide entry
    /// count that `ScanBudget` keeps.
    local: u64,
    start_entry: u64,
    reason: WorkspaceScanStop,
    next: Option<u64>,
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
        let reason = if self.reason == WorkspaceScanStop::End && self.depth_capped {
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

    fn stop(&mut self, reason: WorkspaceScanStop, next: Option<u64>) {
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
            self.next = next;
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

    /// Loads this directory's own rule files, returning whether a rule set was
    /// pushed. It stops the traversal instead of applying partial rules when
    /// the rule budget is exhausted.
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
        if self.stopped() {
            return Ok(None);
        }
        let metadata = match std_fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
            self.stop(WorkspaceScanStop::Rules, None);
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
                // Applying part of a rule file would silently change what the
                // scan sees, so the traversal stops and says so instead.
                self.stop(WorkspaceScanStop::Rules, None);
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
        let mut entries = match std_fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => {
                self.unreadable += 1;
                if !self.budget.claim_entry() {
                    self.stop(WorkspaceScanStop::Entries, None);
                }
                return Ok(());
            }
        };
        while !self.stopped() {
            self.check()?;
            if self.stopped() {
                break;
            }
            let Some(item) = entries.next() else {
                break;
            };
            if !self.budget.claim_entry() {
                self.stop(WorkspaceScanStop::Entries, None);
                break;
            }
            let ordinal = self.local;
            self.local += 1;
            // A resumed request replays every raw entry before its cursor,
            // including ignored ones, without asking the visitor and without
            // emitting results, but it still descends into directories so the
            // ordinals inside a subtree line up with the first page.
            let positioning = ordinal < self.start_entry;
            let entry = match item {
                Ok(entry) => entry,
                Err(_) => {
                    self.unreadable += 1;
                    continue;
                }
            };
            let next = self.examine(relative, display, depth, &entry, positioning)?;
            if positioning {
                continue;
            }
            if let Some(visit) = next {
                match visit {
                    Visit::Next => {}
                    Visit::Stop(reason) => {
                        self.stop(reason, Some(ordinal + 1));
                        break;
                    }
                    Visit::StopBefore(reason) => {
                        self.stop(reason, Some(ordinal));
                        break;
                    }
                    Visit::StopInside {
                        reason,
                        line,
                        offset,
                    } => {
                        self.resume = Some((line, offset));
                        self.stop(reason, Some(ordinal));
                        break;
                    }
                }
            }
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
    ) -> Result<Option<Visit>, AgentError> {
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
                    return Ok(None);
                }
            },
        };
        let is_dir = file_type.is_dir();
        if self.excluded(&child_relative, &name, is_dir) {
            return Ok(None);
        }
        let Some(part) = name.to_str() else {
            self.unrepresentable += 1;
            return Ok(None);
        };
        let mut child_display = display.to_owned();
        if !child_display.is_empty() {
            child_display.push('/');
        }
        child_display.push_str(part);
        let kind = if is_dir {
            WorkspaceFileKind::Directory
        } else if file_type.is_file() {
            WorkspaceFileKind::File
        } else if file_type.is_symlink() {
            WorkspaceFileKind::Symlink
        } else {
            WorkspaceFileKind::Other
        };
        let size = if self.options.want_size && kind == WorkspaceFileKind::File {
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
        let stop = if positioning {
            None
        } else {
            match (self.visit)(&view, self.budget)? {
                Visit::Next => None,
                stopped => Some(stopped),
            }
        };
        if is_dir && self.options.recursive && stop.is_none() {
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
        Ok(stop)
    }

    /// Visits a requested root that is a single file.
    fn visit_root_file(&mut self, absolute: &Path, relative: &Path) -> Result<(), AgentError> {
        if !self.budget.claim_entry() {
            self.stop(WorkspaceScanStop::Entries, None);
            return Ok(());
        }
        let ordinal = self.local;
        self.local += 1;
        if ordinal < self.start_entry {
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
        let kind = if metadata.is_file() {
            WorkspaceFileKind::File
        } else if metadata.file_type().is_symlink() {
            WorkspaceFileKind::Symlink
        } else {
            WorkspaceFileKind::Other
        };
        let view = WalkEntry {
            relative: display_path(relative),
            kind,
            size: if self.options.want_size && kind == WorkspaceFileKind::File {
                Some(metadata.len())
            } else {
                None
            },
        };
        match (self.visit)(&view, self.budget)? {
            Visit::Next => {}
            Visit::Stop(reason) => self.stop(reason, Some(ordinal + 1)),
            Visit::StopBefore(reason) => self.stop(reason, Some(ordinal)),
            Visit::StopInside {
                reason,
                line,
                offset,
            } => {
                self.resume = Some((line, offset));
                self.stop(reason, Some(ordinal));
            }
        }
        Ok(())
    }
}

/// Runs one scan on a retained blocking worker.
///
/// The worker observes cancellation and the deadline at every examined entry
/// and read chunk, so it exits on its own. When the awaiter observes
/// cancellation or the deadline first it stops the worker and joins it before
/// returning. A drop guard also stops the worker when the awaiting future is
/// dropped by its caller, so a dropped query never leaves a worker walking a
/// large tree.
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
        .checked_add(scan_deadline(workspace.root()))
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
                limits: scan_limits(workspace.root()),
                entries: 0,
                bytes: 0,
                rule_files: 0,
                rule_bytes: 0,
                stop: worker_stop,
                session_cancellation: worker_session,
                shutdown_cancellation: worker_shutdown,
                deadline,
                expired: false,
            };
            #[cfg(test)]
            let held = take_scan_hold(workspace.root());
            // Installed before the gate, so the exit signal is dropped even when
            // the worker returns before reaching the scan closure.
            #[cfg(test)]
            let _exit = ScanExit::install(held.as_ref());
            #[cfg(test)]
            if let Some(hold) = held.as_ref() {
                hold.block(&budget);
            }
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
            let _ = handle.await;
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

/// Exact encoded size of one record, used to keep every page inside its
/// `max_bytes` budget.
pub(crate) fn encoded_len<T: Serialize>(record: &T) -> Result<usize, AgentError> {
    serde_json::to_vec(record)
        .map(|encoded| encoded.len())
        .map_err(|_| AgentError::Internal)
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

/// Largest character-boundary prefix of `text` that is at most `index` bytes.
pub(crate) fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
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
static SCAN_LIMITS_OVERRIDES: OnceLock<Mutex<Vec<(PathBuf, ScanLimits)>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn set_scan_limits(root: PathBuf, limits: ScanLimits) {
    let overrides = SCAN_LIMITS_OVERRIDES.get_or_init(|| Mutex::new(Vec::new()));
    let mut overrides = overrides.lock().unwrap();
    match overrides.iter_mut().find(|(path, _)| *path == root) {
        Some(entry) => entry.1 = limits,
        None => overrides.push((root, limits)),
    }
}

#[cfg(test)]
fn scan_limits(root: &Path) -> ScanLimits {
    if let Some(overrides) = SCAN_LIMITS_OVERRIDES.get() {
        if let Some((_, limits)) = overrides
            .lock()
            .unwrap()
            .iter()
            .find(|(path, _)| path.as_path() == root)
        {
            return *limits;
        }
    }
    ScanLimits::standard()
}

#[cfg(not(test))]
fn scan_limits(_root: &Path) -> ScanLimits {
    ScanLimits::standard()
}

#[cfg(test)]
pub(crate) struct ScanHold {
    started: AtomicBool,
    exited: AtomicBool,
    released: Mutex<bool>,
    release: Condvar,
}

#[cfg(test)]
impl ScanHold {
    pub(crate) fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            exited: AtomicBool::new(false),
            released: Mutex::new(false),
            release: Condvar::new(),
        }
    }

    fn mark_exited(&self) {
        self.exited.store(true, AtomicOrdering::SeqCst);
    }

    /// Whether the blocking worker left its scope, whether it ran the scan, was
    /// cancelled, or panicked.
    pub(crate) fn exited(&self) -> bool {
        self.exited.load(AtomicOrdering::SeqCst)
    }

    /// Blocks the worker until the test releases it, exiting early when the
    /// query is cancelled or the deadline passes.
    pub(crate) fn block(&self, budget: &ScanBudget) {
        self.started.store(true, AtomicOrdering::SeqCst);
        let mut released = self.released.lock().unwrap();
        while !*released && !budget.stopped() {
            let (next, timeout) = self
                .release
                .wait_timeout(released, Duration::from_millis(20))
                .unwrap();
            released = next;
            if timeout.timed_out() && budget.stopped() {
                break;
            }
        }
    }

    pub(crate) async fn wait_started(&self) {
        while !self.started.load(AtomicOrdering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

/// Marks the gated worker as gone when its scope ends, for every early return,
/// successful finish, and panic.
#[cfg(test)]
struct ScanExit(Arc<ScanHold>);

#[cfg(test)]
impl ScanExit {
    fn install(hold: Option<&Arc<ScanHold>>) -> Option<Self> {
        hold.map(|hold| Self(Arc::clone(hold)))
    }
}

#[cfg(test)]
impl Drop for ScanExit {
    fn drop(&mut self) {
        self.0.mark_exited();
    }
}

#[cfg(test)]
type ScanHolds = Mutex<Vec<(PathBuf, Arc<ScanHold>)>>;

#[cfg(test)]
type ScanDeadlines = Mutex<Vec<(PathBuf, Duration)>>;

#[cfg(test)]
static SCAN_DEADLINE_OVERRIDES: OnceLock<ScanDeadlines> = OnceLock::new();

/// Makes every scan of `root` use `deadline` as its wall-clock budget.
#[cfg(test)]
pub(crate) fn set_scan_deadline(root: PathBuf, deadline: Duration) {
    let overrides = SCAN_DEADLINE_OVERRIDES.get_or_init(|| Mutex::new(Vec::new()));
    let mut overrides = overrides.lock().unwrap();
    match overrides.iter_mut().find(|(path, _)| *path == root) {
        Some(entry) => entry.1 = deadline,
        None => overrides.push((root, deadline)),
    }
}

#[cfg(test)]
fn scan_deadline(root: &Path) -> Duration {
    if let Some(overrides) = SCAN_DEADLINE_OVERRIDES.get() {
        if let Some((_, deadline)) = overrides
            .lock()
            .unwrap()
            .iter()
            .find(|(path, _)| path.as_path() == root)
        {
            return *deadline;
        }
    }
    WORKSPACE_SCAN_DEADLINE
}

#[cfg(not(test))]
fn scan_deadline(_root: &Path) -> Duration {
    WORKSPACE_SCAN_DEADLINE
}

#[cfg(test)]
static SCAN_RESULT_EXPIRIES: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();

/// Makes the next scan of `root` observe the deadline as soon as it has
/// committed its first result, so tests can place a timeout mid-scan without
/// waiting for real time to pass.
#[cfg(test)]
pub(crate) fn expire_scan_after_result(root: PathBuf) {
    SCAN_RESULT_EXPIRIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(root);
}

#[cfg(test)]
pub(crate) fn scan_expiry_after_result(root: &Path) -> bool {
    SCAN_RESULT_EXPIRIES.get().is_some_and(|roots| {
        roots
            .lock()
            .unwrap()
            .iter()
            .any(|path| path.as_path() == root)
    })
}

#[cfg(test)]
static SCAN_HOLDS: OnceLock<ScanHolds> = OnceLock::new();

#[cfg(test)]
pub(crate) fn hold_next_scan(root: PathBuf, hold: Arc<ScanHold>) {
    SCAN_HOLDS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push((root, hold));
}

#[cfg(test)]
fn take_scan_hold(root: &Path) -> Option<Arc<ScanHold>> {
    let holds = SCAN_HOLDS.get()?;
    let mut holds = holds.lock().unwrap();
    let index = holds.iter().position(|(path, _)| path.as_path() == root)?;
    Some(holds.remove(index).1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SessionId;

    async fn fixture(label: &str) -> (Arc<Workspace>, PathBuf) {
        let base = std::env::temp_dir().join(format!(
            "minicore-workspace-scan-{label}-{}",
            SessionId::new().unwrap()
        ));
        let root = base.join("root");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let workspace = Arc::new(Workspace::open(root).await.unwrap());
        (workspace, base)
    }

    fn write(workspace: &Workspace, path: &str, bytes: &[u8]) {
        let full = workspace.root().join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, bytes).unwrap();
    }

    fn scan_budget(workspace: &Workspace) -> ScanBudget {
        ScanBudget {
            limits: scan_limits(workspace.root()),
            entries: 0,
            bytes: 0,
            rule_files: 0,
            rule_bytes: 0,
            stop: CancellationToken::new(),
            session_cancellation: CancellationToken::new(),
            shutdown_cancellation: CancellationToken::new(),
            deadline: Instant::now() + WORKSPACE_SCAN_DEADLINE,
            expired: false,
        }
    }

    fn walk(workspace: &Workspace, start_entry: u64) -> (WalkOutcome, Vec<String>) {
        let mut budget = scan_budget(workspace);
        let mut seen = Vec::new();
        let outcome = walk_scope(
            workspace,
            "",
            WalkOptions {
                recursive: true,
                want_size: false,
                directory_only: false,
            },
            start_entry,
            &mut budget,
            |entry, _| {
                seen.push(entry.relative.clone());
                Ok(Visit::Next)
            },
        )
        .unwrap();
        let entries = budget.entries;
        assert!(entries >= seen.len() as u64);
        (outcome, seen)
    }

    #[tokio::test]
    async fn a_deadline_keeps_the_partial_walk_and_drops_the_continuation() {
        let (workspace, base) = fixture("deadline").await;
        write(&workspace, "a.txt", b"a");
        write(&workspace, "b.txt", b"b");

        let mut budget = scan_budget(&workspace);
        let mut seen = Vec::new();
        let outcome = walk_scope(
            &workspace,
            "",
            WalkOptions {
                recursive: true,
                want_size: false,
                directory_only: false,
            },
            0,
            &mut budget,
            |entry, budget| {
                seen.push(entry.relative.clone());
                // The deadline passes right after the first visited entry.
                budget.expire_now();
                Ok(Visit::Next)
            },
        )
        .unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(outcome.reason, WorkspaceScanStop::Deadline);
        assert!(outcome.next.is_none());
        assert!(outcome.resume.is_none());
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn a_cancelled_session_fails_the_scan_and_exits_the_worker() {
        let (workspace, _base) = fixture("deadline-cancel").await;
        let hold = Arc::new(ScanHold::new());
        hold_next_scan(workspace.root().to_path_buf(), Arc::clone(&hold));
        let session = CancellationToken::new();
        let pending = tokio::spawn({
            let workspace = Arc::clone(&workspace);
            let session = session.clone();
            async move { run_scan(workspace, session, CancellationToken::new(), |_, _| Ok(())).await }
        });
        tokio::time::timeout(Duration::from_secs(5), hold.wait_started())
            .await
            .expect("gated worker did not start");

        // Cancellation is not a partial result: the query fails and the worker
        // is joined instead of being left behind.
        session.cancel();
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("a cancelled scan did not return")
            .expect("the scan task panicked");
        assert!(matches!(result, Err(AgentError::QueryLimit)));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !hold.exited() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            hold.exited(),
            "a cancelled query left its blocking worker behind"
        );
    }

    #[tokio::test]
    async fn dropping_the_awaiter_cancels_the_worker() {
        let (workspace, _base) = fixture("drop-guard").await;
        let hold = Arc::new(ScanHold::new());
        hold_next_scan(workspace.root().to_path_buf(), Arc::clone(&hold));
        let pending = tokio::spawn({
            let workspace = Arc::clone(&workspace);
            async move {
                run_scan(
                    workspace,
                    CancellationToken::new(),
                    CancellationToken::new(),
                    |_, _| Ok(()),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), hold.wait_started())
            .await
            .expect("gated worker did not start");

        // Dropping the awaiting future must cancel the worker through the drop
        // guard instead of detaching it. The worker is cancelled before it can
        // run the scan closure, so the signal comes from the worker scope.
        pending.abort();
        let _ = pending.await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !hold.exited() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            hold.exited(),
            "a dropped query left its blocking worker behind"
        );
    }

    #[tokio::test]
    async fn nested_rules_inherit_and_git_metadata_is_never_walked() {
        let (workspace, base) = fixture("rules").await;
        write(&workspace, ".gitignore", b"root-only.txt\ntarget/\n");
        write(&workspace, ".git/HEAD", b"ref: refs/heads/main\n");
        write(&workspace, "root-only.txt", b"x");
        write(&workspace, "kept.txt", b"x");
        write(&workspace, "target/junk.txt", b"x");
        write(&workspace, "sub/.gitignore", b"nested.txt\n!keep.log\n");
        write(&workspace, "sub/nested.txt", b"x");
        write(&workspace, "sub/keep.txt", b"x");
        write(&workspace, "sub/keep.log", b"x");

        let (outcome, mut seen) = walk(&workspace, 0);
        assert_eq!(outcome.reason, WorkspaceScanStop::End);
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ".gitignore",
                "kept.txt",
                "sub",
                "sub/.gitignore",
                "sub/keep.log",
                "sub/keep.txt"
            ]
        );
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn rule_categories_precede_depth_and_rules_never_leak() {
        let (workspace, base) = fixture("rule-categories").await;
        // Category precedence: an `.ignore` at the root outranks a nested
        // `.gitignore`, so the nested whitelist cannot resurrect the path.
        write(&workspace, ".ignore", b"keep-out\n");
        write(&workspace, "sub/.gitignore", b"!keep-out\nsibling-only\n");
        write(&workspace, "sub/keep-out", b"x");
        write(&workspace, "keep-out", b"x");
        write(&workspace, "sibling-only", b"x");
        // A parent pattern that excludes a directory cannot be undone from
        // inside it, and a rule file inside that directory is never read.
        write(&workspace, ".gitignore", b"blocked/\n");
        write(&workspace, "blocked/.gitignore", b"!*\n");
        write(&workspace, "blocked/inside.txt", b"x");
        write(&workspace, "sub/kept.txt", b"x");

        let (outcome, mut seen) = walk(&workspace, 0);
        assert_eq!(outcome.reason, WorkspaceScanStop::End);
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ".gitignore",
                ".ignore",
                "sibling-only",
                "sub",
                "sub/.gitignore",
                "sub/kept.txt"
            ]
        );

        // A requested subdirectory sees exactly the entries the whole walk
        // reports below it.
        let mut budget = scan_budget(&workspace);
        let mut nested = Vec::new();
        let outcome = walk_scope(
            &workspace,
            "sub",
            WalkOptions {
                recursive: true,
                want_size: false,
                directory_only: false,
            },
            0,
            &mut budget,
            |entry, _| {
                nested.push(entry.relative.clone());
                Ok(Visit::Next)
            },
        )
        .unwrap();
        assert_eq!(outcome.reason, WorkspaceScanStop::End);
        nested.sort();
        let whole: Vec<String> = seen
            .iter()
            .filter(|path| path.starts_with("sub/"))
            .cloned()
            .collect();
        assert_eq!(nested, whole);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn a_subdirectory_root_inherits_ancestor_rules() {
        let (workspace, base) = fixture("subdir").await;
        write(&workspace, ".gitignore", b"root-only.txt\n");
        write(&workspace, "sub/root-only.txt", b"x");
        write(&workspace, "sub/kept.txt", b"x");

        let mut budget = scan_budget(&workspace);
        let mut seen = Vec::new();
        let outcome = walk_scope(
            &workspace,
            "sub",
            WalkOptions {
                recursive: true,
                want_size: false,
                directory_only: false,
            },
            0,
            &mut budget,
            |entry, _| {
                seen.push(entry.relative.clone());
                Ok(Visit::Next)
            },
        )
        .unwrap();
        assert_eq!(outcome.reason, WorkspaceScanStop::End);
        assert_eq!(seen, vec!["sub/kept.txt"]);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn an_explicit_root_is_never_filtered_by_its_own_rules() {
        let (workspace, base) = fixture("root-rules").await;
        // The pattern matches the requested root's ancestor names, so only a
        // strict-descendant rule application leaves the root usable.
        write(&workspace, "sub/below/.gitignore", b"sub\nbelow\n");
        write(&workspace, "sub/below/file.txt", b"x");

        let mut budget = scan_budget(&workspace);
        let mut seen = Vec::new();
        let outcome = walk_scope(
            &workspace,
            "sub/below",
            WalkOptions {
                recursive: true,
                want_size: false,
                directory_only: false,
            },
            0,
            &mut budget,
            |entry, _| {
                seen.push(entry.relative.clone());
                Ok(Visit::Next)
            },
        )
        .unwrap();
        assert_eq!(outcome.reason, WorkspaceScanStop::End);
        // The rule file is a normal visible entry: the test is about the rules
        // not filtering the root or its ancestors, not about hiding the file.
        seen.sort();
        assert_eq!(seen, vec!["sub/below/.gitignore", "sub/below/file.txt"]);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn positioning_is_charged_to_the_entry_ceiling() {
        let (workspace, base) = fixture("positioning").await;
        for index in 0..10 {
            write(&workspace, &format!("file-{index}.txt"), b"x");
        }
        set_scan_limits(
            workspace.root().to_path_buf(),
            ScanLimits {
                entries: 4,
                bytes: 1 << 20,
                depth: 8,
                rule_files: 8,
                rule_bytes: 1 << 20,
            },
        );
        let (outcome, seen) = walk(&workspace, 0);
        assert_eq!(outcome.reason, WorkspaceScanStop::Entries);
        assert_eq!(outcome.next, None);
        assert_eq!(seen.len(), 4);

        let (outcome, seen) = walk(&workspace, 9);
        assert_eq!(outcome.reason, WorkspaceScanStop::Entries);
        assert_eq!(outcome.next, None);
        assert!(seen.is_empty());
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn an_exhausted_rule_budget_stops_with_the_rules_reason() {
        let (workspace, base) = fixture("rules-budget").await;
        let mut rules = String::new();
        for index in 0..40 {
            rules.push_str(&format!("ignored-{index}.txt\n"));
        }
        write(&workspace, ".gitignore", rules.as_bytes());
        write(&workspace, "kept.txt", b"x");
        set_scan_limits(
            workspace.root().to_path_buf(),
            ScanLimits {
                entries: 1_000,
                bytes: 1 << 20,
                depth: 8,
                rule_files: 1,
                rule_bytes: 8,
            },
        );
        let (outcome, seen) = walk(&workspace, 0);
        assert_eq!(outcome.reason, WorkspaceScanStop::Rules);
        assert_eq!(outcome.next, None);
        assert!(seen.is_empty());
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn non_repository_defaults_apply_and_local_rules_win() {
        let (workspace, base) = fixture("defaults").await;
        write(&workspace, "node_modules/package/index.js", b"x");
        write(&workspace, "target/build.log", b"x");
        write(&workspace, "src/main.rs", b"x");
        write(&workspace, ".gitignore", b"!target/\n");

        let (outcome, mut seen) = walk(&workspace, 0);
        assert_eq!(outcome.reason, WorkspaceScanStop::End);
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ".gitignore",
                "src",
                "src/main.rs",
                "target",
                "target/build.log"
            ]
        );
        let _ = tokio::fs::remove_dir_all(&base).await;
    }
}
