# Shared Data Development Progress

Specification: `minicore-agent-0914-dev-spec.md` (2026-09-13).
Branch: `feat/0914-shared-data`.
Agent starting HEAD: `8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0`.
Runtime remains pinned to 0.4.1, `6cd2bdbc634437dea925495c61c7eb0be10ba171`.

Historical audit reports remain historical evidence and previous unaccepted
results are not acceptance gates. The current source handoff includes the P3a foundation, the P3b1 automatic
compaction slice, and the P3b2 provider replay budget and overflow recovery slice:
startup projection from a validated summary, same-loop model-update binding,
`session.context`, independent manual utility usage, the Agent-global automatic
policy, startup admission preparation, request-time compaction, exact provider
replay budgeting, and one-shot bounded ContextOverflow recovery. P3a, P3b1 and
P3b2 passed parent review and remote verification. P4 is the next, separate
Workspace slice; its read-only query (P4a) has passed parent review and remote
verification, as have its listing and search queries (P4b). Workspace status
remains pending.

## Execution

The current model preference is the user's latest
`cus-resp/deepseek-v4.1-flash:max`; the earlier model-role compatibility
mismatch is fixed. The P3a implementation preferred
`cus-resp/deepseek-v4.1-flash:high`, with three consecutive helper failures
permitting `cus-resp/gpt-5.6-luna:max`; P3a used that fallback after three
DeepSeek context-compaction failures. Only one helper implements at a time; the
parent owns review, remote verification and commits. This handoff permits
source, focused tests, documentation and formatting only. No local
build/test/check or remote operation is part of the handoff. Transfers of
private configuration and real Session data are excluded; the prior
execution-boundary incident remains in the historical audit. No credentials are
stored in source.

## Stages

| Stage | Deliverable | Status |
| --- | --- | --- |
| P0 | Cancellation-safe RPC framing; bounded deferred admission | Verified, see below |
| P1 | Read-only session pages; retained turn-result queries | Linux stable/MSRV verified; Windows/macOS compile checks passed |
| P2 | Structured tool identity, invocation and query records | Verified; memory-only retention, streams/persistence follow in P5 |
| P3 | Manual acceptance, startup/request compaction, one overflow recovery | Verified; P3b2 stable/MSRV 511 passed, 2 Live ignored; cross-platform compile checks passed |
| P4 | Bounded Workspace files/read/search/status | P4a read and P4b files/search verified; 576 stable/MSRV passed, 2 Live ignored; status pending |
| P5 | Owned Bash streaming, cancellation, result retention | Pending |
| P6 | Workspace and tool change scopes, versioned diffs | Pending |
| P7 | Client contract integration and final verification/documentation | Pending |

Each stage is a vertical implementation/API/RPC/test slice, reviewed before
commit. Shared protocols are developed serially. DTOs are introduced alongside
their first real consumer, not as an unused framework.

## P3a Verification

P3a keeps the complete `history.jsonl` and the Session/Store model unchanged.
At loop admission, only a validated `CompactionState` snapshot can project its
covered prefix away from the Runtime `LoopRequest.history`; the summary is
injected once as labeled user historical data. The uncovered suffix, current
User input, accepted Steers, and complete tool pairs remain intact. Runtime item
and byte limits are checked against the projected history, and an absent or
invalid summary leaves an over-limit request as `history_too_large` rather than
silently slicing it.

The projected suffix and summary are bound to the active loop and reused when
`session.update` changes the model at a request boundary, preventing a second
slice. `session.context` responds while manual compaction is busy and reports
operation state, validated coverage, the latest in-process manual result, and a
conservative projected-history budget. Its history estimate is only the Runtime
`LoopRequest.history` suffix; full prepared-request context is explicitly
unknown in P3a. The scan is bounded and returns unknown rather than blocking on
an unbounded history. Manual utility accounting reports observed calls,
`complete`, and known usage fields only; missing evidence never becomes zero.
Focused tests cover over-limit startup, reopen, unchanged history, no double
slice, same-loop model updates, busy context, bounded context estimation,
multiple utility calls, duplicate usage, missing usage, cancellation, and write
failure. Utility result accounting carries observed call attempts, a `complete`
flag, and known usage from accepted calls. The utility layer now also retains
usage that a stream emitted before failing as a known partial total; the
`complete` flag stays `false`, and no value is filled with zero.

Parent-run remote Linux stable/MSRV suites each passed 451 tests, with 2 Live
tests ignored. Strict Clippy, rustdoc and fmt checks passed. Logs are
`/root/minicore-agent-0914/logs/p3a-{tests,msrv,clippy,doc}.log` on the builder.
Cross-platform checks will be repeated after P3b; P2's checks remain separate.
P3a does not add automatic threshold compaction, current-tool ephemeral
summaries, provider overflow recovery, or TUI `/compact`.

## P3b1 Automatic Compaction

P3b1 implements automatic threshold compaction only. It adds:

- `[compaction]` Agent-global policy (`enabled`, `trigger_percent`,
  `target_percent`) with `0 < target < trigger <= 100` validated at config
  load and reload. The default is enabled at `80`/`50`; existing tests opt out
  with an explicit disabled fixture rather than changing existing assertions.
- Startup admission preparation: an automatic Session first reserves a
  preparation while it computes a bounded projected request estimate. It
  checks Runtime structural limits and the irreducible hard-window minimum
  before composing a full request. When the Runtime item/byte limits or trigger are exceeded, it runs the no-tools
  summary utility before any `AgentLoop` exists; a fitting request starts from
  that same worker without a summary. `turn.send` is a deferred RPC that
  resolves with the real `TurnRef` only after the loop is installed; a
  preparation the reader cannot await is cancelled rather than left running.
  `session.context`
  addresses the preparing operation and `session.compact.cancel` terminates a
  preparation that has not started a loop. Same-session turn admission stays
  single; shutdown cancels and joins the preparation worker.
- Request-time budget: the effective input budget is the model's already
  reduced context window (output allowance and safety margin already removed,
  never subtracted twice). It counts system and AGENTS text, the durable
  summary, the history suffix, current User/Steer text, tool schemas and
  provider-style framing. The hard window is the only failure boundary; the
  trigger starts an attempt and the target is preferred. Complete tool
  exchanges are folded as whole groups, and settled ordinary base items may be
  folded when a smaller hot-swapped model requires it. Current User and Steer
  text is preserved verbatim, no tool is re-run, no fake summary history is
  written, and the summary is never promoted to system.
- Uncompressible detection: a request whose irreducible system/current-input/
  tool-schema minimum exceeds the hard window returns `context_uncompressible`
  (`-32022`) without starting a summary loop. Semantic summary failures
  (no progress, empty, oversized, timeout) are explicit preparation failures.
- Deadline alignment: automatic summaries run inside the Runtime
  operation deadline; startup/manual operations have independent deadlines,
  and request-time utility chunks share the remaining turn
  deadline, and model hot-updates re-derive the budget while preserving only
  source-matching ephemeral summaries. Automatic options raise prompt
  preparation to the model-operation timeout floor.

Parent-run remote verification: Linux stable and Rust 1.85.0 each passed 484
tests, with 2 Live tests ignored. Strict Clippy, rustdoc, fmt, and Windows/macOS
all-target compile checks passed (existing Windows test-helper warning only).
Logs: `/root/minicore-agent-0914/logs/p3b1-{tests,msrv,clippy,doc,windows,macos}.log`.

P3b1 does not add upstream `ContextOverflow` recovery, TUI `/compact`, or any
new AgentLoop/Service architecture. P3b2 is the narrow provider replay
adapter/wrapper and exact request-budget gate; P4 remains Workspace.

## P3b2 Provider Replay Budget And ContextOverflow Recovery

P3b2 implements the provider replay budgeting and one-shot overflow recovery
slice. Current design:

- **Topology**: `Runtime -> readonly PresentationModel -> CompactingModel ->
  rawModel`, plus the existing P3b1 plan/utility/sessions structures. The
  factory installs `CompactingModel` only when automatic compaction is enabled,
  so a disabled session keeps the raw model's established cancellation behavior
  and pays no per-request content/ticket hashing. No new AgentLoop or Service
  architecture.
- **Provider replay budget**: the OpenAI Responses budget is the actual
  normalized serialized body produced by `build_request_with_replay` (provider
  model, reasoning format, opaque replay items, tool schemas, framing), and the
  sender and the estimator share replay selection. Real-HTTP loopback tests
  cover both the provider-rejected path (a structured 400 clears the
  continuation and the retry is a clean folded request) and the preflight path
  (a replay exceeding the effective hard budget is summarized before sending:
  tool -> utility -> folded request, so the over-budget replay body is never
  sent).
- **Structured one-shot recovery**: only `ContextOverflow + NotStarted` from a
  raw `Model::start` is recovered, at most once per logical request. The ticket
  binds `loop_id`, `request_index`, content and ticket hashes, actual
  `ModelLimits`, the normalized pre-start body bytes/tokens, and the utility
  binding captured by the preparing provider. A per-loop high-water mark
  records the highest index that consumed recovery, so a Driver retry re-entry
  and every older index stay blocked while a new loop starts with fresh quota.
  `Unknown` and `Started` deliveries, network interruptions, substrings, and a
  second overflow never auto-retry.
- **Settings ownership**: the ticket stores its own `AutoContextBinding`;
  `ExecutionConfigFactory::build` publishes nothing, and `Session::new`,
  `Session::update`, and `replace_future_config` bump the config generation at
  their commit point. `claim_recovery_ticket` reads the current generations
  itself, so a config or summary change before or during the raw start fails
  closed with zero calls on the newer utility, and a failed update cannot
  republish a stale binding. The state holds no strong reference back to the
  auto context, so no reference cycle exists.
- **Cancellation, deadline, delivery**: recovery shares the remaining
  `ModelCallContext` deadline and cancellation token without resetting timeout.
  Cancellation or deadline observed before the raw future exists reports
  `NotStarted`; any in-flight interruption reports `Unknown`, matching the
  Runtime driver.
- **Measured shrink and budget ordering**: the retained recovery source is
  bounded by a capped serde write (512 KiB, structure and identifiers counted,
  no intermediate copy). Each candidate folds the real sources and measures the
  provider-normalized body; a retry is returned only when it is strictly
  smaller than the failed body, fits the hard window and message ceiling, and
  keeps complete tool pairs. The irreducible floor is rejected before any
  utility call, the utility target leaves room above that floor, an empty cache
  always folds at least one group, and retained summaries are reused only while
  the projection fits.
- **State accounting and isolation**: `session.context` exposes recovery
  observations (`outcome`, `before_tokens`, `after_tokens`, `utility_usage`,
  `failure_kind`) and the retained automatic plan observation. Utility usage
  stays independently categorized, loop-bound caches and tickets are cleaned up
  on loop completion, and sessions remain strictly isolated.

Known limits: replay budgeting, preflight, and recovery are covered by unit and
agent-level loopback HTTP tests with a mock provider, not by a live provider.
Tokens remain a bytes/4 estimate, not a tokenizer measurement. Sources over the
512 KiB recovery-ticket cap are not retained for overflow recovery; ordinary
startup and request-time fitting keep their existing bounded paths. In-flight
cancellation/deadline tests synchronize on actual model starts; the high-water
quota proof beyond the first few indices is unit-level. Disabled-compaction
passthrough is asserted by construction rather than by a dedicated agent test.

Parent-owned remote verification passed after review corrections: stable and
Rust 1.85.0 full all-target suites each passed 511 tests (485 library and 26
integration), with 2 Live tests ignored. Strict Clippy, fmt and rustdoc passed;
Windows GNU and macOS all-target cross-compilation checks passed. Windows
retains the existing unused `write_reload_config` integration-test helper
warning. Logs: `/root/minicore-agent-0914/logs/p3b2-deepseek-{tests,clippy,msrv,fmt,doc,windows,macos}.log`.
The final suite includes real replay preflight, structured overflow delivery
cases with recoverable sources, same logical request/deadline across Driver
re-entry, and synchronized manual-cancel accounting. No local compilation,
native Windows/macOS test execution, Live Provider test, installation, version
bump or push was performed. Runtime remains pinned to the revision above.

## P4a Workspace Read

P4a implements only the read-only workspace file query; files/search/status and
later P4/P5+ slices are not started.

- **Ownership**: `workspace.read` is a loaded-Session query. `Agent::workspace_read`
  and the RPC arm resolve the Session first, then borrow its canonical
  `Workspace` plus a Session-owned cancellation token. A closed or unknown
  Session returns `session_not_loaded`; closing the Session (or dropping it)
  cancels its in-flight reads with `query_limit`, and the RPC `queries` JoinSet
  owns and joins the task without a new service or manager.
- **One bounded read, live observation**: the query opens the regular file
  through a shared `Workspace::open_regular_file` helper (also used by the read
  Tool's `read_prefix`), takes the read Tool's 512 KiB whole-file bound, and
  reads once. The UTF-8/NUL check, the whole-file SHA-256 revision, and the
  returned page all describe the same bytes, so an old hash can never accompany
  newer content. Files over the bound are `too_large` with no preview and no
  revision; a size or full modification-time change across the read is
  `changed` with empty content. The result is an observation of the bytes that
  were read, not a filesystem snapshot, and it never locks out a concurrent
  writer.
- **Lossless pagination**: pages are cut by the requested line window and by
  the encoded byte budget, and a cut inside a line returns `line_truncated`
  with `next_range {start_line, line_byte_offset}` that continues the same line
  at the byte offset after the returned content. Nothing is skipped, CRLF may
  split across pages, and concatenating pages from the same revision reproduces
  the file bytes exactly. Clients carry `if_revision` on continuation pages to
  detect intervening changes. `max_bytes` is an encoded result budget (metadata plus JSON
  escaping) with `1024..=262144` and a 64 KiB default; a budget that cannot
  hold the metadata envelope and one character is an argument error rather than
  a silent truncation.
- **IO and platform boundaries**: path/range validation is lexical (a
  `4096`-byte path cap, a `line_byte_offset` inside the whole-file bound) and
  happens before a query slot is reserved. On Unix the open uses
  `O_NONBLOCK | O_NOFOLLOW` (via the `libc` constants in a `cfg(unix)`
  dependency) and the post-open metadata still requires a regular file, so a
  FIFO swapped in after the metadata check cannot block the open; Windows keeps
  the existing regular-file checks. Cancellation and the 10 s deadline wrap
  each open/read/metadata await, so a pending IO step cannot outlive either.

Known limits: the revision is a live hash of the bytes that were read, not a
lock or snapshot; the path sandbox is not an adversarial same-user defense; a
`line_byte_offset` must be a character boundary inside the requested line
(checked against the file, not lexically); and `too_large` is a whole-file
bound rather than a per-page limit. Search/status, change scopes and versioned
diffs remain pending.

Parent-owned remote verification: stable and Rust 1.85.0 each passed 528 tests
(502 library and 26 integration), with 2 Live tests ignored. Strict Clippy, fmt,
rustdoc and Windows/macOS all-target cross-compilation passed; the existing
Windows `write_reload_config` test-helper warning remains. Tests cover whole-file
revision consistency, encoded size accounting, lossless long unicode/control/
CRLF pagination, limits and binary data, post-metadata FIFO replacement,
Session-close/shutdown/deadline cancellation, no model/history side effects,
and RPC responsiveness/capacity reuse. The shared read Tool regressions passed.
Logs: `/root/minicore-agent-0914/logs/p4a-{tests,clippy,msrv,fmt,doc,windows,macos}.log`.
The parent generated the lockfile remotely with `cargo update --offline
--workspace`; only the root package's dependency on already-locked libc 0.2.189
was added. No dependency was upgraded. No local compilation, native cross-platform
test execution, Live Provider test, installation, version bump or push occurred.

## P4b Workspace Listing And Search

P4b implements `workspace.files` and `workspace.search`; `workspace.status`,
change scopes, versioned diffs, and P5+ are not started. Both queries reuse the
P4a ownership, cancellation, capacity, and result-budget contracts.

- **Shared bounded traversal**: `src/workspace/scan.rs` owns one depth-first
  `std::fs::read_dir` walk with the entry, byte, depth, and rule ceilings, the
  cancellation and deadline checks, the encoded-record accounting, and the
  per-query blocking worker; `src/workspace/listing.rs` and
  `src/workspace/search.rs` only add the per-entry action. Rules come from
  `ignore::gitignore` matchers built from bounded reads of `.ignore`,
  `.gitignore`, and `.git/info/exclude`, never from a hand-written pattern
  parser and never from the global git configuration. `.ignore` outranks
  `.gitignore` outranks `.git/info/exclude`, the closest directory wins inside a
  category, and a rule file only ever matches a strict descendant of its own
  directory, so rules cannot leak onto ancestors or siblings and a requested
  root is never filtered by its own file. `.git` metadata is never returned or
  expanded; requested subdirectories and explicitly named paths inherit every
  ancestor rule up to the Workspace root; symlinked directories are never
  followed and a requested root that resolves outside the Workspace is rejected
  before any walk. A Workspace that is not a git repository also
  excludes a documented default list of build and dependency directories, and
  local rules still win over it.
- **Real work accounting**: the entry ceiling is shared by every root of one
  query and counts every raw directory entry, including entries consumed while
  positioning at a cursor, while a cursor's ordinal is local to its own
  requested root and starts at zero. Positioning replays raw entries, including
  rule-excluded ones, and still descends into directories, so a resumed page
  never loses a subtree. `read_dir` failures, unreadable entries, and non-UTF-8
  paths are counted; ignore files are read in bounded chunks with a per-query
  128-file / 1 MiB / 256 KiB-per-file budget charged as bytes are read, and
  exceeding it stops the scan with `stopped_by: rules` instead of applying
  partial rules. Exhausting the entry budget before reaching a cursor stops
  without a continuation rather than returning a cursor that would not advance,
  so a returned cursor always moves forward.
- **Owned blocking worker**: each scan runs on one `spawn_blocking` worker that
  checks the Session token, the RPC shutdown token, and the deadline at every
  examined entry and read chunk. The awaiter selects on those tokens and on the
  worker; cancellation cancels the worker's own token and joins the handle before
  returning, and a drop guard cancels the worker when the awaiting future is
  dropped by its caller. The deadline is applied by the worker itself, which
  stops with `stopped_by: deadline`, keeps the partial result, and returns no
  cursor, so the awaiter joining that worker is what preserves the partial;
  cancellation is never turned into a stop. RPC dispatch never wraps a scan in an
  outer timeout that would drop that join, and the existing four-query /
  32-deferred pool is reused without a new manager or service. The deadline is
  checked between bounded operations, not as hard preemption: a single blocking
  read on a stalled remote filesystem can outlast it.
- **Bounded, honest pages**: `workspace.files` returns entry kind and optional
  size, requires `directory` to be a directory, and filters entry paths only.
  `workspace.search` matches one literal single-line query (case-insensitive by
  default), returns per-line UTF-8 byte ranges inside a returned slice plus the
  slice's own offset in the original line, and pages a line that does not fit by
  returning a bounded slice that still contains its matches (up to 64 bytes of
  leading context) and stopping before the next match. A page that cannot hold an
  entry or a match even when empty skips and counts it and marks the response
  incomplete, so nothing is silently dropped and no empty record is returned in
  place of a match. Both cap raw entries (100,000), path or content bytes
  (16 MiB), depth (64), and the encoded page (`max_bytes`, 1024..=262144).
  `stopped_by` distinguishes `end`, `page`, `entries`, `bytes`, `depth`, `rules`,
  and `deadline`; `truncated` and `scan_complete` are always explicit, no global
  total is ever invented, and `end`, `depth`, `entries`, `rules`, and `deadline`
  end without a continuation cursor. A deadline keeps whatever the page already
  found, reports `truncated` with `scan_complete: false`, and asks for a narrower
  request instead of a resumption; an already expired budget returns that empty
  partial rather than a false end. Search also keeps the difference between a scope that was
  enumerated and files that were searched: binary, oversized, non-UTF-8, special,
  and out-of-boundary files are counted in `skipped_files` without making the
  result incomplete, while read failures and unrepresentable matches do make it
  incomplete.
- **Live cursors**: a cursor is a small structured record that binds the method,
  Session, query or directory, recursion or case mode, and root list through a
  truncated SHA-256 scope, carries the traversal ordinal (and, for search, the
  line and byte offset inside a file), and is rejected with `-32602` before a
  query slot is reserved when it belongs to another request. Search paths are
  normalized and de-duplicated, and overlapping roots are rejected so no file is
  searched twice. Each page is a fresh live observation with `consistency: live`
  and `observed_at_unix_ms`; a resumed request re-walks to its cursor within the
  same budget, so no index, watcher, or Workspace snapshot is created.

Known limits: ordering within a directory is the filesystem's enumeration order,
so an ordinal cursor is only exact while the tree and that order are unchanged;
positioning a resumed request is bounded by the deadline and the entry ceiling
rather than by the per-page limits; rule-excluded descendants are not reported
as skipped because they are not part of the visible tree; files over the 512 KiB
whole-file bound, binary files, invalid UTF-8 files, and non-UTF-8 paths are
skipped and counted rather than partially searched.

Verified tests include rule inheritance and
`.git` exclusion, non-repository defaults with local overrides, explicit-root
rule enforcement, a rules over-budget stop, positioning charged to the entry
ceiling, symlink and special-file boundaries, lossless pagination and cursor
binding for both methods, a 1024-byte page budget with an exact entry set, a
match far into a 300 KiB line, escaping inflation that does not overflow the
budget, an unrepresentable match reported as incomplete, byte-budget paging with
matches rebuilt exactly across pages, a timeout that keeps a found match without
a cursor, an already expired budget that returns an empty partial instead of an
end, session-close/shutdown cancellation that still fails the query, and a drop
guard that stops a dropped query's worker.

Parent-owned remote verification: stable and Rust 1.85.0 all-target suites each
passed 576 tests (550 library, 26 integration), with 2 Live tests ignored.
Strict Clippy, fmt, rustdoc and Windows/macOS all-target cross-compilation checks
passed. The existing Windows `write_reload_config` test-helper warning remains.
Logs: `/root/minicore-agent-0914/logs/p4b-{tests,clippy,msrv,fmt,doc,windows,macos}.log`.
The parent generated Cargo.lock remotely: 10 packages added for pinned
`ignore = 0.4.23` and `regex = 1.11.1`, with no existing package upgrades.
Resolution selected MSRV-compatible globset 0.4.19 instead of the newer
Rust-1.88-only version; Rust 1.85 verification passed with the resulting lock.
No local compilation, native Windows/macOS test execution, Live Provider test,
installation, version bump or push occurred. Runtime source and pin are unchanged.

## P0 Verification

The RPC reader retains partially consumed frames across select cancellation and
checks the cumulative 1 MiB limit. Deferred waiters share a 32-entry ceiling;
`resource_exhausted` (`-32019`, retryable) rejects new waiters/compaction before
starting work, without denying ping/cancel/shutdown. Existing interfaces remain.

Parent-run Linux stable verification on 2026-09-14:

- `cargo fmt --all -- --check`: passed.
- `cargo test --locked --lib rpc::server::tests`: 43 passed, 0 failed.
- `cargo clippy --locked --all-targets -- -D warnings`: passed.
- Remote logs: `logs/p0-rpc.log`, `logs/p0-clippy.log` under the build root.

P0 was additionally covered by the full P1 gates below. No live Provider tests,
installation, package version change or push has been performed.

## P1 Verification

Adds `session.read` and `turn.result`, advertised through `agent.ping` protocol
version 1 and capabilities. Both return sanitized ordered item JSON fragments
with explicit raw UTF-8 offsets and a hard encoded-result byte budget (default
256 KiB, maximum 1 MiB). The budget excludes the JSON-RPC transport envelope.
Read-only Store scans capture a complete prefix and SHA-256 revision, leave
incomplete tails untouched, and never open execution resources. Loaded reads
validate the committed snapshot. Turn queries reuse the existing completion
report, including failed appends, and fall back to persisted loop records.

Queries have a 10-second deadline, a 64 MiB/100,000-line scan ceiling, four
concurrent-query slots, and the shared 32-deferred-request ceiling. Exceeding a
scan limit returns `query_limit`, not a purported complete page. Query shutdown
requests cancellation and joins owned tasks. Legacy RPC error mappings remain
unchanged; new query parameter errors are mapped only at the new entrypoints.

Parent-run remote verification on 2026-09-14:

- Linux stable 1.97.1 `cargo test --locked --all-targets`: 415 passed, 2 ignored.
- Rust 1.85.0 `cargo test --locked --all-targets`: 415 passed, 2 ignored.
- Stable strict Clippy, rustdoc and formatting checks: passed.
- `cargo check --locked --all-targets --target x86_64-pc-windows-gnu`: passed;
  existing unused `write_reload_config` test helper warning remains.
- `cargo check --locked --all-targets --target x86_64-apple-darwin`: passed
  using the remote Zig C compiler with `CRATE_CC_NO_DEFAULTS=1` to avoid
  conflicting target flags. Native macOS/Windows execution was not performed.
- Logs: `logs/p1-tests.log`, `p1-msrv.log`, `p1-clippy.log`, `p1-doc.log`, and
  `p1-<target>.log` under the remote build root.

Unpersisted results are retained only while the existing completed ActiveLoop
is held (until the next turn or Session close). Reads beyond the scan ceiling
are explicitly rejected; no full-history index or snapshot service is added.

## P2 Verification

P2 adds a narrow, non-authoritative tool-fact layer without a new service
architecture, reusing the existing per-Session `Presentation` ownership and the
Runtime's real boundaries:

- `src/tool_data.rs` holds `ToolRef {session_id, loop_id, request_index,
  tool_call_id}`, `ToolInvocationData` (structured `subject` plus a bounded raw
  input preview), `ToolExecutionData` (state, whitelisted `ToolPhase`, real
  start time, authoritative Runtime outcome, per-stream availability), and the
  `tool.read`/`tool.output` queries. Records are bounded per loaded Session
  (1024 records, 8 MiB of retained data plus counted metadata); eviction frees
  the backing `String` capacity, reports `available`/`partial`/`expired`, and
  keeps input and output availability independent. `tool.output` pages raw
  bytes by UTF-8 offset with an encoded-JSON byte cap, never uses
  `escape_default`, and distinguishes `pending` (still running) from
  `unavailable` (terminal, never observed) from a real empty result.
- Identity comes only from real Runtime values: `ModelCallContext` at
  `Model::start` and the Runtime tool-call id. A new `PresentationPolicy`
  wrapper publishes the requested invocation before an approval decision, so
  approval-time data is obtainable while a call is `awaiting_policy` and never
  reported as `running`; `started_at` is stamped only when the tool truly runs.
  `PresentationTool` retains the raw result; the joined loop report
  unconditionally reconciles state/outcome and captures result text for every
  outcome (including failed/denied), without resurrecting evicted bytes.
- New RPC methods `tool.read`/`tool.output` (capability-declared in
  `agent.ping`) and best-effort `tool_invocation`/`tool_execution` events come
  from the same record source as the queries. `ToolRef` rejects unknown fields.
  The legacy `ToolDisplay` presentation path is retained unchanged for
  compatibility; the new contract has no UI fields. No tool identity is
  guessed from the most recent call.
- Real phases are emitted through `ToolContext.progress` for read/write/edit/
  apply_patch/bash and mapped to a known-stage whitelist. Raw stdout/stderr
  streaming, process ownership, and durable auxiliary files remain P5; change
  references remain P6.

Parent-run remote verification: Linux stable and Rust 1.85.0 full suites each
passed 442 tests, with 2 Live tests ignored. Strict stable Clippy, rustdoc,
formatting, Windows and macOS all-target compile checks passed. The existing
Windows test-helper warning remains. Logs are `logs/p2-tests.log`,
`p2-msrv.log`, `p2-clippy.log`, `p2-doc.log`, `p2-windows.log`, and `p2-macos.log`.
No local compilation, native cross-platform test execution, installation or push.
