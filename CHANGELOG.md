# Changelog

## Unreleased

- A turn cancelled while a model request is streaming keeps the reasoning and
  answer text streamed so far (each cut to 16 KiB) as a display-only
  `interrupted` object on the loop's `history.jsonl` record. `session.read`
  `records` and `turn.result` return it with `stop_reason: "aborted"`, so a
  reopened session still shows the partial reply. It is not a history item: it
  is never sent to a model, replayed, budgeted, or summarized. A cancel after
  the request completed, for example during a tool, records nothing.
  Older Agents reject a loop line that has the field.

## 0.7.5 — 2026-10-22

Runtime stays 0.7.4.

- Visible reasoning also streams from the legacy `response.reasoning.delta`
  event that some proxies send (DeepSeek through a Responses proxy), and a
  response that streams no reasoning text shows the text of its finished
  reasoning items.
- Usage without `cache_write_tokens`, as OpenAI reports it, now gives ordinary
  input tokens as input less the cached read instead of leaving them unknown.

## 0.7.4 — 2026-10-22

- A history too large for the loop limits no longer rejects a send: the send
  first compacts it (`auto-before-…`), covering every settled loop when the
  recent tail alone is too large, and fails with `history_too_large` only when
  that compaction fails.
- A compaction source too large for one summary request is summarized in
  chunks folded into one summary, instead of failing with `too_large`.
- The history byte limit follows the model: eight bytes per token of its
  effective input window, between Runtime's 2 MiB default and 32 MiB. Requires
  Runtime 0.7.4, pinned by tag `v0.7.4`.
- `session.context` history token estimates leave out provider replay bytes.

- A Session follows its profile's system prompt: opening it after a restart,
  or `agent.reload` while it is loaded, replaces the stored prompt from the
  next turn on, and `session.json` records the new one. Without a reload the
  running Agent keeps the prompt it read at startup.

## 0.7.3 — 2026-10-21

- No minimum Rust version is declared or tested; build with current stable.
- Dependencies are no longer pinned with `=`; `Cargo.lock` fixes the build.
  Every dependency is at its latest release, including nix 0.30, similar 3,
  toml 1, getrandom 0.4 and ignore 0.4.33.
- Pin Runtime 0.7.3 by tag `v0.7.3` (no API change).
- Clippy runs on Linux, macOS and Windows in CI, and the Windows-only lint
  failures in tests are fixed.

## 0.7.2 — 2026-10-21

- The command line moves into the library as `minicore_agent::cli::run`, so a
  host such as `minicore-tui` can embed the Agent and start it as a subcommand
  of its own executable. The `minicore-agent` binary behaves as before. Tracing
  is installed only when no subscriber exists yet.

## 0.7.1 — 2026-10-21

- Pin Runtime 0.7.1 at `c0181d1983280c8cdd14a3196b1ea199b7e6206d` (no API change).
- A failed history append now rolls back on Windows. The append handle there
  cannot truncate, so the rollback reported an unknown outcome and blocked the
  session; it now truncates through a separate writable handle.
- CI runs on every branch and tag, tests Linux, macOS and Windows without
  stopping at the first failure, and uploads a release build per platform
  (Linux x86_64, macOS arm64 and x86_64, Windows x86_64) as an artifact.
- The binary runs its Tokio runtime on two worker threads instead of one per
  core. A single-client stdio Agent spends its time waiting on the network and
  on tool processes; on a 24-core host this cuts the process from about 30
  threads to 7 and its resident memory by 10–15 % after a few turns, with no
  change in turn latency. Blocking work keeps Tokio's own pool.

## 0.7.0 — 2026-10-17

Compatibility (RPC protocol stays 1; history, snapshot and auxiliary formats,
history revision strings and event wire shapes are unchanged):

- Pin Runtime 0.7.0 at `b93aea38fea32b4ee416e4bbf5e66254157e1463`. Runtime owns
  model-text normalization (CRLF/CR become LF, other controls except LF/TAB one
  space, never expanding bytes); the adapter no longer rejects control text.
  Future Runtime statuses, cancellation reasons and outcomes map to `unknown`.
- `session.close` and `agent.shutdown` cancel an active turn with reason
  `shutdown` instead of `user`; `turn.cancel` still uses `user`.
- `turn.wait` returns before emergency-projection settlement, auxiliary tool
  record saves and post-turn compaction. Close and the next submit join them.
- Lifecycle events use a separate 64-entry queue drained first;
  `event_capacity` bounds only streaming events (deltas, progress, process
  chunks, intermediate previews). `dropped_before` is attached at delivery.
- `workspace.files`/`workspace.search` cursors carry the last processed path
  instead of an entry ordinal; cursors issued by 0.6 are rejected.
- `changes.list` orders by observation time; its cursors bind that order, and
  detail warnings describe only the returned page.
- Models not referenced by any profile no longer need credentials at startup
  or reload. Keys are read from the process environment; rotate by restarting.

Sessions and RPC:

- The dispatcher admits requests in order but runs disk work, worker joins and
  reload in bounded owned tasks; Session lifecycle mutations reserve their ID
  (`session_busy` on conflict), and cancellation registers at admission.
- Unix SIGTERM and Ctrl-C use orderly shutdown; the binary exits without waiting
  for stdin EOF. Responses are encoded once and output is flushed in batches.
- An oversized frame still reports a parse error and stops the process.

Persistence:

- Loaded Sessions keep an index of JSONL lines, so pages, revisions and stored
  `turn.result` lookups read only the needed lines; cold reads keep the bounded
  scan.
- A failed append that was rejected or rolled back leaves the Session usable;
  only a failed rollback blocks it. A valid final record missing only its
  newline is kept on repair.
- `session.delete` renames the directory to `.deleting-<id>` before reclaiming
  it; startup finishes interrupted deletes and removes stale metadata temps.
- Auxiliary tool records use an in-memory ledger built by one tolerant scan.
  Store warnings keep `io::ErrorKind` without paths; cold tool queries
  distinguish missing, unavailable and corrupt records.

Models:

- Responses: 10-second connect timeout (no idle read timeout, since reasoning
  streams can be silent), a warning for non-loopback `http://`, retryable 503
  and 529 honoring `Retry-After`, and a bounded post-terminal drain for
  connection reuse.
- In-stream context-overflow, quota and rate-limit codes are classified;
  rejections before any output are `NotStarted`, so compaction recovery and
  retries apply. Encrypted reasoning is requested only for models with explicit
  reasoning levels. Token estimates use max(bytes / 4, non-ASCII characters).

Workspace and Tools:

- Bash runs `/bin/bash -c` (falling back to `/bin/sh -c`), not a login shell,
  and drains pipes during the termination grace.
- Atomic writes run on blocking workers. A started commit is owned by the
  Session, so loop completion and close join it before tool records persist.
- Listings escape non-UTF-8 names and skip entries that vanish mid-scan;
  `.minicore-write-*.tmp` artifacts are hidden. Roots whose resolved path is
  not UTF-8 are rejected rather than converted lossily.
- `edit` translates LF multiline input for CRLF files. `apply_patch` accepts
  `diff --git`/`index` headers, trailing blank lines and blank context lines;
  matching otherwise stays exact.

Compaction:

- Runtime `estimate_history_bytes` is the single admission estimator, applied
  to the effective projected history.
- In-turn utility calls get half of the remaining preparation time; a timeout
  leaves a request that still fits unreduced. Utility reasoning is capped at
  `low` where the model supports it. Tool-call arguments are truncated in the
  summary source like tool results; the retained tail is the smaller of 20k
  tokens and a quarter of the hard input budget.
- Unchanged post-turn sources are not summarized again; cancellation releases
  them for retry. Preview failures disable previews without affecting turns.

Tool data:

- Stream tails use ring buffers, and persistence snapshots are hashed outside
  the ToolData lock. Diff planning is deterministic.

Tests are reorganized around shared fixtures (`ScriptedModel`, `AgentFixture`,
`RpcFixture`) with one `it` integration target plus `rpc_soak`.

## 0.6.2 — 2026-10-04

- Project selected history into one bounded, tool-free Pi-style summary request
  instead of chunking and repeatedly merging/reducing. Keep visible reasoning and
  complete tool arguments; summarize only the first 2,000 Unicode characters of
  each tool result with an explicit truncation marker. Stored history is unchanged.
- Reject oversized summary input before a model call; retain existing hard-budget,
  cancellation, output validation, accounting and atomic snapshot safeguards.

- Report the latest API request context usage in session presentation, using the
  provider total or complete normalized components and the physical model window.
  This is distinct from cumulative billing and is not a live request estimate.
- Retain the last report between turns, recover eligible active-history usage on
  reopen, and invalidate it after model changes or successful compaction.
- Runtime remains pinned to 0.6.1; no provider schema or execution changes.

## 0.6.1 — 2026-10-03

- Preserve bounded provider-owned Responses replay through the existing history
  pipeline, including later turns and JSONL process reopen. Remove the old
  loop-local continuation store. Public read/RPC/summary prose stays redacted.
- Keep canonical parts authoritative for display and tool execution; bind replay
  to normalized endpoint and actual provider model. See the replay contract.
- Add safe numeric local context-budget failures and explicit compaction origin.
- Treat omitted cache-write usage as unknown rather than zero.
- Pin Runtime 0.6.1 at `9082184e929508b30cf19f553a6837d075e2fed3`.
- This is a new reconstruction from the official 0.6.0 base. Historical live
  provider/cache experiments are not verification of this build.

## 0.6.0 — 2026-10-03

- Ordinary turns are append-only across soft thresholds; create/open/submit and
  context reads no longer launch routine startup or per-request summaries.
- Completed + Persisted turns can reserve an independent automatic compaction
  using the existing manual operation engine. Original turn completion does not
  wait for summary generation; even a short Noop budget check can make submit
  temporarily busy. Clients retain drafts without automatic resend.
- Confirmed safe pre-output context-capacity rejection permits one bounded
  compact-and-retry of that logical model request. Reduced context survives
  follow-ups and delivery retries, tools are not replayed, and later real growth
  may have its own recovery opportunity. Local serialized hard estimates,
  generic HTTP 413, partial output and unknown delivery do not trigger recovery.
- Existing emergency summaries can become a source-validated durable snapshot
  after history persistence, without another model call. The existing 64 KiB
  summary-content bound remains: explicit `emergency_settlement_*` failures preserve original
  history/results and the old projection; UnknownWrite does not block the
  Session or masquerade as success. Context reads do not reload disk snapshots.
- Manual operation creation reserves the `auto-` namespace for post-turn IDs;
  cancellation still accepts those IDs. The mandatory `automatic` wire object
  remains as a null current/last compatibility shell; operation and recovery
  observations remain distinct. RPC protocol version stays 1.
- Tool-round statistics are `u64` across Runtime, persistence and read/event
  DTOs; old small integers remain readable. Older clients limited to `u16`
  cannot read counts above 65,535. Legacy `max_tool_rounds` remains `u16` and
  zero means unlimited; no new tool-round cap is introduced.
- Edit presentation diffs bounded raw source before sanitizing emitted rows,
  preserving deadline/size controls and distinct CRLF versus literal escapes.
- Standalone builds pin Runtime 0.6.0 at
  `a6bab348194248dc123e10b36a50d713080f3285`; no sibling checkout is needed.
  MSRV remains Rust 1.85. This source release does not publish registry packages
  or install binaries. Historical verification records below remain unchanged.

## 0.5.0 — Unreleased

This is the current 0.5.0 local source freeze (unreleased). It has not been
tagged, pushed, published, or installed. The [0.5.0 freeze verification
record](docs/verification/0.5.0.md) centralizes its measured static checks and
remote gate results.

### Changes Since 0.3.3

#### Developed Features

- bounded Session context projection, manual and automatic compaction, and
  one-shot provider context-overflow recovery with explicit failure boundaries;
- bounded Workspace reads, listings, literal search, and isolated Git status;
- owned Bash process groups/jobs, bounded stdout/stderr retention, durable cold
  Tool reads, and native/workspace change-review records and diffs;
- read-only presentation data, Session rename, prompt-file loading, cumulative
  Bash credential scrubbing across reloads, and source-aware configuration
  reload;
- five executable Tools: `read`, `write`, `edit`, `apply_patch`, and `bash`.
  Current Profiles do not support executable `subagent` delegation. Historical
  Store records that mention `subagent` retain only the documented read-only
  compatibility paths.

#### Implementation Changes

These are behavior-preserving internal refactors and regression coverage; they
add no RPC methods.

- shared query orchestration keeps source selection, synchronous worker
  registration, cancellation, deadlines, and post-processing consistent across
  the Rust API and RPC paths;
- live and stored turn-result reads share one bounded page assembly path, while
  auxiliary change blobs use one validated `input → result → stdout → stderr →
  before → after` commit order;
- presentation-tool variants share identity binding and one finish path, and
  Session create/open share setup and installation without changing their
  lifecycle ordering;
- caller cancellation remains distinct from Session/Store ownership: cold
  `changes.diff` coverage records that a caller wait can end while the Store
  retains its comparison for shutdown joining.

#### Final Freeze Adjustments

This freeze pass records only the package-version and documentation delta; it
does not claim a runtime implementation change in this delta.

- the package and current documentation identify the Agent as `0.5.0`; Runtime
  `0.4.1` remains pinned to the existing Git revision, RPC protocol version
  remains `1`, and storage format versions remain unchanged;
- the current documentation set covers configuration, ownership and
  architecture, context/compaction, security, contribution rules, and the
  complete 33-method RPC surface, including deferred-query ownership and
  cancellation boundaries; the measured freeze status is centralized in the
  [0.5.0 verification record](docs/verification/0.5.0.md);
- historical specifications, context, plans, and 0.3.x release notes are
  indexed separately from current guidance, while verification records and
  native artifacts retain their recorded facts and paths;
- crate-level rustdoc now includes a suitable `no_run` embedded-use example, and
  the example configuration identifies the complete current executable Tool set.

### Compatibility And Records

The current five-tool Profile contract does not include executable `subagent`
delegation, while historical Store records are handled by the compatibility
paths documented in [the RPC contract](docs/rpc.md). This entry does not make a
blanket compatibility claim about every public behavior between 0.3.3 and the
current source. Existing 0.3.x release records remain under `docs/releases/`;
verification records and their limitations are indexed in
[docs/verification](docs/verification/README.md).
