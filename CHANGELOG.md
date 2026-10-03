# Changelog

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
  `9e230617d36130e7ec77aba122b45f1347ac53f2`; no sibling checkout is needed.
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
