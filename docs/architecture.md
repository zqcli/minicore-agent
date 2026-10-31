# Architecture

MiniCore Agent keeps execution, observation, presentation, and persistence as
separate ownership boundaries. Agent, Session, Runtime, Store, Tool owners, and
bounded query workers each retain the resources they are responsible for.

```text
client
  │ NDJSON over stdio
  ▼
RPC server ──► Agent ──► loaded Session ──► Runtime AgentLoop
   │             │             │                 │
   │             │             │                 ├─ Model / Prompt / ToolSet
   │             │             │                 └─ cancellation and deadlines
   │             │             ├─ Workspace + execution snapshot
   │             │             ├─ ToolObserver ──► ToolData
   │             │             ├─ ToolOwners ──► Bash workers, started commits
   │             │             ├─ Presentation ──► display-only facts/events
   │             │             └─ CompactionState
   │             └─ Store ─────► session.json / history.jsonl / summary.json / aux
   └─ bounded response/event writer
```

## Ownership

- `Agent` owns the Store handle, the loaded-Session map, the global event sink,
  model/profile catalogs, and Agent-level shutdown.
- A loaded `Session` owns its canonical Workspace, persisted record and in-memory
  history, execution configuration, compaction state, `ToolData`,
  `ToolOwners`, `ToolObserver`, Presentation state, the input queue, and
  Session-owned query workers. Close cancels and joins those workers before
  unloading the Session.
- Runtime owns the `AgentLoop` semantics, model-text normalization, and
  model/tool scheduling. Immutable text, arguments, replay, and request payloads
  share storage across clones. The
  Agent's Session task observes the loop, persists the durably normalized result (including validated private replay), merges
  settled history, and publishes Agent-level completion.
- The input queue (`sessions/inputs.rs`) is the only way a turn starts. It sits
  behind its own `std::sync::Mutex`, taken after `inner` when both are needed
  and never held across an await. Each loop gets a `LoopInputs` implementation
  bound to its generation: the Runtime calls `take` synchronously at request
  boundaries, `take` locks only the queue, and it hands out every queued steer
  once; a loop of an older generation takes nothing. An idle push, `input.resume`,
  or the end of a manual compaction claims the queue head under the queue lock
  (`running` marks the claim) so exactly one start proceeds.
- `ToolObserver` captures the real request/Tool identity at Runtime boundaries.
  `ToolData` retains bounded invocation, execution, stream, and change facts.
  `ToolOwners` owns Bash child cleanup and started workspace commits even if the
  Runtime drops a Tool future; loop completion joins them before tool records
  are persisted, and close joins them before reporting closure. `Presentation` formats bounded local display data without changing
  execution or persistence.
- `Store` is the durable authority for Session records, JSONL history, derived
  summaries, and auxiliary Tool/change files. A Store clone shares one budget and
  ownership boundary; independent Agent processes sharing a data directory are
  unsupported.

The RPC dispatcher admits requests synchronously. Slow work runs in owned,
bounded tasks whose completed results are applied with exclusive Agent access.
Session lifecycle mutations reserve the affected ID until completion; reload
reserves catalog replacement against concurrent deferred mutations. Shutdown
signals all Sessions before joining any, drains deferred operations, and joins
Session barriers concurrently. The loaded map retains Sessions until their
workers are joined. Session close and Agent shutdown use `LoopHandle::shutdown()`;
only explicit turn cancellation uses the user-cancellation path.

Events use derived tagged serialization, and stored outcomes and public outcome
views share the same redacted types. Byte-level event and history-record golden
tests protect the existing wire and storage formats. Future non-exhaustive Runtime
statuses, cancellation reasons, and outcomes map to explicit `unknown` values.
Runtime's `history::estimate_history_bytes` is the single history size estimator,
used for context queries and the history token estimate. Automatic threshold
compaction checks provider-reported usage only before the first request of a
turn; there are no history item or byte limits. Later requests rely on confirmed
provider-overflow recovery.

## Core Modules

- `sessions.rs` retains Session ownership and shared state. Its child modules
  separate public types, the loaded registry, status-worker ownership, active-loop
  execution, event mapping, and context-budget views. `compact.rs` retains
  compaction ownership and commits.
- One driver task per busy period runs the turns back to back. After a turn's
  report is persisted it decides under the Session locks: a completed, persisted
  turn with a queue head installs the successor loop (after settling its own
  projection, which the successor would project) and only then publishes the
  finished turn's completion with `next`; any other outcome ends the busy period,
  pausing a non-empty queue after a turn that did not complete. The driver never
  joins itself: the next start, compaction, or close joins the one task, which
  covers every turn of the period. Replaced turns stay waitable through a ring
  of the last four completions. Otherwise the task names its join/drain,
  reconciliation, persistence, projection settlement, and completion phases;
  publication still precedes auxiliary persistence when no successor starts, and
  the owned task remains the close/join barrier for those writes.
- `rpc/server.rs` owns deferred tasks and shutdown. `server/dispatch.rs` separates
  agent, session, query, input, and turn admission; `errors.rs` owns error mapping and
  response construction; `io.rs` owns signals, framing, event pumping, and batched
  output. Success responses carry pre-encoded JSON rather than an intermediate
  `Value` tree.
- `agent/session_setup.rs` builds the Session's execution resources and persisted
  settings. Installation and index publication remain on the Agent.

Reliable control, compatibility lifecycle, and streaming events have separate
bounded lanes. Control is serviced before lifecycle and streaming data, while
drop counts are attached only at dequeue. One publisher rotates dirty loads in
batches of at most 16, removing a mark before sampling so a concurrent change
remains pending. A reservation precedes every RPC/publisher snapshot sample;
DTO copies, exact-capacity encoding, queued bytes, and the buffered writer tail
stay charged until release. An impossible frame or a failed reliable lane ends
the connection, rather than silently losing a control. Loaded read snapshots share
immutable timestamp maps; writers copy only when a reader still holds a snapshot.

## Warm, Live, And Cold Data

A loaded Session serves warm reads from its committed in-memory snapshots. A
live `turn.result` may expose a bounded normalized result after a failed append,
independently of the current active loop. One Agent-wide catalog owns completion
suffixes and these sources; in-flight page references keep their budget charge.
Stored results use the actual durable interrupted projection. Result paging
uses borrowed canonical serialization, streaming identity verification on
blocking workers, and a page-sized encoded prefix, rather than cloning a whole
normalized turn per page. Unsaved sources reuse already encoded immutable
items; persisted sources are not kept as an additional long-lived body cache. Completion
registration and successor installation share an observation boundary, while
registered waiters retain their existing full-report API. Settled history is
authoritative only after the Agent persistence step completes.

Session create/open, `session.state`, and `session_snapshot` publication use
one sampler and DTO, identified by session/load/revision. The publication gate
precedes `inner`, queue, and catalog locks; Runtime state is read once for the
execution and actionable interaction. Encoding, budget waits, filesystem work,
and large result destruction occur outside those locks. Claim ownership is
visible as `starting`, never as a missing queued input.

Each installed loop owns a weak-Session Runtime watch observer. It marks the
initial state immediately and stops on Agent completion/close, not sender
closure. The driver and close barrier join it even if Runtime handles remain.
Prompt and model wrappers track the preparation joined by delivered steers;
active config replacements reinstall both wrappers. After tools, the take
before the next prompt belongs to the next request index.

Global catalog eviction marks every affected load without taking its business
locks. Source charges include container/string capacities, metadata, and Arc
ownership overhead; held page references prevent early budget reuse. Catalog
allocation charges cover deque slack, and snapshot descriptor copies use the
separate control budget. An expired source cannot be reconstructed from a
late-wait/active report. An uncertain append remains uncertain until reopen,
including after descriptor eviction and even if a complete record is readable.

Cold Tool reads use the durable auxiliary record when the Session is unloaded or
memory has evicted stream bytes. Missing or corrupt bytes make details unavailable
rather than substituting a nearby Tool call. Workspace queries require a loaded
Session because their source is the Session's canonical Workspace. Workspace
change diffs read fresh Git/worktree sides under their bounded worker contract.

Body and compatibility events are disposable notifications; `session_snapshot`
is the reliable current-control exception. `turn.wait`, `turn.result`, history
queries, and explicit query results are the authoritative terminal/body paths.
The result catalog, not the four-entry late-wait index, defines completion
discovery and unsaved retention. Close publishes its joined final control before
unregistering the load. Unregistration seals sampling under the publication
gate before removing the catalog, so a late reader cannot publish a higher
revision with a reset completion watermark. Dirty batches hold weak Session
references, and samplers release Session resources before encoding. The event
pump preserves charges through outbound
encoding and writer flush; dropping an embedded Rust Agent stops its publisher,
while orderly shutdown joins it.
Explicit queries select warm or durable sources, register any Session/Store
worker synchronously, and apply the same cancellation and absolute-deadline
contract for the Rust API and RPC dispatcher.
