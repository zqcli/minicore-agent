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
  `ToolOwners`, `ToolObserver`, Presentation state, and Session-owned query
  workers. Close cancels and joins those workers before unloading the Session.
- Runtime 0.7.0 owns the `AgentLoop` semantics, model-text normalization, and
  model/tool scheduling. Immutable text, arguments, replay, and request payloads
  share storage across clones. The
  Agent's Session task observes the loop, persists the durably normalized result (including validated private replay), merges
  settled history, and publishes Agent-level completion.
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
used for context queries and the history token estimate. Compaction is triggered
by token estimates alone; there are no history item or byte limits.

## Core Modules

- `sessions.rs` retains Session ownership and shared state. Its child modules
  separate public types, the loaded registry, status-worker ownership, active-loop
  execution, event mapping, and context-budget views. `compact.rs` retains
  compaction ownership and commits.
- The active-loop task names its join/drain, reconciliation, persistence,
  projection settlement, and completion/compaction-handoff phases. Publication
  still precedes auxiliary persistence; the owned task remains the close/join
  barrier for those writes.
- `rpc/server.rs` owns deferred tasks and shutdown. `server/dispatch.rs` separates
  agent, session, query, and turn admission; `errors.rs` owns error mapping and
  response construction; `io.rs` owns signals, framing, event pumping, and batched
  output. Success responses carry pre-encoded JSON rather than an intermediate
  `Value` tree.
- `agent/session_setup.rs` builds the Session's execution resources and persisted
  settings. Installation and index publication remain on the Agent.

Lifecycle and streaming events have separate bounded queues. The event stream
prioritizes lifecycle delivery and accounts for drops when dequeuing, so queue
priority cannot duplicate or misplace drop counts. Loaded read snapshots share
immutable timestamp maps; writers copy only when a reader still holds a snapshot.

## Warm, Live, And Cold Data

A loaded Session serves warm reads from its committed in-memory snapshots. A
live `turn.result` may expose a retained completion report even when the Agent's
history append failed; `persistence` tells the caller whether that append
succeeded. Settled history is authoritative only after the Agent persistence
step completes.

Cold Tool reads use the durable auxiliary record when the Session is unloaded or
memory has evicted stream bytes. Missing or corrupt bytes make details unavailable
rather than substituting a nearby Tool call. Workspace queries require a loaded
Session because their source is the Session's canonical Workspace. Workspace
change diffs read fresh Git/worktree sides under their bounded worker contract.

Events are notifications and may be dropped. `turn.wait`, `turn.result`,
history queries, and explicit query results are the authoritative read paths.
Explicit queries select warm or durable sources, register any Session/Store
worker synchronously, and apply the same cancellation and absolute-deadline
contract for the Rust API and RPC dispatcher.
