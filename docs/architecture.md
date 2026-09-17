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
   │             │             ├─ CommandOwners ─► Bash workers/process scope
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
  `CommandOwners`, `ToolObserver`, Presentation state, and Session-owned query
  workers. Close cancels and joins those workers before unloading the Session.
- The Runtime owns the `AgentLoop` semantics and model/tool scheduling. The
  Agent's Session task observes the loop, persists the sanitized result, merges
  settled history, and publishes Agent-level completion.
- `ToolObserver` captures the real request/Tool identity at Runtime boundaries.
  `ToolData` retains bounded invocation, execution, stream, and change facts.
  `CommandOwners` owns Bash child cleanup even if the Runtime drops a Tool
  future. `Presentation` formats bounded local display data without changing
  execution or persistence.
- `Store` is the durable authority for Session records, JSONL history, derived
  summaries, and auxiliary Tool/change files. A Store clone shares one budget and
  ownership boundary; independent Agent processes sharing a data directory are
  unsupported.

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
