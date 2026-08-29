# MiniCore Agent

This repository is the Phase 2 RPC-first agent core: one Rust package with a
library API, a local Store, multiple loaded `SessionRuntime` owners, and an
offline Fake Model/Tool test seam. It is verified against the
`minicore-runtime` `dev` HEAD
`7e85eaab18e273e43e03c50040b460f1b13f0ac9` through
`tests/runtime_api_compile.rs`. The runtime dependency is pinned to that exact
Git revision; the local sibling checkout is used only for API review and is not
modified here.

## Run

```bash
minicore-agent --config ./example.agent.toml --stdio
```

The current wire surface is deliberately small:

```json
{"jsonrpc":"2.0","id":1,"method":"agent.ping","params":{}}
```

returns:

```json
{"jsonrpc":"2.0","id":1,"result":{"version":"0.1.0"}}
```

`agent.shutdown` is also accepted. One NDJSON frame is read at a time, stdout
is reserved for JSON-RPC, and malformed or unknown requests return standard
JSON-RPC errors without echoing request data. JSON syntax errors return
`-32700`; valid JSON with an invalid request shape returns `-32600`.

For `agent.ping` and `agent.shutdown`, `params` may be omitted or be the empty
object `{}`. `null`, arrays, and non-empty objects return `-32602`. Request IDs
are limited to strings and JSON integers, including negative integers; the
response preserves the original ID. Frames are read incrementally with a 1 MiB
limit. An oversized frame returns a parse error and ends the stdin loop. EOF
performs the same graceful shutdown path. For an explicit shutdown request, the
agent shutdown completes before its success response is written.

The library exposes `AgentConfig`, `AgentError`, `Agent`, the session/turn DTOs,
the single-consumer `AgentEventStream`, `AgentEvent`, and `run_stdio`. `Agent`
opens the local Store, manages multiple loaded SessionRuntime owners, and
forwards typed live events while authoritative durable turn completion is obtained
from a cloned `TurnHandle`. One outbound sequencer owns each loaded session's
Core event stream, state watch, outer sink, and completion-ready queue. Before
emitting a durable Agent `TurnFinished`, it submits a transcript command to the
same actor, drains already-enqueued Core events, and reads the latest state. Core
`TurnFinished` is advisory because the runtime EventStream is best-effort; if
Core drops that envelope, its unknown `dropped_before` metadata is unrecoverable
and the Agent does not fabricate it. A slow consumer blocks only the sequencer,
never Runtime or request submission. The Store uses
`<data_dir>/sessions/<session-id>/` with `session.json`, `manifest.json`, and
`conversation.log`; each append is one durable JSON line containing one batch.

Metadata uses a unique `create_new` temp file, syncs the temp file, atomically
renames it, and syncs its parent directory. Session and conversation directory
entries are also synced before successful return where the platform supports
it. On Unix/macOS this uses an opened directory and `sync_all`; non-Unix builds
compile and sync file contents, but Tokio has no portable directory-fsync
contract there. `manifest.json` is the initialization commit marker: the empty
conversation file is durable before the marker is published. Direct symlinks in
the session tree, including metadata, manifest, conversation, and temp entries,
are rejected.

The production TOML shape retains the OpenAI Responses model configuration, but
that provider is intentionally not implemented in this Phase and `Agent::open`
returns a stable not-implemented error instead of claiming availability.
Offline loop tests use a crate-private injected Fake Model/Tool/Policy seam;
Fake provider and tool implementations are not part of production TOML. Store
assumes one process per data directory and does not implement file locks.
Loading currently reads the complete log into memory; v0.1 defines no production
log-size limit, so very large logs may consume substantial memory. Each loaded
session retains only its active `TurnHandle`; a `TurnRef` is not a historical
handle registry key, so callers such as RPC must clone the handle at request time.
Session metadata `updated_at` is updated in memory immediately and flushed by one
owned, serialized latest-value worker per loaded session. Temporary unavailable
errors leave that worker alive for the next update; terminal metadata failures
stop it, while metadata persistence remains best-effort and never changes a
submitted turn. Closing explicitly cancels pending metadata work and joins the
worker before deletion. Workspace, real Tools, Context, Policy, OpenAI HTTP, and
RPC method extensions remain outside this Phase. The offline process coverage is
in `tests/rpc_stdio.rs`, Agent loop coverage is in `src/agent/tests.rs`, and Store
coverage is in the internal unit tests of `src/store.rs`.
