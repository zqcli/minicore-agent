# Stdio RPC Contract

MiniCore Agent v0.3 exposes JSON-RPC 2.0 over newline-delimited JSON (NDJSON)
on standard input and standard output.

## Transport And Framing

Start the server with:

```text
minicore-agent --config ./agent.toml --stdio
```

Each stdin line is one UTF-8 JSON-RPC request and each stdout line is one
complete JSON object. Requests must fit within 1 MiB including the terminating
newline. Stdout is reserved for RPC responses and `agent.event` notifications;
logs are written to stderr.

The 1 MiB limit covers the whole accumulated request, including bytes received
before an earlier cancellation. The reader retains a partially received frame
across deferred-waiter, signal, and writer-status wakeups, so a request may
arrive in arbitrarily fragmented reads without losing its prefix. A frame's
bytes leave the retained buffer only when the frame completes, at EOF, or when
the frame is rejected as oversized, which discards the accumulated bytes and
reports a parse error.

## Deferred Waiter Capacity

At most 4 read queries may run at once, and at most 32 deferred waiters and
read queries may be registered in total. When either limit is reached, further
deferred requests fail immediately with `-32019` (`resource_exhausted`,
retryable). The reader remains serviceable, so a client can still use ping,
cancel, and shutdown to drain or cancel its work.

A request has this shape:

```json
{"jsonrpc":"2.0","id":1,"method":"agent.ping","params":{}}
```

`id` is required and may be a JSON integer or a string. It is returned without
conversion. `params` may be omitted or must be an object; `null`, arrays, and
unknown fields are rejected. Methods whose params are empty accept either an
omitted `params` member or `{}`.

A successful response has exactly one `result`:

```json
{"jsonrpc":"2.0","id":1,"result":{"version":"0.3.3","protocol_version":1,"capabilities":["session.read","session.context","turn.result","tool.read","tool.output","session.history","workspace.read","workspace.files","workspace.search","workspace.status","changes.list","changes.diff","deferred.waiter_limit"]}}
```

An error response has exactly one `error`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32014,
    "message": "invalid session settings",
    "data": {"kind": "invalid_session_settings", "retryable": false}
  }
}
```

Malformed JSON may produce an error with a null `id` because no request ID can
be recovered.

## Frame Interleaving

All output passes through one bounded channel and one writer task, so every
stdout line is complete and frames are never byte-interleaved. Ordinary
requests are dispatched sequentially. `turn.wait`, `session.compact`,
`session.read`, `workspace.read`, `workspace.files`, `workspace.search`,
`workspace.status`, `changes.list`, and
`turn.result` are exceptions: the server registers one bounded owned task and
immediately continues reading requests. A deferred query/waiter does not own
the underlying Session operation. The two workspace scan queries each own one
retained blocking worker and join it before their response; a drop guard also
cancels that worker immediately when the query task itself is dropped, so a
detached scan stops walking instead of running on. A `workspace.status` query
is different again: its owned worker is registered on the loaded Session, which
runs at most four of them and joins every one of them on close, so a dropped
waiting task cancels only that query's git child and never detaches a child from
the Session that owns it.

Clients correlate responses by `id` and events by Session and loop identifiers.
The following orderings are not guaranteed:

- a `turn.send` response before the corresponding `turn_started` event;
- a `turn_finished` event before the corresponding `turn.wait` response;
- the final `output_delta` or Tool event before `turn_finished`;
- a deferred `turn.wait` response before responses to later requests;
- a deferred `session.compact`, `session.read`, `workspace.read`,
  `workspace.files`, `workspace.search`, or `turn.result` response before
  responses to later requests.

Output deltas and other live events are best effort and may be dropped under
pressure. The authoritative sources are `turn.wait`, `turn.result`, and the
history query results, not the event stream.

`agent.shutdown` waits for the Agent, its active workers, pre-existing
waiter/query tasks, and event pump, then queues its response last. EOF, Ctrl-C,
writer failure, and explicit shutdown enter the same owned-task shutdown path.
MiniCore Agent v0.3 uses the Runtime user-cancellation path when closing or
shutting down an active Session;
it does not currently preserve a distinct shutdown cancellation reason.

## Agent Methods

`agent.ping` accepts omitted params or `{}` and returns the Agent version,
protocol version, and ordered capability names:

```json
{
  "version": "0.3.3",
  "protocol_version": 1,
  "capabilities": [
    "session.read",
    "session.context",
    "turn.result",
    "tool.read",
    "tool.output",
    "session.history",
    "workspace.read",
    "workspace.files",
    "workspace.search",
    "workspace.status",
    "changes.list",
    "changes.diff",
    "deferred.waiter_limit"
  ]
}
```

`agent.shutdown` accepts the same empty params, starts orderly shutdown, and
returns `{"ok":true}` as the final frame on success.

### `agent.reload`

`agent.reload` accepts omitted params or `{}` and rereads only the absolute
lexical configuration path supplied at startup. The request cannot provide a
different path:

```json
{"jsonrpc":"2.0","id":2,"method":"agent.reload","params":{}}
```

On success it returns `{"ok":true}`. The reload parses prompt files and
rebuilds the model/profile catalog and command environment before validating
every loaded Session's future execution snapshot. Session records, selected
models, reasoning, tools, system-prompt snapshots, history, and Store files
are not rewritten. Existing Sessions keep those persisted snapshots; newly
created Sessions use the reloaded profile/default catalog. Active loops keep
their current request configuration and are not updated, cancelled, or
reopened; the new configuration applies to future turns. `data_dir` and the
Agent-level `event_capacity` require restart.

`Agent::open` creates an embedded Agent without a reload source and therefore
returns `reload_unavailable`; the binary uses `Agent::open_file` so its startup
configuration can be reloaded. A profile that names the removed `subagent`
Tool fails the reload with `invalid_profile`; the current catalog, loaded
Sessions, and configuration stay unchanged. A failed reload leaves the current
catalog, loaded Sessions, and configuration unchanged. The reload operation
does not reload the Agent binary or previously stored Session system-prompt
snapshots; workspace `AGENTS.md` remains request-level behavior.

## Discovery

### `profile.list`

Params are omitted or `{}`. Profiles are sorted by `id`.

```json
{
  "profiles": [
    {
      "id": "coding",
      "model": "coding",
      "reasoning": "high",
      "tools": ["read", "write", "edit", "apply_patch", "bash"],
      "approval": "auto"
    }
  ]
}
```

`model` and `reasoning` are defaults for new Sessions.

### `model.list`

Params are omitted or `{}`. Models are sorted by `id`.

```json
{
  "models": [
    {
      "id": "deep",
      "model_ref": "deep",
      "context_window": 197624,
      "supports_tools": true,
      "supported_reasoning": [
        "auto", "disabled", "low", "medium", "high", "xhigh", "max", "ultra"
      ]
    }
  ]
}
```

The example assumes that `deep` is explicitly configured for every listed
value. `supported_reasoning` is a per-model capability allowlist; Agent does
not infer or add reasoning values for every OpenAI model. Add `xhigh`, `max`, or
`ultra` only when the exact model and endpoint document support for that value.
`ultra` is intended only for an endpoint that explicitly supports that custom
or future effort value. OpenAI's official supported effort values remain
specific to the concrete model.

The result does not expose credentials, credential environment names, Provider
URLs, Provider model IDs, request bodies, or pricing.

## Sessions

### SessionInfo

`session.create`, `session.open`, and `session.list` return the same Session
shape:

```json
{
  "session_id": "ses_...",
  "title": "Task",
  "profile": "coding",
  "workspace": "/project",
  "model": "deep",
  "reasoning": "high",
  "loaded": true,
  "created_at": "2026-01-02T03:04:05.006Z",
  "updated_at": "2026-01-02T03:04:05.006Z"
}
```

`title` may be null. `model` and `reasoning` are the actual frozen Session
settings, not newly read Profile defaults. A Session created with the
`disabled` reasoning preference reports it as `"disabled"`.

### `session.list`

Params are omitted or `{}`.

```json
{"sessions":[]}
```

Each element of `sessions` is a SessionInfo object. The Store supplies records
in stable Session ID order. Entries whose persistent record cannot be read are
omitted; explicit operations on such a Session remain strict.

### `session.create`

```json
{
  "workspace": "/project",
  "profile": "coding",
  "model": "deep",
  "reasoning": "high",
  "title": "Task"
}
```

`workspace` is required. `profile`, `model`, `reasoning`, and `title` are
optional. An omitted or empty `profile` selects the configured default Profile.
Omitted `model` and `reasoning` use that Profile's defaults. Explicit overrides
are validated as one combination with the Profile's Tools before any Session is
created; unsupported combinations fail with `-32014` and are never silently
downgraded. Creating a Session never starts a loop.

The result has a `session` member containing the created SessionInfo.

### Other Session Methods

- `session.open` takes `{"session_id":"ses_..."}` and returns a `session`
  member containing SessionInfo (with `loaded: true`) after loading the persistent
  record and history from disk. It never starts a loop.
- `session.close` cancels any active loop or manual compaction, joins all
  Session-owned workers, and returns `{"ok":true}`. MiniCore Agent v0.3 uses
  the Runtime user-cancellation path when closing or shutting down an active
  Session; it does not currently preserve a distinct shutdown cancellation
  reason.
- `session.delete` takes a closed Session ID and returns `{"ok":true}`.
- `session.state` takes a loaded Session ID and returns the current Session
  state projection. While manual compaction is in progress, the optional
  `compaction` member reports only its safe operation ID, phase, and item counts;
  clients use its presence, rather than the ordinary loop `status`, to observe
  compaction busy.
- `session.context` takes a loaded Session ID and returns the current manual
  compaction operation, validated-summary coverage, latest manual result, and
  the estimated Runtime history budget. It is read-only and responds
  immediately, including while manual compaction is busy.
- `session.compact` takes `{"session_id":"ses_...","operation_id":"..."}`
  and returns one deferred result after manual compaction finishes. The
  operation ID is non-empty printable ASCII and at most 128 bytes, and cannot
  be reused during one loaded Session lifetime. At most 4096 IDs are retained
  per loaded Session; the bounded set resets on close/reopen, and a new ID at
  the cap is rejected as invalid input. Compaction is admitted only for a
  loaded, idle, settled, unblocked Session; a busy or duplicate request is
  rejected. `turn.send` and `session.update` are also
  rejected while it is owned by the Session, while metadata-only
  `session.rename` remains allowed.
- `session.compact.cancel` takes the same full identity and returns
  `{"cancelled":true}` only when that exact operation is still owned. A wrong
  or stale ID returns `{"cancelled":false}` and, during that loaded Session
  lifetime, cannot cancel a later operation. Cancellation is cooperative; a
  write already in its atomic commit phase is allowed to finish and an uncertain
  write outcome is reported explicitly.
- `session.presentation` takes a loaded Session ID and returns read-only data
  for a local UI footer and tool cards. It performs no Store mutation, tool
  execution, or loop control.

The manual compaction methods, startup summary projection, automatic compaction
and P3b2 one-shot provider overflow recovery have passed parent-owned remote
verification on this development branch. They are not available in the
previously installed Agent binary; no new installation or release is claimed.
See `archive/0914-progress.md` for acceptance evidence and limits.

The deferred compact result has this shape:

```json
{
  "operation_id": "compact-1",
  "status": "compacted",
  "before_tokens": 4200,
  "after_tokens": 900,
  "covered_loop_count": 6,
  "covered_item_count": 12,
  "retained_item_count": 0,
  "utility_usage": {
    "call_count": 6,
    "complete": true,
    "usage": {
      "input_tokens": 3600,
      "output_tokens": 900,
      "reasoning_tokens": 0
    }
  }
}
```

`status` is one of `compacted`, `noop`, `failed`, or `unknown_write`.
`before_tokens` and `after_tokens` are bounded estimates and may be omitted
when no model call was made. A failed result contains a safe `failure_kind`
but never the generated summary body. `unknown_write` means the atomic
replacement outcome could not be established: the disk may have changed after
the write or rename, while the old in-memory projection remains unpublished.
Clients must reread the Session and snapshot before deciding what to do; they
must not retry blindly.

`utility_usage` is non-null once a utility call attempt has been observed. Its
`call_count` includes an attempt that failed before returning a complete
response. `complete` is true only when generation completed and every completed
call returned usage data; a failed/in-flight call or missing usage makes it
false. The nested `usage` aggregates known fields from completed calls and
from failed streams that emitted a `Usage` event; fields that were not observed
remain unknown. This accounting is independent of ordinary turn
usage; missing fields remain unknown rather than being filled with zero.

The summary utility calls the selected raw model directly with a fresh loop
identity, `request_index: 0`, an empty tool list, and the selected reasoning
preference. History is sent as explicitly labeled data. The complete
`history.jsonl` remains authoritative and unchanged; a successful bounded
`summary.json` is a separately validated derived snapshot. On reopen, the
summary is consumed as a non-system historical-data message and the next
current User message remains a separate input.

### `session.context`

```json
{"session_id":"ses_..."}
```

The result is returned directly:

```json
{
  "session_id": "ses_...",
  "current_operation": null,
  "coverage": {
    "covered_loop_count": 6,
    "covered_item_count": 12,
    "retained_item_count": 3
  },
  "last_result": null,
  "budget": {
    "estimated_history_items": 3,
    "estimated_history_bytes": 1480,
    "estimated_history_tokens": 370,
    "estimated_request_context_tokens": null,
    "input_budget_tokens": 16384,
    "trigger_tokens": 13107,
    "target_tokens": 8192,
    "max_history_items": 4096,
    "max_history_bytes": 1048576,
    "within_runtime_limits": true
  },
  "automatic": {
    "current": null,
    "last": null
  },
  "last_prepare_failure": null
}
```

`current_operation`, when non-null, has the same safe progress shape exposed
by `session.state`; it also names a startup admission preparation. `coverage` is
non-zero only for a currently validated summary snapshot; `retained_item_count`
is calculated against the complete loaded history. `estimated_history_items`,
`estimated_history_bytes`, and `estimated_history_tokens` describe only the
history suffix passed to Runtime as `LoopRequest.history` (or full history when
no valid summary is loaded). They exclude the summary, system/AGENTS text, tool
schemas, current User/Steer input, framing, and provider tokenization. The byte
scan is bounded; bytes and tokens are `null` when the query cannot finish within
that bound, and `within_runtime_limits` is then also `null` unless the item
count already proves an over-limit history. `estimated_request_context_tokens`
is the latest bounded full-request estimate observed by automatic preparation
(the current estimate while preparing, otherwise the last completed estimate);
it is `null` before any automatic preparation. The `automatic` object retains
only current/last operation metadata, fitting estimates, and bounded utility
accounting; it does not expose summary bodies or ordinary model-report usage.
When automatic compaction is enabled, `input_budget_tokens`,
`trigger_tokens`, and `target_tokens` describe the model's already-reduced
context window and the active policy thresholds; they are `null` when the
policy is disabled. `last_prepare_failure` is the most recent
request-preparation failure kind (or `null`, for example before any failure)
and is cleared by a successful preparation. `recovery` describes the latest
provider `ContextOverflow + NotStarted` recovery observation (loop ID, request index,
before/after tokens, independent utility usage, outcome, and failure kind) when
an in-flight recovery has been attempted; the field is omitted when the loaded
Session has no such observation.
The recovery source is retained only up to 512 KiB; a larger source does not
receive a recovery ticket. Token counts are estimates, and recovery never
re-runs tools or duplicates the current User/Steer messages. P3b2 has passed
parent-owned remote verification; see `archive/0914-progress.md`. `last_result` is the latest manual
compaction result retained by this loaded Session process; it is not a durable
history record.

### `session.presentation`

```json
{"session_id":"ses_..."}
```

The result is returned directly (not under another `session` member):

```json
{
  "session_id": "ses_...",
  "model_label": "coding",
  "git_branch": "dev",
  "context": {
    "tokens": null,
    "window": null,
    "percent": null,
    "kind": "unknown"
  },
  "cost_usd": null,
  "using_subscription": null,
  "last_loop": {
    "loop_id": "lup_...",
    "started_at": "2026-01-02T03:04:05.006Z",
    "finished_at": null
  }
}
```

`git_branch` is the branch from the most recent completed explicit
`workspace.status` observation, projected into this read-only view. It starts
as `null` for a freshly created or opened Session and is never obtained by an
implicit Git query: Presentation reads never start Git work, and no lookup runs
at session create/open, after Tool batches, or at loop completion. Clients that
need fresh workspace state call `workspace.status`; `git_branch` is only a
compatibility cache of one observed result. It is `null` for a non-Git,
detached, unborn, or unavailable workspace, and an incomplete or failed status
observation clears it rather than faking a branch.
`model_label` is the configured Session model/profile ID, not a provider model
identifier. Context, cost, and subscription fields are `null`/`unknown` when
MiniCore has no reliable provider source; cumulative Usage is not treated as
current context occupancy.

### `session.read`

`session.read` is a read-only query. It does not load a closed Session, open its
Workspace, validate the configured Model, initialize Tools or PromptProvider,
start a loop, repair `history.jsonl`, or update `session.json`. An already loaded
Session is read from one committed in-memory snapshot and is bounded by that
snapshot's item count. The query therefore continues to work for a closed
Session whose Workspace was deleted or whose Model/Profile is no longer
configured.

```json
{
  "session_id": "ses_...",
  "cursor": {"item": 0, "offset": 0},
  "limit": 100,
  "max_bytes": 262144,
  "captured_end": null,
  "history_revision": null
}
```

`limit` is `1..=100` and is an upper bound; the byte budget may return fewer
items. `max_bytes` defaults to 256 KiB and accepts `1..=1 MiB`. It is an
encoded result-DTO budget, not a character budget. A requested budget outside
the supported range is rejected; it is never silently raised. The entire
encoded result, including Session metadata, record summaries, arrays, commas,
and the next cursor, is checked against that budget. If even one item and its
necessary metadata cannot fit, the query returns `invalid_params` rather than
silently dropping content; raise the budget or reduce the requested range.

The `items` array contains ordered `utf8_json` chunks. Each chunk's `data` is
one UTF-8 slice of the canonical JSON encoding of one sanitized item envelope;
concatenate chunks with the same `index` in offset order and parse the result
when `complete` is true. `offset` and `total_bytes` are UTF-8 byte offsets and
lengths. This permits a single huge User, Assistant, ToolResult, or structured
Assistant part to continue across pages without truncating or skipping it.
The envelope retains the Runtime message parts and full text; only opaque
encrypted/signature-only reasoning is removed by the common history sanitizer.

The response also includes bounded `records` summaries with outcome, usage,
request/tool counts, final config revision, and completion time. It contains
only summaries for turns represented by at least one item chunk in this
response; a small byte budget therefore does not inherit summaries for items
that were not emitted. `history_revision` is a SHA-256 of the
captured complete prefix ending at `captured_end`. Continuation requests must
send both values; a same-length replacement or a non-boundary prefix returns
`invalid_state`. A final incomplete JSONL tail is reported as
`trailing_incomplete` and is never repaired. If the read-only scan reaches its
work/deadline/cancellation bound before establishing the requested prefix, it
returns retryable `query_limit` rather than claiming a complete total; retrying
with a smaller `limit` can reduce retained source work. The item chunks remain
the continuation mechanism when one item itself is larger than the page.

### `session.update`

```json
{"session_id":"ses_...","model":"fast","reasoning":"low"}
```

`model` and `reasoning` are optional but at least one must be present; otherwise
the request is rejected with `-32602`. The change is validated and the persistent
`session.json` updated before the response. Any active loop keeps running with
its current snapshot; the update is forwarded to the loop and takes effect at
the next request boundary.

```json
{
  "session": { "session_id": "ses_...", "...": "..." },
  "active_revision": 3
}
```

`active_revision` is the revision applied to the running loop, or `null` when
the session is idle. It is also `null` when `session.update` races with a loop
that has already sealed: the persistent Session settings are updated for the next
turn, but no revision is applied to the sealed loop.

### `session.rename`

```json
{"session_id":"ses_...","title":"New title"}
```

`title` is required and must be a string. The Agent trims leading and trailing
Unicode whitespace; an empty or all-whitespace title clears the title and is
persisted as `null`. Non-empty titles are limited to 4,096 UTF-8 bytes and may
not contain control characters. The method returns a `session` member with the
complete `SessionInfo` and never changes model, reasoning, tools, history,
workspace, or an active execution revision. It is valid for loaded idle,
running, and blocked Sessions as well as closed Sessions. A closed Session is
renamed from `session.json` metadata only; it does not load its workspace or
history. A successful return confirms persistence. If the request is
cancelled or the response transport disconnects, the outcome may be unknown;
reread the Session before deciding whether to retry.

## Tools

Profiles opt into the five executable Tools explicitly through the `tools` array:
`read`, `write`, `edit`, `apply_patch`, and `bash`. Unknown and duplicate names
are rejected, and a profile that names the removed `subagent` Tool fails to load
or reload instead of silently dropping it. Historical Store v1 records may
contain the removed `subagent` name for read-only compatibility: such records
can be listed, read, and given a new title, and their history, summary, and
retained auxiliary records remain readable through the generic history and
`tool.read`/`tool.output` paths. `session.open` still returns the existing
`invalid_session_settings` error before any model request or Store repair,
because the saved tool list cannot be executed. There is no automatic
migration: the user must close the Session, keep a backup of its data, and then
explicitly change the Session's tool configuration. The Agent never rewrites
an old Profile, `session.json`, or `history.jsonl` on its own. Full delegated
Agent execution (a future `SubagentTool` that drives a complete
`minicore-agent` Session) is separate future work, not implemented here.

## History

### `session.history`

```json
{"session_id":"ses_...","offset":0,"limit":100}
```

`offset` defaults to 0. `limit` defaults to 100 and must be in `1..=100`. Items
are returned in stored history order with contiguous indexes.

```json
{
  "items": [
    {"index": 0, "item": {"type": "user", "data": {"kind": "prompt", "loop_id": "lup_...", "text": "Fix the parser", "timestamp": "2026-01-02T03:04:05.006Z"}}}
  ],
  "next_offset": null,
  "total": 2
}
```

`next_offset` is the offset of the next page, or `null` when the page is the
last. Every `loop_id` refers to the runtime loop that produced the item.

Item types and their `data`:

- `user`: `kind` (prompt or steering), `loop_id`, `text`, and optional
  `timestamp` (the Agent acceptance time in RFC3339; absent for old JSONL or an
  unavailable clock).
- `assistant`: `loop_id`, `request_index`, `model`, `reasoning_level`, `text`,
  `reasoning`, `tool_calls`, `finish_reason`, `usage`, and optional ordered
  `parts`. `reasoning` contains visible reasoning text/summaries; opaque
  provider fields (such as encrypted content) are never included. Each tool
  call exposes only `tool_call_id`, `name`, `call_index`, and optional
  whitelisted `display`; raw arguments are never present in the wire view.
- `tool_result`: `loop_id`, `request_index`, `tool_call_id`, `tool_name`,
  `outcome`, bounded `content`, and optional `content_truncated` when the
  result exceeded the local display cap.

The history view is sanitized: user text and tool results pass through, while
tool arguments and opaque provider content never appear.

## Turns

### `turn.send`

```json
{"session_id":"ses_...","text":"Fix the parser"}
```

Starts one runtime `AgentLoop` for the user message and returns its exact Turn
identity:

```json
{"turn":{"session_id":"ses_...","loop_id":"lup_..."}}
```

A Session runs at most one active loop. Additional sends while one is active
fail with `-32003` (session_busy); a Session blocked by a persistence failure
fails with `-32004`.

The successful result preserves the original `turn` member and may add
`accepted_at`:

```json
{"turn":{"session_id":"ses_...","loop_id":"lup_..."},"accepted_at":"2026-01-02T03:04:05.006Z"}
```

The timestamp is when the Agent accepted the Prompt (for a deferred automatic
submission, when its prepared loop is installed), not when the loop or provider
request completes. It may be omitted if the clock was unavailable.

When automatic compaction is enabled, `turn.send` is deferred while the
Session computes a bounded startup estimate after checking the irreducible
minimum and Runtime structural limits. If the projected history exceeds
the Runtime limits or the request exceeds the trigger, the Session first folds
a settled prefix into a bounded `summary.json`; a fitting request creates its
loop directly from the same preparation worker. The deferred response carries
the same result shape once the loop exists; `session.context` reports the
preparing operation while it runs, and `session.compact.cancel` terminates a
preparation that has not started its loop. If the waiter pool is full, the
just-started preparation is cancelled and the request fails with `-32019`
(`resource_exhausted`) rather than running an unawaitable operation. A request
whose irreducible system/current-input/tool-schema minimum still exceeds the
hard model window fails with `-32022` (`context_uncompressible`); semantic summary
failures return an internal error and do not start a loop, while their stable
failure kind is retained in `session.context.last_prepare_failure`. A disabled
policy keeps the previous immediate `-32015` (`history_too_large`).

### `turn.wait`

```json
{"session_id":"ses_...","loop_id":"lup_..."}
```

Registers a deferred waiter and returns immediately; the reader keeps
processing frames. When the Agent-level persistence step completes, the waiter
response arrives:

```json
{
  "turn": {"session_id":"ses_...","loop_id":"lup_..."},
  "outcome": {"type":"completed"},
  "usage": {"input_tokens":10,"output_tokens":6},
  "requests": 2,
  "tool_rounds": 1,
  "final_config_revision": 0,
  "persistence": "persisted"
}
```

`outcome.type` is `completed`, `cancelled` (with `reason`), or `failed` (with
`kind` and optionally `model_error`). `persistence` is `persisted` or `failed`.
`persistence: persisted` means the Agent's append operation completed
successfully in the running process. The Store is not a transactional ledger and
does not provide an end-to-end crash-durability proof. A failed JSONL append
returns the completed loop report with `persistence: failed` and blocks the
Session from further turns.

### `turn.result`

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "cursor": {"item": 0, "offset": 0},
  "limit": 100,
  "max_bytes": 262144
}
```

`turn.result` returns a bounded page using the same sanitized JSON item chunks
and `1..=100`/`1..=1 MiB` budget rules as `session.read`. Its item envelope
intentionally omits the session User timestamp, so a page sequence remains
stable if a retained live result later falls back to its stored record.
`availability` is `pending` while the current Runtime loop has not published a
report, `live` while its retained completion report is available, and `stored`
when the exact old Turn is read from `history.jsonl`. A live report is preferred
to Store, so an append failure still exposes the retained report with
`persistence: failed`; a stored Turn reports `persistence: persisted`. Runtime
outcome and usage are returned when known.

### `turn.cancel`

```json
{"session_id":"ses_...","loop_id":"lup_..."}
```

Cancels the active loop and returns `{"cancelled":true}`. The corresponding
`turn.wait` then resolves with a `cancelled` outcome.

### `turn.steer`

```json
{"session_id":"ses_...","loop_id":"lup_...","text":"Do not modify config files"}
```

Appends a steering instruction to the active loop and returns `{"ok":true}`
with optional `accepted_at`. The timestamp is the Agent acceptance time for
that Steer, not its later application to a model request. A full steer queue is
reported with `-32016`.

## Tool Data

`tool.read` and `tool.output` expose the structured facts the Agent recorded
for one tool call. Both address a call by the complete identity, never by tool
name or "most recent call":

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "request_index": 0,
  "tool_call_id": "..."
}
```

All four values come from real Runtime boundaries: the session id, the
`ModelCallContext` request at `Model::start`, and the Runtime tool-call id. A
query whose identity was never recorded returns `-32021` (`tool_not_found`,
non-retryable) rather than a nearby match.

In the RPC server, `tool.read` and `tool.output` are deferred queries executed
in the query pool under a 10-second deadline. They are bounded by the 4-query
concurrency ceiling and count against the shared 32-entry deferred admission
capacity; requests exceeding capacity are rejected with `-32019`
(`resource_exhausted`, retryable). Loaded Sessions serve complete records
directly from memory with zero disk IO. Unloaded Sessions or records whose stream
bytes were evicted under memory budget fall back to durable auxiliary storage on
disk (cold read). If auxiliary records are absent or unreadable, the query
cleanly returns `-32021` (`tool_not_found`).

In the Rust library API, `Agent::tool_read` and `Agent::tool_output` are
`async fn` methods returning futures that must be `.await`ed. While the JSON-RPC
wire format remains backward-compatible, this is a source-level breaking change
for Rust consumers migrating from synchronous signatures.

### `tool.read`

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "request_index": 0,
  "tool_call_id": "...",
  "max_bytes": 262144
}
```

`max_bytes` uses the same `1..=1 MiB` rule and defaults to 256 KiB. The result
contains `execution` and, once the call reached its real execution boundary,
an `invocation`. Invocation data is recorded when the validated request reaches
the policy boundary, before the approval decision and before the tool's own
parse/validation, so it is already obtainable while the call awaits approval.
The recorded arguments are the *requested* input; they are not proof that the
tool accepted them.

```json
{
  "invocation": {
    "tool_ref": { "session_id": "ses_...", "loop_id": "lup_...", "request_index": 0, "tool_call_id": "..." },
    "name": "write",
    "subject": { "kind": "file", "path": "src/main.rs" },
    "subject_truncated": false,
    "input": { "total_bytes": 42, "preview": "{\"path\":\"src/main.rs\",...}", "truncated": false, "encoding": "utf8_json" }
  },
  "execution": {
    "tool_ref": { "session_id": "ses_...", "loop_id": "lup_...", "request_index": 0, "tool_call_id": "..." },
    "name": "write",
    "state": "succeeded",
    "phase": "writing",
    "started_at": "2026-09-14T00:00:00.000Z",
    "finished_at": "2026-09-14T00:00:01.000Z",
    "outcome": "success",
    "input_availability": "available",
    "output_availability": "available",
    "input_bytes": 42,
    "result_bytes": 21,
    "input_truncated": false,
    "result_truncated": false
  }
}
```

`state` is one of `requested`, `awaiting_policy`, `running`, `succeeded`,
`failed`, `denied`, `cancelled`, `input_provided`. `awaiting_policy` is never
reported as `running`, and `started_at` is set only when the tool actually
runs: the tool has not been invoked before the decision. `phase` is the last
real, whitelisted execution stage (`reading`, `writing`, `matching`,
`committing`, `running`) — arbitrary `ToolContext.progress` text is ignored.
`outcome` is the authoritative Runtime result outcome.

Availability is per stream, so evicting an input does not make a later result
unqueryable. Each of `input_availability`/`output_availability` is `pending`
(still running, no bytes observed), `unavailable` (terminal, but no bytes were
observed in this process), `available`, `partial` (a prefix only), or `expired`
(retained bytes were evicted). `*_truncated` reports whether the retained bytes
are only a prefix.

`subject` is structured: `{"kind":"file","path":...}` for the file tools,
`{"kind":"command","script":...,"cwd":...}` for Bash, and `{"kind":"other"}`
for unknown tools. Raw arguments remain readable through
`tool.output` on the `input` stream.

### `tool.output`

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "request_index": 0,
  "tool_call_id": "...",
  "stream": "output",
  "offset": 0,
  "max_bytes": 8192
}
```

`stream` is `input` (canonical JSON of the requested invocation arguments),
`output` (the recorded tool result text), `stdout`, or `stderr`. `encoding` is
`utf8_json` for `input`, `utf8` for `output`, and `base64` for the two process
streams, whose bytes are not required to be valid UTF-8. For `input` and
`output`, `offset`, `next_offset`, and `observed_end` are UTF-8 byte offsets;
for `stdout` and `stderr` they are raw byte offsets and never base64 positions,
so decoding a page and continuing from `next_offset` reads the same bytes
exactly once. `base_offset` identifies the first returned byte; an empty gap
notice identifies the retained window start. Continue with `next_offset` until
`eof`. Normal page-budget pagination is not truncation: `truncated` marks
discarded bytes or an observation cut short. A stale-offset gap notice points
`next_offset` to the retained start with `eof: false`, allowing the next page to
read the retained tail even when the process has already exited.

Process streams are written by the owned Bash command as it runs, before any
best-effort event about the same bytes. A page never claims an end of output
that the owner has not observed: `eof` on `stdout`/`stderr` requires an ended
observation (real EOF or an explicitly incomplete cut) and delivery of the
remaining retained bytes. Each stream keeps a bounded tail window (1 MiB),
whose allocation capacity is counted into the Session's 8 MiB /
1024-record tool budget, so the oldest bytes are dropped first and reported as
`truncated` with a `base_offset` that moves forward; evicting a window frees
capacity and never renumbers offsets. The two streams are independent: no total
order between them is implied.

`tool.read` for the same call reports a `command` record while a Bash call is
`running`: `status` (`running`, `cancelling`, `exited`, `cancelled`,
`timed_out`, `spawn_failed`, `failed`), a nullable `exit_code`, an optional
`signal`, `termination_confirmed`, the retained ranges of both streams, and
`output_complete`/`output_truncated`. `termination_confirmed` is true only when
the owned process group (Unix) or job object (Windows) was really observed to be
gone; a requested stop, a failed spawn, a failed reap, or elapsed time is never
reported as termination. The Runtime outcome is separate: a non-zero exit is a
completed tool result, not an RPC error, and a cancelled or timed-out command
never reports a fabricated exit code. A requested `cancel` is recorded as
`cancelling` first; the terminal status follows only after the owner joined the
process.

`execution.recording` reports the auxiliary write result: `memory_only`,
`saved`, or `failed`. Saving follows main History persistence and precedes Turn
completion. A failed auxiliary save does not change the real tool outcome or
main Turn persistence, and retained memory remains readable. `saved` is not a
promise of indefinite retention: the Store applies per-tool (3 MiB), per-Session
(16 MiB/1024 records), and Store-wide (256 MiB/8192 records) auxiliary budgets.
These budgets coordinate one Store and its clones; independent writers sharing
a data directory are not supported. Public queries currently use loaded memory;
stored-data query access follows in P5b2.

`availability` is per stream: `pending` while the call is still running and
that stream has no observed bytes (with `observed_end: 0` and `eof: false`),
`unavailable` when no observation is available for a terminal call, then
`available`, `partial`, or `expired`. A genuinely empty stream that reached EOF
is `available`. An evicted but running process stream is not EOF merely because
its retained bytes were freed. A stream whose observation was cut short by a
stop or a stopped scope is `partial` and reports `eof` with `truncated: true`,
never as a clean empty end. A non-zero `offset` on an unobserved stream, or an
`offset` past `observed_end`, is rejected with `invalid_params`; `next_offset`
never moves backwards. An evicted stream returns an empty `data` with
`availability: expired` rather than claiming no output.

Returned bytes are the recorded original text. They are never rewritten
through `escape_default`, and offsets always refer to the raw bytes. Clients
remain responsible for not executing ANSI/control characters as terminal
instructions. A requested `max_bytes` outside the supported range is rejected
with `invalid_params`; it is never silently raised.

## Workspace

### `workspace.read`

`workspace.read` is a bounded, read-only query over the Workspace of a loaded
Session. It does not create, open, or load a Session, does not start a model
call, does not append history, and does not mutate the file, so a frontend can
page through a file while a turn is in flight. A closed or unknown Session is
rejected with `-32002` (`session_not_loaded`); closing the owning Session
cancels its in-flight reads with `-32020` (`query_limit`). The query never
derives a root from a path. It holds one of the 4 concurrent read-query slots
described above. P4a has passed parent review and remote verification; see
`archive/0914-progress.md` for the gates and platform limits.

```json
{
  "session_id": "ses_...",
  "path": "src/main.rs",
  "start_line": 1,
  "line_byte_offset": 0,
  "max_lines": 400,
  "max_bytes": 65536,
  "if_revision": null
}
```

`path` is a workspace-relative path of a regular file, at most 4096 bytes;
absolute paths, `..`, and NUL are rejected with `-32602` before a read-query
slot is reserved. The path is resolved against the canonical Workspace root, and
the open is non-blocking on Unix and does not follow a final symlink, so a
special file cannot block the query. That boundary is a sandbox for cooperative
local clients, not a defense against an adversarial process running as the same
user.

`start_line` is one-based and defaults to `1`. `line_byte_offset` is a UTF-8
byte offset inside that line and defaults to `0`; it must be sent together with
`start_line`, fall inside the line, land on a character boundary, and not
exceed the 512 KiB whole-file bound. It exists to continue a line that a
previous page cut. `max_lines` accepts `1..=2000` and defaults to `400`,
matching the read Tool's line window. `max_bytes` is an
encoded result budget like `session.read`'s: it accepts `1024..=262144`,
defaults to 64 KiB, and covers this result's metadata and JSON escaping rather
than the content length. A value outside these ranges, an offset outside the
requested line, or a budget that cannot hold the metadata envelope plus one
character is rejected with `-32602`; the budget is never silently raised.

```json
{
  "path": "src/main.rs",
  "content": "fn main() {}\n",
  "start_line": 1,
  "returned_lines": 1,
  "revision": "536e506bb90914c243a12b397b9a998f85ae2cbd9ba02dfd03a9e155ca5ca0f4",
  "truncated": false,
  "line_truncated": false,
  "next_range": null,
  "encoding": "utf8",
  "status": "ok",
  "file_bytes": 13,
  "file_modified_unix_ms": 1760000000000
}
```

One read performs a single bounded read: it uses the read Tool's 512 KiB
whole-file bound (`MAX_READ_BYTES`), one open, one read, and metadata before and
after. The UTF-8/NUL check, `revision`, and the returned page all describe those
same bytes, so a response can never combine an older hash with newer content. A
file larger than the bound returns `status: too_large` with empty `content` and
`revision: null`; there is no partial preview and no partial revision.
`revision` is the whole-file SHA-256 in lowercase hex of the bytes that were
read, present only when they form one consistent whole file. When the file
changes while it is read (size or full modification time differs before and
after), the response is `status: changed` with empty content and no revision.
Reads describe a live observation of those bytes: they never promise a
filesystem snapshot or lock out a concurrent writer.

`content` is the raw file text exactly as stored: line numbers are never
prepended, CRLF is preserved, and escaping is never applied. This differs
intentionally from the numbered rendering of the `read` Tool, and reading a
workspace file adds nothing to history. `returned_lines` counts the lines the
content touches; a final line without a trailing newline still counts once, and
a trailing newline does not create an extra line. `file_bytes` and
`file_modified_unix_ms` are the metadata observed after the read, and the latter
is `null` when the filesystem does not report a modification time.

`status` describes what the response contains:

| Status | Meaning |
|---|---|
| `ok` | `content` holds the requested page. |
| `binary` | The whole file contains NUL or is not valid UTF-8; `content` is empty and `encoding` is `unknown`. |
| `changed` | The bytes did not form one consistent whole file, or `if_revision` no longer matches; `content` is empty. |
| `too_large` | The file is larger than the whole-file bound; `content` is empty and `revision` is `null`. |

`if_revision` is compared case-insensitively against `revision`; a mismatch
returns `changed` with no content, and `revision` carries the revision that was
observed so a client can adopt it instead of silently reading a newer file.

Pagination is byte-exact. `next_range` carries the next `start_line` and
`line_byte_offset`, and it always advances. A page that ends inside a line
(`line_truncated: true`) continues that same line at the byte offset after the
returned content, so the remainder is never skipped. Carry `if_revision` on
continuation requests: concatenating pages of the same revision reproduces the
file bytes exactly, and a CRLF may be split across pages. Without that condition,
each page is an independent live observation and a concurrent edit may change it.
`truncated` is true when the content does not reach the end of the requested
range. Both the line window and the encoded byte budget can cut a page, and a
single line longer than the budget is returned across as many pages as it
needs. `truncated` and `next_range` describe an `ok` page; `binary`, `changed`,
and `too_large` return empty content, no next range, and `truncated: false`.

Cancellation and deadlines wrap the actual IO: the session close token, RPC
shutdown, and the shared 10 s query deadline each interrupt a pending open,
read, or metadata call and return retryable `-32020` (`query_limit`). Finishing
or cancelling a query releases its slot, and shutdown cancels and joins all
owned queries before the shutdown response.

### `workspace.files`

`workspace.files` lists one bounded page of Workspace entries. It shares the
loaded-Session ownership, the ignore-aware traversal, the four read-query
slots, and the cancellation rules of `workspace.read`. The P4b files/search
slice has passed parent-owned remote verification; see `archive/0914-progress.md`.

```json
{
  "session_id": "ses_...",
  "directory": "src",
  "recursive": false,
  "query": "main",
  "cursor": null,
  "limit": 200,
  "max_bytes": 65536
}
```

`directory` is a workspace-relative directory of at most 4096 bytes and
defaults to the Workspace root. Absolute paths, `..`, NUL, and a path that names
a file are rejected: lexical problems fail with `-32602` before a query slot is
reserved, and a directory that does not exist, is not a directory, or resolves
outside the Workspace fails with `-32010`. `recursive` defaults to `false` (one
level); recursive listings stop descending at a depth of 64 and report that as
`stopped_by: depth`. `query` filters entry paths only and never looks at file
contents; directories that do not match are still traversed so matching
descendants are found. `limit` accepts `1..=1000` and defaults to `200`;
`max_bytes` accepts `1024..=262144`, defaults to 64 KiB, and bounds the encoded
page including metadata and JSON escaping.

```json
{
  "directory": "src",
  "entries": [
    {"path": "src/main.rs", "kind": "file", "size": 4096}
  ],
  "next_cursor": {"entry": 12, "scope": "3f2a91c4d0b7e615"},
  "truncated": false,
  "scan_complete": true,
  "stopped_by": "end",
  "skipped_count": 0,
  "consistency": "live",
  "observed_at_unix_ms": 1760000000000
}
```

The traversal is a depth-first walk in the filesystem's directory order; entries
are not sorted. `.git` metadata is never returned or expanded. `.ignore`,
`.gitignore`, and `.git/info/exclude` files inside the Workspace are applied
whether or not the Workspace is a git repository, with `.ignore` outranking
`.gitignore` outranking `.git/info/exclude`, and the closest directory winning
inside a category. A rule file only ever matches a strict descendant of the
directory it was loaded from, so a rule cannot leak onto an ancestor or a
sibling and a requested root is never filtered by its own file. A requested
subdirectory inherits the rules of every ancestor up to the Workspace root, so
an explicit `directory` cannot bypass them. Ignore files above the
Workspace root and the global git configuration are never read, so a parent
`.gitignore` outside the Workspace has no effect. A Workspace that is not a git
repository additionally excludes directories named `node_modules`,
`bower_components`, `vendor`, `target`, `dist`, `build`, `.venv`, `venv`,
`__pycache__`, `.pytest_cache`, `.mypy_cache`, `.tox`, `.gradle`, `.next`, and
`.nuxt` at any depth; local rules still take precedence over that list. Rule
reading is bounded per query at 128 files, 1 MiB in total, and 256 KiB per file,
with bytes charged as they are read: an oversized or over-budget rule set stops
the scan with `stopped_by: rules` instead of applying partial rules. A symlinked
ignore file is read only when it resolves inside the Workspace; a rule file that
exists but cannot be read, or that resolves outside it, is counted as skipped
and makes the response incomplete instead of being treated as absent. Symlinks are listed as `symlink` entries but
never followed: a requested root that is itself a symlinked directory is
rejected instead of being expanded through the alias, and a requested directory
that resolves outside the Workspace is rejected.

`kind` is `file`, `directory`, `symlink`, or `other`, and `size` is present for
regular files only. `skipped_count` counts visited entries that were not
returned: entries filtered out by `query`, entries whose path is not valid
UTF-8, entries that could not be read, explicitly requested roots excluded by
the rules, and single entries that cannot fit the result budget. Rule-excluded
descendants are not counted; they are not part of the visible tree.

Results are live observations, never filesystem snapshots: `consistency` is
`live` and `observed_at_unix_ms` records when the scan started. Concurrent
changes may require a fresh request. `stopped_by` explains why the scan ended:

| Stop | Meaning | Continues |
|---|---|---|
| `end` | The traversal reached the end of the requested scope. | No |
| `page` | `limit` or `max_bytes` filled the page. | Yes |
| `entries` | The 100,000-entry ceiling was reached. | No |
| `bytes` | The 16 MiB path-byte ceiling was reached. | Yes |
| `depth` | A directory at the 64-level depth ceiling was not expanded. | No |
| `rules` | The ignore-rule budget was exceeded. | No |
| `deadline` | The 10 s query deadline passed. The page keeps what it found. | No |

The entry ceiling is per query and counts every raw directory entry every root
consumes, including entries consumed while positioning at a cursor, so a resumed
request cannot sidestep it. A cursor's `entry` is an ordinal inside its own
requested root, starting at zero, while that ceiling is shared by all roots of
the query. Positioning replays raw entries, including rule-excluded ones, and
still descends into directories, so a resumed page never loses a subtree; when
positioning cannot reach the cursor, or the rule budget stops the scan, no
continuation is returned. The 10 s deadline is checked between bounded
operations. When it expires the scan stops with `stopped_by: deadline`, keeps the
entries already found, reports `truncated` with `scan_complete: false`, and
returns no `next_cursor` on purpose: a timeout is answered with a narrower
request or a later retry, never with a continuation. Session close, RPC shutdown,
and caller cancellation are not stops: they fail the query with `-32020`, and the
worker is joined before that error is returned. A single blocking read on a
stalled remote filesystem can outlast the deadline, so the timeout is enforced
at operation granularity rather than as a hard preemption guarantee; results
already produced still appear in the partial page. `truncated` is true whenever
the response is a partial view (`page`, `entries`, `bytes`, `depth`, `rules`,
`deadline`, or a skipped entry), and `scan_complete` is true only for a complete
traversal in which nothing was skipped or unreadable.
`next_cursor` is an exact continuation point bound to the Session, method, and
traversal parameters, and it is present only when the scan can advance: `end`,
`depth`, `entries`, `rules`, and `deadline` stop without one, and a resumed
request whose positioning exhausts the entry budget also stops without one, so a
returned cursor always moves forward. Paging a static tree with it never skips or
duplicates entries; a cursor from a different request or Session is rejected
with `-32602` before a query slot is reserved. A resumed request re-walks to its
cursor within the same budget instead of caching an index, so no snapshot,
index, or watcher is ever created.

### `workspace.search`

`workspace.search` returns one bounded page of literal matches. It shares the
traversal, ignore rules, bounds, result budget, ownership, and cancellation
contract of `workspace.files`.

```json
{
  "session_id": "ses_...",
  "query": "needle",
  "paths": ["src", "docs/rpc.md"],
  "case_sensitive": false,
  "cursor": null,
  "max_matches": 100,
  "max_bytes": 65536
}
```

`query` is a single literal line of at most 1024 bytes and is never interpreted
as a regular expression: metacharacters match themselves. Newlines and carriage
returns are rejected with `-32602`. `case_sensitive` defaults to `false`; a
case-insensitive search escapes the query and matches it with a case-folding
expression over the original line text, so reported ranges always fall on UTF-8
boundaries of the returned `line_text`. `paths` restricts the search to at most
32 workspace-relative files or directories and defaults to the whole Workspace.
The list is normalized and de-duplicated, and overlapping roots such as `src`
together with `src/main.rs` are rejected with `-32602` so no file is searched
twice; each path is validated lexically before a query slot is reserved, a path
that does not exist or resolves outside the Workspace fails with `-32010`, and
explicitly named paths are still subject to the same ignore rules as a walk from
the Workspace root. `max_matches` accepts `1..=1000` and defaults to `100`;
`max_bytes` has the same range and default as `workspace.files`.

```json
{
  "matches": [
    {
      "path": "src/main.rs",
      "line_number": 12,
      "line_text_byte_offset": 0,
      "match_byte_ranges": [{"start": 4, "end": 10}],
      "line_text": "let needle = 1;",
      "line_truncated": false
    }
  ],
  "next_cursor": {"path_index": 0, "entry": 3, "line": 12, "line_byte_offset": 0, "scope": "3f2a91c4d0b7e615"},
  "truncated": false,
  "scan_complete": true,
  "stopped_by": "end",
  "skipped_files": 0,
  "consistency": "live",
  "observed_at_unix_ms": 1760000000000
}
```

Line numbers are one-based and a match is one record per matching line segment.
`line_text` is raw line text without its `
` or `
` terminator, starting at
`line_text_byte_offset` inside the original line; `match_byte_ranges` are
half-open UTF-8 byte offsets inside `line_text`, so adding
`line_text_byte_offset` yields offsets in the original line. When the whole line
fits the result budget it is returned whole with `line_truncated: false`.
Otherwise the record carries a bounded slice that still contains every match of
that record plus up to 64 bytes of leading context, and `line_truncated` is
true. A page that cannot hold the next match stops before it and resumes there,
so paging a static tree yields every match exactly once across the returned
slices, and a line whose occurrences are cut by `max_matches` or the result
budget continues through the cursor. A match that cannot be represented in an
otherwise empty page is skipped, counted in `skipped_files`, and marks the
response incomplete instead of returning an empty record or a complete-looking
one.

Files larger than the 512 KiB whole-file bound, binary or non-UTF-8 files,
special files, files outside the Workspace boundary, and files with non-UTF-8
paths are skipped and counted in `skipped_files` without making the result
incomplete: the scope was enumerated, but those files were not searched. A file
that could not be opened, typed, or read, and a match that could not be
represented, are also counted and make the response incomplete. Content bytes
are charged against the 16 MiB ceiling as they are read, so a file that is read
and then skipped is still accounted for. The total searched
content is capped at 16 MiB, the raw entry ceiling at 100,000, the depth at 64
levels, and the rule budget as for `workspace.files`; `stopped_by`, `truncated`,
`scan_complete`, and `next_cursor` follow the same rules and table, so a deadline
keeps the matches already found, reports `deadline`, and returns no cursor.
Files are read through the same Workspace boundary as `workspace.read`, in
bounded chunks that re-check cancellation and the deadline between chunks, lines,
and matches, so a timeout never discards committed matches and never asks for a
continuation.

### `workspace.status`

The P4c slice has passed parent-owned remote verification; see
`archive/0914-progress.md` for the gates and platform limits.

```json
{"session_id": "ses_...", "max_bytes": 65536}
```

`max_bytes` is optional and accepts `1024..=262144`, defaulting to 64 KiB; it
bounds the encoded response, not git output. Only a loaded Session locates its
Workspace, and closing that Session cancels the query.

```json
{
  "repo_available": true,
  "head_oid": "3f2a91c4d0b7e6153f2a91c4d0b7e6153f2a91c4",
  "branch": "main",
  "detached": false,
  "staged": 1,
  "unstaged": 1,
  "untracked": 2,
  "conflicted": 0,
  "entries": [
    {"path": "src/main.rs", "kind": "ordinary",
     "index_status": "M", "worktree_status": ".", "original_path": null}
  ],
  "skipped_paths": 0,
  "complete": true,
  "warnings": [],
  "consistency": "live",
  "observed_at_unix_ms": 1760000000000
}
```

One query runs at most three fixed git commands without a shell:
`rev-parse --show-toplevel` locates the work tree, `rev-parse
--is-bare-repository` classifies a workspace without a work tree, and `status
--porcelain=v2 -z --branch --no-ahead-behind --ignore-submodules=dirty
--untracked-files=normal` reads it, with `--no-optional-locks` so a status query
never writes the optional index state, `--literal-pathspecs` so no pathspec is
interpreted, and fixed `-c` values that disable the file system monitor,
submodule recursion, and colour, and fix rename detection. Every
inherited `GIT_*` variable is dropped before the child starts (by name, ASCII
case-insensitively), and system and user configuration are disabled explicitly
with `GIT_CONFIG_NOSYSTEM=1` and an empty `GIT_CONFIG_GLOBAL`, so no `GIT_DIR`,
`GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_CONFIG_*`, `GIT_TRACE*`, `HOME`, or
`XDG_CONFIG_HOME` value can redirect, instrument, or start an external program
for the query. Git's standard error is counted and discarded, so no diagnostic
and no path from it can reach a response or a log; a child that floods it is
stopped instead of drained without bound. The query never stages, commits,
fetches, restores, or calls a model.

Submodule internals are outside the coverage of this observation: the status
command compares the recorded gitlink with the index but never scans a
submodule's work tree for modifications or untracked files and never recurses
into it, so a file modified or added inside a submodule is not reported, and no
monitor or hook configured inside a submodule runs. Commit-level gitlink changes
are ordinary entries: the comparison uses the commits recorded in the
superproject, so a gitlink recorded in the index that differs from the committed
one is reported with its `XY` codes and counted like any other recorded change.

Untracked directories are reported the way git collapses them; ignored entries
are not requested at all. `entries` is a prefix of the changed paths with git's
own `XY` codes, and `staged`, `unstaged`, `untracked`, and `conflicted` count
the whole observation, which may be longer than that list when `max_bytes` cut
it short. `kind` is `ordinary`, `renamed`, `unmerged`, or `untracked`;
`original_path` carries the previous name of a rename and is absent when that
name lies outside the workspace. A rename record whose previous name did not
arrive is malformed and is never reported as a complete rename.

Some states are ordinary answers rather than errors: `repo_available` is false
when the workspace is not a work tree for a definite reason (`--is-bare-repository`
answers for a bare repository or a git directory without a work tree);
`head_oid` is absent for an unborn `HEAD`; `branch` is absent for a detached
`HEAD`; conflicts appear as `kind: unmerged` with `conflicted` counted. A
workspace git cannot read for an unexplained reason (dubious ownership, a
corrupt configuration, a permission problem, a signal death) is reported as
`repo_available: false` with `complete: false` and the `status_failed` warning,
never as a definite missing repository. Session close, RPC shutdown, and
cancellation fail the query with `-32020` after the owned git process is
stopped and reaped. The 10 s deadline keeps whatever the query already captured,
reports `deadline`, and marks the result incomplete. Stopping a child is not a
latency guess: the owner waits until the operating system reports the exit, so a
system that never reports it can delay a query or a Session close past the
deadline, exactly like a blocking filesystem read, and an unconfirmed kill or
wait is reported as a failed observation. `complete` is true only
when nothing was cut short or skipped, and `warnings` names each limitation
with a code (`git_unavailable`, `status_failed`, `output_truncated`,
`deadline`, `skipped_paths`, `nested_repository`) and never a path. The budget
reserves room for the widest form of the summary, so a result never exceeds
`max_bytes` even when a warning is added while entries are appended.

The loaded Session owns each status worker: the worker keeps the git child
until it is stopped and reaped, the query caller only observes the worker's
result, and dropping that caller cancels that one child without touching the
Session. Closing the Session stops its workers and joins them, so no child is
left behind, and a Session runs at most four status workers at once (a fifth
query is refused with `-32020`). Both the
public Agent method and the RPC method go through this same Session-owned path.
When the workspace is a subdirectory of a larger repository, git is given the
workspace as a literal pathspec, every reported path is checked against the
canonical workspace root, and paths outside it are neither counted nor
returned; that case adds the `nested_repository` warning. A path that is not
valid UTF-8 or is longer than 4096 bytes is counted in `skipped_paths` and
makes the result incomplete. Results are live observations with no author
attribution: changes made before, by other tools, or by other processes look
the same, and no Turn, history entry, or model call is involved. There is no
watcher and no cache: every result comes from the query that produced it.

### `changes.list`

The P6a change-review query lists bounded file-change records. It is read-only
and does not call a model, write History, create a Session, or run a garbage
collector. `changes.diff` resolves the references this method returns.

```json
{
  "session_id": "ses_...",
  "scope": "workspace",
  "cursor": null,
  "limit": 100,
  "max_bytes": 65536
}
```

`scope` is either `workspace`, `session`, or
`{"turn":{"loop_id":"lup_..."}}`. The workspace scope is a live Git
observation and has `origin: "workspace_unknown"`; it never attributes a
pre-existing, Bash, editor, or other-Agent change to a ToolRef. Session and
turn scopes use only retained native `write`, `edit`, and `apply_patch`
records with their complete ToolRef identity. A turn scope is filtered by the
exact Loop ID, not by presentation text or a request key.

```json
{
  "scope": "session",
  "records": [{
    "change_ref": "tool:<sha256>",
    "path": "src/main.rs",
    "kind": "modified",
    "origin": "tool",
    "tool_ref": {
      "session_id": "ses_...",
      "loop_id": "lup_...",
      "request_index": 0,
      "tool_call_id": "call_..."
    },
    "before": {"kind": "content", "bytes": 12, "sha256": "..."},
    "after": {"kind": "content", "bytes": 15, "sha256": "..."},
    "commit_state": "applied",
    "details_available": true,
    "coverage": "complete"
  }],
  "next_cursor": null,
  "total": 1,
  "complete": true,
  "stale": false,
  "consistency": "cold",
  "warnings": []
}
```

The response is charged by encoded JSON bytes and accepts `1024..=262144`
bytes, defaulting to 64 KiB. `limit` is optional, defaults to 100, and accepts
`1..=1000`; the byte budget may return fewer records. `cursor` contains the
record offset and the scope binding. A continuation cursor
is returned only when the page ended at a record boundary and the observation
or durable record set remains the same. Workspace cursors bind the Git
observation fingerprint; a changed observation returns `stale` rather than
silently reading a new version. Session and turn cursors bind the retained
record fingerprint and are likewise stale when the record set changes.
The page end is planned once against the worst-case header and
`details_unavailable`/`records_skipped` warnings with the longer
`complete: false` encoding, then reused as an upper bound after blob
verification. A page can therefore only shrink, never include a record whose
`before.bin`/`after.bin` were already verified and then dropped, so continuation
cursors stay continuous with no gaps or duplicates.

Native records capture facts at the actual mutation boundary. `edit` and
`apply_patch` reuse the source read used for matching and the computed result;
`write` best-effort captures an existing regular file. Captures are bounded to
512 KiB per before/after snapshot. Missing files are represented as
`{"kind":"missing"}`; failed or post-rename-uncertain commits use
`commit_state: "unknown"`, not a false no-op. A successful mutation can still
have `coverage: "partial"` when a capture is unavailable. Auxiliary
`before.bin` and `after.bin` files are persisted under the existing bounded
ToolData/Store budgets; a missing or corrupt blob keeps the revision metadata
but returns `details_available: false`. Memory eviction and auxiliary
persistence failure do not change the native tool result.

Workspace records report Git `modified`, `renamed`, `unmerged`, and
`untracked` entries, subject to the same bounded status observation. A rename
carries `original_path` when Git provided a safe workspace-relative old name.
List records use unknown content revisions; `changes.diff` reads and reports the
actual compared versions with `versions_refreshed: true`.

### `changes.diff`

`changes.diff` returns one bounded, read-only comparison for a retained
`tool:` change reference or a `workspace:` reference from `changes.list`. A
tool reference compares the real captured before/after buffers; a workspace
reference compares Git objects and the worktree as described below.

```json
{
  "session_id": "ses_...",
  "change_ref": "workspace:<opaque-token-from-changes.list>",
  "comparison": "head_to_index",
  "context_lines": 3,
  "cursor": null,
  "max_bytes": 65536
}
```

A `tool:` reference is resolved with an in-memory change when the Session is
loaded, and otherwise through the same bounded metadata scan as `changes.list`;
only the matched record's snapshots are read. A warm change with retained bytes
answers without disk I/O, and a warm record missing bytes is only completed
from disk when the disk metadata is identical, so a different revision is never
substituted. An unknown reference is `tool_not_found`. A cold `tool:` diff is
resolved without loading the Session or its Workspace. A `workspace:` reference,
by contrast, requires the Session to be loaded because its sources come from the
Session's Workspace; an unloaded Session is `session_not_loaded`.

The tool comparison is `comparison: "tool_before_after"` over the actual
captured before and after buffers, never Git `HEAD`. A missing before is a
genuine addition (`base_version: {"kind":"missing"}`). `unknown`, expired, or
corrupt snapshots report `availability: "unavailable"`; NUL or invalid UTF-8
reports `binary: true` with no hunks.

A `workspace:` reference is an opaque base64url token that only `changes.list`
mints and `changes.diff` decodes. Clients must not parse or construct it. It
carries the Session, the list-time `HEAD` OID, and the observed status entry.

This token replaces the earlier hashed form, a development-protocol change with
no Runtime version bump: a saved `workspace:` reference from before that change
must be re-listed, and a reference's long-term liveness is not promised because
it pins a list-time `HEAD` OID and status entry. The diff compares real Git
objects and the worktree, never a list-time cached content:

- `comparison` defaults to `index_to_worktree` when the entry has an unstaged
  change or is untracked, otherwise `head_to_index`. An explicit `comparison`
  must be `head_to_index`, `index_to_worktree`, or `head_to_worktree`; a
  `tool:`/`tool_before_after` mismatch is rejected as invalid arguments.
- The three sides are read each query with fixed, shell-free, read-only Git
  arguments: `ls-files --stage -z` for the index OID, `ls-tree -z <head>` for
  the `HEAD` OID, and `cat-file blob <oid>` for object bytes; the worktree side
  is one bounded `capture_file`. Paths are literal pathspecs; no diff driver,
  clean filter, textconv, external diff, lazy fetch, or replacement object runs.
- `HEAD` is the list-time OID from the reference, so a concurrent commit is not
  silently folded in; index and worktree are fresh observations, not a CAS.
- Each side is bounded to 512 KiB. An untracked regular file is compared against a missing base as a
  genuine addition; a removed worktree file is a genuine deletion. Unmerged,
  gitlink, directory, symlink, oversized, or otherwise unreadable sides report
  `availability: "unavailable"` rather than a fabricated file.
- `versions_refreshed: true` means the reported `base_version`/`target_version`
  are the versions actually compared this query, not the list-time snapshot.
- When the workspace is a nested subdirectory, the reference path is resolved
  against the repository top level and must stay inside the workspace.

Each Workspace result carries `origin: "workspace_unknown"` (attribution is never
claimed), `base_version`, `target_version`, `commit_state`, `coverage`,
`binary`, `stale`, `availability`, `versions_refreshed`, structured `hunks`,
`complete`, `truncated`, and an optional `next_cursor`.

Each hunk has `old_start`/`old_count`/`new_start`/`new_count` and `lines`.
Every line reports `kind` (`context`/`added`/`removed`), optional `old_index`
and `new_index`, `line_byte_offset` inside `line_byte_len`, `text`, and
`line_complete`. Concatenating fragments in offset order reconstructs the exact
original bytes, including CRLF, a bare CR, and a missing final newline. The
response is charged by encoded JSON bytes within `2048..=262144` (default
64 KiB) and reserves the actual measured room for a continuation cursor.

The cursor binds the session, change reference, the actual diff-op fingerprint,
context, and hunk/line/offset; `base_version` and `target_version` are carried
by the result. A different diff returns `stale: true` instead of continuing a
new comparison under an old cursor. A start tuple that is not a real position
inside the plan (a hunk/line index past its range, a byte offset past the line,
or a non-UTF-8-boundary offset) is rejected as invalid arguments. The CPU
comparison has a bounded deadline and runs in an owned
Store worker set (max four); dropping the caller cancels it and Agent shutdown
cancels and joins every handler. Each ToolRef remains an independent segment:
this slice performs no same-file aggregation.

## Interactions

### `interaction.answer`

Give one answer to a pending interaction surfaced through `session.state` or an
`interaction_requested` event:

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "interaction_id": "int_...",
  "answer": {"type":"approval","decision":"allow_once"}
}
```

`approval.decision` is `allow_once` or `deny`.

## Session State

`session.state` returns this object directly; the `session_state` event carries
it under `params.data.state`.

```json
{
  "session_id":"ses_...",
  "status":"running",
  "active_loop":null,
  "block_reason":null
}
```

`status` is `idle`, `running`, `waiting_for_input`, `finishing`, or `blocked`.
`active_loop` is null when idle or after a turn completes; otherwise it carries
`loop_id`, `status`, `request_index`, `config_revision`, `model`, and
`pending_interaction`. `block_reason` is `persistence` or `internal` when the
Session is blocked, else null.

Mapping: no active loop and not blocked is `idle`; a blocked Session is
`blocked`; a running loop maps to `running`; a loop awaiting input maps to
`waiting_for_input`; runtime-finishing but agent-persistence-incomplete maps to
`finishing`; once agent completion is published the Session is `idle` again.

## Events

All events are notifications `{"jsonrpc":"2.0","method":"agent.event",...}`.
Every event carries `params.data.meta` with `session_id`, an optional
`loop_id`, and `dropped_before` counting best-effort drops preceding that
event. Session lifecycle events do not carry `data.turn`; loop-scoped events
carry the exact `{session_id, loop_id}` reference in `data.turn` plus
`request_index` where relevant.

Session lifecycle events:

- `session_opened` (`session`, `meta`)
- `session_closed` (`session_id`, `meta`)
- `session_state` (`state`, `meta`)

Loop-scoped events:

- `turn_started` (`turn`, `meta`)
- `request_started` (`turn`, `request_index`, `config_revision`, `model`, `reasoning`)
- `output_delta` (`turn`, `request_index`, `channel` `text`/`reasoning`, `delta`, `meta`)
- `tool_started` (`turn`, `request_index`, `tool_call_id`, `tool_name`, `meta`)
- `tool_invocation` (`turn`, `data`, `meta`); `data` is the same structured
  invocation record `tool.read` returns, emitted once the validated request
  reaches the policy boundary (before the approval decision and before the
  work). Best effort; use `tool.read` to reconcile a missed event.
- `tool_execution` (`turn`, `data`, `meta`); terminal structured execution
  facts from the Runtime `tool_finished` boundary, from the same record as
  `tool.read`.
- `tool_process` (`turn`, `data`, `meta`); live facts for one owned command.
  `data` is the complete `tool_ref`, an optional base64 `chunk`
  (`stream`, `encoding` `base64`, `base_offset`, `next_offset`, `observed_end`,
  `dropped`, `expired`), and/or the current structured `command` record. The
  chunk bytes were already written to the authoritative window before this
  event, so a missed event loses a notification only; use `tool.output` to
  reconcile. It is best effort and carries no total order between the two
  streams.
- `tool_progress` (`turn`, `request_index`, `tool_call_id`, `progress`, `meta`)
- `tool_presentation` (`turn`, `request_index`, `tool_call_id`, `tool_name`,
  `display`, `meta`); it is best effort and may arrive before or after
  `tool_started`.
- `tool_finished` (`turn`, `request_index`, `tool_call_id`, `result`, `meta`);
  `result` contains `outcome`, `content_bytes`, and optional bounded `content`
  plus optional `content_truncated`.
- `interaction_requested` (`turn`, `interaction`, `meta`)
- `interaction_resolved` (`turn`, `interaction_id`, `meta`)
- `turn_finished` (`turn`, `outcome`, `persistence`, `meta`)

## Errors

JSON syntax errors use `-32700`; invalid request shape, unknown methods,
invalid params, and internal errors use `-32600` through `-32603`. Domain
errors:

| Code | Kind |
|---|---|
| `-32001` | `session_not_found` |
| `-32002` | `session_not_loaded` |
| `-32003` | `session_busy` |
| `-32004` | `session_blocked` |
| `-32005` | `invalid_state` |
| `-32006` | `interaction_not_found` |
| `-32007` | `turn_not_found` |
| `-32008` | `profile_not_found` |
| `-32009` | `model_not_found` |
| `-32010` | `workspace_error` |
| `-32011` | `store_error` |
| `-32012` | `provider_error` (reserved; Agent startup/provider build failures occur before the stdio service accepts requests, so there is currently no runtime RPC response path) |
| `-32013` | `runtime_error` |
| `-32014` | `invalid_session_settings` |
| `-32015` | `history_too_large` |
| `-32016` | `steer_queue_full` |
| `-32017` | `reload_requires_restart` |
| `-32018` | `reload_unavailable` |
| `-32019` | `resource_exhausted` |
| `-32020` | `query_limit` |
| `-32021` | `tool_not_found` |
| `-32022` | `context_uncompressible` |

Error data contains only `{kind,retryable}` and stable short messages; it never
serializes an error source, raw provider response, Tool arguments, API key, or
panic payload.

## Presentation Data And Local Permission

Presentation fields are additive and read-only. The Agent keeps the existing
execution RPCs and sanitized history contract; clients may ignore unknown
fields and unknown read-only event types.

`ToolDisplay` is generated by one Agent-owned whitelist formatter and is shared
by live events and history. It exposes only a bounded detail line and, for
`write`, `edit`, and `apply_patch`, a bounded `expanded_input` body. Bash detail
is the bounded command line; read detail is the bounded path/range; other tool
names receive only a generic identity. Raw invocation JSON, environment data,
provider objects, and arbitrary unknown-tool arguments are not sent as display
data. `hidden_line_count` follows the pinned Rail `executionHiddenLineCount` policy:
write/edit bodies use their whitelisted input rows, while other native tool calls
use their bounded JSON-argument row count, in both cases plus bounded result
rows. It is a source display statistic, not permission to expose those raw
arguments; `truncated: true` means the client must not promise unavailable source
rows.

This is an intentional local-display permission change: a command, workspace
path, write body, edit body, or patch supplied by the user/model may reach the
trusted local TUI through the RPC result/event. Deployments that forward RPC
frames beyond that local UI must apply their own policy. Those values remain
excluded from tracing, Agent errors, and redacted `Debug` implementations.

Assistant `parts` are created only from sanitized history and preserve visible
text, visible reasoning, and ToolCall ID order. Encrypted/signature-only
reasoning is never included. User `timestamp` values are optional RFC3339
acceptance times stored inside the same `history.jsonl` loop record as
`user_times`; old records have no timestamps and are never back-filled with
the current time. The metadata is FIFO by User occurrence, so repeated text is
not used as a map key.

The client never reads Store or Workspace for `session.presentation`: the
Agent returns the branch from the most recent completed explicit
`workspace.status` observation and honest `null`/`unknown` context, cost, and
subscription values when no reliable source exists. Presentation reads never
start Git work; fresh branch state comes only from `workspace.status`. A client
must not run `git`, execute a Tool, or use presentation data as an execution
request.