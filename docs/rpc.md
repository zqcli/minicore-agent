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
{"jsonrpc":"2.0","id":1,"result":{"version":"0.3.3","protocol_version":1,"capabilities":["session.read","session.context","turn.result","tool.read","tool.output","session.history","deferred.waiter_limit"]}}
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
`session.read`, and `turn.result` are exceptions: the server registers one
bounded owned task and immediately continues reading requests. A deferred
query/waiter does not own the underlying Session operation.

Clients correlate responses by `id` and events by Session and loop identifiers.
The following orderings are not guaranteed:

- a `turn.send` response before the corresponding `turn_started` event;
- a `turn_finished` event before the corresponding `turn.wait` response;
- the final `output_delta` or Tool event before `turn_finished`;
- a deferred `turn.wait` response before responses to later requests;
- a deferred `session.compact`, `session.read`, or `turn.result` response before
  responses to later requests.

Output deltas and other live events are best effort and may be dropped under
pressure. The authoritative sources are `turn.wait`, `turn.result`, and the
history query results, not the event stream.

`agent.shutdown` waits for the Agent, its active child workers, pre-existing
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
configuration can be reloaded. A failed reload leaves the current catalog,
loaded Sessions, and configuration unchanged. The reload operation does not
reload the Agent binary or previously stored Session system-prompt snapshots;
workspace `AGENTS.md` remains request-level behavior.

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
      "tools": ["read", "write", "edit", "apply_patch", "bash", "subagent"],
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

The manual compaction methods, startup summary projection, and automatic
compaction described here are source-handoff APIs. They are not available in
the previously installed Agent binary and remain subject to parent acceptance.
P3b2 provider overflow recovery is separate pending work.

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
parent-owned remote verification; see `0914-progress.md`. `last_result` is the latest manual
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

`git_branch` is obtained by the Agent with fixed arguments equivalent to
`git -C <workspace> symbolic-ref --short HEAD`; it is `null` for a non-Git,
detached, or unavailable workspace. The lookup runs at session create/open,
after Tool batches, and at loop completion, never once per UI frame.
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

Profiles opt into Tools explicitly through the `tools` array. The native
`subagent` Tool is available only when a profile names it; it is never added
automatically. Its OpenAI function schema is strict (`additionalProperties`
is `false` and optional values are represented as nullable required fields).

The Tool accepts exactly one of these modes:

```json
{
  "model": null,
  "reasoning": null,
  "task": "Review the parser and report the highest-risk issue",
  "tasks": null,
  "chain": null,
  "cwd": null
}
```

The `task` form runs one stateless child loop. Each task may select a
configured model and reasoning value; `model: "id:reasoning"` is accepted as a
convenient suffix form when `reasoning` is not also supplied. `tasks` runs up
to eight independent child loops with at most four workers at once:

```json
{
  "model": null,
  "reasoning": null,
  "task": null,
  "tasks": [
    {"model": null, "reasoning": null, "task": "Inspect the parser", "cwd": null},
    {"model": null, "reasoning": null, "task": "Inspect the tests", "cwd": null}
  ],
  "chain": null,
  "cwd": null
}
```

`chain` runs up to eight stages sequentially. Each stage may contain the
bounded `{previous}` placeholder, which is replaced with the preceding stage's
final output. Final output is the text projection of the last assistant
response; a later response without text does not reuse an earlier round. A
stage result reports its status, selected model and reasoning,
child loop ID, bounded output, usage, request count, tool-round count, and a
static failure reason when applicable. Child output is capped at 50 KiB and
aggregate Tool details are capped at 512 KiB. Generic progress events report
stage starts, output activity, child Tool activity, and completed counts without
forwarding raw child output. The top-level status is `completed` only when
every returned stage completed, `partial` when at least one stage completed and
another returned a non-completed status, and `failed` when no stage completed.
Unknown aggregate usage fields remain `null`/unknown, empty usage is `null`, and
reported fields are summed with checked arithmetic. Request and Tool-round
aggregates are `null` when any included (non-skipped) stage omits that
observation; skipped chain stages are not included in the aggregate. Per-stage
observations remain in each stage object.

Child loops have empty, independent history and presentation state, with no
Store or Session record. They inherit the parent system prompt and the selected
workspace's `AGENTS.md`, receive the parent's ordinary Tools except `subagent`,
and inherit the parent's approval mode. A child `cwd` must be the parent workspace or a
descendant. Child loops cannot delegate recursively. The inherited approval
mode is enforced by the child policy; a child approval/input request cannot be
answered through this stateless Tool and is returned as a failed stage. The child
runner uses Runtime's authoritative `LoopHandle::watch_state()` and
`WaitingForInput` state rather than the best-effort `InteractionRequested` event,
which may be dropped under pressure. Native interaction coverage uses an offline
fake model/provider seam; it is evidence for this Agent/Runtime integration, not
for real-provider behavior. A normally completed subagent Tool joins its child workers before returning. If
external Tool or turn cancellation/timeout drops the Tool future, its scope
only cancels children synchronously; the Agent-owned registry retains their
handles until Session loop completion, `session.close`, or Agent shutdown drains
them. The parent Tool may return before that deferred join completes. Session
and Agent shutdown still cancel and join outstanding child workers.

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
non-retryable) rather than a nearby match. Records are held in memory for a
loaded Session only, under a fixed per-session record and byte budget; a call
whose bytes were evicted is reported through `availability` instead of being
dropped silently.

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

`stream` is `input` (canonical JSON of the requested invocation arguments) or
`output` (the recorded tool result text). `encoding` is `utf8_json` for `input`
and `utf8` for `output`. `offset`, `next_offset`, and `observed_end` are UTF-8
byte offsets; `base_offset` is always the retained window start. Continue with
`next_offset` until `eof`. `truncated` marks a page cut short or a stream larger
than the retained cap.

`availability` is per stream: `pending` while the call is still running and
that stream has no observed bytes (with `observed_end: 0` and `eof: false`),
`unavailable` once the call is terminal but no bytes were observed this
process, then `available`, `partial`, or `expired`. `eof` is true only when the
stream can no longer yield bytes: the retained prefix was fully delivered, or
the bytes were evicted/truncated. A non-zero `offset` on an unobserved stream,
or an `offset` past `observed_end`, is rejected with `invalid_params`;
`next_offset` never moves backwards. An evicted stream returns an empty `data`
with `availability: expired` rather than claiming no output.

Returned bytes are the recorded original text. They are never rewritten
through `escape_default`, and offsets always refer to the raw bytes. Clients
remain responsible for not executing ANSI/control characters as terminal
instructions. A requested `max_bytes` outside the supported range is rejected
with `invalid_params`; it is never silently raised.

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
Agent performs its own fixed-argument branch lookup and returns honest
`null`/`unknown` context, cost, and subscription values when no reliable source
exists. A client must not run `git`, execute a Tool, or use presentation data
as an execution request.