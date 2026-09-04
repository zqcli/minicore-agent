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
{"jsonrpc":"2.0","id":1,"result":{"version":"0.3.0"}}
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
requests are dispatched sequentially. `turn.wait` is the exception: the server
registers one owned waiter and immediately continues reading requests.

Clients correlate responses by `id` and events by Session and loop identifiers.
The following orderings are not guaranteed:

- a `turn.send` response before the corresponding `turn_started` event;
- a `turn_finished` event before the corresponding `turn.wait` response;
- the final `output_delta` or Tool event before `turn_finished`;
- a deferred `turn.wait` response before responses to later requests.

Output deltas and other live events are best effort and may be dropped under
pressure. The authoritative sources are `turn.wait` and `session.history`, not
the event stream.

`agent.shutdown` waits for the Agent, pre-existing waiter tasks, and event pump,
then queues its response last. EOF, Ctrl-C, writer failure, and explicit
shutdown enter the same owned-task shutdown path. MiniCore Agent v0.3 uses the
Runtime user-cancellation path when closing or shutting down an active Session;
it does not currently preserve a distinct shutdown cancellation reason.

## Agent Methods

`agent.ping` accepts omitted params or `{}` and returns
`{"version":"0.3.0"}`. `agent.shutdown` accepts the same empty params, starts
orderly shutdown, and returns `{"ok":true}` as the final frame on success.

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
      "supported_reasoning": ["auto", "disabled", "low", "medium", "high"]
    }
  ]
}
```

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
- `session.close` cancels any active loop, joins its worker, and returns
  `{"ok":true}`. MiniCore Agent v0.3 uses the Runtime user-cancellation path
  when closing or shutting down an active Session; it does not currently preserve
  a distinct shutdown cancellation reason.
- `session.delete` takes a closed Session ID and returns `{"ok":true}`.
- `session.state` takes a loaded Session ID and returns the current Session
  state projection.

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
    {"index": 0, "item": {"type": "user", "data": {"kind": "prompt", "loop_id": "lup_...", "text": "Fix the parser"}}}
  ],
  "next_offset": null,
  "total": 2
}
```

`next_offset` is the offset of the next page, or `null` when the page is the
last. Every `loop_id` refers to the runtime loop that produced the item.

Item types and their `data`:

- `user`: `kind` (prompt), `loop_id`, `text`.
- `assistant`: `loop_id`, `request_index`, `model`, `reasoning_level`, `text`,
  `reasoning`, `tool_calls`, `finish_reason`, `usage`. `reasoning` contains
  visible reasoning text/summaries; opaque provider fields (such as encrypted
  content) are never included. Each tool call exposes only `tool_call_id`,
  `name`, and `call_index`; arguments are never present in the wire view.
- `tool_result`: `loop_id`, `request_index`, `tool_call_id`, `tool_name`,
  `outcome`, `content`.

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

Appends a steering instruction to the active loop and returns `{"ok":true}`. A
full steer queue is reported with `-32016`.

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
- `tool_progress` (`turn`, `request_index`, `tool_call_id`, `progress`, `meta`)
- `tool_finished` (`turn`, `request_index`, `tool_call_id`, `result`, `meta`);
  `result` contains `outcome` and `content_bytes`.
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

Error data contains only `{kind,retryable}` and stable short messages; it never
serializes an error source, raw provider response, Tool arguments, API key, or
panic payload.