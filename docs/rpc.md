# Stdio RPC Contract

MiniCore Agent exposes JSON-RPC 2.0 over newline-delimited JSON (NDJSON) on
standard input and standard output.

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
{"jsonrpc":"2.0","id":1,"result":{"version":"0.1.0"}}
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
clones the exact active Turn handle, spawns an owned waiter, and immediately
continues reading requests.

Consequently, clients must correlate responses by `id` and events by Session,
instance, Turn, and Tool call identifiers. The following orderings are not
guaranteed:

- a `turn.send` response before the corresponding `turn_started` event;
- a `turn_finished` event before the corresponding `turn.wait` response;
- the final `output_delta` or Tool event before `turn_finished`;
- a deferred `turn.wait` response before responses to later requests.

`agent.shutdown` waits for the Agent, pre-existing waiter tasks, and event pump,
then queues its response last. EOF, Ctrl-C, writer failure, and explicit
shutdown enter the same owned-task shutdown path.

## Agent Methods

`agent.ping` accepts omitted params or `{}` and returns
`{"version":"0.1.0"}`. `agent.shutdown` accepts the same empty params, starts
orderly shutdown, and returns `{"ok":true}` as the final frame on success.

## Discovery

### `profile.list`

Params are omitted or `{}`. Profiles are sorted by `id`.

```json
{
  "profiles": [
    {
      "id": "coding",
      "model": "deep",
      "reasoning": "high",
      "tools": ["read", "write"],
      "approval": "auto"
    }
  ]
}
```

`model` and `reasoning` are defaults for new Sessions. `approval` is retained
for wire compatibility.

### `model.list`

Params are omitted or `{}`. Models are sorted by `id`.

```json
{
  "models": [
    {
      "id": "deep",
      "model_ref": "deep",
      "context_window": 128000,
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
  "instance_id": "ins_...",
  "created_at": "2026-01-02T03:04:05.006Z",
  "updated_at": "2026-01-02T03:04:05.006Z"
}
```

`title` and `instance_id` may be null. `model` and `reasoning` are the actual
frozen Session settings, not newly read Profile defaults.

### `session.list`

Params are omitted or `{}`.

```json
{"sessions":[]}
```

Each element of `sessions` is a SessionInfo object.

The Store supplies records in stable Session ID order. Entries whose durable
manifest cannot be read, or whose loaded spec disagrees with that manifest, are
omitted. Explicit operations on such a Session remain strict.

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
downgraded.

The result has a `session` member containing the created SessionInfo.

### Other Session Methods

- `session.open` takes `{"session_id":"ses_..."}` and returns a `session`
  member containing SessionInfo.
- `session.close` takes a Session ID and returns `{"ok":true}`.
- `session.delete` takes a closed Session ID and returns `{"ok":true}`.
- `session.state` takes a loaded Session ID and returns the current safe Session
  state projection.

There are no methods to change the model or reasoning of an existing Session.

## Transcript

### `session.transcript`

```json
{"session_id":"ses_...","after":12,"limit":100}
```

`after` is optional and selects entries strictly after that conversation
sequence. `limit` defaults to 100 and must be in `1..=100`.

The response is:

```json
{
  "entries": [],
  "next_after": null,
  "observed_head": 12,
  "complete": true
}
```

Entries preserve durable order and are tagged as `user_message`,
`assistant_message`, `tool_result`, `summary`, or `turn_terminal`. The safe RPC
projection includes sequence numbers, Turn IDs, timestamps, execution
model/reasoning/max Tool rounds, assistant text/reasoning/usage/finish reason,
durable Tool result content, summaries, and terminal outcomes. Assistant Tool
calls expose their ID, name, and call index but omit arguments. Diagnostics omit
message text and expose only code, category, and retryability.

Clients use `next_after` for pagination. `observed_head` is the head observed by
that read, and `complete` indicates whether the returned page reaches it.

## Shared Wire Schemas

IDs serialize as strings. Conversation sequence values and token counts serialize
as unsigned JSON numbers.

### EventMeta And TurnRef

| Schema | Fields | Optionality |
|---|---|---|
| `EventMeta` | `session_id`, `instance_id`, `dropped_before: u64` | All fields are always present |
| `TurnRef` | `session_id`, `instance_id`, `turn_id` | All fields are always present |

`TurnRef` is the exact identity accepted by `turn.cancel` and `turn.wait` and is
returned by `turn.send`. An old instance or non-active Turn does not match. The
Agent keeps no historical Turn registry.

### Usage

Every Usage member is an optional `u64` and is omitted, not serialized as
`null`, when the Provider did not report it:

| Field | Meaning |
|---|---|
| `input_tokens` | Input tokens |
| `output_tokens` | Output tokens |
| `reasoning_tokens` | Reasoning tokens |
| `cache_read_tokens` | Tokens read from Provider cache |
| `cache_write_tokens` | Tokens written to Provider cache |
| `provider_total_tokens` | Provider-reported total |

The Usage object itself is always present where specified and may therefore be
`{}`. The same schema is used by `TurnOutcome`, transcript
`assistant_message.usage`, and transcript `turn_terminal.usage`.

### TurnOutcome

| Field | Type | Optionality |
|---|---|---|
| `turn_id` | Turn ID string | Always present |
| `terminal` | terminal enum | Always present |
| `usage` | Usage | Always present; individual Usage fields may be omitted |

`terminal` is one of the strings `completed`, `cancelled_by_user`,
`cancelled_by_shutdown`, `cancelled_by_restart`, or `budget_exceeded`; failure
uses `{"failed":{"diagnostic":Diagnostic}}`. `Diagnostic` contains required
`code`, `category`, and `retryable` fields and never includes diagnostic message
text. TurnOutcome is reused by `turn.wait`, `SessionState.last_terminal`, and
`turn_finished.data.outcome`.

### SessionState

`session.state` returns this object directly; the `session_state` event carries
it under `data.state`.

| Field | Wire type and values | Presence |
|---|---|---|
| `session_id` | Session ID string | Always present |
| `instance_id` | Session instance ID string | Always present |
| `status` | `idle`, `running`, `waiting_for_input`, or `closing` | Always present |
| `health` | `"healthy"` or `{"degraded":{"diagnostic":Diagnostic}}` | Always present |
| `active_turn` | Turn ID string or `null` | Always present; nullable |
| `pending_interaction` | compatibility PendingInteraction object or `null` | Always present; nullable |
| `conversation_seq` | unsigned conversation sequence | Always present |
| `last_terminal` | TurnOutcome or `null` | Always present; nullable |

The compatibility PendingInteraction projection contains `interaction_id`,
`turn_id`, `tool_call_id`, `tool_name`, and the existing tagged `kind`. This
document does not define a new interaction or approval workflow.

## Turns

### `turn.send`

`{"session_id":"ses_...","text":"Hello"}` submits a Turn and promptly returns
`{"turn":TurnRef}`. Clients should issue `turn.wait` immediately and consume
`agent.event` in parallel.

### `turn.cancel`

Params are TurnRef. The result is `{"cancelled":true}` when this request
triggered cancellation. A completed active handle may return `false`; use
`turn.wait` for its terminal result.

### `turn.wait`

Params are TurnRef. Waiting does not block the request reader, and multiple
waiters may wait on the same exact active Turn. The result is TurnOutcome, for
example:

```json
{
  "turn_id": "trn_...",
  "terminal": "completed",
  "usage": {"input_tokens": 10, "output_tokens": 4}
}
```

Runtime wait failures map to `-32013 core_error` without diagnostic text. If the
active handle has already been cleaned up, the method returns `turn_not_found`;
recover the durable result from `session.transcript`.

## Agent Events

Events are notifications without an `id`:

```json
{
  "jsonrpc": "2.0",
  "method": "agent.event",
  "params": {
    "type": "turn_started",
    "data": {
      "turn": {
        "session_id": "ses_...",
        "instance_id": "ins_...",
        "turn_id": "trn_..."
      },
      "meta": {
        "session_id": "ses_...",
        "instance_id": "ins_...",
        "dropped_before": 0
      }
    }
  }
}
```

Every Event `data` object contains the complete EventMeta object.

| `params.type` | `params.data` fields |
|---|---|
| `session_opened` | `session: SessionInfo`, `meta: EventMeta` |
| `session_closed` | `session_id`, `meta: EventMeta` |
| `session_state` | `state: SessionState`, `meta: EventMeta` |
| `turn_started` | `turn: TurnRef`, `meta: EventMeta` |
| `output_delta` | `turn: TurnRef`, `channel: "text"` or `"reasoning"`, `delta: string`, `meta: EventMeta` |
| `tool_started` | `turn: TurnRef`, `tool_call_id`, `tool_name`, `meta: EventMeta` |
| `tool_progress` | `turn: TurnRef`, `tool_call_id`, `progress: ToolProgress`, `meta: EventMeta` |
| `tool_finished` | `turn: TurnRef`, `tool_call_id`, `result: ToolResult`, `meta: EventMeta` |
| `interaction_requested` | `session_id`, `interaction: PendingInteraction`, `meta: EventMeta` |
| `interaction_resolved` | `session_id`, `interaction_id`, `meta: EventMeta` |
| `turn_finished` | `turn: TurnRef`, `outcome: TurnOutcome`, `meta: EventMeta` |

`ToolProgress` always contains `message: string|null`, `completed: u64|null`,
and `total: u64|null`. `ToolResult` always contains `outcome` and
`content_bytes`; outcome is `success`, `failed`, `denied`, `cancelled`, or
`input_provided`. Interaction events and the existing `interaction.answer`
method remain compatibility surfaces only; no new decisions or approval flow
are defined here.

Events are best effort. `meta.dropped_before` is the number of Core and outer
Agent events attributed to the gap immediately before that delivered event; it
is not a global sequence number. A value greater than zero means realtime data
was lost, and the Agent does not replay it. Event loss does not affect Turn
execution or durability: recover with `turn.wait`, `session.state`, and
`session.transcript`.

Events and responses share the single writer described in [Frame
Interleaving](#frame-interleaving), but no additional ordering is guaranteed.
Clients must correlate frames using request IDs, EventMeta, TurnRef, and Tool
call IDs rather than arrival order.

## Errors

All error data uses `{"kind":string,"retryable":bool}`. Messages and kinds are
stable and never contain Provider responses, credentials, prompts, Tool
arguments, or diagnostic message text.

| Code | Kind | Meaning |
|---:|---|---|
| `-32700` | `parse_error` | Invalid JSON or oversized request frame |
| `-32600` | `invalid_request` | Invalid JSON-RPC request shape or ID |
| `-32601` | `method_not_found` | Unknown method |
| `-32602` | `invalid_params` | Missing, malformed, out-of-range, or unknown params |
| `-32603` | `internal_error` | Internal, serialization, configuration, or I/O failure |
| `-32001` | `session_not_found` | Session does not exist |
| `-32002` | `session_not_loaded` | Operation requires a loaded Session |
| `-32003` | `session_busy` | Session is running another Turn; retryable |
| `-32004` | `session_closed` | Session Runtime is closed |
| `-32005` | `invalid_state` | Operation is incompatible with current Session/interaction state |
| `-32006` | `interaction_not_found` | Interaction is absent or already resolved |
| `-32007` | `turn_not_found` | Exact active Turn was not found |
| `-32008` | `profile_not_found` | Profile ID is not configured |
| `-32009` | `model_not_found` | Model ID is not configured |
| `-32010` | `workspace_error` | Workspace cannot be opened or validated |
| `-32011` | `store_error` | Durable Store operation failed |
| `-32012` | `provider_error` | Provider/unsupported model operation failed |
| `-32013` | `core_error` | Runtime operation failed; retryability comes from its safe diagnostic |
| `-32014` | `invalid_session_settings` | Selected Profile/model/reasoning/Tools are incompatible |

Except for `session_busy` and retryable Core diagnostics, current domain errors
are non-retryable. Clients should still use the response's `data.retryable`
value rather than hard-coding that policy.
