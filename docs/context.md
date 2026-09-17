# Context And Compaction

Context reduction changes what a model request receives; it does not delete the
conversation. The full persisted history remains the durable record.

## Durable And Ephemeral Data

- `history.jsonl` contains the sanitized completed-loop records and remains
  authoritative. The Agent appends a complete record before merging it into the
  in-memory history; an append failure blocks later turns. On reopen, only a
  trailing incomplete JSONL line may be truncated. A complete malformed record
  is corruption, not recoverable crash state.
- `summary.json` is a bounded, independently validated derived snapshot of a
  settled prefix. Invalid, stale, corrupt, or missing summary data falls back to
  full history without weakening core Session/history validation.
- A Session stores its profile system-prompt snapshot in `session.json`.
  Workspace `AGENTS.md` content is request-level input and is reread during
  prompt preparation.
- Request-time summaries of still-live Tool exchanges are ephemeral. They are
  loop-bound, bounded, never promoted to durable history, and never reused by a
  different Session or loop.

## Operations

Manual `session.compact` is a loaded-Session operation with an exact operation
ID and a deferred result. Automatic compaction can run during startup admission
or at a request boundary when the prepared request reaches its model threshold.
It preserves the current User/Steer input and complete Tool exchanges; it does
not rerun Tools or silently slice history by index.

The provider recovery path is deliberately narrow: only a `ContextOverflow` that
was not started can receive one bounded retry for the same logical request. A
started, unknown, partial, or second overflow does not auto-retry. Recovery and
automatic fitting share the remaining request deadline and cancellation boundary.

## Estimates And Accounting

`session.context` exposes several different observations:

- `estimated_history_items` is the item count in the current projected history
  suffix snapshot: the history that preparation would pass to Runtime after a
  validated summary projection, or the full history when no valid summary is
  loaded. `estimated_history_bytes` is a bounded scan of that snapshot and
  `estimated_history_tokens` is the documented bytes/4 estimate.
- Those history estimates exclude summary text, system/AGENTS text, Tool
  schemas, current input, framing, and provider tokenization.
- `estimated_request_context_tokens` is a bounded estimate of the prepared full
  request observed by automatic preparation. It is not provider metering.
- `input_budget_tokens`, `trigger_tokens`, and `target_tokens` derive from the
  model's effective input budget after its output allowance and safety margin.
- Utility-call usage is reported separately from ordinary turn usage. Missing
  fields remain unknown; values are never filled with zero merely for display.

## Boundaries And Errors

A disabled or unfit projected history can return `history_too_large`. If the
irreducible system/current-input/Tool-schema minimum cannot fit, the result is
`context_uncompressible`; no summary can safely solve that request. Manual
compaction reports `status: failed` with a bounded `failure_kind` for a failed
operation, or `status: unknown_write` when an atomic summary replacement may
have changed disk without publishing the new in-memory projection. Automatic
preparation records its stable failure kind in `last_prepare_failure`; it does
not turn a read-query timeout into a generic compaction error. `query_limit` is
the retryable error for bounded read/query cancellation or deadline paths, not
the general compaction result. Callers must reread after `unknown_write` before
retrying.

`last_prepare_failure`, automatic observations, and recovery observations are
process-local projections, not durable Session state. The full `history.jsonl`
remains authoritative; the current Store does not migrate old v0.2 Session data.
Compaction never deletes `history.jsonl`, backfills old timestamps, or turns a
summary into a system instruction. Workspace `AGENTS.md` is reread at every
model request, bounded to a 64 KiB prefix with boundary lookahead; invalid UTF-8
or disallowed controls fail prompt preparation. See [the RPC contract](rpc.md)
for the exact response fields.
