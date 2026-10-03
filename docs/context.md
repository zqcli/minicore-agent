# Context And Compaction

Context reduction changes the effective model input, not the authoritative
conversation. A turn is the entire user task, including all its model requests
and tool rounds.

## Ordinary Turns And Durable History

- Ordinary requests append history and do not summarize merely because a soft
  threshold was crossed. Session create/open, submit, context reads, and reload
  do not start routine compaction. Existing validated summaries may be loaded.
- `history.jsonl` retains the complete sanitized turn and tool results. The Agent
  appends before merging in-memory history; an append failure blocks later turns.
  Reopen may truncate only a trailing incomplete line, not a malformed complete
  record. Provider-private fields excluded by sanitization are not promised to
  survive across turns.
- `summary.json` is an independently validated, bounded derived snapshot of a
  settled prefix. Missing, stale, or corrupt summaries fall back to full history
  without relaxing Session/history validation.
- Explicit model/reasoning updates still take effect at request boundaries;
  reload affects future operations. Neither restores raw history already covered
  by an active emergency reduction. Workspace `AGENTS.md` is reread at each
  preparation, with its existing 64 KiB prefix and UTF-8/control validation.

## Independent Manual And Post-Turn Operations

Manual `session.compact` is available only while idle and uses the existing
half-window target and full-coverage Noop semantics. It has an exact operation ID
and deferred result. Manual creation rejects the reserved `auto-` prefix;
cancellation accepts an actual automatic ID.

With automatic compaction enabled, only a Completed turn whose history was
Persisted can reserve a post-turn operation. Failed/cancelled turns, failed
persistence, blocked or closing Sessions do not start new automatic utility work.
The operation is reserved before the next submit can enter, but the original
turn completion is published without waiting for summary generation. An
`auto-{completed_loop_id}` operation owns its own result, cancellation and utility
usage and does not consume the manual operation-ID quota.

The worker estimates the complete effective context, including the final answer,
existing summary, remaining history, system/AGENTS and tools. Below the configured
trigger it finishes Noop without a utility call; otherwise it uses the existing
bounded summary engine and automatic target. Even a check that becomes Noop may
briefly return `session_busy` to a new submit. `turn.wait` does not wait for this
operation, and submit does not secretly wait for a budget decision or queue an
automatic resend. Clients retain drafts and reconcile state/context.

Close cancels all captured turn/compaction owners before joining them. Cancelling
an independent operation does not change an already completed turn. Reload does
not retroactively cancel a reserved operation's captured configuration.

## Bounded Emergency Recovery

Within an active turn, the only summary exception is a confirmed upstream
context-capacity rejection with `ContextOverflow + NotStarted`, known-safe
delivery and no text, reasoning or tool-call output. One bounded compaction and
retry is allowed for that rejected logical model request, never for the whole
turn. Completed tools are not redispatched. Partial output, unknown delivery,
generic network errors, length endings, and generic HTTP 400/413 are not evidence
of this condition.

Local serialized-body bytes/4 preflight is a conservative estimate, not an
upstream rejection. Exceeding that local hard budget returns a distinct local
`InvalidRequest` failure without a summary or model retry. Hard checks are not
disabled to make the upstream recovery path reachable.

Recovery shares the active request's cancellation/deadline and does not finish
the turn early. It accumulates summaries locally, validates tool pairing and the
actual provider-normalized reduced request, and installs only a successful,
strictly smaller fitting projection. Failure, no reduction or a second overflow
exits finitely. The installed source-hash-bound base survives subsequent requests
and ordinary delivery retries; later genuine context growth in a new logical
request can have a new recovery opportunity, without imposing a tool-round cap.

After source history is definitely persisted, existing emergency summaries can
be promoted through the same atomic snapshot store without another utility call.
This encodes historical data, not a newly generated semantic summary. The
existing 64 KiB summary-content limit still applies (including the encoded
historical data); the entire snapshot file has a separate 256 KiB bound. A large remaining tail, source
mismatch or write failure can prevent promotion. Such failures are explicitly
observed as `recovery_failed` with `emergency_settlement_*` failure kinds; they do
not change the original turn result or erase its history. The previous durable
in-memory projection remains, so a later turn may again encounter capacity limits.
There is no guarantee that an arbitrarily long turn can always be recovered.

## Estimates, Accounting And Failure Outcomes

`session.context` is an in-memory observation, not a disk reload:

- History item/byte/token estimates describe only the effective history suffix;
  they exclude summary, system/AGENTS, tools, current input and provider framing.
- `estimated_request_context_tokens` is the latest serialized request estimate
  recorded before the wrapped model starts, not a live measure of current
  context or provider metering. It may retain the original rejected request's
  estimate after successful recovery; the reduced value is in `recovery.after_tokens`.
- Budget/trigger/target use the already-reduced effective model input window;
  output reserve and safety margin are not subtracted a second time.
- `automatic` remains a required compatibility object with null `current` and
  `last`. Independent operations use `current_operation` and `last_result`;
  in-turn recovery has its separate bounded `recovery` observation.
- Utility usage is separate from ordinary turn usage; unknown fields are not
  filled with fabricated zeros. `tool_rounds` statistics are `u64`; the legacy
  configuration field remains `u16`, with zero meaning unlimited.

Manual/post-turn results are `compacted`, `noop`, `failed`, or `unknown_write`.
UnknownWrite means disk may have changed while the old in-memory projection
remains. It does not set a permanent Session block or pretend a successful
compaction. Clients may reread state/context to confirm operation termination;
those queries do not reload the disk snapshot. Subsequent ordinary loading uses
the existing snapshot validation. Do not blindly retry an uncertain operation.

`last_prepare_failure` and recovery observations are process-local. Bounded read
cancellation/deadline uses `query_limit`, not a generic compaction error. No
compaction deletes `history.jsonl`, backfills old timestamps, or promotes summary
text to a system instruction. See [the RPC contract](rpc.md) for wire fields.
