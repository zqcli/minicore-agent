# Context And Compaction

Context reduction changes the effective model input, not the authoritative
conversation. A turn is the entire user task, including all its model requests
and tool rounds.

## Ordinary Turns And Durable History

- Request preparation checks existing settled context before first user input,
  then checks the effective projection before each subsequent assistant request.
  Runtime has already accepted the turn, so utility work never blocks RPC submit.
  Create/open, context reads and reload do not themselves invoke a model.
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

## Pi-Style Request Boundaries

The default trigger is `estimated_context >= 95% * effective_input_window`;
existing explicit percentages are preserved. The denominator is physical context
minus the configured output and safety allowances. This deliberately differs
from Pi 1.0.1's strict `>` physical-window-minus-16384 condition and its recent-token
cutoff. Target percent remains advisory.

First-request checks exclude the newly accepted Prompt and Steers. Later checks
run only after complete assistant/tool exchanges, under the active turn's normal
cancellation and deadline. A single no-tools utility consolidates the effective
old summary and all eligible old groups. Current User/Steer text and the newest
unread complete tool exchange stay verbatim; incomplete exchanges are never cut.
A previous fully covered summary may be refreshed after reopen/model change.

Source range/hash coverage and the original summary hash bind each turn-local
projection. Only a strictly smaller, well-paired complete provider request is
installed. One logical request permits at most one threshold utility; a stable
raw-source/binding fingerprint prevents another attempt for unchanged sources,
including after a failed attempt. Generated summary text/generations are not new
source. If the irreducible retained floor itself meets the soft trigger, there is
no useful threshold reduction and no utility call is spent. These are bounded
attempts, not a guarantee of reaching target or fitting arbitrary contexts.

Threshold input is streamed from borrowed history through the existing 256 KiB
source projection, with 2,000-character tool-result projection. It does not clone
raw history or apply the separate 512 KiB recovery-ticket retention limit to
safely projectable tool history. Pure prose that still exceeds the source cap
fails explicitly. Failure preserves the old projection and the normal request
can still be sent; cancellation/deadline ends preparation instead.

A provider/binding/framing and summary-generation anchor proves when actual
assistant usage may be combined with estimates for later content. Otherwise
current provider-body bytes/4 is used. Compaction, changed normalization, reload
or changed framing invalidate old usage; no missing usage component becomes zero.

## Independent Manual And Post-Turn Operations

Manual `session.compact` is available only while idle. By default it retains
approximately 20,000 recent tokens, rounded toward older history to complete
stored loop records. A loop, including its tool calls and results, is never split.
The summary covers only the older prefix; display history and the next model
request both retain the real recent answers. No raw history bytes are rewritten.
Already summarized items never return to the retained tail. Empty/short history,
an indivisible large loop, or repeating compact without a newer eligible prefix
returns Noop without a utility call. It has an exact operation ID and deferred result. Manual creation rejects the reserved `auto-` prefix;
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
trigger it finishes Noop without a utility call. At or above the trigger it uses
the single-call summary engine only if a safe older prefix can advance. If the
recent complete-loop tail leaves no progress or cannot fit with fixed request
content and a summary, the operation fails explicitly without dropping that tail.
An existing fully covered emergency projection keeps its automatic refresh path;
manual compaction of fully covered history remains Noop. The configured target is advisory, not a reason
to repeat a summary call. Even a check that becomes Noop may briefly return `session_busy` to a new submit. `turn.wait` does not wait for this
operation, and submit does not secretly wait for a budget decision or queue an
automatic resend. Clients retain drafts and reconcile state/context.

Close cancels all captured turn/compaction owners before joining them. Cancelling
an independent operation does not change an already completed turn. Reload does
not retroactively cancel a reserved operation's captured configuration.

## Single-Call Summary Projection

Each selected source is projected into one bounded text request using the Pi
initial/update summary prompts. User text, assistant text, visible reasoning
text/summary, embedded historical summaries and complete tool-call arguments are
retained. Provider replay, encrypted reasoning, signatures and history accounting
metadata are excluded. Each tool result keeps its tool name, call ID and outcome,
plus the first 2,000 Unicode scalar values of its text and an explicit omitted
character count. This can omit important details at the end of a long result;
the authoritative stored result is unchanged. This summary-only limit is separate
from tool execution output limits.

For manual/post-turn retention, an existing summary is supplied once alongside
only the newly covered prefix. The before estimate includes the old summary and
all uncovered history; the after estimate includes the new summary and the
complete retained tail. Both include fixed instructions and tool schemas. Tail
and fixed-content headroom are checked before the utility, then the complete
result is checked against the hard budget and for actual progress before commit.
Other threshold/emergency operations keep their own selected-source rules.
Project instructions and tool schemas remain in the ordinary request and its
budget checks, but are not repeated in the tool-free summary request. The source
builder is bounded to the Runtime's 256 KiB message limit and checks cancellation
and deadline while writing. The complete framed, JSON-escaped request must fit
the existing effective input budget before any model call. Oversized input fails
explicitly; there is no chunking, map/reduce or silent trimming of other content.

One successful generation must be nonempty, finish normally without tool calls,
fit the existing 64 KiB summary limit, and produce a strictly smaller ordinary
request. Manual/post-turn and recovery acceptance additionally require the
reduced request to fit the hard budget; a threshold reduction does not promise
that the complete ordinary request fits that heuristic ceiling. Utility input
always retains its own hard budget. A result is not repeatedly summarized merely
to reach a preferred target. Model output configuration is unchanged. Emergency
recovery may select several independent safe groups; each group uses at most one
summary call, and all actual calls remain accounted. The recent-token target is
a bytes/4 heuristic, not a provider-tokenizer guarantee. Summary v1 only supports
complete stored-loop prefixes: this does not add Pi's item-level split-turn
summaries or file-operation ledger. Small windows or one oversized recent loop
can therefore leave compaction unable to make safe progress.

## Bounded Emergency Recovery

Independent of request-boundary thresholds, emergency recovery requires a confirmed upstream
context-capacity rejection with `ContextOverflow + NotStarted`, known-safe
delivery and no text, reasoning or tool-call output. One bounded compaction and
retry is allowed for that rejected logical model request, never for the whole
turn. Completed tools are not redispatched. Partial output, unknown delivery,
generic network errors, length endings, and generic HTTP 400/413 are not evidence
of this condition.

Local serialized-body bytes/4 is a conservative estimate, not an upstream
rejection. It no longer refuses ordinary HTTP requests. Explicit utility handles
share the same provider transport/serializer but enforce the exact emitted-body
input estimate before sending. No local estimate is renamed ContextOverflow.
The existing Runtime admission history bounds and per-request message/item
bounds remain; there is no new aggregate HTTP-body quota. A long live turn can
still accumulate a large serialized body/allocation, an existing resource limit
that the former post-serialization token veto did not prevent.

Recovery shares the active request's cancellation/deadline and does not finish
the turn early. It accumulates summaries locally, validates tool pairing and the
actual provider-normalized reduced request, and installs only a successful,
strictly smaller fitting projection. Failure, no reduction or a second overflow
exits finitely. The installed source-hash-bound base survives subsequent requests
and ordinary delivery retries; later genuine context growth in a new logical
request can have a new recovery opportunity, without imposing a tool-round cap.

After source history is definitely persisted, existing in-turn threshold or
emergency reductions can be promoted through the same atomic snapshot store without another utility call.
This encodes historical data, not a newly generated semantic summary. The
64 KiB generated-summary limit still applies to every utility output. The full
settled projection is a different object: snapshots larger than 64 KiB are valid
only as nonempty, sanitized, well-paired historical message arrays, with no
provider replay or opaque reasoning fields. Literal historical text and tool
arguments are preserved. The complete historical User envelope must fit Runtime's
single-message bound and the actual JSON-escaped snapshot file must fit its
unchanged 256 KiB limit. The whole-record source anchor and format version 1 are
unchanged; loaded and cold readers apply the same validation. A large remaining
tail, source mismatch or write failure can still prevent promotion. Such failures are explicitly
observed as `recovery_failed` with `emergency_settlement_*` failure kinds when
recovery was used. A request-boundary observation keeps its original values and
utility usage, with `_settlement_failed` or `_settlement_unknown` appended to its
outcome. Thus `compacted_settlement_unknown` is not an unqualified durable
success or a confirmed rollback. They do not change the original turn result or
erase its history. The previous durable
in-memory projection remains, so a later turn may again encounter capacity limits.
A successfully promoted snapshot survives post-turn Noop, utility failure or
cancellation; those operations do not restore its covered raw history. Failed or
cancelled turns can also promote their complete projection after their original
history is persisted, while append failure and close-race rejection keep their
existing behavior. Older builds that impose 64 KiB on every snapshot will reject
a newer large projection and fall back to authoritative history. No history is
lost, but that downgraded raw context can exceed Runtime history admission
limits as well as model capacity. A compatible newer build may be needed;
a larger model alone cannot relax Runtime history limits.
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
- `automatic.current` / `last` describe the active/latest request-boundary
  threshold attempt, keyed by loop and request index. A drop-safe guard clears
  current before request/terminal events, retaining observed utility usage.
  These are bounded latest observations, not cumulative utility accounting.
  Independent operations use `current_operation` and `last_result`; overflow
  recovery uses `recovery`. Active threshold work is cancelled through the turn.
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
