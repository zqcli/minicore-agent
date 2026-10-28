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
  appends before merging in-memory history. An unknown append outcome blocks
  later turns; a confirmed rollback preserves the old history and permits a retry.
  Reopen may truncate only a trailing incomplete line, not a malformed complete
  record. Provider-private fields excluded by sanitization are not promised to
  survive across turns. A partial reply interrupted by a cancel is stored on its
  loop record as display-only `interrupted` data; it is not a history item and
  no prompt, estimate, budget or summary source ever includes it.
- `summary.json` is an independently validated, bounded derived snapshot of a
  settled prefix. Missing, stale, or corrupt summaries fall back to full history
  without relaxing Session/history validation.
- Explicit model/reasoning updates still take effect at request boundaries;
  reload affects future operations. Neither restores raw history already covered
  by an active emergency reduction. Workspace `AGENTS.md` is reread at each
  preparation, with its existing 64 KiB prefix and UTF-8/control validation.

## Pi-Style Request Boundaries

The default trigger is `reported_context >= 95% * effective_input_window`;
existing explicit percentages are preserved. The denominator is physical context
minus the configured output and safety allowances. Reported context is the
latest Assistant item's provider total, or the sum of all five disjoint usage
components when all are known, in the effective history. It already includes
that response's output. No estimate or allowance for later prompts, steers or
tool results is added. Missing usage on that latest Assistant means no threshold
compaction; an older report is never substituted. History size alone neither
compacts nor refuses a send. Target percent remains advisory.

First-request checks exclude the newly accepted Prompt and Steers. Later checks
run only after complete assistant/tool exchanges, under the active turn's normal
cancellation and deadline. A single no-tools utility consolidates the effective
old summary and all eligible old groups. Current User/Steer text and the newest
unread complete tool exchange stay verbatim; incomplete exchanges are never cut.
A fully covered summary without a subsequent Assistant report does not trigger
threshold compaction.

Source range/hash coverage and the original summary hash bind each turn-local
projection. Only a strictly smaller, well-paired complete provider request is
installed. One logical request permits at most one threshold utility; a stable
raw-source/binding fingerprint prevents another attempt for unchanged sources,
including after a failed attempt, but cancellation releases the fingerprint for
a later retry. Generated summary text/generations are not new source. The retained
floor reserves up to 1,024 tokens (at most one eighth of the effective window)
for a summary. If that floor meets the soft trigger, there is
no useful threshold reduction and no utility call is spent. These are bounded
attempts, not a guarantee of reaching target or fitting arbitrary contexts.

Threshold input is streamed from borrowed history through the existing 256 KiB
source projection, with 2,000-character tool-result and serialized-argument projections. It does not clone
raw history. A source over that projection is summarized in chunks, as described
below. Failure preserves the old projection and the normal request
can still be sent. In-turn utility work receives half the remaining preparation
time. A utility timeout sends the unchanged projection only if its local estimate
fits the hard limit and the parent deadline/cancellation still permits sending;
otherwise preparation fails. Emergency recovery keeps its original deadline.

Reported usage comes from persisted history: restart, reopen, configuration
reload, provider changes and model changes do not invalidate it. A model change
changes only the trigger denominator. Successful compaction supersedes the latest
Assistant report, including reports in a retained tail, until a new Assistant
response with usage arrives. `summary.json` persists that Assistant's identity as
the optional `superseded_usage` pair `[loop_id, request_index]`; it is validated
against loaded history. Legacy snapshots without this field use their validated
source coverage as the cutoff: only Assistant items in the retained suffix are
eligible. In-turn reductions apply the same supersession rule before settlement.
Emergency overflow recovery remains the safety net for unmeasured growth.

## Independent Manual And Post-Turn Operations

Manual `session.compact` is available only while idle. By default it retains
approximately `min(20,000, effective_input_window / 4)` recent tokens, rounded toward older history to complete
stored loop records. A loop, including its tool calls and results, is never split.
The summary covers only the older prefix; display history and the next model
request both retain the real recent answers. No raw history bytes are rewritten.
Already summarized items never return to the retained tail. Empty/short history,
an indivisible large loop, or repeating compact without a newer eligible prefix
returns Noop without a utility call. It has an exact operation ID and deferred result. Manual creation rejects the reserved `auto-` prefix;
cancellation accepts an actual automatic ID.

With automatic compaction enabled, only a Completed turn whose history was
Persisted can run a post-turn check. Failed/cancelled turns, failed
persistence, blocked or closing Sessions do not start new automatic utility work.
The check belongs to the turn's task and only measures; it is not an operation,
reports no progress and does not block a submit. The original turn completion is
published without waiting for it or for summary generation. Once the threshold is
crossed it becomes the `auto-{completed_loop_id}` operation, which owns its own
result, cancellation and utility usage and does not consume the manual
operation-ID quota.

`turn.send` never compacts first and never refuses a send for the size of the
history. A history that no longer fits the model is reduced by the request
boundary threshold, by emergency recovery after a provider rejection, or by a
manual compaction.

The worker compares the latest eligible provider-reported context with the
configured trigger, just like request preparation. Below the trigger it finishes
Noop, silently and without a utility call or scanning history anchors. Unknown
or superseded usage finishes Noop with `reported_context_unavailable`; no local
estimate substitutes for it. At or above the trigger the operation becomes
visible (`preparing`, `summarizing`, `committing`) and uses
the single-call summary engine only if a safe older prefix can advance. If the
recent complete-loop tail leaves no older prefix to fold, such as a single huge
loop, the operation ends Noop. If it cannot fit with fixed request
content and a summary, the operation fails explicitly without dropping that tail.
A fully covered projection has no eligible report and does not automatically
refresh; manual compaction of fully covered history remains Noop. An unchanged eligible prefix and request binding are not summarized again after
an attempt; cancellation and timeout leave that source retryable. The configured
target is advisory, not a reason to repeat a summary call. A submit that arrives during the check abandons it and proceeds; the next turn
checks again. Once the operation is visible, a submit returns `session_busy`.
`turn.wait` does not wait for either, and submit does not queue an automatic
resend. Clients retain drafts and reconcile state/context.

Close cancels all captured turn/compaction owners before joining them. Cancelling
an independent operation does not change an already completed turn. Reload does
not retroactively cancel a reserved operation's captured configuration.

## Single-Call Summary Projection

Each selected source is projected into one bounded text request using the Pi
initial/update summary prompts. User text, assistant text, visible reasoning
text/summary and embedded historical summaries are retained. Tool-call arguments
are serialized as JSON and limited to the first 2,000 Unicode scalar values,
with an explicit omitted-character count, like tool results. Provider replay, encrypted reasoning, signatures and history accounting
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
An old summary used for refresh retains its first 16,384 Unicode scalar values
(at most 64 KiB), with an explicit omission marker; the original snapshot is
unchanged. The adapted update prompt asks for condensation, not unbounded growth.
Utility reasoning is Low, or Disabled when the session disables reasoning.
Project instructions and tool schemas remain in the ordinary request and its
budget checks, but are not repeated in the tool-free summary request. The source
builder is bounded to the Runtime's 256 KiB message limit and checks cancellation
and deadline while writing. The complete framed, JSON-escaped request must fit
the existing effective input budget before any model call. A source that does
not fit one request is split at item boundaries, keeping each tool result with
its call, into chunks that each fit next to a 64 KiB running summary. Each chunk
is folded into the summary of the chunks before it with the update prompt, and
the last output becomes the summary. Every call is accounted and all share the
operation deadline. An item that cannot fit a request alone still fails
explicitly; nothing is silently trimmed.

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

Every request keeps a recovery ticket whatever the size of its source: the system
prompt, summary, history and tool schemas it was built from. The ticket holds
copies, not shared references, so it costs about one more copy of the request
history while the request starts. It is dropped as soon as the request has
started, when recovery claims it, when a newer request replaces it, and when the
loop ends. A reduction that is too large to persist as a snapshot (256 KiB)
still serves the retried request and is reported as an explicit settlement
failure.

Local serialized-body bytes/4 is a conservative estimate, not an upstream
rejection. It no longer refuses ordinary HTTP requests. Explicit utility handles
share the same provider transport/serializer but enforce the exact emitted-body
input estimate before sending. No local estimate is renamed ContextOverflow.
The Runtime's per-item bounds remain, and there is no history item or byte limit
or aggregate HTTP-body quota. A long live turn can
still accumulate a large serialized body/allocation, an existing resource limit
that the former post-serialization token veto did not prevent.

Recovery shares the active request's cancellation/deadline and does not finish
the turn early. It accumulates summaries locally, validates tool pairing and the
actual provider-normalized reduced request, and installs only a successful,
strictly smaller fitting projection. Independent source groups are tried largest
byte size first, with source order breaking ties. A non-shrinking group is skipped
and its utility usage is still counted. Recovery stops once the reconstruction
fits the soft trigger; if candidates are exhausted first, it accepts the smallest
valid reconstruction found only when it is strictly smaller than the rejected
body and within the hard limit. Exhausting candidates without an acceptable
reconstruction reports `context_uncompressible`, including when every summary
fails to shrink its group. Failure, no reduction or a second overflow exits finitely. The installed source-hash-bound base survives subsequent requests
and ordinary delivery retries; later genuine context growth in a new logical
request can have a new recovery opportunity, without imposing a tool-round cap.

After source history is definitely persisted and turn completion has been
published, existing in-turn threshold or emergency reductions can be promoted
through the same atomic snapshot store without another utility call. `turn.wait`
does not wait for promotion. The session-owned task still owns settlement, and
close/next-submit cleanup joins it. The post-turn check runs after settlement and
respects the promoted snapshot's supersession point, rather than restoring
covered raw history or estimating its size.
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
lost, but that downgraded raw context can exceed model capacity. A compatible
newer build may be needed.
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
  The latest observation accumulates utility usage across attempts in the same
  turn; a new turn starts a new total. Unknown usage remains unknown.
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

`last_prepare_failure` and recovery observations are process-local. A successful
preparation, including a threshold Noop, clears an earlier preparation failure.
Bounded read cancellation/deadline uses `query_limit`, not a generic compaction error. No
compaction deletes `history.jsonl`, backfills old timestamps, or promotes summary
text to a system instruction. See [the RPC contract](rpc.md) for wire fields.
