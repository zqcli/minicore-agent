# Long-Conversation Compaction Plan

Status: **The Task 1 snapshot foundation is accepted. The manual Agent/RPC
draft was checkpointed at `5397a65`. P3a and its manual-compaction regressions
are parent-reviewed and remotely verified: 451 stable/MSRV tests passed, 2 Live
tests ignored, strict Clippy/fmt/rustdoc passed. P3b1 automatic compaction is
parent-reviewed and remotely verified: 484 stable/MSRV tests passed, 2 Live
tests ignored; strict Clippy/fmt/rustdoc and Windows/macOS compile checks passed.
P3b2 provider replay budgeting and one-shot overflow recovery are also
parent-reviewed and remotely verified: 511 stable/MSRV tests passed, 2 Live
ignored; strict Clippy/fmt/rustdoc and Windows/macOS compile checks passed**. See the
[manual execution audit](verification/compaction-manual-audit.md) for historical
incident context. Foundation acceptance baseline: Agent `eec636a`, TUI `6ecd736`,
Runtime 0.4.1 revision
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. Runtime source, its pin, existing
package versions and user data stay unchanged. The slice uses the authorized
direct `sha2 = "=0.10.9"` dependency; the parent owns remote lock regeneration.
The current source handoff uses `cus-resp/deepseek-v4.1-flash:max`; the parent owns
review, remote verification and commits. This handoff permits source, focused
tests, documentation and formatting only; no local build/test/check or remote
operation is part of it.
Implementation acceptance remains independently gated per task. The prior
September 13, 2026 grouped source-checkpoint commit/push authorization and
execution-boundary incident remain historical context; they do not accept new
code or authorize private-copy cleanup.

The manual slice adds the bounded no-tools summary utility and deferred
`session.compact` / `session.compact.cancel` RPC on top of the snapshot loader.
P3a additionally projects a validated summary and its complete history suffix
into a Runtime-compliant startup `LoopRequest`, binds that projection through a
same-loop model update, exposes `session.context`, and keeps manual utility usage
separate from ordinary turn usage. Context budget fields describe only the
Runtime history suffix; automatic preparation also retains a bounded current/last
full-request estimate and utility observation, while bounded scans do not block
on unbounded history. Utility accounting retains observed call attempts and known
usage from accepted calls
on later failure with an explicit `complete` flag; usage a stream emitted before
failing is retained as a known partial total, never filled with zero. The
complete `history.jsonl` remains
authoritative.
Automatic compaction, TUI `/compact`, and upstream overflow recovery are not
part of P3a. Final release review and acceptance remain
parent-owned; child-run results are unaccepted and must not be used as gates.
The private-copy cleanup decision remains separate from this source handoff.

## P3b1 Automatic Compaction

P3b1 adds the Agent-global `[compaction]` policy
(`enabled = true`, `trigger_percent = 80`, `target_percent = 50`; the default
is enabled and validates `0 < target < trigger <= 100`) and request-time
threshold compaction. It does not add upstream overflow recovery; that remains
the separate P3b2 adapter/wrapper slice.

In an automatic session, admission first reserves a Session-owned
preparation while it computes a bounded startup estimate. An over-limit projected
history or over-trigger request then folds a settled prefix into durable
`summary.json`; a request that fits starts directly from the same worker. No
`AgentLoop` exists while this decision or summary work is pending, and
`turn.send` returns a deferred RPC response that resolves with the real `TurnRef`
only after the loop is created. `session.context` reports the preparing
operation, and `session.compact.cancel` terminates a preparation that has not
started its loop. A disabled policy keeps the previous
`history_too_large` behavior; existing tests enable automatic compaction only
explicitly.

At every request boundary the effective input budget is derived from the
model's already-reduced context window (its adapter has subtracted output and
safety reserves, so no reserve is subtracted twice). The hard window is the only
uncompressible boundary; the trigger only starts an attempt, and the target is
a preferred size. The estimate covers system and AGENTS text, the durable
summary, the history suffix, current User/Steer text, tool schemas, and bounded
provider-style framing. Complete tool exchanges are folded as whole groups;
settled ordinary base items may also receive temporary summaries when a hot
smaller window requires them. Current User and Steer text are preserved
verbatim. A request whose irreducible system/User/tool-schema minimum exceeds
the hard window fails as `context_uncompressible` without a utility call. A
request above the trigger but within the hard window may be accepted when the
target is unreachable. Model hot-updates re-derive the budget while preserving
an ephemeral summary only when its loop, source range, and source hash still
match.

Automatic request preparation raises the configured prompt timeout to at least
the configured/raw model-operation timeout, while Runtime's overall turn
deadline remains authoritative. Startup admission and manual compaction each
have their own operation deadline; utility chunks share the remaining deadline
of that operation. Semantic summary failures (no progress, empty, oversized,
or timeout) are reported as request-preparation failures, not silently
truncated. `session.context` retains only bounded current/last estimates and
utility accounting; ordinary main-turn usage is never mixed into it.

## Goals

1. Explicit `session.compact` RPC, exposed by TUI `/compact`.
2. Automatic compaction before a model request exceeds its configured budget.
3. One bounded recovery when the upstream explicitly rejects context before
   model output has begun; do not restart the user turn.

Compaction is semantic summarization, not `/clear`, transcript folding, silent
head/tail truncation or deletion of old history. Generating a summary adds model
calls/token cost; those calls must not execute tools. A readable progress/result
must distinguish completion, no-op, failure and uncertain persistence.

## Ownership And Durable History

`history.jsonl` stays the complete authoritative append-only conversation.
Its existing indexes, pagination, timestamps, tool outcomes and append-before-
completion contract do not change. Compression must not rewrite or delete it.

An independent, versioned `summary.json` is a bounded derived snapshot. It covers
only a settled prefix ending on a complete loop-record boundary and records
its source boundary/integrity anchor. Corrupt, stale, mismatched, oversized or
unsupported summary data must not make otherwise valid history unreadable.
Read the full history, validate the snapshot, and ignore invalid derived data.
The snapshot format must not require new fields in strict `SessionRecord`.

Snapshot writes use existing per-session IO ownership and atomic file replacement.
Generate the summary without holding Session locks; validate the captured source
again before committing. The commit point is the serialized operation/state
check immediately before the write: cancellation may prevent the write before
that point, but cannot cancel the bounded write after it. Publish new in-memory
state only after a successful write. A failed or cancelled operation before the
point is definite; a failure after rename may be `unknown_write`, because disk
may have changed while the old in-memory projection remains unpublished. Never
discard full history, roll back, or automatically retry an uncertain write.
Prefer existing primitives; do not invent a global cache or storage-transaction
framework. Choose a stable integrity mechanism before coding; process-dependent
hashes are unsuitable for persisted anchors.

## Request-Time Composition

A Session-scoped `Arc<CompactionState>` is shared by its prompt provider and model
wrapper, not by the global model catalog. Existing configured system/workspace
instructions remain intact. A summary is labeled conversation data, not promoted
to new authoritative instructions. Original current-turn User and accepted Steer
messages remain intact. Tool calls/results are summarized as complete groups;
request construction must still pass the Runtime exchange validator.

Distinguish a durable snapshot from current-loop summaries. Active tool results
are not yet persisted and may only enter an ephemeral projection. Bind reusable
current-loop summaries to the source group and owning loop; never silently reuse
a summary from another Session/config/model. Reuse successful group summaries
across subsequent requests to avoid repeatedly summarizing the same giant result.
Invalidate or re-budget projections when model/settings change.

The effective input budget already subtracts output allowance and safety margin
from `physical_context_window`. Request estimation must include system text,
conversation projection, tool schemas and serialized framing, without subtracting
reserves twice. Estimates are heuristics, not exact tokenizer measurements.

Proposed Agent-global runtime policy (not persisted in `SessionRecord`):

```toml
[compaction]
enabled = true
trigger_percent = 80
target_percent = 50
```

`enabled` controls the P3b1 automatic compaction paths; explicit manual
compaction remains available. P3b2 adds the provider replay adapter/wrapper and
normalized request-body budget gate; tokens remain an estimate. The same
`enabled` policy controls overflow recovery. Validate `0 < target < trigger <= 100`.
Use the currently selected model's effective budget. Apply threshold checks at
**every request boundary**, including requests after tools, not only at turn
admission. Defaults and config placement remain subject to focused validation,
not silent changes to private user configuration.

Semantic summarization uses the raw configured model with a distinct utility
identity, no tools or tool policy, and no recursive compaction wrapper. Preserve
goals, constraints, decisions, changed files, tool outcomes, unresolved work and
relevant exact identifiers. Treat embedded instructions in history as data.
Chunk inputs that do not fit, including an individually huge tool result; keep
all utility requests and outputs bounded. No-progress/oversized output must reduce
within fixed limits or fail honestly, not truncate and claim a semantic success.

System + current User/Steers + tool schemas alone may exceed the budget. Detect
this uncompressible case before starting a futile summary loop and return a clear
failure without altering those inputs. Summarizer failures retain the old state.

## Model Boundary And Overflow Recovery

Keep the existing display-only wrapper unchanged in responsibility:

```text
raw Model -> CompactingModel -> PresentationModel -> ExecutionConfig
```

P3b2, not P3b1, is the narrow `CompactingModel` adapter/wrapper slice. It is
separate from `PresentationModel`; it can catch a structured
`ContextOverflow + NotStarted` from the raw `Model::start`, obtain a smaller
request, and invoke the raw model once more with the same `ModelCallContext`.
Runtime receives one logical request and never re-applies its boundary or tools.
This avoids changing Runtime's generic retry behavior, which clones the original
payload. It also avoids restarting a failed loop and duplicating User/Steer/tool
history. There may still be two actual provider HTTP requests.

A prepared recovery ticket must match the owning loop, request index, original
messages, tool schemas, model/reference/budget, reasoning and policy generation.
Missing or stale metadata cannot authorize a guessed projection. Recovery is
one-shot for that logical request, including any ordinary driver start retries.
A second overflow returns failure; no retry loops or automatic turn resubmission.

Only explicit context rejection before any delivered model output is eligible.
HTTP context-rejection paths currently classified by the adapter are test inputs,
not a claim about any deployed proxy. Generic network failures, unknown delivery,
partial output and started stream failures do not trigger an automatic resend.
An explicit terminal SSE context rejection may still permit preparing a smaller
context for a later user retry, but must not silently replay already-started work.
Do not infer context overflow from arbitrary provider message substrings.

The OpenAI adapter clears replay state after permanent start failures. Recovery
must therefore fold all replay-dependent old complete tool groups, including a
huge latest group, or use a separately validated clean reconstruction. Do not
assume opaque reasoning survives the failure. Real loopback tests now cover
both rejection followed by clean recovery and oversized replay compressed
before sending. The 512 KiB recovery-source cap refuses oversized tickets;
source serialization stops at the cap before copying retained history.

## Admission, Cancellation And RPC

Manual compaction is permitted only for loaded, idle, settled, unblocked sessions.
Reserve the operation under the short Session lock; reject duplicate compaction
or concurrent turn admission. Use the existing RPC `Dispatch::Deferred`/waiter
pattern so a model call cannot block stdin, `agent.ping`, other sessions or
shutdown. The waiter observes completion; it does not own an orphan worker.

The Session owns the operation's cancellation, watch result, and join handle
slot. Summary utilities use the selected raw model with a fresh child token and
one operation deadline no longer than the configured model timeout, and reject
tool events, incomplete/non-stop finishes, late content, empty output, no
progress, and bound violations. Close/shutdown cancel and join owned work;
ordinary drop only cancels. A raw utility loop dropped by an outer timeout must
not escape cleanup. No Session `inner` or IO lock spans a model await.
A cancellation-safe, observable busy state must also protect lifecycle operations.

TUI uses only RPC, never direct Store/config reads. It shows manual/automatic
compaction progress and a safe result (estimated before/after, retained/covered
counts, no summary body in logs). Pending/manual state protects close/delete,
submit and duplicate compaction, while preserving existing uncertain-delivery,
late-response tombstones, selection and history-reconciliation rules.

## Independent Tasks And Commit Gates

1. **Agent manual compaction + RPC**: semantic no-tools summary utility,
   bounded streaming UTF-8 source serialization, source/merge/output bounds,
   settled-prefix snapshot persistence/loading, prompt projection, Session-owned
   asynchronous admission/cancellation and typed deferred `session.compact`
   result. Source coverage includes unchanged history bytes, actual next model
   request uses summary, reopen reuse, no-op, failure retention, ping
   responsiveness, busy/blocked/cancel admission and no tool execution. The
   draft remains pending parent acceptance.
2. **P3a startup projection and context**: use only a validated summary to pass a
   compliant Runtime history suffix, preserve one summary data message across a
   same-loop model update, expose `session.context`, and report independent
   manual utility accounting with bounded suffix estimates and no zero-filling
   of unknowns. Source, focused tests and documentation are pending parent
   review.
3. **TUI `/compact`**: command parsing/completion/help, typed RPC, progress/result,
   lifecycle guards, no blind retry and late-response isolation. Test fake App
   and real-Agent loopback behavior, transcript preservation, narrow layout and
   old-Agent errors. Review and commit.
3. **Automatic threshold compaction**: validated policy/defaults, per-request
   budget estimation and ephemeral current-tool summaries. Test below/exact/above
   threshold, model switching, giant single tool results, no-progress failure,
   cancellation and utility input bounds. Review and commit.
4. **P3b2 provider context recovery**: one-shot inner model wrapper, exact request
   binding and clean tool/replay projection. Test same logical loop/request index,
   one User/Steer occurrence, tools exactly once after prior batches, new Steers
   queued for the next boundary, second rejection, unknown/started/partial errors,
   cancellation, HTTP local/upstream distinction and session isolation. Review
   and commit.

Tests and documentation accompany each task; acceptance is not postponed into a
single large last commit. After all four, rerun stable/MSRV, strict stable
Clippy/fmt/rustdoc, the full real-Agent suite, and native Debug/Release/TTY
regressions. Existing MSRV lint limitations must stay explicit. Native pixels,
real upstream/TLS and proxy-specific diagnostics need separate evidence.

## Current Verification State

P3a, P3b1 and P3b2 now have parent-owned remote acceptance as summarized above
and in [development progress](0914-progress.md). Native cross-platform and Live
Provider tests were not performed. The following original checkpoint account
remains historical evidence, not the acceptance status of the current branch.

The accepted foundation at `eec636a` passed Agent stable/MSRV 375/2 and paired
TUI E2E 18 each through parent-owned remote verification. The manual draft is now committed as a source checkpoint but has
not completed parent-owned acceptance. A child-run verification and broad
working-directory transfer violated its source-only restriction and copied
private configuration/Session data to the builder. These results are retained
as unaccepted exploratory evidence. The parent stopped the identified hung
child-run test while preserving the unrelated Runtime build; remote private
copies remain pending the user's cleanup authorization. No new native artifact
or installation was made.

See [staged compaction verification](verification/compaction.md). TUI
`/compact`, automatic compaction, P3b2 overflow recovery, and persistent
subagents remain separate work. P4 remains the bounded Workspace slice.

> Superseded for the deleted legacy stateless subagent: the 0916 closeout
> removed that execution path and keeps only read-only history compatibility.
> See [the 0916 closeout acceptance](verification/0916-closeout.md).
