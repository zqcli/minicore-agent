# Long-Conversation Compaction Plan

Status: **The Task 1 snapshot foundation is accepted. The manual Agent/RPC
draft is committed at `5397a65` but unaccepted; implementation and builds remain
paused following the September 13, 2026 execution-boundary incident. Ordinary
worker lifecycle changes are checkpointed separately at `1771898`.
Tasks 2–4 remain pending**. See the
[manual execution audit](verification/compaction-manual-audit.md) before resuming.
Foundation acceptance baseline: Agent `eec636a`, TUI `6ecd736`, Runtime 0.4.1 revision
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. Runtime source, its pin, existing
package versions and user data stay unchanged. The slice uses the authorized
direct `sha2 = "=0.10.9"` dependency; the parent owns remote lock regeneration.
The intended workflow restricts `cus-resp/gpt-5.6-luna:max` to source development;
the parent owns review, remote verification and commits. The helper violated that
restriction during R1 and is now stopped.
Implementation acceptance remains independently gated per task. On September 13,
2026 the user separately authorized grouped source-checkpoint commits and push of
the existing draft. This Git-only authorization does not accept the code, resume
builds, restart the helper, or authorize private-copy cleanup.

The active slice adds the bounded manual summary utility and the deferred
`session.compact` / `session.compact.cancel` RPC on top of the snapshot loader.
It captures the selected raw model and future-turn inputs, uses fresh utility
loop identities with no tools, bounds source chunks/merge/output, and commits
only after a second complete-history anchor check. The complete
`history.jsonl` remains authoritative. Automatic compaction, TUI `/compact`,
and upstream overflow recovery are not part of this slice. Final release review and
acceptance remain parent-owned. Child-run results are unaccepted and
must not be used as gates. The private-copy cleanup decision requires user
confirmation before work resumes.

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

`enabled` controls automatic compaction and automatic overflow recovery; explicit
manual compaction remains available. Validate `0 < target < trigger <= 100`.
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

`CompactingModel` is separate from `PresentationModel`. It can catch a structured
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
assume opaque reasoning survives the failure. This needs a real loopback test.

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
   asynchronous admission/cancellation and
   typed deferred `session.compact` result. Source coverage includes unchanged
   history bytes, actual next model request uses summary, reopen reuse, no-op,
   failure retention, ping responsiveness, busy/blocked/cancel admission and
   no tool execution. Remote review and full gates remain pending.
2. **TUI `/compact`**: command parsing/completion/help, typed RPC, progress/result,
   lifecycle guards, no blind retry and late-response isolation. Test fake App
   and real-Agent loopback behavior, transcript preservation, narrow layout and
   old-Agent errors. Review and commit.
3. **Automatic threshold compaction**: validated policy/defaults, per-request
   budget estimation and ephemeral current-tool summaries. Test below/exact/above
   threshold, model switching, giant single tool results, no-progress failure,
   cancellation and utility input bounds. Review and commit.
4. **Upstream context recovery**: one-shot inner model wrapper, exact request
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
`/compact`, automatic compaction, overflow recovery, and persistent subagents
remain separate work.
