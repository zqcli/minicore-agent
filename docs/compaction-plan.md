# Long-Conversation Compaction Plan

Status: **planned; implementation and Rust validation have not started**.
Baseline: Agent `14799cd`, TUI `80e985e`, Runtime 0.4.1 revision
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. Runtime source, its pin, package
versions and user data stay unchanged. Two independent `cus-resp/gpt-5.6-luna:max`
sessions perform implementation and review; the parent owns staging/commits.
Each independently verified task is committed immediately, without push.

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
again before committing. Publish new in-memory state only after a successful
write. Cancellation/transport loss during persistence may leave an unknown result,
not a rollback guarantee. Never discard full history or automatically retry an
uncertain write. Prefer existing primitives; do not invent a global cache or
storage-transaction framework. Choose a stable integrity mechanism before coding;
process-dependent hashes are unsuitable for persisted anchors.

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

The Session owns the operation's cancellation and completion. Summary utilities
inherit their caller's cancellation/deadline and have explicit lifetime ownership.
Close/shutdown cancel and join owned work. A raw utility loop dropped by an outer
timeout must not escape cleanup. No Session `inner` or IO lock spans a model await.
A cancellation-safe, observable busy state must also protect lifecycle operations.

TUI uses only RPC, never direct Store/config reads. It shows manual/automatic
compaction progress and a safe result (estimated before/after, retained/covered
counts, no summary body in logs). Pending/manual state protects close/delete,
submit and duplicate compaction, while preserving existing uncertain-delivery,
late-response tombstones, selection and history-reconciliation rules.

## Independent Tasks And Commit Gates

1. **Agent manual compaction + RPC**: semantic no-tools summary utility,
   settled-prefix snapshot persistence/loading, prompt projection, asynchronous
   admission/cancellation and typed `session.compact` result. Test history bytes
   unchanged, actual next model request uses summary, restart reuse, corrupt
   snapshot fallback, failure rollback boundaries, ping/shutdown responsiveness,
   busy/blocked/duplicate rejection and no tool execution. Review and commit.
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

## Current Verification Blocker

The former authorized builder control socket has expired. A fresh batch SSH
attempt to `root@192.168.20.199` reaches the server but is rejected with
`Permission denied (publickey,password)`. No usable password environment or
replacement control socket is currently available. Restore authorized SSH access
before claiming Rust RED/GREEN, committing a completed code task, or installing
new artifacts. Do not compile locally or recover credentials from user history.
The planning commit is documentation-only and does not implement compaction.
