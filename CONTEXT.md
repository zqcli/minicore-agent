# Agent Conversation Context

The Agent owns persistent conversations and the context supplied to each model
request. Compaction changes request context without deleting conversation history.

## Current Implementation Status

The Task 1 snapshot foundation remains the accepted historical baseline at
Agent `eec636a`, TUI `6ecd736`, with Runtime pinned at
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. The current P3a source slice is
parent-reviewed and remotely verified (451 stable/MSRV tests passed, 2 Live
tests ignored; strict Clippy/fmt/rustdoc passed): at startup it
projects a validated summary plus the complete history suffix into a
Runtime-compliant `LoopRequest`, preserves the projection through same-loop
model updates, and exposes the read-only `session.context` capability. Its
history estimate is bounded to the Runtime input suffix (or full history when
no valid summary is loaded); automatic preparation additionally records bounded
current/last full-request and utility estimates without mixing main-turn usage.

P3b1 (automatic threshold compaction, not overflow recovery) is implemented in
this working tree: the `[compaction]` policy, startup preparation with a bounded
deferred `turn.send`, per-request budget estimation, loop/source-bound ephemeral
summaries, bounded automatic observations, and `context_uncompressible`
reporting. Parent-run stable/MSRV suites each passed 484 tests (2 Live tests
ignored); strict Clippy/fmt/rustdoc and Windows/macOS compile checks passed.
P3b2 provider replay budgeting and one-shot overflow recovery have passed parent
review and remote verification: 511 stable/MSRV tests passed, 2 Live ignored;
strict Clippy/fmt/rustdoc and Windows/macOS compile checks passed. P4 is the
next, separate Workspace slice; P4a `workspace.read` is verified with 528
stable/MSRV tests passed, 2 Live ignored, and strict Clippy/fmt/rustdoc plus
Windows/macOS compile checks passed. P4b `workspace.files` and
`workspace.search` are also verified: 576 stable/MSRV tests passed, 2 Live
ignored; strict Clippy/fmt/rustdoc and Windows/macOS compile checks passed.
P4c `workspace.status` is verified: 613 stable/MSRV tests passed, 2 Live ignored;
strict Clippy/fmt/rustdoc and Windows/macOS compile checks passed. It provides
machine-readable, workspace-scoped Git status with isolated configuration and
Session-owned child reaping. P4 is complete. P5a passed parent review and remote
verification: stable/MSRV each 651 passed, 2 Live ignored; strict Clippy/fmt/doc
and Windows/macOS compile checks passed. Every Bash command has a Session-owned worker
registered under its complete `ToolRef` that keeps, stops, and reaps the child,
both pipes stream into bounded 1 MiB tail windows served as base64 pages with
raw offsets. Capacity is accounted under the 8 MiB Session budget; stale-offset
pages can recover retained tails. Turn/close joins retain handles across dropped
or concurrent join futures before publishing the ordinary Turn result. P5b durable process records, P6 change review and P7 integration
remain pending.

The R1 source checkpoint at `5397a65` adds direct no-tools manual summary
generation, bounded source/merge/output handling, atomic sidecar persistence,
Session-owned cancellation/join lifecycle, and deferred `session.compact` RPC.
The ordinary worker lifecycle was split into `1771898`. Both source checkpoints
were originally unaccepted checkpoints. The current P3a implementation and
manual-compaction regressions now have parent-owned remote verification; see
`docs/0914-progress.md`. There is no local compilation or installation.

The September 13, 2026 source-only execution violation and private-data-copy
incident remains historical audit context; do not use its child-run claims as
acceptance gates. No new compaction installation or native artifact is claimed.
P3b1 automatic compaction has parent-owned remote verification;
P3b2 provider overflow recovery now has the same parent-owned verification.
TUI `/compact` remains outside this backend scope. A snapshot anchor binds raw
JSONL prefix bytes and complete loop
boundaries to the sanitized in-memory history; its covered-item count is not the
raw record item count. See
`docs/verification/compaction-manual-audit.md` for the historical incident scope
and `docs/verification/compaction.md` for the unchanged foundation evidence.

## Language

**Session**:
A persistent conversation with its own workspace, settings and complete history.

**Turn**:
One accepted user prompt and its agent loop, including any accepted steering,
model requests and tool work before its final outcome.

**Settled History**:
Conversation items whose turn completion has been successfully persisted by the
Agent. Retained but unsaved results are not settled history.

**Context Projection**:
The conversation information selected for one model request, which may differ
from the complete history while preserving the current instructions.

**Compaction Snapshot**:
A derived semantic summary of an identified settled history prefix. It can stand
in for that prefix in model context but is not a replacement for the original history.
_Avoid_: History deletion, clear

**Ephemeral Summary**:
A summary used during a live turn before its source becomes settled history.
It is not a durable record of completed work.
