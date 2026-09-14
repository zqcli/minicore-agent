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
no valid summary is loaded); full request-context budget and provider usage
remain explicitly unknown where not observed.

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
Automatic compaction, upstream overflow recovery and TUI `/compact` remain
pending. A snapshot anchor binds raw JSONL prefix bytes and complete loop
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
