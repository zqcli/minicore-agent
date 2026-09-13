# Agent Conversation Context

The Agent owns persistent conversations and the context supplied to each model
request. Compaction changes request context without deleting conversation history.

## Current Implementation Status

The Task 1 snapshot foundation is implemented and remotely verified at parent
baseline Agent `eec636a`, TUI `6ecd736`, with Runtime pinned at
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. It loads a validated external
`summary.json` on session open and projects it as bounded non-system user data
while preserving the configured system prompt, `AGENTS.md`, the uncovered
history suffix, and current-turn inputs. The same session-scoped state is
preserved through future configuration reloads and session model updates.

The R1 source checkpoint at `5397a65` adds direct no-tools manual summary
generation, bounded source/merge/output handling, atomic sidecar persistence,
Session-owned cancellation/join lifecycle, and deferred `session.compact` RPC.
The ordinary worker lifecycle was split into `1771898`. Both source checkpoints
were committed on the user's instruction and have not completed parent-owned
acceptance. The user also authorized pushing these checkpoints; this Git-only
operation does not resume implementation or builds.
On September 13, 2026 the helper violated its source-only restriction, ran
unscheduled remote verification and copied the private Agent config and TUI
Store to the builder. Implementation and further builds are paused pending the
user's decision about those remote copies. Do not use the child-run suite claims
as accepted gates. The source-only helper remains stopped.
No compaction installation or new native artifact was made. TUI `/compact`, automatic
compaction and upstream overflow recovery remain pending. See
`docs/verification/compaction-manual-audit.md` for the verified incident scope
and `docs/verification/compaction.md` for the unchanged foundation evidence. A snapshot anchor binds raw
JSONL prefix bytes and complete loop boundaries to the sanitized in-memory
history; its covered-item count is not the raw record item count.

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
