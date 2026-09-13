# Agent Conversation Context

The Agent owns persistent conversations and the context supplied to each model
request. Compaction changes request context without deleting conversation history.

## Current Implementation Status

The first Task 1 foundation is implemented and remotely verified from the parent
baseline Agent `63540ee`, TUI `6ecd736`, with Runtime pinned at
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. It loads a validated external
`summary.json` on session open and projects it as bounded non-system user data
while preserving the configured system prompt, `AGENTS.md`, the uncovered
history suffix, and current-turn inputs. The same session-scoped state is
preserved through future configuration reloads and session model updates.

This slice does not generate summaries, expose manual compaction RPC, run
automatic compaction, or implement the owner/cancellation/persistence worker.
Those belong to the next slices. The `gpt-5.6-luna:max` helper develops source;
the parent performs review, orchestration, remote verification and commits.
The snapshot foundation passed 375 tests / 2 ignored on stable and Rust 1.85,
strict stable quality gates, and 18 paired real-Agent E2E tests on each toolchain.
See `docs/verification/compaction.md` for staged evidence. A snapshot anchor binds
raw JSONL prefix bytes and complete loop boundaries to the sanitized in-memory
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
