# Agent Conversation Context

The Agent owns persistent conversations and the context supplied to each model
request. Compaction changes request context without deleting conversation history.

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
