# MiniCore Agent

This repository is the RPC-first agent core: one Rust package with a library
API, a local Store, a rooted local Workspace, multiple loaded `SessionRuntime`
owners, and an offline Fake Model/Tool test seam. It is verified against the
`minicore-runtime` `dev` HEAD
`7e85eaab18e273e43e03c50040b460f1b13f0ac9` through
`tests/runtime_api_compile.rs`. The runtime dependency is pinned to that exact
Git revision; the local sibling checkout is used only for API review and is not
modified here.

## Run

```bash
minicore-agent --config ./example.agent.toml --stdio
```

The current wire surface is deliberately small:

```json
{"jsonrpc":"2.0","id":1,"method":"agent.ping","params":{}}
```

returns:

```json
{"jsonrpc":"2.0","id":1,"result":{"version":"0.1.0"}}
```

`agent.shutdown` is also accepted. One NDJSON frame is read at a time, stdout
is reserved for JSON-RPC, and malformed or unknown requests return standard
JSON-RPC errors without echoing request data. JSON syntax errors return
`-32700`; valid JSON with an invalid request shape returns `-32600`.

For `agent.ping` and `agent.shutdown`, `params` may be omitted or be the empty
object `{}`. `null`, arrays, and non-empty objects return `-32602`. Request IDs
are limited to strings and JSON integers, including negative integers; the
response preserves the original ID. Frames are read incrementally with a 1 MiB
limit. An oversized frame returns a parse error and ends the stdin loop. EOF
performs the same graceful shutdown path. For an explicit shutdown request, the
agent shutdown completes before its success response is written.

The library exposes `AgentConfig`, `AgentError`, `Agent`, `Workspace`,
`WorkspaceError`, the session/turn DTOs, the single-consumer `AgentEventStream`,
`AgentEvent`, and `run_stdio`. `Agent`
opens the local Store, manages multiple loaded SessionRuntime owners, and
forwards typed live events while authoritative durable turn completion is obtained
from a cloned `TurnHandle`. One outbound sequencer owns each loaded session's
Core event stream, state watch, outer sink, and completion-ready queue. When a
state update and Core event are both ready, it emits the latest state first; this
preserves Runtime's publish-before-enqueue ordering for actionable interaction
state. Before emitting a durable Agent `TurnFinished`, it submits a transcript
command to the same actor, emits the current latest state, refreshes state before
each drained Core envelope, and emits the latest state again if it changed during
the drain. Successfully sent identical states are deduplicated. State remains
best-effort: outer-channel backpressure records a drop and does not block the
following Core event. Core `TurnFinished` is advisory because the runtime
EventStream is best-effort; if Core drops that envelope, its unknown
`dropped_before` metadata is unrecoverable and the Agent does not fabricate it.
A slow consumer blocks only the sequencer, never Runtime or request submission.
After a successful
Runtime shutdown barrier and all owned workers have joined, the loaded-session
owner makes the sole best-effort `SessionClosed` send; failed shutdowns do not
send it, and no `TurnFinished` can follow it. The Store uses
`<data_dir>/sessions/<session-id>/` with `session.json`, `manifest.json`, and
`conversation.log`; each append is one durable JSON line containing one batch.

Metadata uses a unique `create_new` temp file, syncs the temp file, atomically
renames it, and syncs its parent directory. Session and conversation directory
entries are also synced before successful return where the platform supports
it. On Unix/macOS this uses an opened directory and `sync_all`; non-Unix builds
compile and sync file contents, but Tokio has no portable directory-fsync
contract there. `manifest.json` is the initialization commit marker: the empty
conversation file is durable before the marker is published. Direct symlinks in
the session tree, including metadata, manifest, conversation, and temp entries,
are rejected.

`Workspace::open` stores a canonical directory root. Tool-style paths are UTF-8
relative strings; absolute paths, parent traversal, NUL, and empty file paths are
rejected. Existing files and directories are canonicalized and must remain under
the root. Symlinks that resolve inside the root may be read, while escape
symlinks are rejected; atomic writes also reject a symlink as the final target.
Write parents are rechecked while missing directories are created. Atomic writes
use short opaque `.minicore-write-<pid>-<counter>.tmp` `create_new` files. A
temp candidate whose basename is exactly or ASCII-case-insensitively equal to
the target is skipped before any filesystem access; only that counter is skipped,
and counter wrapping remains safe through normal alias/collision retry. Write
path resolution and canonicalization are asynchronous but do not mutate the
filesystem. After resolution, parent creation and rechecks, temp creation,
write/flush/file sync, final-target recheck, rename, and directory sync run as
one non-yielding `std::fs` commit section. The write Tool accepts at most 512 KiB,
so this deliberately permits brief executor-thread blocking instead of allowing
Runtime cancellation to drop a partially executed mutation. No spawned,
`spawn_blocking`, or detached task owns the commit. Once the synchronous section
starts it runs to its real `Success`, pre-rename failure, or
`UnknownOutcome`; cancellation and deadlines apply only before that boundary. A
parent-directory sync failure after rename returns
`WorkspaceError::UnknownOutcome`: the complete new target may already be visible
and is not rolled back. Failures before rename
return `Unavailable` and leave an existing target unchanged. `read_text` accepts
UTF-8 including Unicode, newlines, and tabs, but rejects NUL bytes and invalid
UTF-8 as `Binary`. Non-Unix platforms have no portable directory-fsync
contract. These checks prevent ordinary traversal and symlink mistakes but do
not fully defend against a same-user process concurrently swapping path
components between checks and filesystem operations; no `openat`/OS locking
scheme is implemented. Workspace is not yet assembled into Agent capabilities,
and the existing temporary Agent workspace validation remains unchanged.

The crate-private Tool module currently implements the exact `read`, `write`,
`edit`, `apply_patch`, and `bash` tools; they are not yet assembled into Agent
sessions. All five use strict object schemas and reject unknown input fields. `read`
supports one-based line offsets, a default 400-line limit (maximum 2000),
one-level sorted directory listings capped at 1000 entries, and `[truncated]`
markers for line, 512 KiB file prefix, directory-count, or Runtime output limits.
Workspace retains up to four lookahead bytes beyond the visible 512 KiB cap. The
decoder uses them only to validate a 2/3/4-byte UTF-8 code point or CRLF split by
the cap; lookahead bytes are never emitted. A valid crossing code point is
omitted with `[truncated]`, while an invalid continuation or true-EOF incomplete
sequence fails. CR is accepted only as CRLF and is stripped during newline
normalization. NUL, other unsafe controls, invalid UTF-8, and any non-UTF-8
directory entry name are execution failures.

`write` accepts at most 512 KiB of UTF-8 content, including empty content.
`edit` reads an existing UTF-8 file of at most 512 KiB and performs only
non-overlapping exact literal matches. Empty or over-limit input is invalid; no
match fails, and multiple matches fail unless `replace_all=true`. Its result
length is calculated before replacement and cannot exceed 512 KiB.
`apply_patch` uses a local exact-position unified-diff parser/applier for an
existing UTF-8 file. The patch, source, and result are each capped at 512 KiB;
the result builder checks the cap before every append. Headerless input must
begin directly with `@@`; standard input has exactly one optional `---`/`+++`
pair followed by one or more strictly counted, ordered, non-overlapping hunks.
Hunk positions are applied exactly as declared with no fuzzy search. Standard
header paths may be unquoted with a tab timestamp or Git/C quoted with supported
control, quote, backslash, and one-to-three-digit octal byte escapes. Decoded
paths must be UTF-8 without NUL and, after optional old `a/` or new `b/` prefixes,
must exactly match the separately supplied safe relative Tool path. Multiple
file sections, absolute or parent-traversing headers, create/delete via
`/dev/null`, rename/copy, git preambles or index metadata, binary patches,
trailing content, partial apply, and path selection from patch headers are
rejected.

Patch transport accepts LF or CRLF but strips transport CR from hunk content.
Source text is split into logical lines retaining `None`, LF, or CRLF endings.
Context and removals compare exact text and exact no-final-newline marker state;
unchanged mixed endings are copied byte-for-byte. Additions use the source's
majority line ending (first observed style breaks ties), so CRLF sources remain
CRLF, while a valid marker can explicitly produce no final newline. The parser
consumes patch transport lines once, each hunk is applied with forward-only
source and patch-content cursors, and the result is appended once without front
`Vec` splices. Structural tests cover thousands of hunks and prove apply steps
are bounded by source logical lines plus patch transport lines, giving
`O(source bytes + patch bytes)` behavior.

`bash` runs `/bin/sh -lc` on Unix and non-interactive PowerShell on Windows. It
inherits the Agent environment, adds `MINICORE_AGENT=1`, and resolves its
relative working directory through Workspace before spawning. The Tool future
owns one `tokio::process::Child` with null stdin, piped stdout/stderr, and
`kill_on_drop(true)`. Two pinned reader futures are polled in the same Tool
future alongside `child.wait`; no detached reader task is created. Each stream
retains at most 512 KiB while continuing to drain excess bytes, so total raw
capture is at most 1 MiB. The final labeled output is additionally bounded by
the Runtime 256 KiB ToolOutput cap. Invalid UTF-8 becomes U+FFFD; ANSI ESC, NUL,
CR, and other controls are escaped while newline and tab remain readable. Each
truncated stream retains its own `[truncated]` marker. Nonzero and signal exits
are completed Tool outcomes with a numeric code or `exit_code: unavailable`.

The effective command deadline is the earlier of ToolContext and input timeout,
with cancellation biased first. Explicit cancellation or timeout starts killing
the direct child and awaits reap before returning the exact ToolError; dropping
the Tool future relies on `kill_on_drop` to terminate that direct child. v0.1
does not create Unix process groups or Windows Job Objects and does not guarantee
that grandchildren, daemons, or inherited pipe handles are reclaimed.

All file-mutating Tools fully construct their one-line success output before
calling Workspace atomic replacement. Control characters in relative path
displays are escaped while ordinary Unicode remains readable, so output
validation cannot fail after a successful commit.
Invalid JSON, bounds, and path traversal map to invalid invocation; missing,
permission, binary, atomic failure, and unknown outcomes map to failed execution.
Cancellation and deadline race asynchronous source reads and the remaining
non-mutating pre-commit awaits. Edit and patch parsing/application are bounded
synchronous compute: once such compute starts it completes in the same poll and
is not interruptible midway. After the synchronous Workspace commit boundary,
the real commit result wins even if cancellation or the deadline becomes ready.

The production TOML shape retains the OpenAI Responses model configuration, but
that provider is intentionally not implemented in this Phase and `Agent::open`
returns a stable not-implemented error instead of claiming availability.
Offline loop tests use a crate-private injected Fake Model/Tool/Policy seam;
Fake provider and tool implementations are not part of production TOML. Store
assumes one process per data directory and does not implement file locks.
Loading currently reads the complete log into memory; v0.1 defines no production
log-size limit, so very large logs may consume substantial memory. Each loaded
session retains only its active `TurnHandle`; a `TurnRef` is not a historical
handle registry key, so callers such as RPC must clone the handle at request time.
Session metadata `updated_at` is updated in memory immediately and flushed by one
owned, serialized latest-value worker per loaded session. Temporary unavailable
errors leave that worker alive for the next update; terminal metadata failures
stop it, while metadata persistence remains best-effort and never changes a
submitted turn. Closing stops the worker from receiving or starting another
latest-value update, so a queued update that has not started may be discarded.
Once `Store::touch_at` has entered filesystem I/O, shutdown awaits that operation
to completion before joining the worker and allowing deletion. Context, Policy,
OpenAI HTTP, Workspace/Tool capability assembly, and RPC method extensions remain
outside this Phase. The offline process coverage is
in `tests/rpc_stdio.rs`, Agent loop coverage is in `src/agent/tests.rs`, and
Store, Workspace, and Tool coverage is in the internal unit tests of
`src/store.rs`, `src/workspace.rs`, and `src/tools/`.
