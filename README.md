# MiniCore Agent

This repository is the RPC-first agent core: one Rust package with a library
API, a local Store, a rooted local Workspace, multiple loaded `SessionRuntime`
owners, and an offline Fake Model test seam. It is verified against the
`minicore-runtime` `dev` HEAD
`7e85eaab18e273e43e03c50040b460f1b13f0ac9` through
`tests/runtime_api_compile.rs`. The runtime dependency is pinned to that exact
Git revision; the local sibling checkout is used only for API review and is not
modified here.

## Run

```bash
minicore-agent --config ./example.agent.toml --stdio
```

The stdio protocol implements the complete v0.1 JSON-RPC method set:

```text
agent.ping             agent.shutdown
profile.list           model.list
session.list           session.create         session.open
session.close          session.delete         session.state
session.transcript     turn.send              turn.cancel
turn.wait              interaction.answer
```

One NDJSON frame is read at a time and stdout is reserved for JSON-RPC. A single
bounded outbound channel carries ordinary responses, asynchronous `turn.wait`
responses, and exact event notifications of the form
`{"jsonrpc":"2.0","method":"agent.event","params":<AgentEvent>}`. One owned
writer task is the only code that writes stdout; it serializes one JSON value per
line and flushes every frame. The event pump awaits outbound capacity, so slow
stdout propagates backpressure into the Agent's existing outer drop accounting
without adding fanout or replay.

Except for `turn.wait`, requests dispatch sequentially against the one mutable
Agent owned by the RPC server; there is no Agent actor or global mutex.
`turn.wait` clones the exact current `TurnHandle`, registers one owned waiter,
and lets the reader continue, so its response may arrive after later responses.
No historical handle registry is retained. Params DTOs reject unknown fields.
Empty methods accept omitted params or `{}` but reject `null`; transcript defaults
to 100 entries and permits only `1..=100`. Interaction answers use the internal
`type` tag with approval `allow_once`/`deny`, bounded non-control text, or a
choice index.

Request IDs are strings or JSON integers, including negative integers, and are
preserved exactly. JSON syntax errors use `-32700`; invalid request shape,
unknown methods, invalid params, and internal errors use `-32600` through
`-32603`. Domain errors use `-32001` through `-32013` for `session_not_found`,
`session_not_loaded`, `session_busy`, `session_closed`, `invalid_state`,
`interaction_not_found`, `turn_not_found`, `profile_not_found`,
`model_not_found`, `workspace_error`, `store_error`, `provider_error`, and
`core_error`. Error data contains only `{kind,retryable}` and stable short
messages; it never serializes an error source, raw provider response, Tool
arguments, API key, or panic payload. Session-state and Turn-outcome diagnostics
likewise expose only code, category, and retryability, not diagnostic text.

`session.transcript` never serializes Runtime's `TranscriptPage` or
`ConversationEntry` types directly. An RPC-owned projection preserves entry
order, sequence numbers, canonical timestamps, paging fields, user text and safe
execution settings, assistant model/text/reasoning/usage/finish reason, Tool
results including durable content, summaries, and terminal outcomes. Assistant
Tool calls expose only `tool_call_id`, name, and `call_index`; their `arguments`
field and value do not exist in the wire view. Failed terminal diagnostics use
the same safe code/category/retryability projection and omit diagnostic message
text.

Frames are read incrementally with a 1 MiB limit. Oversize, stdin EOF, Ctrl-C,
explicit `agent.shutdown`, and writer failure all enter the same explicit Agent
shutdown path. Shutdown waits for the Agent durability barrier, joins every
pre-existing waiter, naturally drains and joins the event pump, then queues the
explicit shutdown response. Only after that does it close outbound and await the
writer, so no waiter or event can write a frame after the shutdown response.
Writer failures are propagated after owned tasks have been reclaimed.

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

`Workspace::open` follows symlinks in the input path, requires the resolved target
to be a directory, and stores its canonical path. Tool-style paths are UTF-8
relative strings; absolute paths, parent traversal, NUL, and empty file paths are
rejected. Existing files and directories are canonicalized and must remain under
the root. Symlinks that resolve inside the root may be read, while escape symlinks
are rejected; atomic writes also
reject a symlink as the final target.
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
scheme is implemented.

The crate-private concrete `ProjectContext` implements MiniCore's
`ContextProvider` and is bound to every production Agent session. Every provider
call rereads only `<workspace>/AGENTS.md`; there is no cache, recursive
`AGENTS.md` search, source-composition trait, RAG, or other context source. A
missing root file returns an empty bundle. A present file returns one
`ProjectInstructions` block with source `agents-md` and priority 100, including
an empty-content block for an empty file when its envelope fits the remaining
budget. Reads retain at most the 256 KiB visible prefix plus four UTF-8/CRLF
lookahead bytes. Invalid UTF-8, NUL, bare CR, other unsafe controls, wrong file
types, inaccessible paths, and Workspace escapes are unavailable; CRLF is
normalized to LF while ordinary Unicode, newline, and tab are preserved. If a
read cap falls between CR and LF, the visible CR is retained as the logical LF
and the content is marked truncated.

Project context budgeting uses a conservative full-request delta with Core's
byte/4 ceiling estimator. For each candidate content prefix it constructs the
final system `ModelMessage` text,
`[minicore-context slot=project_instructions source=agents-md]\n...`, and counts
its serde JSON bytes. Since a valid fixed `ModelRequest` already has a nonempty
messages array, inserting one context message adds exactly one comma, so
`delta_bytes = message_bytes + 1` and the provider reserves
`ceil(delta_bytes / 4)` tokens. For any fixed serialized byte count `F` and
delta `D`, `ceil((F + D) / 4) - ceil(F / 4) <= ceil(D / 4)`; equality is reached
when `F % 4 == 0`. The resulting predicate is therefore safe for every unknown
fixed-request residue and is the tight attainable residue-independent bound,
not a claim that the provider knows the exact prefix possible for a particular
fixed residue. A bounded binary search selects the largest UTF-8 prefix under
that predicate. File-prefix, final-message-size, and token-budget truncation all
retain `[truncated]`; if even the marker's complete system-message envelope does
not fit, the provider returns an empty bundle instead of failing the Turn.
Cancellation and deadlines are biased ahead of the asynchronous Workspace read,
and dropping the provider future drops that read without mutation or a detached
task.

The crate-private concrete `Policy` implements MiniCore's `ToolPolicy` and is
bound whenever a profile enables at least one Tool; an empty Tool profile binds
no policy. Classification is an exact five-name match: `read` is read-only,
while `write`, `edit`, `apply_patch`, and `bash` are mutating. `Auto` allows every
known Tool. `Ask` allows `read` and
returns a `Medium` risk approval request for each mutating call with the prompt
``Allow tool `<name>` for this call?``. `ReadOnly` allows `read` and denies every
mutating Tool with a short stable reason. Unknown names and mismatched
invocation/spec names fail closed with a typed denial in every mode. Approval
prompts and denial reasons never include Tool arguments, paths, content,
commands, or raw JSON. A pre-cancelled request returns `Cancelled`; an already
expired deadline returns the stable `Failed` category. Decisions are immediate,
with no external await, spawned task, approval cache, session/project grant, or
persistence; repeated and newly constructed `Ask` policies always request a new
per-call approval, leaving `AllowOnce`/`Deny` handling to Core.

The crate-private Tool module implements the exact `read`, `write`, `edit`,
`apply_patch`, and `bash` tools. Each production session receives a new ToolSet
containing exactly its profile list and sharing only that session's Workspace.
All five use strict object schemas and reject unknown input fields. `read`
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
owns one `tokio::process::Child` with null stdin and `kill_on_drop(true)`. Unix
uses the child's anonymous piped stdout/stderr as reactor-backed readers.
Windows instead creates unique first-instance, inbound byte-mode Tokio named
pipe servers and gives matching `std::fs::File` clients to PowerShell as its
stdout/stderr write handles. The Tool future reads only the overlapped named
pipe servers, so it creates neither a Tokio blocking file-read job nor a
detached reader task. Dropping the Tool future drops pending Unix pipe or
Windows named-pipe reads together with the child. Two reader futures are polled
by one pinned collector in the same Tool future alongside `child.wait`. Each
stream retains at most 512 KiB while continuing to drain excess bytes, so total
raw capture is at most 1 MiB. The final labeled output is additionally bounded
by the Runtime 256 KiB ToolOutput cap. Invalid UTF-8 becomes U+FFFD; ANSI ESC,
NUL, CR, and other controls are escaped while newline and tab remain readable.
Each truncated stream retains its own `[truncated]` marker. Nonzero and signal
exits are completed Tool outcomes with a numeric code or
`exit_code: unavailable`.

The effective command deadline is the earlier of ToolContext and input timeout,
with cancellation biased first. Explicit cancellation or timeout starts killing
the direct child and awaits reap before returning the exact ToolError. If the
child already exited, termination succeeds without another kill. A `try_wait`,
`start_kill`, or post-kill `wait` error is internal and takes precedence over a
requested cancellation or timeout; the Tool never claims that termination
completed after such an error. Dropping the Tool future relies on `kill_on_drop`
to terminate that direct child. v0.1 does not create Unix process groups or
Windows Job Objects and does not guarantee that grandchildren or daemons are
reclaimed. Inherited output handles can keep a normal capture open, but timeout,
cancellation, or future drop closes this Tool's readers without waiting for a
grandchild to close its copy.

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

`Models` eagerly constructs every configured OpenAI Responses adapter during
`Agent::open`. The API key is read from `api_key_env` and kept only inside the
private adapter; missing or empty values fail startup. Base URLs must be HTTP(S)
without credentials, query, or fragment, and resolve to `<base>/responses`.
The descriptor uses the Agent model-profile ID as `ModelRef`; its exposed
context window is `physical - output budget - safety margin`, while the provider
request uses the configured output budget as `max_output_tokens`.

The private reqwest adapter sends `stream:true`, `store:false`, and
`truncation:"disabled"`. System messages become developer `input_text`, user
messages become user `input_text`, assistant text becomes completed assistant
output, Tool calls preserve their Core call ID and JSON arguments, Tool results
become completed `function_call_output`, and Tool schemas become flat function
tools. `Auto` omits reasoning, `Disabled` sends effort `none`, and low/medium/high
send the exact effort plus summary `auto`. MiniCore's public `ReasoningContent`
does not retain the complete provider reasoning-item identity required for safe
Responses replay, so historical reasoning parts are omitted rather than
fabricated. Historical assistant text uses deterministic local message IDs;
provider message phase and original message identity are not available through
the Runtime public history. Text and Tool history remain replayable.

The Model stream directly owns reqwest's body stream, cancellation token,
deadline, bounded incremental SSE parser, and pending typed events; no parser
task is spawned. It accepts arbitrary chunks, CR/LF/CRLF, comments, and multi-line
`data:` frames, with 1 MiB line/frame and queued-frame bounds. Provider text is
UTF-8/control validated and split on UTF-8 boundaries into MiniCore's 64 KiB
event limit. Tool calls support split arguments, done-only items, and multiple
calls with exactly one start/end. Only `response.completed` or
`response.incomplete` can produce Usage/Finish; early EOF never synthesizes
success. Usage separates direct input/output from cache-read, cache-write, and
reasoning tokens while retaining the provider total; the Agent does not compute
prices or costs.

Delivery mapping is conservative: local build/preflight/context failures are
permanent `NotStarted`; connect failures and HTTP 429 are the only retryable
`NotStarted` paths; send/response timeouts, 5xx, and uncertain transport outcomes
are `Unknown`; stream failures after any typed semantic event are `Started`.
HTTP error bodies are capped at 64 KiB and only machine-readable code/type fields
are inspected privately. API keys, base URLs, raw bodies, and provider messages
never enter diagnostics, Debug output, RPC, events, transcripts, or stderr.
Redirects are disabled and default tests use only test-owned loopback servers.

For every successful create or unloaded open, Agent opens a fresh concrete
Workspace, builds the profile's concrete ToolSet, conditionally creates the
concrete Policy, always creates ProjectContext, and supplies those ports through one
`SessionBindings::new` call; compaction remains disabled with no strategy bound.
Session records persist the canonical Workspace root as durable identity.
Unloaded open requires the freshly canonicalized root to equal that stored path,
so replacing it with a symlink or other redirection to a different directory is
rejected before capability construction; an already loaded open remains
idempotent without filesystem I/O. Unknown or duplicate profile Tool names fail
configuration before Store/session startup. Offline loop tests inject only a
Fake Model: Workspace,
Tools, Policy, and Context are the production implementations. Store
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
to completion before joining the worker and allowing deletion. Default tests are
offline: unit and Agent-loop suites use Tokio loopback HTTP, and
`tests/openai_rpc_process.rs` runs the real stdio binary through a two-request
OpenAI→read Tool→OpenAI completion plus a provider-secret error case. Empty-config
framing tests remain in `tests/rpc_stdio.rs`.

The ignored live text smoke runs only when both required variables are present:

```bash
OPENAI_API_KEY=... \
MINICORE_AGENT_LIVE_MODEL=... \
cargo test --locked openai_live_smoke -- --ignored --nocapture
```

`MINICORE_AGENT_LIVE_BASE_URL` optionally overrides
`https://api.openai.com/v1`. The offline process coverage is in
`tests/rpc_stdio.rs` and `tests/openai_rpc_process.rs`; Agent loop coverage is in
`src/agent/tests.rs`. Store, Workspace, Tool, Context, and Policy coverage is in
the internal unit tests of `src/store.rs`, `src/workspace.rs`, `src/tools/`, `src/context.rs`, and
`src/policy.rs`.
