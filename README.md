# MiniCore Agent

[![CI](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml)

MiniCore Agent v0.3 is the RPC-first agent core: one Rust package with a
library API, a local Store, a rooted local Workspace, multiple loaded Session
owners, and an offline Fake Model test seam. It is verified against the
`minicore-runtime` 0.4 Git revision
`87f3cf92b9b5980b0f468174a319cf53427d858e`. The runtime dependency is pinned to
that exact Git revision; the local sibling checkout is used only for API review
and is not modified here.

## Run

```bash
minicore-agent --config ./example.agent.toml --stdio
```

## Scope

The Agent is the local backend for one client TUI. The TUI communicates with it
exclusively through the stdio JSON-RPC interface (see
[docs/rpc.md](docs/rpc.md)); it does not call the Rust library API directly.
This repository does not ship a real TUI, plugin system, MCP integration,
Subagent implementation, or compaction.

A Session may select a configured `model` and `reasoning` value, or inherit the
Profile defaults. Those settings are frozen when the Session is created and
persist across close and reopen. `session.update` changes them later: the
persistent record is rewritten and any active loop receives the new execution
config at its next request boundary while the current tool batch keeps its old
snapshot.

## Architecture

Each user message becomes one runtime `AgentLoop`. A Session runs at most one
active loop; there is no queueing or auto-cancellation. Sending while a loop
runs fails with `session_busy`.

One stored Session owns the full conversation:

```text
<data_dir>/sessions/<session_id>/
    session.json      session record (settings, profile, workspace)
    history.jsonl     one JSON line per completed loop record
```

Every completed loop appends its sanitized history as a single JSON line. The
append happens before the in-memory history is merged: if the append fails, the
turn returns `persistence: failed` and the Session becomes blocked, refusing
further turns so memory and disk never diverge. Reopening a Session replays
`history.jsonl` into memory, providing best-effort crash tail repair for trailing
incomplete lines. `persistence: persisted` means the Agent's append operation
completed successfully in the running process. The Store is not a transactional
ledger and does not provide an end-to-end crash-durability proof. Old v0.2
session data is not migrated.

Runtime `Finished` is not an Agent `TurnFinished`. After the loop finishes, the
Agent attempts to persist the report, conditionally merges its history, and only
then publishes completion. `turn.wait` resolves against that Agent-level
completion; callers must inspect `persistence`, because a failed append still
returns the Runtime report and blocks the Session. `turn.steer` and
`session.update` forward to the live `LoopHandle`; updates during a run never
disturb the current request.

Live `AgentEvent`s (output deltas, tool lifecycle, state changes) are best
effort and may be dropped under pressure. The authoritative sources are
`turn.wait` and `session.history`. Opaque provider reasoning is never persisted
and never included in RPC views.

## Tracing

Set `RUST_LOG` to enable more detailed binary diagnostics:

```bash
RUST_LOG=minicore_agent=debug \
  minicore-agent --config ./example.agent.toml --stdio
```

Tracing is initialized only by the binary and writes only to stderr. Stdout
remains reserved for newline-delimited JSON-RPC responses and `agent.event`
notifications. Regardless of how broadly `RUST_LOG` is configured, only
tracing events whose target is `minicore_agent` or begins with
`minicore_agent::` are emitted.

Logs use stable operation and error classifications with safe Session and loop
identifiers. They do not record API keys, Authorization headers, Provider base
URLs or request/response bodies, user or system prompts, reasoning text,
encrypted Provider content, Tool arguments, or Bash commands, paths, and
content.

## RPC Surface

The stdio protocol implements the v0.3 method set:

```text
agent.ping             agent.shutdown
profile.list           model.list
session.list           session.create         session.open
session.close          session.delete         session.state
session.update         session.history
turn.send              turn.cancel            turn.wait
turn.steer             interaction.answer
```

`session.transcript` is gone; `session.history` returns the sanitized stored
history. Errors are classified as before with domain codes `-32001`
through `-32016` plus `-32015 history_too_large` and `-32016
steer_queue_full`. The full wire contract, frame interleaving guarantees, event
shapes, and error mapping are documented in [docs/rpc.md](docs/rpc.md).

## Security Boundary

`Workspace::open` follows symlinks in the input path, requires the resolved
target to be a directory, and stores its canonical path. Tool-style paths are
UTF-8 relative strings; absolute paths, parent traversal, NUL, and empty file
paths are rejected. Existing files and directories are canonicalized and must
remain under the root. Symlinks that resolve inside the root may be read, while
escape symlinks are rejected; atomic writes also reject a symlink as the final
target. Write parents are rechecked while missing directories are created.
Atomic writes use short opaque temp files with `create_new`; after resolution,
parent creation and rechecks, temp creation, write/flush/file sync,
final-target recheck, rename, and directory sync run as one non-yielding
`std::fs` commit section, so cancellation cannot drop a partially executed
mutation. A parent-directory sync failure after rename returns
`WorkspaceError::UnknownOutcome`; failures before rename leave the existing
target unchanged. `read_text` accepts UTF-8 but rejects NUL bytes and invalid
UTF-8 as `Binary`. These checks prevent ordinary traversal and symlink mistakes
but do not fully defend against a same-user process concurrently swapping path
components between checks and filesystem operations.

The crate-private `ProjectPromptProvider` is bound to every production Session.
Each prompt preparation rereads only `<workspace>/AGENTS.md` and merges it with
the profile's system prompt; there is no cache, recursive search, RAG, or other
context source. A missing file yields no project instruction. Invalid content
(bad UTF-8 or disallowed control characters) is a prompt error and fails the
turn as a normal `Failed` result, never silently treated as an empty file.
Content is retained up to a 64 KiB cap with lookahead, so a multi-byte code
point or CRLF pair straddling the boundary stays intact; longer files are
truncated at a UTF-8 boundary and marked `[truncated]`.

The crate-private `Policy` implements MiniCore's `ToolPolicy` and is bound
whenever a profile enables at least one Tool. Classification is an exact
five-name match: `read` is read-only, while `write`, `edit`, `apply_patch`, and
`bash` are mutating. `Auto` allows every known Tool; `Ask` requests a
`Medium`-risk approval per mutating call with the prompt
``Allow tool `<name>` for this call?``; `ReadOnly` denies every mutating Tool.
Approval prompts and denial reasons never include Tool arguments, paths,
content, commands, or raw JSON.

The Tool module implements the exact `read`, `write`, `edit`, `apply_patch`,
and `bash` tools. Each production Session receives a new ToolSet sharing only
that Session's Workspace. All five use strict object schemas and reject unknown
input fields. `read` supports one-based line offsets, line and byte limits, and
safe directory listings. `bash` is not a sandbox and retains the host authority
of the Agent process; use external container or OS isolation for untrusted
models or commands. Run only one Agent process for a given `data_dir`; the
Store has no cross-process lock.

## Library

The crate exposes `AgentConfig`, `AgentError`, `LoopOverrides`, `Profile`,
`ApprovalMode`, `Agent`, `Workspace`, `WorkspaceError`, `SessionId`, the
session/turn DTOs (`CreateSession`, `UpdateSession`, `SendMessage`,
`SteerMessage`, `AnswerInteraction`, `GetHistory`, `HistoryPage`,
`SessionState`, `SessionStatus`, `SessionUpdateResult`, `TurnRef`,
`TurnResult`, `TurnPersistence`), the single-consumer `AgentEventStream`,
`AgentEvent`, and `run_stdio`. The crate is `#![forbid(unsafe_code)]`.

`Agent` opens the local Store and manages multiple loaded Sessions. `Agent`
owns the global event channel; one Session owns its own history and runs at
most one loop task at a time. `session.create` and `session.open` never start a
loop. `session.close` cancels any active loop, joins the worker, and persists
cleanly before returning.

`Agent::shutdown` is the cleanup barrier for embedded Rust callers. It cancels
active loops, waits for Agent-owned loop tasks, and awaits persistence wrap-up.
Dropping an Agent with live turns does not synchronously wait for Agent-owned
loop tasks. MiniCore Agent v0.3 uses the Runtime user-cancellation path when
closing or shutting down an active Session; it does not currently preserve a
distinct shutdown cancellation reason.

## Development

The package keeps Live smoke tests (real provider credentials) behind `#[ignore]`.
Routine verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

The runtime toolchain floor is Rust 1.85; the suite also passes on current
stable.
