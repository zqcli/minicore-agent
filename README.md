# MiniCore Agent

[![CI](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml)

MiniCore Agent v0.3 is the RPC-first agent core: one Rust package with a
library API, a local Store, a rooted local Workspace, multiple loaded Session
owners, and an offline Fake Model test seam. It is verified against the
`minicore-runtime` 0.4.1 Git revision
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. The runtime dependency is pinned to
that exact Git revision; the local sibling checkout is used only for API review
and is not modified here.

Current release: **0.3.3**, paired with MiniCore TUI **0.2.8**. See
[release notes](docs/release-0.3.3.md) and the subsequent
[presentation-path source fixes](docs/verification/presentation-risk-fixes.md).
These follow-up fixes preserve the pinned Runtime revision and RPC surface.
The subsequent [Session rename and prompt-file features](docs/verification/session-config.md)
are separately committed and natively verified with the paired TUI; package
versions and the Runtime pin are unchanged. Current
[Tool, configuration reload and native stateless delegation acceptance](docs/verification/followups.md)
uses unchanged Agent `f1697f7` / corrected TUI `a604e55` after the
[public reload correction](docs/verification/reload-refresh.md), verified and
installed separately without another version bump or push. Persistent subagent
orchestration is not included.

## Run

```bash
minicore-agent --config ./example.agent.toml --stdio
```

## Scope

The Agent is the local backend for one client TUI. The TUI communicates with it
exclusively through the stdio JSON-RPC interface (see
[docs/rpc.md](docs/rpc.md)); it does not call the Rust library API directly.
This repository does not ship a real TUI, plugin system or MCP integration.
The development branch includes verified manual, startup and request-time
compaction, provider replay budgeting and one-shot context overflow recovery.
P3b2 passed parent-owned remote stable/MSRV suites (511 passed, 2 Live tests
ignored), strict Clippy/fmt/rustdoc, and Windows/macOS compile checks.
P4's `workspace.read` slice is also verified (528 stable/MSRV tests passed,
2 Live tests ignored); `workspace.files` and `workspace.search` are verified too
(576 stable/MSRV tests passed, 2 Live ignored; strict and cross-platform compile
gates passed). `workspace.status` completes P4 with 613 stable/MSRV tests
passed and 2 Live ignored; the same strict and cross-platform compile gates
passed. Bash streaming/control, change review and final integration remain
pending. These changes are not a new installed
Agent release. It also includes a native, stateless `subagent` Tool for explicitly
delegated child loops.

Profile `system_prompt` keeps its existing inline string form and also accepts
`{ file = "..." }`. Relative prompt paths are resolved against the parent of
the absolute config path supplied to `AgentConfig::load`; a config symlink keeps
its supplied alias directory as the base. The target must resolve to a regular
UTF-8 file no larger than 128 KiB; CRLF is normalized to LF, and symlinks to
regular files are allowed. The content is loaded when the configuration is
opened or reloaded, and each created Session stores its prompt snapshot in
`session.json`. The Agent never rereads the file on turn or Session reopen.
This is configuration-path handling, not a race-proof filesystem sandbox.

`agent.reload` rereads only the startup configuration file path. It rebuilds
the candidate model/profile catalog and future-turn execution snapshots before
swapping them; active loops keep their current configuration, and Session
records, history, and stored system-prompt snapshots are not rewritten. New
Sessions use the reloaded profiles and defaults; existing Sessions keep their
persisted snapshots. It does not reload the Agent binary. Future Bash
children scrub credential environment names from the existing cumulative
scrub set and the candidate model catalog. `data_dir` and Agent-level `event_capacity` changes
require restart. An embedded `Agent::open` has no reload source;
`Agent::open_file` is the source-aware
entrypoint used by the binary.

A Session may select a configured `model` and `reasoning` value, or inherit the
Profile defaults. Runtime-backed reasoning values include `auto`, `disabled`,
`low`, `medium`, `high`, `xhigh`, `max`, and `ultra`; each model's
`supported_reasoning` is an explicit capability allowlist. The Agent does not
infer these values for every OpenAI model, and `ultra` is only for an endpoint
that explicitly documents that custom or future effort value. Those settings
are frozen when the Session is created and persist across close and reopen.
`session.update` changes them later: the persistent record is rewritten and
any active loop receives the new execution config at its next request boundary
while the current tool batch keeps its old snapshot.

`session.rename` changes only the persisted title and metadata timestamp. It
accepts a trimmed string, treats an empty title as clear, and is safe while a
Session is idle, running, blocked, or closed.

## Architecture

Each user message becomes one runtime `AgentLoop`. A Session runs at most one
active loop; there is no queueing or auto-cancellation. Sending while a loop
runs fails with `session_busy`.

One stored Session owns the full conversation:

```text
<data_dir>/sessions/<session_id>/
    session.json      session record (settings, profile, workspace)
    history.jsonl     one JSON line per completed loop record
    summary.json      bounded, independently validated derived snapshot
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

Live `AgentEvent`s (output deltas, tool lifecycle, state changes, and bounded
presentation data) are best effort and may be dropped under pressure. The
authoritative sources are `turn.wait` and `session.history`. Opaque provider
reasoning is never persisted and never included in RPC views. The read-only
`session.presentation` method supplies local UI footer facts and honest unknown
context/cost state; it never starts a loop or executes a Tool.

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
content. Presentation data deliberately grants the trusted local TUI access to
bounded command/path/input detail; those values remain excluded from logs,
errors, and redacted Debug output.

## RPC Surface

The stdio protocol implements the v0.3 method set:

```text
agent.ping             agent.reload             agent.shutdown
profile.list           model.list
session.list           session.create         session.open
session.close          session.delete         session.state
session.context
session.compact        session.compact.cancel
session.update         session.rename        session.history
session.presentation
session.read           workspace.read
workspace.files        workspace.search        workspace.status
turn.send              turn.cancel            turn.wait
turn.steer             interaction.answer
```

`session.transcript` is gone; `session.history` returns the sanitized stored
history. Errors are classified with domain codes `-32001` through `-32022`;
`-32017 reload_requires_restart`, `-32018 reload_unavailable`, and
`-32022 context_uncompressible` are the latest additions. The full wire contract,
frame interleaving
guarantees, event shapes, and error mapping are documented in
[docs/rpc.md](docs/rpc.md).

`session.compact` is a deferred, Session-owned manual summary operation;
`session.compact.cancel` matches its exact operation identity. Operation IDs are
reserved for the loaded Session lifetime in a bounded 4096-entry set; closing
and reopening resets that set. The full-history JSONL remains authoritative and
the bounded summary sidecar is derived. An uncertain atomic replacement may
have changed disk while leaving the old in-memory projection unpublished, so
clients must reread before retrying.

The R1 manual-compaction draft is committed as source checkpoint `5397a65`,
following ordinary worker lifecycle checkpoint `1771898`. The current P3a slice
has parent-owned remote acceptance (see [progress](docs/0914-progress.md)), but
no new installation. It projects a validated summary and
its complete history suffix into the next Runtime `LoopRequest`; it also exposes
`session.context` and reports manual utility usage separately from ordinary turn
usage. P3b1 automatic threshold compaction has passed parent-owned remote
verification: the `[compaction]` policy, a bounded deferred
startup preparation, per-request budget estimation and ephemeral tool-exchange
summaries, bounded automatic observations, and `context_uncompressible`
reporting. P3b2 provider replay budgeting and one-shot overflow recovery have
also passed parent review and remote verification; see
[development progress](docs/0914-progress.md) for evidence and limits.
TUI `/compact` is outside this backend scope; subsequent work proceeds to P4
Workspace, whose first read-only `workspace.read` slice has also passed parent
review and remote verification, together with `workspace.files` and
`workspace.search` and `workspace.status`. P4 is verified; P5 Bash
streaming/control and P6 change review remain separate work. The
[execution audit](docs/verification/compaction-manual-audit.md) remains
historical evidence, not an acceptance gate. The
[foundation verification](docs/verification/compaction.md) remains separate;
there is no new release,
installation or native-artifact acceptance.

`session.context` is a read-only Session-owned projection of the current manual
or startup compaction operation, validated-summary coverage, recent manual
result, and the estimated Runtime history budget. Its `estimated_history_*`
fields cover only the projected history suffix (or full history when no valid
summary is loaded), not the summary, system/AGENTS text, tools, current input,
or exact provider tokenization. `estimated_request_context_tokens` is the latest
bounded automatic full-request estimate, and the `automatic` object retains
current/last operation metadata plus utility accounting without summary bodies.
When automatic compaction is enabled, `input_budget_tokens`,
`trigger_tokens`, and `target_tokens` expose the model's already-reduced context
window and policy thresholds, and `last_prepare_failure` may report
`context_uncompressible`. A manual
result with `utility_usage.complete: false` carries incomplete accounting; its
nested `usage` contains known fields from completed calls and failed streams
that emitted usage, and may be `null`. Usage emitted by a stream that later
fails is retained as a known partial total rather than dropped. It is never a
zero-filled copy of the main turn usage.

`session.presentation` is a read-only footer/tool-card projection. It returns
the configured Session model label, a fixed-argument Git branch lookup, the
last-loop activity timestamps, and explicit unknown/null context, cost, and
subscription fields. Tool presentation is bounded and whitelist-based; live
and history views use the same formatter. Successful `turn.send` and
`turn.steer` responses may include Agent acceptance timestamps without
changing their existing `turn`/`ok` fields.

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
six-name match: `read` is read-only, while `write`, `edit`, `apply_patch`,
`bash`, and `subagent` are mutating. `Auto` allows every known Tool; `Ask`
requests a `Medium`-risk approval per mutating call with the prompt
``Allow tool `<name>` for this call?``; `ReadOnly` denies every mutating Tool.
Approval prompts and denial reasons never include Tool arguments, paths,
content, commands, or raw JSON.

The Tool module implements the exact `read`, `write`, `edit`, `apply_patch`,
`bash`, and `subagent` tools. Each production Session receives a new ToolSet
sharing only that Session's Workspace. All six use strict object schemas and
reject unknown input fields. `subagent` is registered only when the profile
explicitly names it, runs stateless child `AgentLoop`s without Store records,
supports one task, bounded parallel `tasks`, and sequential `chain` handoff via
`{previous}`. Each stage propagates the text from its final assistant response;
a later textless response does not reuse an earlier round. It excludes itself
from child tools. Child work is limited to
the parent Workspace or descendants, uses a fresh model/config snapshot, and
joins before a normally completed subagent Tool returns. If external Tool or
turn cancellation/timeout drops that Tool future, its scope only cancels
children synchronously; the Agent-owned registry retains their handles until
the Session loop completion, `session.close`, or `Agent::shutdown` drains them.
The parent Tool may therefore return before that deferred join completes. The
inherited approval policy remains active; child approval/input requests fail as
a static stage result because stateless child calls have no separate interaction
channel. The child runner uses Runtime's authoritative `LoopHandle::watch_state()`
and `WaitingForInput` state rather than the best-effort `InteractionRequested` event,
which may be dropped under pressure. Native interaction coverage uses an offline
fake model/provider seam; it is evidence for this Agent/Runtime integration, not for
real-provider behavior. `read` supports one-based line offsets, line and byte limits,
and safe directory listings.
`bash` is not a sandbox and retains the host authority of the Agent process;
use external container or OS isolation for untrusted models or commands. Run
only one Agent process for a given `data_dir`; the Store has no cross-process
lock.

## Library

The crate exposes `AgentConfig`, `AgentError`, `LoopOverrides`, `Profile`,
`ApprovalMode`, `Agent`, `Workspace`, `WorkspaceError`, `SessionId`, the
session/turn DTOs (`CreateSession`, `RenameSession`, `UpdateSession`, `SendMessage`,
`SteerMessage`, `AnswerInteraction`, `GetHistory`, `HistoryPage`,
`SessionState`, `SessionStatus`, `SessionUpdateResult`, `CompactSession`,
`CompactionResult`, `CompactionUtilityUsage`, `CompactionStatus`,
`CompactionPhase`, `CompactionProgress`, `SessionContext`, `SummaryCoverage`, `ContextBudget`, `TurnRef`, `TurnResult`,
`TurnPersistence`, `PresentationView`, `ToolDisplay`, and
`AssistantDisplayPart`), the single-consumer `AgentEventStream`, `AgentEvent`,
and `run_stdio`. The crate is `#![forbid(unsafe_code)]`.

`Agent` opens the local Store and manages multiple loaded Sessions. `Agent`
owns the global event channel; one Session owns its own history and runs at
most one loop, manual compaction, or startup admission operation at a time.
`session.create` and `session.open` never start a loop. `session.close` cancels
active work, joins Session-owned workers, and persists cleanly before returning.

`Agent::shutdown` is the cleanup barrier for embedded Rust callers. It cancels
active loops, manual compaction, and startup admission, waits for Agent-owned
workers and any native child workers, and awaits persistence wrap-up.
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
