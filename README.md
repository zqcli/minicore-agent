# MiniCore Agent

[![CI](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/zqcli/minicore-agent/actions/workflows/ci.yml)

MiniCore Agent **0.6.2** is an RPC-first local agent core. It provides a Rust
library, a local Store, rooted Workspaces, multiple loaded Sessions, bounded
Tool data, and a stdio JSON-RPC service for a client UI.

- Rust edition: 2024; MSRV: **1.85**
- Runtime: `minicore-runtime 0.6.1`, pinned to Git revision
  `666d5a15ac08c614ce75dd90337f95f10ec29f80`.
- RPC protocol version: **1**
- Versioned source delivery uses the `dev` branch and annotated release tags;
  this does not imply a package-registry publication or binary installation.
  Historical verification records retain their original scope and dates.

## Quickstart

A standalone checkout fetches the fixed Runtime Git revision from `Cargo.toml`
and `Cargo.lock`; no adjacent Runtime checkout or path override is required.
Tool-round execution defaults to unlimited (`0`), while cancellation, deadlines,
and other resource limits remain active.

Edit `example.agent.toml` with a real provider model ID and provide the API key
through the environment variable named by `api_key_env`. The example enables
`approval = "auto"`; use it only in a trusted local environment.

```bash
cargo build --locked --release
./target/release/minicore-agent --version
./target/release/minicore-agent --config ./example.agent.toml --stdio
```

The binary accepts `--version` or `--config <path> --stdio`. It has no
`--help` option. The stdio service reads one JSON-RPC object per input line and
writes responses/events as newline-delimited JSON; see [the RPC contract](docs/rpc.md).

## API And Design

The public Rust entry point is [`src/lib.rs`](src/lib.rs). Generate local
rustdoc with:

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --open
```

Start with `Agent`, `AgentConfig`, `Workspace`, the session/turn DTOs,
`AgentEventStream`, and `run_stdio`. `Agent::open_file` is the source-aware
async constructor used by the binary; `Agent::open` is the embedded constructor
without a reload source. `Agent::shutdown` is the async cleanup barrier for
embedded callers; dropping an `Agent` with live work does not synchronously wait
for its loop tasks.

A Session runs at most one active Runtime loop. Its persisted `session.json`,
`history.jsonl`, and derived `summary.json` live below:

```text
<data_dir>/sessions/<session_id>/
```

The complete history is authoritative. `persistence: persisted` means the
Agent's append completed in the running process; the Store is not a transactional
ledger and this is not an end-to-end crash-durability guarantee. A failed Agent
append returns the Runtime result with `persistence: failed` and blocks further
turns. On reopen, only a trailing incomplete JSONL line may be truncated; a
complete malformed record is corruption, not recoverable crash state. Live
events are best-effort; use `turn.wait`, `turn.result`, and history queries for
authority. Existing v0.2 Session data is not migrated.
Warm in-memory observations are preferred, while bounded cold reads use durable
Store data when a Session is unloaded or retained bytes were evicted.

## Durable Provider Replay

Validated Responses output now follows the existing session history and JSONL
pipeline across tool rounds, later turns, and process restarts. It is private
provider metadata, excluded from RPC/read/summary prose and redacted Debug.
See [the replay contract](docs/provider-replay.md) for identity fallback, bounds,
usage uncertainty, and compatibility.

## Context And Compatibility

Automatic compaction checks settled context before the first request, then
checks between completed assistant/tool rounds, and independently after a
Completed + Persisted turn. The default trigger is 95% of the effective input
window; explicitly configured percentages remain unchanged. New User/Steer text
and the newest unread tool exchange remain verbatim. Manual `/compact` is idle-only. A confirmed safe pre-output upstream
context-capacity rejection can compact and retry that logical model request
once, without restarting the task or replaying tools. Ordinary local token estimates do not veto provider requests. Utility calls
retain explicit provider-aware budget checks. Partial output, unknown delivery
and generic HTTP 413 are not capacity-recovery permission.

`turn.wait` does not wait for post-turn compaction. Even a budget check that
becomes Noop can briefly make submit busy; clients preserve drafts and reconcile
state/context without auto-resending. Manual operation IDs cannot use `auto-`.
`tool_rounds` statistics are now `u64`; clients restricted to `u16` cannot read
counts above 65,535. RPC version and storage formats do not mechanically change.

In-turn threshold/emergency reductions are promoted only after their source is persisted. The
generated semantic summary remains limited to 64 KiB. A complete settled
projection, including its retained tail, instead uses the existing 256 KiB
snapshot-file limit and the Runtime historical-message envelope limit.
`emergency_settlement_*` failures preserve the original turn/history and old
in-memory projection. UnknownWrite remains non-blocking and does not imply
success or a disk reload from context queries. See [context and compaction](docs/context.md).

## RPC Surface

The service implements 33 methods, grouped here for orientation rather than as
a second wire specification:

- Agent: `agent.*`
- Discovery: `profile.list`, `model.list`
- Sessions: `session.*`
- Turns: `turn.send`, `turn.steer`, `turn.cancel`, `turn.wait`, `turn.result`
- Read/review: `session.history`, `session.read`, `session.presentation`,
  `tool.read`, `tool.output`, `workspace.*`, `changes.*`
- Interaction: `interaction.answer`

The exact method inventory, parameters, response shapes, event payloads,
framing, ordering, limits, and error mapping are maintained in
[docs/rpc.md](docs/rpc.md). The removed `session.transcript` and executable
`subagent` Tool are not part of this surface.

## Trust Boundary

This is a local backend, not a security sandbox. Bash runs with the Agent
process's host authority. Workspace path checks prevent ordinary traversal and
symlink mistakes, but do not defend against a same-user process racing the
filesystem. Use OS/container isolation for untrusted commands or models.

RPC stdout is reserved for protocol frames and events; diagnostics go to
stderr. API key values, prompts, provider bodies, Tool arguments, and Bash
commands are excluded from logs, errors, and redacted debug output. Bounded
presentation data intentionally exposes selected local command/path/input detail
to a trusted local UI, so deployments forwarding RPC beyond that UI need their
own policy. Run only one Agent process for a given `data_dir`; the Store has no
cross-process lock.

## Documentation And Evidence

- [Documentation index](docs/README.md)
- [Configuration](docs/configuration.md)
- [Architecture and ownership](docs/architecture.md)
- [Context and compaction](docs/context.md)
- [Security boundary](docs/security.md)
- [Verification index](docs/verification/README.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)

The [0.5.0 freeze verification record](docs/verification/0.5.0.md) documents
that historical local freeze, not the current release. Verification records
preserve the facts and limitations of the runs they record and are not substitutes
for this current contract. Historical release notes remain under
[`docs/releases`](docs/releases/0.3.3.md), and archived plans and specifications
are indexed by [docs/archive](docs/archive/README.md).
