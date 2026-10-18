# AGENTS.md

Guidance for coding agents working in this repository. Humans should start with
[README.md](README.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## What This Project Is

`minicore-agent` is a local coding-agent backend: a Rust library plus a binary
that serves newline-delimited JSON-RPC 2.0 on stdio. The Agent owns Sessions,
persistence, providers, tools, Workspaces, compaction, and the RPC surface.
The model/tool loop itself comes from `minicore-runtime`, pinned by Git revision
in `Cargo.toml` and `Cargo.lock`. Keep that split: loop semantics belong in the
Runtime repository, and session or product policy belongs here.

## Repository Map

| Path | Responsibility |
|---|---|
| `src/lib.rs`, `src/agent.rs`, `src/agent/session_setup.rs` | Public API (`Agent`, DTOs) and Session construction |
| `src/sessions.rs`, `src/sessions/` | Loaded Sessions, the active-loop task, event mapping, compaction ownership |
| `src/rpc/` | JSON-RPC protocol types, dispatcher, deferred work, stdio I/O |
| `src/store.rs`, `src/store/` | `session.json`, `history.jsonl`, summaries, auxiliary tool records |
| `src/models.rs`, `src/models/openai/` | Model catalog and the OpenAI Responses adapter |
| `src/tools/` | `read`, `write`, `edit`, `apply_patch`, `bash`; `ToolOwners` joins started work |
| `src/workspace/` | Rooted path resolution, atomic writes, scan, search, Git status |
| `src/compaction/` | Projection, thresholds, emergency recovery, summary utility, prompts |
| `src/tool_data/`, `src/changes.rs`, `src/diff.rs`, `src/read.rs` | Tool observations, change tracking, diffs, read queries |
| `src/event.rs`, `src/presentation/` | Agent events and bounded display data |
| `src/test_support/` | `#[cfg(test)]` helpers shared by unit tests |
| `tests/it/`, `tests/rpc_soak.rs`, `tests/support/` | Process-level integration tests and their shared support |
| `docs/` | Current guides; `docs/rpc.md` is the normative wire contract |

## Commands

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo +1.85.0 test --locked --all-targets   # MSRV
```

Focused runs: `cargo test --locked --lib store::` for unit tests, or
`cargo test --locked --test it rpc_stdio::` for one integration suite. Always
pass `--locked`. Do not run a broad `cargo update`.

## Code Rules

- Rust edition 2024, MSRV 1.85. Do not use std APIs stabilized later, such as
  `str::floor_char_boundary`. Use `crate::text::{floor_char_boundary, truncate}`
  instead.
- `unsafe` is forbidden (`unsafe_code = "forbid"`).
- Dependencies are pinned with `=`. Add one only with a clear need. A version
  bump changes `Cargo.toml` and the `minicore-agent` entry in `Cargo.lock`, and
  nothing else.
- Keep code lean. Validate inputs once at the boundary that owns them, do not
  re-check invariants the types or earlier validation already guarantee, and add
  no speculative abstractions or configuration.
- Every queue, read, scan, and buffer needs an explicit bound.
- Keep blocking filesystem and CPU work off async workers (`spawn_blocking`).
  Work that has started a side effect must be owned and joined by the loop and
  close barriers (see `ToolOwners` in `src/tools/command.rs`). Do not detach it.
- Stdout carries RPC frames only; diagnostics go through `tracing` to stderr.
  Never log or return API keys, prompts, provider bodies, tool arguments, Bash
  commands, or filesystem paths in errors.

## Contracts

These are compatibility decisions. Do not change them as a side effect:

- The RPC protocol version, method shapes, and event wire format. `docs/rpc.md`
  is normative, and `src/event/events.jsonl` is a byte-level event snapshot.
- On-disk formats: `session.json`, `history.jsonl`, `summary.json`, and
  `tools/`. History revision strings must stay byte-identical.
- The pinned Runtime revision. When moving it, update the `rev` in `Cargo.toml`
  and the matching `minicore-runtime` lines in `Cargo.lock` together.

A user-visible behavior change updates `docs/rpc.md` or the relevant guide, plus
`CHANGELOG.md`. Records under `docs/verification/`, `docs/archive/`, and
`docs/releases/` are historical; do not rewrite them.

## Tests

- Unit tests live in `<module>/tests.rs`, declared with `#[cfg(test)] mod tests;`.
  Do not add large inline test modules.
- Use the shared helpers: `crate::test_support::{ScriptedModel, Step}` for model
  scripts; `Registry` and `Gate` for `#[cfg(test)]` fault-injection and pause
  hooks; `TempDir` for scratch directories. Agent tests use `AgentFixture`
  (`src/agent/tests/fixture.rs`); RPC tests use `RpcFixture` and `RpcHarness`
  (`src/rpc/server/tests.rs`).
- Integration suites are modules of the `it` target. `rpc_soak` stays a separate
  target. Shared process helpers live in `tests/support/`.
- Tests must be deterministic. Synchronize with gates or notifications instead
  of sleeps, and keep scan or size limits small instead of writing large data.
- Use only the loopback provider mock (`tests/support/openai_mock.rs`). Never
  use real credentials. Live provider tests stay `#[ignore]`.

## Commits

- Subject format: `type(scope): summary`, for example
  `fix(store): sync the parent directory after rename`. The summary is
  lowercase and imperative. Types: `feat`, `fix`, `refactor`, `perf`, `test`,
  `docs`, `build`, `ci`, `chore`. The scope is the module or area.
- One logical change per commit, with its tests and docs. Stage explicit paths.
- Never commit secrets, local Stores (`.minicore-agent*/`), local configs such
  as `cus-resp.agent.toml`, or build output.
