# AGENTS.md

Guidance for coding agents working in this repository. Humans should start with
[README.md](README.md) and [CONTRIBUTING.md](CONTRIBUTING.md). The source and
`docs/rpc.md` define behavior; where this file and the code disagree, the code
wins and this file gets fixed.

## What This Project Is

`minicore-agent` is a local coding-agent backend: a Rust library plus a binary
that serves newline-delimited JSON-RPC 2.0 on stdio. The Agent owns Sessions,
persistence, providers, tools, Workspaces, compaction, and the RPC surface.
The model/tool loop itself comes from `minicore-runtime`, pinned by Git revision
in `Cargo.toml` and `Cargo.lock`. Keep that split: loop semantics belong in the
Runtime repository, and session or product policy belongs here. Frontends such
as [minicore-tui](https://github.com/zqcli/minicore-tui) only speak the RPC
contract; do not shape the Agent's schema around one frontend's convenience.

The Agent serves one client and spends its time waiting on providers and tool
processes. Keep it small: bounded memory over long sessions, little CPU while
waiting, and nothing per core that a single client does not need (the binary
runs Tokio on two workers).

## Repository Map

| Path | Responsibility |
|---|---|
| `src/main.rs` | Binary entry: Tokio runtime (two workers), `run_stdio` |
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
pass `--locked`. Do not run a broad `cargo update`; when one package must move,
check that the lock diff touches nothing else.

## Code Rules

- Rust edition 2024, MSRV 1.85. Do not use std APIs stabilized later, such as
  `str::floor_char_boundary`. Use `crate::text::{floor_char_boundary, truncate}`
  instead.
- `unsafe` is forbidden (`unsafe_code = "forbid"`).
- Keep existing exact (`=`) dependency pins, and add a dependency only with a
  clear need.
- Write the least code that solves the problem. No speculative abstractions,
  configuration, or features; no state for cases that cannot happen.
- No defensive programming: validate inputs once at the boundary that owns them
  (RPC params, config, provider responses, tool arguments) and do not re-check
  invariants the types or earlier validation already guarantee.
- Delete dead code, unused parameters and always-true flags instead of keeping
  them "just in case".
- Comments:
  - `///` is one sentence, and only where the name and signature leave
    something unsaid.
  - `//` explains why (a constraint, an ordering, a platform trap), at most two
    lines. Never narrate the code.
  - No citations of specs, reviews, findings or section numbers (`§`) in code.
- Every queue, read, scan, and buffer needs an explicit bound.
- Keep blocking filesystem and CPU work off async workers (`spawn_blocking`).
  Work that has started a side effect must be owned and joined by the loop and
  close barriers (see `ToolOwners` in `src/tools/command.rs`). Do not detach it.
- Stdout carries RPC frames only; diagnostics go through `tracing` to stderr.
  Never log or return API keys, prompts, provider bodies, tool arguments, Bash
  commands, or filesystem paths in errors. Secrets come only from the
  environment variable named by `api_key_env`, and are scrubbed from tool
  processes.

## Platforms

CI tests Linux, macOS and Windows; code must behave the same on all three.

- Windows file handles opened with `append(true)` cannot truncate. Truncate or
  roll back through a separate writable handle (see `rollback_append`).
- Windows reserves device names such as `nul`, `con` and `aux` in every
  directory; never use them as file names, in fixtures either.
- macOS (APFS) rejects file names that are not UTF-8; tests that need them are
  `#[cfg(target_os = "linux")]`.
- Byte-compared fixtures and snapshots (such as `src/event/events.jsonl`) are
  listed in `.gitattributes` with `eol=lf` so a Windows checkout matches.
- Windows counts `Instant` from boot: never compute `Instant::now() - d` with a
  large `d`, in tests either.
- Put platform-only code behind `cfg` at the call site and keep its pure logic
  compiled and tested everywhere.

## Contracts

These are compatibility decisions. Do not change them as a side effect:

- The RPC protocol version, method shapes, and event wire format. `docs/rpc.md`
  is normative, and `src/event/events.jsonl` is a byte-level event snapshot.
- On-disk formats: `session.json`, `history.jsonl`, `summary.json`, and
  `tools/`. History revision strings must stay byte-identical.
- The pinned Runtime revision. When moving it, update the `rev` and `version`
  in `Cargo.toml` and only the matching `minicore-runtime` entry in
  `Cargo.lock`, plus the Runtime line in `README.md` and `docs/README.md`.

Change a contract only with an explicit compatibility decision, tests and docs.
A user-visible behavior change updates `docs/rpc.md` or the relevant guide,
plus `CHANGELOG.md`. Records under `docs/verification/`, `docs/archive/`, and
`docs/releases/` are historical; do not rewrite them.

## Tests

- A bug fix comes with a test that fails without the fix; check it by reverting
  the fix once. Do not write tests that only restate the implementation.
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

## CI And Releases

- `.github/workflows/ci.yml` runs on every branch and tag: quality (fmt,
  Clippy, rustdoc), tests on Linux, macOS and Windows with `--no-fail-fast`,
  MSRV, and a release build per platform uploaded as
  `minicore-agent-<version>-<target>`.
- A release bumps `version` in `Cargo.toml` and the `minicore-agent` entry in
  `Cargo.lock` only, moves `Unreleased` in `CHANGELOG.md` to the version, and
  updates the version shown in `README.md`, `docs/README.md` and the
  `docs/rpc.md` examples. Tag `vX.Y.Z` (annotated) only after CI is green on
  that commit. A Runtime release is adopted before the Agent release that
  depends on it; `minicore-tui` then pins the Agent tag's full SHA.

## Commits

- Subject: `type(scope): summary`, scope required, for example
  `fix(store): sync the parent directory after rename`. Lowercase, imperative,
  no trailing period. Types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`,
  `build`, `ci`, `chore`. The scope is the module or area.
- One logical change per commit, with its tests and docs. Stage explicit paths;
  leave untracked files you did not create alone.
- Do not rewrite pushed history, force-push, move tags or change the Git
  identity unless asked.
- Never commit secrets, local Stores (`.minicore-agent*/`), local configs such
  as `cus-resp.agent.toml`, or build output.
- Code, comments, docs and commit messages are in English.
