# Session Rename And Prompt Files

This is the historical rename/prompt-file boundary. The later same-version
[Tool/reload/stateless subagent acceptance](followups.md) records current artifacts;
the source and hashes below are intentionally preserved.

Accepted source: `22ecb3df5fe20ddc8fa37c7fb74aa0cc21f6e416`, paired with TUI
`cc7675728fdc87754a495cec7db889a1a5074ada`. Agent **0.3.3**, TUI **0.2.8**,
and Runtime **0.4.1** revision `6cd2bdbc634437dea925495c61c7eb0be10ba171`
remain unchanged. Feature commits are `ebd0ef7` (rename) and `22ecb3d`
(prompt files). They were committed separately; no push or new hosted CI is
claimed for these changes.

## Contracts

`session.rename` takes a stable `session_id` and `title`, returns the persisted
`SessionInfo`, and does not issue an execution-config update. Titles are trimmed;
empty clears the title; the limit is 4096 UTF-8 bytes and disallowed controls are
rejected. Loaded idle/running/blocked sessions serialize metadata writes with
existing session IO. Closed-session rename edits metadata without opening its
workspace or history. Failed awaited writes preserve the tested prior state.
Cancellation or lost transport has an **unknown outcome**, not a rollback
promise; do not automatically retry. Existing single-Agent-per-store ownership
and Store durability limits still apply. See [RPC contract](../rpc.md).

Existing inline prompt strings remain valid. Explicit file configuration is:

```toml
[profiles.coding]
system_prompt = { file = "prompts/coding.md" }
```

The path is relative to the caller-supplied config path's parent, including a
config symlink's alias directory. Absolute prompt paths also work. There is no
`~` or environment expansion. A symlink to a regular file is allowed; content
must be UTF-8, at most 128 KiB raw bytes, and pass prompt validation after CRLF
normalization. Errors do not expose the file path or contents. This is a trusted
configuration feature, not a race-proof hostile-filesystem sandbox.

Only explicit `AgentConfig::load/from_toml` processes file objects. Derived
`Deserialize` remains pure and inline-only; relative `from_toml` file references
without a base directory are rejected. Content is resolved once at configuration
load and each Session persists its own prompt snapshot. Turns and reopen do not
reread the file; editing it requires a fresh config load for future sessions and
does not retroactively replace existing Session prompts. The TUI never reads it.

## Verification

Parent built a fresh Git archive on the authorized Linux builder. Stable and
Rust 1.85 each passed **318 tests, 2 ignored**. Stable strict Clippy, rustfmt and
warning-denied rustdoc passed. Agent's pre-existing MSRV Clippy diagnostics in
`presentation.rs` are not claimed fixed. The paired TUI passed **464/18** on
both toolchains, with **17 real-Agent E2E** cases explicitly passing on both.
Parent independently repeated stable E2E against the rebuilt Agent.

`session-config/logs/` contains final Agent gates, bounded-file/config-alias/
snapshot checks, and the Windows TOML-path regression. The Windows path test
serializes a Windows path on Linux; it does not claim a current Windows CI run.
Some early focused commands selected zero tests; only nonzero test output and
final all-target results establish coverage.

Native macOS Debug and Release each passed real iTerm2 flows with fresh owned
stores and a loopback Responses provider. After startup the synthetic prompt
file was changed; all three actual requests, including one from a later-created
Session, retained the original prompt. Busy rename, selected-session close,
default-Cancel deletion, explicit deletion and survivor isolation also passed.
Structured request-presence assertions are in `session-config/native/`.
No real-upstream/TLS or pixel screenshot coverage is claimed.

## Artifacts

Linux cross-build: Rust 1.98.0, LLVM/LLD 19, SDK 26.2, x86_64 macOS target,
minimum OS metadata 11.0. Local execution, Mach-O and signature checks passed;
no local Rust compilation was used. Accepted Agent SHA-256 values:

- Debug: `84c85c768f17741ac69e49e81da1c7b2dd92d7c4eed360a51d43e337c86d0bc8`
- Release: `aee24246491bdb13ffb95dff4fccc95895800e5f19b67c51c815132540385642`
- Debug/dSYM UUID: `4C4C441C-5555-3144-A180-DED71724829D`

Both were installed at the existing `target/debug/minicore-agent` and
`target/release/minicore-agent` paths with exact byte comparisons. Old binary
inodes and Debug symbols remain in `target/preserved-before-session-M0rGv9/`.
`session-config/installation.log` includes paired TUI installation checks.
User configuration, Store data and existing processes were not changed or
restarted. Current processes may still run an older image despite unchanged
version strings.

The complete paired safety regressions, native failures/fixes, streaming/FIFO
checks and real-PTY normal/panic restoration evidence live in the sibling TUI
checkout at `docs/verification/session-management/`.
