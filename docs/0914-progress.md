# Shared Data Development Progress

Specification: `minicore-agent-0914-dev-spec.md` (2026-09-13).
Branch: `feat/0914-shared-data`.
Agent starting HEAD: `8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0`.
Runtime remains pinned to 0.4.1, `6cd2bdbc634437dea925495c61c7eb0be10ba171`.

The user's September 14 instruction authorizes resumed source development and
remote verification. Historical audit reports remain historical evidence;
previous unaccepted results are not acceptance gates for this work.

## Execution

One `implement` helper uses `cus-resp/deepseek-v4.1-flash:high`; the parent owns
review, remote verification and commits. Per the user's updated instruction,
three consecutive helper failures switch implementation to
`cus-resp/gpt-5.6-luna:max`; user cancellations do not count. P1 was completed by
the fallback after upstream/context-compaction failures. No local compilation. Remote build root:
`root@192.168.20.199:/root/minicore-agent-0914`.
Transfers allow only source, tests, Cargo files and public fixtures. Private
configuration and real Session data are excluded. Git commits use repository-local
`zqcli <zqcli@users.noreply.github.com>`; no credentials are stored in source.

## Stages

| Stage | Deliverable | Status |
| --- | --- | --- |
| P0 | Cancellation-safe RPC framing; bounded deferred admission | Verified, see below |
| P1 | Read-only session pages; retained turn-result queries | Linux stable/MSRV verified; Windows/macOS compile checks passed |
| P2 | Structured tool identity, invocation and query records | Pending |
| P3 | Manual acceptance, startup/request compaction, one overflow recovery | Pending |
| P4 | Bounded Workspace files/read/search/status | Pending |
| P5 | Owned Bash streaming, cancellation, result retention | Pending |
| P6 | Workspace and tool change scopes, versioned diffs | Pending |
| P7 | Client contract integration and final verification/documentation | Pending |

Each stage is a vertical implementation/API/RPC/test slice, reviewed before
commit. Shared protocols are developed serially. DTOs are introduced alongside
their first real consumer, not as an unused framework.

## P0 Verification

The RPC reader retains partially consumed frames across select cancellation and
checks the cumulative 1 MiB limit. Deferred waiters share a 32-entry ceiling;
`resource_exhausted` (`-32019`, retryable) rejects new waiters/compaction before
starting work, without denying ping/cancel/shutdown. Existing interfaces remain.

Parent-run Linux stable verification on 2026-09-14:

- `cargo fmt --all -- --check`: passed.
- `cargo test --locked --lib rpc::server::tests`: 43 passed, 0 failed.
- `cargo clippy --locked --all-targets -- -D warnings`: passed.
- Remote logs: `logs/p0-rpc.log`, `logs/p0-clippy.log` under the build root.

P0 was additionally covered by the full P1 gates below. No live Provider tests,
installation, package version change or push has been performed.

## P1 Verification

Adds `session.read` and `turn.result`, advertised through `agent.ping` protocol
version 1 and capabilities. Both return sanitized ordered item JSON fragments
with explicit raw UTF-8 offsets and a hard encoded-result byte budget (default
256 KiB, maximum 1 MiB). The budget excludes the JSON-RPC transport envelope.
Read-only Store scans capture a complete prefix and SHA-256 revision, leave
incomplete tails untouched, and never open execution resources. Loaded reads
validate the committed snapshot. Turn queries reuse the existing completion
report, including failed appends, and fall back to persisted loop records.

Queries have a 10-second deadline, a 64 MiB/100,000-line scan ceiling, four
concurrent-query slots, and the shared 32-deferred-request ceiling. Exceeding a
scan limit returns `query_limit`, not a purported complete page. Query shutdown
requests cancellation and joins owned tasks. Legacy RPC error mappings remain
unchanged; new query parameter errors are mapped only at the new entrypoints.

Parent-run remote verification on 2026-09-14:

- Linux stable 1.97.1 `cargo test --locked --all-targets`: 415 passed, 2 ignored.
- Rust 1.85.0 `cargo test --locked --all-targets`: 415 passed, 2 ignored.
- Stable strict Clippy, rustdoc and formatting checks: passed.
- `cargo check --locked --all-targets --target x86_64-pc-windows-gnu`: passed;
  existing unused `write_reload_config` test helper warning remains.
- `cargo check --locked --all-targets --target x86_64-apple-darwin`: passed
  using the remote Zig C compiler with `CRATE_CC_NO_DEFAULTS=1` to avoid
  conflicting target flags. Native macOS/Windows execution was not performed.
- Logs: `logs/p1-tests.log`, `p1-msrv.log`, `p1-clippy.log`, `p1-doc.log`, and
  `p1-<target>.log` under the remote build root.

Unpersisted results are retained only while the existing completed ActiveLoop
is held (until the next turn or Session close). Reads beyond the scan ceiling
are explicitly rejected; no full-history index or snapshot service is added.
