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
review, remote verification and commits. A provider failure switches implementation
to `cus-resp/gpt-5.6-luna:max`. No local compilation. Remote build root:
`root@192.168.20.199:/root/minicore-agent-0914`.
Transfers allow only source, tests, Cargo files and public fixtures. Private
configuration and real Session data are excluded. Git commits use repository-local
`zqcli <zqcli@users.noreply.github.com>`; no credentials are stored in source.

## Stages

| Stage | Deliverable | Status |
| --- | --- | --- |
| P0 | Cancellation-safe RPC framing; bounded deferred admission | Verified, see below |
| P1 | Read-only session pages; retained turn-result queries | Pending |
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

Full-suite/MSRV/rustdoc/platform gates remain pending. No live Provider tests,
installation, package version change or push has been performed.
