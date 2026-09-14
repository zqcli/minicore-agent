# Shared Data Development Progress

Specification: `minicore-agent-0914-dev-spec.md` (2026-09-13).
Branch: `feat/0914-shared-data`.
Agent starting HEAD: `8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0`.
Runtime remains pinned to 0.4.1, `6cd2bdbc634437dea925495c61c7eb0be10ba171`.

Historical audit reports remain historical evidence and previous unaccepted
results are not acceptance gates. The current source handoff implements P3a only:
startup projection from a validated summary, same-loop model-update binding,
`session.context`, and independent manual utility usage reporting. P3a source,
tests and documentation passed parent review and remote acceptance. Automatic compaction and
upstream overflow recovery remain pending.

## Execution

Implementation prefers `cus-resp/deepseek-v4.1-flash:high`; three consecutive
helper failures permit `cus-resp/gpt-5.6-luna:max`. P3a used that fallback after
three DeepSeek context-compaction failures. Only one helper implements at a
time; the parent owns
review, remote verification and commits. This handoff permits source, focused
tests, documentation and formatting only. No local build/test/check or remote
operation is part of the handoff. Transfers of private configuration and real
Session data are excluded; the prior execution-boundary incident remains in the
historical audit. No credentials are stored in source.

## Stages

| Stage | Deliverable | Status |
| --- | --- | --- |
| P0 | Cancellation-safe RPC framing; bounded deferred admission | Verified, see below |
| P1 | Read-only session pages; retained turn-result queries | Linux stable/MSRV verified; Windows/macOS compile checks passed |
| P2 | Structured tool identity, invocation and query records | Verified; memory-only retention, streams/persistence follow in P5 |
| P3 | Manual acceptance, startup/request compaction, one overflow recovery | P3a verified; P3b automatic/overflow pending |
| P4 | Bounded Workspace files/read/search/status | Pending |
| P5 | Owned Bash streaming, cancellation, result retention | Pending |
| P6 | Workspace and tool change scopes, versioned diffs | Pending |
| P7 | Client contract integration and final verification/documentation | Pending |

Each stage is a vertical implementation/API/RPC/test slice, reviewed before
commit. Shared protocols are developed serially. DTOs are introduced alongside
their first real consumer, not as an unused framework.

## P3a Verification

P3a keeps the complete `history.jsonl` and the Session/Store model unchanged.
At loop admission, only a validated `CompactionState` snapshot can project its
covered prefix away from the Runtime `LoopRequest.history`; the summary is
injected once as labeled user historical data. The uncovered suffix, current
User input, accepted Steers, and complete tool pairs remain intact. Runtime item
and byte limits are checked against the projected history, and an absent or
invalid summary leaves an over-limit request as `history_too_large` rather than
silently slicing it.

The projected suffix and summary are bound to the active loop and reused when
`session.update` changes the model at a request boundary, preventing a second
slice. `session.context` responds while manual compaction is busy and reports
operation state, validated coverage, the latest in-process manual result, and a
conservative projected-history budget. Its history estimate is only the Runtime
`LoopRequest.history` suffix; full prepared-request context is explicitly
unknown in P3a. The scan is bounded and returns unknown rather than blocking on
an unbounded history. Manual utility accounting reports observed calls,
`complete`, and known usage fields only; missing evidence never becomes zero.
Focused tests cover over-limit startup, reopen, unchanged history, no double
slice, same-loop model updates, busy context, bounded context estimation,
multiple utility calls, duplicate usage, missing usage, cancellation, and write
failure. Utility result accounting carries observed call attempts, a `complete`
flag, and known usage from completed calls only. A failed stream's own usage
is currently treated as unknown; no value is filled with zero.

Parent-run remote Linux stable/MSRV suites each passed 451 tests, with 2 Live
tests ignored. Strict Clippy, rustdoc and fmt checks passed. Logs are
`/root/minicore-agent-0914/logs/p3a-{tests,msrv,clippy,doc}.log` on the builder.
Cross-platform checks will be repeated after P3b; P2's checks remain separate.
P3a does not add automatic threshold compaction, current-tool ephemeral
summaries, provider overflow recovery, or TUI `/compact`.

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

## P2 Verification

P2 adds a narrow, non-authoritative tool-fact layer without a new service
architecture, reusing the existing per-Session `Presentation` ownership and the
Runtime's real boundaries:

- `src/tool_data.rs` holds `ToolRef {session_id, loop_id, request_index,
  tool_call_id}`, `ToolInvocationData` (structured `subject` plus a bounded raw
  input preview), `ToolExecutionData` (state, whitelisted `ToolPhase`, real
  start time, authoritative Runtime outcome, per-stream availability), and the
  `tool.read`/`tool.output` queries. Records are bounded per loaded Session
  (1024 records, 8 MiB of retained data plus counted metadata); eviction frees
  the backing `String` capacity, reports `available`/`partial`/`expired`, and
  keeps input and output availability independent. `tool.output` pages raw
  bytes by UTF-8 offset with an encoded-JSON byte cap, never uses
  `escape_default`, and distinguishes `pending` (still running) from
  `unavailable` (terminal, never observed) from a real empty result.
- Identity comes only from real Runtime values: `ModelCallContext` at
  `Model::start` and the Runtime tool-call id. A new `PresentationPolicy`
  wrapper publishes the requested invocation before an approval decision, so
  approval-time data is obtainable while a call is `awaiting_policy` and never
  reported as `running`; `started_at` is stamped only when the tool truly runs.
  `PresentationTool` retains the raw result; the joined loop report
  unconditionally reconciles state/outcome and captures result text for every
  outcome (including failed/denied), without resurrecting evicted bytes.
- New RPC methods `tool.read`/`tool.output` (capability-declared in
  `agent.ping`) and best-effort `tool_invocation`/`tool_execution` events come
  from the same record source as the queries. `ToolRef` rejects unknown fields.
  The legacy `ToolDisplay` presentation path is retained unchanged for
  compatibility; the new contract has no UI fields. No tool identity is
  guessed from the most recent call.
- Real phases are emitted through `ToolContext.progress` for read/write/edit/
  apply_patch/bash and mapped to a known-stage whitelist. Raw stdout/stderr
  streaming, process ownership, and durable auxiliary files remain P5; change
  references remain P6.

Parent-run remote verification: Linux stable and Rust 1.85.0 full suites each
passed 442 tests, with 2 Live tests ignored. Strict stable Clippy, rustdoc,
formatting, Windows and macOS all-target compile checks passed. The existing
Windows test-helper warning remains. Logs are `logs/p2-tests.log`,
`p2-msrv.log`, `p2-clippy.log`, `p2-doc.log`, `p2-windows.log`, and `p2-macos.log`.
No local compilation, native cross-platform test execution, installation or push.
