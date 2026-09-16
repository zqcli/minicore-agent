# 0916 Shared Data Cleanup Closeout Acceptance

This is the current acceptance record for the 0916 implementation spec
(`minicore-agent-0916-implementation-spec.md`). It replaces the phase-by-phase
progress notes as the live evidence; the older 0914 documents are kept as
historical material and marked superseded where they describe behavior this
closeout removed.

The parent remote run is the acceptance. This file records what was actually
run and what was not; it is not itself a gate, and a local pass is not claimed.

## Source, Runtime And Commits

- Branch: `fix/0914-shared-data-closeout`.
- Start HEAD: `6e6732e9c7c800157923b842c6964abaec30a4a9` (P6b, after the P0–P6b
  work packages).
- Final HEAD: the commit titled
  `test(docs): close shared data cleanup acceptance` (this record). The three
  code/document commits below precede it.
- Runtime: unchanged at `0.4.1`, Git revision
  `6cd2bdbc634437dea925495c61c7eb0be10ba171` (`Cargo.toml` and `Cargo.lock`).
- No new dependency. `Cargo.lock` was not edited by hand.

Commits from the reviewed base `58b011f` through P6b were already present. The
P7 additions are new, independent commits (no amend, no rebase):

| Commit | Scope |
|---|---|
| `5e547c7` | Record and assert installed Loop options in the F2 regressions |
| `78a3e6a` | F1/F2/I1/I2/S1/E2E-A/E2E-B gap tests in `src/agent/tests.rs` |
| `08dabcd` | Process-level E2E-C (`tests/legacy_closeout.rs`) |

A plain `cargo check --target x86_64-apple-darwin` cannot build the `ring` C
sources on this Linux host without an Apple toolchain; the wrapper above is a
zig-based `cc`/linker, so the macOS row is a genuine cross-compile, not a
native run.

## Final Gates (remote, isolated copy only)

All commands ran on `root@192.168.20.199` in the isolated copy
`/root/minicore-agent-0914/src`; logs are under `/root/minicore-agent-0914/logs`.
Local `cargo` was never run.

| Gate | Command | Result |
|---|---|---|
| Format | `cargo fmt --all -- --check` | pass (`p7-fmt.log`) |
| Check | `cargo check --locked --all-targets` | pass (`p7-check.log`) |
| Clippy | `cargo clippy --locked --all-targets -- -D warnings` | pass (`p7-clippy.log`) |
| Rustdoc | `RUSTDOCFLAGS=-D warnings cargo doc --locked --no-deps` | pass (`p7-doc.log`) |
| Tests (stable) | `cargo test --locked --all-targets` | pass (`p7-tests.log`) |
| Tests (MSRV) | `cargo +1.85.0 test --locked --all-targets` | pass (`p7-msrv.log`) |
| Windows cross | `cargo check --locked --all-targets --target x86_64-pc-windows-gnu` | pass (`p7-windows.log`) |
| macOS cross | `cargo check --locked --all-targets --target x86_64-apple-darwin` with `CC_x86_64_apple_darwin` / `CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER` pointing at the remote `darwin-cc` zig wrapper | pass (`p7-macos.log`) |

### Test counts

The suite is split into the library target and the integration crates. Counts
are the real `test result:` lines, not a sum of test names:

- Library: **715 passed / 0 failed / 2 ignored**.
- Integration crates: 28 passed / 0 failed / 0 ignored
  (`openai_rpc_process` 6, `rpc_soak` 2, `rpc_stdio` 5, `shared_data_clients` 1,
  `legacy_closeout` 1, `skeleton` 3, `startup_errors` 4, `tui_rpc_flow` 6).
- Total: **743 passed / 0 failed / 2 ignored**.

P6b ran 706 library plus 27 integration (733 passed / 2 ignored). P7 adds nine
library tests plus one integration test (10 total); the two ignored tests are
the unchanged real-Provider Live tests. Counting limits: the "passed" numbers
are per-binary result lines, so a filtered or ignored test is not counted as a
pass, and this record does not claim platform-native execution from a
cross-compile.

## Not Run

- Native Windows and native macOS test execution. The two cross-target checks
  are compile-only (`cargo check`), which is not native execution.
- The two real-Provider Live tests remain `#[ignore]`; no paid model was called
  and no test credentials were required.
- No Git push, tag, install, or hosted-CI run.
- No change to the real user `data_dir`, `config`, Store, or history JSONL.

## F1: Runtime Spawn And Observation Identity

Implemented in `461bb4d` (split `reset_before_loop_start` from
`bind_started_loop`, reset before `AgentLoop::start`). P7 adds the missing
native-tool identity proof and re-runs the P1 Bash regression:

- `f1_native_write_changes_keep_the_first_request_identity` forces the first
  `Model::start` to race the start-to-bind window and then reads the write's
  `ToolRef` through `changes.list` and the before/after sides through
  `changes.diff`.
- `i2_06_first_request_observation_survives_runtime_start_binding` remains the
  Bash counterpart.

Negative control (remote isolated copy only): the reset was moved back after
`AgentLoop::start` to reproduce the old ordering. Both
`f1_native_write_changes_keep_the_first_request_identity` and
`i2_06_first_request_observation_survives_runtime_start_binding` failed (no
change record; Bash startup timed out). The good source was restored and both
passed; the remote `sessions.rs` SHA-256 matches the local one
(`e7d313ec…`). The bad variant was never committed.

## F2: Future LoopOptions

Implemented in `f039baa`. P7 strengthens the regressions and adds the
automatic-compaction budget case:

- `f2_02` now asserts the running loop's install-time `prompt_timeout` (via the
  test-only `installed_loop_options_for_test`) stays at the old model's value
  while the future options already carry the candidate's.
- `f2_03` now asserts the reopened loop derives exactly the options the update
  installed, so a future Turn and a reopen cannot drift.
- `f2_06_sealed_update_installs_the_new_model_context_budget` enables automatic
  compaction and proves a sealed update (`active_revision = None`) installs the
  candidate model's window/trigger/target budget, with two distinct model
  windows.

## I1: Git Out Of The Execution Path

Implemented in `f3d955a`. The Git call sites and branch cache semantics:

- Removed: `forward_loop_event_and_refresh` (tool-batch refresh),
  `run_active_loop`'s `refresh_branch().await`, and `build_presentation`'s
  Workspace read. `Workspace::git_branch` is no longer called by the Agent's
  automatic path.
- Remaining explicit query: `workspace.status` (and its status query worker) is
  the only Git entry. `Session::complete_status_query` projects one complete,
  available result onto the compatibility `session.presentation.git_branch`
  cache; an unavailable/incomplete/cancelled/closed observation clears or skips
  it. The branch starts `None` and only explicit status updates it.

P7 adds `i1_status_stub_proves_no_implicit_git_during_session_and_turns`: a
counting Git stub leaves no marker across create, open, a no-tool Turn, and a
tool-bearing Turn.

## S1: Legacy Stateless Subagent Removed

Implemented in `a6c4a3c` (module `src/subagents.rs` deleted; five-tool builder;
read-only history name compatibility). Entry behavior:

- Current TOML Profile naming `subagent`: load/reload fails with
  `invalid_profile`; a failed reload keeps the old catalog.
- New Session: five executable tools only.
- Old six-tool SessionRecord: `session.list`/`session.read`/`session.rename`
  work; `session.open` returns `invalid_session_settings` before any model call
  or Store repair; history/summary/auxiliary bytes are unchanged.
- History that called `subagent` under a saved five-tool list: readable and
  executable.
- Hallucinated `subagent`: the Runtime's normal unknown-tool handling; no child
  loop or process is dispatched.

P7 adds three unit tests (`s1_legacy_history_and_aux_are_readable_through_public_queries`,
`s1_five_tool_subset_record_with_old_history_still_executes`,
`s1_hallucinated_subagent_is_an_unavailable_tool_not_a_dispatch`) and the
process-level `legacy_six_tool_session_is_readable_but_never_executed`. The
E2E-C fixture bytes were generated by the real Store/History types on the
remote copy and embedded; the emitter was never committed.

## I2: Session Owns Execution Data

Implemented in `b4ae849`. Ownership/reference direction:

- `SessionShared` owns `Arc<ToolData>`, `Arc<CommandOwners>`, and
  `Arc<ToolObserver>`; `ToolObserver` holds only the SessionId, a `ToolData`
  reference, the event sink, and the request-key slot.
- `CommandOwners → worker → CommandStreamSink` where the sink is the same
  `ToolObserver`; the sink never holds `CommandOwners`, a Session, or the Agent.
  There is no `registry → sink → registry/Session` cycle.
- Join authority: the Turn joins that loop's command owners; Session close and
  Agent shutdown join all. Bash no longer resolves its binding through
  `presentation().command_owners()`.

P7 adds `i2_real_command_releases_owners_and_observation_resources`: after a
real, cancelled Bash command the owners are joined, and `Weak` handles to
`ToolData`/`CommandOwners`/`ToolObserver` fail after close/shutdown. Cleanup
runs unconditionally so a failed assertion cannot leak a process.

## E2E Workflows (Spec §12)

- **E2E-A** `e2e_a_multi_turn_hot_switch_tool_data_workflow`: create → first
  request's invocation is read before the tool runs → live Bash stdout/stderr →
  same-loop model update (running options frozen, future options updated) →
  wait/turn.result/changes.diff → next-Turn options and model → close/open
  consistency.
- **E2E-B** `e2e_b_lost_events_failed_append_and_clean_close_workflow`: a
  capacity-1 event queue plus an injected JSONL append failure → `turn.wait`
  `persistence=failed` → a rejected new send keeps the prior result →
  `turn.result` is live → `tool.read`/`tool.output` still serve Bash → close and
  shutdown reclaim the command.
- **E2E-C** `legacy_six_tool_session_is_readable_but_never_executed`:
  list/read, `turn.result`, and auxiliary `tool.read`/`tool.output` succeed;
  `session.open` is refused; bytes are unchanged; a fresh five-tool Session
  runs.

## Change Statistics (production vs tests vs moves)

These are line counts from the reviewed base through P7, separated so a large
move is not presented as complexity change. Counts come from `git show
--numstat`; they are line churn, not semantic complexity, and a move appears as
equal additions/deletions.

| Stage | Production added/removed | Test added/removed | Notes |
|---|---|---|---|
| P1 `461bb4d` | +99 / −9 | +309 / −2 | F1 fix plus regressions |
| P2 `f039baa` | +12 / −2 | +396 / −8 | F2 fix plus regressions |
| P3 `f3d955a` | +56 / −209 | +285 / −0 | Git call sites removed |
| P4 `a6c4a3c` | +232 / −2539 | +168 / −516 | Subagent deletion, read compat |
| P5 `b4ae849` | +671 / −306 | +108 / −33 | new `tools/observe.rs`, Presentation slimmed |
| P6a `e246693` | +4 / −7554 | +7523 / −0 | pure test move out of production files |
| P6b `6e6732e` | +4353 / −4303 | +0 / −0 | pure production split/move |
| P7 (`5e547c7`,`78a3e6a`,`08dabcd`) | +26 / −0 | +1517 / −110 | test-only except the test observation hook |

Aggregate over `58b011f..HEAD`: 32 files, +18,372 / −18,204. Interpretation
limit: P6a/P6b are moves, so their additions and deletions approximately cancel
and must not be read as net growth.

## Retained Limits

- No executable `subagent` Tool. Full delegated execution (a future
  `SubagentTool` driving a complete `minicore-agent` Session over its normal
  API) is separate future work.
- The presentation branch cache does not refresh itself; clients call
  `workspace.status` for fresh state.
- Legacy wire compatibility fields remain.
- Auxiliary records are retained within the existing budget; eviction keeps
  explicit offsets/availability.

## Superseded Historical Documents

- `docs/0914-progress.md`: restored the original wording P4 had edited and added
  a pointer that the legacy stateless subagent was removed by this closeout.
- `docs/compaction-plan.md`: restored the original closing paragraph and added
  the same superseded pointer.
- `docs/verification/followups.md`: marked historical; the old subagent evidence
  and checksums are preserved unaltered and are not a current-behavior claim.

Past evidence was not rewritten; only a forward pointer was added.
