# 0916 Shared Data Cleanup Closeout Acceptance

This is the current acceptance record for the 0916 implementation spec
(`minicore-agent-0916-implementation-spec.md`). It replaces the phase-by-phase
progress notes as the live evidence; the older 0914 documents are kept as
historical material and marked superseded where they describe behavior this
closeout removed.

This file records what the implementing agent actually ran and what it did not;
it is not itself a gate. The parent-agent review separately inspected the source
and independently ran only the P5 gates (see "Parent Review Sources" below); the
implementing agent ran the P0–P7 gates listed here on the isolated remote copy.

## Source, Runtime And Commits

- Branch: `fix/0914-shared-data-closeout`.
- Branch start (reviewed base): `58b011fd0752820a4917ec9ccf12d7846daa0caf`
  (`feat/0914-shared-data`).
- P7 start: `6e6732e9c7c800157923b842c6964abaec30a4a9` (P6b, completed by the
  prior subagent before the final parent review).
- Final HEAD: the parent-review fix commits on top of `c1e6c9c`
  (`test(docs): close shared data cleanup acceptance`). The first round is
  `54d5fae`; the follow-up fix is a separate new commit.
- Runtime: unchanged at `0.4.1`, Git revision
  `6cd2bdbc634437dea925495c61c7eb0be10ba171` (`Cargo.toml` and `Cargo.lock`).
- No new dependency. `Cargo.lock` was not edited by hand.

The P1–P6b work packages were already committed from the reviewed base
`58b011f` to `6e6732e`, then the P7 additions were committed on top. Each fix
commit for the parent review is a new, independent commit (no amend, no rebase).

Full P1–P6b SHAs:

| Stage | Full SHA |
|---|---|
| P1 | `461bb4d6b338720686e7463db4c9e14c9fde37a0` |
| P2 | `f039baaa9691a537ae33734bdd607e7e0d7f5d6a` |
| P3 | `f3d955aa93b995e50b5a7558a560919c2efbcb6c` |
| P4 | `a6c4a3c19ad7d3ec186f0cedb1cdf74c19319406` |
| P5 | `b4ae849849d0db2b5fb4970d1aa53d346ba53b5e` |
| P6a | `e246693a23c5a570741a67a8a117f6893c1461a0` |
| P6b | `6e6732e9c7c800157923b842c6964abaec30a4a9` |

| Commit | Scope |
|---|---|
| `461bb4d` | P1: split `reset_before_loop_start` from `bind_started_loop` (F1) |
| `f039baa` | P2: install future `LoopOptions` on model updates (F2) |
| `f3d955a` | P3: remove Git lookups from execution/completion (I1) |
| `a6c4a3c` | P4: remove the legacy stateless subagent execution (S1) |
| `b4ae849` | P5: Session owns `ToolData`/`CommandOwners`; `ToolObserver` (I2) |
| `e246693` | P6a: pure move of large inline test suites (I3) |
| `6e6732e` | P6b: pure split of `sessions`/`store` responsibilities (I3) |
| `5e547c7` | Record and assert installed Loop options in the F2 regressions |
| `78a3e6a` | F1/F2/I1/I2/S1/E2E-A/E2E-B gap tests in `src/agent/tests.rs` |
| `08dabcd` | Process-level E2E-C (`tests/legacy_closeout.rs`) |
| `c1e6c9c` | P7 closeout acceptance document |

A plain `cargo check --target x86_64-apple-darwin` cannot build the `ring` C
sources on this Linux host without an Apple toolchain; the macOS row below uses
a zig-based `cc`/linker wrapper, so it is a genuine cross-compile, not a native
run.

## Parent Review Sources

The parent agent reviewed this branch after `c1e6c9c`. Its review log and the
source diff are the acceptance basis for the parent-review fix commits; the
parent agent independently ran only the P5 gates, not P0–P7. This record does
not claim the parent ran the P1–P7 suites. Two parent-review rounds were
addressed: `54d5fae` and the follow-up commit that drops the test-only process
scan.

## Final Gates (remote, isolated copy only)

All commands ran on `root@192.168.20.199` in the isolated copy
`/root/minicore-agent-0914/src`; logs are under `/root/minicore-agent-0914/logs`.
Local `cargo` was never run. The table lists the final parent-review run
(`p7fix2-*` logs) on the committed tree; the first parent-review round produced
the earlier `p7fix-*` logs.

| Gate | Command | Result |
|---|---|---|
| Format | `cargo fmt --all -- --check` | pass (`p7fix2-fmt.log`) |
| Check | `cargo check --locked --all-targets` | pass (`p7fix2-check.log`) |
| Clippy | `cargo clippy --locked --all-targets -- -D warnings` | pass (`p7fix2-clippy.log`) |
| Rustdoc | `RUSTDOCFLAGS=-D warnings cargo doc --locked --no-deps` | pass (`p7fix2-doc.log`) |
| Tests (stable) | `cargo test --locked --all-targets` | pass (`p7fix2-tests.log`) |
| Tests (MSRV) | `cargo +1.85.0 test --locked --all-targets` | pass (`p7fix2-msrv.log`) |
| Windows cross | `cargo check --locked --all-targets --target x86_64-pc-windows-gnu` | pass (`p7fix2-windows.log`) |
| macOS cross | `cargo check --locked --all-targets --target x86_64-apple-darwin` with `CC_x86_64_apple_darwin` / `CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER` pointing at the remote `darwin-cc` zig wrapper and `/root/.local/bin` on `PATH` | pass (`p7fix2-macos.log`) |

The targeted I2 regression passed on its own (`p7fix2-i2.log`). An earlier first
MSRV run (`p7fix-msrv.log`) failed on the pre-existing, unrelated
`workspace::status::diff_tests::workspace_pagination_refreshes_versions_and_rejects_changed_content`
pagination test (`diff_tests.rs` is untouched by this branch). It passes on
rerun on both stable and 1.85.0, so it is recorded as a flaky test, not an MSRV
regression; the full MSRV suite passes.

### Test counts

The suite is split into the library target and the integration crates. Counts
are the real `test result:` lines, not a sum of test names:

- Library: **715 passed / 0 failed / 2 ignored**.
- Integration crates: 28 passed / 0 failed / 0 ignored
  (`openai_rpc_process` 6, `rpc_soak` 2, `rpc_stdio` 5, `shared_data_clients` 1,
  `legacy_closeout` 1, `skeleton` 3, `startup_errors` 4, `tui_rpc_flow` 6).
- Total: **743 passed / 0 failed / 2 ignored**.

The two ignored tests are the unchanged real-Provider Live tests. Counting
limits: the "passed" numbers are per-binary result lines, so a filtered or
ignored test is not counted as a pass, and this record does not claim
platform-native execution from a cross-compile.

## Not Run

- Native Windows and native macOS test execution. The two cross-target checks
  are compile-only (`cargo check`), which is not native execution.
- The two real-Provider Live tests remain `#[ignore]`; no paid model was called
  and no test credentials were required.
- No Git push, tag, install, or hosted-CI run.
- No explicit MSRV *cross*-target run; MSRV is the native stable-host suite only.
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
- `f2_04_invalid_and_failed_updates_leave_future_state_unchanged` now selects a
  valid candidate (`other` with explicit `reasoning = disabled`, its only
  supported mode) before injecting `fail_next_record_write`, and asserts
  `AgentError::Store`. This proves the failure occurred in the record write, not
  in an early settings rejection; the unchanged record/options/generation/budget
  assertions are retained.

## I1: Git Out Of The Execution Path

Implemented in `f3d955a`. The Git call sites and branch cache semantics:

- Removed: `forward_loop_event_and_refresh` (tool-batch refresh),
  `run_active_loop`'s `refresh_branch().await`, and `build_presentation`'s
  Workspace read. `Workspace::git_branch` is no longer called by the Agent's
  automatic path.
- Remaining explicit queries: `workspace.status` and `changes.diff` for a
  `workspace:` change reference. Both use the existing status query worker /
  Git runner. `Session::complete_status_query` projects one complete, available
  status result onto the compatibility `session.presentation.git_branch` cache;
  an unavailable/incomplete/cancelled/closed observation clears or skips it.
  The branch starts `None` and only an explicit status updates it.

P7 adds `i1_status_stub_proves_no_implicit_git_during_session_and_turns`: a
counting Git stub leaves no marker across create, open, a no-tool Turn, and a
tool-bearing Turn. The same test now performs an explicit `workspace_status`
call and asserts the marker *appears*, proving the stub actually observes Git
invocations (a positive control against a stub that never runs).

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
E2E-C fixture record is now built with `serde_json` `Value` for the
workspace/session fields and carries the real six tools
(`read`, `write`, `edit`, `apply_patch`, `bash`, `subagent`), not the earlier
two-tool placeholder. A fresh five-tool Session additionally asserts that the
provider request offers exactly the five executable tools and never `subagent`.
The fixture bytes
were generated by the real Store/History types on the remote copy; the emitter
was never committed.

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

P7 adds `i2_real_command_releases_owners_and_observation_resources`. The Turn is
cancelled and awaited; the reclamation evidence (a `Cancelled` command with
`termination_confirmed`, an empty `ps` listing for the child, and
`command_owners().active() == 0`) is captured *before* the unconditional
`close_session`, so the close cannot turn a failed assertion into a pass. The
post-startup body runs under `catch_unwind`; the close runs on both the success
and panic paths and is the only cleanup mechanism: it joins the Session's
command owners (via `join_all`) and then releases the
`Weak<ToolData>`/`Weak<CommandOwners>`/`Weak<ToolObserver>` handles. The "no pid"
early-exit path likewise closes the Session to let its owners join and revert
the command before it panics. The test introduces no `kill`, signal, or
process-table scan; it relies solely on the production join boundary, so a
failed assertion can never be hidden by an out-of-band reap.

On the negative-control boundary: removing `join_loop` from `run_active_loop`
was tried on the remote copy, but that removal does **not** reliably fail this
test. When the Runtime drops the cancelled tool future, the running-command
guard cancels the underlying worker, so the command usually finishes and is
reaped on its own before `turn.wait` returns; `join_loop` is the deterministic
barrier for the dropped-future case, not the only thing that stops the command.
The single failure observed while developing this was a lost race, not a
reproducible detection, so it is not cited as proof. The restored `sessions.rs`
SHA-256 matches the local one (`e7d313ec…`).

## E2E Workflows (Spec §12)

- **E2E-A** `e2e_a_multi_turn_hot_switch_tool_data_workflow`: create → the first
  request's `ToolInvocation` is read during that request, before the Turn
  finishes and while the gated Bash command is still running → Bash stdout and
  stderr are awaited on the query path until both reach their exact bytes (a pid
  file alone does not prove the drainers committed) → same-loop model update
  (running options frozen, future options updated) → wait/turn.result/
  changes.diff → next-Turn options and model → close/open consistency. Every
  failure after the command started still closes the Session before rethrowing.
- **E2E-B** `e2e_b_lost_events_failed_append_and_clean_close_workflow`: a
  capacity-1 event queue plus an injected JSONL append failure → `turn.wait`
  `persistence=failed` → a rejected new send keeps the prior result →
  `turn.result` is live → `tool.read`/`tool.output` still serve the Bash stdout
  and stderr → close still shuts the Session down. The Bash command is a short
  normal command, not a fixed long sleep, and the test does not start a status
  query, so it makes **no** claim about reclaiming never-started query workers.
- **E2E-C** `legacy_six_tool_session_is_readable_but_never_executed`:
  list/read, `turn.result`, and auxiliary `tool.read`/`tool.output` succeed;
  `session.open` is refused; bytes are unchanged; a fresh five-tool Session runs
  and its provider request offers only the configured tools.

## Change Statistics (production vs tests vs moves)

Line counts from the reviewed base through P7. These are a **heuristic split,
not a precise semantic classification**: a script attributes each changed line
by whether it sits inside a top-level `#[cfg(test)]` item or a test file.
Counts themselves come from `git show --numstat` and are exact line churn, not
semantic complexity; a move appears as near-equal additions/deletions.
"logic" excludes lines inside `#[cfg(test)]` items and test files; "test
support" is `#[cfg(test)]` code that stays in production files (injection hooks
and gates); "test files" is `src/**/tests.rs` and `tests/**`.

| Stage | Logic +/− | Test support +/− | Test files +/− | Docs +/− |
|---|---|---|---|---|
| P1 `461bb4d` | +37 / −6 | +62 / −3 | +309 / −2 | 0 / 0 |
| P2 `f039baa` | +8 / −2 | +4 / −0 | +396 / −8 | 0 / 0 |
| P3 `f3d955a` | +50 / −113 | 0 / −93 | +285 / −0 | +6 / −3 |
| P4 `a6c4a3c` | +42 / −1483 | +162 / −946 | +168 / −516 | +28 / −110 |
| P5 `b4ae849` | +662 / −304 | +9 / −2 | +108 / −33 | 0 / 0 |
| P6a `e246693` (move) | 0 / 0 | +4 / −7554 | +7523 / −0 | 0 / 0 |
| P6b `6e6732e` (move) | +4309 / −4260 | +44 / −43 | 0 / 0 | 0 / 0 |
| P7 pre-review (`5e547c7`,`78a3e6a`,`08dabcd`) | 0 / 0 | +26 / −0 | +1517 / −110 | 0 / 0 |
| Parent-review fixes (`c1e6c9c`..final, test/docs only) | 0 / 0 | 0 / 0 | +509 / −356 | +169 / −67 |

Interpretation limits:

- P6a/P6b are moves; their additions and deletions approximately cancel and
  must not be read as net growth. P6b's "logic" figures are the same functions
  moving between files, not new behavior.
- P6a's only in-file additions are the four `#[cfg(test)] mod tests;`
  declarations after the inline suites moved out; the two removed top-level
  test-only `use` lines are counted as test support, not production logic.
- P4's logic deletions are the removed stateless-subagent module; the test-file
  deletions are its acceptance tests, not coverage of retained behavior.
- P7 adds no non-test production logic: its whole production diff is
  `#[cfg(test)]` support (the F2 install-time observation hook plus its doc
  comment), so it is classified entirely as test support.
- This is raw churn plus a line-level `cfg(test)` heuristic; the intended
  semantic change is the F1/F2 fixes, the I1 removal, and the I2 ownership move,
  all described above.

Aggregate over `58b011f..HEAD` (before the parent-review fix): 34 files,
+18,664 / −18,213, dominated by the P6a/P6b moves.

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
