# 0917 Nonbreaking Simplification — P7 Audit

**Review status:** parent-reviewed source `f95a319` passed fresh remote stable/MSRV
full suites (756 passed, 0 failed, 2 existing ignored), fmt, check, strict Clippy,
rustdoc, and Windows compile-only check. macOS cross-check is blocked in `ring`'s C
build; native Windows/macOS and hosted CI were not run. Review found no compatibility
regression in `66f1834..f95a319`. The supplied Spec remains untracked and unmodified.

## Scope And Provenance

| Item | Value |
|---|---|
| Repository / branch | `zqcli/minicore-agent` / `refactor/0917-nonbreaking-simplify` |
| Reviewed base | `66f183470bd9c5bdb359b1c7666381135898861f` |
| Source tip | `f95a319c413bfc3f0311a0448117dfd14d2b2b54` |
| Runtime | `minicore-runtime 0.4.1`, revision `6cd2bdbc634437dea925495c61c7eb0be10ba171` |
| Agent version / edition | `0.3.3` / Rust 2024, MSRV 1.85 |
| Dependencies | unchanged; `futures-util` was already present |
| Changed source files | 11 Rust files, plus this verification document |
| `src/sessions.rs` | unchanged |
| Parent remote source | `/root/minicore-agent-0917/source` |
| Parent logs | `/root/minicore-agent-0917/logs` |
| Parent tools | rustc/cargo `1.97.1`; MSRV toolchain `1.85` |
| Local Cargo execution | intentionally not run |

The reviewed source range is the ten commits from P0 through P7:
`ed582a9`, `fdbc4ea`, `c412906`, `a66b52a`, `d287832`, `abd38ee`, `08d96f5`,
`6be1a26`, `911fdc7`, and `f95a319`. Before the final run, old inactive Agent targets
totaling about 10 GiB were removed without touching Runtime. The pre-existing round target
(about 4.7 GiB) was removed before the independent `target-final` run. After the P7
test commit, both `target-final` and `target-msrv` were removed and rebuilt from scratch.
No Runtime/source/user data was removed. Logs are also copied locally to
`/tmp/minicore-agent-0917-evidence`, outside the repository.

## Compatibility Contract

### Rust API and module surface

Parent review programmatically compared all `Agent` public signatures and public
reexports with the base: both inventories are identical. `src/lib.rs` adds only private
`mod queries;` at line 17; its public `pub use` groups/items remain unchanged, including
legacy `session.history` and `session.presentation` types. The groups are `agent`,
`changes`, `compaction`, `config`, `diff`, `error`, `event`, `ids`, `models`,
`presentation`, `profiles`, `rpc`, `sessions`, `tool_data`, and `workspace`. The
`impl Agent` retains 37 public methods and `agent_version` remains public.

### RPC methods

Parent compared the `canonical_method` inventory and dispatch with the base; both still
cover exactly these 33 methods. Unknown methods still return the same `method_not_found`
object:

```text
agent.ping, agent.reload, agent.shutdown
profile.list, model.list
session.list, session.create, session.open, session.close, session.delete
session.state, session.context, session.compact, session.compact.cancel
session.update, session.rename, session.history, session.read, session.presentation
workspace.read, workspace.files, workspace.search, workspace.status
changes.list, changes.diff
tool.read, tool.output
turn.send, turn.steer, turn.cancel, turn.wait, turn.result
interaction.answer
```

`src/rpc/protocol.rs` is unchanged. Parameter DTOs, response DTOs, request IDs, error
`code`/`kind`/`retryable`, null/omission rules, and JSON-RPC framing are therefore
outside the refactor's change surface.

### Events, storage, and runtime

`src/event.rs`, `AgentEvent`, `EventMeta`, and all event payload variants are unchanged:
`SessionOpened`, `SessionClosed`, `SessionState`, `TurnStarted`, `RequestStarted`,
`RequestUsage`, `OutputDelta`, `ToolStarted`, `ToolInvocation`, `ToolExecution`,
`ToolProcess`, `ToolPresentation`, `ToolProgress`, `ToolFinished`, `SteerProgress`,
`InteractionRequested`, `InteractionResolved`, and `TurnFinished`.

`Cargo.toml`, `Cargo.lock`, the Runtime checkout, Session storage schema, JSONL history,
auxiliary-tool file names, `src/sessions.rs`, and CI files are unchanged. No old
Presentation/history path was removed, no subagent executor was restored, and no new
background owner or global Agent lock was introduced. Parent diff review found no
changes to `src/event.rs`, `src/rpc/protocol.rs`, dependencies, Session production
code, or CI.

## Implemented Work Packages

### P1 — RPC `Result` boundary (`fdbc4ea`)

`RpcServer::dispatch` now delegates to private `dispatch_inner -> Result<Dispatch,
RpcResponse>` (`src/rpc/server.rs:227-234`). Parameter decoding and early business
errors use `?`; the outer method still converts those responses to `Dispatch::Response`.
`canonical_method` logging remains before the match. The change is transport plumbing
only; turn admission, query capacity, timeout placement, and error mapping stay in the
branches.


### P2a/P2b — Shared query orchestration (`c412906`, `a66b52a`)

The new private `src/queries.rs` contains 207 physical lines and three concrete
preparation functions:

- `prepare_workspace_status` (`queries.rs:41`) registers the Session-owned worker
  synchronously and returns a wait-only future that performs the compatibility branch
  cache completion.
- `prepare_changes_list` (`queries.rs:68`) retains Workspace versus Session/Turn
  source selection. Workspace scope requires a loaded Session; cold tool changes still
  allow an unloaded Session.
- `prepare_changes_diff` (`queries.rs:156`) retains the `workspace:` source worker and
  the cold ToolRef path. Workspace diff keeps `tokio::time::sleep_until(deadline.into())`;
  the other branches retain their existing `Instant::from_std` deadline conversion.

The callers retain validation, admission, error precedence, cancellation, absolute
deadline, and response ownership. Registration occurs before the returned future can be
polled, so closing a Session after `prepare_*` still joins the worker. The future is not
a new owner; Session or Store remains the join authority. No QueryManager, registry,
trait, or new scheduler was introduced.


### P2c — Narrow deferred-response helper (`d287832`)

`RpcServer::defer_query` (`src/rpc/server.rs:793`) only inserts an already-prepared
future into the existing `queries` JoinSet and maps `Ok` with `success` or `Err` with
`query_error`. It adds no validation, timeout, retry, cancellation, admission, or
second JoinSet. Existing outer timeouts remain around `session.read`, `workspace.read`,
and `turn.result`; retained-worker `workspace.files/search` paths remain unwrapped.


### P3 — Live/Stored page assembly (`abd38ee`)

`live_turn_page` and `stored_turn_page` retain their source-specific cleaning and
metadata, then call `assemble_turn_page` (`src/read.rs:553-678`). The helper centralizes
cursor/range assembly, timestamps, JSON-envelope encoding, and the final page construction;
it invokes the existing `pack_items` path, whose UTF-8 boundary and encoded-byte-budget
behavior is unchanged. Live remains `availability=Live`, preserves its real persistence
and no `completed_at`; Stored remains `availability=Stored` with its stored metadata.
Pending results do not enter the helper.

The inline regression is `read::tests::live_and_stored_turn_pages_preserve_metadata_and_encoded_items`.
The initial P3 fixture budget of 256 was insufficient for the intended multi-page case
and was changed to 512; the paging algorithm was not changed.

### P4 — Auxiliary blob validation and writes (`08d96f5`)

`Store::commit_tool_record_locked` (`src/store/tool_records.rs:95-303`) now validates a
borrowed fixed array for exactly four streams: `input`, `result`, `stdout`, `stderr`.
`validate_stored_tool_record` was not changed. The six write entries are also a fixed,
borrowed order:

```text
input → result → stdout → stderr → before → after
```

The existing `aux_lock`, reserve/quota accounting, 3 MiB raw cap, one absolute deadline,
temp directory, `write_sync_file`, directory sync, rename, idempotency/inconsistent
handling, and cleanup path remain in place. `None` omits a file; `Some(&[])` is not
filtered by `is_empty`.

The empty-stream edge is deliberately unchanged: the stored validator represents a
zero-length stream hash as `None`, while commit validation hashes every `Some(bytes)`. Therefore
`Some(empty)` for any of the four validated blobs (`input`, `result`, `stdout`, or
`stderr`) remains `StoreError::Corrupt`. Empty `before`/`after` blobs remain legal because
they use the file-change snapshot validation path rather than the four-stream hash loop.
The illustrative Spec wording that treats every `Some(&[])` as a legal file is not a
baseline fact for these four streams; this refactor preserves the observed baseline
rejection, rather than changing that contract.


### P5 — Presentation wrapper (`6be1a26`)

`PresentationTool` is now a struct with shared `presentation` and `observer` fields and
private `ToolImpl` variants `Plain`, `Bash`, `Write`, `Edit`, and `ApplyPatch`
(`src/presentation.rs:702-860`). The synchronous `Tool::execute` boundary captures the
request key, `ToolRef`, display, begin, and invocation publication. One `Box::pin` async
dispatch selects the original concrete execution path, passes the captured `ToolRef`
and `ToolData` explicitly, then calls `finish_presentation_tool` exactly once.

The refactor does not use `Any`/downcasting, does not re-read mutable observer state
inside the future, does not turn Plain into Bash, and does not alter cancellation,
`RequestInput`, native-file binding, Bash command ownership, raw result retention, or
runtime-drop terminal behavior.

The focused `presentation_tool_variants_share_binding_and_finish` test creates each
future without polling, changes the observer's current request, and proves invocation
and finish still use the old identity. Existing F1/Bash/native/ToolDisplay coverage is
listed in the focused and V-index tables.

### P6 — Create/open Session setup (`911fdc7`)

Private `SessionSetup` (`src/agent.rs:279`) carries the shared Presentation, ToolData,
CommandOwners, ToolObserver, ExecutionConfig, AutoContext, LoopOptions, and
CompactionState. `prepare_session_setup` (`agent.rs:1259`) builds those resources;
`install_session` (`agent.rs:1296`) performs `Session::new`, Sessions-map insertion,
and the single `SessionOpened` publication.

The callers intentionally retain their distinct I/O and validation order:

- Create: resolve settings/profile → open and canonicalize Workspace → build record →
  `loop_options_for_model` → `CompactionState::new` → setup preflight → Store create →
  install → created log.
- Open: already-loaded fast path → load record → removed-tool check → load stored
  session → second removed-tool check → open/check Workspace identity →
  `loop_options_for_model` → `load_state` → setup → install → opened log.

Thus options remain in the caller before create's `CompactionState::new` and before
open's `load_state`. `update` and `reload` retain the original `ExecutionConfigFactory`
path and do not call this helper. The duplicate-known-tool precheck exercises the real
`build_tools_with_presentation` failure and proves Store creation is not reached.

### P7 — Cold ToolRef diff cancellation regression (`f95a319`)

`store::tests::cold_changes_diff_caller_cancel_keeps_store_owned_worker` commits a
real before/after snapshot, runs `prepare_changes_diff(..., None, ...)`, waits for the
existing `DiffGate` to admit the CPU worker, cancels only the caller, and verifies the
Store owner remains registered until explicit release and shutdown/join. This is test
coverage only: no production hook or behavior changed.

## Closeout Workflows

| Workflow | Real test and correspondence |
|---|---|
| E2E-A: query/execution parallelism | `agent::tests::e2e_a_multi_turn_hot_switch_tool_data_workflow`: create, observe the first ToolInvocation while Bash is running, read stdout/stderr through ToolData, update the model in the same loop, verify future options, read `turn.result`/changes, run the next Turn, close/open, and verify history/options. |
| E2E-B: lost events and failed append | `agent::tests::e2e_b_lost_events_failed_append_and_clean_close_workflow`: capacity-1 event stream plus injected JSONL append failure, failed persistence, rejected follow-up send preserving the old result, live `turn.result`, `tool.read`/`tool.output`, and clean close. It does not claim a never-started status worker was reclaimed. |
| E2E-C: legacy compatibility | `tests/legacy_closeout.rs::legacy_six_tool_session_is_readable_but_never_executed`: authentic six-tool record/history/auxiliary bytes are listable and readable, `session.open` refuses before execution or repair, bytes remain unchanged, and a fresh five-tool session exposes only executable tools. Unit companions are `s1_legacy_history_and_aux_are_readable_through_public_queries`, `s1_five_tool_subset_record_with_old_history_still_executes`, and `s1_hallucinated_subagent_is_an_unavailable_tool_not_a_dispatch`. |

## Focused Stage Tests

The refactor added 13 regression tests: P1 (1), P2a (1), P2b (2), P3 (1), P4 (4),
P5 (1), P6 (2), and P7 (1). Each stage also passed remote fmt/check. Commands below
use `cargo test --locked --lib <filter>`; counts overlap across filters.

| Stage | Actual test filters | Passed |
|---|---|---:|
| P1 | `rpc::server::tests` | 79 |
| P2a | `status` | 51 |
| P2b | `changes`; `prepared_`; `rpc::server::tests` | 22; 2; 80 |
| P2c | `rpc::server::tests` | 80 |
| P3 | `read::`; `turn_result` | 13; 6 |
| P4 | `store::` | 73 |
| P5 | `presentation`; `tools::`; `f1_`; `i2_` | 20; 103; 1; 7 |
| P6 | `agent::tests` | 143 |
| P7 | `cold_changes_diff_caller_cancel_keeps_store_owned_worker` | 1 |

## V01–V40 Evidence Index

The IDs below are grouped where one source test proves several adjacent invariants.
Static entries mean the corresponding source/diff audit is part of the evidence; they do
not claim a runtime gate pass.

| IDs | Evidence |
|---|---|
| V01–V03 | Static compatibility diff; public exports in `src/lib.rs` unchanged despite private `mod queries`; unchanged `event.rs`, `rpc/protocol.rs`, Runtime revision; `dispatch_inner_propagates_the_full_error_object`. |
| V04–V07 | `fragmented_frame_survives_deferred_waiter_interleaving`, `read_frame_limit_covers_cumulative_length_after_cancellation`, `changes_capacity_precedes_missing_session_and_starts_no_worker`, `prepared_status_future_never_polled_is_still_reclaimed_by_close`, `prepared_workspace_diff_future_never_polled_is_still_reclaimed_by_close`. |
| V08–V11 | `workspace_status_answers_for_the_session_workspace`, `workspace_status_projects_the_shared_presentation_branch_and_clears_failures`, `changes_list_separates_workspace_session_and_turn_scopes`, `changes_diff_public_rpc_compares_native_before_and_after`, `changes_diff_public_rpc_pages_a_long_line_losslessly`. |
| V12–V15 | `cold_read_is_strictly_read_only_and_survives_missing_workspace_and_partial_history`, `bash_dual_stream_cold_read_closure_after_restart_without_session_loaded`, `cold_changes_diff_caller_cancel_keeps_store_owned_worker`, `pending_workspace_scans_do_not_block_ping_or_shutdown`, `changes_list_shutdown_cancels_a_waiting_query_within_deadline`, `deferred_query_capacity_is_four_with_shared_total_limit`. |
| V16–V18 | `turn_result_reports_pending_and_stored_availability`, `turn_result_can_continue_from_live_to_stored_items`, inline `live_and_stored_turn_pages_preserve_metadata_and_encoded_items`, `turn_result_reads_failed_live_report_without_history`. |
| V19–V22 | `auxiliary_blob_states_preserve_snapshot_files_and_metadata`, `empty_process_stream_blobs_remain_corrupt`, `invalid_metadata_ranges_and_hashes_rejected`, `auxiliary_mid_write_failure_removes_partial_commit`, `auxiliary_deadline_after_temp_creation_removes_partial_commit`, `idempotent_commit_with_corrupt_existing_fails`. |
| V23–V25 | `presentation_tool_variants_share_binding_and_finish`, `f1_native_write_changes_keep_the_first_request_identity`, `i2_02_tool_execution_event_matches_the_tool_read_query`, `tool_read_exposes_approval_time_data_without_running`, `i2_04_bash_owned_command_matches_events_queries_and_the_close_join`. |
| V26–V28 | `presentation_tool_variants_share_binding_and_finish`, `cancelling_wrapped_tool_preserves_runtime_outcome_and_recovery`, `a_turn_join_stops_and_reaps_a_command_whose_future_was_dropped`, `result_truncation_marks_display_and_bounds_hidden_rows`, `history_view_exposes_whitelisted_tool_detail_without_raw_arguments`. |
| V29–V32 | `create_capability_precheck_leaves_no_session_directory`, `create_and_loaded_open_emit_session_opened_once`, `reopen_rejects_missing_workspace_and_keeps_type_identity`, `legacy_six_tool_session_is_listable_renameable_but_not_openable`, `i2_01_session_reuses_observation_resources_across_updates_and_reload`, `f2_03_close_and_reopen_rederives_installed_future_options`, `invalid_update_does_not_write_or_change_memory`. |
| V33–V35 | `e2e_b_lost_events_failed_append_and_clean_close_workflow`, `full_agent_event_channel_keeps_wait_and_history_authoritative`, `automatic_request_compaction_rederives_after_a_smaller_model_update`, `auto_compaction_recovers_from_runtime_bytes_with_huge_tool_results`, `s1_legacy_history_and_aux_are_readable_through_public_queries`, `legacy_six_tool_session_is_readable_but_never_executed`. |
| V36–V37 | `redacted_debug_never_exposes_raw_text`, `tool_error_presentation_is_static_for_every_variant`, `i2_real_command_releases_owners_and_observation_resources`, `i2_05_session_observation_resources_are_released_without_cycles`, plus static ownership review of `queries.rs` and `PresentationTool`. |
| V38 | Fresh post-f95 `target-final` fmt/check/test/Clippy/rustdoc and fresh `target-msrv` tests PASS; see final logs below. |
| V39 | Windows GNU compile-only PASS, with the same five test-only warnings reproduced on the baseline. macOS cross-check blocked in `ring`; native Windows/macOS and hosted CI not run. |
| V40 | Parent-recounted unified-0 accounting includes `queries.rs` and separates tests/hooks/comments/blanks. |

## Test Hooks And Test Scope

P2 tests use the existing Unix status/change gates (`set_status_program`,
`StatusReapGate`, `ChangeListGate`) and add test-only orchestration tests in
`src/agent/tests.rs` and `src/rpc/server/tests.rs`; no production hook was added there.
P3 adds one inline test to `src/read.rs`; it does not move or truncate the rest of that
file's inline tests.

P4 adds only `#[cfg(test)]` support in `src/store/tool_records.rs`:
`AUX_BLOB_WRITE_FAILURES`, `AUX_TEMP_CREATION_GATES`,
`fail_aux_blob_write_after`, `register_aux_temp_creation_gate`,
`wait_aux_temp_creation_gate`, and the per-session write-failure check. The final cfg
ranges are source lines `4-62`, `259-261`, and `275-278`: 66 hook lines including
attached blank lines. The isolated temp-created gate avoids cross-test consumption;
the deadline test releases it with `tokio::time::sleep_until(deadline.into())`. Hooks
do not compile into production behavior or alter Store APIs.

P5 and P6 add no production test hooks. P5 uses channels and an unpolled future to prove
synchronous identity capture. P6 uses `futures_util::poll!(pin!(events.recv()))` for the
no-extra-`SessionOpened` assertion. P7 reuses the existing `auxiliary_snapshot`,
`DiffGate`, `gate_next_diff`, and Store worker counters; no production hook, framework, or
dependency was introduced.

## Physical Lines And Churn

Counts use physical lines, including comments and blank lines. The per-stage split is a
manual diff-hunk classification: standalone test files and inline `#[cfg(test)]` code
are separated from production implementation, but the numbers are raw churn, not
semantic complexity or a parser-derived net production LOC claim. Later stages can
rewrite lines added by earlier stages, so the per-stage rows are not additive to the
final range diff.

| Stage | Commit | Production +/− (net) | Tests/hooks +/− (net) | Docs +/− (net) |
|---|---|---:|---:|---:|
| P0 | `ed582a9` | 0 / 0 (0) | 0 / 0 (0) | 246 / 0 (+246) |
| P1 | `fdbc4ea` | 80 / 175 (-95) | 61 / 0 (+61) | 0 / 0 (0) |
| P2a | `c412906` | 66 / 21 (+45) | 86 / 0 (+86) | 0 / 0 (0) |
| P2b | `a66b52a` | 230 / 289 (-59) | 276 / 0 (+276) | 0 / 0 (0) |
| P2c | `d287832` | 73 / 110 (-37) | 0 / 0 (0) | 0 / 0 (0) |
| P3 | `abd38ee` | 39 / 27 (+12) | 176 / 0 (+176) inline | 0 / 0 (0) |
| P4 | `08d96f5` | 44 / 58 (-14) | 389 / 1 (+388) | 0 / 0 (0) |
| P5 | `6be1a26` | 70 / 163 (-93) | 170 / 1 (+169) | 0 / 0 (0) |
| P6 | `911fdc7` | 113 / 82 (+31) | 121 / 0 (+121) | 0 / 0 (0) |
| P7 | `f95a319` | 0 / 0 (0) | 82 / 0 (+82) | 0 / 0 (0) |

The parent-recounted final Rust range is `+2046 / -897`, net `+1149`. Baseline tracked
Rust was 87,685 physical lines in 64 files; final source is 88,834 lines in 65 files.
The new `src/queries.rs` is 207 physical lines and is included in that total. This
verification document is separate from Rust counts; no source files were purely moved.

Physical lines per changed Rust file (baseline → source tip) are:

| File | Base | Tip | Delta |
|---|---:|---:|---:|
| `src/agent.rs` | 1543 | 1482 | -61 |
| `src/agent/tests.rs` | 10801 | 11113 | +312 |
| `src/lib.rs` | 91 | 92 | +1 |
| `src/presentation.rs` | 1339 | 1246 | -93 |
| `src/presentation/tests.rs` | 459 | 628 | +169 |
| `src/queries.rs` | 0 | 207 | +207 |
| `src/read.rs` | 1191 | 1379 | +188 |
| `src/rpc/server.rs` | 1472 | 1210 | -262 |
| `src/rpc/server/tests.rs` | 6615 | 6847 | +232 |
| `src/store/tests.rs` | 4789 | 5193 | +404 |
| `src/store/tool_records.rs` | 1754 | 1806 | +52 |

The parent used `git diff --unified=0` with manual target ranges, not a parser or a claim
of syntax LOC. For each changed hunk, blank lines were counted separately; lines whose
trimmed text starts with `//` were comment-only; all other changed lines were classified
as code-containing. No changed block comments occurred. `src/read.rs` test scope was
counted from baseline `1066..EOF` and final `1078..EOF`, not by truncating at the first
`#[cfg(test)]`. Pure moves: 0. No tests were deleted; the two deleted lines are import
lines replaced by expanded imports. The Spec's conflict/idempotency expectation is not
used as baseline evidence; the actual unchanged conflict path is separately exercised
by `idempotent_commit_with_corrupt_existing_fails`.

| Final manual class | Added | Removed | Net |
|---|---:|---:|---:|
| Production physical | 685 | 895 | -210 |
| └ code-containing | 583 | 882 | -299 |
| └ comment-only | 87 | 10 | +77 |
| └ blank | 15 | 3 | +12 |
| Tests | 1295 | 2 | +1293 |
| `#[cfg(test)]` hooks | 66 | 0 | +66 |
| Tests + hooks | 1361 | 2 | +1359 |
| Rust total | 2046 | 897 | +1149 |

## Verification Status

At baseline `66f1834`, remote fmt/check and full tests passed: 743 passed / 0 failed /
2 existing ignored (`baseline-{fmt,check,tests}.log`). Final post-f95 closeout ran on
`root@192.168.20.199` with rustc/cargo 1.97.1 and MSRV 1.85.0, `CARGO_BUILD_JOBS=4`.
Both independent target directories started empty. Stable and MSRV each passed
756 tests / 0 failures / 2 existing ignored; the ignored tests require live providers.

| Gate / target | Command or disposition | Current record | Log |
|---|---|---|---|
| Format | `cargo fmt --all -- --check` | PASS | `final-fmt.log` |
| Stable check | `cargo check --locked` | PASS | `final-check.log` |
| Stable tests | `cargo test --locked --all-targets` | PASS: 756 / 0 / 2 | `final-tests.log` |
| Clippy | `cargo clippy --locked --all-targets -- -D warnings` | PASS | `final-clippy.log` |
| Rustdoc | `RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps` | PASS | `final-doc.log` |
| MSRV tests | `cargo +1.85.0 test --locked --all-targets` | PASS: 756 / 0 / 2 | `final-msrv.log` |
| Windows GNU | `cargo check --locked --all-targets --target x86_64-pc-windows-gnu` | PASS, compile-only; same 5 test-only warnings as baseline | `final-windows-check.log` |
| macOS cross | `cargo check --locked --all-targets --target x86_64-apple-darwin` | BLOCKED at 911fdc7 in `ring` C build (`-arch`/`-gfull`, no `clang`); not repeated after test-only P7 | `final-macos-check.log` |

Native Windows/macOS execution was not performed, CI was not triggered, and CI files are
unchanged. Logs are under `/root/minicore-agent-0917/logs` and copied to
`/tmp/minicore-agent-0917-evidence`. No local Cargo build/check/test was run.

## Residual Limits And Disposition

The previously reported cold ToolRef cancellation gap is closed by
`store::tests::cold_changes_diff_caller_cancel_keeps_store_owned_worker`: it commits a
real before/after snapshot, admits the DiffGate CPU worker through
`prepare_changes_diff(..., None, ...)`, cancels only the caller, observes `QueryLimit`
while the Store owner remains registered, then releases and joins it to zero. Remaining
limits are the blocked macOS cross-check, unexecuted native Windows/macOS and Live
Provider tests, and no performance claim. No new framework was introduced.

All final source gates refer to `f95a319c413bfc3f0311a0448117dfd14d2b2b54`;
the following P7 documentation commit changes only this record. Reproduce source
churn with `git diff --numstat 66f1834 f95a319 -- src tests` and whitespace validation
with `git diff --check 66f1834 HEAD`. Document physical lines are counted separately
with `wc -l docs/verification/0917-simplify.md`; its baseline was absent.
