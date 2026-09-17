# 0917 Nonbreaking Simplification

P0 records the compatibility surface and six work-package targets before any
production edit. Later sections record reviewed stages and remote verification.

## Source And Runtime

| Item | Value |
|---|---|
| Repository | `zqcli/minicore-agent` |
| Branch start | `dev` |
| Baseline SHA | `66f183470bd9c5bdb359b1c7666381135898861f` |
| minicore-agent | `0.3.3` |
| minicore-runtime | `0.4.1` |
| Runtime revision (fixed) | `6cd2bdbc634437dea925495c61c7eb0be10ba171` |
| MSRV / edition | `1.85` / `2024` |
| New dependencies | none (existing `futures-util` only) |

`minicore-agent-0917-spec.md` is an untracked user file at baseline; it must not
be added to any commit.

## Baseline Gate Results

All builds run on Linux `root@192.168.20.199`, in
`/root/minicore-agent-0917/source`; logs are in the adjacent `logs` directory.
Two inactive old Agent target directories were removed before building (about
10 GiB); no source, Runtime, active build, or user data was removed. The baseline
used a new `/root/minicore-agent-0917/target`, `CARGO_BUILD_JOBS=4`.
Tools: rustc 1.97.1 (8bab26f4f), cargo 1.97.1, git 2.47.3; GNU awk/wc for physical
counts. No local builds. Command set:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo +1.85.0 test --locked --all-targets
```

| Gate | Result |
|---|---|
| fmt | PASS (`baseline-fmt.log`) |
| check | PASS (`baseline-check.log`) |
| test (stable) | PASS: 743 passed, 0 failed, 2 existing ignored (`baseline-tests.log`) |
| clippy | Deferred to final gates |
| rustdoc | Deferred to final gates |
| test (1.85.0) | Deferred to final gates |
| Windows cross check | Not run |
| macOS cross check | Not run |

Known baseline note (from `0916-closeout.md`): the workspace pagination test
`workspace::status::diff_tests::workspace_pagination_refreshes_versions_and_rejects_changed_content`
was observed flaky once; not a baseline failure to attribute to this work.

## Compatibility Surface

### Public Rust re-exports (`src/lib.rs`)

All `pub use` items must remain. Inventory at baseline:

- agent: `Agent, AnswerInteraction, CompactSession, CreateSession, GetHistory,
  HistoryPage, PingResponse, RPC_CAPABILITIES, RPC_PROTOCOL_VERSION, ReadCursor,
  ReadItemChunk, ReadSession, ReadSessionResult, ReadTurnSummary, ReloadResult,
  RenameSession, SendMessage, SessionInfo, SessionState, SessionStatus,
  SessionUpdateResult, SteerMessage, TurnPersistence, TurnRef, TurnResult,
  TurnResultAvailability, TurnResultPage, TurnResultRequest, UpdateSession,
  agent_version`
- changes: `ChangeCommitState, ChangeCoverage, ChangeCursor, ChangeKind,
  ChangeListConsistency, ChangeListWarning, ChangeOrigin, ChangeRecord,
  ChangeRevision, ChangeScope, ChangesListRequest, ChangesListResult`
- compaction: `AutomaticCompactionObservation, AutomaticCompactionView,
  CompactionResult, CompactionStatus, CompactionUtilityUsage, RecoveryObservation`
- config: `AgentConfig, ApprovalMode, CompactionConfig, ConfigError, LoopOverrides, Profile`
- diff: `ChangesDiffRequest, DiffAvailability, DiffComparison, DiffCursor,
  DiffHunk, DiffLine, DiffLineKind, DiffResult`
- error: `AgentError, RuntimeErrorView`
- event: `AgentEvent, AgentEventStream, EventMeta, OutputChannel,
  ToolProgressView, ToolResultView`
- ids: `SessionId, SessionIdError`
- models: `ModelConfig, ModelInfo`
- presentation: `AssistantDisplayPart, PresentationView, ToolDisplay`
- profiles: `ProfileInfo`
- rpc: `run_stdio`
- sessions: `CompactionPhase, CompactionProgress, ContextBudget, LoopAccepted,
  SessionBlockReason, SessionContext, SteerAccepted, SummaryCoverage`
- tool_data: `CommandResult, CommandStatus, ToolDataAvailability, ToolDataStream,
  ToolExecutionData, ToolExecutionState, ToolInputSummary, ToolInvocationData,
  ToolOutputPage, ToolOutputRequest, ToolPhase, ToolProcessChunk, ToolProcessData,
  ToolReadRequest, ToolReadResult, ToolRecordingState, ToolRef, ToolSubject`
- workspace: `WorkspaceFileEntry, WorkspaceFilesRequest, WorkspaceFilesResult,
  WorkspaceListCursor, WorkspaceReadEncoding, WorkspaceReadRange,
  WorkspaceReadRequest, WorkspaceReadResult, WorkspaceReadStatus,
  WorkspaceFileKind, WorkspaceScanConsistency, WorkspaceScanStop,
  WorkspaceByteRange, WorkspaceSearchCursor, WorkspaceSearchMatch,
  WorkspaceSearchRequest, WorkspaceSearchResult, WorkspaceStatusEntry,
  WorkspaceStatusEntryKind, WorkspaceStatusRequest, WorkspaceStatusResult,
  WorkspaceStatusWarning, Workspace, WorkspaceError`

### RPC methods (`src/rpc/server.rs` dispatch + `canonical_method`)

`canonical_method` maps exactly these; unknown methods return `"unknown"`:

```
agent.ping / agent.reload / agent.shutdown
profile.list / model.list
session.list / session.create / session.open / session.close / session.delete
session.state / session.context / session.compact / session.compact.cancel
session.update / session.rename / session.history / session.read
session.presentation
workspace.read / workspace.files / workspace.search / workspace.status
changes.list / changes.diff
tool.read / tool.output
turn.send / turn.steer / turn.cancel / turn.wait / turn.result
interaction.answer
```

No public `Agent` method may be removed or renamed; the list above is the
`src/rpc/server.rs` dispatch set and is the compatibility contract.

### Events (`src/event.rs`)

`EventMeta { session_id, loop_id, dropped_before }` and `AgentEvent` variants:
`SessionOpened, SessionClosed, SessionState, TurnStarted, RequestStarted,
RequestUsage, OutputDelta, ToolStarted, ToolInvocation, ToolExecution,
ToolProcess, ToolPresentation, ToolProgress, ToolFinished, SteerProgress,
InteractionRequested, InteractionResolved, TurnFinished`. Event names, payload
fields, and best-effort semantics stay; no field removed or renamed.

### RPC wire DTOs (`src/rpc/protocol.rs`)

`RpcRequest, RpcId, RpcResponse, RpcError, RpcErrorData, RpcOutbound,
AgentEventNotification` plus per-method param/result structs (`EmptyParams,
SessionCreateParams, SessionParams, SessionCompactParams, SessionHistoryParams,
SessionReadParams, TurnResultParams, ToolReadParams, ToolOutputParams,
SessionUpdateParams, SessionRenameParams, TurnSendParams, TurnParams,
TurnSteerParams, InteractionAnswerParams, InteractionAnswerWire,
ApprovalDecisionWire, ProfilesResult, ModelsResult, SessionsResult, SessionResult,
SessionUpdateResult, TurnResult, SteerResult, CancelledResult, OkResult`).
Error `code`/`kind`/`retryable` and null/omission rules stay unchanged.

## Work Packages And Target Functions

### N1 — Shared query orchestration (`src/agent.rs`, `src/rpc/server.rs`, new `src/queries.rs`)

| Entry | Duplicated sites at baseline |
|---|---|
| `workspace.status` | `Agent::workspace_status` (agent.rs:599), RPC branch (server.rs:580) |
| `changes.list` | `Agent::changes_list_with_cancellation` (agent.rs:626), RPC branch (server.rs:620) |
| `changes.diff` | `Agent::changes_diff_with_cancellation` (agent.rs:712), RPC branch (server.rs:741) |

Session-owned workers to preserve: `Session::spawn_status_query`,
`spawn_status_query_with_deadline` (sessions.rs:928/938),
`spawn_workspace_diff` (sessions.rs:979), `Session::complete_status_query`
(sessions.rs:902). Prepare must register the owned worker synchronously and
return a wait-only future; do not push registration into a polled async block.

### N2 — RPC boilerplate (`src/rpc/server.rs`)

- `dispatch` (server.rs:229) → private `dispatch_inner -> Result<Dispatch, RpcResponse>`,
  preserving `canonical_method` log position.
- Narrow `defer_query` helper wrapping only response plumbing; no business
  timeout, no second JoinSet.
- Preserve `MAX_DEFERRED_WAITERS` (server.rs:48), `query_capacity_available`
  (server.rs:1161), `params_or_error` (server.rs:1229), `query_error`
  (server.rs:1287), `agent_error` (server.rs:1295).

### N3 — Turn page assembly (`src/read.rs`)

- `live_turn_page` (read.rs:553), `stored_turn_page` (read.rs:613): share cursor
  validation, slice, timestamp array, `encoded_items`, `pack_items`, final page.
- Preserve `encoded_items` (read.rs:666), `pack_items` (read.rs:690),
  `largest_fitting_prefix` (read.rs:805), `sanitize_history`/budgeted cleaners,
  Live vs Stored metadata (`availability`, `persistence`, `completed_at`).

### N4 — Auxiliary blob validation/write (`src/store/tool_records.rs`)

- `Store::commit_tool_record_locked` (tool_records.rs:38): length+hash checks for
  input/result/stdout/stderr, then six optional blob writes in fixed order.
- Preserve `validate_file_change_snapshot` (tool_records.rs:1501), the 3 MiB
  raw cap, `reserve`, `aux_lock`, temp-dir cleanup, `write_sync_file`,
  `sync_directory`, rename ordering, idempotent/inconsistent handling.

### N5 — Tool wrapper (`src/presentation.rs`, `src/tools/mod.rs`)

- `PresentationTool` 5-variant enum (presentation.rs:702), getters `inner`,
  `presentation`, `observer`, `impl Tool::execute` (presentation.rs:824),
  `finish_presentation_tool` (presentation.rs:953) called once per branch.
- Constructors `new_with_observer` / `new_bash_with_observer` /
  `new_write_with_observer` / `new_edit_with_observer` /
  `new_apply_patch_with_observer`; call sites in `src/tools/mod.rs` (lines
  102, 115, 135, 149, 167).
- Identity captured synchronously at execute boundary; `ToolRef` passed down.

### N6 — Session assembly (`src/agent.rs`)

- `Agent::create_session` (agent.rs:830) and `Agent::open_session` (agent.rs:914):
  shared `SessionSetup`-shaped assembly plus install; keep I/O and error order,
  `Session::new`, `SessionOpened` send, one settings-binding install.

## Suggested Baseline Tests And Statistics

Existing tests usable as the first regression net (names as in the tree):

| Area | Tests |
|---|---|
| status/status cache | `i1_explicit_status_projects_and_failed_status_clears_branch_cache`, `an_owned_status_worker_outlives_its_awaiter_until_the_session_closes`, `workspace_status_projects_the_shared_presentation_branch_and_clears_failures`, `workspace_status_answers_for_the_session_workspace`, `closing_a_session_stops_pending_status_queries_and_frees_capacity` |
| changes list/diff | `changes_list_separates_workspace_session_and_turn_scopes`, `changes_list_deferred_query_shares_pool_and_ping_stays_available`, `changes_list_queries_share_the_total_waiter_ceiling`, `changes_diff_public_rpc_compares_native_before_and_after`, `changes_diff_public_rpc_pages_a_long_line_losslessly`, `changes_diff_shares_the_four_query_slots_and_joins_on_shutdown` |
| RPC framing/capacity | `fragmented_frame_survives_deferred_waiter_interleaving`, `read_frame_accumulates_across_a_cancelled_poll`, `read_frame_limit_covers_cumulative_length_after_cancellation`, `deferred_waiter_limit_rejects_new_waiters_but_keeps_control_methods`, `deferred_query_capacity_is_four_with_shared_total_limit` |
| cold read / pages | `bash_dual_stream_cold_read_closure_after_restart_without_session_loaded`, `cold_read_is_strictly_read_only_and_survives_missing_workspace_and_partial_history`, `fragments_reconstruct_escaped_unicode_json_without_loss`, `page_budget_includes_metadata_and_rejects_too_small_pages`, `cursor_must_end_on_a_utf8_boundary` |
| aux blobs | `atomic_publishing_idempotent_overwrite_and_cleanup_on_failure`, `metadata_and_blob_corruption_handling`, `invalid_metadata_ranges_and_hashes_rejected`, `temp_dir_removal_failure_rejects_aux_commit`, `file_change_auxiliary_round_trip_and_missing_blob_degrade_details` |
| tool wrapper | `live_tool_presentation_and_result_keep_runtime_identity`, `f1_native_write_changes_keep_the_first_request_identity`, `i2_04_bash_owned_command_matches_events_queries_and_the_close_join`, `a_reused_call_id_in_a_new_loop_gets_its_own_identity`, `result_truncation_marks_display_and_bounds_hidden_rows` |
| create/open | `create_and_open_do_not_start_agent_loop`, `create_open_history_and_send_wait_deferred`, `f2_03_close_and_reopen_rederives_installed_future_options`, `legacy_six_tool_session_is_listable_renameable_but_not_openable`, `reopen_rejects_missing_workspace_and_keeps_type_identity` |

Statistics caliber (from Spec §4.2/§15.2):

1. Source physical lines per file (`wc -l`), all platforms.
2. Production change: exclude standalone test files and `#[cfg(test)]` items;
   state comment/blank-line handling explicitly. Do not truncate a file at its
   first `#[cfg(test)]`.
3. Report test/test-hook and doc/move changes separately.
4. Churn via `git diff --numstat "$BASE" HEAD -- src tests`; classification is a
   heuristic, never called net production LOC. No new dependency or Rust parser.

Test counts sum the real `test result:` lines (library and each integration
binary): baseline re-measured 743 passed / 0 failed / 2 existing ignored.
Baseline tracked Rust files under `src` and `tests`: 87,685 physical lines,
including inline tests, comments and blanks (not production LOC).

## Risks To Watch (P0 view)

- Query prepare must register the Session-owned worker synchronously; a deferred
  registration can reorder close/shutdown and break I2 ownership.
- Capacity check must stay before any worker start; keep error precedence
  (missing Session vs resource exhausted) per entry.
- Live vs Stored page metadata and byte-offset cursor semantics must not merge;
  JSON-envelope byte budget, not body length.
- `None` vs `Some(&[])` blob distinction; 3 MiB cap, cleanup path, rename/sync
  order unchanged.
- `PresentationTool` identity is captured at the synchronous execute boundary,
  not re-derived inside the async block; `finish` exactly once, no fake finish
  on cancellation.
- create must fail all predictable config before `Store::create_session`; open
  keeps two removed-tool checks and workspace identity check order.
- Do not commit the untracked `minicore-agent-0917-spec.md`; do not touch
  `Cargo.toml`/`Cargo.lock`/version or Storage schema.
