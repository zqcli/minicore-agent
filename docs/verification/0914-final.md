# 0914 Blueprint Acceptance Map

This maps the blueprint's §12 acceptance checklist (30 items) to the source
tests that exercise each one. It is an evidence index, not a new gate: parent
remote runs remain the acceptance, and this file must not be read as claiming a
local pass. Final remote acceptance passed 749 tests on both stable and Rust
1.85, with 2 Live Provider tests ignored. Strict Clippy, fmt, rustdoc and Windows
GNU/macOS all-target compilation passed. Logs are under
`/root/minicore-agent-0914/logs/p7-{tests,msrv,clippy,fmt,doc,windows,macos}.log`.
Native Windows/macOS execution and Live Provider tests were not performed.

Source: `docs/archive/specs/minicore-agent-0914-dev-spec.md` §12 (30 items, grouped 5/9/7/9).
Agent branch: `feat/0914-shared-data`; P6a `fd08b2b`, tool diff `c6cb248`,
Workspace diff `023e34a`. The P7 process-client test and this acceptance map are
committed together. Agent remains 0.3.3; Runtime remains 0.4.1 pinned to
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. No installation or push was performed.

## Session and RPC

1. Session/mode/profile change still reads old sessions —
   `rpc::server::tests::session_read_pages_are_bounded_lossless_and_do_not_need_workspace`,
   `agent::tests::reopened_session_projects_external_summary_as_bounded_user_data`.
2. read never creates dirs, truncates JSONL, updates metadata, starts a Model —
   `rpc::server::tests::session_read_reports_incomplete_tail_and_corrupt_middle_without_repair`,
   `store::tests::readonly_history_scan_honors_cancel_deadline_and_byte_budget`.
3. Appending tail not repaired; cross-page limited to a captured prefix —
   `rpc::server::tests::loaded_session_read_keeps_a_captured_prefix_across_append`,
   `rpc::server::tests::loaded_session_read_does_not_expose_a_disk_tail`.
4. Live event lost + JSONL append failed still yields turn.result —
   `rpc::server::tests::turn_result_reads_failed_live_report_without_history`.
5. Half-frame + deferred waiter interleave loses no prefix; encoded byte cap —
   `rpc::server::tests::fragmented_frame_survives_deferred_waiter_interleaving`,
   `rpc::server::tests::read_frame_limit_covers_cumulative_length_after_cancellation`.

## Compaction

6. Manual compaction calls no-tools raw Model; byte/order unchanged —
   `rpc::server::tests::manual_compaction_generates_no_tools_summary_and_reopens_atomic_snapshot`.
7. Summary anchor validated; corrupt/expired does not block session.read —
   `agent::tests::invalid_external_summary_falls_back_to_full_history`,
   `agent::tests::invalid_summary_does_not_mask_core_history_corruption`.
8. Over-limit history still yields a valid LoopRequest after summary —
   `agent::tests::valid_summary_allows_history_over_runtime_item_limit_to_start`,
   `agent::tests::valid_summary_allows_history_over_runtime_byte_limit_to_start`.
9. Pre-trimmed startup history is not re-trimmed by index —
   `agent::tests::snapshot_with_same_count_different_loaded_history_is_ignored`,
   `agent::tests::projected_summary_keeps_suffix_tool_exchange_and_current_user`.
10. Every request computes budget; huge ToolResult/small window/steer covered —
    `agent::tests::auto_compaction_recovers_from_runtime_bytes_with_huge_tool_results`,
    `agent::tests::automatic_request_compaction_rederives_after_a_smaller_model_update`.
11. Current user/steer kept, tool pairs intact, no extra user/side effects —
    `agent::tests::automatic_request_compaction_preserves_a_steer_alongside_tool_summary`,
    `agent::tests::projected_summary_keeps_suffix_tool_exchange_and_current_user`.
12. ContextOverflow+NotStarted recovers once; Started/Unknown/second refuses —
    `compaction::recovery::tests::recovery_ticket_claims_once_per_logical_request`,
    `compaction::recovery::tests::recovery_high_water_blocks_reentry_and_older_indices`.
13. Manual/auto/recovery cancellable; no utility worker leak; no-progress fails —
    `rpc::server::tests::manual_compaction_cancel_is_exact_and_keeps_ping_responsive`,
    `agent::tests::automatic_startup_total_deadline_cancels_the_utility_worker`.
14. Utility usage stays separate; estimates do not claim exact metering —
    `rpc::server::tests::manual_compaction_reports_independent_utility_usage`,
    `rpc::server::tests::manual_compaction_aggregates_usage_across_multiple_utility_calls`.

## Workspace and changes

15. files/search ignore + path scope; symlink/parent/repo boundary respected —
    `workspace::listing::tests::ignore_rules_and_git_metadata_are_respected`,
    `workspace::listing::tests::symlinks_are_listed_but_never_followed_or_escaped`,
    `workspace::search::tests::paths_restrict_the_search_and_must_resolve`.
16. read returns raw data, not rendered lines; non-UTF-8/oversize/race explicit —
    `rpc::server::tests::workspace_read_query_serves_raw_content_and_rejects_invalid_requests`,
    `workspace::query::tests::binary_files_are_reported_without_content`,
    `workspace::query::tests::revision_mismatch_reports_changed_without_content`,
    `workspace::query::tests::pagination_is_lossless_for_long_unicode_and_control_lines`.
17. scan_incomplete does not fake a global total; ping/cancel still served —
    `workspace::search::tests::a_record_that_cannot_fit_even_an_empty_page_is_reported_incomplete`,
    `rpc::server::tests::pending_workspace_scans_do_not_block_ping_or_shutdown`.
18. status distinguishes index/worktree/untracked/conflict; no Git is not clean —
    `workspace::status::tests::staged_unstaged_and_untracked_entries_are_counted`,
    `workspace::status::tests::a_conflict_is_reported_as_unmerged`,
    `workspace::status::tests::a_non_repository_is_reported_conservatively`,
    `workspace::status::tests::a_missing_git_executable_is_reported_as_unavailable`.
19. Pre-existing user changes stay out of turn scope; no unfounded Bash attribution —
    `tools::write::tests::bound_write_records_exact_user_dirty_before_and_after`,
    `changes::tests::workspace_records_never_attribute_a_tool`.
20. edit/write/patch preview/applied/unknown separated; aux failure no redo —
    `tools::write::tests::bound_write_preserves_not_committed_and_post_rename_unknown_states`,
    `tools::edit::tests::bound_edit_records_a_conflict_when_the_target_changes_before_commit`,
    `tools::apply_patch::tests::bound_patch_records_the_same_source_and_computed_result`.
21. Same-file race or broken version chain returns partial/conflict —
    `workspace::status::diff_tests::workspace_pagination_refreshes_versions_and_rejects_changed_content`,
    `store::tests::diff_metadata_conflict_keeps_the_valid_warm_side`.

## Bash and tool data

22. stdout then stderr visible before exit, no newline required —
    `tools::command::tests::a_chunk_is_stored_before_it_is_observed_and_while_the_command_runs`
    (stdout chunk authoritative with no terminal record, stderr written later),
    `tools::bash::tests::nonzero_exit_stdout_and_stderr_are_a_completed_outcome`.
23. Over pipe capacity / high output / no consumer still drains —
    `tools::bash::tests::simultaneous_large_streams_are_drained_bounded_and_marked`.
24. Multibyte across chunk, invalid UTF-8, ANSI, binary do not break RPC/offsets —
    `tools::bash::tests::unicode_and_binary_bytes_are_reported_exactly`.
25. gap/evicted window/missing aux explicit; restart reads retained results —
    `tool_data::tests::an_evicted_stream_window_anchors_at_the_observed_end`,
    `tool_data::tests::an_abandoned_stream_reports_truncated_and_eof_not_an_endless_page`,
    `store::tests::file_change_auxiliary_round_trip_and_missing_blob_degrade_details`,
    `agent::tests::bash_dual_stream_cold_read_closure_after_restart_without_session_loaded`.
26. exit_code/signal/spawn failure/timeout/cancel never fake success 0 —
    `tools::bash::tests::signal_exit_uses_unavailable_exit_code`,
    `tools::bash::tests::timeout_kills_and_reaps_the_direct_child`,
    `tools::command::tests::a_spawn_failure_is_reported_without_a_process`,
    `tools::bash::tests::a_nonzero_exit_is_a_completed_result_not_an_error`.
27. Cancel shows cancelling then confirmed terminal; child process tests —
    `tools::command::tests::cancelling_records_cancelling_then_a_confirmed_group_end`,
    `tools::bash::tests::cancellation_kills_and_reaps_the_direct_child`.
28. Runtime drops the Tool future; the owner still cancels and joins the worker —
    `tools::command::tests::a_turn_join_stops_and_reaps_a_command_whose_future_was_dropped`,
    `tools::bash::tests::a_dropped_execute_future_still_joins_its_owned_command`.
29. read/write fast paths make no fake percentage, keep atomic writes —
    `tools::write::tests::output_is_prevalidated_before_workspace_mutation`,
    `tools::edit::tests::atomic_failures_and_unknown_outcomes_never_report_success`,
    `tools::edit::tests::input_source_and_result_limits_are_enforced_before_allocation_or_write`.
30. One data source serves a rendering-free client and would serve any renderer —
    `tests/shared_data_clients.rs` (`HunkClient` and `LineClient`), which drive one
    Agent over a single stdio connection and reconstruct the same sides. Neither
    is a real GUI/TUI implementation; the test asserts the public RPC contract only.

## Notes

- P6b2 workspace diffs are covered by
  `workspace::status::diff_tests` (staged/unstaged/untracked, explicit
  comparisons, refresh-stale, nested literal path, unborn/deletion, conflict,
  symlink, real Git child cancel/reap) and the cross-cutting
  `changes::tests::workspace_records_never_attribute_a_tool`.
- Items 19, 22, 26, and 28 cite the specific test body that proves the claim
  (user-dirty captured before with no native attribution; a live chunk stored
  while the command runs; spawn failure / signal / timeout; a dropped execute
  future whose owner still joins). Name similarity alone was treated as
  insufficient.
- Item 30 cites `tests/shared_data_clients.rs`, where `HunkClient` and
  `LineClient` are two consumers on a single stdio RPC connection, not two
  independent clients and not a real GUI/TUI. The blueprint excludes
  multi-client synchronization, and this test does not assert any.
- No local run is recorded here. Parent remote logs supply final counts and the
  stable/MSRV gate results.
