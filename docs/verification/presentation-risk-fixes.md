# Presentation Path Risk Fixes

Source follow-up to Agent 0.3.3, starting from `783ac87`. Each fix was reviewed
and committed independently. Implementation and independent review used separate
`gpt-5.6-luna:max` sessions; the parent verified the code, remote checks and commits.

## Changes

| Commit | Fix |
| --- | --- |
| `c50f1e7` | Malformed `user_times` values degrade without blocking valid core history load/append. |
| `0f617e0` | Git branch queries use a fixed one-second cooperative deadline, null stdin and child termination on drop. |
| `731093b` | Tool-result collection is limited to page-relevant calls and the adjacent result batch, with borrowed full-identity hash lookup. |

Production changes are confined to `src/store.rs`, `src/workspace.rs` and
`src/history.rs`. No new dependencies, configuration fields, persistent cache or
index were introduced. Runtime remains 0.4.1 at
`6cd2bdbc634437dea925495c61c7eb0be10ba171`. TUI source and the RPC surface are unchanged.

Store parsing remains strict for unknown, duplicate and wrongly typed core
fields. Invalid timestamp entries retain their occurrence as unknown; excess
entries make the whole time array unknown. Complete old JSONL is not rewritten.
Metadata is dropped if it alone pushes an append over the existing line limit;
core records exceeding the limit are still rejected.

The pagination window follows the pinned Runtime's append order: Assistant,
then its contiguous ToolResult batch. Matching retains `(LoopId, request_index,
ToolCallId)`. Result truncation and diagnostic redaction remain unchanged.

## Verification

All Rust builds, formatting and tests ran on the authorized Linux builder or
GitHub Actions. No local Rust toolchain was run. Tests used isolated temporary
stores and workspaces, dummy keys and loopback providers, never user history or
real upstream requests.

| Stage | Linux stable all-targets | Linux Rust 1.85.0 all-targets | Focused tests |
| --- | --- | --- | --- |
| Store | 289 passed, 2 ignored | 289 passed, 2 ignored | 33 Store tests |
| Branch | 292 passed, 2 ignored | 292 passed, 2 ignored | 4 branch tests |
| Pagination / closeout | 296 passed, 2 ignored | 296 passed, 2 ignored | 6 history tests |

Totals include library and integration tests. Strict Clippy (`-D warnings`),
format checking and warning-denied rustdoc passed on stable Rust 1.97.1. The parent
also ran all **16 TUI Agent E2E tests** against the freshly built real Agent;
all passed. The separate Agent gate also passed its four existing RPC process
tests. Existing ignored tests were not added to or used to hide failures.

Red-capable evidence included invalid time metadata rejecting append/load,
metadata alone exceeding the line limit, a delayed real query child exceeding
the branch budget, and full-history result scans. Tool-result scan regressions
changed **129 to 1** for a small Assistant page and **128 to 0** for a page with
no tool calls. A two-call batch beyond the page boundary scans exactly two
results; the closeout also asserts that both results contribute to the display.
These are component work-count invariants, not end-to-end timing claims.

Source CI runs (Windows, Linux, macOS, MSRV and quality):

- [Store: 34172543900](https://github.com/zqcli/minicore-agent/actions/runs/34172543900)
- [Branch: 34174650624](https://github.com/zqcli/minicore-agent/actions/runs/34174650624)
- [Pagination: 34176987662](https://github.com/zqcli/minicore-agent/actions/runs/34176987662)

## Boundaries

- The Git deadline is cooperative: it cannot preempt synchronous OS work in a
  future's poll. `kill_on_drop` requests termination of the owned direct child,
  not arbitrary descendants or a process stuck in an uninterruptible OS state.
- User-occurrence prefix counting remains proportional to the page offset.
  This change removes global tool-result processing, not all history-sized work.
- JSON syntax, structural validation and existing storage limits remain strict;
  this is display-value degradation, not general corruption repair.
- Package versions remain Agent 0.3.3 / TUI 0.2.8. Installed local executables were
  not replaced, and no user processes were restarted. A matching version string
  does not establish that an older executable contains these source fixes.
