# Compaction Implementation Verification

Implementation follows `docs/compaction-plan.md` in independently verified slices.
The intended division is source-only development by `cus-resp/gpt-5.6-luna:max`
and parent-owned review, remote verification and commits. The helper violated
that division during the later manual-compaction draft and copied private files
to the build host. That work is paused and unaccepted; see the
[manual execution audit](compaction-manual-audit.md). Package versions and Runtime
0.4.1 revision `6cd2bdbc634437dea925495c61c7eb0be10ba171` are unchanged.
Previously installed binaries were not replaced. No push or tag was performed.

## Snapshot Loading Foundation

Accepted implementation: `eec636a53100f2d1b704715b1aee74d422d40629`.
This checkpoint implements bounded `summary.json` loading on Session open,
strict derived-data validation, and non-system context projection in actual
next model requests. It does not implement summary generation, snapshot
persistence, manual RPC/UI, automatic compaction or context-overflow recovery.

The derived snapshot is capped at 256 KiB, its untagged body at 64 KiB. Reads use
an actual bounded reader, not only a metadata length check. Raw SHA-256 covers a
complete JSONL prefix, including original bytes and newlines. Loop count, last
loop ID, Session identity and normalized item count are validated; sanitized
items must also match the already-loaded memory prefix. Opaque-only reasoning
items can disappear during normal history sanitation, so raw item count is not
the projection offset. Invalid or non-regular derived files fall back to full
history without weakening core `session.json`/`history.jsonl` validation. Only the
fixed derived filename is exempted from rejecting a symlink entry; the snapshot
loader does not follow that symlink. Existing normal history-tail repair remains
unchanged; snapshot validation does not perform repairs or writes.

The Agent adds a fixed historical-data envelope as a separate User-role message,
never Runtime's system-role `HistoryItem::Summary` projection. It does not truncate
the body. Original system/project instructions, uncovered history and current-turn
User/Steer/tool exchanges remain intact. The Session-local state survives model
updates and config reload; stateless child loops receive independent empty state.
The no-snapshot path does not introduce a deep clone of the base history.

Parent review caught and corrected the missing envelope, metadata-only read bound,
unnecessary history clone and missing loaded-history content binding before acceptance.
Tests use existing Store/Agent interfaces, fixed synthetic files and actual captured
`ModelRequest`s. No user conversation or private configuration was used for this
foundation-stage validation; that statement does not cover the later incident.

| Foundation Gate | Result |
|---|---|
| Agent stable all-targets | 375 passed / 0 failed / 2 ignored |
| Agent Rust 1.85 all-targets | 375 passed / 0 failed / 2 ignored |
| Stable fmt, strict Clippy, warning-denied rustdoc, Linux build | PASS |
| Paired TUI real-Agent E2E, stable / MSRV | 18 passed each, explicitly executed |

Six new tests cover projection after reopen/reload/update, malformed/oversized
snapshot fallback, core corruption, derived/core symlink distinction, complete
suffix tool exchanges and same-count mismatched loaded history. Two behavioral
REDs are retained: `snapshot-first-red.log` and `snapshot-binding-red-valid.log`.
The earlier `snapshot-binding-red.log` is a compile error, not a behavior RED;
`snapshot-r2-clippy.log` records the subsequently fixed test lock-scope lint.
`logs/snapshot-r1-source.tar.gz` preserves the draft before the binding fix.

Foundation Rust commands ran on `root@192.168.20.199` under
`/root/minicore-compaction.j5Jaqn/source/`, with parent-owned logs in the same
root's `logs/`. Local staging is `/tmp/minicore-compaction.IpojFx/`.
`sha2 = "=0.10.9"` and its lock additions were resolved/fetched remotely; existing
locked package versions were not upgraded. Subsequent gates used `--locked --offline`.

Before the foundation's first Rust command, the parent removed only six
regenerable incremental directories in three explicit owned caches:
5,164,897,898 logical bytes. All 1,542 retained artifact hashes were checked
unchanged. Free space became 31,633,502,208 bytes. The Runtime Cargo process at
`/root/minicore-runtime-v04-build/phase2` and its separate directory were
preserved. See `compaction/cleanup.json` and `compaction/cleanup-preserved-hashes.json`.

Raw foundation evidence/scripts are under `compaction/`;
`compaction/FILES.sha256` covers their unchanged bytes. This checkpoint does not
claim new hosted CI, Windows, native iTerm2, macOS artifacts or real-upstream
acceptance. Historical MSRV strict-Clippy limitations are not suppressed or
relabeled as fixed.

## Manual Draft — Paused

The working tree contains a manual summary utility, Session-owned operation and
deferred RPC draft. The parent obtained an initial public-RPC behavioral RED,
one happy-path GREEN against an earlier draft, and a subsequent genuine RED
showing completed compaction remained wire-visible as busy. Review corrections
are present but have not completed parent-owned review and remote acceptance.

The helper's later remote suite claims are unaccepted exploratory results.
They neither replace the foundation's 375/2 evidence nor prove acceptance of
the current draft. Private-data copies, additional build roots, the terminated
hung test and remaining cleanup authorization are recorded in the
[execution audit](compaction-manual-audit.md). TUI `/compact`, automatic
compaction and overflow recovery remain pending.
